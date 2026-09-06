// ===== Data Explorer: browse (seek-based), live tail (SSE), produce =====
import { get, post, clusterPath, loadTopics } from '../api.js';
import {
  esc, fmtNum, fmtTs, fmtRel, badge, spinner, toast, modal, jsonBlock, previewValue, copyText,
} from '../ui.js';

let topicOptions = [];
let tailTimer = null;

export async function renderExplorer(view, query = {}) {
  window.__tailClose?.();
  window.__tailClose = null;
  if (tailTimer) { clearInterval(tailTimer); tailTimer = null; }
  view.innerHTML = spinner();
  const tab = query.tab || 'browse';
  topicOptions = await loadTopics().catch(() => []);
  const selected = query.topic || topicOptions[0]?.name || '';

  view.innerHTML = `
    <div class="toolbar" style="align-items:center">
      <h1 style="margin:0">Data Explorer</h1>
      <div class="spacer" style="flex:1"></div>
      <div class="field" style="min-width:260px">
        <label>Topic</label>
        <select id="ex-topic">
          ${topicOptions.map((t) => `<option value="${esc(t.name)}" ${t.name === selected ? 'selected' : ''}>${esc(t.name)}</option>`).join('')}
        </select>
      </div>
    </div>
    <div class="btn-row mb16" id="ex-tabs">
      <button class="btn ${tab === 'browse' ? 'primary' : 'ghost'}" data-tab="browse">Browse</button>
      <button class="btn ${tab === 'tail' ? 'primary' : 'ghost'}" data-tab="tail">Live tail</button>
      <button class="btn ${tab === 'produce' ? 'primary' : 'ghost'}" data-tab="produce">Produce</button>
    </div>
    <div id="ex-panel"></div>`;

  const panel = view.querySelector('#ex-panel');
  const topicSelect = view.querySelector('#ex-topic');
  let partitionCount = topicOptions.find((t) => t.name === topicSelect.value)?.partitions ?? 1;

  const show = (which) =>
    ({ browse: renderBrowse, tail: renderTail, produce: renderProduce })[which](panel, topicSelect, () => partitionCount);

  view.querySelector('#ex-tabs').addEventListener('click', (e) => {
    const btn = e.target.closest('[data-tab]');
    if (!btn) return;
    view.querySelectorAll('#ex-tabs .btn').forEach((b) => b.classList.toggle('primary', b === btn));
    show(btn.dataset.tab);
  });

  topicSelect.addEventListener('change', () => {
    partitionCount = topicOptions.find((t) => t.name === topicSelect.value)?.partitions ?? 1;
  });

  await show(tab);
}

function partitionOptions(count, selected) {
  return `<option value="">all</option>` +
    Array.from({ length: count }, (_, i) => `<option value="${i}" ${String(i) === selected ? 'selected' : ''}>${i}</option>`).join('');
}

// ---------------- Browse ----------------

let producePrefill = null; // set by "Reproduce" → consumed by the produce tab

function renderBrowse(panel, topicSelect, getPartitions) {
  let all = []; // accumulated across "Load more"
  const lastOffsets = {}; // partition -> last seen offset + 1
  let reachedEnd = false;

  panel.innerHTML = `
    <div class="card">
      <div class="toolbar">
        <div class="field"><label>Partition</label><select id="br-partition">${partitionOptions(getPartitions())}</select></div>
        <div class="field"><label>Start</label>
          <select id="br-start">
            <option value="earliest">earliest (first messages)</option>
            <option value="latest">latest (last messages)</option>
            <option value="offsets">specific offsets…</option>
            <option value="timestamp">from timestamp…</option>
          </select></div>
        <div class="field" id="br-offsets-wrap" style="display:none"><label>Offsets <span class="hint">{"0": 123}</span></label>
          <input type="text" id="br-offsets" placeholder='{"0": 0}' style="width:150px" /></div>
        <div class="field" id="br-ts-wrap" style="display:none"><label>From timestamp</label>
          <input type="datetime-local" id="br-ts" style="width:190px" /></div>
        <div class="field"><label>Limit</label><input type="number" id="br-limit" value="50" min="1" max="1000" style="width:90px" /></div>
        <div class="field"><label>Key contains</label><input type="text" id="br-key" style="width:120px" /></div>
        <div class="field"><label>Value contains</label><input type="text" id="br-value" style="width:140px" /></div>
        <div class="field"><label>&nbsp;</label><button class="btn primary" id="br-go">Browse</button></div>
      </div>
      <div id="br-status" class="small muted"></div>
    </div>
    <div class="card" id="br-results"><div class="empty-state">Run a query to see messages</div></div>`;

  const start = panel.querySelector('#br-start');
  start.addEventListener('change', () => {
    panel.querySelector('#br-offsets-wrap').style.display = start.value === 'offsets' ? '' : 'none';
    panel.querySelector('#br-ts-wrap').style.display = start.value === 'timestamp' ? '' : 'none';
  });
  panel.querySelector('#br-go').addEventListener('click', () => runBrowse());
  panel.querySelector('#br-limit').addEventListener('keydown', (e) => { if (e.key === 'Enter') runBrowse(); });

  function renderResults() {
    const results = panel.querySelector('#br-results');
    results.innerHTML = all.length === 0
      ? '<div class="empty-state">No messages matched</div>'
      : `<div class="table-wrap"><table class="tbl msg-table">
          <thead><tr><th class="num">Partition</th><th class="num">Offset</th><th>Time</th><th>Key</th><th>Value</th><th></th></tr></thead>
          <tbody>${all.map((m, i) => `
            <tr class="clickable" data-msg="${i}">
              <td class="num">${m.partition}</td>
              <td class="num mono">${m.offset}</td>
              <td class="small muted">${fmtRel(m.timestamp)}</td>
              <td class="mono small">${esc(m.key ?? 'null')}</td>
              <td class="value-cell">${previewValue(m.value)}</td>
              <td>${codecChip(m)}${Object.keys(m.headers || {}).length ? badge(Object.keys(m.headers).length + ' hdr', 'cyan') : ''}</td>
            </tr>`).join('')}</tbody>
        </table></div>
        ${reachedEnd ? '' : `<div class="btn-row mt8" style="justify-content:center">
          <button class="btn ghost sm" id="br-more">Load more…</button></div>`}`;
    results.querySelectorAll('tr[data-msg]').forEach((tr) => {
      tr.addEventListener('click', () => messageDetailModal(all[Number(tr.dataset.msg)]));
    });
    results.querySelector('#br-more')?.addEventListener('click', loadMore);
  }

  async function loadMore() {
    const more = panel.querySelector('#br-more');
    if (more) { more.disabled = true; more.textContent = 'Loading…'; }
    await fetchPage({ start: 'offsets', offsets: { ...lastOffsets } }, false);
  }

  async function runBrowse() {
    const startMode = start.value;
    const body = {
      partition: panel.querySelector('#br-partition').value === '' ? null : Number(panel.querySelector('#br-partition').value),
      start: startMode,
      limit: Number(panel.querySelector('#br-limit').value) || 50,
      timeoutMs: 8000,
      keyContains: panel.querySelector('#br-key').value.trim() || null,
      valueContains: panel.querySelector('#br-value').value.trim() || null,
    };
    if (startMode === 'offsets') {
      try { body.offsets = JSON.parse(panel.querySelector('#br-offsets').value || '{}'); }
      catch {
        panel.querySelector('#br-status').textContent = 'Offsets must be JSON like {"0": 123}';
        return;
      }
    }
    if (startMode === 'timestamp') {
      const v = panel.querySelector('#br-ts').value;
      body.timestamp = v ? new Date(v).getTime() : 0;
    }
    all = [];
    Object.keys(lastOffsets).forEach((k) => delete lastOffsets[k]);
    reachedEnd = false;
    await fetchPage(body, true);
  }

  async function fetchPage(body, reset) {
    const topic = topicSelect.value;
    const statusEl = panel.querySelector('#br-status');
    statusEl.textContent = 'Browsing…';
    try {
      const result = await post(clusterPath() + `/topics/${encodeURIComponent(topic)}/browse`, body);
      all = reset ? result.messages : all.concat(result.messages);
      result.messages.forEach((m) => { lastOffsets[m.partition] = m.offset + 1; });
      reachedEnd = result.reachedEnd;
      statusEl.innerHTML = `<b>${all.length}</b> message(s) from partitions [${[...new Set(all.map(m => m.partition))].sort((a,b)=>a-b).join(', ')}]`
        + (result.reachedEnd ? ' · <span class="faint">reached end of topic</span>' : ' · <span class="faint">more available</span>');
      renderResults();
    } catch (err) {
      statusEl.textContent = '';
      panel.querySelector('#br-results').innerHTML = `<div class="error-panel">${esc(err.message)}</div>`;
    }
  }
}

function hexView(base64) {
  const bin = atob(base64 || '');
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return [...bytes].map(b => b.toString(16).padStart(2, '0')).join(' ');
}

function codecChip(m) {
  const sch = m.schema;
  if (!sch || !sch.wireFormat || sch.wireFormat === 'UNKNOWN') {
    return m.binary ? badge('binary', 'warn') : '';
  }
  const label = `${sch.schemaType || sch.wireFormat}${sch.subject ? ' · ' + sch.subject : ''}${sch.schemaId ? ' #' + sch.schemaId : ''}`;
  return badge(label, 'accent');
}

function messageDetailModal(m) {
  const rawValue = m.value ?? '';
  const decoded = m.schema?.decoded;
  const tabs = [];
  if (decoded !== null && decoded !== undefined) tabs.push('Decoded');
  tabs.push('Raw');
  if (m.valueBase64) tabs.push('Hex');
  const note = m.schema?.note ? `<div class="test-result warn" style="background:var(--warn-soft);color:var(--warn)">${esc(m.schema.note)}</div>` : '';
  const error = m.schema?.error ? `<div class="error-panel">${esc(m.schema.error)}</div>` : '';

  modal({
    title: `${m.topic} · partition ${m.partition} · offset ${m.offset}`,
    wide: true,
    body: `
      <dl class="kv mb16">
        <dt>Timestamp</dt><dd>${fmtTs(m.timestamp)} <span class="faint">(${m.timestampType})</span></dd>
        <dt>Key</dt><dd>${esc(m.key ?? 'null')}</dd>
        ${m.schema?.schemaId ? `<dt>Schema</dt><dd>${badge((m.schema.schemaType || 'AVRO') + ' · ' + (m.schema.subject || 'subject n/a') + ' · id ' + m.schema.schemaId, 'accent')}</dd>` : ''}
      </dl>
      ${error}
      <div class="card-title" style="margin-bottom:6px"><h2>Value</h2>
        <div class="btn-row">
          <button class="btn ghost sm copy-btn" id="md-copy">⧉ Copy</button>
          <button class="btn ghost sm" id="md-reproduce">↻ Reproduce…</button>
        </div>
      </div>
      <div class="btn-row mb16" id="md-tabs">
        ${tabs.map((t, i) => `<button class="btn ghost sm ${i === 0 ? 'primary' : ''}" data-tab="${t}">${t}</button>`).join('')}
      </div>
      <div id="md-body"></div>
      ${note}
      ${Object.keys(m.headers || {}).length ? `
        <h2 class="mt16">Headers</h2>
        <div class="table-wrap"><table class="tbl"><tbody>
          ${Object.entries(m.headers).map(([k, v]) => `<tr><td class="mono">${esc(k)}</td><td class="mono small">${esc(v ?? 'null')}</td></tr>`).join('')}
        </tbody></table></div>` : ''}`,
    actions: [{ label: 'Close', class: 'ghost', onClick: (o, close) => close() }],
    onMount: (overlay, close) => {
      const views = {
        Decoded: decoded !== null && decoded !== undefined ? jsonBlock(decoded) : jsonBlock(rawValue),
        Raw: jsonBlock(rawValue),
        Hex: m.valueBase64 ? `<div class="json-view">${esc(hexView(m.valueBase64))}</div>` : '<div class="empty-state">n/a</div>',
      };
      const body = overlay.querySelector('#md-body');
      const paint = (t) => { body.innerHTML = views[t] || ''; };
      overlay.querySelectorAll('#md-tabs [data-tab]').forEach((b) => {
        b.addEventListener('click', () => {
          overlay.querySelectorAll('#md-tabs [data-tab]').forEach(x => x.classList.toggle('primary', x === b));
          paint(b.dataset.tab);
        });
      });
      if (tabs.length) paint(tabs[0]);
      overlay.querySelector('#md-copy')?.addEventListener('click', () => copyText(rawValue, 'Message value copied'));
      overlay.querySelector('#md-reproduce')?.addEventListener('click', () => {
        producePrefill = { topic: m.topic, key: m.key, value: rawValue, headers: m.headers || {} };
        close();
        document.querySelector('#ex-tabs [data-tab="produce"]')?.click();
      });
    },
  });
}

// ---------------- Live tail ----------------

function renderTail(panel, topicSelect, getPartitions) {
  panel.innerHTML = `
    <div class="card">
      <div class="toolbar">
        <div class="field"><label>Partition</label><select id="tl-partition">${partitionOptions(getPartitions())}</select></div>
        <div class="field"><label>Start from</label>
          <select id="tl-from"><option value="latest">latest (only new messages)</option><option value="earliest">earliest (whole topic)</option></select></div>
        <div class="field"><label>Consumer group <span class="hint">(optional)</span></label>
          <input type="text" id="tl-group" placeholder="e.g. my-app" style="width:150px" /></div>
        <div class="field"><label class="checkbox"><input type="checkbox" id="tl-commit" disabled /> commit offsets</label></div>
        <div class="field"><label>&nbsp;</label>
          <div class="btn-row">
            <button class="btn primary" id="tl-start">▶ Start</button>
            <button class="btn ghost" id="tl-stop" disabled>■ Stop</button>
            <button class="btn ghost" id="tl-clear" disabled>Clear</button>
          </div></div>
        <div class="field"><label class="checkbox"><input type="checkbox" id="tl-autoscroll" checked /> auto-scroll</label></div>
      </div>
      <div id="tl-status" class="small muted">Idle — the tail is read-only unless a group with commit is set.</div>
    </div>
    <div class="card">
      <div class="card-title"><h2>Stream</h2><span id="tl-count" class="muted small">0 messages</span></div>
      <div class="table-wrap" style="max-height:460px; overflow-y:auto" id="tl-wrap">
        <table class="tbl msg-table">
          <thead><tr><th class="num">Partition</th><th class="num">Offset</th><th>Time</th><th>Key</th><th>Value</th></tr></thead>
          <tbody id="tl-rows"></tbody>
        </table>
      </div>
    </div>`;

  const rows = panel.querySelector('#tl-rows');
  const wrap = panel.querySelector('#tl-wrap');
  const countEl = panel.querySelector('#tl-count');
  const statusEl = panel.querySelector('#tl-status');
  const startBtn = panel.querySelector('#tl-start');
  const stopBtn = panel.querySelector('#tl-stop');
  const clearBtn = panel.querySelector('#tl-clear');
  const groupInput = panel.querySelector('#tl-group');
  const commitBox = panel.querySelector('#tl-commit');
  let eventSource = null;
  let total = 0;
  const receivedAt = [];

  groupInput.addEventListener('input', () => { commitBox.disabled = !groupInput.value.trim(); });

  startBtn.addEventListener('click', () => {
    const topic = topicSelect.value;
    const params = new URLSearchParams({ from: panel.querySelector('#tl-from').value });
    const partition = panel.querySelector('#tl-partition').value;
    if (partition !== '') params.set('partition', partition);
    const group = groupInput.value.trim();
    if (group) params.set('groupId', group);
    if (group && commitBox.checked) params.set('autoCommit', 'true');

    rows.innerHTML = '';
    total = 0;
    receivedAt.length = 0;
    eventSource = new EventSource(`/api${clusterPath()}/topics/${encodeURIComponent(topic)}/tail?${params}`);
    window.__tailClose = () => { eventSource?.close(); eventSource = null; };

    eventSource.addEventListener('open', () => setStatus('live', `Connected — waiting for messages on ${topic}…`));
    eventSource.addEventListener('message', (e) => {
      const m = JSON.parse(e.data);
      total += 1;
      receivedAt.push(performance.now());
      while (receivedAt.length && receivedAt[0] < performance.now() - 5000) receivedAt.shift();
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td class="num">${m.partition}</td>
        <td class="num mono">${m.offset}</td>
        <td class="small muted">${fmtRel(m.timestamp)}</td>
        <td class="mono small">${esc(m.key ?? 'null')}</td>
        <td class="value-cell">${previewValue(m.value, 200)}</td>`;
      rows.prepend(tr); // newest on top
      while (rows.children.length > 500) rows.lastChild.remove();
      setStatus('live', null);
    });
    eventSource.addEventListener('error', (e) => {
      if (e.data) {
        const err = JSON.parse(e.data);
        setStatus('err', `Tail error: ${err.message}`);
        stop();
      } else if (eventSource && eventSource.readyState === EventSource.CLOSED) {
        setStatus('err', 'Connection closed');
        stop();
      }
    });
    startBtn.disabled = true;
    stopBtn.disabled = false;
    clearBtn.disabled = false;
    setStatus('live', 'Connecting…');
  });

  stopBtn.addEventListener('click', () => stop(true));
  clearBtn.addEventListener('click', () => { rows.innerHTML = ''; total = 0; updateCount(); });

  function stop() {
    eventSource?.close();
    eventSource = null;
    startBtn.disabled = false;
    stopBtn.disabled = true;
    setStatus('idle', 'Stopped.');
  }

  function updateCount() {
    const rate = receivedAt.length ? ` · ${(receivedAt.length / 5).toFixed(1)} msg/s` : '';
    countEl.textContent = `${total} message${total === 1 ? '' : 's'}${rate}`;
  }
  tailTimer = setInterval(() => { if (eventSource) updateCount(); }, 1000);

  function setStatus(kind, text) {
    statusEl.innerHTML = kind === 'live'
      ? `<span class="pulse"></span>${text || 'Live'}`
      : esc(text ?? '');
    if (kind !== 'live') updateCount();
  }
}

// ---------------- Produce ----------------

function renderProduce(panel, topicSelect) {
  panel.innerHTML = `
    <div class="two-col">
      <div class="card">
        <div class="field"><label>Key <span class="hint">(optional — null keys round-robin)</span></label>
          <input type="text" id="pr-key" placeholder="e.g. order-123" /></div>
        <div class="field"><label>Partition <span class="hint">(optional)</span></label>
          <select id="pr-partition"><option value="">auto</option>${partitionOptions(topicOptions.find((t) => t.name === topicSelect.value)?.partitions ?? 1).slice(1)}</select></div>
        <div class="field"><label>Headers <span class="hint">(JSON object, optional)</span></label>
          <textarea id="pr-headers" rows="2" placeholder='{"trace-id": "abc"}'></textarea></div>
        <div class="field"><label>Value</label>
          <textarea id="pr-value" rows="8" placeholder='{"amount": 42.0, "currency": "EUR"}'></textarea></div>
        <div class="btn-row">
          <button class="btn primary" id="pr-send">Send message</button>
          <span class="muted small">Ctrl+Enter to send</span>
        </div>
      </div>
      <div class="card">
        <div class="card-title"><h2>Result</h2></div>
        <div id="pr-result"><div class="empty-state">Nothing sent yet</div></div>
        <div class="card-title mt16"><h2>Recent sends</h2></div>
        <div id="pr-history" class="small muted">—</div>
      </div>
    </div>`;

  if (producePrefill) {
    const p = producePrefill;
    producePrefill = null;
    if (p.topic && topicOptions.some((t) => t.name === p.topic)) {
      topicSelect.value = p.topic;
      topicSelect.dispatchEvent(new Event('change'));
    }
    if (p.key !== null && p.key !== undefined) panel.querySelector('#pr-key').value = p.key;
    if (p.value) panel.querySelector('#pr-value').value = p.value;
    if (p.headers && Object.keys(p.headers).length) {
      panel.querySelector('#pr-headers').value = JSON.stringify(p.headers, null, 2);
    }
    toast('Form prefilled from the reproduced message', 'ok', 2500);
  }

  const valueBox = panel.querySelector('#pr-value');
  const history = [];
  const send = async () => {
    const topic = topicSelect.value;
    const resultEl = panel.querySelector('#pr-result');
    let headers = {};
    const rawHeaders = panel.querySelector('#pr-headers').value.trim();
    if (rawHeaders) {
      try { headers = JSON.parse(rawHeaders); }
      catch { toast('Headers must be valid JSON', 'err'); return; }
    }
    const partition = panel.querySelector('#pr-partition').value;
    try {
      const result = await post(clusterPath() + `/topics/${encodeURIComponent(topic)}/messages`, {
        key: panel.querySelector('#pr-key').value.trim() || null,
        payload: valueBox.value,
        partition: partition === '' ? null : Number(partition),
        headers,
      });
      resultEl.innerHTML = `
        <div class="test-result ok mono" style="color:var(--text)">
          ✓ stored in <b>partition ${result.partition}</b> at <b>offset ${result.offset}</b>
          <div class="muted small">timestamp ${fmtTs(result.timestamp)}</div>
        </div>`;
      history.unshift({ topic, partition: result.partition, offset: result.offset, key: panel.querySelector('#pr-key').value });
      panel.querySelector('#pr-history').innerHTML = history.slice(0, 6).map((h) =>
        `<div class="mono small" style="padding:3px 0">${esc(h.topic)} · p${h.partition} · #${h.offset} ${h.key ? `· ${esc(h.key)}` : ''}</div>`).join('');
      toast(`Produced to ${topic} [p${result.partition} #${result.offset}]`);
    } catch (err) {
      resultEl.innerHTML = `<div class="test-result err">${esc(err.message)}</div>`;
    }
  };
  panel.querySelector('#pr-send').addEventListener('click', send);
  valueBox.addEventListener('keydown', (e) => { if (e.key === 'Enter' && e.ctrlKey) send(); });
}
