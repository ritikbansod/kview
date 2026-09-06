// ===== Topics: list + full topic detail (partitions, configs, lifecycle) =====
import { get, post, put, del, clusterPath } from '../api.js';
import {
  esc, fmtNum, fmtCompact, badge, skeletonTable, toast, confirmDialog, modal, debounce, makeSortable,
} from '../ui.js';

export async function renderTopics(view) {
  view.innerHTML = `
    <div class="toolbar" style="align-items:center">
      <h1 style="margin:0">Topics</h1>
      <div class="spacer" style="flex:1"></div>
      <div class="skeleton" style="width:180px"></div>
    </div>
    <div class="card">${skeletonTable(6, 5)}</div>`;
  const topics = await get(clusterPath() + '/topics?includeCounts=true');

  const compare = (t, other, key) => {
    const a = t[key], b = other[key];
    if (typeof a === 'string') return a.localeCompare(b);
    return a - b;
  };
  const sortState = { key: 'name', dir: 1 };
  const rows = (filter) => topics
    .filter((t) => t.name.toLowerCase().includes(filter.toLowerCase()))
    .sort((a, b) => compare(a, b, sortState.key) * sortState.dir)
    .map((t) => `
      <tr class="clickable" data-topic="${esc(t.name)}">
        <td class="mono">${esc(t.name)} ${t.internal ? badge('internal', 'neutral') : ''}</td>
        <td class="num">${t.partitions}</td>
        <td class="num">${t.replicationFactor}</td>
        <td class="num">${fmtCompact(t.messageCount)}</td>
        <td>${t.underReplicatedPartitions > 0 ? badge(t.underReplicatedPartitions + ' URP', 'err') : badge('healthy', 'ok')}</td>
        <td>
          <a class="link small" href="#/explorer?topic=${encodeURIComponent(t.name)}" data-stop>explore</a>
          <a class="link small" data-stop data-delete="${esc(t.name)}">delete</a>
        </td>
      </tr>`).join('');

  view.innerHTML = `
    <div class="toolbar" style="align-items:center">
      <h1 style="margin:0">Topics <span class="muted small">(${topics.length})</span></h1>
      <div class="spacer" style="flex:1"></div>
      <input type="text" id="topic-search" placeholder="Filter topics…" style="width:220px" />
      <button class="btn primary" id="create-topic-btn">+ Create topic</button>
    </div>
    <div class="card">
      <div class="table-wrap"><table class="tbl">
        <thead><tr>
          <th class="sortable" data-key="name">Topic</th>
          <th class="sortable num" data-key="partitions">Partitions</th>
          <th class="sortable num" data-key="replicationFactor">Replication</th>
          <th class="sortable num" data-key="messageCount">Messages</th>
          <th>Health</th><th></th>
        </tr></thead>
        <tbody id="topic-rows"></tbody>
      </table></div>
    </div>`;

  const tbody = view.querySelector('#topic-rows');
  tbody.innerHTML = rows('');
  makeSortable(view.querySelector('table.tbl'), 'name', 1, (key, dir) => {
    sortState.key = key; sortState.dir = dir;
    tbody.innerHTML = rows(search.value);
  });

  const search = view.querySelector('#topic-search');
  search.addEventListener('input', debounce(() => { tbody.innerHTML = rows(search.value); }, 150));

  tbody.addEventListener('click', (e) => {
    const deleteEl = e.target.closest('[data-delete]');
    if (deleteEl) {
      e.preventDefault();
      confirmDelete(deleteEl.dataset.delete, () => renderTopics(view));
      return;
    }
    if (e.target.closest('[data-stop]')) return;
    const tr = e.target.closest('tr[data-topic]');
    if (tr) location.hash = `#/topics/${encodeURIComponent(tr.dataset.topic)}`;
  });

  view.querySelector('#create-topic-btn').addEventListener('click', () => createTopicModal(() => renderTopics(view)));
}

function confirmDelete(name, done) {
  confirmDialog({
    title: `Delete topic "${name}"?`,
    message: 'All messages in this topic will be permanently removed. This cannot be undone.',
    onConfirm: async () => {
      try {
        await del(clusterPath() + `/topics/${encodeURIComponent(name)}`);
        toast(`Topic "${name}" deleted`);
        done();
      } catch (err) { toast(err.message, 'err'); }
    },
  });
}

function createTopicModal(done) {
  modal({
    title: 'Create topic',
    body: `
      <div class="field"><label>Name</label>
        <input type="text" id="nt-name" placeholder="e.g. orders" /></div>
      <div class="form-grid">
        <div class="field"><label>Partitions</label>
          <input type="number" id="nt-partitions" value="3" min="1" /></div>
        <div class="field"><label>Replication factor</label>
          <input type="number" id="nt-rf" value="1" min="1" /></div>
      </div>
      <div class="field"><label>Topic configs <span class="hint">(JSON object, optional)</span></label>
        <textarea id="nt-configs" rows="3" placeholder='{"retention.ms": "86400000"}'></textarea></div>`,
    actions: [
      { label: 'Cancel', class: 'ghost', onClick: (o, close) => close() },
      {
        label: 'Create', class: 'primary',
        onClick: async (o, close) => {
          const name = o.querySelector('#nt-name').value.trim();
          if (!name) { toast('Topic name is required', 'warn'); return; }
          let configs = {};
          const raw = o.querySelector('#nt-configs').value.trim();
          try { configs = raw ? JSON.parse(raw) : {}; }
          catch { toast('Configs must be valid JSON', 'err'); return; }
          try {
            const result = await post(clusterPath() + '/topics', {
              name,
              partitions: Number(o.querySelector('#nt-partitions').value) || 1,
              replicationFactor: Number(o.querySelector('#nt-rf').value) || 1,
              configs,
            });
            toast(`Topic "${result.topic}" ${result.status}`);
            close();
            done();
          } catch (err) { toast(err.message, 'err'); }
        },
      },
    ],
  });
}

// ================= Topic detail =================

export async function renderTopicDetail(view, topic) {
  view.innerHTML = `<div class="toolbar" style="align-items:center"><h1 class="mono" style="margin:0">${esc(topic)}</h1></div>${skeletonTable(8, 5)}`;
  const detail = await get(clusterPath() + `/topics/${encodeURIComponent(topic)}`);
  const totalMessages = detail.partitions.reduce((sum, p) => sum + p.messageCount, 0);

  view.innerHTML = `
    <div class="toolbar" style="align-items:center">
      <div>
        <h1 class="mono" style="margin:0">${esc(detail.name)}</h1>
        <p class="muted small" style="margin:4px 0 0">
          ${detail.internal ? badge('internal', 'neutral') + ' ' : ''}
          ${detail.topicId ? `<span class="mono">id ${esc(detail.topicId)}</span> · ` : ''}
          ${detail.partitions.length} partitions · ${fmtCompact(totalMessages)} messages
        </p>
      </div>
      <div class="spacer" style="flex:1"></div>
      <div class="btn-row">
        <a class="btn ghost" href="#/explorer?topic=${encodeURIComponent(detail.name)}">Explore data</a>
        <a class="btn ghost" href="#/explorer?topic=${encodeURIComponent(detail.name)}&tab=tail">Live tail</a>
        <a class="btn ghost" href="#/explorer?topic=${encodeURIComponent(detail.name)}&tab=produce">Produce</a>
        <button class="btn ghost" id="resize-btn">Partitions…</button>
        <button class="btn ghost" id="configs-btn">Edit configs…</button>
        <button class="btn danger" id="delete-btn">Delete</button>
      </div>
    </div>

    <div class="card">
      <div class="card-title"><h2>Partitions</h2></div>
      <div class="table-wrap"><table class="tbl">
        <thead><tr><th>Partition</th><th class="num">Leader</th><th>Replicas</th><th>ISR</th><th class="num">Beginning</th><th class="num">End</th><th class="num">Messages</th></tr></thead>
        <tbody>
          ${detail.partitions.map((p) => `
            <tr>
              <td class="mono">${p.partition}</td>
              <td class="num">${p.leader < 0 ? badge('none', 'err') : p.leader}</td>
              <td class="mono small">${p.replicas.join(', ')}</td>
              <td>${p.isr.length < p.replicas.length
                ? `<span class="mono small" style="color:var(--warn)">${p.isr.join(', ')}</span>`
                : `<span class="mono small muted">${p.isr.join(', ')}</span>`}</td>
              <td class="num">${fmtNum(p.beginningOffset)}</td>
              <td class="num">${fmtNum(p.endOffset)}</td>
              <td class="num">${fmtNum(p.messageCount)}</td>
            </tr>`).join('')}
        </tbody>
      </table></div>
    </div>

    <div class="card">
      <div class="card-title"><h2>Configs</h2>
        <input type="text" id="config-search" placeholder="Filter configs…" style="width:200px" /></div>
      <div class="table-wrap" style="max-height:420px; overflow-y:auto"><table class="tbl">
        <thead><tr><th>Name</th><th>Value</th><th>Source</th><th></th></tr></thead>
        <tbody id="config-rows">${configRows(detail.configs, '')}</tbody>
      </table></div>
    </div>`;

  const search = view.querySelector('#config-search');
  const tbody = view.querySelector('#config-rows');
  search.addEventListener('input', debounce(() => {
    tbody.innerHTML = configRows(detail.configs, search.value);
  }, 150));

  view.querySelector('#delete-btn').addEventListener('click', () => {
    confirmDelete(detail.name, () => { location.hash = '#/topics'; });
  });

  view.querySelector('#resize-btn').addEventListener('click', () => {
    const current = detail.partitions.length;
    modal({
      title: `Increase partitions of "${detail.name}"`,
      body: `
        <p class="muted small">Currently <b>${current}</b>. Kafka only supports increasing the partition count —
        key-based ordering is affected for existing keys.</p>
        <div class="field"><label>New total partition count</label>
          <input type="number" id="rp-count" min="${current}" value="${current}" /></div>`,
      actions: [
        { label: 'Cancel', class: 'ghost', onClick: (o, close) => close() },
        {
          label: 'Apply', class: 'primary',
          onClick: async (o, close) => {
            try {
              await post(clusterPath() + `/topics/${encodeURIComponent(detail.name)}/partitions`,
                { totalPartitions: Number(o.querySelector('#rp-count').value) });
              toast('Partitions updated');
              close();
              renderTopicDetail(view, topic);
            } catch (err) { toast(err.message, 'err'); }
          },
        },
      ],
    });
  });

  view.querySelector('#configs-btn').addEventListener('click', () => {
    const editable = Object.fromEntries(detail.configs
      .filter((c) => c.source !== 'DEFAULT_CONFIG' && c.source !== 'STATIC_BROKER_CONFIG' && !c.readOnly)
      .map((c) => [c.name, c.value]));
    modal({
      title: `Edit configs of "${detail.name}"`,
      body: `
        <p class="muted small">JSON object of topic config overrides to set. Existing overrides are preloaded.</p>
        <div class="field"><textarea id="ec-json" rows="10">${esc(JSON.stringify(editable, null, 2))}</textarea></div>`,
      actions: [
        { label: 'Cancel', class: 'ghost', onClick: (o, close) => close() },
        {
          label: 'Save configs', class: 'primary',
          onClick: async (o, close) => {
            try {
              const configs = JSON.parse(o.querySelector('#ec-json').value);
              await put(clusterPath() + `/topics/${encodeURIComponent(detail.name)}/configs`, { configs });
              toast('Configs altered');
              close();
              renderTopicDetail(view, topic);
            } catch (err) { toast(err.message, 'err'); }
          },
        },
      ],
    });
  });
}

function configRows(configs, filter) {
  const f = filter.toLowerCase();
  return configs
    .filter((c) => c.name.toLowerCase().includes(f))
    .map((c) => `
      <tr>
        <td class="mono">${esc(c.name)}</td>
        <td class="mono small">${esc(c.value ?? 'null')}</td>
        <td>${badge(c.source.replace(/_CONFIG$/, '').toLowerCase(), sourceTone(c.source))}</td>
        <td>${c.readOnly ? badge('read-only', 'neutral') : ''}</td>
      </tr>`).join('');
}

function sourceTone(source) {
  switch (source) {
    case 'DYNAMIC_TOPIC_CONFIG': return 'accent';
    case 'DYNAMIC_BROKER_CONFIG': return 'warn';
    default: return 'neutral';
  }
}
