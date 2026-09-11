// ===== Topics: list + full topic detail (partitions, configs, lifecycle) =====
import { get, post, put, del, clusterPath } from '../api.js';
import {
  esc, fmtNum, fmtCompact, badge, skeletonTable, toast, confirmDialog, modal, debounce, makeSortable, fmtRel,
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
  const [detail, brokers, history] = await Promise.all([
    get(clusterPath() + `/topics/${encodeURIComponent(topic)}`),
    get(clusterPath() + '/brokers').catch(() => []),
    get(clusterPath() + `/topics/${encodeURIComponent(topic)}/history?limit=1000`).catch(() => null),
  ]);
  const historyEvents = history?.events || [];
  let historyPage = 1;
  const HISTORY_PAGE_SIZE = 25;
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

    <div class="two-col-topic">
      <div class="card collapsible" id="card-partitions">
        <div class="card-title collapsible-title" data-collapse="partitions-body">
          <h2>Partitions</h2>
          <span class="chev">▾</span>
        </div>
        <div class="collapsible-body">
          <div class="table-wrap"><table class="tbl">
            <thead><tr><th>Partition</th><th class="num">Leader</th><th>Replicas</th><th>ISR</th><th class="num">Beginning</th><th class="num">End</th><th class="num">Messages</th></tr></thead>
            <tbody id="partitions-tbody">
              ${detail.partitions.map((p) => `
                <tr data-partition="${p.partition}" title="Show p${p.partition} history" style="cursor:pointer">
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
      </div>

      <div class="card collapsible collapsed" id="card-configs">
        <div class="card-title collapsible-title" data-collapse="configs-body">
          <h2>Configs</h2>
          <span class="chev">▾</span>
        </div>
        <div class="collapsible-body">
          <div style="margin-bottom:8px"><input type="text" id="config-search" placeholder="Filter configs…" style="width:100%" /></div>
          <div class="table-wrap" style="max-height:420px; overflow-y:auto"><table class="tbl">
            <thead><tr><th>Name</th><th>Value</th><th>Source</th><th></th></tr></thead>
            <tbody id="config-rows">${configRows(detail.configs, '')}</tbody>
          </table></div>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-title"><h2>Replica placement</h2>
        <div class="btn-row">
          <button class="btn ghost sm" id="rp-flow-btn">Flow view</button>
          <button class="btn ghost sm" id="rp-matrix-btn">Matrix view</button>
        </div>
      </div>
      <p class="muted small" style="margin:0 0 12px">
        Producers write to the partition <b>leader</b> (orange). Followers on other brokers copy every
        message — the copies that stay caught-up form the <b>ISR</b> (in-sync replicas, teal). If a leader
        dies, one of its in-sync followers is promoted. Click any partition card to see its history.
      </p>
      <div id="rp-flow" class="pcards">
        ${detail.partitions.map((p) => partitionFlowCard(p, brokers)).join('')}
      </div>
      <div id="rp-matrix" style="display:none">
        <div class="table-wrap"><table class="tbl matrix">
          <thead><tr><th>Broker</th>${detail.partitions.map((p) => `<th class="num" data-matrix-partition="${p.partition}" title="Show p${p.partition} history" style="cursor:pointer">p${p.partition}</th>`).join('')}</tr></thead>
          <tbody>
            ${brokers.map((b) => `
              <tr>
                <td class="mono">${b.id < 0 ? badge('offline', 'err') : `broker ${b.id}`} <span class="faint small">${esc(b.host)}:${esc(b.port)}</span></td>
                ${detail.partitions.map((p) => {
                  const cell = roleCell(b.id, p);
                  return `<td class="num"><span class="role-cell ${cell.cls}">${cell.label}</span></td>`;
                }).join('')}
              </tr>`).join('')}
          </tbody>
        </table></div>
      </div>
    </div>
    <div class="card" id="history-card">
      <div class="card-title"><h2>Partition history</h2>
        <span class="faint small">sampled every 15s \u00b7 ${history && history.monitored
          ? `since \${fmtRel(history.firstSeen)}`
          : 'waiting for first sample'}</span>
      </div>
      <p class="muted small" style="margin:0 0 10px">
        Every 15 seconds the wrapper snapshots all partition leaders. When a leader moves
        (election, broker restart), the ISR shrinks or replicas are reassigned, the change is
        recorded here. Click an event for full details.
      </p>
      <div class="hist-summary" id="hist-summary"></div>
      <div class="chip-row" id="hist-chips"></div>
      <div id="hist-body"></div>
    </div>`;


  // ---- collapsible cards (click header to expand/collapse) ----
  view.querySelectorAll('.collapsible-title').forEach((title) => {
    title.addEventListener('click', (e) => {
      if (e.target.closest('input, select, button')) return;
      title.closest('.collapsible').classList.toggle('collapsed');
    });
  });

  // ---- partition history: filters, per-partition tenure, grouped timeline ----
  const histBody = view.querySelector('#hist-body');
  const histSummary = view.querySelector('#hist-summary');
  const histChips = view.querySelector('#hist-chips');
  let partFilter = '';
  let typeFilter = '';
  let histPage = 1;

  const TYPE_META = {
    LEADER_CHANGED: { icon: '\u26a1', label: 'Leader changed', tone: 'accent' },
    LEADER_OFFLINE: { icon: '\u26a1', label: 'Leader offline', tone: 'err' },
    ISR_CHANGED: { icon: '\u21c4', label: 'ISR changed', tone: 'warn' },
    REASSIGNED: { icon: '\u21c9', label: 'Reassigned', tone: 'cyan' },
    PARTITION_ADDED: { icon: '\u2795', label: 'Partition added', tone: 'ok' },
    PARTITION_REMOVED: { icon: '\u2796', label: 'Partition removed', tone: 'neutral' },
  };

  const matchesFilters = (e) =>
    (partFilter === '' || e.partition === partFilter) &&
    (typeFilter === '' || e.type === typeFilter);

  // current leader + since-when per partition (from newest event that set a leader)
  const tenure = (() => {
    const map = {};
    for (let i = historyEvents.length - 1; i >= 0; i--) {
      const e = historyEvents[i];
      if (!(e.partition in map) && e.toLeader != null) {
        map[e.partition] = { leader: e.toLeader, since: e.ts };
      }
    }
    return map;
  })();

  function chip(label, active, onClick) {
    const b = document.createElement('button');
    b.className = 'chip' + (active ? ' active' : '');
    b.textContent = label;
    b.addEventListener('click', onClick);
    return b;
  }

  function renderHistory() {
    // summary: current leader per partition
    histSummary.innerHTML = detail.partitions.map((p) => {
      const t = tenure[p.partition];
      const leaderTxt = t ? `broker ${t.leader}` : 'unknown';
      const since = t ? ` \u00b7 since ${fmtRel(t.since)}` : '';
      const active = partFilter === '' || partFilter === p.partition;
      return `<button class="hist-tenure ${active ? '' : 'dim'}" data-p="${p.partition}"
        title="Show p${p.partition} history only">
        <span class="mono">p${p.partition}</span> \u2192 leader <b>broker ${t ? t.leader : '?'}</b>${since}
      </button>`;
    }).join('');
    histSummary.querySelectorAll('.hist-tenure').forEach((b) => {
      b.addEventListener('click', () => {
        partFilter = Number(b.dataset.p);
        typeFilter = '';
        renderHistory();
      });
    });

    // filter chips
    histChips.innerHTML = '';
    const all = chip('All events', partFilter === '' && typeFilter === '',
      () => { partFilter = ''; typeFilter = ''; histPage = 1; renderHistory(); });
    histChips.appendChild(all);
    const counts = {};
    historyEvents.forEach((e) => { counts[e.type] = (counts[e.type] || 0) + 1; });
    Object.keys(TYPE_META).forEach((type) => {
      if (!counts[type]) return;
      const meta = TYPE_META[type];
      histChips.appendChild(chip(`${meta.icon} ${meta.label} (${counts[type]})`,
        typeFilter === type, () => { typeFilter = typeFilter === type ? '' : type; histPage = 1; renderHistory(); }));
    });
    histChips.appendChild(chip('Elections only', typeFilter === 'LEADER_CHANGED',
      () => { typeFilter = typeFilter === 'LEADER_CHANGED' ? '' : 'LEADER_CHANGED'; histPage = 1; renderHistory(); }));

    // timeline grouped by day, paginated (25 per page)
    const events = historyEvents.filter(matchesFilters);
    const PAGE = 25;
    const pages = Math.max(1, Math.ceil(events.length / PAGE));
    if (histPage > pages) histPage = pages;
    const pageEvents = events.slice((histPage - 1) * PAGE, histPage * PAGE);
    if (pageEvents.length === 0) {
      histBody.innerHTML = `<div class="empty-state">No matching events \u2014 leaders and ISR have been
        stable since monitoring started. Changes appear here within 15 seconds of happening.</div>`;
      return;
    }
    const parts = [];
    let lastDay = '';
    pageEvents.forEach((e) => {
      const meta = TYPE_META[e.type] || { icon: '\u2022', label: e.type, tone: 'neutral' };
      const day = new Date(e.ts).toLocaleDateString('en-GB', { weekday: 'short', day: '2-digit', month: 'short' });
      if (day !== lastDay) {
        parts.push(`<div class="tl-day">${esc(day)}</div>`);
        lastDay = day;
      }
      const leaderLine = e.type.startsWith('LEADER') && e.fromLeader != null
        ? `<div class="tl-move mono small">broker ${e.fromLeader} \u2192 ${e.toLeader != null ? e.toLeader : '(offline)'}</div>`
        : '';
      const extra = (e.isr && e.type === 'ISR_CHANGED')
        ? `<div class="mono small muted">ISR = [${e.isr.join(', ')}]</div>` : '';
      parts.push(`
        <div class="tl-item tone-${meta.tone}" data-ts="${e.ts}" data-topic="${esc(e.topic)}"
             data-partition="${e.partition}">
          <div class="tl-head"><span class="tl-icon">${meta.icon}</span> ${eventBadge(meta.label, meta.tone)}
            <span class="mono small">p${e.partition}</span>
            <span class="small muted" style="margin-left:auto">${fmtRel(e.ts)}</span></div>
          <div class="small tl-detail">${esc(e.detail || '')}</div>
          ${leaderLine}${extra}
        </div>`);
    });
    const from = (histPage - 1) * PAGE + 1;
    const to = Math.min(histPage * PAGE, events.length);
    const pager = pages > 1
      ? `<div class="pager">
          <button class="btn ghost sm" id="hist-prev" ${histPage <= 1 ? 'disabled' : ''}>← Prev</button>
          <span class="small muted">page ${histPage} of ${pages}</span>
          <button class="btn ghost sm" id="hist-next" ${histPage >= pages ? 'disabled' : ''}>Next →</button>
        </div>`
      : '';
    histBody.innerHTML = `<div class="timeline">${parts.join('')}</div>${pager}`;

    // click an event -> toggle full detail (replicas + ISR)
    histBody.querySelectorAll('.tl-item').forEach((item) => {
      item.addEventListener('click', () => item.classList.toggle('open'));
    });
    histBody.querySelector('#hist-prev')?.addEventListener('click', () => { histPage--; renderHistory(); });
    histBody.querySelector('#hist-next')?.addEventListener('click', () => { histPage++; renderHistory(); });
  }

  renderHistory();

  // silent auto-refresh of history data every 15s while this page is visible
  clearInterval(window.__histTimer);
  window.__histTimer = setInterval(async () => {
    if (!document.getElementById('history-card')) { clearInterval(window.__histTimer); return; }
    try {
      historyEvents.length = 0;
      const fresh = await get(clusterPath() + `/topics/${encodeURIComponent(topic)}/history?limit=300`);
      (fresh?.events || []).forEach((e) => historyEvents.push(e));
      renderHistory();
    } catch { /* cluster briefly unreachable - keep old view */ }
  }, 15000);

  const search = view.querySelector('#config-search');
  const tbody = view.querySelector('#config-rows');
  search.addEventListener('input', debounce(() => {
    tbody.innerHTML = configRows(detail.configs, search.value);
  }, 150));

  const historyCard = view.querySelector('.card-title .faint.small');
  const refreshHistory = debounce(() => renderTopicDetail(view, topic), 400);

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


function partitionFlowCard(p, brokers) {
  const healthy = p.isr.length === p.replicas.length && p.leader >= 0;
  const leaderBroker = brokers.find((b) => b.id === p.leader);
  const followers = p.replicas.filter((r) => r !== p.leader);
  const followerChip = (id) => {
    const inSync = p.isr.includes(id);
    const b = brokers.find((x) => x.id === id);
    const host = b ? `<span class="faint small"> ${esc(b.host)}:${esc(b.port)}</span>` : '';
    return `<div class="flow-row ${inSync ? 'f-in' : 'f-oos'}">
      <span class="flow-arrow">↳ replicate ▸</span>
      <span class="broker-chip ${inSync ? 'chip-in' : 'chip-oos'}"><span class="role-tag">F</span> broker ${id}</span>${host}
      <span class="small muted" style="margin-left:auto">${inSync ? 'in-sync' : 'out of sync'}</span>
    </div>`;
  };
  return `
    <div class="pcard ${healthy ? '' : 'pcard-bad'}" data-p="${p.partition}">
      <div class="pcard-head">
        <span class="mono pcard-title">p${p.partition}</span>
        <span class="pcard-rf">RF ${p.replicas.length}</span>
        ${healthy ? badge('healthy', 'ok') : badge('under-replicated', 'err')}
      </div>
      <div class="pcard-body">
        <div class="flow-row">
          <span class="broker-chip chip-leader"><span class="role-tag">L</span> broker ${p.leader}</span>
          ${leaderBroker ? `<span class="faint small">${esc(leaderBroker.host)}:${esc(leaderBroker.port)}</span>` : ''}
          <span class="small muted" style="margin-left:auto">leader — all writes land here</span>
        </div>
        ${followers.map(followerChip).join('')}
      </div>
      <div class="pcard-foot small muted">
        offsets ${fmtNum(p.beginningOffset)} → ${fmtNum(p.endOffset)} · ${fmtNum(p.messageCount)} messages
        <button class="btn ghost sm pcard-hist" data-p="${p.partition}" style="float:right">history ↗</button>
      </div>
    </div>`;
}

function roleCell(brokerId, p) {
  if (p.leader === brokerId) return { cls: 'cell-leader', label: 'L' };
  if (p.replicas.includes(brokerId)) {
    return p.isr.includes(brokerId)
      ? { cls: 'cell-follower', label: 'F' }
      : { cls: 'cell-oor', label: 'F*' };
  }
  return { cls: 'cell-none', label: '·' };
}

function timelineItem(e) {
  const cls = { LEADER_OFFLINE: 'tl-offline', ISR_CHANGED: 'tl-isr', REASSIGNED: 'tl-reassigned' }[e.type] || '';
  const leaderMove = e.type.startsWith('LEADER') && e.fromLeader !== null && e.fromLeader !== undefined;
  return `
    <div class="tl-item ${cls}">
      <div class="tl-head">${eventBadge(e.type)} <span class="mono small muted">p${e.partition}</span>
        <span class="small muted" style="margin-left:auto">${fmtRel(e.ts)}</span></div>
      <div class="small">${esc(e.detail || '')}</div>
      ${leaderMove ? `<div class="mono small muted">broker ${e.fromLeader}${e.toLeader != null ? ' → ' + e.toLeader : ' (offline)'}</div>` : ''}
    </div>`;
}

function eventBadge(type) {
  const map = {
    LEADER_CHANGED: ['leader changed', 'accent'],
    LEADER_OFFLINE: ['leader offline', 'err'],
    ISR_CHANGED: ['ISR changed', 'warn'],
    REASSIGNED: ['reassigned', 'cyan'],
    PARTITION_ADDED: ['partition added', 'ok'],
    PARTITION_REMOVED: ['partition removed', 'neutral'],
  };
  const [label, tone] = map[type] || [type, 'neutral'];
  return badge(label, tone);
}

function configRows(configs, filter) {  const f = filter.toLowerCase();
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
