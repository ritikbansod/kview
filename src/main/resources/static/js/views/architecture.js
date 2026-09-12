// ===== Architecture: producers → cluster (brokers · topics · partitions) → consumers =====
import { get, clusterPath } from '../api.js';
import { esc, fmtNum, fmtCompact, fmtRel, badge, skeletonTable, modal, debounce } from '../ui.js';

const MAX_TOPIC_CARDS = 12;
const MAX_GROUP_CARDS = 8;
const PRODUCER_NODES = 3; // Kafka does not expose producer identity — logical ingress nodes

let archTimer = null;
let autoRefresh = false;
let prevSnap = null;            // { ts, ends: Map<topic, endOffsetSum> } — for msg/s between polls
const rates = new Map();        // topic -> msgs/sec observed since last poll
let shownGroups = [];           // groups currently rendered as cards (for edge wiring)

export async function renderArchitecture(view) {
  if (archTimer) { clearInterval(archTimer); archTimer = null; }
  rates.clear();
  prevSnap = null;

  view.innerHTML = `
    <div class="toolbar" style="align-items:center">
      <h1 style="margin:0">Cluster architecture</h1>
      <div class="spacer" style="flex:1"></div>
      <label class="checkbox small"><input type="checkbox" id="arch-auto-refresh" ${autoRefresh ? 'checked' : ''} /> auto-refresh (10s)</label>
      <span class="faint small" id="arch-updated">updated ${fmtRel(Date.now())}</span>
    </div>
    <div class="arch-board">${skeletonTable(6, 3)}</div>`;

  await load(view);
}

async function load(view) {
  const topo = await get(clusterPath() + '/topology');

  // per-topic throughput between polls (end-offset growth)
  const now = Date.now();
  const ends = new Map(topo.topics.map((t) => [t.name, t.partitions.reduce((s, p) => s + Math.max(0, p.endOffset), 0)]));
  if (prevSnap && now > prevSnap.ts) {
    const dt = (now - prevSnap.ts) / 1000;
    rates.clear();
    ends.forEach((sum, topic) => {
      const prev = prevSnap.ends.get(topic) ?? sum;
      const r = (sum - prev) / dt;
      if (r > 0.01) rates.set(topic, r);
    });
  }
  prevSnap = { ts: now, ends };

  const visible = [...topo.topics]
    .filter((t) => !t.internal)
    .sort((a, b) => b.messageCount - a.messageCount || a.name.localeCompare(b.name));
  const shownTopics = visible.slice(0, MAX_TOPIC_CARDS);
  const hiddenTopics = visible.length - shownTopics.length;
  const internalCount = topo.topics.filter((t) => t.internal).length;

  shownGroups = [...topo.groups]
    .sort((a, b) => (b.members > 0) - (a.members > 0) || a.groupId.localeCompare(b.groupId))
    .slice(0, MAX_GROUP_CARDS);
  const hiddenGroups = topo.groups.length - shownGroups.length;

  const urp = topo.topics.reduce((s, t) => s + t.partitions.filter((p) => p.leader >= 0 && p.isr.length < p.replicas.length).length, 0);
  const offline = topo.topics.reduce((s, t) => s + t.partitions.filter((p) => p.leader < 0).length, 0);
  const totalPartitions = topo.topics.reduce((s, t) => s + t.partitions.length, 0);

  view.innerHTML = `
    <div class="toolbar" style="align-items:center">
      <h1 style="margin:0">Cluster architecture</h1>
      <div class="spacer" style="flex:1"></div>
      <label class="checkbox small"><input type="checkbox" id="arch-auto-refresh" ${autoRefresh ? 'checked' : ''} /> auto-refresh (10s)</label>
      <span class="faint small" id="arch-updated">updated ${fmtRel(now)}</span>
    </div>

    <div class="arch-strip">
      <span class="badge tone-accent">${fmtNum(topo.brokers.length)} brokers</span>
      <span class="badge tone-neutral">controller: broker ${topo.controller < 0 ? '—' : topo.controller}</span>
      <span class="badge tone-cyan">${fmtNum(topo.topics.length)} topics (${internalCount} internal)</span>
      <span class="badge tone-neutral">${fmtNum(totalPartitions)} partitions</span>
      <span class="badge tone-purple">${fmtNum(topo.groups.length)} consumer groups</span>
      ${urp > 0 ? `<span class="badge tone-warn">${urp} under-replicated</span>` : ''}
      ${offline > 0 ? `<span class="badge tone-err">${offline} offline</span>` : ''}
    </div>

    <div class="arch-board">
      <svg class="arch-edges" aria-hidden="true"></svg>
      <div class="arch-grid">

        <div class="arch-lane" id="lane-producers">
          <div class="arch-lane-title">Producers</div>
          ${Array.from({ length: PRODUCER_NODES }, (_, i) => `
            <div class="arch-card arch-producer" title="External application writing to the cluster">
              <div class="arch-card-head"><span class="arch-ico prod">▦</span><span class="fw-bold">Producer app</span></div>
              <div class="faint small">external client</div>
            </div>`).join('')}
          <div class="faint small arch-note">Kafka does not expose producer identity — these are logical ingress points.</div>
        </div>

        <div class="arch-cluster-box" id="cluster-box">
          <div class="arch-cluster-label">Kafka cluster</div>
          <div class="arch-brokers">
            ${topo.brokers.map((b) => `
              <div class="arch-broker-chip ${b.id === topo.controller ? 'is-controller' : ''}" data-broker="${b.id}"
                   title="${esc(b.host)}:${esc(b.port)}${b.rack ? ' · rack ' + esc(b.rack) : ''} — click for partitions">
                <span class="mono fw-bold">broker ${b.id}</span>
                ${b.id === topo.controller ? '<span class="arch-ctl" title="cluster controller">★</span>' : ''}
              </div>`).join('')}
          </div>
          ${shownTopics.length === 0 ? '<div class="empty-state">No user topics — create one from the Topics view</div>' : `
          <div class="arch-topic-grid">
            ${shownTopics.map((t) => topicCard(t)).join('')}
          </div>
          ${hiddenTopics > 0 ? `<a class="link small" href="#/topics">+ ${hiddenTopics} more topics →</a>` : ''}`}
        </div>

        <div class="arch-lane" id="lane-consumers">
          <div class="arch-lane-title">Consumers</div>
          ${shownGroups.length === 0 ? '<div class="empty-state">No consumer groups</div>' : shownGroups.map((g) => `
            <div class="arch-card arch-group" data-group="${esc(g.groupId)}" title="open group detail">
              <div class="arch-card-head">
                <span class="arch-ico cons">❖</span>
                <span class="fw-bold arch-ellipsis">${esc(g.groupId)}</span>
              </div>
              <div class="arch-card-meta">
                ${badge(g.state.toLowerCase(), g.state === 'STABLE' ? 'ok' : g.state === 'EMPTY' ? 'neutral' : 'warn')}
                <span class="faint small">${g.members} member${g.members === 1 ? '' : 's'}</span>
                ${g.totalLag > 0 ? `<span class="badge tone-warn">lag ${fmtCompact(g.totalLag)}</span>` : '<span class="badge tone-ok">lag 0</span>'}
              </div>
            </div>`).join('')}
          ${hiddenGroups > 0 ? `<a class="link small" href="#/groups">+ ${hiddenGroups} more groups →</a>` : ''}
        </div>

      </div>
    </div>

    <div class="arch-legend small faint">
      <span><span class="legend-swatch prod"></span> producer flow</span>
      <span><span class="legend-swatch ok"></span> in-sync partition</span>
      <span><span class="legend-swatch warn"></span> under-replicated</span>
      <span><span class="legend-swatch err"></span> offline (no leader)</span>
      <span><span class="legend-swatch edge"></span> consumed-by edge — <span style="color:var(--warn)">amber</span> when the group lags</span>
      <span><span class="legend-swatch pulse"></span> live traffic (offsets advancing)</span>
    </div>`;

  // interactions
  view.querySelectorAll('.arch-broker-chip').forEach((chip) => {
    chip.addEventListener('click', () => brokerPartitionsModal(Number(chip.dataset.broker)));
  });
  view.querySelectorAll('.arch-topic-card').forEach((card) => {
    card.addEventListener('click', () => { location.hash = `#/topics/${encodeURIComponent(card.dataset.topic)}`; });
    card.addEventListener('mouseenter', () => highlight(card.dataset.topic, null));
    card.addEventListener('mouseleave', () => highlight(null, null));
  });
  view.querySelectorAll('.arch-group').forEach((card) => {
    card.addEventListener('click', () => { location.hash = `#/groups/${encodeURIComponent(card.dataset.group)}`; });
    card.addEventListener('mouseenter', () => highlight(null, card.dataset.group));
    card.addEventListener('mouseleave', () => highlight(null, null));
  });

  drawEdges(view);                            // layout is measurable synchronously
  requestAnimationFrame(() => drawEdges(view)); // post-paint correction
  wireAutoRefresh(view);
}

function topicCard(t) {
  const rate = rates.get(t.name);
  return `
    <div class="arch-topic-card ${rate ? 'flow' : ''}" data-topic="${esc(t.name)}" title="open topic detail">
      <div class="arch-card-head">
        <span class="fw-bold arch-ellipsis mono">${esc(t.name)}</span>
        ${rate ? `<span class="badge tone-ok arch-rate">${rate >= 10 ? Math.round(rate) : rate.toFixed(1)} msg/s</span>` : ''}
      </div>
      <div class="arch-card-meta">
        <span class="badge tone-neutral">RF ${t.replicationFactor}</span>
        <span class="faint small">${t.partitions.length} partition${t.partitions.length === 1 ? '' : 's'}</span>
        <span class="faint small">${fmtCompact(t.messageCount)} msgs</span>
      </div>
      <div class="arch-part-chips">
        ${t.partitions.slice(0, 24).map((p) => {
          const cls = p.leader < 0 ? 'err' : p.isr.length < p.replicas.length ? 'warn' : 'ok';
          const tip = `partition ${p.partition} · leader ${p.leader < 0 ? 'none' : 'broker ' + p.leader} · replicas [${p.replicas.join(',')}] · isr [${p.isr.join(',')}] · end offset ${p.endOffset}`;
          return `<span class="arch-pchip ${cls}" title="${esc(tip)}">p${p.partition}</span>`;
        }).join('')}
        ${t.partitions.length > 24 ? `<span class="arch-pchip more" title="${t.partitions.length - 24} more partitions">+${t.partitions.length - 24}</span>` : ''}
      </div>
    </div>`;
}

// ---- SVG edges -------------------------------------------------------

function drawEdges(view) {
  const board = view.querySelector('.arch-board');
  const svg = view.querySelector('.arch-edges');
  const box = view.querySelector('#cluster-box');
  if (!board || !svg || !box) return;
  if (board.clientWidth < 900) { svg.innerHTML = ''; return; } // stacked layout: no edges

  const brect = board.getBoundingClientRect();
  const boxRect = relRect(box, brect);
  const paths = [];

  // producers fan into the cluster box
  const producers = [...view.querySelectorAll('.arch-producer')];
  producers.forEach((p, i) => {
    const r = relRect(p, brect);
    const targetY = boxRect.top + (boxRect.height / (producers.length + 1)) * (i + 1);
    paths.push(edgePath(r.right + 4, r.top + r.height / 2, boxRect.left - 4, targetY, 'edge-producer', '', ''));
  });

  // topic → group edges from committed offsets. The line leaves the cluster
  // box at the topic card's row height, so it never crosses sibling cards.
  const groupEls = new Map();
  view.querySelectorAll('.arch-group').forEach((el) => groupEls.set(el.dataset.group, el));
  const topicEls = new Map();
  view.querySelectorAll('.arch-topic-card').forEach((el) => topicEls.set(el.dataset.topic, el));

  const perTopic = new Map();
  for (const g of shownGroups) {
    const gEl = groupEls.get(g.groupId);
    if (!gEl) continue;
    const readTopics = g.topics.filter((t) => topicEls.has(t));
    readTopics.forEach((topic, gi) => {
      const tEl = topicEls.get(topic);
      const tr = relRect(tEl, brect);
      const gr = relRect(gEl, brect);
      const ti = perTopic.get(topic) || 0;
      perTopic.set(topic, ti + 1);
      const srcY = Math.min(Math.max(tr.top + tr.height / 2 + ti * 8, boxRect.top + 8), boxRect.top + boxRect.height - 8);
      const tgtY = gr.top + gr.height / 2 + (gi - (readTopics.length - 1) / 2) * 7;
      const cls = g.totalLag > 0 ? 'edge-topic edge-lag' : 'edge-topic';
      paths.push(edgePath(boxRect.right - 2, srcY, gr.left - 4, tgtY, cls, topic, g.groupId));
    });
  }

  svg.setAttribute('viewBox', `0 0 ${Math.max(1, brect.width)} ${Math.max(1, board.scrollHeight)}`);
  svg.setAttribute('preserveAspectRatio', 'none');
  svg.innerHTML = paths.join('');
}

function relRect(el, brect) {
  const r = el.getBoundingClientRect();
  return {
    left: r.left - brect.left, right: r.right - brect.left,
    top: r.top - brect.top, height: r.height, width: r.width,
  };
}

function edgePath(x1, y1, x2, y2, cls, topic, group) {
  const mx = (x1 + x2) / 2;
  return `<path d="M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}"
    class="${cls}" data-topic="${esc(topic)}" data-group="${esc(group)}" />`;
}

function highlight(topic, group) {
  const svg = document.querySelector('.arch-edges');
  if (!svg) return;
  svg.classList.toggle('dim', !!(topic || group));
  svg.querySelectorAll('path').forEach((p) => {
    const hit = topic ? p.dataset.topic === topic : p.dataset.group === group;
    p.classList.toggle('hl', hit);
  });
}

// ---- broker drill-down ------------------------------------------------

async function brokerPartitionsModal(brokerId) {
  let data;
  try {
    data = await get(clusterPath() + `/brokers/${brokerId}/partitions`);
  } catch (err) {
    modal({ title: `Broker ${brokerId} partitions`, body: `<div class="error-panel">${esc(err.message)}</div>`, actions: [{ label: 'Close', class: 'ghost', onClick: (o, c) => c() }] });
    return;
  }
  const table = (items, role) => items.length === 0
    ? `<div class="empty-state">No ${role} partitions</div>`
    : `<table class="tbl"><thead><tr><th>Topic</th><th class="num">Partition</th><th class="num">Leader</th><th>ISR</th></tr></thead><tbody>
        ${items.map((p) => `<tr><td class="mono">${esc(p.topic)}</td><td class="num">${p.partition}</td>
          <td class="num">${p.leader}</td><td class="mono small">[${p.isr.join(',')}]</td></tr>`).join('')}
      </tbody></table>`;
  modal({
    title: `Broker ${brokerId} — ${data.leader.length} leader + ${data.follower.length} follower partitions`,
    wide: true,
    body: `<h3 style="margin:0 0 8px;color:var(--accent)">Leader partitions (${data.leader.length})</h3>${table(data.leader, 'leader')}
      <h3 style="margin:16px 0 8px;color:var(--cyan)">Follower partitions (${data.follower.length})</h3>${table(data.follower, 'follower')}`,
    actions: [{ label: 'Close', class: 'ghost', onClick: (o, c) => c() }],
  });
}

// ---- auto-refresh -----------------------------------------------------

function wireAutoRefresh(view) {
  const box = view.querySelector('#arch-auto-refresh');
  const onResize = debounce(() => drawEdges(view), 150);
  window.addEventListener('resize', onResize);
  document.addEventListener('visibilitychange', onVisibility);
  function onVisibility() { if (!document.hidden) drawEdges(view); }
  window.__dashStop = stop; // main.js render() calls this on every route change
  function stop() {
    if (archTimer) { clearInterval(archTimer); archTimer = null; }
    window.removeEventListener('resize', onResize);
    document.removeEventListener('visibilitychange', onVisibility);
  }

  if (autoRefresh && !archTimer) archTimer = setInterval(() => load(view), 10_000);
  if (!box) return;
  box.checked = autoRefresh;
  box.addEventListener('change', () => {
    autoRefresh = box.checked;
    if (autoRefresh) archTimer = setInterval(() => load(view), 10_000);
    else stop();
  });
}
