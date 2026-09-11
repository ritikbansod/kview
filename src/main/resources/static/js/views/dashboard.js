// ===== Dashboard: cluster KPIs, brokers (+configs), partition distribution, top topics, ACLs =====
import { get, clusterPath } from '../api.js';
import { esc, fmtNum, fmtCompact, fmtRel, badge, skeletonTable, modal, debounce } from '../ui.js';

let dashTimer = null;

export async function renderDashboard(view) {
  if (dashTimer) { clearInterval(dashTimer); dashTimer = null; }

  view.innerHTML = `
    <div class="toolbar" style="align-items:center">
      <h1 style="margin:0">Cluster overview</h1>
      <div class="spacer" style="flex:1"></div>
      <label class="checkbox small"><input type="checkbox" id="auto-refresh" /> auto-refresh (10s)</label>
      <span class="faint small" id="updated-at">updated ${fmtRel(Date.now())}</span>
    </div>
    <div class="grid-kpi">
      ${Array.from({ length: 7 }, () => '<div class="kpi"><div class="label">&nbsp;</div><div class="skeleton" style="height:24px"></div></div>').join('')}
    </div>
    ${skeletonTable(5, 4)}`;

  await load(view);
}

async function load(view) {
  const [overview, topics, acls, distribution] = await Promise.all([
    get(clusterPath() + '/overview'),
    get(clusterPath() + '/topics?includeCounts=true'),
    get(clusterPath() + '/acls').catch(() => null),
    get(clusterPath() + '/broker-distribution').catch(() => null),
  ]);

  const offline = overview.offlinePartitions ?? 0;
  const urp = overview.underReplicatedPartitions ?? 0;

  const topTopics = [...topics]
    .filter((t) => !t.internal)
    .sort((a, b) => b.messageCount - a.messageCount)
    .slice(0, 6);
  const maxCount = Math.max(1, ...topTopics.map((t) => t.messageCount));

  view.innerHTML = `
    <div class="toolbar" style="align-items:center">
      <h1 style="margin:0">Cluster overview</h1>
      <div class="spacer" style="flex:1"></div>
      <label class="checkbox small"><input type="checkbox" id="auto-refresh" /> auto-refresh (10s)</label>
      <span class="faint small" id="updated-at">updated ${fmtRel(Date.now())}</span>
    </div>

    <div class="grid-kpi">
      <div class="kpi clickable" data-scroll="broker-table"><div class="label">Brokers</div><div class="value">${fmtNum(overview.brokers?.length)}</div></div>
      <div class="kpi clickable" data-goto="#/topics"><div class="label">Topics</div><div class="value">${fmtNum(overview.topicCount)}</div></div>
      <div class="kpi clickable" data-goto="#/topics"><div class="label">Partitions</div><div class="value">${fmtNum(overview.partitionCount)}</div></div>
      <div class="kpi clickable" data-goto="#/explorer"><div class="label">Messages (est.)</div><div class="value">${fmtCompact(overview.estimatedMessages)}</div></div>
      <div class="kpi clickable" data-goto="#/groups"><div class="label">Consumer groups</div><div class="value">${fmtNum(overview.consumerGroupCount < 0 ? '—' : overview.consumerGroupCount)}</div></div>
      <div class="kpi clickable ${urp > 0 ? 'warn' : 'ok'}" data-goto="#/topics"><div class="label">Under-replicated</div><div class="value">${fmtNum(urp)}</div></div>
      <div class="kpi clickable ${offline > 0 ? 'err' : 'ok'}" data-goto="#/topics"><div class="label">Offline partitions</div><div class="value">${fmtNum(offline)}</div></div>
    </div>

    <div class="card" style="margin-bottom:16px">
      <div class="card-title"><h2>Partition distribution across brokers</h2>
        <span class="badge ${distribution?.balanced ? 'tone-ok' : 'tone-warn'}">${distribution?.balanced ? 'balanced' : 'skewed'}</span>
      </div>
      ${renderDistribution(distribution)}
    </div>

    <div class="two-col">
      <div>
        <div class="card" data-broker-table>
          <div class="card-title"><h2>Brokers</h2><span class="faint small">click a broker for its config</span></div>
          <div class="table-wrap"><table class="tbl">
            <thead><tr><th>ID</th><th>Endpoint</th><th>Rack</th><th></th></tr></thead>
            <tbody>
              ${(overview.brokers ?? []).map((b) => `
                <tr class="clickable" data-broker="${b.id}">
                  <td class="mono">${esc(b.id)}</td>
                  <td class="mono">${esc(b.host)}:${esc(b.port)}</td>
                  <td>${b.rack ? esc(b.rack) : '<span class="faint">—</span>'}</td>
                  <td>${b.id === overview.controller ? badge('controller', 'accent') : ''}</td>
                </tr>`).join('')}
            </tbody>
          </table></div>
        </div>

        <div class="card">
          <div class="card-title"><h2>ACLs</h2></div>
          ${renderAcls(acls)}
        </div>
      </div>

      <div>
        <div class="card">
          <div class="card-title"><h2>Messages per topic</h2>
            <a class="link small" href="#/topics">all topics →</a></div>
          ${topTopics.length === 0 ? '<div class="empty-state">No topics yet</div>' :
            topTopics.map((t) => `
              <div class="hbar-row">
                <span class="hbar-name" title="${esc(t.name)}"><a class="link" href="#/topics/${encodeURIComponent(t.name)}">${esc(t.name)}</a></span>
                <div class="hbar-track"><div class="hbar-fill" style="width:${Math.max(1, (t.messageCount / maxCount) * 100)}%"></div></div>
                <span class="hbar-val">${fmtCompact(t.messageCount)}</span>
              </div>`).join('')}
        </div>

        <div class="card">
          <div class="card-title"><h2>Internal topics</h2></div>
          ${renderInternal(topics)}
        </div>
      </div>
    </div>`;

  view.querySelectorAll('tr[data-broker]').forEach((tr) => {
    tr.addEventListener('click', () => brokerConfigsModal(Number(tr.dataset.broker)));
  });

  view.querySelectorAll('.kpi[data-goto]').forEach((card) => {
    card.addEventListener('click', () => { location.hash = card.dataset.goto; });
  });
  view.querySelectorAll('.kpi[data-scroll]').forEach((card) => {
    card.addEventListener('click', () => {
      document.querySelector('[data-broker-table]')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  });

  // partition distribution: click a broker bar to drill down into its partitions
  view.querySelectorAll('.dist-broker').forEach((el) => {
    el.style.cursor = 'pointer';
    el.addEventListener('click', () => {
      const brokerId = Number(el.querySelector('[data-broker-id]')?.dataset.brokerId);
      if (brokerId != null) brokerPartitionsModal(brokerId);
    });
  });

  wireAutoRefresh(view);
}

function renderDistribution(distribution) {
  if (!distribution) return '<div class="empty-state">Not loaded</div>';
  const brokers = distribution.brokers || [];
  if (brokers.length === 0) return '<div class="empty-state">No brokers</div>';

  const maxPartitions = Math.max(1, ...brokers.map((b) => b.totalPartitions || 0));

  return `
    <div class="dist-grid">
      ${brokers.map((b) => {
        const leaderPct = b.totalPartitions > 0 ? Math.round((b.leaderCount / b.totalPartitions) * 100) : 0;
        const followerPct = b.totalPartitions > 0 ? 100 - leaderPct : 0;
        return `
          <div class="dist-broker">
            <div class="dist-head">
              <span class="mono fw-bold">broker ${b.id}</span>
              <span class="faint small">${esc(b.host)}:${esc(b.port)}</span>
            </div>
            <div class="dist-bar">
              <div class="dist-leader" style="width:${leaderPct}%" title="${b.leaderCount} leader partitions">
                <span class="dist-label">${b.leaderCount} L</span>
              </div>
              <div class="dist-follower" style="width:${followerPct}%" title="${b.followerCount} follower partitions">
                <span class="dist-label">${b.followerCount} F</span>
              </div>
            </div>
            <div class="dist-nums">
              <span class="badge tone-accent">${b.leaderCount} leader</span>
              <span class="badge tone-cyan">${b.followerCount} follower</span>
              <span class="badge tone-neutral">${b.totalPartitions} total</span>
              ${b.underReplicated > 0 ? `<span class="badge tone-err">${b.underReplicated} URP</span>` : ''}
            </div>
          </div>`;
      }).join('')}
    </div>
    <div class="dist-legend small faint" style="margin-top:10px">
      <span style="color:var(--accent)">■</span> leader partitions &nbsp;&nbsp;
      <span style="color:var(--cyan)">■</span> follower partitions &nbsp;&nbsp;
      Total replicas: ${distribution.total?.totalReplicas ?? '—'}
    </div>`;
}

function wireAutoRefresh(view) {
  const box = view.querySelector('#auto-refresh');
  const stamp = () => {
    const el = view.querySelector('#updated-at');
    if (el) el.innerHTML = `updated ${fmtRel(Date.now())}`;
  };
  const stop = () => { if (dashTimer) { clearInterval(dashTimer); dashTimer = null; } };
  window.__dashStop = stop; // main.js render() calls this on every route change
  box?.addEventListener('change', () => {
    if (box.checked) {
      dashTimer = setInterval(() => load(view), 10_000);
      stamp();
    } else {
      stop();
    }
  });
}

async function brokerConfigsModal(brokerId) {
  let configs;
  try {
    configs = await get(clusterPath() + `/brokers/${brokerId}/configs`);
  } catch (err) {
    modal({ title: `Broker ${brokerId} configs`, body: `<div class="error-panel">${esc(err.message)}</div>`, actions: [{ label: 'Close', class: 'ghost', onClick: (o, c) => c() }] });
    return;
  }
  modal({
    title: `Broker ${brokerId} — config (${configs.length} entries)`,
    wide: true,
    body: `
      <div class="field"><input type="text" id="bc-search" placeholder="Filter configs…" /></div>
      <div class="table-wrap" style="max-height:480px; overflow-y:auto"><table class="tbl">
        <thead><tr><th>Name</th><th>Value</th><th>Source</th><th></th></tr></thead>
        <tbody id="bc-rows"></tbody>
      </table></div>`,
    actions: [{ label: 'Close', class: 'ghost', onClick: (o, c) => c() }],
    onMount: (overlay) => {
      const paint = (filter) => {
        const f = filter.toLowerCase();
        overlay.querySelector('#bc-rows').innerHTML = configs
          .filter((c) => c.name.toLowerCase().includes(f))
          .map((c) => `<tr>
            <td class="mono">${esc(c.name)}</td>
            <td class="mono small">${esc(c.value ?? 'null')}</td>
            <td><span class="badge tone-neutral">${esc(c.source.replace(/_CONFIG$/, '').toLowerCase())}</span></td>
            <td>${c.readOnly ? '<span class="badge tone-neutral">read-only</span>' : ''}</td>
          </tr>`).join('');
      };
      paint('');
      overlay.querySelector('#bc-search').addEventListener('input', debounce((e) => paint(e.target.value), 150));
    },
  });
}

function renderInternal(topics) {
  const internal = topics.filter((t) => t.internal);
  if (internal.length === 0) return '<div class="empty-state">None</div>';
  return `<div class="table-wrap"><table class="tbl"><tbody>
    ${internal.map((t) => `<tr>
      <td class="mono">${esc(t.name)}</td>
      <td class="num muted">${t.partitions} partition${t.partitions === 1 ? '' : 's'}</td>
      <td class="num muted">${fmtCompact(t.messageCount)} msgs</td>
    </tr>`).join('')}
  </tbody></table></div>`;
}

function renderAcls(acls) {
  if (!acls) return '<div class="empty-state">Not loaded</div>';
  if (!acls.supported) {
    return `<p class="muted small">${esc(acls.reason || 'ACLs are not readable on this cluster')}</p>`;
  }
  if (acls.bindings.length === 0) return '<div class="empty-state">No ACLs defined (cluster-wide allow)</div>';
  return `<div class="table-wrap"><table class="tbl">
    <thead><tr><th>Resource</th><th>Principal</th><th>Operation</th><th>Permission</th><th>Host</th></tr></thead>
    <tbody>${acls.bindings.map((b) => `
      <tr>
        <td class="mono">${esc(b.resourceType)}:${esc(b.resourceName)}</td>
        <td class="mono small">${esc(b.principal)}</td>
        <td>${badge(b.operation, 'cyan')}</td>
        <td>${badge(b.permissionType, b.permissionType === 'ALLOW' ? 'ok' : 'err')}</td>
        <td class="mono small">${esc(b.host)}</td>
      </tr>`).join('')}
    </tbody></table></div>`;
}


async function brokerPartitionsModal(brokerId) {
  let data;
  try {
    data = await get(clusterPath() + `/brokers/${brokerId}/partitions`);
  } catch (err) {
    modal({ title: `Broker ${brokerId} partitions`, body: `<div class="error-panel">${esc(err.message)}</div>`, actions: [{ label: 'Close', class: 'ghost', onClick: (o, c) => c() }] });
    return;
  }
  const renderList = (items, role) => items.length === 0
    ? `<div class="empty-state">No ${role} partitions</div>`
    : `<table class="tbl"><thead><tr><th>Topic</th><th class="num">Partition</th><th class="num">Leader</th><th>ISR</th><th>In sync</th></tr></thead><tbody>
        ${items.map((p) => `<tr>
          <td class="mono">${esc(p.topic)}</td>
          <td class="num">${p.partition}</td>
          <td class="num">${p.leader}</td>
          <td class="mono small">[${p.isr.join(',')}]</td>
          <td>${p.inSync ? badge('yes', 'ok') : badge('no', 'err')}</td>
        </tr>`).join('')}
      </tbody></table>`;

  modal({
    title: `Broker ${brokerId} — ${data.leader.length} leader + ${data.follower.length} follower partitions`,
    wide: true,
    body: `
      <h3 style="margin:0 0 8px;color:var(--accent)">Leader partitions (${data.leader.length})</h3>
      ${renderList(data.leader, 'leader')}
      <h3 style="margin:16px 0 8px;color:var(--cyan)">Follower partitions (${data.follower.length})</h3>
      ${renderList(data.follower, 'follower')}`,
    actions: [{ label: 'Close', class: 'ghost', onClick: (o, c) => c() }],
  });
}
