// ===== Consumer Groups: list with lag + detail with per-partition lag bars =====
import { get, post, del, clusterPath, loadTopics } from '../api.js';
import {
  esc, fmtNum, fmtCompact, badge, stateTone, skeletonTable, toast, confirmDialog, modal, makeSortable,
} from '../ui.js';

export async function renderGroups(view) {
  view.innerHTML = `<h1>Consumer Groups</h1>${skeletonTable(5, 4)}`;
  const groups = await get(clusterPath() + '/groups');

  const sortState = { key: 'groupId', dir: 1 };
  const sorted = () => [...groups].sort((a, b) => {
    const k = sortState.key;
    const va = k === 'groupId' ? a.groupId.toLowerCase() : a[k];
    const vb = k === 'groupId' ? b.groupId.toLowerCase() : b[k];
    return (va < vb ? -1 : va > vb ? 1 : 0) * sortState.dir;
  });
  const bodyRows = () => sorted().map((g) => `
    <tr class="clickable" tabindex="0" data-group="${esc(g.groupId)}">
      <td class="mono">${esc(g.groupId)}</td>
      <td>${badge(g.state, stateTone(g.state))}</td>
      <td class="num">${g.committedPartitions}</td>
      <td class="num">${g.totalLag > 0 ? badge(fmtCompact(g.totalLag), 'warn') : `<span class="muted">0</span>`}</td>
      <td><a class="link small">details</a></td>
    </tr>`).join('');

  view.innerHTML = `
    <div class="toolbar" style="align-items:center">
      <h1 style="margin:0">Consumer Groups <span class="muted small">(${groups.length})</span></h1>
    </div>
    <div class="card">
      ${groups.length === 0 ? `<div class="empty-state">No consumer groups on this cluster
        <p class="small faint" style="margin:8px 0 0">Groups appear once an application consumes a topic with a
        group.id — or tail a topic with a group set in the Data Explorer to see one form.</p></div>` : `}
      <div class="table-wrap"><table class="tbl">
        <thead><tr>
          <th class="sortable" data-key="groupId">Group ID</th>
          <th class="sortable" data-key="state">State</th>
          <th class="sortable num" data-key="committedPartitions">Committed partitions</th>
          <th class="sortable num" data-key="totalLag">Total lag</th>
          <th></th>
        </tr></thead>
        <tbody id="group-rows"></tbody>
      </table></div>`}
    </div>`;

  if (groups.length > 0) {
    const tbody = view.querySelector('#group-rows');
    tbody.innerHTML = bodyRows();
    makeSortable(view.querySelector('table.tbl'), 'totalLag', -1, (key, dir) => {
      sortState.key = key; sortState.dir = dir;
      tbody.innerHTML = bodyRows();
    });
    view.querySelectorAll('tr[data-group]').forEach((tr) => {
      tr.addEventListener('click', () => {
        location.hash = `#/groups/${encodeURIComponent(tr.dataset.group)}`;
      });
    });
    tbody.addEventListener('keydown', (e) => {
      if ((e.key === 'Enter' || e.key === ' ') && e.target.matches('tr[data-group]')) {
        e.preventDefault();
        e.target.click();
      }
    });
  }
}

export async function renderGroupDetail(view, groupId) {
  view.innerHTML = `<div class="toolbar" style="align-items:center"><h1 class="mono" style="margin:0">${esc(groupId)}</h1></div>${skeletonTable(6, 4)}`;
  const [detail, topics] = await Promise.all([
    get(clusterPath() + `/groups/${encodeURIComponent(groupId)}`),
    loadTopics().catch(() => []),
  ]);

  const maxLag = Math.max(1, ...Object.values(detail.partitionsByTopic).flat().map((p) => p.lag));

  view.innerHTML = `
    <div class="toolbar" style="align-items:center">
      <div>
        <h1 class="mono" style="margin:0">${esc(detail.groupId)}</h1>
        <p class="muted small" style="margin:4px 0 0">
          ${badge(detail.state, stateTone(detail.state))}
          ${detail.coordinatorId !== null ? ` · coordinator broker <span class="mono">${detail.coordinatorId}</span>` : ''}
          · total lag <b style="color:${detail.totalLag > 0 ? 'var(--warn)' : 'var(--ok)'}">${fmtNum(detail.totalLag)}</b>
        </p>
      </div>
      <div class="spacer" style="flex:1"></div>
      <div class="btn-row">
        <a class="btn ghost" href="#/groups">← all groups</a>
        <button class="btn ghost" id="reset-btn">Reset offsets…</button>
        <button class="btn danger" id="delete-btn">Delete group</button>
      </div>
    </div>

    <div class="card">
      <div class="card-title"><h2>Members (${detail.members.length})</h2></div>
      ${detail.members.length === 0 ? '<div class="empty-state">No active members — the group is idle</div>' : `
      <div class="table-wrap"><table class="tbl">
        <thead><tr><th>Client ID</th><th>Host</th><th class="num">Assigned partitions</th><th>Assignments</th></tr></thead>
        <tbody>${detail.members.map((m) => `
          <tr>
            <td class="mono small">${esc(m.clientId)}</td>
            <td class="mono small">${esc(m.host)}</td>
            <td class="num">${m.assignments.length}</td>
            <td class="mono small muted">${m.assignments.slice(0, 8).map(esc).join(', ')}${m.assignments.length > 8 ? ' …' : ''}</td>
          </tr>`).join('')}
        </tbody></table></div>`}
    </div>

    <div class="card">
      <div class="card-title"><h2>Committed offsets & lag</h2></div>
      ${detail.committedPartitions === 0 ? '<div class="empty-state">No committed offsets yet</div>' : `
        ${Object.entries(detail.partitionsByTopic).map(([topic, partitions]) => `
          <h2 class="mt16 mono small">${esc(topic)}</h2>
          <div class="table-wrap"><table class="tbl">
            <thead><tr><th>Partition</th><th class="num">Committed</th><th class="num">End offset</th><th style="width:40%">Lag</th></tr></thead>
            <tbody>${partitions.map((p) => `
              <tr>
                <td class="mono">${p.partition}</td>
                <td class="num">${fmtNum(p.committedOffset)}</td>
                <td class="num">${p.endOffset === null ? '—' : fmtNum(p.endOffset)}</td>
                <td>
                  <div class="hbar-row" style="grid-template-columns: 1fr 80px">
                    <div class="hbar-track"><div class="hbar-fill ${p.lag > 0 ? '' : 'cyan'}" style="width:${Math.max(1, (p.lag / maxLag) * 100)}%"></div></div>
                    <span class="hbar-val">${fmtNum(p.lag)}</span>
                  </div>
                </td>
              </tr>`).join('')}
            </tbody></table></div>`).join('')}`}
    </div>`;

  view.querySelector('#delete-btn').addEventListener('click', () => {
    confirmDialog({
      title: `Delete group "${groupId}"?`,
      message: 'The group and its committed offsets will be removed from the cluster.',
      onConfirm: async () => {
        try {
          const result = await del(clusterPath() + `/groups/${encodeURIComponent(groupId)}`);
          toast(`Group ${groupId}: ${result.status}`);
          location.hash = '#/groups';
        } catch (err) { toast(err.message, 'err'); }
      },
    });
  });

  view.querySelector('#reset-btn').addEventListener('click', () => resetOffsetsModal(detail, topics, view));
}

function resetOffsetsModal(detail, topics, view) {
  modal({
    title: `Reset offsets of "${detail.groupId}"`,
    body: `
      <p class="muted small">Offsets can only be reset while the group is inactive
      (state Empty/Dead). Current state: <b>${esc(detail.state)}</b>.</p>
      <div class="form-grid">
        <div class="field"><label>Topic</label>
          <select id="ro-topic">${topics.map((t) => `<option value="${esc(t.name)}">${esc(t.name)}</option>`).join('')}</select></div>
        <div class="field"><label>Mode</label>
          <select id="ro-mode">
            <option value="EARLIEST">earliest</option>
            <option value="LATEST">latest</option>
            <option value="OFFSET">specific offset</option>
            <option value="TIMESTAMP">timestamp</option>
          </select></div>
      </div>
      <div class="field" id="ro-value-wrap" style="display:none"><label>Value</label>
        <input type="text" id="ro-value" placeholder="offset number or epoch ms" /></div>`,
    actions: [
      { label: 'Cancel', class: 'ghost', onClick: (o, close) => close() },
      {
        label: 'Reset offsets', class: 'primary',
        onClick: async (o, close) => {
          const mode = o.querySelector('#ro-mode').value;
          const needsValue = mode === 'OFFSET' || mode === 'TIMESTAMP';
          const raw = o.querySelector('#ro-value').value.trim();
          if (needsValue && (raw === '' || Number.isNaN(Number(raw)))) {
            toast('A numeric value is required for this mode', 'warn');
            return;
          }
          try {
            const result = await post(clusterPath() + `/groups/${encodeURIComponent(detail.groupId)}/offsets/reset`, {
              topic: o.querySelector('#ro-topic').value,
              mode,
              value: needsValue ? Number(raw) : null,
            });
            if (result.status === 'reset') {
              toast(`Reset ${result.partitionsReset} partition(s) to ${result.mode.toLowerCase()}`);
              close();
              renderGroupDetail(view, detail.groupId);
            } else {
              toast(result.reason || 'Reset failed', 'err');
            }
          } catch (err) { toast(err.message, 'err'); }
        },
      },
    ],
    onMount: (overlay) => {
      overlay.querySelector('#ro-mode').addEventListener('change', (e) => {
        overlay.querySelector('#ro-value-wrap').style.display =
          e.target.value === 'OFFSET' || e.target.value === 'TIMESTAMP' ? '' : 'none';
      });
    },
  });
}
