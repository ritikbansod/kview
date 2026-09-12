// ===== Shared broker partitions drill-down: fixed-height modal, scrolling body, broker switcher =====
import { get, clusterPath } from '../api.js';
import { esc, modal } from '../ui.js';

export function brokerPartitionsModal(brokerId, brokerIds = []) {
  const ids = brokerIds.length ? brokerIds : [brokerId];
  const close = modal({
    title: `Broker ${brokerId} partitions`,
    wide: true,
    body: `
      <div class="arch-broker-switch">
        ${ids.map((id) => `
          <button type="button" class="arch-broker-chip ${id === brokerId ? 'is-active' : ''}" data-switch="${id}"
                  title="Show partitions hosted by broker ${id}">
            <span class="mono fw-bold">broker ${id}</span>
          </button>`).join('')}
      </div>
      <div class="bp-content"><div class="empty-state">Loading…</div></div>`,
    actions: [{ label: 'Close', class: 'ghost', onClick: (o, c) => c() }],
    onMount: (overlay) => {
      overlay.querySelector('.modal').classList.add('modal-tall'); // static card, scrolling body
      overlay.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-switch]');
        if (btn) loadBroker(overlay, Number(btn.dataset.switch));
      });
      loadBroker(overlay, brokerId);
    },
  });
  return close;
}

async function loadBroker(overlay, id) {
  const content = overlay.querySelector('.bp-content');
  overlay.querySelectorAll('[data-switch]').forEach((b) =>
    b.classList.toggle('is-active', Number(b.dataset.switch) === id));
  content.innerHTML = '<div class="empty-state">Loading…</div>';
  let data;
  try {
    data = await get(clusterPath() + `/brokers/${id}/partitions`);
  } catch (err) {
    content.innerHTML = `<div class="error-panel">${esc(err.message)}</div>`;
    return;
  }
  overlay.querySelector('h2').textContent =
    `Broker ${id} — ${data.leader.length} leader + ${data.follower.length} follower partitions`;
  content.innerHTML = `
    <h3 style="margin:0 0 8px;color:var(--accent)">Leader partitions (${data.leader.length})</h3>
    ${table(data.leader, 'leader')}
    <h3 style="margin:16px 0 8px;color:var(--cyan)">Follower partitions (${data.follower.length})</h3>
    ${table(data.follower, 'follower')}`;
}

function table(items, role) {
  if (items.length === 0) return `<div class="empty-state">No ${role} partitions</div>`;
  return `<table class="tbl"><thead><tr><th>Topic</th><th class="num">Partition</th><th class="num">Leader</th><th>ISR</th></tr></thead><tbody>
    ${items.map((p) => `<tr>
      <td class="mono">${esc(p.topic)}</td>
      <td class="num">${p.partition}</td>
      <td class="num">${p.leader}</td>
      <td class="mono small">[${p.isr.join(',')}]</td>
    </tr>`).join('')}
  </tbody></table>`;
}
