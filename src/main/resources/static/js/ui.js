// ===== Small UI helpers: escaping, formatting, modals, toasts, JSON viewer =====

export function esc(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;').replaceAll("'", '&#39;');
}

export function fmtNum(n) {
  if (n === null || n === undefined || n < 0) return '—';
  return Number(n).toLocaleString('en-US');
}

export function fmtCompact(n) {
  if (n === null || n === undefined || n < 0) return '—';
  if (n < 1000) return String(n);
  if (n < 1e6) return (n / 1e3).toFixed(n < 1e4 ? 1 : 0) + 'k';
  if (n < 1e9) return (n / 1e6).toFixed(1) + 'M';
  return (n / 1e9).toFixed(1) + 'B';
}

export function fmtTs(ms) {
  if (!ms && ms !== 0) return '—';
  const d = new Date(ms);
  return d.toLocaleString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

/** "just now" / "4m ago" — absolute time goes into the title attribute. */
export function fmtRel(ms) {
  if (!ms && ms !== 0) return '—';
  const diff = Date.now() - ms;
  const title = ` title="${esc(fmtTs(ms))}"`;
  if (diff < 15_000) return `<span${title}>just now</span>`;
  if (diff < 60_000) return `<span${title}>${Math.floor(diff / 1000)}s ago</span>`;
  if (diff < 3_600_000) return `<span${title}>${Math.floor(diff / 60_000)}m ago</span>`;
  if (diff < 86_400_000) return `<span${title}>${Math.floor(diff / 3_600_000)}h ago</span>`;
  return `<span${title}>${fmtTs(ms)}</span>`;
}

export function badge(text, tone = 'neutral') {
  return `<span class="badge tone-${tone}">${esc(text)}</span>`;
}

/** Shimmering placeholder rows shown while a view loads. cols: [flex, flex, ...] grid template. */
export function skeletonTable(rows = 6, cols = 5) {
  const row = `<div class="skeleton-row" style="grid-template-columns:repeat(${cols}, 1fr)">` +
    Array.from({ length: cols }, () => '<div class="skeleton"></div>').join('') + '</div>';
  return `<div>${row.repeat(rows)}</div>`;
}

export async function copyText(text, label = 'Copied to clipboard') {
  try {
    await navigator.clipboard.writeText(text);
    toast(label, 'ok', 2000);
  } catch {
    toast('Copy failed — clipboard not available', 'warn');
  }
}

/** Wires click-to-sort on a table's th.sortable headers. reRender(sortKey, dir) is called on change. */
export function makeSortable(tableEl, defaultKey, defaultDir, reRender) {
  let sortKey = defaultKey;
  let dir = defaultDir; // 1 asc, -1 desc
  const paint = () => {
    tableEl.querySelectorAll('th.sortable').forEach(th => {
      const arrow = th.dataset.key === sortKey ? `<span class="sort-arrow">${dir === 1 ? '▲' : '▼'}</span>` : '';
      const label = th.textContent.replace(/[▲▼]\s*$/, '').replace(/ $/, '');
      th.innerHTML = esc(label) + arrow;
    });
  };
  paint();
  tableEl.addEventListener('click', (e) => {
    const th = e.target.closest('th.sortable');
    if (!th) return;
    const key = th.dataset.key;
    if (key === sortKey) dir = -dir; else { sortKey = key; dir = 1; }
    paint();
    reRender(sortKey, dir);
  });
  return { get key() { return sortKey; }, get dir() { return dir; } };
}

export function protocolBadge(security) {
  const proto = security?.protocol || 'PLAINTEXT';
  const tone = proto === 'PLAINTEXT' ? 'neutral' : proto === 'SSL' ? 'cyan' : 'purple';
  const mech = proto.startsWith('SASL') && security?.saslMechanism ? ' · ' + security.saslMechanism : '';
  return badge(proto + mech, tone);
}

export function stateTone(state) {
  switch (state) {
    case 'Stable': return 'ok';
    case 'Empty': return 'neutral';
    case 'Dead': return 'err';
    case 'NoActiveGroup': return 'neutral';
    default: return 'warn'; // PreparingRebalance, CompletingRebalance, unknown
  }
}

export function spinner() {
  return '<div class="spinner"></div>';
}

export function errorPanel(err) {
  return `<div class="error-panel"><b>Request failed:</b> ${esc(err.message || err)}</div>`;
}

// ---- toasts ----
export function toast(message, type = 'ok', ms = 4200) {
  const root = document.getElementById('toast-root');
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.textContent = message;
  root.appendChild(el);
  setTimeout(() => el.remove(), ms);
}

// ---- modal ----
export function modal({ title, body, wide = false, onMount, actions = [] }) {
  const root = document.getElementById('modal-root');
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal ${wide ? 'wide' : ''}">
      <h2>${esc(title)}</h2>
      <div class="modal-body">${body}</div>
      <div class="modal-actions">
        ${actions.map((a, i) => `<button class="btn ${a.class || ''}" data-action="${i}">${esc(a.label)}</button>`).join('')}
      </div>
    </div>`;
  const close = () => { document.removeEventListener('keydown', onKey); overlay.remove(); };
  const onKey = (e) => { if (e.key === 'Escape') close(); };
  document.addEventListener('keydown', onKey);
  overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });
  actions.forEach((a, i) => {
    overlay.querySelector(`[data-action="${i}"]`).addEventListener('click', () => a.onClick?.(overlay, close));
  });
  root.appendChild(overlay);
  onMount?.(overlay, close);
  return close;
}

export function confirmDialog({ title, message, confirmLabel = 'Delete', onConfirm }) {
  modal({
    title,
    body: `<p class="muted">${esc(message)}</p>`,
    actions: [
      { label: 'Cancel', class: 'ghost', onClick: (o, close) => close() },
      { label: confirmLabel, class: 'danger', onClick: async (o, close) => { await onConfirm(); close(); } },
    ],
  });
}

// ---- JSON ----
function highlightJson(html) {
  return html
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"([^"]+)":/g, '<span class="j-key">"$1"</span>:')
    .replace(/: ?"((?:[^"\\]|\\.)*)"/g, ': <span class="j-str">"$1"</span>')
    .replace(/: ?(-?\d+\.?\d*(?:[eE][+-]?\d+)?)/g, ': <span class="j-num">$1</span>')
    .replace(/: ?(true|false)/g, ': <span class="j-bool">$1</span>')
    .replace(/: ?(null)/g, ': <span class="j-null">$1</span>');
}

export function jsonBlock(value) {
  let text;
  if (typeof value === 'string') {
    try { value = JSON.parse(value); } catch { /* plain text */ }
  }
  if (value !== null && typeof value === 'object') {
    text = JSON.stringify(value, null, 2);
  } else {
    text = String(value ?? '');
    if (text === '') return '<div class="json-view faint">(empty)</div>';
    return `<div class="json-view">${esc(text)}</div>`;
  }
  return `<div class="json-view">${highlightJson(text)}</div>`;
}

/** Pretty-printed preview for a value cell (single line, truncated). */
export function previewValue(value, max = 140) {
  if (value === null || value === undefined) return '<span class="faint">null</span>';
  let text = value;
  try { text = JSON.stringify(JSON.parse(value)); } catch { /* keep raw */ }
  const oneLine = text.replaceAll('\n', ' ').replaceAll('\t', ' ');
  return esc(oneLine.length > max ? oneLine.slice(0, max) + '…' : oneLine);
}

export function debounce(fn, ms = 300) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

export function groupStatusTone(state) { return stateTone(state); }
