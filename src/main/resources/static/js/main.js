// ===== App bootstrap: cluster selector + hash router =====
import { get, store, setActiveCluster } from './api.js';
import { esc, protocolBadge } from './ui.js';
import { renderDashboard } from './views/dashboard.js';
import { renderTopics, renderTopicDetail } from './views/topics.js';
import { renderExplorer } from './views/explorer.js';
import { renderGroups, renderGroupDetail } from './views/groups.js';
import { renderConnections } from './views/connections.js';

const viewEl = document.getElementById('view');
const clusterSelect = document.getElementById('cluster-select');

const routes = [
  { pattern: /^#\/$/, render: () => renderDashboard(viewEl) },
  { pattern: /^#\/topics$/, render: () => renderTopics(viewEl) },
  { pattern: /^#\/topics\/(.+)$/, render: (m) => renderTopicDetail(viewEl, decodeURIComponent(m[1])) },
  { pattern: /^#\/explorer$/, render: (m, query) => renderExplorer(viewEl, query) },
  { pattern: /^#\/groups$/, render: () => renderGroups(viewEl) },
  { pattern: /^#\/groups\/(.+)$/, render: (m) => renderGroupDetail(viewEl, decodeURIComponent(m[1])) },
  { pattern: /^#\/connections$/, render: () => renderConnections(viewEl) },
];

function parseHash() {
  const raw = location.hash || '#/';
  const [path, queryString] = raw.split('?');
  const query = Object.fromEntries(new URLSearchParams(queryString || ''));
  return { path: path === '' ? '#/' : path, query };
}

export async function render() {
  window.__tailClose?.();
  window.__tailClose = null;
  window.__dashStop?.();   // stops dashboard auto-refresh when leaving the view
  window.__dashStop = null;
  const { path, query } = parseHash();
  const route = routes.find((r) => r.pattern.test(path)) || routes[0];
  const match = path.match(route.pattern);

  // active nav item
  const navPath = path === '#/' ? '/' : path.slice(1).replace(/\/[^/]*$/, '') || path.slice(1);
  document.querySelectorAll('.nav-item').forEach((a) => {
    const r = a.dataset.route;
    a.classList.toggle('active', path === `#${r}` || path.startsWith(`#${r}/`));
  });

  try {
    await route.render(match, query);
  } catch (err) {
    viewEl.innerHTML = `
      <h1>Cluster unreachable</h1>
      <p class="page-sub">The active cluster <span class="mono">${esc(store.clusterId)}</span> did not respond.</p>
      <div class="error-panel"><b>${esc(err.message || err)}</b></div>
      <p class="muted">Start a broker on the configured bootstrap servers, or point the wrapper at another
      cluster. Everything else in the app keeps working in the meantime.</p>
      <div class="btn-row mt8">
        <a class="btn primary" href="#/connections">Manage connections</a>
        <button class="btn ghost" id="retry-btn">Retry</button>
      </div>`;
    viewEl.querySelector('#retry-btn').addEventListener('click', render);
  }
}

async function loadClusters() {
  const clusters = await get('/clusters');
  store.clusters = clusters;
  clusterSelect.innerHTML = clusters
    .map((c) => `<option value="${esc(c.id)}">${esc(c.name)}</option>`)
    .join('');
  const known = clusters.some((c) => c.id === store.clusterId);
  if (!known) setActiveCluster('default');
  clusterSelect.value = store.clusterId;
  updateProtocolBadge();
}

function updateProtocolBadge() {
  const active = store.clusters.find((c) => c.id === store.clusterId);
  const current = document.getElementById('cluster-protocol');
  if (!current) return;
  const tmp = document.createElement('div');
  tmp.innerHTML = protocolBadge(active?.security);
  current.replaceWith(tmp.firstChild);
}

clusterSelect.addEventListener('change', () => {
  setActiveCluster(clusterSelect.value);
  updateProtocolBadge();
  render();
});

document.getElementById('refresh-btn').addEventListener('click', render);
window.addEventListener('hashchange', render);

// ---- theme toggle ----
const themeBtn = document.getElementById('theme-btn');
function paintThemeBtn() {
  const dark = document.documentElement.dataset.theme !== 'light';
  themeBtn.textContent = dark ? '☀︎ Light' : '☾ Dark';
  themeBtn.title = dark ? 'Switch to light theme' : 'Switch to dark theme';
}
themeBtn.addEventListener('click', () => {
  const next = document.documentElement.dataset.theme === 'light' ? 'dark' : 'light';
  document.documentElement.dataset.theme = next;
  localStorage.setItem('kw.theme', next);
  paintThemeBtn();
});
paintThemeBtn();

loadClusters().then(render).catch((err) => {
  document.getElementById('view').innerHTML =
    `<h1>Backend unreachable</h1><div class="error-panel">${esc(err.message || err)}</div>
     <p class="muted">Is the kview process running on port 8090?</p>`;
});
