// ===== API client + global store =====

const TOKEN_KEY = 'kview.token';

export const store = {
  clusterId: localStorage.getItem('kw.cluster') || 'default',
  clusters: [],
};

export const getToken = () => localStorage.getItem(TOKEN_KEY) || '';

export function setToken(token) {
  if (token && token.trim()) localStorage.setItem(TOKEN_KEY, token.trim());
  else localStorage.removeItem(TOKEN_KEY);
}

function headers(body) {
  const h = { 'X-Kview-Client': 'kview-ui' };
  const token = getToken();
  if (token) h.Authorization = `Bearer ${token}`;
  if (body !== undefined) h['Content-Type'] = 'application/json';
  return h;
}

export async function api(method, path, body, retry = true) {
  const res = await fetch('/api' + path, {
    method,
    headers: headers(body),
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (res.status === 401 && retry) {
    const entered = prompt('This Kview server requires an API token.\n'
      + 'Enter a token configured in KVIEW_AUTH_TOKENS on the server:');
    if (entered !== null && entered.trim()) {
      setToken(entered);
      return api(method, path, body, false);
    }
  }
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      const err = await res.json();
      if (err.detail) detail = err.detail;
      else if (err.error) detail = err.error;
    } catch { /* non-JSON error */ }
    const e = new Error(detail);
    e.status = res.status;
    throw e;
  }
  if (res.status === 204) return null;
  const text = await res.text();
  return text ? JSON.parse(text) : null;
}

export const get = (p) => api('GET', p);
export const post = (p, b) => api('POST', p, b ?? {});
export const put = (p, b) => api('PUT', p, b);
export const del = (p) => api('DELETE', p);

export const clusterPath = (id = store.clusterId) => '/clusters/' + encodeURIComponent(id);

export function setActiveCluster(id) {
  store.clusterId = id;
  localStorage.setItem('kw.cluster', id);
}

/** All non-internal topics of the active cluster: [{name, partitions}] */
export async function loadTopics() {
  const topics = await get(clusterPath() + '/topics?includeCounts=false');
  return topics
    .filter((t) => !t.internal)
    .map((t) => ({ name: t.name, partitions: t.partitions }))
    .sort((a, b) => a.name.localeCompare(b.name));
}
