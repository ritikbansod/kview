// ===== API client + global store =====

export const store = {
  clusterId: localStorage.getItem('kw.cluster') || 'default',
  clusters: [],
};

export async function api(method, path, body) {
  const res = await fetch('/api' + path, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
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
