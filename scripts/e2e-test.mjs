#!/usr/bin/env node
/**
 * End-to-end functional test for the Kview.
 * Requires: kview app on :8090 and a reachable Kafka broker per the default cluster.
 * Run: node scripts/e2e-test.mjs
 *
 * Note: on Windows, deleting a topic immediately after writing to it can crash a local
 * dev broker (memory-mapped index files block the log-dir rename). This suite detects
 * that, restarts the local broker (.kafka/start-kafka.bat) and re-verifies.
 */
import { spawn } from 'node:child_process';

const BASE = process.env.BASE_URL || 'http://localhost:8090';
const START_KAFKA_BAT = process.env.START_KAFKA_BAT || '.kafka/start-kafka.bat';

let passed = 0, failed = 0;
const failures = [];

function check(name, cond, detail = '') {
  if (cond) { passed++; console.log(`  PASS  ${name}`); }
  else { failed++; failures.push(name + (detail ? ` — ${detail}` : '')); console.log(`  FAIL  ${name}${detail ? ' — ' + detail : ''}`); }
}

async function api(method, path, body) {
  const res = await fetch(BASE + path, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  let json = null;
  const text = await res.text();
  try { json = text ? JSON.parse(text) : null; } catch { json = text; }
  return { status: res.status, json };
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function kafkaUp() {
  try {
    const r = await api('GET', '/api/clusters/default/overview');
    return r.status === 200;
  } catch { return false; }
}

/** Restart the local dev broker if a destructive test took it down (Windows rename crash). */
async function recoverBrokerIfNeeded() {
  await sleep(5000); // the log-dir failure surfaces asynchronously after a delete
  if (await kafkaUp()) return true;
  console.log('  ..    broker down after destructive test — restarting local Kafka');
  const { resolve } = await import('node:path');
  const fs = await import('node:fs');
  const kafkaDir = resolve(START_KAFKA_BAT, '..', 'kafka');
  const dataDir = resolve(kafkaDir, '..', 'data');
  // remove leftovers of already-deleted topics so startup doesn't trip over them
  try {
    for (const dir of fs.readdirSync(dataDir)) {
      if (/^(e2e-topic|legacy-topic)/.test(dir)) {
        fs.rmSync(resolve(dataDir, dir), { recursive: true, force: true });
      }
    }
  } catch { /* best effort */ }
  const java = process.env.JAVA_EXE || 'C:/Program Files/Java/jdk-21/bin/java.exe';
  const logFd = fs.openSync(resolve(kafkaDir, '..', 'broker.log'), 'a');
  const child = spawn(java, [
    '-Xmx1G', '-server', '-XX:+UseG1GC',
    `-Dkafka.logs.dir=${resolve(kafkaDir, 'logs').replaceAll('\\', '/')}`,
    '-Dlog4j2.configurationFile=file:config/log4j2.yaml',
    '-cp', 'libs/*',
    'kafka.Kafka', 'server.properties',
  ], { cwd: kafkaDir, detached: true, stdio: ['ignore', logFd, logFd] });
  child.unref();
  for (let i = 0; i < 24; i++) {
    await sleep(5000);
    if (await kafkaUp()) { console.log('  ..    broker is back'); return true; }
  }
  return false;
}
// unique per run — immune to leftovers from a crashed previous run (Windows broker crashes)
const RUN = String(Date.now()).slice(-6);
const TOPIC = `e2e-topic-${RUN}`;
const LEGACY_TOPIC = `legacy-topic-${RUN}`;
const MISSING_TOPIC = `no-such-topic-${RUN}`;

async function main() {
  console.log(`\n=== Kview E2E — ${BASE} ===\n`);
  await recoverBrokerIfNeeded();

  // ---------- 1. Health & cluster ----------
  console.log('[1] Cluster, brokers, ACLs');
  const health = await api('GET', '/actuator/health');
  check('health endpoint returns UP', health.json?.status === 'UP');

  let overview = await api('GET', '/api/clusters/default/overview');
  check('overview 200', overview.status === 200);
  check('overview has broker', (overview.json?.brokers?.length ?? 0) >= 1);
  check('overview has controller', Number.isInteger(overview.json?.controller));
  check('overview counts topics', (overview.json?.topicCount ?? 0) >= 1);

  const brokers = await api('GET', '/api/clusters/default/brokers');
  check('brokers list 200 with endpoint', brokers.status === 200 && brokers.json[0]?.host?.length > 0);

  const brokerConfigs = await api('GET', '/api/clusters/default/brokers/1/configs');
  check('broker configs non-empty', brokerConfigs.status === 200 && brokerConfigs.json.length > 5);
  const brokerConfigs404 = await api('GET', '/api/clusters/default/brokers/999/configs');
  check('unknown broker -> 404', brokerConfigs404.status === 404);

  const acls = await api('GET', '/api/clusters/default/acls');
  check('acls endpoint answers', acls.status === 200 && typeof acls.json?.supported === 'boolean');

  // ---------- 2. Topic lifecycle ----------
  console.log('[2] Topic lifecycle');
  const created = await api('POST', '/api/clusters/default/topics', { name: TOPIC, partitions: 2, replicationFactor: 1, configs: { 'retention.ms': '604800000' } });
  check('create topic -> created/already-exists', created.status === 200 || created.status === 201);
  const dup = await api('POST', '/api/clusters/default/topics', { name: TOPIC, partitions: 2, replicationFactor: 1 });
  check('duplicate create -> already-exists', dup.json?.status === 'already-exists');

  let detail = await api('GET', `/api/clusters/default/topics/${TOPIC}`);
  check('topic detail has 2 partitions', detail.json?.partitions?.length === 2);
  check('topic detail has offsets', detail.json.partitions.every(p => p.endOffset >= 0 && p.beginningOffset >= 0));
  check('topic detail has configs', detail.json.configs?.length > 5);
  check('created config applied', detail.json.configs?.some(c => c.name === 'retention.ms' && c.value === '604800000'));

  const resized = await api('POST', `/api/clusters/default/topics/${TOPIC}/partitions`, { totalPartitions: 4 });
  check('increase partitions -> altered', resized.status === 200 && resized.json?.status === 'altered');
  detail = await api('GET', `/api/clusters/default/topics/${TOPIC}`);
  check('topic now has 4 partitions', detail.json.partitions.length === 4);

  const altered = await api('PUT', `/api/clusters/default/topics/${TOPIC}/configs`, { configs: { 'retention.ms': '86400000' } });
  check('alter configs -> altered', altered.json?.status === 'altered');

  const badTopic = await api('POST', '/api/clusters/default/topics', { name: '', partitions: 0, replicationFactor: 0 });
  check('invalid topic create -> 400', badTopic.status === 400);
  const missing = await api('GET', `/api/clusters/default/topics/${MISSING_TOPIC}`);
  check('unknown topic -> 404', missing.status === 404);

  // ---------- 3. Produce ----------
  console.log('[3] Produce');
  const offsetsByPartition = new Map();
  let produceOk = true;
  for (let i = 1; i <= 10; i++) {
    const r = await api('POST', `/api/clusters/default/topics/${TOPIC}/messages`, {
      key: `key-${i}`,
      payload: JSON.stringify({ n: i, even: i % 2 === 0 }),
      headers: { source: 'e2e-test', index: String(i) },
    });
    if (r.status !== 201) { produceOk = false; break; }
    const { partition, offset } = r.json;
    check(`produce #${i} returns partition/offset`, Number.isInteger(partition) && Number.isInteger(offset) && partition >= 0 && partition <= 3, `p${partition} #${offset}`);
    if (offset !== offsetsByPartition.get(partition) && offsetsByPartition.has(partition)) produceOk = false;
    offsetsByPartition.set(partition, offset + 1);
  }
  check('offsets increase monotonically per partition', produceOk);

  const pinned = await api('POST', `/api/clusters/default/topics/${TOPIC}/messages`, { key: 'pinned', payload: '"pinned"', partition: 2 });
  check('pinned partition honored', pinned.json?.partition === 2);

  const emptyPayload = await api('POST', `/api/clusters/default/topics/${TOPIC}/messages`, { payload: '' });
  check('empty payload -> 400', emptyPayload.status === 400);

  // ---------- 4. Browse ----------
  console.log('[4] Browse (read-only explorer)');
  const browseAll = await api('POST', `/api/clusters/default/topics/${TOPIC}/browse`, { start: 'earliest', limit: 100, timeoutMs: 10000 });
  check('browse earliest returns 11', browseAll.json?.messages?.length === 11, `got ${browseAll.json?.messages?.length}`);
  const first = browseAll.json.messages[0];
  check('messages sorted by partition/offset', browseAll.json.messages.every((m, i, a) => i === 0 || a[i - 1].partition < m.partition || (a[i - 1].partition === m.partition && a[i - 1].offset < m.offset)));
  check('headers decoded', first.headers?.source === 'e2e-test');
  check('key preserved', first.key === 'key-1');
  check('value preserved', JSON.parse(first.value).n === 1);
  check('timestamp plausible', Math.abs(Date.now() - first.timestamp) < 10 * 60 * 1000);

  const browseLatest = await api('POST', `/api/clusters/default/topics/${TOPIC}/browse`, { start: 'latest', limit: 8, timeoutMs: 10000 });
  check('browse latest returns newest records', (browseLatest.json?.messages?.length ?? 0) >= 3 && browseLatest.json.messages.length <= 8);
  const lastAll = browseAll.json.messages.at(-1);
  check('latest includes the pinned record', browseLatest.json.messages.some(m => m.key === 'pinned'));

  const firstPartitionOffset = first.partition === 0 ? first.offset : (browseAll.json.messages.find(m => m.partition === 0)?.offset ?? 0);
  const browseOffsets = await api('POST', `/api/clusters/default/topics/${TOPIC}/browse`, { start: 'offsets', offsets: { 0: firstPartitionOffset + 1 }, limit: 10, timeoutMs: 10000 });
  check('browse from specific offset skips earlier ones', browseOffsets.json?.messages?.every(m => !(m.partition === 0 && m.offset <= firstPartitionOffset)));
  check('browse offsets limited to partition 0', browseOffsets.json?.messages?.every(m => m.partition === 0));

  const browseTs = await api('POST', `/api/clusters/default/topics/${TOPIC}/browse`, { start: 'timestamp', timestamp: Date.now() - 5 * 60 * 1000, limit: 100, timeoutMs: 10000 });
  check('browse from timestamp returns records', (browseTs.json?.messages?.length ?? 0) >= 1);

  const browseFiltered = await api('POST', `/api/clusters/default/topics/${TOPIC}/browse`, { start: 'earliest', limit: 100, timeoutMs: 10000, keyContains: 'key-1' });
  check('key filter narrows results', browseFiltered.json?.messages?.length === 2 && browseFiltered.json.messages.every(m => /^key-1/.test(m.key)), `got ${browseFiltered.json?.messages?.length}`);

  const browsePartition = await api('POST', `/api/clusters/default/topics/${TOPIC}/browse`, { partition: 1, start: 'earliest', limit: 100, timeoutMs: 10000 });
  check('partition filter honored', browsePartition.json?.messages?.every(m => m.partition === 1));

  const browseMissing = await api('POST', `/api/clusters/default/topics//browse`, { start: 'earliest' });
  check('browse unknown topic -> 404', browseMissing.status === 404);

  // ---------- 5. Dynamic consumers ----------
  console.log('[5] Dynamic consumers (subscribe/receive/unsubscribe)');
  const sub = await api('POST', '/api/clusters/default/consumers', { groupId: 'e2e-group', topic: TOPIC });
  check('subscribe started', sub.json?.status === 'started' || sub.json?.status === 'already-running');
  await sleep(4000);
  const received = await api('GET', '/api/clusters/default/consumers/e2e-group/messages');
  check('consumer received the produced records', (received.json?.length ?? 0) >= 11, `got ${received.json?.length}`);
  check('received records carry headers', received.json[0]?.headers !== undefined);
  const unsub = await api('DELETE', '/api/clusters/default/consumers/e2e-group/' + TOPIC);
  check('unsubscribe stopped', unsub.json?.status === 'stopped');

  // ---------- 6. Consumer groups ----------
  console.log('[6] Consumer groups, lag, offset reset');
  const groups = await api('GET', '/api/clusters/default/groups');
  check('groups list includes e2e-group', groups.json?.some(g => g.groupId === 'e2e-group'));
  check('groups list includes billing-service with lag', groups.json?.some(g => g.groupId === 'billing-service' && g.totalLag >= 1));

  let groupDetail = await api('GET', '/api/clusters/default/groups/e2e-group');
  check('group detail has state + coordinator', typeof groupDetail.json?.state === 'string' && groupDetail.json.coordinatorId !== undefined);
  check('group detail has committed partitions with lag', groupDetail.json.committedPartitions >= 1 && groupDetail.json.partitionsByTopic && Object.keys(groupDetail.json.partitionsByTopic).length >= 1);

  const reset = await api('POST', `/api/clusters/default/groups/e2e-group/offsets/reset`, { topic: TOPIC, mode: 'LATEST' });
  check('offset reset to latest works', reset.json?.status === 'reset', JSON.stringify(reset.json));
  groupDetail = await api('GET', '/api/clusters/default/groups/e2e-group');
  check('lag is zero after reset-to-latest', groupDetail.json.totalLag === 0, `lag=${groupDetail.json.totalLag}`);

  const resetActive = await api('POST', `/api/clusters/default/groups/billing-service/offsets/reset`, { topic: 'orders', mode: 'EARLIEST' });
  check('reset of active/absent state handled', resetActive.status === 200 && (resetActive.json.status === 'reset-failed' || resetActive.json.status === 'reset'));

  const delGroup = await api('DELETE', '/api/clusters/default/groups/e2e-group');
  check('delete group -> deleted', delGroup.json?.status === 'deleted');
  const delGroupAgain = await api('DELETE', '/api/clusters/default/groups/e2e-group');
  check('delete unknown group -> 404', delGroupAgain.status === 404);

  // ---------- 7. Connections ----------
  console.log('[7] Connection profiles (PLAINTEXT profile against local broker)');
  const listBefore = await api('GET', '/api/clusters');
  check('connections list has built-in default', listBefore.json?.some(c => c.id === 'default' && c.builtIn === true));
  check('built-in profile has no raw secrets', listBefore.json.every(c => !JSON.stringify(c).includes('saslPassword": "')),

  );
  const createConn = await api('POST', '/api/clusters', {
    name: 'E2E local', bootstrapServers: ['localhost:9092'],
    security: { protocol: 'PLAINTEXT' },
  });
  check('create connection returns id', createConn.status === 201 && !!createConn.json?.id);
  const connId = createConn.json.id;

  const testConn = await api('POST', '/api/clusters/test', {
    name: 'E2E local', bootstrapServers: ['localhost:9092'], security: { protocol: 'PLAINTEXT' },
  });
  check('test connection ok with nodes', testConn.json?.ok === true && testConn.json.nodes.length >= 1);

  const testBad = await api('POST', '/api/clusters/test', {
    name: 'bad', bootstrapServers: ['localhost:65111'], security: { protocol: 'PLAINTEXT' },
  });
  check('test bad endpoint -> ok:false with error', testBad.json?.ok === false && !!testBad.json.error);

  const scoped = await api('GET', `/api/clusters/${connId}/topics`);
  check('new connection is usable (scoped topics 200)', scoped.status === 200 && scoped.json.length >= 1);

  const upd = await api('PUT', `/api/clusters/${connId}`, {
    name: 'E2E local renamed', bootstrapServers: ['localhost:9092'], security: { protocol: 'PLAINTEXT' },
  });
  check('update connection keeps id', upd.json?.id === connId && upd.json?.name === 'E2E local renamed');

  const updUnknown = await api('PUT', '/api/clusters/does-not-exist', { name: 'x', bootstrapServers: ['localhost:9092'] });
  check('update unknown connection -> 404', updUnknown.status === 404);

  const delConn = await api('DELETE', `/api/clusters/${connId}`);
  check('delete connection -> deleted', delConn.json?.status === 'deleted');

  const unknownCluster = await api('GET', '/api/clusters/nope/topics');
  check('unknown cluster -> 404', unknownCluster.status === 404);

  // ---------- 8. Legacy API ----------
  console.log('[8] Legacy API compatibility');
  const legacyTopics = await api('GET', '/api/topics');
  check('GET /api/topics lists names incl. e2e-topic', Array.isArray(legacyTopics.json) && legacyTopics.json.includes(TOPIC));
  const legacyDetail = await api('GET', `/api/topics/${TOPIC}`);
  check('GET /api/topics/{name} returns detail', legacyDetail.json?.name === TOPIC);
  const legacyProduce = await api('POST', '/api/messages', { topic: TOPIC, key: 'legacy', payload: '{"legacy": true}' });
  check('POST /api/messages produces', legacyProduce.status === 201 && Number.isInteger(legacyProduce.json.offset));
  const legacyConsumers = await api('GET', '/api/consumers');
  check('GET /api/consumers responds', legacyConsumers.status === 200);
  const legacyCreate = await api('POST', '/api/topics', { name: LEGACY_TOPIC, partitions: 1, replicationFactor: 1 });
  check('POST /api/topics creates', legacyCreate.json?.status === 'created' || legacyCreate.json?.status === 'already-exists');
  // (deletion of legacy-topic happens in cleanup — deleting right after writing can
  //  crash a Windows dev broker, so it is tested last with broker recovery)

  // ---------- 9. Cleanup ----------
  console.log('[9] Cleanup');
  const delTopic = await api('DELETE', `/api/clusters/default/topics/${TOPIC}`);
  check('delete topic -> deleted', delTopic.json?.status === 'deleted');
  const delLegacy = await api('DELETE', `/api/topics/${LEGACY_TOPIC}`);
  check('delete legacy topic -> deleted/gone', ['deleted', 'delete-failed'].includes(delLegacy.json?.status));
  await recoverBrokerIfNeeded();
  const delCheck = await api('GET', `/api/clusters/default/topics/${TOPIC}`);
  check('deleted topic is gone -> 404', delCheck.status === 404);
  const legacyDelCheck = await api('GET', `/api/topics/${LEGACY_TOPIC}`);
  check('deleted legacy topic is gone -> 404', legacyDelCheck.status === 404);

  // ---------- Summary ----------
  console.log(`\n=== RESULT: ${passed} passed, ${failed} failed ===`);
  if (failed > 0) {
    console.log('Failures:');
    failures.forEach(f => console.log('  ✗ ' + f));
    process.exit(1);
  }
}

main().catch(e => { console.error('E2E crashed:', e); process.exit(1); });
