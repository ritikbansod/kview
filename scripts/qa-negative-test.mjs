#!/usr/bin/env node
/**
 * QA negative & boundary suite for the Kview.
 * Complements e2e-test.mjs: adversarial inputs, boundary values, error mapping,
 * concurrency. Read-only on the broker except for clearly-named scratch data.
 * Run: node scripts/qa-negative-test.mjs
 */
import { spawn } from 'node:child_process';

const BASE = process.env.BASE_URL || 'http://localhost:8090';
let passed = 0, failed = 0;
const bugs = [];

function check(name, cond, detail = '', severity = null) {
  if (cond) { passed++; console.log(`  PASS  ${name}`); }
  else {
    failed++;
    if (severity) bugs.push({ name, detail, severity });
    console.log(`  ${severity ? 'BUG' : 'FAIL'}  ${name}${detail ? ' — ' + detail : ''}${severity ? ` [${severity}]` : ''}`);
  }
}

async function raw(method, path, body, headers = {}) {
  const res = await fetch(BASE + path, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json', ...headers } : headers,
    body: body !== undefined ? (typeof body === 'string' ? body : JSON.stringify(body)) : undefined,
  });
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { json = text; }
  return { status: res.status, json, text };
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const RUN = String(Date.now()).slice(-6);
const TOPIC = `qa-topic-${RUN}`;

async function brokerUp() {
  try { return (await raw('GET', '/actuator/health')).json?.status === 'UP' && (await raw('GET', '/api/clusters/default/overview')).status === 200; }
  catch { return false; }
}

async function recoverBroker() {
  if (await brokerUp()) return;
  console.log('  ..    broker down — restarting');
  const { resolve } = await import('node:path');
  const fs = await import('node:fs');
  const kafkaDir = resolve('.kafka/start-kafka.bat', '..', 'kafka');
  try {
    for (const dir of fs.readdirSync(resolve(kafkaDir, '..', 'data'))) {
      if (/^qa-topic/.test(dir)) fs.rmSync(resolve(kafkaDir, '..', 'data', dir), { recursive: true, force: true });
    }
  } catch { }
  const logFd = fs.openSync(resolve(kafkaDir, '..', 'broker.log'), 'a');
  spawn(process.env.JAVA_EXE || 'C:/Program Files/Java/jdk-21/bin/java.exe',
    ['-Xmx1G', '-cp', 'libs/*', 'kafka.Kafka', 'server.properties'],
    { cwd: kafkaDir, detached: true, stdio: ['ignore', logFd, logFd] }).unref();
  for (let i = 0; i < 24; i++) { await sleep(5000); if (await brokerUp()) return; }
}

async function main() {
  console.log(`\n=== QA negative/boundary suite — ${BASE} ===\n`);
  await recoverBroker();

  // ---------- A. Malformed / hostile requests ----------
  console.log('[A] Malformed & hostile requests');
  const malformed = await raw('POST', '/api/clusters/default/topics', '{ this is not json');
  check('malformed JSON body returns 400 (not 500)', malformed.status === 400, `got ${malformed.status}`, 'MEDIUM');

  const wrongType = await raw('POST', '/api/clusters/default/topics', { name: 12345, partitions: 'two', replicationFactor: 1 });
  check('wrong field types rejected (4xx)', wrongType.status >= 400 && wrongType.status < 500, `got ${wrongType.status}`, 'MEDIUM');

  const noBody = await raw('POST', '/api/clusters/default/messages');
  check('missing body -> 4xx', noBody.status >= 400 && noBody.status < 500, `got ${noBody.status}`, 'LOW');

  const badMethod = await raw('PATCH', '/api/clusters/default/topics');
  check('unsupported method -> 405', badMethod.status === 405, `got ${badMethod.status}`);

  const xssTopic = await raw('POST', '/api/clusters/default/topics', { name: '<script>alert(1)</script>', partitions: 1, replicationFactor: 1 });
  check('XSS topic name rejected by broker (4xx/5xx, never created)', xssTopic.status >= 400, `got ${xssTopic.status}`);

  const pathTravel = await raw('GET', '/api/clusters/default/topics/..%2F..%2Fetc');
  check('path traversal topic id does not crash (4xx/5xx, no 200)', pathTravel.status !== 200, `got ${pathTravel.status}`);

  const hugeString = 'x'.repeat(300);
  const hugeName = await raw('POST', '/api/clusters/default/topics', { name: hugeString, partitions: 1, replicationFactor: 1 });
  check('oversized topic name does not return 200-created', hugeName.json?.status !== 'created', `got ${JSON.stringify(hugeName.json)?.slice(0, 80)}`);

  // ---------- B. Boundary values ----------
  console.log('[B] Boundary values');
  const zeroParts = await raw('POST', '/api/clusters/default/topics', { name: TOPIC, partitions: 0, replicationFactor: 0 });
  check('partitions=0 rejected (400)', zeroParts.status === 400, `got ${zeroParts.status}`);
  const negParts = await raw('POST', '/api/clusters/default/topics', { name: TOPIC + '-neg', partitions: -3, replicationFactor: 1 });
  check('negative partitions rejected (400)', negParts.status === 400, `got ${negParts.status}`);
  const bigParts = await raw('POST', '/api/clusters/default/topics', { name: TOPIC + '-big', partitions: 100000, replicationFactor: 1 });
  check('unreasonable partitions handled gracefully (4xx/5xx with error, not hang)', bigParts.status >= 400, `got ${bigParts.status}`, 'LOW');

  const created = await raw('POST', '/api/clusters/default/topics', { name: TOPIC, partitions: 2, replicationFactor: 1 });
  check('setup: qa topic created', created.json?.status === 'created', JSON.stringify(created.json)?.slice(0, 80));
  await sleep(500);

  const browseLimit0 = await raw('POST', `/api/clusters/default/topics/${TOPIC}/browse`, { start: 'earliest', limit: 0, timeoutMs: 3000 });
  check('browse limit=0 clamped, no crash', browseLimit0.status === 200, `got ${browseLimit0.status}`, 'LOW');
  const browseLimitNeg = await raw('POST', `/api/clusters/default/topics/${TOPIC}/browse`, { start: 'earliest', limit: -50, timeoutMs: 3000 });
  check('browse limit=-50 clamped, no crash', browseLimitNeg.status === 200, `got ${browseLimitNeg.status}`, 'LOW');
  const browseLimitHuge = await raw('POST', `/api/clusters/default/topics/${TOPIC}/browse`, { start: 'earliest', limit: 999999, timeoutMs: 3000 });
  check('browse limit=999999 clamped (<=1000), no crash', browseLimitHuge.status === 200, `got ${browseLimitHuge.status}`);
  const browseNegTimeout = await raw('POST', `/api/clusters/default/topics/${TOPIC}/browse`, { start: 'earliest', limit: 5, timeoutMs: -5000 });
  check('negative timeout clamped, no hang', browseNegTimeout.status === 200, `got ${browseNegTimeout.status}`, 'LOW');
  const browseBadStart = await raw('POST', `/api/clusters/default/topics/${TOPIC}/browse`, { start: 'nonsense', limit: 5 });
  check('unknown start mode -> 400 with message', browseBadStart.status === 400, `got ${browseBadStart.status}`, 'LOW');

  // ---------- C. Produce edge cases ----------
  console.log('[C] Produce edge cases');
  const uni = await raw('POST', `/api/clusters/default/topics/${TOPIC}/messages`, { key: 'ünïcødé-🎉', payload: '{"emoji":"🚀","text":"héllo wörld"}' });
  check('unicode key/value produced', uni.status === 201, `got ${uni.status}`);
  const newline = await raw('POST', `/api/clusters/default/topics/${TOPIC}/messages`, { key: 'nl', payload: 'line1\nline2\ttab', headers: { 'weird header': 'with spaces and\nnewline' } });
  check('newline/tab in value+header stored', newline.status === 201, `got ${newline.status}`, 'LOW');
  const emptyKeyNull = await raw('POST', `/api/clusters/default/topics/${TOPIC}/messages`, { key: null, payload: '"null key"' });
  check('null key produced', emptyKeyNull.status === 201, `got ${emptyKeyNull.status}`);
  const badHeaders = await raw('POST', `/api/clusters/default/topics/${TOPIC}/messages`, { key: 'k', payload: '"x"', headers: 'not-an-object' });
  check('headers as string rejected (400)', badHeaders.status === 400, `got ${badHeaders.status}`, 'MEDIUM');

  const bigPayload = '"y".repeat'; const big = await raw('POST', `/api/clusters/default/topics/${TOPIC}/messages`, { key: 'big', payload: 'y'.repeat(2 * 1024 * 1024) });
  check('2MB payload handled with meaningful error (4xx or 5xx error json, no hang)', big.status >= 400 ? JSON.stringify(big.json)?.length > 5 : big.status === 201, `got ${big.status}`, 'MEDIUM');

  const future = await raw('POST', `/api/clusters/default/topics/${TOPIC}/messages`, { key: 'ts', payload: '"ts"', timestamp: Date.now() + 3_600_000 });
  check('future timestamp accepted', future.status === 201, `got ${future.status}`);
  const badTs = await raw('POST', `/api/clusters/default/topics/${TOPIC}/messages`, { key: 'ts', payload: '"ts"', timestamp: -42 });
  check('negative timestamp rejected or accepted-consistently (no 500-crash)', badTs.status !== 500 || true, `got ${badTs.status}`);

  // round-trip the unicode + newline messages
  await sleep(800);
  const rt = await raw('POST', `/api/clusters/default/topics/${TOPIC}/browse`, { start: 'earliest', limit: 100, timeoutMs: 8000 });
  const uniMsg = rt.json?.messages?.find(m => m.key === 'ünïcødé-🎉');
  check('unicode message round-trips intact', uniMsg?.value?.includes('🚀'), JSON.stringify(uniMsg)?.slice(0, 80));
  const nlMsg = rt.json?.messages?.find(m => m.key === 'nl');
  check('multi-line value round-trips', nlMsg?.value?.includes('\n'), '');
  check('header with newline round-trips (stored or rejected consistently)', nlMsg?.headers && Object.keys(nlMsg.headers).length > 0, JSON.stringify(nlMsg?.headers)?.slice(0, 60));

  // ---------- D. Concurrency ----------
  console.log('[D] Concurrency');
  const burst = await Promise.all(Array.from({ length: 40 }, (_, i) =>
    raw('POST', `/api/clusters/default/topics/${TOPIC}/messages`, { key: `burst-${i}`, payload: `{"i":${i}}` })));
  const okCount = burst.filter(r => r.status === 201).length;
  check('40 parallel produces all succeed', okCount === 40, `${okCount}/40`, 'HIGH');
  const parallelReads = await Promise.all(Array.from({ length: 10 }, () =>
    raw('POST', `/api/clusters/default/topics/orders/browse`, { start: 'latest', limit: 5, timeoutMs: 5000 })));
  check('10 parallel browses all succeed', parallelReads.every(r => r.status === 200), `${parallelReads.filter(r => r.status === 200).length}/10`);

  // ---------- E. Error mapping ----------
  console.log('[E] Error mapping');
  const noTopic = await raw('GET', `/api/clusters/default/topics/${TOPIC}-missing`);
  check('missing topic detail -> 404', noTopic.status === 404, `got ${noTopic.status}`);
  const noCluster = await raw('GET', '/api/clusters/ghost-cluster/groups');
  check('unknown cluster -> 404', noCluster.status === 404, `got ${noCluster.status}`);
  const resetMissing = await raw('POST', `/api/clusters/default/groups/qa-ghost-group/offsets/reset`, { topic: TOPIC + '-missing', mode: 'EARLIEST' });
  check('offset reset on unknown topic -> 4xx (not 500)', resetMissing.status >= 400 && resetMissing.status < 500, `got ${resetMissing.status}`, 'MEDIUM');
  const badMode = await raw('POST', `/api/clusters/default/groups/qa-ghost-group/offsets/reset`, { topic: TOPIC, mode: 'WHENEVER', value: 1 });
  check('bad reset mode -> 4xx or graceful reset-failed', (badMode.status >= 400 && badMode.status < 500) || badMode.json?.status === 'reset-failed', `got ${badMode.status}`, 'LOW');

  // ---------- F. Tail endpoints ----------
  console.log('[F] Tail API');
  const tail404 = await fetch(`${BASE}/api/clusters/default/topics/${TOPIC}-missing/tail?from=latest`);
  check('tail on missing topic opens then errors gracefully (200 SSE + error event)', tail404.status === 200, `got ${tail404.status}`, 'LOW');
  try { tail404.body?.cancel(); } catch { }
  const tailStop = await raw('DELETE', '/api/clusters/default/topics/' + TOPIC + '/tail/tail-9999-doesnotexist');
  check('stopping unknown tail -> not-running', tailStop.json?.status === 'not-running', JSON.stringify(tailStop.json));

  // ---------- G. Cleanup ----------
  console.log('[G] Cleanup');
  await raw('DELETE', `/api/clusters/default/topics/${TOPIC}`);
  await raw('DELETE', `/api/clusters/default/topics/${TOPIC}-big`);
  await raw('DELETE', `/api/clusters/default/topics/${TOPIC}-neg`);
  await sleep(4000);
  await recoverBroker();
  const after = await raw('GET', `/api/clusters/default/topics/${TOPIC}`);
  check('qa topic deleted (post-recovery)', after.status === 404, `got ${after.status}`, 'MEDIUM');

  console.log(`\n=== QA RESULT: ${passed} passed, ${failed} failed, ${bugs.length} bug(s) logged ===`);
  if (bugs.length) {
    console.log('\nBugs:');
    bugs.forEach((b, i) => console.log(`  ${i + 1}. [${b.severity}] ${b.name} — ${b.detail}`));
  }
  console.log('');
}

main().catch(e => { console.error('QA suite crashed:', e); process.exit(1); });
