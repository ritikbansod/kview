#!/usr/bin/env node
/**
 * kview CLI — thin client for a running Kview server (default http://localhost:8090).
 * Dependency-free; needs Node 18+. Works against local or remote Kview instances:
 *   node cli/kview.mjs <command> [args] [--server URL] [--cluster ID] [--json]
 */
const VERSION = '1.0.0';

const argv = process.argv.slice(2);

// ---- global options ---------------------------------------------------------
let SERVER = process.env.KVIEW_URL || 'http://localhost:8090';
let CLUSTER = process.env.KVIEW_CLUSTER || 'default';
let JSON_OUT = false;
const flags = {};
const positional = [];

for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (a === '--server' || a === '-s') SERVER = argv[++i];
  else if (a === '--cluster' || a === '-c') CLUSTER = argv[++i];
  else if (a === '--json' || a === '-j') JSON_OUT = true;
  else if (a === '--help' || a === '-h') { positional.push('help'); }
  else if (a === '--version') { console.log(`kview ${VERSION}`); process.exit(0); }
  else if (a.startsWith('-') && a !== '-') { flags[a.replace(/^-+/, '')] = argv[++i] ?? true; }
  else positional.push(a);
}

// repeated flags come through space-separated; normalize arrays where needed
const list = (name) => flags[name] === undefined ? [] : String(flags[name]).split(/\s+/);
const num = (name) => flags[name] === undefined ? undefined : Number(flags[name]);

const clusterPath = (suffix = '') => `/api/clusters/${encodeURIComponent(CLUSTER)}${suffix}`;

// API bearer token: --token flag or KVIEW_TOKEN (needed when the server runs with kview.auth.mode != none)
const TOKEN = typeof flags.token === 'string' && flags.token ? flags.token : (process.env.KVIEW_TOKEN || '');
const authHeaders = () => ({
  'X-Kview-Client': `kview-cli/${VERSION}`,
  ...(TOKEN ? { Authorization: `Bearer ${TOKEN}` } : {}),
});

async function api(method, path, body) {
  let res;
  try {
    res = await fetch(SERVER + path, {
      method,
      headers: { ...authHeaders(), ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}) },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
  } catch (e) {
    fail(`cannot reach Kview at ${SERVER} (${e.message})`);
  }
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { /* non-JSON error page */ }
  if (!res.ok) {
    fail(`HTTP ${res.status} ${json?.error || res.statusText}${json?.detail ? ' — ' + json.detail : ''}`);
  }
  return json;
}

function fail(msg) {
  console.error(`error: ${msg}`);
  process.exit(1);
}

/** Starts are async: poll the search until done or the wait budget runs out. */
async function pollSearch(searchId, waitSec) {
  const deadline = Date.now() + waitSec * 1000;
  let status;
  do {
    status = await api('GET', clusterPath(`/searches/${encodeURIComponent(searchId)}`));
    if (status.state === 'RUNNING') await new Promise((r) => setTimeout(r, 500));
  } while (status.state === 'RUNNING' && Date.now() < deadline);
  return status;
}

// ---- output helpers ----------------------------------------------------------
const oneLine = (s, max = 70) =>
  s === null || s === undefined ? '' :
  String(s).replace(/\s+/g, ' ').slice(0, max);

function table(rows, cols) {
  if (rows.length === 0) { console.log('(none)'); return; }
  const width = {};
  for (const c of cols) width[c.title] = Math.max(c.title.length,
    ...rows.map(r => String(r[c.key] ?? '').length));
  console.log(cols.map(c => c.title.padEnd(width[c.title])).join('  '));
  for (const r of rows) {
    console.log(cols.map(c => String(r[c.key] ?? '').padEnd(width[c.title])).join('  '));
  }
}

const fmtTs = (t) => t === undefined || t === null ? '' :
  new Date(Number(t)).toISOString().replace('T', ' ').slice(0, 23);

function msgLine(m) {
  const hasDecoded = m.schema && m.schema.decoded !== undefined && m.schema.decoded !== null;
  const val = hasDecoded ? oneLine(JSON.stringify(m.schema.decoded))
    : m.value ? oneLine(m.value)
    : m.valueBase64 ? `<binary ${m.valueBase64.length} b64 chars>` : '(empty)';
  const key = m.key === null ? '(null)' : oneLine(m.key, 24);
  const hdr = m.headers && Object.keys(m.headers).length ? ` {${Object.keys(m.headers).length} headers}` : '';
  return `[p${m.partition}@${m.offset}] ${fmtTs(m.timestamp)}  ${key}  ${val}${hdr}`;
}

const decodedSuffix = (m) =>
  m.schema ? `  (${[m.schema.schemaType || m.schema.wireFormat, m.schema.subject,
    m.schema.version ? 'v' + m.schema.version : ''].filter(Boolean).join(' ')})` : '';

// ---- commands -----------------------------------------------------------------
const commands = {

  help() {
    console.log(`kview ${VERSION} — CLI for a Kview server (REST API for Apache Kafka)

Usage: kview <command> [args] [options]

Cluster:   overview | clusters | health
Topics:    topics | topic <name> | topic-create <name> [--partitions N] [--rf N] [--config k=v...]
           topic-delete <name> | partitions <topic> <total>
Messages:  produce <topic> [-k key] [-v payload] [--file path] [--partition N] [--header k=v...]
           browse <topic> [--start latest|earliest|timestamp] [--limit N] [--partition N]
                  [--key-contains S] [--value-contains S] [--ts 2026-01-01T10:00:00Z]
           search <topic> [--value-regex RE] [--key-contains S] [--value-contains S]
                  [--from-ts MS|ISO] [--to-ts MS|ISO] [--limit N] [--partition N] [--wait SEC]
           search-status <searchId>
           replay <topic> --to-topic T [--to-cluster ID] [--limit N] [--key-contains S]
                  [--value-contains S] [--dry-run] [--no-headers]
           tail <topic> [--from latest|earliest] [--partition N]     (Ctrl+C stops)
Groups:    groups | lag <group> | reset-offsets <group> --topic T --mode earliest|latest|offset|timestamp
           [--value V] | group-delete <group>

Options:   --server URL (-s)   Kview server   [KVIEW_URL, default http://localhost:8090]
           --cluster ID (-c)   cluster id     [KVIEW_CLUSTER, default "default"]
           --token T           API bearer token (auth-enabled servers) [KVIEW_TOKEN]
           --json (-j)         raw JSON output

Examples:  kview overview
           kview browse shipments --start latest --limit 10 --value-contains orderId
           kview produce shipments -k k1 -v '{"orderId":42}' -H source=ci
           kview tail shipments
           kview lag orders-worker`);
  },

  async health() {
    const out = await api('GET', '/actuator/health');
    console.log(JSON_OUT ? JSON.stringify(out) : `${SERVER} → ${out.status ?? 'UP'}`);
  },

  async clusters() {
    const out = await api('GET', '/api/clusters');
    if (JSON_OUT) { console.log(JSON.stringify(out, null, 2)); return; }
    const rows = out.map((c) => ({ ...c, securityProtocol: c.security?.protocol || 'PLAINTEXT' }));
    table(rows, [
      { title: 'ID', key: 'id' }, { title: 'NAME', key: 'name' },
      { title: 'BOOTSTRAP', key: 'bootstrapServers' },
      { title: 'SECURITY', key: 'securityProtocol' },
    ]);
  },

  async overview() {
    const out = await api('GET', clusterPath('/overview'));
    if (JSON_OUT) { console.log(JSON.stringify(out, null, 2)); return; }
    const b = out.brokers?.length ?? 0;
    console.log(`cluster      ${out.clusterId ?? CLUSTER}`);
    console.log(`brokers      ${b} (controller: ${out.controller ?? '?'})`);
    console.log(`topics       ${out.topicCount ?? '?'} in ${out.partitionCount ?? '?'} partitions`);
    console.log(`messages     ~${out.estimatedMessages ?? '?'}`);
    console.log(`groups       ${out.consumerGroupCount ?? '?'}`);
    console.log(`health       under-replicated: ${out.underReplicatedPartitions ?? '?'}, offline: ${out.offlinePartitions ?? '?'}`);
  },

  async topics() {
    const out = await api('GET', clusterPath('/topics?includeCounts=true'));
    if (JSON_OUT) { console.log(JSON.stringify(out, null, 2)); return; }
    table(out, [
      { title: 'TOPIC', key: 'name' }, { title: 'PARTITIONS', key: 'partitions' },
      { title: 'RF', key: 'replicationFactor' }, { title: 'MESSAGES', key: 'messageCount' },
    ]);
  },

  async topic() {
    const name = positional[1] ?? fail('usage: kview topic <name>');
    const out = await api('GET', clusterPath(`/topics/${encodeURIComponent(name)}`));
    if (JSON_OUT) { console.log(JSON.stringify(out, null, 2)); return; }
    for (const p of out.partitions ?? []) {
      console.log(`p${p.partition}  leader=${p.leader}  replicas=${(p.replicas ?? []).join(',')}  ` +
        `isr=${(p.isr ?? []).join(',')}  offsets=${p.beginningOffset}..${p.endOffset}`);
    }
    for (const [k, v] of Object.entries(out.configs ?? {})) console.log(`config ${k}=${oneLine(v)}`);
  },

  async 'topic-create'() {
    const name = positional[1] ?? fail('usage: kview topic-create <name> [--partitions N] [--rf N] [--config k=v...]');
    const configs = {};
    for (const kv of list('config')) { const [k, ...v] = kv.split('='); configs[k] = v.join('='); }
    const body = { name, partitions: num('partitions') ?? 1, replicationFactor: num('rf') ?? 1, configs };
    const out = await api('POST', clusterPath('/topics'), body);
    console.log(JSON_OUT ? JSON.stringify(out, null, 2) : `created ${name} (${body.partitions} partition(s), rf=${body.replicationFactor})`);
  },

  async 'topic-delete'() {
    const name = positional[1] ?? fail('usage: kview topic-delete <name>');
    await api('DELETE', clusterPath(`/topics/${encodeURIComponent(name)}`));
    console.log(`deleted ${name}`);
  },

  async partitions() {
    const topic = positional[1] ?? fail('usage: kview partitions <topic> <totalPartitions>');
    const totalPartitions = Number(positional[2]) || fail('usage: kview partitions <topic> <totalPartitions>');
    await api('POST', clusterPath(`/topics/${encodeURIComponent(topic)}/partitions`), { totalPartitions });
    console.log(`${topic} now has ${totalPartitions} partitions`);
  },

  async produce() {
    const topic = positional[1] ?? fail('usage: kview produce <topic> [-k key] [-v payload] [--file path] [--header k=v...]');
    let payload = flags.v !== undefined ? String(flags.v) : undefined;
    if (flags.file) payload = (await import('node:fs')).readFileSync(flags.file, 'utf8');
    if (payload === undefined) fail('provide the payload with -v or --file');
    const headers = {};
    for (const kv of [...list('header'), ...list('H')]) { const [k, ...v] = kv.split('='); headers[k] = v.join('='); }
    const body = { key: flags.k ?? null, payload, partition: num('partition'), headers };
    const out = await api('POST', clusterPath(`/topics/${encodeURIComponent(topic)}/messages`), body);
    if (JSON_OUT) { console.log(JSON.stringify(out, null, 2)); return; }
    console.log(`produced → ${out.topic ?? topic} p${out.partition}@${out.offset}`);
  },

  async browse() {
    const topic = positional[1] ?? fail('usage: kview browse <topic> [--start latest|earliest|timestamp] [--limit N] [--key-contains S] [--value-contains S]');
    const body = {
      start: flags.start ?? 'latest',
      limit: num('limit') ?? 20,
      partition: num('partition'),
      keyContains: flags['key-contains'],
      valueContains: flags['value-contains'],
    };
    if (flags.ts) body.timestamp = /\d+$/.test(flags.ts) && !flags.ts.includes('-') ? Number(flags.ts) : Date.parse(flags.ts);
    const out = await api('POST', clusterPath(`/topics/${encodeURIComponent(topic)}/browse`), body);
    if (JSON_OUT) { console.log(JSON.stringify(out, null, 2)); return; }
    for (const m of out.messages ?? []) console.log(msgLine(m) + decodedSuffix(m));
    console.error(`-- ${out.messages?.length ?? 0} message(s) from ${topic}` +
      (out.reachedEnd ? ' (end of partition reached)' : ' (older messages may exist — raise --limit)'));
  },

  async search() {
    const topic = positional[1] ?? fail('usage: kview search <topic> [--value-regex RE] [--key-contains S] [--value-contains S] [--from-ts MS|ISO] [--to-ts MS|ISO] [--limit N] [--partition N] [--wait SEC]');
    const toMs = (v) => (v === undefined ? undefined : (/^-?\d{13,}$/.test(v) ? Number(v) : Date.parse(v)));
    const body = {
      keyContains: flags['key-contains'],
      valueContains: flags['value-contains'],
      valueRegex: flags['value-regex'],
      fromTs: toMs(flags['from-ts']),
      toTs: toMs(flags['to-ts']),
      limit: num('limit'),
      partition: num('partition'),
    };
    const { searchId } = await api('POST', clusterPath(`/topics/${encodeURIComponent(topic)}/search`), body);
    const status = await pollSearch(searchId, num('wait') ?? 60);
    if (JSON_OUT) { console.log(JSON.stringify(status, null, 2)); return; }
    for (const m of status.results ?? []) console.log(msgLine(m) + decodedSuffix(m));
    console.error(`-- ${status.matched} matched · scanned ${status.scanned} · state ${status.state}`
      + (status.state === 'RUNNING' ? ` — still running, poll with: kview search-status ${searchId}` : '')
      + (status.limitReached ? ' · limit reached' : ''));
  },

  async 'search-status'() {
    const searchId = positional[1] ?? fail('usage: kview search-status <searchId>');
    const status = await api('GET', clusterPath(`/searches/${encodeURIComponent(searchId)}`));
    console.log(JSON_OUT ? JSON.stringify(status, null, 2)
      : `search ${status.searchId}  state=${status.state}  matched=${status.matched}  scanned=${status.scanned}  partitions=${status.partitionsDone}/${status.partitionsTotal}`);
  },

  async replay() {
    const topic = positional[1] ?? fail('usage: kview replay <sourceTopic> --to-topic T [--to-cluster ID] [--limit N] [--key-contains S] [--value-contains S] [--dry-run] [--no-headers]');
    if (!flags['to-topic']) fail('--to-topic is required');
    const body = {
      targetTopic: flags['to-topic'],
      targetClusterId: flags['to-cluster'],
      keyContains: flags['key-contains'],
      valueContains: flags['value-contains'],
      limit: num('limit'),
      copyHeaders: !flags['no-headers'],
      dryRun: Boolean(flags['dry-run']),
    };
    const out = await api('POST', clusterPath(`/topics/${encodeURIComponent(topic)}/replay`), body);
    if (JSON_OUT) { console.log(JSON.stringify(out, null, 2)); return; }
    console.log(out.dryRun
      ? `dry run: would copy ${out.selected} message(s) from ${topic} to ${out.targetClusterId}/${out.targetTopic}`
      : `replayed ${out.copied}/${out.selected} message(s) from ${topic} → ${out.targetClusterId}/${out.targetTopic}`
        + (out.reachedEnd ? ' (end reached)' : ' (more may remain — raise --limit)'));
    for (const e of out.errors ?? []) console.error(`error: ${e}`);
  },

  async tail() {
    const topic = positional[1] ?? fail('usage: kview tail <topic> [--from latest|earliest] [--partition N]');
    const qs = new URLSearchParams({ from: flags.from ?? 'latest' });
    if (flags.partition !== undefined) qs.set('partition', String(flags.partition));
    const res = await fetch(`${SERVER}${clusterPath(`/topics/${encodeURIComponent(topic)}/tail`)}?${qs}`, {
      headers: authHeaders(),
    });
    if (!res.ok || !res.body) fail(`HTTP ${res.status} while opening tail`);
    console.error(`tailing ${topic} (${flags.from ?? 'latest'}) — Ctrl+C to stop`);
    const dec = new TextDecoder();
    let buf = '';
    for (let reader = res.body.getReader();;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let i;
      while ((i = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, i).trim(); buf = buf.slice(i + 1);
        if (!line.startsWith('data:')) continue;
        let ev; try { ev = JSON.parse(line.slice(5).trim()); } catch { continue; }
        if (ev.tailId) console.error(`connected (${ev.tailId})`);
        else if (ev.message) console.error(`error: ${ev.message}`);
        else console.log(msgLine(ev) + decodedSuffix(ev));
      }
    }
  },

  async groups() {
    const out = await api('GET', clusterPath('/groups'));
    if (JSON_OUT) { console.log(JSON.stringify(out, null, 2)); return; }
    table(out, [
      { title: 'GROUP', key: 'groupId' }, { title: 'STATE', key: 'state' },
      { title: 'TOPICS', key: 'committedPartitions' }, { title: 'TOTAL LAG', key: 'totalLag' },
    ]);
  },

  async lag() {
    const group = positional[1] ?? fail('usage: kview lag <group>');
    const out = await api('GET', clusterPath(`/groups/${encodeURIComponent(group)}`));
    if (JSON_OUT) { console.log(JSON.stringify(out, null, 2)); return; }
    console.log(`group ${out.groupId}  state=${out.state}  members=${(out.members ?? []).length}`);
    for (const [topic, parts] of Object.entries(out.partitionsByTopic ?? {})) {
      for (const p of parts) {
        console.log(`  ${topic} p${p.partition}  committed=${p.committedOffset}  end=${p.endOffset ?? '?'}  lag=${p.lag}`);
      }
    }
  },

  async 'reset-offsets'() {
    const group = positional[1] ?? fail('usage: kview reset-offsets <group> --topic T --mode earliest|latest|offset|timestamp [--value V]');
    const topic = flags.topic ?? fail('--topic is required');
    const mode = (flags.mode ?? fail('--mode is required')).toUpperCase();
    if (!['EARLIEST', 'LATEST', 'OFFSET', 'TIMESTAMP'].includes(mode)) fail(`bad mode: ${mode}`);
    let value = flags.value;
    if (mode === 'TIMESTAMP' && value && /-/.test(value)) value = Date.parse(value);
    if (mode === 'OFFSET') value = Number(value ?? fail('--value <offset> is required for OFFSET mode'));
    const out = await api('POST', clusterPath(`/groups/${encodeURIComponent(group)}/offsets/reset`), { topic, mode, value });
    console.log(JSON_OUT ? JSON.stringify(out, null, 2) : `reset ${group} on ${topic} → ${mode}`);
  },

  async 'group-delete'() {
    const group = positional[1] ?? fail('usage: kview group-delete <group>');
    await api('DELETE', clusterPath(`/groups/${encodeURIComponent(group)}`));
    console.log(`deleted group ${group}`);
  },
};

// ---- dispatch ------------------------------------------------------------------
const cmd = positional[0] ?? 'help';
const fn = commands[cmd];
if (!fn) fail(`unknown command: ${cmd} (try: kview help)`);
await fn();
