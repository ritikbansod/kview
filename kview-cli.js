#!/usr/bin/env node
// ===== Kview CLI — terminal interface for the Kafka Wrapper REST API =====
// Usage: node kview-cli.js <command> [args]
// Requires: Kview app running on localhost:8090

const BASE = process.env.KVIEW_URL || 'http://localhost:8090';
const args = process.argv.slice(2);
const cmd = args[0];

// ─── helpers ────────────────────────────────────────────────────────────────

const C = {
  reset: '\x1b[0m', bold: '\x1b[1m', dim: '\x1b[2m',
  red: '\x1b[31m', green: '\x1b[32m', yellow: '\x1b[33m',
  blue: '\x1b[34m', cyan: '\x1b[36m', magenta: '\x1b[35m',
  orange: '\x1b[38;5;208m', gray: '\x1b[90m',
};

function ok(msg) { console.log(`${C.green}✓${C.reset} ${msg}`); }
function err(msg) { console.error(`${C.red}✗${C.reset} ${msg}`); }
function info(msg) { console.log(`${C.cyan}ℹ${C.reset} ${msg}`); }
function header(msg) { console.log(`\n${C.bold}${C.orange}${msg}${C.reset}\n`); }

async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(BASE + path, opts);
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = text; }
  if (!res.ok) {
    const detail = json?.detail || json?.error || text.slice(0, 200);
    throw new Error(`${res.status} ${detail}`);
  }
  return json;
}

// ─── commands ───────────────────────────────────────────────────────────────

const commands = {
  // ---- info ----
  async overview() {
    header('Cluster Overview');
    const r = await api('GET', '/api/clusters/default/overview');
    console.log(`  Brokers:       ${r.brokers?.length ?? '?'}`);
    console.log(`  Topics:        ${r.topicCount}`);
    console.log(`  Partitions:    ${r.partitionCount}`);
    console.log(`  Messages:      ~${r.estimatedMessages}`);
    console.log(`  Groups:        ${r.consumerGroupCount < 0 ? '?' : r.consumerGroupCount}`);
    console.log(`  URP:           ${r.underReplicatedPartitions}`);
    console.log(`  Offline:       ${r.offlinePartitions}`);
    console.log(`  Controller:    broker ${r.controller}`);
  },

  async brokers() {
    header('Brokers');
    const list = await api('GET', '/api/clusters/default/brokers');
    list.forEach(b => console.log(`  ${C.bold}${b.id}${C.reset}  ${b.host}:${b.port}${b.rack ? '  rack=' + b.rack : ''}`));
  },

  async topics() {
    header('Topics');
    const list = await api('GET', '/api/clusters/default/topics?includeCounts=true');
    if (list.length === 0) { info('No topics'); return; }
    const w = Math.max(...list.map(t => t.name.length));
    list.sort((a, b) => a.name.localeCompare(b.name)).forEach(t => {
      const health = t.underReplicatedPartitions > 0 ? `${C.red}${t.underReplicatedPartitions} URP${C.reset}` : `${C.green}ok${C.reset}`;
      console.log(`  ${t.name.padEnd(w)}  ${String(t.partitions).padStart(3)}p  RF${t.replicationFactor}  ${String(t.messageCount).padStart(8)} msgs  [${health}]${t.internal ? '  (internal)' : ''}`);
    });
    console.log(`\n  ${list.length} topic(s)`);
  },

  async topic(name) {
    if (!name) { err('usage: topic <name>'); return; }
    header(`Topic: ${name}`);
    const d = await api('GET', `/api/clusters/default/topics/${encodeURIComponent(name)}`);
    console.log(`  ID:            ${d.topicId}`);
    console.log(`  Partitions:    ${d.partitions.length}`);
    console.log(`  Replication:   RF${d.partitions[0]?.replicas.length ?? '?'}`);
    let total = 0;
    d.partitions.forEach(p => {
      total += p.messageCount;
      console.log(`    p${p.partition}: leader=b${p.leader}  replicas=[${p.replicas}]  isr=[${p.isr}]  ${p.messageCount} msgs  (${p.beginningOffset}→${p.endOffset})`);
    });
    console.log(`\n  Total: ${total} messages`);
  },

  async create([name, partitions, rf]) {
    if (!name) { err('usage: create <name> [partitions] [rf]'); return; }
    const p = parseInt(partitions || '3'), r = parseInt(rf || '1');
    const res = await api('POST', '/api/clusters/default/topics', { name, partitions: p, replicationFactor: r });
    ok(`Topic "${res.topic}" ${res.status}`);
  },

  async delete([name]) {
    if (!name) { err('usage: delete <name>'); return; }
    const res = await api('DELETE', `/api/clusters/default/topics/${encodeURIComponent(name)}`);
    ok(`Topic "${name}" ${res.status}`);
  },

  // ---- produce ----
  async produce([topic, key, ...rest]) {
    if (!topic) { err('usage: produce <topic> [key] [value]'); return; }
    const value = rest.join(' ') || JSON.stringify({ produced: new Date().toISOString(), via: 'kview-cli' });
    const r = await api('POST', `/api/clusters/default/topics/${encodeURIComponent(topic)}/messages`,
      { topic, key: key || null, payload: value });
    ok(`Produced to ${r.topic} p${r.partition} offset ${r.offset}`);
  },

  async produceN([topic, count]) {
    const n = parseInt(count || '10');
    header(`Producing ${n} messages to ${topic}`);
    for (let i = 0; i < n; i++) {
      const r = await api('POST', `/api/clusters/default/topics/${encodeURIComponent(topic)}/messages`,
        { topic, key: `msg-${i}`, payload: JSON.stringify({ seq: i, ts: Date.now(), via: 'kview-cli' }) });
      console.log(`  ${i + 1}/${n}  → p${r.partition} offset ${r.offset}`);
    }
    ok(`${n} messages produced`);
  },

  // ---- browse ----
  async browse([topic]) {
    if (!topic) { err('usage: browse <topic>'); return; }
    header(`Browsing ${topic} (latest 10)`);
    const r = await api('POST', `/api/clusters/default/topics/${encodeURIComponent(topic)}/browse`,
      { start: 'latest', limit: 10, timeoutMs: 5000 });
    if (r.messages.length === 0) { info('No messages'); return; }
    r.messages.reverse().forEach(m => {
      const ts = new Date(m.timestamp).toISOString().slice(11, 19);
      console.log(`  ${C.dim}p${m.partition} #${m.offset}${C.reset}  ${ts}  ${C.yellow}${m.key ?? 'null'}${C.reset}  ${m.value?.slice(0, 80)}`);
    });
    console.log(`\n  ${r.messages.length} shown, reached end: ${r.reachedEnd}`);
  },

  async browseFirst([topic]) {
    if (!topic) { err('usage: browse-first <topic>'); return; }
    const r = await api('POST', `/api/clusters/default/topics/${encodeURIComponent(topic)}/browse`,
      { start: 'earliest', limit: 10, timeoutMs: 5000 });
    header(`First messages in ${topic}`);
    r.messages.forEach(m => {
      const ts = new Date(m.timestamp).toISOString().slice(11, 19);
      console.log(`  ${C.dim}p${m.partition} #${m.offset}${C.reset}  ${ts}  ${C.yellow}${m.key ?? 'null'}${C.reset}  ${m.value?.slice(0, 80)}`);
    });
  },

  // ---- groups ----
  async groups() {
    header('Consumer Groups');
    const list = await api('GET', '/api/clusters/default/groups');
    if (list.length === 0) { info('No consumer groups'); return; }
    list.forEach(g => {
      const lag = g.totalLag > 0 ? `${C.yellow}${g.totalLag} lag${C.reset}` : `${C.green}0 lag${C.reset}`;
      console.log(`  ${g.groupId}  [${g.state}]  ${g.committedPartitions}p  ${lag}`);
    });
  },

  async group(name) {
    if (!name) { err('usage: group <name>'); return; }
    header(`Consumer Group: ${name}`);
    const d = await api('GET', `/api/clusters/default/groups/${encodeURIComponent(name)}`);
    console.log(`  State:      ${d.state}`);
    console.log(`  Total lag:  ${d.totalLag}`);
    console.log(`  Members:    ${d.members.length}`);
    for (const [topic, parts] of Object.entries(d.partitionsByTopic)) {
      console.log(`\n  ${topic}:`);
      parts.forEach(p => {
        const bar = '█'.repeat(Math.min(20, Math.ceil(p.lag / 10)));
        console.log(`    p${p.partition}  committed=${p.committedOffset}  end=${p.endOffset ?? '?'}  lag=${p.lag}  ${bar}`);
      });
    }
  },

  // ---- distribution ----
  async distribution() {
    header('Partition Distribution Across Brokers');
    const r = await api('GET', '/api/clusters/default/broker-distribution');
    r.brokers.forEach(b => {
      const l = b.leaderCount, f = b.followerCount;
      const bar = '█'.repeat(Math.min(30, Math.ceil((b.totalPartitions / 10))));
      console.log(`  broker ${b.id}  ${C.orange}${l}L${C.reset} + ${C.cyan}${f}F${C.reset} = ${b.totalPartitions} total  ${bar}`);
    });
    console.log(`\n  Balanced: ${r.balanced ? 'yes' : 'no'}${r.skewNote ? ' — ' + r.skewNote : ''}`);
  },

  // ---- connections ----
  async connections() {
    header('Connections');
    const list = await api('GET', '/api/clusters');
    list.forEach(c => {
      console.log(`  ${c.id}  "${c.name}"  → ${c.bootstrapServers.join(', ')}  [${c.security?.protocol}]${c.builtIn ? ' (built-in)' : ''}`);
    });
  },

  // ---- history ----
  async history([topic]) {
    header('Partition History' + (topic ? ` (${topic})` : ' (all)'));
    const q = topic ? `?topic=${encodeURIComponent(topic)}&limit=20` : '?limit=20';
    const r = await api('GET', `/api/clusters/default/history${q}`);
    (r.events || []).forEach(e => {
      const ts = new Date(e.ts).toISOString().slice(0, 19);
      console.log(`  ${ts}  ${C.orange}${e.type}${C.reset}  ${e.topic} p${e.partition}  ${e.detail}`);
    });
    if (!r.events?.length) info('No events recorded yet');
  },

  // ---- help ----
  help() {
    header('Kview CLI — Kafka management from the terminal');
    console.log(`  ${C.bold}Usage:${C.reset} node kview-cli.js <command> [args]\n`);
    console.log(`  ${C.bold}Cluster:${C.reset}`);
    console.log(`    overview              cluster KPI summary`);
    console.log(`    brokers               list brokers`);
    console.log(`    distribution          partition distribution across brokers`);
    console.log(`    connections           list configured connections\n`);
    console.log(`  ${C.bold}Topics:${C.reset}`);
    console.log(`    topics                list all topics with counts`);
    console.log(`    topic <name>          topic detail with partitions`);
    console.log(`    create <name> [p] [rf] create a topic`);
    console.log(`    delete <name>         delete a topic\n`);
    console.log(`  ${C.bold}Messages:${C.reset}`);
    console.log(`    produce <topic> [key] [value]   produce a message`);
    console.log(`    produce-n <topic> [n]           produce n test messages`);
    console.log(`    browse <topic>                  show latest messages`);
    console.log(`    browse-first <topic>            show earliest messages\n`);
    console.log(`  ${C.bold}Consumer Groups:${C.reset}`);
    console.log(`    groups                list consumer groups`);
    console.log(`    group <name>          group detail with lag bars`);
    console.log(`    history [topic]       partition leader/ISR history\n`);
    console.log(`  ${C.bold}Env:${C.reset} KVIEW_URL=${BASE} (default)`);
  },
};

// alias map for multi-word commands
const aliases = {
  'produce-n': 'produceN',
  'browse-first': 'browseFirst',
};

const fn = commands[aliases[cmd] || cmd];
if (!fn) {
  commands.help();
  process.exit(1);
}

Promise.resolve(fn(args.slice(1))).catch(e => {
  err(e.message);
  process.exit(1);
});
