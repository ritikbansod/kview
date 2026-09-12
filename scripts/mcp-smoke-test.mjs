#!/usr/bin/env node
// ===== MCP server smoke test =====
// Spawns mcp/kview-mcp.js as a real subprocess and speaks the MCP stdio
// protocol to it: initialize -> tools/list -> tool calls against a running
// Kview (default http://localhost:8090, override with KVIEW_URL).
//
//   node scripts/mcp-smoke-test.mjs

import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const SERVER = join(ROOT, 'mcp', 'kview-mcp.js');
const KVIEW_URL = process.env.KVIEW_URL || 'http://localhost:8090';

let passed = 0;
let failed = 0;
const check = (name, ok, extra = '') => {
  if (ok) { passed++; console.log(`  PASS  ${name}`); }
  else { failed++; console.log(`  FAIL  ${name}${extra ? ' — ' + extra : ''}`); }
};

// ---- tiny MCP client ----
const child = spawn(process.execPath, [SERVER], {
  env: { ...process.env, KVIEW_URL },
  stdio: ['pipe', 'pipe', 'pipe'],
});
child.stderr.on('data', (d) => process.stderr.write(`  [server] ${d}`));

const pending = new Map();
let nextId = 1;
const rl = createInterface({ input: child.stdout });
rl.on('line', (line) => {
  if (!line.trim()) return;
  let msg;
  try { msg = JSON.parse(line); } catch { return; }
  if (msg.id !== undefined && pending.has(msg.id)) {
    pending.get(msg.id)(msg);
    pending.delete(msg.id);
  }
});

function request(method, params) {
  const id = nextId++;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`timeout waiting for response to ${method}`));
    }, 20_000);
    pending.set(id, (msg) => { clearTimeout(timer); resolve(msg); });
    child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
  });
}

function notify(method, params) {
  child.stdin.write(JSON.stringify({ jsonrpc: '2.0', method, params }) + '\n');
}

const textOf = (res) => res.result?.content?.[0]?.text ?? '';
const parseTool = (res) => {
  const raw = textOf(res);
  try { return JSON.parse(raw); } catch { return raw; }
};

try {
  // ---- protocol handshake ----
  const init = await request('initialize', {
    protocolVersion: '2025-06-18',
    capabilities: {},
    clientInfo: { name: 'kview-smoke', version: '1.0' },
  });
  check('initialize returns serverInfo kview', init.result?.serverInfo?.name === 'kview');
  check('initialize declares tools capability', !!init.result?.capabilities?.tools);
  check('initialize echoes protocol version', typeof init.result?.protocolVersion === 'string');
  notify('notifications/initialized', {});

  // notification must not produce a response — the next response must be for tools/list
  const list = await request('tools/list', {});
  check('notifications/initialized is silent', !!list.result, 'first response after init was not tools/list');

  const tools = list.result?.tools ?? [];
  check('tools/list exposes 16 tools', tools.length === 16, `got ${tools.length}`);
  const names = new Set(tools.map((t) => t.name));
  for (const expected of ['kview_overview', 'kview_topics', 'kview_browse', 'kview_produce',
    'kview_consumer_groups', 'kview_delete_topic', 'kview_topology']) {
    check(`tool present: ${expected}`, names.has(expected));
  }
  check('every tool has an input schema', tools.every((t) => t.inputSchema?.type === 'object'));

  // ---- live calls against Kview ----
  const ping = await request('ping', {});
  check('ping responds empty result', !!ping.result && textOf(ping) === '');

  const overview = parseTool(await request('tools/call', { name: 'kview_overview', arguments: {} }));
  check('kview_overview returns brokers', !overview.isError && Number(overview[0]?.brokers?.length ?? overview.brokers?.length ?? 0) > 0
    || (overview.brokers?.length ?? 0) > 0, textOf({ result: { content: [{ type: 'text', text: JSON.stringify(overview) }] } }).slice(0, 120));

  const topics = parseTool(await request('tools/call', { name: 'kview_topics', arguments: {} }));
  const topicNames = Array.isArray(topics) ? topics.map((t) => t.name) : [];
  check('kview_topics lists orders', topicNames.includes('orders'), `got: ${topicNames.join(',')}`);

  const detail = parseTool(await request('tools/call', { name: 'kview_topic_detail', arguments: { topic: 'orders' } }));
  check('kview_topic_detail shows partitions', Array.isArray(detail?.partitions) && detail.partitions.length > 0);

  const marker = `mcp-smoke-${Date.now()}`;
  const produce = parseTool(await request('tools/call', {
    name: 'kview_produce',
    arguments: { topic: 'orders', key: marker, payload: { orderId: 999, note: marker } },
  }));
  check('kview_produce returns 201-style result', !produce.isError && (produce.status === 'sent' || produce.offset !== undefined || produce.partition !== undefined || JSON.stringify(produce).length > 0), JSON.stringify(produce).slice(0, 120));

  const browse = parseTool(await request('tools/call', {
    name: 'kview_browse',
    arguments: { topic: 'orders', start: 'latest', limit: 50, keyContains: marker },
  }));
  const browseMsgs = browse?.messages ?? [];
  check('kview_browse finds the produced marker', browseMsgs.some((m) => m.key === marker), JSON.stringify(browse).slice(0, 200));

  const groups = parseTool(await request('tools/call', { name: 'kview_consumer_groups', arguments: {} }));
  check('kview_consumer_groups returns array', Array.isArray(groups));

  const missing = await request('tools/call', { name: 'kview_topic_detail', arguments: { topic: 'no-such-topic-mcp' } });
  check('missing topic → isError result', missing.result?.isError === true, textOf(missing).slice(0, 120));

  const noConfirm = await request('tools/call', { name: 'kview_delete_topic', arguments: { topic: 'orders' } });
  check('delete without confirm is refused', noConfirm.result?.isError === true && /confirm=true/.test(textOf(noConfirm)));

  const unknown = await request('tools/call', { name: 'kview_nope', arguments: {} });
  check('unknown tool → isError result', unknown.result?.isError === true && /Unknown tool/.test(textOf(unknown)));

  const badMethod = await request('resources/read', { uri: 'file:///x' });
  check('unknown method → JSON-RPC -32601', badMethod.error?.code === -32601);

  const topology = parseTool(await request('tools/call', { name: 'kview_topology', arguments: {} }));
  check('kview_topology returns brokers+topics+groups',
    Array.isArray(topology?.brokers) && Array.isArray(topology?.topics) && Array.isArray(topology?.groups));

} catch (e) {
  failed++;
  console.log(`  FAIL  unexpected exception — ${e.message}`);
} finally {
  child.kill();
}

console.log(`\nmcp smoke test: ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
