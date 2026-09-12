#!/usr/bin/env node
// ===== Kview MCP server =====
// Exposes Kview's REST API as Model Context Protocol tools so AI clients
// (Claude Desktop, Cursor, ZCode, ...) can inspect and operate Kafka.
//
// Zero dependencies — implements the MCP stdio transport directly:
// newline-delimited JSON-RPC 2.0 on stdin/stdout, logs on stderr.
//
// Usage:
//   KVIEW_URL=http://localhost:8090 node mcp/kview-mcp.js
//
// Environment:
//   KVIEW_URL      base URL of the kview server   (default http://localhost:8090)
//   KVIEW_CLUSTER  default cluster/connection id  (default "default")

import { createInterface } from 'node:readline';

const KVIEW_URL = (process.env.KVIEW_URL || 'http://localhost:8090').replace(/\/+$/, '');
const DEFAULT_CLUSTER = process.env.KVIEW_CLUSTER || 'default';
const VERSION = '2.1.0';
const MAX_VALUE_CHARS = 4000; // per-string truncation inside tool output

function log(msg) {
  process.stderr.write(`[kview-mcp] ${msg}\n`);
}

// ---- Kview REST client ------------------------------------------------

async function api(path, options = {}) {
  let res;
  try {
    res = await fetch(KVIEW_URL + path, {
      headers: { 'Content-Type': 'application/json' },
      ...options,
    });
  } catch (e) {
    throw new Error(`Kview is not reachable at ${KVIEW_URL} (${e.message}). Is the kview process running?`);
  }
  const text = await res.text();
  let body;
  try { body = text ? JSON.parse(text) : null; } catch { body = text; }
  if (!res.ok) {
    const detail = body && typeof body === 'object' ? (body.detail || body.error) : String(body ?? '');
    throw new Error(`Kview API ${res.status} ${res.statusText}${detail ? `: ${detail}` : ''}`);
  }
  return body;
}

const cluster = (p = {}) => p.cluster || DEFAULT_CLUSTER;

function requireArg(args, name) {
  const v = args?.[name];
  if (v === undefined || v === null || v === '') throw new Error(`Missing required argument: ${name}`);
  return v;
}

/** Deep-copy with long strings truncated so tool output stays bounded. */
function slim(value, depth = 0) {
  if (typeof value === 'string') {
    return value.length > MAX_VALUE_CHARS ? value.slice(0, MAX_VALUE_CHARS) + `… [+${value.length - MAX_VALUE_CHARS} chars]` : value;
  }
  if (Array.isArray(value)) return value.slice(0, 500).map((v) => slim(v, depth + 1));
  if (value && typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) out[k] = slim(v, depth + 1);
    return out;
  }
  return value;
}

// ---- Tool implementations ---------------------------------------------

const impl = {
  async connections() {
    return api('/api/clusters');
  },

  async overview(p) {
    return api(`/api/clusters/${cluster(p)}/overview`);
  },

  async brokers(p) {
    return api(`/api/clusters/${cluster(p)}/brokers`);
  },

  async brokerDistribution(p) {
    return api(`/api/clusters/${cluster(p)}/broker-distribution`);
  },

  async brokerPartitions(p) {
    const id = requireArg(p, 'brokerId');
    return api(`/api/clusters/${cluster(p)}/brokers/${id}/partitions`);
  },

  async brokerConfigs(p) {
    const id = requireArg(p, 'brokerId');
    return api(`/api/clusters/${cluster(p)}/brokers/${id}/configs`);
  },

  async topics(p) {
    return api(`/api/clusters/${cluster(p)}/topics?includeCounts=true`);
  },

  async topicDetail(p) {
    const topic = requireArg(p, 'topic');
    return api(`/api/clusters/${cluster(p)}/topics/${encodeURIComponent(topic)}`);
  },

  async topicHistory(p) {
    const topic = requireArg(p, 'topic');
    const limit = Math.min(Number(p.limit) || 200, 1000);
    return api(`/api/clusters/${cluster(p)}/topics/${encodeURIComponent(topic)}/history?limit=${limit}`);
  },

  async topology(p) {
    return api(`/api/clusters/${cluster(p)}/topology`);
  },

  async createTopic(p) {
    const body = {
      name: requireArg(p, 'topic'),
      partitions: Number(p.partitions) || 1,
      replicationFactor: Number(p.replicationFactor) || 1,
    };
    if (p.configs && typeof p.configs === 'object') body.configs = p.configs;
    return api(`/api/clusters/${cluster(p)}/topics`, { method: 'POST', body: JSON.stringify(body) });
  },

  async deleteTopic(p) {
    const topic = requireArg(p, 'topic');
    if (p.confirm !== true) {
      throw new Error(`Refusing to delete topic '${topic}' — pass confirm=true to acknowledge that this permanently deletes its data.`);
    }
    return api(`/api/clusters/${cluster(p)}/topics/${encodeURIComponent(topic)}`, { method: 'DELETE' });
  },

  async produce(p) {
    const topic = requireArg(p, 'topic');
    if (p.key === undefined && p.payload === undefined && p.payloadBase64 === undefined) {
      throw new Error('Provide at least a key or a payload (both may be empty strings to send a tombstone).');
    }
    const body = {};
    if (p.key !== undefined) body.key = String(p.key);
    if (p.payload !== undefined) body.payload = typeof p.payload === 'string' ? p.payload : JSON.stringify(p.payload);
    if (p.payloadBase64 !== undefined) body.payloadBase64 = p.payloadBase64;
    if (p.partition !== undefined) body.partition = Number(p.partition);
    if (p.headers && typeof p.headers === 'object') body.headers = p.headers;
    return api(`/api/clusters/${cluster(p)}/topics/${encodeURIComponent(topic)}/messages`,
      { method: 'POST', body: JSON.stringify(body) });
  },

  async browse(p) {
    const topic = requireArg(p, 'topic');
    const body = { limit: Math.min(Number(p.limit) || 20, 100) };
    if (p.start) body.start = p.start;                     // earliest | latest
    if (p.partition !== undefined) body.partition = Number(p.partition);
    if (p.keyContains) body.keyContains = p.keyContains;
    if (p.valueContains) body.valueContains = p.valueContains;
    if (p.timestamp) body.timestamp = Number(p.timestamp);
    return api(`/api/clusters/${cluster(p)}/topics/${encodeURIComponent(topic)}/browse`,
      { method: 'POST', body: JSON.stringify(body) });
  },

  async groups(p) {
    return api(`/api/clusters/${cluster(p)}/groups`);
  },

  async groupDetail(p) {
    const group = requireArg(p, 'group');
    return api(`/api/clusters/${cluster(p)}/groups/${encodeURIComponent(group)}`);
  },
};

// ---- Tool definitions (name, description, inputSchema, run) ------------

const object = (properties, required = []) => ({ type: 'object', properties, required });
const str = (description) => ({ type: 'string', description });
const num = (description) => ({ type: 'number', description });
const bool = (description) => ({ type: 'boolean', description });

const TOOLS = [
  {
    name: 'kview_connections',
    description: 'List the Kafka connections configured in Kview (cluster ids usable by every other kview tool).',
    inputSchema: object({}),
    run: impl.connections,
  },
  {
    name: 'kview_overview',
    description: 'Cluster overview KPIs: broker count, topic/partition counts, estimated messages, consumer groups, under-replicated and offline partitions.',
    inputSchema: object({ cluster: str('Kview connection id (optional)') }),
    run: impl.overview,
  },
  {
    name: 'kview_brokers',
    description: 'List brokers (id, host, port, rack) in the cluster.',
    inputSchema: object({ cluster: str('Kview connection id (optional)') }),
    run: impl.brokers,
  },
  {
    name: 'kview_broker_distribution',
    description: 'Partition distribution across brokers: leader/follower counts per broker and balance assessment.',
    inputSchema: object({ cluster: str('Kview connection id (optional)') }),
    run: impl.brokerDistribution,
  },
  {
    name: 'kview_broker_partitions',
    description: 'All leader and follower partitions hosted by one broker, with ISR state.',
    inputSchema: object({
      brokerId: num('Broker id, e.g. 1'),
      cluster: str('Kview connection id (optional)'),
    }, ['brokerId']),
    run: impl.brokerPartitions,
  },
  {
    name: 'kview_broker_configs',
    description: 'Effective broker configuration entries (value, source, read-only).',
    inputSchema: object({
      brokerId: num('Broker id, e.g. 1'),
      cluster: str('Kview connection id (optional)'),
    }, ['brokerId']),
    run: impl.brokerConfigs,
  },
  {
    name: 'kview_topics',
    description: 'List topics with partition counts, replication factor, message counts and under-replicated partitions.',
    inputSchema: object({ cluster: str('Kview connection id (optional)') }),
    run: impl.topics,
  },
  {
    name: 'kview_topic_detail',
    description: 'One topic in depth: per-partition leader/replicas/ISR/end offsets plus all topic configs.',
    inputSchema: object({
      topic: str('Topic name'),
      cluster: str('Kview connection id (optional)'),
    }, ['topic']),
    run: impl.topicDetail,
  },
  {
    name: 'kview_topic_history',
    description: 'Recent partition events for a topic recorded by Kview\'s background sampler: LEADER_CHANGED, ISR_CHANGED, OFFLINE.',
    inputSchema: object({
      topic: str('Topic name'),
      limit: num('Max events to return (default 200, max 1000)'),
      cluster: str('Kview connection id (optional)'),
    }, ['topic']),
    run: impl.topicHistory,
  },
  {
    name: 'kview_topology',
    description: 'Whole-cluster topology in one call: brokers + controller, topics with partition assignment, consumer groups and the topics they read.',
    inputSchema: object({ cluster: str('Kview connection id (optional)') }),
    run: impl.topology,
  },
  {
    name: 'kview_create_topic',
    description: 'Create a topic.',
    inputSchema: object({
      topic: str('New topic name'),
      partitions: num('Partition count (default 1)'),
      replicationFactor: num('Replication factor (default 1)'),
      configs: { type: 'object', description: 'Optional topic configs, e.g. {"retention.ms":"86400000"}' },
      cluster: str('Kview connection id (optional)'),
    }, ['topic']),
    run: impl.createTopic,
  },
  {
    name: 'kview_delete_topic',
    description: 'Delete a topic and all of its data. Destructive — requires confirm=true.',
    inputSchema: object({
      topic: str('Topic name'),
      confirm: bool('Must be true to actually delete'),
      cluster: str('Kview connection id (optional)'),
    }, ['topic']),
    run: impl.deleteTopic,
  },
  {
    name: 'kview_produce',
    description: 'Produce one message to a topic. JSON payloads are sent as-is; schema-registry-backed topics get encoded automatically when the connection has a registry configured.',
    inputSchema: object({
      topic: str('Topic name'),
      key: { type: 'string', description: 'Message key (optional)' },
      payload: { type: 'object', description: 'Payload — object/JSON value or string (optional)' },
      partition: num('Target partition (optional, default: hashed/murmur2 by key or sticky)'),
      headers: { type: 'object', description: 'Optional headers as string map' },
      cluster: str('Kview connection id (optional)'),
    }, ['topic']),
    run: impl.produce,
  },
  {
    name: 'kview_browse',
    description: 'Read messages from a topic (read-only, does not commit offsets). Supports earliest/latest starts, partition scoping, text filters and a limit. Schema-registry payloads are decoded automatically.',
    inputSchema: object({
      topic: str('Topic name'),
      start: str('"earliest" (default) or "latest"'),
      partition: num('Restrict to one partition (optional)'),
      limit: num('Max messages (default 20, max 100)'),
      keyContains: str('Only messages whose key contains this text (optional)'),
      valueContains: str('Only messages whose value contains this text (optional)'),
      cluster: str('Kview connection id (optional)'),
    }, ['topic']),
    run: impl.browse,
  },
  {
    name: 'kview_consumer_groups',
    description: 'List consumer groups with state, committed partition count and total lag.',
    inputSchema: object({ cluster: str('Kview connection id (optional)') }),
    run: impl.groups,
  },
  {
    name: 'kview_consumer_group_detail',
    description: 'One consumer group in depth: members (client id, host, assignments), per-topic/per-partition lag and the coordinator broker.',
    inputSchema: object({
      group: str('Consumer group id'),
      cluster: str('Kview connection id (optional)'),
    }, ['group']),
    run: impl.groupDetail,
  },
];

// ---- MCP protocol plumbing (JSON-RPC 2.0 over stdio, NDJSON framing) ---

function send(obj) {
  process.stdout.write(JSON.stringify(obj) + '\n');
}

function toolResult(text, isError = false) {
  return { content: [{ type: 'text', text }], isError };
}

async function dispatch(method, params = {}) {
  switch (method) {
    case 'initialize':
      return {
        protocolVersion: params.protocolVersion || '2024-11-05',
        capabilities: { tools: { listChanged: false } },
        serverInfo: { name: 'kview', title: 'Kview — Apache Kafka MCP', version: VERSION },
      };
    case 'tools/list':
      return { tools: TOOLS.map(({ name, description, inputSchema }) => ({ name, description, inputSchema })) };
    case 'tools/call': {
      const tool = TOOLS.find((t) => t.name === params.name);
      if (!tool) return toolResult(`Unknown tool: ${params.name}`, true);
      try {
        const data = await tool.run(params.arguments || {});
        return toolResult(JSON.stringify(slim(data), null, 2));
      } catch (e) {
        log(`tool ${params.name} failed: ${e.message}`);
        return toolResult(`Error: ${e.message}`, true);
      }
    }
    case 'ping':
      return {};
    case 'resources/list':
      return { resources: [] };
    case 'prompts/list':
      return { prompts: [] };
    default:
      throw Object.assign(new Error(`Method not found: ${method}`), { code: -32601 });
  }
}

async function handleMessage(msg) {
  const { id, method, params } = msg;
  if (!method) return;                        // a response — we never send requests
  if (method.startsWith('notifications/')) return; // notifications get no reply
  if (id === undefined) return;
  try {
    const result = await dispatch(method, params);
    send({ jsonrpc: '2.0', id, result });
  } catch (err) {
    send({ jsonrpc: '2.0', id, error: { code: err.code || -32603, message: err.message } });
  }
}

const rl = createInterface({ input: process.stdin, terminal: false });
rl.on('line', (line) => {
  const trimmed = line.trim();
  if (!trimmed) return;
  let msg;
  try {
    msg = JSON.parse(trimmed);
  } catch (e) {
    log(`ignoring unparseable line: ${e.message}`);
    return;
  }
  handleMessage(msg).catch((e) => log(`handler crashed: ${e.message}`));
});
rl.on('close', () => process.exit(0));

log(`kview MCP server ready — REST base ${KVIEW_URL}, default cluster "${DEFAULT_CLUSTER}", ${TOOLS.length} tools`);
