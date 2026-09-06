#!/usr/bin/env node
/**
 * Distribution-shape compatibility verification for the Kview.
 *
 * Drives Kview's connection-profile API against a local broker configured
 * with the security shapes used by real distributions:
 *   TLS :9093  SSL + client auth required   -> Strimzi mTLS shape (PEM client cert)
 *   SCRAM:9094 SASL_SSL + SCRAM-SHA-512     -> Strimzi/MSK SCRAM-over-TLS shape
 *   CLOUD:9095 SASL_SSL + PLAIN              -> Confluent Cloud API-key shape
 * Plus negative cases (missing client cert, wrong password).
 *
 * Run: node scripts/verify-compatibility.mjs
 */
import { readFileSync } from 'node:fs';

const BASE = process.env.BASE_URL || 'http://localhost:8090';
const CERTS = '.kafka/certs';
const ca = readFileSync(`${CERTS}/ca.pem`, 'utf8');
const clientCert = readFileSync(`${CERTS}/client.pem`, 'utf8');
const clientKey = readFileSync(`${CERTS}/client.key`, 'utf8');

let passed = 0, failed = 0;
const rows = [];
function check(name, cond, detail = '') {
  if (cond) { passed++; console.log(`  PASS  ${name}`); rows.push({ name, ok: true }); }
  else { failed++; console.log(`  FAIL  ${name}${detail ? ' — ' + detail : ''}`); rows.push({ name, ok: false, detail }); }
}

async function api(method, path, body) {
  const res = await fetch(BASE + path, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let json = null; try { json = text ? JSON.parse(text) : null; } catch { json = text; }
  return { status: res.status, json };
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function verifyProfileFlow(profile, label) {
  // 1. test-connection
  const t = await api('POST', '/api/clusters/test', profile);
  check(`[${label}] test-connection succeeds`, t.json?.ok === true && t.json.nodes?.length >= 1,
    JSON.stringify(t.json)?.slice(0, 100));
  if (!t.json?.ok) return;

  // 2. persist profile
  const created = await api('POST', '/api/clusters', profile);
  const id = created.json?.id;
  check(`[${label}] profile saved`, !!id);

  // 3. full data flow through the secured connection
  const topic = `compat-${id}`;
  const c = await api('POST', `/api/clusters/${id}/topics`, { name: topic, partitions: 1, replicationFactor: 1 });
  check(`[${label}] create topic over secure channel`, c.json?.status === 'created', JSON.stringify(c.json)?.slice(0, 80));
  const p = await api('POST', `/api/clusters/${id}/topics/${topic}/messages`, {
    key: `${label}-1`, payload: JSON.stringify({ via: label, seq: 1 }), headers: { marker: label },
  });
  check(`[${label}] produce over secure channel`, p.status === 201, JSON.stringify(p.json)?.slice(0, 80));
  const b = await api('POST', `/api/clusters/${id}/topics/${topic}/browse`, { start: 'earliest', limit: 10, timeoutMs: 8000 });
  check(`[${label}] browse over secure channel`, b.json?.messages?.length === 1, `got ${b.json?.messages?.length}`);
  const o = await api('GET', `/api/clusters/${id}/overview`);
  check(`[${label}] cluster overview over secure channel`, o.status === 200);

  // 4. cleanup profile (topics stay; harmless)
  await api('DELETE', `/api/clusters/${id}`);
}

async function main() {
  console.log(`\n=== Distribution-shape compatibility verification — ${BASE} ===\n`);

  console.log('[Shape 1] Strimzi-style mTLS — SSL, client-auth required, PEM identity + PEM CA trust');
  await verifyProfileFlow({
    name: 'Compat mTLS (Strimzi shape)',
    bootstrapServers: ['localhost:9093'],
    security: {
      protocol: 'SSL',
      keystoreCertificateChainPem: clientCert + '\n' + ca,
      keystoreKeyPem: clientKey,
      truststoreCertificatesPem: ca,
      endpointVerificationEnabled: true,
    },
  }, 'mtls');

  console.log('\n[Shape 2] Strimzi/MSK-style SCRAM — SASL_SSL, SCRAM-SHA-512, PEM CA trust');
  await verifyProfileFlow({
    name: 'Compat SCRAM-512 (Strimzi shape)',
    bootstrapServers: ['localhost:9094'],
    security: {
      protocol: 'SASL_SSL', saslMechanism: 'SCRAM-SHA-512',
      saslUsername: 'wrapper', saslPassword: 'wrappier-secret',
      truststoreCertificatesPem: ca, endpointVerificationEnabled: true,
    },
  }, 'scram');

  console.log('\n[Shape 3] Confluent Cloud-style — SASL_SSL, PLAIN (API key/secret), PEM CA trust');
  await verifyProfileFlow({
    name: 'Compat PLAIN (Confluent Cloud shape)',
    bootstrapServers: ['localhost:9095'],
    security: {
      protocol: 'SASL_SSL', saslMechanism: 'PLAIN',
      saslUsername: 'wrapper', saslPassword: 'wrappier-secret',
      truststoreCertificatesPem: ca, endpointVerificationEnabled: true,
    },
  }, 'ccloud');

  console.log('\n[Negative] mTLS listener WITHOUT client certificate must be refused');
  const noCert = await api('POST', '/api/clusters/test', {
    name: 'no cert', bootstrapServers: ['localhost:9093'], security: { protocol: 'SSL', truststoreCertificatesPem: ca },
  });
  check('no-client-cert connection refused', noCert.json?.ok === false, JSON.stringify(noCert.json)?.slice(0, 90));

  console.log('\n[Negative] SCRAM with WRONG password must be refused');
  const badPw = await api('POST', '/api/clusters/test', {
    name: 'bad pw', bootstrapServers: ['localhost:9094'],
    security: { protocol: 'SASL_SSL', saslMechanism: 'SCRAM-SHA-512', saslUsername: 'wrapper', saslPassword: 'wrong-password', truststoreCertificatesPem: ca },
  });
  check('wrong SCRAM password refused', badPw.json?.ok === false, JSON.stringify(badPw.json)?.slice(0, 90));

  console.log('\n[Negative] TLS trust mismatch (untrusted CA) must be refused');
  const rogue = '-----BEGIN CERTIFICATE-----\nMIIBhTCCASugAwIBAgIRAKvL2eGfZ1FqCggZz8H2XjAwCgYIKoZIzj0EAwIwESEQMA4GA1UEAxMHcm9ndWVBDjAeFw0yNjAxMDEwMDAwMDBaFw0yNzAxMDEwMDAwMDBaMBIxEDAOBgNVBAMTB3JvZ3VlQTAKBggqhkjOPQQDAgNHADBEAiA=\n-----END CERTIFICATE-----';
  const badCa = await api('POST', '/api/clusters/test', {
    name: 'rogue CA', bootstrapServers: ['localhost:9094'],
    security: { protocol: 'SASL_SSL', saslMechanism: 'SCRAM-SHA-512', saslUsername: 'wrapper', saslPassword: 'wrappier-secret', truststoreCertificatesPem: rogue + '-----END CERTIFICATE-----' },
  });
  check('untrusted CA refused', badCa.json?.ok === false, JSON.stringify(badCa.json)?.slice(0, 90));

  console.log(`\n=== COMPAT RESULT: ${passed} passed, ${failed} failed ===`);
  if (failed > 0) process.exit(1);
}

main().catch(e => { console.error('Compat verification crashed:', e); process.exit(1); });
