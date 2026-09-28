#!/usr/bin/env node
// kview npm dispatcher.
//
// Two modes:
//  - Direct mode (no server): when a kview jar is available — KVIEW_JAR env or a
//    kview-*.jar in the current directory — commands run against the broker with
//    the bundled-Java launcher (needs Java 21).
//  - Thin client (default): talks to a running Kview server (KVIEW_URL, default
//    http://localhost:8090). Zero dependencies.
import { readdirSync, existsSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const thinClient = join(here, '..', 'cli', 'kview.mjs');
const args = process.argv.slice(2);

function findJar() {
  if (process.env.KVIEW_JAR) return process.env.KVIEW_JAR;
  if (!existsSync(process.cwd())) return null;
  const jars = readdirSync(process.cwd()).filter((f) => /^kview-.+\.jar$/.test(f)).sort();
  return jars.length ? join(process.cwd(), jars[jars.length - 1]) : null;
}

const jar = findJar();
if (jar) {
  const result = spawnSync('java', ['-jar', jar, '--cli', ...args], { stdio: 'inherit' });
  if (result.error) {
    console.error(`error: could not run java for direct mode (${result.error.message}) — falling back to the thin client`);
  } else {
    process.exit(result.status ?? 1);
  }
}

if (args.includes('--bootstrap-server')) {
  console.error('error: --bootstrap-server needs direct mode — download the kview jar from');
  console.error('       https://github.com/ritikbansod/kview/releases and set KVIEW_JAR (Java 21 required),');
  console.error('       or run the Kview server and use it without --bootstrap-server.');
  process.exit(1);
}

const result = spawnSync(process.execPath, [thinClient, ...args], { stdio: 'inherit' });
process.exit(result.status ?? 1);
