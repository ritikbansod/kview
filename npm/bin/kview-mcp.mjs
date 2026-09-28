#!/usr/bin/env node
// kview-mcp npm bin — runs the bundled MCP server (npx kview-mcp).
import { spawnSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const server = join(dirname(fileURLToPath(import.meta.url)), '..', 'mcp', 'kview-mcp.js');
const result = spawnSync(process.execPath, [server, ...process.argv.slice(2)], { stdio: 'inherit' });
process.exit(result.status ?? 1);
