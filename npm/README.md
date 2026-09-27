# kview (npm package)

Installs two commands:

- **`kview`** — the Kview CLI.
  - **Thin client (default):** talks to a running Kview server — `KVIEW_URL` (default
    `http://localhost:8090`). Zero dependencies, Node 18+.
  - **Direct mode (no server):** when a kview jar is reachable — `KVIEW_JAR=/path/to/kview.jar`
    or a `kview-*.jar` in the current directory — commands run straight against the broker
    (needs Java 21): `kview topics --bootstrap-server localhost:9092`.
- **`kview-mcp`** — the Model Context Protocol server for AI assistants
  (Claude Desktop, Cursor, ZCode): `npx kview-mcp` with `KVIEW_URL`/`KVIEW_TOKEN`/`KVIEW_CLUSTER`
  env vars.

The npm package ships the thin client and MCP server only; the full server, the direct-mode
jar and native bundles come from the [GitHub releases](https://github.com/ritikbansod/kview/releases).
