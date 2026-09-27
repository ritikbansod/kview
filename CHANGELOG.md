# Changelog

All notable changes to Kview will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- **Prometheus metrics** (F6): `/actuator/prometheus` now serves `kview_*` series — per-cluster topic/partition counts, under-replicated and offline partitions, reachability (`kview_cluster_*` with a `cluster` tag), per-group consumer lag (`kview_consumer_group_lag` with `cluster`/`group` tags) and a produced-messages counter (`kview_messages_produced_total`). A 15-second in-process sampler (`KVIEW_METRICS_INTERVAL_MS`) keeps scrapes off the broker; series appear and disappear with the clusters and groups they describe. Endpoint follows the actuator auth rules (public in the default no-auth mode).
- **Bulk message replay / DLQ triage** (F3): `POST /api/clusters/{clusterId}/topics/{topic}/replay` selects messages with the browser's filters and copies them to another topic — same or a different connected cluster — preserving keys, headers and original timestamps (max 1000 per call, oldest first, `dryRun` previews the selection). Exposed in the UI as a **Replay** tab in the explorer, in the CLI as `kview replay <topic> --to-topic T [...]`, and in MCP as `kview_replay_messages` (19 tools now; `confirm=true` required, `dryRun` recommended first).
- **Whole-topic message search** (F2): `POST /api/clusters/{clusterId}/topics/{topic}/search` starts a bounded background scan (regex / contains / key / timestamp filters, capped at 1000 results and 2M scanned messages, max 3 concurrent jobs) with progress polling and cancellation (`GET`/`DELETE .../searches/{id}`). Exposed in the UI as a new **Search** tab in the explorer, in the CLI as `kview search` / `kview search-status`, and in MCP as `kview_search_messages` + `kview_search_status` (18 tools now). Read-only like the browser: throwaway group, never commits.
- **API authentication with roles** (`kview.auth.*`, off by default): `token` mode (static bearer tokens) and `oidc` mode (JWTs from any issuer, configurable role claim), each with an **admin** and a **read-only** role — reads for everyone, mutations (produce, topic/group management, offset resets, connection management) admin-only. The web UI prompts for the token (🔑 in the top bar), the CLI takes `--token`/`KVIEW_TOKEN`, the MCP server takes `KVIEW_TOKEN`. In authenticated modes CORS is closed unless `KVIEW_AUTH_ALLOWED_ORIGINS` lists origins; the static UI and `/actuator/health` stay public. See the new Authentication section in the README.
- **AI/MCP safety pack**: global read-only mode (`KVIEW_READONLY=true` — every mutation refused for all clients and roles, UI shows a banner), an append-only mutation audit trail (`KVIEW_AUDIT=true` → `<data-dir>/audit.log` JSONL with client, method, path and status; all three clients send an `X-Kview-Client` identifier), a public `GET /api/meta` endpoint, and a per-tool allowlist for the MCP server (`KVIEW_ALLOWED_TOOLS`, e.g. serve only read-only tools to an AI agent).

### Fixed
- **MCP server**: `mcp/kview-mcp.js` crashed on startup with `SyntaxError: Cannot use import statement outside a module` on Node ≤ 22.6 (ESM in a `.js` file with no `package.json`) — fixed by shipping `mcp/package.json` with `"type": "module"`.
- **CLI**: `kview lag <group>` read a non-existent `byTopic` field and silently printed no per-partition lag rows — now reads `partitionsByTopic`, as the API serializes it.
- **CLI**: `kview clusters` tabled a `securityProtocol` field the API never returns, leaving SECURITY always empty — now derived from the masked `security` object.
- **Concurrency**: `DynamicConsumerService` had a check-then-act race on subscribe that could leak a permanently running consumer container; subscribe/unsubscribe are now serialized, group buffers are freed when their last subscription ends, and all containers stop on shutdown.
- **Concurrency**: `RegistrySettingsStore.find()` read a plain `LinkedHashMap` while writers mutated it — now a `ConcurrentHashMap`.
- **Concurrency**: schema-registry cache loads (remote HTTP, 10s timeout) ran inside one shared lock, so a slow registry stalled decodes cluster-wide — now per-key.
- **Concurrency**: `KafkaClusterManager.get`/`evict` could hand out a handle that was concurrently being closed — now mutually excluded.
- **Tests**: integration tests overrode a wrong property name (`kafka-wrapper.data-dir`) and rewrote the developer's real `./data` directory on every run — both suites now point `kview.data-dir` at `target/`.

### Changed
- **Spring Boot 3.3.4 → 3.5.16** (completes the release plan's P2.3 — the 3.3 line is past OSS support end): lifts Spring Framework to 6.2.x, Spring Security to 6.5.x, spring-kafka to 3.3.x, micrometer to 1.15.x and **kafka-clients to 3.9.2**; `KafkaClusterManager` now passes `SslBundles` to the Kafka properties builders, as required by the new builder signatures.
- Docs: corrected the README `reset-offsets` example; the README roadmap and COMPATIBILITY.md no longer describe the shipped Schema Registry support (Avro/JSON) as unbuilt; internal working documents moved from the repo root to `docs/`.
- Removed the divergent legacy `kview-cli.js` client; `cli/kview.mjs` is the CLI.
- CI: the MCP server startup is now smoke-tested on Node 20 (which would have caught the ESM crash), alongside the CLI smoke test.
- Release: artifacts are built with the test suite green, and a release tag must match the versions in `pom.xml`, `cli/kview.mjs` and `mcp/kview-mcp.js`.
- Added the Maven wrapper so a clone builds with just a JDK (`./mvnw`).

## [1.0.0] - 2026-09-13

### Initial Public Release

#### Web UI & Management
- **Dashboard**: Real-time cluster KPIs (broker counts, controller ID, topic and partition counts, estimated messages, consumer groups, under-replicated and offline partitions).
- **Broker Partition Distribution**: Visual analysis of partition balance across brokers with config inspector modal.
- **Cluster Architecture & Topology**: Live end-to-end diagram visualizing Producers → Cluster (Brokers + Topics + Partitions) → Consumers with SVG connections and throughput metrics.
- **Topic Lifecycle**: Create, describe, expand partitions, edit configs, and delete topics with safety confirmation.
- **Replica Placement**: Flow and Matrix views illustrating partition leaders, followers, and In-Sync Replicas (ISR).
- **Partition History**: Background 15-second state sampler and diff engine tracking leader moves, elections, and ISR changes over time.
- **Consumer Groups**: Lag tracking per partition, member assignment details, group deletion, and offset resets (earliest, latest, offset, timestamp).
- **Theme**: Token-based dark and light themes following system preferences.

#### Data Explorer
- **Read-Only Safety**: Non-committing message browser using partition assignment and explicit seeks without triggering group rebalances.
- **Flexible Seeks**: Seek by earliest, latest (scans backwards from high watermarks), explicit partition offsets map, or timestamp.
- **Live Tail**: Real-time Server-Sent Events (SSE) streaming with auto-scroll, rate counter, and optional real consumer group attachment.
- **Message Inspection**: Multi-tab viewer (Decoded, Raw, Hex, Schema metadata, Headers) with one-click reproduction to produce tab.
- **Producer**: Built-in produce form with key, headers, partition pinning, and live JSON syntax validation.

#### Schema Registry Integration
- **Generic SPI**: Pluggable `SchemaRegistryAdapter` supporting Confluent Schema Registry, Confluent Cloud, Redpanda, Karapace, and Apicurio.
- **Wire Format Sniffer**: Detects 5-byte Confluent wire format with safe raw fallback for unknown binaries.
- **Avro & JSON Schema Codecs**: Generic decoding and encoding supporting logical types (dates, timestamps, decimals, UUIDs).
- **Produce Validation**: Subject-aware payload generation and pre-flight schema compatibility validation.
- **Schema Caching**: 5-minute memory TTL cache for schema resolution at $O(1)$ during high-throughput tails.

#### Multi-Cluster Connections & Security
- **Multi-Cluster Support**: Connect to and switch between multiple Kafka clusters dynamically at runtime.
- **Security Protocols**: PLAINTEXT, TLS/SSL, mTLS (with keystores or direct PEM certificate paste), and SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER).
- **OAuth2 OIDC**: Automatic token fetching and refresh via client-credentials flow.
- **Credential Protection**: Cluster secrets stored server-side in `data/connections.json` and masked in all API responses.
- **Default Loopback Binding**: Binds to `127.0.0.1` by default for safe local development.

#### Interfaces & Tooling
- **REST API**: Clean RESTful endpoints mounted under `/api/clusters/{clusterId}/...`.
- **Node.js CLI (`cli/kview.mjs`)**: Dependency-free CLI for cluster operations, message browsing, producing, and offset resets.
- **MCP Server (`mcp/kview-mcp.js`)**: 16 Model Context Protocol tools over JSON-RPC stdio for AI assistants (Claude Desktop, Cursor, ZCode).
- **Docker**: Official multi-stage `Dockerfile` and `docker-compose.yml` stack.
