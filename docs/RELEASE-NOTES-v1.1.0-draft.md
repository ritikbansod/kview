# Kview v1.1.0 — Release notes (final draft)

> **Status:** draft. Everything below is implemented across ten stacked pull requests
> (#1–#10) that originated in the v1.0.0 deep analysis. Merge them in order (#1 → #10),
> follow [RELEASING.md](RELEASING.md), then tag `v1.1.0`.

---

## Headline

Kview v1.0.0 shipped as a working tool with known sharp edges. v1.1.0 turns it into a
defensible open-source project: the wide-open API is now secured, the AI/MCP surface got a
purpose-built safety pack, two flagship features landed (whole-topic search and DLQ replay),
Protobuf works end to end, the CLI runs standalone without a server, and Kview is
installable through every standard channel — native bundles, npm, Docker Hub, Homebrew.
Underneath, the runtime was brought current (Spring Boot 3.5.16 / kafka-clients 3.9.2) and
all **18 independently verified issues** from the analysis were resolved. The test suite
grew from **30 to 66 tests** across **18 classes**, all green on the Linux + Windows CI
matrix. Six of the analysis's seven top-ranked feature candidates shipped (the seventh,
the schema registry browser, is the remaining roadmap item).

## 1. Security & safety

- **API authentication with roles** (was: completely unauthenticated with permissive CORS —
  the analysis's only high-severity code finding). `KVIEW_AUTH_MODE=token` (static bearer
  tokens) or `oidc` (JWTs from any issuer), each with an **admin** and a **read-only**
  role: reads for everyone, mutations admin-only. CORS closes when auth is on; the default
  remains zero-config and open on loopback, regression-tested.
- **AI/MCP safety pack**: global read-only mode (`KVIEW_READONLY=true` — every mutation
  refused, UI banner), an append-only mutation **audit trail** (`KVIEW_AUDIT=true` →
  `data/audit.log`, with every client identifying itself via `X-Kview-Client`, so an
  agent's writes are provably distinguishable from a human's), and a per-tool allowlist for
  the MCP server (`KVIEW_ALLOWED_TOOLS`).
- MCP hardening: the server no longer crashes on startup for Node ≤ 22.6 (ESM fix), and
  destructive MCP calls keep their `confirm=true` guard.

## 2. New features

- **Whole-topic message search** — the biggest explorer gap. A bounded background job scans
  every partition (regex / contains / key filters, timestamp window; max 1000 results,
  2M scanned, 3 concurrent jobs) with progress and cancellation. Available in the UI
  (**Search** tab), CLI, and MCP (`kview_search_messages` + `kview_search_status`).
- **DLQ triage & bulk replay** — copy filtered messages to another topic, on the same or a
  different connected cluster, preserving keys, headers and original timestamps; `dryRun`
  previews the selection. UI (**Replay** tab), CLI, MCP (with `confirm=true`).
- **Direct-mode CLI** — `java -jar kview.jar --cli topics --bootstrap-server localhost:9092`
  talks to the broker with **no server**, reusing the same services: browse comes out
  schema-registry decoded, plus overview/topics/groups/lag/produce/search/replay, `--json`
  output and the full SASL/mTLS/OAuth flag set. Shipped `kview` / `kview.cmd` wrappers.
- **Protobuf codecs** — PROTOBUF subjects decode to proto3 JSON in the browser, live tail
  and `/decode`, and JSON payloads encode into Confluent wire bytes (message-index prefix
  included) on produce. Nested-message indexes are reported as unsupported rather than
  mis-decoded.
- **Prometheus metrics** — `/actuator/prometheus` serves `kview_*` series: per-cluster
  topics/partitions, under-replicated and offline partitions, reachability, per-group
  consumer lag, and a produced-messages counter. A 15-second in-process sampler keeps
  scrapes off the broker.

## 3. Distribution & install

| Channel | What you get |
|---|---|
| **JAR** (GitHub Releases) | tested `kview.jar` — starts the server UI *or* the direct CLI |
| **Native bundles** | `kview-<v>-linux-x64.tar.gz` (+ `.deb`), `kview-<v>-macos-aarch64.tar.gz`, `kview-<v>-windows-x64.zip` — bundled JRE, server launcher **and** `kview` CLI; no Java install needed |
| **npm** | `npm install -g kview` → thin client + `npx kview-mcp`; direct mode when a jar/`KVIEW_JAR` is present |
| **Docker** | `ghcr.io/ritikbansod/kview:{v,latest}` (amd64+arm64), mirrored to **Docker Hub** on release; CLI passthrough: `docker run --rm ghcr.io/ritikbansod/kview --cli topics --bootstrap-server host:9092` |
| **Homebrew / SDKMAN** | formula template pushed to the tap on release; SDKMAN submission documented |

The release pipeline is fully automated in three jobs (build → per-OS jpackage bundles →
publish), with npm/Homebrew/Docker-Hub steps activating only when their repo secrets are
configured. The full per-channel checklist lives in [RELEASING.md](RELEASING.md).

## 4. Fixed (verified issues from the deep analysis — all 18 resolved)

- Leaked consumer containers from a subscribe race; per-group buffers freed on unsubscribe;
  containers stopped on shutdown.
- Data race on the schema-registry settings map (hot decode path) — now a `ConcurrentHashMap`.
- A slow or unreachable schema registry stalled every decode cluster-wide — cache loads are
  per-key now.
- `KafkaClusterManager` get/evict could hand out a handle being closed — mutually excluded.
- Integration tests overrode a wrong property name and rewrote the developer's real `./data`
  on every run — both suites now use `target/`.
- `kview lag` read a non-existent API field (silent zero lag rows); `kview clusters` tabled a
  field the API never returned; the README's `reset-offsets` example failed as written.
- Doc drift: README/COMPATIBILITY no longer claim the shipped Schema Registry support is
  "on the roadmap"; the divergent legacy `kview-cli.js` was removed; internal runbooks moved
  out of the repo root.
- Release trust: releases build **with the test suite green**, the release tag must match the
  versions in `pom.xml`/`cli`/`mcp`, CI runs the MCP server and CLI smokes, and the Maven
  wrapper ships so a clone builds with just a JDK.

## 5. Platform

- Spring Boot **3.3.4 → 3.5.16** (the 3.3 line is past OSS support end): Spring Framework
  6.2.x, Spring Security 6.5.x, spring-kafka 3.3.x, micrometer 1.15.x, **kafka-clients
  3.9.2**. Full suite green with zero test changes.
- **Java package renamed** `com.ritikbansod.kafkawrapper` → `com.ritikbansod.kview` (main
  class `KviewApplication`) — the project is now uniformly Kview; no behavior change, and
  clone folders can be renamed to `kview` too.

## 6. Upgrading from v1.0.0

- **Nothing breaks by default**: authentication and the safety switches are off, the config
  surface is additive, and the default-mode behavior is regression-tested.
- New optional env vars: `KVIEW_AUTH_MODE`, `KVIEW_AUTH_TOKENS`, `KVIEW_AUTH_ALLOWED_ORIGINS`,
  `KVIEW_READONLY`, `KVIEW_AUDIT`, `KVIEW_METRICS_INTERVAL_MS`.
- The legacy `kview-cli.js` is gone — `cli/kview.mjs` (thin client) and the jar's direct mode
  are the two supported CLIs.
- Docker users: image available from GHCR (as before) and Docker Hub (once release secrets
  are configured).

## 7. Quality statement

- Every one of the 18 issues was **independently reproduced** before being fixed, and every
  feature was **feasibility-checked against the code** by a dedicated reviewer.
- **66/66 tests green** across 18 classes on the ubuntu + windows CI matrix, including
  embedded-Kafka integration suites for browse, search, replay, protobuf, auth, audit,
  read-only mode, metrics and the direct CLI.
- The auth role matrix was additionally verified against the packaged jar over live HTTP,
  and the jpackage bundled-runtime launcher was smoke-tested with no system Java present.
- Not yet verified: a live Confluent Cloud/Strimzi registry round-trip for protobuf, and the
  packaging pipeline's first real tagged run (recommend `v1.0.1-rc.1`).

## 8. Remaining roadmap

- **Schema registry browser** (version diff, compatibility mode, upload dry-run) — the one
  feature candidate not yet built.
- Nested protobuf message indexes (rare subjects).
- `docs/RELEASING.md` secrets (`NPM_TOKEN`, `HOMEBREW_TAP_TOKEN`) to activate npm/Homebrew,
  and the SDKMAN submission after the first release.
- COMPATIBILITY matrix re-verification against live distributions on kafka-clients 3.9.2.

## 9. For maintainers

- Merge order: #1 trust fixes → #2 auth → #3 safety pack → #4 search → #5 replay →
  #6 metrics → #7 Boot 3.5 → #8 protobuf → #9 direct CLI → #10 packaging.
- Then: [RELEASING.md](RELEASING.md) — bump the three versions, roll the CHANGELOG, tag.
