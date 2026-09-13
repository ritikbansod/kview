# Changelog

All notable changes to Kview will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

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
