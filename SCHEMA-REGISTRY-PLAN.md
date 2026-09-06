# Schema Registry Integration — Deep Analysis & Full-Fledged Plan

> Goal: make the Kview **schema-aware** for *every* schema registry in the wild —
> Confluent Schema Registry, Apicurio Registry, AWS Glue, Azure Schema Registry, Redpanda,
> Karapace, Cloudera CDP — with first-class serialization/deserialization in the Data Explorer,
> a registry browser, and produce-time encoding/validation, all designed around great
> UX for users and a clean SPI for developers.

---

## 1. Executive summary

Today Kview reads and writes **raw bytes rendered as UTF-8 strings**. That is fine for
JSON/text payloads, but schema-registry-encoded messages are *(a)* prefixed with wire-format
headers and *(b)* usually Avro/Protobuf **binary** — so the Explorer shows mojibake or nothing
useful, and producing valid schema-encoded messages from the UI is impossible.

This plan adds a **Schema Registry layer** with three pillars:

1. **Decode anything** — a wire-format sniffer + pluggable registry adapters resolve the schema
   for every message and render it as readable JSON in the Explorer, Live Tail and message viewer
   (with raw-bytes fallback always available).
2. **Encode safely** — the produce form gains subject-aware encoding (Avro/Protobuf/JSON Schema),
   pre-flight compatibility validation, and correct wire-format headers for the connected registry.
3. **Registry browser** — subjects, versions, diffs, compatibility policies, references, search,
   upload — proxied through Kview so the UI never talks to the registry directly.

Built as an **SPI** (`SchemaRegistryAdapter`) so each registry product is an adapter; the first
release covers ~90% of the market via the *Confluent-compatible* adapter (Confluent SR/Cloud,
Redpanda, Karapace, Apicurio in compat mode, WarpStream…).

---

## 2. Current-state analysis (why this is needed)

| Area today | Gap |
|---|---|
| Browser/tail/produce use `StringDeserializer`/`StringSerializer` | Binary Avro/Protobuf payloads arrive as mojibake; producing schema-encoded data is impossible |
| No registry concept | Users must decode messages out-of-band (jq + schema-id lookups by hand) |
| Wire-format headers | The 5-byte Confluent header, 18-byte Glue header, ~36-byte Azure header and Apicurio variants corrupt the first characters of the "string" |
| Settings | No place to store registry URLs/credentials per cluster profile |

**Design consequence:** Kview needs (1) per-profile **registry attachments**, (2) a
**byte-sniffing decoding pipeline** that runs *after* fetching raw bytes, and (3) an **encoding
pipeline** on produce. Decoding must never depend on generated classes (no per-user codegen) —
it uses *generic* decoding: Avro `GenericDatumReader`, Protobuf `DynamicMessage`, plain JSON for
JSON-Schema.

---

## 3. Registry landscape analysis (everything available today)

### 3.1 Products and their shapes

| Registry | Vendor | REST API | Formats | Wire format on Kafka | Auth | Notes |
|---|---|---|---|---|---|---|
| **Confluent Schema Registry** | Confluent (OSS + Cloud) | `/subjects/{s}/versions/{v}` | Avro, Protobuf, JSON Schema | magic `0x00` + 4-byte schema ID (Protobuf adds message-index array) | none / Basic / OAuth / mTLS | De-facto standard; subject strategies: TopicName/RecordName/TopicRecordName; compatibility modes incl. `_TRANSITIVE` |
| **Confluent Cloud SR** | Confluent | same as above | same | same | Basic (API key/secret) or OAuth bearer | Same adapter as #1 |
| **Redpanda Schema Registry** | Redpanda | Confluent-compatible API | Avro, Protobuf, JSON | Confluent-compatible | Basic | Covered by adapter #1 |
| **Karapace** (Aiven) | Aiven (OSS) | Confluent-compatible | Avro, Protobuf, JSON | Confluent-compatible | Basic | Covered by adapter #1 |
| **Apicurio Registry 2.x/3.x** | Red Hat / Apicurio (OSS) | Own v2/v3 API **+ Confluent-compat API** (`/apis/registry/v2`) | Avro, Protobuf, JSON, + AsyncAPI/OpenAPI/GraphQL/XML | 3.x default = **Confluent-compatible** (`0x0`+4B globalId); optional: globalId/contentId in **Kafka headers**, legacy magic `0x03`+8B id | none / Basic / OAuth / mTLS | IBM Event Streams ships it; convert-to-subject mapping differs from subjects |
| **AWS Glue Schema Registry** | AWS | AWS SDK (GSR API) | Avro, JSON, Protobuf | magic `0x03` + 1B compression flag + 16-byte schema-version **UUID** (18B header); newer serializers default to **Kafka headers** | IAM (keys/roles) | Also does validation-only mode (no wire header) |
| **Azure Schema Registry** (Event Hubs) | Microsoft | Azure SDK | Avro (JSON/Protobuf on roadmap) | 4-byte format marker + 32-byte schema **GUID** (~36B header) | Entra ID / SAS | Schema identity = group + name + version + GUID |
| **Cloudera CDP Schema Registry** (ex-Hortonworks) | Cloudera | Own REST API | Avro, JSON | proprietary | Kerberos/SAML | Niche; ANALYSIS-tier adapter |
| **WarpStream built-in SR** | WarpStream | Confluent-compatible | Avro/Protobuf/JSON | Confluent-compatible | Basic | Covered by adapter #1 |
| **Byte-oriented homebrew** | — | — | any | custom magic/headers | — | Detect & surface as "unknown binary" (never guess) |

### 3.2 Wire-format cheat sheet (what the sniffer must know)

| Magic prefix | Registry family | ID layout |
|---|---|---|
| `0x00` + 4B | Confluent-compatible (SR, Cloud, Redpanda, Karapace, Apicurio-compat) | numeric schema/global ID |
| `0x03` + 1B + 16B UUID | AWS Glue (payload mode) | schema-version UUID |
| 4B marker + 32B GUID | Azure Schema Registry | schema GUID |
| (headers only) | Glue headers mode, Apicurio headers mode | ID in message headers, payload clean |
| none / valid UTF-8 | plain JSON/text (fallback) | — |

Sniffing is **conservative**: on any ambiguity → return raw bytes with an "unrecognized binary"
badge. Never fabricate a decode.

### 3.3 Compatibility of semantics

| Capability | Confluent | Apicurio | Glue | Azure |
|---|---|---|---|---|
| Compatibility policies (BACKWARD/FORWARD/FULL ± TRANSITIVE) | ✔ | ✔ (per artifact) | ✔ (per registry, coarser) | ✖ (validation-only via Avro) |
| Subject naming strategies | ✔ 3 built-in | ✔ lookup strategies (topic/record + prefix) | ✖ (schema name) | ✖ (group+name) |
| References (nested schemas) | ✔ | ✔ (artifact references) | partial | ✖ (Avro-only flat) |
| Search / listing | ✔ | ✔ + richer metadata/labels | ✔ (list schema versions) | ✔ per group |

---

## 4. Target architecture

```
┌────────────┐   REST (proxied)   ┌──────────────────────────────┐   adapter   ┌──────────────┐
│  SPA (UI)  │ ─────────────────▶ │  Kview backend             │ ──────────▶ │ registry      │
│            │  /api/.../registry │  SchemaRegistryService       │  Confluent  │ Confluent SR  │
│            │                    │  CodecService (decode/encode)│  Apicurio   │ Apicurio      │
└────────────┘                    │  WireFormatSniffer           │  Glue       │ Glue (SDK)    │
       Kafka reads/writes         │  SchemaCache (TTL)           │  Azure      │ Azure (SDK)   │
────────────────────────────────▶ │  KafkaConsumer/Producer      │             └──────────────┘
```

Principles:

1. **UI never talks to the registry directly** — Kview proxies (kills CORS, hides secrets,
   applies timeouts, enables offline caching).
2. **Decode is a post-step.** The Kafka client keeps using the raw `ByteArrayDeserializer`
   for schema-bearing topics; Kview decodes bytes → JSON at API time (Explorer pagination
   stays lazy; each page decodes in O(page size) thanks to the schema cache).
3. **Adapter SPI** — one Java interface per concern; adding a registry = implement 5–7 methods +
   register via Spring config. No changes to UI/decode logic.

### 4.1 SPI sketch (developer contract)

```java
public interface SchemaRegistryAdapter {
    String type();                                   // "confluent" | "apicurio" | "glue" | "azure"
    void   test(SchemaRegistrySettings s);           // connectivity + auth probe
    List<SubjectRef> subjects(SchemaRegistrySettings s, String topicPrefix);
    List<SchemaVersion> versions(SchemaRegistrySettings s, SubjectRef subject);
    SchemaContent   schema(SchemaRegistrySettings s, SubjectRef ref, int version); // + references
    int    register(SchemaRegistrySettings s, SubjectRef subject, SchemaContent schema, boolean normalize);
    CompatibilityReport compatibility(SchemaRegistrySettings s, SubjectRef subject, SchemaContent candidate);
    void   setCompatibility(SchemaRegistrySettings s, SubjectRef subject, String mode);
    ResolvedSchema byWireId(SchemaRegistrySettings s, WireId id); // numeric id / UUID / content-id
}
```

Adapters ship as Spring `@Configuration` beans; `ConfluentCompatibleAdapter` is the default
(covers Confluent SR/Cloud, Redpanda, Karapace, Apicurio-compat, WarpStream).

### 4.2 Decoding engine

- **Sniffer** (`WireFormatSniffer`): inspects first bytes + (optionally) message headers →
  `WireId` or `Unknown`.
- **Avro**: writer schema from registry; optional reader schema (latest-compatible or pinned
  version) → `GenericDatumReader` → JSON. Handles logical types (decimal/date/timestamp).
- **Protobuf**: descriptor set from registry → `DynamicMessage` → JSON mapping (well-known types
  handled per proto3 JSON mapping).
- **JSON Schema**: payload is JSON text → validate against schema (optional toggle), render as-is.
- **Key decoding** mirrors value decoding (keys can have their own subject).
- **Errors are data**: undecodable → `{"_kview":{"error":"…","wireFormat":"…"}}` + raw view,
  never a failed API call.

### 4.3 Encoding engine (produce path)

- User picks a subject (auto-suggested via topic name + strategy) and schema version (default latest).
- Input: JSON (for Avro rendered from the schema, or free JSON for JSON-Schema; for Protobuf a
  JSON representation mapped via `JsonFormat`) → **encode** to the registry's wire format with the
  right magic/ID (or headers-mode for Apicurio/Glue).
- **Pre-flight validation**: compatibility test against the subject's latest version; incompatible
  payloads are blocked with the registry's incompatibility explanation (HTTP 409 + details).
- **Auto-register toggle** (Confluent `auto.register.schemas` equivalent) — off by default.

### 4.4 Schema cache

- In-memory `ConcurrentHashMap<CacheKey, CachedSchema>` keyed by (registry, wireId/subject+version);
  TTL 5 min; LRU cap (e.g. 2 000 entries); explicit invalidation on registry mutations through the
  Kview; `?refresh=true` on endpoints; stats exposed in UI (hit rate, entries).
- Decoding of a tail stream hits the cache after the first record per schema → O(1).

---

## 5. Feature catalogue ("everything possible")

### 5.1 Read path (consume/visualize)
1. Automatic wire-format detection per message (sniffer + headers).
2. Decode Avro/Protobuf/JSON-Schema to readable JSON with logical types.
3. Raw-bytes view + hex view toggle (always available).
4. Schema context on every decoded message: registry, subject, version, ID, compatibility mode,
   link to schema, link to previous version diff.
5. Reader-schema selector: "latest compatible" or pinned version (simulate consumer evolution).
6. Key decoding with its own subject.
7. Live-tail decoding with schema cache (no per-record registry calls).
8. Graceful degradation: unknown wire format / registry down → raw + warning badge (Explorer never
   fails because a registry is down).
9. Batch decode stats on browse pages: "48/50 decoded (Avro), 2 unknown".

### 5.2 Write path (produce)
10. Subject-aware produce form: subject dropdown (filtered by topic strategy), version picker.
11. One-click sample payload generation from the schema (Avro/Proto defaults, respect defaults
    and logical types).
12. Inline schema-aware validation before send (required fields, enums, patterns) + registry
    compatibility pre-check with human-readable incompatibility reasons.
13. Correct wire-format encoding per connected registry (including Apicurio/Glue headers modes).
14. Key-schema support (encode keys too).
15. Auto-register toggle + dry-run register (register schema version without producing).
16. Reproduce-message integration: reproducing a decoded message re-encodes losslessly.

### 5.3 Registry browser (management)
17. Subjects list: search, filter by format/strategy, compatibility mode badges, message counts
    (join with broker data: which topics use which subject).
18. Version timeline per subject with **visual diff** between any two versions (JSON diff for
    Avro/JSON; descriptor diff for Protobuf) and breaking-change highlighting.
19. Compatibility mode viewer + editor (where supported).
20. References tree (nested Avro / proto imports) with navigation.
21. Schema upload: paste schema + choose subject + auto compatibility pre-check + register
    (dry-run first, then real).
22. Export: subject/version schema download, full-subject JSON export.
23. Registry health & connectivity panel: latency, auth status, cache stats.

### 5.4 Cross-cutting
24. Per-cluster registry attachment (0..n registries per Kafka cluster — a cluster can have
    Confluent SR *and* a Glue registry for different topics; the sniffer resolves per message).
25. Registry settings stored with profiles; secrets masked in API responses (same policy as connections).
26. Metrics: decode success rate, registry call count/latency, cache hit rate (actuator + UI panel).
27. Accessibility & theming: all new UI built on the existing token system (dark/light).

---

## 6. API design (new endpoints, all under the existing patterns)

```
# registry attachment management (stored per cluster profile)
PUT    /api/clusters/{id}/registry                    # attach/update settings (type, url, auth…)
GET    /api/clusters/{id}/registry                    # masked view + health
DELETE /api/clusters/{id}/registry

# browsing (proxied)
GET    /api/clusters/{id}/registry/subjects?format=&q=
GET    /api/clusters/{id}/registry/subjects/{subject}/versions
GET    /api/clusters/{id}/registry/subjects/{subject}/versions/{v}        # schema + references
GET    /api/clusters/{id}/registry/subjects/{subject}/diff?v1=&v2=        # normalized diff
GET|PUT /api/clusters/{id}/registry/subjects/{subject}/compatibility
POST   /api/clusters/{id}/registry/subjects/{subject}/versions            # register (+dryRun)
POST   /api/clusters/{id}/registry/compatibility                          # pre-check candidate

# decode/encode helpers (used by explorer; also handy for humans)
POST   /api/clusters/{id}/decode                # bytes(base64) → {wireFormat, subject, version, json}
POST   /api/clusters/{id}/encode                # {subject, version?, json} → base64 wire bytes
```

Explorer/tail/produce pages get `?schema=on` semantics automatically when a registry is attached —
no client-side registry calls, ever.

---

## 7. UX design

### 7.1 Connections → "Schema registry" step
Third step in the connection wizard (after security): registry type cards
(Confluent-compatible / Apicurio / AWS Glue / Azure / None), URL, auth fields (per type),
**Test registry** button (validates + shows version string and latency), and a note when the
registry is optional ("you can attach it later; topics just show raw until then").

### 7.2 Data Explorer
- New **codec chip** per message: `Avro · orders-value v4` (click → schema), `Raw` (unknown).
- Message viewer gains tabs: **Decoded** (default) / **Raw** / **Hex** / **Schema**.
- Reader-schema selector in the toolbar (dropdown of compatible versions) with
  "simulate consumer" caption.
- Produce tab: subject dropdown (grouped by strategy), version picker,
  **"Generate sample"** and **"Validate"** buttons beside Send; validation panel lists
  issues in plain language ("field `amount` must be a decimal(12,2)").

### 7.3 Schemas page (new sidebar entry)
- Master–detail: subjects list (search, format badges, compat badges) → version timeline
  → schema view (pretty + raw tabs), diff slider between versions, references tree.
- Actions: register new version (upload), edit compatibility, export, (guarded) delete.

### 7.4 Language & states
- Registry down → amber banner in schema-dependent widgets: "Schema registry unreachable —
  showing raw messages", never red errors in the Explorer.
- Loading = skeletons (consistent with existing design system); empty states explain how to attach
  a registry.

---

## 8. Developer experience

- **One-jar run**: adapters are regular Maven deps of Kview; enabling = attach a registry in
  the UI. No codegen, no restart for schema changes (cache TTL), optional reload endpoint.
- **Adding an adapter** = implement the SPI (7 methods), add a `@Bean`, add a wire-format rule to
  the sniffer, add fixtures. Target: a competent dev ships a new adapter in ≤ 1 day; the doc
  includes a worked example.
- **Local development**: `Testcontainers` modules exist for Confluent SR and Apicurio; a
  `docker-compose.registry.yml` profile boots Confluent SR + Apicurio + a registry-enabled broker
  for manual QA. AWS Glue/Azure are covered with SDK contract tests + LocalStack/Azurite where
  feasible.
- **OpenAPI**: new endpoints documented in the same style as the rest of the API (examples included
  in the doc).
- **Configuration as code**: registry settings can also come from `application.yml`
  (`kview.registries.<name>`) for IaC-driven setups.

---

## 9. Data model & storage

```
SchemaRegistrySettings {
  type: CONFLUENT | APICURIO | GLUE | AZURE,
  url, authType: NONE|BASIC|BEARER|MTLS|IAM,
  username?, password?, bearerTokenUrl?, clientId?, clientSecret?, scope?,
  keystore/truststore (PEM or file, same shape as broker TLS),
  awsRegion?, azureFullyQualifiedNamespace?, azureSchemaGroup?,
  cacheTtlSeconds?, requestTimeoutMs?
}
```
Stored inside the existing `ConnectionProfile` (per Kafka cluster) *or* standalone and referenced —
profiles support both ("attach shared registry"). Persisted in the same atomically-written
connections file; secrets masked in responses.

---

## 10. Testing strategy

| Layer | What | Tooling |
|---|---|---|
| Unit | sniffer fixture table (every magic/headers case), strategy resolution, Avro/Proto/JSON codecs incl. logical types, cache TTL/LRU | JUnit + fixture files |
| Adapter | REST contract tests per adapter against recorded responses (WireMock) + live Testcontainers for Confluent SR & Apicurio | Testcontainers, WireMock |
| Integration | full loop: register schema → produce encoded (via Kview) → browse decoded (via Kview) → schema evolution case (backward-compatible + breaking) | embedded Kafka + Testcontainers SR |
| Negative | registry down mid-browse, 401/403, huge schema, reference cycles, malformed candidate schema, wrong-CA TLS to registry | contract tests |
| QA suite | extend `qa-negative-test.mjs` + new `verify-schema-registry.mjs` (21-style checks per adapter) | node scripts |

Acceptance gates per phase are listed in the roadmap; "done" always includes fixtures + a UI pass
in both themes + docs updates (README + this file's matrix moving rows from PLAN → LIVE).

---

## 11. Phased roadmap

| Phase | Scope | Size (dev-days) | Acceptance criteria |
|---|---|---|---|
| **0. Foundations** ✅ **DONE** | ByteArray pipeline for explorer/tail/produce (behind the scenes), wire-format sniffer with fixtures, "unknown binary" badges, settings model | done | ✅ E2E/QA suites green; byte pipeline live; unknown formats render raw+badge |
| **1. Confluent-compatible read path** ✅ **DONE (LIVE-VERIFIED)** | Confluent adapter (subjects/versions/schema/byWireId), Avro+JSON decode, schema cache, Explorer decode + codec chips, `/decode` endpoint | done | ✅ Live-verified against local broker + mock registry: Avro messages decode (`orders-value` id 1 → JSON), codec chips, Decoded/Raw/Hex tabs, registry test+attach via UI/API (`scripts/verify-compatibility.mjs`, `SchemaDecodeIntegrationTest`) |
| **2. Protobuf + write path** | Protobuf decode (DynamicMessage), produce encoding for all 3 formats, subject/version picker, pre-flight compatibility, key schemas, sample generator | 8–10 | Round-trip: encode via UI → decode via UI for all formats; incompatible produce blocked with reason |
| **3. Registry browser** | Subjects/versions/diff/compat mode/references/upload/export endpoints + Schemas page UI | 8–10 | All §5.3 features in both themes; upload with dry-run; diff shows breaking changes |
| **4. Apicurio native** | v2/v3 adapter, headers-mode & legacy wire formats, convert-to-subject mapping, lookup strategies | 5–6 | Apicurio Testcontainers flow parity with phase 1–2 features |
| **5. Cloud registries** | AWS Glue adapter (IAM, payload+headers modes, compression flag), Azure adapter (GUID schema resolution, group model) | 6–8 | LocalStack/emsurier-style fixtures + contract tests; documented credentials setup |
| **6. Polish** | Metrics panel, shared registries, config-as-code, perf tuning (cache hit-rate > 95% on tails) | 3–4 | QA suites extended; docs/matrix updated |

Total ≈ 39–50 dev-days for the full scope; **phase 1 alone (read path for the ~90% case) is a
6–8 day slice** that delivers most user value.

---

## 12. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Ambiguous wire formats (same magic, different registry) | Sniffer attaches per *configured* registry for the cluster; multiple attachments resolved by trial decode + score; ambiguous → raw + badge |
| Avro references cycles / deep nesting | Reference resolver with depth limit + cycle detection; failure = raw view |
| Registry latency on tails | Schema cache (TTL+LRU); registry calls only on new schema IDs |
| Protobuf JSON mapping edge cases (well-known types) | Use official `proto3` JSON mapping via `JsonFormat`; fixtures for common WKT |
| Vendor API drift (Apicurio v3, Glue changes) | Contract tests against recorded responses + Testcontainers matrix in CI |
| Secrets in registry settings | Same masking/keep-on-edit policy as broker connections |
| Large payloads | Decode guarded by size cap (configurable); hex view chunked |

---

## 13. Out of scope (explicitly)

- Schema **authoring/editing IDE** (visual schema editor) — the plan covers viewing, diffing,
  uploading and validating; a full editor is a separate product surface.
- Kafka Connect / Streams serde configuration management.
- Migration tooling between registries (possible follow-up: export/import bundle).

---

## 14. Sources (wire-format research)

- [Apicurio Registry 3.3.x — configuring Kafka SerDes](https://www.apicur.io/registry/docs/apicurio-registry/3.3.x/getting-started/assembly-configuring-kafka-client-serdes.html) (globalId/contentId placement, header modes)
- [Apicurio Registry 2.4 SerDes guide (Red Hat)](https://docs.redhat.com/en/documentation/red_hat_build_of_apicurio_registry/2.4/html/apicurio_registry_user_guide/configuring-kafka-client-serdes_registry)
- [Headers vs magic byte — Croz](https://croz.net/headers-vs-magic-byte-schema-registry/)
- [Demystifying Confluent's Schema Registry wire format](https://dev.to/stevenjdh/demystifying-confluents-schema-registry-wire-format-5465)
- [Confluent Cloud SerDes overview](https://docs.confluent.io/cloud/current/sr/fundamentals/serdes-develop/index.html)
- [The Glue Schema that binds Apache Kafka — Lenses](https://lenses.io/blog/aws_glue_schema_registry_that_binds_apache_kafka) (18-byte header, `0x03`, UUID)
- [awslabs/aws-glue-schema-registry](https://github.com/awslabs/aws-glue-schema-registry) · [AWS Glue SR integrations](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations.html)
- [Azure Schema Registry Avro serializer (Microsoft Learn)](https://learn.microsoft.com/en-us/java/api/overview/azure/data-schemaregistry-avro-readme?view=azure-java-preview) · [Azure schema-registry-for-kafka](https://github.com/Azure/azure-schema-registry-for-kafka/) · [Azure Event Hubs Schema Registry concepts](https://learn.microsoft.com/en-us/azure/event-hubs/schema-registry-concepts)
