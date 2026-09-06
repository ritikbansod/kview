# Compatibility — Kafka distributions, versions and security shapes

**Question this document answers:** *does the Kview work with every sort of cluster — Apache Kafka, Strimzi, Confluent, and the rest?*

**Short answer:** yes for every cluster that speaks the Apache Kafka protocol. Kview is built strictly on the official Apache `kafka-clients` Java library — the very same client code that Strimzi, Confluent Platform, Redpanda, MSK and Event Hubs clients use. Distribution differences are never at the protocol level; they are only in **security configuration shapes** and optional add-ons (Schema Registry, MDS, IAM). Those shapes are exactly what Kview's connection profiles model.

---

## 1. Why it is distribution-agnostic (architecture-level analysis)

| Layer | What Kview uses | Why it is portable |
|---|---|---|
| Protocol | `kafka-clients` (Apache, Apache-2.0) | Implements the official Kafka wire protocol with runtime `ApiVersions` negotiation — it adapts itself to whatever the broker announces. Strimzi, Confluent Platform, Confluent Cloud, Redpanda, MSK and Event Hubs all serve this same protocol. |
| Version range | kafka-clients **3.7.1** | Clients negotiate down: officially compatible with brokers **2.1 → 4.x**. Live-verified in this repo against brokers 3.7.0, 3.7.1 **and** 4.3.1 (cross-major-version, newer broker than client). |
| Security | `KafkaClientPropertiesFactory` → raw Kafka `ssl.*` / `sasl.*` properties | Covers every standard shape: PLAINTEXT, TLS (keystore files **or pasted PEM**), client-cert mTLS, SASL PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 / OAUTHBEARER, hostname-verification toggle. |
| Authentication extras | `HttpOAuthBearerLoginCallbackHandler` | OAuth2 **client-credentials** against any OIDC provider (Keycloak, Entra ID, Okta, Auth0, Confluent Cloud OAuth) — tokens fetched and refreshed automatically, no Kafka-version-specific server code involved. |
| Metadata/admin | `AdminClient` | Same portability as above: topic/config/group/ACL APIs exist on every distribution. |

> A Kafka "distribution" is Apache Kafka plus packaging (operator, cloud control plane) and optional extras. None of the extras change the wire protocol, so a tool that speaks the protocol with configurable security speaks to all of them.

---

## 2. Verification matrix

Legend: **LIVE** = verified end-to-end against a real cluster in this repo · **SHAPE** = the distribution's exact security/config shape verified end-to-end against a real broker (distributions only re-package these shapes) · **ANALYSIS** = reasoning + extension point, no live cluster available.

| # | Distribution / flavor | Typical connection shape | Status | Evidence |
|---|---|---|---|---|
| 1 | **Apache Kafka 4.3** (KRaft) | PLAINTEXT `:9092` | **LIVE** | Daily-dev broker in this repo; 79/79 E2E + 35/35 negative QA + 21/21 compat checks |
| 2 | **Apache Kafka 3.7** (docker image) | PLAINTEXT | **LIVE** | Verified during development (produce/browse/overview) |
| 3 | **Apache Kafka 2.1 – 3.x** | any | ANALYSIS | Client 3.7.x is officially wire-compatible with brokers 2.1+ (Apache compatibility policy); no Kview code pins API versions |
| 4 | **Strimzi (Kubernetes) — mTLS** | `SSL` + client certificate + custom CA (often hostname-verification off) | **LIVE (shape)** | `.kafka` broker with `ssl.client.auth=required`; Kview profile with **pasted PEM** client cert + PEM CA; full produce/browse/overview flow — `scripts/verify-compatibility.mjs` shape 1 |
| 5 | **Strimzi — SCRAM-SHA-512 over TLS** | `SASL_SSL` + SCRAM-SHA-512 + custom CA | **LIVE (shape)** | `SCRAM://:9094` listener, SCRAM credential for user `wrapper`; shape 2 of the script |
| 6 | **Confluent Cloud** | `SASL_SSL` + `PLAIN` (API key as username, secret as password), TLS with Confluent CA | **LIVE (shape)** | `CLOUD://:9095` listener replicating the Cloud auth shape; shape 3 of the script. For real Cloud: bootstrap `<cluster>.<region>.azure.confluent.cloud:9092`, username = API key |
| 7 | **Confluent Platform (self-hosted)** | Same as Apache + optional SASL/OAUTH | **SHAPE** | Identical client config to shapes above; Confluent extras (MDS, Schema Registry ACLs) are control-plane features Kview does not need |
| 8 | **Redpanda** | `SASL_SSL` SCRAM/PLAIN or mTLS | **SHAPE** | Redpanda implements the Kafka protocol + standard SASL; same profile shapes |
| 9 | **Amazon MSK** | `SASL_SSL` SCRAM or TLS (IAM optional) | **SHAPE** | SCRAM-over-TLS verified (shape 2). IAM auth needs Amazon's `aws-msk-iam-auth` jar + its callback handler — see extension points |
| 10 | **Azure Event Hubs (Kafka endpoint)** | `SASL_SSL` + `PLAIN`, username = literally `$ConnectionString` | **SHAPE** | PLAIN-over-TLS verified; set username to the connection string, password = key |
| 11 | **Aiven / Instaclustr / Upstash / Scaleway …** | `SASL_SSL` SCRAM/PLAIN + provider CA (PEM) | **SHAPE** | Same two ingredients as shapes 2/3 |
| 12 | **OAuth2 / OIDC-secured clusters** (Keycloak, Entra ID, Okta, Confluent OAuth) | `SASL_SSL` + `OAUTHBEARER` | **ANALYSIS + unit-LIVE** | Client-credentials flow verified against a real HTTP token endpoint (`HttpOAuthBearerLoginCallbackHandlerTest`); broker-side OAuth config is distribution-specific (Strimzi/Confluent provide operators) |

**Negative/security guarantees verified live:** connections *without* the required client certificate are refused; wrong SCRAM passwords are refused; untrusted CAs are refused — each with a clear error surfaced through Kview's API/UI.

---

## 3. How to point Kview at each distribution

Create the profile in **Connections → Add cluster** (or `POST /api/clusters`). Fields per shape:

| Shape | Fields |
|---|---|
| Unsecured dev cluster | protocol `PLAINTEXT`, bootstrap servers |
| TLS (server auth only) | protocol `SSL`, trust = CA PEM (or truststore file), hostname verification on |
| mTLS (Strimzi style) | protocol `SSL`, trust = CA PEM, client cert = PEM chain + key (or keystore file), optionally hostname verification off for the Strimzi broker cert |
| SCRAM over TLS (Strimzi/MSK/Aiven) | protocol `SASL_SSL`, mechanism `SCRAM-SHA-256` or `SCRAM-SHA-512`, username/password, trust = CA PEM |
| Confluent Cloud | protocol `SASL_SSL`, mechanism `PLAIN`, username = API key, password = API secret, trust = system default (Confluent CA is public) |
| Azure Event Hubs | protocol `SASL_SSL`, mechanism `PLAIN`, username `$ConnectionString`, password = the connection string |
| OAuth2 (OIDC) | protocol `SASL_SSL`, mechanism `OAUTHBEARER`, token URL + client id + secret (+ scope) |

## 4. Extension points (things that are NOT plain Kafka)

| Add-on | Status |
|---|---|
| **AWS MSK IAM** | Not built in (requires Amazon's proprietary `aws-msk-iam-auth` SASL mechanism). Extension point: drop the jar in, add a mechanism option mapped to `AWS_MSK_IAM` + its callback handler class. |
| **Schema Registry (Confluent/Apicurio)** | Not needed for raw-message visualization; Avro/Protobuf decoding is on the roadmap. |
| **Confluent MDS / RBAC** | Control-plane auth for Confluent tooling — unrelated to the data-plane protocol Kview uses. |
| **Brokers older than 2.1** | Not supported by modern Apache clients at all (EOL). |

## 5. Reproducing the verification

```bash
# certificates + secure listeners (see appendix) then:
node scripts/verify-compatibility.mjs
# → 21 checks: mTLS / SCRAM-512 / PLAIN shapes × (test, save, create, produce, browse, overview)
#   + 3 negative cases (no client cert, wrong password, rogue CA)
```

### Appendix — secure local broker config used for the verification

Listeners (single KRaft node, `.kafka/kafka/server.properties`):

```
listeners=BROKER://:9092,TLS://:9093,SCRAM://:9094,CLOUD://:9095,CONTROLLER://:19099
listener.security.protocol.map=BROKER:PLAINTEXT,TLS:SSL,SCRAM:SASL_SSL,CLOUD:SASL_SSL,CONTROLLER:PLAINTEXT
ssl.client.auth=none
listener.name.tls.ssl.client.auth=required
listener.name.scram.sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.scram.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin-secret";
listener.name.cloud.sasl.enabled.mechanisms=PLAIN
listener.name.cloud.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret" user_wrapper="wrappier-secret";
```

Certificates: local CA (`ca.pem`), server keystore `server.keystore.p12` with `subjectAltName=DNS:localhost,IP:127.0.0.1`, client cert `client.pem`/`client.key` signed by the same CA. SCRAM user: `wrapper` / `wrappier-secret` (SCRAM-SHA-512, created via `kafka.admin.ConfigCommand`).

> Kafka 4.x note learned during this verification: **server-side** per-listener JAAS must be mechanism-prefixed — `listener.name.<listener>.<mechanism>.sasl.jaas.config` (the un-prefixed `listener.name.<listener>.sasl.jaas.config` is ignored with a warning). Client-side profiles in Kview are unaffected.
