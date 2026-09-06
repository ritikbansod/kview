# Kview

Kview is a web UI and REST API for Apache Kafka. Point it at a cluster and you can browse topics and
messages, tail a topic live, check consumer group lag, produce test messages and manage
topics, all from the browser. Any number of clusters can be added at runtime and you can
switch between them from the sidebar.

It is built on the official apache kafka java client, so it works with any distribution
that speaks the kafka protocol: Apache Kafka 2.1 to 4.x, Strimzi, Confluent Cloud,
Confluent Platform, Redpanda, Amazon MSK, Azure Event Hubs, Aiven etc. Security support
covers PLAINTEXT, TLS, mTLS, SASL (PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512) and OAUTHBEARER.

## Getting started

Needs Java 21 and Maven.

```
mvn package
java -jar target/kview-2.0.0.jar
```

Then open http://localhost:8090. By default it connects to localhost:9092, set
`KAFKA_BOOTSTRAP_SERVERS` to point somewhere else.

If you don't have a broker handy, `docker-compose.yml` starts a single node KRaft broker:

```
docker compose up -d
```

The app also starts fine when no broker is reachable, pages just show a
"cluster unreachable" message until one answers.

## What it can do

- **Dashboard**: brokers (with full broker config viewer), controller, topic and
  partition counts, under replicated / offline partitions, messages per topic chart,
  ACLs. Optional 10s auto refresh.
- **Topics**: partitions, leaders, replicas, ISR, begin/end offsets, message counts and
  configs for every topic. Create and delete topics, increase partitions, edit configs.
- **Data explorer**: read messages from any topic (latest N, exact offsets, from a
  timestamp, filter by key or value contains), pretty printed JSON with a copy button,
  and a reproduce button that re-sends a message to any topic. Live tail over SSE if you
  want to watch records arrive in real time.
- **Produce**: key, value, partition, timestamp and headers.
- **Consumer groups**: state, total and per partition lag with lag bars, member
  assignments, delete a group, reset offsets (earliest / latest / exact offset /
  timestamp).
- **Connections**: add, edit, test and reconnect clusters at runtime.

Browsing and tailing are read only: they don't commit offsets and they never join a real
consumer group, so it's safe to point this at production topics.

The UI has a dark and light theme (follows your system setting), and works fine on
smaller screens.

## Connecting to a cluster

Go to Connections -> Add cluster (or `POST /api/clusters`). What you need for each setup:

| Setup | Fields |
|---|---|
| local dev cluster | protocol `PLAINTEXT`, bootstrap servers |
| TLS | truststore as file or pasted PEM, hostname verification toggle |
| mTLS (e.g. Strimzi) | CA PEM plus client certificate and key (PEM or keystore) |
| SASL | `PLAIN` or `SCRAM-SHA-256/512` with username/password. Confluent Cloud is PLAIN with the API key as username |
| OAuth2 | token endpoint URL, client id, secret, optional scope. Tokens are fetched and refreshed automatically (Keycloak, Entra ID, Okta etc) |

Secrets are stored in `data/connections.json` on the server and are never sent back to
the browser. The API returns masked values, and saving a masked value keeps the stored
secret.

COMPATIBILITY.md has the exact settings per distribution, including the Confluent Cloud
API key shape, the Azure Event Hubs connection string trick and Strimzi mTLS.

## REST API

There is also a JSON API that mirrors everything the UI does (produce, browse, tail,
topic and group management), mounted under `/api/clusters/{clusterId}/...`, with a
connection tester at `POST /api/clusters/test`. Health check at `/actuator/health`.

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | bootstrap servers of the default cluster |
| `KVIEW_DATA_DIR` | `./data` | where connections.json is stored |
| `server.port` (application.yml) | `8090` | http port |

## Roadmap

- Schema Registry support, design in [SCHEMA-REGISTRY-PLAN.md](SCHEMA-REGISTRY-PLAN.md)
- Message replay: copy a filtered set of messages to another topic or cluster
- Prometheus metrics for lag and produce rate
- Authentication for the wrapper API itself

## Contributing

Bug reports and PRs are welcome. If you want to check compatibility against a
distribution, `node scripts/verify-compatibility.mjs` re-runs the whole verification
matrix against a local broker, see COMPATIBILITY.md for what it covers.

## License

TODO: pick a license before the first public release, probably Apache-2.0.
