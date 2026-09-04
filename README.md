# Kafka Wrapper

A REST-based abstraction layer over Apache Kafka. Manage topics, produce messages,
and run dynamic consumers through plain HTTP calls — no Kafka CLI, no restarts.

## Why

Teams integrating with Kafka repeatedly need the same chores: create a topic, send a
test payload, peek at what a consumer group receives. This service exposes those
chores as a small, standard REST API so any service (or human with curl) can use
Kafka without installing CLI tooling or embedding client libraries.

## API

| Method | Path | Purpose |
|--------|------|---------|
| POST   | `/api/topics`                 | Create a topic (partitions + replication factor) |
| GET    | `/api/topics`                 | List topics |
| GET    | `/api/topics/{name}`          | Describe a topic |
| DELETE | `/api/topics/{name}`          | Delete a topic |
| POST   | `/api/messages`               | Produce a message (returns partition + offset) |
| POST   | `/api/consumers`              | Start a dynamic consumer (`groupId` + `topic`) |
| DELETE | `/api/consumers/{groupId}/{topic}` | Stop a dynamic consumer |
| GET    | `/api/consumers/{groupId}/messages` | Messages received by that group (last 100) |
| GET    | `/api/consumers`              | Running consumer containers |

## Quick start

```bash
# 1. Start Kafka (KRaft, single node)
docker compose up -d

# 2. Run the service
mvn spring-boot:run
```

### Try it

```bash
# Create a topic
curl -X POST localhost:8090/api/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "orders", "partitions": 3, "replicationFactor": 1}'

# Start a consumer
curl -X POST localhost:8090/api/consumers \
  -H "Content-Type: application/json" \
  -d '{"groupId": "demo-group", "topic": "orders"}'

# Produce a message
curl -X POST localhost:8090/api/messages \
  -H "Content-Type: application/json" \
  -d '{"topic": "orders", "key": "order-1", "payload": "{\"id\": 1}"}'

# Read what the consumer received
curl localhost:8090/api/consumers/demo-group/messages
```

## Tech

- Java 21, Spring Boot 3.3, spring-kafka
- `KafkaAdmin` for topic lifecycle
- Runtime-registered `ConcurrentMessageListenerContainer` instances for dynamic consumers
- In-memory receive buffer (last 100 messages per group) — this is a developer/tooling
  service, not a replacement for real consumer applications

## Ideas welcome

- Schema Registry integration for payload validation
- Authentication for management endpoints
- Metrics export (consumer lag, produce rate)
