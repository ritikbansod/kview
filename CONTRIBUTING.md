# Contributing to Kview

Thank you for your interest in contributing to Kview! We welcome bug reports, feature requests, docs improvements, and code contributions.

---

## Code of Conduct

All contributors are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md) in all interactions.

---

## Development Setup

### Prerequisites

- **Java 21** or later (Eclipse Temurin or OpenJDK recommended)
- **Node.js 18+** (for CLI, MCP server, and QA test scripts)
- (Optional) Docker for running local Kafka brokers

No Maven install needed — the repo ships the [Maven wrapper](https://maven.apache.org/wrapper/):
use `./mvnw` (Linux/macOS) or `mvnw.cmd` (Windows). A pre-installed Maven 3.9+ works too.

### Building from Source

Clone the repository:

```bash
git clone https://github.com/ritikbansod/kview.git
cd kview
```text

Build the executable JAR:

```bash
./mvnw clean package
```text

Run Kview locally:

```bash
java -jar target/kview-1.1.0.jar
```text

Then open `http://localhost:8090` in your browser.

### Running a Local Test Kafka Broker

If you don't have an existing Kafka cluster, start a local KRaft broker with:

```bash
docker compose up -d kafka
```text

### Project Structure

```text
src/main/java/com/ritikbansod/kview/   Spring Boot backend, one package per feature:
                                       auth, browse, cluster, cli, connection, consumer,
                                       group, history, live, message, metrics, replay,
                                       schema, topic, web
src/main/resources/static/             the web UI (vanilla JS, no build step)
src/test/java/                         unit + embedded-Kafka integration suites
cli/kview.mjs                          thin-client CLI (Node 18+, zero dependencies)
mcp/kview-mcp.js                       MCP server for AI assistants
docs/                                  release process, plans, historical reports
```text

For quick manual verification without the UI, use the direct-mode CLI:
`java -jar target/kview-1.1.0.jar --cli topics --bootstrap-server localhost:9092`.

> Found a security vulnerability? Please **do not open a public issue** — see
> [SECURITY.md](SECURITY.md) for the private reporting process.

---

## Testing & Verification

Before submitting a Pull Request, please ensure all automated test suites pass.

### 1. Java Unit & Embedded Integration Tests

```bash
./mvnw test
```text

### 2. End-to-End Functional Test Suite

With Kview running on `http://localhost:8090` and Kafka on `localhost:9092`:

```bash
node scripts/e2e-test.mjs
```text

### 3. Negative & Boundary Testing

```bash
node scripts/qa-negative-test.mjs
```text

### 4. MCP Server Smoke Test

```bash
node scripts/mcp-smoke-test.mjs
```text

---

## Pull Request Guidelines

1. **Keep PRs focused**: Each PR should address a single feature or bug fix.
2. **Preserve backward compatibility**: Ensure connection profile formats and existing REST endpoints remain compatible.
3. **Add tests**: Add unit tests or test script coverage for new endpoints or features.
4. **Follow code style**: Keep code readable, concise, and preserve existing naming conventions.
