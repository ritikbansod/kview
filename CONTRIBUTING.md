# Contributing to Kview

Thank you for your interest in contributing to Kview! We welcome bug reports, feature requests, docs improvements, and code contributions.

---

## Code of Conduct

All contributors are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md) in all interactions.

---

## Development Setup

### Prerequisites
- **Java 21** or later (Eclipse Temurin or OpenJDK recommended)
- **Maven 3.9+**
- **Node.js 18+** (for CLI, MCP server, and QA test scripts)
- (Optional) Docker for running local Kafka brokers

### Building from Source

Clone the repository:
```bash
git clone https://github.com/ritikbansod/kview.git
cd kview
```

Build the executable JAR:
```bash
mvn clean package
```

Run Kview locally:
```bash
java -jar target/kview-1.0.0.jar
```
Then open `http://localhost:8090` in your browser.

### Running a Local Test Kafka Broker
If you don't have an existing Kafka cluster, start a local KRaft broker with:
```bash
docker compose up -d kafka
```

---

## Testing & Verification

Before submitting a Pull Request, please ensure all automated test suites pass.

### 1. Java Unit & Embedded Integration Tests
```bash
mvn test
```

### 2. End-to-End Functional Test Suite
With Kview running on `http://localhost:8090` and Kafka on `localhost:9092`:
```bash
node scripts/e2e-test.mjs
```

### 3. Negative & Boundary Testing
```bash
node scripts/qa-negative-test.mjs
```

### 4. MCP Server Smoke Test
```bash
node scripts/mcp-smoke-test.mjs
```

---

## Pull Request Guidelines

1. **Keep PRs focused**: Each PR should address a single feature or bug fix.
2. **Preserve backward compatibility**: Ensure connection profile formats and existing REST endpoints remain compatible.
3. **Add tests**: Add unit tests or test script coverage for new endpoints or features.
4. **Follow code style**: Keep code readable, concise, and preserve existing naming conventions.
