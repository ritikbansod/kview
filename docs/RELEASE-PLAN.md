# Kview — First Public Release Analysis & Runbook

**Date:** 2026-09-12 · **Scope:** what it takes to cut v1 so users can actually *start using* Kview.
**Sync:** updated after `1eee3e7` ("docs: restructure readme around the three ways to use kview") —
screenshots are now embedded and the README is organized as Web UI / CLI / MCP; see §3 for what that closes.

---

## 1. Where the project stands (verified evidence)

| Area | State | Evidence |
|---|---|---|
| Build | ✅ Green | `mvn package` passes on Java 21.0.5 (run today), Spring Boot 3.3.4 |
| Unit + integration tests | ✅ 30/30 | `target/surefire-reports` — 8 classes incl. 2 embedded-Kafka suites (BrowserIntegrationTest, SchemaDecodeIntegrationTest) |
| E2E / negative QA | ✅ 79/79 + 35/35 | QA-REPORT.md (2026-09-06), 5 HIGH/MED bugs found & fixed, XSS escaping verified, 40-way parallel produce OK |
| MCP server | ✅ Works | `mcp/kview-mcp.js` + 25-check smoke test (`scripts/mcp-smoke-test.mjs`) |
| Docs | ✅ Good | README (178 lines, restructured in `1eee3e7`) opens with the dashboard screenshot and is organized around three usage paths — Web UI / CLI / MCP, each with real screenshot/terminal imagery; covers connections, config, roadmap; COMPATIBILITY.md per distribution |
| Screenshots | ✅ Embedded | `docs/images/`: logo.svg + ui-dashboard.png (hero), ui-explorer.png, cli-terminal.png, mcp-terminal.png — all five referenced in README |
| Git state | ✅ Clean | main == origin/main, 30 commits, working tree clean |
| GitHub repo | ⚠️ **Already public** | github.com/ritikbansod/kview — 0 stars, **no license, no tags, no releases** |
| Secrets hygiene | ✅ Clean | `data/` gitignored, never committed; code scan hits were test fixtures only |

**The core problem:** the code is publicly visible but not legally usable (no license = all rights
reserved) and not practically usable (no artifact to download — a user must install JDK 21 + Maven
and build from source). "Release" = closing exactly those two gaps, plus the plumbing that makes
the release repeatable.

---

## 2. Blockers — must fix before announcing anything (P0)

### B1. No LICENSE file
README still says "TODO: pick a license". The repo is already public, so right now nobody —
including companies evaluating it — may legally copy, modify, or run it beyond GitHub ToS.
**Fix:** add Apache-2.0 (matches README intent; all dependencies — Spring Boot, kafka-clients,
Avro, networknt json-schema-validator — are Apache-2.0, so no conflicts). Replace the README
License TODO section. GitHub auto-detects it and shows the license badge.

### B2. No runnable artifact / distribution channel
The only install path today is "install Java 21 + Maven, clone, build". That kills adoption for
a ops tool whose audience expects `docker run` or a jar download (compare AKHQ, kafka-ui,
Redpanda Console — all ship images + release binaries).
**Fix (two primary channels):**
1. **GitHub Release with the boot jar** (46 MB) + SHA256 checksums — users need only a JRE 21.
2. **Docker image on ghcr.io/ritikbansod/kview** — needs a Dockerfile (doesn't exist yet);
   `docker-compose.yml` currently only starts a *broker*, not Kview.

Note: even after the README restructure, the "Web UI" section still only offers
`mvn package && java -jar` — once the artifacts exist, add "download the release jar" and
"docker pull ghcr.io/ritikbansod/kview" as the first two options in that section.

```dockerfile
# Multi-stage: build once, ship a ~250MB JRE image
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/kview-*.jar app.jar
EXPOSE 8090
ENTRYPOINT ["java", "-jar", "app.jar"]
```

### B3. Version inconsistency — ✅ FIXED
Aligned to **1.0.0** across `pom.xml`, `README.md`, `cli/kview.mjs`, `mcp/kview-mcp.js`, `QA-REPORT.md`, and `src/main/resources/static/index.html`.

### B4. Security posture is undocumented and unsafe by default
The REST API has **no authentication** — anyone who can reach port 8090 can read every message,
produce, delete topics, and reset consumer offsets. `data/connections.json` stores cluster
credentials in plaintext on disk. This is fine for a local dev tool, but a new user who maps port
8090 on a public host will get burned, and the README only hints at it (Roadmap: "Authentication
for the wrapper API itself").
**Fix (minimum for v1):**
- Loud warning box at the top of README ("designed for localhost / trusted networks; put an
  authenticating reverse proxy in front for anything else").
- SECURITY.md with the same warning + how to report vulnerabilities.
- Consider defaulting to loopback: `server.address: ${KVIEW_BIND:127.0.0.1}` in application.yml
  (Docker image overrides with `KVIEW_BIND=0.0.0.0`). Cheap, prevents the worst foot-gun.

---

## 3. Should fix in the same release commit (P1)

~~Embed screenshots in README~~ — **done in `1eee3e7`**: logo hero, dashboard, explorer, CLI and
MCP terminal images are all embedded now.

| # | Item | Why |
|---|---|---|
| 1 | Extend the new "Three ways to use it" README with artifact install paths | The structure landed, but "1. Web UI" is still build-from-source only; add download-jar and docker-pull ahead of it once B2 ships |
| 2 | GitHub Actions CI (`.github/workflows/ci.yml`) | `mvn verify` on push/PR (ubuntu + windows, Java 21, m2 cache); badge in README; no CI = every future release is manual |
| 3 | Release workflow (`release.yml`) | On tag `v*`: build jar → checksums → GitHub Release → build & push ghcr image with same tag |
| 4 | Community files | SECURITY.md, CONTRIBUTING.md (promote the README prose), CODE_OF_CONDUCT.md, CHANGELOG.md (v1 entry), issue templates (bug/feature) |
| 5 | Repo settings | About description + link, Discussions on, license badge appears automatically |

---

## 4. Fast-follow, explicitly NOT blocking v1 (P2)

1. **npm package** for `kview-cli` + `kview-mcp` (`npx kview-mcp` is how the AI-assistant
   ecosystem consumes MCP servers; both files are zero-dependency so packaging is trivial).
2. **API auth** — at minimum a shared token / basic-auth option; biggest feature gap vs hosted tools.
3. **Spring Boot 3.3.4 is past OSS support end** — bump to the latest 3.5.x patch before or right
   after v1 (parent version bump + rerun the QA suites; also lifts kafka-clients to 3.9.x).
4. Prometheus metrics (already on roadmap), message replay (roadmap).
5. Homebrew tap / scoop manifest; demo video or GIF in README.

---

## 5. Release runbook

**Phase A — prep (≈1 day)**
1. LICENSE (Apache-2.0) + README license section.
2. Align versions → 1.0.0 everywhere (pom.xml, kview-cli.js, cli/kview.mjs, mcp/kview-mcp.js, README jar path).
3. Dockerfile + .dockerignore; add `kview` service to docker-compose.yml commented as optional.
4. README: screenshots ✅ done (`1eee3e7`) — remaining: artifact install paths (download jar /
   docker pull) at the top of "1. Web UI", plus the security warning box.
5. SECURITY.md, CONTRIBUTING.md, CODE_OF_CONDUCT.md, CHANGELOG.md, issue templates.
6. (Recommended) loopback bind default via `KVIEW_BIND`.

**Phase B — automation (≈half day)**
7. `ci.yml`: matrix build + tests on push/PR.
8. `release.yml`: tag-triggered jar + checksums + GHCR image + release notes from CHANGELOG.

**Phase C — cut v1 (≈1 hour)**
9. Merge to main → tag `v1.0.0` → push tag → verify workflow green.
10. Verify release page shows jar + checksums; `docker pull ghcr.io/ritikbansod/kview:1.0.0` and
    smoke-test against the docker-compose broker; run the jar on a machine with only a JRE 21.
11. Update repo About/topics; announce-ready.

**Phase D — launch (day after)**
12. Show HN ("Show HN: Kview – self-hosted Kafka UI with live tail and MCP support"), r/apachekafka,
    dev.to write-up with the dashboard screenshot, LinkedIn/X, PRs to awesome-kafka lists,
    Confluent community forum. The MCP angle is a genuine differentiator — lead with it for the AI-tools audience, the UI for the Kafka audience.
13. Immediately after: watch issues, add 2–3 `good first issue` labels, land P2.1 (npm) within a week.

---

## 6. Risks

| Risk | Mitigation |
|---|---|
| No-auth API exposed by users | Warning box, loopback default, auth fast-follow |
| Plaintext secrets in connections.json | Already documented; advise file perms; encryption later |
| "kview" name collision (there are small projects with similar names) | GitHub/npm search before HN launch; rename now is cheap, later is expensive |
| Single maintainer bus factor | Normal for v1; clear CONTRIBUTING + good-first-issues |
| 46 MB jar per release | Fine on GitHub Releases |
| KNOWN-01 (Windows broker topic-delete crash) | Environmental, already documented in QA report — not a Kview bug |

## 7. Definition of done for v1

- [ ] LICENSE in repo, detected by GitHub
- [ ] Tag v1.0.0 exists with a GitHub Release containing jar + SHA256
- [ ] Docker image published and pullable by a stranger
- [ ] Fresh-clone `mvn package` and the published jar both start and serve :8090
- [ ] README shows screenshots, 3 install paths, security warning; versions consistent
- [ ] CI green on main; release workflow produced the artifacts (not a local machine)
- [ ] SECURITY.md / CONTRIBUTING.md / CHANGELOG.md / issue templates present
