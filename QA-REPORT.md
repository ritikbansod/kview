# QA Report — Kview v2.0

**Date:** 2026-09-06 · **Tester:** automated QA pass (LLM-driven, black-box + white-box)
**Environment:** Windows 11 · Kafka 4.3.1 (local KRaft, `localhost:9092`) · kview jar on `localhost:8090` · Chrome (in-app browser)

**Scope:** full functional regression (API), negative/boundary testing (API), black-box UI testing (rendering, forms, XSS, resource leaks, races), error mapping, concurrency.

---

## Summary

| Suite | Checks | Result |
|---|---|---|
| Functional E2E (`scripts/e2e-test.mjs`) | 79 | **79 passed / 0 failed** |
| Negative & boundary (`scripts/qa-negative-test.mjs`) | 35 | **35 passed / 0 failed** after fixes (was 30/5) |
| Unit + embedded-Kafka integration (`mvn test`) | 14 | **14 passed / 0 failed** |
| Black-box UI pass | 20+ manual checks | see below |

**Verdict:** all found defects are fixed and re-verified. One known environmental limitation (not a product bug) documented below.

---

## Bugs found

### BUG-01 — Dashboard auto-refresh timer leaks and hijacks other views — **HIGH — FIXED, VERIFIED**
*UI · dashboard*
- **Repro:** Dashboard → tick "auto-refresh (10s)" → navigate to any other page.
- **Impact:** a leaked `setInterval` kept polling `/overview` forever and **re-rendered the dashboard over whichever page the user was on** (observed: Consumer Groups view replaced by the dashboard ~10s after navigating away). With `wireAutoRefresh` called twice per render, two intervals were created but cleanup only cleared the last one.
- **Fix:** removed the duplicate `wireAutoRefresh` call; route changes now invoke a `window.__dashStop` cleanup (same pattern as the live tail).
- **Verified:** instrumented `setInterval` — exactly one interval created, cleared on navigation, **0** `/overview` calls after leaving the view; target view stays put.

### BUG-02 — Live tail could not connect from the UI — **HIGH — FIXED, VERIFIED**
*UI · data explorer*
- **Repro:** Data Explorer → Live tail → Start. Status flipped to "Stopped." immediately; no messages ever streamed.
- **Root cause:** the `EventSource` URL was built without the `/api` prefix and without `/topics/` (`/clusters/default/shipments/tail`).
- **Fix:** corrected URL to `/api/clusters/{id}/topics/{topic}/tail`.
- **Verified:** UI tail connects ("Connected — waiting for messages"), messages produced via API appear in the stream in <1s with rate counter; Stop works.

### BUG-03 — Malformed request bodies returned HTTP 500 — **MEDIUM — FIXED, VERIFIED**
*API · error mapping*
- **Repro:** `POST /api/clusters/default/topics` with `{bad json` → 500. Same for wrong field types (`"partitions": "two"`), missing body, and `headers` sent as a string.
- **Expected:** 400. **Root cause:** `HttpMessageNotReadableException` had no mapping in `ApiExceptionHandler`.
- **Fix:** added mappings for `HttpMessageNotReadableException` (400), `MethodArgumentTypeMismatchException` (400), `MissingServletRequestParameterException` (400), `HttpRequestMethodNotSupportedException` (405).
- **Verified:** malformed JSON → **400**, wrong types → **400**, missing body → **400**, headers-as-string → **400**, PATCH → **405**.

### BUG-04 — Consumer group detail crashed with HTTP 500 — **MEDIUM — FIXED, VERIFIED** *(found during earlier E2E run)*
*API · consumer groups*
- **Root cause:** `TreeMap<TopicPartition, …>` — `TopicPartition` no longer implements `Comparable` on Kafka 4.3 clients.
- **Fix:** replaced with `HashMap` (output already sorted by topic/partition explicitly).
- **Verified:** `GET /api/clusters/default/groups/{id}` returns 200 with lag breakdown.

### BUG-05 — Browse with an explicit offsets map returned other partitions too — **MEDIUM — FIXED, VERIFIED** *(found during earlier E2E run)*
*API · data explorer*
- **Fix:** offsets mode now restricts browsing to exactly the partitions named in the map.
- **Verified:** `{"0": 5}` returns partition-0 records with offset ≥ 5 only.

### KNOWN-01 — Windows dev broker crashes when deleting a topic right after writing to it — **OPEN — environmental, not a Kview bug**
- `AccessDeniedException` on log-dir rename (memory-mapped index files) → Kafka treats the dir as failed and shuts down. Known Kafka-on-Windows limitation; does not occur on Linux. Mitigated in the QA/E2E suites with automatic broker restart + re-verify. Documented in README.

### Notes (by design, worth knowing)
- The legacy dynamic-consumer endpoint (`/api/consumers`) never commits offsets by design (read-only verification tool), so such groups show committed=0 with lag = full topic depth. Use the live tail with `groupId` + `autoCommit=true` to visualize real committing consumption.
- Switching the topic dropdown while a live tail is running does not move the tail to the new topic (the tail is pinned at start). Stop and restart the tail.

---

## What passed (highlights)

**Backend:** topic lifecycle (create/duplicate/resize/reconfigure/delete), per-partition metadata (leader/replicas/ISR/offsets/counts), produce (keys, headers, partition pinning, future timestamps, unicode 🚀, multi-line values, null keys), browse in all modes with filters, consumer groups (state, lag, offset reset to latest → lag 0, delete), multi-cluster connection profiles (CRUD, test with reachable + unreachable endpoints, secret masking, 404s), legacy v1 API compatibility, ACLs, broker configs, health endpoint.
**Robustness:** 40 parallel produces → 40/40 OK; 10 parallel browses → 10/10 OK; boundary clamping (limit 0/-50/999999, negative timeout) with no crash or hang; 2MB payload → clean error, no hang; path-traversal and XSS topic names rejected broker-side.
**UI:** all routes render; theme (dark/light) persists across reload and follows system preference on first visit; XSS payloads in message key/value/header are fully escaped in table cells and the JSON viewer (no execution, no injected DOM — probe with `<img onerror>` and `<script>` confirmed inert); modals close via Esc/overlay/buttons; create-topic validation blocks empty names and invalid config JSON with toasts; connections "Test" shows brokers on success and styled errors on failure; rapid navigation across all routes produced **zero** JS errors; skeletons/empty states/error panels behave.

---

---

## Addendum — Schema Registry integration QA (same day)

Scope: phases 0–1 of SCHEMA-REGISTRY-PLAN.md (byte pipeline, registry attachments, decode pipeline, UI).

| # | Severity | Finding | Status |
|---|---|---|---|
| SR-01 | HIGH | Registry settings collected in the connection wizard were **silently dropped** — `ConnectionProfile` never carried them and no `PUT /registry` was called, so attaching via the UI had no effect (only the API path worked). Verified by creating a profile with `schemaRegistry` via the wizard's exact payload and observing 404 on `GET /registry`. | **FIXED** — wizard save now attaches/detaches via `PUT/DELETE /api/clusters/{id}/registry`; a dedicated registry-only modal was added for the built-in default cluster. Re-verified in the UI. |
| SR-02 | LOW | Decode of an unknown schema id returns `wireFormat: "UNKNOWN"` although the wire header *was* recognized (Confluent) — slightly misleading semantics; the `error` field does carry the registry 404. | OPEN (cosmetic) — suggest returning the detected format with a separate failure flag. |

**Re-verified after fixes:** produce Avro wire message → decode endpoint correct (subject `orders-value`, decoded JSON); browse enrichment carries schema/decoded; negatives (unknown schema id, plain JSON, random binary, invalid base64, detach cycle) all graceful; UI shows codec chips, Decoded/Raw/Hex tabs, registry modal prefill + test + save.

## Recommendations (post-QA backlog)

1. Add pagination count + "jump to offset" input in the explorer (nice-to-have on top of Load more).
2. Consider mapping `RecordTooLargeException` to HTTP 413 for clearer semantics.
3. Optionally disable the dashboard auto-refresh checkbox when the cluster is unreachable.
4. Long-term: run the dev broker in WSL/Linux to avoid KNOWN-01 entirely.
