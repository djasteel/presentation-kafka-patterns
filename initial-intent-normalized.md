# Kafka in Investment Banking — Animated Learning Guide

Normalized requirements, derived from `initial-intent.md` and the clarification interview (2026-09-30).

## 1. Goal

An animated web-application presentation that teaches application engineers which Kafka patterns to use in investment-bank applications, and how to compensate for Kafka's delivery-semantics limitations. It is presented live as a workshop and afterwards serves as a standalone reference guide ("cradle of experience").

Every pattern is explained with **why** (pros and cons), animated data flow, and a short code snippet.

## 2. Audience and format

| Aspect | Decision |
|---|---|
| Audience | Application engineers (Java / Spring Boot) |
| Session | 90-minute workshop, ~35–45 slides |
| Language | English |
| Visual style | Neutral modern technical look, dark/light theme toggle, no company branding |
| Running example | Trade lifecycle: `TradeEvent` keyed by `tradeId`, NEW → AMEND → CANCEL, idempotency key `tradeId + version` |
| Code on slides | Focused Java / Spring Boot 4 snippets (10–20 lines) beside the animation; Resilience4j where relevant |

## 3. Application requirements

### 3.1 Technology and hosting
- Vite + React + TypeScript, built to static files.
- Hosted on an internal static host (e.g. GitLab Pages / Azure Static Web Apps); built output must also open offline.
- Animations: Framer Motion + SVG (default, pending confirmation).

### 3.2 Navigation
- Full-screen slide deck with arrow-key navigation for presenting.
- Collapsible chapter/slide sidebar.
- Deep-link URL per slide.
- Speaker-notes panel (toggle).
- Every slide must be self-explanatory when read without the presenter.

### 3.3 Animations
- Fully **browser-simulated**, deterministic, scripted per slide — no real Kafka/Postgres backend.
- Show components (producers, topics/partitions, consumers, thread pools, DB tables, outbox, DLT, regions) connected by arrows; messages visibly travel along arrows and land as rows in DB tables.
- Failures injected on cue (e.g. failure at message 51, region outage, poison message).
- Playback: step-by-step (each key press advances one step), optional autoplay, speed control, reset.

### 3.4 Performance data
- All performance charts read from a single `perf-data.json`.
- Initially filled with clearly marked **TODO placeholders**; presenter replaces with real measured numbers.
- Metrics to cover: single-row vs batched insert TPS, index impact on insert TPS, same-region vs cross-region consumer↔DB latency, p50/p99 processing latency.

## 4. Cross-cutting concerns (emphasized throughout)

1. **Retention & archiving** — 14-day topic retention as default (RCA + replay window). Long-term (regulatory) record is the DB/ADLS where consumers persist every message plus Kafka coordinates. Message: *Kafka is not your system of record.*
2. **Consistency** — no data loss, no duplication.
3. **Schema evolution** — backwards-compatible changes.
4. **Resiliency** — region failover runbook preserving consistency.

## 5. Content

### Chapter 1 — Kafka consumption

**1.1 Listener styles (progression)**
- Single-record listener (1 read → 1 process): preferred default in financial apps, simplest.
- Batch listener (incl. topic-pattern subscription): prone to errors; all-or-nothing vs partial offset commit.
- Concurrent processing of a batch:
  - Batch of 100 processed by an executor thread pool; message 51 fails while 52–100 may already have succeeded.
  - **Contiguous watermark** semantics: commit offset 51 (next to read = failed message); 52–100 are redelivered and absorbed by idempotency.
  - Animate the watermark advancing and duplicate rejection on redelivery.
  - Show Spring's `BatchListenerFailedException(index)` as the built-in equivalent.

**1.2 Idempotency**
- Primary mechanism: **unique constraint on the business key** (`tradeId + version` / producer `eventId`) enforced on insert.
- Misconception slide: dedup on (topic, partition, offset) alone misses producer duplicates — a retried or outbox-republished message gets a new offset.
- Kafka does not support distributed (XA/2PC) transactions spanning Kafka and a DB.
- Caching for uniqueness is an optimization only, with maintenance cost: uniqueness (positive) cache vs negative cache (misses).
- Other approaches briefly, with disadvantages.
- **DB commit first, Kafka offset commit second** — explain why (crash between the two ⇒ redelivery, which idempotency absorbs; reverse order ⇒ data loss).
- If the service writes to the DB and also publishes to another topic ⇒ transactional outbox (Chapter 2).

**1.3 Persisting Kafka coordinates**
- Persist topic, consumer group id, partition, offset as separate columns alongside business data.
- Enables deterministic replay and RPO calculation at DR: compare offsets in DB vs failed region's cluster/DB to know exactly which messages were not consumed.
- Idempotency is what makes the overlap during DR replay safe.

**1.4 Retries (split by ordering requirement)**
- Ordered flows: blocking in-place retry with backoff (Spring `DefaultErrorHandler` / Resilience4j) for transient failures, then per-key parking.
- Unordered flows: non-blocking retry topics (`@RetryableTopic`), then DLT.
- Animate both paths.

**1.5 Rebalancing**
- Cooperative-sticky assignor, static membership (`group.instance.id`), `max.poll.interval.ms` traps causing rebalance storms.

**1.6 Dead-letter handling**
- A DLT must always exist.
- Order not important: publishing to DLT is straightforward.
- Order important: **per-key parking** — when a message fails, all subsequent messages with the same key are parked (tracked in a DB table); other keys keep flowing.
- Release: upstream republishes a corrected message (same business id, higher version) which **supersedes the failed head**; the consumer then drains the parked messages for that key in offset order — automatic, no ops step.
- **Common upstream error topic**: one shared topic all upstreams subscribe to (filtered by producer/app id) instead of subscribing to individual DLTs. Event contents: source topic/partition/offset, business id, producer/app id, error code + reason, consumer, timestamp, schema version — **no original payload** (avoids PII and size duplication).

**1.7 Schema evolution**
- JSON Schema + Schema Registry.
- Schema version also carried in a message header (important for blue-green deployments).
- Compatibility mode: **FULL_TRANSITIVE** — both colors coexist during blue-green, and 14-day-old data may be replayed.
- Unknown (newer) schema version at a consumer: **park, don't DLT** — treat as retriable (per-key if ordered) until the new consumer version is deployed, then drain. Avoids flooding the DLT and the upstream error topic during rollout.

**1.8 Performance**
- Normal consumer TPS with PostgreSQL (placeholder data).
- Insert optimization and DB index organization.
- Co-location of consumer and DB: cross-region RTT adds directly to per-message latency.

**1.9 Monitoring**
- Metrics: Micrometer → Prometheus → Grafana — throughput (TPS), success/error rate, consumer lag per partition, group rebalance statistics.
- Alerts fire only when success/error rate crosses a threshold.
- Monitoring is required from performance tests onwards, not only in production.
- Traces: App Insights (OpenTelemetry) separating Kafka poll vs DB insert vs other actions (e.g. ADLS write) as distinct spans.
- Obtaining lag and rebalance metrics in Java / Spring Boot: Spring Boot Actuator + Micrometer for app metrics; Kafka `AdminClient` for consumer-group offsets, lag and group state.

### Chapter 2 — Kafka publishing

- Importance of the partition key when order matters; how to choose it (`tradeId` in the running example).
- Misconception slide — null key: since Kafka 2.4 (sticky partitioner) / 3.3 (KIP-794 uniform sticky), null keys do **not** create a permanently hot partition. The real side effect is **loss of ordering** for related messages. Hot partitions come from **skewed non-null keys** (e.g. one large client or instrument).
- Idempotent producer (`enable.idempotence`, `acks=all`) — preventing duplicates from producer retries.
- Transactional outbox — consistency between DB write and publish, at the cost of possible duplicates (consumers must be idempotent).
  - Option A: Debezium (CDC).
  - Option B: Spring Boot scheduled poller (e.g. every 30 s) reading the outbox table — **preferred for simplicity and quick results**.
  - Multiple poller instances: `SELECT … FOR UPDATE SKIP LOCKED` in Postgres to share work without double-publishing.

### Chapter 3 — Availability and resiliency

- Platform: **single self-managed Kafka cluster stretched across two Azure regions**.
- Committed offsets live in the internal `__consumer_offsets` topic, replicated across the cluster with minimal but non-zero lag ⇒ the **offsets persisted in the DB are the real source of truth**.
- RPO = 0 requires readiness to replay. Runbook (script primary):
  1. Read max persisted offset per topic/partition from the DB.
  2. Compare with the cluster's offsets/log-end offsets; compute the gap (= RPO).
  3. Reset consumer-group offsets (`kafka-consumer-groups --reset-offsets` or `AdminClient`).
  4. Resume consumption; idempotency absorbs the overlap.
  - Custom support UI mentioned as a later maturity step.
- Without persisted topic / consumer group / partition / offset, DR and consistency checks become complex and non-deterministic.
- Animate: region failure, offset comparison, gap calculation, replay, duplicate rejection.

### Chapter 4 — Closing thoughts

- **Verdict: pro-Kafka.** Consistency problems are inherent to any broker + DB combination; the patterns shown solve them. Replay-from-offset and retention are what make the DR and RCA story possible.
- Misconception slide: Azure Event Hubs is also a partitioned log (same per-partition ordering model), not a FIFO broker; true per-entity FIFO on Azure is Service Bus sessions.
- Platform observability: application teams must have access to broker performance, topic performance and broker availability.
- Emphasize Spring Boot Actuator + Kafka `AdminClient` for observing core Kafka metrics from the application.
- "Further topics" slide: Kafka EOS / read-process-write transactions (Kafka→Kafka only), regulatory archiving mechanics (tiered storage, ADLS sink), security / ACLs / PII vs immutable logs, compacted topics for reference data, testing (Testcontainers, failure injection), parallel consumer (per-key concurrency).

## 6. Prerequisites stated in the deck

- Java
- Spring Boot 4
- Resilience4j (where needed)

## 7. Defaults pending confirmation

- Project built in this folder; `git init` the repository.
- Framer Motion + SVG for animations.
