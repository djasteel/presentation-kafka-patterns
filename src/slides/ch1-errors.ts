import type { Slide } from '../engine/types';
import { r, rt } from './helpers';

// Chapter 1, part B: retries, rebalancing, dead letters, ordered parking, upstream errors, schema evolution.

export const consumptionB: Slide[] = [
  {
    id: 'retries',
    title: 'Retries: split by whether order matters',
    subtitle: 'Transient failures get retried; permanent failures get parked or dead-lettered',
    bullets: [
      '**Ordered flows** (trades): **blocking** in-place retry with exponential backoff (`DefaultErrorHandler`, Resilience4j around downstream calls). Exhausted ⇒ **park the key**.',
      '**Unordered flows** (e.g. independent notifications): **non-blocking** retry topics (`@RetryableTopic`): the partition keeps flowing. Exhausted ⇒ **DLT**.',
      'Retry topics re-order messages by design: never use them where a later event for the same key can overtake an earlier one.',
      'Classify exceptions: validation errors are **not retryable**: skip straight to park / DLT.',
    ],
    code: {
      lang: 'java',
      caption: 'Ordered: blocking retry, then recover',
      src: `
@Bean
DefaultErrorHandler errorHandler(ParkingRecoverer parking) {
    var backOff = new ExponentialBackOffWithMaxRetries(4);
    backOff.setInitialInterval(500);
    backOff.setMultiplier(2.0);              // 0.5s, 1s, 2s, 4s
    var handler = new DefaultErrorHandler(parking, backOff);
    handler.addNotRetryableExceptions(ValidationException.class);
    return handler;
}

// Unordered: non-blocking retry topics, then DLT
@Bean
RetryTopicConfiguration notificationRetry(KafkaTemplate<String, Object> t) {
    return RetryTopicConfigurationBuilder.newInstance()
        .exponentialBackoff(1_000, 2, 30_000).maxAttempts(4)
        .includeTopic("notifications").create(t);
}`,
    },
    animation: {
      scene: {
        height: 430,
        nodes: [
          { id: 'ra', kind: 'region', label: 'Ordered: blocking retry', x: 10, y: 10, w: 780, h: 190 },
          { id: 'rb', kind: 'region', label: 'Unordered: retry topics', x: 10, y: 220, w: 780, h: 200 },
          { id: 'ta', kind: 'topic', label: 'trades', x: 40, y: 70, w: 120, h: 66 },
          { id: 'ca', kind: 'consumer', label: 'consumer', sub: 'retry in place', x: 280, y: 70, w: 160, h: 66 },
          { id: 'pk', kind: 'db', label: 'parked', sub: 'per key', x: 600, y: 62, w: 130, h: 80 },
          { id: 'tb', kind: 'topic', label: 'notifications', x: 40, y: 290, w: 140, h: 66 },
          { id: 'cb', kind: 'consumer', label: 'consumer', x: 240, y: 290, w: 120, h: 66 },
          { id: 'rt', kind: 'topic', label: 'retry-1s…', sub: 'retry-2s, -4s', x: 410, y: 255, w: 140, h: 60 },
          { id: 'dl', kind: 'dlt', label: 'notifications-dlt', x: 600, y: 352, w: 170, h: 52 },
        ],
        edges: [
          { id: 'a1', from: 'ta', to: 'ca' },
          { id: 'a2', from: 'ca', to: 'pk', label: 'exhausted' },
          { id: 'b1', from: 'tb', to: 'cb' },
          { id: 'b2', from: 'cb', to: 'rt', label: 'fail' },
          { id: 'b3', from: 'rt', to: 'cb', curve: -40 },
          { id: 'b4', from: 'rt', to: 'dl', label: 'exhausted' },
        ],
      },
      steps: [
        { caption: 'Both consumers receive a message that fails.' },
        {
          caption: 'Ordered: the consumer retries **in place** (0.5s, 1s, 2s…). The partition waits; order is preserved.',
          messages: [{ edge: 'a1', label: 'T-7 v1' }],
          nodes: { ca: { tone: 'warn', badge: 'attempt 1…4' } },
        },
        {
          caption: 'Unordered: the failed message moves to a **retry topic**; following messages keep flowing.',
          messages: [
            { edge: 'b1', label: 'N-1' },
            { edge: 'b2', label: 'N-1', tone: 'warn', delay: 2.6 },
            { edge: 'b1', label: 'N-2', delay: 3.5 },
          ],
          nodes: { cb: { badge: 'N-2 processed' } },
        },
        {
          caption: 'Retries exhausted. Ordered ⇒ **park the key** (later T-7 events wait). Unordered ⇒ **DLT**.',
          messages: [
            { edge: 'a2', label: 'T-7 v1', tone: 'parked' },
            { edge: 'b4', label: 'N-1', tone: 'fail' },
          ],
          nodes: { ca: {}, pk: { tone: 'parked', badge: 'T-7 blocked' }, dl: { tone: 'fail' } },
        },
      ],
    },
    notes:
      'Blocking retries hold the whole partition, so keep the total backoff well below max.poll.interval.ms (next slide), or the consumer is kicked out of the group mid-retry.\n\nResilience4j is the right tool around downstream HTTP calls (circuit breaker + retry); the Kafka error handler decides what happens to the record when those give up.',
  },
  {
    id: 'rebalancing',
    title: 'Rebalancing: the hidden source of redelivery',
    subtitle: 'Most "random duplicates" in production are rebalances',
    bullets: [
      '`max.poll.interval.ms` (default 5 min): if processing a poll takes longer, the consumer is **evicted** ⇒ rebalance ⇒ its uncommitted records are redelivered elsewhere.',
      'Blocking retries + large `max.poll.records` + slow DB = **rebalance storm**.',
      '**Cooperative rebalancing**: `CooperativeStickyAssignor` (classic protocol) or the new **KIP-848** protocol (`group.protocol=consumer`, GA in Kafka 4.0): only moved partitions pause.',
      '**Static membership** (`group.instance.id`): a pod restart within `session.timeout.ms` does not trigger a rebalance.',
    ],
    code: {
      lang: 'yaml',
      caption: 'application.yml',
      src: `
spring:
  kafka:
    consumer:
      max-poll-records: 100          # bound work per poll
      properties:
        max.poll.interval.ms: 300000 # > worst-case batch + retries
        group.protocol: consumer     # KIP-848 (Kafka 4.x brokers + clients)
        group.instance.id: \${HOSTNAME} # static membership
        session.timeout.ms: 45000`,
    },
    animation: {
      scene: {
        height: 380,
        nodes: [
          { id: 'tp', kind: 'topic', label: 'trades', sub: 'p0 · p1 · p2', x: 20, y: 150, w: 140, h: 80 },
          { id: 'c1', kind: 'consumer', label: 'consumer-1', sub: 'p0, p1', x: 330, y: 60, w: 160, h: 60 },
          { id: 'c2', kind: 'consumer', label: 'consumer-2', sub: 'p2', x: 330, y: 260, w: 160, h: 60 },
          { id: 'gc', kind: 'service', label: 'Group coordinator', x: 610, y: 160, w: 160, h: 60 },
        ],
        edges: [
          { id: 'e1', from: 'tp', to: 'c1' },
          { id: 'e2', from: 'tp', to: 'c2' },
          { id: 'h1', from: 'c1', to: 'gc', dashed: true, label: 'poll / heartbeat' },
          { id: 'h2', from: 'c2', to: 'gc', dashed: true },
          { id: 'mv', from: 'c1', to: 'c2', curve: 60, label: 'p1 moves' },
        ],
      },
      steps: [
        { caption: 'Two consumers share three partitions.', edges: { mv: { hidden: true } } },
        {
          caption: 'consumer-1 is stuck in a slow batch (blocking retries + slow DB): no `poll()` for > `max.poll.interval.ms`.',
          nodes: { c1: { tone: 'warn', badge: 'processing 6 min…' } },
          edges: { h1: { tone: 'fail' } },
        },
        {
          caption: 'Coordinator evicts consumer-1 and **rebalances**. Its uncommitted work is abandoned.',
          nodes: { c1: { tone: 'fail', badge: 'evicted' }, gc: { tone: 'active', badge: 'rebalance' } },
        },
        {
          caption: 'Partitions move to consumer-2, which resumes from the **last committed offset**: redelivery.',
          nodes: { c1: { tone: 'muted' }, c2: { tone: 'active', badge: 'p0, p1, p2' }, gc: {} },
          edges: { mv: {}, e1: { tone: 'muted' } },
          messages: [{ edge: 'e2', label: 'redelivered', tone: 'dup' }],
        },
        {
          caption: 'consumer-1 recovers and rejoins ⇒ **another** rebalance. Repeat = storm. Fix the timing, not the symptom.',
          nodes: { c1: { tone: 'warn', badge: 'rejoin' }, c2: { badge: 'rebalance again' } },
          edges: { mv: { hidden: true }, e1: {}, h1: {} },
        },
      ],
    },
    notes:
      'Rebalance metrics (rate per hour, latency) are among the first things to check in any RCA involving duplicates or lag spikes. We show how to get them in the monitoring section.\n\nWith Spring Boot 4 you get Kafka 4.x clients. KIP-848 moves assignment to the broker and makes rebalances incremental by default. Check your broker version before enabling it.',
  },
  {
    id: 'dlt-unordered',
    title: 'Dead-letter topic: mandatory, and easy when order does not matter',
    subtitle: 'Every consumer has a DLT. No exceptions.',
    bullets: [
      'A poison message (cannot deserialize, fails validation) must not block the partition forever.',
      'Unordered flows: publish to `<topic>-dlt` with headers (original topic/partition/offset, exception) and **continue**.',
      'Spring: `DeadLetterPublishingRecoverer` as the recoverer of `DefaultErrorHandler`.',
      'DLT is not a bin: it is monitored, alerted on, and has an owner and a replay procedure.',
    ],
    code: {
      lang: 'java',
      caption: 'DLT recoverer',
      src: `
@Bean
DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
    var recoverer = new DeadLetterPublishingRecoverer(template,
        (rec, ex) -> new TopicPartition(rec.topic() + "-dlt", rec.partition()));
    return new DefaultErrorHandler(recoverer, new FixedBackOff(1_000, 3));
}`,
    },
    animation: {
      scene: {
        height: 320,
        nodes: [
          { id: 'tp', kind: 'topic', label: 'notifications', x: 20, y: 110, w: 140, h: 70 },
          { id: 'cs', kind: 'consumer', label: 'consumer', x: 290, y: 113, w: 150, h: 64 },
          { id: 'db', kind: 'db', label: 'DB', x: 640, y: 40, w: 110, h: 76 },
          { id: 'dl', kind: 'dlt', label: 'notifications-dlt', x: 590, y: 200, w: 180, h: 60 },
        ],
        edges: [
          { id: 'e1', from: 'tp', to: 'cs' },
          { id: 'e2', from: 'cs', to: 'db' },
          { id: 'e3', from: 'cs', to: 'dl', label: 'after retries' },
        ],
      },
      steps: [
        { caption: 'Normal messages flow to the DB.', messages: [{ edge: 'e1', label: 'N-1' }, { edge: 'e2', label: 'N-1', delay: 2.6 }] },
        {
          caption: 'N-2 cannot be deserialized: retries cannot fix it.',
          messages: [{ edge: 'e1', label: 'N-2', tone: 'fail' }],
          nodes: { cs: { tone: 'fail' } },
        },
        {
          caption: 'It goes to the DLT with diagnostic headers; offset committed; the partition moves on.',
          messages: [
            { edge: 'e3', label: 'N-2', tone: 'fail' },
            { edge: 'e1', label: 'N-3', delay: 2.6 },
            { edge: 'e2', label: 'N-3', delay: 5 },
          ],
          nodes: { cs: {}, dl: { tone: 'fail', badge: 'alert → owner' } },
        },
      ],
    },
    notes: 'Easy case first. The next two slides deal with the hard case: when the failing message has successors that must not overtake it.',
  },
  {
    id: 'ordered-parking',
    title: 'Ordered flows: park the key, not the partition',
    subtitle: 'If T-7 v1 fails, every later T-7 event must wait, but T-9 should not',
    bullets: [
      'On final failure: record the key in `blocked_key`, store the message in `parked_message`, **commit the offset**.',
      'Every incoming message checks `blocked_key` first: if its key is blocked ⇒ **park it too** (in offset order).',
      'Other keys continue at full speed: one bad trade does not stall thousands of good ones.',
      'Blocking the whole partition is simpler but turns one bad message into a partition-wide outage.',
    ],
    code: {
      lang: 'java',
      caption: 'Park-aware processing (same DB transaction)',
      src: `
@Transactional
public void handle(ConsumerRecord<String, TradeEvent> rec) {
    if (parking.isBlocked(rec.key())) {          // SELECT 1 FROM blocked_key
        parking.park(rec, "predecessor blocked"); // keep offset order
        return;
    }
    try {
        tradeStore.insert(rec);
    } catch (ValidationException e) {             // permanent failure
        parking.block(rec.key(), rec, e);         // blocked_key + parked_message
        errorEvents.publishViaOutbox(rec, e);     // notify upstream
    }
}`,
    },
    animation: {
      scene: {
        height: 440,
        nodes: [
          { id: 'cs', kind: 'consumer', label: 'trade-store', x: 30, y: 150, w: 160, h: 64 },
          { id: 'db', kind: 'db', label: 'trade_event', x: 600, y: 140, w: 170, h: 84 },
        ],
        edges: [
          { id: 'ok', from: 'cs', to: 'db', label: 'insert (key not blocked)' },
        ],
        logs: [{ id: 'p0', title: 'trades · partition 3', x: 30, y: 40, cells: ['T-7 v1', 'T-9 v1', 'T-7 v2', 'T-9 v2', 'T-7 v3'], base: 500, cellW: 74 }],
        tables: [
          { id: 'bk', title: 'blocked_key', x: 30, y: 280, w: 310, columns: ['key', 'failed', 'reason'], maxRows: 2 },
          { id: 'pm', title: 'parked_message', x: 370, y: 280, w: 400, columns: ['key', 'ver', 'offset', 'status'], widths: [0.8, 0.6, 0.9, 1.7], maxRows: 4 },
        ],
      },
      steps: [
        { caption: 'Partition 3 interleaves two trades.', logs: { p0: { committed: 500 } }, tables: { bk: [], pm: [] } },
        {
          caption: '**T-7 v1 fails validation** (unknown book). Key T-7 is blocked; v1 is parked; offset committed.',
          logs: { p0: { committed: 501, marks: { 500: 'fail' } } },
          tables: { bk: [rt('fail', 'T-7', '500', 'unknown book')], pm: [rt('fail', 'T-7', '1', '500', 'FAILED (head)')] },
        },
        {
          caption: 'T-9 v1: key not blocked ⇒ processed normally.',
          logs: { p0: { committed: 502, marks: { 500: 'fail', 501: 'ok' } } },
          messages: [{ edge: 'ok', label: 'T-9 v1' }],
        },
        {
          caption: 'T-7 v2 (AMEND): key is blocked ⇒ **parked behind v1**. Applying it now would amend a trade that does not exist.',
          logs: { p0: { committed: 503, marks: { 500: 'fail', 501: 'ok', 502: 'parked' } } },
          tables: { pm: [rt('fail', 'T-7', '1', '500', 'FAILED (head)'), rt('parked', 'T-7', '2', '502', 'PARKED')] },
        },
        {
          caption: 'T-9 v2 flows; T-7 v3 parks. The partition never stopped.',
          logs: { p0: { committed: 505, marks: { 500: 'fail', 501: 'ok', 502: 'parked', 503: 'ok', 504: 'parked' } } },
          messages: [{ edge: 'ok', label: 'T-9 v2' }],
          tables: {
            pm: [rt('fail', 'T-7', '1', '500', 'FAILED (head)'), rt('parked', 'T-7', '2', '502', 'PARKED'), rt('parked', 'T-7', '3', '504', 'PARKED')],
          },
        },
      ],
    },
    notes:
      'The block check, the park and the offset commit order all happen inside the same DB transaction as normal processing, so parking itself is consistent and idempotent.\n\nParked rows keep their Kafka coordinates, so draining them in offset order is trivial.',
  },
  {
    id: 'unpark',
    title: 'Releasing a parked key: the correction supersedes the failed head',
    subtitle: 'Fully automatic: no ops step on the happy path',
    bullets: [
      'Upstream receives the error event, fixes the data and **republishes** the event: same `tradeId + version`, corrected payload, incremented `revision` header.',
      'The consumer sees a message for a blocked key that **matches the failed head** ⇒ processes it instead of parking it.',
      'On success, in the same transaction: mark head resolved, then **drain parked messages for that key in offset order**.',
      'If a drained message fails, the key stays blocked at the new head: the same loop repeats.',
    ],
    callout: {
      kind: 'tip',
      title: 'The failed head was never inserted, so the correction does not collide with the idempotency key.',
      text: 'Keep a manual "skip head" action in the support tooling for the rare case where upstream cannot correct.',
    },
    animation: {
      scene: {
        height: 385,
        nodes: [
          { id: 'up', kind: 'producer', label: 'Booking system', x: 20, y: 40, w: 150, h: 60 },
          { id: 'cs', kind: 'consumer', label: 'trade-store', x: 300, y: 40, w: 150, h: 60 },
          { id: 'db', kind: 'db', label: 'trade_event', x: 600, y: 30, w: 150, h: 80 },
        ],
        edges: [
          { id: 'fix', from: 'up', to: 'cs', label: 'corrected T-7 v1' },
          { id: 'ins', from: 'cs', to: 'db', label: 'insert' },
        ],
        tables: [
          { id: 'bk', title: 'blocked_key', x: 20, y: 160, w: 330, columns: ['key', 'failed offset', 'reason'], maxRows: 2 },
          { id: 'pm', title: 'parked_message', x: 20, y: 270, w: 470, columns: ['key', 'version', 'offset', 'status'], maxRows: 4 },
          { id: 'te', title: 'trade_event (T-7)', x: 530, y: 160, w: 250, columns: ['version', 'action'], maxRows: 4 },
        ],
      },
      steps: [
        {
          caption: 'T-7 is blocked with v1 failed and v2, v3 parked.',
          tables: {
            bk: [rt('fail', 'T-7', '500', 'unknown book')],
            pm: [rt('fail', 'T-7', '1', '500', 'FAILED (head)'), rt('parked', 'T-7', '2', '502', 'PARKED'), rt('parked', 'T-7', '3', '504', 'PARKED')],
            te: [],
          },
        },
        {
          caption: 'Upstream republishes **T-7 v1** with the corrected book (`revision=2`).',
          messages: [{ edge: 'fix', label: 'T-7 v1 rev2', tone: 'ok' }],
        },
        {
          caption: 'It matches the failed head ⇒ processed and inserted. Head marked RESOLVED.',
          messages: [{ edge: 'ins', label: 'T-7 v1' }],
          tables: {
            pm: [rt('muted', 'T-7', '1', '500', 'RESOLVED'), rt('parked', 'T-7', '2', '502', 'PARKED'), rt('parked', 'T-7', '3', '504', 'PARKED')],
            te: [r('1', 'NEW')],
          },
        },
        {
          caption: 'Drain in offset order: v2 then v3 applied.',
          messages: [
            { edge: 'ins', label: 'T-7 v2' },
            { edge: 'ins', label: 'T-7 v3', delay: 2 },
          ],
          tables: {
            pm: [rt('muted', 'T-7', '1', '500', 'RESOLVED'), rt('muted', 'T-7', '2', '502', 'DRAINED'), rt('muted', 'T-7', '3', '504', 'DRAINED')],
            te: [r('1', 'NEW'), r('2', 'AMEND'), r('3', 'CANCEL')],
          },
        },
        {
          caption: 'Key unblocked. New T-7 events flow normally again; the trade is complete and correctly ordered.',
          tables: { bk: [] },
          nodes: { cs: { tone: 'ok', badge: 'T-7 unblocked' } },
        },
      ],
    },
    notes:
      'Why match on the failed head rather than accept any message for the key? Because a newer event (v4) arriving while blocked must still wait, otherwise it would overtake v2 and v3.\n\nThe revision header lets you tell a correction from a plain duplicate of the original bad message, which should stay parked.',
  },
  {
    id: 'error-topic',
    title: 'Tell the upstream: one common error topic',
    subtitle: 'Upstreams subscribe once, not to every consumer\'s DLT',
    bullets: [
      'Every consumer publishes an **error event** for each parked / dead-lettered message to one shared topic, e.g. `platform.processing-errors`.',
      'Upstream teams subscribe once and **filter by their producer app id**: no need to discover and subscribe to N DLTs.',
      'Event carries coordinates and ids, **not the payload**: avoids copying PII and large messages into another topic.',
      'Upstream reacts by correcting and republishing (previous slide) — the loop closes automatically.',
    ],
    code: {
      lang: 'java',
      caption: 'ProcessingErrorEvent.java',
      src: `
public record ProcessingErrorEvent(
    String producerAppId,     // upstream filters on this
    String consumerAppId,     // who failed
    String sourceTopic, int sourcePartition, long sourceOffset,
    String businessKey,       // e.g. tradeId
    String businessVersion,   // e.g. "1"
    int schemaVersion,
    String errorCode,         // e.g. UNKNOWN_BOOK
    String errorReason,
    String disposition,       // PARKED | DEAD_LETTERED
    Instant occurredAt) {}`,
    },
    animation: {
      scene: {
        height: 400,
        nodes: [
          { id: 'c1', kind: 'consumer', label: 'trade-store', x: 20, y: 40, w: 150, h: 54 },
          { id: 'c2', kind: 'consumer', label: 'risk-feed', x: 20, y: 170, w: 150, h: 54 },
          { id: 'c3', kind: 'consumer', label: 'settlement', x: 20, y: 300, w: 150, h: 54 },
          { id: 'et', kind: 'topic', label: 'processing-errors', sub: 'one shared topic', x: 300, y: 155, w: 190, h: 84 },
          { id: 'u1', kind: 'producer', label: 'Booking system', sub: 'filter app=booking', x: 600, y: 90, w: 180, h: 64 },
          { id: 'u2', kind: 'producer', label: 'Static data', sub: 'filter app=refdata', x: 600, y: 240, w: 180, h: 64 },
        ],
        edges: [
          { id: 'e1', from: 'c1', to: 'et' },
          { id: 'e2', from: 'c2', to: 'et' },
          { id: 'e3', from: 'c3', to: 'et' },
          { id: 's1', from: 'et', to: 'u1' },
          { id: 's2', from: 'et', to: 'u2' },
        ],
      },
      steps: [
        { caption: 'Three consumers, two upstream systems.' },
        {
          caption: 'trade-store parks T-7; settlement dead-letters a static-data record.',
          messages: [
            { edge: 'e1', label: 'T-7 · booking', tone: 'fail' },
            { edge: 'e3', label: 'SSI-3 · refdata', tone: 'fail', delay: 1 },
          ],
        },
        {
          caption: 'Each upstream receives only its own errors, with enough coordinates to find and fix the message.',
          messages: [
            { edge: 's1', label: 'T-7 · booking', tone: 'fail' },
            { edge: 's2', label: 'SSI-3 · refdata', tone: 'fail', delay: 1 },
          ],
          nodes: { u1: { badge: 'fix + republish' }, u2: { badge: 'fix + republish' } },
        },
      ],
    },
    notes:
      'Without this, upstream teams find out about bad data from a reconciliation break days later. With it, the producer that caused the problem gets told within seconds, with the exact business key.\n\nPublish error events through the outbox so that "parked" and "upstream notified" are consistent.',
  },
  {
    id: 'schema-evolution',
    title: 'Schema evolution: JSON Schema, a registry and a version header',
    subtitle: 'Compatibility mode FULL_TRANSITIVE',
    bullets: [
      'Payloads are **JSON Schema**, registered in the **Schema Registry**; producers fail fast on an incompatible schema.',
      'Mode **FULL_TRANSITIVE**: any reader version can read any writer version, against **all** previous versions.',
      'Why FULL, not BACKWARD: during **blue-green** old and new consumers run side by side, and a **14-day replay** re-reads old data with new code.',
      'Also put `schemaVersion` in a **message header**: routing and parking decisions without deserializing the body.',
    ],
    table: {
      head: ['Change', 'FULL_TRANSITIVE?'],
      rows: [
        ['Add optional field with default', '✅ allowed'],
        ['Remove optional field', '✅ allowed'],
        ['Add required field', '❌ old writers do not send it'],
        ['Rename / change type of field', '❌ add a new field, deprecate the old'],
      ],
    },
    code: {
      lang: 'java',
      caption: 'Producer: header + registry-validated serializer',
      src: `
var rec = new ProducerRecord<>("trades", event.tradeId(), event);
rec.headers().add("schemaVersion", "3".getBytes(UTF_8));
kafkaTemplate.send(rec);

// application.yml
// spring.kafka.producer.value-serializer:
//   io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
// spring.kafka.producer.properties.auto.register.schemas: false
// spring.kafka.producer.properties.use.latest.version: true`,
    },
    notes:
      'Set compatibility per subject in the registry and disable auto-registration in production: schemas are registered by the CI pipeline, where an incompatible change fails the build, not the deployment.\n\nFULL_TRANSITIVE is strict. That is the point: it is the only mode that makes "deploy in any order, replay anything" safe.',
  },
  {
    id: 'unknown-version',
    title: 'Blue-green: a consumer meets a schema version it does not know',
    subtitle: 'Park it; do not dead-letter it',
    bullets: [
      'During rollout the producer may emit `schemaVersion=4` while some consumers still understand only ≤ 3.',
      'This is **not** a poison message. It is a message from the near future, and it becomes valid once the new consumer is deployed.',
      'Treat as **retriable**: park it (per key if ordered), do **not** send it to the DLT or the upstream error topic.',
      'New version deployed ⇒ drain parked messages. No noise, no false alarms during every release.',
    ],
    code: {
      lang: 'java',
      caption: 'Version gate before deserialization',
      src: `
int v = Integer.parseInt(new String(
    rec.headers().lastHeader("schemaVersion").value(), UTF_8));
if (v > SUPPORTED_SCHEMA_VERSION) {
    parking.park(rec, "UNSUPPORTED_SCHEMA_VERSION");  // no DLT, no upstream error
    return;
}`,
    },
    animation: {
      scene: {
        height: 380,
        nodes: [
          { id: 'pr', kind: 'producer', label: 'Producer', sub: 'already on v4', x: 20, y: 50, w: 150, h: 64 },
          { id: 'tp', kind: 'topic', label: 'trades', x: 250, y: 47, w: 120, h: 70 },
          { id: 'bl', kind: 'consumer', label: 'consumer (blue)', sub: 'supports ≤ v3', x: 460, y: 20, w: 170, h: 60 },
          { id: 'gr', kind: 'consumer', label: 'consumer (green)', sub: 'supports ≤ v4', x: 460, y: 110, w: 170, h: 60 },
          { id: 'pk', kind: 'db', label: 'parked', x: 670, y: 220, w: 110, h: 76 },
          { id: 'dl', kind: 'dlt', label: 'DLT', x: 470, y: 260, w: 110, h: 50 },
        ],
        edges: [
          { id: 'e1', from: 'pr', to: 'tp' },
          { id: 'e2', from: 'tp', to: 'bl' },
          { id: 'e3', from: 'bl', to: 'pk', label: 'park' },
          { id: 'e4', from: 'bl', to: 'dl', dashed: true },
          { id: 'e5', from: 'pk', to: 'gr', curve: 40, label: 'drain' },
        ],
      },
      steps: [
        {
          caption: 'Producer is upgraded first and emits v4. Only blue is running.',
          nodes: { gr: { tone: 'muted' } },
          edges: { e5: { hidden: true } },
          messages: [
            { edge: 'e1', label: 'T-3 v4' },
            { edge: 'e2', label: 'T-3 v4', delay: 2.6 },
          ],
        },
        {
          caption: 'Blue cannot read v4 ⇒ **parks** it. The DLT path is deliberately not used.',
          messages: [{ edge: 'e3', label: 'T-3 v4', tone: 'parked' }],
          nodes: { pk: { tone: 'parked', badge: '1 parked' }, dl: { tone: 'muted' } },
          edges: { e4: { tone: 'muted' } },
        },
        {
          caption: 'Green (supports v4) is deployed and takes over.',
          nodes: { gr: { tone: 'active' }, bl: { tone: 'muted' } },
        },
        {
          caption: 'Green drains parked v4 messages. Nothing reached the DLT; upstream was never paged.',
          edges: { e5: {} },
          messages: [{ edge: 'e5', label: 'T-3 v4', tone: 'ok' }],
          nodes: { pk: { badge: '0 parked' }, gr: { tone: 'ok' } },
        },
      ],
    },
    notes:
      'With FULL_TRANSITIVE, an old consumer can usually read new data by ignoring unknown fields. The version gate is for the cases where semantics changed and the old code must not guess.\n\nPrefer deploying consumers before producers; the parking rule is the safety net when that order is not possible.',
  },
];
