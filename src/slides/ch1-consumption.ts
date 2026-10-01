import type { Slide } from '../engine/types';
import { r, range, rt } from './helpers';

// Chapter 1, part A: listener styles, idempotency, commit order, Kafka coordinates.

export const consumptionA: Slide[] = [
  {
    id: 'ch1',
    layout: 'section',
    title: 'Kafka consumption',
    subtitle: 'Listener styles, idempotency, commit order, error handling, schema evolution, performance and monitoring',
    notes: 'Chapter 1 is the longest: most consistency bugs live on the consumer side.',
  },
  {
    id: 'delivery-semantics',
    title: 'Delivery semantics: what Kafka actually promises',
    subtitle: 'At-least-once + idempotent processing = effectively once',
    table: {
      head: ['Semantics', 'How you get it', 'Failure mode', 'Acceptable in a bank?'],
      rows: [
        ['At-most-once', 'Commit offset **before** processing', 'Crash after commit ⇒ message **lost**', 'No'],
        ['At-least-once', 'Commit offset **after** processing', 'Crash before commit ⇒ message **redelivered**', 'Yes, with idempotency'],
        ['Exactly-once (Kafka EOS)', 'Kafka transactions, read-process-write', 'Covers Kafka → Kafka only, **not** your database', 'Not for DB sinks'],
        ['Effectively once', 'At-least-once + idempotent DB write', 'Duplicates arrive but are rejected', '**Target**'],
      ],
    },
    callout: {
      kind: 'rule',
      title: 'Design for redelivery, always.',
      text: 'Rebalances, restarts, timeouts and DR replays all redeliver messages. If a duplicate can change state, it is a bug waiting for a busy day.',
    },
    notes:
      'There is no distributed transaction between Kafka and PostgreSQL. So we pick at-least-once and make the database reject duplicates. That combination is what the rest of the chapter builds.',
  },
  {
    id: 'single-record',
    title: 'Single-record listener: 1 read → 1 process',
    subtitle: 'The default for financial flows: simplest to reason about',
    bullets: [
      'One record per invocation; offset committed **after** the DB transaction commits.',
      'Failure handling is per message: retry it, park it or dead-letter it.',
      'Throughput is bounded by per-message DB round trips, usually enough for trade flows.',
      'Start here; move to batches only when measurements say you must.',
    ],
    code: {
      lang: 'java',
      caption: 'TradeListener.java',
      src: `
@KafkaListener(topics = "trades", groupId = "trade-store")
public void onTrade(ConsumerRecord<String, TradeEvent> rec) {
    // @Transactional service: DB commit happens before return
    tradeService.store(rec.value(), KafkaCoords.of(rec, "trade-store"));
}   // container commits offset after the method returns

// application.yml
// spring.kafka.listener.ack-mode: record`,
    },
    animation: {
      scene: {
        height: 360,
        nodes: [
          { id: 'cs', kind: 'consumer', label: 'trade-store', sub: 'ack-mode: record', x: 40, y: 140, w: 170, h: 64 },
          { id: 'db', kind: 'db', label: 'PostgreSQL', x: 600, y: 130, w: 150, h: 84 },
        ],
        edges: [{ id: 'w', from: 'cs', to: 'db', label: 'INSERT + COMMIT' }],
        logs: [{ id: 'p0', title: 'trades · partition 0', x: 40, y: 50, cells: ['T-1', 'T-2', 'T-1', 'T-3', 'T-4', 'T-2'], base: 100, cellW: 48 }],
        tables: [{ id: 'tbl', title: 'trade_event', x: 270, y: 250, w: 480, columns: ['trade_id', 'version', 'offset'], maxRows: 3 }],
      },
      steps: [
        { caption: 'Committed offset = 100: the next record to read.', logs: { p0: { committed: 100 } }, tables: { tbl: [] } },
        {
          caption: 'Read offset 100 → process → INSERT + COMMIT in PostgreSQL…',
          logs: { p0: { committed: 100, marks: { 100: 'active' } } },
          nodes: { cs: { tone: 'active' } },
          messages: [{ edge: 'w', label: 'T-1 v1' }],
          tables: { tbl: [r('T-1', '1', '100')] },
        },
        {
          caption: '…then the container commits offset 101. One message, one DB transaction, one offset commit.',
          logs: { p0: { committed: 101, marks: { 100: 'ok' } } },
          nodes: { cs: {} },
        },
        {
          caption: 'Repeat for 101. A failure only ever affects the record in hand.',
          logs: { p0: { committed: 102, marks: { 100: 'ok', 101: 'ok' } } },
          messages: [{ edge: 'w', label: 'T-2 v1' }],
          tables: { tbl: [r('T-1', '1', '100'), r('T-2', '1', '101')] },
        },
        {
          caption: 'Steady state: committed offset always trails the DB by at most one record.',
          logs: { p0: { committed: 104, marks: range(100, 103, 'ok') } },
          messages: [
            { edge: 'w', label: 'T-1 v2' },
            { edge: 'w', label: 'T-3 v1', delay: 2.4 },
          ],
          tables: { tbl: [r('T-1', '1', '100'), r('T-2', '1', '101'), r('T-1', '2', '102'), r('T-3', '1', '103')] },
        },
      ],
    },
    notes:
      'AckMode RECORD commits after each record once the listener returns normally. Because the service method is @Transactional, the DB commit has already happened by then: DB first, offset second.\n\nThe "committed offset" in Kafka is always the NEXT offset to read, not the last processed one.',
  },
  {
    id: 'batch-listener',
    title: 'Batch listener: all-or-nothing vs partial commit',
    subtitle: 'Higher throughput, but one bad record affects the whole batch',
    bullets: [
      '**All-or-nothing**: any failure rolls back the batch; the whole batch is redelivered (records before the failure become duplicates).',
      '**Partial commit**: commit everything before the failing record, retry from the failure.',
      'Spring: throw `BatchListenerFailedException(index)`; the error handler commits offsets before `index` and seeks to it.',
      'Topic-pattern subscriptions (`topicPattern = "trades\\..*"`) mix partitions from many topics in one batch: per-record failure handling becomes essential.',
    ],
    code: {
      lang: 'java',
      caption: 'Batch listener with partial commit',
      src: `
@KafkaListener(topics = "trades", batch = "true")
public void onBatch(List<ConsumerRecord<String, TradeEvent>> recs) {
    for (int i = 0; i < recs.size(); i++) {
        try {
            tradeService.store(recs.get(i));   // own tx per record
        } catch (Exception e) {
            // offsets < i committed, seek to i, retry from there
            throw new BatchListenerFailedException("store failed", e, i);
        }
    }
}`,
    },
    animation: {
      scene: {
        height: 330,
        nodes: [
          { id: 'cs', kind: 'consumer', label: 'trade-store', sub: 'batch listener', x: 40, y: 170, w: 170, h: 64 },
          { id: 'db', kind: 'db', label: 'PostgreSQL', x: 600, y: 160, w: 150, h: 84 },
        ],
        edges: [{ id: 'w', from: 'cs', to: 'db', label: 'store' }],
        logs: [{ id: 'p0', title: 'trades · partition 0 · batch 200–205', x: 40, y: 60, cells: ['T-1', 'T-2', 'T-3', 'T-4', 'T-5', 'T-6'], base: 200, cellW: 60 }],
      },
      steps: [
        { caption: 'A batch of 6 records (200–205) is polled.', logs: { p0: { committed: 200, marks: range(200, 205, 'active') } } },
        {
          caption: '200–202 stored; **203 fails** (e.g. invalid book).',
          logs: { p0: { committed: 200, marks: { ...range(200, 202, 'ok'), 203: 'fail', 204: 'active', 205: 'active' } } },
          messages: [
            { edge: 'w', label: '200' },
            { edge: 'w', label: '201', delay: 1.5 },
            { edge: 'w', label: '202', delay: 3 },
            { edge: 'w', label: '203', tone: 'fail', outcome: 'stop', delay: 4.5 },
          ],
        },
        {
          caption: '**All-or-nothing**: nothing committed. Whole batch redelivered; 200–202 come back as duplicates.',
          logs: { p0: { committed: 200, marks: { ...range(200, 202, 'dup'), 203: 'fail', 204: 'muted', 205: 'muted' } } },
          nodes: { cs: { badge: 'redeliver 200–205' } },
        },
        {
          caption: '**Partial commit**: `BatchListenerFailedException(index=3)` commits 203 (= next to read) and seeks there.',
          logs: { p0: { committed: 203, marks: { ...range(200, 202, 'ok'), 203: 'fail', 204: 'muted', 205: 'muted' } } },
          nodes: { cs: { badge: 'retry from 203' } },
        },
        {
          caption: 'Retry of 203 succeeds; 204–205 follow. No duplicates, no gaps.',
          logs: { p0: { committed: 206, marks: range(200, 205, 'ok') } },
          nodes: { cs: {} },
          messages: [
            { edge: 'w', label: '203', tone: 'ok' },
            { edge: 'w', label: '204', delay: 1.5 },
            { edge: 'w', label: '205', delay: 3 },
          ],
        },
      ],
    },
    notes:
      'All-or-nothing is only safe if processing is idempotent AND the batch is one DB transaction; even then, a single poison record blocks the partition forever unless the error handler eventually dead-letters it.\n\nPartial commit needs per-record transactions, so you lose some of the batch speed-up. Measure before choosing.',
  },
  {
    id: 'concurrent-watermark',
    title: 'Concurrent processing: commit up to the first failure',
    subtitle: 'Batch of 100 processed by a thread pool; message 51 fails',
    bullets: [
      'Threads finish **out of order**. Kafka has one committed offset per partition, not a per-message ack.',
      'Track completions; commit the **contiguous watermark**: lowest offset not yet successfully done.',
      '51 fails while 52–100 already succeeded ⇒ commit **51**. 52–100 are redelivered and **rejected as duplicates** by the unique constraint.',
      'Built-in equivalent: Spring\'s `BatchListenerFailedException(index)` after joining all futures.',
    ],
    code: {
      lang: 'java',
      caption: 'Wait for all, then commit up to first failure',
      src: `
List<CompletableFuture<Void>> futures = recs.stream()
    .map(rec -> CompletableFuture.runAsync(
        () -> tradeService.store(rec), executor))
    .toList();

int firstFailed = -1;
for (int i = 0; i < futures.size(); i++) {       // join in offset order
    try { futures.get(i).join(); }
    catch (CompletionException e) { if (firstFailed < 0) firstFailed = i; }
}
if (firstFailed >= 0)                             // commit < firstFailed
    throw new BatchListenerFailedException("failed", firstFailed);`,
    },
    animation: {
      scene: {
        height: 430,
        nodes: [
          { id: 'cs', kind: 'consumer', label: 'Dispatcher', sub: 'batch 0–99', x: 20, y: 200, w: 140, h: 60 },
          { id: 't1', kind: 'thread', label: 'thread-1', x: 250, y: 150, w: 110, h: 34 },
          { id: 't2', kind: 'thread', label: 'thread-2', x: 250, y: 196, w: 110, h: 34 },
          { id: 't3', kind: 'thread', label: 'thread-3', x: 250, y: 242, w: 110, h: 34 },
          { id: 't4', kind: 'thread', label: 'thread-4', x: 250, y: 288, w: 110, h: 34 },
          { id: 'db', kind: 'db', label: 'PostgreSQL', sub: 'unique(trade_id, version)', x: 470, y: 190, w: 190, h: 84 },
        ],
        edges: [
          { id: 'd1', from: 'cs', to: 't1' },
          { id: 'd2', from: 'cs', to: 't2' },
          { id: 'd3', from: 'cs', to: 't3' },
          { id: 'd4', from: 'cs', to: 't4' },
          { id: 'w1', from: 't1', to: 'db' },
          { id: 'w2', from: 't2', to: 'db' },
          { id: 'w3', from: 't3', to: 'db' },
          { id: 'w4', from: 't4', to: 'db' },
        ],
        logs: [{ id: 'p0', title: 'partition 0 · batch offsets 0–99 (window 46–57 shown)', x: 40, y: 50, cells: ['46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57'], base: 46, cellW: 52 }],
        tables: [{ id: 'res', title: 'insert results (redelivery)', x: 470, y: 330, w: 300, columns: ['offset', 'result'], maxRows: 3 }],
      },
      steps: [
        {
          caption: 'Batch 0–99 polled; work is fanned out to 4 threads.',
          logs: { p0: { marks: range(46, 57, 'active') } },
          tables: { res: [] },
          messages: [
            { edge: 'd1', label: '…' },
            { edge: 'd2', label: '…', delay: 0.5 },
            { edge: 'd3', label: '…', delay: 1 },
            { edge: 'd4', label: '…', delay: 1.5 },
          ],
        },
        {
          caption: 'Threads complete out of order. **51 fails**; 52–57 (and up to 99) succeed.',
          logs: { p0: { marks: { ...range(46, 50, 'ok'), 51: 'fail', ...range(52, 57, 'ok') } } },
          nodes: { t2: { tone: 'fail' } },
          messages: [
            { edge: 'w1', label: '54' },
            { edge: 'w3', label: '49', delay: 0.6 },
            { edge: 'w2', label: '51', tone: 'fail', outcome: 'stop', delay: 1.2 },
            { edge: 'w4', label: '57', delay: 1.8 },
          ],
        },
        {
          caption: 'Highest contiguous success is 50 ⇒ **commit offset 51**. Nothing after the gap is committed.',
          logs: { p0: { committed: 51, marks: { ...range(46, 50, 'ok'), 51: 'fail', ...range(52, 57, 'ok') } } },
          nodes: { t2: {} },
        },
        {
          caption: 'Redelivery from 51: 51 now succeeds; 52–57 hit the unique constraint and are **skipped as duplicates**.',
          logs: { p0: { committed: 51, marks: { ...range(46, 50, 'ok'), 51: 'ok', ...range(52, 57, 'dup') } } },
          messages: [
            { edge: 'w2', label: '51', tone: 'ok' },
            { edge: 'w1', label: '52', tone: 'dup', outcome: 'reject', delay: 1 },
            { edge: 'w3', label: '53', tone: 'dup', outcome: 'reject', delay: 1.6 },
          ],
          tables: { res: [rt('ok', '51', 'inserted'), rt('dup', '52', 'duplicate → skip'), rt('dup', '53', 'duplicate → skip')] },
        },
        {
          caption: 'Batch done ⇒ commit 100. No loss (51 was retried), no duplicates (DB rejected them).',
          logs: { p0: { committed: 58, committedLabel: 'committed=100 (end of batch)', marks: range(46, 57, 'ok') } },
          nodes: { cs: { badge: 'committed → 100' } },
        },
      ],
    },
    notes:
      'The key insight: you cannot commit "51 failed but 52–100 are fine". Kafka only stores one number per partition. So commit the watermark and let idempotency absorb the redelivered tail.\n\nAlternative designs that cancel in-flight work on first failure reduce redelivery but add coordination complexity. Keep the simple version unless redelivery volume is a measured problem.\n\nPer-key ordering: if two records for the same tradeId are in the batch, run them on the same thread (hash by key) or the AMEND may overtake its NEW.',
  },
  {
    id: 'idempotency-constraint',
    title: 'Idempotency: let the database say no',
    subtitle: 'Unique constraint on the business key, checked on insert',
    bullets: [
      'Unique constraint on **`(trade_id, version)`**, the business identity of the event.',
      '`INSERT … ON CONFLICT DO NOTHING`: 0 rows affected ⇒ duplicate ⇒ acknowledge and move on.',
      'Atomic with the business write: same transaction, no race window, no extra infrastructure.',
      'Kafka coordinates are stored as plain columns (next slides), **not** as the uniqueness key.',
    ],
    code: {
      lang: 'sql',
      caption: 'schema + idempotent insert',
      src: `
CREATE TABLE trade_event (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  trade_id       TEXT    NOT NULL,
  version        INT     NOT NULL,
  payload        JSONB   NOT NULL,
  kafka_topic    TEXT    NOT NULL,
  kafka_group    TEXT    NOT NULL,
  kafka_partition INT    NOT NULL,
  kafka_offset   BIGINT  NOT NULL,
  CONSTRAINT uq_trade_version UNIQUE (trade_id, version)
);

INSERT INTO trade_event (...) VALUES (...)
ON CONFLICT ON CONSTRAINT uq_trade_version DO NOTHING;
-- 0 rows affected => duplicate, ack without side effects`,
    },
    animation: {
      scene: {
        height: 340,
        nodes: [
          { id: 'tp', kind: 'topic', label: 'trades', x: 20, y: 60, w: 130, h: 70 },
          { id: 'cs', kind: 'consumer', label: 'trade-store', x: 250, y: 63, w: 150, h: 64 },
          { id: 'db', kind: 'db', label: 'PostgreSQL', sub: 'uq(trade_id, version)', x: 520, y: 52, w: 200, h: 86 },
        ],
        edges: [
          { id: 'e1', from: 'tp', to: 'cs' },
          { id: 'e2', from: 'cs', to: 'db', label: 'INSERT … ON CONFLICT' },
        ],
        tables: [{ id: 'tbl', title: 'trade_event', x: 250, y: 200, w: 470, columns: ['trade_id', 'version', 'offset', 'result'], maxRows: 4 }],
      },
      steps: [
        { caption: 'Table already holds T-1 v1.', tables: { tbl: [r('T-1', '1', '100', 'inserted')] } },
        {
          caption: 'T-1 v2 arrives: new key ⇒ inserted.',
          messages: [
            { edge: 'e1', label: 'T-1 v2' },
            { edge: 'e2', label: 'T-1 v2', delay: 2.6 },
          ],
          tables: { tbl: [r('T-1', '1', '100', 'inserted'), r('T-1', '2', '101', 'inserted')] },
        },
        {
          caption: 'A rebalance redelivers T-1 v2. The constraint rejects it: 0 rows affected.',
          messages: [
            { edge: 'e1', label: 'T-1 v2' },
            { edge: 'e2', label: 'T-1 v2', tone: 'dup', outcome: 'reject', delay: 2.6 },
          ],
          tables: {
            tbl: [r('T-1', '1', '100', 'inserted'), r('T-1', '2', '101', 'inserted'), rt('dup', 'T-1', '2', '101', 'duplicate')],
          },
        },
        {
          caption: 'Consumer acknowledges the duplicate and continues. State unchanged: **effectively once**.',
          nodes: { cs: { tone: 'ok', badge: 'ack, no side effects' } },
          tables: { tbl: [r('T-1', '1', '100', 'inserted'), r('T-1', '2', '101', 'inserted')] },
        },
      ],
    },
    notes:
      'The database already solves concurrency for us: a unique index is checked atomically inside the transaction. No cache, no lock, no race.\n\nImportant: if the listener has other side effects (e.g. sending an email or calling an API), do them only when the insert actually inserted a row, or put them behind an outbox.',
  },
  {
    id: 'offset-dedup-misconception',
    title: 'Why not deduplicate on the Kafka offset?',
    subtitle: 'Offsets identify a position in the log, not a business event',
    bullets: [
      'Unique on `(topic, partition, offset)` catches **consumer redelivery** only.',
      'A **producer duplicate** (retry without idempotence, outbox republish after crash, upstream resend) is written to the log again with a **new offset**.',
      'Offset-keyed dedup accepts it ⇒ the trade is booked twice.',
      'Keep offsets as **columns** for replay and RPO; keep uniqueness on the **business key**.',
    ],
    callout: {
      kind: 'misconception',
      title: '"Offsets are unique, so they make a perfect idempotency key."',
      text: 'They are unique per log position. The same business event can occupy two positions. Only a business key (e.g. `tradeId + version` or a producer-assigned `eventId`) identifies the event.',
    },
    animation: {
      scene: {
        height: 400,
        nodes: [
          { id: 'ob', kind: 'service', label: 'Outbox poller', x: 20, y: 40, w: 150, h: 56 },
          { id: 'cs', kind: 'consumer', label: 'trade-store', sub: 'two tables, two keys', x: 300, y: 160, w: 200, h: 60 },
        ],
        logs: [{ id: 'p0', title: 'trades · partition 0', x: 230, y: 50, cells: ['T-5', 'T-1', 'T-6', 'T-1'], base: 7, cellW: 60 }],
        tables: [
          { id: 'off', title: 'unique(partition, offset)', x: 20, y: 290, w: 370, columns: ['trade', 'v', 'offset', 'result'], widths: [0.8, 0.5, 0.8, 1.6], maxRows: 3 },
          { id: 'biz', title: 'unique(trade_id, version)', x: 410, y: 290, w: 370, columns: ['trade', 'v', 'offset', 'result'], widths: [0.8, 0.5, 0.8, 1.6], maxRows: 3 },
        ],
      },
      steps: [
        {
          caption: 'Outbox publishes T-1 v1 at **offset 8**, then crashes before marking the row as sent.',
          logs: { p0: { marks: { 7: 'muted', 8: 'active', 9: 'muted', 10: 'muted' } } },
          nodes: { ob: { tone: 'fail' } },
          tables: { off: [], biz: [] },
        },
        {
          caption: 'After restart it republishes the same event: it lands at **offset 10**.',
          logs: { p0: { marks: { 7: 'muted', 8: 'active', 9: 'muted', 10: 'warn' } } },
          nodes: { ob: {} },
        },
        {
          caption: 'Both copies are consumed. Offset-keyed table: two different keys ⇒ **both inserted**.',
          tables: {
            off: [r('T-1', '1', '8', 'ok'), rt('fail', 'T-1', '1', '10', 'BOOKED TWICE')],
            biz: [],
          },
        },
        {
          caption: 'Business-keyed table: second copy violates `(T-1, 1)` ⇒ **rejected**.',
          tables: {
            biz: [r('T-1', '1', '8', 'ok'), rt('dup', 'T-1', '1', '10', 'duplicate')],
          },
        },
      ],
    },
    notes:
      'This is the most common idempotency bug we see in reviews. It passes every test that only exercises consumer redelivery, then fails the first time the producer side retries.\n\nThe outbox pattern (chapter 2) guarantees this scenario will happen eventually, by design.',
  },
  {
    id: 'idempotency-alternatives',
    title: 'Other idempotency approaches, and why they come second',
    subtitle: 'Caches are optimisations; transactions do not span Kafka and your DB',
    table: {
      head: ['Approach', 'How it works', 'Disadvantages'],
      rows: [
        ['**Unique constraint on insert** (recommended)', 'DB rejects duplicate business key inside the business transaction.', 'Needs an index; insert cost grows slightly.'],
        ['Read-before-write', '`SELECT` to check existence, then `INSERT`.', 'Race between check and insert under concurrency; 2 round trips. Still needs the constraint.'],
        ['Uniqueness (positive) cache', 'Cache of processed keys (e.g. Redis) checked before DB.', 'Someone must size, expire and warm it; not transactional with the DB; on a miss you still hit the DB.'],
        ['Negative cache (misses)', 'Cache / Bloom filter of keys known **not** to exist; skip the existence check on a definite miss.', 'Must be updated on every insert; false positives fall back to DB; operational burden.'],
        ['Kafka transactions (EOS)', 'Atomic consume-transform-produce within Kafka.', 'Only Kafka → Kafka. A DB write is outside the transaction.'],
        ['XA / two-phase commit', 'Distributed transaction across Kafka and DB.', '**Not supported** by Kafka.'],
      ],
    },
    callout: {
      kind: 'tip',
      title: 'Add a cache only when profiling shows the uniqueness check is the bottleneck.',
      text: 'Even then, keep the constraint: the cache reduces DB load; the constraint guarantees correctness.',
    },
    notes:
      'People reach for Redis first because it feels faster. But it is not in the same transaction as the business write, so it can disagree with the DB after a crash. The unique constraint is the only option that is correct on its own.',
  },
  {
    id: 'commit-order',
    title: 'Database commit first, Kafka offset second',
    subtitle: 'The order decides whether a crash causes a duplicate or a loss',
    bullets: [
      '**DB → offset**: crash in between ⇒ message redelivered ⇒ idempotency rejects the duplicate. **Safe.**',
      '**Offset → DB**: crash in between ⇒ offset already past the message ⇒ it is **never processed**. **Data loss.**',
      'Spring: `@Transactional` service + `ack-mode: record` (or manual ack after the service call).',
      'Avoid `enable.auto.commit=true`: commits on a timer, regardless of processing.',
    ],
    callout: {
      kind: 'rule',
      title: 'A duplicate is recoverable. A gap is not.',
      text: 'Order your commits so every failure produces a duplicate, then make duplicates harmless.',
    },
    code: {
      lang: 'java',
      caption: 'Manual acknowledgement after the DB transaction',
      src: `
@KafkaListener(topics = "trades")   // ack-mode: manual_immediate
public void onTrade(ConsumerRecord<String, TradeEvent> rec, Acknowledgment ack) {
    tradeService.store(rec);   // @Transactional: commits or throws
    ack.acknowledge();         // only reached after DB commit
}`,
    },
    animation: {
      scene: {
        height: 400,
        nodes: [
          { id: 'ra', kind: 'region', label: '✓ DB commit → offset commit', x: 10, y: 10, w: 780, h: 180 },
          { id: 'rb', kind: 'region', label: '✗ offset commit → DB commit', x: 10, y: 210, w: 780, h: 180 },
          { id: 'ca', kind: 'consumer', label: 'consumer', x: 40, y: 70, w: 130, h: 56 },
          { id: 'da', kind: 'db', label: 'DB', x: 340, y: 60, w: 110, h: 76 },
          { id: 'ka', kind: 'topic', label: 'offset', sub: 'committed', x: 600, y: 64, w: 140, h: 68 },
          { id: 'cb', kind: 'consumer', label: 'consumer', x: 40, y: 270, w: 130, h: 56 },
          { id: 'kb', kind: 'topic', label: 'offset', sub: 'committed', x: 340, y: 264, w: 140, h: 68 },
          { id: 'db', kind: 'db', label: 'DB', x: 630, y: 260, w: 110, h: 76 },
        ],
        edges: [
          { id: 'a1', from: 'ca', to: 'da', label: '1. INSERT + COMMIT' },
          { id: 'a2', from: 'da', to: 'ka', label: '2. commit offset' },
          { id: 'b1', from: 'cb', to: 'kb', label: '1. commit offset' },
          { id: 'b2', from: 'kb', to: 'db', label: '2. INSERT + COMMIT' },
        ],
      },
      steps: [
        { caption: 'Two orderings, same crash: the consumer dies between step 1 and step 2.' },
        {
          caption: 'Top: row committed to DB. Bottom: offset committed to Kafka.',
          messages: [
            { edge: 'a1', label: 'T-1 v1' },
            { edge: 'b1', label: 'T-1 v1' },
          ],
          nodes: { da: { tone: 'ok', badge: 'row stored' }, kb: { tone: 'ok', badge: 'offset 101' } },
        },
        {
          caption: '💥 Crash before step 2 in both cases.',
          nodes: { ca: { tone: 'fail' }, cb: { tone: 'fail' } },
          edges: { a2: { tone: 'fail' }, b2: { tone: 'fail' } },
        },
        {
          caption: 'Top: offset not committed ⇒ **redelivered** ⇒ duplicate rejected. Bottom: offset says "done" ⇒ **message lost**.',
          nodes: {
            ca: { tone: 'ok' },
            cb: {},
            ka: { badge: 'redeliver 100 → dup, skip' },
            db: { tone: 'fail', badge: 'row never written' },
          },
          edges: { a2: {}, b2: { tone: 'fail' } },
          messages: [{ edge: 'a1', label: 'T-1 v1', tone: 'dup', outcome: 'reject' }],
        },
      ],
    },
    notes:
      'This is the single most important ordering rule in the deck. Every time you see a consumer that commits offsets on a timer, or before calling the service, ask what happens if the pod is killed right there.\n\nDo not rely on chaining a KafkaTransactionManager with a JDBC transaction manager to make this atomic; it is best-effort synchronisation, not a distributed transaction.',
  },
  {
    id: 'kafka-coordinates',
    title: 'Persist the Kafka coordinates with every row',
    subtitle: 'topic · consumer group · partition · offset',
    bullets: [
      'Store **topic, consumer group id, partition and offset** as columns next to the business data.',
      'They tell you exactly **how far each partition was durably processed**, per consumer group.',
      'At DR: compare max persisted offset per partition with the cluster ⇒ the exact set of unconsumed messages (**the real RPO**).',
      'Replay that range; idempotency absorbs any overlap. Without coordinates DR is guesswork.',
    ],
    code: {
      lang: 'sql',
      caption: 'How far did we get? (per partition)',
      src: `
CREATE INDEX ix_trade_event_coords
  ON trade_event (kafka_group, kafka_topic, kafka_partition, kafka_offset);

SELECT kafka_topic, kafka_partition, MAX(kafka_offset) + 1 AS next_offset
FROM   trade_event
WHERE  kafka_group = 'trade-store'
GROUP  BY kafka_topic, kafka_partition;`,
    },
    animation: {
      scene: {
        height: 360,
        nodes: [
          { id: 'cs', kind: 'consumer', label: 'trade-store', x: 30, y: 40, w: 160, h: 60 },
          { id: 'db', kind: 'db', label: 'PostgreSQL', x: 600, y: 30, w: 150, h: 80 },
        ],
        edges: [{ id: 'w', from: 'cs', to: 'db', label: 'business row + coordinates' }],
        tables: [
          {
            id: 'tbl',
            title: 'trade_event',
            x: 30,
            y: 160,
            w: 720,
            columns: ['trade_id', 'version', 'topic', 'group', 'partition', 'offset'],
            widths: [1, 0.8, 1, 1.3, 1, 1],
          },
        ],
      },
      steps: [
        {
          caption: 'Every insert carries where the record came from.',
          messages: [{ edge: 'w', label: 'T-1 v1 @ p0/1040' }],
          tables: { tbl: [r('T-1', '1', 'trades', 'trade-store', '0', '1040')] },
        },
        {
          caption: 'Records from several partitions accumulate.',
          messages: [
            { edge: 'w', label: 'T-2 v1 @ p1/877' },
            { edge: 'w', label: 'T-1 v2 @ p0/1041', delay: 2 },
          ],
          tables: {
            tbl: [
              r('T-1', '1', 'trades', 'trade-store', '0', '1040'),
              r('T-2', '1', 'trades', 'trade-store', '1', '877'),
              r('T-1', '2', 'trades', 'trade-store', '0', '1041'),
            ],
          },
        },
        {
          caption: 'Query: partition 0 durably processed up to 1041, partition 1 up to 877. **Deterministic restart point.**',
          tables: {
            tbl: [
              r('T-1', '1', 'trades', 'trade-store', '0', '1040'),
              r('T-2', '1', 'trades', 'trade-store', '1', '877'),
              rt('active', 'T-1', '2', 'trades', 'trade-store', '0', '1041'),
            ],
          },
          nodes: { db: { badge: 'p0 → 1042 · p1 → 878' } },
        },
      ],
    },
    notes:
      'These four columns cost a few bytes per row and turn disaster recovery from an investigation into a query. We come back to them in chapter 3.\n\nInclude the consumer group: the same topic is often consumed by several groups writing to different tables.',
  },
];
