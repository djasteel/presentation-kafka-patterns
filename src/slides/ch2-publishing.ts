import type { Chapter } from '../engine/types';
import { r, rt } from './helpers';

export const publishing: Chapter = {
  id: 'publishing',
  title: '2 · Kafka publishing',
  slides: [
    {
      id: 'ch2',
      layout: 'section',
      title: 'Kafka publishing',
      subtitle: 'Partition keys, idempotent producers and the transactional outbox',
      notes: 'The consumer side can only be as consistent as what is published. Chapter 2 is about the producer.',
    },
    {
      id: 'partition-key',
      title: 'The partition key decides your ordering guarantee',
      subtitle: 'Kafka orders messages per partition, and only per partition',
      bullets: [
        'Same key ⇒ same partition ⇒ **strict order** for that key.',
        'Choose the key = the **entity whose events must stay ordered**: `tradeId` for the trade lifecycle, `accountId` for balances.',
        'Too coarse (e.g. `book` or `clientId`) ⇒ big clients create **hot partitions**.',
        'Changing the partition count re-maps keys: plan partitions up front for ordered topics.',
      ],
      callout: {
        kind: 'tip',
        title: 'Key choice checklist',
        text: 'Which events must never overtake each other? Is the key evenly distributed? Will it still be when one client trades 50× more?',
      },
      animation: {
        scene: {
          height: 360,
          nodes: [{ id: 'pr', kind: 'producer', label: 'Booking system', sub: 'key = tradeId', x: 20, y: 140, w: 160, h: 66 }],
          logs: [
            { id: 'p0', title: 'partition 0', x: 330, y: 50, cells: ['T-1 v1', 'T-4 v1', 'T-1 v2', 'T-1 v3'], cellW: 72 },
            { id: 'p1', title: 'partition 1', x: 330, y: 160, cells: ['T-2 v1', 'T-2 v2', 'T-5 v1', ''], cellW: 72 },
            { id: 'p2', title: 'partition 2', x: 330, y: 270, cells: ['T-3 v1', 'T-6 v1', 'T-3 v2', ''], cellW: 72 },
          ],
        },
        steps: [
          { caption: '`hash(tradeId) % 3` picks the partition.', logs: { p0: { marks: {} }, p1: { marks: {} }, p2: { marks: {} } } },
          { caption: 'Every T-1 event lands in partition 0, in publish order.', logs: { p0: { marks: { 0: 'active', 2: 'active', 3: 'active' } } } },
          {
            caption: 'T-2 and T-3 likewise in their partitions. Order across trades is not guaranteed and not needed.',
            logs: { p0: { marks: { 0: 'active', 2: 'active', 3: 'active' } }, p1: { marks: { 0: 'ok', 1: 'ok' } }, p2: { marks: { 0: 'warn', 2: 'warn' } } },
          },
        ],
      },
      notes:
        'Ordering is a property of the key, not of the topic. Anything you want ordered together must share a key.\n\nAdding partitions later changes hash(key) % n for most keys: in-flight events for a trade could land in a different partition from their predecessors. For ordered topics, pick a generous partition count at creation.',
    },
    {
      id: 'null-key',
      title: 'What happens with a null key?',
      subtitle: 'Not a hot partition, but lost ordering',
      bullets: [
        'Since Kafka 2.4 (sticky partitioner) and **3.3 (KIP-794, uniform sticky)**, null-key records are batched to one partition, then the producer **switches**: load spreads evenly over time.',
        'Side effect: related events (NEW, AMEND of the same trade) land in **different partitions** ⇒ consumed in **any order**.',
        '**Hot partitions** come from **skewed non-null keys**: one huge client, one popular instrument.',
        'Null key is fine only for events that are truly independent and order-free.',
      ],
      callout: {
        kind: 'misconception',
        title: '"Null key makes one partition hot."',
        text: 'Not with modern clients: null keys spread evenly. The real damage is silent: per-entity ordering disappears, and an AMEND can be processed before its NEW.',
      },
      animation: {
        scene: {
          height: 380,
          nodes: [
            { id: 'pr', kind: 'producer', label: 'Producer', sub: 'key = null', x: 20, y: 140, w: 140, h: 66 },
            { id: 'cs', kind: 'consumer', label: 'consumer', x: 640, y: 290, w: 140, h: 60 },
          ],
          logs: [
            { id: 'p0', title: 'partition 0', x: 260, y: 50, cells: ['T-1 v1', '', ''], cellW: 76 },
            { id: 'p1', title: 'partition 1', x: 260, y: 150, cells: ['T-1 v2', '', ''], cellW: 76 },
            { id: 'p2', title: 'partition 2', x: 260, y: 250, cells: ['', '', ''], cellW: 76 },
          ],
          tables: [{ id: 'tbl', title: 'processing order', x: 520, y: 60, w: 260, columns: ['#', 'event', 'result'], maxRows: 3 }],
        },
        steps: [
          {
            caption: 'NEW (v1) goes to partition 0; the sticky batch fills and the producer switches. AMEND (v2) lands in partition 1.',
            logs: { p0: { marks: { 0: 'active' } }, p1: { marks: { 0: 'warn' } } },
            tables: { tbl: [] },
          },
          {
            caption: 'Partition 0 is behind (slower consumer, rebalance): partition 1 is processed first.',
            tables: { tbl: [rt('fail', '1', 'T-1 v2 AMEND', 'trade not found')] },
          },
          {
            caption: 'AMEND before NEW ⇒ failure or, worse, a silently wrong state.',
            tables: { tbl: [rt('fail', '1', 'T-1 v2 AMEND', 'trade not found'), r('2', 'T-1 v1 NEW', 'inserted')] },
            nodes: { cs: { tone: 'fail', badge: 'out of order' } },
          },
        ],
      },
      notes:
        'Engineers often pass null "because the topic has one partition today". Then someone scales the topic, and ordering breaks with no code change.\n\nWhen reviewing producers: a null key should come with a comment explaining why ordering does not matter for this event.',
    },
    {
      id: 'idempotent-producer',
      title: 'Idempotent producer: no duplicates from retries',
      subtitle: 'enable.idempotence = true (default since Kafka 3.0)',
      bullets: [
        'The broker tracks **producer id + sequence number** per partition and discards a retried batch it already wrote.',
        'Requires `acks=all`, `retries > 0`, `max.in.flight.requests.per.connection ≤ 5`: ordering is preserved across retries.',
        'Protects against **network-level** retries within one producer session only.',
        'It does **not** protect against application-level resends (restart, outbox republish) ⇒ consumers still need idempotency.',
      ],
      code: {
        lang: 'yaml',
        caption: 'application.yml: make the defaults explicit',
        src: `
spring:
  kafka:
    producer:
      acks: all
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
        delivery.timeout.ms: 120000`,
      },
      animation: {
        scene: {
          height: 320,
          nodes: [
            { id: 'pr', kind: 'producer', label: 'Producer', sub: 'PID 42', x: 30, y: 120, w: 150, h: 66 },
            { id: 'br', kind: 'topic', label: 'Broker', sub: 'partition 0', x: 560, y: 115, w: 180, h: 76 },
          ],
          edges: [
            { id: 'send', from: 'pr', to: 'br', label: 'send', curve: -50 },
            { id: 'ack', from: 'br', to: 'pr', label: 'ack', curve: -50, dashed: true },
          ],
          tables: [{ id: 'log', title: 'partition 0 log', x: 330, y: 230, w: 300, columns: ['offset', 'event', 'pid/seq'], maxRows: 3 }],
        },
        steps: [
          {
            caption: 'Producer sends T-1 v1 with sequence 7.',
            messages: [{ edge: 'send', label: 'T-1 v1 · seq 7' }],
            tables: { log: [r('900', 'T-1 v1', '42/7')] },
          },
          {
            caption: 'Broker writes it, but the **ack is lost** on the network.',
            messages: [{ edge: 'ack', label: 'ack', outcome: 'stop', tone: 'fail' }],
          },
          {
            caption: 'Producer retries the same batch (same seq 7).',
            messages: [{ edge: 'send', label: 'T-1 v1 · seq 7', tone: 'warn' }],
          },
          {
            caption: 'Broker sees seq 7 already written for PID 42 ⇒ **discards the duplicate**, acks again.',
            messages: [{ edge: 'ack', label: 'ack (dup)', tone: 'ok' }],
            nodes: { br: { tone: 'ok', badge: 'dedup by PID+seq' } },
          },
        ],
      },
      notes: 'It is on by default in modern clients unless conflicting settings disable it (e.g. acks=1). Make it explicit in config so a future change cannot silently turn it off.',
    },
    {
      id: 'dual-write',
      title: 'The dual-write problem',
      subtitle: 'Write to the DB and publish to Kafka: two systems, no shared transaction',
      bullets: [
        'Service stores the trade **and** publishes `TradeBooked` downstream.',
        'Commit DB, then publish ⇒ crash in between ⇒ **event lost**; downstream never learns of the trade.',
        'Publish, then commit DB ⇒ DB rollback ⇒ **phantom event** for a trade that does not exist.',
        'Retrying in-process does not help: the process is the thing that crashed.',
      ],
      animation: {
        scene: {
          height: 320,
          nodes: [
            { id: 'sv', kind: 'service', label: 'trade-service', x: 30, y: 120, w: 160, h: 66 },
            { id: 'db', kind: 'db', label: 'PostgreSQL', x: 400, y: 30, w: 140, h: 76 },
            { id: 'kf', kind: 'topic', label: 'trade-booked', x: 400, y: 210, w: 150, h: 66 },
            { id: 'ds', kind: 'consumer', label: 'Settlement', x: 630, y: 210, w: 140, h: 66 },
          ],
          edges: [
            { id: 'w', from: 'sv', to: 'db', label: '1. commit' },
            { id: 'p', from: 'sv', to: 'kf', label: '2. publish' },
            { id: 'c', from: 'kf', to: 'ds' },
          ],
        },
        steps: [
          { caption: 'Two writes, two systems.' },
          { caption: 'Step 1: trade committed in the DB.', messages: [{ edge: 'w', label: 'T-1' }], nodes: { db: { tone: 'ok', badge: 'T-1 stored' } } },
          {
            caption: '💥 Crash before step 2. The event is never published.',
            nodes: { sv: { tone: 'fail' } },
            edges: { p: { tone: 'fail' }, c: { tone: 'muted' } },
          },
          {
            caption: 'DB says the trade exists; settlement never hears of it. A silent break, found at reconciliation.',
            nodes: { ds: { tone: 'warn', badge: 'T-1 unknown' } },
          },
        ],
      },
      notes: 'This is the general case of the commit-order problem, now on the producer side. The fix is to make the event part of the DB transaction: the transactional outbox.',
    },
    {
      id: 'outbox',
      title: 'Transactional outbox with a Spring Boot poller',
      subtitle: 'Event written in the same transaction as the business data; published later',
      bullets: [
        'In **one DB transaction**: insert the trade **and** an `outbox` row with the event.',
        'A `@Scheduled` poller (e.g. every 30 s, or faster) reads unpublished rows **in id order**, publishes, waits for acks, marks them published.',
        'Crash after publish, before marking ⇒ rows republished ⇒ **duplicates are possible** (consumer idempotency handles them).',
        'Consistency guaranteed; latency = poll interval. Shorten it, or wake the poller after commit, if needed.',
      ],
      code: {
        lang: 'java',
        caption: 'OutboxPoller.java',
        src: `
@Scheduled(fixedDelayString = "PT30S")
@Transactional
public void publishPending() {
    List<OutboxRow> rows = outbox.lockNextBatch(500);   // FOR UPDATE SKIP LOCKED
    var acks = rows.stream()
        .map(r -> kafka.send(r.topic(), r.messageKey(), r.payload()))
        .toList();                                       // pipelined, in order
    acks.forEach(CompletableFuture::join);               // all acked by broker
    outbox.markPublished(rows);                          // commit after acks
}`,
      },
      animation: {
        scene: {
          height: 420,
          nodes: [
            { id: 'sv', kind: 'service', label: 'trade-service', x: 20, y: 40, w: 160, h: 60 },
            { id: 'pl', kind: 'service', label: 'Outbox poller', sub: '@Scheduled 30s', x: 20, y: 300, w: 170, h: 66 },
            { id: 'kf', kind: 'topic', label: 'trade-booked', x: 620, y: 300, w: 150, h: 66 },
          ],
          edges: [
            { id: 'tx', from: 'sv', to: 'pl', dashed: true, label: 'same DB' },
            { id: 'pub', from: 'pl', to: 'kf', label: 'publish' },
          ],
          tables: [
            { id: 'tr', title: 'trade', x: 240, y: 40, w: 220, columns: ['trade_id', 'version'], maxRows: 3 },
            { id: 'ob', title: 'outbox', x: 240, y: 170, w: 540, columns: ['id', 'key', 'event', 'published_at'], maxRows: 3 },
          ],
        },
        steps: [
          { caption: 'Nothing pending.', tables: { tr: [], ob: [] } },
          {
            caption: '**One transaction**: trade row + outbox row commit together, or neither does.',
            tables: { tr: [r('T-1', '1')], ob: [rt('warn', '71', 'T-1', 'TradeBooked v1', 'NULL')] },
          },
          {
            caption: 'Poller picks up unpublished rows in id order and publishes them.',
            messages: [{ edge: 'pub', label: 'T-1 v1' }],
            nodes: { pl: { tone: 'active' } },
          },
          {
            caption: 'After the broker acks, rows are marked published.',
            tables: { ob: [rt('ok', '71', 'T-1', 'TradeBooked v1', '10:00:30')] },
            nodes: { pl: {} },
          },
          {
            caption: 'If the poller crashes between publish and mark, the next run republishes row 71: a **duplicate**, never a loss.',
            tables: { ob: [rt('warn', '71', 'T-1', 'TradeBooked v1', 'NULL')] },
            messages: [{ edge: 'pub', label: 'T-1 v1 (again)', tone: 'dup' }],
            nodes: { kf: { badge: 'consumer dedups' } },
          },
        ],
      },
      notes:
        'The outbox turns the dual-write problem into the at-least-once problem we already know how to solve.\n\nClean up published rows with a scheduled DELETE or by dropping old partitions; keep them long enough to help RCA (e.g. the same 14 days as topic retention).',
    },
    {
      id: 'outbox-options',
      title: 'Outbox relay: Debezium or a Spring Boot poller?',
      subtitle: 'For quick results and simplicity, start with the poller',
      table: {
        head: ['', 'Spring Boot poller', 'Debezium (CDC)'],
        rows: [
          ['How', '`@Scheduled` query on the outbox table', 'Reads PostgreSQL WAL via logical replication'],
          ['Latency', 'Poll interval (seconds)', 'Sub-second'],
          ['DB load', 'Periodic indexed query', 'Replication slot; WAL retained if connector lags'],
          ['Infrastructure', 'None beyond the app', 'Kafka Connect cluster, connector config, slot monitoring'],
          ['Ownership', 'Application team, plain Java', 'Platform team usually involved'],
          ['Duplicates', 'Possible after crash', 'Possible after connector restart'],
          ['Choose when', '**Default**: most flows tolerate seconds', 'High volume or latency-sensitive flows'],
        ],
      },
      callout: {
        kind: 'warning',
        title: 'A stalled Debezium replication slot makes PostgreSQL retain WAL until the disk fills.',
        text: 'If you adopt CDC, monitor slot lag as a first-class production metric.',
      },
      notes: 'Both options give at-least-once publishing. The poller wins on simplicity and team autonomy; Debezium wins on latency and DB-friendliness at high volume.',
    },
    {
      id: 'outbox-skip-locked',
      title: 'Several poller instances: SKIP LOCKED',
      subtitle: 'Share the work without double-publishing',
      bullets: [
        '`SELECT … FOR UPDATE SKIP LOCKED`: each instance locks a different set of rows; locked rows are skipped, not waited on.',
        'No external lock library; scales out with the service.',
        '**Ordering caveat**: two instances could publish two events for the same key concurrently, out of order.',
        'Keep per-key order by locking **shards** of the outbox (shard = hash of key) instead of individual rows.',
      ],
      code: {
        lang: 'sql',
        caption: 'Lock shards, then publish their rows in id order',
        src: `
-- outbox.shard = abs(hashtext(message_key)) % 16, set on insert
WITH my_shards AS (
  SELECT shard FROM outbox_shard
  ORDER BY shard
  LIMIT 4
  FOR UPDATE SKIP LOCKED          -- other instances take other shards
)
SELECT o.* FROM outbox o
WHERE  o.shard IN (SELECT shard FROM my_shards)
AND    o.published_at IS NULL
ORDER  BY o.id
LIMIT  500;`,
      },
      animation: {
        scene: {
          height: 410,
          nodes: [
            { id: 'p1', kind: 'service', label: 'poller A', x: 40, y: 240, w: 140, h: 56 },
            { id: 'p2', kind: 'service', label: 'poller B', x: 40, y: 330, w: 140, h: 56 },
            { id: 'kf', kind: 'topic', label: 'trade-booked', x: 600, y: 280, w: 160, h: 70 },
          ],
          edges: [
            { id: 'a', from: 'p1', to: 'kf' },
            { id: 'b', from: 'p2', to: 'kf' },
          ],
          tables: [
            { id: 'sh', title: 'outbox_shard (lock)', x: 20, y: 40, w: 340, columns: ['shard', 'locked by'], maxRows: 4 },
            { id: 'ob', title: 'outbox (unpublished)', x: 400, y: 40, w: 380, columns: ['id', 'key', 'shard'], maxRows: 4 },
          ],
        },
        steps: [
          {
            caption: 'Four unpublished rows across two shards.',
            tables: {
              sh: [r('0', '–'), r('1', '–')],
              ob: [r('71', 'T-1', '0'), r('72', 'T-2', '1'), r('73', 'T-1', '0'), r('74', 'T-2', '1')],
            },
          },
          {
            caption: 'A locks shard 0; B tries shard 0, **skips** it, locks shard 1.',
            tables: {
              sh: [rt('active', '0', 'poller A'), rt('warn', '1', 'poller B')],
              ob: [rt('active', '71', 'T-1', '0'), rt('warn', '72', 'T-2', '1'), rt('active', '73', 'T-1', '0'), rt('warn', '74', 'T-2', '1')],
            },
          },
          {
            caption: 'Each publishes its rows in id order: all T-1 events by A, all T-2 events by B. Order per key preserved.',
            messages: [
              { edge: 'a', label: 'T-1 #71' },
              { edge: 'b', label: 'T-2 #72' },
              { edge: 'a', label: 'T-1 #73', delay: 2.5 },
              { edge: 'b', label: 'T-2 #74', delay: 2.5 },
            ],
          },
          {
            caption: 'Commit releases the shard locks; the next run may pick different shards.',
            tables: { sh: [r('0', '–'), r('1', '–')], ob: [] },
          },
        ],
      },
      notes:
        'Row-level SKIP LOCKED is enough when events are independent. For ordered topics, the shard-lock variant keeps all events of a key on one poller per run while still letting instances share load.',
    },
  ],
};
