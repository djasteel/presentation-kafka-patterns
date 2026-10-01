import type { Chapter } from '../engine/types';
import { r, rt } from './helpers';

export const resiliency: Chapter = {
  id: 'resiliency',
  title: '3 · Availability & resiliency',
  slides: [
    {
      id: 'ch3',
      layout: 'section',
      title: 'Availability & resiliency',
      subtitle: 'Region failover with a deterministic, provable recovery point',
      notes: 'Everything so far pays off here: idempotency plus persisted Kafka coordinates make DR a procedure instead of an investigation.',
    },
    {
      id: 'stretched-cluster',
      title: 'Topology: one Kafka cluster stretched across two Azure regions',
      subtitle: 'Kafka replicates data and offsets; your database replicates separately',
      bullets: [
        'Brokers in both regions; topics replicated with `min.insync.replicas` spanning regions.',
        'Committed offsets live in the internal topic `__consumer_offsets`, **replicated like any other topic**.',
        'Consumers and the primary PostgreSQL run in the **active region**; the DB replicates to the DR region (often asynchronously).',
        'Two independent replication streams ⇒ after a failure they can disagree. That gap is your RPO problem.',
      ],
      animation: {
        scene: {
          height: 420,
          nodes: [
            { id: 'ra', kind: 'region', label: 'Region A (active)', x: 10, y: 10, w: 380, h: 400 },
            { id: 'rb', kind: 'region', label: 'Region B (DR)', x: 410, y: 10, w: 380, h: 400 },
            { id: 'ka', kind: 'topic', label: 'Brokers A', sub: 'trades, __consumer_offsets', x: 40, y: 60, w: 200, h: 80 },
            { id: 'kb', kind: 'topic', label: 'Brokers B', sub: 'trades, __consumer_offsets', x: 560, y: 60, w: 200, h: 80 },
            { id: 'ca', kind: 'consumer', label: 'trade-store', x: 40, y: 200, w: 150, h: 56 },
            { id: 'cb', kind: 'consumer', label: 'trade-store', sub: 'standby', x: 610, y: 200, w: 150, h: 56 },
            { id: 'da', kind: 'db', label: 'PostgreSQL', sub: 'primary', x: 40, y: 310, w: 150, h: 80 },
            { id: 'db', kind: 'db', label: 'PostgreSQL', sub: 'replica', x: 610, y: 310, w: 150, h: 80 },
          ],
          edges: [
            { id: 'kr', from: 'ka', to: 'kb', label: 'Kafka replication' },
            { id: 'c1', from: 'ka', to: 'ca' },
            { id: 'w1', from: 'ca', to: 'da' },
            { id: 'dr', from: 'da', to: 'db', label: 'DB replication (async)' },
          ],
        },
        steps: [
          { caption: 'Active consumer and primary DB in region A.', nodes: { cb: { tone: 'muted' } } },
          {
            caption: 'Messages are consumed and stored; offsets committed to `__consumer_offsets`.',
            messages: [
              { edge: 'c1', label: 'T-1' },
              { edge: 'w1', label: 'T-1', delay: 2.6 },
            ],
          },
          {
            caption: 'Kafka replicates topic data **and** committed offsets to region B.',
            messages: [{ edge: 'kr', label: 'data + offsets' }],
          },
          {
            caption: 'PostgreSQL replicates rows to region B on its **own** schedule. Two streams, two lags.',
            messages: [{ edge: 'dr', label: 'WAL' }],
            nodes: { db: { badge: 'lag: seconds' }, kb: { badge: 'lag: ms' } },
          },
        ],
      },
      notes:
        'Inside one stretched cluster, offset replication lag is tiny but not zero. The bigger risk is the database: asynchronous replication means the DR replica can be behind what Kafka believes was committed.\n\nA stretched cluster also needs a tie-breaker (third zone or region) for the KRaft controller quorum. Check how your platform team handles that.',
    },
    {
      id: 'source-of-truth',
      title: 'The database offsets are the source of truth',
      subtitle: 'Kafka says "committed up to 1050". The DR database only has up to 1032.',
      bullets: [
        'The consumer committed offsets to Kafka **after** each DB commit in region A: correct.',
        'But the **DR replica** received rows only up to offset 1032 before region A failed.',
        'Resume from Kafka\'s committed offset (1050) ⇒ offsets **1033–1049 are lost** in the DR database.',
        'Resume from **max persisted offset in the DR DB + 1** ⇒ nothing lost; any overlap is rejected by idempotency.',
      ],
      callout: {
        kind: 'rule',
        title: 'After failover, never trust the committed offset alone.',
        text: 'Restart each partition from the offset the surviving database can prove it has stored.',
      },
      animation: {
        scene: {
          height: 380,
          nodes: [
            { id: 'kf', kind: 'topic', label: '__consumer_offsets', sub: 'trade-store / p0', x: 30, y: 40, w: 200, h: 70 },
            { id: 'dbb', kind: 'db', label: 'DR PostgreSQL', sub: 'max offset p0', x: 30, y: 240, w: 200, h: 84 },
          ],
          logs: [
            {
              id: 'p0',
              title: 'trades · partition 0',
              x: 290,
              y: 150,
              cells: ['1030', '1031', '1032', '1033', '…', '1048', '1049', '1050'],
              cellW: 58,
              showOffsets: false,
            },
          ],
        },
        steps: [
          { caption: 'Region A has failed. What does each system believe?', nodes: { kf: {}, dbb: {} } },
          {
            caption: 'Kafka: group committed up to **1050** (next to read).',
            nodes: { kf: { tone: 'active', badge: 'committed = 1050' } },
            logs: { p0: { marks: { 0: 'ok', 1: 'ok', 2: 'ok', 3: 'ok', 4: 'ok', 5: 'ok', 6: 'ok' } } },
          },
          {
            caption: 'DR database: highest stored offset is **1032**. Rows 1033–1049 never reached the replica.',
            nodes: { dbb: { tone: 'warn', badge: 'max(offset) = 1032' } },
            logs: { p0: { marks: { 0: 'ok', 1: 'ok', 2: 'ok', 3: 'fail', 4: 'fail', 5: 'fail', 6: 'fail' } } },
          },
          {
            caption: 'Gap = **17 messages**: that is the exact RPO for this partition, and exactly what to replay.',
            nodes: { kf: { tone: 'muted', badge: 'committed = 1050' } },
            logs: { p0: { marks: { 0: 'ok', 1: 'ok', 2: 'ok', 3: 'warn', 4: 'warn', 5: 'warn', 6: 'warn' } } },
          },
        ],
      },
      notes:
        'The offsets stored with the data are the only offsets that are guaranteed consistent with the data. That is why chapter 1 insisted on persisting them.\n\nThe opposite case also happens: the DB is ahead of Kafka\'s committed offset (offset commit lost). Then resuming from Kafka causes duplicates, which idempotency already handles.',
    },
    {
      id: 'dr-runbook',
      title: 'DR runbook: replay from the persisted offsets',
      subtitle: 'RPO = 0 means being ready to replay, deterministically',
      bullets: [
        '**1.** Stop consumers in both regions (no one consuming while offsets are reset).',
        '**2.** Query the DR DB: `max(kafka_offset) + 1` per topic / partition / group.',
        '**3.** Compare with the group\'s committed offsets and log-end offsets ⇒ gap per partition (record it: this is your measured RPO).',
        '**4.** Reset the group to the DB-derived offsets (`kafka-consumer-groups --reset-offsets --from-file`, or an `AdminClient` job).',
        '**5.** Start consumers in region B. Idempotency rejects any overlap. Verify lag returns to zero.',
      ],
      code: {
        lang: 'bash',
        caption: 'steps 2 + 4',
        src: `
# 2. restart points from the surviving database
psql -At -F, -c "SELECT kafka_topic, kafka_partition, MAX(kafka_offset)+1
                 FROM trade_event WHERE kafka_group='trade-store'
                 GROUP BY 1,2" > reset.csv

# 4. dry run first, then execute (group must have no active members)
kafka-consumer-groups --bootstrap-server $BOOTSTRAP --group trade-store \\
  --reset-offsets --from-file reset.csv --dry-run
kafka-consumer-groups --bootstrap-server $BOOTSTRAP --group trade-store \\
  --reset-offsets --from-file reset.csv --execute`,
      },
      animation: {
        scene: {
          height: 400,
          nodes: [
            { id: 'rb', kind: 'region', label: 'Region B (now active)', x: 10, y: 10, w: 780, h: 380 },
            { id: 'dbb', kind: 'db', label: 'DR PostgreSQL', x: 40, y: 60, w: 160, h: 84 },
            { id: 'ops', kind: 'service', label: 'Runbook script', x: 300, y: 70, w: 170, h: 60 },
            { id: 'kb', kind: 'topic', label: 'Brokers B', sub: 'group trade-store', x: 580, y: 64, w: 180, h: 72 },
            { id: 'cb', kind: 'consumer', label: 'trade-store', x: 580, y: 260, w: 180, h: 60 },
          ],
          edges: [
            { id: 'q', from: 'dbb', to: 'ops', label: 'max(offset)+1' },
            { id: 'rs', from: 'ops', to: 'kb', label: 'reset offsets' },
            { id: 'rp', from: 'kb', to: 'cb', label: 'replay' },
            { id: 'wr', from: 'cb', to: 'dbb', curve: 40, label: 'insert / dedup' },
          ],
          tables: [{ id: 'gap', title: 'gap report', x: 40, y: 220, w: 420, columns: ['partition', 'DB next', 'Kafka committed', 'gap'], widths: [0.9, 0.9, 1.4, 0.6], maxRows: 3 }],
        },
        steps: [
          { caption: 'Consumers stopped. Region B DB and brokers available.', nodes: { cb: { tone: 'muted' } }, tables: { gap: [] } },
          {
            caption: 'Query restart points from the DR database.',
            messages: [{ edge: 'q', label: 'p0:1033 p1:878 p2:2210' }],
          },
          {
            caption: 'Compare with Kafka committed offsets: per-partition gap = measured RPO.',
            tables: { gap: [rt('warn', 'p0', '1033', '1050', '17'), r('p1', '878', '878', '0'), rt('warn', 'p2', '2210', '2214', '4')] },
          },
          {
            caption: 'Reset the consumer group to the DB-derived offsets.',
            messages: [{ edge: 'rs', label: '--reset-offsets' }],
            nodes: { kb: { tone: 'active', badge: 'p0→1033 p2→2210' } },
          },
          {
            caption: 'Start consumers: 21 messages replayed and stored; any overlap rejected. Lag → 0.',
            nodes: { cb: { tone: 'ok' }, kb: {} },
            messages: [
              { edge: 'rp', label: '1033…' },
              { edge: 'wr', label: 'stored', tone: 'ok', delay: 2.6 },
            ],
          },
        ],
      },
      notes:
        'Rehearse this. A runbook that has never been executed is a hypothesis. Run it in a DR test with production-like volumes and time it.\n\nThe 14-day retention matters here: the replay source is the topic itself, so the messages must still be there.',
    },
    {
      id: 'replay-tooling',
      title: 'Replay tooling: from script to support UI',
      subtitle: 'Without persisted coordinates, every option is guesswork',
      table: {
        head: ['Option', 'Good for', 'Watch out'],
        rows: [
          ['**CLI script** (`kafka-consumer-groups --reset-offsets`)', 'DR runbook; simple, auditable, no extra service', 'Group must be inactive; manual input errors; use `--dry-run`'],
          ['**AdminClient job** (Spring Boot command)', 'Automated DR step computing offsets straight from the DB', 'Needs tests and access controls like any prod tool'],
          ['**Support UI** (internal app)', 'Day-to-day: replay a time window, a key, a parked message; show gaps', 'A maturity step: build it once the script is proven'],
          ['Time-based reset (`--to-datetime`)', 'Rough recovery when coordinates are missing', 'Imprecise ⇒ large overlaps or gaps; last resort'],
        ],
      },
      callout: {
        kind: 'warning',
        title: 'No coordinates in the DB ⇒ non-deterministic recovery.',
        text: 'You are left estimating from timestamps and reconciling by hand. Persisting topic / group / partition / offset is what makes DR and consistency checks a query.',
      },
      notes: 'Start with the script in the runbook. Graduate to a support UI when replay requests become routine (parked messages, upstream corrections, partial reprocessing).',
    },
    {
      id: 'retention-archiving',
      title: 'Retention and archiving',
      subtitle: 'The topic is a 14-day replay buffer; the database and ADLS are the record',
      bullets: [
        '**14-day topic retention** (proposed default): covers incident discovery after a long weekend, RCA and DR replay.',
        'Consumers persist **every message + Kafka coordinates** to PostgreSQL and archive to **ADLS**: that is the long-term (regulatory) record.',
        'Do not stretch topic retention to years to satisfy regulation: Kafka is not a queryable archive.',
        'Retention ≥ the longest time you could need to replay: DR, delayed consumers, reprocessing after a bug fix.',
      ],
      callout: {
        kind: 'rule',
        title: 'Kafka is not your system of record.',
        text: 'It is a transport with a replay window. What happened is proven from the database and the archive.',
      },
      code: {
        lang: 'bash',
        caption: 'topic config',
        src: `
kafka-configs --bootstrap-server $BOOTSTRAP --alter \\
  --entity-type topics --entity-name trades \\
  --add-config retention.ms=1209600000   # 14 days`,
      },
      animation: {
        scene: {
          height: 340,
          nodes: [
            { id: 'cs', kind: 'consumer', label: 'trade-store', x: 300, y: 150, w: 160, h: 60 },
            { id: 'db', kind: 'db', label: 'PostgreSQL', sub: 'queryable record', x: 600, y: 60, w: 170, h: 84 },
            { id: 'ad', kind: 'db', label: 'ADLS archive', sub: 'years', x: 600, y: 220, w: 170, h: 84 },
          ],
          edges: [
            { id: 'w', from: 'cs', to: 'db' },
            { id: 'a', from: 'cs', to: 'ad' },
          ],
          logs: [{ id: 'tp', title: 'trades · retention window (days)', x: 20, y: 50, cells: ['d-16', 'd-15', 'd-14', '…', 'd-1', 'today'], cellW: 44 }],
        },
        steps: [
          {
            caption: 'Messages older than 14 days are deleted from the topic.',
            logs: { tp: { marks: { 0: 'muted', 1: 'muted', 2: 'active', 3: 'active', 4: 'active', 5: 'active' } } },
          },
          {
            caption: 'Every message was persisted and archived on consumption.',
            messages: [
              { edge: 'w', label: 'T-1 + coords' },
              { edge: 'a', label: 'raw event', delay: 1 },
            ],
          },
          {
            caption: 'Replay within 14 days from the topic; history beyond that from DB / ADLS.',
            nodes: { db: { tone: 'ok' }, ad: { tone: 'ok' } },
          },
        ],
      },
      notes: 'Archiving mechanics beyond the consumer (tiered storage, a Connect sink to ADLS) are listed in "further topics". The principle stays: the long-term record is outside Kafka.',
    },
  ],
};
