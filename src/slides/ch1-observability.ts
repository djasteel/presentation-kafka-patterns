import type { Slide } from '../engine/types';

// Chapter 1, part C: performance and monitoring.

export const consumptionC: Slide[] = [
  {
    id: 'consumer-tps',
    title: 'Consumer throughput with PostgreSQL',
    subtitle: 'Know your numbers before production does',
    bullets: [
      'Per-message cost is dominated by **DB round trips and commits**, not by Kafka.',
      'Batching (multi-row insert, one transaction) multiplies throughput, at the price of the error-handling complexity seen earlier.',
      'Executor-based concurrency helps when processing is CPU- or IO-bound outside the DB; it does not beat a well-batched insert.',
      'Measure in **performance tests** with production-like data volume, indexes and network placement.',
    ],
    chart: 'insertTps',
    notes:
      'Replace these values with your own measurements in src/data/perf-data.json. The shape of the chart matters more than the absolute numbers: the gap between single-record and batched inserts, and where your actual traffic sits.',
  },
  {
    id: 'insert-indexes',
    title: 'Optimise the insert: index organisation',
    subtitle: 'Every index is paid for on every insert',
    bullets: [
      'The idempotency constraint `(trade_id, version)` is **mandatory**: budget for it.',
      'Prefer **monotonic** primary keys (identity / time-ordered UUIDv7); random UUIDs scatter writes across the B-tree.',
      'Reporting indexes belong on a **read replica** or a reporting store, not on the ingest table.',
      'Use `JSONB` for the raw payload; extract only the columns you query.',
      'Partition very large tables by time (e.g. monthly) to keep indexes hot and archiving cheap.',
    ],
    chart: 'indexImpact',
    code: {
      lang: 'sql',
      caption: 'Lean ingest table',
      src: `
-- monotonic PK, one unique business index, coordinates index for DR
CREATE TABLE trade_event (...) PARTITION BY RANGE (received_at);
CREATE UNIQUE INDEX uq_trade_version
  ON trade_event (trade_id, version, received_at);
CREATE INDEX ix_coords
  ON trade_event (kafka_group, kafka_topic, kafka_partition, kafka_offset);`,
    },
    notes:
      'On a partitioned table, PostgreSQL requires unique indexes to include the partition key. Factor that into the idempotency design: either partition by something derived from the business key, or include received_at and accept that uniqueness is per partition window.',
  },
  {
    id: 'colocation',
    title: 'Co-locate the consumer and its database',
    subtitle: 'Cross-region latency is added to every round trip, on every message',
    bullets: [
      'A message typically needs **2–3 DB round trips** (check / insert / commit). Each pays the network RTT.',
      'Same zone ≈ sub-millisecond; cross-region adds **tens of ms** per round trip ⇒ throughput per consumer thread collapses.',
      'In a stretched setup, run the active consumer **in the same region as the primary DB**; fail both over together.',
      'Watch for "accidental" cross-region traffic after a partial failover: consumer moved, DB did not.',
    ],
    chart: 'regionLatency',
    notes:
      'Simple arithmetic: at 35 ms per message, one consumer thread cannot exceed ~28 msg/s. No amount of Kafka tuning fixes that; placement does.',
  },
  {
    id: 'monitoring-architecture',
    title: 'Monitoring: metrics and traces, from the first performance test',
    subtitle: 'Micrometer → Prometheus → Grafana for metrics · App Insights for traces',
    bullets: [
      '**Throughput** (TPS), **success / error rate**, **consumer lag per partition**, **rebalance rate**: on one dashboard.',
      'Alerts fire on **rates crossing thresholds** (e.g. error rate > 1% for 5 min), not on single errors.',
      '**Traces** (OpenTelemetry → App Insights) split each message into spans: Kafka poll, DB insert, ADLS write.',
      'Set it up for **performance tests**, not just production: that is where you learn what normal looks like.',
    ],
    code: {
      lang: 'yaml',
      caption: 'application.yml',
      src: `
management:
  endpoints.web.exposure.include: health,prometheus
  metrics.tags.application: trade-store
  tracing.sampling.probability: 0.1
spring:
  kafka:
    listener:
      observation-enabled: true   # spans + timers per record
    template:
      observation-enabled: true`,
    },
    animation: {
      scene: {
        height: 400,
        nodes: [
          { id: 'app', kind: 'consumer', label: 'trade-store', sub: 'Micrometer + OTel', x: 20, y: 160, w: 170, h: 70 },
          { id: 'pr', kind: 'service', label: 'Prometheus', x: 300, y: 60, w: 150, h: 56 },
          { id: 'gf', kind: 'service', label: 'Grafana', sub: 'dashboards + alerts', x: 570, y: 60, w: 180, h: 60 },
          { id: 'ai', kind: 'service', label: 'App Insights', sub: 'distributed traces', x: 300, y: 280, w: 170, h: 60 },
          { id: 'oc', kind: 'note', label: 'on-call', x: 660, y: 190, w: 80, h: 30 },
        ],
        edges: [
          { id: 'm1', from: 'app', to: 'pr', label: 'scrape /prometheus' },
          { id: 'm2', from: 'pr', to: 'gf' },
          { id: 't1', from: 'app', to: 'ai', label: 'OTLP spans' },
          { id: 'al', from: 'gf', to: 'oc', dashed: true, label: 'error rate > 1%' },
        ],
      },
      steps: [
        { caption: 'The application exposes metrics and emits spans.' },
        {
          caption: 'Metrics path: TPS, error rate, lag, rebalances scraped into Prometheus and graphed in Grafana.',
          messages: [
            { edge: 'm1', label: 'tps, lag…' },
            { edge: 'm2', label: 'query', delay: 2.6 },
          ],
        },
        {
          caption: 'Trace path: each record becomes a trace with Kafka / DB / ADLS child spans.',
          messages: [{ edge: 't1', label: 'trace' }],
        },
        {
          caption: 'Alert only when a **rate** crosses a threshold: no paging on a single transient error.',
          messages: [{ edge: 'al', label: 'alert', tone: 'fail' }],
          nodes: { gf: { tone: 'warn', badge: 'error rate 2.3%' } },
        },
      ],
    },
    notes:
      'Two complementary views: metrics tell you THAT something is wrong (cheap, aggregated, alertable); traces tell you WHERE (per message, sampled).\n\nAgree alert thresholds during performance testing, when you can see the baseline error rate and latency distribution.',
  },
  {
    id: 'traces',
    title: 'Traces: make Kafka vs DB vs other work visible',
    subtitle: 'An RCA should never start with "is it Kafka or the database?"',
    bullets: [
      'Spring Kafka observation creates a span per received record; JDBC and HTTP/ADLS clients add child spans.',
      'Name spans by operation: `kafka.receive trades`, `db.insert trade_event`, `adls.write archive`.',
      'p99 by span shows the real bottleneck; often not Kafka at all (here: the ADLS archive write).',
      'Propagate trace context in **message headers** so a trace spans producer → topic → consumer.',
    ],
    chart: 'processingLatency',
    notes:
      'In our experience the first trace dashboard usually surprises the team: Kafka poll time is negligible, and a side write (archive, audit, HTTP call) dominates the tail latency.',
  },
  {
    id: 'lag-rebalance-metrics',
    title: 'Consumer lag and rebalance metrics in Java / Spring Boot',
    subtitle: 'Per partition, per consumer group',
    bullets: [
      '**Client-side** (free with Spring Boot + Micrometer): `kafka.consumer.fetch.manager.records.lag` (per partition), `kafka.consumer.coordinator.rebalance.rate.per.hour`, `…rebalance.latency.avg`, `…last.rebalance.seconds.ago`.',
      '**Server-side truth** via Kafka `AdminClient`: committed offset vs log-end offset ⇒ lag that still reports when the consumer is **down** (client metrics then disappear).',
      'Log every partition assignment / revocation (`ConsumerRebalanceListener`): invaluable in RCA timelines.',
    ],
    code: {
      lang: 'java',
      caption: 'Lag from AdminClient (scheduled, exported as gauges)',
      src: `
@Scheduled(fixedDelay = 30_000)
void exportLag() throws Exception {
    Map<TopicPartition, OffsetAndMetadata> committed = admin
        .listConsumerGroupOffsets("trade-store")
        .partitionsToOffsetAndMetadata().get();
    Map<TopicPartition, OffsetSpec> latest = committed.keySet().stream()
        .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
    var ends = admin.listOffsets(latest).all().get();
    committed.forEach((tp, om) -> lag
        .computeIfAbsent(tp, this::registerGauge)       // AtomicLong per partition
        .set(ends.get(tp).offset() - om.offset()));
}`,
    },
    notes:
      'Client-side lag metrics vanish exactly when you need them most, when the consumer is dead. An AdminClient-based exporter (or a platform tool like Burrow / Kafka Lag Exporter) keeps reporting lag from outside.\n\nAlert on lag growth rate and on time-based lag where possible, not on an absolute message count.',
  },
];
