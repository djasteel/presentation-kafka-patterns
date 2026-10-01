import type { Chapter } from '../engine/types';

export const closing: Chapter = {
  id: 'closing',
  title: '4 · Closing thoughts',
  slides: [
    {
      id: 'ch4',
      layout: 'section',
      title: 'Closing thoughts',
      subtitle: 'Was it worth it? What else do application teams need?',
      notes: 'Step back from the patterns.',
    },
    {
      id: 'worth-it',
      title: 'With all these consistency problems, was Kafka worth it?',
      subtitle: 'Yes. The problems are not Kafka\'s; the solutions depend on Kafka.',
      bullets: [
        'Lost and duplicated messages come from **any broker plus a database**: no distributed transaction, crashes between two commits. A queue does not remove them.',
        '**Replay from an offset** is what makes the DR runbook, RCA and reprocessing possible. Queues delete on acknowledge; there is nothing to replay.',
        '**14-day retention** lets you reprocess after a bug fix or a late-discovered incident.',
        '**Fan-out**: settlement, risk, reporting each read the same stream at their own pace, without the producer knowing.',
        '**Per-key ordering at scale**: thousands of trades in parallel, each strictly ordered.',
      ],
      table: {
        head: ['Need', 'Kafka', 'Classic queue'],
        rows: [
          ['Replay from a point in time / offset', '✅ native', '❌ messages gone after ack'],
          ['Deterministic DR via offsets', '✅', '❌ no positions to compare'],
          ['Many independent consumers', '✅ consumer groups', '⚠️ copy per subscriber'],
          ['Per-entity ordering at scale', '✅ partition key', '⚠️ sessions, limited throughput'],
          ['Idempotent processing still needed?', 'Yes', 'Yes'],
        ],
      },
      notes:
        'The patterns in this guide (idempotency, commit order, outbox, parking) are needed with any broker. Kafka additionally gives us the replayable log, which is what makes recovery provable. That is the trade we chose.',
    },
    {
      id: 'event-hubs-misconception',
      title: 'Would Event Hubs or a FIFO broker have been simpler?',
      subtitle: 'Know what each Azure option actually is',
      bullets: [
        '**Azure Event Hubs** is a **partitioned log**, the same model as Kafka (it even speaks the Kafka protocol): same per-partition ordering, same need for idempotency and checkpoints.',
        'True per-entity FIFO on Azure is **Service Bus sessions**: ordered per session id, but a queue: no replay after completion, lower throughput.',
        'Switching broker does not remove the commit-order, idempotency or dual-write problems.',
      ],
      callout: {
        kind: 'misconception',
        title: '"Event Hubs is a FIFO broker, so ordering and consistency would be easier."',
        text: 'Event Hubs orders per partition exactly like Kafka. FIFO-per-entity is Service Bus sessions, which trades replayability for queue semantics.',
      },
      notes: 'This comes up in every architecture review. The honest answer: the hard parts of this guide are broker-independent.',
    },
    {
      id: 'platform-observability',
      title: 'Platform observability: application teams need to see Kafka too',
      subtitle: '"Kafka as a platform" must not mean "Kafka as a black box"',
      bullets: [
        '**Broker health**: availability, under-replicated / offline partitions, ISR shrinks, controller changes.',
        '**Broker performance**: request latency (produce / fetch p99), network and disk utilisation.',
        '**Topic metrics**: bytes in/out, messages/s, partition skew (hot partitions), retention usage.',
        '**Consumer groups**: lag per partition, rebalances, as seen by the brokers.',
        'Give application teams **read access** to these dashboards: an RCA needs both sides of the wire on one timeline.',
      ],
      callout: {
        kind: 'tip',
        title: 'Agree on this with the platform team before the first incident.',
        text: 'During an outage is the worst time to request dashboard access.',
      },
      notes: 'Most "is it us or Kafka?" debates end in minutes when both teams look at the same dashboard.',
    },
    {
      id: 'actuator-adminclient',
      title: 'Observe core Kafka metrics from the application',
      subtitle: 'Spring Boot Actuator + Micrometer, and the Kafka AdminClient',
      bullets: [
        '**Actuator + Micrometer**: Spring Boot binds Kafka client metrics automatically: producer/consumer rates, errors, lag, rebalance stats on `/actuator/prometheus`.',
        '**AdminClient** (a Spring bean via `KafkaAdmin`): describe groups, committed vs end offsets, topic configs (e.g. verify `retention.ms`, `min.insync.replicas` at startup).',
        'Expose a small health indicator: can we reach the cluster, does our group have members, is lag bounded.',
      ],
      code: {
        lang: 'java',
        caption: 'Startup check of topic configuration',
        src: `
@Bean
ApplicationRunner verifyTopic(KafkaAdmin kafkaAdmin) {
    return args -> {
        try (var admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            var res = new ConfigResource(ConfigResource.Type.TOPIC, "trades");
            Config cfg = admin.describeConfigs(List.of(res)).all().get().get(res);
            long retention = Long.parseLong(cfg.get("retention.ms").value());
            if (retention < Duration.ofDays(14).toMillis())
                log.warn("trades retention {} ms is below the 14-day policy", retention);
        }
    };
}`,
      },
      notes: 'Applications verifying their own assumptions about the platform (retention, replication, partition count) catch configuration drift before it becomes an incident.',
    },
    {
      id: 'further-topics',
      title: 'Further topics',
      subtitle: 'Important, but beyond the scope of this session',
      table: {
        head: ['Topic', 'Why it matters'],
        rows: [
          ['**Kafka EOS / read-process-write transactions**', 'Exactly-once for Kafka → Kafka pipelines; does not cover DB sinks.'],
          ['**Regulatory archiving mechanics**', 'Tiered storage to Azure Blob, or a Kafka Connect sink to ADLS, independent of consumers.'],
          ['**Security & PII**', 'ACLs, encryption, PII in immutable logs vs. right-to-erasure (crypto-shredding).'],
          ['**Compacted topics**', 'Last-value-per-key for reference data: instruments, counterparties, SSIs.'],
          ['**Testing**', 'Testcontainers Kafka + PostgreSQL; failure-injection tests for commit order, partial batches and DR replay.'],
          ['**Parallel consumer**', 'Per-key concurrency beyond the partition count, while keeping key order.'],
        ],
      },
      notes: 'Each of these deserves its own session. Pick based on what your team is about to build.',
    },
    {
      id: 'takeaways',
      layout: 'title',
      title: 'Takeaways',
      subtitle: 'At-least-once delivery + these patterns = no loss, no duplicates, provable recovery',
      bullets: [
        '**Idempotency** on the business key via a unique constraint. Offsets are not identities.',
        '**DB commit first, offset second.** Make every failure a duplicate, then make duplicates harmless.',
        '**Persist topic / group / partition / offset**: DR becomes a query, RPO becomes a number.',
        '**Park per key** for ordered flows; tell upstreams through **one error topic**.',
        '**Transactional outbox** for DB + publish; poller first, CDC when needed.',
        '**FULL_TRANSITIVE** schemas + version header; unknown versions are parked, not dead-lettered.',
        '**Monitor from the first performance test**: TPS, error rate, lag, rebalances, traces.',
      ],
      notes: 'Thank you. Questions?\n\nThe guide stays online: every slide has its own link (see the address bar) for later reference.',
    },
  ],
};
