import type { Chapter } from '../engine/types';
import { r, rt } from './helpers';

export const intro: Chapter = {
  id: 'intro',
  title: 'Introduction',
  slides: [
    {
      id: 'title',
      layout: 'title',
      title: 'Kafka Patterns for Investment Banking',
      subtitle: 'Consistency, replay and resiliency: a field guide for application engineers',
      bullets: [
        'Prerequisites: **Java**, **Spring Boot 4**, **Resilience4j** where needed',
        'Kafka gives you **at-least-once** delivery. Everything else is your job.',
        'Every pattern: why it exists, how the data moves, pros & cons, and a code sketch',
      ],
      notes:
        'Welcome. This guide distils what we learned running Kafka-based services in an investment bank: where data gets lost, where it gets duplicated, and what to do about it.\n\nUse → / Space to step through animations, N for these notes, M for contents, ? for all shortcuts.',
    },
    {
      id: 'guarantees',
      title: 'What a bank needs from a messaging pipeline',
      subtitle: 'Four concerns that run through every chapter',
      table: {
        head: ['Concern', 'What it means here', 'Where we address it'],
        rows: [
          ['**Retention & archiving**', '14-day topic retention by default: long enough for RCA and replay. Long-term record lives in DB/ADLS.', 'Consumption · Resiliency'],
          ['**Consistency**', 'No data loss, no duplication, end to end, including across failures and failover.', 'Idempotency · Commit order · Outbox'],
          ['**Schema evolution**', 'Backwards- and forwards-compatible changes; blue-green deployments run two versions at once.', 'Schema chapter'],
          ['**Resiliency**', 'Region failover with a runbook that proves which messages were or were not consumed.', 'Availability & resiliency'],
        ],
      },
      callout: {
        kind: 'rule',
        title: 'Kafka is a transport and a replay buffer, not your system of record.',
        text: 'The database (and ADLS archive) is the source of truth for what was processed. Most patterns in this guide follow from that one decision.',
      },
      notes:
        'Frame the talk: regulators and RCA both need us to prove what happened to every message. Kafka helps (ordered, replayable log) but gives only at-least-once to applications.\n\n14 days is a proposed default: long enough that an incident discovered after a long weekend can still be replayed from the topic.',
    },
    {
      id: 'running-example',
      title: 'Running example: the trade lifecycle',
      subtitle: 'One business flow used on every slide',
      bullets: [
        'A booking system publishes `TradeEvent`s to topic `trades`, **keyed by `tradeId`**.',
        'Lifecycle per trade: **NEW → AMEND → CANCEL**. Order per trade matters; order across trades does not.',
        'The consumer `trade-store` persists every event to PostgreSQL table `trade_event`.',
        'Idempotency key: **`tradeId + version`**.',
      ],
      code: {
        lang: 'java',
        caption: 'TradeEvent.java',
        src: `
public record TradeEvent(
    String tradeId,      // partition key
    int version,         // 1 = NEW, increments on every AMEND/CANCEL
    TradeAction action,  // NEW | AMEND | CANCEL
    String book,
    BigDecimal notional,
    Instant eventTime) {}`,
      },
      animation: {
        scene: {
          height: 380,
          nodes: [
            { id: 'bk', kind: 'producer', label: 'Booking system', x: 20, y: 70, w: 150, h: 64 },
            { id: 'tp', kind: 'topic', label: 'trades', sub: 'key = tradeId', x: 300, y: 62, w: 150, h: 80 },
            { id: 'cs', kind: 'consumer', label: 'trade-store', x: 590, y: 70, w: 170, h: 64 },
          ],
          edges: [
            { id: 'e1', from: 'bk', to: 'tp', label: 'publish' },
            { id: 'e2', from: 'tp', to: 'cs', label: 'consume' },
          ],
          tables: [
            {
              id: 'db',
              title: 'PostgreSQL · trade_event',
              x: 300,
              y: 210,
              w: 460,
              columns: ['trade_id', 'version', 'action', 'notional'],
            },
          ],
        },
        steps: [
          { caption: 'The booking system books trade **T-1**.', tables: { db: [] } },
          {
            caption: 'NEW: version 1 flows through the topic to the consumer and lands in the table.',
            messages: [
              { edge: 'e1', label: 'T-1 v1 NEW' },
              { edge: 'e2', label: 'T-1 v1 NEW', delay: 2.6 },
            ],
            tables: { db: [r('T-1', '1', 'NEW', '10,000,000')] },
          },
          {
            caption: 'AMEND: notional changes. Same key ⇒ same partition ⇒ processed after NEW.',
            messages: [
              { edge: 'e1', label: 'T-1 v2 AMEND' },
              { edge: 'e2', label: 'T-1 v2 AMEND', delay: 2.6 },
            ],
            tables: { db: [r('T-1', '1', 'NEW', '10,000,000'), r('T-1', '2', 'AMEND', '12,500,000')] },
          },
          {
            caption: 'CANCEL closes the lifecycle. Every version is kept: an append-only audit trail.',
            messages: [
              { edge: 'e1', label: 'T-1 v3 CANCEL' },
              { edge: 'e2', label: 'T-1 v3 CANCEL', delay: 2.6 },
            ],
            tables: {
              db: [
                r('T-1', '1', 'NEW', '10,000,000'),
                r('T-1', '2', 'AMEND', '12,500,000'),
                rt('warn', 'T-1', '3', 'CANCEL', '12,500,000'),
              ],
            },
          },
        ],
      },
      notes:
        'Everything that follows uses this flow. If an AMEND overtakes its NEW, or a NEW is stored twice, we have a booking break; that is the cost of getting these patterns wrong.\n\nWe store every version rather than updating in place: it gives a natural audit trail and a natural idempotency key.',
    },
  ],
};
