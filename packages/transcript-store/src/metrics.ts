// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Counter, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

export const registry = new Registry();
collectDefaultMetrics({ register: registry, prefix: 'transcript_store_' });

export const appendTotal = new Counter({
  name: 'transcript_store_append_total',
  help: 'Total Append calls (KafkaTranscriptStore.append)',
  labelNames: ['status'] as const,
  registers: [registry],
});

export const readTotal = new Counter({
  name: 'transcript_store_read_total',
  help: 'Total Read calls (KafkaTranscriptStore.read)',
  labelNames: ['status'] as const,
  registers: [registry],
});

export const tailTotal = new Counter({
  name: 'transcript_store_tail_total',
  help: 'Total Tail calls (KafkaTranscriptStore.tail)',
  labelNames: ['status'] as const,
  registers: [registry],
});

export const archiveTotal = new Counter({
  name: 'transcript_store_archive_total',
  help: 'Total Archive calls (KafkaTranscriptStore.archive)',
  labelNames: ['status'] as const,
  registers: [registry],
});

export const appendLatency = new Histogram({
  name: 'transcript_store_append_latency_seconds',
  help: 'Append latency',
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [registry],
});

export const readFirstByteLatency = new Histogram({
  name: 'transcript_store_read_first_byte_latency_seconds',
  help: 'Read time-to-first-event',
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [registry],
});

export const dedupHits = new Counter({
  name: 'transcript_store_dedup_hits_total',
  help: 'Append events that hit the LRU dedup cache',
  registers: [registry],
});

export const subagentMessageRate = new Counter({
  name: 'transcript_store_subagent_message_rate',
  help: 'Events with non-empty subpath (i.e., subagent activity).',
  labelNames: ['workspace_id', 'produced_by'] as const,
  registers: [registry],
});
