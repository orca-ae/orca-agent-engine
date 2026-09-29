// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export type { Event } from './types.js';
export { RetryableSessionEventError, SessionEventBarrierError } from './types.js';
export type {
  TranscriptStore,
  ReadOptions,
  TailOptions,
  SessionEventSourceStatus,
} from './store.js';
export { KafkaTranscriptStore } from './kafka-store.js';
export type { KafkaReadOptions, KafkaTranscriptStoreOptions } from './kafka-store.js';
export { KafkaSessionEventSource } from './kafka-event-source.js';
export type {
  KafkaSessionEventHandler,
  KafkaSessionEventSourceOptions,
} from './kafka-event-source.js';
export {
  PostgresSessionEventSource,
  PostgresTranscriptStore,
  applyPostgresTranscriptMigrations,
} from './postgres-store.js';
export type {
  PostgresSessionEventHandler,
  PostgresSessionEventSourceOptions,
  PostgresTranscriptStoreOptions,
} from './postgres-store.js';
export {
  PulsarSessionEventSource,
  PulsarTranscriptStore,
  pulsarTopicName,
} from './pulsar-store.js';
export type {
  PulsarAuthConfig,
  PulsarSessionEventHandler,
  PulsarSessionEventSourceOptions,
  PulsarTranscriptStoreOptions,
} from './pulsar-store.js';
export {
  sessionTopicName,
  sessionTopicPattern,
  parseSessionTopic,
  matchSessionTopic,
  validateTopicPrefix,
  SESSION_TOPIC_PATTERN,
  SESSION_TOPIC_REGEX,
  TOPIC_PREFIX_REGEX,
  parseCursor,
  formatCursor,
} from './kafka/topic.js';
export type { SessionTopicMatch } from './kafka/topic.js';
export { registry as transcriptStoreMetricsRegistry } from './metrics.js';
export { createKafkaTranscriptCodec } from './kafka/codec.js';
export type { KafkaTranscriptCodec, KafkaTranscriptCodecOptions } from './kafka/codec.js';
export { KafkaTranscriptCodecError } from './kafka/codec-error.js';
export type { KafkaTranscriptCodecErrorCode } from './kafka/codec-error.js';
export { parseKafkaTranscriptConfig } from './kafka/config.js';
