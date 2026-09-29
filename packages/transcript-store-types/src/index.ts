// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Types-only surface for the transcript event log. These are pure interfaces —
// the package ships NO runtime: no kafkajs / pg / pulsar-client, no broker
// wiring at all. Consumers that only need the contract (e.g. the session-runner,
// whose self-hosted boot must not drag in a broker client) depend on this package
// so a future value-import cannot silently pull a broker into their runtime
// closure. `@orca/transcript-store` re-exports these same types alongside its
// concrete Kafka / Postgres / Pulsar implementations.
export type { Event } from './types.js';
export type { TranscriptStore, ReadOptions, TailOptions } from './store.js';
