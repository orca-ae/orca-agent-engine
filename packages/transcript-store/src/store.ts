// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `TranscriptStore` / `ReadOptions` / `TailOptions` interfaces now live in the
// types-only `@orca/transcript-store-types` package so type-only consumers (e.g. the
// session-runner) can depend on the contract without any path to a broker client.
// They are re-exported here so the in-package source — and every existing
// `@orca/transcript-store` consumer — keeps importing them from the same place.
export type {
  Event,
  TranscriptStore,
  ReadOptions,
  TailOptions,
} from '@orca/transcript-store-types';

/**
 * Liveness-independent state exposed by long-running session event sources.
 * It deliberately contains no backend error details.
 */
export interface SessionEventSourceStatus {
  ready: boolean;
  state: 'running' | 'stopped' | 'failed';
}
