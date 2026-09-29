// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `Event` interface now lives in the types-only `@orca/transcript-store-types`
// package so type-only consumers (e.g. the session-runner) can depend on the
// contract without any path to a broker client. It is re-exported here so the
// in-package source — and every existing `@orca/transcript-store` consumer —
// keeps importing it from the same place.
export type { Event } from '@orca/transcript-store-types';

/**
 * A handler failure caused by transient event-pipeline infrastructure rather
 * than an invalid source event. Event sources must keep these failures
 * retryable instead of applying a poison-message drop policy.
 */
export class RetryableSessionEventError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'RetryableSessionEventError';
  }
}

/**
 * A partially persisted outcome must be repaired before the source delivers
 * more events. Retry the captured persistence operation, not the handler's
 * preparation or other side effects. The callback retains its lifecycle fence.
 */
export class SessionEventBarrierError extends RetryableSessionEventError {
  constructor(
    message: string,
    readonly retry: () => Promise<void>,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = 'SessionEventBarrierError';
  }
}
