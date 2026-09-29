// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { isAgentEventId } from '@orca/agent-event-contract';

const MAX_PERSISTED_EVENT_ID_BYTES = 512;
const DIGESTED_EVENT_ID_PREFIX = 'evt_digest_';

/** Private reducer correlation only: even ordinary outcome slugs may contain content. */
export function outcomeIdentityKey(value: unknown): string | undefined {
  if (typeof value !== 'string' || value.length === 0 || value.length > 512) return undefined;
  const digest = createHash('sha256')
    .update('orca-observability-outcome-id-v1')
    .update('\0')
    .update(Buffer.from(value, 'utf16le'))
    .digest('hex');
  return 'outcome_digest_' + digest;
}

/** Accept only the opaque representation persisted in open evaluation state. */
export function isOutcomeIdentityKey(value: unknown): value is string {
  return typeof value === 'string' && /^outcome_digest_[0-9a-f]{64}$/u.test(value);
}

/** Fixed-width database/index identity for any Transcript event ID. */
export function eventIdentityKey(value: string): string {
  return createHash('sha256')
    .update('orca-observability-event-id-v1')
    .update('\0')
    .update(Buffer.from(value, 'utf16le'))
    .digest('hex');
}

/**
 * Preserve ordinary canonical IDs for diagnostics and determinism. Canonical
 * IDs whose raw representation is unsafe, too large, or occupies the digest
 * namespace become a stable bounded identity before entering reducer state or
 * outbox metadata.
 */
export function boundedAgentEventId(value: unknown): string | undefined {
  if (!isAgentEventId(value)) return undefined;
  if (
    !value.startsWith(DIGESTED_EVENT_ID_PREFIX) &&
    Buffer.byteLength(value, 'utf8') <= MAX_PERSISTED_EVENT_ID_BYTES &&
    isVisibleAscii(value)
  ) {
    return value;
  }
  return `${DIGESTED_EVENT_ID_PREFIX}${eventIdentityKey(value)}`;
}

/** True only for the bounded representation persisted by this exporter. */
export function isBoundedAgentEventId(value: unknown): value is string {
  if (typeof value !== 'string' || !isAgentEventId(value)) return false;
  if (value.startsWith(DIGESTED_EVENT_ID_PREFIX)) {
    return /^evt_digest_[0-9a-f]{64}$/u.test(value);
  }
  return Buffer.byteLength(value, 'utf8') <= MAX_PERSISTED_EVENT_ID_BYTES && isVisibleAscii(value);
}

function isVisibleAscii(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code < 0x21 || code > 0x7e) return false;
  }
  return true;
}
