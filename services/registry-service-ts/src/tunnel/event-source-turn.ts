// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Event } from '@orca/transcript-store';

/** Registry-stamped source identity, including historical completion markers. */
export function eventSourceTurnId(event: Event): string | undefined {
  try {
    const payload: unknown = JSON.parse(Buffer.from(event.payload).toString('utf8'));
    if (!payload || typeof payload !== 'object') return undefined;
    const fields = payload as Record<string, unknown>;
    const id = fields.source_event_id ?? fields.turn_event_id;
    return typeof id === 'string' && id.length > 0 ? id : undefined;
  } catch {
    return undefined;
  }
}
