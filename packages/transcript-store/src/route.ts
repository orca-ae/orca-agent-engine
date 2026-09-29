// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Event } from './types.js';

export interface SessionRoute {
  workspaceId: string;
  sessionId: string;
}

/**
 * The append target is authoritative. Refuse to put an event whose embedded
 * route names a different tenant/session onto that target's transport topic.
 */
export function assertEventsMatchRoute(
  workspaceId: string,
  sessionId: string,
  events: readonly Event[],
): void {
  for (const event of events) {
    if (event.workspaceId !== workspaceId || event.sessionId !== sessionId) {
      throw new Error(
        `event route mismatch: expected ${workspaceId}/${sessionId}, got ${event.workspaceId}/${event.sessionId}`,
      );
    }
  }
}

export function eventMatchesRoute(event: Event, route: SessionRoute): boolean {
  return event.workspaceId === route.workspaceId && event.sessionId === route.sessionId;
}
