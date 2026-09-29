// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { AgentEventId } from './events.js';

/** True only for an `evt_` identifier with a non-empty suffix. */
export function isAgentEventId(value: unknown): value is AgentEventId {
  return typeof value === 'string' && value.startsWith('evt_') && value.length > 'evt_'.length;
}
