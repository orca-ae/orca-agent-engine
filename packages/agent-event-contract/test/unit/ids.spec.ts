// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { isAgentEventId, type AgentEventId } from '../../src/index.js';

describe('isAgentEventId', () => {
  it('narrows a non-empty evt_ identifier', () => {
    const candidate: unknown = 'evt_019d';

    expect(isAgentEventId(candidate)).toBe(true);
    if (!isAgentEventId(candidate)) throw new Error('expected a valid event id');

    const eventId: AgentEventId = candidate;
    expect(eventId).toBe('evt_019d');
  });

  it('rejects non-strings, other prefixes, and an empty suffix', () => {
    for (const value of [undefined, null, 42, '', 'event_019d', 'evt_']) {
      expect(isAgentEventId(value)).toBe(false);
    }
  });
});
