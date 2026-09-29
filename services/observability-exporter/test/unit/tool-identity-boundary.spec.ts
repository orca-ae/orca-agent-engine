// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { boundedAgentEventId } from '../../src/event-identity.js';
import { projectCanonicalTurns } from '../../src/projector.js';
import { event } from '../support/events.js';

describe('bounded canonical tool correlation identities', () => {
  it.each(['evt_' + 'x'.repeat(600), 'evt_digest_' + 'a'.repeat(64)])(
    'pairs separate producer source identity %s without exporting its raw form',
    (sourceId) => {
      const [trace] = projectCanonicalTurns([
        event(1, 'user.message', {}, { producedBy: 'client' }),
        event(2, 'session.user_event_processed', { user_event_id: 'evt_1' }),
        event(3, 'agent.tool_use', { id: 'native-id-ignored' }, { id: sourceId }),
        event(4, 'agent.tool_result', { tool_use_id: sourceId }),
        event(5, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
      ]);
      expect(trace?.spans).toHaveLength(1);
      expect(trace?.spans[0]).toMatchObject({
        sourceEventId: boundedAgentEventId(sourceId),
        status: 'ok',
        metadata: { 'orca.tool.outcome': 'success', 'orca.source.end_event_id': 'evt_4' },
      });
      expect(JSON.stringify(trace)).not.toContain(sourceId);
    },
  );

  it('does not silently reinterpret an explicitly invalid native ID as a canonical fallback', () => {
    const [trace] = projectCanonicalTurns([
      event(1, 'user.message', {}, { producedBy: 'client' }),
      event(2, 'session.user_event_processed', { user_event_id: 'evt_1' }),
      event(3, 'agent.tool_use', { tool_use_id: null }),
      event(4, 'agent.tool_result', { tool_use_id: 'evt_3' }),
      event(5, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ]);
    expect(trace?.spans.some((span) => span.metadata['orca.tool.outcome'] === 'success')).toBe(
      false,
    );
    expect(trace?.root.metadata['orca.turn.unmatched_tool_result_count']).toBe(1);
  });

  it('counts accepted pre-turn results without guessing a match from later uses', () => {
    const [trace] = projectCanonicalTurns([
      event(1, 'user.tool_result', { tool_use_id: 'native' }, { producedBy: 'client' }),
      event(2, 'user.message', {}, { producedBy: 'client' }),
      event(3, 'session.user_event_processed', { user_event_id: 'evt_2' }),
      event(4, 'agent.tool_use', { tool_use_id: 'native' }),
      event(5, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
      event(6, 'session.user_event_processed', { user_event_id: 'evt_1' }),
      event(7, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ]);
    expect(trace?.spans[0]?.status).toBe('unset');
    expect(trace?.root.metadata['orca.turn.unmatched_tool_result_count']).toBe(1);
  });
});
