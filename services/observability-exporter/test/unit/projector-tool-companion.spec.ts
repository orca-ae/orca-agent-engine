// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  reduceCanonicalEventBatch,
} from '../../src/projector.js';
import { TRACE_SAMPLING_VERSION } from '../../src/sampling.js';
import { event, TRANSCRIPT_SECRET } from '../support/events.js';

describe.each(['local', 'custom'] as const)(
  '%s client tool result companion acceptance',
  (family) => {
    const sources = (companionId: string | undefined) => [
      event(1, 'user.message', {}, { producedBy: 'client' }),
      event(2, 'session.user_event_processed', { user_event_id: 'evt_1' }),
      event(3, family === 'local' ? 'agent.tool_use' : 'agent.custom_tool_use'),
      event(4, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
      event(
        5,
        family === 'local' ? 'user.tool_result' : 'user.custom_tool_result',
        {
          [family === 'local' ? 'tool_use_id' : 'custom_tool_use_id']: 'evt_3',
          _orca_companion_system_event_id: companionId,
          content: TRANSCRIPT_SECRET,
          is_error: false,
        },
        { producedBy: 'client' },
      ),
      event(
        6,
        'system.message',
        { content: TRANSCRIPT_SECRET },
        { id: 'evt_companion', producedBy: 'client' },
      ),
      event(7, 'session.user_event_processed', { user_event_id: 'evt_5' }),
      event(8, 'session.user_event_processed', { user_event_id: 'evt_companion' }),
      event(9, 'session.status_running'),
      event(10, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ];

    describe.each([0, 1])('sample rate %s', (sampleRate) => {
      const policy = {
        algorithmVersion: TRACE_SAMPLING_VERSION,
        bindingId: 'aob_companion',
        bindingVersion: 1,
        sampleRate,
      };

      it('retains only the companion control identity and restores every batch boundary', () => {
        const events = sources('evt_companion');
        const full = reduceCanonicalEventBatch(
          initialCanonicalProjectionState(),
          events,
          new Set(),
          policy,
        );
        expect(full.issues).toEqual([]);
        expect(full.acceptedSourceIds).toEqual(['evt_1', 'evt_5', 'evt_companion']);
        expect(full.state.pendingInputs).toEqual([]);
        expect(full.completedTraces).toHaveLength(sampleRate);
        if (sampleRate === 1) {
          expect(full.completedTraces[0]!.root.metadata).toMatchObject({
            'orca.turn.companion_system_message_count': 1,
            'orca.turn.continuation_count': 1,
          });
          expect(full.completedTraces[0]!.spans[0]).toMatchObject({
            observationType: 'tool',
            status: 'ok',
          });
        } else {
          expect(full.state.sampling?.suppressed?.turnCount).toBe('1');
        }
        for (let split = 0; split <= events.length; split++) {
          const first = reduceCanonicalEventBatch(
            initialCanonicalProjectionState(),
            events.slice(0, split),
            new Set(),
            policy,
          );
          const restored = parseCanonicalProjectionState(JSON.parse(JSON.stringify(first.state)));
          if (split === 5 || split === 6) {
            const result = restored.pendingInputs.find((input) => input.eventId === 'evt_5');
            expect(result?.companionSystemEventId).toBe('evt_companion');
            if (sampleRate === 0) {
              expect(result).not.toHaveProperty('toolResult');
              expect(result).not.toHaveProperty('toolTraceId');
            }
          }
          const second = reduceCanonicalEventBatch(
            restored,
            events.slice(split),
            new Set(first.acceptedSourceIds),
          );
          expect([...first.issues, ...second.issues]).toEqual([]);
          expect(second.state).toEqual(full.state);
          expect([...first.completedTraces, ...second.completedTraces]).toEqual(
            full.completedTraces,
          );
          expect(JSON.stringify([first, restored, second])).not.toContain(TRANSCRIPT_SECRET);
        }
      });

      it.each(['evt_wrong_companion', undefined])(
        'rejects unmatched/orphan companion reference %j after restart',
        (id) => {
          const events = sources(id);
          const first = reduceCanonicalEventBatch(
            initialCanonicalProjectionState(),
            events.slice(0, 7),
            new Set(),
            policy,
          );
          const second = reduceCanonicalEventBatch(
            parseCanonicalProjectionState(JSON.parse(JSON.stringify(first.state))),
            events.slice(7),
            new Set(first.acceptedSourceIds),
          );
          expect(first.issues).toEqual([]);
          expect(second.issues).toEqual([
            expect.objectContaining({
              code: 'orphan_companion_system_message',
              markerEventId: 'evt_8',
              sourceEventId: 'evt_companion',
            }),
          ]);
          expect(second.acceptedSourceIds).not.toContain('evt_companion');
        },
      );
    });
  },
);
