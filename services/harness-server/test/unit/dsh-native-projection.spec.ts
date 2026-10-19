// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import { exerciseNativeTree } from '../../../../packages/dsh-harness/test/support/native-tree.js';
import { DshEventProjection } from '../../src/harness/codex-sdk/dsh-events.js';
import { projectCanonicalTurns } from '../../../observability-exporter/src/projector.js';
import { parseProjectedTrace } from '../../../observability-exporter/src/canonical-validation.js';
import { event as transcriptEvent } from '../../../observability-exporter/test/support/events.js';

it('admits the actual native tree wire including cold lineage and child tools', async () => {
  let projection = new DshEventProjection('ws_fixture', 'orca-session', [
    'spawn',
    'fork',
    'send_message',
    'interrupt_agent',
  ]);
  const errors: unknown[] = [];
  let projected = 0;
  let restored = 0;
  const projectedEvents: ReturnType<DshEventProjection['validate']>[] = [];
  await exerciseNativeTree((event) => {
    try {
      if (event.type === 'event' && event.event.type === 'thread.started')
        projection = new DshEventProjection('ws_fixture', 'orca-session', [
          'spawn',
          'fork',
          'send_message',
          'interrupt_agent',
        ]);
      if (event.type === 'event' && event.event.type === 'turn.started') projection.beginTurn();
      if (event.type === 'thread_tree') {
        projection.restoreTree(event.threads);
        restored += event.threads.length;
      }
      if (event.type === 'canonical_event') {
        projectedEvents.push(
          projection.validate(event.event, String(event.event.payload.turn_event_id)),
        );
        projected++;
      }
      if (event.type === 'tool_call') projection.assertOrigin(event.origin);
      if (event.type === 'orchestration_request') projection.authorize(event);
    } catch (error) {
      errors.push(
        String(error) +
          ' ' +
          (event.type === 'canonical_event'
            ? JSON.stringify({
                kind: event.event.kind,
                subpath: event.event.subpath,
                turn: event.event.payload.turn_event_id,
              })
            : event.type),
      );
    }
  });
  expect(errors.map(String)).toEqual([]);
  expect(projected).toBeGreaterThan(10);
  expect(restored).toBeGreaterThanOrEqual(3);
  const delegation = projectedEvents.filter(
    (event) => event.payload.turn_event_id === 'evt_delegate',
  );
  const events = [
    transcriptEvent(
      1,
      'user.message',
      { content: [{ type: 'text', text: 'delegate' }] },
      { id: 'evt_delegate', producedBy: 'client' },
    ),
    transcriptEvent(2, 'session.user_event_processed', { user_event_id: 'evt_delegate' }),
    ...delegation.map((event, index) =>
      transcriptEvent(index + 3, event.kind, event.payload, {
        id: event.id,
        subpath: event.subpath,
      }),
    ),
    transcriptEvent(delegation.length + 3, 'session.status_idle', {
      stop_reason: { type: 'end_turn' },
    }),
  ];
  const traces = projectCanonicalTurns(events);
  expect(traces).toHaveLength(1);
  const trace = parseProjectedTrace(traces[0]);
  expect(
    trace.spans.filter(
      (span) => span.observationType === 'turn_model_summary' && span.subpath !== '',
    ),
  ).toHaveLength(2);
  expect(trace.spans.every((span) => span.parentSpanId === trace.root.spanId)).toBe(true);
  expect(projectCanonicalTurns(events)).toEqual(traces);
}, 45000);
