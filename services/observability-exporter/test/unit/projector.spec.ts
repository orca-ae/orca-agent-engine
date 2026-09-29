// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  CanonicalProjectionError,
  CanonicalProjectionStateError,
  initialCanonicalProjectionState,
  projectCanonicalTurns,
  reduceCanonicalEventBatch,
} from '../../src/projector.js';
import {
  TRANSCRIPT_SECRET,
  USER_ID,
  completedPrimaryTurnEvents,
  event,
} from '../support/events.js';

describe('projectCanonicalTurns', () => {
  it('projects one accepted primary client turn with stable IDs and user attribution', () => {
    const events = completedPrimaryTurnEvents();
    const first = projectCanonicalTurns(events);
    const replay = projectCanonicalTurns(events);

    expect(first).toEqual(replay);
    expect(first).toHaveLength(1);
    const trace = first[0]!;
    expect(trace).toMatchObject({
      anchorEventId: 'evt_user_turn',
      acceptanceEventId: 'evt_accept',
      userId: USER_ID,
      root: {
        name: 'orca.agent.turn',
        observationType: 'agent_turn',
        status: 'ok',
      },
    });
    expect(trace.traceId).toMatch(/^[0-9a-f]{32}$/);
    expect(trace.root.spanId).toMatch(/^[0-9a-f]{16}$/);
    expect(trace.root.metadata).toMatchObject({
      'orca.turn.model_summary_count': 1,
      'orca.turn.source_event_count': 6,
      'orca.turn.terminal_reason': 'end_turn',
    });
    expect(trace.spans).toHaveLength(1);
    expect(trace.spans[0]).toMatchObject({
      observationType: 'turn_model_summary',
      name: 'orca.agent.turn_model_summary',
      status: 'ok',
      modelSummary: {
        provider: 'anthropic',
        requestedModel: 'claude-test',
        usage: { inputTokens: 11, outputTokens: 7 },
      },
    });
  });

  it('never copies transcript content and ignores child-path detail', () => {
    const projected = projectCanonicalTurns(completedPrimaryTurnEvents());
    const encoded = JSON.stringify(projected);

    expect(encoded).not.toContain(TRANSCRIPT_SECRET);
    expect(encoded).not.toContain('queued and unaccepted');
    expect(encoded).not.toContain('subagent secret must stay absent');
    expect(encoded).not.toContain('content');
    expect(encoded).not.toContain('subagents/child_1');
  });

  it('does not export malformed lifecycle values as metadata', () => {
    const events = completedPrimaryTurnEvents();
    const terminal = events.find((candidate) => candidate.kind === 'session.status_idle')!;
    terminal.payload = Buffer.from(JSON.stringify({ stop_reason: { type: TRANSCRIPT_SECRET } }));

    const [trace] = projectCanonicalTurns(events);
    expect(trace?.root.metadata['orca.turn.terminal_reason']).toBe('unknown');
    expect(JSON.stringify(trace)).not.toContain(TRANSCRIPT_SECRET);
  });

  it('records an accepted interrupt while retaining the compatible end_turn reason', () => {
    const events = [
      ...completedPrimaryTurnEvents().slice(0, -1),
      event(8, 'user.interrupt', {}, { id: 'evt_interrupt', producedBy: 'client' }),
      event(9, 'session.user_event_processed', { user_event_id: 'evt_interrupt' }),
      event(10, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ];

    const [trace] = projectCanonicalTurns(events);
    expect(trace?.root.metadata['orca.turn.terminal_reason']).toBe('end_turn');
    expect(trace?.root.metadata['orca.turn.interrupt_count']).toBe(1);
    expect(trace?.root.status).toBe('ok');
  });

  it('requires an explicit coarse-summary label on both model events', () => {
    const events = completedPrimaryTurnEvents();
    const start = events.find((candidate) => candidate.kind === 'span.model_request_start')!;
    const end = events.find((candidate) => candidate.kind === 'span.model_request_end')!;
    start.payload = Buffer.from(JSON.stringify({ model: 'unlabeled' }));
    end.payload = Buffer.from(
      JSON.stringify({ model_request_start_id: 'evt_model_start', is_error: false }),
    );

    const [trace] = projectCanonicalTurns(events);
    expect(trace?.spans).toEqual([]);
  });

  it('deduplicates replayed model summary event identities inside an active turn', () => {
    const events = completedPrimaryTurnEvents();
    const start = events.find((candidate) => candidate.kind === 'span.model_request_start')!;
    const end = events.find((candidate) => candidate.kind === 'span.model_request_end')!;
    const terminalIndex = events.findIndex((candidate) => candidate.kind === 'session.status_idle');
    events[terminalIndex]!.seq = 10;
    events.splice(
      terminalIndex,
      0,
      { ...start, seq: 8 },
      { ...end, id: 'evt_model_end_duplicate', seq: 9 },
    );

    const [trace] = projectCanonicalTurns(events);
    expect(trace?.spans).toHaveLength(1);
    expect(new Set(trace?.spans.map((span) => span.spanId)).size).toBe(1);
  });

  it('accepts explicit observation_type label without inferring a provider request', () => {
    const [trace] = projectCanonicalTurns(completedPrimaryTurnEvents('observation_type'));

    expect(trace?.spans[0]).toMatchObject({
      observationType: 'turn_model_summary',
      name: 'orca.agent.turn_model_summary',
    });
    expect(JSON.stringify(trace)).not.toContain('generation');
  });

  it('uses exact accepted source join instead of queued-input proximity', () => {
    const events = [
      event(1, 'user.message', { content: 'first' }, { id: 'evt_first', producedBy: 'client' }),
      event(2, 'user.message', { content: 'second' }, { id: 'evt_second', producedBy: 'client' }),
      event(
        3,
        'session.user_event_processed',
        { user_event_id: 'evt_second' },
        { id: 'evt_accept_second' },
      ),
      event(4, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ];

    const [trace] = projectCanonicalTurns(events);
    expect(trace?.anchorEventId).toBe('evt_second');
    expect(trace?.acceptanceEventId).toBe('evt_accept_second');
  });

  it('does not look ahead when an acceptance marker precedes its source event', () => {
    const events = [
      event(
        1,
        'session.user_event_processed',
        { user_event_id: 'evt_future' },
        { id: 'evt_early_marker' },
      ),
      event(2, 'user.message', {}, { id: 'evt_future', producedBy: 'client' }),
      event(3, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ];

    const error = captureProjectionError(events);
    expect(error.issues).toEqual([
      {
        code: 'unresolved_acceptance_source',
        workspaceId: 'ws_observability',
        sessionId: 'ses_observability',
        markerEventId: 'evt_early_marker',
        sourceEventId: 'evt_future',
      },
    ]);
  });

  it('joins an accepted companion system message without changing the turn anchor or content', () => {
    const events = [
      event(
        1,
        'user.message',
        {
          content: TRANSCRIPT_SECRET,
          _orca_companion_system_event_id: 'evt_system_companion',
        },
        { id: 'evt_user_companion', producedBy: 'client', userId: USER_ID },
      ),
      event(
        2,
        'system.message',
        { content: `system ${TRANSCRIPT_SECRET}` },
        { id: 'evt_system_companion', producedBy: 'client' },
      ),
      event(
        3,
        'session.user_event_processed',
        { user_event_id: 'evt_user_companion' },
        { id: 'evt_accept_user' },
      ),
      event(
        4,
        'session.user_event_processed',
        { user_event_id: 'evt_system_companion' },
        { id: 'evt_accept_system' },
      ),
      event(5, 'session.status_running'),
      event(6, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ];

    const [trace] = projectCanonicalTurns(events);
    expect(trace?.anchorEventId).toBe('evt_user_companion');
    expect(trace?.acceptanceEventId).toBe('evt_accept_user');
    expect(trace?.root.metadata).toMatchObject({
      'orca.turn.companion_system_message_count': 1,
      'orca.turn.source_event_count': 6,
    });
    expect(JSON.stringify(trace)).not.toContain(TRANSCRIPT_SECRET);
  });

  it('records an accepted interrupt without opening or replacing the active turn', () => {
    const events = [
      event(1, 'user.message', {}, { id: 'evt_turn', producedBy: 'client' }),
      event(
        2,
        'session.user_event_processed',
        { user_event_id: 'evt_turn' },
        { id: 'evt_accept_turn' },
      ),
      event(3, 'session.status_running'),
      event(4, 'user.interrupt', {}, { id: 'evt_interrupt', producedBy: 'client' }),
      event(
        5,
        'session.user_event_processed',
        { user_event_id: 'evt_interrupt' },
        { id: 'evt_accept_interrupt' },
      ),
      event(6, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ];

    const [trace] = projectCanonicalTurns(events);
    expect(trace?.anchorEventId).toBe('evt_turn');
    expect(trace?.acceptanceEventId).toBe('evt_accept_turn');
    expect(trace?.root.metadata).toMatchObject({
      'orca.turn.interrupt_count': 1,
      'orca.turn.source_event_count': 6,
    });
  });

  it('keeps a required-action phase open until the harness resumes after its final pending action', () => {
    // ClaudeAgentSdkHarness emits an updated idle event when another action
    // becomes pending. Each accepted continuation gets its marker first; only
    // acceptance of the final pending action emits status_running.
    const events = [
      event(1, 'user.message', {}, { id: 'evt_turn', producedBy: 'client' }),
      event(
        2,
        'session.user_event_processed',
        { user_event_id: 'evt_turn' },
        { id: 'evt_accept_turn' },
      ),
      event(3, 'session.status_running'),
      event(4, 'agent.mcp_tool_use', { id: 'evt_pending_first' }, { id: 'evt_pending_first' }),
      event(5, 'session.status_idle', {
        stop_reason: { type: 'requires_action', event_ids: ['evt_pending_first'] },
      }),
      event(6, 'agent.mcp_tool_use', { id: 'evt_pending_second' }, { id: 'evt_pending_second' }),
      event(7, 'session.status_idle', {
        stop_reason: {
          type: 'requires_action',
          event_ids: ['evt_pending_first', 'evt_pending_second'],
        },
      }),
      event(
        8,
        'user.tool_confirmation',
        { tool_use_id: 'evt_pending_first', result: 'allow' },
        { id: 'evt_confirm_first', producedBy: 'client' },
      ),
      event(
        9,
        'session.user_event_processed',
        { user_event_id: 'evt_confirm_first' },
        { id: 'evt_accept_confirm_first' },
      ),
      event(
        10,
        'user.tool_confirmation',
        { tool_use_id: 'evt_pending_second', result: 'allow' },
        { id: 'evt_confirm_second', producedBy: 'client' },
      ),
      event(
        11,
        'session.user_event_processed',
        { user_event_id: 'evt_confirm_second' },
        { id: 'evt_accept_confirm_second' },
      ),
      event(12, 'session.status_running'),
      event(13, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ];

    const [trace] = projectCanonicalTurns(events);
    expect(trace?.root.metadata).toMatchObject({
      'orca.turn.continuation_count': 2,
      'orca.turn.source_event_count': 13,
      'orca.turn.terminal_reason': 'end_turn',
    });
  });

  it('keeps two pending required actions open across reducer batches until running resumes', () => {
    const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      event(1, 'user.message', {}, { id: 'evt_turn', producedBy: 'client' }),
      event(
        2,
        'session.user_event_processed',
        { user_event_id: 'evt_turn' },
        { id: 'evt_accept_turn' },
      ),
      event(3, 'session.status_running'),
      event(4, 'agent.mcp_tool_use', { id: 'evt_pending_first' }, { id: 'evt_pending_first' }),
      event(5, 'session.status_idle', {
        stop_reason: { type: 'requires_action', event_ids: ['evt_pending_first'] },
      }),
      event(6, 'agent.mcp_tool_use', { id: 'evt_pending_second' }, { id: 'evt_pending_second' }),
      event(7, 'session.status_idle', {
        stop_reason: {
          type: 'requires_action',
          event_ids: ['evt_pending_first', 'evt_pending_second'],
        },
      }),
      event(
        8,
        'user.tool_confirmation',
        { tool_use_id: 'evt_pending_first', result: 'allow' },
        { id: 'evt_confirm_first', producedBy: 'client' },
      ),
      event(
        9,
        'session.user_event_processed',
        { user_event_id: 'evt_confirm_first' },
        { id: 'evt_accept_confirm_first' },
      ),
    ]);

    expect(first.issues).toEqual([]);
    expect(first.completedTraces).toEqual([]);
    expect(first.state.activeTurn).toMatchObject({
      awaitingAction: true,
      continuationCount: 1,
    });

    const second = reduceCanonicalEventBatch(
      first.state,
      [
        event(
          10,
          'user.tool_confirmation',
          { tool_use_id: 'evt_pending_second', result: 'allow' },
          { id: 'evt_confirm_second', producedBy: 'client' },
        ),
        event(
          11,
          'session.user_event_processed',
          { user_event_id: 'evt_confirm_second' },
          { id: 'evt_accept_confirm_second' },
        ),
        event(12, 'session.status_running'),
        event(13, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
      ],
      new Set(first.acceptedSourceIds),
    );

    expect(second.issues).toEqual([]);
    expect(second.state.activeTurn).toBeNull();
    expect(second.completedTraces[0]?.root.metadata).toMatchObject({
      'orca.turn.continuation_count': 2,
      'orca.turn.source_event_count': 13,
      'orca.turn.terminal_reason': 'end_turn',
    });
  });

  it('rejects a new turn accepted during a required-action phase', () => {
    const events = [
      event(1, 'user.message', {}, { id: 'evt_turn', producedBy: 'client' }),
      event(
        2,
        'session.user_event_processed',
        { user_event_id: 'evt_turn' },
        { id: 'evt_accept_turn' },
      ),
      event(3, 'session.status_idle', {
        stop_reason: { type: 'requires_action', event_ids: ['evt_pending'] },
      }),
      event(4, 'user.message', {}, { id: 'evt_overlapping_turn', producedBy: 'client' }),
      event(
        5,
        'session.user_event_processed',
        { user_event_id: 'evt_overlapping_turn' },
        { id: 'evt_accept_overlapping_turn' },
      ),
    ];

    const error = captureProjectionError(events);
    expect(error.issues).toEqual([
      {
        code: 'overlapping_turn_acceptance',
        workspaceId: 'ws_observability',
        sessionId: 'ses_observability',
        markerEventId: 'evt_accept_overlapping_turn',
        sourceEventId: 'evt_overlapping_turn',
      },
    ]);
  });

  it('rejects another continuation after status_running resumes model work', () => {
    const events = [
      event(1, 'user.message', {}, { id: 'evt_turn', producedBy: 'client' }),
      event(
        2,
        'session.user_event_processed',
        { user_event_id: 'evt_turn' },
        { id: 'evt_accept_turn' },
      ),
      event(3, 'session.status_running'),
      event(4, 'session.status_idle', {
        stop_reason: { type: 'requires_action', event_ids: ['evt_pending_first'] },
      }),
      event(
        5,
        'user.tool_confirmation',
        { tool_use_id: 'evt_pending_first', result: 'allow' },
        { id: 'evt_confirm_first', producedBy: 'client' },
      ),
      event(
        6,
        'session.user_event_processed',
        { user_event_id: 'evt_confirm_first' },
        { id: 'evt_accept_confirm_first' },
      ),
      event(7, 'session.status_running'),
      event(
        8,
        'user.tool_confirmation',
        { tool_use_id: 'evt_pending_second', result: 'allow' },
        { id: 'evt_confirm_second', producedBy: 'client' },
      ),
      event(
        9,
        'session.user_event_processed',
        { user_event_id: 'evt_confirm_second' },
        { id: 'evt_accept_confirm_second' },
      ),
    ];

    const error = captureProjectionError(events);
    expect(error.issues).toEqual([
      {
        code: 'overlapping_turn_acceptance',
        workspaceId: 'ws_observability',
        sessionId: 'ses_observability',
        markerEventId: 'evt_accept_confirm_second',
        sourceEventId: 'evt_confirm_second',
      },
    ]);
  });

  it('clamps skewed timestamps into a causally valid hierarchy', () => {
    const events = completedPrimaryTurnEvents();
    events[0]!.producedAt = '2026-01-01T00:00:10.000Z';
    events[2]!.producedAt = '2026-01-01T00:00:09.000Z';
    events[3]!.producedAt = '2026-01-01T00:00:08.000Z';
    events[4]!.producedAt = '2026-01-01T00:00:07.000Z';
    events[6]!.producedAt = '2026-01-01T00:00:20.000Z';
    events[7]!.producedAt = '2026-01-01T00:00:15.000Z';

    const [trace] = projectCanonicalTurns(events);
    const summary = trace!.spans[0]!;
    expect(trace!.root.metadata['orca.turn.accepted_at']).toBe('2026-01-01T00:00:10.000Z');
    expect(trace!.root.startedAt).toBe('2026-01-01T00:00:10.000Z');
    expect(summary.startedAt).toBe('2026-01-01T00:00:10.000Z');
    expect(summary.endedAt).toBe('2026-01-01T00:00:20.000Z');
    expect(trace!.root.endedAt).toBe('2026-01-01T00:00:20.000Z');
  });

  it('keeps the first execution start when duplicate running events precede model activity', () => {
    const events = completedPrimaryTurnEvents();
    events[3]!.producedAt = '2026-01-01T00:00:04.000Z';
    events.splice(4, 0, event(4.5, 'session.status_running', {}, { id: 'evt_duplicate_running' }));
    events[4]!.producedAt = '2026-01-01T00:00:30.000Z';

    const [trace] = projectCanonicalTurns(events);
    expect(trace?.root.startedAt).toBe('2026-01-01T00:00:04.000Z');
  });

  it('retains only safe accepted-input identity across Kafka replay batches', () => {
    const source = event(
      1,
      'user.message',
      { content: TRANSCRIPT_SECRET },
      { id: 'evt_cross_batch_source', producedBy: 'client', userId: USER_ID },
    );
    const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [source]);

    expect(first.completedTraces).toEqual([]);
    expect(first.issues).toEqual([]);
    expect(JSON.stringify(first.state)).not.toContain(TRANSCRIPT_SECRET);

    const second = reduceCanonicalEventBatch(first.state, [
      event(
        2,
        'session.user_event_processed',
        { user_event_id: 'evt_cross_batch_source' },
        { id: 'evt_cross_batch_accept' },
      ),
      event(3, 'session.status_running'),
      event(4, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ]);

    expect(second.issues).toEqual([]);
    expect(second.completedTraces).toHaveLength(1);
    expect(second.completedTraces[0]).toMatchObject({
      anchorEventId: 'evt_cross_batch_source',
      acceptanceEventId: 'evt_cross_batch_accept',
    });
  });

  it('tolerates a duplicate accepted marker after its source was consumed', () => {
    const result = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      event(1, 'user.message', {}, { id: 'evt_once', producedBy: 'client' }),
      event(
        2,
        'session.user_event_processed',
        { user_event_id: 'evt_once' },
        { id: 'evt_accept_once' },
      ),
      event(
        3,
        'session.user_event_processed',
        { user_event_id: 'evt_once' },
        { id: 'evt_accept_duplicate' },
      ),
      event(4, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ]);

    expect(result.issues).toEqual([]);
    expect(result.completedTraces).toHaveLength(1);
  });

  it('retains accepted-source identity beyond the old bounded duplicate window', () => {
    const accepted = new Set(Array.from({ length: 300 }, (_, index) => `evt_accepted_${index}`));
    const result = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      [
        event(
          301,
          'session.user_event_processed',
          { user_event_id: 'evt_accepted_0' },
          { id: 'evt_old_duplicate_marker' },
        ),
      ],
      accepted,
    );

    expect(result.issues).toEqual([]);
    expect(result.acceptedSourceIds).toEqual([]);
  });

  it('bounds never-accepted client input state', () => {
    const inputs = Array.from({ length: 1_025 }, (_, index) =>
      event(index, 'user.message', {}, { id: `evt_pending_${index}`, producedBy: 'client' }),
    );

    expect(() => reduceCanonicalEventBatch(initialCanonicalProjectionState(), inputs)).toThrow(
      CanonicalProjectionStateError,
    );
  });

  it('accepts canonical evt_ identities without inventing a narrower character set', () => {
    const sourceId = 'evt_external:id.with/slash';
    const result = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      event(1, 'user.message', {}, { id: sourceId, producedBy: 'client' }),
      event(
        2,
        'session.user_event_processed',
        { user_event_id: sourceId },
        { id: 'evt_accept_external' },
      ),
      event(3, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ]);

    expect(result.issues).toEqual([]);
    expect(result.completedTraces[0]?.anchorEventId).toBe(sourceId);
  });

  it('ignores native harness IDs and accepts a raw-UUID Transcript archive sentinel', () => {
    const result = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      event(0, 'harness.claude.session_entry', {}, { id: 'native-entry-uuid' }),
      event(1, 'user.message', {}, { id: 'evt_archive_turn', producedBy: 'client' }),
      event(
        2,
        'session.user_event_processed',
        { user_event_id: 'evt_archive_turn' },
        { id: 'evt_accept_archive_turn' },
      ),
      event(3, 'session.status_running'),
      event(4, 'harness.claude.session_entry', {}, { id: 'another-native-entry' }),
      event(5, 'session.archived', {}, { id: '01999f4e-0b22-7000-8000-123456789abc' }),
    ]);

    expect(result.issues).toEqual([]);
    expect(result.completedTraces[0]?.root.metadata).toMatchObject({
      'orca.turn.terminal_reason': 'session.archived',
      'orca.turn.source_event_count': 4,
    });
  });

  it('rejects persisted active-turn collections above their hard limits', () => {
    const started = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      event(1, 'user.message', {}, { id: 'evt_bounded_turn', producedBy: 'client' }),
      event(
        2,
        'session.user_event_processed',
        { user_event_id: 'evt_bounded_turn' },
        { id: 'evt_accept_bounded_turn' },
      ),
    ]);
    const active = started.state.activeTurn;
    expect(active).not.toBeNull();
    Object.assign(active!, {
      companionSystemEventIds: Array.from({ length: 65 }, (_, index) => `evt_companion_${index}`),
    });

    expect(() => reduceCanonicalEventBatch(started.state, [])).toThrow(
      CanonicalProjectionStateError,
    );
  });
});

function captureProjectionError(events: Parameters<typeof projectCanonicalTurns>[0]) {
  try {
    projectCanonicalTurns(events);
  } catch (error) {
    if (error instanceof CanonicalProjectionError) return error;
    throw error;
  }
  throw new Error('expected canonical projection error');
}
