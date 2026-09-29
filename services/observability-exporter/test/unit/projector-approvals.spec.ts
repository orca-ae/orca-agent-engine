// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  parseCompletedToolSpan,
  reduceCanonicalEventBatch,
} from '../../src/projector.js';
import { TRACE_SAMPLING_VERSION } from '../../src/sampling.js';
import { event } from '../support/events.js';
import { parseProjectedTrace } from '../../src/canonical-validation.js';

const accept = (seq: number, id: number) =>
  event(seq, 'session.user_event_processed', { user_event_id: `evt_${id}` });
const end = (seq = 10) => event(seq, 'session.status_idle', { stop_reason: { type: 'end_turn' } });
const confirmation = (result: unknown = 'allow', id = 'native-secret') =>
  event(
    5,
    'user.tool_confirmation',
    { tool_use_id: id, result, deny_message: 'private-denial' },
    { producedBy: 'client' },
  );
const start = (kind = 'agent.tool_use') => [
  event(1, 'user.message', {}, { producedBy: 'client' }),
  accept(2, 1),
  event(3, kind, { tool_use_id: 'native-secret' }),
  event(4, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
];
const reduce = (
  events: ReturnType<typeof event>[],
  state = initialCanonicalProjectionState(),
  ids = new Set<string>(),
) => reduceCanonicalEventBatch(state, events, ids, undefined, 'raw_io');
const restore = (state: unknown) =>
  parseCanonicalProjectionState(JSON.parse(JSON.stringify(state)));
const key = 'orca.tool.last_approval.';

describe('explicit accepted approval facts', () => {
  it.each(['allow', 'deny'])('records %s without asserting execution success/failure', (result) => {
    const projected = reduce([...start(), confirmation(result), accept(6, 5), end()]);
    const trace = projected.completedTraces[0]!;
    const tool = trace.spans[0]!;
    expect(tool.metadata).toMatchObject({
      [key + 'result']: result,
      [key + 'source_event_id']: 'evt_5',
      [key + 'acceptance_event_id']: 'evt_6',
      [key + 'accepted_at']: event(6, '').producedAt,
    });
    expect(tool.status).toBe('unset');
    expect(tool.metadata['orca.tool.outcome']).toBe('incomplete');
    expect(trace.root.status).toBe('ok');
    expect(parseCompletedToolSpan(tool)).toEqual(tool);
    expect(parseProjectedTrace(trace)).toEqual(trace);
    expect(JSON.stringify(trace)).not.toMatch(/native-secret|private-denial/);
  });

  it('is identical at every batch/restart boundary, including duplicate source/marker replay', () => {
    const events = [...start(), confirmation(), accept(6, 5), end()];
    const expected = reduce(events).completedTraces;
    for (let split = 0; split <= events.length; split++) {
      const first = reduce(events.slice(0, split));
      const second = reduce(
        events.slice(split),
        restore(first.state),
        new Set(first.acceptedSourceIds),
      );
      expect([...first.completedTraces, ...second.completedTraces]).toEqual(expected);
    }
    const first = reduce([...start(), confirmation(), confirmation(), accept(6, 5)]);
    expect(first.state.activeTurn?.awaitingAction).toBe(true);
    expect(first.state.activeTurn?.continuationCount).toBe(1);
    expect(
      reduce(
        [confirmation(), accept(6, 5), end()],
        restore(first.state),
        new Set(first.acceptedSourceIds),
      ).completedTraces,
    ).toEqual(expected);
  });

  it.each([undefined, 'ALLOW', 'approve', true, null])(
    'omits non-contract verdict %s',
    (result) => {
      const source = event(
        5,
        'user.tool_confirmation',
        { tool_use_id: 'native-secret', result },
        { producedBy: 'client' },
      );
      expect(
        JSON.stringify(reduce([...start(), source, accept(6, 5), end()]).completedTraces),
      ).not.toContain(key);
    },
  );

  it('requires an exact source marker and unique supported correlation', () => {
    for (const events of [
      [...start(), confirmation(), end()],
      [...start(), confirmation(), accept(6, 99), end()],
      [...start(), confirmation('allow', 'orphan'), accept(6, 5), end()],
      [
        ...start(),
        event(4, 'agent.mcp_tool_use', { tool_use_id: 'native-secret' }, { id: 'evt_other' }),
        confirmation(),
        accept(6, 5),
        end(),
      ],
      [...start('agent.custom_tool_use'), confirmation('allow', 'evt_3'), accept(6, 5), end()],
    ]) {
      const projected = reduce(events);
      expect(JSON.stringify(projected.completedTraces)).not.toContain(key);
      expect(
        projected.completedTraces[0]?.root.metadata['orca.turn.unmatched_tool_result_count'] ?? 0,
      ).toBe(0);
    }
    expect(
      JSON.stringify(
        reduce([...start('agent.mcp_tool_use'), confirmation(), accept(6, 5), end()])
          .completedTraces,
      ),
    ).toContain(key);
  });

  it('omits metadata-only and sampled-out facts', () => {
    const events = [...start(), confirmation(), accept(6, 5), end()];
    expect(
      JSON.stringify(reduceCanonicalEventBatch(initialCanonicalProjectionState(), events)),
    ).not.toContain(key);
    const sampled = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      events,
      new Set(),
      {
        algorithmVersion: TRACE_SAMPLING_VERSION,
        bindingId: 'binding',
        bindingVersion: 1,
        sampleRate: 0,
      },
      'raw_io',
    );
    expect(sampled.completedTraces).toEqual([]);
    expect(JSON.stringify(sampled.state)).not.toMatch(/approval|native-secret|private-denial/);
  });

  it('scrubs pending and completed approvals on restriction and legacy restore', () => {
    const first = reduce([
      ...start(),
      confirmation(),
      accept(6, 5),
      event(7, 'agent.tool_result', { tool_use_id: 'native-secret' }),
    ]);
    expect(JSON.stringify(first.state)).toContain(key);
    const restricted = reduceCanonicalEventBatch(
      restore(first.state),
      [],
      new Set(),
      undefined,
      'metadata_only',
    );
    expect(JSON.stringify(restricted.state)).not.toMatch(/approval/i);
    expect(JSON.stringify(restore({ ...first.state, captureMode: 'metadata_only' }))).not.toMatch(
      /approval/i,
    );
    expect(JSON.stringify(restore({ ...first.state, version: 3 }))).not.toMatch(/approval/i);
    expect(JSON.stringify(restore({ ...first.state, version: 1 }))).not.toMatch(/approval/i);
    const pending = reduce([...start(), confirmation()]);
    expect(
      JSON.stringify(
        reduceCanonicalEventBatch(pending.state, [], new Set(), undefined, 'metadata_only').state,
      ),
    ).not.toMatch(/approval/i);
  });

  it('does not bind pre-turn receipts or old-trace receipts to the current turn', () => {
    const preturn = event(
      0,
      'user.tool_confirmation',
      { tool_use_id: 'native-secret', result: 'allow' },
      { producedBy: 'client' },
    );
    const first = reduce([preturn]);
    expect(first.state.pendingInputs[0]?.toolApproval).toBeUndefined();
    const next = reduce([...start(), accept(6, 0), end()], restore(first.state));
    expect(JSON.stringify(next.completedTraces)).not.toContain(key);
    const old = reduce([...start(), confirmation(), end()]);
    const events = start().map((entry) => ({
      ...entry,
      seq: entry.seq + 20,
      id: entry.id === 'evt_1' ? 'evt_new' : entry.id,
    }));
    events[1] = event(22, 'session.user_event_processed', { user_event_id: 'evt_new' });
    const later = reduce(
      [...events, accept(26, 5), end(30)],
      restore(old.state),
      new Set(old.acceptedSourceIds),
    );
    expect(later.issues).toEqual([]);
    expect(JSON.stringify(later.completedTraces)).not.toContain(key);
  });

  it('rechecks cross-family ambiguity at acceptance, including custom source IDs', () => {
    const first = reduce([...start(), confirmation()]);
    const next = reduce(
      [event(6, 'agent.mcp_tool_use', { tool_use_id: 'native-secret' }), accept(7, 5), end()],
      restore(first.state),
    );
    expect(JSON.stringify(next.completedTraces)).not.toContain(key);
    const prefix = start();
    prefix[2] = event(3, 'agent.tool_use', { tool_use_id: 'evt_shared' });
    const custom = reduce([
      ...prefix,
      event(4, 'agent.custom_tool_use', {}, { id: 'evt_shared' }),
      confirmation('allow', 'evt_shared'),
      accept(6, 5),
      end(),
    ]);
    expect(JSON.stringify(custom.completedTraces)).not.toContain(key);
  });

  it.each(['agent.tool_use', 'agent.mcp_tool_use'])(
    'supports public source-ID correlation for %s',
    (kind) => {
      const events = start(kind);
      events[2] = event(3, kind, { name: 'read' });
      const projected = reduce([...events, confirmation('allow', 'evt_3'), accept(6, 5), end()]);
      expect(projected.completedTraces[0]?.spans[0]?.metadata[key + 'result']).toBe('allow');
    },
  );

  it('retains only the last accepted approval, without changing completed tool status', () => {
    const first = reduce([
      ...start(),
      confirmation(),
      accept(6, 5),
      event(7, 'agent.tool_result', { tool_use_id: 'native-secret', is_error: true }),
    ]);
    const second = reduce(
      [
        event(
          8,
          'user.tool_confirmation',
          { tool_use_id: 'native-secret', result: 'deny' },
          { producedBy: 'client' },
        ),
        accept(9, 8),
      ],
      restore(first.state),
      new Set(first.acceptedSourceIds),
    );
    const trace = reduce([end()], restore(second.state)).completedTraces[0]!;
    expect(trace.spans[0]?.metadata[key + 'result']).toBe('deny');
    expect(trace.spans[0]?.metadata[key + 'source_event_id']).toBe('evt_8');
    expect(trace.spans[0]?.metadata['orca.tool.outcome']).toBe('error');
    expect(trace.spans[0]?.status).toBe('error');
    expect(trace.root.status).toBe('ok');
  });

  it('strictly parses pending receipts, accepted facts and TOOL metadata', () => {
    const pending = reduce([...start(), confirmation()]).state;
    const accepted = reduce([accept(6, 5)], pending).state;
    for (const field of ['result', 'sourceEventId', 'acceptanceEventId', 'acceptedAt']) {
      const bad = structuredClone(accepted);
      Object.assign(bad.activeTurn!.tools.uses[0]!.lastApproval!, { [field]: 'bad' });
      expect(() => restore(bad)).toThrow();
    }
    const badPending = structuredClone(pending);
    badPending.pendingInputs[0]!.toolApproval!.correlationKeys.local = 'raw-native';
    expect(() => restore(badPending)).toThrow();
    const badTrace = structuredClone(pending);
    badTrace.pendingInputs[0]!.toolTraceId = 'bad';
    expect(() => restore(badTrace)).toThrow();
    const tool = reduce([end()], accepted).completedTraces[0]!.spans[0]!;
    for (const field of ['result', 'source_event_id', 'acceptance_event_id', 'accepted_at']) {
      const bad = structuredClone(tool);
      bad.metadata[key + field] = 'bad';
      expect(() => parseCompletedToolSpan(bad)).toThrow();
      delete bad.metadata[key + field];
      expect(() => parseCompletedToolSpan(bad)).toThrow();
    }
    const v1 = structuredClone(tool);
    delete v1.io;
    v1.metadata['orca.projection.schema_version'] = 'orca.observability.projected-trace.v1';
    expect(() => parseCompletedToolSpan(v1)).toThrow();
  });

  it('keeps metadata-only duplicate receipt behavior unchanged', () => {
    const source = confirmation();
    const repeated = { ...source, producedAt: event(9, '').producedAt };
    expect(() =>
      reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
        ...start(),
        source,
        repeated,
        accept(6, 5),
        end(),
      ]),
    ).not.toThrow();
  });

  it('scrubs facts on sampled-out restore and never exports receipt correlation hashes', () => {
    const pending = reduce([...start(), confirmation()]).state;
    const keys = Object.values(pending.pendingInputs[0]!.toolApproval!.correlationKeys);
    const accepted = reduce([accept(6, 5)], pending).state;
    const projected = JSON.stringify(reduce([end()], accepted).completedTraces);
    for (const hash of keys) expect(projected).not.toContain(hash);
    const sampling = {
      policy: {
        algorithmVersion: TRACE_SAMPLING_VERSION,
        bindingId: 'binding',
        bindingVersion: 1,
        sampleRate: 0,
      },
      suppressed: null,
    };
    expect(JSON.stringify(restore({ ...pending, sampling }))).not.toMatch(/approval/i);
    expect(JSON.stringify(restore({ ...accepted, sampling }))).not.toMatch(/approval/i);
  });
});
