// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  CanonicalProjectionStateError,
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  parseCompletedToolSpan,
  reduceCanonicalEventBatch,
} from '../../src/projector.js';
import { MAX_PROJECTED_TOOLS_PER_TURN } from '../../src/tool-projection.js';
import { boundedAgentEventId } from '../../src/event-identity.js';
import { deterministicChildSpanId } from '../../src/ids.js';
import { TRACE_SAMPLING_VERSION } from '../../src/sampling.js';
import { event } from '../support/events.js';

const start = () => [
  event(1, 'user.message', {}, { producedBy: 'client' }),
  event(2, 'session.user_event_processed', { user_event_id: 'evt_1' }),
];
const end = () => event(20, 'session.status_idle', { stop_reason: { type: 'end_turn' } });
describe('metadata-only primary tool projection', () => {
  it.each(['local', 'mcp'])(
    'pairs %s public and native correlation without exporting content',
    (family) => {
      const prefix = family === 'local' ? 'agent.tool' : 'agent.mcp_tool';
      for (const native of [false, true]) {
        const use = event(3, `${prefix}_use`, {
          id: 'ignored-payload-id',
          ...(native ? { tool_use_id: 'native-secret' } : {}),
          name: 'secret-name',
          input: 'secret-input',
        });
        const result = event(4, `${prefix}_result`, {
          tool_use_id: native ? 'native-secret' : 'evt_3',
          content: 'secret-content',
          is_error: true,
        });
        const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
          ...start(),
          use,
        ]);
        const restored = parseCanonicalProjectionState(JSON.parse(JSON.stringify(first.state)));
        const trace = reduceCanonicalEventBatch(restored, [result, end()]).completedTraces[0]!;
        expect(trace.spans).toHaveLength(1);
        expect(trace.spans[0]).toMatchObject({
          name: 'orca.agent.tool',
          observationType: 'tool',
          status: 'error',
          parentSpanId: trace.root.spanId,
          spanId: deterministicChildSpanId(trace.traceId, 'tool', '', 'evt_3'),
          metadata: {
            'orca.tool.family': family,
            'orca.tool.outcome': 'error',
            'orca.source.end_event_id': 'evt_4',
          },
        });
        expect(trace.root.status).toBe('ok');
        expect(JSON.stringify([first.state, trace])).not.toContain('secret');
      }
    },
  );
  it('waits for exact acceptance of multiple client results, ignores confirmation', () => {
    const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...start(),
      event(3, 'agent.custom_tool_use'),
      event(4, 'agent.tool_use'),
      event(5, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
      event(
        6,
        'user.custom_tool_result',
        { custom_tool_use_id: 'evt_3', is_error: false },
        { producedBy: 'client' },
      ),
      event(
        7,
        'user.tool_result',
        { tool_use_id: 'evt_4', is_error: true },
        { producedBy: 'client' },
      ),
      event(8, 'user.tool_confirmation', { tool_use_id: 'evt_4' }, { producedBy: 'client' }),
      event(9, 'session.user_event_processed', { user_event_id: 'evt_8' }),
    ]);
    const trace = reduceCanonicalEventBatch(parseCanonicalProjectionState(first.state), [
      event(10, 'session.user_event_processed', { user_event_id: 'evt_7' }),
      end(),
    ]).completedTraces[0]!;
    expect(trace.spans).toHaveLength(2);
    expect(trace.spans.find((s) => s.sourceEventId === 'evt_3')).toMatchObject({
      status: 'unset',
      metadata: { 'orca.tool.outcome': 'incomplete' },
    });
    expect(trace.spans.find((s) => s.sourceEventId === 'evt_4')).toMatchObject({
      status: 'error',
      metadata: { 'orca.source.end_event_id': 'evt_7' },
    });
  });
  it('skips sampled-out agent tool payloads but reads client acceptance-control metadata', () => {
    const sources = [
      ...start(),
      event(3, 'agent.tool_use'),
      event(4, 'agent.tool_result'),
      event(5, 'user.tool_result', {}, { producedBy: 'client' }),
      end(),
    ];
    const getter = vi.fn(() => {
      throw new Error('payload read');
    });
    for (const e of sources.slice(2, 4)) Object.defineProperty(e, 'payload', { get: getter });
    const clientPayload = sources[4]!.payload;
    const clientGetter = vi.fn(() => clientPayload);
    Object.defineProperty(sources[4]!, 'payload', { get: clientGetter });
    expect(
      reduceCanonicalEventBatch(initialCanonicalProjectionState(), sources, new Set(), {
        algorithmVersion: TRACE_SAMPLING_VERSION,
        bindingId: 'aob_tools',
        bindingVersion: 1,
        sampleRate: 0,
      }).completedTraces,
    ).toEqual([]);
    expect(getter).not.toHaveBeenCalled();
    expect(clientGetter).toHaveBeenCalledOnce();
  });
  it('pairs oversized canonical IDs without leaking or double-normalizing them', () => {
    const id = 'evt_' + 'x'.repeat(800);
    const trace = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...start(),
      event(3, 'agent.tool_use', {}, { id }),
      event(4, 'agent.tool_result', { tool_use_id: id }),
      end(),
    ]).completedTraces[0]!;
    expect(trace.spans[0]).toMatchObject({ sourceEventId: boundedAgentEventId(id), status: 'ok' });
    expect(JSON.stringify(trace)).not.toContain(id);
  });
  it('deduplicates replayed sources and rejects conflicting correlation identities', () => {
    const use = event(3, 'agent.tool_use', { tool_use_id: 'native' });
    const result = event(4, 'agent.tool_result', { tool_use_id: 'native' });
    const project = (events: ReturnType<typeof event>[]) =>
      reduceCanonicalEventBatch(initialCanonicalProjectionState(), events);
    expect(project([...start(), use, use, result, result, end()]).completedTraces).toEqual(
      project([...start(), use, result, end()]).completedTraces,
    );
    expect(() =>
      project([...start(), use, event(3, 'agent.tool_use', { tool_use_id: 'different' })]),
    ).toThrow(CanonicalProjectionStateError);
    expect(() =>
      project([
        ...start(),
        use,
        result,
        event(4, 'agent.tool_result', { tool_use_id: 'native', is_error: true }),
      ]),
    ).toThrow(CanonicalProjectionStateError);
  });
  it('keeps family/subpath isolated and does not invent cancellation or missing-ID completion', () => {
    const sources = [
      ...start(),
      event(3, 'agent.tool_use', { tool_use_id: 'shared' }),
      event(4, 'agent.mcp_tool_result', { tool_use_id: 'shared' }),
      event(5, 'agent.tool_result'),
      event(6, 'agent.tool_result', { tool_use_id: 'shared' }, { subpath: 'child' }),
      event(7, 'user.interrupt', {}, { producedBy: 'client' }),
      event(8, 'session.user_event_processed', { user_event_id: 'evt_7' }),
    ];
    const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), sources);
    expect(first.issues).toEqual([]);
    expect(first.state.activeTurn?.tools.unmatchedResultCount).toBe(2);
    const span = reduceCanonicalEventBatch(first.state, [end()]).completedTraces[0]!.spans[0]!;
    expect(span).toMatchObject({
      status: 'unset',
      endedAt: end().producedAt,
      metadata: { 'orca.tool.outcome': 'incomplete', 'orca.source.end_event_id': end().id },
    });
    expect(parseCompletedToolSpan(span)).toEqual(span);
    expect(() =>
      parseCompletedToolSpan({ ...span, metadata: { ...span.metadata, name: 'secret' } }),
    ).toThrow(CanonicalProjectionStateError);
    expect(() => parseCompletedToolSpan({ ...span, status: 'ok' })).toThrow(
      CanonicalProjectionStateError,
    );
  });
  it('bounds retained tools/results and saturates the orphan counter', () => {
    const base = reduceCanonicalEventBatch(initialCanonicalProjectionState(), start()).state;
    const uses = Array.from({ length: MAX_PROJECTED_TOOLS_PER_TURN }, (_, i) =>
      event(i + 3, 'agent.tool_use'),
    );
    const full = reduceCanonicalEventBatch(base, uses).state;
    expect(full.activeTurn?.tools.uses).toHaveLength(MAX_PROJECTED_TOOLS_PER_TURN);
    expect(() => reduceCanonicalEventBatch(full, [event(999, 'agent.tool_use')])).toThrow(
      CanonicalProjectionStateError,
    );
    base.activeTurn!.tools.unmatchedResultCount = Number.MAX_SAFE_INTEGER;
    expect(
      reduceCanonicalEventBatch(base, [event(3, 'agent.tool_result')]).state.activeTurn?.tools
        .unmatchedResultCount,
    ).toBe(Number.MAX_SAFE_INTEGER);
    const results = Array.from({ length: MAX_PROJECTED_TOOLS_PER_TURN }, (_, i) =>
      event(i + 3, 'agent.tool_result'),
    );
    const orphans = reduceCanonicalEventBatch(base, results).state;
    expect(() => reduceCanonicalEventBatch(orphans, [event(999, 'agent.tool_result')])).toThrow(
      CanonicalProjectionStateError,
    );
  });
  it('does not attach an old unaccepted result to a later turn reusing a native ID', () => {
    const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...start(),
      event(3, 'agent.tool_use', { tool_use_id: 'reused' }),
      event(4, 'user.tool_result', { tool_use_id: 'reused' }, { producedBy: 'client' }),
      end(),
    ]);
    const next = [
      event(21, 'user.message', {}, { producedBy: 'client' }),
      event(22, 'session.user_event_processed', { user_event_id: 'evt_21' }),
      event(23, 'agent.tool_use', { tool_use_id: 'reused' }),
      event(24, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
      event(25, 'session.user_event_processed', { user_event_id: 'evt_4' }),
      event(26, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ];
    const trace = reduceCanonicalEventBatch(parseCanonicalProjectionState(first.state), next)
      .completedTraces[0]!;
    expect(trace.spans[0]?.status).toBe('unset');
  });
  it('rejects conflicting pending client result identities without reading content', () => {
    const use = event(3, 'agent.tool_use');
    const response = event(
      4,
      'user.tool_result',
      { tool_use_id: 'evt_3' },
      { producedBy: 'client' },
    );
    const state = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...start(),
      use,
      response,
    ]).state;
    expect(reduceCanonicalEventBatch(state, [response]).state).toEqual(state);
    expect(() =>
      reduceCanonicalEventBatch(state, [
        event(4, 'user.tool_result', { tool_use_id: 'different' }, { producedBy: 'client' }),
      ]),
    ).toThrow(CanonicalProjectionStateError);
  });
  it('retains invalid explicit use correlation as incomplete, never as canonical fallback', () => {
    const trace = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...start(),
      event(3, 'agent.tool_use', { tool_use_id: null }),
      event(4, 'agent.tool_result', { tool_use_id: 'evt_3' }),
      end(),
    ]).completedTraces[0]!;
    expect(trace.spans).toHaveLength(1);
    expect(trace.spans[0]?.status).toBe('unset');
  });
  it.each([1, 2] as const)(
    'upgrades v%s state without accepting injected new tool fields',
    (version) => {
      const state = reduceCanonicalEventBatch(initialCanonicalProjectionState(), start()).state;
      const legacy = {
        ...state,
        version,
        ...(version === 2
          ? {
              sampling: {
                policy: {
                  algorithmVersion: TRACE_SAMPLING_VERSION,
                  bindingId: 'aob_tools',
                  bindingVersion: 1,
                  sampleRate: 1,
                },
                suppressed: null,
              },
            }
          : {}),
      };
      legacy.activeTurn!.tools = { uses: [], results: [], unmatchedResultCount: 999 };
      const restored = reduceCanonicalEventBatch(legacy, []);
      expect(restored.state.version).toBe(3);
      expect(restored.state.activeTurn?.tools.unmatchedResultCount).toBe(0);
    },
  );
});
