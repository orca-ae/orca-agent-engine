// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  parseCompletedToolSpan,
  reduceCanonicalEventBatch,
  type CanonicalProjectionState,
} from '../../src/projector.js';
import {
  IO_PROJECTED_TRACE_SCHEMA_VERSION,
  PROJECTED_TRACE_SCHEMA_VERSION,
  IO_VERSION,
} from '../../src/types.js';
import {
  MAX_PENDING_CAPTURED_IO_BYTES,
  MAX_CAPTURED_IO_BYTES_PER_TURN,
  MAX_CAPTURED_VALUE_BYTES,
} from '../../src/captured-io.js';
import { deterministicTraceId } from '../../src/ids.js';
import { parseProjectedTrace } from '../../src/canonical-validation.js';
import { isTraceSampled, TRACE_SAMPLING_VERSION } from '../../src/sampling.js';
import { completedPrimaryTurnEvents, event, SESSION_ID, WORKSPACE_ID } from '../support/events.js';

const user = (seq: number, content: unknown) =>
  event(seq, 'user.message', { content }, { producedBy: 'client' });
const accept = (seq: number, source: number) =>
  event(seq, 'session.user_event_processed', { user_event_id: `evt_${source}` });
const end = (seq = 20) => event(seq, 'session.status_idle', { stop_reason: { type: 'end_turn' } });
const start = () => [user(1, 'question'), accept(2, 1)];
const reduce = (events: ReturnType<typeof event>[], state = initialCanonicalProjectionState()) =>
  reduceCanonicalEventBatch(state, events, new Set(), undefined, 'raw_io');
const restore = (state: CanonicalProjectionState) =>
  parseCanonicalProjectionState(JSON.parse(JSON.stringify(state)));

describe('raw turn/tool projection', () => {
  it('preserves the v3 metadata baseline but pins meaningful legacy state against expansion', () => {
    const empty = reduceCanonicalEventBatch(initialCanonicalProjectionState(), []);
    expect(empty.state).toEqual(initialCanonicalProjectionState());
    const metadata = reduceCanonicalEventBatch(empty.state, start());
    expect(metadata.state.version).toBe(3);
    expect(metadata.state.captureMode).toBeUndefined();
    const denied = reduce([end()], restore(metadata.state));
    expect(denied.state.version).toBe(4);
    expect(denied.state.captureMode).toBe('metadata_only');
    expect(denied.completedTraces[0]?.root.io).toBeUndefined();
    const later = reduce(
      [user(21, 'must-not-reopen'), accept(22, 21), end(23)],
      restore(denied.state),
    );
    expect(later.completedTraces[0]?.root.io).toBeUndefined();
    const knownCompleted = reduceCanonicalEventBatch(
      empty.state,
      [user(21, 'must-not-expand')],
      new Set(['evt_1']),
      undefined,
      'raw_io',
    );
    expect(knownCompleted.state.captureMode).toBe('metadata_only');
    expect(knownCompleted.state.pendingInputs[0]?.input).toBeUndefined();
  });

  it.each([false, true])(
    'marks oversized output gaps without reopening accumulation (prior message: %s)',
    (prior) => {
      const result = reduce([
        ...start(),
        ...(prior ? [event(3, 'agent.message', { content: 'before' })] : []),
        event(4, 'agent.message', { content: 'x'.repeat(9000) }),
        event(5, 'agent.message', { content: 'after-gap-canary' }),
      ]);
      expect(JSON.stringify(restore(result.state))).not.toContain('after-gap-canary');
      expect(result.state.activeTurn?.io?.output).toMatchObject(
        prior ? { truncated: true } : { omitted: 'too_large' },
      );
      expect(parseProjectedTrace(reduce([end()], result.state).completedTraces[0])).toBeDefined();
    },
  );

  it.each(['local', 'mcp', 'custom'])(
    'projects complete %s fixtures identically across every batch boundary',
    (family) => {
      const prefix =
        family === 'mcp'
          ? 'agent.mcp_tool'
          : family === 'custom'
            ? 'agent.custom_tool'
            : 'agent.tool';
      const events = [
        ...start(),
        event(3, `${prefix}_use`, {
          name: 'lookup',
          input: { query: 'weather', password: 'secret-canary' },
        }),
        ...(family === 'custom'
          ? [
              event(4, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
              event(
                5,
                'user.custom_tool_result',
                { custom_tool_use_id: 'evt_3', content: 'sunny' },
                { producedBy: 'client' },
              ),
              accept(6, 5),
            ]
          : [
              event(4, `${prefix}_result`, {
                tool_use_id: 'evt_3',
                content: [{ type: 'text', text: 'sunny' }],
              }),
            ]),
        event(7, 'agent.message', { content: [{ type: 'text', text: 'answer' }] }),
        end(),
      ];
      const whole = reduce(events);
      let state = initialCanonicalProjectionState();
      const traces = [];
      for (const entry of events) {
        const result = reduce([entry], restore(state));
        state = result.state;
        traces.push(...result.completedTraces);
      }
      expect(traces).toEqual(whole.completedTraces);
      const trace = traces[0]!;
      expect(parseProjectedTrace(trace)).toEqual(trace);
      expect(trace.schemaVersion).toBe(IO_PROJECTED_TRACE_SCHEMA_VERSION);
      expect(trace.root.io?.input?.json).toContain('question');
      expect(trace.root.io?.output?.json).toContain('answer');
      expect(trace.root.io?.outputScope).toBe('turn_messages');
      expect(trace.spans[0]?.name).toBe('orca.agent.tool');
      expect(trace.spans[0]?.io?.toolName).toBe('lookup');
      expect(JSON.parse(trace.spans[0]!.io!.input!.json!)).toEqual({
        query: 'weather',
        password: 'secret-canary',
      });
      expect(trace.spans[0]?.io?.output?.json).toContain('sunny');
      for (const span of [trace.root, ...trace.spans])
        expect(span.metadata['orca.projection.schema_version']).toBe(trace.schemaVersion);
      expect(parseCompletedToolSpan(trace.spans[0])).toEqual(trace.spans[0]);
      expect(() =>
        parseCompletedToolSpan({
          ...trace.spans[0],
          metadata: {
            ...trace.spans[0]!.metadata,
            'orca.projection.schema_version': PROJECTED_TRACE_SCHEMA_VERSION,
          },
        }),
      ).toThrow();
    },
  );

  it('finalizes model-summary schema with the same v2 as the root', () => {
    const trace = reduce(completedPrimaryTurnEvents()).completedTraces[0]!;
    expect(trace.spans[0]?.observationType).toBe('turn_model_summary');
    expect(trace.spans[0]?.metadata['orca.projection.schema_version']).toBe(
      IO_PROJECTED_TRACE_SCHEMA_VERSION,
    );
    expect(parseProjectedTrace(trace)).toEqual(trace);
  });

  it('joins only accepted input; excludes system, interrupt, thinking and partial/delta content', () => {
    const trace = reduce([
      user(1, 'unaccepted-canary'),
      user(2, 'accepted question'),
      accept(3, 2),
      event(4, 'system.message', { content: 'system-canary' }, { producedBy: 'client' }),
      event(5, 'user.interrupt', { content: 'interrupt-canary' }, { producedBy: 'client' }),
      event(6, 'agent.message', {
        partial: true,
        content: [{ type: 'text', text: 'partial-canary' }],
      }),
      event(7, 'agent.message', {
        content: [
          { type: 'thinking', thinking: 'thinking-canary' },
          { type: 'text_delta', text: 'delta-canary' },
        ],
      }),
      event(8, 'agent.message', { content: [{ type: 'text', text: 'first' }] }),
      event(9, 'agent.message', { content: [{ type: 'text', text: 'second' }] }),
      end(),
    ]).completedTraces[0]!;
    expect(JSON.stringify(trace)).not.toContain('canary');
    expect(trace.root.io?.input?.json).toContain('accepted question');
    expect(trace.root.io?.output?.json?.indexOf('first')).toBeLessThan(
      trace.root.io!.output!.json!.indexOf('second'),
    );
  });

  it.each([0, 0.35])('samples candidates before acceptance at rate %s', (sampleRate) => {
    const policy = {
      algorithmVersion: TRACE_SAMPLING_VERSION,
      bindingId: 'binding',
      bindingVersion: 1,
      sampleRate,
    };
    const inputs = Array.from({ length: 50 }, (_, i) => user(i + 1, `candidate-${i + 1}`));
    const result = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      inputs,
      new Set(),
      policy,
      'raw_io',
    );
    for (const input of result.state.pendingInputs) {
      const sampled = isTraceSampled(
        policy,
        deterministicTraceId(WORKSPACE_ID, SESSION_ID, input.eventId),
      );
      expect(input.input !== undefined).toBe(sampled);
    }
    expect(restore(result.state)).toEqual(result.state);
  });

  it('caps pending and per-turn bytes, including completed tools and root accumulation', () => {
    const pending = reduce(
      Array.from({ length: 100 }, (_, i) => user(i + 1, 'x'.repeat(7000))),
    ).state;
    const bytes = (value: unknown) => Buffer.byteLength(JSON.stringify(value));
    expect(
      pending.pendingInputs.reduce((sum, input) => sum + bytes(input.input), 0),
    ).toBeLessThanOrEqual(MAX_PENDING_CAPTURED_IO_BYTES);
    expect(pending.pendingInputs.some((input) => input.input?.omitted === 'budget')).toBe(true);
    expect(restore(pending)).toEqual(pending);
    const events = start();
    for (let i = 0; i < 60; i++) {
      events.push(
        event(3 + i * 3, 'agent.tool_use', { name: 'lookup', input: { data: 'x'.repeat(7000) } }),
      );
      events.push(
        event(4 + i * 3, 'agent.tool_result', {
          tool_use_id: `evt_${3 + i * 3}`,
          content: 'y'.repeat(7000),
        }),
      );
      events.push(event(5 + i * 3, 'agent.message', { content: 'z'.repeat(7000) }));
    }
    const result = reduce(events);
    expect(restore(result.state)).toEqual(result.state);
    const trace = reduce([end(200)], result.state).completedTraces[0]!;
    expect(
      [trace.root, ...trace.spans].reduce(
        (sum, span) => sum + (span.io === undefined ? 0 : bytes(span.io)),
        0,
      ),
    ).toBeLessThanOrEqual(MAX_CAPTURED_IO_BYTES_PER_TURN);
    expect(Buffer.byteLength(trace.root.io!.output!.json!)).toBeLessThanOrEqual(
      MAX_CAPTURED_VALUE_BYTES,
    );
    expect(trace.root.io?.output?.truncated).toBe(true);
    const forged = structuredClone(pending);
    for (const input of forged.pendingInputs)
      input.input = { json: JSON.stringify('x'.repeat(7000)) };
    expect(() => restore(forged)).toThrow();
  });

  it('scrubs pending, open and completed I/O on revoke and never reopens capture', () => {
    const first = reduce([
      ...start(),
      event(3, 'agent.tool_use', { name: 'tool-canary', input: 'arg-canary' }),
      event(4, 'agent.tool_result', { tool_use_id: 'evt_3', content: 'result-canary' }),
      event(5, 'agent.tool_use', { name: 'open-canary', input: 'open-input-canary' }),
      user(6, 'pending-canary'),
    ]);
    const restricted = reduceCanonicalEventBatch(
      first.state,
      [],
      new Set(),
      undefined,
      'metadata_only',
    );
    expect(restricted.state.captureMode).toBe('metadata_only');
    expect(JSON.stringify(restricted.state)).not.toContain('canary');
    expect(JSON.stringify(restore({ ...first.state, captureMode: 'metadata_only' }))).not.toContain(
      'canary',
    );
    const completed = reduce(
      [event(7, 'agent.message', { content: 'new-canary' }), end()],
      restricted.state,
    );
    expect(completed.completedTraces[0]?.schemaVersion).toBe(PROJECTED_TRACE_SCHEMA_VERSION);
    expect(JSON.stringify(completed)).not.toContain('canary');
    const later = reduce([user(21, 'later-canary'), accept(22, 21), end(23)], completed.state);
    expect(later.completedTraces[0]?.root.io).toBeUndefined();
  });

  it('retains client result text only until exact acceptance and preserves duplicate receipts', () => {
    const use = event(3, 'agent.tool_use', { name: 'lookup', input: { query: 'weather' } });
    const result = event(
      5,
      'user.tool_result',
      { tool_use_id: 'evt_3', content: 'client-result-canary' },
      { producedBy: 'client' },
    );
    const waiting = reduce([
      ...start(),
      use,
      event(4, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
      result,
    ]);
    expect(waiting.state.activeTurn?.tools.uses[0]?.completed).toBeUndefined();
    expect(waiting.state.pendingInputs[0]?.toolResult?.output?.json).toContain(
      'client-result-canary',
    );
    const revoked = reduceCanonicalEventBatch(
      waiting.state,
      [],
      new Set(),
      undefined,
      'metadata_only',
    );
    expect(JSON.stringify(revoked.state)).not.toContain('client-result-canary');
    const accepted = reduce([result, accept(6, 5)], restore(waiting.state));
    expect(accepted.state.activeTurn?.tools.results[0]?.result.output).toBeUndefined();
    expect(accepted.state.activeTurn?.tools.uses[0]?.completed?.io?.output?.json).toContain(
      'client-result-canary',
    );
    const completed = reduce([accept(6, 5), end()], restore(accepted.state));
    expect(completed.completedTraces[0]?.spans).toHaveLength(1);
  });

  it('bounds tiny output message counts as well as bytes across checkpoints', () => {
    const messages = Array.from({ length: 2100 }, (_, index) =>
      event(index + 3, 'agent.message', { content: '' }),
    );
    const result = reduce([...start(), ...messages]);
    expect(restore(result.state)).toEqual(result.state);
    expect(result.state.activeTurn?.io?.output?.truncated).toBe(true);
  });

  it('does not retain unmatched result payloads or expand legacy metadata checkpoints', () => {
    const result = reduce([
      ...start(),
      event(3, 'agent.tool_result', { tool_use_id: 'orphan', content: 'orphan-canary' }),
    ]);
    expect(JSON.stringify(result.state)).not.toContain('orphan-canary');
    const baseline = reduceCanonicalEventBatch(initialCanonicalProjectionState(), start()).state;
    const legacy = { ...baseline, version: 3 as const };
    delete legacy.captureMode;
    const resumed = reduce(
      [event(3, 'agent.message', { content: 'legacy-canary' }), end()],
      legacy,
    );
    expect(JSON.stringify(resumed)).not.toContain('legacy-canary');
    const injected = {
      ...baseline,
      activeTurn: {
        ...baseline.activeTurn!,
        io: { version: IO_VERSION, input: { json: JSON.stringify('injected-canary') } },
      },
    };
    expect(JSON.stringify(restore(injected))).not.toContain('injected-canary');
  });
});
