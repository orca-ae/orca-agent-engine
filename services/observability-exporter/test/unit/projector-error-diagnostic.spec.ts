// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  reduceCanonicalEventBatch,
} from '../../src/projector.js';
import { encodeLangfuseOtlpJson } from '../../src/otlp-json.js';
import { parseProjectedIo, MAX_CAPTURED_IO_BYTES_PER_TURN } from '../../src/captured-io.js';
import { parseProjectedTrace } from '../../src/canonical-validation.js';
import { IO_VERSION } from '../../src/types.js';
import { TRACE_SAMPLING_VERSION } from '../../src/sampling.js';
import { event } from '../support/events.js';

const start = [
  event(1, 'user.message', { content: 'hello' }, { producedBy: 'client' }),
  event(2, 'session.user_event_processed', { user_event_id: 'evt_1' }),
];
const error = event(3, 'session.error', {
  error: { type: 'rate_limit', message: 'retry: {"password":"json-canary"} Bearer bearer-canary' },
  retry_status: { will_retry: true, next_attempt: 2 },
  retry_delay_ms: 1500,
  unrelated: 'ignored-canary',
});
const end = event(10, 'session.status_idle', { stop_reason: { type: 'end_turn' } });
const run = (events: ReturnType<typeof event>[], state = initialCanonicalProjectionState()) =>
  reduceCanonicalEventBatch(state, events, new Set(), undefined, 'raw_io');

describe('last reported error diagnostic', () => {
  it('retains original reported retry facts through replay without changing successful terminal status', () => {
    const first = run([...start, error]);
    expect(JSON.stringify(first.state)).toContain('json-canary');
    expect(JSON.stringify(first.state)).toContain('bearer-canary');
    expect(JSON.stringify(first.state)).not.toContain('ignored-canary');
    const restored = parseCanonicalProjectionState(JSON.parse(JSON.stringify(first.state)));
    const trace = run([end], restored).completedTraces[0]!;
    expect(trace).toEqual(run([...start, error, end]).completedTraces[0]);
    expect(trace.root.status).toBe('ok');
    expect(JSON.parse(trace.root.io!.diagnostic!.json!)).toMatchObject({
      type: 'rate_limit',
      message: 'retry: {"password":"json-canary"} Bearer bearer-canary',
      will_retry: true,
      next_attempt: 2,
      retry_delay_ms: 1500,
    });
    expect(parseProjectedTrace(trace)).toEqual(trace);
    const wire = encodeLangfuseOtlpJson(trace).resourceSpans[0]!.scopeSpans[0]!.spans[0]!;
    expect(wire.status.code).toBe(1);
    expect(wire.attributes).toEqual(
      expect.arrayContaining([
        {
          key: 'langfuse.observation.metadata.orca.last_error.type',
          value: { stringValue: 'rate_limit' },
        },
        { key: 'langfuse.observation.metadata.orca.retry.will_retry', value: { boolValue: true } },
        { key: 'langfuse.observation.metadata.orca.retry.next_attempt', value: { intValue: '2' } },
        {
          key: 'langfuse.observation.metadata.orca.retry.retry_delay_ms',
          value: { intValue: '1500' },
        },
      ]),
    );
    expect(JSON.stringify(wire)).toContain('json-canary');
    expect(JSON.stringify(wire)).toContain('bearer-canary');
    expect(JSON.stringify(wire)).not.toContain('ignored-canary');
  });

  it('replaces the last diagnostic and never fabricates omitted or invalid facts', () => {
    const trace = run([
      ...start,
      error,
      event(4, 'session.error', {
        error: { type: 'network', message: 'last reported error' },
        retry_status: { will_retry: 'yes', next_attempt: -2 },
        retry_delay_ms: -1,
      }),
      end,
    ]).completedTraces[0]!;
    expect(JSON.parse(trace.root.io!.diagnostic!.json!)).toEqual({
      type: 'network',
      message: 'last reported error',
    });
    expect(JSON.stringify(encodeLangfuseOtlpJson(trace))).not.toContain('orca.retry.');
    expect(
      run([...start, error, event(4, 'session.error'), end]).completedTraces[0]?.root.io
        ?.diagnostic,
    ).toEqual({ omitted: 'unavailable' });
  });

  it('omits diagnostics by default and for sampled-out turns, and scrubs on revoke', () => {
    expect(
      reduceCanonicalEventBatch(initialCanonicalProjectionState(), [...start, error, end])
        .completedTraces[0]?.root.io,
    ).toBeUndefined();
    const policy = {
      algorithmVersion: TRACE_SAMPLING_VERSION,
      bindingId: 'binding',
      bindingVersion: 1,
      sampleRate: 0,
    };
    const suppressed = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      [...start, error],
      new Set(),
      policy,
      'raw_io',
    );
    expect(suppressed.state.activeTurn?.io).toBeUndefined();
    const active = run([...start, error]);
    expect(active.state.activeTurn?.io?.diagnostic).toBeDefined();
    const revoked = reduceCanonicalEventBatch(active.state, []);
    expect(revoked.state.activeTurn?.io).toBeUndefined();
    expect(run([error, end], revoked.state).completedTraces[0]?.root.io).toBeUndefined();
  });

  it('rejects diagnostic passthrough and counts diagnostic bytes in the turn budget', () => {
    for (const diagnostic of [
      { extra: 'arbitrary' },
      { next_attempt: 0 },
      { will_retry: 'true' },
      { message: { text: 'nested' } },
    ]) {
      expect(() =>
        parseProjectedIo({
          version: IO_VERSION,
          diagnostic: { json: JSON.stringify(diagnostic) },
        }),
      ).toThrow();
    }
    const events = [...start, event(3, 'session.error', { error: { message: 'x'.repeat(7000) } })];
    for (let i = 0; i < 40; i++)
      events.push(event(i + 4, 'agent.tool_use', { name: 'lookup', input: 'y'.repeat(7000) }));
    const state = run(events).state;
    expect(state.activeTurn?.io?.diagnostic?.json).toBeDefined();
    expect(parseCanonicalProjectionState(JSON.parse(JSON.stringify(state)))).toEqual(state);
    const total = [
      state.activeTurn!.io,
      ...state.activeTurn!.tools.uses.map((use) => use.io),
    ].reduce((sum, io) => sum + (io === undefined ? 0 : Buffer.byteLength(JSON.stringify(io))), 0);
    expect(total).toBeLessThanOrEqual(MAX_CAPTURED_IO_BYTES_PER_TURN);
    expect(
      run([...start, event(3, 'session.error', { error: { message: 'x'.repeat(9000) } }), end])
        .completedTraces[0]?.root.io?.diagnostic,
    ).toEqual({ omitted: 'too_large' });
  });
});
