// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { guardrailMetadata, parseGuardrailMetadata } from '../../src/guardrail-projection.js';

import { describe, expect, it } from 'vitest';
import {
  projectCanonicalTurns,
  initialCanonicalProjectionState,
  reduceCanonicalEventBatch,
  parseCanonicalProjectionState,
} from '../../src/projector.js';
import { parseProjectedTrace } from '../../src/canonical-validation.js';
import { event, TRANSCRIPT_SECRET } from '../support/events.js';

const prefix = () => [
  event(1, 'user.message', {}, { producedBy: 'client' }),
  event(2, 'session.user_event_processed', { user_event_id: 'evt_1' }),
];
const decision = (seq = 3) =>
  event(seq, 'session.guardrail_decision', {
    phase: 'tool_call',
    decision: 'allow',
    tool_use_id: 'toolu_one',
    guardrails: [{ guardrail_id: 'grd_one', verdict: 'deny', mode: 'observe' }],
    input: TRANSCRIPT_SECRET,
    output: TRANSCRIPT_SECRET,
  });
const terminal = () => event(999, 'session.status_idle', { stop_reason: { type: 'end_turn' } });

describe('guardrail decision projection', () => {
  it('uses a new trace schema and restores the same bounded evidence at every batch boundary', () => {
    const events = [...prefix(), decision(), terminal()];
    const [trace] = projectCanonicalTurns(events);
    expect(trace).toMatchObject({
      schemaVersion: 'orca.observability.projected-trace.v5',
      spans: [
        {
          observationType: 'guardrail_decision',
          name: 'orca.agent.guardrail_decision',
          metadata: { 'orca.guardrail.phase': 'tool_call', 'orca.guardrail.decision': 'allow' },
        },
      ],
    });
    expect(JSON.stringify(trace)).not.toContain(TRANSCRIPT_SECRET);
    expect(parseProjectedTrace(JSON.parse(JSON.stringify(trace)))).toEqual(trace);
    for (let split = 0; split <= events.length; split++) {
      const first = reduceCanonicalEventBatch(
        initialCanonicalProjectionState(),
        events.slice(0, split),
      );
      const second = reduceCanonicalEventBatch(
        parseCanonicalProjectionState(JSON.parse(JSON.stringify(first.state))),
        events.slice(split),
        new Set(first.acceptedSourceIds),
      );
      expect([...first.completedTraces, ...second.completedTraces]).toEqual([trace]);
    }
  });
  it('does not let an old schema carry new attributes and ignores malformed producer data', () => {
    const [trace] = projectCanonicalTurns([...prefix(), decision(), terminal()]);
    expect(() =>
      parseProjectedTrace({ ...trace, schemaVersion: 'orca.observability.projected-trace.v1' }),
    ).toThrow();
    expect(
      projectCanonicalTurns([
        ...prefix(),
        event(3, 'session.guardrail_decision', {
          decision: 'allow',
          phase: 'tool_call',
          guardrails: [{ guardrail_id: TRANSCRIPT_SECRET }],
        }),
        terminal(),
      ])[0]?.spans,
    ).toEqual([]);
  });
});

describe('guardrail decisions with raw I/O', () => {
  it('keeps the new schema readable alongside tool observations across restore', () => {
    const events = [
      ...prefix(),
      decision(),
      event(4, 'agent.tool_use', { id: 'toolu_one', name: 'Bash', input: { command: 'ls' } }),
      event(5, 'agent.tool_result', { tool_use_id: 'toolu_one', content: 'ok', is_error: false }),
      terminal(),
    ];
    const first = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      events.slice(0, 4),
      new Set(),
      undefined,
      'raw_io',
    );
    const second = reduceCanonicalEventBatch(
      parseCanonicalProjectionState(JSON.parse(JSON.stringify(first.state))),
      events.slice(4),
      new Set(first.acceptedSourceIds),
      undefined,
      'raw_io',
    );
    const trace = second.completedTraces[0]!;
    expect(trace.schemaVersion).toBe('orca.observability.projected-trace.v6');
    expect(trace.spans.map((span) => span.observationType)).toEqual(
      expect.arrayContaining(['tool', 'guardrail_decision']),
    );
    expect(parseProjectedTrace(JSON.parse(JSON.stringify(trace)))).toEqual(trace);
  });
});

describe('Dogwood decision metadata', () => {
  it('retains bounded rule IDs/errors and enforcer without policy text or tool data', () => {
    const payload = {
      phase: 'tool_call',
      decision: 'ask',
      enforcer: 'harness',
      tool_use_id: 'tool-1',
      guardrails: [
        {
          guardrail_id: 'grd_one',
          mode: 'enforce',
          verdict: 'ask',
          deciding_rules: ['confirm'],
          errors: [],
        },
      ],
      policy: 'private policy',
      input: { secret: 'excluded' },
    };
    const metadata = guardrailMetadata(payload, 'evt_decision')!;
    expect(metadata['orca.guardrail.enforcer']).toBe('harness');
    expect(JSON.parse(metadata['orca.guardrail.verdicts'] as string)[0]).toMatchObject({
      deciding_rules: ['confirm'],
      errors: [],
    });
    expect(JSON.stringify(metadata)).not.toContain('private policy');
    expect(JSON.stringify(metadata)).not.toContain('secret');
    expect(parseGuardrailMetadata(metadata, 'evt_decision')).toEqual(metadata);
    expect(
      guardrailMetadata(
        { ...payload, guardrails: [{ ...payload.guardrails[0], errors: ['x'.repeat(1025)] }] },
        'evt_decision',
      ),
    ).toBeUndefined();
  });
});
