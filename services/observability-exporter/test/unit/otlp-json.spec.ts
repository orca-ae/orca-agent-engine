// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { encodeLangfuseOtlpJson } from '../../src/otlp-json.js';
import { TRANSCRIPT_SECRET, completedProjectedTrace } from '../support/events.js';
import { toolSpan } from '../support/tools.js';

describe('encodeLangfuseOtlpJson', () => {
  it('maps tool outcomes to OTLP status without changing the coarse summary type', () => {
    const trace = completedProjectedTrace();
    trace.spans.push(
      ...(['success', 'error', 'incomplete'] as const).map((outcome) => toolSpan(trace, outcome)),
    );
    const spans = encodeLangfuseOtlpJson(trace).resourceSpans[0]!.scopeSpans[0]!.spans;
    expect(spans.slice(2).map((span) => span.status.code)).toEqual([1, 2, 0]);
    for (const span of spans.slice(2)) {
      expect(span.name).toBe('orca.agent.tool');
      expect(span.parentSpanId).toBe(trace.root.spanId);
      expect(span.attributes).toContainEqual({
        key: 'langfuse.observation.type',
        value: { stringValue: 'tool' },
      });
    }
    expect(spans[1]!.attributes).toContainEqual({
      key: 'langfuse.observation.type',
      value: { stringValue: 'span' },
    });
  });

  it('maps evaluator identity, verdict and timing without fabricated model/usage attributes', () => {
    const trace = completedProjectedTrace();
    const span = trace.spans[0]!;
    span.observationType = 'outcome_evaluation';
    span.name = 'orca.agent.outcome_evaluation';
    span.status = 'error';
    span.metadata = {
      observation_type: 'outcome_evaluation',
      'orca.source.start_event_id': 'evt_eval',
      'orca.outcome.iteration': 2,
      'orca.outcome.result': 'failed',
    };
    // Even an incorrectly supplied model summary must never become evaluator usage.
    const wire = encodeLangfuseOtlpJson(trace).resourceSpans[0]!.scopeSpans[0]!.spans[1]!;
    expect(wire).toMatchObject({
      traceId: trace.traceId,
      spanId: span.spanId,
      parentSpanId: trace.root.spanId,
      name: span.name,
      status: { code: 2 },
      startTimeUnixNano: String(BigInt(Date.parse(span.startedAt)) * 1_000_000n),
    });
    expect(wire.attributes).toEqual(
      expect.arrayContaining([
        { key: 'langfuse.observation.type', value: { stringValue: 'evaluator' } },
        { key: 'langfuse.observation.metadata.orca.outcome.iteration', value: { intValue: '2' } },
        {
          key: 'langfuse.observation.metadata.orca.outcome.result',
          value: { stringValue: 'failed' },
        },
        { key: 'session.id', value: { stringValue: 'ws_observability:ses_observability' } },
      ]),
    );
    expect(JSON.stringify(wire)).not.toMatch(
      /gen_ai|usage_details|cost_details|observation.input|observation.output|orca.outcome.id|outcome_digest_/,
    );
  });

  it('encodes agent root and explicitly coarse model-summary span without content', () => {
    const trace = completedProjectedTrace();
    const request = encodeLangfuseOtlpJson(trace);
    const spans = request.resourceSpans[0]!.scopeSpans[0]!.spans;
    const [root, summary] = spans;

    expect(root).toMatchObject({
      traceId: trace.traceId,
      spanId: trace.root.spanId,
      name: 'orca.agent.turn',
    });
    expect(root?.attributes).toContainEqual({
      key: 'langfuse.observation.type',
      value: { stringValue: 'agent' },
    });
    expect(root?.attributes).toContainEqual({
      key: 'langfuse.user.id',
      value: { stringValue: 'ws_observability:user_observability' },
    });
    expect(summary).toMatchObject({
      traceId: trace.traceId,
      parentSpanId: trace.root.spanId,
      name: 'orca.agent.turn_model_summary',
    });
    expect(summary?.attributes).toContainEqual({
      key: 'langfuse.observation.type',
      value: { stringValue: 'span' },
    });
    expect(summary?.attributes).toContainEqual({
      key: 'langfuse.observation.metadata.orca.model.observation.kind',
      value: { stringValue: 'turn_model_summary' },
    });
    expect(summary?.attributes).toContainEqual({
      key: 'langfuse.observation.metadata.observation_type',
      value: { stringValue: 'turn_model_summary' },
    });
    expect(summary?.attributes).toContainEqual({
      key: 'langfuse.observation.metadata.orca.turn.anchor_event_id',
      value: { stringValue: 'evt_user_turn' },
    });
    const usageDetails = summary?.attributes.find(
      ({ key }) => key === 'langfuse.observation.usage_details',
    )?.value.stringValue;
    expect(JSON.parse(usageDetails ?? '')).toEqual({
      input: 11,
      output: 7,
      cache_creation_input_tokens: 2,
      input_cached_tokens: 3,
    });
    expect(summary?.attributes).not.toContainEqual({
      key: 'langfuse.observation.type',
      value: { stringValue: 'generation' },
    });
    expect(JSON.stringify(request)).not.toContain(TRANSCRIPT_SECRET);
    expect(JSON.stringify(request)).not.toContain('langfuse.observation.input');
    expect(JSON.stringify(request)).not.toContain('langfuse.observation.output');
  });
});
