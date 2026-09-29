// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { parseKafkaDeliveryContext, parseKafkaProjectedTrace } from '../../src/kafka-validation.js';
import { projectCanonicalTurns } from '../../src/projector.js';
import type { PinnedDeliveryContext, ProjectedTrace } from '../../src/types.js';
import { completedProjectedTrace, event, TRANSCRIPT_SECRET } from '../support/events.js';
import { toolSpan } from '../support/tools.js';

// Importing the Kafka validators must not initialize persistence or the PG runtime.
vi.mock('pg', () => {
  throw new Error('Kafka validation must be PG-free');
});
vi.mock('../../src/persistence.js', () => {
  throw new Error('Kafka validation must not import persistence');
});
vi.mock('../../src/runtime.js', () => {
  throw new Error('Kafka validation must not import runtime');
});

const deliveryContext: PinnedDeliveryContext = {
  organizationId: 'org_test',
  bindingId: 'aob_test',
  bindingVersion: 1,
  adapterType: 'otlp_http',
  endpointKind: 'traces_endpoint',
  endpointClass: 'public',
  endpointUrl: 'https://collector.example/api/public/otel/v1/traces',
  semanticProfile: 'langfuse',
  protocol: 'http/json',
  compression: 'none',
  timeoutMs: 1000,
  captureMode: 'metadata_only',
  sampleRate: 1,
  configSchemaVersion: 1,
};

function mixedTrace(): ProjectedTrace {
  const trace = completedProjectedTrace();
  trace.spans.push(...(['success', 'error', 'incomplete'] as const).map((s) => toolSpan(trace, s)));
  trace.root.metadata['orca.turn.unmatched_tool_result_count'] = 1;
  return trace;
}

describe('parseKafkaProjectedTrace', () => {
  it.each([completedProjectedTrace, mixedTrace])(
    'accepts canonical traces without PG',
    (fixture) => {
      const trace = fixture();
      const parsed = parseKafkaProjectedTrace(JSON.parse(JSON.stringify(trace)));
      expect(parsed).toEqual(trace);
      expect(parsed).not.toBe(trace);
    },
  );

  it.each(['satisfied', 'needs_revision', 'max_iterations_reached', 'failed', 'interrupted'])(
    'accepts canonical %s evaluations',
    (result) => {
      const trace = projectCanonicalTurns([
        event(1, 'user.message', {}, { id: 'evt_user', producedBy: 'client' }),
        event(2, 'session.user_event_processed', { user_event_id: 'evt_user' }),
        event(3, 'span.outcome_evaluation_start', { outcome_id: 'outc_test', iteration: 0 }),
        event(4, 'span.outcome_evaluation_end', {
          outcome_evaluation_start_id: 'evt_3',
          outcome_id: 'outc_test',
          iteration: 0,
          result,
          explanation: TRANSCRIPT_SECRET,
        }),
        event(5, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
      ])[0]!;
      expect(trace.spans[0]?.observationType).toBe('outcome_evaluation');
      expect(parseKafkaProjectedTrace(trace)).toEqual(trace);
      Object.assign(trace.spans[0]!, { explanation: TRANSCRIPT_SECRET });
      expect(JSON.stringify(parseKafkaProjectedTrace(trace))).not.toContain(TRANSCRIPT_SECRET);
      trace.spans[0]!.metadata.explanation = TRANSCRIPT_SECRET;
      expect(() => parseKafkaProjectedTrace(trace)).toThrow();
    },
  );

  it('reconstructs allowlisted fields and strips injected payloads at every level', () => {
    const expected = mixedTrace();
    const trace = structuredClone(expected);
    Object.assign(trace, { payload: TRANSCRIPT_SECRET });
    Object.assign(trace.root, { input: TRANSCRIPT_SECRET, output: TRANSCRIPT_SECRET });
    for (const span of trace.spans) {
      Object.assign(span, { arguments: TRANSCRIPT_SECRET, output: TRANSCRIPT_SECRET });
      if (span.modelSummary !== undefined) {
        Object.assign(span.modelSummary, { messages: TRANSCRIPT_SECRET });
        Object.assign(span.modelSummary.usage!, { payload: TRANSCRIPT_SECRET });
      }
    }
    const parsed = parseKafkaProjectedTrace(trace);
    expect(parsed).toEqual(expected);
    expect(JSON.stringify(parsed)).not.toContain(TRANSCRIPT_SECRET);
    expect(parsed.root.metadata).not.toBe(trace.root.metadata);
    expect(trace).toHaveProperty('payload', TRANSCRIPT_SECRET);
  });

  it.each(['root', 'summary', 'tool'])('rejects payload injection into %s metadata', (target) => {
    const trace = mixedTrace();
    const span = target === 'root' ? trace.root : trace.spans[target === 'summary' ? 0 : 1]!;
    span.metadata.payload = TRANSCRIPT_SECRET;
    expect(() => parseKafkaProjectedTrace(trace)).toThrow();
  });

  it.each([
    [
      'workspace mismatch',
      (t: ProjectedTrace) => {
        t.workspaceId = 'ws_other';
      },
    ],
    [
      'invalid schema',
      (t: ProjectedTrace) => {
        Object.assign(t, { schemaVersion: 'invalid' });
      },
    ],
    [
      'duplicate span',
      (t: ProjectedTrace) => {
        t.spans.push(t.spans[0]!);
      },
    ],
    [
      'unrelated parent',
      (t: ProjectedTrace) => {
        t.spans[0]!.parentSpanId = 'a'.repeat(16);
      },
    ],
    [
      'invalid tool identity',
      (t: ProjectedTrace) => {
        t.spans[1]!.spanId = 'a'.repeat(16);
      },
    ],
    [
      'invalid usage',
      (t: ProjectedTrace) => {
        t.spans[0]!.modelSummary!.usage!.inputTokens = -1;
      },
    ],
    [
      'invalid user ID',
      (t: ProjectedTrace) => {
        t.userId = 'user\nsecret';
      },
    ],
  ] as const)('rejects %s', (_name, mutate) => {
    const trace = mixedTrace();
    mutate(trace);
    expect(() => parseKafkaProjectedTrace(trace)).toThrow();
  });

  it.each([null, [], 'trace', {}])('rejects malformed envelopes: %j', (value) => {
    expect(() => parseKafkaProjectedTrace(value)).toThrow();
  });
});

describe('parseKafkaDeliveryContext', () => {
  it.each([0, 0.5, 1])(
    'accepts a pinned metadata-only context with sample rate %s',
    (sampleRate) => {
      const context = { ...deliveryContext, sampleRate };
      expect(parseKafkaDeliveryContext(context)).toEqual(context);
    },
  );

  it('strips injected secrets and returns a fresh pinned context', () => {
    const context = {
      ...deliveryContext,
      headers: { Authorization: TRANSCRIPT_SECRET },
      credentials: TRANSCRIPT_SECRET,
      payload: TRANSCRIPT_SECRET,
    };
    const parsed = parseKafkaDeliveryContext(context);
    expect(parsed).toEqual(deliveryContext);
    expect(parsed).not.toBe(context);
    expect(JSON.stringify(parsed)).not.toContain(TRANSCRIPT_SECRET);
  });

  it.each([
    { organizationId: '' },
    { bindingId: 'aob_\nsecret' },
    { bindingVersion: 0 },
    { bindingVersion: Number.MAX_SAFE_INTEGER + 1 },
    { adapterType: 'other' },
    { endpointKind: 'base_url' },
    { endpointClass: 'private' },
    { endpointUrl: 'http://collector.example/api/public/otel/v1/traces' },
    { endpointUrl: 'https://user:secret@collector.example/api/public/otel/v1/traces' },
    { endpointUrl: `${deliveryContext.endpointUrl}?token=secret` },
    { endpointUrl: `${deliveryContext.endpointUrl}#secret` },
    { endpointUrl: `${deliveryContext.endpointUrl}/` },
    { semanticProfile: 'other' },
    { protocol: 'http/protobuf' },
    { compression: 'gzip' },
    { timeoutMs: 0 },
    { timeoutMs: 1.5 },
    { captureMode: 'full' },
    { sampleRate: -0.1 },
    { sampleRate: 1.1 },
    { sampleRate: NaN },
    { configSchemaVersion: 2 },
  ])('rejects unsupported or invalid pinned settings: %j', (override) => {
    expect(() => parseKafkaDeliveryContext({ ...deliveryContext, ...override })).toThrow();
  });

  it.each([null, [], 'context', {}])('rejects malformed contexts: %j', (value) => {
    expect(() => parseKafkaDeliveryContext(value)).toThrow();
  });
});
