// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Pool } from 'pg';
import { describe, expect, it, vi } from 'vitest';
import { ObservabilityExporterRepository } from '../../src/persistence.js';
import { initialCanonicalProjectionState } from '../../src/projector.js';
import { deterministicChildSpanId } from '../../src/ids.js';
import { MAX_PROJECTED_TOOLS_PER_TURN } from '../../src/tool-projection.js';
import type { PinnedDeliveryContext, ProjectedTrace } from '../../src/types.js';
import { completedProjectedTrace, TRANSCRIPT_SECRET } from '../support/events.js';
import { toolSpan } from '../support/tools.js';

const deliveryContext: PinnedDeliveryContext = {
  organizationId: 'org_tools',
  bindingId: 'aob_tools',
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

function fixture(trace: ProjectedTrace) {
  const query = vi.fn(async (_sql: string, _values?: unknown[]) => ({ rowCount: 1, rows: [] }));
  const repository = new ObservabilityExporterRepository({
    connect: async () => ({ query, release: vi.fn() }),
  } as unknown as Pool);
  const state = initialCanonicalProjectionState();
  const complete = () =>
    repository.completeProjection(
      {
        workspaceId: trace.workspaceId,
        sessionId: trace.sessionId,
        nextSeq: '0',
        firstPendingSeq: '1',
        state,
        leaseOwner: 'tools',
        leaseGeneration: '1',
      },
      '2',
      state,
      [{ trace, deliveryContext }],
    );
  return { query, complete };
}

describe('durable tool trace parser', () => {
  it('preserves legacy v1 and metadata-only tool traces at the SQL write boundary', async () => {
    for (const withTools of [false, true]) {
      const trace = completedProjectedTrace();
      if (withTools) {
        trace.spans.push(
          ...(['success', 'error', 'incomplete'] as const).map((outcome) =>
            toolSpan(trace, outcome),
          ),
        );
        trace.root.metadata['orca.turn.unmatched_tool_result_count'] = 1;
      }
      const { query, complete } = fixture(trace);
      await complete();
      const insert = query.mock.calls.find(([sql]) =>
        sql.includes('INSERT INTO observability_exporter_trace_outbox'),
      );
      expect(JSON.parse(insert![1]![7] as string)).toEqual(trace);
      expect(JSON.stringify(query.mock.calls)).not.toContain(TRANSCRIPT_SECRET);
    }
  });

  it.each([
    [
      'span identity',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.spanId = 'a'.repeat(16);
      },
    ],
    [
      'parent identity',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.parentSpanId = 'b'.repeat(16);
      },
    ],
    [
      'coordinated root and tool parent identity',
      (trace: ProjectedTrace) => {
        trace.root.spanId = 'b'.repeat(16);
        trace.spans[0]!.parentSpanId = trace.root.spanId;
      },
    ],
    [
      'name content',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.name = TRANSCRIPT_SECRET;
      },
    ],
    [
      'metadata content',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.metadata['input'] = TRANSCRIPT_SECRET;
      },
    ],
    [
      'invalid family',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.metadata['orca.tool.family'] = TRANSCRIPT_SECRET;
      },
    ],
    [
      'invalid outcome',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.metadata['orca.tool.outcome'] = TRANSCRIPT_SECRET;
      },
    ],
    [
      'status mismatch',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.status = 'unset';
      },
    ],
    [
      'source mismatch',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.metadata['orca.source.start_event_id'] = 'evt_other';
      },
    ],
    [
      'missing end source',
      (trace: ProjectedTrace) => {
        delete trace.spans[0]!.metadata['orca.source.end_event_id'];
      },
    ],
    [
      'model fields',
      (trace: ProjectedTrace) => {
        trace.spans[0]!.modelSummary = { provider: TRANSCRIPT_SECRET };
      },
    ],
    [
      'duplicate source',
      (trace: ProjectedTrace) => {
        trace.spans.push(trace.spans[0]!);
      },
    ],
    [
      'counter zero',
      (trace: ProjectedTrace) => {
        trace.root.metadata['orca.turn.unmatched_tool_result_count'] = 0;
      },
    ],
    [
      'counter unsafe',
      (trace: ProjectedTrace) => {
        trace.root.metadata['orca.turn.unmatched_tool_result_count'] = Number.MAX_SAFE_INTEGER + 1;
      },
    ],
  ])('rejects %s before outbox insert', async (_label, mutate) => {
    const trace = completedProjectedTrace();
    trace.spans = [toolSpan(trace)];
    mutate(trace);
    const { query, complete } = fixture(trace);
    await expect(complete()).rejects.toThrow();
    expect(
      query.mock.calls.some(([sql]) =>
        sql.includes('INSERT INTO observability_exporter_trace_outbox'),
      ),
    ).toBe(false);
    expect(query).toHaveBeenCalledWith('ROLLBACK');
  });

  it('enforces the tool-specific bound even when total children fit the combined bound', async () => {
    const trace = completedProjectedTrace();
    trace.spans = Array.from({ length: MAX_PROJECTED_TOOLS_PER_TURN + 1 }, (_, index) => {
      const span = toolSpan(trace);
      span.sourceEventId = `evt_tool_${index}`;
      span.metadata['orca.source.start_event_id'] = span.sourceEventId;
      span.spanId = deterministicChildSpanId(trace.traceId, 'tool', '', span.sourceEventId);
      return span;
    });
    await expect(fixture(trace).complete()).rejects.toThrow();
    trace.spans.pop();
    await expect(fixture(trace).complete()).resolves.toBeUndefined();
  });
});
