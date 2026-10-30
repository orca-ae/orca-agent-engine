// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import { exerciseInspectionTree } from '../../../../packages/dsh-harness/test/support/inspection-tree.js';
import { DshEventProjection } from '../../src/harness/codex-sdk/dsh-events.js';
import { projectCanonicalTurns } from '../../../observability-exporter/src/projector.js';
import { parseProjectedTrace } from '../../../observability-exporter/src/canonical-validation.js';
import { event as transcriptEvent } from '../../../observability-exporter/test/support/events.js';

it('admits native inspection once and keeps embedded history opaque through replay and export', async () => {
  const names = [
    'spawn',
    'fork',
    'send_message',
    'interrupt_agent',
    'list_child_sessions',
    'read_child_session',
  ];
  let projection = new DshEventProjection('ws_fixture', 'inspection-session', names);
  const errors: string[] = [];
  const projected: Array<{
    id: string;
    subpath: string;
    kind: string;
    payload: Record<string, unknown>;
  }> = [];
  await exerciseInspectionTree((event) => {
    try {
      if (event.type === 'event' && event.event.type === 'thread.started')
        projection = new DshEventProjection('ws_fixture', 'inspection-session', names);
      if (event.type === 'event' && event.event.type === 'turn.started') projection.beginTurn();
      if (event.type === 'thread_tree') projection.restoreTree(event.threads);
      if (event.type === 'canonical_event')
        projected.push(projection.validate(event.event, String(event.event.payload.turn_event_id)));
      if (event.type === 'tool_call') projection.assertOrigin(event.origin);
      if (event.type === 'orchestration_request') projection.authorize(event);
    } catch (error) {
      errors.push(String(error));
    }
  });
  expect(errors).toEqual([]);
  const fullBuild = projected.filter((event) => event.payload.turn_event_id === 'evt_build');
  expect(new Set(fullBuild.map((event) => event.id)).size).toBe(fullBuild.length);
  const uses = fullBuild.filter((event) => event.kind === 'agent.tool_use');
  const results = fullBuild.filter((event) => event.kind === 'agent.tool_result');
  expect(results).toHaveLength(uses.length);
  for (const use of uses)
    expect(results.filter((event) => event.payload.tool_use_id === use.id)).toHaveLength(1);
  const summaries = fullBuild.filter((event) => event.kind === 'span.model_request_end');
  // Root, A, B and grandchild each have one native summary; embedded history adds none.
  expect(summaries).toHaveLength(4);
  expect(new Set(summaries.map((event) => event.subpath)).size).toBe(4);
  const build = fullBuild.filter((event) => event.subpath === '');
  const use = build.find(
    (event) => event.kind === 'agent.tool_use' && event.payload.name === 'read_child_session',
  )!;
  const result = build.find(
    (event) => event.kind === 'agent.tool_result' && event.payload.tool_use_id === use.id,
  )!;
  expect(result.payload.is_error).toBe(false);
  const page = JSON.parse(
    (result.payload.content as Array<{ text?: string }>).map((block) => block.text ?? '').join(''),
  );
  expect(page.history_source).toBe('live');
  expect(page.records.some((record: { kind: string }) => record.kind === 'tool/call')).toBe(true);
  expect(page.records.some((record: { kind: string }) => record.kind === 'tool/result')).toBe(true);
  const summary = build.filter((event) => event.kind === 'span.model_request_end').at(-1)!;
  const start = build.find((event) => event.id === summary.payload.model_request_start_id)!;
  // Isolate one real query pair, preserving native identities and its accepted turn.
  const query = [start, use, result, summary];
  expect(query.filter((event) => event.kind === 'agent.tool_use')).toHaveLength(1);
  expect(query.filter((event) => event.kind === 'agent.tool_result')).toHaveLength(1);
  expect(query.every((event) => event.subpath === '')).toBe(true);
  const events = [
    transcriptEvent(
      1,
      'user.message',
      { content: [{ type: 'text', text: 'inspect child' }] },
      { id: 'evt_build', producedBy: 'client' },
    ),
    transcriptEvent(2, 'session.user_event_processed', { user_event_id: 'evt_build' }),
    ...query.map((event, index) =>
      transcriptEvent(index + 3, event.kind, event.payload, {
        id: event.id,
        subpath: event.subpath,
      }),
    ),
    transcriptEvent(query.length + 3, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
  ];
  const traces = projectCanonicalTurns(events);
  expect(traces).toHaveLength(1);
  const trace = parseProjectedTrace(traces[0]);
  expect(trace.spans.filter((span) => span.observationType === 'tool')).toHaveLength(1);
  expect(trace.spans.filter((span) => span.observationType === 'turn_model_summary')).toHaveLength(
    1,
  );
  expect(trace.spans.every((span) => span.subpath === '')).toBe(true);
  expect(JSON.stringify(trace)).not.toContain('child-record-');
  const model = trace.spans.find((span) => span.observationType === 'turn_model_summary')!;
  const usage = summary.payload.model_usage as { input_tokens: number; output_tokens: number };
  expect(model.modelSummary!.usage).toMatchObject({
    inputTokens: usage.input_tokens,
    outputTokens: usage.output_tokens,
  });
  const replay = events.flatMap((event) =>
    event.kind === 'agent.tool_use' || event.kind === 'agent.tool_result'
      ? [event, event]
      : [event],
  );
  expect(projectCanonicalTurns(replay)).toEqual(traces);

  const fullEvents = [
    ...events.slice(0, 2),
    ...fullBuild.map((event, index) =>
      transcriptEvent(index + 3, event.kind, event.payload, {
        id: event.id,
        subpath: event.subpath,
      }),
    ),
    transcriptEvent(fullBuild.length + 3, 'session.status_idle', {
      stop_reason: { type: 'end_turn' },
    }),
  ];
  const fullTraces = projectCanonicalTurns(fullEvents);
  expect(fullTraces).toHaveLength(1);
  const fullTrace = parseProjectedTrace(fullTraces[0]);
  expect(fullTrace.spans.filter((span) => span.observationType === 'tool')).toHaveLength(
    uses.length,
  );
  const models = fullTrace.spans.filter((span) => span.observationType === 'turn_model_summary');
  expect(models).toHaveLength(4);
  expect(models.reduce((sum, span) => sum + (span.modelSummary?.usage?.inputTokens ?? 0), 0)).toBe(
    summaries.reduce(
      (sum, event) => sum + (event.payload.model_usage as { input_tokens: number }).input_tokens,
      0,
    ),
  );
  expect(models.reduce((sum, span) => sum + (span.modelSummary?.usage?.outputTokens ?? 0), 0)).toBe(
    summaries.reduce(
      (sum, event) => sum + (event.payload.model_usage as { output_tokens: number }).output_tokens,
      0,
    ),
  );
  // A new transport position must not defeat identity-level replay deduplication.
  const resequenced = fullEvents.flatMap((event, index) => [
    { ...event, seq: index * 2 + 1 },
    { ...event, seq: index * 2 + 2 },
  ]);
  const replayed = projectCanonicalTurns(resequenced);
  expect(replayed).toHaveLength(1);
  expect(parseProjectedTrace(replayed[0]).spans).toEqual(fullTrace.spans);
}, 60000);
