// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'vitest';
import {
  isAgentEventId,
  isAgentEventSubpath,
  ModelObservationKind,
  PRIMARY_AGENT_SUBPATH,
} from '@orca/agent-event-contract';
import type { AgentEvent } from '../../src/harness/agent-harness.js';

export function expectUniqueAgentEventEnvelopes(events: readonly AgentEvent[]): void {
  const ids = new Set<string>();
  for (const event of events) {
    expect(isAgentEventId(event.id), `${event.kind} has invalid id ${String(event.id)}`).toBe(true);
    expect(
      isAgentEventSubpath(event.subpath),
      `${event.kind} has invalid subpath ${String(event.subpath)}`,
    ).toBe(true);
    expect(ids.has(event.id), `duplicate AgentEvent.id ${String(event.id)}`).toBe(false);
    ids.add(event.id);
  }
}

/** Shared conformance check for the coarse model pair emitted by both Harness paths. */
export function expectTurnModelSummary(
  events: readonly AgentEvent[],
  expected: { provider?: string; model?: string } = {},
): { start: AgentEvent; end: AgentEvent } {
  const starts = events.filter((event) => event.kind === 'span.model_request_start');
  const ends = events.filter((event) => event.kind === 'span.model_request_end');
  expect(starts).toHaveLength(1);
  expect(ends).toHaveLength(1);

  const start = starts[0]!;
  const end = ends[0]!;
  expect(events.indexOf(start)).toBeLessThan(events.indexOf(end));
  expect(isAgentEventId(start.id)).toBe(true);
  expect(isAgentEventId(end.id)).toBe(true);
  expect(end.id).not.toBe(start.id);
  expect(end.subpath).toBe(start.subpath);

  const startPayload = start.payload as Record<string, unknown>;
  const endPayload = end.payload as Record<string, unknown>;
  expect(startPayload).not.toHaveProperty('id');
  expect(endPayload).not.toHaveProperty('id');
  expect(startPayload).toMatchObject({
    model_observation_kind: ModelObservationKind.turnSummary,
    ...expected,
  });
  expect(endPayload).toMatchObject({
    model_request_start_id: start.id,
    model_observation_kind: ModelObservationKind.turnSummary,
    is_error: expect.any(Boolean),
    model_usage: expect.any(Object),
    ...expected,
  });
  return { start, end };
}

/** Shared root-path envelope check for current single-agent producer coverage. */
export function expectCanonicalRootEnvelopes(events: readonly AgentEvent[]): void {
  expectUniqueAgentEventEnvelopes(events);
  for (const event of events) {
    expect(event.subpath, `${event.kind} must remain on the primary path`).toBe(
      PRIMARY_AGENT_SUBPATH,
    );
  }
}

/**
 * Shared successful-turn conformance at current root/coarse-summary granularity.
 * It deliberately checks ordering relations, not an identical backend sequence.
 */
export function expectCanonicalRootTurn(
  events: readonly AgentEvent[],
  expected: {
    provider?: string;
    model?: string;
  } = {},
): { start: AgentEvent; end: AgentEvent; terminalIdle: AgentEvent } {
  expectCanonicalRootEnvelopes(events);
  const { provider, model } = expected;
  const { start, end } = expectTurnModelSummary(events, {
    ...(provider !== undefined ? { provider } : {}),
    ...(model !== undefined ? { model } : {}),
  });
  expect((end.payload as { is_error?: unknown }).is_error).toBe(false);
  const startIndex = events.indexOf(start);
  const endIndex = events.indexOf(end);
  const firstRunningIndex = events.findIndex((event) => event.kind === 'session.status_running');
  const terminalIdle = [...events]
    .reverse()
    .find((event) => event.kind === 'session.status_idle' && stopReasonType(event) === 'end_turn');

  expect(firstRunningIndex).toBeGreaterThanOrEqual(0);
  expect(firstRunningIndex).toBeLessThan(startIndex);
  expect(startIndex).toBeLessThan(endIndex);
  expect(terminalIdle).toBeDefined();
  expect(endIndex).toBeLessThan(events.indexOf(terminalIdle!));

  return { start, end, terminalIdle: terminalIdle! };
}

/** Assert one current required-action pause resumes same coarse turn summary. */
export function expectRequiredActionContinuation(
  events: readonly AgentEvent[],
  actionKind = 'agent.custom_tool_use',
): { action: AgentEvent; requiredActionIdle: AgentEvent; resumedRunning: AgentEvent } {
  const modelStartIndex = events.findIndex((event) => event.kind === 'span.model_request_start');
  const actionIndex = events.findIndex((event) => event.kind === actionKind);
  expect(modelStartIndex).toBeGreaterThanOrEqual(0);
  expect(actionIndex).toBeGreaterThanOrEqual(0);

  const action = events[actionIndex]!;
  const requiredActionIndex = events.findIndex(
    (event, index) =>
      index > actionIndex &&
      event.kind === 'session.status_idle' &&
      stopReasonType(event) === 'requires_action',
  );
  expect(requiredActionIndex).toBeGreaterThanOrEqual(0);

  const requiredActionIdle = events[requiredActionIndex]!;
  const eventIds = (requiredActionIdle.payload as { stop_reason: { event_ids?: unknown } })
    .stop_reason.event_ids;
  expect(eventIds).toEqual([action.id]);

  const resumedRunningIndex = events.findIndex(
    (event, index) => index > requiredActionIndex && event.kind === 'session.status_running',
  );
  expect(modelStartIndex).toBeLessThan(actionIndex);
  expect(actionIndex).toBeLessThan(requiredActionIndex);
  expect(resumedRunningIndex).toBeGreaterThanOrEqual(0);
  expect(requiredActionIndex).toBeLessThan(resumedRunningIndex);
  const modelEndIndex = events.findIndex(
    (event, index) => index > resumedRunningIndex && event.kind === 'span.model_request_end',
  );
  expect(modelEndIndex).toBeGreaterThanOrEqual(0);
  expect(resumedRunningIndex).toBeLessThan(modelEndIndex);

  return {
    action,
    requiredActionIdle,
    resumedRunning: events[resumedRunningIndex]!,
  };
}

/** Assert a tool result references its matching tool-use correlation field. */
export function expectToolUseResultCorrelation(
  events: readonly AgentEvent[],
  expected: { toolUseKind?: string; toolResultKind?: string } = {},
): { toolUse: AgentEvent; toolResult: AgentEvent } {
  const toolUseKind = expected.toolUseKind ?? 'agent.tool_use';
  const toolResultKind = expected.toolResultKind ?? 'agent.tool_result';
  const toolUseIndex = events.findIndex((event) => event.kind === toolUseKind);
  const toolResultIndex = events.findIndex(
    (event, index) => index > toolUseIndex && event.kind === toolResultKind,
  );
  expect(toolUseIndex).toBeGreaterThanOrEqual(0);
  expect(toolResultIndex).toBeGreaterThanOrEqual(0);

  const toolUse = events[toolUseIndex]!;
  const toolResult = events[toolResultIndex]!;
  const toolUsePayload = toolUse.payload as { id?: unknown; tool_use_id?: unknown };
  // Separated Claude exposes its public relation as `id`; the in-sandbox
  // mapper preserves the native relation as `tool_use_id`.
  const toolUseCorrelationId =
    typeof toolUsePayload.id === 'string' ? toolUsePayload.id : toolUsePayload.tool_use_id;
  expect(typeof toolUseCorrelationId).toBe('string');
  expect((toolUseCorrelationId as string).length).toBeGreaterThan(0);
  expect((toolResult.payload as { tool_use_id?: unknown }).tool_use_id).toBe(toolUseCorrelationId);

  return { toolUse, toolResult };
}

/** Shared terminal-error check; current sandbox wire has no retry-frame equivalent. */
export function expectTerminalError(events: readonly AgentEvent[]): {
  error: AgentEvent;
  terminalIdle: AgentEvent;
} {
  expectCanonicalRootEnvelopes(events);
  const errorIndex = events.findIndex((event) => event.kind === 'session.error');
  const terminalIdleIndex = events.findIndex(
    (event, index) =>
      index > errorIndex &&
      event.kind === 'session.status_idle' &&
      stopReasonType(event) === 'retries_exhausted',
  );
  expect(errorIndex).toBeGreaterThanOrEqual(0);
  expect(terminalIdleIndex).toBeGreaterThanOrEqual(0);

  const error = events[errorIndex]!;
  expect(error.payload).toMatchObject({
    error: { type: expect.any(String), message: expect.any(String) },
    retry_status: { will_retry: false },
  });
  return { error, terminalIdle: events[terminalIdleIndex]! };
}

function stopReasonType(event: AgentEvent): unknown {
  return (event.payload as { stop_reason?: { type?: unknown } }).stop_reason?.type;
}
