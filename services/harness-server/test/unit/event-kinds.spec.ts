// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  AgentEventKind as CanonicalAgentEventKind,
  AgentRuntimeSignalKind,
  CANONICAL_AGENT_EVENT_KINDS,
  InternalTranscriptEventKind,
  SessionEventKind as ContractSessionEventKind,
  SpanEventKind as ContractSpanEventKind,
} from '@orca/agent-event-contract';
import {
  AgentEventKind,
  SessionEventKind,
  SpanEventKind,
  ALL_SESSION_EVENT_KINDS,
  sessionIdlePayload,
  sessionErrorPayload,
  turnModelSummaryEndPayload,
  turnModelSummaryStartPayload,
} from '../../src/harness/event-kinds.js';

describe('sessionIdlePayload', () => {
  it('emits event_ids only for requires_action', () => {
    expect(sessionIdlePayload('end_turn')).toEqual({ stop_reason: { type: 'end_turn' } });
    expect(sessionIdlePayload('retries_exhausted')).toEqual({
      stop_reason: { type: 'retries_exhausted' },
    });
    expect(sessionIdlePayload('requires_action', ['evt_1', 'evt_2'])).toEqual({
      stop_reason: { type: 'requires_action', event_ids: ['evt_1', 'evt_2'] },
    });
    expect(() => Reflect.apply(sessionIdlePayload, undefined, ['requires_action'])).toThrow(
      'requires_action stop reason requires at least one event id',
    );
  });
});

describe('sessionErrorPayload', () => {
  it('shapes a non-retrying error', () => {
    expect(
      sessionErrorPayload({ type: 'setup_failed', message: 'boom', willRetry: false }),
    ).toEqual({
      error: { type: 'setup_failed', message: 'boom' },
      retry_status: { will_retry: false },
    });
  });

  it('shapes a retrying error with next_attempt + extra fields', () => {
    expect(
      sessionErrorPayload({
        type: 'transient_error',
        message: 'rate limit',
        willRetry: true,
        nextAttempt: 3,
        extra: { attempt: 2 },
      }),
    ).toEqual({
      error: { type: 'transient_error', message: 'rate limit' },
      retry_status: { will_retry: true, next_attempt: 3 },
      attempt: 2,
    });
  });
});

describe('turn model summary payloads', () => {
  const usage = {
    input_tokens: 1,
    output_tokens: 2,
    cache_creation_input_tokens: 3,
    cache_read_input_tokens: 4,
  };

  it('labels starts and requires envelope correlation on ends', () => {
    expect(turnModelSummaryStartPayload({ provider: 'anthropic', model: 'm' })).toEqual({
      model_observation_kind: 'turn_model_summary',
      provider: 'anthropic',
      model: 'm',
    });
    expect(
      turnModelSummaryEndPayload({
        modelUsage: usage,
        isError: false,
        modelRequestStartId: 'evt_start',
        provider: 'anthropic',
        model: 'm',
        totalCostUsd: 0.0234,
      }),
    ).toEqual({
      model_usage: usage,
      is_error: false,
      model_request_start_id: 'evt_start',
      model_observation_kind: 'turn_model_summary',
      provider: 'anthropic',
      model: 'm',
      // Fractional dollar cost must be preserved (not floored).
      total_cost_usd: 0.0234,
    });
  });

  it('rejects an invalid model-start correlation id', () => {
    expect(() =>
      turnModelSummaryEndPayload({
        modelUsage: usage,
        isError: true,
        modelRequestStartId: 'not-an-event' as `evt_${string}`,
      }),
    ).toThrow('model request start id must be a non-empty evt_ identifier');
  });
});

describe('event-kind vocabulary', () => {
  it('uses package-backed Claude RECEIVED taxonomy names', () => {
    expect(AgentEventKind).toEqual({
      ...CanonicalAgentEventKind,
      usage: AgentRuntimeSignalKind.usage,
    });
    expect(SessionEventKind).toEqual({
      ...ContractSessionEventKind,
      warning: 'session.warning',
    });
    expect(SpanEventKind).toBe(ContractSpanEventKind);
    expect(AgentEventKind.thinking).toBe('agent.thinking');
    expect(AgentEventKind.toolResult).toBe('agent.tool_result');
    expect(AgentEventKind.threadContextCompacted).toBe('agent.thread_context_compacted');
    expect(SessionEventKind.statusRunning).toBe('session.status_running');
    expect(SessionEventKind.error).toBe('session.error');
    expect(SpanEventKind.modelRequestEnd).toBe('span.model_request_end');
    expect(SpanEventKind.outcomeEvaluationEnd).toBe('span.outcome_evaluation_end');
  });

  it('keeps legacy aggregate output and separates runtime-only usage', () => {
    expect(ALL_SESSION_EVENT_KINDS).toEqual([
      'agent.message',
      'agent.thinking',
      'agent.tool_use',
      'agent.tool_result',
      'agent.mcp_tool_use',
      'agent.mcp_tool_result',
      'agent.custom_tool_use',
      'agent.thread_context_compacted',
      'agent.usage',
      'session.status_running',
      'session.status_idle',
      'session.status_rescheduled',
      'session.status_terminated',
      'session.error',
      'session.warning',
      'span.model_request_start',
      'span.model_request_end',
      'span.outcome_evaluation_start',
      'span.outcome_evaluation_ongoing',
      'span.outcome_evaluation_end',
    ]);
    expect(AgentEventKind.usage).toBe(AgentRuntimeSignalKind.usage);
    expect(CANONICAL_AGENT_EVENT_KINDS).not.toContain(AgentRuntimeSignalKind.usage);
    expect(CANONICAL_AGENT_EVENT_KINDS).not.toContain(
      InternalTranscriptEventKind.userEventProcessed,
    );
    expect(new Set(CANONICAL_AGENT_EVENT_KINDS).size).toBe(CANONICAL_AGENT_EVENT_KINDS.length);
    expect(Object.isFrozen(ALL_SESSION_EVENT_KINDS)).toBe(true);
  });
});
