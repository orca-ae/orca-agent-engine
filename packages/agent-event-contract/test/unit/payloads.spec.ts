// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import {
  ModelObservationKind,
  SpanEventKind,
  sessionErrorPayload,
  sessionIdlePayload,
  turnModelSummaryEndPayload,
  turnModelSummaryStartPayload,
  userEventProcessedPayload,
  type AgentEvent,
  type AgentEventId,
  type ModelUsageCounts,
  type OutcomeEvaluationResult,
  type SessionStopReason,
  type SessionStopReasonType,
  type TurnModelSummaryStartPayload,
} from '../../src/index.js';

const modelUsage: ModelUsageCounts = {
  input_tokens: 10,
  output_tokens: 20,
  cache_creation_input_tokens: 3,
  cache_read_input_tokens: 4,
};

describe('session lifecycle payloads', () => {
  it('matches the vendored Anthropic idle stop-reason discriminator', () => {
    const spec = JSON.parse(
      readFileSync(
        new URL(
          '../../../../services/registry-service-ts/vendor/anthropic/openapi.json',
          import.meta.url,
        ),
        'utf8',
      ),
    );
    const reasons = {
      end_turn: sessionIdlePayload('end_turn'),
      requires_action: sessionIdlePayload('requires_action', ['evt_pending']),
      retries_exhausted: sessionIdlePayload('retries_exhausted'),
    } satisfies Record<SessionStopReasonType, unknown>;
    const mapping =
      spec.components.schemas.BetaManagedAgentsSessionStatusIdleEvent.properties.stop_reason
        .discriminator.mapping;
    expect(Object.keys(reasons).sort()).toEqual(Object.keys(mapping).sort());
  });
  it('builds idle payloads for terminal and retry-exhausted turns', () => {
    expect(sessionIdlePayload('end_turn')).toEqual({ stop_reason: { type: 'end_turn' } });
    expect(sessionIdlePayload('retries_exhausted')).toEqual({
      stop_reason: { type: 'retries_exhausted' },
    });
  });

  it('requires and preserves at least one pending event for requires_action', () => {
    const eventIds = ['evt_pending'] as [AgentEventId, ...AgentEventId[]];
    const reason: SessionStopReason = { type: 'requires_action', event_ids: eventIds };

    expect(sessionIdlePayload('requires_action', eventIds)).toEqual({ stop_reason: reason });
  });

  it('rejects missing, empty, or invalid required-action event IDs at runtime', () => {
    expect(() => sessionIdlePayload('requires_action', undefined as never)).toThrow(
      'requires_action stop reason requires at least one event id',
    );
    expect(() => sessionIdlePayload('requires_action', [] as never)).toThrow(
      'requires_action stop reason requires at least one event id',
    );
    expect(() =>
      sessionIdlePayload('requires_action', ['evt_pending', 'not_an_event'] as never),
    ).toThrow('required action event id must be a non-empty evt_ identifier');
  });

  it('rejects event IDs for an idle reason that cannot carry them', () => {
    expect(() =>
      Reflect.apply(sessionIdlePayload, undefined, ['end_turn', ['evt_pending']]),
    ).toThrow('end_turn stop reason must not include event ids');
  });

  it('builds valid discriminated error retry statuses and retains extra error context', () => {
    expect(
      sessionErrorPayload({
        type: 'processing_error',
        message: 'retry later',
        willRetry: true,
        nextAttempt: 1,
        extra: { user_event_kind: 'user.message' },
      }),
    ).toEqual({
      error: { type: 'processing_error', message: 'retry later' },
      retry_status: { will_retry: true, next_attempt: 1 },
      user_event_kind: 'user.message',
    });
    expect(
      sessionErrorPayload({ type: 'setup_failed', message: 'stopped', willRetry: false }),
    ).toEqual({
      error: { type: 'setup_failed', message: 'stopped' },
      retry_status: { will_retry: false },
    });
  });

  it('rejects contradictory and invalid retry inputs at runtime', () => {
    for (const nextAttempt of [undefined, 1]) {
      expect(() =>
        sessionErrorPayload({
          type: 'setup_failed',
          message: 'stopped',
          willRetry: false,
          nextAttempt,
        } as never),
      ).toThrow('non-retrying session error must not include next attempt');
    }

    for (const nextAttempt of [undefined, 0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() =>
        sessionErrorPayload({
          type: 'setup_failed',
          message: 'retry automatically',
          willRetry: true,
          nextAttempt,
        } as never),
      ).toThrow('retrying session error requires a positive integer next attempt');
    }
  });
});

describe('turn model summary payloads', () => {
  it('uses a typed start envelope as sole model-request identity', () => {
    const startEvent: AgentEvent<
      typeof SpanEventKind.modelRequestStart,
      TurnModelSummaryStartPayload
    > = {
      id: 'evt_start',
      subpath: '',
      kind: SpanEventKind.modelRequestStart,
      payload: turnModelSummaryStartPayload({}),
    };

    expect(startEvent.payload).toEqual({
      model_observation_kind: ModelObservationKind.turnSummary,
    });
    expect(startEvent.payload).not.toHaveProperty('id');
    expect(turnModelSummaryStartPayload({ provider: 'anthropic', model: 'claude-opus' })).toEqual({
      model_observation_kind: ModelObservationKind.turnSummary,
      provider: 'anthropic',
      model: 'claude-opus',
    });

    expect(
      turnModelSummaryEndPayload({
        modelRequestStartId: startEvent.id,
        modelUsage,
        isError: false,
      }),
    ).toEqual({
      model_request_start_id: startEvent.id,
      model_observation_kind: 'turn_model_summary',
      model_usage: modelUsage,
      is_error: false,
    });
  });

  it('validates summary correlation and preserves optional terminal fields', () => {
    expect(
      turnModelSummaryEndPayload({
        modelRequestStartId: 'evt_start',
        modelUsage,
        isError: true,
        provider: 'anthropic',
        model: 'claude-opus',
        totalCostUsd: 0,
      }),
    ).toEqual({
      model_request_start_id: 'evt_start',
      model_observation_kind: 'turn_model_summary',
      model_usage: modelUsage,
      is_error: true,
      provider: 'anthropic',
      model: 'claude-opus',
      total_cost_usd: 0,
    });
    expect(() =>
      turnModelSummaryEndPayload({
        modelRequestStartId: 'not_an_event' as AgentEventId,
        modelUsage,
        isError: false,
      }),
    ).toThrow('model request start id must be a non-empty evt_ identifier');
  });
});

describe('acceptance and outcome payload types', () => {
  it('builds an acceptance marker only for a non-empty event id', () => {
    expect(userEventProcessedPayload('evt_user')).toEqual({ user_event_id: 'evt_user' });
    expect(() => userEventProcessedPayload('evt_')).toThrow(
      'user event id must be a non-empty evt_ identifier',
    );
    expect(() => userEventProcessedPayload('user_event')).toThrow(
      'user event id must be a non-empty evt_ identifier',
    );
  });

  it('retains the full outcome result union', () => {
    const results: readonly OutcomeEvaluationResult[] = [
      'satisfied',
      'needs_revision',
      'max_iterations_reached',
      'failed',
      'interrupted',
    ];

    expect(results).toHaveLength(5);
  });
});
