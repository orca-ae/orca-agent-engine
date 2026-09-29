// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { AgentEventId } from './events.js';
import { isAgentEventId } from './ids.js';

function assertAgentEventId(value: unknown, label: string): asserts value is AgentEventId {
  if (!isAgentEventId(value)) {
    throw new Error(`${label} must be a non-empty evt_ identifier`);
  }
}

/** Why a session reached its idle boundary. */
export type SessionStopReasonType = 'end_turn' | 'requires_action' | 'retries_exhausted';

/**
 * `requires_action` is the only stop reason that carries event IDs, and it
 * must identify at least one pending event.
 */
export type SessionStopReason =
  | { type: 'end_turn' }
  | {
      type: 'requires_action';
      event_ids: readonly [AgentEventId, ...AgentEventId[]];
    }
  | { type: 'retries_exhausted' };

export interface SessionIdlePayload {
  stop_reason: SessionStopReason;
}

export function sessionIdlePayload(
  reason: Exclude<SessionStopReasonType, 'requires_action'>,
): SessionIdlePayload;
export function sessionIdlePayload(
  reason: 'requires_action',
  eventIds: readonly [AgentEventId, ...AgentEventId[]],
): SessionIdlePayload;
/** Build a `session.status_idle` payload without an empty required-action path. */
export function sessionIdlePayload(
  reason: SessionStopReasonType,
  eventIds?: readonly AgentEventId[],
): SessionIdlePayload {
  if (reason === 'requires_action') {
    if (!Array.isArray(eventIds) || eventIds.length === 0) {
      throw new Error('requires_action stop reason requires at least one event id');
    }
    eventIds.forEach((eventId) => assertAgentEventId(eventId, 'required action event id'));
    return {
      stop_reason: {
        type: reason,
        event_ids: [...eventIds] as [AgentEventId, ...AgentEventId[]],
      },
    };
  }
  if (eventIds !== undefined) {
    throw new Error(`${reason} stop reason must not include event ids`);
  }
  return { stop_reason: { type: reason } };
}

/** Typed error object carried by `session.error`. */
export interface SessionErrorObject {
  type: string;
  message: string;
}

/** Retry state for a handled session error. */
export type SessionErrorRetryStatus =
  | { will_retry: false }
  | { will_retry: true; next_attempt: number };

export interface SessionErrorPayload {
  error: SessionErrorObject;
  retry_status: SessionErrorRetryStatus;
  [key: string]: unknown;
}

type SessionErrorPayloadArgs =
  | {
      type: string;
      message: string;
      willRetry: false;
      extra?: Record<string, unknown>;
    }
  | {
      type: string;
      message: string;
      willRetry: true;
      nextAttempt: number;
      extra?: Record<string, unknown>;
    };

/** Build a `session.error` payload and preserve caller-specific error details. */
export function sessionErrorPayload(args: SessionErrorPayloadArgs): SessionErrorPayload {
  let retry_status: SessionErrorRetryStatus;
  if (args.willRetry) {
    if (!Number.isInteger(args.nextAttempt) || args.nextAttempt <= 0) {
      throw new Error('retrying session error requires a positive integer next attempt');
    }
    retry_status = { will_retry: true, next_attempt: args.nextAttempt };
  } else {
    if ('nextAttempt' in args) {
      throw new Error('non-retrying session error must not include next attempt');
    }
    retry_status = { will_retry: false };
  }
  return {
    ...(args.extra ?? {}),
    error: { type: args.type, message: args.message },
    retry_status,
  };
}

/** Token counts carried by a coarse turn model summary. */
export interface ModelUsageCounts {
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number;
  cache_read_input_tokens: number;
}

/** Proven granularity for currently emitted model observations. */
export const ModelObservationKind = {
  turnSummary: 'turn_model_summary',
} as const;

export interface TurnModelSummaryStartPayload {
  model_observation_kind: typeof ModelObservationKind.turnSummary;
  /** Trusted configured logical provider; not a verified served route. */
  provider?: string;
  /** Configured/requested model for this aggregate turn summary. */
  model?: string;
}

export interface TurnModelSummaryEndPayload {
  model_request_start_id: AgentEventId;
  model_observation_kind: typeof ModelObservationKind.turnSummary;
  model_usage: ModelUsageCounts;
  is_error: boolean;
  /** Trusted configured logical provider; not a verified served route. */
  provider?: string;
  /** Configured/requested model for this aggregate turn summary. */
  model?: string;
  total_cost_usd?: number;
}

/** Build the start of a coarse turn-level model summary. */
export function turnModelSummaryStartPayload(args: {
  provider?: string;
  model?: string;
}): TurnModelSummaryStartPayload {
  return {
    model_observation_kind: ModelObservationKind.turnSummary,
    ...(args.provider !== undefined ? { provider: args.provider } : {}),
    ...(args.model !== undefined ? { model: args.model } : {}),
  };
}

/** Build the terminal payload for a coarse turn-level model summary. */
export function turnModelSummaryEndPayload(args: {
  modelRequestStartId: AgentEventId;
  modelUsage: ModelUsageCounts;
  isError: boolean;
  provider?: string;
  model?: string;
  totalCostUsd?: number;
}): TurnModelSummaryEndPayload {
  assertAgentEventId(args.modelRequestStartId, 'model request start id');
  return {
    model_request_start_id: args.modelRequestStartId,
    model_observation_kind: ModelObservationKind.turnSummary,
    model_usage: args.modelUsage,
    is_error: args.isError,
    ...(args.provider !== undefined ? { provider: args.provider } : {}),
    ...(args.model !== undefined ? { model: args.model } : {}),
    ...(args.totalCostUsd !== undefined ? { total_cost_usd: args.totalCostUsd } : {}),
  };
}

export type OutcomeEvaluationResult =
  | 'satisfied'
  | 'needs_revision'
  | 'max_iterations_reached'
  | 'failed'
  | 'interrupted';

/** Payload marking exact acceptance of one persisted user event. */
export interface UserEventProcessedPayload {
  user_event_id: AgentEventId;
}

/** Build an acceptance marker only for a non-empty canonical user event ID. */
export function userEventProcessedPayload(userEventId: string): UserEventProcessedPayload {
  assertAgentEventId(userEventId, 'user event id');
  return { user_event_id: userEventId };
}
