// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  AgentEventKind as CanonicalAgentEventKind,
  AgentRuntimeSignalKind,
  SessionEventKind as CanonicalSessionEventKind,
  SpanEventKind,
  sessionErrorPayload,
  sessionIdlePayload,
  turnModelSummaryEndPayload,
  turnModelSummaryStartPayload,
} from '@orca/agent-event-contract';
import type {
  ModelUsageCounts,
  OutcomeEvaluationResult,
  SessionErrorObject,
  SessionErrorPayload,
  SessionErrorRetryStatus,
  SessionIdlePayload,
  SessionStopReason,
  SessionStopReasonType,
  TurnModelSummaryEndPayload,
  TurnModelSummaryStartPayload,
} from '@orca/agent-event-contract';

/**
 * Compatibility facade for the shared producer vocabulary.
 *
 * The registry wire contract stays a free-form string (`HttpEvent.type =
 * z.string().min(1)`); this is a producer-side convention, not a wire schema.
 * Visibility is still decided by the registry (`transcriptEventVisibility`): any
 * kind here that is not `harness.`-prefixed streams to clients as `public`.
 */

/**
 * Agent turn-content events (server -> client).
 *
 * `usage` is an internal runtime signal rather than a canonical persisted
 * producer event. Keep this legacy facade property for existing consumers.
 */
export const AgentEventKind = {
  ...CanonicalAgentEventKind,
  usage: AgentRuntimeSignalKind.usage,
} as const;

/**
 * Session lifecycle events, plus the harness-local `warning`.
 *
 * A warning reports that the runtime could not honour something it was asked
 * to do — a guardrail this topology cannot enforce — without failing the
 * session. It is not part of the canonical producer vocabulary, so it is
 * layered on here exactly as `AgentEventKind.usage` is above rather than
 * widening the shared contract for one harness's needs.
 */
export const SessionEventKind = {
  ...CanonicalSessionEventKind,
  warning: 'session.warning',
} as const;

/** Build the payload for a non-fatal session policy/topology warning. */
export function sessionWarningPayload(args: {
  type: string;
  message: string;
  guardrailId?: string;
  guardrailName?: string;
}): { warning: { type: string; message: string }; [k: string]: unknown } {
  return {
    warning: { type: args.type, message: args.message },
    ...(args.guardrailId ? { guardrail_id: args.guardrailId } : {}),
    ...(args.guardrailName ? { guardrail_name: args.guardrailName } : {}),
  };
}

export {
  AgentRuntimeSignalKind,
  SpanEventKind,
  sessionErrorPayload,
  sessionIdlePayload,
  turnModelSummaryEndPayload,
  turnModelSummaryStartPayload,
};
export type {
  ModelUsageCounts,
  OutcomeEvaluationResult,
  SessionErrorObject,
  SessionErrorPayload,
  SessionErrorRetryStatus,
  SessionIdlePayload,
  SessionStopReason,
  SessionStopReasonType,
  TurnModelSummaryEndPayload,
  TurnModelSummaryStartPayload,
};

/** Every known server -> client event kind, for conformance assertions/tests. */
export const ALL_SESSION_EVENT_KINDS: readonly string[] = Object.freeze([
  ...Object.values(AgentEventKind),
  ...Object.values(SessionEventKind),
  ...Object.values(SpanEventKind),
]);
