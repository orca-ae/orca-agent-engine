// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  withCanonicalAgentEventEnvelope,
  type AgentEvent,
  type AgentEventInput,
} from '../agent-harness.js';
import { isAgentEventId, type AgentEventId } from '@orca/agent-event-contract';
import {
  AgentEventKind,
  AgentRuntimeSignalKind,
  SessionEventKind,
  SpanEventKind,
  sessionErrorPayload,
  sessionIdlePayload,
  turnModelSummaryEndPayload,
  type ModelUsageCounts,
} from '../event-kinds.js';
import type { RawSandboxEvent } from './transport.js';
import { cacheCreationTotalFromRaw } from '../../usage-normalization.js';

/**
 * Map one stamped sandbox-harness event to zero or more orca AgentEvents.
 *
 * Both harness backends emit the same Claude RECEIVED taxonomy. The
 * in-sandbox wire is the sandbox-native vocabulary
 * (`session.status_idle`/`session.status_error`); this bridge translates it into
 * the orca output names — a terminal `session.status_idle` frame fans out to the
 * internal usage sink + `span.model_request_end` + a `session.status_idle`
 * carrying `stop_reason:end_turn`, and an error frame becomes `session.error` +
 * `session.status_idle{retries_exhausted}` (the session stays idle, not killed).
 *
 * Parity with the client ingress: the sandbox echoes the user turn, but the
 * registry already recorded it, so `user.message` is dropped. A valid mapped
 * `AgentEvent.id` becomes Transcript `Event.id` and matching
 * `idempotencyKey` metadata.
 */
export function mapSandboxEvent(
  raw: RawSandboxEvent,
  modelSummary?: { modelRequestStartId: AgentEventId; provider?: string; model?: string },
): AgentEvent[] {
  return mapSandboxEventPayload(raw, modelSummary).map(withCanonicalAgentEventEnvelope);
}

function mapSandboxEventPayload(
  raw: RawSandboxEvent,
  modelSummary?: { modelRequestStartId: AgentEventId; provider?: string; model?: string },
): AgentEventInput[] {
  const id = raw.id;
  switch (raw.type) {
    case 'user.message':
      return []; // registry already recorded the client's user message
    case 'agent.message':
      return [{ kind: AgentEventKind.message, id, payload: { content: raw['content'] ?? [] } }];
    case 'agent.thinking':
      return [{ kind: AgentEventKind.thinking, id, payload: { content: raw['content'] ?? [] } }];
    case 'agent.tool_use': {
      const toolUseId =
        typeof raw['tool_use_id'] === 'string' && raw['tool_use_id'].length > 0
          ? raw['tool_use_id']
          : id;
      return [
        {
          kind: AgentEventKind.toolUse,
          id,
          payload: {
            name: raw['name'],
            input: raw['input'] ?? {},
            tool_use_id: toolUseId,
          },
        },
      ];
    }
    case 'agent.custom_tool_use':
      if (!isAgentEventId(id)) {
        throw new Error('agent.custom_tool_use requires a canonical sandbox event id');
      }
      return [
        {
          kind: AgentEventKind.customToolUse,
          id,
          payload: { id, name: raw['name'], input: raw['input'] ?? {} },
        },
        {
          kind: SessionEventKind.statusIdle,
          id: `${id}:idle`,
          payload: sessionIdlePayload('requires_action', [id]),
        },
      ];
    case 'agent.tool_result':
      return [
        {
          kind: AgentEventKind.toolResult,
          id,
          payload: {
            tool_use_id:
              typeof raw['tool_use_id'] === 'string' && raw['tool_use_id'].length > 0
                ? raw['tool_use_id']
                : id,
            content: raw['content'],
            is_error: Boolean(raw['is_error']),
          },
        },
      ];
    case 'agent.usage': {
      const usage = raw['usage'] ?? {};
      const model = typeof raw['model'] === 'string' ? raw['model'] : undefined;
      const subagentId = typeof raw['subagent_id'] === 'string' ? raw['subagent_id'] : undefined;
      return [
        {
          kind: AgentRuntimeSignalKind.usage,
          id,
          payload: {
            usage,
            // Registry prices a delta against the model that produced it and
            // attributes a dispatched subagent's spend to its own thread, so
            // both ride along when the sandbox frame carries them.
            ...(model ? { model } : {}),
            ...(subagentId ? { subagent_id: subagentId } : {}),
          },
        },
      ];
    }
    case 'session.status_idle': {
      if (!modelSummary) {
        throw new Error('session.status_idle requires an open turn model summary');
      }
      const usage = raw['usage'] ?? {};
      const totalCostUsd = typeof raw['total_cost_usd'] === 'number' ? raw['total_cost_usd'] : 0;
      const model = typeof raw['model'] === 'string' ? raw['model'] : undefined;
      const subagentId = typeof raw['subagent_id'] === 'string' ? raw['subagent_id'] : undefined;
      return [
        // Internal usage sink (diverted by SessionRunner, never streamed). A
        // provider that emitted per-assistant usage sets usage_already_reported
        // so this aggregate remains span-only and is not counted twice.
        ...(raw['usage_already_reported'] === true
          ? []
          : [
              {
                kind: AgentEventKind.usage,
                id,
                payload: {
                  usage,
                  ...(model ? { model } : {}),
                  ...(subagentId ? { subagent_id: subagentId } : {}),
                },
              } as AgentEvent,
            ]),
        {
          kind: SpanEventKind.modelRequestEnd,
          id: `${id}:model_end`,
          payload: turnModelSummaryEndPayload({
            modelRequestStartId: modelSummary.modelRequestStartId,
            modelUsage: modelUsageFromRaw(usage),
            isError: false,
            ...(modelSummary.provider !== undefined ? { provider: modelSummary.provider } : {}),
            ...(modelSummary.model !== undefined ? { model: modelSummary.model } : {}),
            totalCostUsd,
          }),
        },
        {
          kind: SessionEventKind.statusIdle,
          id: `${id}:idle`,
          payload: sessionIdlePayload('end_turn'),
        },
      ];
    }
    case 'session.status_error': {
      const message = typeof raw['error'] === 'string' ? raw['error'] : 'unknown error';
      return [
        {
          kind: SessionEventKind.error,
          id,
          payload: sessionErrorPayload({ type: 'processing_error', message, willRetry: false }),
        },
        {
          kind: SessionEventKind.statusIdle,
          id: `${id}:idle`,
          payload: sessionIdlePayload('retries_exhausted'),
        },
      ];
    }
    default:
      return []; // unknown frames are dropped, never thrown
  }
}

function modelUsageFromRaw(usage: unknown): ModelUsageCounts {
  const u = usage && typeof usage === 'object' ? (usage as Record<string, unknown>) : {};
  return {
    input_tokens: numberField(u['input_tokens']),
    output_tokens: numberField(u['output_tokens']),
    cache_creation_input_tokens: cacheCreationTotalFromRaw(u),
    cache_read_input_tokens: numberField(u['cache_read_input_tokens']),
  };
}

function numberField(value: unknown): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) return 0;
  return Math.floor(value);
}
