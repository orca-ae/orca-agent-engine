// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Abstract harness interface for an agent runtime.
 *
 * The harness encapsulates the SDK-specific machinery for driving a
 * conversational agent over the lifetime of a single managed-agent session.
 * It is owned by a SessionRunner, which feeds user events in via
 * `submit()` and forwards harness-emitted events from `events()` to the
 * transcript stream.
 *
 * Each harness validates the `user.*` kinds it supports and emits kinds from
 * the canonical vocabulary this module re-exports. The sandbox (with resources
 * already mounted by the dispatcher), its tools, and MCP servers arrive via
 * {@link SessionStartInput}.
 */

import type { SandboxHandle } from '../sandbox/sandbox-runtime.js';
import type { ToolDefinition } from '../sandbox/agent-toolset.js';
import type { SessionJwtProvider } from '../mcp/session-jwt-provider.js';
import { v7 as uuidv7 } from 'uuid';
import {
  isAgentEventId,
  isAgentEventSubpath,
  PRIMARY_AGENT_SUBPATH,
  type AgentEventId,
  type AgentEventSubpath,
} from '@orca/agent-event-contract';
import { CLAUDE_MODEL_EFFORT_LEVELS, type ClaudeModelEffort } from '@orca/harness-catalog';

export const MODEL_SPEEDS = ['standard', 'fast'] as const;
export type ModelSpeed = (typeof MODEL_SPEEDS)[number];
export const MODEL_EFFORTS = CLAUDE_MODEL_EFFORT_LEVELS;
export type ModelEffort = ClaudeModelEffort;

// Re-export the canonical event-kind vocabulary so `agent-harness.ts` stays the
// single shared contract both harness backends + the runner import from.
export {
  AgentEventKind,
  AgentRuntimeSignalKind,
  SessionEventKind,
  SpanEventKind,
  sessionIdlePayload,
  sessionErrorPayload,
  sessionWarningPayload,
  turnModelSummaryEndPayload,
  turnModelSummaryStartPayload,
  ALL_SESSION_EVENT_KINDS,
} from './event-kinds.js';
export type {
  SessionStopReason,
  SessionStopReasonType,
  SessionErrorObject,
  SessionErrorPayload,
  SessionErrorRetryStatus,
  SessionIdlePayload,
  ModelUsageCounts,
  OutcomeEvaluationResult,
  TurnModelSummaryEndPayload,
  TurnModelSummaryStartPayload,
} from './event-kinds.js';

export interface CustomToolDefinition {
  name: string;
  description?: string;
  input_schema?: Record<string, unknown>;
}

export interface RemoteMcpToolsetPolicy {
  serverName: string;
  permissionPolicy?: 'always_allow' | 'always_ask' | 'always_deny';
}

export interface SessionGuardrail {
  id: string;
  name: string;
  tier: 'session' | 'agent' | 'workspace' | 'organization';
  phases: string[];
  rule: unknown;
  stateful: boolean;
  stateScope?: 'turn' | 'session' | 'subject_window';
  subagentId?: string;
}

export interface SessionStartInput {
  workspaceId: string;
  sessionId: string;
  /**
   * Execute the built-in agent toolset through public user.tool_result events.
   * Only enabled for self-hosted sessions using the separated Claude SDK
   * harness, matching the Claude Managed Agents capability boundary.
   */
  clientToolExecution?: boolean;
  /**
   * Guardrails applying to this session, already ordered by authority. Sits at
   * session level rather than on the agent snapshot because composition spans
   * four tiers, only one of which is the agent — and a subagent-sourced entry
   * carries the subagent it came from so its state can be namespaced apart.
   */
  guardrails?: SessionGuardrail[];
  /** Restored guardrail counters, so a respawn cannot reset a cap. */
  guardrailState?: Record<string, unknown>;
  agentSnapshot: {
    name?: string;
    model_provider?: string;
    model_id?: string;
    model_speed?: ModelSpeed;
    model_effort?: ModelEffort;
    system?: string;
    /** Logical sandbox tool names (e.g. `bash`, `read`) declared by the AgentVersion. */
    allowed_tool_names?: string[];
    /** SDK-facing tool permission policies, keyed by exact tool name or `mcp__<server>__*`. */
    tool_permission_policies?: Record<string, 'always_allow' | 'always_ask' | 'always_deny'>;
    /** Client-executed tools declared on the agent. */
    custom_tools?: CustomToolDefinition[];
    multiagent?: {
      type: 'coordinator';
      agents: Array<{
        id: string;
        name: string;
        version: number;
        model_provider?: string;
        model_id?: string;
        model_speed?: ModelSpeed;
        model_effort?: ModelEffort;
        system?: string;
        allowed_tool_names?: string[];
        tool_permission_policies?: Record<string, 'always_allow' | 'always_ask' | 'always_deny'>;
      }>;
    };
  };
  /**
   * MCP servers to wire into the underlying SDK. Already rewritten by the
   * dispatcher to point at ai-gateway with the per-session JWT in the
   * Authorization header. Absent when the agent declares no MCP servers or
   * the rewrite fails.
   */
  mcpServers?: Record<string, { type: 'http'; url: string; headers: Record<string, string> }>;
  /** Session-scoped JWT source; the harness closes it before joining query startup on stop. */
  mcpJwtProvider?: Pick<SessionJwtProvider, 'getValidToken' | 'close'>;
  /**
   * Remote MCP server names enabled by `tools: [{ type: 'mcp_toolset', ... }]`.
   * The Claude SDK exposes concrete upstream tool names only after connecting
   * to each MCP server, so the harness grants `mcp__<server>__*` permission
   * while still denying built-in host tools.
   */
  remoteMcpToolsets?: RemoteMcpToolsetPolicy[];
  /**
   * Sandbox handle for tool dispatch + file mounts. Absent when the
   * dispatcher acquired no sandbox for the session (for example, when it has
   * no SandboxRuntime).
   */
  sandbox?: SandboxHandle;
  /**
   * Custom tool definitions (agent_toolset) bound to the sandbox.
   * Forwarded to the underlying SDK so the model can dispatch sandbox tools.
   */
  tools?: ToolDefinition[];
}

/**
 * Logical tool handlers that must exist for at least one runtime agent. This
 * registration union is intentionally separate from each agent's own SDK
 * visibility and permission policy.
 */
export function runtimeAgentToolNameUnion(snapshot: SessionStartInput['agentSnapshot']): string[] {
  const names = new Set(snapshot.allowed_tool_names ?? []);
  for (const agent of snapshot.multiagent?.agents ?? []) {
    for (const name of agent.allowed_tool_names ?? []) names.add(name);
  }
  return [...names];
}

export interface UserEvent {
  /** Stable transcript event id, used to resolve Registry-authenticated turn context. */
  id?: string;
  /** Event kind on the wire. Each harness validates its supported `user.*` subset. */
  kind: string;
  /** Already-parsed JSON payload (the HTTP wire body). */
  payload: unknown;
  /**
   * Optional `system.message` immediately following this event in the same
   * public event batch. The dispatcher resolves the companion without
   * reordering the public transcript; harnesses may apply it as privileged
   * context for the turn started or resumed by this event.
   */
  systemMessage?: unknown;
}

export interface SubmitHooks {
  /**
   * Persist the fact that this event left the harness queue. Implementations
   * must await this exactly once after validation/selection and before they
   * mutate agent state, resume a blocked query, or start new agent work.
   */
  onAccepted(): Promise<void>;
}

export interface AgentEvent {
  /** Service-local durability handshake. Never serialized into the event payload. */
  persistence?: { resolve(): void; reject(error: unknown): void };
  /** Pending pauses settle delivery only; explicit sources finish one durable runtime turn. */
  completionPolicy?: 'pending' | { sourceIds: string[] };
  /** Stable production time for durable terminal receipt replay. */
  producedAt?: string;
  /** Event kind on the wire (e.g. `'agent.message'`). */
  kind: string;
  payload: unknown;
  /** Stable producer identity preserved as Transcript `Event.id`. */
  id: AgentEventId;
  /** Canonical primary or child producer path. */
  subpath: AgentEventSubpath;
}

/** Draft accepted only at a producer's envelope-completion boundary. */
export interface AgentEventInput extends Omit<AgentEvent, 'id' | 'subpath'> {
  id?: string;
  subpath?: string;
}

/**
 * Complete one service-local producer envelope at the emission boundary.
 * Omitted fields become a fresh event ID and the primary path; malformed
 * supplied fields fail closed rather than being rewritten into another event.
 */
export function withCanonicalAgentEventEnvelope(event: AgentEventInput): AgentEvent {
  const id = event.id === undefined ? (`evt_${uuidv7()}` as AgentEventId) : event.id;
  if (!isAgentEventId(id)) throw new Error(`invalid AgentEvent.id: ${String(id)}`);
  const subpath = event.subpath === undefined ? PRIMARY_AGENT_SUBPATH : event.subpath;
  if (!isAgentEventSubpath(subpath)) {
    throw new Error(`invalid AgentEvent.subpath: ${String(subpath)}`);
  }
  return { ...event, id, subpath };
}

export type UserEventSubmitResult = 'submitted' | 'deferred';

/** Ambiguous durability must retire the runtime and retain source delivery for recovery. */
export class DurableHarnessStateError extends Error {}

/** A fatal worker is retired after its turn outcome is already durable. */
export class SettledHarnessFailureError extends Error {}

export class UnappliedUserEventError extends Error {
  constructor(
    readonly kind: string,
    message: string,
  ) {
    super(message);
    this.name = 'UnappliedUserEventError';
  }
}

/** Expected, non-retryable policy refusal after the source event was accepted. */
export class GuardrailPolicyDeniedError extends Error {
  constructor(
    readonly reasons: readonly string[],
    message: string,
  ) {
    super(message);
    this.name = 'GuardrailPolicyDeniedError';
  }
}

/** Further work is unsafe until Registry acknowledges authoritative usage state. */
export class GuardrailUsageUnavailableError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'GuardrailUsageUnavailableError';
  }
}

export type TerminationReason =
  | 'client.archived'
  | 'replica.shutting_down'
  | 'session.updated'
  | 'idle.timeout'
  | 'error';

export interface AgentHarness {
  /** Capture the session boot context (workspace, session id, agent snapshot). */
  start(input: SessionStartInput): Promise<void>;
  /** Forward a single user event into the underlying SDK. */
  submit(event: UserEvent, hooks?: SubmitHooks): Promise<UserEventSubmitResult | void>;
  /**
   * True while the harness is parked waiting for client input such as
   * `user.tool_confirmation` or `user.custom_tool_result`.
   */
  hasPendingRequiredAction?(): boolean;
  /** True while an unacknowledged usage delta keeps stateful guardrails fail-closed. */
  hasPendingGuardrailUsage?(): boolean;
  /** Replace usage-backed shared guardrail facts with Registry's acknowledged totals. */
  applyGuardrailUsageState?(state: Readonly<Record<string, unknown>>): void;
  /** Stop the harness, abort any in-flight SDK call, and drain consumers. */
  stop(reason: TerminationReason): Promise<void>;
  /** Async iterable of harness-emitted agent events. */
  events(): AsyncIterable<AgentEvent>;
}
