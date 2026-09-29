// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { CustomToolDefinition, ModelEffort, ModelSpeed, UserEvent } from '../agent-harness.js';
import type { AgentDefinition } from '@anthropic-ai/claude-agent-sdk';

/** Internal subagent definition; modelSpeed is stripped before SDK invocation. */
export type SandboxAgentDefinition = AgentDefinition & {
  modelSpeed?: ModelSpeed;
  managedAgentId?: string;
};

/** A stamped event as emitted by the @orca/sandbox-harness SSE stream. */
export interface RawSandboxEvent {
  id: string;
  session_id: string;
  created_at: string;
  type: string;
  [key: string]: unknown;
}

/** Options for opening a session on the in-sandbox harness. */
export interface OpenSessionOptions {
  /** Provider/harness id the sandbox-harness server resolves (e.g. 'claude'). */
  agent: string;
  model?: string;
  modelSpeed?: ModelSpeed;
  modelEffort?: ModelEffort;
  /** Agent + skill instructions composed by the dispatcher. */
  systemPrompt?: string;
  /**
   * SDK built-ins selected for the in-sandbox agent. An empty array disables
   * caller-selected built-ins; the provider may still add `Agent` for subagents.
   */
  tools?: string[];
  /** Available tools whose managed-agent policy allows execution without prompting. */
  allowedTools?: string[];
  /** Logical handlers required by the primary agent or any configured subagent. */
  runtimeTools?: string[];
  /** Prior turns to rehydrate a fresh process (resume); omitted on cold first start. */
  replay?: Array<{ role: 'user' | 'assistant'; text?: string }>;
  /** SDK-facing subagent definitions exposed through the Claude Agent tool. */
  agents?: Record<string, SandboxAgentDefinition>;
  /** Forward full subagent messages instead of only heartbeat/tool frames. */
  forwardSubagentText?: boolean;
  /** Client-executed tools declared on the managed agent. */
  customTools?: CustomToolDefinition[];
}

/** A live link to one in-sandbox harness session. Transport-agnostic. */
export interface HarnessChannel {
  /** Async stream of raw sandbox events; reconnect-safe (dedups by event id). */
  events(): AsyncIterable<RawSandboxEvent>;
  /** Forward a user turn. */
  submit(event: UserEvent): Promise<void>;
  /** Internal SDK control, acknowledged by the subprocess. Never recorded as a user event. */
  sdkCommand?(command: unknown, afterSequence?: number): Promise<number>;
  /** Tear the session down and stop the stream. */
  stop(): Promise<void>;
}

/** Opens a HarnessChannel. DialInTransport (cloud/local) dials an HTTP endpoint;
 *  a future SelfHostedTransport multiplexes over an outbound runner link. */
export interface HarnessTransport {
  open(opts: OpenSessionOptions): Promise<HarnessChannel>;
}
