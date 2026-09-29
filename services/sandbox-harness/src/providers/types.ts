// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Provider interface — the extension seam for harness providers (the registered
// set is in ./registry.ts). A provider declares an id (+ optional aliases /
// metadata) and a `createRuntime` factory; the runtime drives one model in one
// session and yields canonical stream-json frames from `runTurn`.
//
// How Session (../session.ts) calls a runtime:
//   - `runTurn` is an async generator yielding WireFrames as they arrive
//     (streamed, never buffered into an array).
//   - `model` is a getter (read-only); `setModel` / `setPermissionMode` /
//     `interrupt` are optional control hooks.
//   - `createRuntime` receives `env` + `diagnostics` so providers stay
//     env-driven and write nothing to stdout (stdout is the NDJSON wire;
//     diagnostics is the stderr writer).
//
// No provider-specific fields leak into this interface. Content blocks, stream
// events, MCP server descriptors, and usage are typed forward-compatibly
// (open records) so a newer provider never breaks an older client.

import type { AgentDefinition } from '@anthropic-ai/claude-agent-sdk';
import { CLAUDE_MODEL_EFFORT_LEVELS, type ClaudeModelEffort } from '@orca/harness-catalog';

export const MODEL_SPEEDS = ['standard', 'fast'] as const;
export type ModelSpeed = (typeof MODEL_SPEEDS)[number];
export const MODEL_EFFORTS = CLAUDE_MODEL_EFFORT_LEVELS;
export type ModelEffort = ClaudeModelEffort;

/** Opaque environment map handed to a provider (defaults to `process.env`). */
export type Env = Record<string, string | undefined>;

/**
 * Stderr writer. Providers use this for diagnostics; they MUST NOT write to
 * stdout, which carries the NDJSON wire. Mirrors `s => stderr.write(s)`.
 */
export type Diagnostics = (message: string) => void;

/** Permission mode forwarded to the runtime (e.g. "default", "acceptEdits"). */
export type PermissionMode = string;

/** Internal model controls are stripped before definitions reach Claude SDK. */
export type RuntimeAgentDefinition = AgentDefinition & {
  modelSpeed?: ModelSpeed;
  /** Orca control-plane identity; stripped before the definition reaches Claude SDK. */
  managedAgentId?: string;
};

/** SDK-facing subagent definitions exposed through Claude's Agent tool. */
export type RuntimeAgentDefinitions = Record<string, RuntimeAgentDefinition>;

// ---------------------------------------------------------------------------
// Canonical wire — stream-json frames (PROTOCOL.md). The Claude Agent SDK's
// stream-json IS this wire; every provider transforms its native events to it.
// ---------------------------------------------------------------------------

/**
 * An Anthropic-style content block (text / tool_use / tool_result / …). Kept
 * open: `type` discriminates and blocks pass through transformations unchanged,
 * so new block kinds must not require a code change here.
 */
export interface ContentBlock {
  type: string;
  [key: string]: unknown;
}

/** Assistant/user message envelope as it appears on the wire. */
export interface WireMessage {
  model?: string;
  content?: ContentBlock[];
  usage?: Usage;
  [key: string]: unknown;
}

/**
 * A raw provider streaming event (e.g. `content_block_delta`), forwarded inside
 * a `stream_event` frame. Open by design — partial-message shapes evolve.
 */
export interface StreamEvent {
  type: string;
  [key: string]: unknown;
}

/** Token/cost accounting reported on the result frame; provider-defined shape. */
export type Usage = Record<string, unknown>;

/**
 * Opaque SDK MCP server descriptor (from `initialize`'s `sdk_mcp_servers`),
 * threaded through to providers that speak MCP. Not interpreted here.
 */
export type McpServer = Record<string, unknown>;

/** `system`/`init` frame. Session emits this; providers normally drop theirs. */
export interface SystemInitFrame {
  type: 'system';
  subtype: 'init';
  session_id: string;
  model?: string;
  tools: unknown[];
  mcp_servers: McpServer[];
}

/** `assistant` frame — a model message with content blocks. */
export interface AssistantFrame {
  type: 'assistant';
  message: WireMessage;
  parent_tool_use_id: string | null;
}

/** `user` frame — e.g. a tool-result echo threaded back into the transcript. */
export interface UserFrame {
  type: 'user';
  message: WireMessage;
}

/** Client-executed custom tool request emitted by a provider runtime. */
export interface CustomToolUseFrame {
  type: 'custom_tool_use';
  id: string;
  name: string;
  input: Record<string, unknown>;
}

/** `stream_event` frame — a partial delta, tagged with our session id. */
export interface StreamEventFrame {
  type: 'stream_event';
  session_id: string;
  event: StreamEvent;
}

/** `result` frame — terminal frame of a turn with canonical accounting fields. */
export interface ResultFrame {
  type: 'result';
  subtype: string;
  session_id: string;
  duration_ms: number;
  duration_api_ms: number;
  is_error: boolean;
  num_turns: number;
  total_cost_usd: number;
  usage: Usage;
  result: string;
}

/** The canonical stream-json frame union yielded by `runTurn`. */
export type WireFrame =
  | SystemInitFrame
  | AssistantFrame
  | UserFrame
  | CustomToolUseFrame
  | StreamEventFrame
  | ResultFrame;

// ---------------------------------------------------------------------------
// Runtime call shapes.
// ---------------------------------------------------------------------------

/** Prompt content for a turn: flat text or an array of content blocks. */
export type TurnContent = string | ContentBlock[];

export interface CustomToolDefinition {
  name: string;
  description?: string;
  input_schema?: Record<string, unknown>;
}

export interface CustomToolResultPayload {
  custom_tool_use_id: string;
  content?: unknown;
  result?: unknown;
  is_error?: boolean;
  [key: string]: unknown;
}

/** A prior conversation turn carried in session history (`{ role, text }`). */
export interface HistoryEntry {
  role: 'user' | 'assistant';
  text: string;
}

/**
 * Per-turn session snapshot Session passes to the runtime. Read-only context;
 * the runtime owns none of it. `history` excludes the current user message and
 * supports provider-agnostic replay; providers with native continuation must
 * not flatten it back into a warm-turn prompt.
 */
export interface SessionSnapshot {
  sessionId: string;
  turns: number;
  startedAt: number;
  history: HistoryEntry[];
  mcpServers: McpServer[];
}

/** Arguments to {@link Runtime.runTurn}. */
export interface RunTurnArgs {
  /** Flattened prompt text (content collapsed to a string). */
  prompt: string;
  /** Original prompt content — text or content blocks. */
  content: TurnContent;
  /** Read-only session context for this turn. */
  session: SessionSnapshot;
}

/**
 * Arguments to {@link Provider.createRuntime}. `env` + `diagnostics` are always
 * supplied so providers resolve endpoints/keys from env and never touch stdout.
 */
export interface CreateRuntimeArgs {
  model?: string;
  modelSpeed?: ModelSpeed;
  modelEffort?: ModelEffort;
  permissionMode?: PermissionMode;
  cwd?: string;
  /** Platform-provided instructions for the current sandbox session. */
  systemPrompt?: string;
  /**
   * Built-in SDK tools available to the agent. `[]` disables caller-selected
   * built-ins; providers may still inject required orchestration tools such as
   * `Agent` when subagents are configured.
   */
  tools?: string[];
  /** Available SDK tools that execute without an interactive permission prompt. */
  allowedTools?: string[];
  /** Logical handlers required by the primary agent or any configured subagent. */
  runtimeTools?: string[];
  /** Programmatic subagent roster for multi-agent coordinator sessions. */
  agents?: RuntimeAgentDefinitions;
  /** Forward full subagent messages when the SDK supports nested transcript streaming. */
  forwardSubagentText?: boolean;
  /** Client-executed custom tools declared on the managed agent. */
  customTools?: CustomToolDefinition[];
  /** Environment for endpoint/key resolution (typically `process.env`). */
  env: Env;
  /** Stderr writer for diagnostics. */
  diagnostics: Diagnostics;
}

// ---------------------------------------------------------------------------
// Provider + Runtime.
// ---------------------------------------------------------------------------

/**
 * One model session. Created per process by a {@link Provider}; drives the
 * native agent SDK in-process and streams canonical frames.
 */
export interface Runtime {
  /** Active model id (getter — set via {@link Runtime.setModel}). */
  readonly model: string;

  /** Private SDK commands; native events are consumed by the owning host adapter. */
  handleSdkCommand?(command: unknown, emit: (event: unknown) => void): Promise<void>;

  /** Switch the active model mid-session (control: `set_model`). */
  setModel?(model: string): void;

  /** Switch the permission mode mid-session (control: `set_permission_mode`). */
  setPermissionMode?(mode: PermissionMode): void;

  /** Abort the in-flight turn (control: `interrupt`). */
  interrupt?(): void;

  /** Resolve a pending client-executed custom tool call. */
  handleCustomToolResult?(payload: CustomToolResultPayload): boolean;

  /**
   * Run one turn, yielding canonical {@link WireFrame}s as they arrive. Errors
   * surface by throwing — Session maps them to an error `result`; runtimes do
   * not write frames or wrap their own errors.
   */
  runTurn(args: RunTurnArgs): AsyncGenerator<WireFrame, void, void>;
}

/**
 * A harness provider. Discovered/registered by `id` (+ `aliases`); `createRuntime`
 * is the only required behavior. Add a provider by implementing this — no
 * downstream code branches on provider identity.
 */
export interface Provider {
  /** Canonical provider id (e.g. "anthropic"). */
  id: string;
  /** Alternate ids that also resolve to this provider (e.g. "claude", "cc"). */
  aliases?: string[];
  /** Public harness id surfaced in metadata (e.g. "claude-code"); defaults to `id`. */
  harnessId?: string;
  /** Human-readable name for metadata listings; defaults to `id`. */
  displayName?: string;
  /** Advertised model ids, if the provider enumerates them. */
  models?: string[];
  /** Factory for this provider's per-session runtime. */
  createRuntime(args: CreateRuntimeArgs): Runtime;
}
