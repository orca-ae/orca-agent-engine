// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// ---------------------------------------------------------------------------
// Subprocess-side Session. Owns ALL process-local turn state. The wire
// (protocol.ts → StreamJsonServer) calls handleControl(request) for
// control_request messages and iterates runTurn() for user turns. The runtime
// (from a provider) does the actual model work and yields canonical frames;
// Session wraps those with system/init and a terminating result.
//
// Resume: at construction Session decodes an optional `replay` (prior
// conversation, the caller having read+decoded it from SANDBOX_HARNESS_REPLAY)
// and seeds it as a PREAMBLE so a *fresh* subprocess rehydrates context on its
// first real turn. The seam is deliberately provider-AGNOSTIC — it only touches
// Session.history and the first user message, never a provider — so every
// provider inherits resume for free. Keeping the injection here, not in a
// provider, is the whole point.
// ---------------------------------------------------------------------------

import { randomUUID } from 'node:crypto';

import {
  resultFrame,
  systemInit,
  type ContentBlock,
  type CustomToolResultMessage,
  type ControlRequest,
  type ControlResult,
  type Frame,
  type MessageContent,
  type ResultFrame,
  type Session as SessionContract,
  type SessionTurnInput,
} from './protocol.js';
import {
  listProviderMetadata,
  type HistoryEntry,
  type ModelEffort,
  type ModelSpeed,
  type Provider,
  type ProviderMetadata,
  type RuntimeAgentDefinitions,
  type CustomToolDefinition,
  type Runtime,
  type TurnContent,
} from './providers/index.js';

// ---------------------------------------------------------------------------
// Session-owned types. These describe process-local turn state and the resume
// payload Session accepts. Wire-frame shapes and the provider interface live in
// their own modules; what remains here is genuinely Session's domain.
// ---------------------------------------------------------------------------

/** Permission mode forwarded to the runtime (e.g. "default", "acceptEdits"). */
export type PermissionMode = string;

/**
 * One prior conversation turn handed in to resume a session. Provider-agnostic
 * and intentionally loose: it may carry flattened `text`, or `parts` (content
 * blocks, e.g. an assistant message echoed back) which collapse to text the
 * same way assistant frames do. The parent base64-encodes a JSON array of these
 * into the replay env var; the subprocess entry decodes it and passes it here.
 */
export interface ReplayEntry {
  /** "user" | "assistant"; anything else is treated as "user". */
  role?: string;
  /** Already-flattened text for this turn (takes precedence when a string). */
  text?: string;
  /** Structured content blocks; flattened to text when `text` is absent. */
  parts?: ContentBlock[];
}

/**
 * Constructor options. Mirrors the keys `subprocess-entry.ts` supplies. `replay`
 * (NEW) seeds resume context as a preamble; it is already decoded by the caller
 * (env read + base64/JSON parse happen in the entry, which owns that policy and
 * fails loudly on a malformed value).
 */
export interface SessionOptions {
  /** The resolved provider whose runtime drives the model. */
  provider: Provider;
  /** Initial model id; `null` (no `--model` flag) defers to the provider default. */
  model?: string | null;
  /** Session-wide inference speed requested by Managed Agents. */
  modelSpeed?: ModelSpeed;
  /** Reasoning effort requested for the primary agent. */
  modelEffort?: ModelEffort;
  /** Initial permission mode (defaults to "default"). */
  permissionMode?: PermissionMode;
  /** Working directory for the runtime (defaults to `process.cwd()`). */
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
  /** Environment for the runtime's endpoint/key resolution (defaults to `process.env`). */
  env?: NodeJS.ProcessEnv;
  /** Stderr sink for runtime diagnostics; stdout is reserved for the NDJSON wire. */
  stderr: { write(chunk: string): unknown };
  /** Prior conversation to rehydrate. Seeded ONCE, before any client turn. */
  replay?: ReplayEntry[];
  /** Programmatic subagent roster for multi-agent coordinator sessions. */
  agents?: RuntimeAgentDefinitions;
  /** Forward full subagent messages when the SDK supports nested transcript streaming. */
  forwardSubagentText?: boolean;
  /** Client-executed custom tools declared on the managed agent. */
  customTools?: CustomToolDefinition[];
}

/** Opaque hook registration map carried by an `initialize` control request. */
export type Hooks = Record<string, unknown>;

/** Opaque in-process MCP server descriptor carried by `initialize`. */
export type McpServer = Record<string, unknown>;

// ---------------------------------------------------------------------------
// Helpers (pure).
// ---------------------------------------------------------------------------

/**
 * Concatenate the text of every `text` block; non-text / malformed blocks
 * contribute "". Accepts `unknown` so it can read straight off a forwarded
 * frame's `message.content`.
 */
function assistantText(content: unknown): string {
  if (!Array.isArray(content)) return '';
  return content
    .map((block) => {
      if (block === null || typeof block !== 'object') return '';
      const b = block as ContentBlock;
      return b.type === 'text' && typeof b.text === 'string' ? b.text : '';
    })
    .join('');
}

/** Flatten a replay entry to a single string: explicit `text`, else its `parts`. */
function replayEntryText(entry: ReplayEntry): string {
  if (typeof entry.text === 'string') return entry.text;
  if (Array.isArray(entry.parts)) return assistantText(entry.parts);
  return '';
}

/** Normalize a loose replay role to one of the two history roles. */
function replayRole(role: unknown): HistoryEntry['role'] {
  return role === 'assistant' ? 'assistant' : 'user';
}

// ---------------------------------------------------------------------------
// Session.
// ---------------------------------------------------------------------------

/**
 * The process-local session. `implements SessionContract` guarantees structural
 * compatibility with {@link StreamJsonServer}, which depends only on that
 * surface (sessionId, turns, handleControl, runTurn).
 */
export class Session implements SessionContract {
  /** Stable per-process session id, stamped into every frame. */
  readonly sessionId: string;
  /** Monotonic turn counter; stamps `num_turns` on synthesized results. */
  turns: number;
  /** Accumulated conversation, oldest first. The current user turn is included while running. */
  history: HistoryEntry[];
  /** SDK MCP server descriptors from the most recent `initialize`. */
  mcpServers: McpServer[];
  /** Hook registrations from the most recent `initialize`. */
  hooks: Hooks;
  /** Active model id, or null when deferring to the provider default. */
  model: string | null;
  /** Active permission mode. */
  permissionMode: PermissionMode;

  /** The provider runtime; private so all model work flows through this Session. */
  private readonly runtime: Runtime;

  /** Best-effort stderr writer; diagnostics must never affect the NDJSON wire. */
  private readonly diagnostics: (message: string) => void;

  /**
   * The resume transcript, flattened into a single user message, prepended to
   * the FIRST real prompt and then cleared. `undefined` once consumed (or when
   * there was no replay), so it can never be injected twice.
   */
  private preamble: string | undefined;

  constructor(options: SessionOptions) {
    const {
      provider,
      model = null,
      modelSpeed,
      modelEffort,
      permissionMode,
      cwd,
      systemPrompt,
      tools,
      allowedTools,
      runtimeTools,
      env = process.env,
      stderr,
      replay,
      agents,
      forwardSubagentText,
      customTools,
    } = options;

    this.sessionId = `sess_${randomUUID().replace(/-/g, '').slice(0, 24)}`;
    this.turns = 0;
    this.history = [];
    this.mcpServers = [];
    this.hooks = {};
    this.model = model;
    this.permissionMode = permissionMode ?? 'default';

    this.diagnostics = (message: string): void => {
      try {
        stderr.write(message);
      } catch {
        // A broken diagnostics sink must not turn a completed turn into a wire error.
      }
    };

    this.runtime = provider.createRuntime({
      // `model` is optional on the provider; under exactOptionalPropertyTypes we
      // omit the key entirely rather than pass `null`/`undefined` through.
      ...(model != null ? { model } : {}),
      ...(modelSpeed !== undefined ? { modelSpeed } : {}),
      ...(modelEffort !== undefined ? { modelEffort } : {}),
      permissionMode: this.permissionMode,
      cwd: cwd ?? process.cwd(),
      ...(systemPrompt !== undefined ? { systemPrompt } : {}),
      ...(tools !== undefined ? { tools } : {}),
      ...(allowedTools !== undefined ? { allowedTools } : {}),
      ...(runtimeTools !== undefined ? { runtimeTools } : {}),
      ...(agents && Object.keys(agents).length > 0 ? { agents } : {}),
      ...(forwardSubagentText !== undefined ? { forwardSubagentText } : {}),
      ...(customTools && customTools.length > 0 ? { customTools } : {}),
      env,
      diagnostics: this.diagnostics,
    });

    // Resume seam: provider-agnostic, injected ONCE here before any client turn,
    // so a fresh subprocess rehydrates prior context and every provider inherits
    // resume without changes.
    this.seedReplay(replay);
  }

  /**
   * Flatten prior turns into both `history` (so the per-turn snapshot carries
   * them) and a single `preamble` user message (so the provider — which only
   * sees a prompt — rehydrates context on its first real turn). Purely a Session
   * concern: no provider is touched. Empty / textless entries are skipped; an
   * all-empty replay leaves the session pristine.
   */
  private seedReplay(replay: ReplayEntry[] | undefined): void {
    if (!Array.isArray(replay) || replay.length === 0) return;

    const lines: string[] = [];
    for (const entry of replay) {
      const text = replayEntryText(entry);
      if (!text) continue;
      const role = replayRole(entry.role);
      this.history.push({ role, text });
      lines.push(`${role}: ${text}`);
    }

    if (lines.length > 0) {
      this.preamble = lines.join('\n');
    }
  }

  /**
   * Dispatch a control_request. Returns `undefined` for a bare success or an
   * object whose fields are spread into the control_response; throwing yields an
   * error response. The param is the protocol's loose request shape; we narrow
   * on `subtype`.
   */
  async handleControl(
    request: ControlRequest,
    emit?: (event: unknown) => void,
  ): Promise<ControlResult | undefined> {
    switch (request.subtype) {
      case 'sdk_command':
        if (!this.runtime.handleSdkCommand || !emit)
          throw new Error('provider does not support SDK commands');
        await this.runtime.handleSdkCommand(request.command, emit);
        return undefined;

      case 'initialize':
        this.hooks = isRecord(request.hooks) ? request.hooks : {};
        this.mcpServers = Array.isArray(request.sdk_mcp_servers)
          ? (request.sdk_mcp_servers as McpServer[])
          : [];
        return undefined;

      case 'interrupt':
        this.runtime.interrupt?.();
        return undefined;

      case 'set_permission_mode':
        // A missing, non-string, OR empty value falls back to "default".
        this.permissionMode =
          typeof request.permission_mode === 'string' && request.permission_mode
            ? request.permission_mode
            : 'default';
        this.runtime.setPermissionMode?.(this.permissionMode);
        return undefined;

      case 'set_model': {
        // Adopt a non-empty string, otherwise keep the current model.
        const nextModel =
          typeof request.model === 'string' && request.model ? request.model : this.model;
        // The runtime's setModel takes a concrete string; skip the call when we
        // have no model (null defers to the provider's own default).
        if (nextModel != null) this.runtime.setModel?.(nextModel);
        // Commit Session-visible state only after the runtime accepts the switch.
        this.model = nextModel;
        return undefined;
      }

      case 'list_harnesses':
        return { harnesses: await this.listHarnesses() };

      default:
        throw new Error(`unsupported control request subtype: ${request.subtype ?? ''}`);
    }
  }

  handleCustomToolResult(message: CustomToolResultMessage): boolean {
    return (
      this.runtime.handleCustomToolResult?.({
        ...message,
        custom_tool_use_id:
          typeof message.custom_tool_use_id === 'string'
            ? message.custom_tool_use_id
            : typeof message.tool_use_id === 'string'
              ? message.tool_use_id
              : '',
      }) ?? false
    );
  }

  /** Public harness catalog for `list_harnesses` (one entry per distinct provider). */
  private async listHarnesses(): Promise<ProviderMetadata[]> {
    return listProviderMetadata();
  }

  /**
   * Drive ONE user turn. Ordering is load-bearing and preserved exactly:
   *   1. yield systemInit FIRST;
   *   2. stream every provider frame in arrival order;
   *   3. if the provider never yielded a `result`, yield ONE synthesized success
   *      result; if the provider threw, yield ONE is_error result and return
   *      (no success result, no double result).
   *
   * History threading is preserved too: push the user message before the turn,
   * pass `history.slice(0, -1)` (prior context, excluding this user message) to
   * the runtime, and push the assistant message after the turn iff any assistant
   * text accumulated.
   */
  async *runTurn(input: SessionTurnInput): AsyncGenerator<Frame, void, void> {
    const content: MessageContent = input.content;
    let prompt = input.prompt;

    this.turns += 1;
    const startedAt = Date.now();

    // Prepend the resume preamble to the FIRST real prompt only, then drop it so
    // it never replays twice. The provider sees one rehydrated prompt and knows
    // nothing about resume.
    if (this.preamble !== undefined) {
      prompt = prompt ? `${this.preamble}\n\n${prompt}` : this.preamble;
      this.preamble = undefined;
    }

    this.history.push({ role: 'user', text: prompt });

    yield systemInit({
      sessionId: this.sessionId,
      model: this.runtime.model,
      mcpServers: this.mcpServers,
    });

    let sawResult = false;
    let text = '';

    try {
      const stream = this.runtime.runTurn({
        prompt,
        content: toRuntimeContent(content),
        session: {
          sessionId: this.sessionId,
          turns: this.turns,
          startedAt,
          history: this.history.slice(0, -1),
          mcpServers: this.mcpServers,
        },
      });

      // Provider WireFrames are structurally assignable into protocol's `Frame`
      // (by design — see protocol.ts), so they forward without a cast.
      for await (const frame of stream) {
        if (frame.type === 'result') sawResult = true;
        if (frame.type === 'assistant') text += assistantText(frame.message.content);
        yield frame;
      }
    } catch (err) {
      // Runtime cleanup may throw after it has already yielded a terminal
      // result. Keep that terminal authoritative; synthesize only if absent.
      const message = err instanceof Error ? err.message : String(err);
      if (sawResult) {
        this.diagnostics(`runtime error after terminal result: ${message}\n`);
      } else {
        const errorResult: ResultFrame = resultFrame({
          sessionId: this.sessionId,
          turns: this.turns,
          startedAt,
          text: message,
          isError: true,
        });
        sawResult = true;
        yield errorResult;
      }
    }

    if (text) this.history.push({ role: 'assistant', text });
    if (!sawResult) {
      const successResult: ResultFrame = resultFrame({
        sessionId: this.sessionId,
        turns: this.turns,
        startedAt,
        text,
      });
      yield successResult;
    }
  }
}

// ---------------------------------------------------------------------------
// Content boundary.
//
// `SessionTurnInput.content` (protocol's `MessageContent`) and the runtime's
// `RunTurnArgs.content` (providers' `TurnContent`) are near-identical unions
// that differ only in a content block's `type` optionality. The runtime treats
// `content` opaquely (the prompt text is already flattened into `prompt`), so
// this single, documented widening is the ONLY place the two content
// vocabularies bridge.
// ---------------------------------------------------------------------------
function toRuntimeContent(content: MessageContent): TurnContent {
  return content as TurnContent;
}

/** Narrow `unknown` to a plain object for reading optional control-request fields. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object';
}
