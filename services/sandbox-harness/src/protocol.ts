// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// ---------------------------------------------------------------------------
// The SUBPROCESS-side NDJSON wire.
//
// The harness spawns this server as a child process and speaks the Claude Agent
// SDK stream-json control protocol to it — the exact wire language the official
// `claude` CLI uses in `--input-format stream-json --output-format stream-json`
// mode. This module owns:
//   - parseLaunchArgs:  mirror the claude CLI launch flags
//   - contentToText:    flatten a message `content` into plain text
//   - frame builders:   the ONE place the canonical wire shapes live
//   - StreamJsonServer: read NDJSON from stdin, demux on `type`, correlate
//                       control_request -> control_response, and stream a user
//                       turn's frames to stdout AS THEY ARRIVE.
//
// Frame shapes here are the contract with the session-manager's `translateFrame`
// and MUST match byte-for-byte. The field set and JSON key spellings below are
// load-bearing; do not reorder or rename them without updating the consumer.
// ---------------------------------------------------------------------------

import { createInterface } from 'node:readline';
import type { Readable, Writable } from 'node:stream';

// ---------------------------------------------------------------------------
// Content blocks & helpers.
// ---------------------------------------------------------------------------

/** A single content block. Only `text` is interpreted here; other shapes pass through opaquely. */
export interface ContentBlock {
  type?: string;
  text?: string;
  [key: string]: unknown;
}

/** Message `content` is either a plain string or an array of content blocks. */
export type MessageContent = string | ContentBlock[];

/**
 * Flatten a message `content` into plain text: a string passes through; an array
 * joins non-empty text blocks with newlines (non-text / malformed blocks
 * contribute nothing); anything else collapses to "".
 */
export function contentToText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content
    .filter(
      (block) =>
        block !== null &&
        typeof block === 'object' &&
        (block as ContentBlock).type === 'text' &&
        typeof (block as ContentBlock).text === 'string' &&
        (block as ContentBlock).text!.length > 0,
    )
    .map((block) => (block as ContentBlock).text!)
    .join('\n');
}

// ---------------------------------------------------------------------------
// Launch flags. The harness spawns us mirroring the claude CLI:
//   --input-format stream-json --output-format stream-json --verbose
//   [--agent <a>] [--model <m>] [--permission-mode <p>] [--cwd <dir>]
// Unknown flags are tolerated and ignored (forward-compatible with the CLI).
// `--input-format` / `--output-format`, when present, MUST equal "stream-json";
// any other value throws so the subprocess can exit non-zero (code 2).
// ---------------------------------------------------------------------------

/** Resolved launch options after parsing argv (every field is concrete). */
export interface LaunchOptions {
  agent: string;
  model: string | null;
  permissionMode: string;
  cwd: string;
  verbose: boolean;
}

/** Per-field overrides for values absent from argv. */
export interface LaunchDefaults {
  agent?: string;
  model?: string | null;
  permissionMode?: string;
  cwd?: string;
}

export function parseLaunchArgs(
  argv: readonly string[],
  defaults: LaunchDefaults = {},
): LaunchOptions {
  const options: LaunchOptions = {
    agent: defaults.agent ?? 'claude',
    model: defaults.model ?? null,
    permissionMode: defaults.permissionMode ?? 'default',
    cwd: defaults.cwd ?? process.cwd(),
    verbose: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === undefined) continue;
    if (arg === '--verbose') {
      options.verbose = true;
      continue;
    }
    if (!arg.startsWith('--')) continue;

    // Flags below take a value; a missing/flag-looking next token means "no value".
    const value = argv[i + 1];
    if (value === undefined || value.startsWith('--')) continue;
    i += 1;

    switch (arg) {
      case '--input-format':
        if (value !== 'stream-json') throw new Error(`unsupported input format: ${value}`);
        break;
      case '--output-format':
        if (value !== 'stream-json') throw new Error(`unsupported output format: ${value}`);
        break;
      case '--agent':
        options.agent = value;
        break;
      case '--model':
        options.model = value;
        break;
      case '--permission-mode':
        options.permissionMode = value;
        break;
      case '--cwd':
        options.cwd = value;
        break;
      default:
        // Unknown flag: tolerate and ignore (we already consumed its value above).
        break;
    }
  }

  return options;
}

// ---------------------------------------------------------------------------
// Canonical frame shapes — the wire contract with session-manager.translateFrame.
// These MUST serialize byte-for-byte as the consumer expects.
// ---------------------------------------------------------------------------

/** Per-turn usage counters; opaque to the wire (forwarded as-is). */
export type Usage = Record<string, unknown>;

/**
 * The `model` carried on `system`/`assistant` frames. Optional AND nullable in
 * value: the provider may omit it, and Session forwards the runtime's `model`
 * which is itself `string | undefined`. Kept loose so a `WireFrame` from any
 * provider is structurally assignable into {@link Frame}.
 */
export type FrameModel = string | undefined;

/** `system`/`init` line — the first frame of every turn. */
export interface SystemInitFrame {
  type: 'system';
  subtype: 'init';
  session_id: string;
  model?: FrameModel;
  tools: unknown[];
  mcp_servers: unknown[];
}

/** Internal turn barrier. Written only after the server releases its turn lock. */
export interface SystemTurnCompleteFrame {
  type: 'system';
  subtype: 'turn_complete';
  session_id: string;
  turn_id?: string;
}

/**
 * `assistant` line — one model message; content blocks pass through unchanged.
 * `content` is optional on the type so any provider `WireMessage` (whose content
 * is optional) is assignable into {@link Frame}; the {@link assistantFrame} builder
 * always emits it, so the on-wire shape is unaffected.
 */
export interface AssistantFrame {
  type: 'assistant';
  message: { model?: FrameModel; content?: MessageContent };
  parent_tool_use_id: string | null;
}

/** `user` line — a tool-result echo threaded back into the transcript. */
export interface UserFrame {
  type: 'user';
  message: unknown;
}

/** `custom_tool_use` line — provider asks the client application to run a custom tool. */
export interface CustomToolUseFrame {
  type: 'custom_tool_use';
  id: string;
  name: string;
  input: Record<string, unknown>;
}

/** `stream_event` line — a partial streaming delta (only with include_partial_messages). */
export interface StreamEventFrame {
  type: 'stream_event';
  session_id: string;
  event: unknown;
}

/** `result` subtypes. `success` on normal completion; the others on failure. */
export type ResultSubtype = 'success' | 'error_max_turns' | 'error_during_execution';

/** `result` line — TERMINATES the turn. */
export interface ResultFrame {
  type: 'result';
  // `string`, not the narrowed `ResultSubtype`, so a provider that forwards an
  // SDK-supplied subtype string flows in without a cast. Builders here still only
  // ever emit a {@link ResultSubtype} value.
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

/**
 * Frames produced during a turn (by Session and the provider runtime). This is
 * the union `session.ts` imports as `Frame`: the runtime yields these and
 * Session re-yields them, so every provider `WireFrame` must be assignable here.
 */
export type Frame =
  | SystemInitFrame
  | AssistantFrame
  | UserFrame
  | CustomToolUseFrame
  | StreamEventFrame
  | ResultFrame;

/** `control_response` line — replies to an SDK control_request, matched by request_id. */
export interface ControlResponseSuccessFrame {
  type: 'control_response';
  response: { request_id: string; subtype: 'success' } & Record<string, unknown>;
}
export interface ControlResponseErrorFrame {
  type: 'control_response';
  response: { request_id: string; subtype: 'error'; error: string };
}
export type ControlResponseFrame = ControlResponseSuccessFrame | ControlResponseErrorFrame;

/** Any frame written to stdout: a public turn frame, control reply, or internal turn barrier. */
export type SdkEventFrame = { type: 'sdk_event'; event: unknown };
export type ServerFrame = Frame | ControlResponseFrame | SystemTurnCompleteFrame | SdkEventFrame;

// ---------------------------------------------------------------------------
// Canonical frame builders — the one place the wire shapes are constructed.
// ---------------------------------------------------------------------------

/**
 * Build a `control_response`. When `error` is present (even ""/null/undefined as
 * a key was passed via the error branch), emit the error subtype with a stringified
 * message; otherwise emit success and spread any extra fields into the response.
 */
export interface ControlSuccess {
  error?: undefined;
  [key: string]: unknown;
}
export interface ControlError {
  error: unknown;
}
export type ControlResult = ControlSuccess | ControlError;

export function controlResponse(
  requestId: string,
  result: ControlResult = {},
): ControlResponseFrame {
  if ('error' in result && result.error !== undefined) {
    return {
      type: 'control_response',
      response: { request_id: requestId, subtype: 'error', error: String(result.error) },
    };
  }
  // Success: drop the (absent/undefined) `error` key and spread the rest.
  const { error: _ignored, ...fields } = result as ControlSuccess;
  void _ignored;
  return {
    type: 'control_response',
    response: { request_id: requestId, subtype: 'success', ...fields },
  };
}

export interface SystemInitArgs {
  sessionId: string;
  /** Active model id; `string | undefined` to mirror the runtime's own `model` getter. */
  model: FrameModel;
  mcpServers?: unknown[];
  tools?: unknown[];
}

export function systemInit({
  sessionId,
  model,
  mcpServers = [],
  tools = [],
}: SystemInitArgs): SystemInitFrame {
  return {
    type: 'system',
    subtype: 'init',
    session_id: sessionId,
    model,
    tools,
    mcp_servers: mcpServers,
  };
}

/** Build the internal marker emitted after a turn has fully unwound. */
export function turnComplete(sessionId: string, turnId?: string): SystemTurnCompleteFrame {
  return {
    type: 'system',
    subtype: 'turn_complete',
    session_id: sessionId,
    ...(turnId !== undefined ? { turn_id: turnId } : {}),
  };
}

export interface AssistantFrameArgs {
  model: FrameModel;
  content: MessageContent;
  parentToolUseId?: string | null;
}

export function assistantFrame({
  model,
  content,
  parentToolUseId = null,
}: AssistantFrameArgs): AssistantFrame {
  return { type: 'assistant', message: { model, content }, parent_tool_use_id: parentToolUseId };
}

export interface CustomToolUseFrameArgs {
  id: string;
  name: string;
  input?: Record<string, unknown>;
}

export function customToolUseFrame({
  id,
  name,
  input = {},
}: CustomToolUseFrameArgs): CustomToolUseFrame {
  return { type: 'custom_tool_use', id, name, input };
}

export interface StreamEventFrameArgs {
  sessionId: string;
  event: unknown;
}

export function streamEventFrame({ sessionId, event }: StreamEventFrameArgs): StreamEventFrame {
  return { type: 'stream_event', session_id: sessionId, event };
}

export interface ResultFrameArgs {
  sessionId: string;
  turns?: number;
  startedAt?: number;
  text?: string;
  subtype?: ResultSubtype;
  isError?: boolean;
  usage?: Usage;
  totalCostUsd?: number;
}

export function resultFrame({
  sessionId,
  turns = 1,
  startedAt,
  text = '',
  subtype,
  isError = false,
  usage = {},
  totalCostUsd = 0,
}: ResultFrameArgs): ResultFrame {
  // Truthy (not just `!== undefined`): a falsy `startedAt` — absent OR the
  // degenerate epoch `0` — means "no start time", so duration is 0.
  const duration = startedAt ? Math.max(0, Date.now() - startedAt) : 0;
  return {
    type: 'result',
    subtype: subtype ?? (isError ? 'error_during_execution' : 'success'),
    session_id: sessionId,
    duration_ms: duration,
    duration_api_ms: duration,
    is_error: isError,
    num_turns: turns,
    total_cost_usd: totalCostUsd,
    usage,
    result: text,
  };
}

// ---------------------------------------------------------------------------
// Incoming line shapes (stdin). The receiver demuxes on the top-level `type`.
// Only the fields the wire reads are typed; everything else is opaque.
// ---------------------------------------------------------------------------

/** `control_request` — an SDK control message correlated by `request_id`. */
export interface ControlRequestMessage {
  type: 'control_request';
  request_id: string;
  request?: ControlRequest;
}

/** The inner control request payload; `subtype` selects the handler in Session. */
export interface ControlRequest {
  subtype?: string;
  [key: string]: unknown;
}

/** `user` — starts a turn. The open process IS the session; no method call. */
export interface UserMessage {
  type: 'user';
  message?: { role?: string; content?: MessageContent };
  session_id?: string | null;
  parent_tool_use_id?: string | null;
  /** Parent-assigned correlation id for the private turn-complete barrier. */
  turn_id?: string;
}

/** `user.custom_tool_result` — resolves a pending provider-side custom tool call. */
export interface CustomToolResultMessage {
  type: 'user.custom_tool_result';
  custom_tool_use_id?: string;
  tool_use_id?: string;
  content?: unknown;
  result?: unknown;
  is_error?: boolean;
  [key: string]: unknown;
}

// ---------------------------------------------------------------------------
// Session contract. The wire depends only on this structural surface; the
// concrete Session lives in a sibling module and is injected at construction
// (keeps protocol.ts free of provider/runtime imports). A control request is
// handled by `handleControl`; a user turn is driven by iterating `runTurn`,
// whose yielded frames are written to stdout AS THEY ARRIVE.
// ---------------------------------------------------------------------------

export interface SessionTurnInput {
  prompt: string;
  content: MessageContent;
}

export interface Session {
  /** Stable per-process session id, stamped into every frame. */
  readonly sessionId: string;
  /** Monotonic turn counter (used to stamp `num_turns` on synthesized results). */
  readonly turns: number;
  /**
   * Handle a control_request. Return `undefined` for a bare success, or an object
   * whose fields are spread into the `control_response`. Throwing yields an error
   * response carrying the message.
   */
  handleControl(
    request: ControlRequest,
    emit?: (event: unknown) => void,
  ): Promise<ControlResult | undefined> | ControlResult | undefined;
  /** Drive one user turn, yielding canonical {@link Frame}s in arrival order. */
  runTurn(input: SessionTurnInput): AsyncIterable<Frame>;
  /** Resolve a pending custom tool call without starting a new user turn. */
  handleCustomToolResult?(
    message: CustomToolResultMessage,
  ): Promise<void> | void | Promise<boolean> | boolean;
}

// ---------------------------------------------------------------------------
// The wire. Reads NDJSON from stdin, demuxes on `type`, correlates control
// requests, and streams a turn's frames to stdout AS THEY ARRIVE.
//
// Invariants:
//   - malformed JSON line  -> warn to stderr and skip (never crash)
//   - control_request      -> handleControlRequest (always replies, even on throw)
//   - user message         -> startTurn
//   - ONE active turn at a time: a 2nd `user` while a turn is in flight writes an
//     is_error result "A turn is already in progress" and is otherwise dropped.
//   - every frame is written line-delimited (`\n`) and flushed per line.
// ---------------------------------------------------------------------------

export interface StreamJsonServerOptions {
  session: Session;
  stdin?: Readable;
  stdout?: Writable;
  stderr?: Writable;
}

export class StreamJsonServer {
  private readonly session: Session;
  private readonly stdin: Readable;
  private readonly stdout: Writable;
  private readonly stderr: Writable;
  /** The in-flight turn's settle promise, or null when idle. Enforces one-at-a-time. */
  private activeTurn: Promise<void> | null = null;

  constructor({
    session,
    stdin = process.stdin,
    stdout = process.stdout,
    stderr = process.stderr,
  }: StreamJsonServerOptions) {
    this.session = session;
    this.stdin = stdin;
    this.stdout = stdout;
    this.stderr = stderr;
  }

  /** Begin consuming stdin line-by-line. Returns `this` for chaining. */
  start(): this {
    const rl = createInterface({ input: this.stdin });
    rl.on('line', (line) => {
      void this.handleLine(line);
    });
    return this;
  }

  /** Serialize one frame as a single NDJSON line and flush it to stdout. */
  private write(frame: ServerFrame): void {
    this.stdout.write(`${JSON.stringify(frame)}\n`);
  }

  private async handleLine(line: string): Promise<void> {
    const trimmed = line.trim();
    if (!trimmed) return;

    let msg: unknown;
    try {
      msg = JSON.parse(trimmed);
    } catch {
      this.stderr.write('Ignoring malformed JSON line\n');
      return;
    }
    if (msg === null || typeof msg !== 'object') return;

    const type = (msg as { type?: unknown }).type;
    if (type === 'control_request') {
      await this.handleControlRequest(msg as ControlRequestMessage);
      return;
    }
    if (type === 'user') {
      this.startTurn(msg as UserMessage);
      return;
    }
    if (type === 'user.custom_tool_result') {
      const handler = this.session.handleCustomToolResult;
      const applied = handler
        ? await handler.call(this.session, msg as CustomToolResultMessage)
        : false;
      if (applied === false) {
        this.reportCustomToolResultMismatch(msg as CustomToolResultMessage);
      }
      return;
    }
    // Unknown top-level types are ignored (forward-compatible).
  }

  private reportCustomToolResultMismatch(msg: CustomToolResultMessage): void {
    const id =
      typeof msg.custom_tool_use_id === 'string'
        ? msg.custom_tool_use_id
        : typeof msg.tool_use_id === 'string'
          ? msg.tool_use_id
          : null;
    const suffix = id ? ` custom_tool_use_id=${id}` : '';
    const text = `No pending custom tool use matches user.custom_tool_result${suffix}`;
    this.stderr.write(`${text}\n`);
  }

  private async handleControlRequest(msg: ControlRequestMessage): Promise<void> {
    const requestId = msg.request_id;
    const request: ControlRequest =
      msg.request !== null && typeof msg.request === 'object' ? msg.request : {};
    try {
      const result = await this.session.handleControl(request, (event) =>
        this.write({ type: 'sdk_event', event }),
      );
      this.write(
        controlResponse(requestId, result !== null && typeof result === 'object' ? result : {}),
      );
    } catch (err) {
      this.write(controlResponse(requestId, { error: err instanceof Error ? err.message : err }));
    }
  }

  private startTurn(msg: UserMessage): void {
    const turnId =
      typeof msg.turn_id === 'string' && msg.turn_id.length > 0 ? msg.turn_id : undefined;

    if (this.activeTurn) {
      this.write(
        resultFrame({
          sessionId: this.session.sessionId,
          turns: this.session.turns,
          text: 'A turn is already in progress',
          isError: true,
        }),
      );
      this.write(turnComplete(this.session.sessionId, turnId));
      return;
    }

    const content: MessageContent =
      msg.message !== null && typeof msg.message === 'object' ? (msg.message.content ?? '') : '';

    this.activeTurn = this.runTurn(content)
      .catch((err: unknown) => {
        this.write(
          resultFrame({
            sessionId: this.session.sessionId,
            turns: this.session.turns,
            text: err instanceof Error ? err.message : String(err),
            isError: true,
          }),
        );
      })
      .finally(() => {
        this.activeTurn = null;
        // Public terminal result can precede generator cleanup. Tell the parent
        // only after this server can accept another user turn.
        this.write(turnComplete(this.session.sessionId, turnId));
      });
  }

  private async runTurn(content: MessageContent): Promise<void> {
    for await (const frame of this.session.runTurn({ prompt: contentToText(content), content })) {
      this.write(frame);
    }
  }
}
