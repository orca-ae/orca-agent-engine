// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// session-manager.ts — harness resolution, NDJSON frame translation, and the
// per-session subprocess wrapper. Pure Node (node:child_process +
// node:readline), no external deps.
//
// Three responsibilities, in increasing order of statefulness:
//
//   resolveHarness(agent, model?, replay?) -> SpawnArgs
//     Validate the requested harness and produce the launch descriptor the
//     subprocess needs: claude-CLI-style flags plus, when `replay` is present,
//     the base64-JSON resume preamble carried in `replayEnv`. A FRESH subprocess
//     reads that env (SANDBOX_HARNESS_REPLAY) and injects it as a provider-
//     agnostic preamble first user message, rehydrating prior context. The
//     manager only forwards the bytes; the subprocess decides how to replay them.
//
//   translateFrame(frame) -> BareEvent[]
//     Map ONE raw NDJSON wire frame from the subprocess into ZERO OR MORE bare
//     managed-agents events. Never throws on unknown/garbage frames — returns [].
//
//   createManagedSession({ sessionId, spawnArgs, subprocessEntryPath, env, emit })
//     Owns ONE subprocess for ONE session: spawns `node <entry> ...flags`, sends
//     the initialize control_request, runs a long-lived stdout reader that
//     translates frames and calls emit(event), and serializes whole TURNS with a
//     turn lock so a second sendUserMessage never writes mid-turn. Public
//     terminal results are translated immediately; internal `turn_complete`
//     releases the lock after child cleanup finishes.

import { randomUUID } from 'node:crypto';
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { createInterface } from 'node:readline';

import {
  HttpError,
  agentMessageEvent,
  agentThinkingEvent,
  agentToolUseEvent,
  agentToolResultEvent,
  agentCustomToolUseEvent,
  agentUsageEvent,
  sessionIdleEvent,
  sessionErrorEvent,
  type BareEvent,
  type ContentBlock,
  type AgentMessageEvent,
  type AgentToolUseEvent,
  type AgentToolResultEvent,
  type AgentCustomToolUseEvent,
  type AgentUsageEvent,
  type SessionIdleEvent,
  type SessionErrorEvent,
} from './core.js';
import type {
  CustomToolDefinition,
  ModelEffort,
  ModelSpeed,
  RuntimeAgentDefinitions,
} from './providers/index.js';

// Re-export the bare-event surface so consumers that work with the manager can
// import its event vocabulary from one place. The canonical definitions live in
// `core.ts`; this is the same union `translateFrame` produces and `emit` sinks.
export type {
  BareEvent,
  AgentMessageEvent,
  AgentToolUseEvent,
  AgentToolResultEvent,
  AgentCustomToolUseEvent,
  AgentUsageEvent,
  SessionIdleEvent,
  SessionErrorEvent,
};

// ── wire frame shapes ─────────────────────────────────────────────────────────

/**
 * A raw NDJSON frame emitted by the subprocess (the contract with protocol.ts).
 * Only `type` is strictly required; every other field is read defensively
 * because the translator must survive partial or malformed frames.
 */
export interface WireFrame {
  type?: string;
  message?: { content?: unknown; usage?: unknown; model?: unknown; id?: unknown } | null;
  is_error?: boolean;
  result?: unknown;
  usage?: unknown;
  total_cost_usd?: unknown;
  [key: string]: unknown;
}

/** One content block inside an `assistant` / `user` frame's `message.content`. */
interface FrameContentBlock {
  type?: string;
  text?: unknown;
  name?: unknown;
  input?: unknown;
  id?: unknown;
  tool_use_id?: unknown;
  content?: unknown;
  is_error?: unknown;
  [key: string]: unknown;
}

// ── harness resolution ───────────────────────────────────────────────────────

/**
 * One entry of prior conversation history supplied via the create call's
 * `replay`. Provider-agnostic: either `parts` (structured content blocks) or
 * `text` (a plain-string convenience) may be set. The subprocess flattens both
 * into a single preamble first user message.
 */
export interface ReplayEntry {
  role: string;
  parts?: unknown;
  text?: string;
}

/**
 * Env var the subprocess reads to rehydrate prior context. Carries a
 * base64-encoded JSON array of {@link ReplayEntry}. base64 is used because the
 * payload is free-form conversation text (newlines, quotes, unicode) that must
 * survive transport as a single environment-variable string. This name MUST stay
 * in sync with the subprocess decoder (`subprocess-entry.ts`).
 */
export const REPLAY_ENV_VAR = 'SANDBOX_HARNESS_REPLAY';
export const AGENTS_ENV_VAR = 'SANDBOX_HARNESS_AGENTS';
export const FORWARD_SUBAGENT_TEXT_ENV_VAR = 'SANDBOX_HARNESS_FORWARD_SUBAGENT_TEXT';
export const CUSTOM_TOOLS_ENV_VAR = 'SANDBOX_HARNESS_CUSTOM_TOOLS';
export const SYSTEM_PROMPT_ENV_VAR = 'SANDBOX_HARNESS_SYSTEM_PROMPT';
export const TOOLS_ENV_VAR = 'SANDBOX_HARNESS_TOOLS';
export const ALLOWED_TOOLS_ENV_VAR = 'SANDBOX_HARNESS_ALLOWED_TOOLS';
export const RUNTIME_TOOLS_ENV_VAR = 'SANDBOX_HARNESS_RUNTIME_TOOLS';
export const MODEL_SPEED_ENV_VAR = 'SANDBOX_HARNESS_MODEL_SPEED';
export const MODEL_EFFORT_ENV_VAR = 'SANDBOX_HARNESS_MODEL_EFFORT';

/**
 * The launch descriptor produced by {@link resolveHarness} and consumed by
 * {@link createManagedSession}. Threaded verbatim through the server's
 * `spawnManagedSession`, so it is the single hand-off between resolution and
 * spawning.
 */
export interface SpawnArgs {
  /** The validated agent id (echoed for the session record). */
  agent: string;
  /**
   * claude-CLI-style flags for the subprocess:
   * `--input-format stream-json --output-format stream-json --agent <a> [--model <m>]`.
   */
  args: string[];
  /**
   * Extra env to merge into the subprocess. Holds the base64-JSON resume
   * preamble under {@link REPLAY_ENV_VAR} when `replay` was supplied; otherwise
   * empty. Kept out of `args` (the model flags) and separate from `process.env`
   * so the caller stays in control of what the child inherits.
   */
  replayEnv: Record<string, string>;
}

/**
 * Validate the requested harness and produce its launch descriptor.
 *
 * No allowlist check: provider auto-discovery inside the subprocess is the
 * source of truth. An invalid agent makes the subprocess exit non-zero with
 * "unsupported agent: ...", which surfaces here as `session.status_error`.
 *
 * When `replay` is present it is encoded into `replayEnv` (NOT into the model
 * flags) so a fresh process can inject it as a preamble first user message. The
 * encoding is provider-agnostic: the manager forwards bytes, the subprocess
 * decides how to replay them.
 */
export function resolveHarness(
  agent: string,
  model?: string,
  replay?: ReplayEntry[],
  agents?: RuntimeAgentDefinitions,
  forwardSubagentText?: boolean,
  customTools?: CustomToolDefinition[],
  systemPrompt?: string,
  tools?: string[],
  allowedTools?: string[],
  runtimeTools?: string[],
  modelSpeed?: ModelSpeed,
  modelEffort?: ModelEffort,
): SpawnArgs {
  if (!agent || typeof agent !== 'string') {
    throw new HttpError(400, 'agent must be a non-empty string');
  }

  const args = [
    '--input-format',
    'stream-json',
    '--output-format',
    'stream-json',
    '--agent',
    agent,
  ];
  if (typeof model === 'string' && model.length > 0) {
    args.push('--model', model);
  }

  return {
    agent,
    args,
    replayEnv: {
      ...encodeReplayEnv(replay),
      ...encodeAgentOptionsEnv(agents, forwardSubagentText),
      ...encodeCustomToolsEnv(customTools),
      ...encodeSdkOptionsEnv(
        systemPrompt,
        tools,
        allowedTools,
        runtimeTools,
        modelSpeed,
        modelEffort,
      ),
    },
  };
}

/**
 * Serialize replay history to a `{ [REPLAY_ENV_VAR]: base64(json) }` map, or an
 * empty map when there is nothing to replay. Exported so the wire encoding has
 * exactly one definition shared by `resolveHarness` and tests.
 */
export function encodeReplayEnv(replay?: ReplayEntry[]): Record<string, string> {
  if (!Array.isArray(replay) || replay.length === 0) return {};
  const json = JSON.stringify(replay);
  return { [REPLAY_ENV_VAR]: Buffer.from(json, 'utf8').toString('base64') };
}

/**
 * Inverse of {@link encodeReplayEnv}: decode a base64-JSON replay value back
 * into entries (e.g. for the subprocess decoder or a test). Returns `[]` on
 * anything malformed — resume is best-effort and must never throw here.
 */
export function decodeReplayEnv(value: string | undefined): ReplayEntry[] {
  if (!value) return [];
  try {
    const parsed: unknown = JSON.parse(Buffer.from(value, 'base64').toString('utf8'));
    return Array.isArray(parsed) ? (parsed as ReplayEntry[]) : [];
  } catch {
    return [];
  }
}

export function encodeAgentOptionsEnv(
  agents?: RuntimeAgentDefinitions,
  forwardSubagentText?: boolean,
): Record<string, string> {
  const env: Record<string, string> = {};
  if (agents && Object.keys(agents).length > 0) {
    env[AGENTS_ENV_VAR] = Buffer.from(JSON.stringify(agents), 'utf8').toString('base64');
  }
  if (forwardSubagentText !== undefined) {
    env[FORWARD_SUBAGENT_TEXT_ENV_VAR] = forwardSubagentText ? '1' : '0';
  }
  return env;
}

export function encodeCustomToolsEnv(customTools?: CustomToolDefinition[]): Record<string, string> {
  if (!Array.isArray(customTools) || customTools.length === 0) return {};
  return {
    [CUSTOM_TOOLS_ENV_VAR]: Buffer.from(JSON.stringify(customTools), 'utf8').toString('base64'),
  };
}

export function encodeSdkOptionsEnv(
  systemPrompt?: string,
  tools?: string[],
  allowedTools?: string[],
  runtimeTools?: string[],
  modelSpeed?: ModelSpeed,
  modelEffort?: ModelEffort,
): Record<string, string> {
  const env: Record<string, string> = {};
  if (systemPrompt !== undefined) {
    env[SYSTEM_PROMPT_ENV_VAR] = Buffer.from(systemPrompt, 'utf8').toString('base64');
  }
  if (tools !== undefined) {
    env[TOOLS_ENV_VAR] = Buffer.from(JSON.stringify(tools), 'utf8').toString('base64');
  }
  if (allowedTools !== undefined) {
    env[ALLOWED_TOOLS_ENV_VAR] = Buffer.from(JSON.stringify(allowedTools), 'utf8').toString(
      'base64',
    );
  }
  if (runtimeTools !== undefined) {
    env[RUNTIME_TOOLS_ENV_VAR] = Buffer.from(JSON.stringify(runtimeTools), 'utf8').toString(
      'base64',
    );
  }
  if (modelSpeed !== undefined) env[MODEL_SPEED_ENV_VAR] = modelSpeed;
  if (modelEffort !== undefined) env[MODEL_EFFORT_ENV_VAR] = modelEffort;
  return env;
}

export function decodeCustomToolsEnv(value: string | undefined): CustomToolDefinition[] {
  if (!value) return [];
  try {
    const parsed: unknown = JSON.parse(Buffer.from(value, 'base64').toString('utf8'));
    return Array.isArray(parsed) ? (parsed as CustomToolDefinition[]) : [];
  } catch {
    return [];
  }
}

// ── frame translation ────────────────────────────────────────────────────────

/**
 * Translate ONE raw NDJSON frame from the subprocess into ZERO OR MORE bare
 * managed-agents events. Never throws on unknown frames — returns [].
 *
 * Frame → events (must stay in lock-step with the subprocess wire):
 *   - system / control_response → []  (housekeeping, no events)
 *   - stream_event → final per-message agent.usage at message_stop
 *   - assistant → one event per content block: text → agent.message,
 *     tool_use → agent.tool_use
 *   - user → one agent.tool_result per tool_result block
 *   - result → is_error ? [session.status_error] : [session.status_idle]
 *   - anything else → []
 */
export interface FrameTranslationState {
  readonly subagentIdByType: ReadonlyMap<string, string>;
  readonly subagentIdByParentToolUseId: Map<string, string>;
  reportedAssistantUsage: boolean;
  /** Turn-local message IDs suppress early assistant snapshots and repeated blocks. */
  usageByMessage?: Map<string, PendingMessageUsage>;
  activeUsageByParent?: Map<string, PendingMessageUsage>;
}

interface PendingMessageUsage {
  usage: Record<string, unknown>;
  model?: string;
  parentToolUseId?: string;
  reported: boolean;
}

export function translateFrame(frame: unknown, state?: FrameTranslationState): BareEvent[] {
  if (!frame || typeof frame !== 'object') return [];
  const f = frame as WireFrame;

  switch (f.type) {
    case 'system':
    case 'control_response':
      return [];

    case 'stream_event':
      return translateStreamUsage(f, state);

    case 'assistant': {
      rememberSubagentDispatches(f, state);
      const events = translateAssistant(f.message?.content);
      const usage = isRecord(f.message?.usage) ? f.message.usage : null;
      const parentToolUseId =
        typeof f.parent_tool_use_id === 'string' ? f.parent_tool_use_id : undefined;
      const messageKey =
        typeof f.message?.id === 'string'
          ? JSON.stringify([parentToolUseId ?? '', f.message.id])
          : undefined;
      if (messageKey && state?.usageByMessage?.has(messageKey)) return events;
      if (usage && hasProviderUsage(usage)) {
        if (state) state.reportedAssistantUsage = true;
        if (state && messageKey) {
          (state.usageByMessage ??= new Map()).set(messageKey, { usage, reported: true });
        }
        events.push(
          agentUsageEvent({
            usage,
            ...(typeof f.message?.model === 'string' ? { model: f.message.model } : {}),
            ...(parentToolUseId && state?.subagentIdByParentToolUseId.get(parentToolUseId)
              ? { subagent_id: state.subagentIdByParentToolUseId.get(parentToolUseId)! }
              : {}),
          }),
        );
      }
      return events;
    }

    case 'user':
      return translateUser(f.message?.content);

    case 'custom_tool_use':
      return [
        agentCustomToolUseEvent({
          id: typeof f.id === 'string' ? f.id : '',
          name: typeof f.name === 'string' ? f.name : '',
          input: isRecord(f.input) ? f.input : null,
        }),
      ];

    case 'result': {
      const events: BareEvent[] = [];
      // Preserve usage already observed when an interrupted stream has no message_stop.
      for (const pending of state?.activeUsageByParent?.values() ?? []) {
        events.push(...reportMessageUsage(pending, state!));
      }
      events.push(...translateResult(f, state?.reportedAssistantUsage === true));
      if (state) {
        state.reportedAssistantUsage = false;
        state.subagentIdByParentToolUseId.clear();
        state.usageByMessage?.clear();
        state.activeUsageByParent?.clear();
      }
      return events;
    }

    default:
      return [];
  }
}

function translateStreamUsage(f: WireFrame, state?: FrameTranslationState): BareEvent[] {
  if (!state || !isRecord(f.event)) return [];
  const event = f.event;
  const parentToolUseId =
    typeof f.parent_tool_use_id === 'string' ? f.parent_tool_use_id : undefined;
  const scope = parentToolUseId ?? '';
  if (event.type === 'message_start' && isRecord(event.message)) {
    const message = event.message;
    if (typeof message.id !== 'string') return [];
    const key = JSON.stringify([scope, message.id]);
    const messages = (state.usageByMessage ??= new Map());
    let pending = messages.get(key);
    if (!pending) {
      pending = {
        usage: isRecord(message.usage) ? mergeUsage({}, message.usage) : {},
        ...(typeof message.model === 'string' ? { model: message.model } : {}),
        ...(parentToolUseId ? { parentToolUseId } : {}),
        reported: false,
      };
      messages.set(key, pending);
    }
    (state.activeUsageByParent ??= new Map()).set(scope, pending);
    return [];
  }
  const pending = state.activeUsageByParent?.get(scope);
  if (!pending || pending.reported) return [];
  if (event.type === 'message_delta' && isRecord(event.usage)) {
    // Message deltas are cumulative. Null/omitted fields do not erase the
    // initial input/cache counters; nested cache TTL counters follow the same rule.
    pending.usage = mergeUsage(pending.usage, event.usage);
  }
  if (event.type === 'message_stop') {
    state.activeUsageByParent!.delete(scope);
    return reportMessageUsage(pending, state);
  }
  return [];
}

function mergeUsage(
  base: Record<string, unknown>,
  delta: Record<string, unknown>,
): Record<string, unknown> {
  const merged = { ...base };
  for (const [key, value] of Object.entries(delta)) {
    if (value === null || value === undefined) continue;
    merged[key] = isRecord(value) ? mergeUsage(isRecord(base[key]) ? base[key] : {}, value) : value;
  }
  return merged;
}

function reportMessageUsage(
  pending: PendingMessageUsage,
  state: FrameTranslationState,
): BareEvent[] {
  if (
    pending.reported ||
    ![
      'input_tokens',
      'output_tokens',
      'cache_read_input_tokens',
      'cache_creation_input_tokens',
    ].some(
      (key) =>
        typeof pending.usage[key] === 'number' &&
        Number.isFinite(pending.usage[key]) &&
        (pending.usage[key] as number) >= 0,
    )
  )
    return [];
  pending.reported = true;
  state.reportedAssistantUsage = true;
  const subagentId = pending.parentToolUseId
    ? state.subagentIdByParentToolUseId.get(pending.parentToolUseId)
    : undefined;
  return [
    agentUsageEvent({
      usage: pending.usage,
      ...(pending.model ? { model: pending.model } : {}),
      ...(subagentId ? { subagent_id: subagentId } : {}),
    }),
  ];
}

/** `assistant` frame → agent.message (text) / agent.tool_use (tool_use) events. */
function translateAssistant(content: unknown): BareEvent[] {
  if (!Array.isArray(content)) return [];
  const events: BareEvent[] = [];
  for (const raw of content) {
    if (!raw || typeof raw !== 'object') continue;
    const block = raw as FrameContentBlock;
    if (block.type === 'text' && typeof block.text === 'string') {
      events.push(agentMessageEvent([{ type: 'text', text: block.text }]));
    } else if (block.type === 'thinking' || block.type === 'redacted_thinking') {
      // Thinking is surfaced as its own agent.thinking event (Claude taxonomy
      // parity), not folded into agent.message content.
      events.push(agentThinkingEvent([block as unknown as ContentBlock]));
    } else if (block.type === 'tool_use') {
      events.push(
        agentToolUseEvent({
          name: typeof block.name === 'string' ? block.name : '',
          input: isRecord(block.input) ? block.input : null,
          tool_use_id: typeof block.id === 'string' ? block.id : '',
        }),
      );
    }
  }
  return events;
}

/** `user` frame → one agent.tool_result per tool_result block. */
function translateUser(content: unknown): BareEvent[] {
  if (!Array.isArray(content)) return [];
  const events: BareEvent[] = [];
  for (const raw of content) {
    if (!raw || typeof raw !== 'object') continue;
    const block = raw as FrameContentBlock;
    if (block.type === 'tool_result') {
      events.push(
        agentToolResultEvent({
          tool_use_id: typeof block.tool_use_id === 'string' ? block.tool_use_id : '',
          content: block.content,
          is_error: Boolean(block.is_error),
        }),
      );
    }
  }
  return events;
}

/** `result` frame → terminal session.status_error / session.status_idle event. */
function translateResult(f: WireFrame, usageAlreadyReported = false): BareEvent[] {
  if (f.is_error) {
    // Any falsy result (absent, empty string, null) falls back to the generic
    // "error during execution"; a truthy non-string is passed through and
    // String()-ified by the factory.
    const message = f.result ? f.result : 'error during execution';
    return [sessionErrorEvent(message)];
  }
  return [
    sessionIdleEvent({
      usage: isRecord(f.usage) ? f.usage : {},
      total_cost_usd: typeof f.total_cost_usd === 'number' ? f.total_cost_usd : 0,
      usage_already_reported: usageAlreadyReported,
    }),
  ];
}

function rememberSubagentDispatches(f: WireFrame, state?: FrameTranslationState): void {
  if (!state || !Array.isArray(f.message?.content)) return;
  for (const raw of f.message.content) {
    if (!isRecord(raw) || raw.type !== 'tool_use' || raw.name !== 'Agent') continue;
    const toolUseId = typeof raw.id === 'string' ? raw.id : undefined;
    const subagentType =
      isRecord(raw.input) && typeof raw.input.subagent_type === 'string'
        ? raw.input.subagent_type
        : undefined;
    const managedId = subagentType ? state.subagentIdByType.get(subagentType) : undefined;
    if (toolUseId && managedId) state.subagentIdByParentToolUseId.set(toolUseId, managedId);
  }
}

function hasProviderUsage(usage: Record<string, unknown>): boolean {
  return Object.values(usage).some(
    (value) => typeof value === 'number' && Number.isFinite(value) && value > 0,
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Internal child marker: not a public event, only a parent turn-lock barrier. */
function turnCompleteId(frame: unknown): string | undefined {
  if (!frame || typeof frame !== 'object') return undefined;
  const f = frame as { type?: unknown; subtype?: unknown; session_id?: unknown; turn_id?: unknown };
  if (
    f.type === 'system' &&
    f.subtype === 'turn_complete' &&
    typeof f.session_id === 'string' &&
    f.session_id.length > 0 &&
    typeof f.turn_id === 'string' &&
    f.turn_id.length > 0
  ) {
    return f.turn_id;
  }
  return undefined;
}

// ── per-session subprocess ───────────────────────────────────────────────────

/** Content of a user message: a plain string or an array of content blocks. */
export type UserMessageContent = string | unknown[];

export interface CreateManagedSessionInput {
  /** Opaque session id; used for diagnostics / log correlation. */
  sessionId: string;
  /** Launch descriptor from {@link resolveHarness} (flags + replay env). */
  spawnArgs: SpawnArgs;
  /** Absolute path to the subprocess entry: spawned as `node <entry> ...flags`. */
  subprocessEntryPath: string;
  /**
   * Base env for the child. Defaults to `process.env`. The descriptor's
   * `replayEnv` is layered ON TOP so the resume preamble survives a
   * caller-provided env (LLM endpoint/key also live in this env).
   */
  env?: NodeJS.ProcessEnv;
  /** Sink for every translated bare event, in emission order. */
  emit: (event: BareEvent) => void;
}

/** Live handle to one managed subprocess. */
export interface ManagedSession {
  /** Spawn the subprocess and send the initialize handshake. Call once. */
  start(): void;
  /**
   * Queue a user turn. Resolves only after the child releases its turn lock
   * with `system/turn_complete`. Rejects if session is not alive or stdin write fails.
   */
  sendUserMessage(content: UserMessageContent): Promise<void>;
  /** Resolve a pending custom tool call without starting a new user turn. */
  sendCustomToolResult(payload: Record<string, unknown>): Promise<void>;
  /** Acknowledges only after the child handler completes; errors are never accepted silently. */
  sendSdkCommand?(command: unknown): Promise<number>;
  /** Terminate the subprocess. Idempotent; suppresses the crash error event. */
  kill(): void;
  /** Whether the subprocess is currently running. */
  isAlive(): boolean;
}

/**
 * Owns ONE harness subprocess for ONE session. Spawns it, sends the
 * initialize control request, runs a long-lived stdout reader that translates
 * frames and calls emit(event), and serializes user-message writes.
 */
export function createManagedSession({
  sessionId: _sessionId,
  spawnArgs,
  subprocessEntryPath,
  env,
  emit,
}: CreateManagedSessionInput): ManagedSession {
  let child: ChildProcessWithoutNullStreams | null = null;
  let alive = false;
  let deliberateKill = false;
  const translationState = decodeFrameTranslationState(spawnArgs.replayEnv[AGENTS_ENV_VAR]);

  // Per-session turn lock: public terminal results translate immediately, but
  // only matching child `system/turn_complete` releases this lock after cleanup.
  let tail: Promise<void> = Promise.resolve();
  let pendingTurn: { id: string; settle: () => void } | null = null;
  let nextTurnId = 0;
  let sdkSequence = 0;
  const controls = new Map<
    string,
    { resolve: (sequence: number) => void; reject: (error: Error) => void }
  >();
  function rejectControls(): void {
    for (const pending of controls.values()) pending.reject(new Error('session not alive'));
    controls.clear();
  }

  function settlePendingTurn(turnId?: string): void {
    if (pendingTurn && (turnId === undefined || pendingTurn.id === turnId)) {
      const { settle } = pendingTurn;
      pendingTurn = null;
      settle();
    }
  }

  // Emit translated public events. Completion marker settles separately.
  function emitEvent(ev: BareEvent): void {
    emit(ev);
  }

  function start(): void {
    // `replayEnv` is layered over the base env so the resume preamble survives a
    // caller-provided env; an empty replayEnv (no replay) is a no-op spread.
    const childEnv: NodeJS.ProcessEnv = { ...(env ?? process.env), ...spawnArgs.replayEnv };
    const proc = spawn('node', [subprocessEntryPath, ...spawnArgs.args], {
      env: childEnv,
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    child = proc;
    alive = true;

    // Initialize handshake.
    proc.stdin.write(
      `${JSON.stringify({
        type: 'control_request',
        request_id: 'req_init',
        request: { subtype: 'initialize', hooks: {}, sdk_mcp_servers: [] },
      })}\n`,
    );

    // Drain stderr, keeping the last ~1000 bytes so an unexpected exit can report why.
    let stderrTail = '';
    proc.stderr.setEncoding('utf8');
    proc.stderr.on('data', (d: string) => {
      stderrTail = (stderrTail + d).slice(-1000);
    });

    // Reader loop over child stdout (NDJSON → translate → emit).
    const rl = createInterface({ input: proc.stdout, crlfDelay: Infinity });
    rl.on('line', (line: string) => {
      const trimmed = line.trim();
      if (!trimmed) return;
      let frame: unknown;
      try {
        frame = JSON.parse(trimmed);
      } catch {
        return;
      }
      if (isRecord(frame) && frame.type === 'control_response' && isRecord(frame.response)) {
        const response = frame.response;
        const id = String(response.request_id);
        const pending = controls.get(id);
        if (pending) {
          controls.delete(id);
          if (response.subtype === 'success') pending.resolve(sdkSequence);
          else pending.reject(new Error(String(response.error ?? 'SDK command failed')));
        }
        return;
      }
      if (isRecord(frame) && frame.type === 'sdk_event') {
        emitEvent({ type: 'harness.sdk_event', event: frame.event, sequence: ++sdkSequence });
        return;
      }
      const turnId = turnCompleteId(frame);
      if (turnId !== undefined) {
        settlePendingTurn(turnId);
        return;
      }
      for (const event of translateFrame(frame, translationState)) emitEvent(event);
    });

    let errorEmitted = false;
    function onExit(code: number | null, signal: NodeJS.Signals | null): void {
      alive = false;
      rejectControls();
      rl.close();
      // Only a crash (non-zero code or a signal) is an error. A clean exit(0)
      // after a turn must not flip a freshly-idle session to "error".
      const crashed = signal != null || (code != null && code !== 0);
      if (!deliberateKill && !errorEmitted && crashed) {
        errorEmitted = true;
        const reason = signal != null ? `signal ${signal}` : `code ${code}`;
        const detail = stderrTail.trim() ? `: ${stderrTail.trim()}` : '';
        emitEvent(sessionErrorEvent(`harness exited (${reason})${detail}`));
      }
      settlePendingTurn(); // unblock the chain even on a silent/deliberate exit
    }
    proc.on('exit', onExit);
    proc.on('error', (err: Error) => {
      alive = false;
      rejectControls();
      rl.close();
      if (!deliberateKill && !errorEmitted) {
        errorEmitted = true;
        emitEvent(sessionErrorEvent(`harness error: ${err.message}`));
      }
      settlePendingTurn();
    });
  }

  function writeOnce(line: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (!child || !child.stdin.writable) {
        reject(new Error('session not alive'));
        return;
      }
      child.stdin.write(line, (err) => (err ? reject(err) : resolve()));
    });
  }

  function sendUserMessage(content: UserMessageContent): Promise<void> {
    if (!alive) return Promise.reject(new Error('session not alive'));
    const turnId = `turn_${++nextTurnId}`;
    const line = `${JSON.stringify({
      type: 'user',
      message: { role: 'user', content },
      session_id: null,
      parent_tool_use_id: null,
      turn_id: turnId,
    })}\n`;

    // Run a whole turn: write the user frame, then keep the lock until matching
    // `system/turn_complete`. Chaining `runTurn` on BOTH fulfilment and rejection means a
    // failed turn still lets the next queued message proceed instead of wedging
    // the chain.
    const runTurn = (): Promise<void> => {
      if (!alive) return Promise.reject(new Error('session not alive'));
      return new Promise<void>((resolve, reject) => {
        pendingTurn = { id: turnId, settle: resolve };
        writeOnce(line).catch((err: unknown) => {
          if (pendingTurn?.settle === resolve) pendingTurn = null;
          reject(err instanceof Error ? err : new Error(String(err)));
        });
      });
    };

    tail = tail.then(runTurn, runTurn);
    return tail;
  }

  function sendCustomToolResult(payload: Record<string, unknown>): Promise<void> {
    if (!alive) return Promise.reject(new Error('session not alive'));
    const line = `${JSON.stringify({ type: 'user.custom_tool_result', ...payload })}\n`;
    return writeOnce(line);
  }

  async function sendSdkCommand(command: unknown): Promise<number> {
    if (!alive) throw new Error('session not alive');
    const id = randomUUID();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await new Promise<number>((resolve, reject) => {
        controls.set(id, { resolve, reject });
        // Bounds a wedged child while allowing the host's ten-minute turn deadline.
        timer = setTimeout(() => reject(new Error('SDK command timed out')), 660_000);
        void writeOnce(
          `${JSON.stringify({
            type: 'control_request',
            request_id: id,
            request: { subtype: 'sdk_command', command },
          })}\n`,
        ).catch(reject);
      });
    } finally {
      clearTimeout(timer);
      controls.delete(id);
    }
  }

  function kill(): void {
    if (deliberateKill) return; // idempotent
    deliberateKill = true;
    alive = false;
    rejectControls();
    try {
      child?.stdin?.end();
    } catch {
      /* ignore */
    }
    try {
      child?.kill();
    } catch {
      /* ignore */
    }
    settlePendingTurn(); // don't leave a queued sendUserMessage hanging
  }

  function isAlive(): boolean {
    return alive;
  }

  return { start, sendUserMessage, sendCustomToolResult, sendSdkCommand, kill, isAlive };
}

function decodeFrameTranslationState(encodedAgents: string | undefined): FrameTranslationState {
  const subagentIdByType = new Map<string, string>();
  if (encodedAgents) {
    try {
      const parsed: unknown = JSON.parse(Buffer.from(encodedAgents, 'base64').toString('utf8'));
      if (isRecord(parsed)) {
        for (const [type, raw] of Object.entries(parsed)) {
          if (isRecord(raw) && typeof raw.managedAgentId === 'string') {
            subagentIdByType.set(type, raw.managedAgentId);
          }
        }
      }
    } catch {
      // The subprocess decoder treats malformed agent options as absent too.
    }
  }
  return {
    subagentIdByType,
    subagentIdByParentToolUseId: new Map(),
    reportedAssistantUsage: false,
  };
}
