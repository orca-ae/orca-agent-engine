// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The 'claude' provider.
//
// Drives `@anthropic-ai/claude-agent-sdk`'s `query()` IN-PROCESS (one model, one
// session) and maps every SDK message to the canonical stream-json wire via
// `toFrames` (the pure mapping in ./claude-transformation.ts). The Claude Agent SDK
// already emits the canonical wire — it IS the claude CLI's stream-json — so the
// runtime is a thin loop: open `query`, forward mapped frames, handle abort.
//
// The LLM endpoint/key are read from the environment only. The SDK reads
// `ANTHROPIC_BASE_URL` / `ANTHROPIC_API_KEY` itself; this module never hardcodes a
// host. An OPTIONAL gateway override (LITELLM_API_BASE / LITELLM_API_KEY — the
// gateway LLM endpoint and per-session JWT) derives those two, so the provider
// reaches the ai-gateway with no code change — a pre-set `ANTHROPIC_BASE_URL`
// always wins.

import { query } from '@anthropic-ai/claude-agent-sdk';
import type {
  AgentDefinition,
  Options,
  PermissionMode as SdkPermissionMode,
} from '@anthropic-ai/claude-agent-sdk';
import {
  CLAUDE_FAST_MODE_MODEL_IDS,
  getClaudeModelEffortCapability,
  isClaudeFastModeModel,
  isClaudeModelEffortSupported,
} from '@orca/harness-catalog';
import { randomUUID } from 'node:crypto';

import { toFrames } from './claude-transformation.js';
import {
  buildCustomToolsMcpServer,
  customToolAllowedToolNames,
  LOCAL_ORCA_READ_TOOL_NAME,
  ORCA_MCP_SERVER_NAME,
} from './custom-tools.js';
import {
  assertNoWritableAliasToSkills,
  claudeSandboxSettings,
  parseSandboxWritePolicy,
  writePolicyPermissionHandler,
} from '../write-policy.js';
import type {
  CreateRuntimeArgs,
  CustomToolResultPayload,
  Env,
  ModelSpeed,
  PermissionMode,
  Provider,
  Runtime,
  RunTurnArgs,
  WireFrame,
} from './types.js';

// ---------------------------------------------------------------------------
// Defaults:
//   - default model 'claude-sonnet-4-6' (env override wins) when none is given,
//   - default permission mode 'default'.
// ---------------------------------------------------------------------------

/** Fallback model when neither the caller nor the env supplies one. */
const DEFAULT_MODEL = 'claude-sonnet-4-6';

/** Fallback permission mode (forwarded to the SDK's `permissionMode`). */
const DEFAULT_PERMISSION_MODE: PermissionMode = 'default';

// ---------------------------------------------------------------------------
// Optional gateway override.
// ---------------------------------------------------------------------------

/**
 * If both `LITELLM_API_BASE` and `LITELLM_API_KEY` are set, route the SDK through
 * the gateway by deriving the Anthropic env it reads — UNLESS the operator already
 * pinned `ANTHROPIC_BASE_URL`, which always wins. The Anthropic SDK appends
 * `/v1/messages`, so the derived base strips a trailing slash run and a trailing
 * `/v1`. Keys default to the gateway key only when not already set to a truthy
 * value, so an explicit `ANTHROPIC_API_KEY` / `ANTHROPIC_AUTH_TOKEN` is never
 * clobbered (a blank pre-set falls through to the gateway key).
 *
 * Writes to `process.env` (not the passed-in `env`) because that is what the SDK
 * subprocess inherits. Setting the two gateway env vars is the single lever that
 * points the SDK at the ai-gateway without touching this code.
 */
function applyGatewayEnv(env: Env): void {
  const base = env.LITELLM_API_BASE;
  const key = env.LITELLM_API_KEY;
  if (!base || !key) return;

  if (!process.env.ANTHROPIC_BASE_URL) {
    process.env.ANTHROPIC_BASE_URL = base.replace(/\/+$/, '').replace(/\/v1$/, '');
  }
  // Truthy fallback (`||`, not `??`): an unset OR empty pre-set defaults to the
  // gateway key, so a blank `ANTHROPIC_API_KEY=""` never ships as the credential.
  process.env.ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || key;
  process.env.ANTHROPIC_AUTH_TOKEN = process.env.ANTHROPIC_AUTH_TOKEN || key;
}

/**
 * Resolve the initial model: explicit `model` arg, else `LITELLM_DEFAULT_MODEL`
 * from the env, else {@link DEFAULT_MODEL}. Truthy fallbacks, so an empty string
 * falls through to the next one.
 */
function resolveInitialModel(model: string | undefined, env: Env): string {
  return model || env.LITELLM_DEFAULT_MODEL || DEFAULT_MODEL;
}

function resolveSessionModelSpeed(
  primaryModel: string,
  primarySpeed: ModelSpeed | undefined,
  primaryEffort: CreateRuntimeArgs['modelEffort'],
  agents: CreateRuntimeArgs['agents'],
): ModelSpeed {
  const controls = [
    {
      label: 'primary agent',
      model: primaryModel,
      speed: modelSpeedOrStandard(primarySpeed, 'primary agent'),
      effort: primaryEffort,
    },
    ...Object.entries(agents ?? {}).map(([name, definition]) => ({
      label: `subagent ${name}`,
      model:
        definition.model === undefined || definition.model === 'inherit'
          ? primaryModel
          : definition.model,
      speed: modelSpeedOrStandard(definition.modelSpeed, `subagent ${name}`),
      effort: definition.effort,
    })),
  ];
  for (const entry of controls) {
    validateModelEffortForRuntime(entry.model, entry.effort, entry.label);
  }
  const unsupportedFast = controls.find(
    (entry) => entry.speed === 'fast' && !isClaudeFastModeModel(entry.model),
  );
  if (unsupportedFast) {
    throw new Error(
      `${unsupportedFast.label} requests fast mode for unsupported model ${unsupportedFast.model}; ` +
        `supported models are ${CLAUDE_FAST_MODE_MODEL_IDS.join(' and ')}`,
    );
  }
  const requested = controls[0]?.speed ?? 'standard';
  const conflicting = controls.find((entry) => entry.speed !== requested);
  if (conflicting) {
    throw new Error(
      `Claude Agent SDK fast mode is session-wide; mixed model.speed values are unsupported ` +
        `(${controls.map((entry) => `${entry.label}=${entry.speed}`).join(', ')})`,
    );
  }
  return requested;
}

function validateModelEffortForRuntime(model: string, effort: unknown, label: string): void {
  if (effort === undefined) return;
  const capability = getClaudeModelEffortCapability(model);
  if (!capability) {
    throw new Error(`${label} requests model.effort for unsupported model ${model}`);
  }
  if (typeof effort !== 'string' || !isClaudeModelEffortSupported(model, effort)) {
    throw new Error(
      `${label} requests model.effort ${JSON.stringify(effort)} for model ${model}; ` +
        `supported levels are ${capability.supportedEfforts.join(', ')}`,
    );
  }
}

function modelSpeedOrStandard(value: unknown, label: string): ModelSpeed {
  if (value === undefined) return 'standard';
  if (value === 'standard' || value === 'fast') return value;
  throw new Error(`${label} has unsupported model.speed ${JSON.stringify(value)}`);
}

function stripManagedAgentControls(
  agents: CreateRuntimeArgs['agents'],
): Record<string, AgentDefinition> | undefined {
  if (!agents) return undefined;
  const sdkAgents: Record<string, AgentDefinition> = {};
  for (const [name, definition] of Object.entries(agents)) {
    const {
      modelSpeed: _modelSpeed,
      managedAgentId: _managedAgentId,
      ...sdkDefinition
    } = definition;
    sdkAgents[name] = sdkDefinition;
  }
  return sdkAgents;
}

// ---------------------------------------------------------------------------
// Runtime factory.
// ---------------------------------------------------------------------------

/**
 * Build the per-session {@link Runtime} for the claude provider.
 *
 * Holds mutable per-session state — the active model, permission mode, and the
 * in-flight turn's `AbortController` — behind the {@link Runtime} surface. A fresh
 * controller is created per turn; `interrupt()` aborts it. The SDK's endpoint/key
 * come from the environment (optionally via the gateway override applied here).
 */
function createRuntime({
  model,
  modelSpeed,
  modelEffort,
  permissionMode,
  cwd,
  systemPrompt,
  tools,
  allowedTools,
  runtimeTools,
  agents,
  forwardSubagentText,
  customTools = [],
  env = process.env,
  diagnostics = (): void => {},
}: CreateRuntimeArgs): Runtime {
  applyGatewayEnv(env);
  const writePolicy = parseSandboxWritePolicy(env);
  if (writePolicy) assertNoWritableAliasToSkills(writePolicy);
  const localOrcaToolNames = runtimeTools?.includes(LOCAL_ORCA_READ_TOOL_NAME)
    ? [LOCAL_ORCA_READ_TOOL_NAME]
    : [];
  const sdkBuiltInTools =
    localOrcaToolNames.length > 0 ? tools?.filter((name) => name !== 'Read') : tools;
  let currentModel = resolveInitialModel(model, env);
  const requestedSpeed = resolveSessionModelSpeed(currentModel, modelSpeed, modelEffort, agents);
  const sdkAgents = stripManagedAgentControls(agents);

  let mode: PermissionMode = permissionMode || DEFAULT_PERMISSION_MODE;
  // SDK-generated native session identity. Keep this runtime-local: Orca's
  // `sess_*` id is only for our wire protocol and is never a valid SDK resume id.
  let nativeSessionId: string | undefined;
  // The AbortController for the in-flight turn; null between turns. Created fresh
  // per `runTurn`, aborted by `interrupt`, and nulled in the turn's `finally`.
  let controller: AbortController | null = null;
  const pendingCustomToolResults = new Map<
    string,
    { resolve: (payload: CustomToolResultPayload) => void }
  >();

  function resolvePendingCustomTools(message: string): void {
    for (const [id, pending] of pendingCustomToolResults.entries()) {
      pending.resolve({
        custom_tool_use_id: id,
        result: { error: message },
        is_error: true,
      });
      pendingCustomToolResults.delete(id);
    }
  }

  return {
    get model(): string {
      return currentModel;
    },

    setModel(next: string): void {
      if (!next) return;
      resolveSessionModelSpeed(next, requestedSpeed, modelEffort, agents);
      currentModel = next;
    },

    setPermissionMode(next: PermissionMode): void {
      mode = next || DEFAULT_PERMISSION_MODE;
    },

    interrupt(): void {
      resolvePendingCustomTools('Session interrupted before custom tool result was received.');
      controller?.abort();
    },

    handleCustomToolResult(payload: CustomToolResultPayload): boolean {
      const customToolUseId =
        typeof payload.custom_tool_use_id === 'string'
          ? payload.custom_tool_use_id
          : typeof payload.tool_use_id === 'string'
            ? payload.tool_use_id
            : null;
      if (!customToolUseId) return false;
      const pending = pendingCustomToolResults.get(customToolUseId);
      if (!pending) return false;
      pendingCustomToolResults.delete(customToolUseId);
      pending.resolve({ ...payload, custom_tool_use_id: customToolUseId });
      return true;
    },

    async *runTurn({ prompt, session }: RunTurnArgs): AsyncGenerator<WireFrame, void, void> {
      // Fresh controller per turn so a prior turn's abort never leaks into this one.
      const turnController = new AbortController();
      controller = turnController;
      const frameQueue = new AsyncFrameQueue<WireFrame>();
      const customNames = new Set(
        customToolAllowedToolNames(
          customTools.filter((tool) => !localOrcaToolNames.includes(tool.name)),
        ),
      );
      const customCallIds = new Set<string>();

      const requestCustomToolUse = (
        name: string,
        input: Record<string, unknown>,
      ): Promise<CustomToolResultPayload> => {
        const id = `evt_${randomUUID().replace(/-/g, '')}`;
        const result = new Promise<CustomToolResultPayload>((resolve) => {
          pendingCustomToolResults.set(id, { resolve });
        });
        frameQueue.push({ type: 'custom_tool_use', id, name, input });
        return result;
      };

      // `permissionMode` is a free-form string on our interface; the SDK narrows it
      // to its PermissionMode union. We forward as-is — the CLI validates the
      // value — and only assert the type at this single seam.
      //
      // `cwd` is optional on CreateRuntimeArgs; under exactOptionalPropertyTypes we
      // OMIT the key when undefined (so the SDK falls back to its own default)
      // rather than pass `cwd: undefined`, which the `Options` type rejects.
      const options: Options = {
        model: currentModel,
        ...(modelEffort !== undefined ? { effort: modelEffort } : {}),
        settings: { fastMode: requestedSpeed === 'fast' },
        // Avoid SDK auto-title inference, which is a separate request without
        // inheriting Messages API speed. Orca owns the durable session identity.
        title: session.sessionId,
        // Keep SDK transcript state in sandbox-local SDK storage for native warm resume.
        persistSession: true,
        permissionMode: mode as SdkPermissionMode,
        // Do not let image/user/project settings broaden managed filesystem
        // permissions beyond the control-plane policy.
        settingSources: [],
        includePartialMessages: true,
        // Managed wire has no prompt-suggestion event, and that SDK feature
        // issues a separate request without inheriting Messages API speed.
        promptSuggestions: false,
        abortController: turnController,
        ...(cwd !== undefined ? { cwd } : {}),
        ...(systemPrompt !== undefined ? { systemPrompt } : {}),
        ...(sdkBuiltInTools !== undefined ? { tools: sdkBuiltInTools } : {}),
        ...(allowedTools !== undefined ? { allowedTools } : {}),
        ...(writePolicy
          ? {
              sandbox: claudeSandboxSettings(writePolicy),
              canUseTool: writePolicyPermissionHandler(writePolicy, cwd),
            }
          : {}),
        // Omit on the first turn so the SDK creates its own session. Never set
        // `sessionId`: that would incorrectly reuse Orca's `sess_*` identity.
        ...(nativeSessionId !== undefined ? { resume: nativeSessionId } : {}),
      };
      const hasSubagents = Boolean(sdkAgents && Object.keys(sdkAgents).length > 0);
      if (hasSubagents) {
        options.tools = [
          ...new Set([...(Array.isArray(options.tools) ? options.tools : []), 'Agent']),
        ];
      }
      if (customTools.length > 0 || localOrcaToolNames.length > 0) {
        options.mcpServers = {
          [ORCA_MCP_SERVER_NAME]: buildCustomToolsMcpServer(
            customTools,
            requestCustomToolUse,
            localOrcaToolNames,
            writePolicy,
          ),
        };
        options.strictMcpConfig = true;
        options.allowedTools = [
          ...new Set([...(options.allowedTools ?? []), ...customToolAllowedToolNames(customTools)]),
        ];
      }
      if (hasSubagents) {
        const primaryPrompt = typeof options.systemPrompt === 'string' ? options.systemPrompt : '';
        delete options.systemPrompt;
        const primaryBoundary = buildPrimaryAgentBoundary(
          sdkAgents!,
          primaryPrompt,
          currentModel,
          modelEffort,
          [
            ...(Array.isArray(options.tools) ? options.tools : []),
            ...(allowedTools ?? []).filter((name) => name.startsWith('mcp__')),
            ...customToolAllowedToolNames(customTools),
          ],
        );
        options.agent = primaryBoundary.name;
        options.agents = primaryBoundary.agents;
        options.hooks = primaryAgentSelfDelegationGuard(primaryBoundary.name);
        options.forwardSubagentText = forwardSubagentText ?? true;
      }

      const stream = query({ prompt, options });
      const pump = (async () => {
        let sdkInitMessage: unknown;
        let fastModeValidated = false;
        let apiSpeedObserved = false;
        let providerErrorObserved = false;
        try {
          for await (const msg of stream) {
            const providerError = isSdkProviderErrorMessage(msg);
            providerErrorObserved ||= providerError;
            if (isSdkSystemInit(msg)) {
              sdkInitMessage = msg;
            } else if (
              requestedSpeed === 'fast' &&
              sdkInitMessage === undefined &&
              !providerError &&
              (msg as { type?: unknown }).type !== 'system'
            ) {
              throw new Error(
                'Claude fast mode requested but SDK produced model output before reporting fast_mode_state',
              );
            }
            if (
              sdkInitMessage !== undefined &&
              !fastModeValidated &&
              !providerError &&
              (msg as { type?: unknown }).type !== 'system'
            ) {
              assertRequestedFastMode(sdkInitMessage, requestedSpeed, true);
              fastModeValidated = true;
            }
            if ((msg as { type?: unknown }).type === 'result' && !providerError) {
              assertRequestedFastMode(msg, requestedSpeed, false);
            }
            if (
              !providerError &&
              ((msg as { type?: unknown }).type !== 'result' || !apiSpeedObserved)
            ) {
              apiSpeedObserved = assertRequestedApiSpeed(msg, requestedSpeed) || apiSpeedObserved;
            }
            // Every top-level SDK stream message carries the native session id.
            // Capture first non-empty value only; later turns resume this exact
            // SDK transcript, including tool and compaction context.
            nativeSessionId ??= sdkSessionId(msg);
            for (const frame of mapToWireFrames(msg, session.sessionId)) {
              // Public custom callbacks already use their own event IDs. Suppress
              // the SDK's implementation-detail MCP echoes, retaining usage and
              // unrelated built-in/remote tool events in the same message.
              if (frame.type === 'assistant') {
                frame.message.content = (frame.message.content ?? []).filter((block) => {
                  if (
                    block.type !== 'tool_use' ||
                    typeof block.name !== 'string' ||
                    !customNames.has(block.name)
                  )
                    return true;
                  if (typeof block.id === 'string') customCallIds.add(block.id);
                  return false;
                });
              } else if (frame.type === 'user' && Array.isArray(frame.message.content)) {
                frame.message.content = frame.message.content.filter(
                  (block) =>
                    block.type !== 'tool_result' ||
                    typeof block.tool_use_id !== 'string' ||
                    !customCallIds.has(block.tool_use_id),
                );
              }
              frameQueue.push(frame);
            }
          }
          if (!providerErrorObserved) {
            if (requestedSpeed === 'fast' && sdkInitMessage === undefined) {
              throw new Error(
                'Claude fast mode requested but SDK did not report fast_mode_state; refusing standard-speed fallback',
              );
            }
            if (sdkInitMessage !== undefined && !fastModeValidated) {
              assertRequestedFastMode(sdkInitMessage, requestedSpeed, true);
            }
          }
          frameQueue.close();
        } catch (err) {
          if (turnController.signal.aborted) {
            frameQueue.close();
            return;
          }
          diagnostics(`claude runtime error: ${errorMessage(err)}\n`);
          frameQueue.fail(err);
        }
      })();

      try {
        for await (const frame of frameQueue) yield frame;
        await pump;
      } finally {
        frameQueue.close();
        // Drop the controller so `interrupt()` between turns is a no-op.
        controller = null;
      }
    },
  };
}

const INTERNAL_PRIMARY_AGENT_BASE_NAME = '__orca_primary';

function buildPrimaryAgentBoundary(
  subagents: Record<string, AgentDefinition>,
  primaryPrompt: string,
  model: string,
  effort: CreateRuntimeArgs['modelEffort'],
  primaryToolNames: readonly string[],
): { name: string; agents: Record<string, AgentDefinition> } {
  let name = INTERNAL_PRIMARY_AGENT_BASE_NAME;
  let suffix = 2;
  while (Object.hasOwn(subagents, name)) {
    name = `${INTERNAL_PRIMARY_AGENT_BASE_NAME}_${suffix}`;
    suffix += 1;
  }

  const primary: AgentDefinition = {
    description: 'Internal primary managed-agent boundary. Never delegate to this agent.',
    prompt: primaryPrompt,
    model,
    ...(effort !== undefined ? { effort } : {}),
    tools: [...new Set(primaryToolNames)],
  };
  return {
    name,
    agents: { ...subagents, [name]: primary },
  };
}

function primaryAgentSelfDelegationGuard(name: string): NonNullable<Options['hooks']> {
  return {
    PreToolUse: [
      {
        matcher: 'Agent',
        hooks: [
          async (input) => {
            if (input.hook_event_name !== 'PreToolUse' || input.tool_name !== 'Agent') return {};
            const toolInput = input.tool_input;
            const target =
              toolInput && typeof toolInput === 'object'
                ? (toolInput as Record<string, unknown>)['subagent_type']
                : undefined;
            if (target !== name) return {};
            return {
              hookSpecificOutput: {
                hookEventName: 'PreToolUse',
                permissionDecision: 'deny',
                permissionDecisionReason:
                  'The internal primary managed-agent boundary cannot be delegated to.',
              },
            };
          },
        ],
      },
    ],
  };
}

/** Extract a non-empty SDK session id without coupling to individual message variants. */
function sdkSessionId(message: unknown): string | undefined {
  if (typeof message !== 'object' || message === null) return undefined;
  const sessionId = (message as { session_id?: unknown }).session_id;
  return typeof sessionId === 'string' && sessionId.length > 0 ? sessionId : undefined;
}

function isSdkSystemInit(message: unknown): boolean {
  if (typeof message !== 'object' || message === null) return false;
  const candidate = message as { type?: unknown; subtype?: unknown };
  return candidate.type === 'system' && candidate.subtype === 'init';
}

function isSdkProviderErrorMessage(message: unknown): boolean {
  if (typeof message !== 'object' || message === null) return false;
  const candidate = message as {
    type?: unknown;
    subtype?: unknown;
    is_error?: unknown;
    error?: unknown;
  };
  if (candidate.type === 'assistant') return typeof candidate.error === 'string';
  return (
    candidate.type === 'result' && (candidate.subtype !== 'success' || candidate.is_error === true)
  );
}

function assertRequestedFastMode(
  message: unknown,
  requestedSpeed: ModelSpeed,
  requireState: boolean,
): void {
  const candidate = message as {
    fast_mode_state?: unknown;
    fast_mode_disabled_reason?: unknown;
  };
  const state = candidate.fast_mode_state;
  if (requestedSpeed === 'standard') {
    if (state === 'on') {
      throw new Error('Claude standard speed requested but SDK activated fast mode');
    }
    return;
  }
  if (state === 'on') return;
  if (!requireState && state === undefined) return;
  const reason =
    typeof candidate.fast_mode_disabled_reason === 'string'
      ? ` (${candidate.fast_mode_disabled_reason})`
      : '';
  throw new Error(
    `Claude fast mode requested but SDK reported fast_mode_state=${JSON.stringify(state)}${reason}`,
  );
}

/**
 * Verify API-observed speed before forwarding model output. SDK fast-mode state
 * alone proves only local opt-in; `usage.speed` proves the provider or gateway
 * served the request at the requested inference speed.
 */
function assertRequestedApiSpeed(message: unknown, requestedSpeed: ModelSpeed): boolean {
  const observation = apiSpeedObservation(message);
  if (!observation) return false;

  const { source, speed } = observation;
  if (speed === undefined || speed === null) {
    if (requestedSpeed === 'fast') {
      throw new Error(
        `Claude fast speed requested but SDK ${source} omitted usage.speed; ` +
          'refusing unverified standard-speed fallback',
      );
    }
    return false;
  }
  if (speed !== 'standard' && speed !== 'fast') {
    throw new Error(
      `Claude SDK ${source} reported unsupported usage.speed=${JSON.stringify(speed)}`,
    );
  }
  if (speed !== requestedSpeed) {
    throw new Error(
      `Claude ${requestedSpeed} speed requested but SDK ${source} reported ` +
        `usage.speed=${JSON.stringify(speed)}`,
    );
  }
  return true;
}

function apiSpeedObservation(message: unknown): { source: string; speed: unknown } | null {
  const candidate = message as {
    type?: unknown;
    subtype?: unknown;
    is_error?: unknown;
    num_turns?: unknown;
    error?: unknown;
    message?: { usage?: { speed?: unknown } };
    event?: {
      type?: unknown;
      message?: { usage?: { speed?: unknown } };
    };
    usage?: { speed?: unknown };
  };
  if (candidate.type === 'stream_event' && candidate.event?.type === 'message_start') {
    return {
      source: 'stream message_start',
      speed: candidate.event.message?.usage?.speed,
    };
  }
  if (candidate.type === 'assistant' && candidate.error === undefined) {
    return { source: 'assistant message', speed: candidate.message?.usage?.speed };
  }
  if (
    candidate.type === 'result' &&
    candidate.subtype === 'success' &&
    candidate.is_error !== true &&
    typeof candidate.num_turns === 'number' &&
    candidate.num_turns > 0
  ) {
    return { source: 'result', speed: candidate.usage?.speed };
  }
  return null;
}

/**
 * Best-effort message extraction for diagnostics: read `.message` off any
 * non-nullish value (so a thrown `Error` OR a plain `{ message }` object surfaces
 * its message), fall back to the value itself, then string-coerce it. `unknown`
 * keeps the boundary type-safe without `any`.
 */
function errorMessage(err: unknown): string {
  const message = (err as { message?: unknown } | null | undefined)?.message;
  return String(message ?? err);
}

/**
 * Map one SDK message to canonical frames, typed as the provider-interface
 * {@link WireFrame}s that {@link Runtime.runTurn} must yield.
 *
 * `toFrames` (the pure transformation) returns {@link CanonicalFrame}s — an
 * SDK-faithful view of the stream-json wire whose content blocks, message
 * params, and usage are the agent SDK's own precise types. The provider
 * interface declares the SAME wire as {@link WireFrame}, but with deliberately
 * OPEN records (`ContentBlock = { type: string; [k]: unknown }`, `WireMessage`,
 * `Usage`) so a newer provider never breaks an older client. The two are equal
 * by construction — `toFrames` is the function that produces the canonical wire —
 * yet the compiler cannot prove it: the SDK's `BetaContentBlock` lacks the open
 * index signature `ContentBlock` requires, and `MessageParam.content` is
 * `string | ContentBlockParam[]` rather than `ContentBlock[]`. No field-level
 * massaging can close that gap without an assertion (you cannot make an SDK block
 * satisfy `[k]: unknown` structurally), so we assert ONCE here, at the single
 * boundary where the two equivalent wire views meet. The runtime forwards
 * `toFrames`' output verbatim — nothing is reshaped — so the bytes on the wire are
 * exactly what the transformation produced.
 */
function mapToWireFrames(msg: Parameters<typeof toFrames>[0], sessionId: string): WireFrame[] {
  // Equal-by-construction cast across two independent views of the canonical wire.
  return toFrames(msg, { sessionId }) as unknown as WireFrame[];
}

class AsyncFrameQueue<T> implements AsyncIterable<T> {
  private readonly items: T[] = [];
  private readonly waiters: Array<(result: IteratorResult<T>) => void> = [];
  private readonly errorWaiters: Array<(error: unknown) => void> = [];
  private closed = false;
  private error: unknown;

  push(item: T): void {
    if (this.closed) return;
    const waiter = this.waiters.shift();
    this.errorWaiters.shift();
    if (waiter) {
      waiter({ value: item, done: false });
      return;
    }
    this.items.push(item);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    while (this.waiters.length > 0) {
      const waiter = this.waiters.shift();
      this.errorWaiters.shift();
      waiter?.({ value: undefined as T, done: true });
    }
  }

  fail(error: unknown): void {
    if (this.closed) return;
    this.error = error;
    this.closed = true;
    while (this.errorWaiters.length > 0) {
      const reject = this.errorWaiters.shift();
      this.waiters.shift();
      reject?.(error);
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    while (true) {
      if (this.items.length > 0) {
        yield this.items.shift() as T;
        continue;
      }
      if (this.error !== undefined) throw this.error;
      if (this.closed) return;
      const next = await new Promise<IteratorResult<T>>((resolve, reject) => {
        this.waiters.push(resolve);
        this.errorWaiters.push(reject);
      });
      if (next.done) return;
      yield next.value;
    }
  }
}

// ---------------------------------------------------------------------------
// Provider descriptor.
// ---------------------------------------------------------------------------

/**
 * The single 'claude' provider. Registered in ./registry.ts; resolved by `id` or
 * any alias (case-insensitively). `harnessId`/`displayName` are the public labels
 * surfaced via `list_harnesses`.
 */
export const claudeProvider: Provider = {
  id: 'claude',
  aliases: ['anthropic', 'claude-agent', 'claude-code', 'cc'],
  harnessId: 'claude-code',
  displayName: 'Claude Code',
  createRuntime,
};
