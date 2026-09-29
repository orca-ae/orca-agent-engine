// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  BUILTIN_EVALUATORS,
  InMemoryGuardrailStateStore,
  SHARED_USAGE_KEYS,
  evaluateGuardrails,
  type PreparedGuardrail,
  type StateUpdate,
  type Verdict,
} from '@orca/guardrails';
/**
 * InSandboxHarness — AgentHarness that drives an @orca/sandbox-harness
 * server over HTTP/SSE via an injected HarnessTransport.
 *
 * Lifecycle:
 *   start(input)  → opens HarnessChannel; background loop maps raw sandbox
 *                   events → AgentEvents and pushes them into an async queue.
 *   submit(ev)    → forwards to channel.submit().
 *   events()      → drains the internal queue (async generator).
 *   stop(reason)  → stops the channel and closes the queue.
 *
 * The async queue is promise-based (no external deps): items pushed before
 * a consumer calls events() accumulate in `items`; waiting consumers register
 * a resolver that is called on the next push. This ensures:
 *   - Events pushed before the consumer iterates are delivered.
 *   - The consumer blocks cleanly when the queue is empty.
 *   - stop() drains any waiting consumer with a done signal.
 */

import type {
  AgentEvent,
  AgentEventInput,
  AgentHarness,
  SessionStartInput,
  SubmitHooks,
  TerminationReason,
  UserEvent,
  UserEventSubmitResult,
} from '../agent-harness.js';
import type { AgentEventId } from '@orca/agent-event-contract';
import {
  GuardrailPolicyDeniedError,
  runtimeAgentToolNameUnion,
  UnappliedUserEventError,
  withCanonicalAgentEventEnvelope,
} from '../agent-harness.js';
import { v7 as uuidv7 } from 'uuid';
import {
  AgentRuntimeSignalKind,
  SessionEventKind,
  SpanEventKind,
  sessionWarningPayload,
  turnModelSummaryStartPayload,
} from '../event-kinds.js';
import type {
  HarnessChannel,
  HarnessTransport,
  OpenSessionOptions,
  SandboxAgentDefinition,
} from './transport.js';
import { DialInTransport } from './dial-in.js';
import { mapSandboxEvent } from './event-mapper.js';
import type { ReplayTurn } from './replay.js';
import { ORCA_MCP_SERVER_NAME } from '../claude/mcp-tools.js';
import { resolveClaudeSessionSpeed } from '../model-controls.js';
import { evaluateGuardrailExpression } from '../guardrail-expression.js';

export interface InSandboxHarnessOptions {
  providerId: string;
  /** Harness HTTP port inside the sandbox (from the catalog). */
  port: number;
  /** Test override; when set, used instead of dialing the sandbox endpoint. */
  transport?: HarnessTransport;
  /** Returns prior turns to rehydrate a cold session; omitted/[] = fresh start. */
  replaySource?: () => Promise<ReplayTurn[]>;
  /**
   * Maximum time to wait for the resumed turn after the final custom-tool
   * result. Bounds duplicate-after-apply and lost sandbox-state wedges.
   * Defaults to 5 minutes.
   */
  customToolResultTerminalTimeoutMs?: number;
  onGuardrailState?: (updates: readonly StateUpdate[]) => Promise<void> | void;
  refreshGuardrailSubjectWindow?: (
    turnEventId: string,
  ) => Promise<Readonly<Record<string, unknown>>>;
}

export class InSandboxHarness implements AgentHarness {
  private channel: HarnessChannel | null = null;
  private stopped = false;
  private pumpFailure: Error | null = null;

  // Async queue state.
  private items: AgentEvent[] = [];
  private waiters: Array<(result: IteratorResult<AgentEvent>) => void> = [];
  private terminalWaiters: Array<{ resolve: () => void; reject: (error: Error) => void }> = [];
  private queueClosed = false;
  private modelProvider: string | undefined;
  private model: string | undefined;
  private sessionId = '';
  private preparedGuardrails: PreparedGuardrail[] = [];
  private guardrailStore = new InMemoryGuardrailStateStore();
  private readonly pendingCustomToolUseIds = new Set<string>();
  private readonly sessionSystemMessages: string[] = [];
  /** Start-envelope ID for the current coarse turn model summary. */
  private currentModelSummaryStartId: AgentEventId | null = null;
  /** The turn a usage delta belongs to, so Registry can attribute its spend. */
  private currentTurnEventId: string | undefined;

  constructor(private readonly opts: InSandboxHarnessOptions) {}

  async start(input: SessionStartInput): Promise<void> {
    const unsupportedLlmRequest = (input.guardrails ?? []).filter((guardrail) =>
      guardrail.phases.includes('llm_request'),
    );
    if (unsupportedLlmRequest.length > 0) {
      throw new Error(
        `in_sandbox cannot enforce llm_request guardrails before the sandbox protocol carries a model-call interceptor: ${unsupportedLlmRequest.map((guardrail) => `${guardrail.name} (${guardrail.id})`).join(', ')}`,
      );
    }
    const modelSpeed = resolveClaudeSessionSpeed(input.agentSnapshot);
    let transport = this.opts.transport;
    if (!transport) {
      const sandbox = input.sandbox;
      if (!sandbox?.endpoint) {
        throw new Error(
          'InSandboxHarness: sandbox endpoint unavailable (colocated requires a runtime that exposes the harness port)',
        );
      }
      const ep = await sandbox.endpoint(this.opts.port);
      transport = new DialInTransport({ baseUrl: ep.url, headers: ep.headers });
    }
    this.modelProvider = input.agentSnapshot.model_provider;
    this.model = input.agentSnapshot.model_id;
    this.sessionId = input.sessionId;
    const replay = this.opts.replaySource ? await this.opts.replaySource() : [];
    const preparedGuardrails = (input.guardrails ?? []).map(toPreparedGuardrail);
    this.preparedGuardrails = preparedGuardrails;
    this.guardrailStore = new InMemoryGuardrailStateStore(
      input.guardrailState ? { session: { ...input.guardrailState } } : {},
    );
    for (const guardrail of preparedGuardrails.filter(
      (entry) => entry.stateful && !entry.guardrail.phases.includes('request'),
    )) {
      this.push({
        kind: SessionEventKind.warning,
        payload: sessionWarningPayload({
          type: 'guardrail_not_enforced',
          message: `Guardrail ${guardrail.guardrail.name} is stateful but has no request phase, so in_sandbox cannot enforce it.`,
          guardrailId: guardrail.guardrail.id,
          guardrailName: guardrail.guardrail.name,
        }),
      });
    }
    const agents = buildSubagentDefinitions(input.agentSnapshot.multiagent, preparedGuardrails);
    const customTools = input.agentSnapshot.custom_tools ?? [];
    const toolOptions = buildSdkToolOptions(input.agentSnapshot, preparedGuardrails);
    const runtimeTools = runtimeAgentToolNameUnion(input.agentSnapshot);
    this.channel = await transport.open({
      agent: this.opts.providerId,
      ...(input.agentSnapshot.model_id ? { model: input.agentSnapshot.model_id } : {}),
      modelSpeed,
      ...(input.agentSnapshot.model_effort
        ? { modelEffort: input.agentSnapshot.model_effort }
        : {}),
      ...(input.agentSnapshot.system ? { systemPrompt: input.agentSnapshot.system } : {}),
      ...toolOptions,
      ...(runtimeTools.length > 0 ? { runtimeTools } : {}),
      ...(replay.length > 0 ? { replay } : {}),
      ...(Object.keys(agents).length > 0 ? { agents, forwardSubagentText: true } : {}),
      ...(customTools.length > 0 ? { customTools } : {}),
    });

    // Background loop: consume SSE events, map, push to queue.
    void this.pump();
  }

  applyGuardrailUsageState(state: Readonly<Record<string, unknown>>): void {
    this.guardrailStore.apply(
      Object.entries(state).map(([key, value]) => ({
        scope:
          key === SHARED_USAGE_KEYS.dailyCostUsd
            ? ('subject_window' as const)
            : ('session' as const),
        key,
        action: 'set' as const,
        value,
      })),
    );
  }

  async submit(event: UserEvent, hooks?: SubmitHooks): Promise<UserEventSubmitResult | void> {
    this.assertReadyForSubmit();
    if (event.kind === 'user.message') {
      if (!hasSupportedMessageContent(event.payload)) {
        throw new UnappliedUserEventError(
          event.kind,
          'user.message does not contain supported non-empty text content.',
        );
      }
      if (this.hasPendingRequiredAction()) return 'deferred';
    } else if (event.kind === 'user.custom_tool_result') {
      const customToolUseId = customToolResultId(event.payload);
      if (!customToolUseId || !this.pendingCustomToolUseIds.has(customToolUseId)) {
        throw new UnappliedUserEventError(
          event.kind,
          'No pending custom tool use matches this user.custom_tool_result event.',
        );
      }
    } else {
      throw new UnappliedUserEventError(
        event.kind,
        `${event.kind} is not supported by the colocated harness protocol.`,
      );
    }
    await hooks?.onAccepted();
    this.assertReadyForSubmit();
    if (event.kind === 'user.message') {
      this.guardrailStore.resetTurn();
      this.currentTurnEventId = event.id;
      await this.refreshGuardrailSubjectWindow();
      await this.evaluateRequestGuardrails(systemMessageText(event.payload) ?? '');
    }
    const companionSystemMessage = systemMessageText(event.systemMessage);
    if (companionSystemMessage) this.sessionSystemMessages.push(companionSystemMessage);
    const submittedEvent = withSystemMessageContext(event, this.sessionSystemMessages);
    let shouldWaitForTerminal = true;
    let acceptedCustomToolUseId: string | null = null;
    if (event.kind === 'user.custom_tool_result') {
      acceptedCustomToolUseId = customToolResultId(event.payload)!;
      // A sandbox turn can expose several client-executed tools at once. The
      // remote harness cannot finish the turn until every result arrives, so
      // waiting here after a non-final result would block the serialized event
      // source from delivering the remaining results. Keep the id pending until
      // the remote submission succeeds so a transient transport failure can be
      // retried with the same result.
      shouldWaitForTerminal = this.pendingCustomToolUseIds.size === 1;
    }
    const terminal = shouldWaitForTerminal
      ? this.waitForNextTerminal(
          event.kind === 'user.custom_tool_result'
            ? this.customToolResultTerminalTimeoutMs()
            : undefined,
        )
      : null;
    let openedModelSummary = false;
    // A turn opens with exactly one session.status_running + one
    // span.model_request_start (Claude RECEIVED taxonomy parity). The sandbox
    // wire has no running/start frame, so the bridge emits them at submit time,
    // before the sandbox's turn events arrive; the span id correlates with the
    // span.model_request_end injected in pump().
    const startsOrResumesModelWork =
      isTurnDrivingEvent(event.kind) ||
      (event.kind === 'user.custom_tool_result' && shouldWaitForTerminal);
    if (startsOrResumesModelWork) {
      this.push({ kind: SessionEventKind.statusRunning, payload: {} });
      if (!this.currentModelSummaryStartId) {
        openedModelSummary = true;
        this.currentModelSummaryStartId = `evt_${uuidv7()}`;
        this.push({
          kind: SpanEventKind.modelRequestStart,
          id: this.currentModelSummaryStartId,
          payload: turnModelSummaryStartPayload({
            ...(this.modelProvider !== undefined ? { provider: this.modelProvider } : {}),
            ...(this.model !== undefined ? { model: this.model } : {}),
          }),
        });
      }
    }
    try {
      await this.channel!.submit(submittedEvent);
      await terminal?.promise;
      if (acceptedCustomToolUseId !== null) {
        this.pendingCustomToolUseIds.delete(acceptedCustomToolUseId);
      }
    } catch (err) {
      terminal?.cancel();
      if (openedModelSummary) this.currentModelSummaryStartId = null;
      throw err;
    }
  }

  async stop(_reason: TerminationReason): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;

    if (this.channel) {
      await this.channel.stop();
      this.channel = null;
    }

    this.currentModelSummaryStartId = null;
    this.closeQueue();
    this.pendingCustomToolUseIds.clear();
  }

  hasPendingRequiredAction(): boolean {
    return this.pendingCustomToolUseIds.size > 0;
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (true) {
      // Drain any buffered items first.
      while (this.items.length > 0) {
        const item = this.items.shift();
        if (item !== undefined) yield item;
      }

      if (this.queueClosed) {
        if (this.pumpFailure) throw this.pumpFailure;
        return;
      }

      // Wait for the next item or a close signal.
      const result = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        // Check again inside the promise to avoid a lost-wakeup race: a push
        // (or close) may have happened between the while check and this line.
        if (this.items.length > 0) {
          const item = this.items.shift() as AgentEvent;
          resolve({ value: item, done: false });
          return;
        }
        if (this.queueClosed) {
          resolve({ value: undefined as unknown as AgentEvent, done: true });
          return;
        }
        this.waiters.push(resolve);
      });

      if (result.done) {
        if (this.pumpFailure) throw this.pumpFailure;
        return;
      }
      yield result.value;
    }
  }

  // ── private ────────────────────────────────────────────────────────────────

  private async pump(): Promise<void> {
    if (!this.channel) return;
    try {
      for await (const raw of this.channel.events()) {
        if (this.stopped) break;
        if (raw.type === 'agent.custom_tool_use' && raw.id) {
          this.pendingCustomToolUseIds.add(raw.id);
        } else if (raw.type === 'session.status_error') {
          this.pendingCustomToolUseIds.clear();
        }
        const openedFallbackModelSummary =
          raw.type === 'session.status_idle' && !this.currentModelSummaryStartId;
        if (openedFallbackModelSummary) {
          this.currentModelSummaryStartId = `evt_${uuidv7()}`;
        }
        const modelSummary =
          raw.type === 'session.status_idle' && this.currentModelSummaryStartId
            ? {
                modelRequestStartId: this.currentModelSummaryStartId,
                ...(this.modelProvider !== undefined ? { provider: this.modelProvider } : {}),
                ...(this.model !== undefined ? { model: this.model } : {}),
              }
            : undefined;
        for (const mapped of mapSandboxEvent(raw, modelSummary)) {
          // Registry prices a delta against the model that produced it and
          // attributes it to a turn. A frame that already names its own model
          // keeps it — a subagent's model is not the session's.
          if (mapped.kind === AgentRuntimeSignalKind.usage) {
            const usagePayload = mapped.payload as Record<string, unknown>;
            mapped.payload = {
              ...usagePayload,
              ...(typeof usagePayload['model'] === 'string'
                ? {}
                : this.model
                  ? { model: this.model }
                  : {}),
              ...(this.currentTurnEventId ? { turn_event_id: this.currentTurnEventId } : {}),
            };
          }
          // Preserve the legacy no-submit/replay ordering: the raw terminal
          // frame's diverted usage signal precedes the synthesized start, but
          // the start still precedes its correlated model end.
          if (openedFallbackModelSummary && mapped.kind === SpanEventKind.modelRequestEnd) {
            this.push({
              kind: SpanEventKind.modelRequestStart,
              id: this.currentModelSummaryStartId!,
              payload: turnModelSummaryStartPayload({
                ...(this.modelProvider !== undefined ? { provider: this.modelProvider } : {}),
                ...(this.model !== undefined ? { model: this.model } : {}),
              }),
            });
          }
          this.push(mapped);
        }
      }
    } catch (err) {
      if (!this.stopped) {
        this.pumpFailure =
          err instanceof Error
            ? err
            : new Error('InSandboxHarness: event pump failed', { cause: err });
        console.warn('InSandboxHarness: event pump stopped unexpectedly', this.pumpFailure);
      }
    } finally {
      if (!this.stopped && !this.pumpFailure) {
        this.pumpFailure = new Error('InSandboxHarness: event stream closed unexpectedly');
      }
      // Close the queue when the SSE stream ends so events() terminates.
      this.closeQueue();
    }
  }

  private push(event: AgentEventInput): void {
    const enveloped = withCanonicalAgentEventEnvelope(event);
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter({ value: enveloped, done: false });
    } else {
      this.items.push(enveloped);
    }
    if (closesModelSummary(enveloped)) {
      this.currentModelSummaryStartId = null;
    }
    if (isTerminal(enveloped)) {
      this.terminalWaiters.shift()?.resolve();
    }
  }

  private closeQueue(): void {
    if (this.queueClosed) return;
    this.queueClosed = true;
    for (const waiter of this.waiters.splice(0)) {
      waiter({ value: undefined as unknown as AgentEvent, done: true });
    }
    for (const terminalWaiter of this.terminalWaiters.splice(0)) {
      if (this.pumpFailure) terminalWaiter.reject(this.pumpFailure);
      else terminalWaiter.resolve();
    }
  }

  private assertReadyForSubmit(): void {
    if (!this.channel) throw new Error('InSandboxHarness: not started');
    if (this.pumpFailure) throw this.pumpFailure;
    if (this.queueClosed) throw new Error('InSandboxHarness: event stream is closed');
  }

  private waitForNextTerminal(timeoutMs?: number): { promise: Promise<void>; cancel: () => void } {
    try {
      this.assertReadyForSubmit();
    } catch (error) {
      const failure = error instanceof Error ? error : new Error('event stream unavailable');
      const promise = Promise.reject(failure);
      void promise.catch(() => undefined);
      return { promise, cancel: () => {} };
    }
    let waiter: { resolve: () => void; reject: (error: Error) => void } | null = null;
    let timer: NodeJS.Timeout | null = null;
    const promise = new Promise<void>((resolve, reject) => {
      const clearTimer = () => {
        if (timer !== null) clearTimeout(timer);
        timer = null;
      };
      waiter = {
        resolve: () => {
          clearTimer();
          resolve();
        },
        reject: (error) => {
          clearTimer();
          reject(error);
        },
      };
      this.terminalWaiters.push(waiter);
      if (timeoutMs !== undefined) {
        timer = setTimeout(() => {
          if (!waiter) return;
          const idx = this.terminalWaiters.indexOf(waiter);
          if (idx !== -1) this.terminalWaiters.splice(idx, 1);
          timer = null;
          reject(
            new Error(
              `InSandboxHarness: timed out after ${timeoutMs}ms waiting for the custom tool result turn to finish`,
            ),
          );
        }, timeoutMs);
      }
    });
    void promise.catch(() => undefined);
    return {
      promise,
      cancel: () => {
        if (!waiter) return;
        const idx = this.terminalWaiters.indexOf(waiter);
        if (idx !== -1) this.terminalWaiters.splice(idx, 1);
        if (timer !== null) clearTimeout(timer);
        timer = null;
      },
    };
  }

  private customToolResultTerminalTimeoutMs(): number {
    const configured = this.opts.customToolResultTerminalTimeoutMs ?? 5 * 60_000;
    return Number.isFinite(configured) && configured > 0 ? Math.floor(configured) : 5 * 60_000;
  }

  private async evaluateRequestGuardrails(userText: string): Promise<void> {
    if (this.preparedGuardrails.length === 0) return;
    const decision = evaluateGuardrails(
      this.preparedGuardrails,
      {
        phase: 'request',
        sessionId: this.sessionId,
        userText,
        ...(this.model ? { modelId: this.model } : {}),
      },
      {
        builtins: BUILTIN_EVALUATORS,
        expression: evaluateGuardrailExpression,
        store: this.guardrailStore,
      },
    );
    if (decision.verdict !== 'allow') {
      const message =
        decision.reasons.join(' ') || 'The request was denied by a managed-agent guardrail.';
      throw new GuardrailPolicyDeniedError(decision.reasons, message);
    }
    if (decision.stateUpdates.length > 0) {
      await this.opts.onGuardrailState?.(decision.stateUpdates);
      this.guardrailStore.apply(decision.stateUpdates);
    }
  }

  private async refreshGuardrailSubjectWindow(): Promise<void> {
    if (!this.currentTurnEventId || !this.opts.refreshGuardrailSubjectWindow) return;
    const state = await this.opts.refreshGuardrailSubjectWindow(this.currentTurnEventId);
    this.guardrailStore.replace('subject_window', state);
  }
}

function isTerminal(event: AgentEvent): boolean {
  // Both success and error turns close with session.status_idle in the unified
  // taxonomy; session.status_terminated is a hard terminal too.
  return (
    event.kind === SessionEventKind.statusIdle || event.kind === SessionEventKind.statusTerminated
  );
}

function closesModelSummary(event: AgentEvent): boolean {
  if (event.kind === SessionEventKind.statusTerminated) return true;
  if (event.kind !== SessionEventKind.statusIdle) return false;
  const payload = event.payload;
  if (!payload || typeof payload !== 'object') return true;
  const stopReason = (payload as { stop_reason?: unknown }).stop_reason;
  if (!stopReason || typeof stopReason !== 'object') return true;
  return (stopReason as { type?: unknown }).type !== 'requires_action';
}

function isTurnDrivingEvent(kind: string): boolean {
  return kind === 'user.message';
}

function hasSupportedMessageContent(payload: unknown): boolean {
  if (!payload || typeof payload !== 'object') return false;
  const content = (payload as { content?: unknown }).content;
  if (!Array.isArray(content)) return false;
  return content.some(
    (part) =>
      part !== null &&
      typeof part === 'object' &&
      (part as { type?: unknown }).type === 'text' &&
      typeof (part as { text?: unknown }).text === 'string' &&
      (part as { text: string }).text.length > 0,
  );
}

function customToolResultId(payload: unknown): string | null {
  if (!payload || typeof payload !== 'object') return null;
  const p = payload as { custom_tool_use_id?: unknown; tool_use_id?: unknown };
  if (typeof p.custom_tool_use_id === 'string') return p.custom_tool_use_id;
  return typeof p.tool_use_id === 'string' ? p.tool_use_id : null;
}

function systemMessageText(payload: unknown): string | null {
  if (!payload || typeof payload !== 'object') return null;
  const content = (payload as { content?: unknown }).content;
  if (!Array.isArray(content)) return null;
  const text = content
    .filter(
      (part) =>
        part !== null &&
        typeof part === 'object' &&
        (part as { type?: unknown }).type === 'text' &&
        typeof (part as { text?: unknown }).text === 'string',
    )
    .map((part) => (part as { text: string }).text)
    .filter((part) => part.length > 0)
    .join('\n');
  return text.length > 0 ? text : null;
}

function withSystemMessageContext(event: UserEvent, messages: string[]): UserEvent {
  if (messages.length === 0) return event;
  const payload =
    event.payload && typeof event.payload === 'object' && !Array.isArray(event.payload)
      ? (event.payload as Record<string, unknown>)
      : {};
  const content = Array.isArray(payload.content) ? payload.content : [];
  return {
    ...event,
    payload: {
      ...payload,
      content: [
        ...content,
        {
          type: 'text',
          text: `<system-reminder>\n${messages.join('\n\n')}\n</system-reminder>`,
        },
      ],
    },
  };
}

// Keep this adapter aligned with the logical agent_toolset catalog assembled by
// expandAllowedOrcaToolNames() in runner/dispatcher.ts. `read` deliberately
// stays on Orca's MCP implementation so both harness modes expose the same
// bounded offset/limit contract; the remaining safe one-to-one tools use SDK
// built-ins inside the sandbox.
const SDK_TOOL_BY_LOGICAL_NAME: Readonly<Record<string, string>> = {
  bash: 'Bash',
  write: 'Write',
  edit: 'Edit',
  glob: 'Glob',
  grep: 'Grep',
};

function sdkToolNameForLogicalName(logicalName: string): string | undefined {
  return logicalName === 'read'
    ? `mcp__${ORCA_MCP_SERVER_NAME}__read`
    : SDK_TOOL_BY_LOGICAL_NAME[logicalName];
}

/**
 * Decide which tools a sandboxed session is handed at all.
 *
 * This runs once, before any tool is called, because a sandboxed session has no
 * way to ask for approval mid-turn. That timing is what limits guardrails here
 * to the **stateless** pass: a budget cannot be consulted before the session has
 * spent anything, so stateful guardrails are enforced at the request phase
 * instead, which harness-server still sees per turn.
 *
 * A guardrail resolving to `ask` therefore removes the tool, exactly as an
 * `always_ask` permission policy does — the alternative is falling through to a
 * permission mode that could approve a filesystem write unattended.
 */
function buildSdkToolOptions(
  snapshot: SessionStartInput['agentSnapshot'],
  guardrails: readonly PreparedGuardrail[] = [],
): Pick<OpenSessionOptions, 'tools' | 'allowedTools'> {
  const logicalNames = snapshot.allowed_tool_names;
  if (logicalNames === undefined) return {};

  const policies = snapshot.tool_permission_policies ?? {};
  const tools: string[] = [];
  const allowedTools: string[] = [];
  for (const logicalName of logicalNames) {
    const sdkName = sdkToolNameForLogicalName(logicalName);
    if (!sdkName) {
      console.warn(
        `InSandboxHarness: declared logical tool "${logicalName}" has no Claude SDK built-in mapping; omitting it`,
      );
      continue;
    }
    const policy = statelessToolVerdict(logicalName, sdkName, policies, guardrails);
    if (policy !== 'always_allow') {
      if (policy === 'always_ask') {
        console.warn(
          `InSandboxHarness: declared logical tool "${logicalName}" requires approval, but in-sandbox approvals are unavailable; omitting it`,
        );
      }
      continue;
    }
    if (logicalName !== 'read') tools.push(sdkName);
    allowedTools.push(sdkName);
  }

  return {
    tools: [...new Set(tools)],
    allowedTools: [...new Set(allowedTools)],
  };
}

/**
 * The permission policy, tightened by whatever the stateless guardrail pass
 * decides. Runs the engine with no state store, which is what confines it to
 * guardrails that need no state — see `buildSdkToolOptions`.
 */
/** Session-start wire shape to the engine's shape. */
function toPreparedGuardrail(
  g: NonNullable<SessionStartInput['guardrails']>[number],
): PreparedGuardrail {
  return {
    guardrail: {
      id: g.id,
      name: g.name,
      enabled: true,
      phases: g.phases as PreparedGuardrail['guardrail']['phases'],
      scope: 'explicit',
      rule: g.rule as PreparedGuardrail['guardrail']['rule'],
    },
    tier: g.tier,
    stateful: g.stateful,
    ...(g.stateScope ? { stateScope: g.stateScope } : {}),
    ...(g.subagentId ? { subagentId: g.subagentId } : {}),
  };
}

function statelessToolVerdict(
  logicalName: string,
  sdkName: string,
  policies: Readonly<Record<string, 'always_allow' | 'always_ask' | 'always_deny'>>,
  guardrails: readonly PreparedGuardrail[],
  subagentId?: string,
): 'always_allow' | 'always_ask' | 'always_deny' {
  const seedPolicy = inSandboxToolPolicy(logicalName, sdkName, policies);
  if (guardrails.length === 0) return seedPolicy;

  const seed: Verdict =
    seedPolicy === 'always_deny' ? 'deny' : seedPolicy === 'always_ask' ? 'ask' : 'allow';

  // Tool exposure happens before an invocation exists. A plain `{}` would make
  // argument-sensitive rules look as though they evaluated successfully and
  // abstained, silently exposing the tool. The sentinel lets name-only rules
  // keep resolving normally while any rule that actually reads input becomes
  // explicitly unevaluable and therefore unavailable in this topology.
  let inputRead = false;
  const unavailableInput = new Proxy(Object.create(null) as Record<string, unknown>, {
    get() {
      inputRead = true;
      throw new Error('tool input is unavailable at exposure time');
    },
    has() {
      inputRead = true;
      throw new Error('tool input is unavailable at exposure time');
    },
    ownKeys() {
      inputRead = true;
      throw new Error('tool input is unavailable at exposure time');
    },
    getOwnPropertyDescriptor() {
      inputRead = true;
      throw new Error('tool input is unavailable at exposure time');
    },
  });

  const decision = evaluateGuardrails(
    guardrails,
    {
      phase: 'tool_call',
      sessionId: '',
      tool: { name: sdkName, input: unavailableInput },
      ...(subagentId ? { subagentId } : {}),
    },
    // No store: the stateful partition is skipped rather than evaluated against
    // an empty snapshot, which would read as every counter being at zero.
    { builtins: BUILTIN_EVALUATORS, expression: evaluateGuardrailExpression, seed },
  );

  // `ask` is the exposure-time representation of "cannot decide without the
  // real arguments". In-sandbox approval is unavailable, so the caller removes
  // the tool rather than treating an empty synthetic input as an allow.
  if (inputRead) return 'always_ask';

  return decision.verdict === 'deny'
    ? 'always_deny'
    : decision.verdict === 'ask'
      ? 'always_ask'
      : 'always_allow';
}

function inSandboxToolPolicy(
  logicalName: string,
  sdkName: string,
  policies: Readonly<Record<string, 'always_allow' | 'always_ask' | 'always_deny'>>,
): 'always_allow' | 'always_ask' | 'always_deny' {
  return (
    policies[`mcp__${ORCA_MCP_SERVER_NAME}__${logicalName}`] ??
    policies[logicalName] ??
    policies[sdkName] ??
    policies[`mcp__${ORCA_MCP_SERVER_NAME}__*`] ??
    // In-sandbox approvals are unavailable. Never fall through to the SDK's
    // permission mode, which may approve a filesystem operation.
    'always_ask'
  );
}

function buildSubagentDefinitions(
  multiagent: SessionStartInput['agentSnapshot']['multiagent'] | undefined,
  guardrails: readonly PreparedGuardrail[],
): Record<string, SandboxAgentDefinition> {
  if (!multiagent || multiagent.type !== 'coordinator') return {};
  const out: Record<string, SandboxAgentDefinition> = {};
  const used = new Set<string>();
  for (const agent of multiagent.agents) {
    const key = uniqueSubagentKey(agent.name, agent.id, used);
    const tools = sdkToolNamesForSubagent(
      agent.allowed_tool_names,
      agent.tool_permission_policies ?? {},
      guardrails,
      agent.id,
    );
    out[key] = {
      managedAgentId: agent.id,
      description: `Managed agent ${agent.name} (${agent.id} v${agent.version})`,
      prompt: agent.system ?? `You are ${agent.name}.`,
      ...(agent.model_id ? { model: agent.model_id } : {}),
      ...(agent.model_speed ? { modelSpeed: agent.model_speed } : {}),
      ...(agent.model_effort ? { effort: agent.model_effort } : {}),
      ...(tools ? { tools } : {}),
    };
  }
  return out;
}

function sdkToolNamesForSubagent(
  allowedLogicalNames: string[] | undefined,
  policies: Readonly<Record<string, 'always_allow' | 'always_ask' | 'always_deny'>>,
  guardrails: readonly PreparedGuardrail[],
  subagentId: string,
): string[] | undefined {
  if (allowedLogicalNames === undefined) return undefined;
  const tools: string[] = [];
  for (const logicalName of allowedLogicalNames) {
    const sdkName = sdkToolNameForLogicalName(logicalName);
    if (!sdkName) {
      console.warn(
        `InSandboxHarness: subagent logical tool "${logicalName}" has no Claude SDK built-in mapping; omitting it`,
      );
      continue;
    }
    if (
      statelessToolVerdict(logicalName, sdkName, policies, guardrails, subagentId) ===
      'always_allow'
    ) {
      tools.push(sdkName);
    }
  }
  return [...new Set(tools)];
}

function uniqueSubagentKey(name: string, id: string, used: Set<string>): string {
  const base =
    name
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, '-')
      .replace(/^-+|-+$/g, '') || id;
  let key = base;
  let counter = 2;
  while (used.has(key)) {
    key = `${base}-${counter}`;
    counter += 1;
  }
  used.add(key);
  return key;
}
