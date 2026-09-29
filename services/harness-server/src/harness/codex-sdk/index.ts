// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { assertSdkTerminalUsage as assertCodexTerminalUsage } from '@orca/sdk-harness';
import { PiSdkWorker, assertPiCheckpointInstructions } from '@orca/pi-harness';
import type { HarnessTurnAction, HarnessTurnReceipt } from '@orca/harness-catalog';
import { recoverCodexTurn, receiptTerminalEvents, type CodexTurnStore } from './turn-receipt.js';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import type { CallToolResult, Tool } from '@modelcontextprotocol/sdk/types.js';
import {
  CodexSdkWorker,
  customToolResultToMcp,
  CustomToolResultConversionError,
  assertCodexCheckpointInstructions,
  type CodexCheckpoint,
  type WorkerEvent,
} from '@orca/codex-harness';
import { validateHarnessGuardrails } from '@orca/harness-catalog';
import {
  BUILTIN_EVALUATORS,
  compileGuardrailRule,
  InMemoryGuardrailStateStore,
  SHARED_USAGE_KEYS,
  type StateUpdate,
  evaluateGuardrails,
  evaluateGuardrailExpression,
  type PreparedGuardrail,
  type GuardrailEvent,
  type Verdict,
} from '@orca/guardrails';
import type { ClaudeHarnessOptions } from '../claude/index.js';
import type { SessionJwtProvider } from '../../mcp/session-jwt-provider.js';
import {
  type AgentHarness,
  type AgentEvent,
  type AgentEventInput,
  type SessionStartInput,
  type SubmitHooks,
  type UserEvent,
  type TerminationReason,
  type UserEventSubmitResult,
  withCanonicalAgentEventEnvelope,
  UnappliedUserEventError,
  SettledHarnessFailureError,
  DurableHarnessStateError,
  GuardrailPolicyDeniedError,
  GuardrailUsageUnavailableError,
  SessionEventKind,
  AgentEventKind,
  SpanEventKind,
  sessionIdlePayload,
  turnModelSummaryStartPayload,
  turnModelSummaryEndPayload,
  type ModelUsageCounts,
} from '../agent-harness.js';

const USAGE_PENDING_PREFIX = 'codex_sdk_usage_pending:';

type Worker = import('@orca/sdk-harness').SdkWorker;
export interface CodexSdkHarnessOptions {
  harness?: 'codex_sdk' | 'pi_sdk';
  apiKey: string;
  baseUrl?: string;
  piGatewayUrl?: string;
  checkpoint?: CodexCheckpoint;
  turns: CodexTurnStore;
  llmGatewayJwtProvider?: Pick<SessionJwtProvider, 'getValidToken' | 'close'>;
  createWorker?: (emit: (event: WorkerEvent) => void, input: SessionStartInput) => Worker;
  turnTimeoutMs?: number;
  onGuardrailState?: ClaudeHarnessOptions['onGuardrailState'];
  onUsage?: ClaudeHarnessOptions['onUsage'];
  refreshGuardrailSubjectWindow?: ClaudeHarnessOptions['refreshGuardrailSubjectWindow'];
}
interface Binding {
  tool: Tool;
  policyName: string;
  serverName?: string;
  custom?: boolean;
  call(args: Record<string, unknown>): Promise<CallToolResult>;
}
interface PendingAction {
  kind: 'user.tool_confirmation' | 'user.custom_tool_result';
  resolve(payload: Record<string, unknown>): void;
}

/** Server-side SDK loop. Every executable tool is supplied by Orca, never the CLI host. */
export class CodexSdkHarness implements AgentHarness {
  private input: SessionStartInput | undefined;
  private worker: Worker | undefined;
  private root: string | undefined;
  private clients: Client[] = [];
  private bindings = new Map<string, Binding>();
  private pending = new Map<AgentEvent['id'], PendingAction>();
  private guards: PreparedGuardrail[] = [];
  private guardrailStore = new InMemoryGuardrailStateStore();
  private guarded = false;
  private accountingBlocked = false;
  private interruptVersion = 0;
  private submitTail: Promise<unknown> = Promise.resolve();
  private usageEventId: string | undefined;
  private usageAcknowledged = false;
  private checkpoint: CodexCheckpoint | undefined;
  private receipt: HarnessTurnReceipt | null = null;
  private turnStateTail: Promise<unknown> = Promise.resolve();
  private readonly persistenceWaiters = new Set<(error: unknown) => void>();
  private active: Promise<void> | undefined;
  private starting: Promise<void> | undefined;
  private stopPromise: Promise<void> | undefined;
  private abort: AbortController | undefined;
  private eventTail: Promise<void> = Promise.resolve();
  private toolCalls = new Set<Promise<void>>();
  private idleWaiters = new Set<() => void>();
  private queue: AgentEvent[] = [];
  private readers: Array<(event: IteratorResult<AgentEvent>) => void> = [];
  private stopped = false;
  private failed = false;
  private turnFailure: string | undefined;
  private turnId: string | undefined;
  private usage: ModelUsageCounts = {
    input_tokens: 0,
    output_tokens: 0,
    cache_read_input_tokens: 0,
    cache_creation_input_tokens: 0,
  };

  constructor(private readonly opts: CodexSdkHarnessOptions) {}

  private get sdkName(): string {
    return this.opts.harness === 'pi_sdk' ? 'Pi SDK' : 'Codex SDK';
  }
  private get harnessName(): string {
    return this.opts.harness === 'pi_sdk' ? 'pi_sdk' : 'codex_sdk';
  }

  async start(input: SessionStartInput): Promise<void> {
    if (this.starting) throw new Error(`${this.sdkName} is already starting`);
    try {
      this.starting = this.initialize(input);
      try {
        await this.starting;
      } finally {
        this.starting = undefined;
      }
    } catch (error) {
      await this.stop('error');
      throw error;
    }
  }

  private async initialize(input: SessionStartInput): Promise<void> {
    if (this.input || this.stopped) throw new Error(`${this.sdkName} already started or stopped`);
    if (
      (this.opts.harness !== 'pi_sdk' && input.agentSnapshot.model_provider !== 'openai') ||
      !input.agentSnapshot.model_provider ||
      !input.agentSnapshot.model_id
    )
      throw new Error(`${this.harnessName} requires a pinned supported model`);
    if (this.opts.checkpoint)
      (this.opts.harness === 'pi_sdk'
        ? assertPiCheckpointInstructions
        : assertCodexCheckpointInstructions)(
        this.opts.checkpoint,
        input.agentSnapshot.system ?? '',
      );
    if (input.agentSnapshot.multiagent || input.clientToolExecution)
      throw new Error(`${this.harnessName} does not support multiagent or client tool execution`);
    const resolved = (input.guardrails ?? []).map((g) => {
      const rule = g.rule as PreparedGuardrail['guardrail']['rule'];
      const scope = g.tier === 'organization' || g.tier === 'workspace' ? g.tier : 'explicit';
      const compiled = compileGuardrailRule(rule, scope);
      if (!compiled.ok)
        throw new Error(
          `Invalid guardrail ${g.id}: ${compiled.errors.map((e) => e.message).join(', ')}`,
        );
      if (rule.kind === 'builtin') {
        if (rule.builtin === 'subagent_cost_budget')
          throw new Error(`${this.harnessName} does not support subagent budget rules`);
        if (
          ['ask_thresholds', 'ask_thresholds_usd'].some(
            (key) => Array.isArray(compiled.params[key]) && compiled.params[key].length > 0,
          )
        )
          throw new Error(
            `${this.harnessName} request budgets do not support soft approval thresholds`,
          );
      }
      return { ...g, stateful: compiled.stateful, stateScope: compiled.stateScope, rule };
    });
    const guardrailError = validateHarnessGuardrails(
      { harness: this.opts.harness ?? 'codex_sdk', mode: 'separate' },
      resolved,
    );
    if (guardrailError) throw new Error(guardrailError);
    this.guarded = resolved.some((g) => g.stateful);
    if (this.guarded && (!this.opts.onGuardrailState || !this.opts.onUsage))
      throw new Error(
        `${this.harnessName} stateful request rules require durable guardrail state and usage callbacks`,
      );
    if (
      resolved.some((g) => g.stateScope === 'subject_window') &&
      !this.opts.refreshGuardrailSubjectWindow
    )
      throw new Error(
        `${this.harnessName} daily budgets require authenticated subject-window refresh`,
      );
    const claimed = await this.opts.turns.claim();
    if (this.stopped) throw new Error(`${this.sdkName} stopped during ownership claim`);
    const restored = await recoverCodexTurn(
      {
        ...this.opts.turns,
        update: (action) => this.updateTurn(action),
        recover: async (receipt) => {
          if (this.stopped) throw new Error(`${this.sdkName} stopped before receipt recovery`);
          await this.opts.turns.recover(receipt);
        },
      },
      claimed,
    );
    this.receipt = restored.receipt;
    this.checkpoint = restored.state ? (restored.state as CodexCheckpoint) : undefined;
    if (this.checkpoint)
      (this.opts.harness === 'pi_sdk'
        ? assertPiCheckpointInstructions
        : assertCodexCheckpointInstructions)(this.checkpoint, input.agentSnapshot.system ?? '');
    this.input = input;
    this.guardrailStore = new InMemoryGuardrailStateStore();
    this.applyGuardrailUsageState(restored.guardrailState ?? input.guardrailState ?? {});
    this.guards = resolved.map((g) => ({
      guardrail: {
        id: g.id,
        name: g.name,
        enabled: true,
        scope: 'explicit',
        phases: g.phases as PreparedGuardrail['guardrail']['phases'],
        // Request-only budgets have no later approval boundary. Unknown spend
        // must deny immediately unless the policy explicitly allows it.
        rule:
          g.rule.kind === 'builtin' &&
          ['cost_budget', 'user_daily_cost_budget'].includes(g.rule.builtin)
            ? {
                ...g.rule,
                params: {
                  ...g.rule.params,
                  on_unpriced: g.rule.params?.on_unpriced === 'allow' ? 'allow' : 'deny',
                },
              }
            : g.rule,
      },
      tier: g.tier,
      stateful: g.stateful,
      ...(g.stateScope ? { stateScope: g.stateScope } : {}),
    }));
    const allowed = new Set(input.agentSnapshot.allowed_tool_names ?? []);
    for (const tool of input.tools ?? []) {
      if (!allowed.has(tool.name)) continue;
      this.addBinding(tool.name, {
        tool: {
          name: tool.name,
          description: tool.description,
          inputSchema: tool.input_schema as Tool['inputSchema'],
        },
        policyName: `mcp__orca__${tool.name}`,
        call: async (args) => {
          const result = await tool.execute(args);
          return {
            content: [
              {
                type: 'text',
                text: result.content ?? result.error ?? JSON.stringify(result.output ?? null),
              },
            ],
            isError: Boolean(result.error),
          };
        },
      });
    }
    for (const name of allowed) {
      if (!this.bindings.has(name)) throw new Error(`sandbox tool is unavailable: ${name}`);
    }
    for (const tool of input.agentSnapshot.custom_tools ?? []) {
      this.addBinding(tool.name, {
        tool: {
          name: tool.name,
          description: tool.description ?? '',
          inputSchema: (tool.input_schema ?? { type: 'object' }) as Tool['inputSchema'],
        },
        policyName: `mcp__orca__${tool.name}`,
        custom: true,
        call: async () => {
          throw new Error('custom tools require a client result');
        },
      });
    }
    for (const { serverName } of input.remoteMcpToolsets ?? []) {
      const config = input.mcpServers?.[serverName];
      if (!config || serverName === 'orca')
        throw new Error(`invalid MCP configuration: ${serverName}`);
      const client = new Client({ name: 'orca-codex-sdk', version: '1.0.0' });
      this.clients.push(client);
      await client.connect(
        new StreamableHTTPClientTransport(new URL(config.url), {
          requestInit: { headers: config.headers },
          fetch: async (url, init) => {
            const headers = new Headers(init?.headers);
            if (input.mcpJwtProvider)
              headers.set(
                'Authorization',
                `Bearer ${(await input.mcpJwtProvider.getValidToken()).token}`,
              );
            return fetch(url, { ...init, headers });
          },
        }) as Transport,
      );
      let cursor: string | undefined;
      do {
        const page = await client.listTools(cursor ? { cursor } : {});
        for (const tool of page.tools) {
          const name = `mcp__${serverName}__${tool.name}`;
          this.addBinding(name, {
            tool: { ...tool, name },
            policyName: name,
            serverName,
            call: async (args) =>
              (await client.callTool(
                { name: tool.name, arguments: args },
                undefined,
                this.abort ? { signal: this.abort.signal } : {},
              )) as CallToolResult,
          });
        }
        cursor = page.nextCursor;
      } while (cursor);
    }
    // The SDK must never inspect the service checkout or the remote sandbox as a host path.
    this.root = await mkdtemp(join(tmpdir(), 'orca-codex-work-'));
    this.worker = (
      this.opts.createWorker ??
      ((emit) =>
        this.opts.harness === 'pi_sdk' ? new PiSdkWorker(emit) : new CodexSdkWorker(emit))
    )((event) => this.receive(event), input);
    const apiKey = await this.apiKey();
    if (this.stopped) throw new Error(`${this.sdkName} stopped during startup`);
    await this.worker.handle({
      type: 'start',
      root: this.root,
      sessionId: input.sessionId,
      model: input.agentSnapshot.model_id,
      modelProvider: input.agentSnapshot.model_provider,
      system: input.agentSnapshot.system ?? '',
      apiKey,
      ...(this.opts.baseUrl ? { baseUrl: this.opts.baseUrl } : {}),
      ...(this.opts.harness === 'pi_sdk' && this.opts.piGatewayUrl
        ? { piGatewayUrl: this.opts.piGatewayUrl }
        : {}),
      ...(input.agentSnapshot.model_effort ? { effort: input.agentSnapshot.model_effort } : {}),
      ...(this.checkpoint ? { checkpoint: this.checkpoint } : {}),
      tools: [...this.bindings.values()].map((binding) => binding.tool),
    });
  }

  async submit(event: UserEvent, hooks?: SubmitHooks): Promise<UserEventSubmitResult> {
    if (this.stopped || !this.worker) throw new Error(`${this.sdkName} is unavailable`);
    if (event.kind === 'user.interrupt') {
      if (this.receipt?.phase !== 'pending') {
        await this.active;
        await hooks?.onAccepted();
        if (!event.id) throw new Error(`${this.sdkName} control requires a persisted source id`);
        await this.interrupt();
        await this.emitPersisted({
          kind: SessionEventKind.statusIdle,
          payload: sessionIdlePayload('end_turn'),
          completionPolicy: { sourceIds: [event.id] },
        });
        return 'submitted';
      }
      await this.acceptSource(event);
      await hooks?.onAccepted();
      this.turnFailure ??= `${this.sdkName} turn interrupted`;
      await this.interrupt();
      await this.active;
      return 'submitted';
    }
    if (event.kind === 'user.tool_confirmation' || event.kind === 'user.custom_tool_result') {
      const payload = object(event.payload);
      const id = (
        event.kind === 'user.custom_tool_result' ? payload.custom_tool_use_id : payload.tool_use_id
      ) as AgentEvent['id'];
      const pending = this.pending.get(id);
      if (!pending || pending.kind !== event.kind)
        throw new UnappliedUserEventError(event.kind, 'No pending tool action matches this event.');
      if (event.systemMessage)
        throw new UnappliedUserEventError(
          event.kind,
          `${this.harnessName} does not support system.message companions.`,
        );
      if (
        event.kind === 'user.tool_confirmation' &&
        !['allow', 'deny'].includes(String(payload.result))
      )
        throw new UnappliedUserEventError(
          event.kind,
          'Tool confirmation requires result allow or deny.',
        );
      if (
        event.kind === 'user.custom_tool_result' &&
        payload.content !== undefined &&
        !Array.isArray(payload.content)
      )
        throw new UnappliedUserEventError(event.kind, 'Custom tool result requires content.');
      await hooks?.onAccepted();
      await this.acceptSource(event);
      if (this.pending.get(id) !== pending) return 'submitted';
      this.pending.delete(id);
      if (!this.pending.size) this.emit({ kind: SessionEventKind.statusRunning, payload: {} });
      pending.resolve(payload);
      await this.waitForIdle();
      return 'submitted';
    }
    if (event.kind !== 'user.message')
      throw new UnappliedUserEventError(
        event.kind,
        `${event.kind} is not supported by ${this.harnessName}.`,
      );
    // Claim the lane before acceptance, refresh, or any other asynchronous
    // preflight. Required-action replies and interrupts bypass this lane.
    const submit = this.submitTail.then(() => this.submitMessage(event, hooks));
    this.submitTail = submit.catch(() => undefined);
    return await submit;
  }

  private async submitMessage(
    event: UserEvent,
    hooks?: SubmitHooks,
  ): Promise<UserEventSubmitResult> {
    if (this.stopped || (this.failed && !this.hasPendingGuardrailUsage()))
      throw new Error(`${this.sdkName} is unavailable`);
    if (event.id && this.receipt?.phase === 'settled' && this.receipt.sourceIds.includes(event.id))
      return 'submitted';
    const text = messageText(event.payload);
    if (!text)
      throw new UnappliedUserEventError(
        event.kind,
        `${this.harnessName} requires non-empty text content.`,
      );
    if (event.systemMessage)
      throw new UnappliedUserEventError(
        event.kind,
        `${this.harnessName} does not support system.message companions.`,
      );
    if (this.active) {
      await this.waitForIdle();
      if (this.hasPendingRequiredAction()) return 'deferred';
    }
    if (this.guarded && this.hasPendingGuardrailUsage())
      throw new GuardrailUsageUnavailableError(
        `${this.sdkName} pending usage accounting blocks the next request`,
      );
    if (!event.id) throw new Error(`${this.sdkName} requires a persisted turn event id`);
    const interruptVersion = this.interruptVersion;
    await hooks?.onAccepted();
    if (this.stopped) throw new Error(`${this.sdkName} stopped during acceptance`);
    this.turnId = event.id;
    this.guardrailStore.resetTurn();
    if (this.guarded && this.opts.refreshGuardrailSubjectWindow) {
      const state = await this.opts.refreshGuardrailSubjectWindow(this.turnId!);
      this.guardrailStore.replace('subject_window', state);
    }
    const decision = this.evaluate({ phase: 'request', userText: text });
    if (decision.verdict !== 'allow')
      throw new GuardrailPolicyDeniedError(
        decision.reasons,
        decision.reasons.join(' ') || 'Request denied by policy.',
      );
    this.usageEventId = withCanonicalAgentEventEnvelope({
      kind: AgentEventKind.usage,
      payload: {},
    }).id;
    this.usageAcknowledged = false;
    this.checkpoint = undefined;
    await this.updateTurn({
      type: 'begin',
      receipt: {
        turnId: event.id,
        sourceIds: [event.id],
        usageEventId: this.usageEventId,
        guarded: this.guarded,
        phase: 'pending',
        error: null,
        producedAt: new Date().toISOString(),
        terminalEventId: withCanonicalAgentEventEnvelope({
          kind: SessionEventKind.statusIdle,
          payload: {},
        }).id,
        errorEventId: withCanonicalAgentEventEnvelope({ kind: SessionEventKind.error, payload: {} })
          .id,
      },
    });
    if (this.guarded) {
      const updates: StateUpdate[] = [
        ...decision.stateUpdates,
        {
          scope: 'session',
          key: `${USAGE_PENDING_PREFIX}${this.turnId}`,
          action: 'set',
          value: { version: 1, turnEventId: this.turnId, usageEventId: this.usageEventId },
        },
      ];
      // An uncertain response can mean the write committed. Retire this local
      // generation before releasing work; only acknowledged writes advance state.
      this.accountingBlocked = true;
      try {
        await this.opts.onGuardrailState!(updates);
      } catch (cause) {
        this.failed = true;
        throw new DurableHarnessStateError(
          `${this.sdkName} pending accounting marker was not acknowledged`,
          { cause },
        );
      }
      this.guardrailStore.apply(updates);
    }
    if (this.stopped) throw new Error(`${this.sdkName} stopped during request persistence`);
    if (interruptVersion !== this.interruptVersion) {
      const abandoned = await this.updateTurn({ type: 'abandon', turnId: this.turnId! });
      for (const terminal of receiptTerminalEvents(abandoned.receipt!))
        await this.emitPersisted(terminal);
      await this.updateTurn({ type: 'settle', turnId: this.turnId! });
      return 'submitted';
    }
    this.abort = new AbortController();
    this.turnFailure = undefined;
    this.eventTail = Promise.resolve();
    this.usage = {
      input_tokens: 0,
      output_tokens: 0,
      cache_read_input_tokens: 0,
      cache_creation_input_tokens: 0,
    };
    this.active = this.run(text).finally(() => {
      this.active = undefined;
      this.wakeIdle();
    });
    void this.active.catch(() => undefined);
    await this.waitForIdle();
    return 'submitted';
  }

  hasPendingRequiredAction(): boolean {
    return this.pending.size > 0;
  }

  private async run(text: string): Promise<void> {
    this.emit({ kind: SessionEventKind.statusRunning, payload: {} });
    const start = this.emit({
      kind: SpanEventKind.modelRequestStart,
      payload: turnModelSummaryStartPayload({
        provider: this.input!.agentSnapshot.model_provider!,
        model: this.input!.agentSnapshot.model_id!,
      }),
    });
    const timer = setTimeout(() => {
      this.turnFailure = `${this.sdkName} turn timed out`;
      void this.interrupt();
    }, this.opts.turnTimeoutMs ?? 600_000);
    try {
      await this.worker!.refreshOptions({
        apiKey: await this.apiKey(),
        ...(this.opts.baseUrl ? { baseUrl: this.opts.baseUrl } : {}),
        ...(this.opts.harness === 'pi_sdk' && this.opts.piGatewayUrl
          ? { piGatewayUrl: this.opts.piGatewayUrl }
          : {}),
      });
      if (!this.abort?.signal.aborted) await this.worker!.handle({ type: 'submit', text });
      await Promise.all(this.toolCalls);
      await this.eventTail;
      if (this.guarded && !this.usageAcknowledged)
        this.turnFailure ??= `${this.sdkName} terminal usage was not acknowledged`;
    } catch {
      this.failed = true;
      this.turnFailure ??= `${this.sdkName} execution or durable usage failed`;
    } finally {
      clearTimeout(timer);
      this.abort?.abort();
      this.cancelActions();
    }
    if (this.stopped) return;
    // FIFO acknowledgment proves every preceding response event is durable before
    // native history may advance. A rejected fence leaves the receipt pending.
    await this.emitPersisted({
      kind: SpanEventKind.modelRequestEnd,
      payload: turnModelSummaryEndPayload({
        modelRequestStartId: start.id,
        provider: this.input!.agentSnapshot.model_provider!,
        model: this.input!.agentSnapshot.model_id!,
        modelUsage: this.usage,
        isError: Boolean(this.turnFailure),
      }),
    });
    const committed = this.checkpoint
      ? await this.updateTurn({
          type: 'commit',
          turnId: this.turnId!,
          state: this.checkpoint,
          responsePersisted: true,
          error: this.turnFailure?.slice(0, 4096) ?? null,
        })
      : await this.updateTurn({
          type: 'abandon',
          turnId: this.turnId!,
          error:
            this.turnFailure?.slice(0, 4096) ??
            `${this.sdkName} native checkpoint was unavailable; the unfinished turn was abandoned without replay.`,
        });
    for (const event of receiptTerminalEvents(committed.receipt!)) await this.emitPersisted(event);
    await this.updateTurn({ type: 'settle', turnId: this.turnId! });
    if (this.guarded && !committed.receipt!.error) {
      this.guardrailStore.apply([
        { scope: 'session', key: `${USAGE_PENDING_PREFIX}${this.turnId}`, action: 'delete' },
      ]);
      this.accountingBlocked = false;
    }
    // Surface fatal worker outcomes on this submit, after the durable receipt
    // boundary, so the dispatcher retires the runner before the next message.
    if (this.failed)
      throw new SettledHarnessFailureError(this.turnFailure ?? `${this.sdkName} worker failed`);
  }

  private async updateTurn(action: Exclude<HarnessTurnAction, { type: 'inspect' | 'claim' }>) {
    const task = this.turnStateTail.then(async () => {
      if (this.stopped) throw new Error(`${this.sdkName} stopped before receipt persistence`);
      const result = await this.opts.turns.update(action);
      if (this.stopped) throw new Error(`${this.sdkName} stopped during receipt persistence`);
      this.receipt = result.receipt;
      return result;
    });
    this.turnStateTail = task.catch(() => undefined);
    try {
      return await task;
    } catch (cause) {
      this.failed = true;
      throw new DurableHarnessStateError(`${this.sdkName} turn persistence was not acknowledged`, {
        cause,
      });
    }
  }

  private async acceptSource(event: UserEvent): Promise<void> {
    if (!event.id || !this.receipt)
      throw new Error(`${this.sdkName} control requires a persisted source id`);
    await this.updateTurn({
      type: 'accept_source',
      turnId: this.receipt.turnId,
      sourceId: event.id,
    });
  }

  private async emitPersisted(input: AgentEventInput): Promise<void> {
    if (this.stopped)
      throw new DurableHarnessStateError(`${this.sdkName} stopped before response persistence`);
    let reject!: (error: unknown) => void;
    const persisted = new Promise<void>((resolve, fail) => {
      reject = fail;
      this.persistenceWaiters.add(reject);
      this.emit({ ...input, persistence: { resolve, reject } });
    });
    try {
      await persisted;
    } catch (cause) {
      this.failed = true;
      throw new DurableHarnessStateError(
        `${this.sdkName} response persistence was not acknowledged`,
        {
          cause,
        },
      );
    } finally {
      this.persistenceWaiters.delete(reject);
    }
    if (this.stopped)
      throw new DurableHarnessStateError(`${this.sdkName} stopped after response persistence`);
  }

  private receive(event: WorkerEvent): void {
    // Retirement cannot advance native history without the response/receipt fence.
    if (this.stopped) return;
    if (event.type === 'tool_call') {
      const call = this.callTool(event)
        .catch(() => {
          this.turnFailure = `${this.sdkName} tool relay failed`;
          return this.interrupt();
        })
        .finally(() => this.toolCalls.delete(call));
      this.toolCalls.add(call);
    } else {
      this.eventTail = this.eventTail.then(async () => {
        if (event.type === 'checkpoint') {
          this.checkpoint = event.checkpoint;
        } else if (event.type === 'failure') {
          this.turnFailure ??= event.message;
          if (event.fatal) this.failed = true;
        } else if (event.type === 'event') {
          const sdk = event.event;
          if (sdk.type === 'item.completed' && sdk.item.type === 'agent_message')
            this.emit({
              kind: AgentEventKind.message,
              payload: { content: [{ type: 'text', text: sdk.item.text }] },
            });
          else if (sdk.type === 'turn.completed') {
            assertCodexTerminalUsage(sdk.usage);
            const usage = {
              input_tokens: sdk.usage.input_tokens - sdk.usage.cached_input_tokens,
              output_tokens: sdk.usage.output_tokens,
              cache_read_input_tokens: sdk.usage.cached_input_tokens,
              cache_creation: {
                ephemeral_5m_input_tokens:
                  (sdk.usage.cache_write_input_tokens ?? 0) -
                  (sdk.usage.cache_write_input_tokens_1h ?? 0),
                ephemeral_1h_input_tokens: sdk.usage.cache_write_input_tokens_1h ?? 0,
              },
            };
            this.usage = {
              ...usage,
              cache_creation_input_tokens: sdk.usage.cache_write_input_tokens ?? 0,
            };
            if (this.guarded) {
              const state = await this.opts.onUsage!(
                usage,
                this.input!.agentSnapshot.model_id!,
                undefined,
                this.turnId,
                this.usageEventId,
              );
              if (!state)
                throw new Error('Registry did not return authoritative Codex usage totals');
              this.applyGuardrailUsageState(state);
              this.usageAcknowledged = true;
            }
            this.emit({
              ...(this.usageEventId ? { id: this.usageEventId } : {}),
              kind: AgentEventKind.usage,
              payload: {
                usage,
                ...(this.guarded ? { guardrail_usage_recorded: true } : {}),
                model: this.input!.agentSnapshot.model_id,
                ...(this.turnId ? { turn_event_id: this.turnId } : {}),
              },
            });
          } else if (
            (sdk.type === 'turn.failed' || sdk.type === 'error') &&
            !this.abort?.signal.aborted
          )
            this.turnFailure = sdk.type === 'error' ? sdk.message : sdk.error.message;
          else if (
            sdk.type === 'item.started' &&
            ['command_execution', 'file_change', 'web_search'].includes(sdk.item.type)
          ) {
            this.turnFailure = `${this.sdkName} attempted an unmediated native tool`;
            this.failed = true;
            await this.interrupt();
          }
        }
      });
      // The active turn observes the original rejected tail before emitting its terminal event.
      void this.eventTail.catch(() => {
        this.failed = true;
        void this.interrupt();
      });
    }
  }

  private async callTool(event: Extract<WorkerEvent, { type: 'tool_call' }>): Promise<void> {
    const binding = this.bindings.get(event.name);
    let result: CallToolResult;
    const toolEvent = withCanonicalAgentEventEnvelope({
      kind: binding?.custom
        ? AgentEventKind.customToolUse
        : binding?.serverName
          ? AgentEventKind.mcpToolUse
          : AgentEventKind.toolUse,
      payload: {},
    });
    const name = binding?.serverName
      ? event.name.slice(`mcp__${binding.serverName}__`.length)
      : event.name;
    const payload = {
      id: toolEvent.id,
      name,
      input: event.arguments,
      ...(binding?.serverName ? { mcp_server_name: binding.serverName } : {}),
    };
    this.emit({ ...toolEvent, payload });
    try {
      if (!binding || this.abort?.signal.aborted)
        throw new Error('Tool unavailable or turn interrupted');
      const policy = this.permission(binding);
      const verdict = this.evaluate(
        { phase: 'tool_call', tool: { name: binding.policyName, input: event.arguments } },
        policy,
      );
      if (verdict.verdict === 'deny')
        throw new Error(verdict.reasons.join(' ') || 'Tool denied by policy');
      if (binding.custom && verdict.verdict === 'ask')
        throw new Error('Custom tool denied: guardrail requires explicit tool approval');
      if (verdict.verdict === 'ask') {
        const confirmation = await this.requestAction(toolEvent.id, 'user.tool_confirmation');
        if (confirmation.result !== 'allow')
          throw new Error(
            typeof confirmation.deny_message === 'string'
              ? confirmation.deny_message
              : 'Tool denied by user',
          );
      }
      if (this.abort?.signal.aborted) throw new Error('Turn interrupted');
      if (binding.custom) {
        const reply = await this.requestAction(toolEvent.id, 'user.custom_tool_result');
        result = customToolResultToMcp(reply);
      } else result = await binding.call(event.arguments);
      const outputVerdict = this.evaluate({
        phase: 'tool_result',
        tool: { name: binding.policyName, input: event.arguments },
        result: result.content,
      });
      if (outputVerdict.verdict !== 'allow') throw new Error('Tool output suppressed by policy');
    } catch (error) {
      if (error instanceof CustomToolResultConversionError) {
        this.turnFailure = error.message;
        await this.interrupt();
      }
      result = {
        isError: true,
        content: [{ type: 'text', text: error instanceof Error ? error.message : 'Tool failed' }],
      };
    }
    if (!this.stopped) {
      if (!binding?.custom)
        this.emit({
          kind: binding?.serverName ? AgentEventKind.mcpToolResult : AgentEventKind.toolResult,
          payload: {
            tool_use_id: toolEvent.id,
            content: result.content,
            is_error: result.isError ?? false,
            ...(binding?.serverName ? { mcp_server_name: binding.serverName } : {}),
          },
        });
      await this.worker!.handle({ type: 'tool_result', id: event.id, result });
    }
  }

  private permission(binding: Binding): Verdict {
    if (binding.custom) return 'allow';
    const policies = this.input!.agentSnapshot.tool_permission_policies ?? {};
    const policy =
      policies[binding.policyName] ??
      policies[`mcp__${binding.serverName ?? 'orca'}__*`] ??
      (binding.serverName
        ? (this.input!.remoteMcpToolsets?.find((t) => t.serverName === binding.serverName)
            ?.permissionPolicy ?? 'always_ask')
        : 'always_allow');
    return policy === 'always_allow' ? 'allow' : policy === 'always_deny' ? 'deny' : 'ask';
  }
  applyGuardrailUsageState(state: Readonly<Record<string, unknown>>): void {
    this.guardrailStore.apply(
      Object.entries(state).map(([key, value]) => ({
        scope:
          key === SHARED_USAGE_KEYS.dailyCostUsd || key === SHARED_USAGE_KEYS.dailyCostUnpriced
            ? ('subject_window' as const)
            : ('session' as const),
        key,
        action: 'set' as const,
        value,
      })),
    );
  }

  hasPendingGuardrailUsage(): boolean {
    return (
      this.accountingBlocked ||
      Object.keys(this.guardrailStore.read('session')).some((key) =>
        key.startsWith(USAGE_PENDING_PREFIX),
      )
    );
  }

  private evaluate(event: Omit<GuardrailEvent, 'sessionId' | 'modelId'>, seed: Verdict = 'allow') {
    return evaluateGuardrails(
      this.guards,
      { ...event, sessionId: this.input!.sessionId, modelId: this.input!.agentSnapshot.model_id! },
      {
        builtins: BUILTIN_EVALUATORS,
        expression: evaluateGuardrailExpression,
        seed,
        store: this.guardrailStore,
      },
    );
  }
  private addBinding(name: string, binding: Binding): void {
    if (this.bindings.has(name)) throw new Error(`duplicate tool: ${name}`);
    this.bindings.set(name, binding);
  }
  private requestAction(
    id: AgentEvent['id'],
    kind: PendingAction['kind'],
  ): Promise<Record<string, unknown>> {
    return new Promise((resolve) => {
      if (this.abort?.signal.aborted) {
        resolve({ result: 'deny', content: [], is_error: true });
        return;
      }
      this.pending.set(id, { kind, resolve });
      this.emit({
        kind: SessionEventKind.statusIdle,
        completionPolicy: 'pending',
        payload: sessionIdlePayload('requires_action', [...this.pending.keys()] as [
          AgentEvent['id'],
          ...AgentEvent['id'][],
        ]),
      });
      this.wakeIdle();
    });
  }
  private cancelActions(): void {
    for (const action of this.pending.values())
      action.resolve({ result: 'deny', content: [], is_error: true });
    this.pending.clear();
  }
  private async interrupt(): Promise<void> {
    this.interruptVersion += 1;
    this.abort?.abort();
    this.cancelActions();
    await this.worker?.handle({ type: 'interrupt' });
  }
  private async apiKey(): Promise<string> {
    const key = this.opts.llmGatewayJwtProvider
      ? (await this.opts.llmGatewayJwtProvider.getValidToken(this.abort?.signal)).token
      : this.opts.apiKey;
    if (!key)
      throw new Error(`${this.harnessName} requires gateway LLM credentials or OPENAI_API_KEY`);
    return key;
  }
  private async waitForIdle(): Promise<void> {
    if (!this.active || this.pending.size) return;
    const active = this.active;
    let wake!: () => void;
    const idle = new Promise<void>((resolve) => {
      wake = resolve;
      this.idleWaiters.add(wake);
    });
    try {
      await Promise.race([active, idle]);
      if (!this.pending.size) await active;
    } finally {
      this.idleWaiters.delete(wake);
    }
  }
  private wakeIdle(): void {
    for (const wake of this.idleWaiters) wake();
  }
  private emit(input: AgentEventInput): AgentEvent {
    const event = withCanonicalAgentEventEnvelope(input);
    if (!this.stopped) {
      const reader = this.readers.shift();
      if (reader) reader({ value: event, done: false });
      else this.queue.push(event);
    }
    return event;
  }
  events(): AsyncIterable<AgentEvent> {
    return {
      [Symbol.asyncIterator]: () => ({
        next: async () => {
          const event = this.queue.shift();
          if (event) return { value: event, done: false };
          if (this.stopped) return { value: undefined, done: true };
          return await new Promise<IteratorResult<AgentEvent>>((resolve) =>
            this.readers.push(resolve),
          );
        },
      }),
    };
  }
  async stop(_reason: TerminationReason): Promise<void> {
    this.stopPromise ??= this.close();
    return this.stopPromise;
  }

  private async close(): Promise<void> {
    const alreadyFailed = this.failed;
    this.stopped = true;
    for (const reject of this.persistenceWaiters) reject(new Error(`${this.sdkName} stopped`));
    this.persistenceWaiters.clear();
    this.opts.llmGatewayJwtProvider?.close();
    this.input?.mcpJwtProvider?.close();
    try {
      await this.starting?.catch(() => {});
      await this.interrupt();
      await this.worker?.close();
      await this.active?.catch(() => undefined);
      await this.eventTail.catch((error) => {
        if (!alreadyFailed) throw error;
      });
    } finally {
      await Promise.allSettled(this.clients.map((client) => client.close()));
      if (this.root) await rm(this.root, { recursive: true, force: true });
      this.wakeIdle();
      for (const reader of this.readers.splice(0)) reader({ value: undefined, done: true });
    }
  }
}

function object(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}
function messageText(value: unknown): string | undefined {
  const payload = object(value);
  if (typeof payload.content === 'string') return payload.content || undefined;
  if (!Array.isArray(payload.content) || !payload.content.length) return undefined;
  if (
    payload.content.some(
      (block) => object(block).type !== 'text' || typeof object(block).text !== 'string',
    )
  )
    return undefined;
  return payload.content.map((block) => object(block).text).join('\n') || undefined;
}
