// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { assertSdkTerminalUsage as assertCodexTerminalUsage } from '@orca/sdk-harness';
import { stableSessionThreadId, AgentThreadEventKind } from '@orca/agent-event-contract';
import { DshEventProjection, nativeThreadId } from './dsh-events.js';
import { assertDshCheckpointInstructions } from '@orca/dsh-harness';
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
import { validateHarnessGuardrails, validateHarnessModel } from '@orca/harness-catalog';
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
import {
  SessionJwtLifetimeError,
  type SessionJwtProvider,
} from '../../mcp/session-jwt-provider.js';
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
  harness?: 'codex_sdk' | 'pi_sdk' | 'dsh_sdk';
  apiKey: string;
  baseUrl?: string;
  piGatewayUrl?: string;
  dshGatewayUrl?: string;
  checkpoint?: CodexCheckpoint;
  turns: CodexTurnStore;
  llmGatewayJwtProvider?: Pick<SessionJwtProvider, 'getValidToken' | 'close'>;
  createWorker?: (emit: (event: WorkerEvent) => void, input: SessionStartInput) => Worker;
  turnTimeoutMs?: number;
  /** Owner-local read path; the ordered source retains its own delivery/ACK. */
  rootInterrupts?: (
    turn: UserEvent,
    signal: AbortSignal,
    onReady: () => void,
  ) => AsyncIterable<{ event: UserEvent; hooks?: SubmitHooks }>;
  onGuardrailState?: ClaudeHarnessOptions['onGuardrailState'];
  onUsage?: ClaudeHarnessOptions['onUsage'];
  refreshGuardrailSubjectWindow?: ClaudeHarnessOptions['refreshGuardrailSubjectWindow'];
}
interface Binding {
  tool: Tool;
  policyName: string;
  serverName?: string;
  custom?: boolean;
  call(args: Record<string, unknown>, options?: { signal?: AbortSignal }): Promise<CallToolResult>;
}
interface PendingAction {
  subpath: string;
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
  private dshProjection: DshEventProjection | undefined;
  private cancelledToolCalls = new Set<string>();
  private cancelledSubpaths = new Set<string>();
  private toolOrigins = new Map<string, string>();
  private callControllers = new Map<string, AbortController>();
  private archivedSubpaths = new Set<string>();
  private orchestrationCalls = new Set<string>();
  private nativeRootId: string | undefined;
  private executingTools = new Set<string>();
  private uncertainCancellations = new Set<string>();
  private terminalUsageEvent: AgentEventInput | undefined;
  private guards: PreparedGuardrail[] = [];
  private guardrailStore = new InMemoryGuardrailStateStore();
  private guarded = false;
  private accountingBlocked = false;
  private interruptVersion = 0;
  private readonly rootControls = new Map<string, Promise<void>>();
  private controlAbort: AbortController | undefined;
  private submitTail: Promise<unknown> = Promise.resolve();
  private usageEventId: string | undefined;
  private usageAcknowledged = false;
  private checkpoint: CodexCheckpoint | undefined;
  private receipt: HarnessTurnReceipt | null = null;
  private turnStateTail: Promise<unknown> = Promise.resolve();
  private readonly persistenceWaiters = new Set<(error: unknown) => void>();
  private active: Promise<void> | undefined;
  private execution: Promise<void> | undefined;
  private starting: Promise<void> | undefined;
  private stopPromise: Promise<void> | undefined;
  private abort: AbortController | undefined;
  private eventTail: Promise<void> = Promise.resolve();
  private toolCalls = new Set<Promise<void>>();
  private inFlight = new Map<string, Promise<void>>();
  private idleWaiters = new Set<() => void>();
  private queue: AgentEvent[] = [];
  private readers: Array<(event: IteratorResult<AgentEvent>) => void> = [];
  private stopped = false;
  private failed = false;
  private turnRetired = false;
  private rootCancellation: Promise<void> | undefined;
  private turnFailure: string | undefined;
  private turnId: string | undefined;
  private usage: ModelUsageCounts = {
    input_tokens: 0,
    output_tokens: 0,
    cache_read_input_tokens: 0,
    cache_creation_input_tokens: 0,
  };

  constructor(private readonly opts: CodexSdkHarnessOptions) {}

  get supportsNativeThreadInterrupt(): boolean {
    return this.opts.harness === 'dsh_sdk';
  }

  async applyArchivedThreadSubpaths(paths: string[]): Promise<void> {
    if (!this.dshProjection) return;
    const additions = paths.filter((path) => !this.archivedSubpaths.has(path));
    for (const path of additions) nativeThreadId(path);
    for (const path of additions) this.archivedSubpaths.add(path);
    try {
      for (const path of additions) {
        // A rolled-back checkpoint may not contain a later archived conversation.
        if (!this.dshProjection.subtree(path).includes(path)) continue;
        const threadId = nativeThreadId(path);
        if (!threadId) throw new Error('Root archive requires whole-tree retirement');
        await this.worker!.handle({ type: 'archive_fence', threadId });
      }
    } catch (cause) {
      await this.retireDshTree();
      throw new DurableHarnessStateError('DSH archive refresh was not acknowledged; tree retired', {
        cause,
      });
    }
  }

  private get sdkName(): string {
    return this.opts.harness === 'dsh_sdk'
      ? 'DSH SDK'
      : this.opts.harness === 'pi_sdk'
        ? 'Pi SDK'
        : 'Codex SDK';
  }
  private get harnessName(): string {
    return this.opts.harness ?? 'codex_sdk';
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
      (this.harnessName === 'codex_sdk' && input.agentSnapshot.model_provider !== 'openai') ||
      !input.agentSnapshot.model_provider ||
      !input.agentSnapshot.model_id
    )
      throw new Error(`${this.harnessName} requires a pinned supported model`);
    if (this.opts.harness === 'dsh_sdk') {
      if (!Array.isArray(input.archivedThreadSubpaths))
        throw new Error('DSH requires authoritative archived thread state');
      for (const subpath of input.archivedThreadSubpaths) {
        nativeThreadId(subpath);
        this.archivedSubpaths.add(subpath);
      }
      if (!this.opts.createWorker || !this.opts.dshGatewayUrl || !this.opts.llmGatewayJwtProvider)
        throw new Error('dsh_sdk requires colocated execution and scoped Gateway credentials');
      const error = validateHarnessModel(
        { harness: 'dsh_sdk' },
        {
          provider: input.agentSnapshot.model_provider,
          id: input.agentSnapshot.model_id,
          effort: input.agentSnapshot.model_effort,
        },
      );
      if (error) throw new Error(error);
      if (input.agentSnapshot.custom_tools?.length)
        throw new Error('dsh_sdk does not support custom client tools');
      if (input.agentSnapshot.allowed_tool_names?.includes('web_fetch')) {
        const policies = input.agentSnapshot.tool_permission_policies ?? {};
        if (
          (policies.mcp__orca__web_fetch ?? policies['mcp__orca__*'] ?? 'always_allow') !==
          'always_deny'
        )
          throw new Error('dsh_sdk requires agent_toolset web_fetch permission_policy always_deny');
      }
      this.dshProjection = new DshEventProjection(input.workspaceId, input.sessionId, [
        'spawn',
        'fork',
        'send_message',
        'interrupt_agent',
      ]);
    }
    if (this.opts.checkpoint)
      (this.opts.harness === 'dsh_sdk'
        ? assertDshCheckpointInstructions
        : this.opts.harness === 'pi_sdk'
          ? assertPiCheckpointInstructions
          : assertCodexCheckpointInstructions)(
        this.opts.checkpoint,
        input.agentSnapshot.system ?? '',
      );
    if (input.agentSnapshot.multiagent || input.clientToolExecution)
      throw new Error(`${this.harnessName} does not support multiagent or client tool execution`);
    const resolved = (input.guardrails ?? []).map((g) => {
      let rule = g.rule as PreparedGuardrail['guardrail']['rule'];
      if (
        this.opts.harness === 'dsh_sdk' &&
        rule.kind === 'builtin' &&
        rule.builtin === 'spawn_bounds'
      ) {
        const tools = rule.params?.dispatch_tools;
        if (
          Array.isArray(tools) &&
          tools.length &&
          tools.some((name) => name !== 'spawn' && name !== 'fork')
        )
          throw new Error('DSH spawn_bounds dispatch_tools must explicitly select spawn/fork');
        rule = {
          ...rule,
          params: {
            ...rule.params,
            dispatch_tools: Array.isArray(tools) && tools.length ? tools : ['spawn', 'fork'],
          },
        };
      }
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
      {
        harness: this.opts.harness ?? 'codex_sdk',
        mode: this.opts.harness === 'dsh_sdk' ? 'colocated' : 'separate',
      },
      resolved,
    );
    if (guardrailError) throw new Error(guardrailError);
    this.guarded = resolved.some((g) => g.stateful && g.stateScope !== 'turn');
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
      (this.opts.harness === 'dsh_sdk'
        ? assertDshCheckpointInstructions
        : this.opts.harness === 'pi_sdk'
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
        call: async (args, options) => {
          const result = await (options ? tool.execute(args, options) : tool.execute(args));
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
            call: async (args, options) =>
              (await client.callTool(
                { name: tool.name, arguments: args },
                undefined,
                options ?? (this.abort ? { signal: this.abort.signal } : {}),
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
      ...(this.opts.dshGatewayUrl ? { dshGatewayUrl: this.opts.dshGatewayUrl } : {}),
      ...(this.opts.harness === 'pi_sdk' && this.opts.piGatewayUrl
        ? { piGatewayUrl: this.opts.piGatewayUrl }
        : {}),
      ...(input.agentSnapshot.model_effort ? { effort: input.agentSnapshot.model_effort } : {}),
      ...(this.checkpoint ? { checkpoint: this.checkpoint } : {}),
      tools: [...this.bindings.values()]
        .filter((binding) => this.opts.harness !== 'dsh_sdk' || binding.tool.name !== 'web_fetch')
        .map((binding) => binding.tool),
      ...(this.dshProjection ? { archivedThreadSubpaths: [...this.archivedSubpaths] } : {}),
    });
    // Restored private lineage must be admitted before targeted controls or tools.
    if (this.dshProjection) await this.eventTail;
  }

  async submit(
    event: UserEvent,
    hooks?: SubmitHooks,
    followedTurnId?: string,
  ): Promise<UserEventSubmitResult> {
    if (this.stopped || !this.worker) throw new Error(`${this.sdkName} is unavailable`);
    if (followedTurnId && (this.turnId !== followedTurnId || this.receipt?.phase !== 'pending'))
      return 'submitted';
    let subpath = event.subpath ?? '';
    const payloadSubpath = object(event.payload).subpath;
    if (this.dshProjection && payloadSubpath !== undefined) {
      if (typeof payloadSubpath !== 'string' || (subpath && subpath !== payloadSubpath))
        throw new UnappliedUserEventError(
          event.kind,
          'Payload subpath conflicts with persisted thread address.',
        );
      if (!subpath && payloadSubpath) {
        if (
          !['user.tool_confirmation', 'user.custom_tool_result'].includes(event.kind) ||
          typeof object(event.payload).session_thread_id !== 'string'
        )
          throw new UnappliedUserEventError(
            event.kind,
            'Primary child reply requires a validated session_thread_id.',
          );
        subpath = payloadSubpath;
      }
    }
    const threadId =
      this.opts.harness === 'dsh_sdk'
        ? (nativeThreadId(subpath) ??
          (object(event.payload).stop_reason === 'archived' ? this.nativeRootId : undefined))
        : undefined;
    const publicThreadId = object(event.payload).session_thread_id;
    if (
      this.opts.harness === 'dsh_sdk' &&
      publicThreadId !== undefined &&
      publicThreadId !==
        stableSessionThreadId(this.input!.workspaceId, this.input!.sessionId, subpath)
    )
      throw new UnappliedUserEventError(
        event.kind,
        'Session thread address does not match the persisted subpath.',
      );
    if (event.kind === 'user.interrupt') {
      if (this.dshProjection && object(event.payload).stop_reason === 'archived' && !threadId) {
        this.archivedSubpaths.add(subpath);
        await this.retireDshTree();
        throw new DurableHarnessStateError(
          'DSH root archive fence requires native root identity; tree retired',
        );
      }
      if (threadId) {
        this.dshProjection!.parentSubpath(subpath);
        const archived = object(event.payload).stop_reason === 'archived';
        if (archived) {
          this.archivedSubpaths.add(subpath);
          try {
            await this.worker.handle({ type: 'archive_fence', threadId });
          } catch (error) {
            await this.retireDshTree();
            throw new DurableHarnessStateError(
              'DSH archive fence was not acknowledged; tree retired',
              { cause: error },
            );
          }
        }
        if (this.receipt?.phase === 'pending') await this.acceptSource(event);
        await hooks?.onAccepted();
        try {
          await this.cancelThreadTools(subpath);
          await this.worker.handle({
            type: 'interrupt',
            threadId,
            ...(archived ? { archived: true } : {}),
          });
        } catch (error) {
          throw new UnappliedUserEventError(
            event.kind,
            error instanceof Error ? error.message : 'Targeted cancellation could not be confirmed',
          );
        }
        if (this.receipt?.phase !== 'pending' && event.id)
          await this.emitPersisted({
            kind: SessionEventKind.statusIdle,
            payload: sessionIdlePayload('end_turn'),
            completionPolicy: { sourceIds: [event.id] },
          });
        return 'submitted';
      }
      if (event.id && this.rootControls.has(event.id)) {
        await this.rootControls.get(event.id);
        if (!followedTurnId) await this.active;
        return 'submitted';
      }
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
      await this.interruptPendingTurn(event, hooks);
      if (!followedTurnId) await this.active;
      return 'submitted';
    }
    if (event.kind === 'user.tool_confirmation' || event.kind === 'user.custom_tool_result') {
      const payload = object(event.payload);
      const id = (
        event.kind === 'user.custom_tool_result' ? payload.custom_tool_use_id : payload.tool_use_id
      ) as AgentEvent['id'];
      const pending = this.pending.get(id);
      if (
        !pending ||
        pending.kind !== event.kind ||
        (this.opts.harness === 'dsh_sdk' && pending.subpath !== subpath)
      )
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
    if (this.isArchived(subpath))
      throw new UnappliedUserEventError(event.kind, 'Archived thread cannot be activated.');
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
    this.turnRetired = false;
    this.eventTail = Promise.resolve();
    this.usage = {
      input_tokens: 0,
      output_tokens: 0,
      cache_read_input_tokens: 0,
      cache_creation_input_tokens: 0,
    };
    this.terminalUsageEvent = undefined;
    this.dshProjection?.beginTurn();
    this.cancelledToolCalls.clear();
    this.cancelledSubpaths.clear();
    this.orchestrationCalls.clear();
    this.uncertainCancellations.clear();
    this.rootControls.clear();
    this.rootCancellation = undefined;
    this.active = this.run(text, event.subpath ?? '', event).finally(() => {
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

  hasSettledFailure(): boolean {
    // Dispatcher replaces this tree even when unknown spend still blocks guarded admission.
    return this.opts.harness === 'dsh_sdk' && this.failed && this.receipt?.phase === 'settled';
  }

  async waitForFailedTurnSettlement(): Promise<void> {
    if (this.opts.harness !== 'dsh_sdk' || !this.failed) return;
    // Persistence ACKs are outside cancellation budgets; join their outcome before replacement.
    try {
      await this.active;
    } catch (error) {
      if (!(error instanceof SettledHarnessFailureError)) throw error;
    }
    if (!this.hasSettledFailure())
      throw new DurableHarnessStateError('DSH failed turn settlement was not acknowledged');
  }

  private async interruptPendingTurn(event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    if (event.id && this.rootControls.has(event.id)) return this.rootControls.get(event.id);
    const control = (async () => {
      await this.acceptSource(event);
      await hooks?.onAccepted();
      this.turnFailure ??= `${this.sdkName} turn interrupted`;
      await this.cancelRootTurn();
    })();
    if (event.id) this.rootControls.set(event.id, control);
    return control;
  }

  private cancelRootTurn(): Promise<void> {
    if (this.opts.harness !== 'dsh_sdk') return this.joinRootCancellation();
    return (this.rootCancellation ??= this.joinRootCancellation());
  }

  private async joinRootCancellation(): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const interrupt = this.interrupt();
      if (this.opts.harness !== 'dsh_sdk') await interrupt;
      else {
        await Promise.race([
          Promise.all([interrupt, this.execution]),
          new Promise<never>((_, reject) => {
            timer = setTimeout(
              () => reject(new Error('DSH root interrupt cleanup timed out')),
              15_000,
            );
          }),
        ]);
        if (this.uncertainCancellations.size)
          throw new Error('Root tool termination could not be confirmed');
      }
    } catch (error) {
      this.turnFailure += `; root interruption unconfirmed: ${error instanceof Error ? error.message : 'cleanup failed'}`;
      throw error;
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private async followRootInterrupts(
    turn: UserEvent,
    signal: AbortSignal,
    onReady: () => void,
  ): Promise<void> {
    for await (const { event, hooks } of this.opts.rootInterrupts!(turn, signal, onReady)) {
      if (signal.aborted || this.stopped || this.turnId !== turn.id) return;
      // The receipt stays pending through execution and follower join, until commit/abandon.
      if (this.receipt?.phase !== 'pending') return;
      await this.submit(event, hooks, turn.id);
    }
  }

  private async run(text: string, subpath: string, turn: UserEvent): Promise<void> {
    const controlAbort = new AbortController();
    this.controlAbort = controlAbort;
    let positioned!: () => void;
    const ready = new Promise<void>((resolve) => {
      positioned = resolve;
    });
    const controls = this.opts.rootInterrupts
      ? this.followRootInterrupts(turn, controlAbort.signal, positioned)
      : undefined;
    // Only failure wins this race; normal follower shutdown joins below.
    const controlFailure = controls?.then(() => {
      if (controlAbort.signal.aborted) throw new Error('DSH active control stopped');
      return new Promise<never>(() => {});
    });
    void controlFailure?.catch(() => {});
    this.emit({ kind: SessionEventKind.statusRunning, payload: {} });
    const start =
      this.opts.harness === 'dsh_sdk'
        ? undefined
        : this.emit({
            kind: SpanEventKind.modelRequestStart,
            payload: turnModelSummaryStartPayload({
              provider: this.input!.agentSnapshot.model_provider!,
              model: this.input!.agentSnapshot.model_id!,
            }),
          });
    let failDeadline!: (error: Error) => void;
    const deadlineFailure = new Promise<never>((_, reject) => {
      failDeadline = reject;
    });
    void deadlineFailure.catch(() => {});
    const timer = setTimeout(() => {
      this.turnFailure ??= `${this.sdkName} turn timed out`;
      if (this.opts.harness === 'dsh_sdk')
        void this.cancelRootTurn().then(
          () => failDeadline(new Error(this.turnFailure)),
          () => failDeadline(new Error(this.turnFailure)),
        );
      else void this.interrupt().catch(() => {});
    }, this.opts.turnTimeoutMs ?? 600_000);
    const execution = (async () => {
      if (controlFailure) await Promise.race([ready, controlFailure]);
      await this.worker!.refreshOptions({
        apiKey: await this.apiKey(),
        ...(this.opts.baseUrl ? { baseUrl: this.opts.baseUrl } : {}),
        ...(this.opts.dshGatewayUrl ? { dshGatewayUrl: this.opts.dshGatewayUrl } : {}),
        ...(this.opts.harness === 'pi_sdk' && this.opts.piGatewayUrl
          ? { piGatewayUrl: this.opts.piGatewayUrl }
          : {}),
      });
      if (!this.abort?.signal.aborted) {
        try {
          await this.worker!.handle({
            type: 'submit',
            text,
            ...(this.opts.harness === 'dsh_sdk'
              ? {
                  sourceEventId: this.turnId!,
                  ...(nativeThreadId(subpath) ? { threadId: nativeThreadId(subpath)! } : {}),
                }
              : {}),
          });
        } catch (error) {
          // An aborted SDK submit still needs the tool and event joins below.
          if (
            this.opts.harness !== 'dsh_sdk' ||
            !this.abort?.signal.aborted ||
            !(error instanceof Error) ||
            error.name !== 'AbortError'
          )
            throw error;
        }
      }
      await Promise.all(this.toolCalls);
      await this.eventTail;
    })();
    this.execution = execution;
    try {
      await Promise.race([execution, deadlineFailure, ...(controlFailure ? [controlFailure] : [])]);
      if (this.guarded && !this.usageAcknowledged)
        this.turnFailure ??= `${this.sdkName} terminal usage was not acknowledged`;
    } catch (error) {
      this.failed = true;
      this.turnFailure ??=
        error instanceof SessionJwtLifetimeError
          ? error.message
          : `${this.sdkName} execution or durable usage failed`;
    } finally {
      clearTimeout(timer);
      controlAbort.abort();
      await controls?.catch(() => {
        this.failed = true;
        this.turnFailure ??= 'DSH active control failed; tree retired without replay';
      });
      if (this.controlAbort === controlAbort) this.controlAbort = undefined;
      this.execution = undefined;
      this.abort?.abort();
      this.cancelActions();
    }
    if (this.opts.harness === 'dsh_sdk') this.turnRetired = true;
    if (this.stopped) return;
    // FIFO acknowledgment proves every preceding response event is durable before
    // native history may advance. A rejected fence leaves the receipt pending.
    if (this.opts.harness === 'dsh_sdk') {
      // Usage is the FIFO fence, not another aggregate model observation.
      if (!this.terminalUsageEvent) {
        this.turnFailure ??= 'DSH terminal usage is unavailable; spend remains unknown';
        this.checkpoint = undefined;
      } else {
        try {
          await this.emitPersisted(this.terminalUsageEvent);
        } catch (error) {
          await this.retireDshTree();
          throw error;
        }
      }
      if (this.turnFailure || !this.checkpoint) {
        this.turnFailure ??= 'DSH native checkpoint was unavailable; tree retired without replay';
        this.checkpoint = undefined;
        await this.retireDshTree();
      }
    } else
      await this.emitPersisted({
        kind: SpanEventKind.modelRequestEnd,
        payload: turnModelSummaryEndPayload({
          modelRequestStartId: start!.id,
          provider: this.input!.agentSnapshot.model_provider!,
          model: this.input!.agentSnapshot.model_id!,
          modelUsage: this.usage,
          isError: Boolean(this.turnFailure),
        }),
      });
    let committed;
    try {
      committed = this.checkpoint
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
    } catch (error) {
      if (this.opts.harness !== 'dsh_sdk') throw error;
      await this.retireDshTree();
      // ACK loss is not proof of rollback: inspect the authoritative receipt before abandoning.
      if (!this.opts.turns.inspect) throw error;
      committed = await this.opts.turns.inspect();
      if (committed.receipt?.turnId !== this.turnId) throw error;
      if (committed.receipt!.phase === 'pending')
        committed = await this.updateTurn({
          type: 'abandon',
          turnId: this.turnId!,
          error: 'DSH checkpoint commit was not confirmed; tree retired without replay',
        });
    }
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

  private async retireDshTree(): Promise<void> {
    this.failed = true;
    this.abort?.abort();
    this.cancelActions();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        Promise.all([this.worker?.close(), Promise.allSettled([...this.toolCalls])]),
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => reject(new Error('DSH tree retirement cleanup timed out')),
            15000,
          );
        }),
      ]);
    } catch (error) {
      // Failure settlement must not depend on known usage or successful cleanup. Admission stays blocked.
      this.turnFailure = `${this.turnFailure ?? 'DSH turn failed'}; retirement unconfirmed: ${error instanceof Error ? error.message : 'cleanup failed'}`;
    } finally {
      if (timer) clearTimeout(timer);
    }
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
    if (this.stopped || this.turnRetired) return;
    if (event.type === 'tool_call' || event.type === 'orchestration_request') {
      // Canonical declarations must finish validation before a managed tool runs.
      const call = (this.dshProjection ? this.eventTail : Promise.resolve())
        .then(() =>
          this.turnRetired || (this.dshProjection && this.abort?.signal.aborted)
            ? undefined
            : event.type === 'orchestration_request'
              ? this.authorizeOrchestration(event)
              : this.callTool(event),
        )
        .catch(() => {
          if (this.turnRetired) return;
          this.turnFailure ??= `${this.sdkName} tool relay failed`;
          return this.interrupt();
        })
        .finally(() => {
          this.toolCalls.delete(call);
          this.inFlight.delete(event.id);
        });
      this.toolCalls.add(call);
      void call.catch(() => {});
      this.inFlight.set(event.id, call);
    } else {
      this.eventTail = this.eventTail.then(async () => {
        if (this.turnRetired) return;
        if (event.type === 'thread_tree') {
          if (!this.dshProjection || this.turnId)
            throw new Error('DSH lineage restore requires startup');
          this.dshProjection.restoreTree(event.threads);
        } else if (event.type === 'canonical_event') {
          if (this.abort?.signal.aborted) return;
          if (!this.dshProjection || !this.turnId || this.receipt?.phase !== 'pending')
            throw new Error('Canonical worker events are DSH-only and require an active turn');
          this.emit(this.dshProjection.validate(event.event, this.turnId));
        } else if (event.type === 'checkpoint') {
          if (!this.dshProjection || !this.abort?.signal.aborted)
            this.checkpoint = event.checkpoint;
        } else if (event.type === 'failure') {
          this.turnFailure ??= event.message;
          if (event.fatal) this.failed = true;
        } else if (event.type === 'event') {
          const sdk = event.event;
          if (sdk.type === 'thread.started') this.nativeRootId = sdk.thread_id;
          else if (sdk.type === 'item.completed' && sdk.item.type === 'agent_message') {
            if (this.dshProjection && this.abort?.signal.aborted) return;
            const origin = this.dshProjection?.assertOrigin(event.origin);
            this.emit({
              ...(origin ? { id: origin.eventId, subpath: origin.subpath } : {}),
              kind: AgentEventKind.message,
              payload: {
                content: [{ type: 'text', text: sdk.item.text }],
                ...(origin ? { turn_event_id: this.turnId } : {}),
              },
            });
          } else if (sdk.type === 'turn.completed') {
            if (this.opts.harness === 'dsh_sdk' && this.terminalUsageEvent)
              throw new Error('DSH emitted duplicate tree usage');
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
            const usageEvent: AgentEventInput = {
              ...(this.usageEventId ? { id: this.usageEventId } : {}),
              kind: AgentEventKind.usage,
              payload: {
                usage,
                ...(this.guarded ? { guardrail_usage_recorded: true } : {}),
                model: this.input!.agentSnapshot.model_id,
                ...(this.turnId ? { turn_event_id: this.turnId } : {}),
              },
            };
            if (this.opts.harness === 'dsh_sdk') this.terminalUsageEvent = usageEvent;
            else this.emit(usageEvent);
          } else if (
            (sdk.type === 'turn.failed' || sdk.type === 'error') &&
            !this.abort?.signal.aborted
          )
            this.turnFailure ??= sdk.type === 'error' ? sdk.message : sdk.error.message;
          else if (
            sdk.type === 'item.started' &&
            ['command_execution', 'file_change', 'web_search'].includes(sdk.item.type)
          ) {
            this.turnFailure ??= `${this.sdkName} attempted an unmediated native tool`;
            this.failed = true;
            await this.interrupt();
          }
        }
      });
      // The active turn observes the original rejected tail before emitting its terminal event.
      void this.eventTail.catch(() => {
        this.failed = true;
        if (!this.turnRetired) void this.interrupt().catch(() => {});
      });
    }
  }

  private async callTool(event: Extract<WorkerEvent, { type: 'tool_call' }>): Promise<void> {
    const origin = this.dshProjection?.assertOrigin(event.origin);
    const subpath = origin?.subpath ?? '';
    if (this.cancelledSubpaths.has(subpath)) this.cancelledToolCalls.add(event.id);
    this.toolOrigins.set(event.id, subpath);
    const controller = new AbortController();
    this.callControllers.set(event.id, controller);
    const signal = this.abort
      ? AbortSignal.any([this.abort.signal, controller.signal])
      : controller.signal;
    const binding = this.bindings.get(event.name);
    let result: CallToolResult;
    const toolEvent = withCanonicalAgentEventEnvelope({
      ...(origin ? { id: origin.eventId, subpath } : {}),
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
      ...(origin
        ? {
            turn_event_id: this.turnId,
            subpath,
            parent_subpath: this.dshProjection!.parentSubpath(subpath),
            session_thread_id: stableSessionThreadId(
              this.input!.workspaceId,
              this.input!.sessionId,
              subpath,
            ),
          }
        : {}),
      ...(binding?.serverName ? { mcp_server_name: binding.serverName } : {}),
    };
    this.emit({ ...toolEvent, payload });
    try {
      if (
        !binding ||
        this.abort?.signal.aborted ||
        this.cancelledToolCalls.has(event.id) ||
        this.isArchived(subpath) ||
        (this.opts.harness === 'dsh_sdk' && event.name === 'web_fetch')
      )
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
        const confirmation = await this.requestAction(
          toolEvent.id,
          'user.tool_confirmation',
          subpath,
        );
        if (confirmation.result !== 'allow')
          throw new Error(
            typeof confirmation.deny_message === 'string'
              ? confirmation.deny_message
              : 'Tool denied by user',
          );
      }
      if (this.abort?.signal.aborted || this.cancelledToolCalls.has(event.id))
        throw new Error('Turn interrupted');
      if (binding.custom) {
        const reply = await this.requestAction(toolEvent.id, 'user.custom_tool_result');
        result = customToolResultToMcp(reply);
      } else {
        // TODO: Codex/Pi workers still need native call cancellation and a shell-termination regression.
        const sandboxExecutable =
          !binding.serverName && ['bash', 'glob', 'grep'].includes(event.name);
        if (this.dshProjection && sandboxExecutable && !this.input?.supportsPerCallCancellation)
          throw new Error('DSH executable tools require a sandbox with per-call cancellation');
        if (sandboxExecutable) this.executingTools.add(event.id);
        result = await binding.call(
          event.arguments,
          this.dshProjection && (!sandboxExecutable || this.input?.supportsPerCallCancellation)
            ? { signal }
            : undefined,
        );
        this.executingTools.delete(event.id);
      }
      if (this.cancelledToolCalls.has(event.id)) throw new Error('Thread interrupted');
      const outputVerdict = this.evaluate({
        phase: 'tool_result',
        tool: { name: binding.policyName, input: event.arguments },
        result: result.content,
      });
      if (outputVerdict.verdict !== 'allow') throw new Error('Tool output suppressed by policy');
    } catch (error) {
      if (
        this.executingTools.has(event.id) &&
        signal.aborted &&
        error instanceof Error &&
        error.name !== 'AbortError' &&
        ['bash', 'glob', 'grep'].includes(event.name)
      )
        this.uncertainCancellations.add(event.id);
      if (error instanceof CustomToolResultConversionError) {
        this.turnFailure = error.message;
        await this.interrupt();
      }
      result = {
        isError: true,
        content: [{ type: 'text', text: error instanceof Error ? error.message : 'Tool failed' }],
      };
    }
    if (!this.stopped && !this.turnRetired) {
      if (!binding?.custom)
        this.emit({
          subpath,
          kind: binding?.serverName ? AgentEventKind.mcpToolResult : AgentEventKind.toolResult,
          payload: {
            tool_use_id: toolEvent.id,
            content: result.content,
            is_error: result.isError ?? false,
            ...(origin
              ? {
                  turn_event_id: this.turnId,
                  subpath,
                  parent_subpath: this.dshProjection!.parentSubpath(subpath),
                }
              : {}),
            ...(binding?.serverName ? { mcp_server_name: binding.serverName } : {}),
          },
        });
      await this.worker!.handle({ type: 'tool_result', id: event.id, result });
    }
    this.toolOrigins.delete(event.id);
    this.executingTools.delete(event.id);
    this.callControllers.delete(event.id);
    this.cancelledToolCalls.delete(event.id);
  }

  private async authorizeOrchestration(
    event: Extract<WorkerEvent, { type: 'orchestration_request' }>,
  ): Promise<void> {
    let reason: string | undefined;
    const subpath = event.origin.subpath;
    this.toolOrigins.set(event.id, subpath);
    try {
      if (
        !this.dshProjection ||
        !this.turnId ||
        this.receipt?.phase !== 'pending' ||
        typeof event.id !== 'string' ||
        !event.id ||
        event.id.length > 256 ||
        this.orchestrationCalls.has(event.id)
      )
        throw new Error('Orchestration requires a unique active DSH call');
      this.orchestrationCalls.add(event.id);
      this.dshProjection.authorize(event);
      const args = { ...event.arguments };
      const dispatch = event.name === 'spawn' || event.name === 'fork';
      const fields = dispatch
        ? ['description', 'prompt']
        : event.name === 'send_message'
          ? ['agent_id', 'message']
          : ['agent_id'];
      if (
        !args ||
        typeof args !== 'object' ||
        Array.isArray(args) ||
        Object.keys(args).some((key) => !fields.includes(key)) ||
        fields.some((key) => typeof args[key] !== 'string' || !String(args[key]).trim()) ||
        Buffer.byteLength(JSON.stringify(args)) > 1024 * 1024
      )
        throw new Error('Unsupported DSH orchestration arguments');
      const target = dispatch
        ? undefined
        : args.agent_id === this.nativeRootId
          ? ''
          : `subagents/${String(args.agent_id)}`;
      if (target !== undefined) {
        nativeThreadId(target);
        if (target === subpath) throw new Error('DSH control cannot target the originating thread');
        const parent = this.dshProjection.parentSubpath(target);
        if (
          event.name === 'send_message'
            ? parent !== subpath && this.dshProjection.parentSubpath(subpath) !== target
            : !this.dshProjection.subtree(subpath).includes(target) || target === subpath
        )
          throw new Error('DSH control target is outside source authority');
      }
      const checkLive = () => {
        if (
          this.abort?.signal.aborted ||
          this.cancelledToolCalls.has(event.id) ||
          this.cancelledSubpaths.has(subpath) ||
          this.isArchived(subpath) ||
          (target !== undefined && this.isArchived(target)) ||
          this.receipt?.phase !== 'pending'
        )
          throw new Error('DSH call interrupted or thread archived');
      };
      checkLive();
      const binding: Binding = {
        tool: { name: event.name, inputSchema: { type: 'object' } },
        policyName: event.name,
        call: async () => {
          throw new Error('Native operation');
        },
      };
      const tool = { phase: 'tool_call' as const, tool: { name: event.name, input: args } };
      let verdict = this.evaluate(tool, this.permission(binding));
      if (verdict.verdict === 'deny')
        throw new Error(verdict.reasons.join(' ') || 'Tool denied by policy');
      if (verdict.verdict === 'ask') {
        const reply = await this.requestAction(
          event.origin.eventId as AgentEvent['id'],
          'user.tool_confirmation',
          subpath,
        );
        if (reply.result !== 'allow') throw new Error('Tool denied by user');
        checkLive();
        verdict = this.evaluate(tool);
        if (verdict.verdict === 'deny')
          throw new Error(verdict.reasons.join(' ') || 'Tool denied by policy');
      }
      checkLive();
      // No await between the last check and reservation: concurrent calls cannot consume the last slot twice.
      this.guardrailStore.apply(
        verdict.verdict === 'ask' ? verdict.intendedStateUpdates : verdict.stateUpdates,
      );
      if (event.name === 'interrupt_agent' && target) {
        await this.cancelThreadTools(target, event.id);
        checkLive();
      }
    } catch (error) {
      reason = error instanceof Error ? error.message : 'Orchestration denied';
    } finally {
      this.toolOrigins.delete(event.id);
    }
    if (!this.stopped)
      await this.worker!.handle({
        type: 'orchestration_result',
        id: event.id,
        allowed: !reason,
        ...(reason ? { reason } : {}),
      });
  }

  private async cancelThreadTools(subpath: string, excluding?: string): Promise<void> {
    const targets = new Set(this.dshProjection!.subtree(subpath));
    const calls: Promise<void>[] = [];
    const ids: string[] = [];
    if (
      !this.input?.supportsPerCallCancellation &&
      [...this.toolOrigins].some(
        ([id, origin]) =>
          targets.has(origin) && this.executingTools.has(id) && this.callControllers.has(id),
      )
    )
      throw new Error('Selected sandbox backend cannot confirm targeted tool cancellation');
    for (const target of targets) {
      this.cancelledSubpaths.add(target);
      this.cancelActions(target);
    }
    for (const [id, origin] of this.toolOrigins) {
      if (id === excluding || !targets.has(origin)) continue;
      this.cancelledToolCalls.add(id);
      this.callControllers.get(id)?.abort();
      ids.push(id);
      const call = this.inFlight.get(id);
      if (call) calls.push(call);
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        Promise.all(calls),
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () =>
              reject(new Error('Thread tool cancellation cleanup timed out; termination unknown')),
            15000,
          );
        }),
      ]);
      if (ids.some((id) => this.uncertainCancellations.has(id)))
        throw new Error('Targeted tool termination could not be confirmed');
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private isArchived(subpath: string): boolean {
    return [...this.archivedSubpaths].some(
      (archived) => archived === subpath || this.dshProjection?.subtree(archived).includes(subpath),
    );
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
    subpath = '',
  ): Promise<Record<string, unknown>> {
    return new Promise((resolve) => {
      if (this.abort?.signal.aborted) {
        resolve({ result: 'deny', content: [], is_error: true });
        return;
      }
      this.pending.set(id, { kind, resolve, subpath });
      if (this.dshProjection && subpath)
        this.emit({
          kind: AgentThreadEventKind.messageReceived,
          payload: {
            session_thread_id: stableSessionThreadId(
              this.input!.workspaceId,
              this.input!.sessionId,
              subpath,
            ),
            source_subpath: subpath,
            subpath,
            parent_subpath: this.dshProjection.parentSubpath(subpath),
            source_event_id: id,
            tool_use_id: id,
            content: [{ type: 'text', text: 'Child tool action requires confirmation.' }],
            turn_event_id: this.turnId,
          },
        });
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
  private cancelActions(subpath?: string): void {
    for (const [id, action] of this.pending) {
      if (subpath !== undefined && action.subpath !== subpath) continue;
      action.resolve({ result: 'deny', content: [], is_error: true });
      this.pending.delete(id);
    }
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
    this.controlAbort?.abort();
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
