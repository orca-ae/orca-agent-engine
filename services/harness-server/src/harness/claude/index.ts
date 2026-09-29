// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { v5 as uuidv5, v7 as uuidv7 } from 'uuid';
import {
  BUILTIN_EVALUATORS,
  InMemoryGuardrailStateStore,
  SHARED_USAGE_KEYS,
  evaluateGuardrails,
  isEnforced,
  maxVerdict,
  type GuardrailEvent,
  type PreparedGuardrail,
  type StateUpdate,
  type Verdict,
} from '@orca/guardrails';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  query,
  type McpSdkServerConfigWithInstance,
  type AgentDefinition,
  type PermissionResult,
  type Options,
  type SDKMessage,
  type SDKPartialAssistantMessage,
} from '@anthropic-ai/claude-agent-sdk';
import type { AgentEventId } from '@orca/agent-event-contract';
import type {
  AgentEvent,
  AgentEventInput,
  AgentHarness,
  ModelSpeed,
  RemoteMcpToolsetPolicy,
  SessionStartInput,
  SubmitHooks,
  TerminationReason,
  UserEvent,
  UserEventSubmitResult,
} from '../agent-harness.js';
import {
  GuardrailPolicyDeniedError,
  GuardrailUsageUnavailableError,
  runtimeAgentToolNameUnion,
  UnappliedUserEventError,
  withCanonicalAgentEventEnvelope,
} from '../agent-harness.js';
import { resolveClaudeSessionSpeed } from '../model-controls.js';
import { evaluateGuardrailExpression } from '../guardrail-expression.js';
import {
  AgentEventKind,
  SessionEventKind,
  SpanEventKind,
  sessionIdlePayload,
  sessionErrorPayload,
  sessionWarningPayload,
  turnModelSummaryEndPayload,
  turnModelSummaryStartPayload,
  type ModelUsageCounts,
  type OutcomeEvaluationResult,
} from '../event-kinds.js';
import type { SandboxHandle } from '../../sandbox/sandbox-runtime.js';
import { withMcpAuthorization } from '../../mcp/rewrite.js';
import type { SessionJwtProvider } from '../../mcp/session-jwt-provider.js';
import type { ClaudeAgentSdkAdapter } from './session-adapter.js';
import { buildOrcaSdkMcpServer, ORCA_MCP_SERVER_NAME, ORCA_MCP_TOOL_NAMES } from './mcp-tools.js';
import {
  createMessagesApiOutcomeEvaluator,
  type OutcomeEvaluator,
  type OutcomeCriterion,
  type OutcomeTranscriptTurn,
  type OutcomeVerdict,
} from '../outcome/evaluator.js';
import {
  ClaudeUsageTracker,
  normalizeModelCallUsage,
  awaitUsageAcknowledgment,
} from './usage-tracker.js';

/**
 * The Claude Agent SDK's `Options.sessionId` field requires a UUID-formatted
 * value (see `sdk.d.ts`: "Must be a valid UUID"). Orca session IDs are
 * `ses_<base32>` strings — not UUIDs. We derive a deterministic UUID v5 from
 * the orca session id so that:
 *
 *   1. Every turn for the same orca session resolves to the same Claude
 *      sessionId (the SDK keys its on-disk transcript by it; cross-turn
 *      consistency requires stability).
 *   2. The `SessionKey.sessionId` the SDK passes back to our `SessionStore`
 *      adapter is recoverable (we store both the orca id and the derived UUID
 *      on the adapter so `append` can route to the right transcript-store
 *      topic regardless of which key the SDK hands us).
 *
 * The namespace is fixed: a v5 UUID from any string under
 * `ORCA_CLAUDE_SESSION_NAMESPACE` is collision-resistant for our id space.
 */
const ORCA_CLAUDE_SESSION_NAMESPACE = 'ac15a075-8075-494b-9262-ae6a650ec031';

const CLAUDE_CODE_BUILT_IN_TOOLS = [
  'Bash',
  'Read',
  'Write',
  'Edit',
  'Glob',
  'Grep',
  'WebFetch',
  'WebSearch',
  'NotebookEdit',
  'TodoWrite',
];
const MCP_SERVER_NAME_ALLOWLIST_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;
type ManagedToolPermissionPolicy = 'always_allow' | 'always_ask' | 'always_deny';
type ManagedToolPermissionPolicies = Record<string, ManagedToolPermissionPolicy>;
// Bound the per-session conversation log (memory) and the slice sent to the
// outcome judge (prompt size / cost), so a long-lived session cannot grow either
// without limit.
const CONVERSATION_LOG_MAX_ENTRIES = 200;
const OUTCOME_TRANSCRIPT_MAX_ENTRIES = 40;
const DEFAULT_OUTCOME_MAX_ITERATIONS = 3;
const MAX_OUTCOME_MAX_ITERATIONS = 20;

interface PendingToolConfirmation {
  toolName: string;
  toolInput: Record<string, unknown>;
  toolUseID: string;
  approvedStateUpdates: readonly StateUpdate[];
  runtimeSubagentId?: string;
  resolve: (result: PermissionResult) => void;
}

interface PendingCustomToolResult {
  resolve: (result: UserCustomToolResultPayload) => void;
}

interface PendingAgentToolResult {
  resolve: (result: UserToolResultPayload) => void;
}

interface PreparedRequiredAction {
  isCurrent(): boolean;
  /**
   * Async because approving a guardrailed tool call flushes the state writes
   * that were withheld while the `ask` was outstanding, and the guarded action
   * is not released until Registry has acknowledged them.
   */
  apply(): void | Promise<void>;
}

interface PartialAssistantPreview {
  messageEventId?: string;
  textIndexByContentBlock: Map<number, number>;
  thinkingEventIdByContentBlock: Map<number, string>;
}

export interface UserCustomToolResultPayload {
  custom_tool_use_id: string;
  content?: unknown;
  result?: unknown;
  [key: string]: unknown;
}

export interface UserToolResultPayload {
  tool_use_id: string;
  content?: unknown;
  result?: unknown;
  is_error?: boolean | null;
  [key: string]: unknown;
}

function deriveClaudeSessionId(orcaSessionId: string): string {
  return uuidv5(orcaSessionId, ORCA_CLAUDE_SESSION_NAMESPACE);
}

export interface ClaudeHarnessOptions {
  /**
   * Called with the guardrail state a decision produced, so the runner can
   * persist it. The harness waits for this write-through before allowing the
   * guarded action, so a failed flush cannot silently turn a durable cap into
   * a process-local one.
   */
  onGuardrailState?: (updates: readonly StateUpdate[]) => Promise<void> | void;
  /** Refresh Registry-authenticated cross-session counters before request enforcement. */
  refreshGuardrailSubjectWindow?: (
    turnEventId: string,
  ) => Promise<Readonly<Record<string, unknown>>>;
  /** Persist one model-call usage delta and return authoritative shared totals. */
  onUsage?: (
    usage: {
      cache_creation: {
        ephemeral_1h_input_tokens: number;
        ephemeral_5m_input_tokens: number;
      };
      cache_read_input_tokens: number;
      input_tokens: number;
      output_tokens: number;
    },
    model: string,
    subagentId?: string,
    turnEventId?: string,
    usageEventId?: string,
  ) => Promise<Readonly<Record<string, unknown>> | undefined>;
  apiKey: string;
  baseURL?: string;
  /** Only gateway-selected separate Sessions receive a renewable model JWT. */
  llmGatewayJwtProvider?: Pick<SessionJwtProvider, 'getValidToken' | 'close'>;
  modelDefault: string;
  adapter: ClaudeAgentSdkAdapter;
  workspaceId: string;
  sessionId: string;
  /**
   * Outcome evaluator. Defaults to an Anthropic Messages API LLM judge
   * built from `apiKey`/`baseURL`/`modelDefault`; injectable for tests.
   */
  evaluateOutcome?: OutcomeEvaluator;
}

/**
 * Harness backed by the Claude Agent SDK.
 *
 * Lifecycle:
 *   start(input)      → captures agentSnapshot for model selection.
 *   submit(user.msg)  → calls SDK `query()` with resume=sessionId; the SDK
 *                       reads prior history via `sessionStore` (our adapter)
 *                       and mirrors new local transcript entries through
 *                       `sessionStore.append`.
 *                       Each yielded SDK assistant message is forwarded as
 *                       an `agent.message` AgentEvent through `events()`.
 *   stop(reason)      → drains any iterator consumers; aborts pending query.
 *   events()          → async iterable of AgentEvents emitted by the harness.
 *
 * The adapter is responsible for persisting both sides of the conversation;
 * the SessionRunner forwards the harness-emitted events (other than internal
 * usage signals) to transcript-store for the SSE stream. (The user.message is
 * already in transcript-store from the registry POST handler.)
 *
 * Integration notes:
 *   - Managed tools are exposed through MCP and permission-gated by canUseTool.
 *   - `apiKey` reaches the SDK subprocess as `ANTHROPIC_API_KEY`, unless the
 *     session routes model traffic through the gateway (`llmGatewayJwtProvider`).
 */
export class ClaudeAgentSdkHarness implements AgentHarness {
  private input: SessionStartInput | null = null;
  private mcpServers:
    | Record<string, { type: 'http'; url: string; headers: Record<string, string> }>
    | undefined;
  private remoteMcpToolsets: RemoteMcpToolsetPolicy[] | undefined;
  private mcpJwtProvider: SessionStartInput['mcpJwtProvider'];
  // Per-session sandbox backing the managed tools exposed through orca MCP.
  private sandbox: SandboxHandle | undefined;
  /**
   * In-process MCP server that exposes the per-session sandbox toolset to
   * the SDK. Built once at `start()` (the SandboxHandle is per-session and
   * stable for the harness's lifetime) so the SDK reuses the same instance
   * across turns. See `mcp-tools.ts` for the full rationale.
   */
  private orcaMcpServer: McpSdkServerConfigWithInstance | undefined;
  private outQueue: AgentEvent[] = [];
  private outResolvers: Array<(e: IteratorResult<AgentEvent>) => void> = [];
  private requiresActionResolvers: Array<() => void> = [];
  private pendingToolConfirmations = new Map<string, PendingToolConfirmation>();
  private pendingCustomToolResults = new Map<string, PendingCustomToolResult>();
  private pendingAgentToolResults = new Map<string, PendingAgentToolResult>();
  /** Accepted system.message context that applies to this and later turns. */
  private sessionSystemMessages: string[] = [];
  /** SDK tool-use ids whose public lifecycle is carried by client result events. */
  private clientExecutedSdkToolUseIds = new Set<string>();
  /** Qualified SDK names for declared client-executed custom tools. */
  private customSdkToolNames = new Set<string>();
  private clientToolExecution = false;
  private terminated = false;
  private currentAbort: AbortController | null = null;
  private currentQuery: Promise<void> | null = null;
  private pendingUsageReports: Array<{
    usage: NonNullable<ReturnType<typeof usageFromSdkResult>>;
    model: string;
    subagentId?: string;
    turnEventId?: string;
    usageEventId: string;
  }> = [];
  /** Resume-store probe/setup phase before `currentQuery` becomes available. */
  private queryStartup: Promise<void> | null = null;
  /**
   * Per-turn map of SDK tool_use_id -> remote MCP server name, populated when a
   * tool call is surfaced (assistant content or the permission gate). Used to
   * classify the matching `tool_result` frame as `agent.mcp_tool_result` vs
   * `agent.tool_result`. Cleared at the start of each turn.
   */
  private mcpServerByToolUseId = new Map<string, string>();
  /** SDK tool-use id -> public agent.tool_use / agent.mcp_tool_use event id. */
  private publicEventIdByToolUseId = new Map<string, string>();
  private guardrailToolCallByToolUseId = new Map<
    string,
    {
      toolName: string;
      input: Record<string, unknown>;
      subagentId?: string;
      modelId?: string;
    }
  >();
  /**
   * Active SDK stream scope -> managed-agents preview state. Claude's partial
   * frames and the buffered assistant frame share session_id +
   * parent_tool_use_id; wrapper UUIDs are not a message correlation key.
   */
  private partialAssistantPreviews = new Map<string, PartialAssistantPreview>();
  /** SDK runtime subagent id -> configured AgentDefinition key for the active turn. */
  private runtimeSubagentTypeById = new Map<string, string>();
  /** Parent Agent tool-use id -> persistent managed subagent id for usage attribution. */
  private subagentManagedAgentIdByParentToolUseId = new Map<string, string>();
  /** Configured AgentDefinition key -> that managed child's own permission policies. */
  private subagentPermissionPoliciesByType = new Map<string, ManagedToolPermissionPolicies>();
  /** Configured AgentDefinition key -> persistent managed Agent id. */
  private subagentManagedAgentIdByType = new Map<string, string>();
  private subagentModelIdByType = new Map<string, string>();
  /** Guardrails composed for this session, ordered by authority. */
  private preparedGuardrails: PreparedGuardrail[] = [];
  /**
   * Guardrail counters for this session.
   *
   * Seeded from the prepared runtime so a respawn resumes where the previous
   * runner left off — a cap that reset on restart could be cleared by forcing
   * one.
   */
  private guardrailStore = new InMemoryGuardrailStateStore();
  /** Outcome criteria defined via user.define_outcome, keyed by outcome id. */
  private definedOutcomes = new Map<string, OutcomeCriterion>();
  /** Rolling user/agent text log fed to the outcome judge. */
  private conversationLog: OutcomeTranscriptTurn[] = [];
  private outcomeEvaluator: OutcomeEvaluator | null;
  /** Set true when the current turn closed with a successful result frame. */
  private turnSucceeded = false;
  /** Session-wide SDK fast mode resolved from primary + subagent snapshots. */
  private modelSpeed: ModelSpeed = 'standard';
  /** Ephemeral local mirror required by Claude Agent SDK 0.3 SessionStore. */
  private claudeConfigDir: string | undefined;
  /** Start-envelope ID for the current coarse turn model summary. */
  private currentModelSummaryStartId: AgentEventId | null = null;
  private usageTracker: ClaudeUsageTracker | undefined;
  private currentTurnEventId: string | undefined;

  constructor(private readonly opts: ClaudeHarnessOptions) {
    this.outcomeEvaluator = opts.evaluateOutcome ?? null;
  }

  async start(input: SessionStartInput): Promise<void> {
    this.modelSpeed = resolveClaudeSessionSpeed(input.agentSnapshot);
    if (!this.outcomeEvaluator && (this.opts.apiKey || this.opts.llmGatewayJwtProvider)) {
      this.outcomeEvaluator = createMessagesApiOutcomeEvaluator({
        apiKey: this.opts.apiKey,
        ...(this.opts.baseURL ? { baseURL: this.opts.baseURL } : {}),
        ...(this.opts.llmGatewayJwtProvider
          ? {
              gatewayAuth: {
                sessionId: this.opts.sessionId,
                getToken: async () =>
                  (await this.opts.llmGatewayJwtProvider!.getValidToken()).token,
              },
            }
          : {}),
        model: input.agentSnapshot.model_id ?? this.opts.modelDefault,
        speed: this.modelSpeed,
        ...(input.agentSnapshot.model_effort ? { effort: input.agentSnapshot.model_effort } : {}),
      });
    }
    this.input = input;
    // Seed guardrails and their counters from the prepared runtime. Restoring
    // state here rather than lazily means the first tool call of a respawned
    // session already sees the totals the previous runner reached.
    this.preparedGuardrails = (input.guardrails ?? []).map((g) => ({
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
    }));
    this.reportUnenforceableGuardrails();
    this.guardrailStore = new InMemoryGuardrailStateStore(
      input.guardrailState ? { session: { ...input.guardrailState } } : {},
    );
    this.mcpServers = input.mcpServers;
    this.mcpJwtProvider = input.mcpJwtProvider;
    this.remoteMcpToolsets = input.remoteMcpToolsets;
    this.sandbox = input.sandbox;
    this.clientToolExecution = input.clientToolExecution === true;
    const customTools = input.agentSnapshot.custom_tools ?? [];
    this.customSdkToolNames = new Set(
      customTools.map((customTool) => `mcp__${ORCA_MCP_SERVER_NAME}__${customTool.name}`),
    );
    if (input.sandbox || customTools.length > 0 || this.clientToolExecution) {
      // Bind the SDK's tool dispatch to the per-session sandbox. Without
      // this, the SDK's built-in `Bash`/`Read`/`Edit`/… tools execute on
      // the harness host's filesystem, NOT under `srt`'s OS-level sandbox —
      // so the model can side-step every mount + the sandbox's
      // network/filesystem allow-list, and "summarize /mnt/lorem.txt"
      // ENOENTs because `materializeResources` wrote to the work-dir but
      // SDK Read read from `process.cwd()`.
      this.orcaMcpServer = buildOrcaSdkMcpServer(
        input.sandbox,
        runtimeAgentToolNameUnion(input.agentSnapshot),
        customTools,
        (name, toolInput) => this.requestCustomToolUse(name, toolInput).result,
        this.clientToolExecution
          ? (name, toolInput) => this.requestAgentToolUse(name, toolInput).result
          : undefined,
      );
    }
    this.claudeConfigDir = await mkdtemp(join(tmpdir(), 'orca-claude-sdk-'));
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

  hasPendingGuardrailUsage(): boolean {
    return this.pendingUsageReports.length > 0;
  }

  async submit(event: UserEvent, hooks?: SubmitHooks): Promise<UserEventSubmitResult | void> {
    if (this.terminated) return 'deferred';
    await this.flushPendingUsageReports();
    if (event.kind === 'user.tool_confirmation') {
      const prepared = this.prepareToolConfirmation(event.payload);
      if (!prepared) {
        throw new UnappliedUserEventError(
          event.kind,
          'No pending tool confirmation matches this user.tool_confirmation event.',
        );
      }
      await this.acceptRequiredAction(prepared, hooks);
      return 'submitted';
    }
    if (event.kind === 'user.custom_tool_result') {
      const systemMessage = textOf(event.systemMessage);
      const prepared = this.prepareCustomToolResult(event.payload, systemMessage);
      if (!prepared) {
        throw new UnappliedUserEventError(
          event.kind,
          'No pending custom tool use matches this user.custom_tool_result event.',
        );
      }
      await this.acceptRequiredAction(prepared, hooks, () =>
        this.rememberSystemMessage(systemMessage),
      );
      return 'submitted';
    }
    if (event.kind === 'user.tool_result') {
      const systemMessage = textOf(event.systemMessage);
      const prepared = this.prepareAgentToolResult(event.payload, systemMessage);
      if (!prepared) {
        throw new UnappliedUserEventError(
          event.kind,
          'No pending agent tool use matches this user.tool_result event.',
        );
      }
      await this.acceptRequiredAction(prepared, hooks, () =>
        this.rememberSystemMessage(systemMessage),
      );
      return 'submitted';
    }
    if (event.kind === 'user.interrupt') {
      await hooks?.onAccepted();
      this.currentAbort?.abort();
      return 'submitted';
    }
    if (event.kind === 'user.define_outcome') {
      if (this.currentQuery || this.queryStartup) {
        await this.waitForQueryIdleOrDone();
        if (this.currentQuery || this.queryStartup || this.hasPendingRequiredAction()) {
          throw new UnappliedUserEventError(
            event.kind,
            'Cannot apply user.define_outcome while the session is waiting for another action.',
          );
        }
      }
      const criterion = this.outcomeCriterionFromPayload(event.payload);
      if (!criterion) {
        throw new UnappliedUserEventError(
          event.kind,
          'user.define_outcome requires description and rubric.',
        );
      }
      await hooks?.onAccepted();
      this.guardrailStore.resetTurn();
      this.currentTurnEventId = event.id;
      await this.refreshGuardrailSubjectWindow();
      this.definedOutcomes.set(criterion.id, criterion);
      const text = outcomePrompt(criterion);
      await this.evaluateRequestGuardrails(text);
      this.pushConversation({ role: 'user', text });
      await this.runQuery(text);
      return 'submitted';
    }
    if (event.kind !== 'user.message') return;
    const text = textOf(event.payload);
    if (!text) {
      throw new UnappliedUserEventError(
        event.kind,
        'user.message does not contain supported non-empty text content.',
      );
    }
    if (this.currentQuery || this.queryStartup) {
      await this.waitForQueryIdleOrDone();
      if (this.currentQuery || this.queryStartup || this.hasPendingRequiredAction()) {
        return 'deferred';
      }
    }

    await hooks?.onAccepted();
    this.guardrailStore.resetTurn();
    this.currentTurnEventId = event.id;
    await this.refreshGuardrailSubjectWindow();
    await this.evaluateRequestGuardrails(text);
    this.rememberSystemMessage(textOf(event.systemMessage));
    this.pushConversation({ role: 'user', text });
    await this.runQuery(text);
    return 'submitted';
  }

  private async runQuery(text: string): Promise<void> {
    if (this.terminated || this.currentQuery || this.queryStartup) return;
    this.runtimeSubagentTypeById.clear();
    this.subagentManagedAgentIdByParentToolUseId.clear();
    this.guardrailToolCallByToolUseId.clear();

    let finishStartup!: () => void;
    const startup = new Promise<void>((resolve) => {
      finishStartup = resolve;
    });
    this.queryStartup = startup;
    const completeStartup = (): void => {
      finishStartup();
      if (this.queryStartup === startup) this.queryStartup = null;
    };

    try {
      const env: Record<string, string> = {};
      if (this.opts.apiKey && !this.opts.llmGatewayJwtProvider) {
        env['ANTHROPIC_API_KEY'] = this.opts.apiKey;
      }
      if (this.opts.baseURL) env['ANTHROPIC_BASE_URL'] = this.opts.baseURL;
      if (this.claudeConfigDir) env['CLAUDE_CONFIG_DIR'] = this.claudeConfigDir;
      const remoteMcpToolsetNames = this.remoteMcpToolsets?.map((t) => t.serverName) ?? [];
      if (remoteMcpToolsetNames.length > 0 && !process.env['ENABLE_TOOL_SEARCH']) {
        // Remote MCP servers selected by managed-agents should expose their
        // concrete `mcp__<server>__<tool>` names up front. Tool-search deferral
        // can leave the model without the exact remote tool name on first turn.
        env['ENABLE_TOOL_SEARCH'] = 'false';
      }

      // The SDK's `Options.sessionId` requires a UUID; orca session ids are
      // `ses_<base32>`. Derive a deterministic v5 UUID and register the
      // (sdk-uuid → orca-id) mapping with the adapter so its append/load calls
      // route to the correct transcript-store topic.
      const sdkSessionId = deriveClaudeSessionId(this.opts.sessionId);
      this.opts.adapter.registerSession(sdkSessionId, this.opts.sessionId);

      // Session continuity. The SDK's CLI subprocess CREATES a session
      // for `sessionId` on the first turn and FAILS — "Claude Code process exited
      // with code 1" — if a later turn asks it to create the same id again. So
      // only the first turn may pass `sessionId` alone; every subsequent turn
      // must `resume`. With `sessionStore` set, `resume` makes the SDK rebuild
      // history via `sessionStore.load()` and the CLI resumes the existing
      // session instead of re-creating it (`continue` is not an option — it needs
      // `sessionStore.listSessions()`, which we deliberately omit).
      //
      // We key the create-vs-resume decision off STORE STATE — whether the
      // session already holds a persisted SDK transcript entry — not an in-memory
      // turn counter, so a cold-started or respawned runner (idle reap, session
      // update) that attaches to a session with prior turns also resumes
      // correctly rather than re-creating and exiting 1.
      //
      // Trade-off: a pure-SDK feature like Claude Code's "/resume" history browser
      // won't see our sessions (they live in transcript-store, not in
      // `~/.claude/projects/*`). That's fine — the SSE stream is the canonical
      // replay surface, not the SDK's local transcript.
      let resumeSessionId: string | undefined;
      try {
        if (await this.opts.adapter.hasClaudeTranscript(this.opts.sessionId)) {
          resumeSessionId = sdkSessionId;
        }
      } catch (e) {
        console.error(
          `claude harness: resume probe failed for ${this.opts.sessionId}; cannot safely decide create vs resume`,
          e,
        );
        throw new Error(
          `claude harness: resume probe failed for ${this.opts.sessionId}; cannot safely decide create vs resume`,
        );
      }
      if (this.terminated) return;

      this.currentAbort = new AbortController();
      const queryAbort = this.currentAbort;
      if (this.opts.llmGatewayJwtProvider) {
        try {
          const { token } = await this.opts.llmGatewayJwtProvider.getValidToken(queryAbort.signal);
          env['ANTHROPIC_AUTH_TOKEN'] = token;
          env['ANTHROPIC_CUSTOM_HEADERS'] = `X-Orca-Session-Id: ${this.opts.sessionId}`;
        } catch (error) {
          if (this.terminated) return;
          const interrupted =
            queryAbort.signal.aborted &&
            error instanceof DOMException &&
            error.name === 'AbortError';
          if (!interrupted) {
            queryAbort.abort();
            if (this.currentAbort === queryAbort) this.currentAbort = null;
            throw error;
          }
        }
        if (queryAbort.signal.aborted) {
          if (this.currentAbort === queryAbort) this.currentAbort = null;
          this.emit({
            kind: SessionEventKind.statusIdle,
            payload: sessionIdlePayload('end_turn'),
          });
          return;
        }
      }
      // Runner startup can precede this query by minutes (sandbox setup, idle
      // time, or earlier turns). A startup header is not a renewable credential.
      if (this.mcpServers && this.mcpJwtProvider) {
        try {
          const auth = await this.mcpJwtProvider.getValidToken(queryAbort.signal);
          if (!this.terminated && !queryAbort.signal.aborted) {
            this.mcpServers = withMcpAuthorization(this.mcpServers, auth.token);
          }
        } catch (error) {
          // Interrupt cancels only this wait, not the shared session mint.
          // A genuine refresh failure (or stop) still follows the failure path.
          const interrupted =
            queryAbort.signal.aborted &&
            error instanceof DOMException &&
            error.name === 'AbortError';
          if (!interrupted || this.terminated) {
            queryAbort.abort();
            if (this.currentAbort === queryAbort) this.currentAbort = null;
            throw error;
          }
        }
        if (this.terminated || queryAbort.signal.aborted) {
          if (this.currentAbort === queryAbort) this.currentAbort = null;
          if (!this.terminated) {
            // No SDK query/model span exists yet, but the accepted user turn
            // needs an idle terminal so its durable completion can be recorded.
            this.emit({
              kind: SessionEventKind.statusIdle,
              payload: sessionIdlePayload('end_turn'),
            });
          }
          return;
        }
      }
      const usageTurnEventId = this.currentTurnEventId;
      let usageMirrorFailure: unknown;
      const usageTracker = new ClaudeUsageTracker(
        this.input?.agentSnapshot?.model_id ?? this.opts.modelDefault,
        (usage, model, parentToolUseId, subagentType) =>
          this.emitUsage(
            usage,
            model,
            (parentToolUseId
              ? this.subagentManagedAgentIdByParentToolUseId.get(parentToolUseId)
              : undefined) ??
              (subagentType ? this.subagentManagedAgentIdByType.get(subagentType) : undefined),
            queryAbort.signal,
            usageTurnEventId,
          ),
        (this.input?.agentSnapshot.multiagent?.agents.length ?? 0) > 0,
      );
      this.usageTracker = usageTracker;
      queryAbort.signal.addEventListener('abort', () => usageTracker.close(), { once: true });

      const options: Options = {
        sessionStore: {
          append: async (key, entries) => {
            try {
              await usageTracker.observeStoredEntries(key, entries);
            } catch (error) {
              // The SDK retries/drops mirror failures independently. Usage ACK
              // failure must instead stop the query and keep tools fail-closed.
              usageMirrorFailure = error;
              queryAbort.abort();
              throw error;
            }
            await this.opts.adapter.append(key, entries);
          },
          load: (key) => this.opts.adapter.load(key),
          listSubkeys: (key) => this.opts.adapter.listSubkeys(key),
        },
        sessionStoreFlush: 'eager',
        // First turn CREATES the session via a deterministic `sessionId`; every
        // later turn RESUMES it. `sessionId` and `resume` are MUTUALLY EXCLUSIVE:
        // the CLI rejects them together — "--session-id can only be used with
        // --continue or --resume if --fork-session is also specified" — and
        // passing both is exactly what made multi-turn exit 1. `resume`
        // already carries the same derived id (the SDK sets its session id from
        // it), so cross-turn continuity and the transcript-store topic stay
        // consistent.
        ...(resumeSessionId ? { resume: resumeSessionId } : { sessionId: sdkSessionId }),
        model: this.input?.agentSnapshot?.model_id ?? this.opts.modelDefault,
        ...(this.input?.agentSnapshot?.model_effort
          ? { effort: this.input.agentSnapshot.model_effort }
          : {}),
        settings: { fastMode: this.modelSpeed === 'fast' },
        // SDK auto-title generation is a separate structured-output model call
        // without Messages API speed. Local SDK titles are not an Orca durability
        // surface, so pin one and avoid that hidden request.
        title: this.opts.sessionId,
        abortController: this.currentAbort,
        ...combinedSystemPrompt(
          this.input?.agentSnapshot?.system,
          this.sessionSystemMessages.join('\n\n'),
        ),
        // Managed-agent permission policy is enforced via `canUseTool`.
        // Headless bypass would skip `always_ask` policies entirely.
        permissionMode: 'default',
        // Managed Agents exposes opt-in event_start/event_delta previews. The
        // registry filters these per SSE connection; the harness must always ask
        // the SDK for partial frames so one producer can serve mixed subscribers.
        includePartialMessages: true,
        // Prompt suggestions issue a separate structured-output model request that
        // does not inherit Messages API speed. Managed turns expose no suggestion
        // event, so disable this typed SDK feature rather than allow hidden standard
        // inference beside a fast turn.
        promptSuggestions: false,
        // External Claude Code settings can contain allow rules that the SDK
        // applies before `canUseTool`. Managed-agent policy must be the only
        // permission source for harness turns.
        settingSources: [],
        canUseTool: (toolName, toolInput, permissionOptions) =>
          this.canUseManagedAgentTool(toolName, toolInput, permissionOptions, usageTracker),
        // Surface the Claude Code subprocess stderr, which the SDK otherwise
        // swallows behind a bare "process exited with code 1" — essential for
        // diagnosing turn failures (this is how the multi-turn exit 1 was traced).
        stderr: (data: string) => {
          console.error(`claude-cli stderr [${this.opts.sessionId}]: ${data}`);
        },
      };
      // Anchor every Claude Code subprocess (and its built-in tool calls) at
      // the per-session sandbox work-dir. Without this, built-in `Bash`/`Read`
      // run from `process.cwd()` of the harness — completely outside the
      // sandbox. With this, `pwd` from a built-in bash call returns the
      // sandbox dir, `cat /mnt/lorem.txt` resolves against the materialized
      // mount, and `srt`'s OS-level FS allow-list (when LocalSandboxRuntime is
      // active) bounds what the host process can actually touch.
      // Anchor the Claude Code subprocess at the per-session sandbox
      // work-dir. The orca MCP server is the model's primary tool surface
      // (see `mcp-tools.ts`), but the SDK's own subprocess + any tooling it
      // spawns inherits `cwd` — so even probes / diagnostics it might run
      // land inside the sandbox tree rather than the harness's CWD.
      if (this.sandbox && typeof (this.sandbox as { rootDir?: unknown }).rootDir === 'function') {
        options.cwd = (this.sandbox as unknown as { rootDir: () => string }).rootDir();
      }
      if (Object.keys(env).length > 0) {
        options.env = env;
      }
      // Merge the orca SDK MCP server (per-session sandbox tools) with any
      // remote MCP servers configured by the agent (e.g. via ai-gateway). The
      // SDK accepts both shapes in the same `mcpServers` map: HTTP/SSE configs
      // for cross-process servers and our in-process `type:'sdk'` config for
      // the orca server.
      const mergedMcpServers: Record<string, unknown> = {};
      if (this.orcaMcpServer) {
        mergedMcpServers[ORCA_MCP_SERVER_NAME] = this.orcaMcpServer;
      }
      if (this.mcpServers) {
        for (const [name, cfg] of Object.entries(this.mcpServers)) {
          if (name === ORCA_MCP_SERVER_NAME) continue; // reserved
          mergedMcpServers[name] = cfg;
        }
      }
      if (Object.keys(mergedMcpServers).length > 0) {
        options.mcpServers = mergedMcpServers as NonNullable<Options['mcpServers']>;
        options.strictMcpConfig = true;
      }
      const builtSubagents = buildSubagentDefinitions(this.input?.agentSnapshot.multiagent);
      const subagents = builtSubagents.definitions;
      this.subagentPermissionPoliciesByType = builtSubagents.permissionPoliciesByType;
      this.subagentManagedAgentIdByType = builtSubagents.managedAgentIdsByType;
      this.subagentModelIdByType = builtSubagents.modelIdsByType;
      Object.assign(
        options,
        buildMcpToolExposureOptions({
          hasOrcaMcpServer: Boolean(this.orcaMcpServer),
          remoteMcpServerNames: remoteMcpToolsetNames,
          allowedOrcaToolNames: this.input?.agentSnapshot.allowed_tool_names,
          enableAgentTool: Object.keys(subagents).length > 0,
        }),
      );
      if (this.tools && this.tools.length > 0) {
        // Legacy `customTools` slot — pre-MCP wiring. The SDK 0.2.x doesn't
        // honor an arbitrary `customTools` key, so this is functionally a
        // no-op now (the orca MCP server above is the actual binding). Kept
        // here so a future SDK that supports a typed-custom-tools slot can
        // pick it up without a separate change. Cast through `unknown` so the
        // strict `Options` type doesn't reject the unknown key.
        (options as Record<string, unknown>)['customTools'] = this.tools;
      }
      if (Object.keys(subagents).length > 0) {
        const primaryPrompt = typeof options.systemPrompt === 'string' ? options.systemPrompt : '';
        delete options.systemPrompt;
        const primaryBoundary = buildPrimaryAgentBoundary(
          subagents,
          this.input!.agentSnapshot,
          remoteMcpToolsetNames,
          primaryPrompt,
        );
        options.agent = primaryBoundary.name;
        options.agents = primaryBoundary.agents;
        options.hooks = managedSubagentHooks(
          primaryBoundary.name,
          (agentId, agentType) => {
            this.runtimeSubagentTypeById.set(agentId, agentType);
          },
          (agentId) => {
            this.runtimeSubagentTypeById.delete(agentId);
          },
        );
        options.forwardSubagentText = true;
      }

      const model = this.input?.agentSnapshot?.model_id ?? this.opts.modelDefault;
      const provider = this.input?.agentSnapshot?.model_provider;
      this.mcpServerByToolUseId.clear();
      this.publicEventIdByToolUseId.clear();
      this.clientExecutedSdkToolUseIds.clear();
      this.partialAssistantPreviews.clear();
      this.turnSucceeded = false;
      // A turn opens with exactly one session.status_running, then one coarse
      // turn-model-summary pair. It may pause on requires_action between the
      // start and end; this does not imply one span per provider request.
      this.currentModelSummaryStartId = `evt_${uuidv7()}`;
      this.emit({ kind: SessionEventKind.statusRunning, payload: {} });
      this.emit({
        kind: SpanEventKind.modelRequestStart,
        id: this.currentModelSummaryStartId,
        payload: turnModelSummaryStartPayload({
          ...(provider !== undefined ? { provider } : {}),
          model,
        }),
      });

      const result = query({ prompt: text, options });
      const queryDone = (async () => {
        let sdkInitMessage: SDKMessage | undefined;
        let fastModeValidated = false;
        let apiSpeedObserved = false;
        let providerErrorObserved = false;
        // The SDK retries transient API failures itself and reports each retry as
        // an `api_retry` system frame; we surface those as session.status_rescheduled.
        // A non-throwing error `result` frame ends the turn as retries_exhausted
        // (session stays idle); a thrown crash re-throws so the dispatcher's
        // handleRunnerSubmitFailure owns the terminal session.error + terminated.
        for await (const message of result) {
          if (this.terminated) break;
          try {
            const providerError = isSdkProviderErrorMessage(message);
            providerErrorObserved ||= providerError;
            if (isSystemInitMessage(message)) {
              sdkInitMessage = message;
            } else if (
              this.modelSpeed === 'fast' &&
              sdkInitMessage === undefined &&
              !providerError &&
              (message as { type?: unknown }).type !== 'system'
            ) {
              throw new Error(
                'Claude fast mode requested but SDK produced model output before reporting fast_mode_state',
              );
            }
            if (
              sdkInitMessage !== undefined &&
              !fastModeValidated &&
              !providerError &&
              (message as { type?: unknown }).type !== 'system'
            ) {
              assertRequestedFastMode(sdkInitMessage, this.modelSpeed, true);
              fastModeValidated = true;
            }
            if (isResultMessage(message) && !providerError) {
              assertRequestedFastMode(message, this.modelSpeed, false);
            }
            if (!providerError && (!isResultMessage(message) || !apiSpeedObserved)) {
              apiSpeedObserved =
                assertRequestedApiSpeed(message, this.modelSpeed) || apiSpeedObserved;
            }
            await usageTracker.observe(message);
            if (isPartialAssistantMessage(message)) {
              this.emitPartialAssistantMessage(message);
            } else if (isAssistantMessage(message)) {
              this.emitAssistantMessage(message);
            } else if (isSystemInitMessage(message)) {
              // The SDK init frame is not part of the Claude RECEIVED taxonomy and
              // carries internal config (cwd, model, tool list, mcp server names).
              // Persist it under a `harness.`-prefixed (internal-visibility) kind so
              // it never streams to clients but stays available for diagnostics.
              this.emit({ kind: 'harness.claude.system_init', payload: message });
            } else if (isSystemCompactBoundary(message)) {
              this.emit({
                kind: AgentEventKind.threadContextCompacted,
                payload: compactBoundaryPayload(message),
              });
            } else if (isApiRetryMessage(message)) {
              this.emitApiRetry(message);
            } else if (isUserToolResultMessage(message)) {
              await this.emitToolResults(message);
            } else if (isResultMessage(message)) {
              await this.emitResult(message, model, provider);
            }
            // Other SDKMessage variants (auth, status, ...)
            // are not part of the Claude RECEIVED taxonomy and are dropped.
          } catch (error) {
            // SDK iterator cleanup can wait for permission callbacks. Release
            // them before unwinding any failed frame, including validation.
            usageTracker.close();
            queryAbort.abort();
            throw error;
          }
        }
        if (!this.terminated && !providerErrorObserved) {
          if (this.modelSpeed === 'fast' && sdkInitMessage === undefined) {
            throw new Error(
              'Claude fast mode requested but SDK did not report fast_mode_state; refusing standard-speed fallback',
            );
          }
          if (sdkInitMessage !== undefined && !fastModeValidated) {
            assertRequestedFastMode(sdkInitMessage, this.modelSpeed, true);
          }
        }
        // The turn finished normally; evaluate any defined outcomes. Skip
        // on error/aborted turns and never let an evaluator failure break the turn.
        if (!this.terminated && this.turnSucceeded) {
          await this.runOutcomeEvaluations();
          if (!this.terminated) {
            this.emit({
              kind: SessionEventKind.statusIdle,
              payload: sessionIdlePayload('end_turn'),
            });
          }
        }
      })();
      const currentQuery = queryDone
        .then(
          () => {
            usageTracker.close(true);
            if (usageMirrorFailure) throw usageMirrorFailure;
          },
          (error) => {
            usageTracker.close();
            throw usageMirrorFailure ?? error;
          },
        )
        .finally(() => {
          this.runtimeSubagentTypeById.clear();
          this.currentAbort = null;
          this.currentQuery = null;
        });
      this.currentQuery = currentQuery;
      completeStartup();
      void currentQuery.catch((err) => {
        console.error('Claude Agent SDK query failed', err);
        // If the turn was parked on requires_action, no submit() is awaiting this
        // rejection (waitForQueryIdleOrDone already returned via the requires-action
        // path), so the dispatcher never sees it — surface a terminal error here and
        // clear the pending gates so the runner is not wedged. When a submit() IS
        // awaiting (turn still running, no gate), the rejection propagates there and
        // the dispatcher's handleRunnerSubmitFailure owns the terminal events, so we
        // skip to avoid a double-emit.
        if (this.terminated) return;
        if (!this.hasPendingRequiredAction()) {
          return;
        }
        this.failPendingRequiredActions('Session query crashed while awaiting confirmation.');
        const message = err instanceof Error ? err.message : String(err);
        this.emit({
          kind: SessionEventKind.error,
          payload: sessionErrorPayload({ type: 'processing_error', message, willRetry: false }),
        });
        this.emit({
          kind: SessionEventKind.statusIdle,
          payload: sessionIdlePayload('retries_exhausted'),
        });
      });
      await this.waitForQueryIdleOrDone(currentQuery);
    } finally {
      completeStartup();
    }
  }

  /** Deny + clear every pending required-action gate. */
  private failPendingRequiredActions(reason: string): void {
    for (const [eventId, pending] of this.pendingToolConfirmations.entries()) {
      pending.resolve({ behavior: 'deny', message: reason, toolUseID: pending.toolUseID });
      this.pendingToolConfirmations.delete(eventId);
    }
    for (const [eventId, pending] of this.pendingCustomToolResults.entries()) {
      pending.resolve({ custom_tool_use_id: eventId, result: { error: reason } });
      this.pendingCustomToolResults.delete(eventId);
    }
    for (const [eventId, pending] of this.pendingAgentToolResults.entries()) {
      pending.resolve({
        tool_use_id: eventId,
        content: [{ type: 'text', text: reason }],
        is_error: true,
      });
      this.pendingAgentToolResults.delete(eventId);
    }
  }

  /**
   * Translate Claude Agent SDK partial message frames into the Managed Agents
   * preview protocol. Text blocks share one eventual agent.message event and
   * stream content_delta fragments. Thinking blocks are start-only previews.
   */
  private emitPartialAssistantMessage(message: SDKPartialAssistantMessage): void {
    const scope = partialAssistantScope(message);
    if (message.event.type === 'message_start') {
      this.partialAssistantPreviews.delete(scope);
      return;
    }
    const preview = this.partialAssistantPreviews.get(scope) ?? {
      textIndexByContentBlock: new Map<number, number>(),
      thinkingEventIdByContentBlock: new Map<number, string>(),
    };
    this.partialAssistantPreviews.set(scope, preview);

    const event = message.event;
    if (event.type === 'content_block_start') {
      const blockType = event.content_block.type;
      if (blockType === 'text') {
        const contentIndex = this.ensureAgentMessagePreview(preview, event.index);
        if (event.content_block.text) {
          this.emitAgentMessageDelta(
            preview.messageEventId!,
            contentIndex,
            event.content_block.text,
          );
        }
      } else if (blockType === 'thinking' || blockType === 'redacted_thinking') {
        this.ensureAgentThinkingPreview(preview, event.index);
      }
      return;
    }

    if (event.type !== 'content_block_delta' || event.delta.type !== 'text_delta') return;
    const contentIndex = this.ensureAgentMessagePreview(preview, event.index);
    if (event.delta.text) {
      this.emitAgentMessageDelta(preview.messageEventId!, contentIndex, event.delta.text);
    }
  }

  private ensureAgentMessagePreview(preview: PartialAssistantPreview, blockIndex: number): number {
    if (!preview.messageEventId) {
      preview.messageEventId = `evt_${uuidv7()}`;
      this.emit({
        kind: 'event_start',
        payload: { event: { id: preview.messageEventId, type: AgentEventKind.message } },
      });
    }
    const existing = preview.textIndexByContentBlock.get(blockIndex);
    if (existing !== undefined) return existing;
    const contentIndex = preview.textIndexByContentBlock.size;
    preview.textIndexByContentBlock.set(blockIndex, contentIndex);
    return contentIndex;
  }

  private ensureAgentThinkingPreview(preview: PartialAssistantPreview, blockIndex: number): string {
    const existing = preview.thinkingEventIdByContentBlock.get(blockIndex);
    if (existing) return existing;
    const eventId = `evt_${uuidv7()}`;
    preview.thinkingEventIdByContentBlock.set(blockIndex, eventId);
    this.emit({
      kind: 'event_start',
      payload: { event: { id: eventId, type: AgentEventKind.thinking } },
    });
    return eventId;
  }

  private emitAgentMessageDelta(eventId: string, index: number, text: string): void {
    this.emit({
      kind: 'event_delta',
      payload: {
        event_id: eventId,
        delta: {
          type: 'content_delta',
          content: { type: 'text', text },
          index,
        },
      },
    });
  }

  /**
   * Decompose an SDK `assistant` frame into the Claude RECEIVED taxonomy:
   * thinking blocks -> `agent.thinking`, tool_use blocks -> `agent.tool_use` /
   * `agent.mcp_tool_use`, and the remaining text blocks -> a text-only
   * `agent.message` (thinking + tool_use are stripped out of its content).
   */
  private emitAssistantMessage(message: SDKMessage): void {
    const betaMessage = (message as { message?: unknown }).message;
    const content = messageContentBlocks(betaMessage);
    const previewScope = partialAssistantScope(message);
    const preview = this.partialAssistantPreviews.get(previewScope);
    const textBlocks: unknown[] = [];
    const toolUseBlocks: unknown[] = [];
    // First pass: surface thinking immediately (in order), partition text vs tool_use.
    for (const [index, block] of content.entries()) {
      const type = (block as { type?: unknown }).type;
      if (type === 'thinking' || type === 'redacted_thinking') {
        const eventId = preview?.thinkingEventIdByContentBlock.get(index);
        this.emit({
          kind: AgentEventKind.thinking,
          ...(eventId ? { id: eventId } : {}),
          payload: {
            ...(eventId ? { id: eventId } : {}),
            content: [block],
          },
        });
      } else if (type === 'text') {
        textBlocks.push(block);
      } else if (type === 'tool_use' || type === 'server_tool_use' || type === 'mcp_tool_use') {
        toolUseBlocks.push(block);
      }
    }
    // agent.message carries text content only; thinking + tool_use are surfaced
    // as their own top-level events (Claude RECEIVED taxonomy parity).
    if (textBlocks.length > 0) {
      const agentText = textBlocks
        .map((b) =>
          typeof (b as { text?: unknown }).text === 'string' ? (b as { text: string }).text : '',
        )
        .join('');
      if (agentText) this.pushConversation({ role: 'agent', text: agentText });
      const parentToolUseId = (message as { parent_tool_use_id?: unknown }).parent_tool_use_id;
      const eventId = preview?.messageEventId;
      this.emit({
        kind: AgentEventKind.message,
        ...(eventId ? { id: eventId } : {}),
        payload: {
          ...(eventId ? { id: eventId } : {}),
          content: textBlocks,
          ...(typeof parentToolUseId === 'string' || parentToolUseId === null
            ? { parent_tool_use_id: parentToolUseId }
            : {}),
        },
      });
    }
    for (const block of toolUseBlocks) this.emitAssistantToolUse(block);
    this.partialAssistantPreviews.delete(previewScope);
  }

  private emitAssistantToolUse(block: unknown): void {
    const b = block as { id?: unknown; name?: unknown; input?: unknown };
    const toolUseId = typeof b.id === 'string' ? b.id : '';
    const name = typeof b.name === 'string' ? b.name : '';
    const input = b.input ?? {};
    if (this.isDeclaredCustomSdkTool(name)) {
      // The custom MCP handler emits agent.custom_tool_use and waits for
      // user.custom_tool_result. Do not duplicate that lifecycle as generic
      // agent.tool_use / agent.tool_result events from the SDK transcript.
      if (toolUseId) this.clientExecutedSdkToolUseIds.add(toolUseId);
      return;
    }
    if (this.clientToolExecution && ORCA_MCP_TOOL_NAMES.includes(name)) {
      if (toolUseId) this.clientExecutedSdkToolUseIds.add(toolUseId);
      return;
    }
    // Tools already surfaced by canUseTool (including a child-specific
    // always_ask policy) must not be emitted a second time from the assistant
    // content block.
    if (toolUseId && this.publicEventIdByToolUseId.has(toolUseId)) return;
    const remote = this.remoteMcpToolPartsForToolName(name);
    const isRemote = remote !== null && remote.serverName !== ORCA_MCP_SERVER_NAME;
    if (isRemote && toolUseId) this.mcpServerByToolUseId.set(toolUseId, remote.serverName);
    const eventId = `evt_${uuidv7()}`;
    if (toolUseId) this.publicEventIdByToolUseId.set(toolUseId, eventId);
    this.emit({
      kind: isRemote ? AgentEventKind.mcpToolUse : AgentEventKind.toolUse,
      id: eventId,
      payload: {
        id: eventId,
        name: isRemote ? remote.toolName : name,
        input,
        ...(isRemote ? { mcp_server_name: remote.serverName } : {}),
      },
    });
  }

  private async emitToolResults(message: SDKMessage): Promise<void> {
    const content = messageContentBlocks((message as { message?: unknown }).message);
    for (const block of content) {
      if ((block as { type?: unknown }).type !== 'tool_result') continue;
      const b = block as { tool_use_id?: unknown; content?: unknown; is_error?: unknown };
      const toolUseId = typeof b.tool_use_id === 'string' ? b.tool_use_id : '';
      const result = await this.applyToolResultGuardrails(toolUseId, b.content);
      if (toolUseId && this.clientExecutedSdkToolUseIds.delete(toolUseId)) {
        this.publicEventIdByToolUseId.delete(toolUseId);
        this.guardrailToolCallByToolUseId.delete(toolUseId);
        continue;
      }
      const server = toolUseId ? this.mcpServerByToolUseId.get(toolUseId) : undefined;
      const publicToolUseId = this.publicEventIdByToolUseId.get(toolUseId) ?? toolUseId;
      this.emit({
        kind: server ? AgentEventKind.mcpToolResult : AgentEventKind.toolResult,
        payload: {
          tool_use_id: publicToolUseId,
          content: result.content,
          is_error: result.denied || Boolean(b.is_error),
          ...(server ? { mcp_server_name: server } : {}),
        },
      });
      if (toolUseId) {
        this.publicEventIdByToolUseId.delete(toolUseId);
        this.guardrailToolCallByToolUseId.delete(toolUseId);
      }
    }
  }

  private emitApiRetry(message: SDKMessage): void {
    const m = message as {
      attempt?: number;
      max_retries?: number;
      retry_delay_ms?: number;
      error_status?: number | null;
      error?: string;
    };
    const status = typeof m.error_status === 'number' ? ` (status ${m.error_status})` : '';
    this.emit({
      kind: SessionEventKind.error,
      payload: sessionErrorPayload({
        type: 'transient_error',
        message: `${m.error ?? 'api_error'}${status}`,
        willRetry: true,
        nextAttempt: (m.attempt ?? 0) + 1,
        extra: { attempt: m.attempt, max_retries: m.max_retries, retry_delay_ms: m.retry_delay_ms },
      }),
    });
    this.emit({
      kind: SessionEventKind.statusRescheduled,
      payload: {
        attempt: m.attempt,
        max_retries: m.max_retries,
        retry_delay_ms: m.retry_delay_ms,
      },
    });
  }

  private async emitResult(
    message: SDKMessage,
    model: string,
    provider: string | undefined,
  ): Promise<void> {
    const usage = usageFromSdkResult(message);
    // agent.usage stays the internal recordUsage signal (never streamed); the
    // public token accounting rides on span.model_request_end.model_usage.
    if (usage && !this.usageTracker?.reported) {
      await this.emitUsage(usage, model, undefined, this.currentAbort?.signal);
    }
    const result = message as {
      subtype?: unknown;
      is_error?: unknown;
      errors?: unknown;
      result?: unknown;
    };
    const subtype = result.subtype;
    const isError = subtype !== 'success' || result.is_error === true;
    if (!this.currentModelSummaryStartId) {
      this.currentModelSummaryStartId = `evt_${uuidv7()}`;
      this.emit({
        kind: SpanEventKind.modelRequestStart,
        id: this.currentModelSummaryStartId,
        payload: turnModelSummaryStartPayload({
          ...(provider !== undefined ? { provider } : {}),
          model,
        }),
      });
    }
    this.emit({
      kind: SpanEventKind.modelRequestEnd,
      id: `evt_${uuidv7()}`,
      payload: turnModelSummaryEndPayload({
        modelUsage: modelUsageFromUsage(usage),
        isError,
        modelRequestStartId: this.currentModelSummaryStartId,
        ...(provider !== undefined ? { provider } : {}),
        model,
        // total_cost_usd is a fractional dollar amount — must NOT be floored.
        totalCostUsd: costField((message as { total_cost_usd?: unknown }).total_cost_usd),
      }),
    });
    if (!isError) {
      this.turnSucceeded = true;
      return;
    }
    // A non-throwing error result means the SDK exhausted its own retries or hit
    // a hard limit (max_turns/max_budget). The session stays idle so the client
    // can send more input; the precise detail rides on session.error.
    const errors = result.errors;
    const detail =
      Array.isArray(errors) && errors.length > 0
        ? errors.join('; ')
        : typeof result.result === 'string' && result.result.length > 0
          ? result.result
          : String(subtype ?? 'error');
    const errorType = subtype === 'success' ? 'processing_error' : subtype;
    this.emit({
      kind: SessionEventKind.error,
      payload: sessionErrorPayload({
        type: typeof errorType === 'string' ? errorType : 'processing_error',
        message: detail,
        willRetry: false,
      }),
    });
    this.emit({
      kind: SessionEventKind.statusIdle,
      payload: sessionIdlePayload('retries_exhausted'),
    });
  }

  private async emitUsage(
    usage: NonNullable<ReturnType<typeof usageFromSdkResult>>,
    model: string,
    subagentId?: string,
    signal?: AbortSignal,
    turnEventId = this.currentTurnEventId,
  ): Promise<void> {
    const usageEventId = `evt_${uuidv7()}`;
    if (this.opts.onUsage) {
      const pending = {
        usage,
        model,
        ...(subagentId ? { subagentId } : {}),
        ...(turnEventId ? { turnEventId } : {}),
        usageEventId,
      };
      try {
        if (signal?.aborted) throw new Error('Usage acknowledgment interrupted.');
        const state = await awaitUsageAcknowledgment(
          this.opts.onUsage(usage, model, subagentId, turnEventId, usageEventId),
          signal,
        );
        if (state) this.applyGuardrailUsageState(state);
      } catch (cause) {
        this.pendingUsageReports.push(pending);
        throw new GuardrailUsageUnavailableError(
          'Registry did not acknowledge usage; stateful-guarded actions are blocked.',
          { cause },
        );
      }
    }
    this.emit({
      id: usageEventId,
      kind: AgentEventKind.usage,
      payload: {
        usage,
        model,
        ...(subagentId ? { subagent_id: subagentId } : {}),
        ...(this.opts.onUsage ? { guardrail_usage_recorded: true } : {}),
      },
    });
  }

  private async flushPendingUsageReports(): Promise<void> {
    if (!this.opts.onUsage) return;
    while (this.pendingUsageReports.length > 0) {
      const pending = this.pendingUsageReports[0]!;
      try {
        const state = await this.opts.onUsage(
          pending.usage,
          pending.model,
          pending.subagentId,
          pending.turnEventId,
          pending.usageEventId,
        );
        if (state) this.applyGuardrailUsageState(state);
        this.pendingUsageReports.shift();
      } catch (cause) {
        throw new GuardrailUsageUnavailableError(
          'Registry did not acknowledge usage; stateful-guarded actions are blocked.',
          { cause },
        );
      }
    }
  }

  private pushConversation(turn: OutcomeTranscriptTurn): void {
    this.conversationLog.push(turn);
    if (this.conversationLog.length > CONVERSATION_LOG_MAX_ENTRIES) {
      this.conversationLog.splice(0, this.conversationLog.length - CONVERSATION_LOG_MAX_ENTRIES);
    }
  }

  private outcomeCriterionFromPayload(payload: unknown): OutcomeCriterion | null {
    if (!payload || typeof payload !== 'object') return null;
    const p = payload as Record<string, unknown>;
    const description = typeof p['description'] === 'string' ? p['description'].trim() : '';
    if (!description || p['rubric'] === undefined) return null;
    const id =
      typeof p['id'] === 'string' && p['id'].length > 0
        ? p['id']
        : `outcome_${slugifyOutcomeId(description)}`;
    const maxIterations = normalizeMaxIterations(p['max_iterations']);
    const criterion: OutcomeCriterion = {
      id,
      description,
      rubric: p['rubric'],
      maxIterations,
      iteration: 0,
    };
    return criterion;
  }

  /**
   * Evaluate every defined outcome against the conversation so far and surface
   * each verdict as the `span.outcome_evaluation_start/ongoing/end` trio.
   * Emitted before the terminal session.status_idle so stream consumers see the
   * outcome result before treating the turn as complete.
   */
  private async runOutcomeEvaluations(): Promise<void> {
    if (this.definedOutcomes.size === 0 || !this.outcomeEvaluator) return;
    // Judge only a bounded recent window so prompt size/cost stays O(1) per turn.
    const transcript = this.conversationLog.slice(-OUTCOME_TRANSCRIPT_MAX_ENTRIES);
    for (const criterion of this.definedOutcomes.values()) {
      if (this.terminated) return;
      const iteration = criterion.iteration;
      const startId = `evt_${uuidv7()}`;
      this.emit({
        kind: SpanEventKind.outcomeEvaluationStart,
        id: startId,
        payload: { id: startId, outcome_id: criterion.id, iteration },
      });
      this.emit({
        kind: SpanEventKind.outcomeEvaluationOngoing,
        payload: { outcome_id: criterion.id, iteration, outcome_evaluation_start_id: startId },
      });
      let verdict: OutcomeVerdict;
      let result: OutcomeEvaluationResult;
      try {
        verdict = await this.outcomeEvaluator({ criterion, transcript });
        result = outcomeResultFromVerdict(verdict, iteration, criterion.maxIterations);
      } catch (err) {
        verdict = {
          achieved: false,
          reasoning: `outcome evaluation failed: ${err instanceof Error ? err.message : String(err)}`,
        };
        result = 'failed';
      }
      this.emit({
        kind: SpanEventKind.outcomeEvaluationEnd,
        payload: {
          outcome_id: criterion.id,
          result,
          explanation: verdict.reasoning,
          iteration,
          usage: zeroModelUsage(),
          outcome_evaluation_start_id: startId,
        },
      });
      criterion.iteration = iteration + 1;
    }
  }

  async stop(reason: TerminationReason): Promise<void> {
    void reason;
    this.terminated = true;
    this.opts.llmGatewayJwtProvider?.close();
    // Startup may be awaiting the shared mint. Cancel it BEFORE joining startup.
    const mcpJwtProvider = this.mcpJwtProvider;
    this.mcpJwtProvider = undefined;
    mcpJwtProvider?.close();
    const startup = this.queryStartup;
    if (startup) await startup.catch(() => undefined);
    const currentQuery = this.currentQuery;
    this.currentAbort?.abort();
    this.currentAbort = null;
    for (const [eventId, pending] of this.pendingToolConfirmations.entries()) {
      pending.resolve({
        behavior: 'deny',
        message: 'Session stopped before tool confirmation was received.',
        toolUseID: pending.toolUseID,
      });
      this.pendingToolConfirmations.delete(eventId);
    }
    for (const [eventId, pending] of this.pendingCustomToolResults.entries()) {
      pending.resolve({
        custom_tool_use_id: eventId,
        result: {
          error: 'Session stopped before custom tool result was received.',
        },
      });
      this.pendingCustomToolResults.delete(eventId);
    }
    for (const [eventId, pending] of this.pendingAgentToolResults.entries()) {
      pending.resolve({
        tool_use_id: eventId,
        content: [
          {
            type: 'text',
            text: 'Session stopped before agent tool result was received.',
          },
        ],
        is_error: true,
      });
      this.pendingAgentToolResults.delete(eventId);
    }
    this.publicEventIdByToolUseId.clear();
    this.clientExecutedSdkToolUseIds.clear();
    this.customSdkToolNames.clear();
    this.partialAssistantPreviews.clear();
    this.runtimeSubagentTypeById.clear();
    this.subagentPermissionPoliciesByType.clear();
    while (this.outResolvers.length) {
      const resolver = this.outResolvers.shift();
      if (resolver) resolver({ value: undefined, done: true });
    }
    if (currentQuery) await currentQuery.catch(() => undefined);
    const configDir = this.claudeConfigDir;
    this.claudeConfigDir = undefined;
    if (configDir) await rm(configDir, { recursive: true, force: true });
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.terminated || this.outQueue.length) {
      if (this.outQueue.length) {
        const head = this.outQueue.shift();
        if (head !== undefined) yield head;
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        this.outResolvers.push(resolve);
      });
      if (next.done) return;
      yield next.value;
    }
  }

  /**
   * Surface every guardrail this topology cannot fully evaluate.
   *
   * `separate` wires `request`, `tool_call` and `tool_result`. A guardrail may
   * still declare `llm_request` — `deny_pii_in_llm_request` does — and until
   * the model-endpoint interceptor exists there is nothing to evaluate it on.
   * Saying nothing was the worse failure: an operator configured a PII screen
   * and silently got only its `request` leg, which is the preview the design
   * calls out as not being a screen at all.
   *
   * A warning rather than a refusal, because that `request` leg does enforce
   * and dropping it would trade a partial screen for none. A guardrail with no
   * enforceable phase *at all* has nothing to degrade to, so it fails closed —
   * matching `InSandboxHarness`, which refuses `llm_request` outright because
   * it cannot even offer the `request` leg.
   */
  private reportUnenforceableGuardrails(): void {
    for (const prepared of this.preparedGuardrails) {
      const { id, name, phases } = prepared.guardrail;
      const unfired = phases.filter((phase) => !isEnforced(phase));
      if (unfired.length === 0) continue;
      if (unfired.length === phases.length) {
        throw new Error(
          `separate cannot enforce ${name} (${id}): no enforcement point fires ` +
            `${unfired.join(', ')}`,
        );
      }
      this.emit({
        kind: SessionEventKind.warning,
        payload: sessionWarningPayload({
          type: 'guardrail_not_enforced',
          message:
            `Guardrail ${name} declares ${unfired.join(', ')}, which separate does not ` +
            `evaluate; only ${phases.filter(isEnforced).join(', ')} is enforced.`,
          guardrailId: id,
          guardrailName: name,
        }),
      });
    }
  }

  private emit(e: AgentEventInput): void {
    const event = withCanonicalAgentEventEnvelope(e);
    if (this.outResolvers.length) {
      const resolver = this.outResolvers.shift();
      if (resolver) {
        resolver({ value: event, done: false });
        return;
      }
    }
    this.outQueue.push(event);
  }

  requestCustomToolUse(
    name: string,
    input: Record<string, unknown>,
  ): { id: string; result: Promise<UserCustomToolResultPayload> } {
    const id = `evt_${uuidv7()}`;
    const result = new Promise<UserCustomToolResultPayload>((resolve) => {
      this.pendingCustomToolResults.set(id, { resolve });
    });
    this.emit({ kind: AgentEventKind.customToolUse, id, payload: { id, name, input } });
    this.emitRequiresAction();
    return { id, result };
  }

  requestAgentToolUse(
    name: string,
    input: Record<string, unknown>,
  ): { id: string; result: Promise<UserToolResultPayload> } {
    const id = `evt_${uuidv7()}`;
    const result = new Promise<UserToolResultPayload>((resolve) => {
      this.pendingAgentToolResults.set(id, { resolve });
    });
    this.emit({ kind: AgentEventKind.toolUse, id, payload: { id, name, input } });
    this.emitRequiresAction();
    return { id, result };
  }

  hasPendingRequiredAction(): boolean {
    return this.pendingRequiredActionIds().length > 0;
  }

  private async canUseManagedAgentTool(
    toolName: string,
    input: Record<string, unknown>,
    options: {
      signal: AbortSignal;
      toolUseID: string;
      suggestions?: unknown;
      blockedPath?: string;
      decisionReason?: string;
      title?: string;
      displayName?: string;
      description?: string;
      agentID?: string;
    },
    usageTracker?: ClaudeUsageTracker,
  ): Promise<PermissionResult> {
    if (options.signal.aborted) {
      return abortedToolPermissionResult(options.toolUseID);
    }

    if (
      usageTracker &&
      !(await usageTracker.waitForTool(options.toolUseID, options.signal, options.agentID))
    ) {
      return {
        behavior: 'deny',
        message: 'Tool blocked because its model usage was not acknowledged.',
        toolUseID: options.toolUseID,
      };
    }
    if (options.signal.aborted || this.terminated)
      return abortedToolPermissionResult(options.toolUseID);

    const isCustomTool = this.isDeclaredCustomSdkTool(toolName, options.agentID);
    // One decision, one fold. The agent's permission policy seeds the verdict
    // and guardrails compose on top of it, so a guardrail can tighten what the
    // policy allows but never loosen what it denies — the ordering is a
    // property of the lattice rather than of this function.
    const permission = this.decideToolPermission(toolName, input, options.agentID);

    if (permission.verdict === 'deny') {
      return {
        behavior: 'deny',
        message:
          permission.reason ?? `Tool ${toolName} is denied by managed-agent permission policy.`,
        toolUseID: options.toolUseID,
      };
    }
    const clientPolicyApproval =
      this.clientToolExecution &&
      ORCA_MCP_TOOL_NAMES.includes(toolName) &&
      permission.verdict === 'ask' &&
      permission.guardrailVerdict === 'allow';
    if (permission.verdict === 'allow' || clientPolicyApproval) {
      if (isCustomTool) this.clientExecutedSdkToolUseIds.add(options.toolUseID);
      await this.applyGuardrailState(
        clientPolicyApproval ? permission.intendedStateUpdates : permission.stateUpdates,
      );
      this.rememberGuardedToolCall(toolName, input, options.toolUseID, options.agentID);
      this.rememberSubagentDispatch(toolName, input, options.toolUseID);
      return { behavior: 'allow', updatedInput: input, toolUseID: options.toolUseID };
    }

    const existingEventId = this.publicEventIdByToolUseId.get(options.toolUseID);
    const eventId = existingEventId ?? `evt_${uuidv7()}`;
    const remoteMcpTool = this.remoteMcpToolPartsForToolName(toolName);
    const isRemoteMcpTool =
      remoteMcpTool !== null && remoteMcpTool.serverName !== ORCA_MCP_SERVER_NAME;
    const payload: Record<string, unknown> = {
      id: eventId,
      name: isRemoteMcpTool ? remoteMcpTool.toolName : toolName,
      input,
    };
    if (existingEventId === undefined) {
      this.publicEventIdByToolUseId.set(options.toolUseID, eventId);
    }
    if (isRemoteMcpTool) payload['mcp_server_name'] = remoteMcpTool.serverName;
    if (options.title) payload['title'] = options.title;
    if (options.displayName) payload['display_name'] = options.displayName;
    if (options.description) payload['description'] = options.description;
    if (options.blockedPath) payload['blocked_path'] = options.blockedPath;
    if (options.decisionReason) payload['decision_reason'] = options.decisionReason;

    const decision = new Promise<PermissionResult>((resolve) => {
      this.pendingToolConfirmations.set(eventId, {
        toolName,
        toolInput: input,
        toolUseID: options.toolUseID,
        approvedStateUpdates: permission.intendedStateUpdates,
        ...(options.agentID ? { runtimeSubagentId: options.agentID } : {}),
        resolve,
      });
    });
    options.signal.addEventListener(
      'abort',
      () => {
        const pending = this.pendingToolConfirmations.get(eventId);
        if (!pending) return;
        this.pendingToolConfirmations.delete(eventId);
        pending.resolve(abortedToolPermissionResult(pending.toolUseID));
      },
      { once: true },
    );

    if (isRemoteMcpTool) {
      // Classify the eventual tool_result frame as agent.mcp_tool_result.
      this.mcpServerByToolUseId.set(options.toolUseID, remoteMcpTool.serverName);
    }
    if (existingEventId === undefined) {
      this.emit({
        kind: isRemoteMcpTool ? AgentEventKind.mcpToolUse : AgentEventKind.toolUse,
        id: eventId,
        payload,
      });
    }
    this.emitRequiresAction();
    return decision;
  }

  /**
   * Resolve a tool call to a single verdict.
   *
   * The permission policy is not consulted separately from guardrails: it is
   * the seed the guardrail fold starts from. That keeps the policy's runtime
   * inputs — remote toolsets, subagent maps, client-execution — where they
   * live, while still producing exactly one decision per call.
   */
  private decideToolPermission(
    toolName: string,
    input: Record<string, unknown>,
    runtimeSubagentId?: string,
  ): {
    verdict: Verdict;
    guardrailVerdict: Verdict;
    reason?: string;
    stateUpdates: readonly StateUpdate[];
    intendedStateUpdates: readonly StateUpdate[];
  } {
    const policy = this.permissionPolicyForTool(toolName, runtimeSubagentId);
    const seed: Verdict =
      policy === 'always_deny' ? 'deny' : policy === 'always_ask' ? 'ask' : 'allow';

    if (seed === 'deny' || this.preparedGuardrails.length === 0) {
      return {
        verdict: seed,
        guardrailVerdict: 'allow',
        stateUpdates: [],
        intendedStateUpdates: [],
      };
    }

    const agentType = runtimeSubagentId
      ? this.runtimeSubagentTypeById.get(runtimeSubagentId)
      : undefined;
    const managedSubagentId = agentType
      ? this.subagentManagedAgentIdByType.get(agentType)
      : undefined;
    const activeModelId = agentType
      ? this.subagentModelIdByType.get(agentType)
      : this.input?.agentSnapshot.model_id;

    const event: GuardrailEvent = {
      phase: 'tool_call',
      sessionId: this.input?.sessionId ?? '',
      tool: { name: toolName, input },
      ...(managedSubagentId ? { subagentId: managedSubagentId } : {}),
      ...(activeModelId ? { modelId: activeModelId } : {}),
    };

    // Evaluate the guardrail portion from allow, then compose the permission
    // policy with the same lattice. Keeping the two source verdicts lets
    // client-executed tools distinguish an ordinary policy ask (the client
    // tool result is its approval) from a guardrail ask that still needs the
    // explicit confirmation round trip.
    const decision = evaluateGuardrails(this.preparedGuardrails, event, {
      builtins: BUILTIN_EVALUATORS,
      expression: evaluateGuardrailExpression,
      seed: 'allow',
      store: this.guardrailStore,
    });

    const verdict = maxVerdict(seed, decision.verdict);

    return {
      verdict,
      guardrailVerdict: decision.verdict,
      ...(decision.reasons.length > 0 ? { reason: decision.reasons.join(' ') } : {}),
      stateUpdates: verdict === 'allow' ? decision.stateUpdates : [],
      intendedStateUpdates: decision.intendedStateUpdates,
    };
  }

  /** Persist guardrail state before exposing it to the next local decision. */
  private async applyGuardrailState(updates: readonly StateUpdate[]): Promise<void> {
    if (updates.length === 0) return;
    await this.opts.onGuardrailState?.(updates);
    this.guardrailStore.apply(updates);
  }

  private async applyToolResultGuardrails(
    toolUseId: string,
    content: unknown,
  ): Promise<{ content: unknown; denied: boolean }> {
    const call = this.guardrailToolCallByToolUseId.get(toolUseId);
    if (!call || this.preparedGuardrails.length === 0) return { content, denied: false };
    const decision = evaluateGuardrails(
      this.preparedGuardrails,
      {
        phase: 'tool_result',
        sessionId: this.input?.sessionId ?? '',
        tool: { name: call.toolName, input: call.input },
        result: content,
        ...(call.subagentId ? { subagentId: call.subagentId } : {}),
        ...(call.modelId ? { modelId: call.modelId } : {}),
      },
      {
        builtins: BUILTIN_EVALUATORS,
        expression: evaluateGuardrailExpression,
        store: this.guardrailStore,
      },
    );
    await this.applyGuardrailState(decision.stateUpdates);
    if (decision.verdict !== 'deny') return { content, denied: false };
    const reason = decision.reasons.join(' ') || 'Tool output denied by a managed-agent guardrail.';
    return {
      content: [{ type: 'text', text: `Tool output suppressed by policy: ${reason}` }],
      denied: true,
    };
  }

  private rememberGuardedToolCall(
    toolName: string,
    input: Record<string, unknown>,
    toolUseId: string,
    runtimeSubagentId?: string,
  ): void {
    const agentType = runtimeSubagentId
      ? this.runtimeSubagentTypeById.get(runtimeSubagentId)
      : undefined;
    const subagentId = agentType ? this.subagentManagedAgentIdByType.get(agentType) : undefined;
    const modelId = agentType
      ? this.subagentModelIdByType.get(agentType)
      : this.input?.agentSnapshot.model_id;
    this.guardrailToolCallByToolUseId.set(toolUseId, {
      toolName,
      input: { ...input },
      ...(subagentId ? { subagentId } : {}),
      ...(modelId ? { modelId } : {}),
    });
  }

  private async evaluateRequestGuardrails(userText: string): Promise<void> {
    if (this.preparedGuardrails.length === 0) return;
    const decision = evaluateGuardrails(
      this.preparedGuardrails,
      {
        phase: 'request',
        sessionId: this.input?.sessionId ?? '',
        userText,
        ...(this.input?.agentSnapshot.model_id
          ? { modelId: this.input.agentSnapshot.model_id }
          : {}),
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
    await this.applyGuardrailState(decision.stateUpdates);
  }

  private async refreshGuardrailSubjectWindow(): Promise<void> {
    if (!this.currentTurnEventId || !this.opts.refreshGuardrailSubjectWindow) return;
    const state = await this.opts.refreshGuardrailSubjectWindow(this.currentTurnEventId);
    this.guardrailStore.replace('subject_window', state);
  }

  private permissionPolicyForTool(
    toolName: string,
    runtimeSubagentId?: string,
  ): ManagedToolPermissionPolicy {
    // A declared custom tool is client-executed. SDK permission must allow its
    // in-process MCP handler to run so that handler can emit the real
    // agent.custom_tool_use required-action gate. Any matching permission policy
    // (exact-name or mcp__orca__* wildcard) would create a spurious
    // user.tool_confirmation gate before the custom handler runs, leaving
    // user.custom_tool_result with no pending custom call to resolve.
    if (this.isDeclaredCustomSdkTool(toolName, runtimeSubagentId)) return 'always_allow';

    let policies: ManagedToolPermissionPolicies;
    if (runtimeSubagentId !== undefined) {
      const agentType = this.runtimeSubagentTypeById.get(runtimeSubagentId);
      if (!agentType) return 'always_deny';
      const childPolicies = this.subagentPermissionPoliciesByType.get(agentType);
      if (!childPolicies) return 'always_deny';
      policies = childPolicies;
    } else {
      policies = this.input?.agentSnapshot.tool_permission_policies ?? {};
    }
    const direct = policies[toolName];
    if (isManagedToolPermissionPolicy(direct)) return direct;
    const remoteServer = this.remoteMcpToolPartsForToolName(toolName)?.serverName ?? null;
    if (remoteServer) {
      const wildcard = policies[`mcp__${remoteServer}__*`];
      if (isManagedToolPermissionPolicy(wildcard)) return wildcard;
      if (runtimeSubagentId === undefined) {
        const toolsetPolicy = this.remoteMcpToolsets?.find((t) => t.serverName === remoteServer);
        if (toolsetPolicy) return toolsetPolicy.permissionPolicy ?? 'always_ask';
      }
      if (remoteServer !== ORCA_MCP_SERVER_NAME) return 'always_deny';
    }
    return 'always_allow';
  }

  private isDeclaredCustomSdkTool(toolName: string, runtimeSubagentId?: string): boolean {
    // Primary custom tools are not inherited by SDK subagents. Keep that
    // boundary explicit instead of letting a child bypass its own tool list.
    return runtimeSubagentId === undefined && this.customSdkToolNames.has(toolName);
  }

  private remoteMcpToolPartsForToolName(
    toolName: string,
  ): { serverName: string; toolName: string } | null {
    if (!toolName.startsWith('mcp__')) return null;
    const body = toolName.slice('mcp__'.length);
    const configuredServerNames = [
      ...(this.remoteMcpToolsets?.map((t) => t.serverName) ?? []),
      ...Object.keys(this.mcpServers ?? {}),
    ].sort((a, b) => b.length - a.length);

    for (const serverName of configuredServerNames) {
      const prefix = `${serverName}__`;
      if (body.startsWith(prefix)) {
        const mcpToolName = body.slice(prefix.length);
        if (mcpToolName.length > 0) return { serverName, toolName: mcpToolName };
      }
    }

    return remoteMcpToolPartsFromToolName(toolName);
  }

  private async acceptRequiredAction(
    prepared: PreparedRequiredAction,
    hooks?: SubmitHooks,
    beforeApply?: () => void,
  ): Promise<void> {
    await hooks?.onAccepted();
    if (!prepared.isCurrent()) {
      await this.waitForQueryIdleOrDone();
      return;
    }
    const resumesModelWork = this.pendingRequiredActionIds().length === 1;
    if (resumesModelWork) this.emit({ kind: SessionEventKind.statusRunning, payload: {} });
    beforeApply?.();
    await prepared.apply();
    await this.waitForQueryIdleOrDone();
  }

  private prepareToolConfirmation(payload: unknown): PreparedRequiredAction | null {
    if (!payload || typeof payload !== 'object') return null;
    const p = payload as Record<string, unknown>;
    const eventId = typeof p['tool_use_id'] === 'string' ? p['tool_use_id'] : null;
    if (!eventId) return null;
    const pending = this.pendingToolConfirmations.get(eventId);
    if (!pending) return null;
    // `result` is the authoritative managed-agents field; `approved` is a
    // deprecated boolean alias accepted as a fallback (defense-in-depth in
    // case an un-normalized payload reaches the data plane).
    const isAllow = p['result'] === 'allow' || (p['result'] == null && p['approved'] === true);
    if (isAllow) {
      return {
        isCurrent: () => this.pendingToolConfirmations.get(eventId) === pending,
        apply: async () => {
          // Inside `apply`, not before it: a stale confirmation never reaches
          // here, so an approval the session has moved past cannot advance a
          // counter for a call that will not run.
          await this.applyGuardrailState(pending.approvedStateUpdates);
          this.rememberGuardedToolCall(
            pending.toolName,
            pending.toolInput,
            pending.toolUseID,
            pending.runtimeSubagentId,
          );
          this.rememberSubagentDispatch(pending.toolName, pending.toolInput, pending.toolUseID);
          this.pendingToolConfirmations.delete(eventId);
          pending.resolve({
            behavior: 'allow',
            updatedInput: pending.toolInput,
            toolUseID: pending.toolUseID,
          });
        },
      };
    }
    const denyMessage =
      typeof p['deny_message'] === 'string'
        ? p['deny_message']
        : `Tool ${pending.toolName} denied by user confirmation.`;
    return {
      isCurrent: () => this.pendingToolConfirmations.get(eventId) === pending,
      apply: () => {
        this.pendingToolConfirmations.delete(eventId);
        pending.resolve({
          behavior: 'deny',
          message: denyMessage,
          toolUseID: pending.toolUseID,
        });
      },
    };
  }

  private rememberSubagentDispatch(
    toolName: string,
    input: Readonly<Record<string, unknown>>,
    toolUseId: string,
  ): void {
    if (toolName !== 'Agent') return;
    const type = input['subagent_type'];
    if (typeof type !== 'string') return;
    const managedId = this.subagentManagedAgentIdByType.get(type);
    if (managedId) this.subagentManagedAgentIdByParentToolUseId.set(toolUseId, managedId);
  }

  private rememberSystemMessage(message: string | null): void {
    if (message) this.sessionSystemMessages.push(message);
  }

  private prepareCustomToolResult(
    payload: unknown,
    systemMessage: string | null,
  ): PreparedRequiredAction | null {
    if (!payload || typeof payload !== 'object') return null;
    const p = payload as UserCustomToolResultPayload;
    const customToolUseId =
      typeof p.custom_tool_use_id === 'string'
        ? p.custom_tool_use_id
        : typeof p.tool_use_id === 'string'
          ? p.tool_use_id
          : null;
    if (!customToolUseId) return null;
    const pending = this.pendingCustomToolResults.get(customToolUseId);
    if (!pending) return null;
    const content = systemMessage
      ? appendSystemMessageToToolResultContent(p.content, systemMessage)
      : p.content;
    return {
      isCurrent: () => this.pendingCustomToolResults.get(customToolUseId) === pending,
      apply: () => {
        this.pendingCustomToolResults.delete(customToolUseId);
        pending.resolve({
          ...p,
          custom_tool_use_id: customToolUseId,
          ...(systemMessage ? { content } : {}),
        });
      },
    };
  }

  private prepareAgentToolResult(
    payload: unknown,
    systemMessage: string | null,
  ): PreparedRequiredAction | null {
    if (!payload || typeof payload !== 'object') return null;
    const p = payload as UserToolResultPayload;
    const toolUseId = typeof p.tool_use_id === 'string' ? p.tool_use_id : null;
    if (!toolUseId) return null;
    const pending = this.pendingAgentToolResults.get(toolUseId);
    if (!pending) return null;
    const content = systemMessage
      ? appendSystemMessageToToolResultContent(p.content, systemMessage)
      : p.content;
    return {
      isCurrent: () => this.pendingAgentToolResults.get(toolUseId) === pending,
      apply: () => {
        this.pendingAgentToolResults.delete(toolUseId);
        pending.resolve({ ...p, tool_use_id: toolUseId, ...(systemMessage ? { content } : {}) });
      },
    };
  }

  private emitRequiresAction(): void {
    const eventIds = this.pendingRequiredActionIds();
    if (eventIds.length === 0) return;
    this.emit({
      kind: SessionEventKind.statusIdle,
      payload: sessionIdlePayload('requires_action', eventIds as [AgentEventId, ...AgentEventId[]]),
    });
    this.resolveRequiresActionWaiters();
  }

  private pendingRequiredActionIds(): string[] {
    return [
      ...this.pendingToolConfirmations.keys(),
      ...this.pendingCustomToolResults.keys(),
      ...this.pendingAgentToolResults.keys(),
    ];
  }

  private async waitForQueryIdleOrDone(current?: Promise<void> | null): Promise<void> {
    const startup = this.queryStartup;
    if (startup) await startup;
    current ??= this.currentQuery;
    if (!current) return;
    if (this.hasPendingRequiredAction()) return;
    let resolveRequiredAction: (() => void) | undefined;
    const requiredAction = new Promise<void>((resolve) => {
      resolveRequiredAction = resolve;
      this.requiresActionResolvers.push(resolve);
    });
    try {
      await Promise.race([current, requiredAction]);
    } finally {
      if (resolveRequiredAction) {
        const index = this.requiresActionResolvers.indexOf(resolveRequiredAction);
        if (index >= 0) this.requiresActionResolvers.splice(index, 1);
      }
    }
  }

  private resolveRequiresActionWaiters(): void {
    while (this.requiresActionResolvers.length) {
      const resolve = this.requiresActionResolvers.shift();
      if (resolve) resolve();
    }
  }
}

function isAssistantMessage(m: SDKMessage): boolean {
  return (m as { type?: string }).type === 'assistant';
}

function isPartialAssistantMessage(m: SDKMessage): m is SDKPartialAssistantMessage {
  return (m as { type?: string }).type === 'stream_event';
}

function partialAssistantScope(m: SDKMessage): string {
  const message = m as {
    session_id?: unknown;
    parent_tool_use_id?: unknown;
    uuid?: unknown;
  };
  const sessionId = typeof message.session_id === 'string' ? message.session_id : '';
  const parentToolUseId =
    typeof message.parent_tool_use_id === 'string' ? message.parent_tool_use_id : '';
  if (sessionId || parentToolUseId) return `${sessionId}\u0000${parentToolUseId}`;
  return typeof message.uuid === 'string' ? message.uuid : '';
}

function isSystemInitMessage(m: SDKMessage): boolean {
  const candidate = m as { type?: string; subtype?: string };
  return candidate.type === 'system' && candidate.subtype === 'init';
}

function isResultMessage(m: SDKMessage): boolean {
  return (m as { type?: string }).type === 'result';
}

function isSdkProviderErrorMessage(m: SDKMessage): boolean {
  const candidate = m as {
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
  message: SDKMessage,
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
 * Verify provider-observed inference speed before forwarding model output.
 * `fast_mode_state` proves the SDK enabled its local fast-mode switch; Anthropic's
 * `usage.speed` proves the API (or a compatible gateway) actually served that
 * request at the requested speed. Streaming `message_start` arrives before text
 * deltas, so a gateway downgrade is rejected before standard-speed output leaks.
 */
function assertRequestedApiSpeed(message: SDKMessage, requestedSpeed: ModelSpeed): boolean {
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

function apiSpeedObservation(message: SDKMessage): { source: string; speed: unknown } | null {
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

function isSystemCompactBoundary(m: SDKMessage): boolean {
  const c = m as { type?: string; subtype?: string };
  return c.type === 'system' && c.subtype === 'compact_boundary';
}

function isApiRetryMessage(m: SDKMessage): boolean {
  const c = m as { type?: string; subtype?: string };
  return c.type === 'system' && c.subtype === 'api_retry';
}

function isUserToolResultMessage(m: SDKMessage): boolean {
  if ((m as { type?: string }).type !== 'user') return false;
  const content = messageContentBlocks((m as { message?: unknown }).message);
  return content.some((b) => (b as { type?: unknown }).type === 'tool_result');
}

function messageContentBlocks(betaMessage: unknown): unknown[] {
  if (!betaMessage || typeof betaMessage !== 'object') return [];
  const content = (betaMessage as { content?: unknown }).content;
  return Array.isArray(content) ? content : [];
}

function compactBoundaryPayload(m: SDKMessage): Record<string, unknown> {
  const meta = (m as { compact_metadata?: unknown }).compact_metadata;
  return meta && typeof meta === 'object' ? { compact_metadata: meta } : {};
}

function modelUsageFromUsage(usage: ReturnType<typeof usageFromSdkResult>): ModelUsageCounts {
  if (!usage) {
    return {
      input_tokens: 0,
      output_tokens: 0,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
    };
  }
  return {
    input_tokens: usage.input_tokens,
    output_tokens: usage.output_tokens,
    cache_creation_input_tokens:
      usage.cache_creation.ephemeral_1h_input_tokens +
      usage.cache_creation.ephemeral_5m_input_tokens,
    cache_read_input_tokens: usage.cache_read_input_tokens,
  };
}

function usageFromSdkResult(m: SDKMessage) {
  return normalizeModelCallUsage((m as { usage?: unknown }).usage);
}

// Cost is a fractional dollar amount; unlike token counts it must NOT be floored.
function costField(value: unknown): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) return 0;
  return value;
}

export interface McpToolExposureInput {
  hasOrcaMcpServer: boolean;
  remoteMcpServerNames: string[];
  allowedOrcaToolNames?: string[];
  enableAgentTool?: boolean;
}

export function buildMcpToolExposureOptions(
  input: McpToolExposureInput,
): Pick<Options, 'tools' | 'allowedTools' | 'disallowedTools'> {
  if (
    !input.hasOrcaMcpServer &&
    input.remoteMcpServerNames.length === 0 &&
    !input.enableAgentTool
  ) {
    return {};
  }
  for (const serverName of input.remoteMcpServerNames) {
    assertValidMcpServerName(serverName);
  }

  // Keep Claude Code built-ins out whenever the agent enables any managed MCP
  // surface. Otherwise remote-only MCP sessions can answer with XML-looking
  // text instead of dispatching structured `mcp__<server>__*` tool calls.
  //
  // Do NOT populate `allowedTools`: the SDK treats those as auto-approved
  // before calling `canUseTool`, which would bypass managed-agent
  // `always_ask` / `always_deny` policies. `canUseTool` is our permission
  // gate for every managed tool, including always-allow tools.
  return {
    tools: input.enableAgentTool ? ['Agent'] : [],
    disallowedTools: [...CLAUDE_CODE_BUILT_IN_TOOLS],
  };
}

function isManagedToolPermissionPolicy(value: unknown): value is ManagedToolPermissionPolicy {
  return value === 'always_allow' || value === 'always_ask' || value === 'always_deny';
}

function abortedToolPermissionResult(toolUseID: string): PermissionResult {
  return {
    behavior: 'deny',
    message: 'Tool permission request was aborted.',
    toolUseID,
  };
}

function remoteMcpToolPartsFromToolName(
  toolName: string,
): { serverName: string; toolName: string } | null {
  if (!toolName.startsWith('mcp__')) return null;
  const body = toolName.slice('mcp__'.length);
  const separatorIndex = body.indexOf('__');
  if (separatorIndex <= 0) return null;
  const serverName = body.slice(0, separatorIndex);
  const mcpToolName = body.slice(separatorIndex + 2);
  if (mcpToolName.length === 0) return null;
  return { serverName, toolName: mcpToolName };
}

function assertValidMcpServerName(serverName: string): void {
  if (!MCP_SERVER_NAME_ALLOWLIST_PATTERN.test(serverName)) {
    throw new Error(`invalid MCP server name for tool allowlist: ${serverName}`);
  }
}

function buildSubagentDefinitions(
  multiagent: SessionStartInput['agentSnapshot']['multiagent'] | undefined,
): {
  definitions: Record<string, AgentDefinition>;
  permissionPoliciesByType: Map<string, ManagedToolPermissionPolicies>;
  managedAgentIdsByType: Map<string, string>;
  modelIdsByType: Map<string, string>;
} {
  if (!multiagent || multiagent.type !== 'coordinator') {
    return {
      definitions: {},
      permissionPoliciesByType: new Map(),
      managedAgentIdsByType: new Map(),
      modelIdsByType: new Map(),
    };
  }
  const out: Record<string, AgentDefinition> = {};
  const permissionPoliciesByType = new Map<string, ManagedToolPermissionPolicies>();
  const managedAgentIdsByType = new Map<string, string>();
  const modelIdsByType = new Map<string, string>();
  const used = new Set<string>();
  for (const agent of multiagent.agents) {
    const key = uniqueSubagentKey(agent.name, agent.id, used);
    const tools = sdkToolNamesForSubagent(agent.allowed_tool_names);
    out[key] = {
      description: `Managed agent ${agent.name} (${agent.id} v${agent.version})`,
      prompt: agent.system ?? `You are ${agent.name}.`,
      ...(agent.model_id ? { model: agent.model_id } : {}),
      ...(agent.model_effort ? { effort: agent.model_effort } : {}),
      ...(tools ? { tools } : {}),
    };
    permissionPoliciesByType.set(key, { ...(agent.tool_permission_policies ?? {}) });
    managedAgentIdsByType.set(key, agent.id);
    if (agent.model_id) modelIdsByType.set(key, agent.model_id);
  }
  return { definitions: out, permissionPoliciesByType, managedAgentIdsByType, modelIdsByType };
}

const INTERNAL_PRIMARY_AGENT_BASE_NAME = '__orca_primary';

function buildPrimaryAgentBoundary(
  subagents: Record<string, AgentDefinition>,
  snapshot: SessionStartInput['agentSnapshot'],
  remoteMcpServerNames: readonly string[],
  primaryPrompt: string,
): { name: string; agents: Record<string, AgentDefinition> } {
  let name = INTERNAL_PRIMARY_AGENT_BASE_NAME;
  let suffix = 2;
  while (Object.hasOwn(subagents, name)) {
    name = `${INTERNAL_PRIMARY_AGENT_BASE_NAME}_${suffix}`;
    suffix += 1;
  }

  const tools = new Set<string>(['Agent']);
  for (const logicalName of snapshot.allowed_tool_names ?? []) {
    tools.add(`mcp__${ORCA_MCP_SERVER_NAME}__${logicalName}`);
  }
  for (const customTool of snapshot.custom_tools ?? []) {
    tools.add(`mcp__${ORCA_MCP_SERVER_NAME}__${customTool.name}`);
  }
  for (const serverName of remoteMcpServerNames) {
    assertValidMcpServerName(serverName);
    tools.add(`mcp__${serverName}__*`);
  }

  const primary: AgentDefinition = {
    description: 'Internal primary managed-agent boundary. Never delegate to this agent.',
    prompt: primaryPrompt,
    ...(snapshot.model_id ? { model: snapshot.model_id } : {}),
    ...(snapshot.model_effort ? { effort: snapshot.model_effort } : {}),
    tools: [...tools],
  };
  return {
    name,
    agents: { ...subagents, [name]: primary },
  };
}

function managedSubagentHooks(
  internalPrimaryName: string,
  onStart: (agentId: string, agentType: string) => void,
  onStop: (agentId: string) => void,
): NonNullable<Options['hooks']> {
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
            // Native Agent otherwise bypasses canUseTool. Ask routes it through
            // the existing managed permission callback, including the usage ACK
            // barrier, guardrail fold, and public approval when policy requires it.
            if (target !== internalPrimaryName) {
              return {
                hookSpecificOutput: {
                  hookEventName: 'PreToolUse',
                  permissionDecision: 'ask',
                  permissionDecisionReason: 'Managed subagent dispatch requires a policy decision.',
                },
              };
            }
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
    SubagentStart: [
      {
        hooks: [
          async (input) => {
            if (input.hook_event_name === 'SubagentStart') {
              onStart(input.agent_id, input.agent_type);
            }
            return {};
          },
        ],
      },
    ],
    SubagentStop: [
      {
        hooks: [
          async (input) => {
            if (input.hook_event_name === 'SubagentStop') {
              onStop(input.agent_id);
            }
            return {};
          },
        ],
      },
    ],
  };
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

function sdkToolNamesForSubagent(allowedLogicalNames: string[] | undefined): string[] | undefined {
  if (allowedLogicalNames === undefined) return undefined;
  // Preserve [] as an explicit deny-all boundary; omitting `tools` lets the SDK
  // inherit the globally registered inline MCP surface.
  return allowedLogicalNames.map((name) => `mcp__${ORCA_MCP_SERVER_NAME}__${name}`);
}

function outcomePrompt(criterion: OutcomeCriterion): string {
  const rubric =
    typeof criterion.rubric === 'string' ? criterion.rubric : JSON.stringify(criterion.rubric);
  return [
    'Complete the following managed-agent outcome. Work toward satisfying the rubric.',
    '',
    `Outcome description:\n${criterion.description}`,
    '',
    `Rubric:\n${rubric}`,
  ].join('\n');
}

function slugifyOutcomeId(description: string): string {
  return (
    description
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '') || 'defined_outcome'
  );
}

function normalizeMaxIterations(value: unknown): number {
  if (!Number.isInteger(value) || typeof value !== 'number' || value < 1) {
    return DEFAULT_OUTCOME_MAX_ITERATIONS;
  }
  return Math.min(value, MAX_OUTCOME_MAX_ITERATIONS);
}

function outcomeResultFromVerdict(
  verdict: OutcomeVerdict,
  iteration: number,
  maxIterations: number,
): OutcomeEvaluationResult {
  if (verdict.achieved) return 'satisfied';
  return iteration + 1 >= maxIterations ? 'max_iterations_reached' : 'needs_revision';
}

function zeroModelUsage(): ModelUsageCounts {
  return {
    input_tokens: 0,
    output_tokens: 0,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
  };
}

function textOf(payload: unknown): string | null {
  if (!payload || typeof payload !== 'object') return null;
  const arr = (payload as { content?: unknown }).content;
  if (!Array.isArray(arr)) return null;
  const text = arr
    .filter(
      (part: unknown) =>
        typeof part === 'object' &&
        part !== null &&
        (part as { type?: unknown }).type === 'text' &&
        typeof (part as { text?: unknown }).text === 'string',
    )
    .map((part) => (part as { text: string }).text)
    .filter((text) => text.length > 0)
    .join('\n');
  return text.length > 0 ? text : null;
}

function appendSystemMessageToToolResultContent(
  content: unknown,
  systemMessage: string,
): Array<{ type: 'text'; text: string } | unknown> {
  const reminder = {
    type: 'text' as const,
    text: `<system-reminder>\n${systemMessage}\n</system-reminder>`,
  };
  if (Array.isArray(content)) return [...content, reminder];
  if (typeof content === 'string') return [{ type: 'text', text: content }, reminder];
  if (content === undefined || content === null) return [reminder];
  return [content, reminder];
}

function combinedSystemPrompt(
  agentSystemPrompt: string | undefined,
  turnSystemPrompt: string | null | undefined,
): Pick<Options, 'systemPrompt'> | Record<string, never> {
  const prompts = [agentSystemPrompt, turnSystemPrompt].filter(
    (prompt): prompt is string => typeof prompt === 'string' && prompt.length > 0,
  );
  return prompts.length > 0 ? { systemPrompt: prompts.join('\n\n') } : {};
}
