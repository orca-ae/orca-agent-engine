// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { CustomTool, CustomToolResult } from '../custom-tools.js';
// The agent-execution seam the runner drives — the cross-component contract.
//
// The runner does NOT implement an agent harness; it CONSTRUCTS one from the
// snapshot the registry delivers and DRIVES it for each user turn, streaming its
// emitted events up the tunnel as Orca agent events. This interface is the seam:
// EVERY registered provider implements it — the in-process Claude Agent SDK
// harnesses (lean `claude` + persistent `claude-sdk-persistent`) in
// `src/harness/claude/`, the native-CLI harnesses (`claude-code` / `codex` /
// `cursor` / `pi` / `custom`) in their sibling directories, and the LLM-free `mock`
// — and the `ProviderRegistry` selects one by `snapshot.provider`. It is declared
// here (rather than imported from the harness-server package) because it is the
// cross-component CONTRACT the runner depends on — the runner is a separate process
// that links a provider, not the harness-server's internals.
//
// RELATIONSHIP TO `services/harness-server/src/harness/agent-harness.ts`: a
// DELIBERATE colocated-side fork, NOT a mirror. The two share one lifecycle spine —
// `start` once, `submit` per turn, `events()` as the single emitted stream, `stop`
// to end it — and that spine is what stays the same when a provider moves between
// the two sides. Everything else has diverged, in BOTH directions, along the axes the
// colocated runner needs and the separated harness-server does not (and vice versa),
// so a provider written against one side is NOT droppable into the other unchanged:
//
//   - {@link AgentHarness.interrupt} is REQUIRED here and absent there entirely. The
//     runner serves an out-of-band interrupt route, so aborting an in-flight turn
//     WITHOUT ending the harness is part of this contract; harness-server has no
//     counterpart, which alone makes the two interfaces non-substitutable.
//   - `submit` is `(event) => Promise<void>` here; harness-server's takes an optional
//     `SubmitHooks` and resolves `UserEventSubmitResult | void` — its durable-
//     acceptance + deferral protocol (with `UnappliedUserEventError`), which the
//     runner does not run.
//   - harness-server declares `hasPendingRequiredAction?()`; this side does not.
//   - {@link AgentEvent} adds `subpath` here (the multiagent thread addressing), and
//     {@link UserEvent} drops harness-server's `systemMessage` companion.
//   - {@link SessionStartInput} adds the runner's own seams — `confirmTool`,
//     `toolPermissions`, `delegate`, `agentSnapshot.skills_plugin_dir` — and omits
//     harness-server's client-tool and roster surface: `clientToolExecution`,
//     `tools` / `CustomToolDefinition`, and `agentSnapshot.multiagent` (with its
//     `runtimeAgentToolNameUnion` helper). The roster never reaches a provider on
//     this side — it stays on the WIRE snapshot (`src/snapshot.ts`), which the loop
//     reads to wrap the base harness in a `CoordinatorHarness`. This
//     `agentSnapshot` is also the narrower of the two — no `name`, `model_speed` /
//     `model_effort`, or `tool_permission_policies` (the `toolPermissions` resolver
//     fills that last role) — and `remoteMcpToolsets` is `{ serverName }` here vs.
//     harness-server's policy-carrying entry.
//   - {@link TerminationReason} omits harness-server's `'session.updated'`.
//
// Nothing enforces parity across the gap and nothing is meant to: a change on one
// side is a change to that side's contract only.
//
// Turn model the runner relies on (the contract a provider must honor): `submit`
// RESOLVES when the turn it drives is COMPLETE — every agent event the turn
// produced has already been emitted through `events()` by the time `submit`'s
// promise settles. The runner's turn handler uses that resolution as the turn
// boundary: it ends the tunneled agent-event stream once `submit` returns. A
// provider whose `submit` returns before the turn's events are drained would
// truncate the turn on the wire, so this resolve-when-done contract is load-bearing
// (the in-process claude harness satisfies it: its `submit` awaits the full SDK
// `query()` loop, which emits into the event queue synchronously as it runs).

import type { SandboxHandle, MaterializableSkillDescriptor } from '../sandbox/seam.js';

/**
 * Boot context captured at session start — the credential-free agent snapshot
 * projected into the shape the underlying SDK needs. Mirrors the harness-server's
 * `SessionStartInput.agentSnapshot` so a provider reads the same fields.
 */
/** Every managed tool result waits for Registry resource persistence before continuing. */
export type RunToolWithResources = <T>(
  operation: () => Promise<T>,
  publish: (checkpoint: import('../resources.js').ResourceCheckpoint, digest: string) => void,
  signal: AbortSignal,
) => Promise<T>;

export interface SessionStartInput {
  /** Verified immutable pins; providers may use these to refresh a native Skill catalog safely. */
  managedSkillCatalog?: { baseSystem: string; skills: readonly MaterializableSkillDescriptor[] };
  /** Park before publishing the matching custom_tool_use; resolved over the independent result route. */
  awaitCustomToolResult?: (id: string, signal: AbortSignal) => Promise<CustomToolResult>;
  toolSandbox?: SandboxHandle;
  runToolWithResources?: RunToolWithResources;
  /** Owning workspace (tenant scope) for the session. */
  workspaceId: string;
  /** Orca session id this harness serves, e.g. `"ses_a1b2..."`. */
  sessionId: string;
  /** The model + system + tool-allowlist the provider configures the SDK with. */
  agentSnapshot: {
    custom_tools?: CustomTool[];
    /** Model provider carried verbatim from the snapshot (e.g. `"anthropic"`). */
    model_provider?: string;
    /** Model id carried verbatim from the snapshot. */
    model_id?: string;
    model_effort?: string;
    /** Composed system prompt (agent + skills), already joined by the registry. */
    system?: string;
    /**
     * Logical sandbox tool names after the skill-allowlist intersection.
     * `[]` DENIES every tool (an mcp-only agent composes to it); only `undefined`
     * means "no restriction".
     */
    allowed_tool_names?: string[];
    /**
     * A sandbox-relative directory the agent's skills are staged under, when the
     * snapshot producer materialized them as a native plugin bundle. A native-CLI
     * provider (the `claude-code` harness) passes it to the CLI as `--plugin-dir`
     * so the bundled skills are discovered natively, in addition to the composed
     * `system` prompt. Absent when skills are delivered only as the composed prompt
     * + allowlist (the in-process claude providers have no native plugin surface and
     * ignore it regardless).
     */
    skills_plugin_dir?: string;
  };
  /**
   * MCP servers to wire into the underlying SDK, already rewritten by the
   * registry to point at the ai-gateway with the per-session JWT in the
   * Authorization header. Absent in chat-only mode.
   */
  mcpServers?: Record<string, { type: 'http'; url: string; headers: Record<string, string> }>;
  /**
   * Remote MCP server names enabled by `mcp_toolset`. The SDK exposes concrete
   * upstream tool names only after connecting, so a provider grants
   * `mcp__<server>__*` while still denying built-in host tools.
   */
  remoteMcpToolsets?: Array<{ serverName: string }>;
  /**
   * The per-session {@link SandboxHandle} the runner acquired for this session, or
   * `undefined` when the runner wired none (chat-only tests, the LLM-free `mock`
   * provider). A provider that has real built-in tools — the in-process claude
   * providers — binds its `orca` MCP tool server to this handle and anchors the SDK
   * `cwd` at its root, so the model's bash/read/write/edit/glob/grep AND the SDK's
   * own built-ins execute INSIDE the sandbox rather than on the runner host. Absent →
   * the provider stays LLM-only (its prior behavior). The loop owns the handle's
   * lifecycle (acquire on snapshot-apply, destroy on teardown); a provider never
   * destroys it.
   */
  sandbox?: SandboxHandle;
  /**
   * The tool-confirmation gate — the runner's `canUseTool` callback. When a provider
   * gates a tool call (the SDK's `canUseTool` fires), it calls this with the tool
   * name, input, and the call's `tool_use_id`; the gate PARKS a verdict and resolves
   * to a permission result once the user's `user.tool_confirmation` (delivered over
   * the tunnel, sourced from the durable transcript) lands — `allow` to proceed,
   * `deny` for a clean denial. Absent when the runner wires no confirmation (a
   * provider then runs without a human gate, e.g. `bypassPermissions`).
   */
  confirmTool?: ToolConfirmer;
  /**
   * Per-tool permission policy — which tool calls must PARK on the human gate
   * ({@link confirmTool}) and which AUTO-APPROVE. Mirrors the Managed Agents
   * `permission_policy` (`always_ask` vs `always_allow`): the registry composes it
   * from the agent's per-tool config and stamps it on the snapshot, so the runner
   * does not re-derive it.
   *
   * Read ONLY when {@link confirmTool} is also present (no gate → nothing to park on).
   * The provider resolves each tool call's effective policy via
   * {@link ToolPermissionResolver.policyFor}: `always_ask` parks (emits
   * `agent.requires_action`, awaits a verdict); `always_allow` proceeds with no human
   * gate and no signal. Absent → the provider parks EVERY gated tool call (fail-
   * closed: a tool with no policy is treated as `always_ask`), so a snapshot that
   * carries no per-tool policy keeps the gate engaged for everything.
   */
  toolPermissions?: ToolPermissionResolver;
  /**
   * The DELEGATION seam — present ONLY when this harness is a coordinator (its snapshot
   * carried a `multiagent` roster). The coordinator's provider exposes a delegate-to-agent
   * tool (the `agent_toolset` delegation tool) to the model; when the model calls it, the
   * provider invokes this callback with the target roster agent + the delegation prompt.
   * The callback SPAWNS a subagent thread (a fresh {@link AgentHarness} built from that
   * roster agent's config, sharing the SAME per-session sandbox), runs its turn, streams
   * its events on the child thread, and RESOLVES with the subagent's final result — which
   * the provider returns to the model as the delegation tool result.
   *
   * Absent for a single-agent harness AND for a subagent harness (a subagent gets no
   * delegate callback, so it exposes no delegation tool — one-level delegation). A
   * provider feature-detects it (`if (input.delegate) …`) and only wires the delegation
   * tool when present, so every existing single-agent provider is unchanged.
   */
  delegate?: DelegateToAgent;
}

/**
 * The delegate-to-agent callback a coordinator provider invokes when the model calls
 * the delegation tool. Spawns the target roster agent's subagent thread, runs the
 * delegated turn, and resolves with its result. Rejects only on an ENFORCED refusal
 * (unknown roster agent, concurrency cap exceeded) so the provider can surface a clean
 * tool error to the model; a subagent that merely produced no text resolves with an
 * empty-but-successful result.
 */
export type DelegateToAgent = (request: DelegateRequest) => Promise<DelegateResult>;

/** One delegation call the coordinator model made. */
export interface DelegateRequest {
  /** The roster agent to delegate to (a name from the coordinator's roster). */
  agentName: string;
  /** The delegation prompt — the task the coordinator hands the subagent. */
  prompt: string;
}

/** The outcome of a delegation — the subagent's final result, returned to the model. */
export interface DelegateResult {
  /** The subagent thread's id (`sth_…`) the delegation ran in. */
  sessionThreadId: string;
  /** The subagent's final text result (empty when it produced no text). */
  result: string;
}

/**
 * A tool-confirmation permission policy — whether a tool call must PARK on the human
 * gate or AUTO-APPROVE. Mirrors the Managed Agents `permission_policy` type so a
 * snapshot composed against either contract reads the same values.
 *
 * - `always_ask` — the call parks on {@link SessionStartInput.confirmTool} (emit
 *   `agent.requires_action`, await the `user.tool_confirmation` verdict).
 * - `always_allow` — the call proceeds immediately with no human gate.
 * - `always_deny` — the call is denied without opening a human gate.
 */
export type ToolPermissionPolicy = 'always_ask' | 'always_allow' | 'always_deny';

/**
 * Resolves the effective {@link ToolPermissionPolicy} for one tool call by name. The
 * provider consults this inside its `canUseTool` to decide park-vs-auto-approve. A
 * resolver is fail-closed by construction: an unknown tool (no per-tool entry and no
 * default) resolves to `always_ask`, so a tool the snapshot did not classify still
 * requires a human verdict.
 */
export interface ToolPermissionResolver {
  /** The effective policy for `toolName` — `always_ask` when unclassified (fail-closed). */
  policyFor(toolName: string): ToolPermissionPolicy;
}

/**
 * The tool-confirmation callback a provider invokes to gate a tool call — shaped to
 * map directly to the Claude Agent SDK's `canUseTool`. Parks the verdict keyed by
 * `toolUseId` and resolves to the permission decision; never rejects (a gated tool
 * always gets a decision: allow, or a clean deny on denial / teardown / timeout).
 */
export type ToolConfirmer = (
  toolName: string,
  input: Record<string, unknown>,
  opts: { toolUseId: string; timeoutMs?: number },
) => Promise<ToolPermissionDecision>;

/**
 * A tool-confirmation decision — the same shape as the SDK's `PermissionResult`.
 * `allow` carries the (possibly unchanged) tool input; `deny` carries a reason.
 */
export type ToolPermissionDecision =
  | { behavior: 'allow'; updatedInput: Record<string, unknown> }
  | { behavior: 'deny'; message: string };

/** One user turn the runner forwards into the provider's `submit`. */
export interface UserEvent {
  /** Event kind on the wire, e.g. `"user.message"`. */
  kind: string;
  /** Already-parsed JSON payload (the user event's transcript payload). */
  payload: unknown;
}

/** One agent event a provider emits, streamed up the tunnel by the runner. */
export interface AgentEvent {
  /** Event kind on the wire, e.g. `"agent.message"` / `"agent.turn_completed"`. */
  kind: string;
  /** Event payload (an object whose fields ride alongside `type` on the wire). */
  payload: unknown;
  /**
   * Optional stable per-event id. The runner emits it as the agent event's `id`
   * so the registry persists it with that identity and a re-push (recovery
   * replay, reconnect) dedups by it. Absent → the registry mints a fresh id.
   */
  id?: string;
  /**
   * Marks an `agent.error` as the harness's OWN TERMINAL explanation of why THIS turn
   * ended — set only by the fault paths that also end the turn (a failed launch, a crash
   * exit status, a turn deadline, the SDK's mid-turn catch, a coordinator base drain
   * fault). The runner loop reads it to decide whether its own generic terminal
   * `agent.error` would merely bury a more precise one.
   *
   * It exists because kind alone does not answer that question: NOT every `agent.error` is
   * terminal. The SDK's `system/mirror_error` — the transcript-mirror DATA-LOSS diagnostic
   * — is a mid-turn `agent.error` that says nothing about the turn ending, so a turn cut
   * short right after one would go UNEXPLAINED if the loop treated it as the last word.
   * Only the producer knows which of its errors is the turn's terminal explanation, so the
   * producer declares it.
   *
   * Purely internal plumbing: {@link SessionLoop} strips it (the NDJSON line carries
   * `payload` + `type`/`id`/`subpath` only), so it never reaches the wire or the transcript.
   */
  terminal?: true;
  /**
   * Optional transcript SUBPATH the event belongs to — the addressing that routes a
   * subagent-thread event to its OWN thread stream. The primary (session-level)
   * thread is the empty subpath; a coordinator's child thread emits its events under
   * `subagents/<session_thread_id>`. The runner surfaces this as a top-level field on
   * the NDJSON line (alongside `type` / `id`), so the registry bridge persists the
   * event under that subpath and the Threads API surfaces it on the right thread.
   *
   * Absent / empty for every single-agent event (the primary stream), so a plain
   * session's wire lines are byte-for-byte unchanged — the field is purely additive
   * and only the coordinator harness ever sets it.
   */
  subpath?: string;
}

/** Why a harness was stopped — informational, threaded into provider teardown. */
export type TerminationReason =
  | 'client.archived'
  | 'replica.shutting_down'
  | 'idle.timeout'
  | 'error';

/**
 * The agent runtime the runner constructs from the snapshot and drives per turn.
 *
 * Lifecycle: `start` once (configure the provider from the snapshot), then
 * `submit` per user turn (drive the turn; resolves when the turn's events are all
 * emitted — see the turn-model note above), with `events()` the single stream of
 * emitted agent events, `interrupt` to abort the in-flight turn WITHOUT ending the
 * harness, and `stop` to abort any in-flight turn + end `events()`.
 */
export interface AgentHarness {
  /** Retain the session filesystem while refreshed credentials rebuild this harness. */
  readonly preserveSandboxOnRefresh?: boolean;
  /** Configure the harness from the session boot context (snapshot). */
  start(input: SessionStartInput): Promise<void>;
  /** Drive one user turn. Resolves when the turn's agent events are all emitted. */
  submit(event: UserEvent): Promise<void>;
  /**
   * Abort the in-flight turn (if any) WITHOUT tearing down the harness — the
   * out-of-band `user.interrupt`. Fires the in-flight turn's abort signal (and
   * force-closes any live underlying process) so a turn blocked on the model unwinds:
   * its `submit` then resolves and any tool gate parked on it releases. The harness
   * STAYS ALIVE — `events()` keeps flowing and the next `submit` drives a fresh turn.
   * A no-op when no turn is in flight (interrupting an already-finished turn is
   * harmless), so a re-delivered interrupt is idempotent. Contrast {@link stop},
   * which also terminates the harness and ends `events()`.
   */
  interrupt(): void;
  /** Stop the harness, abort any in-flight turn, and end {@link events}. */
  stop(reason: TerminationReason): Promise<void>;
  /** The single stream of agent events the harness emits across its lifetime. */
  events(): AsyncIterable<AgentEvent>;
}

/**
 * The event kind every harness fault — terminal or mid-turn — is reported under. Declared
 * here beside {@link AgentEvent} because the two helpers below are the only supported way to
 * build one. `SessionLoop` declares the SAME string independently, as
 * `AGENT_ERROR_EVENT_KIND`, for the consumer-side rules that read it (this module sits below
 * the loop and must not import from it). The two must agree, and the conformance gate is what
 * says so: it filters the wire lines by the LOOP's constant and requires the harness's own
 * fault among them, so a divergence shows up as a turn that reported no error at all.
 */
const AGENT_ERROR_KIND = 'agent.error';

/**
 * Build the harness's OWN TERMINAL explanation of why a turn ended — the only supported way
 * to produce one.
 *
 * It exists because {@link AgentEvent.terminal} is unenforceable at a literal call site. The
 * flag is what suppresses the runner loop's generic terminal error, and the conformance gate
 * detects a MISSING one only INDIRECTLY, by counting a doubled `agent.error` — which works
 * only where the loop also had a terminal error of its own to emit. Wherever the harness
 * ends the turn ITSELF (every deadline handler, codex's `thread/start` / `turn/start`
 * failures, the persistent harness's terminal `api_retry`) nothing reads the flag and
 * nothing notices its absence: deleting `terminal: true` from claude-code's deadline
 * emission left the whole suite green. Stamping it in a helper removes the question from the
 * call site — a producer chooses between {@link terminalError} and {@link diagnosticError}
 * by NAME, and neither spelling can silently omit the flag.
 */
export function terminalError(message: string, extra?: Record<string, unknown>): AgentEvent {
  return { kind: AGENT_ERROR_KIND, terminal: true, payload: { message, ...extra } };
}

/**
 * Build a MID-TURN `agent.error` — a fault worth recording that is NOT why the turn ended,
 * so the runner loop still owes the turn its own terminal explanation.
 *
 * Two real cases: the SDK's `system/mirror_error` (a dropped transcript-mirror batch), and a
 * native CLI that died while the session was IDLE — a genuine fault with no turn to attribute
 * it to, so flagging it terminal would make the previous death the NEXT turn's stated cause
 * and bury that turn's real one.
 */
export function diagnosticError(message: string, extra?: Record<string, unknown>): AgentEvent {
  return { kind: AGENT_ERROR_KIND, payload: { message, ...extra } };
}
