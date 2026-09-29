// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { PendingCustomToolResults } from './pending-custom-tool-results.js';
import type { CustomToolResult } from './custom-tools.js';
// The runner core loop — the heart that holds one session's harness and serves
// the snapshot / turn / replay the registry pushes over the tunnel.
//
// This is the runner-role glue (session lifecycle + turn loop + event emission +
// snapshot consume), wired to Orca's substrate: it CONSTRUCTS an
// {@link AgentHarness} from the delivered snapshot (via the provider seam; claude
// is the default) and DRIVES it per user turn, streaming the harness's emitted
// events up the tunnel as Orca agent events. Everything else is reused from Orca
// rather than reimplemented here: tools / MCP / sandbox
// (the harness + snapshot carry the gateway-rewritten config), turn sequencing +
// catch-up + recovery decision (the OWNER POD's bridge does that — the runner sees
// one turn at a time and applies the pushed replay), and persistence (the bridge
// persists the agent events the runner emits). What is the runner's own job, and
// lives here:
//
//   - SNAPSHOT CONSUME ({@link applySnapshot}): parse the credential-free snapshot,
//     construct the provider's harness, `start` it. A re-delivered snapshot
//     (reconnect, newest-wins) tears down the old harness and builds a fresh one.
//   - TURN LOOP ({@link runTurn}): parse the user event, drive the harness, and
//     stream each emitted agent event back as one NDJSON line, ending the stream
//     when the turn completes (the harness's `submit` resolves) — synthesizing a
//     terminal `agent.turn_completed` if the harness did not emit one, so the
//     owner pod's "answered = a completed marker after the user turn" rule fires.
//   - EVENT EMISSION: one pump reads the harness's single event stream and routes
//     each event to the active turn's sink (turns are serial — the owner pod drives
//     one at a time — so a turn's events all reach its sink before `submit` returns).
//   - RECOVERY APPLY ({@link applyReplay}): apply the pushed `after={cursor}` replay
//     slice, deduping by stable event id, and track the highest consumed id so the
//     next (re)connect's hello advertises it as the per-session resume cursor (the
//     client half the registry's recovery left open).
//
// Concurrency: Node is single-threaded with one event loop. The owner-pod bridge
// drives turns serially (one tunneled turn POST at a time per session), so at most
// one turn sink is active; the pump and the turn handler never race on two turns.

import type { Event } from '@orca/transcript-store-types';
import type {
  AgentEvent,
  AgentHarness,
  SessionStartInput,
  TerminationReason,
  ToolConfirmer,
  UserEvent,
} from './harness/agent-harness.js';
import {
  ProviderRegistry,
  buildSessionStartInput,
  type ProviderSessionContext,
} from './harness/provider.js';
import { MOCK_PROVIDER_NAME } from './harness/mock/provider.js';
import {
  CoordinatorHarness,
  type SubagentBuild,
  type SubagentBuildContext,
} from './harness/multiagent/coordinator-harness.js';
import type { RunnerSnapshot } from './snapshot.js';
import {
  evaluateRequestPhase,
  prepareRunnerGuardrails,
  type PreparedRunnerGuardrails,
} from './guardrails.js';
import type { EnvironmentSpec, SandboxHandle, SandboxRuntime } from './sandbox/seam.js';
import { asTerminalHost, type TerminalHost } from './tools/sys-terminal.js';
import { parseSnapshotBody } from './snapshot.js';
import {
  materializeSkillsPlugin,
  parseSkillsPushBody,
  type ParsedSkillsPush,
  selectManagedSkills,
} from './skills-materialize.js';
import { RunnerResources } from './resources.js';
import { composeSkillsCatalog } from './sandbox/seam.js';
import { managedSkillIsBlocked } from './guardrails.js';
import { PendingApprovals } from './pending-approvals.js';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  TOOL_DENIED_MESSAGE,
  TOOL_DENIED_TIMEOUT_MESSAGE,
  type ToolPermissionResult,
} from './tool-confirmation.js';

/** Structured logger seam (a subset of the usual structured logger). All optional. */
export interface SessionLoopLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/**
 * The tunnel feed for the runner's local transcript log — the half of the
 * self-hosted model where the registry PUSHES the recovery replay DOWN the tunnel.
 *
 * The runner has no direct transcript backend (it is outbound-WSS-only behind a
 * NAT); its in-memory transcript store is the SDK's history substrate. When the
 * registry pushes a resume replay, {@link SessionLoop.applyReplay} feeds each
 * replayed event into this sink so the runner's local log mirrors what the registry
 * served. The {@link InMemoryTranscriptStore} implements this via `ingest`.
 *
 * Optional on the loop: the unit specs that drive the loop against a fake harness do
 * not wire a sink (they assert only the dedup + cursor bookkeeping), so the sink is
 * an additive collaborator `main.ts` injects in production — never required for the
 * loop to function.
 */
export interface TranscriptSink {
  /** Feed one registry-pushed (recovery-replayed) event into the local log. */
  ingest(event: Event): void;
}

/** Options for {@link SessionLoop}. */
export interface SessionLoopOptions {
  resources?: RunnerResources;
  /** Owning workspace (tenant scope) for the session this runner serves. */
  workspaceId: string;
  /** Provider-name → harness-factory registry (claude registered in `main.ts`). */
  providers: ProviderRegistry;
  /** Optional structured logger. */
  logger?: SessionLoopLogger;
  /**
   * Optional tunnel-fed transcript sink — the runner's local log the registry's
   * recovery replay is fed into ({@link TranscriptSink}). When present,
   * {@link SessionLoop.applyReplay} ingests each replayed event into it (in addition
   * to the dedup + cursor bookkeeping), so the local log mirrors the registry's. The
   * SAME store the claude providers' SDK adapter reads + writes, so the runner needs
   * no broker. Absent in the loop unit specs (they assert bookkeeping only).
   */
  transcriptSink?: TranscriptSink;
  /**
   * The runner's per-process {@link SandboxRuntime} (from `@orca/sandbox-runtime`).
   * When present, the loop hands it to every provider it builds via the provider
   * context ({@link ProviderSessionContext.sandboxRuntime}) so a native-CLI provider
   * can acquire a sandbox and drive `SandboxHandle.spawn`. Optional: the in-process
   * claude/mock providers ignore it, and the loop unit specs (which drive fake or
   * in-process providers) omit it — so leaving it unset changes nothing.
   */
  sandboxRuntime?: SandboxRuntime;
  /**
   * The runner's host workspace root, under which the owner-pod-pushed Skill bundles are
   * materialized as a native `--plugin-dir` plugin ({@link SessionLoop.applySkills}). In
   * production this is `config.workspace`. Optional: when unset (the loop unit specs), it
   * falls back to a per-process tmp dir so `applySkills` still works standalone.
   */
  skillsWorkspaceDir?: string;
  /**
   * Monotonic clock (seconds) backing the activity stamp the idle watchdog reads.
   * Defaults to a `performance.now()`-derived clock; a test injects a controllable
   * one to assert the watchdog interplay deterministically.
   */
  now?: () => number;
}

/**
 * Event kind that marks a turn COMPLETE. Mirrors the registry's
 * `COMPLETED_TURN_EVENT_KIND` ("answered up to here"): the owner pod's pending-turn
 * rule treats a user turn as answered only once a `agent.turn_completed` follows
 * it. The runner therefore guarantees every turn it drives ends with this marker —
 * either the harness emitted it, or the loop synthesizes one when the turn ends
 * without one, whether its events simply drained or it FAULTED — so a completed turn
 * is never mis-read as still pending. The one deliberate exception is a CANCELLED
 * turn, which ends with no marker so it stays re-promptable.
 */
export const TURN_COMPLETED_EVENT_KIND = 'agent.turn_completed';

/**
 * Event kind that carries a turn-level FAILURE onto the wire (and so into the durable
 * transcript). The runner emits one immediately before the synthesized
 * {@link TURN_COMPLETED_EVENT_KIND} whenever the turn it drove ended in a fault, so a
 * failed turn is never persisted as a successful but EMPTY answer. The harnesses use
 * the same kind for their own faults (e.g. the claude harness's mid-turn SDK error).
 */
export const AGENT_ERROR_EVENT_KIND = 'agent.error';

/**
 * The message for the terminal {@link AGENT_ERROR_EVENT_KIND}, or `undefined` when the
 * turn simply drained without a marker (no fault — the pre-existing "harness emitted no
 * marker" case, which only needs the synthesized marker).
 *
 * A `submit` rejection is the ROOT cause and wins: the stream ending mid-turn is how a
 * harness fault MANIFESTS (its read loop ends the stream on the way out), so reporting
 * the rejection's message says more than "the stream ended".
 *
 * The caller consults this only when the harness did NOT already end the turn with its
 * own `agent.error` — both messages here are generic next to the ones a harness emits.
 */
function turnFaultMessage(
  submitFailure: string | undefined,
  streamEndedMidTurn: boolean,
): string | undefined {
  if (submitFailure !== undefined) {
    return `harness turn failed: ${submitFailure}`;
  }
  return streamEndedMidTurn ? 'harness event stream ended before the turn completed' : undefined;
}

/**
 * Whether an agent event is the PRIMARY-thread turn-completed marker — the boundary of
 * the turn the runner is driving.
 *
 * A single-agent harness only ever emits primary events, so this is just a kind check
 * for it. A COORDINATOR harness (multiagent) interleaves its subagent threads' events
 * onto the same stream, and each subagent emits ITS OWN `agent.turn_completed` when its
 * delegated turn ends — but that is a CHILD-thread completion (it carries the child's
 * `subagents/<id>` subpath), NOT the coordinator's primary turn boundary. Ending the
 * primary turn on a child's marker would truncate the coordinator turn mid-flight
 * (dropping the coordinator's own remaining events + the delegation's finalization). So
 * the turn boundary is a turn-completed marker on the PRIMARY thread ONLY — an empty /
 * absent subpath. A child marker streams through as an ordinary event and the turn
 * continues, exactly as the multiagent choreography requires.
 */
function isPrimaryTurnCompleted(event: AgentEvent): boolean {
  return event.kind === TURN_COMPLETED_EVENT_KIND && (event.subpath ?? '') === '';
}

/**
 * Whether an agent event belongs to the PRIMARY thread — the turn the runner is driving.
 * A coordinator's subagent rides a `subagents/<id>` subpath (see
 * {@link isPrimaryTurnCompleted}); everything else is primary.
 */
/**
 * The user's text for a `request`-phase evaluation.
 *
 * Only the text blocks: a guardrail screening a message reasons about what the
 * user wrote, and a non-text block carries no prose to screen. Returns
 * `undefined` rather than `''` for a payload with no text at all, so a rule
 * distinguishing "no text" from "empty text" can.
 */
function userTextOf(userEvent: UserEvent): string | undefined {
  const payload = userEvent.payload;
  if (typeof payload !== 'object' || payload === null) return undefined;
  const content = (payload as { content?: unknown }).content;
  if (!Array.isArray(content)) return undefined;
  const text = content
    .filter(
      (block): block is { type: 'text'; text: string } =>
        typeof block === 'object' &&
        block !== null &&
        (block as { type?: unknown }).type === 'text' &&
        typeof (block as { text?: unknown }).text === 'string',
    )
    .map((block) => block.text)
    .join('\n');
  return text.length > 0 ? text : undefined;
}

function isPrimaryEvent(event: AgentEvent): boolean {
  return (event.subpath ?? '') === '';
}

/**
 * Whether an agent event is the harness's OWN TERMINAL explanation of why the turn ended —
 * an `agent.error` the producer marked {@link AgentEvent.terminal}.
 *
 * The flag is what separates a turn-ending error from a MID-TURN one, which kind alone
 * cannot: the SDK's `system/mirror_error` (a dropped transcript-mirror batch) maps to a
 * primary `agent.error` that says nothing about the turn ending. Treating that as the
 * turn's last word silenced the loop's own terminal fault, so a turn truncated right after
 * a dropped batch reported the DROP and never mentioned the truncation.
 */
function isTerminalAgentError(event: AgentEvent): boolean {
  return event.kind === AGENT_ERROR_EVENT_KIND && event.terminal === true;
}

/**
 * The most-recent-id window the replay dedup keeps (see {@link SessionLoop}'s
 * `appliedEventIds`). A hard ceiling on the in-memory dedup set so a long-lived
 * session does not grow it without bound. Comfortably larger than the registry's
 * per-recovery replay bound (`DEFAULT_MAX_REPLAY_EVENTS`, 10k) so a single full
 * replay never self-evicts an id it is still applying, while still bounding total
 * memory. The cursor — not this set — is the durable exactly-once anchor; this
 * window only catches the OVERLAPPING re-pushes a reconnect can deliver (always
 * near the recent tail, since the registry serves strictly `after={cursor}`).
 */
export const DEFAULT_DEDUP_WINDOW = 50_000;

/**
 * Owns one session's harness + the turn / snapshot / replay behaviors.
 *
 * One {@link SessionLoop} per runner process (a runner serves one session). The
 * tunnel handlers ({@link registerSessionHandlers}) call {@link applySnapshot} /
 * {@link runTurn} / {@link applyReplay}; the loop owns the harness lifecycle, the
 * event pump, and the resume-cursor bookkeeping.
 */
export class SessionLoop {
  private readonly workspaceId: string;
  private readonly providers: ProviderRegistry;
  private readonly logger: SessionLoopLogger | undefined;
  /**
   * The tunnel-fed transcript sink (the runner's local log), or `undefined` when no
   * sink is wired. {@link applyReplay} ingests each replayed event into it so the
   * local log mirrors the registry's recovery replay.
   */
  private readonly transcriptSink: TranscriptSink | undefined;
  /**
   * The runner's sandbox runtime (from `@orca/sandbox-runtime`), or `undefined`
   * when none was wired. Handed to every provider via the provider context so a
   * native-CLI provider can `acquire` a sandbox + `spawn` a CLI child. The
   * in-process providers ignore it.
   */
  private readonly sandboxRuntime: SandboxRuntime | undefined;
  /**
   * The runner workspace root the owner-pod-pushed Skills are materialized under (see
   * {@link SessionLoopOptions.skillsWorkspaceDir}); falls back to a per-process tmp dir.
   */
  private readonly skillsWorkspaceDir: string;
  /**
   * The absolute `--plugin-dir` the runner materialized the pushed Skills into, or
   * `undefined` when the session has no Skills. Recorded by {@link applySkills} (which
   * runs BEFORE the snapshot) and injected onto the snapshot by {@link applySnapshot}
   * when the snapshot itself carried no `skills_plugin_dir` (the runner owns the path).
   */
  private skillsPluginDir: string | undefined;
  private skillsPush: ParsedSkillsPush | undefined;
  private readonly resources: RunnerResources;
  private resourceSessionId: string | undefined;
  private configuring = false;
  private configurationEpoch = 0;
  private configurationDone: Promise<void> | undefined;
  private stopped = false;

  /** The constructed harness for the current snapshot; `undefined` before delivery. */
  private harness: AgentHarness | undefined;
  /**
   * The per-session {@link SandboxHandle} acquired for the CURRENT snapshot (the one
   * the real providers bind their `orca` MCP tool server + SDK `cwd` to), or
   * `undefined` when none was acquired (no runtime wired, or the LLM-free `mock`
   * provider). The loop OWNS its lifecycle: it is acquired in {@link applySnapshot}
   * (before the harness is built, so the handle can ride the provider context) and
   * destroyed in {@link teardownHarness}. A provider may preserve it across a
   * refresh of the same session; changing sessions or stopping always releases it.
   */
  private sandbox: SandboxHandle | undefined;
  private snapshotBinding: { sessionId: string; provider: string; revision?: string } | undefined;
  /**
   * The SINGLE iterator over the current harness's `events()` stream, created at
   * snapshot-apply and read by ONE turn at a time. The owner pod drives turns
   * serially, so a single shared reader (rather than a competing background pump)
   * is correct and race-free: a turn's events are pulled directly here, and the
   * turn boundary is settled by racing the next pull against `submit` resolving.
   */
  /**
   * Guardrails composed for this session, plus their state. Rebuilt on every
   * snapshot (newest-wins), so an edited rule takes effect when the owner pod
   * re-delivers rather than living for the session's lifetime.
   */
  private guardrails: PreparedRunnerGuardrails | undefined;
  private eventsIterator: AsyncIterator<AgentEvent> | undefined;
  /**
   * The pending `eventsIterator.next()` carried ACROSS turn iterations. A pull is
   * raced against `submit` resolving; when `submit` wins, the in-flight pull must
   * NOT be discarded (that would drop the event it eventually yields), so it is
   * parked here and consumed first on the next pull.
   */
  private pendingNext: Promise<IteratorResult<AgentEvent>> | undefined;
  /**
   * Per-session last-consumed event id (`sessionId → eventId`), advanced by
   * {@link applyReplay} as the runner consumes the pushed replay slice. Read by
   * the tunnel hello so a (re)connect advertises the resume cursor the owner pod
   * serves an incremental `after={cursor}` slice from.
   */
  private readonly resumeCursorsBySession = new Map<string, string>();
  /**
   * Stable transcript ids the runner has already applied — the replay dedup window.
   *
   * BOUNDED (FIFO-evicting): a long-lived session's transcript grows without limit,
   * but the dedup need does NOT — the registry serves strictly `after={cursor}`, so
   * the only re-pushes a reconnect can deliver are OVERLAPPING chunks near the
   * current resume boundary (a flapping reconnect re-serving the most recent slice).
   * Those overlaps are always near the recent tail, never deep in old history, so a
   * bounded most-recent window catches every duplicate the registry can re-push
   * while giving the set a hard memory ceiling. The window cap is comfortably larger
   * than the registry's per-recovery replay bound (`DEFAULT_MAX_REPLAY_EVENTS`), so
   * a single full replay never self-evicts mid-apply. The resume cursor
   * ({@link resumeCursorsBySession}) is the durable exactly-once anchor; this set is
   * the in-memory fast-path that makes a re-push a no-op without re-deriving.
   */
  private readonly appliedEventIds = new BoundedIdSet(DEFAULT_DEDUP_WINDOW);
  /**
   * The runner-side parked tool-confirmation verdicts (the client half of the
   * uniform transcript approval). The harness's `canUseTool` parks a verdict here
   * via {@link confirmTool}; the registry-delivered `user.tool_confirmation`
   * resolves it via {@link resolveToolConfirmation}. One table per loop (a runner
   * serves one session); a harness teardown denies every outstanding verdict so a
   * gated tool call is never left blocked.
   */
  private readonly approvals = new PendingApprovals();
  private readonly customToolResults = new PendingCustomToolResults();
  /**
   * Monotonic time of the last real work the loop did (a snapshot / turn / replay).
   * The idle watchdog ({@link runInactivityMonitor}) measures the idle window
   * against this — the loop-level counterpart of the serve loop's per-WORK-frame
   * activity touch (these handler entries ARE the runner's work frames).
   */
  private lastActivityAt: number;
  /** Count of turns currently in flight (drives the watchdog's active-work gate). */
  private activeTurns = 0;
  /** Monotonic clock the activity stamp + watchdog share (injectable for tests). */
  private readonly now: () => number;

  constructor(opts: SessionLoopOptions) {
    this.workspaceId = opts.workspaceId;
    this.providers = opts.providers;
    this.logger = opts.logger;
    this.transcriptSink = opts.transcriptSink;
    this.sandboxRuntime = opts.sandboxRuntime;
    this.skillsWorkspaceDir =
      opts.skillsWorkspaceDir ?? join(tmpdir(), 'orca-session-runner-skills');
    this.resources =
      opts.resources ?? new RunnerResources({ workspaceDir: this.skillsWorkspaceDir });
    this.now = opts.now ?? (() => performance.now() / 1000);
    this.lastActivityAt = this.now();
  }

  /** The monotonic time of the loop's last real work (for the idle watchdog). */
  lastActivity(): number {
    return this.lastActivityAt;
  }

  /**
   * Refresh the loop's last-activity stamp to NOW. The serve loop calls this for
   * each real server→runner work frame (`request` / `request.cancel` / `ws.*`, NOT
   * `ping`) via the runner's `onActivity` hook, so a work frame that does not enter
   * a loop method — notably a standalone `request.cancel` between turns — still
   * defers the idle window. The per-method touches ({@link applySnapshot} /
   * {@link runTurn} / {@link applyReplay}) remain the source of truth when a frame
   * does drive the loop; this is the serve-loop-level analog the watchdog relies on.
   */
  touchActivity(): void {
    this.lastActivityAt = this.now();
  }

  /** Whether a turn is currently in flight (the idle watchdog's active-work gate). */
  hasActiveWork(): boolean {
    return this.activeTurns > 0;
  }

  /** The provider names this runner can serve (for the tunnel hello advertise). */
  providerNames(): string[] {
    return this.providers.providerNames();
  }

  /**
   * Whether a harness has been configured (a snapshot was delivered + applied).
   * The turn handler checks this BEFORE streaming so a turn that raced ahead of
   * snapshot delivery surfaces a 503 (not a misleading empty 200).
   */
  hasHarness(): boolean {
    return this.harness !== undefined;
  }

  /**
   * The CURRENT session's {@link TerminalHost}, or `null` when the session has no
   * terminal-capable sandbox (no sandbox acquired yet, or a cloud-only handle that
   * exposes no panes). The remote terminal-attach handler resolves the host through
   * this rather than capturing a fixed one, so it always attaches to the live
   * per-snapshot sandbox — which the loop rebuilds on each snapshot-apply.
   */
  currentTerminalHost(): TerminalHost | null {
    return asTerminalHost(this.sandbox);
  }

  /**
   * The per-session resume cursors to advertise on (re)connect
   * (`sessionId → lastConsumedEventId`). A copy, so the caller cannot mutate the
   * loop's bookkeeping. Empty until the runner has applied at least one replayed
   * event for a session.
   */
  resumeCursors(): Record<string, string> {
    return Object.fromEntries(this.resumeCursorsBySession);
  }

  private async configure<T>(operation: () => Promise<T>): Promise<T> {
    if (this.stopped || this.configuring || this.hasActiveWork())
      throw new Error('runner configuration is busy');
    this.configuring = true;
    this.configurationEpoch += 1;
    let complete!: () => void;
    this.configurationDone = new Promise<void>((resolve) => {
      complete = resolve;
    });
    try {
      return await operation();
    } finally {
      this.configuring = false;
      this.configurationDone = undefined;
      complete();
    }
  }

  assertResourceSession(sessionId: string): void {
    if (
      !/^[A-Za-z0-9_-]{1,128}$/.test(sessionId) ||
      (this.resourceSessionId !== undefined && this.resourceSessionId !== sessionId)
    )
      throw new Error('resource session mismatch');
  }

  async applyResources(sessionId: string, body: unknown): Promise<unknown> {
    this.assertResourceSession(sessionId);
    if (this.snapshotBinding && this.snapshotBinding.sessionId !== sessionId)
      throw new Error('resource session mismatch');
    if (this.hasActiveWork()) throw new Error('cannot configure resources during a turn');
    this.resourceSessionId = sessionId;
    this.touchActivity();
    return this.configure(() => this.resources.push(sessionId, body));
  }

  async resourceChanges(sessionId: string, body: unknown): Promise<unknown> {
    this.assertResourceSession(sessionId);
    if (this.snapshotBinding && this.snapshotBinding.sessionId !== sessionId)
      throw new Error('resource session mismatch');
    this.resourceSessionId = sessionId;
    this.touchActivity();
    return this.resources.changes(sessionId, body);
  }

  async acknowledgeResources(sessionId: string, body: unknown): Promise<void> {
    this.assertResourceSession(sessionId);
    this.touchActivity();
    await this.resources.acknowledge(sessionId, body);
  }

  /**
   * Materialize the owner pod's Skill bundle push into a native `--plugin-dir` plugin
   * under the runner workspace, BEFORE the snapshot arrives.
   *
   * The colocated runner holds no `@orca/skill-store` + no object-store credentials, so
   * the owner pod PUSHES the session's Skill bundle bytes here (verified before they
   * leave the registry). This parses the NDJSON push, writes the plugin layout
   * (delete-and-rebuild + read-only mode policy + reserved-path checks — see
   * {@link materializeSkillsPlugin}), and RECORDS the plugin dir so {@link applySnapshot}
   * can inject it as the snapshot's `skills_plugin_dir` (→ `--plugin-dir`).
   *
   * Idempotent / newest-wins: a re-delivery (reconnect) re-materializes, replacing the
   * prior tree. Throws {@link SkillsPushParseError} on a malformed body (the handler maps
   * it to 400) and rethrows a write failure (mapped to 500) so the owner pod records it
   * undelivered + re-pushes on the next reconnect.
   *
   * @param sessionId The session the Skills belong to (from the request header).
   * @param body The raw skills push body (NDJSON: one manifest line, then one per file).
   */
  async applySkills(sessionId: string, body: Uint8Array): Promise<void> {
    return this.configure(() => this.applySkillsNow(sessionId, body));
  }

  private async applySkillsNow(sessionId: string, body: Uint8Array): Promise<void> {
    this.lastActivityAt = this.now();
    this.assertResourceSession(sessionId);
    if (this.hasActiveWork()) throw new Error('cannot configure Skills during a turn');
    const push = parseSkillsPushBody(body);
    if (this.resources.revision !== undefined) {
      if (!push.descriptors) throw new Error('managed Skills require verified descriptors');
      this.skillsPush = push;
      this.skillsPluginDir = undefined;
      return;
    }
    const pluginDir = await materializeSkillsPlugin({
      workspaceDir: this.skillsWorkspaceDir,
      push,
    });
    this.skillsPluginDir = pluginDir;
    this.logger?.info?.(
      { sessionId, pluginDir, skills: push.skills.length },
      'session runner materialized skills plugin',
    );
  }

  /**
   * Apply a delivered snapshot: construct the provider's harness and start it.
   *
   * Parses the single-line NDJSON body, dispatches on `snapshot.provider` to build
   * the harness, and starts it with the snapshot-derived boot context. A
   * re-delivered snapshot (a reconnect re-runs delivery, newest-wins) STOPS the
   * prior harness + pump and replaces them, so the runner is always driving the
   * harness the latest snapshot describes. Throws on a malformed body, an unknown
   * provider, or a provider/start failure — the snapshot handler maps that to a
   * non-2xx ack (the owner pod records it undelivered and retries on reconnect).
   *
   * @param sessionId The session the snapshot configures (from the request header).
   * @param body The raw snapshot push body (one NDJSON line).
   */
  async applySnapshot(sessionId: string, body: Uint8Array): Promise<void> {
    return this.configure(() => this.applySnapshotNow(sessionId, body));
  }

  private async applySnapshotNow(sessionId: string, body: Uint8Array): Promise<void> {
    this.lastActivityAt = this.now();
    this.assertResourceSession(sessionId);
    if (this.hasActiveWork()) throw new Error('cannot configure snapshot during a turn');
    const snapshot = parseSnapshotBody(body);
    const managed = snapshot.managed_resources;
    if (managed !== undefined || this.resources.revision !== undefined) {
      if (
        !managed ||
        managed.revision !== this.resources.revision ||
        !this.resources.toolSandbox ||
        !this.resources.readyForSnapshot
      )
        throw new Error('managed resource revision is not committed');
      if (!this.providers.supportsManagedResources(snapshot.provider))
        throw new Error('provider does not support managed resources');
      if (this.resources.pendingCheckpoint)
        throw new Error('resource checkpoint requires acknowledgement');
      if (snapshot.multiagent) throw new Error('managed resources do not support multiagent');
    }
    // Refuse before anything is acquired. A guardrail this topology can never
    // honor must not reach a running session — same posture as the sandboxed
    // harness, which rejects `llm_request` rather than opening a sandbox that
    // would send those bytes unscreened. Throwing acks the push non-2xx, so the
    // owner pod records it undelivered and retries.
    const guardrails = prepareRunnerGuardrails(snapshot);
    let managedSkillCatalog: SessionStartInput['managedSkillCatalog'];
    if (managed) {
      managedSkillCatalog = {
        baseSystem: snapshot.system,
        skills:
          selectManagedSkills(this.skillsPush, snapshot.skills ?? [], () => false).descriptors ??
          [],
      };
      const push = selectManagedSkills(this.skillsPush, snapshot.skills ?? [], (skill) =>
        managedSkillIsBlocked(snapshot, sessionId, skill.name),
      );
      await this.resources.replaceSkills(sessionId, push);
      snapshot.system = composeSkillsCatalog(snapshot.system, push.descriptors ?? []);
      delete snapshot.skills_plugin_dir;
    }
    // Inject the Skill plugin dir the runner materialized from the owner pod's skills
    // push (which arrives BEFORE the snapshot). The runner OWNS this path (the registry
    // cannot know the runner's filesystem), so `??=` fills it in only when the snapshot
    // itself did not carry one — a producer that pinned a dir up front keeps it. A
    // session with no Skills leaves both undefined, so the field stays absent
    // (`buildSessionStartInput` then omits `--plugin-dir`). Newest-wins: a re-delivery
    // re-materialized the tree, so `this.skillsPluginDir` is the current path.
    if (
      !managed &&
      snapshot.skills_plugin_dir === undefined &&
      this.skillsPluginDir !== undefined
    ) {
      snapshot.skills_plugin_dir = this.skillsPluginDir;
    }
    // Replace any prior harness (newest-wins on a reconnect re-delivery): stop the
    // old one so its event stream cannot race the new one. Only an explicitly
    // resumable provider may keep the sandbox when the same session is refreshed.
    const keepSandbox =
      this.harness?.preserveSandboxOnRefresh === true &&
      this.snapshotBinding?.sessionId === sessionId &&
      this.snapshotBinding.provider === snapshot.provider &&
      this.snapshotBinding.revision === managed?.revision;
    await this.teardownHarness('replica.shutting_down', keepSandbox);
    this.snapshotBinding = {
      sessionId,
      provider: snapshot.provider,
      ...(managed ? { revision: managed.revision } : {}),
    };

    // ACQUIRE the per-session sandbox for a REAL provider (before building the harness
    // so the handle can ride the provider context). The runner selects the runtime at
    // boot the same way the harness-server does — by the `SANDBOX_RUNTIME` env var:
    // `local` yields the srt-wrapped Local runtime (an operator sets it on a self-hosted
    // deployment; it degrades to InMemory when the host has no `srt`), unset / `in-memory`
    // yields InMemory for tests/dev — see `sandbox/seam.ts`. Here the loop turns that
    // runtime into a per-session handle the claude providers bind their `orca` tool server
    // + SDK `cwd` to. Skipped for the LLM-free `mock` provider and when no runtime is
    // wired — those stay sandbox-free. Reuse is limited to the same provider and session.
    this.sandbox ??= await this.acquireSandbox(snapshot.provider);

    // Hand the runner's sandbox runtime AND the acquired per-session handle to the
    // provider via the context: the handle gives the in-process claude providers their
    // real built-in tools (bash/read/write in the sandbox), and the runtime lets a
    // native-CLI provider `acquire` its own sandbox to `SandboxHandle.spawn` a CLI.
    // Each is spread in only when present — under `exactOptionalPropertyTypes` an
    // explicit `undefined` is not assignable to the optional slots. A provider that
    // ignores them (mock) is unaffected.
    const ctx: ProviderSessionContext = {
      workspaceId: this.workspaceId,
      sessionId,
      ...(managed
        ? {
            toolSandbox: this.resources.toolSandbox!,
            runToolWithResources: this.resources.runTool.bind(this.resources),
          }
        : {}),
      ...(this.sandbox !== undefined ? { sandbox: this.sandbox } : {}),
      ...(this.sandboxRuntime !== undefined ? { sandboxRuntime: this.sandboxRuntime } : {}),
    };
    const baseHarness = this.providers.build(snapshot, ctx);
    // Bind the tool-confirmation gate onto the boot context so a provider that gates
    // a tool call routes through this loop's parking (the client half of the uniform
    // transcript approval). `confirmTool` is bound to `this` so the harness can hold
    // the reference across turns. The per-tool permission policy (which tool calls
    // park vs auto-approve) rides `startInput.toolPermissions`, projected by
    // `buildSessionStartInput` from the snapshot — a provider parks only `always_ask`
    // tools through this gate (and, with no policy, is fail-closed and parks all).
    const startInput = buildSessionStartInput(snapshot, ctx);
    if (managedSkillCatalog) startInput.managedSkillCatalog = managedSkillCatalog;
    // Production ALWAYS binds the gate here, so the harness always takes the
    // `canUseTool` path: every session (chat-only included) runs through this loop's
    // parking, and with no tool-permission policy it is fail-closed (parks all). The
    // harness's no-gate `bypassPermissions` branch is therefore test-only (reachable
    // solely by a caller that hands a `startInput` without `confirmTool`) — a real
    // session never runs headless without a gate.
    const gate: ToolConfirmer = (toolName, input, opts) => this.confirmTool(toolName, input, opts);
    startInput.confirmTool = gate;
    const callbackEpoch = this.configurationEpoch;
    startInput.awaitCustomToolResult = (id, signal) => {
      if (this.stopped || callbackEpoch !== this.configurationEpoch)
        throw new Error('custom tool callback generation retired');
      return this.customToolResults.park(id, signal);
    };
    // COORDINATOR WRAP (additive): when the snapshot carries a `multiagent` roster, wrap
    // the built base harness in a coordinator that orchestrates subagent threads — ONE
    // session, MULTIPLE threads, all sharing THIS session's sandbox + vault. A
    // single-agent snapshot has no `multiagent`, so `maybeWrapCoordinator` returns the
    // base harness untouched and the single-agent path is byte-for-byte unchanged. The
    // wrap keys on the snapshot DATA (the roster), not the provider name, so the loop
    // still never branches on provider — every provider becomes a coordinator additively.
    const harness = this.maybeWrapCoordinator(snapshot, ctx, baseHarness, gate);
    // `start` the (possibly-wrapped) harness with the confirm-gated boot context. For a
    // coordinator, `start` injects the `delegate` seam and starts the base underneath.
    await harness.start(startInput);
    this.guardrails = guardrails;
    this.harness = harness;
    // One shared iterator over the harness's event stream; serial turns read it.
    this.eventsIterator = harness.events()[Symbol.asyncIterator]();
    this.pendingNext = undefined;
    this.logger?.info?.(
      {
        sessionId,
        provider: snapshot.provider,
        model: snapshot.model.id,
        coordinator: snapshot.multiagent !== undefined,
      },
      'session runner applied snapshot (harness configured)',
    );
  }

  /**
   * Wrap a coordinator snapshot's base harness in a {@link CoordinatorHarness}, or
   * return the base harness untouched for a single-agent snapshot.
   *
   * The wrap is ADDITIVE and keys purely on the snapshot DATA — a `multiagent` roster
   * — never on the provider, so a single-agent session (the overwhelming majority) is
   * byte-for-byte unchanged, and ANY provider becomes a coordinator when its snapshot
   * carries a roster. The subagent factory dispatches each roster member's own snapshot
   * back through the SAME provider registry that built the coordinator, handing it THIS
   * session's sandbox + sandbox-runtime via the provider context — so a subagent's tools
   * execute in the coordinator's ONE shared sandbox (shared filesystem across threads),
   * and its history/model/tools come from its own snapshot. The coordinator starts each
   * subagent itself (with a delegation-free start input), so the factory only CONSTRUCTS.
   *
   * @param snapshot The applied snapshot (its `multiagent` decides the wrap).
   * @param ctx The provider context the base was built with (carries the shared sandbox).
   * @param base The coordinator's own built harness (wrapped) — or the plain harness.
   * @param gate The confirm-tool gate, threaded onto each subagent so a subagent's gated
   *   tool call parks on the SAME uniform transcript approval as the coordinator's.
   */
  private maybeWrapCoordinator(
    snapshot: RunnerSnapshot,
    ctx: ProviderSessionContext,
    base: AgentHarness,
    gate: ToolConfirmer,
  ): AgentHarness {
    const multiagent = snapshot.multiagent;
    if (multiagent === undefined) {
      return base; // single-agent: unchanged.
    }
    // The subagent context shares THIS session's sandbox + runtime, so a subagent
    // provider binds its tools to the coordinator's one sandbox. Spread each in only
    // when present (exactOptionalPropertyTypes) — a chat-only coordinator has neither.
    const subagentProviderCtx: ProviderSessionContext = {
      workspaceId: this.workspaceId,
      sessionId: ctx.sessionId,
      ...(this.sandbox !== undefined ? { sandbox: this.sandbox } : {}),
      ...(this.sandboxRuntime !== undefined ? { sandboxRuntime: this.sandboxRuntime } : {}),
    };
    const coordinatorOpts: ConstructorParameters<typeof CoordinatorHarness>[0] = {
      base,
      multiagent,
      workspaceId: this.workspaceId,
      sessionId: ctx.sessionId,
      confirmTool: gate,
      // Build a subagent harness + its start input from a roster member's OWN snapshot,
      // via the SAME provider registry (its provider is whatever the roster agent
      // declared) and the SAME `buildSessionStartInput` projection the single-agent path
      // uses — so the subagent gets its own model/system/tools/MCP. The context shares
      // THIS session's sandbox; the gate is bound on the start input so the subagent's
      // gated tool calls park on the same uniform approval. The coordinator strips any
      // `delegate` off the returned input (one-level delegation).
      buildSubagent: (member, _ctx: SubagentBuildContext): SubagentBuild => {
        const harness = this.providers.build(member.snapshot, subagentProviderCtx);
        const startInput = buildSessionStartInput(member.snapshot, subagentProviderCtx);
        startInput.confirmTool = gate;
        return { harness, startInput };
      },
    };
    if (this.sandbox !== undefined) {
      coordinatorOpts.sandbox = this.sandbox;
    }
    if (this.logger !== undefined) {
      coordinatorOpts.logger = this.logger;
    }
    return new CoordinatorHarness(coordinatorOpts);
  }

  /**
   * Drive one user turn and stream its agent events back as NDJSON.
   *
   * Parses the user event from the body, then yields each agent event the harness
   * emits for the turn as one NDJSON line (`{...payload, type: kind, id?}` — the
   * shape the owner pod's bridge persists). The turn boundary is the harness's
   * `submit` resolving (the contract: `submit` returns once the turn's events are
   * all emitted); the loop then drains any still-queued events and, if the harness
   * never emitted a {@link TURN_COMPLETED_EVENT_KIND} marker, synthesizes one so the
   * owner pod sees the turn as answered.
   *
   * Yields an empty stream + leaves the turn undriven when no harness is configured
   * yet (the snapshot has not been delivered) — the handler maps that to a 503 so
   * the owner pod retries after delivery. A `request.cancel` (the abort signal)
   * ends the stream early WITHOUT tearing down the harness: the next turn reuses it
   * (the owner pod treats a partial turn as lossy-on-drop and re-prompts). A cancel
   * is per-turn: it stops the turn, not the session.
   *
   * @param sessionId The session the turn belongs to (from the request header).
   * @param body The user event's transcript payload (JSON).
   * @param signal Fires on `request.cancel` / tunnel teardown for this turn.
   * @returns An async-iterable of NDJSON agent-event line bytes.
   */
  runTurn(sessionId: string, body: Uint8Array, signal: AbortSignal): AsyncIterable<Uint8Array> {
    this.lastActivityAt = this.now();
    // Parse EAGERLY (outside the returned generator) so a bad body throws here and
    // the handler maps it to a 400 before any streaming response is framed. The
    // harness is captured at call time so a teardown between call + iteration does
    // not switch the turn onto a different harness instance.
    const harness = this.harness;
    const epoch = this.configurationEpoch;
    this.assertTurnReady(sessionId, harness, epoch);
    const userEvent = parseUserEvent(body);
    return this.streamTurn(harness, sessionId, userEvent, signal, epoch);
  }

  /** Recheck when the deferred body is consumed, before occupying the turn slot. */
  private assertTurnReady(
    sessionId: string,
    harness: AgentHarness | undefined,
    epoch: number,
  ): void {
    this.assertResourceSession(sessionId);
    if (
      this.stopped ||
      this.configuring ||
      this.hasActiveWork() ||
      epoch !== this.configurationEpoch ||
      harness !== this.harness ||
      (this.snapshotBinding !== undefined && this.snapshotBinding.sessionId !== sessionId) ||
      this.snapshotBinding?.revision !== this.resources.revision ||
      (this.snapshotBinding?.revision !== undefined && !this.resources.readyForTurn)
    )
      throw new Error('runner resource configuration is not ready');
  }

  /**
   * The lazy turn stream: a 503-style empty stream when no harness is configured
   * yet, else the driven turn. Split from {@link runTurn} so the generator is a
   * method (no `this` aliasing) and the eager body parse stays in `runTurn`.
   */
  private async *streamTurn(
    harness: AgentHarness | undefined,
    sessionId: string,
    userEvent: UserEvent,
    signal: AbortSignal,
    epoch: number,
  ): AsyncIterable<Uint8Array> {
    this.assertTurnReady(sessionId, harness, epoch);
    if (harness === undefined) {
      this.logger?.warn?.(
        { sessionId },
        'session runner received a turn before a snapshot; nothing to drive',
      );
      // No harness yet: yield nothing. The handler surfaces a 503 (head not 200)
      // so the owner pod retries the turn after snapshot delivery.
      return;
    }
    // No await separates validation from occupation: configuration cannot enter
    // after this point, including while async-generator delegation is queued.
    this.activeTurns += 1;
    try {
      yield* this.driveTurn(harness, sessionId, userEvent, signal);
    } finally {
      this.activeTurns -= 1;
      this.lastActivityAt = this.now();
    }
  }

  /**
   * Apply a pushed resume replay slice, deduping by stable event id.
   *
   * The body is NDJSON (one persisted-event JSON per line, each carrying its stable
   * transcript `id`). The runner applies each event it has NOT already applied
   * (re-applying a known id is a no-op — the dedup that makes a re-push idempotent,
   * the other half of the registry recovery's exactly-once), and advances the
   * session's resume cursor to the LAST id in the slice so the next (re)connect
   * advertises it. A blank / malformed / id-less line is skipped (never aborts the
   * apply). Recovery is state-rebuild only — applying a replay never drives a turn.
   *
   * The runner's local transcript log ({@link TranscriptSink}) is the SDK's history
   * substrate; when a sink is wired, each NEWLY applied event is also INGESTED into
   * it, so the runner's local log mirrors the registry's recovery replay (the tunnel
   * feed). Note the replay carries only PUBLIC events: the registry's recovery
   * deliberately excludes internal `harness.*` events (the SDK's own session entries),
   * so ingesting the replay rebuilds the runner's view of the public transcript — it
   * does NOT re-seed the SDK's per-turn history (which the harness rebuilds from its
   * own `append`s within the live runner). So "apply" here is: dedup + cursor advance
   * + feed the local log. The cursor is what closes the loop the registry's recovery
   * left open (it serves `after={cursor}`; the runner sources the cursor).
   *
   * @param sessionId The session the replay belongs to (from the request header).
   * @param body The replay push body (NDJSON, possibly empty for a caught-up ack).
   * @returns The number of NEWLY applied (non-duplicate) events.
   */
  applyReplay(sessionId: string, body: Uint8Array): number {
    this.assertResourceSession(sessionId);
    this.lastActivityAt = this.now();
    const lines = splitNdjson(Buffer.from(body).toString('utf8'));
    let applied = 0;
    let lastId: string | undefined;
    for (const line of lines) {
      const parsed = parseReplayLine(line);
      if (parsed === undefined) {
        this.logger?.warn?.({ sessionId }, 'session runner dropped a malformed replay line');
        continue;
      }
      lastId = parsed.id;
      if (this.appliedEventIds.has(parsed.id)) {
        continue; // dedup: a re-pushed event the runner already holds is a no-op.
      }
      this.appliedEventIds.add(parsed.id);
      applied += 1;
      // Feed the newly-applied event into the runner's local transcript log (the
      // tunnel feed). Only on a fresh apply, so a re-pushed event is never re-ingested.
      this.transcriptSink?.ingest(replayLineToEvent(this.workspaceId, sessionId, parsed));
    }
    // Advance the resume cursor to the last id the slice carried (even when every
    // event in it was a duplicate) so the next reconnect resumes strictly after it.
    if (lastId !== undefined) {
      this.resumeCursorsBySession.set(sessionId, lastId);
    }
    this.logger?.info?.(
      { sessionId, applied, cursor: lastId ?? null },
      'session runner applied resume replay slice',
    );
    return applied;
  }

  /**
   * The tool-confirmation gate the harness's `canUseTool` calls — park a verdict for
   * a gated tool call and resolve to the SDK permission result once the user's
   * confirmation is delivered (or the wait budget elapses).
   *
   * Parks a verdict keyed by `opts.toolUseId` and awaits it. On ALLOW the tool
   * proceeds with its (unchanged) input (`behavior: 'allow'`); on DENY — an explicit
   * deny verdict, a teardown ({@link stop} denies every outstanding verdict), or a
   * timeout — the tool gets a clean denial (`behavior: 'deny'`) with a reason. The
   * returned promise NEVER rejects, so a gated `canUseTool` always gets a decision.
   *
   * This is the harness-facing seam of the parking: the runner constructs the
   * harness with this bound as its confirmation callback, so a provider that gates a
   * tool routes through the uniform transcript approval without knowing the wire.
   *
   * @param _toolName The tool the model wants to use (informational; the verdict is
   *   keyed by tool-use id, not name). Carried for symmetry with the SDK callback.
   * @param input The tool input; echoed back as `updatedInput` on an allow.
   * @param opts The in-flight tool call's `toolUseId` (+ optional per-call timeout).
   */
  async confirmTool(
    _toolName: string,
    input: Record<string, unknown>,
    opts: { toolUseId: string; timeoutMs?: number },
  ): Promise<ToolPermissionResult> {
    const verdict = await this.approvals.park({
      toolUseId: opts.toolUseId,
      ...(opts.timeoutMs !== undefined ? { timeoutMs: opts.timeoutMs } : {}),
    });
    if (verdict.approved) {
      return { behavior: 'allow', updatedInput: input };
    }
    return {
      behavior: 'deny',
      message: verdict.timedOut ? TOOL_DENIED_TIMEOUT_MESSAGE : TOOL_DENIED_MESSAGE,
    };
  }

  /** Route only to the configured session's live callback generation. */
  resolveCustomToolResult(sessionId: string, result: CustomToolResult): boolean {
    this.assertCallbackSession(sessionId);
    this.touchActivity();
    return this.customToolResults.resolve(result);
  }

  assertCallbackSession(sessionId: string): void {
    if (!sessionId || this.snapshotBinding?.sessionId !== sessionId)
      throw new Error('custom tool result session mismatch');
  }

  /**
   * Resolve a parked tool-confirmation verdict — driven by the confirmation route
   * when a `user.tool_confirmation` arrives. Idempotent: returns `false` for an
   * unknown tool-use id or a verdict that already landed (a re-delivered
   * confirmation), so the first verdict wins and a flapping reconnect's re-push
   * cannot flip an in-flight tool call.
   *
   * @param toolUseId The tool-use id the confirmation correlates to.
   * @param approved `true` on allow, `false` on deny.
   * @returns `true` only when this call settled a still-pending park.
   */
  resolveToolConfirmation(toolUseId: string, approved: boolean): boolean {
    this.lastActivityAt = this.now();
    return this.approvals.resolve(toolUseId, approved);
  }

  /**
   * Whether a tool-confirmation verdict is outstanding (any parked, or a specific
   * tool-use id when given). The "session is awaiting a human approval" signal — a
   * future mid-turn ingest guard reads it so it does not steer a turn past the gate.
   */
  hasPendingApproval(toolUseId?: string): boolean {
    return this.approvals.hasPending(toolUseId);
  }

  /**
   * Interrupt the in-flight turn — the effect of the interrupt route when a
   * `user.interrupt` arrives. Aborts the harness's in-flight turn ({@link
   * AgentHarness.interrupt}) so a turn blocked on the model unwinds: its `submit`
   * resolves, any parked tool gate releases as a denial, and the turn's wire stream
   * ends. The harness STAYS ALIVE — the next turn reuses it — so this is NOT a
   * teardown (contrast {@link stop}, which tears the harness down).
   *
   * Out-of-band by construction: the interrupt route runs CONCURRENTLY with the turn
   * driver (turns are serial and the in-flight turn blocks the owner-pod turn loop
   * inside its streaming POST), so this is the only path that can preempt a blocked
   * turn — an interrupt delivered as a turn body would queue behind the very turn it
   * is meant to unblock. Idempotent + safe between turns: interrupting an
   * already-finished turn (or before any harness exists) is a harmless no-op, so a
   * re-pushed interrupt cannot disturb a later turn.
   *
   * Any tool-confirmation verdict parked for the interrupted turn is moot once the
   * turn is gone, so this also denies every outstanding approval ({@link
   * PendingApprovals.reset}) — clearing the "awaiting a human verdict" signal
   * ({@link hasPendingApproval}) in lockstep with the turn ending, rather than
   * leaving it stranded until the park's deadline elapses. Turns are serial, so at
   * most the in-flight turn's approvals are parked; resetting them cannot disturb a
   * later turn (it parks on a clean table).
   *
   * @returns `true` when a harness was present to interrupt; `false` before any
   *   snapshot configured one (the handler still acks — the interrupt is moot).
   */
  interruptTurn(): boolean {
    this.lastActivityAt = this.now();
    const harness = this.harness;
    if (harness === undefined) {
      return false;
    }
    harness.interrupt();
    // Release any gate the interrupted turn parked on: the turn is gone, so the
    // verdict is moot. Denies outstanding approvals so `hasPendingApproval` clears now.
    this.approvals.reset();
    this.customToolResults.reset();
    return true;
  }

  /**
   * Stop the loop: tear down the harness (aborting any in-flight turn) and drain
   * the pump. Idempotent; safe before any snapshot was applied.
   */
  async stop(reason: TerminationReason = 'replica.shutting_down'): Promise<void> {
    this.stopped = true;
    await this.configurationDone;
    await this.teardownHarness(reason);
    await this.resources.close();
  }

  /**
   * Drive one turn against a configured harness, yielding NDJSON agent-event lines.
   *
   * Reads the loop's SINGLE shared event iterator (serial turns, so no competing
   * reader) and settles the turn boundary by racing each pull against `submit`
   * resolving and the cancel signal:
   *   - a pulled event is yielded; the turn-completed marker ends it;
   *   - when `submit` resolves first (the harness emitted all the turn's events and
   *     none was a terminal marker), the loop flush-drains the already-buffered
   *     events (a pull that does not resolve within a macrotask means the buffer is
   *     empty), then synthesizes the completed marker so the owner pod sees the turn
   *     answered;
   *   - a cancel or consumer close interrupts and drains the turn WITHOUT a
   *     synthetic marker, leaving the partial turn re-promptable.
   * A pull that wins a `submit`-race but is still in flight is PARKED in
   * {@link pendingNext} so its eventual event is consumed by the next turn — never
   * dropped.
   *
   * A FAULTED turn takes the same terminal path, never a silent one. A harness fault
   * surfaces two ways — `submit` REJECTS, and/or the shared event stream ENDS mid-turn
   * (the claude harness's mid-turn catch and every native-CLI read loop end the stream
   * before unwinding) — and either would otherwise leave the turn without its marker
   * (pending forever) or, worse, with a bare marker that reads as a successful EMPTY
   * answer. So both are recorded (`submitFailure` / `streamEndedMidTurn`) and the
   * terminal lines are emitted after cleanup on an open response: an `agent.error`
   * carrying the fault, then the completed marker. The failure lands in the DURABLE
   * transcript, not only on stderr. Cleanup itself never yields, so closing the
   * consumer always releases the turn after the harness settles.
   *
   * That `agent.error` is skipped when the harness already ended the turn with one of its
   * own (the loop sees every event it forwards, so it knows). Most harnesses do, and the
   * loop's generic message would then land LAST — burying the precise cause the harness
   * named. An open, uncancelled response always receives the MARKER: it is the one
   * thing the owner pod's pending-turn rule reads.
   */
  /**
   * Run the `request` phase over a user turn, returning the denial reason or
   * `undefined` to proceed.
   *
   * Turn-scoped counters are reset first, exactly as the host-side harness does
   * at its own turn boundary: a turn budget that never reset would deny every
   * turn after the first.
   *
   * Errors are the engine's problem, not this method's — `request` is a
   * fail-closed phase, so an evaluator that throws already resolves to deny
   * inside the engine and arrives here as a verdict rather than an exception.
   */
  private evaluateRequestGuardrails(sessionId: string, userEvent: UserEvent): string | undefined {
    const guardrails = this.guardrails;
    if (guardrails === undefined || guardrails.prepared.length === 0) return undefined;
    guardrails.store.resetTurn();
    const userText = userTextOf(userEvent);
    const decision = evaluateRequestPhase(guardrails.prepared, guardrails.store, {
      phase: 'request',
      sessionId,
      ...(userText !== undefined ? { userText } : {}),
    });
    if (decision.verdict === 'allow') return undefined;
    const reason =
      decision.reasons.length > 0
        ? decision.reasons.join('; ')
        : 'This message was blocked by a guardrail.';
    this.logger?.info?.(
      { sessionId, verdict: decision.verdict, deniedBy: decision.deniedBy },
      'session runner denied a turn at the request phase',
    );
    return reason;
  }

  private async *driveTurn(
    harness: AgentHarness,
    sessionId: string,
    userEvent: UserEvent,
    signal: AbortSignal,
  ): AsyncIterable<Uint8Array> {
    let sawCompleted = false;
    // The fault, if any, that ended this turn — read after cleanup to emit the
    // terminal `agent.error`. `submitFailure` is the harness's own rejection message
    // (the ROOT cause, so it wins); `streamEndedMidTurn` marks a turn cut short by the
    // shared event stream ending (or a pull faulting) before any completed marker.
    let submitFailure: string | undefined;
    let streamEndedMidTurn = false;
    // Whether the LAST primary-thread event the consumer received was the harness's own
    // TERMINAL `agent.error` — i.e. the harness already made the turn's final word an
    // explanation of the fault. Most harnesses do (the claude ones emit the SDK's message
    // before re-raising; every native-CLI read loop emits the launch failure / crash exit
    // status / turn deadline), and the terminal `agent.error` below would then DOUBLE it,
    // burying the real cause under a generic "harness turn failed: …".
    //
    // TWO things make this the right question, and each was a shipped bug:
    //
    //   LAST, not sticky. It answers "is the line immediately before the marker already the
    //   reason?" — a "saw one somewhere" flag would silence the loop for a turn whose error
    //   came mid-stream and whose last word was an ordinary message, leaving a truncated
    //   turn with no terminal explanation at all.
    //
    //   TERMINAL, not merely of kind `agent.error`. NOT every `agent.error` ends a turn: the
    //   SDK's `system/mirror_error` — the transcript-mirror DATA-LOSS diagnostic, emitted
    //   whenever a store/Kafka append batch is dropped — is a mid-turn error that says
    //   nothing about the turn ending. Keying on kind alone let one of those disarm the
    //   loop, so a turn cut short right after a dropped batch reported the DROP and never
    //   mentioned the truncation. Only the producer knows which of its errors is the turn's
    //   last word, so the producer declares it ({@link AgentEvent.terminal}).
    let lastPrimaryWasTerminalError = false;
    // The `request` phase, evaluated before the model is asked anything.
    //
    // A deny does not need new terminal handling: it takes the path a failed
    // submit already takes. `submitFailure` is what makes the terminal handling below
    // report a turn that produced no answer instead of ending it as a clean
    // empty one, and a resolved `submitSettled` lets the drain fall straight
    // through to that handling with nothing streamed. The turn ends denied,
    // through exactly the machinery a submit rejection already uses.
    const requestDenial = this.evaluateRequestGuardrails(sessionId, userEvent);
    if (requestDenial !== undefined) {
      submitFailure = requestDenial;
    }

    // Kick off the turn. `submit` resolves once the turn's events are all emitted
    // into the harness's stream (the contract); resolving wins the race below and
    // triggers the flush-drain of those buffered events. A denied request submits
    // nothing — the model must not see a message a guardrail refused.
    let submitDone = false;
    const submitSettled = (
      requestDenial === undefined
        ? harness.submit(userEvent).catch((err: unknown) => {
            // Contain the rejection (an uncaught one would kill the turn stream), but
            // RECORD it: a turn whose submit failed produced no answer, so the stream
            // must say so on the wire instead of ending it as a clean empty turn.
            submitFailure = err instanceof Error ? err.message : String(err);
            this.logger?.error?.({ err, sessionId }, 'session runner harness submit failed');
          })
        : Promise.resolve()
    ).finally(() => {
      submitDone = true;
    });

    let streamFinished = false;
    try {
      turn: {
        // Phase 1: stream events as they arrive, racing each pull against submit +
        // cancel. Ends on the completed marker, a cancel, the harness stream ending,
        // or submit resolving (→ phase 2 flush-drain).
        while (!signal.aborted) {
          const pull = this.takeNext();
          const outcome = await raceTurnPull(pull, submitSettled, signal);
          if (outcome === 'aborted') {
            // Cancelled mid-turn. The in-flight pull (and any further events this
            // turn emits) belong to the CANCELLED turn, so they must not bleed into
            // the next turn on the shared iterator. Park the pull, then discard the
            // cancelled turn's remaining buffered events below.
            this.pendingNext = pull;
            break;
          }
          if (outcome === 'submit-done') {
            // Submit resolved first; the in-flight pull is parked for the next turn.
            this.pendingNext = pull;
            break;
          }
          // A pulled event (outcome === 'event'): the pull is consumed.
          this.pendingNext = undefined;
          const result = await this.resolvePull(pull);
          if (result.done) {
            // The harness stream ended (a mid-turn fault, or a stop) or the pull failed:
            // end the turn. Terminal handling emits the error + marker — returning
            // silently here is what left a faulted turn pending forever.
            streamEndedMidTurn = true;
            break turn;
          }
          yield encodeAgentEventLine(result.value);
          // Record whether the turn's last primary word is now the harness's OWN TERMINAL
          // `agent.error`, so terminal handling does not bury it under a generic one. A
          // SUBAGENT's error is that delegation's failure, not this turn's, so it neither
          // sets nor clears the flag (same primary-vs-subpath rule as the marker below).
          if (isPrimaryEvent(result.value)) {
            lastPrimaryWasTerminalError = isTerminalAgentError(result.value);
          }
          // Only a PRIMARY-thread completed marker ends the turn; a coordinator's
          // subagent-thread completion (a child subpath) streams through and the turn
          // continues (see isPrimaryTurnCompleted).
          if (isPrimaryTurnCompleted(result.value)) {
            sawCompleted = true;
            break turn;
          }
        }

        if (signal.aborted) break turn;

        // Phase 2: submit resolved. Flush the events it already buffered (a pull that
        // does not resolve within a macrotask means the buffer is empty), yielding
        // each; stop at the completed marker.
        while (!signal.aborted) {
          const pull = this.takeNext();
          const ready = await raceBufferedPull(pull);
          if (ready === 'empty') {
            // No more buffered events; park the pull for the next turn.
            this.pendingNext = pull;
            break;
          }
          this.pendingNext = undefined;
          const result = await this.resolvePull(pull);
          if (result.done) {
            // Same fault shape as phase 1: the stream ended before the turn completed.
            streamEndedMidTurn = true;
            break turn;
          }
          yield encodeAgentEventLine(result.value);
          // Record whether the turn's last primary word is now the harness's OWN TERMINAL
          // `agent.error`, so terminal handling does not bury it under a generic one. A
          // SUBAGENT's error is that delegation's failure, not this turn's, so it neither
          // sets nor clears the flag (same primary-vs-subpath rule as the marker below).
          if (isPrimaryEvent(result.value)) {
            lastPrimaryWasTerminalError = isTerminalAgentError(result.value);
          }
          // Only a PRIMARY-thread completed marker ends the turn; a coordinator's
          // subagent-thread completion (a child subpath) streams through and the turn
          // continues (see isPrimaryTurnCompleted).
          if (isPrimaryTurnCompleted(result.value)) {
            sawCompleted = true;
            break turn;
          }
        }
      }
      streamFinished = true;
    } finally {
      if (!streamFinished || signal.aborted) {
        // return()/throw() at a yield closes this response, even without an
        // aborted request signal. Interrupt first so parked tools/native turns
        // can settle, then drain their events before another turn uses the stream.
        harness.interrupt();
        this.approvals.reset();
        this.customToolResults.reset();
        await this.discardCancelledTurn(submitSettled);
      } else if (!submitDone) {
        await submitSettled;
      }
    }

    // Keep every yield outside cleanup. A consumer closing its iterator must
    // receive done:true, never a synthetic completion from a suspended finally.
    if (!sawCompleted && !signal.aborted) {
      const fault = lastPrimaryWasTerminalError
        ? undefined
        : turnFaultMessage(submitFailure, streamEndedMidTurn);
      if (fault !== undefined) {
        yield encodeAgentEventLine({ kind: AGENT_ERROR_EVENT_KIND, payload: { message: fault } });
      }
      yield encodeAgentEventLine({ kind: TURN_COMPLETED_EVENT_KIND, payload: {} });
    }
  }

  /**
   * Await a pull, mapping a REJECTED pull (an event-stream fault) to a clean
   * stream-end so the turn ends instead of throwing out of the handler (the
   * registry's awaiter then completes). The fault is logged.
   */
  private async resolvePull(
    pull: Promise<IteratorResult<AgentEvent>>,
  ): Promise<IteratorResult<AgentEvent>> {
    try {
      return await pull;
    } catch (err) {
      this.logger?.error?.({ err }, 'session runner event stream pull failed; ending turn');
      return { value: undefined, done: true };
    }
  }

  /**
   * Discard a cancelled turn's remaining events off the shared iterator so the
   * next turn starts clean. Waits for the cancelled turn's `submit` to resolve
   * (the harness has then emitted all of that turn's events into the buffer), then
   * flush-drains every buffered event WITHOUT yielding it — stopping at the
   * completed marker, the stream end, or an empty buffer. Bounded: a turn emits a
   * finite number of events, all buffered by the time `submit` resolves, and the
   * caller has already {@link AgentHarness.interrupt}ed the harness so even a turn
   * that was BLOCKED on the model unwinds and `submit` resolves promptly (without the
   * interrupt this `await` would hang on a blocked turn).
   */
  private async discardCancelledTurn(submitSettled: Promise<void>): Promise<void> {
    await submitSettled;
    for (;;) {
      const pull = this.takeNext();
      const ready = await raceBufferedPull(pull);
      if (ready === 'empty') {
        // Nothing more buffered: park the (next-turn) pull and stop discarding.
        this.pendingNext = pull;
        return;
      }
      this.pendingNext = undefined;
      const result = await this.resolvePull(pull);
      // Stop discarding at the PRIMARY turn boundary (or stream end); a coordinator's
      // subagent-thread completion is not the turn boundary (see isPrimaryTurnCompleted).
      if (result.done || isPrimaryTurnCompleted(result.value)) {
        return;
      }
    }
  }

  /** The next pull from the shared iterator: reuse a parked one, else pull fresh. */
  private takeNext(): Promise<IteratorResult<AgentEvent>> {
    const parked = this.pendingNext;
    if (parked !== undefined) {
      this.pendingNext = undefined;
      return parked;
    }
    const iterator = this.eventsIterator;
    if (iterator === undefined) {
      // No harness stream (torn down): an immediately-done pull ends the turn.
      return Promise.resolve({ value: undefined, done: true } as IteratorResult<AgentEvent>);
    }
    return iterator.next();
  }

  /**
   * Acquire the per-session {@link SandboxHandle} for a provider, or `undefined` when
   * none should be acquired.
   *
   * Real providers (the in-process claude providers, and any native-CLI provider) get a
   * sandbox so their tools execute in isolation; the LLM-free `mock` provider does NOT
   * (it answers deterministically with no filesystem/exec, and the tunnel-only e2e path
   * selects it precisely to run without a sandbox). When no runtime is wired (a loop
   * built without one — the unit specs that drive fake/in-process providers), there is
   * nothing to acquire. The `EnvironmentSpec` is minimal: the runtime was already
   * selected by target at boot, so `acquire` needs no per-session shape here.
   *
   * A failure to acquire is fatal to the snapshot apply (the provider would otherwise
   * run without the sandbox the model expects), so this rethrows — the snapshot handler
   * maps it to a non-2xx ack and the owner pod retries on the next reconnect.
   */
  private async acquireSandbox(provider: string): Promise<SandboxHandle | undefined> {
    if (this.sandboxRuntime === undefined || !providerUsesSandbox(provider)) {
      return undefined;
    }
    const env: EnvironmentSpec = {};
    return this.sandboxRuntime.acquire(env);
  }

  /** Destroy the current per-session sandbox handle (best-effort), then forget it. */
  private async releaseSandbox(): Promise<void> {
    const sandbox = this.sandbox;
    this.sandbox = undefined;
    if (sandbox === undefined) {
      return;
    }
    try {
      await sandbox.destroy();
    } catch (err) {
      this.logger?.error?.({ err }, 'session runner sandbox destroy failed');
    }
  }

  /** Stop + forget the current harness and its event stream. A no-op when none. */
  private async teardownHarness(reason: TerminationReason, keepSandbox = false): Promise<void> {
    const harness = this.harness;
    this.harness = undefined;
    this.eventsIterator = undefined;
    this.pendingNext = undefined;
    // Deny every outstanding tool-confirmation verdict so a gated tool call parked
    // on the (now stopping) harness is never left blocked forever. A fresh snapshot
    // builds a new harness; its tool calls park on a clean table.
    this.approvals.reset();
    this.customToolResults.reset();
    if (harness !== undefined) {
      try {
        await harness.stop(reason);
      } catch (err) {
        this.logger?.error?.({ err }, 'session runner harness stop failed');
      }
    }
    // Release the per-session sandbox AFTER the harness has stopped: the harness may
    // still touch it while draining its final events, so destroying it first could
    // fault an in-flight tool call. Providers can opt into preserving the handle on refresh.
    if (!keepSandbox) await this.releaseSandbox();
  }
}

/**
 * Whether a provider gets a per-session sandbox. Real providers (the in-process claude
 * providers + native-CLI providers) do — their tools must execute in isolation, not on
 * the runner host. The LLM-free `mock` provider does NOT: it answers deterministically
 * with no filesystem/exec, and the sandbox is only for real providers.
 */
function providerUsesSandbox(provider: string): boolean {
  return provider !== MOCK_PROVIDER_NAME;
}

/** A turn pull's race outcome: a real event, submit resolving first, or a cancel. */
type TurnPullOutcome = 'event' | 'submit-done' | 'aborted';

/**
 * Race a pull against `submit` resolving and the cancel signal. Resolves `'event'`
 * when the pull settles first, `'submit-done'` when submit finishes first (the
 * pull stays in flight — the caller parks it), `'aborted'` on cancel. None of the
 * three consumes the pull's value (the caller awaits the pull on the `'event'`
 * branch), so a parked pull is never dropped.
 */
function raceTurnPull(
  pull: Promise<IteratorResult<AgentEvent>>,
  submitSettled: Promise<void>,
  signal: AbortSignal,
): Promise<TurnPullOutcome> {
  return new Promise<TurnPullOutcome>((resolve) => {
    let settled = false;
    const done = (outcome: TurnPullOutcome): void => {
      if (settled) {
        return;
      }
      settled = true;
      signal.removeEventListener('abort', onAbort);
      resolve(outcome);
    };
    const onAbort = (): void => done('aborted');
    if (signal.aborted) {
      resolve('aborted');
      return;
    }
    signal.addEventListener('abort', onAbort, { once: true });
    // A settled pull (resolved OR rejected) counts as an 'event' — the caller then
    // awaits it via resolvePull, which maps a rejection to a clean stream-end. This
    // keeps a rejected pull from surfacing as an unhandled rejection.
    void pull.then(
      () => done('event'),
      () => done('event'),
    );
    void submitSettled.then(() => done('submit-done'));
  });
}

/**
 * Race a buffered pull against a macrotask: `'ready'` when the event is already
 * buffered (resolves within the macrotask), `'empty'` when it is not (the buffer
 * is drained). Used after `submit` resolves to flush exactly the events the turn
 * already emitted without blocking on the next turn's (absent) events.
 */
function raceBufferedPull(pull: Promise<IteratorResult<AgentEvent>>): Promise<'ready' | 'empty'> {
  return new Promise<'ready' | 'empty'>((resolve) => {
    let settled = false;
    const settle = (v: 'ready' | 'empty'): void => {
      if (!settled) {
        settled = true;
        resolve(v);
      }
    };
    // A settled pull (resolved OR rejected) is 'ready' — resolvePull then maps a
    // rejection to a clean end, so a rejected parked pull is never unhandled.
    void pull.then(
      () => settle('ready'),
      () => settle('ready'),
    );
    // A macrotask boundary: a buffer-backed pull (the fake harness's queue, the
    // claude harness's synchronously-filled queue) resolves before this fires; a
    // pull blocked on an unfilled queue does not, so the buffer is empty.
    setImmediate(() => settle('empty'));
  });
}

/**
 * Encode one agent event as an NDJSON line: spread the payload, then stamp `type`
 * (the kind always wins over any `type` in the payload — matching the harness
 * server's `pumpEvents`), the optional stable `id`, and the optional transcript
 * `subpath`. The owner pod's bridge reads exactly this shape
 * (`{ type, id?, subpath?, ... }`): the `subpath` routes a coordinator's
 * subagent-thread event to its own thread stream (`subagents/<id>`); a primary /
 * single-agent event carries no `subpath`, so its line is unchanged.
 */
function encodeAgentEventLine(event: AgentEvent): Uint8Array {
  const payloadObj =
    event.payload !== null && typeof event.payload === 'object'
      ? (event.payload as Record<string, unknown>)
      : {};
  const line: Record<string, unknown> = { ...payloadObj, type: event.kind };
  if (event.id !== undefined) {
    line.id = event.id;
  }
  // Only stamp a non-empty subpath — a primary-thread event (empty / absent) keeps
  // the single-agent line shape, so the bridge persists it at the session subpath.
  if (event.subpath !== undefined && event.subpath.length > 0) {
    line.subpath = event.subpath;
  }
  return new TextEncoder().encode(`${JSON.stringify(line)}\n`);
}

/** Thrown when the turn body is not a well-formed user event object. */
export class UserEventParseError extends Error {
  constructor(message: string) {
    super(`turn body parse failed: ${message}`);
    this.name = 'UserEventParseError';
  }
}

/**
 * Parse the turn body (the user event's transcript payload) into a
 * {@link UserEvent}. The payload is the public `user.*` event JSON (it carries its
 * own `type`); the harness reads the kind + payload. A non-object body is a
 * {@link UserEventParseError} (the handler maps it to a 400).
 */
export function parseUserEvent(body: Uint8Array): UserEvent {
  const text = Buffer.from(body).toString('utf8').trim();
  if (text.length === 0) {
    throw new UserEventParseError('empty body');
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (err) {
    throw new UserEventParseError(err instanceof Error ? err.message : 'invalid JSON');
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new UserEventParseError('body is not a JSON object');
  }
  const obj = parsed as Record<string, unknown>;
  // The event kind rides the payload's `type` (the public user.* convention); a
  // body with no `type` defaults to `user.message` (the only chat-only kind).
  const kind = typeof obj.type === 'string' && obj.type.length > 0 ? obj.type : 'user.message';
  return { kind, payload: obj };
}

/**
 * One parsed NDJSON replay line — its stable `id` (the dedup key + cursor) plus the
 * whole object, so the loop can both dedup by id and reconstruct the transcript
 * {@link Event} it carries for the local-log feed.
 */
interface ReplayLine {
  id: string;
  obj: Record<string, unknown>;
}

/** Parse one NDJSON replay line, returning its id + object, or `undefined` to skip. */
function parseReplayLine(line: string): ReplayLine | undefined {
  const trimmed = line.trim();
  if (trimmed.length === 0) {
    return undefined;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return undefined;
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return undefined;
  }
  const obj = parsed as Record<string, unknown>;
  const id = obj.id;
  if (typeof id !== 'string' || id.length === 0) {
    return undefined;
  }
  return { id, obj };
}

/**
 * Reconstruct the transcript {@link Event} a replay line carries, for the local-log
 * feed ({@link TranscriptSink}).
 *
 * The replay body is the registry's `protoToHttpEvent` JSON: `{ id, type,
 * produced_at, produced_by, subpath?, seq, content?, ... }`. This lifts those fields
 * back into the `Event` shape `@orca/transcript-store-types` defines (the inverse of the
 * registry's serialization), preserving the WHOLE line as the payload so a reader of
 * the local log sees the same event body the registry persisted. The store re-stamps
 * its own `seq` on ingest, so the line's `seq` (a string) is not load-bearing here.
 * Defensive coercion: any absent/typed-wrong field falls back to a sane default so a
 * partially-shaped line never throws out of the replay apply (which is contractually
 * skip-on-malformed).
 */
function replayLineToEvent(workspaceId: string, sessionId: string, line: ReplayLine): Event {
  const obj = line.obj;
  const kind = typeof obj.type === 'string' ? obj.type : '';
  const subpath = typeof obj.subpath === 'string' ? obj.subpath : '';
  const producedBy = typeof obj.produced_by === 'string' ? obj.produced_by : 'harness';
  const producedAt =
    typeof obj.produced_at === 'string' ? obj.produced_at : new Date().toISOString();
  return {
    id: line.id,
    workspaceId,
    sessionId,
    subpath,
    seq: 0,
    producedAt,
    producedBy,
    kind,
    payload: Buffer.from(JSON.stringify(obj), 'utf8'),
    idempotencyKey: '',
  };
}

/** Split an NDJSON body into its lines (dropping a trailing blank). */
function splitNdjson(text: string): string[] {
  if (text.length === 0) {
    return [];
  }
  return text.split('\n').filter((l) => l.trim().length > 0);
}

/**
 * A bounded, FIFO-evicting set of stable event ids — the replay dedup window.
 *
 * Keeps at most `capacity` ids; adding past the cap evicts the OLDEST-inserted id
 * (insertion-order FIFO). That is the right eviction for replay dedup: the registry
 * serves strictly `after={cursor}`, so the only re-pushes a reconnect can deliver
 * are overlaps near the RECENT tail — never old history — so the most-recent window
 * catches every duplicate the registry can re-push while bounding memory. The cap is
 * chosen (see {@link DEFAULT_DEDUP_WINDOW}) comfortably above the registry's
 * per-recovery replay bound so a single full replay never evicts an id it is still
 * applying.
 *
 * A JS `Set` already iterates in insertion order, so a single `Set` is both the
 * O(1) membership index AND the FIFO order: eviction drops the first key the set
 * yields. {@link has} / {@link add} match the `Set` surface the loop used before, so
 * this is a drop-in for the previously-unbounded `Set<string>`.
 */
export class BoundedIdSet {
  private readonly ids = new Set<string>();
  private readonly capacity: number;

  /**
   * @param capacity Max ids retained; a non-positive value falls back to
   *   {@link DEFAULT_DEDUP_WINDOW} (the set is never unbounded).
   */
  constructor(capacity: number = DEFAULT_DEDUP_WINDOW) {
    this.capacity = capacity > 0 ? capacity : DEFAULT_DEDUP_WINDOW;
  }

  /** Whether `id` is in the current window. */
  has(id: string): boolean {
    return this.ids.has(id);
  }

  /**
   * Record `id` as applied. A no-op when already present (no churn / no re-order). A
   * fresh id past the capacity evicts the oldest-inserted id first, keeping the set
   * at its ceiling.
   */
  add(id: string): void {
    if (this.ids.has(id)) {
      return;
    }
    this.ids.add(id);
    if (this.ids.size > this.capacity) {
      // Evict the oldest-inserted id: the first key the insertion-ordered Set yields.
      const oldest = this.ids.values().next().value;
      if (oldest !== undefined) {
        this.ids.delete(oldest);
      }
    }
  }

  /** The number of ids currently retained (for tests / observability). */
  get size(): number {
    return this.ids.size;
  }
}
