// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The runner-side thread orchestration — the {@link CoordinatorHarness}.
//
// This is the whole of Anthropic's thread-model multiagent on the runner side: ONE
// session, MULTIPLE threads. A coordinator agent (its snapshot carried a `multiagent`
// roster) delegates — via the `agent_toolset` delegate-to-agent tool the provider
// exposes — to roster agents, each of which runs in its OWN session thread: a
// context-isolated event stream + its own history. SHARED across all threads: the
// SAME per-session sandbox / filesystem / vault credentials (the coordinator's).
// ISOLATED per subagent: its own model/system/tools/MCP/skills (its own snapshot) +
// its own thread.
//
// The design is a decorator: a `CoordinatorHarness` WRAPS the coordinator's own base
// {@link AgentHarness} (built by the ordinary provider registry from the coordinator
// snapshot, so single-agent providers are entirely unchanged) and layers the thread
// orchestration on top:
//
//   - it injects a `delegate` seam onto the base harness's start input. The
//     coordinator's provider feature-detects that seam and exposes the delegation
//     tool to the model; when the model calls it, the provider invokes `delegate`.
//   - on a `delegate({ agentName, prompt })` call it: resolves the roster agent;
//     enforces the <=25 concurrent-thread cap + (structurally) one-level delegation;
//     mints the child thread id; emits `session.thread_created` +
//     `agent.thread_message_received` + `session.thread_status_running` on the PRIMARY
//     thread; BUILDS a fresh subagent harness from the roster agent's snapshot SHARING
//     the coordinator's sandbox + confirm-tool gate; runs its delegated turn, RE-EMITTING
//     the subagent's own events under its `subagents/<id>` subpath; collects the
//     subagent's final text; emits `session.thread_status_idle` +
//     `agent.thread_message_sent`; frees the thread slot; and RESOLVES with the result,
//     which the provider returns to the model as the delegation tool result.
//   - it presents ONE merged `events()` stream (the base harness's events + every
//     subagent thread's events) so the runner's existing turn loop pumps the whole
//     multiagent choreography up the tunnel with no loop change: lifecycle + message
//     events (primary subpath) drive the registry's `session_threads` read model, and
//     each subagent's turn events (child subpath) land on that child's thread stream.
//
// Everything is IN-PROCESS: threads are internal concurrency in the ONE runner that
// owns the ONE session (claim-distribution is untouched — no child sessions, no extra
// runners). Node's single event loop serializes the merged event queue, so the base
// pump and the subagent pumps never race on two emissions.

import type {
  AgentEvent,
  AgentHarness,
  DelegateRequest,
  DelegateResult,
  SessionStartInput,
  TerminationReason,
  ToolConfirmer,
  UserEvent,
} from '../agent-harness.js';
import { terminalError } from '../agent-harness.js';
import type { SandboxHandle } from '../../sandbox/seam.js';
import type { MultiagentRosterMember, MultiagentSnapshot, RunnerSnapshot } from '../../snapshot.js';
import {
  subagentThreadSubpath,
  threadCreatedEvent,
  threadMessageReceivedEvent,
  threadMessageSentEvent,
  threadStatusIdleEvent,
  threadStatusRunningEvent,
  threadStatusTerminatedEvent,
} from './thread-events.js';

/**
 * Anthropic's published cap on CONCURRENT session threads. The coordinator refuses a
 * delegation that would push the in-flight thread count past this — a fail-fast the
 * provider surfaces to the model as a tool error, so a runaway fan-out is bounded
 * rather than exhausting the runner.
 */
export const MAX_CONCURRENT_THREADS = 25;

/** The default idle stop reason recorded when a subagent turn completes normally. */
const DEFAULT_STOP_REASON = 'end_turn';

/** The kind that marks a turn complete (mirrors the loop / provider constant). */
const TURN_COMPLETED_EVENT_KIND = 'agent.turn_completed';
/** The agent-message kind whose text the coordinator collects as the delegation result. */
const AGENT_MESSAGE_EVENT_KIND = 'agent.message';
/** The kind that carries a turn-level FAILURE onto the wire (mirrors the loop constant). */
const AGENT_ERROR_EVENT_KIND = 'agent.error';

/** A structured-logger seam (a subset of the runner's logger). All optional. */
export interface CoordinatorLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/**
 * The context passed to {@link CoordinatorHarnessOptions.buildSubagent} for one
 * delegation — everything the factory needs to construct a subagent harness that runs
 * the delegated turn inside the coordinator's session thread.
 */
export interface SubagentBuildContext {
  /** The child thread's id (`sth_…`) the subagent runs in. */
  sessionThreadId: string;
  /** The roster agent's own resolved snapshot (its model/system/tools/MCP/egress). */
  snapshot: RunnerSnapshot;
  /**
   * The SHARED per-session sandbox handle — the SAME one the coordinator holds, so a
   * subagent's file/exec tools land in the coordinator's filesystem (shared FS across
   * threads). `undefined` only when the coordinator itself has none.
   */
  sandbox?: SandboxHandle;
  /**
   * The confirm-tool gate the coordinator was started with, to thread onto the
   * subagent's start input so its gated tool call parks on the SAME uniform transcript
   * approval. `undefined` when the coordinator runs without a gate.
   */
  confirmTool?: ToolConfirmer;
}

/**
 * The result of {@link SubagentHarnessFactory}: a constructed (NOT yet started)
 * subagent harness plus the {@link SessionStartInput} the coordinator will `start` it
 * with. The factory produces BOTH so the start input is projected from the roster
 * agent's OWN snapshot (its model/system/tools/MCP) by the SAME projection the loop
 * uses for a single-agent harness — the coordinator only guarantees the input carries
 * NO `delegate` seam (one-level delegation) before starting it.
 */
export interface SubagentBuild {
  /** The constructed subagent harness (the coordinator starts + drives it). */
  harness: AgentHarness;
  /**
   * The start input to start the subagent with — projected from the roster agent's own
   * snapshot, carrying the shared sandbox + confirm-tool gate. The coordinator strips
   * any `delegate` off it before starting (one-level delegation), so a factory need not.
   */
  startInput: SessionStartInput;
}

/**
 * Builds (but does NOT start) a subagent {@link AgentHarness} for one delegation, plus
 * the start input to run it with. Typically dispatches the roster agent's snapshot
 * through the SAME provider registry the coordinator was built from and projects its
 * start input the same way the loop does — sharing the coordinator's sandbox via `ctx`.
 */
export type SubagentHarnessFactory = (
  member: MultiagentRosterMember,
  ctx: SubagentBuildContext,
) => SubagentBuild;

/** Options for {@link CoordinatorHarness}. */
export interface CoordinatorHarnessOptions {
  /**
   * The coordinator's OWN harness (built by the provider registry from the coordinator
   * snapshot, with the multiagent block stripped). The coordinator harness wraps it:
   * it drives the base per turn and merges its events into the one presented stream.
   */
  base: AgentHarness;
  /** The parsed coordinator roster (the agents the base may delegate to). */
  multiagent: MultiagentSnapshot;
  /** Owning workspace (tenant scope). */
  workspaceId: string;
  /** The Orca session id (all threads run in this ONE session). */
  sessionId: string;
  /** Builds a subagent harness from a roster member's snapshot for one delegation. */
  buildSubagent: SubagentHarnessFactory;
  /**
   * The SHARED per-session sandbox handle (the coordinator's), passed to every
   * subagent so all threads share ONE filesystem. `undefined` when the coordinator
   * has none (chat-only).
   */
  sandbox?: SandboxHandle;
  /**
   * The confirm-tool gate the coordinator was started with, threaded onto each
   * subagent's start input so a subagent's gated tool call parks on the SAME uniform
   * transcript approval. `undefined` when the coordinator runs without a gate.
   */
  confirmTool?: ToolConfirmer;
  /**
   * Mints a child thread id (`sth_…`). Injectable so tests predict ids; defaults to a
   * random `sth_<uuid>`.
   */
  mintThreadId?: () => string;
  /** Optional structured logger. */
  logger?: CoordinatorLogger;
}

/**
 * The coordinator harness — wraps a base harness and orchestrates subagent threads.
 * Implements {@link AgentHarness} so the runner's loop drives it exactly like any
 * single-agent harness; the multiagent behavior is entirely internal.
 */
export class CoordinatorHarness implements AgentHarness {
  private readonly base: AgentHarness;
  private readonly multiagent: MultiagentSnapshot;
  private readonly workspaceId: string;
  private readonly sessionId: string;
  private readonly buildSubagent: SubagentHarnessFactory;
  private readonly sandbox: SandboxHandle | undefined;
  private readonly confirmTool: ToolConfirmer | undefined;
  private readonly mintThreadId: () => string;
  private readonly logger: CoordinatorLogger | undefined;
  /** Roster agents indexed by name for O(1) delegation resolution. */
  private readonly rosterByName: Map<string, MultiagentRosterMember>;
  /** The active subagent threads (the concurrency-cap ledger + interrupt/stop targets). */
  private readonly threads = new SubagentThreadRegistry();

  /** The merged output stream (base events + all subagent thread events). */
  private readonly out = new MergedEventStream();
  private started = false;
  private stopped = false;

  constructor(opts: CoordinatorHarnessOptions) {
    this.base = opts.base;
    this.multiagent = opts.multiagent;
    this.workspaceId = opts.workspaceId;
    this.sessionId = opts.sessionId;
    this.buildSubagent = opts.buildSubagent;
    this.sandbox = opts.sandbox;
    this.confirmTool = opts.confirmTool;
    this.mintThreadId = opts.mintThreadId ?? defaultThreadId;
    this.logger = opts.logger;
    this.rosterByName = new Map(this.multiagent.agents.map((m) => [m.agentName, m]));
  }

  /**
   * Start the coordinator: wire the `delegate` seam onto the base harness's start
   * input (so its provider exposes the delegation tool) and start the base. The base's
   * events are NOT background-pumped — they are drained INLINE per turn (see
   * {@link submit}) so `submit` honors the harness contract (it resolves only once the
   * turn's events are all on the presented stream), which the runner loop's turn
   * boundary relies on.
   */
  async start(input: SessionStartInput): Promise<void> {
    // Inject the delegation seam. A copy so the caller's start input is not mutated;
    // spread first so a caller that already set `delegate` (it never does in
    // production) is overridden by the coordinator's own delegation handler.
    const baseInput: SessionStartInput = {
      ...input,
      delegate: (request) => this.delegate(request),
    };
    await this.base.start(baseInput);
    this.started = true;
    this.stopped = false;
  }

  /**
   * Drive one user turn on the base harness, draining its events INLINE onto the merged
   * stream until the turn completes.
   *
   * Delegations happen INSIDE the base's turn (its model calls the delegation tool →
   * `delegate` → a subagent turn, whose events this coordinator emits onto the merged
   * stream inline as it runs). So a coordinator turn's events on the merged stream are
   * an interleaving of the base's own events (primary subpath) and the delegated
   * subagents' events (child subpath). Draining the base inline (rather than on a
   * background pump) is what makes THIS `submit` resolve only after every one of those
   * events is on the presented stream — the load-bearing contract the runner loop uses
   * to end the tunneled turn.
   *
   * REJECTS when the base harness's own `submit` rejected, like every other
   * {@link AgentHarness}: the runner loop contains that rejection, names it on the wire and
   * ends the turn with its marker. A base fault that only ended the base's EVENT STREAM
   * cannot surface that way (the merged stream this decorator presents is still open, so the
   * loop sees no stream end), so `drainTurn` puts it on the merged stream as a terminal
   * `agent.error` instead. Either way the turn never ends with a bare marker.
   */
  async submit(event: UserEvent): Promise<void> {
    if (!this.started || this.stopped) {
      return;
    }
    await this.drainTurn(this.base, event, undefined, undefined);
  }

  /**
   * Interrupt the in-flight turn WITHOUT tearing the harness down — forwards to the
   * base AND every live subagent thread (a delegated turn blocked on its model unwinds
   * too), so a coordinator turn parked on a delegation releases. The harness stays
   * alive; the next turn reuses it.
   */
  interrupt(): void {
    this.base.interrupt();
    this.threads.interruptAll();
  }

  /**
   * Stop the coordinator: stop the base + every live subagent thread, then end the
   * merged event stream. Idempotent.
   */
  async stop(reason: TerminationReason): Promise<void> {
    if (this.stopped) {
      return;
    }
    this.stopped = true;
    // Stop every live subagent first so their in-flight delegated turns unwind + their
    // threads free. A thread that was STILL LIVE (a delegation cut off by this teardown,
    // rather than run to completion — the completed path already emitted its idle) gets
    // a `session.thread_status_terminated` on the PRIMARY stream so the read model
    // reflects the forced termination. Emitted BEFORE the merged stream closes below.
    const terminated = await this.threads.stopAll(reason);
    for (const sessionThreadId of terminated) {
      this.out.emit(threadStatusTerminatedEvent({ sessionThreadId }));
    }
    try {
      await this.base.stop(reason);
    } catch (err) {
      this.logger?.error?.({ err, sessionId: this.sessionId }, 'coordinator base stop failed');
    }
    this.out.end();
  }

  /** The single merged stream of all thread events (base + subagents). */
  events(): AsyncIterable<AgentEvent> {
    return this.out.iterable();
  }

  /**
   * The delegation handler the base provider's delegation tool invokes. Spawns the
   * target roster agent's subagent thread, runs the delegated turn, and resolves with
   * its result. Rejects fail-fast on an unknown roster agent or the concurrency cap
   * (the provider surfaces those as a tool error to the model).
   */
  private async delegate(request: DelegateRequest): Promise<DelegateResult> {
    const member = this.rosterByName.get(request.agentName);
    if (member === undefined) {
      throw new DelegationError(
        `unknown roster agent '${request.agentName}' (not in the coordinator's roster)`,
      );
    }
    // Enforce the concurrent-thread cap BEFORE minting an id / emitting events, so a
    // refused delegation leaves no half-created thread.
    if (this.threads.count() >= MAX_CONCURRENT_THREADS) {
      throw new DelegationError(
        `delegation refused: the concurrency limit of ${MAX_CONCURRENT_THREADS} session threads is reached`,
      );
    }

    const sessionThreadId = this.mintThreadId();
    const subpath = subagentThreadSubpath(sessionThreadId);
    const parentThreadId =
      this.multiagent.primaryThreadId.length > 0 ? this.multiagent.primaryThreadId : null;

    // Announce the thread on the PRIMARY stream: created → the delegation prompt it
    // received → running. The registry projects `thread_created` into a new
    // `session_threads` row; the message/running events surface the choreography.
    this.out.emit(
      threadCreatedEvent({ sessionThreadId, agentName: member.agentName, parentThreadId }),
    );
    this.out.emit(
      threadMessageReceivedEvent({
        sessionThreadId,
        fromSessionThreadId: this.primaryThreadIdForMessages(),
        content: request.prompt,
      }),
    );
    this.out.emit(threadStatusRunningEvent({ sessionThreadId }));

    // Build the subagent harness + its start input from the roster snapshot (its own
    // model/system/tools/MCP), SHARING the coordinator's sandbox + confirm-tool gate.
    // A BUILD failure (e.g. the roster agent's provider is unregistered on this runner)
    // must not leave the just-announced thread stuck in `running`: terminate it on the
    // PRIMARY stream and surface a clean delegation error to the model.
    const ctx: SubagentBuildContext = {
      sessionThreadId,
      snapshot: member.snapshot,
      ...(this.sandbox !== undefined ? { sandbox: this.sandbox } : {}),
      ...(this.confirmTool !== undefined ? { confirmTool: this.confirmTool } : {}),
    };
    let subagent: AgentHarness;
    let startInput: SessionStartInput;
    try {
      const build = this.buildSubagent(member, ctx);
      subagent = build.harness;
      // Strip any `delegate` off the subagent's start input: one-level delegation — a
      // subagent provider must not wire a delegation tool, so it never sees the seam.
      startInput = withoutDelegate(build.startInput);
    } catch (err) {
      // The thread was announced (created/running) but could not be built: terminate it
      // so the read model does not strand a `running` row, then fail the delegation.
      this.out.emit(threadStatusTerminatedEvent({ sessionThreadId }));
      throw new DelegationError(
        `failed to build subagent '${request.agentName}': ${(err as Error).message}`,
      );
    }
    // Register the thread as the concurrency-cap ledger entry + an interrupt/stop target.
    const entry = this.threads.add(sessionThreadId, subagent);

    let result = '';
    let stopReason = DEFAULT_STOP_REASON;
    // The fault that ended this delegation, if any: a `start` that threw, or a turn whose
    // `submit` REJECTED (recorded on `entry` by `drainTurn`, which contains the rejection so
    // the drain can still finish). Either shape means the subagent produced NO answer.
    let failure: Error | undefined;
    try {
      await subagent.start(startInput);
      // Run the delegated turn AND drain its events INLINE so the result is fully
      // accumulated by the time this returns: each event is re-emitted onto the merged
      // stream under the child subpath and observed (via `entry`) for the final text +
      // stop reason. The drain ends at the subagent's turn-completed marker (the
      // contract: `submit` resolves once the turn's events are all emitted).
      await this.drainTurn(
        subagent,
        { kind: 'user.message', payload: userMessagePayload(request.prompt) },
        subpath,
        entry,
      );
      result = entry.collectedText();
      stopReason = entry.stopReason() ?? DEFAULT_STOP_REASON;
      failure = entry.failure();
    } catch (err) {
      // `start` threw (the subagent could not boot): same outcome as a crashed turn — no
      // answer — so it takes the same faulted path below rather than escaping raw.
      failure = err instanceof Error ? err : new Error(String(err));
    } finally {
      // Free the thread slot + stop the subagent (ends its event stream) regardless of
      // outcome. A re-delivered snapshot / coordinator stop then never leaks a subagent.
      this.threads.remove(sessionThreadId);
      try {
        await subagent.stop('client.archived');
      } catch (err) {
        this.logger?.error?.(
          { err, sessionId: this.sessionId, sessionThreadId },
          'coordinator subagent stop failed',
        );
      }
    }

    // A FAULTED delegation is NOT an empty success. Falling through here would emit
    // `thread_status_idle` + a `thread_message_sent` carrying `''`, and hand the model
    // `{ text: '', isError: false }` — which reads as "the specialist had nothing useful to
    // say", so it reasons on from a fabricated empty answer instead of learning the turn
    // crashed. So: TERMINATED (the turn never finished — never `idle`), and raise, so the
    // delegation tool answers with `isError: true` carrying the reason. The terminated
    // signal is skipped when the coordinator is already stopping — `stop` emits one for
    // every still-live thread, and a second would be a contradictory double-signal.
    if (failure !== undefined) {
      if (!this.stopped) {
        this.out.emit(threadStatusTerminatedEvent({ sessionThreadId }));
      }
      throw new DelegationError(`subagent '${request.agentName}' turn failed: ${failure.message}`);
    }

    // Finalize the thread on the PRIMARY stream: idle (with the stop reason) → the
    // result it sent back to the coordinator. SKIPPED when the coordinator was stopped
    // mid-delegation: `stop` already emitted a `session.thread_status_terminated` for
    // this still-live thread (and is closing the merged stream), so emitting an idle
    // now would be a contradictory double-signal on a torn-down thread. This makes the
    // normal path (idle + sent) and the teardown path (terminated) mutually exclusive
    // and deterministic — no race on the merged stream's close.
    if (this.stopped) {
      return { sessionThreadId, result };
    }
    this.out.emit(threadStatusIdleEvent({ sessionThreadId, stopReason }));
    this.out.emit(
      threadMessageSentEvent({
        sessionThreadId,
        toSessionThreadId: this.primaryThreadIdForMessages(),
        content: result,
      }),
    );
    return { sessionThreadId, result };
  }

  /**
   * Drive one turn on a harness and drain its events INLINE onto the merged stream
   * until the turn completes. Shared by the base (`subpath = undefined`, no `entry`)
   * and each subagent (`subpath = subagents/<id>`, an `entry` collecting the result).
   *
   * Drives the harness's `submit` and consumes its `events()` in lockstep: each event
   * is re-emitted onto the merged stream (stamped with the child `subpath` for a
   * subagent; passed through untouched for the base, whose events are already primary)
   * and observed by the `entry` (a subagent) so the delegated turn's final text is
   * collected. The drain ends when the harness emits `agent.turn_completed`, its stream
   * ends, or `submit` resolves with no further buffered events — racing each event pull
   * against `submit` exactly like the runner loop's own turn boundary. Because a
   * well-behaved harness emits its turn's events before `submit` resolves, all of them
   * are drained here, so THIS method resolving means the turn is fully on the merged
   * stream (the contract the coordinator's own `submit`/`delegate` rely on).
   *
   * For the BASE turn, delegations fire inside `submit`; each runs a NESTED `drainTurn`
   * that emits the subagent's events onto the SAME merged stream — so a coordinator
   * turn's merged events are the base's + the delegated subagents', correctly ordered.
   *
   * A turn that ended WITHOUT a completed marker is a fault, and this method returns `void`
   * either way — so both shapes are reported before it returns rather than inferred from the
   * return: a `submit` rejection, and the event stream ending (or a pull faulting) mid-turn.
   * A subagent's fault lands on its thread `entry` (so `delegate` fails the delegation); the
   * base's lands on the merged stream as a terminal `agent.error`, and a base `submit`
   * rejection is rethrown so this coordinator's `submit` rejects too.
   */
  private async drainTurn(
    harness: AgentHarness,
    event: UserEvent,
    subpath: string | undefined,
    entry: SubagentThread | undefined,
  ): Promise<void> {
    const iterator = harness.events()[Symbol.asyncIterator]();
    let submitDone = false;
    // A BASE turn's `submit` rejection, held for the rethrow at the end of this method.
    // Boxed so a harness that rejects with `undefined` is still distinguishable from one
    // that did not reject at all.
    let baseFailure: { err: unknown } | undefined;
    const submitSettled = harness
      .submit(event)
      .catch((err: unknown) => {
        // The rejection is CONTAINED here — an uncaught one would break the drain mid-flush,
        // and it would leave `submitSettled` REJECTED, which the pull races below observe
        // with no rejection handler (an unhandled rejection kills the process). It is not
        // SWALLOWED. A SUBAGENT turn records it on the thread entry: `delegate` reads it and
        // fails the delegation, so a crashed subagent turn never reaches the coordinator
        // model as an empty success. A BASE turn has no entry, so it is held and RETHROWN
        // once the drain has finished — making this coordinator's `submit` reject exactly as
        // the wrapped harness's did, like every other `AgentHarness`. Without that rethrow
        // the runner loop never learns the turn faulted and ends it with a BARE marker: a
        // crashed turn recorded as a successful, empty answer.
        if (entry !== undefined) {
          entry.fail(err);
        } else {
          baseFailure = { err };
        }
        this.logger?.error?.({ err, sessionId: this.sessionId }, 'coordinator turn submit failed');
      })
      .finally(() => {
        submitDone = true;
      });

    // Whether the LAST event this drain put on the merged stream was the wrapped harness's
    // OWN TERMINAL `agent.error` — the same question the runner loop asks before adding its
    // generic terminal error, asked here because the coordinator gets to it FIRST. Without
    // it a base that reported "claude-code CLI failed to launch: spawn claude ENOENT" had
    // "coordinator base turn failed: harness event stream ended before the turn completed"
    // appended after it, and being last that generic line is the one an operator reads.
    let lastEmittedWasTerminalError = false;
    const emit = (value: AgentEvent): void => {
      entry?.observe(value);
      lastEmittedWasTerminalError =
        value.kind === AGENT_ERROR_EVENT_KIND && value.terminal === true;
      // A subagent event rides its child subpath; a base event passes through with the
      // subpath it already carries (a coordinator's own events are primary — none).
      this.out.emit(subpath !== undefined ? { ...value, subpath } : value);
    };

    // The fault that ended this drain WITHOUT a turn-completed marker, if any: the wrapped
    // harness's event stream simply ENDED mid-turn (how every harness's read loop unwinds a
    // fault), or a pull itself FAULTED. Both are invisible from this method's return value —
    // it returns `void` either way — which is why they were read as clean turns. Reported at
    // the end of the drain (`??=`: the FIRST, most specific fault wins).
    let drainFault: Error | undefined;
    const noteStreamEnded = (): void => {
      drainFault ??= new Error('harness event stream ended before the turn completed');
    };
    const notePullFault = (err: unknown): void => {
      drainFault ??= err instanceof Error ? err : new Error(String(err));
      this.logger?.error?.(
        { err, sessionId: this.sessionId, subpath },
        'coordinator turn event stream pull failed',
      );
    };

    // A parked pull carried across the phase switch (a pull that won a submit-race is
    // still in flight; its eventual event must not be dropped).
    let parked: Promise<IteratorResult<AgentEvent>> | undefined;
    // Whether the drain saw the turn's completed marker — the clean end of both phases.
    // A FLAG rather than an early `return`: this method's tail still owes the caller two
    // things after the marker (settling `submit`, and RETHROWING a base `submit` rejection
    // so this coordinator's own `submit` rejects as the wrapped harness's did). Returning
    // from inside phase 1 skipped both, so a base that emitted its marker and THEN rejected
    // was reported to the runner loop as a clean turn — the exact "reported before it
    // returns" promise this method's contract makes.
    let sawMarker = false;

    // Phase 1: stream events as they arrive, racing each pull against submit. Ends on
    // the completed marker, the stream ending, or submit resolving (→ phase 2 flush).
    for (;;) {
      const pull = parked ?? iterator.next();
      parked = undefined;
      const outcome = await raceSubagentPull(pull, submitSettled);
      if (outcome === 'submit-done') {
        parked = pull; // park the in-flight pull for the flush phase.
        break;
      }
      const res = await resolveSubagentPull(pull, notePullFault);
      if (res.done) {
        // The stream ended and no marker was emitted — the turn produced no answer.
        noteStreamEnded();
        break;
      }
      emit(res.value);
      if (res.value.kind === TURN_COMPLETED_EVENT_KIND) {
        sawMarker = true;
        break;
      }
    }

    // Phase 2: submit resolved — flush exactly the events it already buffered (a pull
    // that does not settle within a macrotask means the buffer is drained), stopping at
    // the completed marker or the stream end.
    while (parked !== undefined) {
      const pull = parked;
      parked = undefined;
      const ready = await raceBufferedSubagentPull(pull);
      if (ready === 'empty') {
        break;
      }
      const res = await resolveSubagentPull(pull, notePullFault);
      if (res.done) {
        // Same shape as phase 1: the stream ended before the turn completed.
        noteStreamEnded();
        break;
      }
      emit(res.value);
      if (res.value.kind === TURN_COMPLETED_EVENT_KIND) {
        sawMarker = true;
        break;
      }
      parked = iterator.next();
    }

    // Settle submit (its error was already contained) before returning.
    if (!submitDone) {
      await submitSettled;
    }

    // Report a turn that ended without a marker. A `submit` rejection is the ROOT cause and
    // WINS (the stream ending is how that fault manifests), so this only speaks when nothing
    // more specific was recorded. A SUBAGENT records it on its thread entry, so `delegate`
    // fails the delegation rather than handing the coordinator model an empty success. A BASE
    // turn has no entry and the merged stream is still open — the runner loop therefore CANNOT
    // see that the base's own stream ended — so the reason goes on the merged stream as a
    // terminal `agent.error`, immediately before the marker the loop synthesizes. Without it
    // the consumer got a BARE marker: a crashed coordinator turn recorded as a successful,
    // empty answer.
    //
    // The merged-stream emission is SKIPPED when the base already made the turn's last word
    // its own TERMINAL error — this generic line would then be the SECOND and, being last,
    // the one an operator reads, burying "spawn claude ENOENT" under "harness event stream
    // ended before the turn completed". The SUBAGENT branch is never skipped: `entry.fail`
    // is not a wire event, it is what stops `delegate` handing the coordinator model an
    // empty success, and the base's error is on the merged stream either way.
    if (
      !sawMarker &&
      drainFault !== undefined &&
      baseFailure === undefined &&
      entry?.failure() === undefined
    ) {
      if (entry !== undefined) {
        entry.fail(drainFault);
      } else if (!this.stopped && !lastEmittedWasTerminalError) {
        this.out.emit(terminalError(`coordinator base turn failed: ${drainFault.message}`));
      }
    }

    // A BASE `submit` that REJECTED rejects this one too, now that the drain has finished —
    // see the `catch` above.
    if (baseFailure !== undefined) {
      throw baseFailure.err;
    }
  }

  /** The primary thread id used in cross-thread message events (empty → falls back to the id). */
  private primaryThreadIdForMessages(): string {
    return this.multiagent.primaryThreadId.length > 0 ? this.multiagent.primaryThreadId : 'primary';
  }
}

/** A subagent pull's race outcome: a settled pull (an event/end), or submit resolving first. */
type SubagentPullOutcome = 'event' | 'submit-done';

/**
 * Race a subagent event pull against its `submit` resolving. Resolves `'event'` when
 * the pull settles first (resolved OR rejected — the caller maps a rejection to a
 * clean end), `'submit-done'` when submit finishes first (the pull stays in flight and
 * the caller parks it, so its eventual event is never dropped).
 */
function raceSubagentPull(
  pull: Promise<IteratorResult<AgentEvent>>,
  submitSettled: Promise<void>,
): Promise<SubagentPullOutcome> {
  return new Promise<SubagentPullOutcome>((resolve) => {
    let settled = false;
    const done = (outcome: SubagentPullOutcome): void => {
      if (!settled) {
        settled = true;
        resolve(outcome);
      }
    };
    void pull.then(
      () => done('event'),
      () => done('event'),
    );
    void submitSettled.then(() => done('submit-done'));
  });
}

/**
 * Race a buffered pull against a macrotask: `'ready'` when the event is already
 * buffered (settles within the macrotask), `'empty'` when it is not (the buffer is
 * drained). Used after `submit` resolves to flush exactly the events the turn already
 * emitted without blocking on absent further events.
 */
function raceBufferedSubagentPull(
  pull: Promise<IteratorResult<AgentEvent>>,
): Promise<'ready' | 'empty'> {
  return new Promise<'ready' | 'empty'>((resolve) => {
    let settled = false;
    const settle = (v: 'ready' | 'empty'): void => {
      if (!settled) {
        settled = true;
        resolve(v);
      }
    };
    void pull.then(
      () => settle('ready'),
      () => settle('ready'),
    );
    setImmediate(() => settle('empty'));
  });
}

/**
 * Await a pull, mapping a rejected pull (an event-stream fault) to a clean stream-end so the
 * drain finishes instead of throwing mid-flush — and REPORTING it through `onFault` on the
 * way past. The mapping is what makes the drain robust; it must not also make the fault
 * disappear. An empty catch here turned a broken event stream into a turn that ended with a
 * bare completed marker, which the transcript records as a successful, empty answer.
 */
async function resolveSubagentPull(
  pull: Promise<IteratorResult<AgentEvent>>,
  onFault: (err: unknown) => void,
): Promise<IteratorResult<AgentEvent>> {
  try {
    return await pull;
  } catch (err) {
    onFault(err);
    return { value: undefined, done: true };
  }
}

/** Payload for a `user.message` turn body handed to a subagent. */
function userMessagePayload(prompt: string): Record<string, unknown> {
  return { type: 'user.message', content: [{ type: 'text', text: prompt }] };
}

/**
 * A start input with any `delegate` seam removed — the one-level delegation guard. A
 * subagent must never be started with a delegation seam (that would let its provider
 * wire a delegation tool and spawn its own subagents). Returns the input unchanged when
 * it carries no `delegate` (the common case), else a shallow copy without it.
 */
function withoutDelegate(input: SessionStartInput): SessionStartInput {
  if (input.delegate === undefined) {
    return input;
  }
  const { delegate: _dropped, ...rest } = input;
  return rest;
}

/** Thrown by a delegation refusal (unknown roster agent / concurrency cap). */
export class DelegationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DelegationError';
  }
}

/**
 * One live subagent thread — the harness + the accumulator that collects its turn's
 * final text (the delegation result) and last stop reason from its event stream.
 */
class SubagentThread {
  private text = '';
  private lastStopReason: string | undefined;
  private turnFailure: Error | undefined;

  constructor(
    readonly sessionThreadId: string,
    readonly harness: AgentHarness,
  ) {}

  /** Observe one of the subagent's events, accumulating its result text + stop reason. */
  observe(event: AgentEvent): void {
    if (event.kind === AGENT_MESSAGE_EVENT_KIND) {
      const text = agentMessageText(event.payload);
      if (text.length > 0) {
        this.text = this.text.length > 0 ? `${this.text}\n${text}` : text;
      }
    } else if (event.kind === TURN_COMPLETED_EVENT_KIND) {
      const reason = (event.payload as { stop_reason?: unknown } | null)?.stop_reason;
      if (typeof reason === 'string') {
        this.lastStopReason = reason;
      }
    }
  }

  /** The subagent's collected final text (the delegation result). */
  collectedText(): string {
    return this.text;
  }

  /** The subagent's last `agent.turn_completed` stop reason, if any. */
  stopReason(): string | undefined {
    return this.lastStopReason;
  }

  /**
   * Record the fault that ended this thread's delegated turn — its `submit` REJECTED.
   * `drainTurn` contains that rejection (so the drain still finishes and the merged stream
   * is not left mid-flush), which would otherwise erase it entirely: the accumulator would
   * report a clean empty result and the coordinator model would be told the specialist
   * simply had nothing to say.
   */
  fail(err: unknown): void {
    this.turnFailure = err instanceof Error ? err : new Error(String(err));
  }

  /** The fault that ended the delegated turn, or `undefined` when it ran to completion. */
  failure(): Error | undefined {
    return this.turnFailure;
  }
}

/**
 * The active-subagent-thread ledger. Tracks live threads for the concurrency cap and
 * as interrupt/stop targets. A runner serves one session on one event loop, so this is
 * a plain map (no locking); `count()` is the live thread count the cap reads.
 */
class SubagentThreadRegistry {
  private readonly live = new Map<string, SubagentThread>();

  /** Register a new live thread and return its accumulator entry. */
  add(sessionThreadId: string, harness: AgentHarness): SubagentThread {
    const entry = new SubagentThread(sessionThreadId, harness);
    this.live.set(sessionThreadId, entry);
    return entry;
  }

  /** Remove a thread (its delegated turn finished + it was stopped). */
  remove(sessionThreadId: string): void {
    this.live.delete(sessionThreadId);
  }

  /** The number of CURRENTLY live (in-flight) threads — the concurrency-cap reading. */
  count(): number {
    return this.live.size;
  }

  /** Interrupt every live thread's in-flight turn (the coordinator was interrupted). */
  interruptAll(): void {
    for (const entry of this.live.values()) {
      entry.harness.interrupt();
    }
  }

  /**
   * Stop every live thread (best-effort) — used on coordinator teardown. Returns the
   * ids of the threads that were STILL LIVE (a delegation cut off mid-flight), so the
   * coordinator can emit a `session.thread_status_terminated` for each before the
   * merged stream closes.
   */
  async stopAll(reason: TerminationReason): Promise<string[]> {
    const entries = [...this.live.values()];
    this.live.clear();
    await Promise.all(
      entries.map(async (entry) => {
        try {
          await entry.harness.stop(reason);
        } catch {
          // Best-effort teardown: a subagent that already ended is fine to ignore.
        }
      }),
    );
    return entries.map((e) => e.sessionThreadId);
  }
}

/** Extract the concatenated text from an `agent.message` payload's content array. */
function agentMessageText(payload: unknown): string {
  if (payload === null || typeof payload !== 'object') {
    return '';
  }
  const content = (payload as { content?: unknown }).content;
  if (!Array.isArray(content)) {
    return '';
  }
  const parts: string[] = [];
  for (const part of content) {
    if (
      part !== null &&
      typeof part === 'object' &&
      (part as { type?: unknown }).type === 'text' &&
      typeof (part as { text?: unknown }).text === 'string'
    ) {
      parts.push((part as { text: string }).text);
    }
  }
  return parts.join('');
}

/**
 * A single-consumer merged async event stream. Every thread's pump `emit`s into ONE
 * queue; the runner's turn loop reads it via {@link iterable}. Node's single event
 * loop serializes emissions, so concurrent subagent pumps never interleave a partial
 * event. `end()` resolves any waiting consumer as done.
 */
class MergedEventStream {
  private readonly queue: AgentEvent[] = [];
  private resolvers: Array<(r: IteratorResult<AgentEvent>) => void> = [];
  private ended = false;

  /** Push one event onto the stream (or hand it directly to a waiting consumer). */
  emit(event: AgentEvent): void {
    if (this.ended) {
      return;
    }
    const resolver = this.resolvers.shift();
    if (resolver !== undefined) {
      resolver({ value: event, done: false });
      return;
    }
    this.queue.push(event);
  }

  /** End the stream — resolves every waiting consumer as done. Idempotent. */
  end(): void {
    if (this.ended) {
      return;
    }
    this.ended = true;
    while (this.resolvers.length > 0) {
      this.resolvers.shift()?.({ value: undefined, done: true });
    }
  }

  /** The single async iterable the runner's loop consumes. */
  async *iterable(): AsyncIterable<AgentEvent> {
    for (;;) {
      const head = this.queue.shift();
      if (head !== undefined) {
        yield head;
        continue;
      }
      if (this.ended) {
        return;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        this.resolvers.push(resolve);
      });
      if (next.done) {
        return;
      }
      yield next.value;
    }
  }
}

/** The default child thread id minter — a random `sth_<uuid>`. */
function defaultThreadId(): string {
  return `sth_${cryptoRandomId()}`;
}

/** A compact random id (crypto UUID with dashes stripped). */
function cryptoRandomId(): string {
  return globalThis.crypto.randomUUID().replace(/-/g, '');
}
