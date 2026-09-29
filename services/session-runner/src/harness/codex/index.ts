// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `codex` native-CLI harness — boots `codex app-server` and drives it over its
// JSON-RPC-over-stdio protocol, as an Orca {@link AgentHarness}.
//
// This is the real harness the runner constructs from a `provider: "codex"` snapshot and
// drives per turn. It boots the `codex` BINARY as a long-lived child INSIDE the per-session
// {@link SandboxHandle} — through the 4a native-CLI launcher ({@link launchNativeCli}) — as
// `codex app-server`, and speaks its JSON-RPC protocol (the harness is the CLIENT):
//
//   start(input)     → resolve the sandbox + wire the native-CLI tool-bridge as codex's
//                      `orca` MCP server (so the model's orca built-ins resolve inside the
//                      sandbox), build the `app-server` launch argv (see `launch-args.ts`),
//                      boot codex, start the background reader that normalizes its JSON-RPC
//                      stdout into Orca-native events, then run the `initialize` handshake.
//   submit(user.msg) → send `thread/start` (the FIRST turn only, to open the thread) then
//                      `turn/start` with the user text; await this turn's `turn/completed` /
//                      `turn/failed` (the turn boundary), and emit a terminal
//                      `agent.turn_completed`. `submit` RESOLVES only once the turn's events
//                      are all emitted — honoring the runner's turn contract.
//   interrupt()      → send `turn/interrupt` + abort the in-flight turn WITHOUT ending the
//                      harness (the next `submit` drives a fresh turn).
//   stop(reason)     → kill the codex child and end `events()`.
//   events()         → the single stream of emitted agent events.
//
// Approval routing: codex is launched with `approval_policy=on-request`, so it raises a
// server→client approval REQUEST for each risky built-in action (shell exec / apply-patch).
// The reader routes each to the uniform transcript approval — a tool whose policy is
// `always_ask` (or fail-closed with no policy) emits an Anthropic-native
// `agent.requires_action` signal then PARKS the verdict; a tool whose policy is
// `always_allow` auto-approves — and writes the decision back as a JSON-RPC RESULT on
// codex's stdin (the codex-native `{decision}` shape). With NO gate the policy is `never`
// (the OS sandbox + composed policies are the guardrail).
//
// Emits Orca-native Anthropic-shaped {@link AgentEvent}s ONLY (the SAME shapes the in-process
// providers + the claude-code native-CLI provider emit) — the normalization lives in
// {@link CodexProtocol}.

import type {
  AgentEvent,
  AgentHarness,
  SessionStartInput,
  TerminationReason,
  ToolConfirmer,
  ToolPermissionDecision,
  ToolPermissionResolver,
  UserEvent,
} from '../agent-harness.js';
import { diagnosticError, terminalError } from '../agent-harness.js';
import type { SandboxHandle } from '../../sandbox/seam.js';
import {
  awaitTurnWithinDeadline,
  cliExitFaultMessage,
  DEFAULT_TURN_DEADLINE_MS,
  launchNativeCli,
  turnDeadlineFaultMessage,
  type NativeCliLogger,
  type NativeCliProcess,
} from '../../sandbox/native-cli-launcher.js';
import { buildCodexLaunchConfig, type CodexBridgeCommand } from './launch-args.js';
import {
  CodexProtocol,
  type CodexApprovalRequest,
  type CodexApprovalDecision,
} from './protocol.js';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import {
  BRIDGE_ROOT_FLAG,
  BRIDGE_TMUX_SOCKET_FLAG,
  pushAllowedToolsFlag,
} from '../claude-code/bridge-runtime.js';

/**
 * The Anthropic-native `agent.requires_action` signal payload emitted when an approval parks
 * on the human gate: the action kind, a per-approval `tool_use_id` (the verdict's routing
 * key), and the tool name. Mirrors the in-process + claude-code providers' signal exactly.
 */
export interface RequiresActionSignal {
  action: 'tool_confirmation';
  tool_use_id: string;
  tool_name: string;
}

/** Construction options for {@link CodexCliHarness}. */
export interface CodexHarnessOptions {
  /** Default model id when the snapshot did not pin one. */
  modelDefault: string;
  /** Owning workspace (tenant scope). */
  workspaceId: string;
  /** Orca session id this harness serves. */
  sessionId: string;
  /**
   * The per-session {@link SandboxHandle} the provider captured from the session context,
   * used as a FALLBACK when a `start` input omits its own `sandbox`. A native-CLI harness
   * REQUIRES a sandbox (it launches a real child + wires the tool-bridge to the session
   * tree), so when neither carries one, `start` fails fast rather than run unsandboxed.
   */
  sandbox?: SandboxHandle;
  /** The scoped session JWT forwarded to codex as `OPENAI_API_KEY` (credential-free). */
  apiKey?: string;
  /** The gateway LLM-proxy base URL forwarded to codex as `OPENAI_BASE_URL`. */
  baseURL?: string;
  /**
   * The CLI binary path/name. Defaults to `codex` on the sandbox PATH; a test injects a fake
   * CLI here (e.g. `node`) with {@link cliPrefixArgs} carrying the script path.
   */
  cliCommand?: string;
  /** Prefix args before the generated flags (e.g. a fake CLI script path under `node`). */
  cliPrefixArgs?: string[];
  /** Extra suffix args after the generated flags (e.g. a test probe flag). */
  cliExtraArgs?: string[];
  /**
   * The bridge entrypoint executable codex spawns as its `orca` MCP server. Defaults to the
   * current Node executable; a test injects `tsx` here (with {@link bridgePrefixArgs}
   * carrying the TS entry path) so the CLI child speaks MCP to the TS source.
   */
  bridgeCommand?: string;
  /** Prefix args before the bridge entry's own flags (the bridge entry module path). */
  bridgePrefixArgs?: string[];
  /** The native-CLI launcher (injected for tests). Defaults to the real {@link launchNativeCli}. */
  launch?: typeof launchNativeCli;
  /**
   * How long a JSON-RPC request waits for its response before it is settled with a timeout
   * error. Defaults to {@link DEFAULT_REQUEST_TIMEOUT_MS}. Bounded so a codex that accepts a
   * frame and never answers cannot park `start()` / `submit` forever; a test shortens it.
   */
  requestTimeoutMs?: number;
  /**
   * How long a turn waits for codex's terminal `turn/completed` / `turn/failed` notification
   * before it is ended with a timeout error. Defaults to {@link DEFAULT_TURN_DEADLINE_MS}.
   * This is a DIFFERENT bound from {@link requestTimeoutMs}: that one bounds the JSON-RPC
   * REQUEST leg (an app-server that never answers `turn/start`), this one bounds the TURN
   * (an app-server that answers `turn/start` and then never notifies completion). A test
   * shortens it.
   */
  turnTimeoutMs?: number;
  /**
   * Optional structured logger. codex's approval result is a bare decision string with no
   * message field, so a faulted approval GATE has nowhere else to report: this is the only
   * channel that reason reaches.
   */
  logger?: NativeCliLogger;
}

/** Default bound on a JSON-RPC response wait (see {@link CodexHarnessOptions.requestTimeoutMs}). */
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

/** The in-flight turn's state: a resolver fired when the turn's terminal notification lands. */
interface TurnState {
  resolve: () => void;
  /**
   * The turn's abort signal — fired by {@link CodexCliHarness.interrupt} /
   * {@link CodexCliHarness.stop}. An approval parked on the human gate races this so an
   * interrupted / stopped turn releases the parked gate as a DENY (writing the deny decision
   * back to codex) rather than stalling the reader.
   */
  abort: AbortController;
}

/**
 * Native-CLI harness backed by `codex app-server`. One per session; the runner `start`s it
 * from the snapshot then `submit`s each user turn against the SAME live app-server.
 */
export class CodexCliHarness implements AgentHarness {
  private readonly launch: typeof launchNativeCli;
  private input: SessionStartInput | undefined;
  private sandbox: SandboxHandle | undefined;
  /** The JSON-RPC protocol normalizer for this session (framing + event mapping). */
  private readonly protocol = new CodexProtocol();
  /** The booted codex child, or `undefined` before `start` / after `stop`. */
  private cli: NativeCliProcess | undefined;
  /** The background stdout reader (resolves when the app-server's stdout ends). */
  private readerDone: Promise<void> | undefined;
  private outQueue: AgentEvent[] = [];
  private outResolvers: Array<(e: IteratorResult<AgentEvent>) => void> = [];
  private terminated = false;
  /** The in-flight turn, or `undefined` between turns. */
  private turn: TurnState | undefined;
  /** The codex thread id, minted by the first `thread/start`; reused for later turns. */
  private threadId: string | undefined;
  /**
   * Pending JSON-RPC responses the harness is awaiting, keyed by request id — resolved by the
   * reader when a matching `response` line lands. This is how a `thread/start` / `turn/start`
   * request gets its result (the reader owns the stdout stream, so requests can't read it).
   */
  private readonly pendingResponses = new Map<number, (r: JsonRpcResponse) => void>();
  /**
   * In-flight approval parks, keyed by their codex request id. Teardown aborts every entry so
   * a fire-and-forget approval awaiting the human gate releases as a deny.
   */
  private readonly pendingApprovals = new Map<number | string, AbortController>();
  /** Per-session monotonically increasing counter for JSON-RPC request ids + signal ids. */
  private seq = 0;

  /** Bound on how long a JSON-RPC request waits for its response (see {@link request}). */
  private readonly requestTimeoutMs: number;
  /** Bound on how long one turn may park on codex's terminal notification (see {@link submit}). */
  private readonly turnTimeoutMs: number;

  constructor(private readonly opts: CodexHarnessOptions) {
    this.launch = opts.launch ?? launchNativeCli;
    this.requestTimeoutMs = opts.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    this.turnTimeoutMs = opts.turnTimeoutMs ?? DEFAULT_TURN_DEADLINE_MS;
  }

  async start(input: SessionStartInput): Promise<void> {
    this.input = input;
    this.terminated = false;
    // A native-CLI harness must have a sandbox: it launches a real child and binds the
    // tool-bridge to the session tree. Fail fast rather than run unsandboxed.
    const sandbox = input.sandbox ?? this.opts.sandbox;
    if (sandbox === undefined) {
      throw new Error('codex provider requires a per-session sandbox but none was wired');
    }
    this.sandbox = sandbox;

    const config = buildCodexLaunchConfig(this.buildLaunchInput(sandbox, input));
    // Boot codex inside the sandbox and start reading its JSON-RPC stdout.
    this.cli = this.launch(sandbox, config);
    this.readerDone = this.readLoop(this.cli);
    // Run the initialize handshake (codex requires it before any thread/turn request).
    const initialized = await this.request('initialize', {
      clientInfo: { name: 'orca-session-runner', version: '0.1' },
      capabilities: { experimentalApi: true },
    });
    if (initialized.error !== undefined) {
      // The handshake never completed: codex failed to launch, exited, or did not answer
      // within the request deadline. Every later `thread/start` would fail the same way, so
      // fail `start` LOUDLY here rather than run a session whose turns all silently no-op.
      // The reader has already put the underlying fault on the wire as an `agent.error`.
      throw new Error(`codex initialize failed: ${jsonRpcErrorMessage(initialized.error)}`);
    }
    this.notify('initialized');
  }

  async submit(event: UserEvent): Promise<void> {
    if (this.terminated) {
      return;
    }
    if (event.kind === 'user.interrupt') {
      this.interrupt();
      return;
    }
    if (event.kind !== 'user.message') {
      return;
    }
    const text = textOf(event.payload);
    if (text === null || this.cli === undefined) {
      return;
    }

    // Open the thread on the first turn; reuse it thereafter.
    let threadStartError: string | undefined;
    if (this.threadId === undefined) {
      const started = await this.request('thread/start', this.threadStartParams(text));
      this.threadId = threadIdOf(started);
      if (started.error !== undefined) {
        threadStartError = jsonRpcErrorMessage(started.error);
      }
    }
    if (this.threadId === undefined) {
      // No thread id: codex answered with an error (or timed out — `request` settles a
      // deadline the same way), or violated the protocol. Say WHY on the wire before the
      // completed marker — a bare marker with no events reads as a successful empty answer.
      this.emit(
        terminalError(
          `codex thread/start failed: ${threadStartError ?? 'no thread id in the response'}`,
        ),
      );
      // Settle the turn so `submit` does not hang.
      this.emit({ kind: 'agent.turn_completed', payload: {} });
      return;
    }

    // Drive the turn: send `turn/start`, then await this turn's terminal notification.
    let turn!: TurnState;
    const turnDone = new Promise<void>((resolve) => {
      turn = { resolve, abort: new AbortController() };
      this.turn = turn;
    });
    // `turn/start` returns the turn id in its response; the turn's events stream as
    // notifications the reader normalizes. We do not need to await the response before the
    // terminal notification — the reader settles the turn on `turn/completed` / `turn/failed`.
    // But we DO have to read the response when it FAILS, because for a codex that took the
    // frame and went mute nothing else ever will: `request` settles its bounded deadline by
    // RESOLVING with an `error` response (never rejecting — a `void`ed request must not become
    // an unhandled rejection), so discarding it parks `turnDone` forever, pins `activeTurns`,
    // and leaves the registry believing the turn is still pending. So an error response ends
    // the turn the same way `thread/start` does: say WHY on the wire, then release it — the
    // rejection handler stays for the other shape, a `request` that cannot even be sent
    // because the child is already gone.
    void this.request('turn/start', {
      threadId: this.threadId,
      input: [{ type: 'text', text }],
    }).then(
      (started) => {
        // `this.turn !== turn` means this turn already ended by another path (the reader's
        // terminal notification, an interrupt, teardown) and the late response is moot —
        // emitting then would bleed an error into the NEXT turn.
        if (started.error === undefined || this.turn !== turn) {
          return;
        }
        this.emit(terminalError(`codex turn/start failed: ${jsonRpcErrorMessage(started.error)}`));
        turn.resolve();
      },
      () => {
        // A rejected turn/start (the app-server is gone) settles the turn so `submit` unwinds.
        turn.resolve();
      },
    );
    // BOUNDED (see DEFAULT_TURN_DEADLINE_MS). `requestTimeoutMs` above bounds only the
    // `turn/start` REQUEST; an app-server that ANSWERS it and then never notifies
    // `turn/completed` / `turn/failed` — a wedged child, a model call that never returns —
    // parks this await forever, and nothing on the stream reports that state. On expiry,
    // name the timeout on the wire FIRST (a turn released with nothing said reaches the
    // client as a bare `agent.turn_completed`), then TEAR THE CHILD DOWN — a deadline that
    // ends the turn and leaves the app-server alive desynchronizes the two for good — and
    // only then release the park.
    await awaitTurnWithinDeadline(turnDone, this.turnTimeoutMs, () => {
      // `this.turn !== turn` means the turn already ended by another path (the reader's
      // terminal notification, an interrupt, teardown) and this deadline lost the race.
      if (this.turn !== turn) {
        return;
      }
      this.emit(terminalError(turnDeadlineFaultMessage('codex', this.turnTimeoutMs)));
      // The deadline ends the SESSION, not just the turn: kill the child and drop it, so the
      // read loop's `finally` runs, `endEventStream()` fires, and every LATER turn ends
      // loudly on the loop's own "harness event stream ended before the turn completed".
      // Ending only the turn left the CLI ALIVE with this turn's frame still in flight, and
      // a reader credits a terminal frame to whatever turn is CURRENT — so the next turn was
      // answered by THIS one's late answer, at HTTP 200, to a question the transcript had
      // already recorded as failed. Nothing can resynchronize that: none of these protocols
      // correlates a terminal frame back to the frame that asked for it, so once a turn is
      // abandoned while the child lives the turn accounting is permanently off by one. A
      // dead session that SAYS it is dead is the only honest outcome, and at a 30-minute
      // bound the child being killed is a wedged one, not a slow one.
      this.cli?.kill();
      this.cli = undefined;
      turn.resolve();
    });
    this.turn = undefined;

    // The turn's events are all emitted; close it with a terminal completed marker so the
    // runner's turn boundary fires (mirrors the in-process + claude-code providers).
    this.emit({ kind: 'agent.turn_completed', payload: {} });
  }

  interrupt(): void {
    if (this.terminated) {
      return;
    }
    const turn = this.turn;
    if (turn === undefined) {
      return; // no turn in flight — idempotent no-op.
    }
    // Fire the turn's abort so an approval parked on the human gate releases as a DENY.
    turn.abort.abort();
    // Best-effort: ask codex to interrupt the in-flight turn over the control channel.
    if (this.threadId !== undefined) {
      this.notify('turn/interrupt', { threadId: this.threadId });
    }
    // Also settle the turn locally in case codex does not emit a fresh terminal notification.
    turn.resolve();
  }

  async stop(reason: TerminationReason): Promise<void> {
    void reason; // Informational; the runner logs it.
    this.terminated = true;
    this.turn?.abort.abort();
    this.turn?.resolve();
    this.turn = undefined;
    // Abort every in-flight approval park so a fire-and-forget approval releases as a deny.
    for (const cancel of [...this.pendingApprovals.values()]) {
      cancel.abort();
    }
    // Settle any pending JSON-RPC waiter so an in-flight request does not hang past teardown.
    this.settlePendingResponses('harness stopped');
    // Kill the codex child (ends its stdout → the reader loop completes) and end the stream.
    this.cli?.kill();
    this.cli = undefined;
    this.endEventStream();
    await this.readerDone?.catch(() => {});
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.terminated || this.outQueue.length > 0) {
      if (this.outQueue.length > 0) {
        const head = this.outQueue.shift();
        if (head !== undefined) {
          yield head;
        }
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        this.outResolvers.push(resolve);
      });
      if (next.done) {
        return;
      }
      yield next.value;
    }
  }

  /**
   * The background reader: consume codex's JSON-RPC stdout LINES through the protocol
   * normalizer, dispatching each:
   *   - `response` → resolve the matching pending JSON-RPC waiter;
   *   - `events` → emit them;
   *   - `turn_end` → emit its usage/error events then settle the in-flight turn;
   *   - `approval_request` → route to the gate (NON-blocking), writing the JSON-RPC decision
   *     RESULT on stdin when it resolves;
   *   - `ignore` → drop.
   * A non-JSON line is skipped. The loop completes when codex's stdout ends (exit / kill),
   * at which point it ends the event stream so a direct consumer is not stranded.
   * On the way out it reports an ABNORMAL exit as a terminal `agent.error` (a non-zero code,
   * or a signal this harness did not send) — stdout ending looks the same for a clean finish
   * and for a crash, so without that a dead CLI ends the turn as an empty success.
   */
  private async readLoop(cli: NativeCliProcess): Promise<void> {
    try {
      const failure = await cli.failure;
      if (failure !== undefined) {
        // codex never started — a missing / misnamed `codex` on the sandbox PATH is the
        // likeliest production misconfiguration. Its stdout is empty, so falling through
        // would end the turn as a clean, silent no-op. Put the launch failure on the wire as
        // a terminal `agent.error` (the established way a fault reaches the transcript) and
        // drop into the `finally`, which settles the parked turn AND every pending JSON-RPC
        // waiter — the `initialize` handshake `start()` is blocked on above all.
        this.emit(launchFailureEvent(failure));
        return;
      }
      for await (const raw of cli.lines()) {
        if (this.terminated) {
          break;
        }
        const trimmed = raw.trim();
        if (trimmed.length === 0) {
          continue;
        }
        let parsed: unknown;
        try {
          parsed = JSON.parse(trimmed);
        } catch {
          continue; // a non-JSON diagnostic line — skip.
        }
        const result = this.protocol.map(parsed);
        if (result.kind === 'response') {
          const resolve = this.pendingResponses.get(result.id as number);
          if (resolve !== undefined) {
            this.pendingResponses.delete(result.id as number);
            const response: JsonRpcResponse = { id: result.id };
            if (result.result !== undefined) {
              response.result = result.result;
            }
            if (result.error !== undefined) {
              response.error = result.error;
            }
            resolve(response);
          }
        } else if (result.kind === 'events') {
          for (const ev of result.events) {
            this.emit(ev);
          }
        } else if (result.kind === 'turn_end') {
          for (const ev of result.events) {
            this.emit(ev);
          }
          this.turn?.resolve();
        } else if (result.kind === 'approval_request') {
          // Dispatch WITHOUT blocking the reader (a later interrupt / stop must stay
          // reachable). `handleApproval` NEVER rejects — it catches a gate fault as a deny and
          // always answers codex — so this fire-and-forget raises no unhandled rejection.
          void this.handleApproval(cli, result.request);
        }
        // `ignore` → drop.
      }
    } finally {
      // codex's stdout ended — which looks IDENTICAL for a clean finish, a crash, and the kill
      // `stop()` issues. Ask the exit status which it was, and when it was a CRASH put that on
      // the wire FIRST: after `endEventStream()` below the stream is closed and an emitted
      // event would never be read, and a turn released with nothing said reaches the client as
      // a bare `agent.turn_completed` — a dead app-server recorded as a successful, empty
      // answer. It is also emitted BEFORE `settlePendingResponses`, so the crash — not the
      // "stdout ended before responding" that settling the in-flight `turn/start` reports — is
      // the turn's terminal explanation. Skipped once terminated: teardown killed the child,
      // so the exit is one we asked for (and the turn was already released by `stop`).
      //
      // `parked` decides the WORDING and the `terminal` flag — never WHETHER the fault is
      // reported. A child that dies while IDLE (an OOM between turns) is a real fault with no
      // turn to attribute it to: `emit` hands it to the pull the loop parks BETWEEN turns, so
      // the NEXT turn's first pull yields it, and flagging it terminal there would make the
      // PREVIOUS death that turn's stated cause and suppress its real one. So the flag — and
      // only the flag — is withheld. DROPPING the fault, as this guard used to, destroyed the
      // only record that the child had died at all; an unflagged `agent.error` disarms nothing
      // (the loop's suppression reads `AgentEvent.terminal`, not the kind), so both reach the
      // wire in the order they happened. Sampled BEFORE the awaited grace, so a `stop()` that
      // lands inside it cannot change the answer.
      const parked = this.turn !== undefined;
      // `catch`: a rejecting `exit` must not abort this `finally` before the turn is
      // released, the pending waiters settled and the stream ended — that would reintroduce
      // the very hang the exit status exists to prevent (and would strand `start()`'s
      // `initialize` await). Nothing to report is the same answer as no fault.
      const exitFault = await cliExitFaultMessage(cli, 'codex', parked ? 'mid-turn' : 'idle').catch(
        () => undefined,
      );
      // `terminated` is re-read AFTER the grace: `stop()` can land inside that await, and it
      // drains every event resolver — an error emitted past it is queued behind a returned
      // generator and never read.
      if (exitFault !== undefined && !this.terminated) {
        this.emit(parked ? terminalError(exitFault) : diagnosticError(exitFault));
        if (!parked) {
          // The wire is not a reliable channel for an IDLE death: `emit` hands it to the pull
          // the loop parks BETWEEN turns, so it is delivered only if another turn is ever
          // driven. A session that dies here and is never prompted again would record nothing
          // at all, so the three harnesses that HAVE a logger also log it.
          this.opts.logger?.error?.(
            { sessionId: this.opts.sessionId, message: exitFault },
            'codex app-server died while the session was idle',
          );
        }
      }
      this.turn?.resolve();
      // codex's stdout has ended, so no response will EVER arrive for a request still in
      // flight. Settle them all: a `request()` waiter left pending here hangs its caller
      // forever — `start()`'s `initialize` handshake most damagingly, which would keep
      // `activeTurns` pinned and the idle watchdog from ever firing.
      this.settlePendingResponses('codex app-server stdout ended before responding');
      if (!this.terminated) {
        this.terminated = true;
        this.endEventStream();
      }
    }
  }

  /**
   * Settle every in-flight JSON-RPC waiter with an error response and clear the ledger.
   * Called on teardown and when the reader ends — the two moments after which a pending
   * waiter can never be answered. Resolving (rather than rejecting) keeps the
   * `Promise<JsonRpcResponse>` contract, so a `void`-ed `request` cannot become an
   * unhandled rejection; callers that must fail loudly inspect `response.error`.
   */
  private settlePendingResponses(message: string): void {
    for (const [id, resolve] of [...this.pendingResponses.entries()]) {
      this.pendingResponses.delete(id);
      resolve({ id, error: { message } });
    }
  }

  /**
   * Route a codex `on-request` approval to the uniform transcript approval and write the
   * decision back as a JSON-RPC RESULT on codex's stdin (the codex-native `{decision}` shape).
   *
   * With no confirmation gate wired the launch used `approval_policy=never` and codex never
   * raises this; if it somehow does, the harness auto-allows. With a gate: the per-tool
   * policy decides — `always_allow` auto-approves with no signal; `always_ask` (or fail-
   * closed with no resolver) emits the Anthropic-native `agent.requires_action` signal then
   * PARKS the verdict through the gate.
   */
  private async handleApproval(
    cli: NativeCliProcess,
    request: CodexApprovalRequest,
  ): Promise<void> {
    const cancel = new AbortController();
    this.pendingApprovals.set(request.requestId, cancel);
    // A gate fault must NOT escape: this runs fire-and-forget, so a throw would become an
    // unhandled rejection (fatal to this process) AND skip the JSON-RPC result below, leaving
    // codex blocked on this approval for the rest of the session. So the decision is seeded
    // fail-closed and the result frame is written from the `finally` — every path answers once.
    let decision: CodexApprovalDecision = 'deny';
    try {
      decision = await this.resolveApproval(request, cancel.signal);
    } catch (err) {
      // The gate itself faulted: keep the fail-closed `deny` seeded above — a tool must never
      // run on an unresolved verdict — and LOG the reason. A bare `catch` did not even BIND
      // the error, so the cause was unrecoverable: a permission-store outage denied every
      // tool call in the session with nothing anywhere saying why. codex's approval result
      // is a bare decision string with no message field, so the log is the ONLY channel
      // this reason can reach.
      this.opts.logger?.error?.(
        { err, sessionId: this.opts.sessionId, tool: request.toolName },
        'codex approval gate failed; denying the tool call',
      );
    } finally {
      this.pendingApprovals.delete(request.requestId);
      try {
        const result = this.protocol.approvalResult(request.method, decision);
        cli.write(`${JSON.stringify({ id: request.requestId, result })}\n`);
      } catch {
        // codex's stdin is gone (it exited / was killed): there is nobody left to answer.
      }
    }
  }

  /**
   * Resolve an approval request to an allow/deny decision (the gate + policy logic). The
   * parked verdict is raced against the turn's abort (interrupt / stop) so it releases as a
   * deny rather than stalling. The gate's {@link ToolPermissionDecision} maps to the codex
   * allow/deny: `allow` → allow, `deny` → deny.
   */
  private async resolveApproval(
    request: CodexApprovalRequest,
    cancelSignal: AbortSignal,
  ): Promise<CodexApprovalDecision> {
    const toolPermissions: ToolPermissionResolver | undefined = this.input?.toolPermissions;
    const policy = toolPermissions ? toolPermissions.policyFor(request.toolName) : 'always_ask';
    if (policy === 'always_deny') return 'deny';
    const confirmTool: ToolConfirmer | undefined = this.input?.confirmTool;
    if (confirmTool === undefined) {
      // No gate → launched with approval_policy=never; auto-allow defensively.
      return 'allow';
    }
    if (policy === 'always_allow') {
      return 'allow';
    }
    const abortSignal = this.turn?.abort.signal;
    if (abortSignal?.aborted === true || cancelSignal.aborted) {
      return 'deny';
    }
    // Park on the human gate: emit the requires_action signal keyed by a per-approval id,
    // then await the verdict delivered through the gate. The signal id is derived from the
    // codex call id (stable) or a fresh synthesized id, so a client can correlate it.
    const toolUseId = request.callId ?? `codex_appr_${++this.seq}`;
    const signal: RequiresActionSignal = {
      action: 'tool_confirmation',
      tool_use_id: toolUseId,
      tool_name: request.toolName,
    };
    this.emit({ kind: 'agent.requires_action', id: toolUseId, payload: signal });
    const verdict = confirmTool(request.toolName, request.input, { toolUseId });
    const decisionFrom = (d: ToolPermissionDecision): CodexApprovalDecision =>
      d.behavior === 'allow' ? 'allow' : 'deny';
    // Race the parked verdict against the turn's abort AND this request's cancel so an
    // interrupt / stop releases the gate as a deny.
    const races: Array<Promise<CodexApprovalDecision>> = [verdict.then(decisionFrom)];
    const cleanups: Array<() => void> = [];
    for (const sig of [abortSignal, cancelSignal]) {
      if (sig === undefined) {
        continue;
      }
      let onAbort: (() => void) | undefined;
      races.push(
        new Promise<CodexApprovalDecision>((resolve) => {
          onAbort = (): void => resolve('deny');
          sig.addEventListener('abort', onAbort, { once: true });
        }),
      );
      cleanups.push(() => {
        if (onAbort !== undefined) {
          sig.removeEventListener('abort', onAbort);
        }
      });
    }
    try {
      return await Promise.race(races);
    } finally {
      for (const cleanup of cleanups) {
        cleanup();
      }
    }
  }

  /**
   * Send a JSON-RPC request and await its response (routed back by the reader), under a
   * BOUNDED deadline.
   *
   * The waiter is settled by whichever comes first: the reader routing a matching response,
   * the reader ending (see {@link settlePendingResponses}), teardown, or this deadline. The
   * deadline is the backstop for a codex that accepted the frame and simply never answers
   * (a protocol mismatch, a wedged child) — a case no stream event reports. Without it the
   * promise parks forever, and with it `start()`'s `initialize` await, `activeTurns` never
   * drops and the idle watchdog never fires. The timer is unref'd (it must not hold the
   * process open) and cleared the moment the response lands.
   */
  private request(method: string, params: Record<string, unknown>): Promise<JsonRpcResponse> {
    const id = ++this.seq;
    if (this.cli === undefined) {
      return Promise.reject(new Error('codex app-server is not running'));
    }
    this.protocol.expectResponse(id);
    let timer: NodeJS.Timeout | undefined;
    const done = new Promise<JsonRpcResponse>((resolve) => {
      this.pendingResponses.set(id, resolve);
      timer = setTimeout(() => {
        // `delete` returns false when the reader already settled this waiter — then the
        // timer lost the race and there is nothing to do.
        if (this.pendingResponses.delete(id)) {
          const message = `codex request '${method}' timed out after ${this.requestTimeoutMs}ms`;
          resolve({ id, error: { message } });
        }
      }, this.requestTimeoutMs);
      timer.unref?.();
    });
    this.cli.write(`${JSON.stringify({ id, method, params })}\n`);
    return done.finally(() => clearTimeout(timer));
  }

  /** Send a JSON-RPC notification (no id, no response). */
  private notify(method: string, params?: Record<string, unknown>): void {
    const frame: Record<string, unknown> = { method };
    if (params !== undefined) {
      frame['params'] = params;
    }
    this.cli?.write(`${JSON.stringify(frame)}\n`);
  }

  /** The `thread/start` params: the approval policy is a launch-arg, so this stays minimal. */
  private threadStartParams(_firstText: string): Record<string, unknown> {
    void _firstText;
    const params: Record<string, unknown> = {};
    const system = this.input?.agentSnapshot.system;
    if (system !== undefined && system.length > 0) {
      // Codex accepts session developer instructions on thread/start.
      params['developerInstructions'] = system;
    }
    return params;
  }

  /** Build the launch input for {@link buildCodexLaunchConfig} from the boot context. */
  private buildLaunchInput(
    sandbox: SandboxHandle,
    input: SessionStartInput,
  ): Parameters<typeof buildCodexLaunchConfig>[0] {
    const model = input.agentSnapshot.model_id;
    const out: Parameters<typeof buildCodexLaunchConfig>[0] = {
      bridge: this.buildBridgeCommand(sandbox, input),
      // A gate is wired iff the runner supplied `confirmTool`. Gate → on-request approvals;
      // no gate → never (the OS sandbox + policies are the guardrail).
      gateEnabled: input.confirmTool !== undefined,
    };
    if (this.opts.cliCommand !== undefined) {
      out.cliPath = this.opts.cliCommand;
    }
    if (this.opts.cliPrefixArgs !== undefined) {
      out.prefixArgs = this.opts.cliPrefixArgs;
    }
    if (this.opts.cliExtraArgs !== undefined) {
      out.suffixArgs = this.opts.cliExtraArgs;
    }
    if (model !== undefined && model.length > 0) {
      out.model = model;
    } else if (this.opts.modelDefault.length > 0) {
      out.model = this.opts.modelDefault;
    }
    if (this.opts.baseURL !== undefined) {
      out.baseURL = this.opts.baseURL;
    }
    if (this.opts.apiKey !== undefined) {
      out.apiKey = this.opts.apiKey;
    }
    // A private CODEX_HOME under the sandbox root keeps the app-server off the operator's
    // ~/.codex. The root is a host path the bridge (and codex, running in the same tree)
    // can reach; a `.codex-home` subdir sits inside the session work-dir.
    const root = sandboxRootDir(sandbox);
    if (root !== undefined) {
      out.codexHome = `${root}/.codex-home`;
    }
    return out;
  }

  /**
   * Build the `orca` MCP server command codex spawns: the bridge entrypoint pointed at the
   * session sandbox root (+ its tmux socket on the local-attach transport, + the skill-
   * allowlist), so the bridge REATTACHES to the same session tree.
   *
   * @throws Error when the sandbox handle exposes no `rootDir()` (a cloud-only handle needs a
   *   different bridge transport — out of scope for the root-dir-backed reusable runtimes).
   */
  private buildBridgeCommand(sandbox: SandboxHandle, input: SessionStartInput): CodexBridgeCommand {
    const args = [...(this.opts.bridgePrefixArgs ?? [resolveBridgeEntryPath()])];
    const root = sandboxRootDir(sandbox);
    if (root === undefined) {
      throw new Error(
        `codex provider requires a root-dir-backed sandbox (the native-CLI tool-bridge ` +
          `reattaches to an absolute host work-dir), but the session sandbox '${sandbox.id}' ` +
          `exposes no rootDir(); a cloud-only handle needs a different bridge transport.`,
      );
    }
    args.push(BRIDGE_ROOT_FLAG, root);
    const socket = tmuxSocketOf(sandbox);
    if (socket !== undefined) {
      args.push(BRIDGE_TMUX_SOCKET_FLAG, socket);
    }
    // `[]` is DENY-ALL, not "no restriction" — see `claude-code/bridge-runtime.ts`.
    pushAllowedToolsFlag(args, input.agentSnapshot.allowed_tool_names);
    return { command: this.opts.bridgeCommand ?? process.execPath, args };
  }

  /** Resolve every waiting `events()` consumer as done — ends the stream. */
  private endEventStream(): void {
    while (this.outResolvers.length > 0) {
      const resolver = this.outResolvers.shift();
      if (resolver) {
        resolver({ value: undefined, done: true });
      }
    }
  }

  private emit(e: AgentEvent): void {
    const resolver = this.outResolvers.shift();
    if (resolver) {
      resolver({ value: e, done: false });
      return;
    }
    this.outQueue.push(e);
  }
}

// ── module-private helpers ──────────────────────────────────────────────────────

/**
 * The terminal `agent.error` a FAILED CLI LAUNCH is reported as.
 *
 * {@link launchNativeCli} settles its `failure` promise when the sandbox could not start
 * the binary at all — a missing or misnamed CLI on the sandbox PATH being the common case.
 * Its stdout is then empty, which is byte-for-byte what a CLI that started and exited
 * immediately looks like, so the read loop maps it to this event: the fault reaches the
 * wire and the durable transcript instead of vanishing into a clean, empty turn.
 */
function launchFailureEvent(err: Error): AgentEvent {
  return terminalError(`codex CLI failed to launch: ${err.message}`);
}

/** A minimal JSON-RPC response envelope the reader hands back to a pending request. */
interface JsonRpcResponse {
  id: number | string;
  result?: unknown;
  error?: unknown;
}

/**
 * A human-readable message out of a JSON-RPC `error` member. codex's own errors are
 * `{ code, message }`; the harness's synthesized ones (teardown, reader end, request
 * deadline) carry just `{ message }`. Anything else is stringified so the reason still
 * reaches the caller rather than collapsing to `[object Object]`.
 */
function jsonRpcErrorMessage(error: unknown): string {
  if (typeof error === 'string') {
    return error;
  }
  if (error !== null && typeof error === 'object') {
    const message = (error as { message?: unknown }).message;
    if (typeof message === 'string' && message.length > 0) {
      return message;
    }
  }
  return JSON.stringify(error) ?? 'unknown error';
}

/**
 * Resolve the runnable bridge entrypoint path — the module codex launches as `node <path>
 * --root …`. It sits at a KNOWN location relative to this harness module in both build
 * layouts (a built runner emits `dist/harness/codex/bridge-entry.js`; a source layout keeps
 * the sibling `./bridge-entry.js`). Falls back to the sibling path. Production + tests never
 * rely on the fallback — the built entry exists, and tests inject the entry path explicitly.
 */
function resolveBridgeEntryPath(): string {
  const sibling = new URL('./bridge-entry.js', import.meta.url);
  const bundled = new URL('./harness/codex/bridge-entry.js', import.meta.url);
  for (const candidate of [bundled, sibling]) {
    const path = fileURLToPath(candidate);
    if (existsSync(path)) {
      return path;
    }
  }
  return fileURLToPath(sibling);
}

/** The codex thread id from a `thread/start` response, or `undefined`. */
function threadIdOf(response: JsonRpcResponse): string | undefined {
  const result = response.result;
  if (result === null || typeof result !== 'object') {
    return undefined;
  }
  const thread = (result as { thread?: unknown }).thread;
  if (thread === null || typeof thread !== 'object') {
    return undefined;
  }
  const id = (thread as { id?: unknown }).id;
  return typeof id === 'string' && id.length > 0 ? id : undefined;
}

/** The sandbox's host-side work-dir root, or `undefined` when the handle exposes none. */
function sandboxRootDir(sandbox: SandboxHandle): string | undefined {
  const rootDir = (sandbox as { rootDir?: unknown }).rootDir;
  if (typeof rootDir !== 'function') {
    return undefined;
  }
  const value = (rootDir as () => unknown).call(sandbox);
  return typeof value === 'string' ? value : undefined;
}

/** The tmux socket path of a tmux-backed handle, or `undefined` for a non-tmux handle. */
function tmuxSocketOf(sandbox: SandboxHandle): string | undefined {
  const hasPanes = typeof (sandbox as { launchTerminal?: unknown }).launchTerminal === 'function';
  if (!hasPanes) {
    return undefined;
  }
  const root = sandboxRootDir(sandbox);
  if (root === undefined) {
    return undefined;
  }
  return `${root}/.orca-tmux/server.sock`;
}

/** Extract the first text part from a `user.message` payload's content array. */
function textOf(payload: unknown): string | null {
  if (!payload || typeof payload !== 'object') {
    return null;
  }
  const arr = (payload as { content?: unknown }).content;
  if (!Array.isArray(arr)) {
    return null;
  }
  const first = arr.find(
    (p: unknown) =>
      typeof p === 'object' && p !== null && (p as { type?: unknown }).type === 'text',
  ) as { text?: unknown } | undefined;
  return typeof first?.text === 'string' ? first.text : null;
}
