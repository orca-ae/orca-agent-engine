// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `pi` native-CLI harness — boots `pi --mode rpc` and drives it over pi's newline-delimited
// JSON command/event protocol, as an Orca {@link AgentHarness}.
//
// This is the real harness the runner constructs from a `provider: "pi"` snapshot and drives per
// turn. It boots the `pi` BINARY as a long-lived child INSIDE the per-session {@link SandboxHandle}
// — through the 4a native-CLI launcher ({@link launchNativeCli}) — as `pi --mode rpc`, and speaks
// its RPC protocol (the harness is the CLIENT):
//
//   start(input)     → resolve the sandbox + render the managed `models.json` (pi authenticates
//                      from it) + wire the native-CLI tool-bridge as pi's orca EXTENSION (so the
//                      model's orca tools resolve inside the sandbox), build the `--mode rpc`
//                      launch argv (see `launch-args.ts`), boot pi, and start the background
//                      reader that normalizes pi's JSON stdout into Orca-native events.
//   submit(user.msg) → send a `{type:"prompt",message}` command with the user text; await this
//                      turn's terminal `agent_end` (the turn boundary) and emit a terminal
//                      `agent.turn_completed`. `submit` RESOLVES only once the turn's events are
//                      all emitted — honoring the runner's turn contract.
//   interrupt()      → send an `{type:"abort"}` command + abort the in-flight turn WITHOUT ending
//                      the harness (the next `submit` drives a fresh turn).
//   stop(reason)     → kill the pi child and end `events()`.
//   events()         → the single stream of emitted agent events.
//
// Approval routing: pi's RPC mode has NO server→client approval request (unlike codex's
// `on-request`). The sound PRE-EXECUTION gate is the orca extension's `tool_call` hook, which asks
// the client to approve via `ctx.ui.select(...)`; pi serializes that as an `extension_ui_request`
// on stdout and BLOCKS the tool until the client replies with an `extension_ui_response` on stdin.
// The harness routes each such request to the uniform transcript approval: a tool whose policy is
// `always_ask` (or fail-closed with no policy) emits an Anthropic-native `agent.requires_action`
// signal then PARKS the verdict; on a DENY (or interrupt / stop) it replies `Block` so the hook
// returns `{block:true}` and pi never runs the tool (surfacing an error tool_result) — a genuine
// per-tool, pre-exec block, NOT a whole-turn abort. On an ALLOW it replies `Allow`. A tool whose
// policy is `always_allow` is answered `Allow` with no signal. With NO gate wired the extension's
// hook sees a UI channel but the harness answers every request `Allow` — equivalently, the OS
// sandbox + composed policies are the guardrail. `interrupt()` still sends `{type:"abort"}` to
// cancel the whole in-flight TURN (the turn-level lever), distinct from the per-tool block above.
//
// Emits Orca-native Anthropic-shaped {@link AgentEvent}s ONLY (the SAME shapes the in-process
// providers + the claude-code / codex native-CLI providers emit; NO pi dialect) — including the
// internal `agent.usage` token-accounting event (mapped from each assistant `message_end`'s usage;
// the runner records it into the session usage sink and does NOT persist it publicly). The
// normalization lives in {@link PiProtocol}.

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
import { buildPiLaunchConfig, type PiBridgeCommand } from './launch-args.js';
import {
  PiProtocol,
  type PiUiRequest,
  ORCA_APPROVAL_ALLOW,
  ORCA_APPROVAL_BLOCK,
} from './protocol.js';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import {
  BRIDGE_ROOT_FLAG,
  BRIDGE_TMUX_SOCKET_FLAG,
  pushAllowedToolsFlag,
} from '../claude-code/bridge-runtime.js';

/**
 * The Anthropic-native `agent.requires_action` signal payload emitted when a tool call parks on
 * the human gate: the action kind, a per-approval `tool_use_id` (the verdict's routing key), and
 * the tool name. Mirrors the in-process + claude-code + codex providers' signal exactly.
 */
export interface RequiresActionSignal {
  action: 'tool_confirmation';
  tool_use_id: string;
  tool_name: string;
}

/** Construction options for {@link PiCliHarness}. */
export interface PiHarnessOptions {
  /** Default model id when the snapshot did not pin one. */
  modelDefault: string;
  /** Owning workspace (tenant scope). */
  workspaceId: string;
  /** Orca session id this harness serves. */
  sessionId: string;
  /**
   * The per-session {@link SandboxHandle} the provider captured from the session context, used as
   * a FALLBACK when a `start` input omits its own `sandbox`. A native-CLI harness REQUIRES a
   * sandbox (it launches a real child + wires the tool-bridge to the session tree), so when
   * neither carries one, `start` fails fast rather than run unsandboxed.
   */
  sandbox?: SandboxHandle;
  /** The scoped session JWT forwarded to pi (credential-free) + rendered into models.json. */
  apiKey?: string;
  /** The gateway LLM-proxy base URL forwarded to pi + rendered into models.json. */
  baseURL?: string;
  /**
   * The CLI binary path/name. Defaults to `pi` on the sandbox PATH; a test injects a fake CLI here
   * (e.g. `node`) with {@link cliPrefixArgs} carrying the script path.
   */
  cliCommand?: string;
  /** Prefix args before the generated flags (e.g. a fake CLI script path under `node`). */
  cliPrefixArgs?: string[];
  /** Extra suffix args after the generated flags (e.g. a test probe flag). */
  cliExtraArgs?: string[];
  /**
   * The bridge entrypoint executable pi's orca extension spawns as its MCP server. Defaults to the
   * current Node executable; a test injects `tsx` here (with {@link bridgePrefixArgs} carrying the
   * TS entry path) so pi's extension speaks MCP to the TS source.
   */
  bridgeCommand?: string;
  /** Prefix args before the bridge entry's own flags (the bridge entry module path). */
  bridgePrefixArgs?: string[];
  /**
   * The pi orca-extension module path wired via `--extension`. Defaults to the shipped
   * `orca-extension.mjs` beside this harness; a test injects the source path explicitly.
   */
  extensionPath?: string;
  /** The native-CLI launcher (injected for tests). Defaults to the real {@link launchNativeCli}. */
  launch?: typeof launchNativeCli;
  /**
   * How long a turn waits for pi's terminal `agent_end` before it is ended with a timeout
   * error. Defaults to {@link DEFAULT_TURN_DEADLINE_MS}. Bounded so a CLI that accepted the
   * prompt command and went MUTE cannot park the turn forever; a test shortens it.
   */
  turnTimeoutMs?: number;
  /**
   * Optional structured logger. pi's extension-UI protocol answers an approval with a bare
   * `Allow`/`Block` option and carries no message, so a faulted approval GATE has nowhere
   * else to report: this is the only channel that reason reaches.
   */
  logger?: NativeCliLogger;
}

/** The in-flight turn's state: a resolver fired when the turn's terminal `agent_end` lands. */
interface TurnState {
  resolve: () => void;
  /**
   * The turn's abort signal — fired by {@link PiCliHarness.interrupt} / {@link PiCliHarness.stop}.
   * An approval parked on the human gate races this so an interrupted / stopped turn releases the
   * parked gate as a DENY (aborting the in-flight pi tool) rather than stalling the reader.
   */
  abort: AbortController;
}

/**
 * Native-CLI harness backed by `pi --mode rpc`. One per session; the runner `start`s it from the
 * snapshot then `submit`s each user turn against the SAME live pi process.
 */
export class PiCliHarness implements AgentHarness {
  private readonly launch: typeof launchNativeCli;
  private input: SessionStartInput | undefined;
  private sandbox: SandboxHandle | undefined;
  /** The RPC protocol normalizer for this session (framing + event mapping). */
  private readonly protocol = new PiProtocol();
  /** The booted pi child, or `undefined` before `start` / after `stop`. */
  private cli: NativeCliProcess | undefined;
  /** The background stdout reader (resolves when pi's stdout ends). */
  private readerDone: Promise<void> | undefined;
  private outQueue: AgentEvent[] = [];
  private outResolvers: Array<(e: IteratorResult<AgentEvent>) => void> = [];
  private terminated = false;
  /** The in-flight turn, or `undefined` between turns. */
  private turn: TurnState | undefined;
  /**
   * In-flight approval parks, keyed by their extension-UI request id. Teardown aborts every entry
   * so a fire-and-forget approval awaiting the human gate releases as a deny (a `Block` reply).
   */
  private readonly pendingApprovals = new Map<string, AbortController>();
  /** Per-session monotonically increasing counter for command ids + signal ids. */
  private seq = 0;

  /** Bound on how long one turn may park on pi's terminal frame (see {@link submit}). */
  private readonly turnTimeoutMs: number;

  constructor(private readonly opts: PiHarnessOptions) {
    this.launch = opts.launch ?? launchNativeCli;
    this.turnTimeoutMs = opts.turnTimeoutMs ?? DEFAULT_TURN_DEADLINE_MS;
  }

  async start(input: SessionStartInput): Promise<void> {
    this.input = input;
    this.terminated = false;
    // A native-CLI harness must have a sandbox: it launches a real child and binds the tool-bridge
    // to the session tree. Fail fast rather than run unsandboxed.
    const sandbox = input.sandbox ?? this.opts.sandbox;
    if (sandbox === undefined) {
      throw new Error('pi provider requires a per-session sandbox but none was wired');
    }
    this.sandbox = sandbox;

    // Render the managed models.json (pi authenticates from it) before booting pi.
    const root = requireSandboxRootDir(sandbox);
    const agentDir = `${root}/.pi-agent-home`;
    this.writeModelsConfig(agentDir, input);

    const bridge = this.buildBridgeCommand(sandbox, input);
    const config = buildPiLaunchConfig(this.buildLaunchInput(sandbox, input, agentDir, bridge));
    // The pi orca extension reads the bridge child launch off the env (pi has no MCP-config arg),
    // so stamp it onto the CLI env alongside the launch-args builder's own env.
    config.env = {
      ...(config.env ?? {}),
      ORCA_PI_BRIDGE_COMMAND: bridge.command,
      ORCA_PI_BRIDGE_ARGS: JSON.stringify(bridge.args),
    };

    // Boot pi inside the sandbox and start reading its RPC stdout.
    this.cli = this.launch(sandbox, config);
    this.readerDone = this.readLoop(this.cli);
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

    // Drive the turn: send the prompt command, then await this turn's terminal `agent_end`.
    let turn!: TurnState;
    const turnDone = new Promise<void>((resolve) => {
      turn = { resolve, abort: new AbortController() };
      this.turn = turn;
    });
    this.sendCommand({ type: 'prompt', message: text });
    // BOUNDED (see DEFAULT_TURN_DEADLINE_MS): a CLI that took the command and went MUTE says
    // nothing on any channel, so an unbounded park here never ends — `submit` never
    // resolves, the loop parks on both legs of its turn race, and the consumer never gets a
    // marker. On expiry, name the timeout on the wire FIRST (a turn released with nothing
    // said reaches the client as a bare `agent.turn_completed` — a wedged CLI recorded as a
    // successful, empty answer), then TEAR THE CHILD DOWN — a deadline that ends the turn
    // and leaves the CLI alive desynchronizes the two for good — and only then release the
    // park.
    await awaitTurnWithinDeadline(turnDone, this.turnTimeoutMs, () => {
      // `this.turn !== turn` means the turn already ended by another path (the reader's
      // `agent_end`, an interrupt, teardown) and this deadline lost the race.
      if (this.turn !== turn) {
        return;
      }
      this.emit(terminalError(turnDeadlineFaultMessage('pi', this.turnTimeoutMs)));
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

    // The turn's events are all emitted; close it with a terminal completed marker so the runner's
    // turn boundary fires (mirrors the in-process + claude-code + codex providers).
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
    // Best-effort: ask pi to abort the in-flight turn over the control channel.
    this.sendCommand({ type: 'abort' });
    // Also settle the turn locally in case pi does not emit a fresh terminal `agent_end`.
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
    // Kill the pi child (ends its stdout → the reader loop completes) and end the stream.
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
   * The background reader: consume pi's RPC stdout LINES through the protocol normalizer,
   * dispatching each:
   *   - `response` → drop (the harness drives fire-and-forget commands; the turn settles on
   *     `agent_end`, not on the command's `{success}` ack);
   *   - `events` → emit them;
   *   - `ui_request` → route the extension-UI approval to the gate (NON-blocking), replying
   *     `Allow`/`Block` on stdin so the extension's `tool_call` hook proceeds or blocks the tool
   *     PRE-EXECUTION;
   *   - `turn_end` → settle the in-flight turn (`agent_end` is the turn boundary);
   *   - `ignore` → drop.
   * A non-JSON line is skipped. The loop completes when pi's stdout ends (exit / kill), at which
   * point it ends the event stream so a direct consumer is not stranded.
   * On the way out it reports an ABNORMAL exit as a terminal `agent.error` (a non-zero code,
   * or a signal this harness did not send) — stdout ending looks the same for a clean finish
   * and for a crash, so without that a dead CLI ends the turn as an empty success.
   */
  private async readLoop(cli: NativeCliProcess): Promise<void> {
    try {
      const failure = await cli.failure;
      if (failure !== undefined) {
        // pi never started — a missing / misnamed `pi` on the sandbox PATH is the likeliest
        // production misconfiguration. Its stdout is empty, so falling through would end the
        // turn as a clean, silent no-op. Put the launch failure on the wire as a terminal
        // `agent.error` (the established way a fault reaches the transcript) and drop into
        // the `finally`, which releases the parked turn and ends the stream.
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
        if (result.kind === 'events') {
          for (const ev of result.events) {
            this.emit(ev);
          }
        } else if (result.kind === 'ui_request') {
          // Dispatch WITHOUT blocking the reader (a later interrupt / stop must stay reachable).
          // `handleApproval` NEVER rejects — it catches a gate fault as a deny (a `Block` reply)
          // and always answers pi — so this fire-and-forget raises no unhandled rejection.
          void this.handleApproval(result.request);
        } else if (result.kind === 'turn_end') {
          this.turn?.resolve();
        }
        // `response` / `ignore` → drop.
      }
    } finally {
      // pi's stdout ended — which looks IDENTICAL for a clean finish, a crash, and the kill
      // `stop()` issues. Ask the exit status which it was, and when it was a CRASH put that on
      // the wire FIRST: after `endEventStream()` below the stream is closed and an emitted
      // event would never be read, and a turn released with nothing said reaches the client as
      // a bare `agent.turn_completed` — a dead CLI recorded as a successful, empty answer.
      // Skipped once terminated: teardown killed the child, so the exit is one we asked for
      // (and the turn was already released by `stop`).
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
      // released and the stream ended — that would reintroduce the very hang the exit
      // status exists to prevent. Nothing to report is the same answer as no fault.
      const exitFault = await cliExitFaultMessage(cli, 'pi', parked ? 'mid-turn' : 'idle').catch(
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
            'pi CLI died while the session was idle',
          );
        }
      }
      this.turn?.resolve();
      if (!this.terminated) {
        this.terminated = true;
        this.endEventStream();
      }
    }
  }

  /**
   * Route a pi extension-UI approval request to the uniform transcript approval, then answer it with
   * an `extension_ui_response` on stdin: `Block` on a DENY (or interrupt / stop) so the extension's
   * `tool_call` hook returns `{block:true}` and pi never runs the tool (surfacing an error
   * tool_result), or `Allow` so the tool proceeds. This is a genuine PRE-EXECUTION, per-tool block —
   * NOT a whole-turn abort.
   *
   * With no confirmation gate wired the harness answers `Allow` (the OS sandbox + composed policies
   * are the guardrail); a tool whose policy is `always_allow` is answered `Allow` with no signal.
   */
  private async handleApproval(request: PiUiRequest): Promise<void> {
    const cancel = new AbortController();
    this.pendingApprovals.set(request.requestId, cancel);
    // A gate fault must NOT escape: this runs fire-and-forget, so a throw would become an
    // unhandled rejection (fatal to this process) AND skip the reply below, leaving pi's parked
    // `ctx.ui.select` — and the tool call behind it — hung for the rest of the session. So the
    // decision is seeded fail-closed and the reply is sent from the `finally`.
    let decision: ToolPermissionDecision = {
      behavior: 'deny',
      message: 'Tool use denied: the permission gate failed.',
    };
    try {
      decision = await this.resolveApproval(request, cancel.signal);
    } catch (err) {
      // The gate itself faulted: DENY (a tool must never run on an unresolved verdict) and
      // record the reason. A bare `catch` did not even BIND the error, so the cause was
      // unrecoverable: a permission-store outage denied every tool call in the session with
      // nothing anywhere saying why. pi's extension-UI reply is a bare `Allow`/`Block`
      // option with no message field, so the LOG is the only channel that reason reaches;
      // the deny message is set too, matching the claude-code / cursor harnesses.
      const reason = err instanceof Error ? err.message : String(err);
      decision = {
        behavior: 'deny',
        message: `Tool use denied: the permission gate failed: ${reason}`,
      };
      this.opts.logger?.error?.(
        { err, sessionId: this.opts.sessionId, tool: request.toolName },
        'pi approval gate failed; denying the tool call',
      );
    } finally {
      this.pendingApprovals.delete(request.requestId);
      // ALWAYS reply on the extension-UI sub-protocol; the parked `ctx.ui.select` resolves to this
      // option and the `tool_call` hook proceeds (Allow) or blocks the tool pre-execution (Block).
      try {
        this.sendUiResponse(
          request.requestId,
          decision.behavior === 'allow' ? ORCA_APPROVAL_ALLOW : ORCA_APPROVAL_BLOCK,
        );
      } catch {
        // pi's stdin is gone (it exited / was killed): there is nobody left to answer.
      }
    }
  }

  /**
   * Resolve an approval to an allow/deny decision (the gate + policy logic). The parked verdict is
   * raced against the turn's abort (interrupt / stop) so it releases as a deny rather than stalling.
   */
  private async resolveApproval(
    request: PiUiRequest,
    cancelSignal: AbortSignal,
  ): Promise<ToolPermissionDecision> {
    const toolPermissions: ToolPermissionResolver | undefined = this.input?.toolPermissions;
    const policy = toolPermissions ? toolPermissions.policyFor(request.toolName) : 'always_ask';
    if (policy === 'always_deny')
      return { behavior: 'deny', message: 'Tool use denied by policy.' };
    const confirmTool: ToolConfirmer | undefined = this.input?.confirmTool;
    if (confirmTool === undefined) {
      // No gate → the OS sandbox + policies are the guardrail; answer Allow.
      return { behavior: 'allow', updatedInput: {} };
    }
    if (policy === 'always_allow') {
      return { behavior: 'allow', updatedInput: {} };
    }
    const abortSignal = this.turn?.abort.signal;
    if (abortSignal?.aborted === true || cancelSignal.aborted) {
      return { behavior: 'deny', message: 'Tool use denied: turn aborted.' };
    }
    // Park on the human gate: emit the requires_action signal keyed by the tool-call id, then await
    // the verdict delivered through the gate. The gate's tool input is unavailable in pi's UI
    // request (the `tool_call` hook holds it, not the UI frame), so the signal carries `{}`.
    const toolUseId = request.toolCallId;
    const signal: RequiresActionSignal = {
      action: 'tool_confirmation',
      tool_use_id: toolUseId,
      tool_name: request.toolName,
    };
    this.emit({ kind: 'agent.requires_action', id: toolUseId, payload: signal });
    const verdict = confirmTool(request.toolName, {}, { toolUseId });
    // Race the parked verdict against the turn's abort AND this request's cancel so an interrupt /
    // stop releases the gate as a deny.
    const races: Array<Promise<ToolPermissionDecision>> = [verdict];
    const cleanups: Array<() => void> = [];
    for (const sig of [abortSignal, cancelSignal]) {
      if (sig === undefined) {
        continue;
      }
      let onAbort: (() => void) | undefined;
      races.push(
        new Promise<ToolPermissionDecision>((resolve) => {
          onAbort = (): void =>
            resolve({ behavior: 'deny', message: 'Tool use denied: turn aborted.' });
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

  /** Send a pi RPC command (fire-and-forget; the reader settles turns on `agent_end`). */
  private sendCommand(command: Record<string, unknown>): void {
    if (this.cli === undefined) {
      return;
    }
    const frame = { id: `c${++this.seq}`, ...command };
    this.cli.write(`${JSON.stringify(frame)}\n`);
  }

  /**
   * Reply to a pi `extension_ui_request` with the chosen `select` option, keyed by the ORIGINAL
   * request id (NOT a fresh command id — the extension awaits its own id). `value` is the option
   * string (`Allow`/`Block`); the extension's `tool_call` hook resolves its `ctx.ui.select` to it.
   */
  private sendUiResponse(requestId: string, value: string): void {
    if (this.cli === undefined) {
      return;
    }
    const frame = { type: 'extension_ui_response', id: requestId, value };
    this.cli.write(`${JSON.stringify(frame)}\n`);
  }

  /**
   * Render the per-session managed `models.json` pi authenticates from — the `orca` provider entry
   * pointing at the gateway LLM egress (Anthropic Messages surface; the gateway swaps the scoped
   * JWT for the real upstream secret). Absent egress → no file (pi falls back to its ambient auth).
   */
  private writeModelsConfig(agentDir: string, input: SessionStartInput): void {
    const apiKey = this.opts.apiKey;
    const baseURL = this.opts.baseURL;
    if (
      apiKey === undefined ||
      apiKey.length === 0 ||
      baseURL === undefined ||
      baseURL.length === 0
    ) {
      return;
    }
    const model = effectiveModel(input.agentSnapshot.model_id, this.opts.modelDefault);
    const models = model !== undefined ? [{ id: model }] : [];
    const config = {
      providers: {
        orca: {
          baseUrl: baseURL,
          api: 'anthropic-messages',
          apiKey,
          authHeader: true,
          models,
        },
      },
    };
    mkdirSync(agentDir, { recursive: true });
    writeFileSync(`${agentDir}/models.json`, `${JSON.stringify(config, null, 2)}\n`, {
      encoding: 'utf8',
    });
  }

  /** Build the launch input for {@link buildPiLaunchConfig} from the boot context. */
  private buildLaunchInput(
    sandbox: SandboxHandle,
    input: SessionStartInput,
    agentDir: string,
    bridge: PiBridgeCommand,
  ): Parameters<typeof buildPiLaunchConfig>[0] {
    void sandbox;
    const model = effectiveModel(input.agentSnapshot.model_id, this.opts.modelDefault);
    const out: Parameters<typeof buildPiLaunchConfig>[0] = { bridge, agentDir };
    if (this.opts.cliCommand !== undefined) {
      out.cliPath = this.opts.cliCommand;
    }
    if (this.opts.cliPrefixArgs !== undefined) {
      out.prefixArgs = this.opts.cliPrefixArgs;
    }
    if (this.opts.cliExtraArgs !== undefined) {
      out.suffixArgs = this.opts.cliExtraArgs;
    }
    if (model !== undefined) {
      out.model = model;
    }
    const system = input.agentSnapshot.system;
    if (system !== undefined && system.trim().length > 0) {
      out.system = system;
    }
    if (this.opts.baseURL !== undefined) {
      out.baseURL = this.opts.baseURL;
    }
    if (this.opts.apiKey !== undefined) {
      out.apiKey = this.opts.apiKey;
    }
    return out;
  }

  /**
   * Build the orca extension wiring pi loads via `--extension`: the extension module path plus
   * the bridge entrypoint pointed at the session sandbox root (+ its tmux socket on the local-
   * attach transport, + the skill-allowlist), so the bridge REATTACHES to the same session tree.
   *
   * @throws Error when the sandbox handle exposes no `rootDir()` (a cloud-only handle needs a
   *   different bridge transport — out of scope for the root-dir-backed reusable runtimes).
   */
  private buildBridgeCommand(sandbox: SandboxHandle, input: SessionStartInput): PiBridgeCommand {
    const args = [...(this.opts.bridgePrefixArgs ?? [resolveBridgeEntryPath()])];
    const root = requireSandboxRootDir(sandbox);
    args.push(BRIDGE_ROOT_FLAG, root);
    const socket = tmuxSocketOf(sandbox);
    if (socket !== undefined) {
      args.push(BRIDGE_TMUX_SOCKET_FLAG, socket);
    }
    // `[]` is DENY-ALL, not "no restriction" — see `claude-code/bridge-runtime.ts`.
    pushAllowedToolsFlag(args, input.agentSnapshot.allowed_tool_names);
    return {
      extension: this.opts.extensionPath ?? resolveExtensionPath(),
      command: this.opts.bridgeCommand ?? process.execPath,
      args,
    };
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
  return terminalError(`pi CLI failed to launch: ${err.message}`);
}

/** The effective model id: the snapshot's, else the configured default, else `undefined`. */
function effectiveModel(
  snapshotModel: string | undefined,
  modelDefault: string,
): string | undefined {
  if (snapshotModel !== undefined && snapshotModel.length > 0) {
    return snapshotModel;
  }
  if (modelDefault.length > 0) {
    return modelDefault;
  }
  return undefined;
}

/**
 * Resolve the runnable bridge entrypoint path — the module pi's orca extension launches as
 * `node <path> --root …`. It sits at a KNOWN location relative to this harness module in both build
 * layouts (a built runner emits `dist/harness/pi/bridge-entry.js`; a source layout keeps the
 * sibling `./bridge-entry.js`). Falls back to the sibling path.
 */
function resolveBridgeEntryPath(): string {
  const sibling = new URL('./bridge-entry.js', import.meta.url);
  const bundled = new URL('./harness/pi/bridge-entry.js', import.meta.url);
  for (const candidate of [bundled, sibling]) {
    const path = fileURLToPath(candidate);
    if (existsSync(path)) {
      return path;
    }
  }
  return fileURLToPath(sibling);
}

/**
 * Resolve the shipped orca-extension module path — the `.mjs` pi loads via `--extension`. It is
 * NOT compiled by the runner build (it is loaded directly by pi's Node runtime), so it ships beside
 * this module's SOURCE; both the source layout (`./orca-extension.mjs`) and a built layout that
 * copied it next to `dist/harness/pi/` resolve here. Falls back to the sibling path.
 */
function resolveExtensionPath(): string {
  const sibling = new URL('./orca-extension.mjs', import.meta.url);
  const bundled = new URL('./harness/pi/orca-extension.mjs', import.meta.url);
  for (const candidate of [bundled, sibling]) {
    const path = fileURLToPath(candidate);
    if (existsSync(path)) {
      return path;
    }
  }
  return fileURLToPath(sibling);
}

/**
 * The sandbox's host-side work-dir root. A native-CLI harness reattaches the tool-bridge to an
 * absolute host work-dir, so a handle that exposes no `rootDir()` (a cloud-only handle) cannot back
 * this provider — fail fast rather than emit a `--root`-less bridge.
 *
 * @throws Error when the handle exposes no `rootDir()`.
 */
function requireSandboxRootDir(sandbox: SandboxHandle): string {
  const root = sandboxRootDir(sandbox);
  if (root === undefined) {
    throw new Error(
      `pi provider requires a root-dir-backed sandbox (the native-CLI tool-bridge reattaches to ` +
        `an absolute host work-dir), but the session sandbox '${sandbox.id}' exposes no rootDir(); ` +
        `a cloud-only handle needs a different bridge transport.`,
    );
  }
  return root;
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
