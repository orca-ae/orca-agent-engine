// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The generic `custom` native-CLI harness — boots an operator-declared CLI and drives it per the
// declarative spec, as an Orca {@link AgentHarness}.
//
// This is the real harness the runner constructs from a `provider: "custom"` snapshot and drives
// per turn. It lets an operator register ANY CLI agent WITHOUT a bespoke provider: the snapshot
// carries a declarative {@link CustomAgentSpec} (command / argv-with-placeholders / env / cwd / a
// stdout→AgentEvent mapping / an optional approvals opt-in). The harness boots that CLI as a long-
// lived child INSIDE the per-session {@link SandboxHandle} — through the 4a native-CLI launcher
// ({@link launchNativeCli}) — and wires the 4a native-CLI tool-bridge as the CLI's `orca` MCP
// server (so the model's orca built-ins resolve inside the sandbox):
//
//   start(input)     → parse the spec + resolve the sandbox, build the bridge command, substitute
//                      the launch argv/env placeholders (`launch-args.ts`), boot the CLI, and start
//                      the background reader that normalizes the CLI's stdout into Orca-native
//                      events per the spec's stdout mapping (`normalizer.ts`).
//   submit(user.msg) → write the user turn to the CLI's stdin (per the spec's stdin template) and
//                      await this turn's boundary — a `turn_completed` frame / an `end_sentinel`
//                      line (`jsonLine`/`text` mode) or the CLI's stdout ending (a one-shot CLI).
//                      `submit` RESOLVES only once the turn's events are all emitted — honoring the
//                      runner's turn contract.
//   interrupt()      → abort the in-flight turn WITHOUT ending the harness (the next `submit`
//                      drives a fresh turn); releases any approval parked on the gate as a deny.
//   stop(reason)     → kill the CLI child and end `events()`.
//   events()         → the single stream of emitted agent events.
//
// Approval routing (opt-in): when the spec declares an `approvals` block AND a `stdout` approval
// rule, an approval frame the CLI raises on stdout is routed to the uniform transcript approval —
// a tool whose policy is `always_ask` (or fail-closed with no policy) emits an Anthropic-native
// `agent.requires_action` signal then PARKS the verdict; a tool whose policy is `always_allow`
// auto-approves — and the harness writes the spec's response frame back on the CLI's stdin
// (`{decision}` = the spec's allow/deny value). With NO gate wired every approval is auto-allowed
// (the OS sandbox + composed policies are the guardrail).
//
// Emits Orca-native Anthropic-shaped {@link AgentEvent}s ONLY (the SAME shapes the in-process
// providers + the other native-CLI providers emit; NO CLI-specific dialect) — the normalization
// lives in {@link CustomStreamNormalizer}.

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
import { buildCustomLaunchConfig, type CustomBridgeCommand } from './launch-args.js';
import { CustomStreamNormalizer, type CustomApprovalRequest } from './normalizer.js';
import { parseCustomAgentSpec, type CustomAgentSpec } from './spec.js';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import {
  BRIDGE_ROOT_FLAG,
  BRIDGE_TMUX_SOCKET_FLAG,
  pushAllowedToolsFlag,
} from '../claude-code/bridge-runtime.js';

/**
 * The Anthropic-native `agent.requires_action` signal payload emitted when an approval parks on the
 * human gate: the action kind, a per-approval `tool_use_id` (the verdict's routing key), and the
 * tool name. Mirrors the in-process + other native-CLI providers' signal exactly.
 */
export interface RequiresActionSignal {
  action: 'tool_confirmation';
  tool_use_id: string;
  tool_name: string;
}

/** Construction options for {@link CustomCliHarness}. */
export interface CustomHarnessOptions {
  /**
   * The declarative custom-agent spec (parsed or still-opaque). The provider passes the snapshot's
   * `custom_spec` block here; the harness parses it at `start`. REQUIRED — a `custom` snapshot with
   * no spec fails fast (there is nothing to launch).
   */
  spec?: unknown;
  /** Default model id when the snapshot did not pin one. */
  modelDefault: string;
  /** Owning workspace (tenant scope). */
  workspaceId: string;
  /** Orca session id this harness serves. */
  sessionId: string;
  /**
   * The per-session {@link SandboxHandle} the provider captured from the session context, used as a
   * FALLBACK when a `start` input omits its own `sandbox`. A native-CLI harness REQUIRES a sandbox
   * (it launches a real child + wires the tool-bridge to the session tree), so when neither carries
   * one, `start` fails fast rather than run unsandboxed.
   */
  sandbox?: SandboxHandle;
  /** The scoped session JWT forwarded to the CLI as `ORCA_LLM_API_KEY` (credential-free). */
  apiKey?: string;
  /** The gateway LLM-proxy base URL forwarded to the CLI as `ORCA_LLM_BASE_URL`. */
  baseURL?: string;
  /**
   * Override the spec's `command` with this binary path/name. Defaults to the spec's `command`; a
   * test injects a fake CLI here (e.g. `node`) with {@link cliPrefixArgs} carrying the script path.
   */
  cliCommand?: string;
  /** Prefix args before the spec argv (e.g. a fake CLI script path under `node`). */
  cliPrefixArgs?: string[];
  /** Extra suffix args after the spec argv (e.g. a test probe flag). */
  cliExtraArgs?: string[];
  /**
   * The bridge entrypoint executable the CLI spawns as its `orca` MCP server. Defaults to the
   * current Node executable; a test injects `tsx` here (with {@link bridgePrefixArgs} carrying the
   * TS entry path) so the CLI child speaks MCP to the TS source.
   */
  bridgeCommand?: string;
  /** Prefix args before the bridge entry's own flags (the bridge entry module path). */
  bridgePrefixArgs?: string[];
  /** The native-CLI launcher (injected for tests). Defaults to the real {@link launchNativeCli}. */
  launch?: typeof launchNativeCli;
  /**
   * How long a turn waits for the spec's `turn_completed` frame before it is ended with a
   * timeout error. Defaults to {@link DEFAULT_TURN_DEADLINE_MS}. Bounded so a CLI that
   * accepted the turn and went MUTE cannot park the turn forever; a test shortens it.
   */
  turnTimeoutMs?: number;
  /**
   * Optional structured logger. The spec's approval response frame carries a bare
   * allow/deny VALUE and no message, so a faulted approval GATE has nowhere else to report:
   * this is the only channel that reason reaches.
   */
  logger?: NativeCliLogger;
}

/** The in-flight turn's state: a resolver fired when the turn's boundary lands. */
interface TurnState {
  resolve: () => void;
  /**
   * The turn's abort signal — fired by {@link CustomCliHarness.interrupt} /
   * {@link CustomCliHarness.stop}. An approval parked on the human gate races this so an interrupted
   * / stopped turn releases the parked gate as a DENY (writing the deny frame back to the CLI)
   * rather than stalling the reader.
   */
  abort: AbortController;
}

/**
 * Native-CLI harness backed by an operator-declared CLI. One per session; the runner `start`s it
 * from the snapshot then `submit`s each user turn against the SAME live CLI process.
 */
export class CustomCliHarness implements AgentHarness {
  private readonly launch: typeof launchNativeCli;
  private input: SessionStartInput | undefined;
  private sandbox: SandboxHandle | undefined;
  /** The parsed spec (resolved at `start`). */
  private spec: CustomAgentSpec | undefined;
  /** The stdout normalizer for this session (framing + event mapping per the spec). */
  private normalizer: CustomStreamNormalizer | undefined;
  /** The booted CLI child, or `undefined` before `start` / after `stop`. */
  private cli: NativeCliProcess | undefined;
  /** The background stdout reader (resolves when the CLI's stdout ends). */
  private readerDone: Promise<void> | undefined;
  private outQueue: AgentEvent[] = [];
  private outResolvers: Array<(e: IteratorResult<AgentEvent>) => void> = [];
  private terminated = false;
  /** The in-flight turn, or `undefined` between turns. */
  private turn: TurnState | undefined;
  /**
   * In-flight approval parks, keyed by their request id. Teardown aborts every entry so a fire-and-
   * forget approval awaiting the human gate releases as a deny.
   */
  private readonly pendingApprovals = new Map<string, AbortController>();
  /** Per-session monotonically increasing counter for signal ids. */
  private seq = 0;

  /** Bound on how long one turn may park on the CLI's terminal frame (see {@link submit}). */
  private readonly turnTimeoutMs: number;

  constructor(private readonly opts: CustomHarnessOptions) {
    this.launch = opts.launch ?? launchNativeCli;
    this.turnTimeoutMs = opts.turnTimeoutMs ?? DEFAULT_TURN_DEADLINE_MS;
  }

  async start(input: SessionStartInput): Promise<void> {
    this.input = input;
    this.terminated = false;
    // Parse the operator spec — a `custom` snapshot with no spec is a capability error (there is
    // nothing to launch), surfaced fail-fast at start.
    if (this.opts.spec === undefined || this.opts.spec === null) {
      throw new Error(
        'custom provider requires a custom agent spec on the snapshot but none was carried',
      );
    }
    const spec = parseCustomAgentSpec(this.opts.spec);
    this.spec = spec;
    this.normalizer = new CustomStreamNormalizer(spec);

    // A native-CLI harness must have a sandbox: it launches a real child and binds the tool-bridge
    // to the session tree. Fail fast rather than run unsandboxed.
    const sandbox = input.sandbox ?? this.opts.sandbox;
    if (sandbox === undefined) {
      throw new Error('custom provider requires a per-session sandbox but none was wired');
    }
    this.sandbox = sandbox;

    const bridge = this.buildBridgeCommand(sandbox, input);
    const config = buildCustomLaunchConfig(this.buildLaunchInput(sandbox, input, spec, bridge));
    // Boot the CLI inside the sandbox and start reading its stdout.
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
    if (text === null || this.cli === undefined || this.spec === undefined) {
      return;
    }

    // Drive the turn: write the user turn to stdin, then await this turn's boundary.
    let turn!: TurnState;
    const turnDone = new Promise<void>((resolve) => {
      turn = { resolve, abort: new AbortController() };
      this.turn = turn;
    });
    this.writeUserTurn(this.spec, text);
    // BOUNDED (see DEFAULT_TURN_DEADLINE_MS): a CLI that took the turn and went MUTE says
    // nothing on any channel, so an unbounded park here never ends — `submit` never
    // resolves, the loop parks on both legs of its turn race, and the consumer never gets a
    // marker. On expiry, name the timeout on the wire FIRST (a turn released with nothing
    // said reaches the client as a bare `agent.turn_completed` — a wedged CLI recorded as a
    // successful, empty answer), then TEAR THE CHILD DOWN — a deadline that ends the turn
    // and leaves the CLI alive desynchronizes the two for good — and only then release the
    // park.
    await awaitTurnWithinDeadline(turnDone, this.turnTimeoutMs, () => {
      // `this.turn !== turn` means the turn already ended by another path (the spec's
      // terminal frame, an interrupt, teardown) and this deadline lost the race.
      if (this.turn !== turn) {
        return;
      }
      this.emit(terminalError(turnDeadlineFaultMessage('custom', this.turnTimeoutMs)));
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
    // turn boundary fires (mirrors the in-process + other native-CLI providers).
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
    // Settle the turn locally — a generic CLI has no universal interrupt control channel, so the
    // in-flight turn is abandoned while the child KEEPS RUNNING. Anything it says afterwards
    // still reaches the reader: events are emitted as they arrive, and a terminal frame resolves
    // whatever turn is CURRENT. The turn deadline answers that hazard by killing the child; an
    // interrupt deliberately keeps the session alive, so it cannot.
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
    // Kill the CLI child (ends its stdout → the reader loop completes) and end the stream.
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
   * The background reader: consume the CLI's stdout LINES through the normalizer, dispatching each:
   *   - `events` → emit them;
   *   - `turn_end` → emit any carried events then settle the in-flight turn;
   *   - `approval_request` → route to the gate (NON-blocking), writing the spec's decision frame on
   *     stdin when it resolves;
   *   - `ignore` → drop.
   * A non-JSON line in jsonLine mode is `ignore`d by the normalizer. The loop completes when the
   * CLI's stdout ends (exit / kill); it settles any in-flight turn (a one-shot CLI ends its turn by
   * exiting) and ends the event stream so a direct consumer is not stranded.
   * On the way out it reports an ABNORMAL exit as a terminal `agent.error` (a non-zero code,
   * or a signal this harness did not send) — stdout ending looks the same for a clean finish
   * and for a crash, so without that a dead CLI ends the turn as an empty success.
   */
  private async readLoop(cli: NativeCliProcess): Promise<void> {
    try {
      const failure = await cli.failure;
      if (failure !== undefined) {
        // The CLI never started — a missing / misnamed binary on the sandbox PATH is the
        // likeliest production misconfiguration, and an operator spec names that binary, so
        // it is the likeliest spec mistake too. Its stdout is empty, so falling through would
        // end the turn as a clean, silent no-op. Put the launch failure on the wire as a
        // terminal `agent.error` (the established way a fault reaches the transcript) and
        // drop into the `finally`, which releases the parked turn and ends the stream.
        this.emit(launchFailureEvent(failure));
        return;
      }
      for await (const raw of cli.lines()) {
        if (this.terminated) {
          break;
        }
        const result = this.normalizer?.map(raw) ?? { kind: 'ignore' as const };
        if (result.kind === 'events') {
          for (const ev of result.events) {
            this.emit(ev);
          }
        } else if (result.kind === 'turn_end') {
          for (const ev of result.events) {
            this.emit(ev);
          }
          this.turn?.resolve();
        } else if (result.kind === 'approval_request') {
          // Dispatch WITHOUT blocking the reader (a later interrupt / stop must stay reachable).
          // `handleApproval` NEVER rejects — it catches a gate fault as a deny and always answers
          // the CLI — so this fire-and-forget raises no unhandled rejection.
          void this.handleApproval(cli, result.request);
        }
        // `ignore` → drop.
      }
    } finally {
      // The stdout ended — which looks IDENTICAL for a clean finish, a crash, and the kill
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
      //
      // This row is the most exposed: a ONE-SHOT `custom` child emits its `done` frame and
      // then exits non-zero, so the read loop's `finally` routinely runs with the turn
      // already released.
      const parked = this.turn !== undefined;
      // `catch`: a rejecting `exit` must not abort this `finally` before the turn is
      // released and the stream ended — that would reintroduce the very hang the exit
      // status exists to prevent. Nothing to report is the same answer as no fault.
      const exitFault = await cliExitFaultMessage(
        cli,
        'custom',
        parked ? 'mid-turn' : 'idle',
      ).catch(() => undefined);
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
            'custom CLI died while the session was idle',
          );
        }
      }
      // A one-shot CLI ends its turn by exiting, so settle any in-flight turn.
      this.turn?.resolve();
      if (!this.terminated) {
        this.terminated = true;
        this.endEventStream();
      }
    }
  }

  /**
   * Route an approval the CLI raised to the uniform transcript approval and write the spec's
   * decision frame back on the CLI's stdin. With no gate wired the harness auto-allows (the OS
   * sandbox + composed policies are the guardrail); a tool whose policy is `always_allow` auto-
   * approves with no signal; otherwise it emits the requires_action signal and PARKS the verdict.
   */
  private async handleApproval(
    cli: NativeCliProcess,
    request: CustomApprovalRequest,
  ): Promise<void> {
    const cancel = new AbortController();
    this.pendingApprovals.set(request.requestId, cancel);
    // A gate fault must NOT escape: this runs fire-and-forget, so a throw would become an
    // unhandled rejection (fatal to this process) AND skip the decision frame below, leaving the
    // CLI blocked on this approval for the rest of the session. So the decision is seeded
    // fail-closed and the frame is written from the `finally` — every path answers the CLI once.
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
      // nothing anywhere saying why. The spec's response frame carries a bare allow/deny
      // VALUE and no message field, so the LOG is the only channel that reason reaches; the
      // deny message is set too, matching the claude-code / cursor harnesses.
      const reason = err instanceof Error ? err.message : String(err);
      decision = {
        behavior: 'deny',
        message: `Tool use denied: the permission gate failed: ${reason}`,
      };
      this.opts.logger?.error?.(
        { err, sessionId: this.opts.sessionId, tool: request.toolName },
        'custom approval gate failed; denying the tool call',
      );
    } finally {
      this.pendingApprovals.delete(request.requestId);
      const approvals = this.spec?.approvals;
      // `approvals === undefined` is unreachable in practice: the spec parser fails fast on a
      // `stdout.approval_request` rule with no `approvals` block (they are one loop, declared
      // together — see `parseCustomAgentSpec`), so an approval frame can only reach here once
      // `approvals` is set. The guard narrows the optional `this.spec?.approvals` for the
      // write-back; it is not a behavioral fallback.
      if (approvals !== undefined) {
        try {
          const value =
            decision.behavior === 'allow' ? approvals.allow_value : approvals.deny_value;
          const frame = renderResponseFrame(approvals.response, request.requestId, value);
          cli.write(`${JSON.stringify(frame)}\n`);
        } catch {
          // The CLI's stdin is gone (it exited / was killed): there is nobody left to answer.
        }
      }
    }
  }

  /**
   * Resolve an approval to an allow/deny decision (the gate + policy logic). The parked verdict is
   * raced against the turn's abort (interrupt / stop) so it releases as a deny rather than stalling.
   */
  private async resolveApproval(
    request: CustomApprovalRequest,
    cancelSignal: AbortSignal,
  ): Promise<ToolPermissionDecision> {
    const toolPermissions: ToolPermissionResolver | undefined = this.input?.toolPermissions;
    const policy = toolPermissions ? toolPermissions.policyFor(request.toolName) : 'always_ask';
    if (policy === 'always_deny')
      return { behavior: 'deny', message: 'Tool use denied by policy.' };
    const confirmTool: ToolConfirmer | undefined = this.input?.confirmTool;
    if (confirmTool === undefined) {
      // No gate → the OS sandbox + policies are the guardrail; auto-allow.
      return { behavior: 'allow', updatedInput: request.input };
    }
    if (policy === 'always_allow') {
      return { behavior: 'allow', updatedInput: request.input };
    }
    const abortSignal = this.turn?.abort.signal;
    if (abortSignal?.aborted === true || cancelSignal.aborted) {
      return { behavior: 'deny', message: 'Tool use denied: turn aborted.' };
    }
    // Park on the human gate: emit the requires_action signal keyed by a per-approval id, then await
    // the verdict delivered through the gate.
    const toolUseId =
      request.requestId.length > 0 ? request.requestId : `custom_appr_${++this.seq}`;
    const signal: RequiresActionSignal = {
      action: 'tool_confirmation',
      tool_use_id: toolUseId,
      tool_name: request.toolName,
    };
    this.emit({ kind: 'agent.requires_action', id: toolUseId, payload: signal });
    const verdict = confirmTool(request.toolName, request.input, { toolUseId });
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

  /** Write the user turn to the CLI's stdin per the spec's stdin mode. */
  private writeUserTurn(spec: CustomAgentSpec, text: string): void {
    if (this.cli === undefined) {
      return;
    }
    if (spec.stdin.mode === 'json') {
      const frame = renderStdinTemplate(spec.stdin.template, text);
      this.cli.write(`${JSON.stringify(frame)}\n`);
      return;
    }
    // Raw mode: write the user text as one line.
    this.cli.write(`${text}\n`);
  }

  /** Build the launch input for {@link buildCustomLaunchConfig} from the boot context. */
  private buildLaunchInput(
    sandbox: SandboxHandle,
    input: SessionStartInput,
    spec: CustomAgentSpec,
    bridge: CustomBridgeCommand,
  ): Parameters<typeof buildCustomLaunchConfig>[0] {
    const model = effectiveModel(input.agentSnapshot.model_id, this.opts.modelDefault);
    const out: Parameters<typeof buildCustomLaunchConfig>[0] = {
      spec,
      bridge,
      sessionId: this.opts.sessionId,
      workspaceId: this.opts.workspaceId,
    };
    const root = sandboxRootDir(sandbox);
    if (root !== undefined) {
      out.sandboxRoot = root;
    }
    if (model !== undefined) {
      out.model = model;
    }
    const system = input.agentSnapshot.system;
    if (system !== undefined && system.length > 0) {
      out.system = system;
    }
    if (this.opts.cliCommand !== undefined) {
      out.cliPath = this.opts.cliCommand;
    }
    if (this.opts.cliPrefixArgs !== undefined) {
      out.prefixArgs = this.opts.cliPrefixArgs;
    }
    if (this.opts.cliExtraArgs !== undefined) {
      out.suffixArgs = this.opts.cliExtraArgs;
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
   * Build the `orca` MCP server command the CLI spawns: the bridge entrypoint pointed at the session
   * sandbox root (+ its tmux socket on the local-attach transport, + the skill-allowlist), so the
   * bridge REATTACHES to the same session tree.
   *
   * @throws Error when the sandbox handle exposes no `rootDir()` (a cloud-only handle needs a
   *   different bridge transport — out of scope for the root-dir-backed reusable runtimes).
   */
  private buildBridgeCommand(
    sandbox: SandboxHandle,
    input: SessionStartInput,
  ): CustomBridgeCommand {
    const args = [...(this.opts.bridgePrefixArgs ?? [resolveBridgeEntryPath()])];
    const root = sandboxRootDir(sandbox);
    if (root === undefined) {
      throw new Error(
        `custom provider requires a root-dir-backed sandbox (the native-CLI tool-bridge reattaches ` +
          `to an absolute host work-dir), but the session sandbox '${sandbox.id}' exposes no ` +
          `rootDir(); a cloud-only handle needs a different bridge transport.`,
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
  return terminalError(`custom CLI failed to launch: ${err.message}`);
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
 * Render the stdin JSON template for a user turn: deep-copy the template, substituting `{userText}`
 * in any string field with the turn's text. A non-string field is preserved verbatim.
 */
function renderStdinTemplate(
  template: Record<string, unknown>,
  text: string,
): Record<string, unknown> {
  return substituteDeep(template, { userText: text }) as Record<string, unknown>;
}

/**
 * Render the approval response frame: deep-copy the template, substituting `{requestId}` and
 * `{decision}` in any string field.
 */
function renderResponseFrame(
  template: Record<string, unknown>,
  requestId: string,
  decision: string,
): Record<string, unknown> {
  return substituteDeep(template, { requestId, decision }) as Record<string, unknown>;
}

/** Deep-substitute `{token}` occurrences in every string leaf of `value` from `subs`. */
function substituteDeep(value: unknown, subs: Record<string, string>): unknown {
  if (typeof value === 'string') {
    return value.replace(/\{([a-zA-Z0-9_]+)\}/g, (match, token: string) =>
      Object.prototype.hasOwnProperty.call(subs, token) ? (subs[token] as string) : match,
    );
  }
  if (Array.isArray(value)) {
    return value.map((v) => substituteDeep(v, subs));
  }
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [key, v] of Object.entries(value as Record<string, unknown>)) {
      out[key] = substituteDeep(v, subs);
    }
    return out;
  }
  return value;
}

/**
 * Resolve the runnable bridge entrypoint path — the module the CLI launches as `node <path>
 * --root …`. It sits at a KNOWN location relative to this harness module in both build layouts (a
 * built runner emits `dist/harness/custom/bridge-entry.js`; a source layout keeps the sibling
 * `./bridge-entry.js`). Falls back to the sibling path. Production + tests never rely on the
 * fallback — the built entry exists, and tests inject the entry path explicitly.
 */
function resolveBridgeEntryPath(): string {
  const sibling = new URL('./bridge-entry.js', import.meta.url);
  const bundled = new URL('./harness/custom/bridge-entry.js', import.meta.url);
  for (const candidate of [bundled, sibling]) {
    const path = fileURLToPath(candidate);
    if (existsSync(path)) {
      return path;
    }
  }
  return fileURLToPath(sibling);
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
