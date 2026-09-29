// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `claude-code` native-CLI harness — boots the headless `claude` binary and drives
// it over its stream-json stdio, as an Orca {@link AgentHarness}.
//
// This is the real harness the runner constructs from a `provider: "claude-code"`
// snapshot and drives per turn. Unlike the in-process claude providers (which run the
// model inside the runner via the Agent SDK), this boots the `claude` BINARY as a
// long-lived child INSIDE the per-session {@link SandboxHandle} — through the 4a
// native-CLI launcher ({@link launchNativeCli}) — and speaks its newline-delimited-JSON
// ("stream-json") protocol:
//
//   start(input)     → resolve the sandbox + wire the native-CLI tool-bridge as the
//                      CLI's `orca` MCP server (so the model's built-ins resolve inside
//                      the sandbox), build the headless launch argv (see `launch-args.ts`),
//                      boot the CLI, and start the background reader that normalizes its
//                      stream-json stdout into Orca-native events.
//   submit(user.msg) → write a stream-json `user` frame to the CLI's stdin, then await
//                      this turn's `result` frame (the turn boundary), and emit a terminal
//                      `agent.turn_completed`. `submit` RESOLVES only once the turn's
//                      events are all emitted — honoring the runner's turn contract.
//   interrupt()      → send an `interrupt` control_request + abort the in-flight turn
//                      WITHOUT ending the harness (the next `submit` drives a fresh turn).
//   stop(reason)     → kill the CLI child and end `events()`.
//   events()         → the single stream of emitted agent events.
//
// Approval routing: when the runner wired a confirmation gate, the CLI is launched with
// `--permission-prompt-tool stdio`, so it raises a `can_use_tool` control_request over
// stream-json for each gated tool call. The reader routes each to the uniform transcript
// approval — a tool whose policy is `always_ask` (or fail-closed with no policy) emits an
// Anthropic-native `agent.requires_action` signal then PARKS the verdict keyed by the
// call's `tool_use_id`; a tool whose policy is `always_allow` auto-approves with no human
// gate — and writes the decision back as a `control_response` on the CLI's stdin. With no
// gate the CLI is launched with `--dangerously-skip-permissions` (the OS sandbox +
// policies are the guardrail).
//
// Emits Orca-native Anthropic-shaped {@link AgentEvent}s ONLY (the SAME shapes the
// in-process providers emit) — the stream-json normalization reuses the shared
// {@link SdkMessageMapper}.

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
  type NativeCliProcess,
} from '../../sandbox/native-cli-launcher.js';
import { buildClaudeCodeLaunchConfig, type ClaudeCodeBridgeCommand } from './launch-args.js';
import { ClaudeCodeStreamNormalizer, type PermissionRequest } from './stream-json.js';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import {
  BRIDGE_ROOT_FLAG,
  BRIDGE_TMUX_SOCKET_FLAG,
  pushAllowedToolsFlag,
} from './bridge-runtime.js';

/**
 * The Anthropic-native `agent.requires_action` signal payload emitted when a tool call
 * parks on the human gate: the action kind, the in-flight call's `tool_use_id` (the
 * verdict's routing key), and the tool name. Mirrors the in-process providers' signal
 * exactly, so a client renders a native-CLI turn's tool-confirmation identically.
 */
export interface RequiresActionSignal {
  action: 'tool_confirmation';
  tool_use_id: string;
  tool_name: string;
}

/** Construction options for {@link ClaudeCodeCliHarness}. */
export interface ClaudeCodeHarnessOptions {
  /** Default model id when the snapshot did not pin one. */
  modelDefault: string;
  /** Owning workspace (tenant scope). */
  workspaceId: string;
  /** Orca session id this harness serves. */
  sessionId: string;
  /**
   * The per-session {@link SandboxHandle} the provider captured from the session context
   * ({@link ProviderSessionContext.sandbox}), used as a FALLBACK when a `start` input
   * omits its own `sandbox`. A native-CLI harness REQUIRES a sandbox (it launches a real
   * child + wires the tool-bridge to the session tree), so when neither the start input
   * nor this carries one, `start` fails fast rather than silently running unsandboxed.
   */
  sandbox?: SandboxHandle;
  /** The scoped session JWT forwarded to the CLI as `ANTHROPIC_API_KEY` (credential-free). */
  apiKey?: string;
  /** The gateway LLM-proxy base URL forwarded to the CLI as `ANTHROPIC_BASE_URL`. */
  baseURL?: string;
  /**
   * The CLI binary path/name. Defaults to `claude` on the sandbox PATH; a test injects a
   * fake CLI here (e.g. `node`) with {@link cliPrefixArgs} carrying the script path.
   */
  cliCommand?: string;
  /** Prefix args before the generated flags (e.g. a fake CLI script path under `node`). */
  cliPrefixArgs?: string[];
  /** Extra suffix args after the generated flags (e.g. a test probe flag). */
  cliExtraArgs?: string[];
  /**
   * The bridge entrypoint executable the CLI spawns as its `orca` MCP server. Defaults to
   * the current Node executable; a test injects `tsx` here (with {@link bridgePrefixArgs}
   * carrying the TS entry path) so the CLI child speaks MCP to the TS source.
   */
  bridgeCommand?: string;
  /** Prefix args before the bridge entry's own flags (the bridge entry module path). */
  bridgePrefixArgs?: string[];
  /**
   * The native-CLI launcher (injected for tests). Defaults to the real
   * {@link launchNativeCli}. A test can substitute a fake to avoid spawning a subprocess.
   */
  launch?: typeof launchNativeCli;
  /**
   * How long a turn waits for the CLI's terminal `result` frame before it is ended with a
   * timeout error. Defaults to {@link DEFAULT_TURN_DEADLINE_MS}. Bounded so a CLI that
   * accepted the turn frame and went MUTE cannot park the turn forever; a test shortens it.
   */
  turnTimeoutMs?: number;
}

/** The in-flight turn's state: a resolver fired when the CLI's `result` frame lands. */
interface TurnState {
  resolve: () => void;
  /**
   * The turn's abort signal — fired by {@link ClaudeCodeCliHarness.interrupt} /
   * {@link ClaudeCodeCliHarness.stop}. A tool permission parked on the human gate races
   * this so an interrupted / stopped turn releases the parked gate as a DENY (writing the
   * deny `control_response` back to the CLI) rather than stalling the reader.
   */
  abort: AbortController;
}

/**
 * Native-CLI harness backed by the headless `claude` binary. One per session; the runner
 * `start`s it from the snapshot then `submit`s each user turn against the SAME live CLI.
 */
export class ClaudeCodeCliHarness implements AgentHarness {
  private readonly launch: typeof launchNativeCli;
  private input: SessionStartInput | undefined;
  private sandbox: SandboxHandle | undefined;
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
   * In-flight permission parks, keyed by their `request_id` (the CLI's routing key).
   * A `can_use_tool` request that parks on the human gate registers its
   * {@link AbortController} here for the lifetime of the park; a
   * `control_cancel_request` naming that id aborts it (releasing the park as a deny),
   * and teardown aborts every entry. The entry is removed once the park resolves.
   * Bounded by the number of concurrently-parked tool calls in a turn.
   */
  private readonly pendingPermissions = new Map<string, AbortController>();
  /** Per-session monotonically increasing counter for synthesized ids. */
  private seq = 0;
  /** Bound on how long one turn may park on the CLI's terminal frame (see {@link submit}). */
  private readonly turnTimeoutMs: number;

  constructor(private readonly opts: ClaudeCodeHarnessOptions) {
    this.launch = opts.launch ?? launchNativeCli;
    this.turnTimeoutMs = opts.turnTimeoutMs ?? DEFAULT_TURN_DEADLINE_MS;
  }

  async start(input: SessionStartInput): Promise<void> {
    this.input = input;
    this.terminated = false;
    // A native-CLI harness must have a sandbox: it launches a real child and binds the
    // tool-bridge to the session tree. Fail fast rather than run unsandboxed.
    const sandbox = input.sandbox ?? this.opts.sandbox;
    if (sandbox === undefined) {
      throw new Error('claude-code provider requires a per-session sandbox but none was wired');
    }
    this.sandbox = sandbox;

    const config = buildClaudeCodeLaunchConfig(this.buildLaunchInput(sandbox, input));
    // Boot the CLI inside the sandbox and start reading its stream-json stdout.
    this.cli = this.launch(sandbox, config);
    this.readerDone = this.readLoop(this.cli);
  }

  async submit(event: UserEvent): Promise<void> {
    if (this.terminated) {
      return;
    }
    // `user.interrupt` is not a turn: it aborts the in-flight turn (delegated to
    // {@link interrupt}); production drives it out-of-band via `interrupt`.
    if (event.kind === 'user.interrupt') {
      this.interrupt();
      return;
    }
    if (event.kind !== 'user.message') {
      return;
    }
    const text = textOf(event.payload);
    if (text === null) {
      return;
    }
    const cli = this.cli;
    if (cli === undefined) {
      return;
    }

    // Drive the turn: write the stream-json user frame, then await this turn's `result`.
    let turn!: TurnState;
    const turnDone = new Promise<void>((resolve) => {
      turn = { resolve, abort: new AbortController() };
      this.turn = turn;
    });
    cli.write(`${JSON.stringify(userFrame(text))}\n`);
    // BOUNDED (see DEFAULT_TURN_DEADLINE_MS): a CLI that took the frame and went MUTE says
    // nothing on any channel, so an unbounded park here never ends — `submit` never
    // resolves, the loop parks on both legs of its turn race, and the consumer never gets a
    // marker. On expiry, name the timeout on the wire FIRST (a turn released with nothing
    // said reaches the client as a bare `agent.turn_completed` — a wedged CLI recorded as a
    // successful, empty answer), then TEAR THE CHILD DOWN — a deadline that ends the turn
    // and leaves the CLI alive desynchronizes the two for good — and only then release the
    // park.
    await awaitTurnWithinDeadline(turnDone, this.turnTimeoutMs, () => {
      // `this.turn !== turn` means the turn already ended by another path (the reader's
      // `result` frame, an interrupt, teardown) and this deadline lost the race.
      if (this.turn !== turn) {
        return;
      }
      this.emit(terminalError(turnDeadlineFaultMessage('claude-code', this.turnTimeoutMs)));
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
    // runner's turn boundary fires (mirrors the in-process providers' turn completion).
    this.emit({ kind: 'agent.turn_completed', payload: {} });
    // `submit` resolves here — after the turn's events were emitted.
  }

  interrupt(): void {
    if (this.terminated) {
      return;
    }
    const turn = this.turn;
    if (turn === undefined) {
      return; // no turn in flight — idempotent no-op.
    }
    // Fire the turn's abort so a tool permission parked on the human gate releases as a
    // DENY (the reader writes the deny `control_response` back to the CLI) rather than
    // stalling — mirrors the in-process providers threading the abort into `canUseTool`.
    turn.abort.abort();
    // Best-effort: ask the CLI to interrupt the in-flight model turn over the control
    // channel. The CLI answers the in-flight turn (releasing any parked permission on its
    // side); the reader then observes the `result` and settles `submit`.
    this.cli?.write(`${JSON.stringify(interruptFrame(`int_${++this.seq}`))}\n`);
    // Also settle the turn locally in case the CLI does not emit a fresh `result` (e.g. a
    // fake that only closes): resolving the parked promise unwinds the blocked `submit`.
    turn.resolve();
  }

  async stop(reason: TerminationReason): Promise<void> {
    void reason; // Informational; the runner logs it.
    this.terminated = true;
    // Fire the in-flight turn's abort (releases a parked tool gate as a deny) and release
    // the turn so a blocked `submit` unwinds.
    this.turn?.abort.abort();
    this.turn?.resolve();
    this.turn = undefined;
    // Abort every in-flight permission park so a fire-and-forget `handlePermission`
    // awaiting the human gate releases as a deny instead of lingering past teardown (its
    // own `finally` removes the entry). Snapshot the values first — aborting resolves the
    // park, which deletes its entry, mutating the map mid-iteration.
    for (const cancel of [...this.pendingPermissions.values()]) {
      cancel.abort();
    }
    // Kill the CLI child (ends its stdout → the reader loop completes) and end the stream.
    this.cli?.kill();
    this.cli = undefined;
    this.endEventStream();
    // Let the reader loop settle so a lingering iteration does not emit after teardown.
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
   * The background reader: consume the CLI's stream-json stdout LINES through the
   * normalizer, emitting agent events and driving the control protocol. Each line is
   * parsed then dispatched:
   *   - `events` → emit them;
   *   - `turn_end` → emit its usage event (if any) then settle the in-flight turn;
   *   - `permission_request` → route to the gate (NON-blocking, so a later cancel is
   *     reachable), writing the `control_response` on stdin when it resolves;
   *   - `permission_cancel` → abort the matching parked gate (releases it as a deny);
   *   - `ignore` → drop.
   * A parse failure (a non-JSON line) is skipped — a malformed line must not wedge the
   * reader. The loop completes when the CLI's stdout ends (exit / kill), at which point it
   * ends the event stream so a direct consumer is not stranded.
   * On the way out it reports an ABNORMAL exit as a terminal `agent.error` (a non-zero code,
   * or a signal this harness did not send) — stdout ending looks the same for a clean finish
   * and for a crash, so without that a dead CLI ends the turn as an empty success.
   */
  private async readLoop(cli: NativeCliProcess): Promise<void> {
    const normalizer = new ClaudeCodeStreamNormalizer();
    try {
      const failure = await cli.failure;
      if (failure !== undefined) {
        // The CLI never started — a missing / misnamed `claude` on the sandbox PATH is the
        // likeliest production misconfiguration. Its stdout is empty, so falling through
        // would end the turn as a clean, silent no-op. Put the launch failure on the wire
        // as a terminal `agent.error` (the established way a fault reaches the transcript)
        // and drop into the `finally`, which releases the parked turn and ends the stream.
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
        const result = normalizer.map(parsed);
        if (result.kind === 'events') {
          for (const ev of result.events) {
            this.emit(ev);
          }
        } else if (result.kind === 'turn_end') {
          for (const ev of result.events) {
            this.emit(ev);
          }
          // End the current turn — its events are all emitted.
          this.turn?.resolve();
        } else if (result.kind === 'permission_request') {
          // Dispatch WITHOUT blocking the reader: the CLI gates the tool's result frame
          // on our `control_response`, but a `control_cancel_request` withdrawing this
          // very request can arrive on stdout while the gate is parked — awaiting here
          // would wedge the reader and make that cancel unreachable. `handlePermission`
          // NEVER rejects — it catches a gate fault as a deny and always answers the CLI —
          // so this fire-and-forget cannot raise an unhandled rejection.
          void this.handlePermission(cli, result.request);
        } else if (result.kind === 'permission_cancel') {
          // The CLI withdrew an earlier request: abort its parked gate so it releases as
          // a deny (the park's tail writes the deny `control_response` back). A cancel for
          // an unknown / already-resolved id is a harmless no-op.
          this.pendingPermissions.get(result.cancel.requestId)?.abort();
        }
        // `ignore` → drop.
      }
    } finally {
      // The CLI's stdout ended — which looks IDENTICAL for a clean finish, a crash, and the
      // kill `stop()` issues. Ask the exit status which it was, and when it was a CRASH put
      // that on the wire FIRST: after `endEventStream()` below the stream is closed and an
      // emitted event would never be read, and a turn released with nothing said reaches the
      // client as a bare `agent.turn_completed` — a dead CLI recorded as a successful, empty
      // answer. Skipped once terminated: teardown killed the child, so the exit is one we
      // asked for (and the turn was already released by `stop`).
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
      const exitFault = await cliExitFaultMessage(
        cli,
        'claude-code',
        parked ? 'mid-turn' : 'idle',
      ).catch(() => undefined);
      // `terminated` is re-read AFTER the grace: `stop()` can land inside that await, and it
      // drains every event resolver — an error emitted past it is queued behind a returned
      // generator and never read.
      if (exitFault !== undefined && !this.terminated) {
        this.emit(parked ? terminalError(exitFault) : diagnosticError(exitFault));
      }
      // Release any parked turn and end the stream.
      this.turn?.resolve();
      if (!this.terminated) {
        this.terminated = true;
        this.endEventStream();
      }
    }
  }

  /**
   * Route a `can_use_tool` control_request to the uniform transcript approval and write
   * the decision back as a `control_response` on the CLI's stdin.
   *
   * With no confirmation gate wired the CLI was launched with skip-permissions and never
   * raises this frame; if it somehow does, the harness auto-allows (there is no gate to
   * park on). With a gate: the per-tool policy decides — `always_allow` auto-approves with
   * no signal; `always_ask` (or fail-closed with no resolver) emits the Anthropic-native
   * `agent.requires_action` signal keyed by the call's `tool_use_id`, then PARKS the
   * verdict through the gate. The reply is the exact Anthropic `PermissionResult` shape the
   * CLI expects inside `control_response.response.response`.
   */
  private async handlePermission(cli: NativeCliProcess, request: PermissionRequest): Promise<void> {
    // Register a per-request abort BEFORE resolving so a `control_cancel_request` (or
    // teardown) that lands mid-park can release this exact gate as a deny. Removed in the
    // `finally` regardless of how the park resolved, so the map only holds live parks.
    const cancel = new AbortController();
    this.pendingPermissions.set(request.requestId, cancel);
    // A gate fault must NOT escape: this runs fire-and-forget, so a throw would become an
    // unhandled rejection (fatal to this process) AND skip the `control_response` below,
    // leaving the CLI blocked on this permission for the rest of the session. So the
    // decision is seeded fail-closed, a throw is caught as a DENY carrying the reason, and
    // the response frame is written from the `finally` — every path answers the CLI once.
    let decision: ToolPermissionDecision = {
      behavior: 'deny',
      message: 'Tool use denied: the permission gate failed.',
    };
    try {
      decision = await this.resolvePermission(request, cancel.signal);
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      decision = {
        behavior: 'deny',
        message: `Tool use denied: the permission gate failed: ${reason}`,
      };
    } finally {
      this.pendingPermissions.delete(request.requestId);
      try {
        const response = permissionResultOf(decision, request.input);
        cli.write(`${JSON.stringify(controlResponseFrame(request.requestId, response))}\n`);
      } catch {
        // The CLI's stdin is gone (it exited / was killed): there is nobody left to answer.
      }
    }
  }

  /**
   * Resolve a permission request to an allow/deny decision (the gate + policy logic).
   *
   * The parked verdict is raced against BOTH the turn's abort (interrupt / stop) AND
   * this request's own cancel signal (a `control_cancel_request` the CLI raised to
   * withdraw the request, or teardown), so either releases the gate as a deny rather
   * than stalling. All abort listeners are removed once the race settles.
   */
  private async resolvePermission(
    request: PermissionRequest,
    cancelSignal: AbortSignal,
  ): Promise<ToolPermissionDecision> {
    const toolPermissions: ToolPermissionResolver | undefined = this.input?.toolPermissions;
    const policy = toolPermissions ? toolPermissions.policyFor(request.toolName) : 'always_ask';
    if (policy === 'always_deny')
      return { behavior: 'deny', message: 'Tool use denied by policy.' };
    const confirmTool: ToolConfirmer | undefined = this.input?.confirmTool;
    if (confirmTool === undefined) {
      // No gate → the CLI was launched with skip-permissions; auto-allow defensively.
      return { behavior: 'allow', updatedInput: request.input };
    }
    if (policy === 'always_allow') {
      return { behavior: 'allow', updatedInput: request.input };
    }
    // If the turn is already aborted (interrupt / stop landed before the park) or the
    // request was already withdrawn, deny immediately — do not emit a requires_action a
    // client can never satisfy.
    const abortSignal = this.turn?.abort.signal;
    if (abortSignal?.aborted === true || cancelSignal.aborted) {
      return { behavior: 'deny', message: 'Tool use denied: turn aborted.' };
    }
    // Park on the human gate: emit the requires_action signal keyed by tool_use_id, then
    // await the verdict delivered as a `user.tool_confirmation` (resolved through the gate).
    const signal: RequiresActionSignal = {
      action: 'tool_confirmation',
      tool_use_id: request.toolUseId,
      tool_name: request.toolName,
    };
    this.emit({ kind: 'agent.requires_action', id: request.toolUseId, payload: signal });
    const verdict = confirmTool(request.toolName, request.input, { toolUseId: request.toolUseId });
    // Race the parked verdict against the turn's abort AND this request's cancel so an
    // interrupt / stop OR a CLI-side withdrawal mid-park releases the gate as a deny (the
    // park's tail then writes the deny back to the CLI). Both listeners are removed
    // regardless of which side wins.
    const races: Array<Promise<ToolPermissionDecision>> = [verdict];
    const cleanups: Array<() => void> = [];
    for (const [sig, message] of [
      [abortSignal, 'Tool use denied: turn aborted.'],
      [cancelSignal, 'Tool use denied: request cancelled.'],
    ] as const) {
      if (sig === undefined) {
        continue;
      }
      let onAbort: (() => void) | undefined;
      races.push(
        new Promise<ToolPermissionDecision>((resolve) => {
          onAbort = (): void => resolve({ behavior: 'deny', message });
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

  /** Build the launch input for {@link buildClaudeCodeLaunchConfig} from the boot context. */
  private buildLaunchInput(
    sandbox: SandboxHandle,
    input: SessionStartInput,
  ): Parameters<typeof buildClaudeCodeLaunchConfig>[0] {
    const model = input.agentSnapshot.model_id;
    const out: Parameters<typeof buildClaudeCodeLaunchConfig>[0] = {
      bridge: this.buildBridgeCommand(sandbox, input),
      // A gate is wired iff the runner supplied `confirmTool` (the loop binds it from the
      // snapshot's permission policy). Gate → route permissions over stdio; no gate → skip.
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
    // The skills plugin-dir is a PER-SESSION snapshot value (like `system` +
    // `allowed_tool_names`), not a boot-time dep — it rides the start input's
    // `agentSnapshot`, projected there by `buildSessionStartInput` from the
    // snapshot's `skills_plugin_dir`. Surface it as `--plugin-dir` so the CLI
    // discovers the bundled skills natively; omitted when the snapshot staged none.
    const pluginDir = input.agentSnapshot.skills_plugin_dir;
    if (pluginDir !== undefined && pluginDir.length > 0) {
      out.pluginDir = pluginDir;
    }
    if (input.agentSnapshot.system !== undefined) {
      out.system = input.agentSnapshot.system;
    }
    if (this.opts.baseURL !== undefined) {
      out.baseURL = this.opts.baseURL;
    }
    if (this.opts.apiKey !== undefined) {
      out.apiKey = this.opts.apiKey;
    }
    // Do NOT set `cwd`: the native-CLI launcher's `cwd` is SANDBOX-RELATIVE (the runtime
    // re-anchors it under the session root), so the CLI already runs at the session root
    // when `cwd` is omitted. Passing the host-side root here would be re-anchored a second
    // time into a non-existent nested path. The bridge is pointed at the real root via
    // `--root` (an absolute host path the bridge process resolves directly), which is the
    // correct place for the absolute path.
    return out;
  }

  /**
   * Build the `orca` MCP server command the CLI spawns: the bridge entrypoint pointed at
   * the session sandbox root (+ its tmux socket on the local-attach transport, + the
   * skill-allowlist), so the bridge REATTACHES to the same session tree. The root is read
   * from the handle's `rootDir()` (present on the reusable runtimes' handles).
   *
   * A handle with NO root — a cloud-only handle whose work-dir is not a host path the
   * bridge process can reach — is unsupported by this bridge transport: the bridge child
   * reattaches by binding `node:fs`/`child_process` to an absolute host root, which a
   * cloud handle does not expose. Rather than emit a `--root`-less bridge command that the
   * bridge entrypoint would reject deep in the CLI child with an opaque error, FAIL FAST
   * here with an actionable one — a cloud target needs a different bridge transport (out
   * of scope for the root-dir-backed reusable runtimes this provider serves).
   *
   * @throws Error when the sandbox handle exposes no `rootDir()`.
   */
  private buildBridgeCommand(
    sandbox: SandboxHandle,
    input: SessionStartInput,
  ): ClaudeCodeBridgeCommand {
    // Default the bridge entry to `node <this package's bridge-entry.js>` (resolved from
    // this module's own location — see {@link resolveBridgeEntryPath}), so a production
    // session needs no extra wiring; a test injects `tsx` + the `.ts` entry path instead.
    const args = [...(this.opts.bridgePrefixArgs ?? [resolveBridgeEntryPath()])];
    const root = sandboxRootDir(sandbox);
    if (root === undefined) {
      throw new Error(
        `claude-code provider requires a root-dir-backed sandbox (the native-CLI ` +
          `tool-bridge reattaches to an absolute host work-dir), but the session ` +
          `sandbox '${sandbox.id}' exposes no rootDir(); a cloud-only handle needs a ` +
          `different bridge transport.`,
      );
    }
    args.push(BRIDGE_ROOT_FLAG, root);
    const socket = tmuxSocketOf(sandbox);
    if (socket !== undefined) {
      args.push(BRIDGE_TMUX_SOCKET_FLAG, socket);
    }
    // `[]` is DENY-ALL, not "no restriction" — the same meaning the claude
    // provider's `buildOrcaSdkTools` gives it. `pushAllowedToolsFlag` emits the
    // flag with an EMPTY value so the bridge can tell a present-but-empty
    // allowlist from an absent one; the length guard this replaced dropped the
    // flag and handed an mcp-only agent (whose composition expands to `[]`) the
    // full orca tool surface.
    pushAllowedToolsFlag(args, input.agentSnapshot.allowed_tool_names);
    return {
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
  return terminalError(`claude-code CLI failed to launch: ${err.message}`);
}

/**
 * Resolve the runnable bridge entrypoint path — the module the CLI launches as
 * `node <path> --root …`. It sits at a KNOWN location relative to this harness module in
 * both build layouts, so we resolve it from this module's own URL and pick the first
 * candidate that exists on disk:
 *   - a BUILT runner bundles this module into `dist/main.js` and emits the entry at
 *     `dist/harness/claude-code/bridge-entry.js` (candidate `./harness/claude-code/…`);
 *   - a source/dev layout keeps this module at `src/harness/claude-code/index.ts` with the
 *     entry as its sibling `./bridge-entry.js`.
 * Falls back to the sibling path when neither is found on disk (so the value is always a
 * concrete path). Production + tests never rely on the fallback — the built entry exists,
 * and tests inject the entry path explicitly.
 */
function resolveBridgeEntryPath(): string {
  const sibling = new URL('./bridge-entry.js', import.meta.url);
  const bundled = new URL('./harness/claude-code/bridge-entry.js', import.meta.url);
  for (const candidate of [bundled, sibling]) {
    const path = fileURLToPath(candidate);
    if (existsSync(path)) {
      return path;
    }
  }
  return fileURLToPath(sibling);
}

/** The stream-json `user` frame written to the CLI's stdin to drive one turn. */
function userFrame(text: string): Record<string, unknown> {
  return {
    type: 'user',
    session_id: '',
    message: { role: 'user', content: [{ type: 'text', text }] },
  };
}

/** The stream-json `control_request` interrupt frame. */
function interruptFrame(requestId: string): Record<string, unknown> {
  return { type: 'control_request', request_id: requestId, request: { subtype: 'interrupt' } };
}

/** The stream-json `control_response` frame carrying a permission result for `requestId`. */
function controlResponseFrame(
  requestId: string,
  response: Record<string, unknown>,
): Record<string, unknown> {
  return {
    type: 'control_response',
    response: { subtype: 'success', request_id: requestId, response },
  };
}

/**
 * Map a runner {@link ToolPermissionDecision} to the Anthropic `PermissionResult` the CLI
 * expects inside `control_response.response.response`. Allow carries the (possibly
 * updated) input; deny carries the reason message. A deny with no message gets a default.
 */
function permissionResultOf(
  decision: ToolPermissionDecision,
  proposedInput: Record<string, unknown>,
): Record<string, unknown> {
  if (decision.behavior === 'allow') {
    return { behavior: 'allow', updatedInput: decision.updatedInput ?? proposedInput };
  }
  return { behavior: 'deny', message: decision.message || 'Tool use denied.' };
}

/**
 * The sandbox's host-side work-dir root, or `undefined` when the handle does not expose
 * one. The reusable runtimes' handles (InMemory / Local / tmux) expose a `rootDir()`
 * accessor; duck-typed so the sandbox boundary type stays minimal and cloud-friendly.
 */
function sandboxRootDir(sandbox: SandboxHandle): string | undefined {
  const rootDir = (sandbox as { rootDir?: unknown }).rootDir;
  if (typeof rootDir !== 'function') {
    return undefined;
  }
  const value = (rootDir as () => unknown).call(sandbox);
  return typeof value === 'string' ? value : undefined;
}

/**
 * The tmux socket path of a tmux-backed handle, or `undefined` for a non-tmux handle. The
 * tmux handle keeps its socket under `<root>/.orca-tmux/server.sock`; the presence of that
 * layout is inferred from the handle being a {@link TerminalHost} (only the tmux handle is)
 * AND exposing a root, so the bridge child can reattach to the same tmux server. Duck-typed
 * so no tmux-specific type leaks into the harness.
 */
function tmuxSocketOf(sandbox: SandboxHandle): string | undefined {
  // Only the tmux-backed handle carries the pane-management (`launchTerminal`) surface; a
  // non-terminal handle (InMemory / cloud) has no tmux socket.
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
