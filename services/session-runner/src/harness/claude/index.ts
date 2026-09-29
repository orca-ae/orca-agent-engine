// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The default `claude` provider: a chat-only {@link AgentHarness} backed by the
// Claude Agent SDK's `query()` loop.
//
// This is the real harness the runner constructs from a `provider: "claude"`
// snapshot and drives per turn. It mirrors the harness-server's
// `ClaudeAgentSdkHarness` (the cross-component contract is identical), scoped to the
// chat-only surface the runner's turn loop needs:
//
//   start(input)     → captures the agent snapshot (model / system) for the turn.
//   submit(user.msg) → calls the SDK `query()` with `sessionStore` = our adapter
//                      and `sessionId` = the derived UUID; the SDK reloads prior
//                      history via the adapter and persists new entries through it.
//                      Each assistant message is forwarded as an `agent.message`
//                      event; the init frame as `system`; the result frame's usage
//                      as `agent.usage` (preceded by an `agent.error` when the result
//                      reports a failure). `submit` RESOLVES only once the `query()`
//                      loop drains — honoring the runner's "end the turn stream when
//                      submit resolves" contract (the loop emits into the event
//                      queue synchronously as it runs).
//   stop(reason)     → aborts any in-flight `query()` and ends `events()`.
//   events()         → the single stream of emitted agent events.
//
// LLM egress is credential-free: the factory reads the gateway egress block from
// the snapshot and passes the gateway LLM base URL + scoped session JWT here; the
// harness forwards them to the SDK as `ANTHROPIC_BASE_URL` / `ANTHROPIC_API_KEY`
// (the gateway swaps the JWT for the real upstream credential). The harness never
// holds an upstream secret.
//
// Beyond the chat-only forwarding, this lean provider streams the SDK's richer
// output into Orca-native events (see `sdk-message-mapper.ts`): extended-thinking as
// Anthropic-native `thinking` content blocks (streamed live + settled), tool calls /
// results paired by `tool_use_id`, and — when the runner wired a confirmation gate —
// it routes each tool call through the SDK's `canUseTool`, emitting an
// `agent.requires_action` signal and PARKING the verdict on the uniform transcript
// approval for tools whose permission policy is `always_ask` (a tool whose policy is
// `always_allow` proceeds with no human gate; with no policy at all the harness is
// fail-closed and parks every tool). It also exposes `interrupt()` — the out-of-band
// `user.interrupt` that aborts the in-flight turn while keeping the harness alive for
// the next turn — and force-closes the SDK query on interrupt / stop / fault so a
// torn-down turn never leaks the CLI subprocess. A fault that kills the turn emits a
// terminal `agent.error` before it unwinds, so the reason reaches the wire (and the
// durable transcript) rather than only the caller's rejection. The turn model stays
// stateless one-shot `query()` + session-store reload per turn.

import { v5 as uuidv5 } from 'uuid';
import {
  query as realQuery,
  type CanUseTool,
  type McpSdkServerConfigWithInstance,
  type Options,
  type PermissionResult,
  type SDKMessage,
} from '@anthropic-ai/claude-agent-sdk';
import type {
  AgentEvent,
  AgentHarness,
  SessionStartInput,
  TerminationReason,
  ToolConfirmer,
  ToolPermissionResolver,
  UserEvent,
} from '../agent-harness.js';
import { terminalError } from '../agent-harness.js';
import type { SandboxHandle } from '../../sandbox/seam.js';
import type { ClaudeAgentSdkAdapter } from './session-adapter.js';
import { SdkMessageMapper } from './sdk-message-mapper.js';
import {
  buildDelegateOnlySdkMcpServer,
  buildOrcaSdkMcpServer,
  buildSandboxSdkOptions,
} from './mcp-tools.js';

/**
 * The SDK `query()` call, narrowed to the surface the harness uses (a prompt + the
 * options, returning an async-iterable of SDK messages). Injected so the harness is
 * unit-testable against scripted SDK messages; defaults to the real SDK `query`.
 *
 * The real SDK returns a `Query` — an async generator that ALSO exposes `close()`
 * (force-terminate the CLI subprocess + clean up resources). The harness probes for
 * that method at teardown ({@link ClaudeAgentSdkHarness.stop}) and calls it when
 * present, so the return type is the iterable plus an OPTIONAL `close`; a scripted
 * fake omits it and the harness degrades to the abort-only path.
 */
export type ClaudeQueryHandle = AsyncIterable<SDKMessage> & { close?: () => void };

export type ClaudeQuery = (args: { prompt: string; options: Options }) => ClaudeQueryHandle;

/**
 * The Anthropic-native `agent.requires_action` signal payload emitted when a tool
 * call parks on the human gate: the action kind, the in-flight call's `tool_use_id`
 * (the verdict's routing key), and the tool name. The verdict is delivered
 * out-of-band as a `user.tool_confirmation`, which the loop resolves through the
 * parked gate (allow → proceed; deny → a clean denial).
 */
export interface RequiresActionSignal {
  action: 'tool_confirmation';
  tool_use_id: string;
  tool_name: string;
}

/**
 * The SDK's `Options.sessionId` must be a UUID; orca session ids are `ses_<base32>`.
 * Derive a deterministic UUID v5 from the orca id (collision-resistant under a fixed
 * namespace) so every turn for the same session resolves to the same SDK session id
 * and the adapter can map it back to the orca id. Matches the harness-server.
 */
const ORCA_CLAUDE_SESSION_NAMESPACE = 'ac15a075-8075-494b-9262-ae6a650ec031';

function deriveClaudeSessionId(orcaSessionId: string): string {
  return uuidv5(orcaSessionId, ORCA_CLAUDE_SESSION_NAMESPACE);
}

/** Construction options for {@link ClaudeAgentSdkHarness}. */
export interface ClaudeHarnessOptions {
  /**
   * The scoped session JWT the gateway swaps for the real upstream credential,
   * forwarded to the SDK as `ANTHROPIC_API_KEY`. Empty when no LLM gateway is
   * configured (the SDK then falls back to its own ambient credential resolution).
   */
  apiKey: string;
  /**
   * The gateway LLM-proxy base URL, forwarded to the SDK as `ANTHROPIC_BASE_URL`.
   * Absent when no LLM gateway is configured.
   */
  baseURL?: string;
  /** Default model id when the snapshot did not pin one. */
  modelDefault: string;
  /** SDK `SessionStore` adapter (transcript-store-backed history). */
  adapter: ClaudeAgentSdkAdapter;
  /** Owning workspace (tenant scope). */
  workspaceId: string;
  /** Orca session id this harness serves. */
  sessionId: string;
  /**
   * The per-session {@link SandboxHandle} the provider captured from the session
   * context ({@link ProviderSessionContext.sandbox}), used as a FALLBACK when a
   * `start` input omits its own `sandbox`. Production always carries the handle on the
   * start input too (the loop projects `ctx.sandbox` through `buildSessionStartInput`),
   * so the two agree; this fallback makes the provider context a SUFFICIENT source, so a
   * caller that builds the harness with a sandboxed context but hand-builds a
   * `SessionStartInput` WITHOUT `.sandbox` still gets sandbox-bound tools rather than an
   * LLM-only harness. Omitted when the context wired none.
   */
  sandbox?: SandboxHandle;
  /** The SDK `query()` call; defaults to the real SDK. Injected for tests. */
  query?: ClaudeQuery;
}

/**
 * Chat-only harness backed by the Claude Agent SDK. One per session; the runner
 * `start`s it from the snapshot then `submit`s each user turn.
 */
export class ClaudeAgentSdkHarness implements AgentHarness {
  private readonly query: ClaudeQuery;
  private input: SessionStartInput | undefined;
  /**
   * The per-session sandbox handle from the boot context, or `undefined` when the
   * runner wired none (the harness then stays LLM-only). Retained so each turn's
   * options carry the sandbox `cwd`.
   */
  private sandbox: SandboxHandle | undefined;
  /**
   * The in-process `orca` MCP server bound to {@link sandbox}, built ONCE in `start`
   * (the handle is per-session + stable for the harness's lifetime) so the SDK reuses
   * the same instance across turns. `undefined` when no sandbox is wired. See
   * `mcp-tools.ts` for the full rationale.
   */
  private orcaMcpServer: McpSdkServerConfigWithInstance | undefined;
  private outQueue: AgentEvent[] = [];
  private outResolvers: Array<(e: IteratorResult<AgentEvent>) => void> = [];
  private terminated = false;
  private currentAbort: AbortController | undefined;
  /**
   * The in-flight SDK query handle for the current turn. Retained so a teardown
   * ({@link stop}) or an interrupt can force-close it (`close()` terminates the CLI
   * subprocess + frees its resources) in addition to aborting the controller, so a
   * torn-down turn never leaks the subprocess. `undefined` between turns.
   */
  private currentQuery: ClaudeQueryHandle | undefined;

  constructor(private readonly opts: ClaudeHarnessOptions) {
    this.query = opts.query ?? (realQuery as unknown as ClaudeQuery);
  }

  async start(input: SessionStartInput): Promise<void> {
    this.input = input;
    this.terminated = false;
    // Prefer the start input's sandbox, else fall back to the one the provider captured
    // from the session context (`opts.sandbox`). Production sets BOTH (the loop projects
    // `ctx.sandbox` through `buildSessionStartInput`), so they agree; the fallback makes
    // the provider context a SUFFICIENT source, so a caller that hand-builds a start
    // input WITHOUT `.sandbox` (but built the harness with a sandboxed context) still
    // gets sandbox-bound tools rather than an LLM-only harness.
    const sandbox = input.sandbox ?? this.opts.sandbox;
    this.sandbox = sandbox;
    // The delegation seam — present ONLY when this harness is a coordinator (its
    // snapshot carried a `multiagent` roster and the loop injected the seam). When
    // present the `orca` server gains the `mcp__orca__delegate_to_agent` tool, giving
    // the model the provider-side delegation surface Anthropic's thread model needs.
    const delegate = input.delegate;
    // Build the `orca` MCP tool server (once — the handle + seam are stable for the
    // harness's lifetime). Three cases:
    //   - a sandbox is present → the full file/exec server, PLUS the delegation tool
    //     when this is a coordinator. Without the server the SDK's own `Bash`/`Read`/…
    //     would run on the runner host, disconnected from the sandbox. The logical tool
    //     set is narrowed to the snapshot's allow-list (post skill intersection).
    //   - no sandbox but a coordinator → a delegation-only server (a chat-only
    //     coordinator still needs the delegation tool even with no filesystem).
    //   - neither → no orca server; the harness stays LLM-only (its prior behavior).
    if (sandbox) {
      this.orcaMcpServer = buildOrcaSdkMcpServer(
        sandbox,
        input.agentSnapshot.allowed_tool_names,
        delegate,
      );
    } else if (delegate !== undefined) {
      this.orcaMcpServer = buildDelegateOnlySdkMcpServer(delegate);
    } else {
      this.orcaMcpServer = undefined;
    }
  }

  async submit(event: UserEvent): Promise<void> {
    if (this.terminated) {
      return;
    }
    // `user.interrupt` is not a turn: it aborts the turn currently in flight. In
    // production the interrupt arrives OUT-OF-BAND through {@link interrupt} (the
    // runner's interrupt route forwards it concurrently with the in-flight turn, so
    // it can preempt a turn blocked on the model); a harness driven DIRECTLY may also
    // hand it in here as a `submit`, so this branch delegates to the same abort. It
    // does NOT terminate the harness — the next `submit` drives a fresh turn. A no-op
    // when no turn is in flight.
    if (event.kind === 'user.interrupt') {
      this.interrupt();
      return;
    }
    // Only `user.message` drives a turn; other kinds are ignored (the runner still
    // synthesizes a terminal completed marker, so the turn is answered).
    if (event.kind !== 'user.message') {
      return;
    }
    const text = textOf(event.payload);
    if (text === null) {
      return;
    }

    this.currentAbort = new AbortController();

    // The SDK reads `ANTHROPIC_*` from the env it spawns the subprocess with. We
    // forward the gateway base URL + scoped JWT here (credential-free egress); the
    // gateway swaps the JWT for the real upstream secret.
    const env: Record<string, string> = {};
    if (this.opts.apiKey) {
      env['ANTHROPIC_API_KEY'] = this.opts.apiKey;
    }
    if (this.opts.baseURL) {
      env['ANTHROPIC_BASE_URL'] = this.opts.baseURL;
    }

    // Derive the SDK's UUID session id and register the (uuid → orca-id) mapping so
    // the adapter's append/load route to the orca-id-keyed transcript topic.
    const sdkSessionId = deriveClaudeSessionId(this.opts.sessionId);
    this.opts.adapter.registerSession(sdkSessionId, this.opts.sessionId);

    // Pass `sessionStore` + `sessionId` only (no `resume`/`continue`): the SDK
    // persists every entry via the adapter and reloads prior history via
    // `adapter.load()` at the start of each turn, so cross-turn continuity holds
    // without the SDK needing its own on-disk transcript. Mirrors the harness-server.
    const snapshotModelId = this.input?.agentSnapshot?.model_id;
    // The runner's tool-confirmation gate, when the loop wired one. Present → the
    // harness gates each tool call through `canUseTool` (the uniform transcript
    // approval); absent → no human gate.
    const confirmTool = this.input?.confirmTool;
    // The per-tool permission policy resolver, when the snapshot carried one. Read
    // only alongside `confirmTool` (no gate → nothing to park on); it decides which
    // tool calls park (`always_ask`) vs auto-approve (`always_allow`). Absent → the
    // harness is fail-closed (parks every gated tool call).
    const toolPermissions = this.input?.toolPermissions;
    const options: Options = {
      sessionStore: this.opts.adapter,
      sessionId: sdkSessionId,
      // An absent OR empty snapshot model id falls back to the runner's default; the
      // snapshot always carries a `model_id` string (possibly empty), so treat empty
      // as unset rather than passing `model: ''` to the SDK.
      model:
        snapshotModelId && snapshotModelId.length > 0 ? snapshotModelId : this.opts.modelDefault,
      abortController: this.currentAbort,
      // Stream partial messages so extended-thinking deltas arrive live (the mapper
      // turns a `thinking_delta` into a partial `agent.message` thinking block). The
      // settled assistant message still arrives after, so the persisted thinking /
      // text blocks are whole.
      includePartialMessages: true,
      ...(this.input?.agentSnapshot?.system
        ? { systemPrompt: this.input.agentSnapshot.system }
        : {}),
      // When the runner wired a confirmation gate, route each tool call through it:
      // an `always_ask` tool emits the `agent.requires_action` signal then parks a
      // verdict keyed by the call's `toolUseID` (resolved by the delivered
      // `user.tool_confirmation`); an `always_allow` tool proceeds with no human gate.
      // Without a gate the harness is headless with no TTY, so `bypassPermissions`
      // keeps the SDK from blocking on an interactive prompt.
      ...(confirmTool !== undefined
        ? { canUseTool: this.makeCanUseTool(confirmTool, toolPermissions) }
        : { permissionMode: 'bypassPermissions' as const }),
    };
    if (Object.keys(env).length > 0) {
      options.env = env;
    }
    // Give the model its real built-in tools: merge the in-process `orca` sandbox tool
    // server with the snapshot's rewritten gateway MCP servers, and anchor the SDK
    // `cwd` at the sandbox root so even the SDK's own built-ins land inside the sandbox
    // (mirrors the harness-server). With no sandbox and no gateway servers this returns
    // an empty fragment, so a chat-only turn's options are unchanged.
    Object.assign(
      options,
      buildSandboxSdkOptions({
        ...(this.orcaMcpServer !== undefined ? { orcaMcpServer: this.orcaMcpServer } : {}),
        ...(this.sandbox !== undefined ? { sandbox: this.sandbox } : {}),
        ...(this.input?.mcpServers !== undefined
          ? { gatewayMcpServers: this.input.mcpServers }
          : {}),
      }),
    );

    // One mapper per turn: it carries the call_id pairing bookkeeping across the
    // turn's messages (a `tool_result` pairs to its `agent.tool_use` by tool_use_id).
    const mapper = new SdkMessageMapper();
    const result = this.query({ prompt: text, options });
    this.currentQuery = result;

    try {
      for await (const message of result) {
        if (this.terminated) {
          break;
        }
        for (const event of mapper.map(message)) {
          this.emit(event);
        }
      }
    } catch (err) {
      // The SDK faulted mid-turn (subprocess died, transport error) — terminal for
      // this harness (its CLI subprocess is gone; the loop tears it down + builds a
      // fresh one on the next snapshot). FIRST emit a terminal `agent.error` for this
      // turn — mirroring the persistent harness — so the reason reaches the wire and
      // the durable transcript; the loop's own handling of the rejection is a stderr
      // log plus a generic terminal error, so without this the SDK's message is lost.
      // The emit must precede `endEventStream()`: after it, `events()` is closed and an
      // emitted event would never be read. Then mark terminated so the event stream ends
      // and no further turn drives the dead SDK, abort the in-flight controller +
      // force-close the query handle (reap the subprocess), end the event stream so a
      // direct consumer is not stranded, and re-raise so the loop settles the turn (it
      // contains the rejection and ends the turn's wire stream with its marker).
      this.emit(
        terminalError(`Claude SDK error: ${err instanceof Error ? err.message : String(err)}`),
      );
      this.terminated = true;
      this.abortAndForceCloseTurn();
      this.endEventStream();
      throw err;
    } finally {
      this.currentAbort = undefined;
      this.currentQuery = undefined;
    }
    // `submit` resolves here — after the `query()` loop drained — honoring the
    // runner's turn-boundary contract (all of the turn's events are already emitted).
  }

  /**
   * Abort the in-flight turn WITHOUT terminating the harness — the out-of-band
   * `user.interrupt`.
   *
   * Fires the in-flight turn's abort signal and force-closes the live query handle so
   * a turn blocked on the model unwinds (force-close terminates the CLI subprocess;
   * aborting alone only signals the loop). The aborted `query()` loop then drains,
   * its `submit` resolves, and any tool gate parked on the turn releases as a denial.
   * Crucially it leaves `terminated` FALSE and does not end `events()`: the harness
   * stays alive and the next `submit` drives a fresh turn (a fresh `AbortController` +
   * `query()`). A no-op when no turn is in flight, so a re-delivered interrupt is
   * idempotent. The runner's interrupt route drives this; {@link stop} is the
   * terminating counterpart.
   */
  interrupt(): void {
    if (this.terminated) {
      return;
    }
    this.abortAndForceCloseTurn();
  }

  /**
   * Abort the in-flight turn (if any): fire the SDK abort signal AND force-close the
   * live query handle so the CLI subprocess is torn down (aborting alone only signals
   * the loop). Drives {@link interrupt} and the mid-turn fault cleanup; the aborted
   * `query()` loop then drains, `submit` resolves, and any tool gate parked on the
   * turn releases as a denial.
   */
  private abortAndForceCloseTurn(): void {
    this.currentAbort?.abort();
    forceCloseQuery(this.currentQuery);
  }

  async stop(reason: TerminationReason): Promise<void> {
    void reason; // Informational; the runner logs it.
    this.terminated = true;
    // Abort the in-flight turn AND force-close the live query handle: aborting alone
    // signals the loop, but the SDK's `close()` is what terminates the CLI subprocess
    // and frees its transports — without it a stop on error / shutdown could leak the
    // process. Probed defensively (a scripted fake omits `close`).
    this.currentAbort?.abort();
    forceCloseQuery(this.currentQuery);
    this.currentAbort = undefined;
    this.currentQuery = undefined;
    this.endEventStream();
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

  private emit(e: AgentEvent): void {
    const resolver = this.outResolvers.shift();
    if (resolver) {
      resolver({ value: e, done: false });
      return;
    }
    this.outQueue.push(e);
  }

  /**
   * Adapt the runner's {@link ToolConfirmer} gate to the SDK's `canUseTool` callback.
   *
   * On a gated tool call the SDK supplies the call's unique `toolUseID`. The
   * effective permission policy (from {@link ToolPermissionResolver}, or fail-closed
   * `always_ask` when none was wired) decides the path:
   *
   *   - `always_allow` → proceed immediately with the (unchanged) input. No
   *     `agent.requires_action` is emitted and nothing parks: the tool was
   *     pre-authorized, so a human verdict is neither awaited nor required.
   *   - `always_ask` → EMIT the Anthropic-native `agent.requires_action` signal (so a
   *     client knows the turn awaits a tool-confirmation verdict for this
   *     `tool_use_id`), then PARK the verdict keyed by it through the runner's
   *     confirmation gate (the uniform transcript approval) and map the result back to
   *     the SDK's `PermissionResult` — allow → proceed with the (possibly updated)
   *     input, deny → a clean denial with a reason.
   *
   * The SDK's abort signal is threaded on the `always_ask` path so a cancelled turn /
   * stopped / interrupted harness releases a parked gate as a denial rather than
   * hanging the `query()` loop.
   */
  private makeCanUseTool(
    confirmTool: ToolConfirmer,
    toolPermissions: ToolPermissionResolver | undefined,
  ): CanUseTool {
    return async (toolName, input, options): Promise<PermissionResult> => {
      // If the turn is already aborted (cancel / stop / interrupt), deny immediately —
      // do not park and do not emit a requires_action a client can never satisfy.
      if (options.signal.aborted) {
        return { behavior: 'deny', message: 'Tool use denied: turn aborted.' };
      }
      // Resolve the per-tool policy. No resolver → fail-closed `always_ask` (a tool
      // the snapshot did not classify still requires a human verdict).
      const policy = toolPermissions ? toolPermissions.policyFor(toolName) : 'always_ask';
      if (policy === 'always_deny')
        return { behavior: 'deny', message: 'Tool use denied by policy.' };
      if (policy === 'always_allow') {
        // Pre-authorized: proceed with no human gate, no requires_action, no park.
        return { behavior: 'allow', updatedInput: input };
      }
      // Surface the tool-confirmation-required signal BEFORE parking, keyed by the
      // call's `tool_use_id` (the verdict's routing key). The verdict arrives
      // out-of-band as a `user.tool_confirmation` the loop resolves through the park.
      const signal: RequiresActionSignal = {
        action: 'tool_confirmation',
        tool_use_id: options.toolUseID,
        tool_name: toolName,
      };
      this.emit({ kind: 'agent.requires_action', id: options.toolUseID, payload: signal });
      const verdict = confirmTool(toolName, input, { toolUseId: options.toolUseID });
      // Race the parked verdict against the SDK abort so a teardown mid-park resolves
      // the gate (the gate itself also denies on the harness stop, but a turn-level
      // abort that does not stop the harness still needs to release here). The abort
      // listener is removed in `finally` REGARDLESS of which side wins — when the
      // verdict wins the race, the listener would otherwise linger on `options.signal`
      // until GC, so the `{ once: true }` auto-removal (which only fires on abort) is
      // not enough on its own.
      let onAbort: (() => void) | undefined;
      const aborted = new Promise<PermissionResult>((resolve) => {
        onAbort = (): void =>
          resolve({ behavior: 'deny', message: 'Tool use denied: turn aborted.' });
        options.signal.addEventListener('abort', onAbort, { once: true });
      });
      try {
        return await Promise.race([verdict, aborted]);
      } finally {
        if (onAbort !== undefined) {
          options.signal.removeEventListener('abort', onAbort);
        }
      }
    };
  }
}

/**
 * Force-close an in-flight SDK query handle, best-effort. The real SDK `Query`
 * exposes `close()` — it terminates the CLI subprocess and frees its transports /
 * pending requests; aborting the controller alone signals the loop but does not
 * reap the process. Probed defensively (a scripted fake omits `close`) and any
 * throw is swallowed: a teardown / interrupt must never raise out of cleanup.
 */
function forceCloseQuery(handle: ClaudeQueryHandle | undefined): void {
  const close = handle?.close;
  if (typeof close !== 'function') {
    return;
  }
  try {
    close.call(handle);
  } catch {
    // Best-effort: a force-close on an already-dead handle must not surface.
  }
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
