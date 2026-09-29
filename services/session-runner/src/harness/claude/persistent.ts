// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `claude-sdk-persistent` provider (B): a PERSISTENT live-session
// {@link AgentHarness} backed by the Claude Agent SDK's streaming-input `query()`.
//
// Where the lean `claude` provider (A) runs a stateless ONE-SHOT `query()` per turn
// (reloading history from the transcript each turn), this provider keeps ONE live
// `query()` ALIVE across turns — the streaming-input mode where the SDK's CLI
// subprocess holds the conversation internally and the harness feeds each turn's user
// message into the live prompt stream. The persistent model is Orca-native and
// Anthropic-only; it provides:
//
//   - LIVE SESSION reuse: the first turn creates the `query()` (a streaming-input
//     prompt iterable + the live `Query` handle, which is an async-generator of SDK
//     messages that ALSO exposes `interrupt`/`setModel`/`close`); every later turn
//     pushes its user message into the SAME live prompt and reads the SAME output
//     generator until that turn's `result` frame. No fresh `query()` per turn.
//   - PER-TURN model selection: the effective model (a per-turn override on the user
//     message, else the snapshot model, else the runner default) is VALIDATED then
//     applied via the live handle's `setModel` — and only when it CHANGES from the
//     live session's current model (the initial model rides `options.model`). A
//     malformed override (blank / control chars / oversized) is rejected back to the
//     snapshot/default model with an `agent.status` diagnostic, and when the runner
//     supplies an `allowedModels` set the override is additionally constrained to it —
//     the harness never forwards an unvetted model id to the live session.
//   - INTERRUPT = interrupt-then-rebuild-history: {@link interrupt} fires the live
//     handle's `interrupt()` then CLOSES the live session. The next turn therefore has
//     no live session, so it REBUILDS the full conversation history (read from the
//     transcript via the session-store adapter) into a fresh live session — replayed
//     as STRUCTURED prior messages (each prior user/assistant turn pushed verbatim with
//     `shouldQuery: false`, preserving thinking / tool_use / tool_result blocks), then
//     the latest user message drives the turn. The abandoned turn is visible-but-
//     superseded rather than silently continued (a live session that kept running would
//     only ever see the latest message and never the interruption), and the rebuilt
//     session retains the SAME tool/thinking context a live session would have held.
//   - CRASHED-SESSION recovery: a turn-stream fault records the session as crashed (+
//     force-closes the live handle) and emits a terminal `agent.error` for that turn.
//     The crash does NOT terminate the harness: the NEXT turn observes the crash as an
//     `agent.status` diagnostic, then SELF-HEALS — it rebuilds a fresh live session from
//     the transcript (the same structured-history rebuild the interrupt path uses) and
//     drives the turn. Recovery is therefore in-harness and deterministic (it does not
//     depend on an external snapshot re-push); the dead handle is never driven.
//   - RETRY observation: an `api_retry` system frame surfaces as an Anthropic-native
//     `agent.status` diagnostic; an auth / endpoint-not-found retry status is treated
//     as a TERMINAL `agent.error` that ends the turn.
//   - the `canUseTool` approval hook wired to the runner's uniform transcript approval
//     (park `always_ask`, auto-approve `always_allow`, fail-closed with no policy) —
//     identical to provider A; and thinking blocks + tool-use/result pairing via the
//     SHARED {@link SdkMessageMapper}.
//   - robust FORCE-CLOSE cleanup on interrupt / stop / fault (the live handle's
//     `close()` terminates the CLI subprocess + frees its transports).
//
// Boundaries: emits Anthropic-native `AgentEvent`s ONLY; base-URL/auth come from the
// snapshot egress (forwarded to the SDK env, credential-free) — the same gateway block
// the lean provider reads. It does no cost accounting and does not wrap the SDK process
// in a sandbox: the runner's sandbox reaches the SDK as the in-process `orca` tool
// server plus a sandbox-rooted `cwd` (see `buildSandboxSdkOptions`).

import { v5 as uuidv5 } from 'uuid';
import {
  query as realQuery,
  type CanUseTool,
  type McpSdkServerConfigWithInstance,
  type Options,
  type PermissionResult,
  type SDKMessage,
  type SDKUserMessage,
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
import type { SessionStoreEntry } from './event-mapper.js';
import { SdkMessageMapper } from './sdk-message-mapper.js';
import {
  buildDelegateOnlySdkMcpServer,
  buildOrcaSdkMcpServer,
  buildSandboxSdkOptions,
} from './mcp-tools.js';

/**
 * The live streaming-input `Query` handle, narrowed to the surface this harness uses.
 * The real SDK `query()` (with an async-iterable prompt) returns a `Query` that is an
 * `AsyncGenerator<SDKMessage>` AND exposes the persistent-control methods below
 * (`interrupt`/`setModel`/`close`). Injected so the harness is unit-testable against a
 * scripted fake; defaults to the real SDK `query`.
 */
export interface PersistentQueryHandle extends AsyncIterable<SDKMessage> {
  /** Interrupt the in-flight model turn (best-effort, fast). Streaming-input only. */
  interrupt(): Promise<void>;
  /** Change the model for subsequent turns on this live session. Streaming-input only. */
  setModel(model?: string): Promise<void>;
  /** Force-close the live session: terminate the CLI subprocess + free its resources. */
  close(): void;
}

/** The streaming-input `query()` call (an async-iterable prompt → a live {@link PersistentQueryHandle}). */
export type PersistentClaudeQuery = (args: {
  prompt: AsyncIterable<SDKUserMessage>;
  options: Options;
}) => PersistentQueryHandle;

/**
 * The Anthropic-native `agent.requires_action` signal payload emitted when a tool call
 * parks on the human gate: the action kind, the in-flight call's `tool_use_id` (the
 * verdict's routing key), and the tool name. Mirrors provider A's signal exactly.
 */
export interface RequiresActionSignal {
  action: 'tool_confirmation';
  tool_use_id: string;
  tool_name: string;
}

/**
 * The SDK's `Options.sessionId` must be a UUID; orca session ids are `ses_<base32>`.
 * Derive a deterministic UUID v5 from the orca id (collision-resistant under a fixed
 * namespace). Same namespace as provider A + the harness-server so the SDK session id
 * is stable + the adapter maps it back to the orca id.
 */
const ORCA_CLAUDE_SESSION_NAMESPACE = 'ac15a075-8075-494b-9262-ae6a650ec031';

function deriveClaudeSessionId(orcaSessionId: string): string {
  return uuidv5(orcaSessionId, ORCA_CLAUDE_SESSION_NAMESPACE);
}

/** Construction options for {@link ClaudePersistentSdkHarness}. */
export interface ClaudePersistentHarnessOptions {
  /** The scoped session JWT forwarded to the SDK as `ANTHROPIC_API_KEY` (credential-free). */
  apiKey: string;
  /** The gateway LLM-proxy base URL forwarded to the SDK as `ANTHROPIC_BASE_URL`. */
  baseURL?: string;
  /** Default model id when neither the turn nor the snapshot pinned one. */
  modelDefault: string;
  /**
   * Optional allow-list of model ids a turn may select. When set, a per-turn model
   * override is admitted ONLY if it is in this set; an override outside it is rejected
   * back to the snapshot/default model (with an `agent.status` diagnostic). When UNSET
   * (the default), a per-turn override is admitted on FORMAT alone (a well-formed model
   * id) — the gateway remains the policy enforcement point, but the harness still never
   * forwards a malformed id. The snapshot's pinned model and {@link modelDefault} are
   * always implicitly permitted (they are the registry-vetted fallbacks).
   */
  allowedModels?: readonly string[];
  /** SDK `SessionStore` adapter (transcript-store-backed history; used for the rebuild). */
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
  /** The streaming-input `query()` call; defaults to the real SDK. Injected for tests. */
  query?: PersistentClaudeQuery;
}

/**
 * A pushable async-iterable of SDK user messages — the live session's prompt stream.
 *
 * The harness pushes one user message per turn ({@link push}); the SDK's streaming-
 * input `query()` consumes them as they arrive (the live CLI runs each as a turn).
 * {@link close} ends the stream (the live session is being torn down). Buffers
 * messages pushed before the consumer pulls (so a turn submitted before the SDK
 * starts reading is not lost) and parks the consumer when the buffer is empty.
 */
class PromptStream implements AsyncIterable<SDKUserMessage> {
  private readonly queue: SDKUserMessage[] = [];
  private resolveNext: ((r: IteratorResult<SDKUserMessage>) => void) | undefined;
  private done = false;

  /** Enqueue one user message for the live session (a no-op once closed). */
  push(message: SDKUserMessage): void {
    if (this.done) {
      return;
    }
    const resolver = this.resolveNext;
    if (resolver !== undefined) {
      this.resolveNext = undefined;
      resolver({ value: message, done: false });
      return;
    }
    this.queue.push(message);
  }

  /** End the prompt stream (the live session is closing). Idempotent. */
  close(): void {
    if (this.done) {
      return;
    }
    this.done = true;
    const resolver = this.resolveNext;
    if (resolver !== undefined) {
      this.resolveNext = undefined;
      resolver({ value: undefined, done: true });
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<SDKUserMessage> {
    return {
      next: (): Promise<IteratorResult<SDKUserMessage>> => {
        if (this.queue.length > 0) {
          const value = this.queue.shift() as SDKUserMessage;
          return Promise.resolve({ value, done: false });
        }
        if (this.done) {
          return Promise.resolve({ value: undefined, done: true });
        }
        return new Promise<IteratorResult<SDKUserMessage>>((resolve) => {
          this.resolveNext = resolve;
        });
      },
    };
  }
}

/** The live session state: the prompt stream + the live query handle + its current model. */
interface LiveSession {
  prompt: PromptStream;
  query: PersistentQueryHandle;
  iterator: AsyncIterator<SDKMessage>;
  /** The model the live session is currently configured for (drives `setModel` on change). */
  model: string;
}

/**
 * Persistent live-session harness backed by the Claude Agent SDK streaming-input
 * `query()`. One per session; the runner `start`s it from the snapshot then `submit`s
 * each user turn against the SAME live session (rebuilding only after an interrupt /
 * crash).
 */
export class ClaudePersistentSdkHarness implements AgentHarness {
  private readonly query: PersistentClaudeQuery;
  private input: SessionStartInput | undefined;
  /**
   * The per-session sandbox handle from the boot context, or `undefined` when the
   * runner wired none (the harness then stays LLM-only). Retained so each live
   * session's options carry the sandbox `cwd`.
   */
  private sandbox: SandboxHandle | undefined;
  /**
   * The in-process `orca` MCP server bound to {@link sandbox}, built ONCE in `start`
   * (the handle is per-session + stable for the harness's lifetime) so every live
   * session — including a post-interrupt / post-crash rebuild — reuses the same
   * instance. `undefined` when no sandbox is wired. See `mcp-tools.ts` for the full
   * rationale.
   */
  private orcaMcpServer: McpSdkServerConfigWithInstance | undefined;
  private outQueue: AgentEvent[] = [];
  private outResolvers: Array<(e: IteratorResult<AgentEvent>) => void> = [];
  private terminated = false;
  /** The live session, or `undefined` before the first turn / after an interrupt / crash. */
  private live: LiveSession | undefined;
  /** The in-flight turn's abort controller (fires on interrupt / stop to release a parked gate). */
  private currentAbort: AbortController | undefined;
  /**
   * The crash reason, set when a turn-stream fault killed the live session and CLEARED
   * once the next turn self-heals. While set, the live handle is dead (force-closed on
   * the fault) and {@link isCrashed} reports true; the next {@link submit} observes the
   * crash as an `agent.status` diagnostic, then rebuilds a fresh live session from the
   * transcript and drives the turn — clearing this. Recovery is in-harness and does not
   * depend on an external snapshot re-push. Scoped to this one session (a runner serves
   * one session).
   */
  private crashReason: string | undefined;

  constructor(private readonly opts: ClaudePersistentHarnessOptions) {
    this.query = opts.query ?? (realQuery as unknown as PersistentClaudeQuery);
  }

  async start(input: SessionStartInput): Promise<void> {
    this.input = input;
    this.terminated = false;
    this.crashReason = undefined;
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
    // Build the `orca` MCP tool server once (the handle + seam are stable for the
    // harness's lifetime), so every live session — including a post-interrupt /
    // post-crash rebuild — reuses it. Three cases mirror the lean provider:
    //   - a sandbox is present → the full file/exec server, PLUS the delegation tool
    //     when this is a coordinator;
    //   - no sandbox but a coordinator → a delegation-only server (a chat-only
    //     coordinator still needs the delegation tool);
    //   - neither → no orca server; the harness stays LLM-only.
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

  /** Whether the live session crashed (test/observability hook). */
  isCrashed(): boolean {
    return this.crashReason !== undefined;
  }

  async submit(event: UserEvent): Promise<void> {
    if (this.terminated) {
      return;
    }
    // `user.interrupt` is not a turn: it interrupts the in-flight turn + closes the
    // live session (so the next turn rebuilds). A harness driven directly may hand it
    // here as a submit; production drives it out-of-band via {@link interrupt}.
    if (event.kind === 'user.interrupt') {
      this.interrupt();
      return;
    }
    if (event.kind !== 'user.message') {
      return;
    }
    // A crashed session SELF-HEALS rather than staying dead: the prior fault force-closed
    // the live handle and dropped the live session, so this turn already has no live
    // session and will rebuild a fresh one below (the same structured-history rebuild
    // the interrupt path uses). Surface the crash as an `agent.status` diagnostic, clear
    // the crash flag, and fall through to drive the turn on the rebuilt session — the
    // dead handle is never touched. Recovery is in-harness and does not depend on an
    // external snapshot re-push.
    if (this.crashReason !== undefined) {
      this.emit({
        kind: 'agent.status',
        payload: {
          status: 'session_recovered',
          message: 'Claude SDK session crashed; rebuilt a fresh live session to continue.',
          cause: this.crashReason,
        },
      });
      this.crashReason = undefined;
    }
    const text = textOf(event.payload);
    if (text === null) {
      return;
    }

    const turnModel = this.resolveTurnModel(event.payload);
    this.currentAbort = new AbortController();
    try {
      const live = await this.ensureLiveSession(turnModel);
      // On a FRESH session, prime the live prompt stream with the rebuilt prior history
      // as STRUCTURED messages (each prior turn pushed verbatim with `shouldQuery: false`
      // so it lands in the live transcript WITHOUT triggering a turn) — preserving the
      // thinking / tool_use / tool_result context a live session would have held. On a
      // REUSED session there is no priming (the live CLI holds prior history internally).
      for (const priming of live.history) {
        live.session.prompt.push(priming);
      }
      // Then push the latest user message — the only message that drives the turn.
      live.session.prompt.push(makeUserMessage(text));
      await this.consumeTurn(live.session);
    } catch (err) {
      // The live session faulted mid-turn (subprocess died / transport error): record
      // the crash, emit a terminal error for THIS turn, force-close the dead handle +
      // abort the in-flight controller, then re-raise so the loop logs + settles the
      // turn. Crucially this does NOT end the event stream and does NOT terminate the
      // harness — the persistent model keeps a crashed harness ALIVE so the NEXT turn
      // self-heals: the crashed-session guard at the top of `submit` emits a
      // `session_recovered` status, clears the crash, and rebuilds a fresh live session
      // from the transcript (the dead handle is never driven again). Only {@link stop}
      // ends the stream.
      this.crashReason = err instanceof Error ? err.message : String(err);
      this.emit(terminalError(`Claude SDK error: ${this.crashReason}`));
      this.closeLiveSession();
      this.currentAbort?.abort();
      throw err;
    } finally {
      this.currentAbort = undefined;
    }
  }

  /**
   * Interrupt the in-flight turn AND close the live session — the out-of-band
   * `user.interrupt` (interrupt-then-rebuild-history).
   *
   * Fires the live handle's `interrupt()` (best-effort, fast) then force-closes the
   * live session: closing ends the live output generator so a turn blocked reading it
   * unwinds (its `submit` resolves), and DROPS the live session so the NEXT turn
   * rebuilds the full conversation history into a fresh session — the abandoned turn is
   * then visible-but-superseded rather than silently continued. Leaves `terminated`
   * FALSE: the harness stays alive and the next `submit` drives a fresh live session. A
   * no-op when terminated. The runner's interrupt route drives this; {@link stop} is
   * the terminating counterpart.
   */
  interrupt(): void {
    if (this.terminated) {
      return;
    }
    const live = this.live;
    // Abort the in-flight turn so a parked tool gate releases as a denial.
    this.currentAbort?.abort();
    if (live !== undefined) {
      // Best-effort interrupt of the model turn, then force-close + drop the session.
      void interruptQuery(live.query);
      this.closeLiveSession();
    }
  }

  async stop(reason: TerminationReason): Promise<void> {
    void reason; // Informational; the runner logs it.
    this.terminated = true;
    this.currentAbort?.abort();
    this.currentAbort = undefined;
    this.closeLiveSession();
    this.endEventStream();
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
   * Get the live session for this turn — reusing the existing one (and applying a
   * per-turn `setModel` when the model changed), or creating a fresh one plus the
   * structured prior history to prime it with.
   *
   * Returns the session plus `history`: the rebuilt prior conversation as structured
   * `SDKUserMessage`s (empty for a reused session, or a true first turn / empty
   * transcript). The caller pushes the history (append-only) ahead of the latest user
   * message so a fresh post-interrupt / post-crash session carries the prior turns —
   * INCLUDING their thinking / tool_use / tool_result blocks — that a fresh CLI
   * subprocess would otherwise hold no record of.
   */
  private async ensureLiveSession(
    turnModel: string,
  ): Promise<{ session: LiveSession; history: SDKUserMessage[] }> {
    const existing = this.live;
    if (existing !== undefined) {
      // Reuse the live session. Apply the model only when it CHANGED (the initial model
      // rode `options.model`; a redundant `setModel` would be wasted work).
      if (existing.model !== turnModel) {
        await existing.query.setModel(turnModel);
        existing.model = turnModel;
      }
      return { session: existing, history: [] };
    }
    // No live session (first turn, or after an interrupt / crash): build a fresh one.
    // Replay the full conversation history from the transcript as structured prior
    // messages so a post-interrupt session sees the prior turns (and the superseded
    // request) as context — a fresh CLI subprocess holds no internal history.
    const history = await this.buildHistoryMessages();
    const session = this.openLiveSession(turnModel);
    this.live = session;
    return { session, history };
  }

  /** Open a fresh live streaming-input `query()` configured for `model`. */
  private openLiveSession(model: string): LiveSession {
    const prompt = new PromptStream();
    const options = this.buildOptions(model);
    const handle = this.query({ prompt, options });
    return {
      prompt,
      query: handle,
      iterator: handle[Symbol.asyncIterator]() as AsyncIterator<SDKMessage>,
      model,
    };
  }

  /**
   * Read the live output generator for the CURRENT turn, mapping each SDK message to
   * Anthropic-native events and emitting them, until this turn's `result` frame (which
   * ends the turn — the live generator stays open for the next turn). Honors the
   * runner's turn-boundary contract: `submit` resolves once the turn's events are all
   * emitted.
   *
   * An `api_retry` system frame is observed as an `agent.status` diagnostic; an auth /
   * endpoint-not-found retry status is TERMINAL — it emits an `agent.error` and ends
   * the turn (the model cannot make progress). A force-close (interrupt / stop) ends
   * the generator → the read loop sees `done` and the turn settles.
   *
   * Replayed history frames are SKIPPED: a fresh (post-interrupt / post-crash) session
   * is primed with prior turns as append-only `shouldQuery: false` messages, and if the
   * live generator echoes any of them back (a replay frame), mapping them would re-emit
   * the prior turn's tool_use / tool_result / thinking. A frame carrying
   * `shouldQuery === false` is append-only history (never current-turn production), so it
   * is dropped before the mapper sees it.
   */
  private async consumeTurn(session: LiveSession): Promise<void> {
    const mapper = new SdkMessageMapper();
    for (;;) {
      if (this.terminated) {
        return;
      }
      const next = await session.iterator.next();
      if (next.done) {
        // The live generator ended (force-closed by interrupt / stop, or the CLI exited).
        return;
      }
      const message = next.value;
      // Drop an echoed append-only history frame so the rebuild does not re-emit prior turns.
      if (isReplayedHistoryFrame(message)) {
        continue;
      }
      // Observe an api_retry diagnostic; a terminal retry status ends the turn.
      const retry = readApiRetry(message);
      if (retry !== undefined) {
        this.emit({ kind: 'agent.status', payload: retry.payload });
        if (retry.terminal) {
          this.emit(terminalError(retry.terminalMessage));
          return;
        }
        continue;
      }
      for (const ev of mapper.map(message)) {
        this.emit(ev);
      }
      if (isResultFrame(message)) {
        // This turn is complete; leave the live generator open for the next turn.
        return;
      }
    }
  }

  /** Force-close + drop the live session (best-effort). A no-op when none. */
  private closeLiveSession(): void {
    const live = this.live;
    this.live = undefined;
    if (live === undefined) {
      return;
    }
    live.prompt.close();
    forceCloseQuery(live.query);
  }

  /** Resolve the closure end-of-stream for every waiting `events()` consumer. */
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

  /**
   * The effective, VALIDATED model for a turn: a per-turn override on the user message
   * (`payload.model`) wins when it passes validation, else the snapshot's pinned model,
   * else the runner default. An empty string is treated as unset.
   *
   * A per-turn override is validated before it reaches the live session: it must be a
   * well-formed model id ({@link isWellFormedModelId} — non-blank, no control chars,
   * bounded length) AND, when the runner supplied an `allowedModels` set, a member of
   * it (the snapshot/default models are always implicitly permitted). An override that
   * fails validation is REJECTED — the turn falls back to the snapshot/default model and
   * an `agent.status` diagnostic records the rejection — so the harness never forwards
   * an unvetted model id to `setModel` / `options.model`.
   */
  private resolveTurnModel(payload: unknown): string {
    const fallback = this.fallbackModel();
    const override =
      payload !== null && typeof payload === 'object'
        ? (payload as { model?: unknown }).model
        : undefined;
    if (override === undefined || override === null) {
      return fallback;
    }
    if (typeof override !== 'string' || override.length === 0) {
      return fallback;
    }
    const rejection = this.rejectModelReason(override);
    if (rejection !== undefined) {
      this.emit({
        kind: 'agent.status',
        payload: {
          status: 'model_override_rejected',
          requested_model: truncateForDiagnostic(override),
          effective_model: fallback,
          reason: rejection,
        },
      });
      return fallback;
    }
    return override;
  }

  /** The snapshot-pinned model, else the runner default — the always-permitted fallback. */
  private fallbackModel(): string {
    const snapshotModel = this.input?.agentSnapshot?.model_id;
    if (typeof snapshotModel === 'string' && snapshotModel.length > 0) {
      return snapshotModel;
    }
    return this.opts.modelDefault;
  }

  /**
   * Why a per-turn model override is rejected, or `undefined` when it is admissible.
   * Order: malformed ids are rejected first (`malformed_model_id`), then — only when the
   * runner supplied an `allowedModels` set — an id outside the allow-list (and outside
   * the always-permitted snapshot/default fallbacks) is rejected (`model_not_allowed`).
   */
  private rejectModelReason(model: string): string | undefined {
    if (!isWellFormedModelId(model)) {
      return 'malformed_model_id';
    }
    const allowed = this.opts.allowedModels;
    if (allowed === undefined || allowed.length === 0) {
      return undefined;
    }
    if (model === this.fallbackModel() || allowed.includes(model)) {
      return undefined;
    }
    return 'model_not_allowed';
  }

  /** Build the live `query()` options for `model` (env + session-store + gate + streaming). */
  private buildOptions(model: string): Options {
    // The SDK reads `ANTHROPIC_*` from the env it spawns the CLI subprocess with. We
    // forward the gateway base URL + scoped JWT (credential-free egress).
    const env: Record<string, string> = {};
    if (this.opts.apiKey) {
      env['ANTHROPIC_API_KEY'] = this.opts.apiKey;
    }
    if (this.opts.baseURL) {
      env['ANTHROPIC_BASE_URL'] = this.opts.baseURL;
    }
    const sdkSessionId = deriveClaudeSessionId(this.opts.sessionId);
    this.opts.adapter.registerSession(sdkSessionId, this.opts.sessionId);

    const confirmTool = this.input?.confirmTool;
    const toolPermissions = this.input?.toolPermissions;
    const options: Options = {
      sessionStore: this.opts.adapter,
      sessionId: sdkSessionId,
      model,
      // Stream partial messages so extended-thinking deltas arrive live.
      includePartialMessages: true,
      // Thread the in-flight turn's abort controller only when present (under
      // `exactOptionalPropertyTypes` an explicit `undefined` is not assignable to the
      // SDK's `abortController?: AbortController`). `submit` sets it before opening the
      // live session, so a fresh session always carries one; spread it conditionally to
      // satisfy strict optionality without widening the option type to `| undefined`.
      ...(this.currentAbort !== undefined ? { abortController: this.currentAbort } : {}),
      ...(this.input?.agentSnapshot?.system
        ? { systemPrompt: this.input.agentSnapshot.system }
        : {}),
      // When the runner wired a confirmation gate, route each tool call through it;
      // otherwise (headless, no TTY) bypass permissions so the SDK does not block on a
      // prompt. Identical gate to provider A.
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
    // (identical to provider A + the harness-server). Empty fragment (options unchanged)
    // when neither a sandbox nor a gateway server is wired.
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
    return options;
  }

  /**
   * Build the structured prior-history messages that prime a FRESH live session: the
   * rebuilt conversation history (read from the transcript via the session-store
   * adapter) as `SDKUserMessage`s the caller pushes ahead of the latest user message.
   * With no prior history (a true first turn, or an empty transcript) this is empty.
   *
   * Each prior conversation entry is replayed VERBATIM — its role (`user`/`assistant`)
   * and its full content (the original block array: text, thinking, tool_use,
   * tool_result) — and marked `shouldQuery: false` so the live CLI appends it to the
   * session transcript WITHOUT running it as a turn. This preserves the thinking /
   * tool_use / tool_result context across the rebuild: a post-interrupt session retains
   * the same internal history a live session would have held, rather than a lossy
   * plain-text summary. Only the latest user message (pushed by the caller after these)
   * drives the turn.
   */
  private async buildHistoryMessages(): Promise<SDKUserMessage[]> {
    const entries = await this.loadHistoryEntries();
    const messages: SDKUserMessage[] = [];
    for (const entry of entries) {
      const message = entryToHistoryMessage(entry);
      if (message !== undefined) {
        messages.push(message);
      }
    }
    return messages;
  }

  /** Load the prior conversation entries from the transcript via the adapter (or none). */
  private async loadHistoryEntries(): Promise<SessionStoreEntry[]> {
    const sdkSessionId = deriveClaudeSessionId(this.opts.sessionId);
    this.opts.adapter.registerSession(sdkSessionId, this.opts.sessionId);
    // `SessionKey` requires a `projectKey` (the tenant/project scope); the adapter
    // resolves history by the registered orca session id + its own workspace, so the
    // `projectKey` it receives is opaque — pass the workspace id as the tenant scope.
    const loaded = await this.opts.adapter.load({
      projectKey: this.opts.workspaceId,
      sessionId: sdkSessionId,
    });
    return Array.isArray(loaded) ? (loaded as SessionStoreEntry[]) : [];
  }

  /**
   * Adapt the runner's {@link ToolConfirmer} gate to the SDK's `canUseTool` callback —
   * IDENTICAL to provider A (the persistent model changes the session lifecycle, not
   * the per-tool gate). On a gated call: `always_allow` proceeds with no human gate +
   * no signal; `always_ask` (or fail-closed, with no resolver) emits the Anthropic-
   * native `agent.requires_action` signal then PARKS the verdict keyed by the call's
   * `tool_use_id` and maps the result to the SDK `PermissionResult`. The SDK abort is
   * threaded so a cancelled / interrupted / stopped turn releases a parked gate as a
   * denial rather than hanging the live generator.
   */
  private makeCanUseTool(
    confirmTool: ToolConfirmer,
    toolPermissions: ToolPermissionResolver | undefined,
  ): CanUseTool {
    return async (toolName, input, options): Promise<PermissionResult> => {
      if (options.signal.aborted) {
        return { behavior: 'deny', message: 'Tool use denied: turn aborted.' };
      }
      const policy = toolPermissions ? toolPermissions.policyFor(toolName) : 'always_ask';
      if (policy === 'always_deny')
        return { behavior: 'deny', message: 'Tool use denied by policy.' };
      if (policy === 'always_allow') {
        return { behavior: 'allow', updatedInput: input };
      }
      const signal: RequiresActionSignal = {
        action: 'tool_confirmation',
        tool_use_id: options.toolUseID,
        tool_name: toolName,
      };
      this.emit({ kind: 'agent.requires_action', id: options.toolUseID, payload: signal });
      const verdict = confirmTool(toolName, input, { toolUseId: options.toolUseID });
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

// ── module-private helpers ──────────────────────────────────────────────────────

/** Wrap a prompt string as an `SDKUserMessage` for the streaming-input prompt. */
function makeUserMessage(text: string): SDKUserMessage {
  return {
    type: 'user',
    parent_tool_use_id: null,
    message: { role: 'user', content: text },
  } as SDKUserMessage;
}

/** Whether an SDK message is a turn-ending `result` frame. */
function isResultFrame(message: unknown): boolean {
  return (message as { type?: unknown }).type === 'result';
}

/**
 * Whether an SDK output frame is an echoed APPEND-ONLY history message (the structured
 * prior-turn priming pushed with `shouldQuery: false` on a rebuilt session). Such a
 * frame is not current-turn production and must not be mapped — re-mapping it would
 * re-emit the prior turn's tool_use / tool_result / thinking. A genuine current-turn
 * tool_result (produced by the SDK running a tool) does not carry `shouldQuery: false`.
 */
function isReplayedHistoryFrame(message: unknown): boolean {
  return (
    message !== null &&
    typeof message === 'object' &&
    (message as { type?: unknown }).type === 'user' &&
    (message as { shouldQuery?: unknown }).shouldQuery === false
  );
}

/** The observed api_retry diagnostic + whether the retry status is terminal. */
interface ApiRetryObservation {
  payload: Record<string, unknown>;
  terminal: boolean;
  terminalMessage: string;
}

/**
 * Read an `api_retry` system frame into a diagnostic observation, or `undefined` for a
 * non-retry message. The diagnostic surfaces as an `agent.status` (`{ status:
 * 'api_retry', attempt, max_retries, error, error_status, retry_delay_ms }`). An
 * authentication failure (status 401/403 or `authentication_failed`) or an endpoint
 * not-found (status 404) is TERMINAL — the model cannot make progress — and carries a
 * human-readable terminal message the caller surfaces as an `agent.error`.
 */
function readApiRetry(message: unknown): ApiRetryObservation | undefined {
  if (message === null || typeof message !== 'object') {
    return undefined;
  }
  const m = message as Record<string, unknown>;
  if (m['type'] !== 'system' || m['subtype'] !== 'api_retry') {
    return undefined;
  }
  const errorStatus = typeof m['error_status'] === 'number' ? (m['error_status'] as number) : null;
  const error = typeof m['error'] === 'string' ? (m['error'] as string) : 'unknown_error';
  const payload: Record<string, unknown> = {
    status: 'api_retry',
    attempt: m['attempt'],
    max_retries: m['max_retries'],
    retry_delay_ms: m['retry_delay_ms'],
    error,
    error_status: errorStatus,
  };
  const isAuth = errorStatus === 401 || errorStatus === 403 || error === 'authentication_failed';
  const isNotFound = errorStatus === 404;
  if (isAuth) {
    return {
      payload,
      terminal: true,
      terminalMessage: `Claude SDK provider authentication failed (${error}, status=${errorStatus ?? 'null'}).`,
    };
  }
  if (isNotFound) {
    return {
      payload,
      terminal: true,
      terminalMessage: `Claude SDK provider endpoint was not found (${error}, status=404); check ANTHROPIC_BASE_URL.`,
    };
  }
  return { payload, terminal: false, terminalMessage: '' };
}

/**
 * Replay one prior conversation entry as a STRUCTURED `SDKUserMessage` for the rebuilt
 * live session, or `undefined` to skip it. The entry's role (`user`/`assistant`) and
 * its FULL content (the original block array — text, thinking, tool_use, tool_result —
 * or a bare string) are carried VERBATIM, so the rebuilt session retains the same
 * thinking / tool context a live session would have held (no plain-text flattening).
 * `shouldQuery: false` marks it append-only: the live CLI records it in the session
 * transcript without running it as a turn.
 *
 * The streaming-input prompt is typed `SDKUserMessage` (outer frame `type: 'user'`),
 * but the inner `message.role` carries the true conversation role — an assistant entry
 * rides an `{ type: 'user', message: { role: 'assistant', ... } }` frame (the same
 * transcript-replay shape the SDK itself uses). Entries with no recoverable role, or
 * with empty content (a blank string / empty block array / system + tool plumbing), are
 * skipped so no empty turn is injected.
 */
function entryToHistoryMessage(entry: SessionStoreEntry): SDKUserMessage | undefined {
  const role = readEntryRole(entry);
  if (role === undefined) {
    return undefined;
  }
  const content = readEntryContent(entry);
  if (content === undefined) {
    return undefined;
  }
  return {
    type: 'user',
    parent_tool_use_id: null,
    message: { role, content },
    shouldQuery: false,
  } as unknown as SDKUserMessage;
}

/** The conversation role of an SDK session entry (`user` / `assistant`), or undefined. */
function readEntryRole(entry: SessionStoreEntry): 'user' | 'assistant' | undefined {
  // The SDK persists entries as `{ type: 'user'|'assistant', message: {...} }` (the
  // CLI transcript shape) — prefer the inner message role, fall back to the entry type.
  const message = (entry as { message?: unknown }).message;
  if (message !== null && typeof message === 'object') {
    const role = (message as { role?: unknown }).role;
    if (role === 'user' || role === 'assistant') {
      return role;
    }
  }
  const type = (entry as { type?: unknown }).type;
  if (type === 'user' || type === 'assistant') {
    return type;
  }
  return undefined;
}

/**
 * The content of an SDK session entry carried VERBATIM for structured replay — a bare
 * string, or the original block array (every block type preserved). Returns `undefined`
 * when there is nothing to replay: a non-string / non-array content, a string that
 * trims to empty, or an empty block array (so no empty message is injected). A block
 * array with ONLY non-text blocks (a tool_use- or tool_result-only turn) is preserved —
 * it carries real tool context even though it has no human-readable text.
 */
function readEntryContent(entry: SessionStoreEntry): string | unknown[] | undefined {
  const message = (entry as { message?: unknown }).message;
  const content =
    message !== null && typeof message === 'object'
      ? (message as { content?: unknown }).content
      : (entry as { content?: unknown }).content;
  if (typeof content === 'string') {
    return content.trim().length === 0 ? undefined : content;
  }
  if (Array.isArray(content)) {
    return content.length === 0 ? undefined : content;
  }
  return undefined;
}

/** The cap on a model id, in chars — a well-formed id is short; a longer one is rejected. */
const MAX_MODEL_ID_LENGTH = 200;

/**
 * Whether a per-turn model override is a WELL-FORMED model id: non-blank, no leading /
 * trailing whitespace, no control characters, and within {@link MAX_MODEL_ID_LENGTH}.
 * This is a structural guard (not a policy allow-list) so the harness never forwards an
 * injection-shaped / oversized / blank id to the live session's `setModel` /
 * `options.model`; the gateway remains the model-policy enforcement point.
 */
function isWellFormedModelId(model: string): boolean {
  if (model.length === 0 || model.length > MAX_MODEL_ID_LENGTH) {
    return false;
  }
  if (model !== model.trim()) {
    return false;
  }
  // Reject ASCII control characters (NUL..US, 0x00-0x1F) and DEL (0x7F): a newline,
  // tab, NUL, etc. would corrupt a model id or inject into the live session config.
  for (let i = 0; i < model.length; i += 1) {
    const code = model.charCodeAt(i);
    if (code <= 0x1f || code === 0x7f) {
      return false;
    }
  }
  return true;
}

/** Truncate a rejected model id for a bounded diagnostic payload (never echo unbounded input). */
function truncateForDiagnostic(model: string): string {
  return model.length <= MAX_MODEL_ID_LENGTH ? model : `${model.slice(0, MAX_MODEL_ID_LENGTH)}…`;
}

/**
 * Best-effort interrupt of a live query handle (fire the model-turn interrupt). Probed
 * defensively (a fake may omit it) and swallows any throw — an interrupt is best-effort
 * and a failure falls through to the force-close.
 */
async function interruptQuery(handle: PersistentQueryHandle): Promise<void> {
  const interrupt = (handle as { interrupt?: unknown }).interrupt;
  if (typeof interrupt !== 'function') {
    return;
  }
  try {
    await (interrupt as () => Promise<void>).call(handle);
  } catch {
    // Best-effort: the close below still tears the session down.
  }
}

/**
 * Force-close a live query handle, best-effort: `close()` terminates the CLI
 * subprocess + frees its transports. Probed defensively (a fake may omit it) and any
 * throw is swallowed — a teardown / interrupt must never raise out of cleanup.
 */
function forceCloseQuery(handle: PersistentQueryHandle): void {
  const close = (handle as { close?: unknown }).close;
  if (typeof close !== 'function') {
    return;
  }
  try {
    (close as () => void).call(handle);
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
