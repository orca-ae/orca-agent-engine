// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { InMemorySkillStore } from '@orca/skill-store';
import {
  RunnerRequestDenied,
  type RunnerTurnAccounting,
} from '../../src/domain/codex-runner-accounting.js';
// Unit spec for the owner-pod session event bridge.
//
// The bridge is the wiring between the runner tunnel (request/response over WS)
// and the session transcript for a self-hosted session whose environment is
// claimed by THIS registry replica (the "owner pod"). The approved model the
// spec pins:
//
//   (a) USER turn — a client `POST /v1/sessions/:id/events` lands on ANY pod and
//       is appended to the transcript (the existing path). The OWNER pod TAILS
//       the transcript for new `user.*` events and sends each to the runner over
//       the runner tunnel as a turn request.
//   (b) AGENT output — the runner streams agent events back over the tunnel; the
//       OWNER pod PERSISTS each to the transcript (single writer,
//       persist-before-forward = the I1 invariant). The existing SSE path tails
//       the transcript to clients — there is no cross-replica HTTP forwarding.
//
// Everything here is driven IN-PROCESS, with no DB / Kafka / real sockets:
//   * the REAL `TunnelRegistry` + `TunnelTransport` (network-free by design),
//   * a FAKE in-process runner — a ws peer that speaks the tunnel frame protocol
//     (decodes the `request` turn frame, answers with `response.head` /
//     `response.body` (NDJSON agent-event lines) / `response.end`), driven the
//     same way the registry's own tunnel-registry spec drives a fake runner,
//   * an in-memory `TranscriptStore` with REAL append/tail ordering semantics, so
//     the spec can assert the single-writer append order AND the persist-before-
//     forward guarantee (an SSE tail only ever observes persisted events).
//
// The bridge → runner turn protocol is Orca-native: a streaming `POST` to the
// runner's turn route carrying the user event JSON; the runner streams agent
// events back as newline-delimited JSON. The agent-event NDJSON line shape and
// the turn route are the cross-component contract asserted here.

import { describe, it, expect, vi } from 'vitest';
import {
  FrameKind,
  decodeFrame,
  encodeFrame,
  decodeBody,
  type Frame,
  type HelloFrame,
  type RequestFrame,
} from '@orca/harness-tunnel';
import type { Event, TranscriptStore, ReadOptions, TailOptions } from '@orca/transcript-store';
import {
  TunnelRegistry,
  type RegistrySession,
  type RegistryWebSocketLike,
} from '../../src/tunnel/tunnel-registry.js';
import {
  httpEventToProto,
  protoToHttpEvent,
  toPublicHttpEvent,
  type HttpEventInput,
} from '../../src/domain/events.js';
import {
  SessionEventBridge,
  SessionEventBridgeManager,
  RUNNER_TURN_PATH,
  RUNNER_CONFIRMATION_PATH,
  RUNNER_CUSTOM_TOOL_RESULT_PATH,
  RUNNER_INTERRUPT_PATH,
  RUNNER_SESSION_HEADER,
  AGENT_EVENT_PRODUCED_BY,
  TOOL_CONFIRMATION_EVENT_KIND,
  INTERRUPT_EVENT_KIND,
  type SessionEventBridgeLogger,
  type BoundSessionResolver,
  type RecoveryObservation,
  type SessionEventBridgeOptions,
} from '../../src/tunnel/session-event-bridge.js';
import {
  RUNNER_REPLAY_PATH,
  RUNNER_RESUME_CURSOR_HEADER,
  COMPLETED_TURN_EVENT_KIND,
} from '../../src/tunnel/session-recovery.js';

const WORKSPACE_ID = 'ws_acme';
const SESSION_ID = 'ses_bridge_1';
const RUNNER_ID = 'runner_token_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

// ── In-memory transcript store with REAL append/tail ordering ──

/**
 * An in-memory {@link TranscriptStore} that honours the contract the bridge
 * relies on: `append` assigns a strictly increasing `seq` per (workspace,
 * session) and returns each event id; `tail` honours the store's FROM-NOW
 * contract — an empty `fromCursor` subscribes at the transcript's CURRENT head
 * (the highest seq present when the tail starts) and delivers only events
 * appended after that point, exactly like the real Kafka (seek to
 * high-watermark), Pulsar ('Latest'), and Postgres (highWatermark+1) backends.
 * An explicit numeric `fromCursor` follows forward from `seq >= fromCursor`
 * (the bounded-resume form). Both are then followed live until the caller's
 * signal aborts.
 *
 * This is a faithful in-process stand-in for the Kafka/Postgres/Pulsar backends
 * — enough to assert single-writer ORDER, the persist-before-forward guarantee
 * (a tail can only ever observe an event after its `append` resolved), AND the
 * from-now guarantee (a `user.*` event already in the transcript when the tail
 * starts is NOT re-delivered). It is intentionally NOT a production store; it
 * lives only in this spec.
 */
class InMemoryTranscriptStore implements TranscriptStore {
  /** Appended events per `${workspaceId}/${sessionId}`, in append order. */
  private readonly logs = new Map<string, Event[]>();
  /** Live tail subscribers per key, woken on each append. */
  private readonly waiters = new Map<string, Set<() => void>>();
  /** Records every append call so a spec can assert the WRITER set + order. */
  readonly appendCalls: Array<{ key: string; events: Event[] }> = [];

  private key(workspaceId: string, sessionId: string): string {
    return `${workspaceId}/${sessionId}`;
  }

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    const key = this.key(workspaceId, sessionId);
    const log = this.logs.get(key) ?? [];
    this.logs.set(key, log);
    this.appendCalls.push({ key, events: events.map((e) => ({ ...e })) });
    const ids: string[] = [];
    for (const event of events) {
      // Assign a strictly increasing 1-based seq, exactly like a real backend
      // would hand back via the message offset.
      const stored: Event = { ...event, seq: log.length + 1 };
      log.push(stored);
      ids.push(stored.id);
    }
    this.wake(key);
    return ids;
  }

  async *read(workspaceId: string, sessionId: string, opts: ReadOptions): AsyncIterable<Event> {
    const log = this.logs.get(this.key(workspaceId, sessionId)) ?? [];
    const from = opts.fromCursor === '' ? 0 : Number(opts.fromCursor);
    for (const event of log) {
      if (event.seq < from) continue;
      if (!subpathMatches(opts.subpath, event.subpath)) continue;
      yield { ...event };
    }
  }

  async *tail(workspaceId: string, sessionId: string, opts: TailOptions): AsyncIterable<Event> {
    const key = this.key(workspaceId, sessionId);
    // From-now contract: an empty cursor starts at the CURRENT head — the
    // highest seq present right now — so events already in the log are skipped
    // and only later appends are delivered. seqs are 1-based and contiguous, so
    // the head seq equals the current log length. An explicit cursor resumes
    // from `seq >= fromCursor` (delivered inclusive). This mirrors the real
    // backends and lets the spec catch a from-beginning replay regression.
    let cursor =
      opts.fromCursor === '' ? (this.logs.get(key)?.length ?? 0) : Number(opts.fromCursor) - 1;
    const signal = opts.signal;
    for (;;) {
      if (signal?.aborted) return;
      const log = this.logs.get(key) ?? [];
      let advanced = false;
      // Snapshot the current head so a re-entrant append during `yield` does not
      // skip rows; the cursor is the lowest seq NOT yet delivered.
      for (const event of log) {
        if (event.seq <= cursor) continue;
        if (!subpathMatches(opts.subpath, event.subpath)) {
          cursor = event.seq;
          continue;
        }
        cursor = event.seq;
        advanced = true;
        yield { ...event };
        if (signal?.aborted) return;
      }
      if (advanced) continue;
      // Park until the next append wakes us (or the signal aborts).
      await this.parkForAppend(key, signal);
    }
  }

  async archive(): Promise<void> {
    /* no-op for the spec */
  }

  async close(): Promise<void> {
    /* no-op for the spec */
  }

  /** Snapshot the persisted log for a session (spec assertions). */
  snapshot(workspaceId: string, sessionId: string): Event[] {
    return (this.logs.get(this.key(workspaceId, sessionId)) ?? []).map((e) => ({ ...e }));
  }

  private wake(key: string): void {
    const set = this.waiters.get(key);
    if (set === undefined) return;
    for (const w of [...set]) w();
  }

  private parkForAppend(key: string, signal: AbortSignal | undefined): Promise<void> {
    return new Promise<void>((resolve) => {
      const set = this.waiters.get(key) ?? new Set<() => void>();
      this.waiters.set(key, set);
      const wake = (): void => {
        set.delete(wake);
        if (signal !== undefined) signal.removeEventListener('abort', wake);
        resolve();
      };
      set.add(wake);
      if (signal !== undefined) {
        if (signal.aborted) {
          wake();
          return;
        }
        signal.addEventListener('abort', wake, { once: true });
      }
    });
  }
}

/** Mirror the store's subpath filter: "" = parent-only, "*" = all, else exact. */
function subpathMatches(filter: string, subpath: string): boolean {
  if (filter === '*') return true;
  if (filter === '') return subpath === '';
  return subpath === filter;
}

// ── Fake in-process runner (a ws peer speaking the tunnel protocol) ──

/**
 * One agent event the fake runner streams back for a turn. The bridge maps each
 * to a transcript event with `producedBy=harness`.
 */
type AgentLine = Record<string, unknown>;

/** What the fake runner does when it receives a turn request for a user event. */
interface TurnScript {
  /** The agent-event NDJSON lines streamed back (each becomes one body frame). */
  lines: AgentLine[];
  /** Optional override of the HTTP status the runner answers with (default 200). */
  status?: number;
  /**
   * When set, the runner sends the head + the given number of body frames and
   * then DROPS the tunnel (mid-stream close) instead of `response.end` — used to
   * exercise the mid-turn tunnel-abort path.
   */
  dropAfter?: number;
}

/**
 * A fake runner that owns the runner end of a tunnel. It registers itself in the
 * REAL `TunnelRegistry` exactly as the production runner-tunnel route does
 * (a `RegistryWebSocketLike` whose `sendText` is the bridge → runner direction),
 * decodes the `request` turn frames the bridge sends, and answers each by
 * streaming `response.head` → N×`response.body` → `response.end` back through the
 * registry's `routeResponseFrame` (the same path the real receive loop drives).
 *
 * Each turn's user event id (read from the request body) selects the scripted
 * response; an unscripted turn gets an empty 200 (no agent events).
 */
class FakeRunner {
  readonly preparations: string[] = [];
  preparationStatus = 200;
  /** Turn requests the bridge sent, in arrival order (decoded request frames). */
  readonly turns: Array<{ frame: RequestFrame; userEvent: Record<string, unknown> }> = [];
  /** Each reverse-lookup resume replay POST the owner pod pushed on connect. */
  readonly replays: Array<{ cursor: string; lines: Array<Record<string, unknown>> }> = [];
  /** Each tool-confirmation POST the bridge pushed to the confirmation route. */
  readonly confirmations: Array<{ frame: RequestFrame; body: Record<string, unknown> }> = [];
  /** Each interrupt POST the bridge pushed to the interrupt route. */
  readonly interrupts: Array<{ frame: RequestFrame; body: Record<string, unknown> }> = [];
  /** Per-tool-use-id ack status the runner answers a confirmation push with (default 200). */
  private readonly confirmationAckStatus = new Map<string, number>();
  /** Ack status the runner answers the NEXT interrupt push with, keyed by call order (default 200). */
  private readonly interruptAckStatus: number[] = [];
  private readonly scripts = new Map<string, TurnScript>();
  /**
   * Turns whose stream stays OPEN (head sent, body/end withheld) until the matching
   * `tool_use_id`'s confirmation push is observed — the "parked on a gated tool"
   * shape. Keyed by user-event id → { toolUseId, lines, frameId }. The held frame
   * id is filled in once the turn POST arrives; the confirmation handler releases it.
   */
  private readonly gatedTurns = new Map<
    string,
    { toolUseId: string; lines: AgentLine[]; frameId?: string }
  >();
  /**
   * Turns whose stream stays OPEN (head sent, body/end withheld) until ANY interrupt
   * push is observed — the "turn blocked on the model, preempted by an interrupt"
   * shape. Keyed by user-event id → { lines, frameId }. The held frame id is filled in
   * once the turn POST arrives; the interrupt handler releases it (streaming `lines`
   * then end), modelling the runner aborting the in-flight turn and the turn's own
   * stream then completing. Distinct from {@link gatedTurns} because an interrupt
   * carries NO routing key — any interrupt push releases a turn parked here.
   */
  private readonly interruptGatedTurns = new Map<
    string,
    { lines: AgentLine[]; frameId?: string }
  >();
  private readonly socket: RunnerSocket;
  private connected = false;

  constructor(
    private readonly registry: TunnelRegistry,
    private readonly runnerId: string,
  ) {
    this.socket = new RunnerSocket((raw) => this.onBridgeFrame(raw));
  }

  /** Script the agent-event lines the runner streams for a given user event id. */
  scriptTurn(userEventId: string, script: TurnScript): this {
    this.scripts.set(userEventId, script);
    return this;
  }

  /**
   * Script a turn that PARKS on a gated tool call: the runner sends the turn's
   * response head but withholds the body + end until it observes a confirmation
   * push for `toolUseId`, then streams `lines` and ends. This is the runner-side
   * shape of "a turn blocked on a human approval" — the turn's tunnel request stays
   * open while the bridge's turn loop is blocked draining it, so the verdict can
   * only be delivered by the bridge's SEPARATE confirmation loop. Proves the two
   * loops run concurrently (no deadlock).
   */
  scriptTurnGatedOnConfirmation(userEventId: string, toolUseId: string, lines: AgentLine[]): this {
    this.gatedTurns.set(userEventId, { toolUseId, lines });
    return this;
  }

  /**
   * Script a turn that BLOCKS on the model and is preempted by an interrupt: the
   * runner sends the turn's response head but withholds the body + end until it
   * observes ANY interrupt push, then streams `lines` and ends. This is the
   * runner-side shape of "a turn blocked on the model, aborted by a `user.interrupt`"
   * — the turn's tunnel request stays open while the bridge's turn loop is blocked
   * draining it, so the interrupt can only be delivered by the bridge's SEPARATE
   * interrupt loop. Proves the two loops run concurrently (no deadlock).
   */
  scriptTurnGatedOnInterrupt(userEventId: string, lines: AgentLine[]): this {
    this.interruptGatedTurns.set(userEventId, { lines });
    return this;
  }

  /**
   * Register the runner session in the shared registry (the dialing runner) and
   * start draining its outbound queue into the runner's frame handler — exactly
   * the role the production runner-tunnel route's sender loop plays. The registry
   * enqueues bridge → runner frames on `session.outboundQueue`; without a drain
   * they would never reach the runner.
   */
  connect(): void {
    const hello: HelloFrame = {
      kind: FrameKind.Hello,
      runnerVersion: '0.1.0-test',
      frameProtocolVersion: 1,
      harnesses: [],
      envs: [],
    };
    const session = this.registry.register(this.runnerId, this.socket, hello, {
      owner: WORKSPACE_ID,
    });
    this.connected = true;
    this.startOutboundDrain(session);
  }

  /** Drop the runner tunnel (deregisters + aborts in-flight, like a real close). */
  disconnect(): void {
    if (!this.connected) return;
    // deregister pushes the `null` stop sentinel onto the outbound queue, which
    // ends the drain loop (and aborts any in-flight request like a real close).
    this.registry.deregister(this.runnerId);
    this.connected = false;
  }

  /** Drain a session's outbound queue to the frame handler, like the route sender. */
  private startOutboundDrain(session: RegistrySession): void {
    void (async () => {
      for (;;) {
        const data = await session.outboundQueue.get();
        if (data === null) {
          return; // stop sentinel (deregister / newest-wins replacement)
        }
        await this.socket.sendText(data);
      }
    })();
  }

  private onBridgeFrame(raw: string): void {
    let frame: Frame;
    try {
      frame = decodeFrame(raw);
    } catch {
      return;
    }
    if (frame.kind === FrameKind.Ping) {
      // Answer keepalives so the registry never declares us dead mid-test.
      void this.registry.get(this.runnerId);
      this.route(encodeFrame({ kind: FrameKind.Pong, ts: frame.ts }));
      return;
    }
    if (frame.kind === FrameKind.RequestCancel) {
      // The bridge cancelled the turn (e.g. it was stopped). End the response so
      // the transport's body iterator completes cleanly.
      this.route(encodeFrame({ kind: FrameKind.ResponseEnd, id: frame.id }));
      return;
    }
    if (frame.kind !== FrameKind.Request) {
      return;
    }
    // Route by path, exactly as a real runner does. The owner pod serves a
    // reverse-lookup resume replay (SessionRecovery) on every connect over the
    // SAME tunnel the bridge drives turns on; the runner answers a replay POST
    // with a 200 ack (it would dedup the body by event id) and does NOT treat it
    // as a turn. Only a turn POST is recorded as a turn + scripted.
    if (frame.path === RUNNER_REPLAY_PATH) {
      this.handleReplay(frame);
      return;
    }
    if (frame.path === RUNNER_CONFIRMATION_PATH || frame.path === RUNNER_CUSTOM_TOOL_RESULT_PATH) {
      this.handleConfirmation(frame);
      return;
    }
    if (frame.path === RUNNER_INTERRUPT_PATH) {
      this.handleInterrupt(frame);
      return;
    }
    if (frame.path === '/v1/runner/skills' || frame.path === '/v1/runner/snapshot') {
      this.preparations.push(frame.path);
      this.route(
        encodeFrame({
          kind: FrameKind.ResponseHead,
          id: frame.id,
          status: this.preparationStatus,
          headers: [],
        }),
      );
      this.route(encodeFrame({ kind: FrameKind.ResponseEnd, id: frame.id }));
      return;
    }
    this.handleTurn(frame);
  }

  /**
   * Script the HTTP status the runner answers a confirmation push with, keyed by
   * the confirmation body's `tool_use_id`. Lets a spec exercise the bridge's
   * contained non-2xx path (the verdict self-heals on the next reconnect).
   */
  scriptConfirmationAck(toolUseId: string, status: number): this {
    this.confirmationAckStatus.set(toolUseId, status);
    return this;
  }

  /**
   * Ack a tool-confirmation push — record the decoded verdict body, then answer the
   * scripted status (default 200, no body), exactly as the real runner's
   * confirmation route does (it resolves the parked approval keyed by `tool_use_id`
   * and acks). A confirmation is NOT a turn: the runner streams no agent events
   * back, so this never produces a body the bridge would persist.
   */
  private handleConfirmation(frame: RequestFrame): void {
    const body = decodeRequestBody(frame);
    this.confirmations.push({ frame, body });
    const toolUseId =
      typeof body['custom_tool_use_id'] === 'string'
        ? body['custom_tool_use_id']
        : typeof body['tool_use_id'] === 'string'
          ? body['tool_use_id']
          : '';
    const status = this.confirmationAckStatus.get(toolUseId) ?? 200;
    this.route(
      encodeFrame({
        kind: FrameKind.ResponseHead,
        id: frame.id,
        status,
        headers: [['content-type', 'application/json']],
      }),
    );
    this.route(encodeFrame({ kind: FrameKind.ResponseEnd, id: frame.id }));
    // Release any turn parked on this verdict: now stream its withheld lines + end,
    // so the bridge's turn loop (blocked draining the open turn stream) completes.
    for (const [userEventId, gated] of this.gatedTurns) {
      if (gated.toolUseId !== toolUseId || gated.frameId === undefined) {
        continue;
      }
      const turnFrameId = gated.frameId;
      this.gatedTurns.delete(userEventId);
      for (const line of gated.lines) {
        this.route(
          encodeFrame({
            kind: FrameKind.ResponseBody,
            id: turnFrameId,
            body: `${JSON.stringify(line)}\n`,
            encoding: 'utf-8',
          }),
        );
      }
      this.route(encodeFrame({ kind: FrameKind.ResponseEnd, id: turnFrameId }));
    }
  }

  /**
   * Script the HTTP status the runner answers the NEXT interrupt push with (consumed
   * in call order). Lets a spec exercise the bridge's contained non-2xx path (the
   * interrupt self-heals on the next reconnect). Unlike a confirmation, an interrupt
   * carries no `tool_use_id` to key on, so scripted statuses are consumed FIFO.
   */
  scriptInterruptAck(status: number): this {
    this.interruptAckStatus.push(status);
    return this;
  }

  /**
   * Ack a `user.interrupt` push — record the decoded body, then answer the scripted
   * status (default 200, no body), exactly as the real runner's interrupt route does
   * (it aborts the in-flight turn keyed to the session and acks). An interrupt is NOT
   * a turn: the runner streams no agent events back on the interrupt request itself,
   * so this never produces a body the bridge would persist. If a turn is parked on an
   * interrupt ({@link scriptTurnGatedOnInterrupt}), this releases it — streaming its
   * withheld lines + end — modelling the aborted turn's own stream completing.
   */
  private handleInterrupt(frame: RequestFrame): void {
    const body = decodeRequestBody(frame);
    this.interrupts.push({ frame, body });
    const status = this.interruptAckStatus.shift() ?? 200;
    this.route(
      encodeFrame({
        kind: FrameKind.ResponseHead,
        id: frame.id,
        status,
        headers: [['content-type', 'application/json']],
      }),
    );
    this.route(encodeFrame({ kind: FrameKind.ResponseEnd, id: frame.id }));
    // Release any turn parked on an interrupt: now stream its withheld lines + end,
    // so the bridge's turn loop (blocked draining the open turn stream) completes —
    // exactly as the runner would abort the in-flight turn and let its stream end.
    for (const [userEventId, gated] of this.interruptGatedTurns) {
      if (gated.frameId === undefined) {
        continue;
      }
      const turnFrameId = gated.frameId;
      this.interruptGatedTurns.delete(userEventId);
      for (const line of gated.lines) {
        this.route(
          encodeFrame({
            kind: FrameKind.ResponseBody,
            id: turnFrameId,
            body: `${JSON.stringify(line)}\n`,
            encoding: 'utf-8',
          }),
        );
      }
      this.route(encodeFrame({ kind: FrameKind.ResponseEnd, id: turnFrameId }));
    }
  }

  /**
   * Ack a reverse-lookup resume replay POST (200, no body) — the runner dedups the
   * body by event id. The decoded NDJSON lines + cursor header are recorded so a
   * spec can assert the owner pushed the persisted events forward on connect.
   */
  private handleReplay(frame: RequestFrame): void {
    const headers = new Map((frame.headers ?? []).map(([k, v]) => [k.toLowerCase(), v]));
    const cursor = headers.get(RUNNER_RESUME_CURSOR_HEADER.toLowerCase()) ?? '';
    this.replays.push({ cursor, lines: decodeNdjsonBody(frame) });
    this.route(
      encodeFrame({
        kind: FrameKind.ResponseHead,
        id: frame.id,
        status: 200,
        headers: [['content-type', 'application/json']],
      }),
    );
    this.route(encodeFrame({ kind: FrameKind.ResponseEnd, id: frame.id }));
  }

  /**
   * Answer one turn request: head, one body frame per scripted agent-event line,
   * then end. When the script has a `dropAfter`, the runner streams that many
   * body frames and then drops the tunnel WITHOUT an end frame — but the drop is
   * deferred to a later macrotask so the bridge first consumes + persists the
   * already-streamed chunks (persist-before-forward survives the drop), exactly
   * as a real runner would crash some time after emitting its last good chunk.
   */
  private handleTurn(frame: RequestFrame): void {
    const userEvent = decodeRequestBody(frame);
    this.turns.push({ frame, userEvent });
    const userEventId = typeof userEvent['id'] === 'string' ? userEvent['id'] : '';
    // A gated turn: send the head, then HOLD the body/end open until the matching
    // confirmation push is observed (released in handleConfirmation). The bridge's
    // turn loop blocks draining this open stream — only the separate confirmation
    // loop can deliver the verdict that releases it.
    const gated = this.gatedTurns.get(userEventId);
    if (gated !== undefined) {
      gated.frameId = frame.id;
      this.route(
        encodeFrame({
          kind: FrameKind.ResponseHead,
          id: frame.id,
          status: 200,
          headers: [['content-type', 'application/x-ndjson']],
        }),
      );
      return; // body + end withheld until the verdict lands
    }
    // An interrupt-gated turn: send the head, then HOLD the body/end open until ANY
    // interrupt push is observed (released in handleInterrupt). The bridge's turn loop
    // blocks draining this open stream — only the separate interrupt loop can deliver
    // the `user.interrupt` that preempts it.
    const interruptGated = this.interruptGatedTurns.get(userEventId);
    if (interruptGated !== undefined) {
      interruptGated.frameId = frame.id;
      this.route(
        encodeFrame({
          kind: FrameKind.ResponseHead,
          id: frame.id,
          status: 200,
          headers: [['content-type', 'application/x-ndjson']],
        }),
      );
      return; // body + end withheld until the interrupt lands
    }
    const script = this.scripts.get(userEventId) ?? { lines: [] };
    const status = script.status ?? 200;
    this.route(
      encodeFrame({
        kind: FrameKind.ResponseHead,
        id: frame.id,
        status,
        headers: [['content-type', 'application/x-ndjson']],
      }),
    );
    const dropAfter = script.dropAfter;
    let sent = 0;
    for (const line of script.lines) {
      if (dropAfter !== undefined && sent >= dropAfter) {
        // Simulate a mid-stream runner crash: drop the tunnel without ending,
        // but only AFTER the bridge has had a chance to persist the chunks sent
        // so far (defer the close to a later macrotask).
        setTimeout(() => this.disconnect(), 5);
        return;
      }
      this.route(
        encodeFrame({
          kind: FrameKind.ResponseBody,
          id: frame.id,
          body: `${JSON.stringify(line)}\n`,
          encoding: 'utf-8',
        }),
      );
      sent += 1;
    }
    this.route(encodeFrame({ kind: FrameKind.ResponseEnd, id: frame.id }));
  }

  /** Route a runner→server frame through the registry's response reassembly. */
  private route(raw: string): void {
    const frame = decodeFrame(raw);
    if (
      frame.kind === FrameKind.ResponseHead ||
      frame.kind === FrameKind.ResponseBody ||
      frame.kind === FrameKind.ResponseEnd
    ) {
      this.registry.routeResponseFrame(this.runnerId, frame);
    }
  }
}

/**
 * The runner side of a tunnel as a {@link RegistryWebSocketLike}: `sendText` is
 * the bridge → runner direction (the registry drains the session's outbound
 * queue into it), delivered synchronously to the runner's frame handler.
 */
class RunnerSocket implements RegistryWebSocketLike {
  closed: { code: number; reason: string } | undefined;
  constructor(private readonly onFrame: (raw: string) => void) {}

  async sendText(data: string): Promise<void> {
    // Deliver on a microtask so the bridge's `await sendText` resolves before the
    // response frames are routed — mirrors a real async socket.
    queueMicrotask(() => this.onFrame(data));
  }

  receiveText(): Promise<string> {
    return new Promise<string>(() => {
      /* the registry never pulls inbound; the fake runner pushes responses */
    });
  }

  async close(opts?: { code?: number; reason?: string }): Promise<void> {
    this.closed = { code: opts?.code ?? 1000, reason: opts?.reason ?? '' };
  }
}

/** Decode a `request` frame body into the user-event JSON the bridge sent. */
function decodeRequestBody(frame: RequestFrame): Record<string, unknown> {
  if (frame.body === null || frame.body === undefined) return {};
  const bytes = decodeBody(frame.body, frame.encoding ?? 'utf-8');
  try {
    const parsed = JSON.parse(Buffer.from(bytes).toString('utf8')) as unknown;
    return parsed !== null && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

/** Decode a replay POST's NDJSON body into the per-line JSON objects. */
function decodeNdjsonBody(frame: RequestFrame): Array<Record<string, unknown>> {
  if (frame.body === null || frame.body === undefined) return [];
  const bytes = decodeBody(frame.body, frame.encoding ?? 'utf-8');
  const text = Buffer.from(bytes).toString('utf8');
  const out: Array<Record<string, unknown>> = [];
  for (const line of text.split('\n')) {
    const trimmed = line.trim();
    if (trimmed.length === 0) continue;
    const parsed = JSON.parse(trimmed) as unknown;
    if (parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)) {
      out.push(parsed as Record<string, unknown>);
    }
  }
  return out;
}

// ── Helpers to seed user events + read agent events ──

let userEventCounter = 0;

/** Append a `user.message` event the way the existing client POST path does. */
async function appendUserMessage(store: TranscriptStore, text: string): Promise<Event> {
  userEventCounter += 1;
  const proto = httpEventToProto({
    workspaceId: WORKSPACE_ID,
    sessionId: SESSION_ID,
    producedBy: 'client',
    idempotencyKey: `req-${userEventCounter}:0`,
    input: {
      id: `evt_user_${userEventCounter}`,
      type: 'user.message',
      content: [{ type: 'text', text: text }],
    },
  });
  const [id] = await store.append(WORKSPACE_ID, SESSION_ID, [proto]);
  return { ...proto, id: id ?? proto.id };
}

let toolConfirmationCounter = 0;

/**
 * Append a `user.tool_confirmation` verdict the way the client POST path does — a
 * public `user.*` client event keyed on `tool_use_id` carrying the authoritative
 * managed-agents-2026-04-01 `result` field (`'allow' | 'deny'`; canonicalized by
 * `validateClientEvent` in `src/domain/events.ts`, which also accepts the
 * deprecated boolean `approved` alias). The bridge must route this to the
 * confirmation route, NOT drive it as a turn.
 */
async function appendToolConfirmation(
  store: TranscriptStore,
  toolUseId: string,
  result: 'allow' | 'deny',
): Promise<Event> {
  toolConfirmationCounter += 1;
  const proto = httpEventToProto({
    workspaceId: WORKSPACE_ID,
    sessionId: SESSION_ID,
    producedBy: 'client',
    idempotencyKey: `conf-${toolConfirmationCounter}:0`,
    input: {
      id: `evt_conf_${toolConfirmationCounter}`,
      type: TOOL_CONFIRMATION_EVENT_KIND,
      tool_use_id: toolUseId,
      result,
    },
  });
  const [id] = await store.append(WORKSPACE_ID, SESSION_ID, [proto]);
  return { ...proto, id: id ?? proto.id };
}

let userInterruptCounter = 0;

/**
 * Append a `user.interrupt` the way the client POST path does — a public `user.*`
 * client event carrying NO routing key and NO parameters (a bare control signal that
 * aborts whatever turn is in flight). The bridge must route this to the interrupt
 * route, NOT drive it as a turn.
 */
async function appendUserInterrupt(store: TranscriptStore): Promise<Event> {
  userInterruptCounter += 1;
  const proto = httpEventToProto({
    workspaceId: WORKSPACE_ID,
    sessionId: SESSION_ID,
    producedBy: 'client',
    idempotencyKey: `intr-${userInterruptCounter}:0`,
    input: {
      id: `evt_intr_${userInterruptCounter}`,
      type: INTERRUPT_EVENT_KIND,
    },
  });
  const [id] = await store.append(WORKSPACE_ID, SESSION_ID, [proto]);
  return { ...proto, id: id ?? proto.id };
}

let bridgeAgentEventCounter = 0;

/** Append one agent (`producedBy=harness`) event of a given kind. */
async function appendAgentEvent(store: TranscriptStore, kind: string): Promise<Event> {
  bridgeAgentEventCounter += 1;
  const proto = httpEventToProto({
    workspaceId: WORKSPACE_ID,
    sessionId: SESSION_ID,
    producedBy: AGENT_EVENT_PRODUCED_BY,
    idempotencyKey: '',
    input: { id: `evt_agent_${bridgeAgentEventCounter}`, type: kind },
  });
  const [id] = await store.append(WORKSPACE_ID, SESSION_ID, [proto]);
  return { ...proto, id: id ?? proto.id };
}

/** Decode the JSON payload of a stored transcript event. */
function payloadJson(event: Event): Record<string, unknown> {
  return JSON.parse(Buffer.from(event.payload).toString('utf8')) as Record<string, unknown>;
}

/** One recorded log call: the structured object and the optional message. */
interface LogCall {
  obj: unknown;
  msg: string | undefined;
}

/** A logger that records every call so the spec can assert error reporting. */
function recordingLogger(): SessionEventBridgeLogger & {
  warns: LogCall[];
  errors: LogCall[];
} {
  const warns: LogCall[] = [];
  const errors: LogCall[] = [];
  return {
    warns,
    errors,
    info() {
      /* ignore */
    },
    warn(obj, msg) {
      warns.push({ obj, msg });
    },
    error(obj, msg) {
      errors.push({ obj, msg });
    },
  };
}

/** Poll `predicate` on each macrotask until true or the deadline elapses. */
async function waitFor(predicate: () => boolean, timeoutMs = 1000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return;
    if (Date.now() > deadline) {
      throw new Error('waitFor: condition not met within timeout');
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 1));
  }
}

/** Build a started bridge + its collaborators, registering a connected runner. */
function buildBridge(
  over: {
    store?: InMemoryTranscriptStore;
    logger?: SessionEventBridgeLogger;
    saveHarnessState?: (state: unknown) => Promise<void>;
    resources?: SessionEventBridgeOptions['resources'];
    beforeTurn?: SessionEventBridgeOptions['beforeTurn'];
    accounting?: RunnerTurnAccounting;
    ownsSession?: () => Promise<boolean>;
  } = {},
): {
  bridge: SessionEventBridge;
  store: InMemoryTranscriptStore;
  registry: TunnelRegistry;
  runner: FakeRunner;
} {
  const store = over.store ?? new InMemoryTranscriptStore();
  const registry = new TunnelRegistry();
  const runner = new FakeRunner(registry, RUNNER_ID);
  const bridge = new SessionEventBridge({
    workspaceId: WORKSPACE_ID,
    sessionId: SESSION_ID,
    runnerId: RUNNER_ID,
    store,
    registry,
    ...(over.logger !== undefined ? { logger: over.logger } : {}),
    ...(over.saveHarnessState ? { saveHarnessState: over.saveHarnessState } : {}),
    ...(over.resources ? { resources: over.resources } : {}),
    ...(over.beforeTurn ? { beforeTurn: over.beforeTurn } : {}),
    ...(over.accounting ? { accounting: over.accounting } : {}),
    ...(over.ownsSession ? { ownsSession: over.ownsSession } : {}),
  });
  return { bridge, store, registry, runner };
}

// ── 1. user-event tail → runner turn request ──

describe('session-event-bridge — user events tail to the runner', () => {
  it('does not forward turns or controls belonging to another owner', async () => {
    let owned = false;
    let checks = 0;
    const { bridge, store, runner } = buildBridge({
      ownsSession: async () => {
        checks++;
        return owned;
      },
    });
    runner.connect();
    bridge.start();
    try {
      await appendUserMessage(store, 'belongs elsewhere');
      await appendToolConfirmation(store, 'tool_other', 'deny');
      await appendUserInterrupt(store);
      await waitFor(() => checks >= 3);
      owned = true;
      const accepted = await appendUserMessage(store, 'owned now');
      await waitFor(() => runner.turns.length > 0);
      expect(runner.turns.map((turn) => turn.userEvent['id'])).toEqual([accepted.id]);
      expect(runner.confirmations).toEqual([]);
      expect(runner.interrupts).toEqual([]);
    } finally {
      await bridge.stop();
    }
  });

  it('retains turns and controls across an unavailable ownership read', async () => {
    let available = false;
    let checks = 0;
    const { bridge, store, runner } = buildBridge({
      ownsSession: async () => {
        checks++;
        if (!available) throw new Error('routing unavailable');
        return true;
      },
    });
    runner.connect();
    bridge.start();
    try {
      const pending = await appendUserMessage(store, 'retry this turn');
      await appendToolConfirmation(store, 'tool_pending', 'deny');
      await appendUserInterrupt(store);
      await waitFor(() => checks >= 3);
      expect(runner.turns).toEqual([]);
      expect(runner.confirmations).toEqual([]);
      expect(runner.interrupts).toEqual([]);
      available = true;
      await waitFor(
        () =>
          runner.turns.length === 1 &&
          runner.confirmations.length === 1 &&
          runner.interrupts.length === 1,
      );
      expect(runner.turns.map((turn) => turn.userEvent['id'])).toEqual([pending.id]);
    } finally {
      await bridge.stop();
    }
  });

  it('cancels ownership retry when the bridge stops', async () => {
    let checks = 0;
    const { bridge, store, runner } = buildBridge({
      ownsSession: async () => {
        checks++;
        throw new Error('routing unavailable');
      },
    });
    runner.connect();
    bridge.start();
    await appendUserMessage(store, 'pending until stopped');
    await waitFor(() => checks > 0);
    await bridge.stop();
    expect(bridge.running).toBe(false);
    expect(runner.turns).toEqual([]);
  });

  it('sends a turn request to the runner for each tailed user.* event', async () => {
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    bridge.start();
    try {
      const u1 = await appendUserMessage(store, 'hello');
      const u2 = await appendUserMessage(store, 'again');

      await waitFor(() => runner.turns.length === 2);

      // Each turn is a streaming POST to the Orca-native runner turn route,
      // carrying the user event JSON + the session id header.
      for (const turn of runner.turns) {
        expect(turn.frame.method).toBe('POST');
        expect(turn.frame.path).toBe(RUNNER_TURN_PATH);
        const headers = new Map((turn.frame.headers ?? []).map(([k, v]) => [k.toLowerCase(), v]));
        expect(headers.get(RUNNER_SESSION_HEADER.toLowerCase())).toBe(SESSION_ID);
        expect(headers.get('content-type')).toBe('application/json');
      }
      // In tail (append) order.
      expect(runner.turns[0]!.userEvent['id']).toBe(u1.id);
      expect(runner.turns[1]!.userEvent['id']).toBe(u2.id);
      expect(runner.turns[0]!.userEvent['type']).toBe('user.message');
      // Canonical managed-agents-2026-04-01 shape: `content` is a non-empty array
      // of content parts, not a bare string (see `validateClientEvent` in
      // src/domain/events.ts).
      expect(payloadOfTurn(runner.turns[0]!.userEvent)).toEqual([{ type: 'text', text: 'hello' }]);
    } finally {
      await bridge.stop();
    }
  });

  it('does NOT send a turn request for non-user (agent / internal) events', async () => {
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    bridge.start();
    try {
      // An agent event already in the transcript (e.g. persisted by the bridge)
      // and an internal harness.* event must never be forwarded as a turn.
      const agent = httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'harness',
        idempotencyKey: '',
        input: { id: 'evt_agent_x', type: 'response.output_text.delta', content: 'x' },
      });
      const internal = httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'harness',
        idempotencyKey: '',
        input: { id: 'evt_internal_x', type: 'harness.state', content: 'x' },
      });
      await store.append(WORKSPACE_ID, SESSION_ID, [agent, internal]);
      const u1 = await appendUserMessage(store, 'real turn');

      await waitFor(() => runner.turns.length === 1);
      expect(runner.turns[0]!.userEvent['id']).toBe(u1.id);
      // Give any erroneous extra turn a chance to surface, then assert none did.
      await new Promise<void>((resolve) => setTimeout(resolve, 10));
      expect(runner.turns.length).toBe(1);
    } finally {
      await bridge.stop();
    }
  });
});

/** Read the `content` field a fake-runner turn carried for a user.message. */
function payloadOfTurn(userEvent: Record<string, unknown>): unknown {
  return userEvent['content'];
}

// ── 2. agent-event runner → persist → SSE ──

describe('session-event-bridge — agent events persist to the transcript', () => {
  it('persists each streamed agent event with producedBy=harness, in order', async () => {
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    const u1Id = 'evt_user_persist';
    runner.scriptTurn(u1Id, {
      lines: [
        { type: 'response.created' },
        { type: 'response.output_text.delta', content: 'Hel' },
        { type: 'response.output_text.delta', content: 'lo' },
        { type: 'response.completed' },
      ],
    });
    bridge.start();
    try {
      const proto = httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: { id: u1Id, type: 'user.message', content: [{ type: 'text', text: 'hi' }] },
      });
      await store.append(WORKSPACE_ID, SESSION_ID, [proto]);

      // The user event is seq 1; the four agent events follow as seq 2..5.
      await waitFor(() => store.snapshot(WORKSPACE_ID, SESSION_ID).length === 5);
      const log = store.snapshot(WORKSPACE_ID, SESSION_ID);
      const agentEvents = log.filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY);
      expect(agentEvents.map((e) => e.kind)).toEqual([
        'response.created',
        'response.output_text.delta',
        'response.output_text.delta',
        'response.completed',
      ]);
      // Strictly increasing seq, contiguous after the user event (single writer).
      expect(agentEvents.map((e) => e.seq)).toEqual([2, 3, 4, 5]);
      expect(payloadJson(agentEvents[1]!)['content']).toBe('Hel');
      // Each agent event carries a fresh evt_ id and harness provenance.
      for (const e of agentEvents) {
        expect(e.producedBy).toBe('harness');
        expect(e.id.startsWith('evt_')).toBe(true);
      }
    } finally {
      await bridge.stop();
    }
  });

  it('makes persisted agent events observable on an SSE-style transcript tail', async () => {
    // This is the persist-before-forward (I1) guarantee from the client's view:
    // the SSE path tails the SAME transcript, so a tail started before the turn
    // sees exactly the persisted agent events, in persisted order — the bridge
    // never writes to a client socket directly.
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    const uId = 'evt_user_sse';
    runner.scriptTurn(uId, {
      lines: [{ type: 'response.created' }, { type: 'response.completed' }],
    });

    const ac = new AbortController();
    const seen: string[] = [];
    const tailDone = (async () => {
      for await (const event of store.tail(WORKSPACE_ID, SESSION_ID, {
        fromCursor: '',
        subpath: '',
        signal: ac.signal,
      })) {
        seen.push(event.kind);
        if (event.kind === 'response.completed') break;
      }
    })();

    bridge.start();
    try {
      const proto = httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: { id: uId, type: 'user.message', content: [{ type: 'text', text: 'hi' }] },
      });
      await store.append(WORKSPACE_ID, SESSION_ID, [proto]);
      await tailDone;
      ac.abort();
      // The client tail observed the user event then the two agent events, in
      // the exact persisted order.
      expect(seen).toEqual(['user.message', 'response.created', 'response.completed']);
    } finally {
      ac.abort();
      await bridge.stop();
    }
  });
});

// ── 3. single-writer ordering ──

describe('session-event-bridge — single-writer ordering', () => {
  it('appends every agent event through the bridge (the single writer) in turn order', async () => {
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    const a = 'evt_user_a';
    const b = 'evt_user_b';
    runner.scriptTurn(a, { lines: [{ type: 'a.1' }, { type: 'a.2' }] });
    runner.scriptTurn(b, { lines: [{ type: 'b.1' }, { type: 'b.2' }] });
    bridge.start();
    try {
      await store.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: { id: a, type: 'user.message', content: [{ type: 'text', text: 'a' }] },
        }),
      ]);
      // Wait for turn A to fully persist before enqueueing turn B so the two
      // turns can't interleave — the owner pod processes user turns serially.
      await waitFor(
        () =>
          store
            .snapshot(WORKSPACE_ID, SESSION_ID)
            .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY).length === 2,
      );
      await store.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: { id: b, type: 'user.message', content: [{ type: 'text', text: 'b' }] },
        }),
      ]);
      await waitFor(
        () =>
          store
            .snapshot(WORKSPACE_ID, SESSION_ID)
            .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY).length === 4,
      );

      const agentKinds = store
        .snapshot(WORKSPACE_ID, SESSION_ID)
        .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY)
        .map((e) => e.kind);
      expect(agentKinds).toEqual(['a.1', 'a.2', 'b.1', 'b.2']);

      // Every agent-event append went through this bridge — assert the writer
      // set: each append batch the store recorded is for THIS session key.
      const agentAppendBatches = store.appendCalls.filter((c) =>
        c.events.every((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY),
      );
      for (const batch of agentAppendBatches) {
        expect(batch.key).toBe(`${WORKSPACE_ID}/${SESSION_ID}`);
      }
    } finally {
      await bridge.stop();
    }
  });
});

// ── 4. persist-before-forward (I1) at the bridge boundary ──

describe('session-event-bridge — persist-before-forward (I1)', () => {
  it('awaits append for each agent event before consuming the next stream chunk', async () => {
    // Wrap the store so append is observably ASYNC and records the interleave of
    // "append started" vs "append resolved". I1 requires that the Nth agent
    // event is fully persisted (append resolved) before the (N+1)th is — the
    // bridge is a strict, in-order single writer, never a fire-and-forget pump.
    const inner = new InMemoryTranscriptStore();
    const order: string[] = [];
    let appendSeq = 0;
    const gated: TranscriptStore = {
      async append(workspaceId, sessionId, events) {
        const label =
          events.length === 1 && events[0]!.producedBy === AGENT_EVENT_PRODUCED_BY
            ? events[0]!.kind
            : 'other';
        if (label !== 'other') {
          appendSeq += 1;
          order.push(`start:${label}:${appendSeq}`);
          // Yield the loop so a non-awaited pump would race ahead here.
          await new Promise<void>((resolve) => setTimeout(resolve, 5));
        }
        const ids = await inner.append(workspaceId, sessionId, events);
        if (label !== 'other') order.push(`done:${label}`);
        return ids;
      },
      read: inner.read.bind(inner),
      tail: inner.tail.bind(inner),
      archive: inner.archive.bind(inner),
      close: inner.close.bind(inner),
    };

    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const uId = 'evt_user_i1';
    runner.scriptTurn(uId, {
      lines: [{ type: 'first' }, { type: 'second' }, { type: 'third' }],
    });
    const bridge = new SessionEventBridge({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      store: gated,
      registry,
    });
    bridge.start();
    try {
      await gated.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: { id: uId, type: 'user.message', content: [{ type: 'text', text: 'go' }] },
        }),
      ]);
      await waitFor(() => order.filter((o) => o.startsWith('done:')).length === 3);
      // Each append fully resolves before the next one starts: strict
      // persist-before-forward, no overlap.
      expect(order).toEqual([
        'start:first:1',
        'done:first',
        'start:second:2',
        'done:second',
        'start:third:3',
        'done:third',
      ]);
    } finally {
      await bridge.stop();
    }
  });
});

// ── 5. error / edge paths ──

describe('session-event-bridge — error paths', () => {
  it('skips a malformed (non-JSON / typeless) agent line without aborting the turn', async () => {
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    const uId = 'evt_user_malformed';
    // Inject a body frame that is not valid JSON between two good events by
    // scripting a line that the runner emits verbatim — modelled here as an
    // object the bridge will see as a raw line; the bridge must drop the bad one
    // and persist the two valid ones.
    runner.scriptTurn(uId, {
      lines: [{ type: 'good.1' }, { __raw: 'not-json' }, { noType: true }, { type: 'good.2' }],
    });
    bridge.start();
    try {
      await store.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: { id: uId, type: 'user.message', content: [{ type: 'text', text: 'hi' }] },
        }),
      ]);
      await waitFor(
        () =>
          store
            .snapshot(WORKSPACE_ID, SESSION_ID)
            .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY).length === 2,
      );
      const kinds = store
        .snapshot(WORKSPACE_ID, SESSION_ID)
        .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY)
        .map((e) => e.kind);
      expect(kinds).toEqual(['good.1', 'good.2']);
    } finally {
      await bridge.stop();
    }
  });

  it('does not crash the bridge when the runner is offline at turn time (logs + continues)', async () => {
    const logger = recordingLogger();
    const { bridge, store, runner } = buildBridge({ logger });
    // Runner is NOT connected — the first turn cannot be delivered.
    bridge.start();
    try {
      const u1 = await appendUserMessage(store, 'while offline');
      await waitFor(() => logger.warns.length + logger.errors.length >= 1);
      const reported = [...logger.warns, ...logger.errors];
      expect(reported.some((r) => JSON.stringify(r.obj).includes(u1.id))).toBe(true);

      // Now the runner connects and a later user event IS delivered: the bridge
      // kept tailing past the failed turn.
      runner.connect();
      const u2 = await appendUserMessage(store, 'after connect');
      await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === u2.id));
    } finally {
      await bridge.stop();
    }
  });

  it('reports a mid-turn tunnel drop and resumes on the next user event', async () => {
    const logger = recordingLogger();
    const { bridge, store, runner } = buildBridge({ logger });
    runner.connect();
    const uId = 'evt_user_drop';
    // Stream one good event, persist it, then drop the tunnel before end.
    runner.scriptTurn(uId, {
      lines: [{ type: 'partial.1' }, { type: 'partial.2' }],
      dropAfter: 1,
    });
    bridge.start();
    try {
      await store.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: { id: uId, type: 'user.message', content: [{ type: 'text', text: 'go' }] },
        }),
      ]);
      // The first agent event persisted before the drop (persist-before-forward
      // means whatever was streamed-and-persisted survives the crash).
      await waitFor(() =>
        store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .some((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY && e.kind === 'partial.1'),
      );
      // The abort was reported (a warn or error mentioning the turn / session).
      await waitFor(() => logger.warns.length + logger.errors.length >= 1);

      // A reconnect + new user event resumes turns (the bridge survived).
      runner.connect();
      const uId2 = 'evt_user_resume';
      runner.scriptTurn(uId2, { lines: [{ type: 'resumed.1' }] });
      await store.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: { id: uId2, type: 'user.message', content: [{ type: 'text', text: 'again' }] },
        }),
      ]);
      await waitFor(() =>
        store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .some((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY && e.kind === 'resumed.1'),
      );
    } finally {
      await bridge.stop();
    }
  });

  it('reports a non-2xx turn response and keeps tailing', async () => {
    const logger = recordingLogger();
    const { bridge, store, runner } = buildBridge({ logger });
    runner.connect();
    const uId = 'evt_user_500';
    runner.scriptTurn(uId, { lines: [{ type: 'ignored' }], status: 500 });
    bridge.start();
    try {
      await store.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: { id: uId, type: 'user.message', content: [{ type: 'text', text: 'go' }] },
        }),
      ]);
      await waitFor(() => logger.warns.length + logger.errors.length >= 1);
      // A non-2xx turn does NOT persist the streamed body as agent events.
      await new Promise<void>((resolve) => setTimeout(resolve, 10));
      expect(
        store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY).length,
      ).toBe(0);
    } finally {
      await bridge.stop();
    }
  });

  it('continues the turn when a single agent-event append fails (logs, persists the rest)', async () => {
    const inner = new InMemoryTranscriptStore();
    let failOnKind: string | null = 'boom';
    const flaky: TranscriptStore = {
      async append(workspaceId, sessionId, events) {
        if (
          events.length === 1 &&
          events[0]!.producedBy === AGENT_EVENT_PRODUCED_BY &&
          events[0]!.kind === failOnKind
        ) {
          failOnKind = null; // fail exactly once
          throw new Error('simulated append failure');
        }
        return inner.append(workspaceId, sessionId, events);
      },
      read: inner.read.bind(inner),
      tail: inner.tail.bind(inner),
      archive: inner.archive.bind(inner),
      close: inner.close.bind(inner),
    };
    const logger = recordingLogger();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const uId = 'evt_user_flaky';
    runner.scriptTurn(uId, {
      lines: [{ type: 'before' }, { type: 'boom' }, { type: 'after' }],
    });
    const bridge = new SessionEventBridge({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      store: flaky,
      registry,
      logger,
    });
    bridge.start();
    try {
      await flaky.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: { id: uId, type: 'user.message', content: [{ type: 'text', text: 'go' }] },
        }),
      ]);
      await waitFor(
        () =>
          inner
            .snapshot(WORKSPACE_ID, SESSION_ID)
            .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY).length === 2,
      );
      const kinds = inner
        .snapshot(WORKSPACE_ID, SESSION_ID)
        .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY)
        .map((e) => e.kind);
      // The failed event ("boom") is dropped; the bridge persisted the rest.
      expect(kinds).toEqual(['before', 'after']);
      expect(logger.errors.length + logger.warns.length).toBeGreaterThanOrEqual(1);
    } finally {
      await bridge.stop();
    }
  });
});

// ── 6. lifecycle: start is idempotent, stop halts tailing ──

describe('session-event-bridge — lifecycle', () => {
  it('start is idempotent and stop halts further turns', async () => {
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    bridge.start();
    bridge.start(); // second start is a no-op, not a second tail loop
    try {
      const u1 = await appendUserMessage(store, 'one');
      await waitFor(() => runner.turns.length === 1);
      expect(runner.turns.length).toBe(1);
      expect(runner.turns[0]!.userEvent['id']).toBe(u1.id);
    } finally {
      await bridge.stop();
    }
    // After stop, a new user event is NOT forwarded.
    await appendUserMessage(store, 'after stop');
    await new Promise<void>((resolve) => setTimeout(resolve, 20));
    expect(runner.turns.length).toBe(1);
  });

  it('stop is safe to call without start and is idempotent', async () => {
    const { bridge } = buildBridge();
    await bridge.stop();
    await bridge.stop();
    expect(bridge.running).toBe(false);
  });
});

// ── 6b. catch-up + no-replay: which pre-existing user turns (re)start drives ──
//
// The single-writer turn guarantee depends on the boundary the bridge draws at
// start: it CATCHES UP on un-driven user turns (a `user.*` event with no agent
// event after it — a pre-connect first turn, or one left un-completed) and then
// follows live from an explicit cursor `head + 1`, so it never re-drives a turn a
// prior owner generation ANSWERED (its agent events sit after it). A from-now tail
// would have skipped the un-driven first turn (the major gap these specs pin shut);
// a naive from-beginning tail would have re-driven the answered ones. The catch-up
// scan does neither.

describe('session-event-bridge — catch-up (drive un-driven pre-start user turns)', () => {
  it('drives a user.* turn appended BEFORE start() that has no agent event yet (first-turn catch-up)', async () => {
    // The pre-connect first-turn case: a client POSTs the first user.message
    // before the runner is online / the bridge starts. The turn has no agent
    // event after it, so the bridge must catch up and drive it — NOT skip it the
    // way a pure from-now tail would.
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    const preId = 'evt_user_firstturn';
    runner.scriptTurn(preId, { lines: [{ type: 'caught.up.1' }, { type: 'caught.up.2' }] });
    await store.append(WORKSPACE_ID, SESSION_ID, [
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: {
          id: preId,
          type: 'user.message',
          content: [{ type: 'text', text: 'first turn before connect' }],
        },
      }),
    ]);
    bridge.start();
    try {
      // The pre-existing un-driven turn is driven (catch-up), and its agent
      // events are persisted by the single writer.
      await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === preId));
      await waitFor(
        () =>
          store
            .snapshot(WORKSPACE_ID, SESSION_ID)
            .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY).length === 2,
      );
      const kinds = store
        .snapshot(WORKSPACE_ID, SESSION_ID)
        .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY)
        .map((e) => e.kind);
      expect(kinds).toEqual(['caught.up.1', 'caught.up.2']);
    } finally {
      await bridge.stop();
    }
  });

  it('drives multiple queued pre-start user turns in order', async () => {
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    const a = 'evt_user_q_a';
    const b = 'evt_user_q_b';
    runner.scriptTurn(a, { lines: [{ type: 'qa.1' }] });
    runner.scriptTurn(b, { lines: [{ type: 'qb.1' }] });
    // Two user turns queued before the bridge starts, neither answered.
    for (const [id, text] of [
      [a, 'first'],
      [b, 'second'],
    ] as const) {
      await store.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: { id, type: 'user.message', content: [{ type: 'text', text: text }] },
        }),
      ]);
    }
    bridge.start();
    try {
      await waitFor(() => runner.turns.length === 2);
      // Driven in transcript (append) order, serially.
      expect(runner.turns.map((t) => t.userEvent['id'])).toEqual([a, b]);
      const agentKinds = store
        .snapshot(WORKSPACE_ID, SESSION_ID)
        .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY)
        .map((e) => e.kind);
      expect(agentKinds).toEqual(['qa.1', 'qb.1']);
    } finally {
      await bridge.stop();
    }
  });

  it.each(['agent.message', 'agent.turn_completed'])(
    'does not lose a queued prompt behind late %s from the preceding turn',
    async (kind) => {
      const { bridge, store, runner } = buildBridge();
      runner.connect();
      const inputs: HttpEventInput[] = [
        { id: 'evt_turn_a', type: 'user.message', content: [{ type: 'text', text: 'A' }] },
        { id: 'evt_turn_b', type: 'user.message', content: [{ type: 'text', text: 'B' }] },
        {
          id: 'evt_a_late',
          type: kind,
          ...(kind === 'agent.turn_completed'
            ? { turn_event_id: 'evt_turn_a' }
            : { source_event_id: 'evt_turn_a' }),
        },
      ];
      if (kind === 'agent.turn_completed')
        inputs.splice(2, 0, {
          id: 'evt_legacy_partial_a',
          type: 'agent.message',
        });
      for (const input of inputs)
        await store.append(WORKSPACE_ID, SESSION_ID, [
          httpEventToProto({
            workspaceId: WORKSPACE_ID,
            sessionId: SESSION_ID,
            producedBy: input.type === 'user.message' ? 'client' : 'harness',
            idempotencyKey: '',
            input,
          }),
        ]);
      runner.scriptTurn('evt_turn_b', {
        lines: [
          { type: 'agent.message', source_event_id: 'forged' },
          { type: 'agent.turn_completed', turn_event_id: 'forged' },
        ],
      });
      bridge.start();
      try {
        await waitFor(() => store.snapshot(WORKSPACE_ID, SESSION_ID).length === inputs.length + 2);
        expect(runner.turns.map((turn) => turn.userEvent['id'])).toEqual(['evt_turn_b']);
        const output = store.snapshot(WORKSPACE_ID, SESSION_ID).slice(-2).map(payloadJson);
        expect(output[0]?.source_event_id).toBe('evt_turn_b');
        expect(output[1]).toMatchObject({
          source_event_id: 'evt_turn_b',
          turn_event_id: 'evt_turn_b',
        });
      } finally {
        await bridge.stop();
      }
    },
  );

  it('catches up an un-driven turn but skips an already-answered earlier turn', async () => {
    // Mixed history: an answered turn (agent events after it) followed by a fresh
    // un-driven turn. Only the un-driven one is caught up.
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const answered = 'evt_user_mixed_answered';
    const fresh = 'evt_user_mixed_fresh';
    runner.scriptTurn(fresh, { lines: [{ type: 'fresh.1' }] });
    // Seed: answered user turn, then its (already-persisted) agent events, then a
    // brand-new user turn nobody has answered.
    await store.append(WORKSPACE_ID, SESSION_ID, [
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: { id: answered, type: 'user.message', content: [{ type: 'text', text: 'old' }] },
      }),
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'harness',
        idempotencyKey: '',
        input: { id: 'evt_agent_old', type: 'response.completed' },
      }),
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: {
          id: fresh,
          type: 'user.message',
          content: [{ type: 'text', text: 'new and un-driven' }],
        },
      }),
    ]);
    const bridge = new SessionEventBridge({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      store,
      registry,
    });
    bridge.start();
    try {
      await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === fresh));
      // Give a (wrong) replay of the answered turn a chance to surface.
      await new Promise<void>((resolve) => setTimeout(resolve, 10));
      expect(runner.turns.map((t) => t.userEvent['id'])).toEqual([fresh]);
      expect(runner.turns.some((t) => t.userEvent['id'] === answered)).toBe(false);
    } finally {
      await bridge.stop();
    }
  });

  it('drives both a caught-up pre-start turn and a live post-start turn, gaplessly', async () => {
    // The boundary must be gapless: the catch-up turn AND a turn appended after
    // start are both driven, exactly once each, in order.
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    const pre = 'evt_user_gap_pre';
    const post = 'evt_user_gap_post';
    runner.scriptTurn(pre, { lines: [{ type: 'pre.1' }] });
    runner.scriptTurn(post, { lines: [{ type: 'post.1' }] });
    await store.append(WORKSPACE_ID, SESSION_ID, [
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: { id: pre, type: 'user.message', content: [{ type: 'text', text: 'pre' }] },
      }),
    ]);
    bridge.start();
    try {
      await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === pre));
      await store.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: { id: post, type: 'user.message', content: [{ type: 'text', text: 'post' }] },
        }),
      ]);
      await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === post));
      // Each turn driven exactly once, in order; no replay of the caught-up turn.
      expect(runner.turns.map((t) => t.userEvent['id'])).toEqual([pre, post]);
    } finally {
      await bridge.stop();
    }
  });

  it('falls back to from-now (NOT from-beginning) when the catch-up read fails', async () => {
    // Minor-fix regression guard. When the catch-up READ throws (the store
    // surfaced an error) the bridge cannot establish the answered-up-to boundary,
    // so it follows from-now. The bug this pins shut: the old fallback resumed at
    // the numeric cursor `"0"` (from `head = -1`), which on the real
    // Kafka/Postgres/Pulsar backends is a seek to offset/seq 0 — a from-BEGINNING
    // replay that re-drives every already-answered turn. The in-memory store here
    // models the SAME cursor semantics as those backends (`"0"` ->
    // `Number("0") - 1 = -1` -> delivers every event from the start; `""` -> the
    // current head), so a from-beginning fallback would re-drive the answered turn
    // and FAIL this test. From-now must drive only turns appended AFTER start.
    const inner = new InMemoryTranscriptStore();
    let failRead = true;
    // An AsyncIterable whose first `next()` rejects — models a store `read` whose
    // error surfaces when the bridge's catch-up `for await` pulls the first event.
    const throwingRead = (): AsyncIterable<Event> => ({
      [Symbol.asyncIterator](): AsyncIterator<Event> {
        return {
          next(): Promise<IteratorResult<Event>> {
            return Promise.reject(new Error('simulated catch-up read failure'));
          },
        };
      },
    });
    const readFailsOnce: TranscriptStore = {
      append: inner.append.bind(inner),
      // The catch-up read fails exactly once (the first start); the live tail
      // (`tail`) is unaffected, so the bridge falls through to the from-now follow.
      read(workspaceId, sessionId, opts) {
        if (failRead) {
          failRead = false;
          return throwingRead();
        }
        return inner.read(workspaceId, sessionId, opts);
      },
      tail: inner.tail.bind(inner),
      archive: inner.archive.bind(inner),
      close: inner.close.bind(inner),
    };

    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const logger = recordingLogger();
    const answered = 'evt_user_readfail_answered';
    const fresh = 'evt_user_readfail_fresh';
    runner.scriptTurn(fresh, { lines: [{ type: 'fresh.1' }] });

    // Populate the transcript BEFORE start with an already-answered turn (a user
    // turn followed by its persisted agent event). A from-beginning fallback would
    // re-drive this; from-now must not.
    await inner.append(WORKSPACE_ID, SESSION_ID, [
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: {
          id: answered,
          type: 'user.message',
          content: [{ type: 'text', text: 'answered before start' }],
        },
      }),
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'harness',
        idempotencyKey: '',
        input: { id: 'evt_agent_readfail', type: 'response.completed' },
      }),
    ]);

    const bridge = new SessionEventBridge({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      store: readFailsOnce,
      registry,
      logger,
    });
    bridge.start();
    try {
      // The catch-up read failure was logged.
      await waitFor(() => logger.errors.length >= 1);
      // A NEW turn appended AFTER start IS driven by the from-now tail.
      await inner.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: {
            id: fresh,
            type: 'user.message',
            content: [{ type: 'text', text: 'after start' }],
          },
        }),
      ]);
      await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === fresh));
      // The already-answered pre-start turn was NOT re-driven (no from-beginning
      // replay): only the fresh post-start turn reached the runner.
      expect(runner.turns.map((t) => t.userEvent['id'])).toEqual([fresh]);
      expect(runner.turns.some((t) => t.userEvent['id'] === answered)).toBe(false);
    } finally {
      await bridge.stop();
    }
  });

  it('re-drives an un-completed turn (runner offline at first start) on reconnect', async () => {
    // The lossless half of the unified-stream tradeoff: a turn that produced NO
    // agent event (the runner was offline when it was first appended) has nothing
    // after it in the transcript, so a fresh bridge start catches it up and
    // completes it — nothing is silently lost.
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const logger = recordingLogger();
    const undriven = 'evt_user_undriven';
    runner.scriptTurn(undriven, { lines: [{ type: 'completed.late.1' }] });

    // First bridge: runner offline, so the turn cannot be driven — it produces no
    // agent event.
    const first = new SessionEventBridge({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      store,
      registry,
      logger,
    });
    first.start();
    await store.append(WORKSPACE_ID, SESSION_ID, [
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: {
          id: undriven,
          type: 'user.message',
          content: [{ type: 'text', text: 'while offline' }],
        },
      }),
    ]);
    await waitFor(() => logger.warns.length + logger.errors.length >= 1);
    await first.stop();
    expect(
      store
        .snapshot(WORKSPACE_ID, SESSION_ID)
        .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY).length,
    ).toBe(0);

    // Runner connects; a fresh bridge catches the un-completed turn up.
    runner.connect();
    const second = new SessionEventBridge({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      store,
      registry,
    });
    second.start();
    try {
      await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === undriven));
      await waitFor(() =>
        store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .some((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY && e.kind === 'completed.late.1'),
      );
    } finally {
      await second.stop();
    }
  });

  it('does NOT re-drive an already-answered turn when a fresh bridge starts (reconnect)', async () => {
    // Drive one full turn, then build a SECOND bridge on the same transcript —
    // the newest-wins reconnect case the manager hits on every runner connect.
    // The new bridge tails from-now, so the already-answered user turn (and its
    // persisted agent events) are NOT replayed as a new turn to the runner.
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const answered = 'evt_user_answered';
    runner.scriptTurn(answered, { lines: [{ type: 'a.1' }, { type: 'a.2' }] });

    const first = new SessionEventBridge({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      store,
      registry,
    });
    first.start();
    await store.append(WORKSPACE_ID, SESSION_ID, [
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: {
          id: answered,
          type: 'user.message',
          content: [{ type: 'text', text: 'answer me' }],
        },
      }),
    ]);
    await waitFor(
      () =>
        store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY).length === 2,
    );
    await first.stop();
    const turnsAfterFirst = runner.turns.length;
    expect(turnsAfterFirst).toBe(1);

    // Fresh bridge (reconnect): from-now means it does NOT re-forward the
    // already-answered user turn.
    const second = new SessionEventBridge({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      store,
      registry,
    });
    second.start();
    try {
      // A genuinely new user turn IS driven by the fresh bridge.
      const next = 'evt_user_next';
      runner.scriptTurn(next, { lines: [{ type: 'b.1' }] });
      await store.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: {
            id: next,
            type: 'user.message',
            content: [{ type: 'text', text: 'something new' }],
          },
        }),
      ]);
      await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === next));
      // Exactly one extra turn (the new one) — the answered turn was NOT replayed.
      expect(runner.turns.length).toBe(turnsAfterFirst + 1);
      expect(runner.turns.filter((t) => t.userEvent['id'] === answered).length).toBe(1);
    } finally {
      await second.stop();
    }
  });
});

// ── 6b. tool-confirmation verdicts route to the confirmation route (NOT a turn) ──

describe('session-event-bridge — tool-confirmation verdicts push to the confirmation route', () => {
  it('pushes a live user.tool_confirmation to the confirmation route, NOT the turn route', async () => {
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    bridge.start();
    try {
      const conf = await appendToolConfirmation(store, 'toolu_live', 'allow');
      await waitFor(() => runner.confirmations.length === 1);

      // It went to the confirmation route as a POST carrying the verdict payload —
      // and was NOT driven as a turn (the runner saw zero turn requests).
      const pushed = runner.confirmations[0]!;
      expect(pushed.frame.method).toBe('POST');
      expect(pushed.frame.path).toBe(RUNNER_CONFIRMATION_PATH);
      expect(pushed.body['type']).toBe(TOOL_CONFIRMATION_EVENT_KIND);
      expect(pushed.body['tool_use_id']).toBe('toolu_live');
      expect(pushed.body['result']).toBe('allow');
      // The session header scopes the push to the right conversation.
      const headers = new Map((pushed.frame.headers ?? []).map(([k, v]) => [k.toLowerCase(), v]));
      expect(headers.get(RUNNER_SESSION_HEADER.toLowerCase())).toBe(SESSION_ID);
      expect(runner.turns.length).toBe(0);
      // A confirmation produces no agent-event stream, so the bridge persists none.
      expect(
        store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY),
      ).toHaveLength(0);
      // The confirmation event itself was NEVER mis-counted as a pending turn.
      expect(conf.kind).toBe(TOOL_CONFIRMATION_EVENT_KIND);
    } finally {
      await bridge.stop();
    }
  });

  it('delivers the verdict to a turn parked mid-stream (concurrent, no deadlock)', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const turnId = 'evt_user_parked';
    // The turn stays open until the runner observes the confirmation push, then ends.
    runner.scriptTurnGatedOnConfirmation(turnId, 'toolu_parked', [{ type: 'after.verdict' }]);
    await store.append(WORKSPACE_ID, SESSION_ID, [
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: {
          id: turnId,
          type: 'user.message',
          content: [{ type: 'text', text: 'do the thing' }],
        },
      }),
    ]);
    const bridge = new SessionEventBridge({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      store,
      registry,
    });
    bridge.start();
    try {
      // The turn is in flight (the bridge POSTed it and is draining the open stream).
      await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === turnId));
      // Now the client delivers the verdict; the SEPARATE confirmation loop pushes
      // it even though the turn loop is blocked on the open turn stream.
      await appendToolConfirmation(store, 'toolu_parked', 'allow');
      await waitFor(() =>
        runner.confirmations.some((c) => c.body['tool_use_id'] === 'toolu_parked'),
      );
      // With the verdict delivered, the runner ends the turn and its post-verdict
      // agent event is persisted — proving the two loops ran concurrently (no
      // deadlock: the turn could not have completed without the verdict push).
      await waitFor(() =>
        store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .some((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY && e.kind === 'after.verdict'),
      );
    } finally {
      await bridge.stop();
    }
  });

  it('re-pushes an UN-ANSWERED confirmation on catch-up so a re-parked tool call gets its verdict', async () => {
    // A confirmation appended BEFORE the bridge starts, with no agent event after it
    // (its turn is still being re-driven). Catch-up must re-push it.
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    await appendToolConfirmation(store, 'toolu_catchup', 'allow');
    bridge.start();
    try {
      await waitFor(() =>
        runner.confirmations.some((c) => c.body['tool_use_id'] === 'toolu_catchup'),
      );
      const pushed = runner.confirmations.find((c) => c.body['tool_use_id'] === 'toolu_catchup')!;
      expect(pushed.frame.path).toBe(RUNNER_CONFIRMATION_PATH);
      // Still not a turn.
      expect(runner.turns.length).toBe(0);
    } finally {
      await bridge.stop();
    }
  });

  it('does NOT re-push a confirmation whose turn already completed (an agent event sits after it)', async () => {
    // A confirmation followed by an agent event = the turn completed; the verdict is
    // moot (nothing is parked on it after a reconnect), so catch-up skips it.
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    await appendToolConfirmation(store, 'toolu_done', 'allow');
    await appendAgentEvent(store, 'agent.turn_completed');
    bridge.start();
    try {
      // Give the confirmation loop a chance to run its catch-up read.
      await new Promise<void>((resolve) => setTimeout(resolve, 30));
      expect(runner.confirmations.some((c) => c.body['tool_use_id'] === 'toolu_done')).toBe(false);
    } finally {
      await bridge.stop();
    }
  });

  it('a non-2xx confirmation ack is contained (logged) and does not tear down the loop', async () => {
    const logger = recordingLogger();
    const { bridge, store, runner } = buildBridge({ logger });
    runner.connect();
    runner.scriptConfirmationAck('toolu_bad', 503);
    bridge.start();
    try {
      await appendToolConfirmation(store, 'toolu_bad', 'allow');
      await waitFor(() =>
        logger.warns.some((w) => w.msg?.includes('non-2xx tool confirmation ack')),
      );
      // The loop survives: a later confirmation still pushes.
      await appendToolConfirmation(store, 'toolu_ok', 'deny');
      await waitFor(() => runner.confirmations.some((c) => c.body['tool_use_id'] === 'toolu_ok'));
    } finally {
      await bridge.stop();
    }
  });
});

// ── 6c. user.interrupt signals route to the interrupt route (NOT a turn) ──

describe('session-event-bridge — user.interrupt signals push to the interrupt route', () => {
  it('pushes a live user.interrupt to the interrupt route, NOT the turn route', async () => {
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    bridge.start();
    try {
      const intr = await appendUserInterrupt(store);
      await waitFor(() => runner.interrupts.length === 1);

      // It went to the interrupt route as a POST carrying the interrupt payload —
      // and was NOT driven as a turn (the runner saw zero turn requests).
      const pushed = runner.interrupts[0]!;
      expect(pushed.frame.method).toBe('POST');
      expect(pushed.frame.path).toBe(RUNNER_INTERRUPT_PATH);
      expect(pushed.body['type']).toBe(INTERRUPT_EVENT_KIND);
      // The session header scopes the push to the right conversation.
      const headers = new Map((pushed.frame.headers ?? []).map(([k, v]) => [k.toLowerCase(), v]));
      expect(headers.get(RUNNER_SESSION_HEADER.toLowerCase())).toBe(SESSION_ID);
      expect(runner.turns.length).toBe(0);
      // An interrupt produces no agent-event stream, so the bridge persists none.
      expect(
        store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .filter((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY),
      ).toHaveLength(0);
      // The interrupt event itself was NEVER mis-counted as a pending turn.
      expect(intr.kind).toBe(INTERRUPT_EVENT_KIND);
    } finally {
      await bridge.stop();
    }
  });

  it('delivers the interrupt to a turn blocked on the model (concurrent, no deadlock)', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const turnId = 'evt_user_blocked';
    // The turn stays open (blocked on the model) until the runner observes the
    // interrupt push, then ends with a post-interrupt agent event.
    runner.scriptTurnGatedOnInterrupt(turnId, [{ type: 'after.interrupt' }]);
    await store.append(WORKSPACE_ID, SESSION_ID, [
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: {
          id: turnId,
          type: 'user.message',
          content: [{ type: 'text', text: 'do the long thing' }],
        },
      }),
    ]);
    const bridge = new SessionEventBridge({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      store,
      registry,
    });
    bridge.start();
    try {
      // The turn is in flight (the bridge POSTed it and is draining the open stream).
      await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === turnId));
      // Now the client delivers the interrupt; the SEPARATE interrupt loop pushes it
      // even though the turn loop is blocked on the open turn stream.
      await appendUserInterrupt(store);
      await waitFor(() => runner.interrupts.length === 1);
      // With the interrupt delivered, the runner ends the turn and its post-interrupt
      // agent event is persisted — proving the two loops ran concurrently (no
      // deadlock: the turn could not have completed without the interrupt push).
      await waitFor(() =>
        store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .some((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY && e.kind === 'after.interrupt'),
      );
    } finally {
      await bridge.stop();
    }
  });

  it('re-pushes an UN-ANSWERED interrupt on catch-up so a re-driven turn gets aborted', async () => {
    // An interrupt appended BEFORE the bridge starts, with no agent event after it
    // (the turn it preempts is still being re-driven). Catch-up must re-push it.
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    await appendUserInterrupt(store);
    bridge.start();
    try {
      await waitFor(() => runner.interrupts.length === 1);
      expect(runner.interrupts[0]!.frame.path).toBe(RUNNER_INTERRUPT_PATH);
      // Still not a turn.
      expect(runner.turns.length).toBe(0);
    } finally {
      await bridge.stop();
    }
  });

  it('does NOT re-push an interrupt whose turn already completed (an agent event sits after it)', async () => {
    // An interrupt followed by an agent event = the turn completed; the interrupt is
    // moot (nothing is in flight to abort after a reconnect), so catch-up skips it.
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    await appendUserInterrupt(store);
    await appendAgentEvent(store, 'agent.turn_completed');
    bridge.start();
    try {
      // Give the interrupt loop a chance to run its catch-up read.
      await new Promise<void>((resolve) => setTimeout(resolve, 30));
      expect(runner.interrupts.length).toBe(0);
    } finally {
      await bridge.stop();
    }
  });

  it('a non-2xx interrupt ack is contained (logged) and does not tear down the loop', async () => {
    const logger = recordingLogger();
    const { bridge, store, runner } = buildBridge({ logger });
    runner.connect();
    runner.scriptInterruptAck(503);
    bridge.start();
    try {
      await appendUserInterrupt(store);
      await waitFor(() => logger.warns.some((w) => w.msg?.includes('non-2xx interrupt ack')));
      // The loop survives: a later interrupt still pushes (this one acks 200).
      await appendUserInterrupt(store);
      await waitFor(() => runner.interrupts.length === 2);
    } finally {
      await bridge.stop();
    }
  });
});

// ── 7. the owner-pod bridge manager (runner connect/disconnect lifecycle) ──

/** An in-memory {@link BoundSessionResolver} keyed by runner id. */
class FakeResolver implements BoundSessionResolver {
  private readonly map = new Map<string, { workspaceId: string; sessionId: string }>();
  failNext = false;
  bind(runnerId: string, workspaceId: string, sessionId: string): this {
    this.map.set(runnerId, { workspaceId, sessionId });
    return this;
  }
  async resolveBoundSession(
    runnerId: string,
  ): Promise<{ workspaceId: string; sessionId: string } | null> {
    if (this.failNext) {
      this.failNext = false;
      throw new Error('resolver boom');
    }
    return this.map.get(runnerId) ?? null;
  }
}

describe('session-event-bridge manager — owner-pod lifecycle', () => {
  it('does not deliver snapshots or recover a connecting runner owned by harness-server', async () => {
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    let snapshots = 0;
    const manager = new SessionEventBridgeManager({
      store: new InMemoryTranscriptStore(),
      registry,
      resolver: new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID),
      ownsSession: async () => false,
      snapshotProvider: {
        resolve: async () => {
          snapshots++;
          return null;
        },
      },
    });
    runner.connect();
    await manager.onRunnerConnect(RUNNER_ID);
    expect(manager.size).toBe(0);
    expect(snapshots).toBe(0);
    expect(runner.turns).toEqual([]);
  });

  it("starts a bridge on runner connect and drives that session's turns", async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver });

    runner.connect();
    const uId = 'evt_user_mgr';
    runner.scriptTurn(uId, { lines: [{ type: 'mgr.1' }] });
    await manager.onRunnerConnect(RUNNER_ID);
    expect(manager.size).toBe(1);
    try {
      await store.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: { id: uId, type: 'user.message', content: [{ type: 'text', text: 'go' }] },
        }),
      ]);
      await waitFor(() =>
        store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .some((e) => e.producedBy === AGENT_EVENT_PRODUCED_BY && e.kind === 'mgr.1'),
      );
    } finally {
      await manager.stopAll();
    }
    expect(manager.size).toBe(0);
  });

  it('is a no-op for a runner bound to no session (cloud / not-distributed here)', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const resolver = new FakeResolver(); // nothing bound
    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    await manager.onRunnerConnect('runner_unbound');
    expect(manager.size).toBe(0);
    await manager.stopAll();
  });

  it('stops the bridge on a true runner disconnect (runner gone from the registry)', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    runner.connect();
    await manager.onRunnerConnect(RUNNER_ID);
    expect(manager.size).toBe(1);
    // A true close: the runner left the registry before the disconnect hook runs.
    runner.disconnect();
    await manager.onRunnerDisconnect(RUNNER_ID);
    expect(manager.size).toBe(0);
    // A second disconnect is idempotent.
    await manager.onRunnerDisconnect(RUNNER_ID);
    expect(manager.size).toBe(0);
  });

  it('keeps the bridge when a disconnect hook fires for a still-online runner (reconnect)', async () => {
    // Newest-wins: the OLD generation's disconnect hook can fire while a NEWER
    // tunnel is already live for the same runner id. The manager must NOT stop
    // the live bridge — the reconnect guard mirrors the distributor's.
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    runner.connect();
    await manager.onRunnerConnect(RUNNER_ID);
    expect(manager.size).toBe(1);
    // Runner is still registered (a newer tunnel) — the stale disconnect is a no-op.
    await manager.onRunnerDisconnect(RUNNER_ID);
    expect(manager.size).toBe(1);
    await manager.stopAll();
  });

  it('replaces the bridge on a reconnect for the same runner id (newest-wins)', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    runner.connect();
    await manager.onRunnerConnect(RUNNER_ID);
    const firstSize = manager.size;
    // Reconnect: a fresh bridge replaces the old one, still exactly one live.
    await manager.onRunnerConnect(RUNNER_ID);
    expect(firstSize).toBe(1);
    expect(manager.size).toBe(1);
    await manager.stopAll();
  });

  it('does NOT replay an already-answered user turn across a newest-wins reconnect', async () => {
    // The exact production path: onRunnerConnect builds a FRESH bridge on each
    // runner connect/reconnect. With a from-now tail, the replacement bridge
    // does not re-forward a user turn the transcript already holds (and the
    // prior generation already answered) — no agent re-driving on old prompts.
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    runner.connect();
    await manager.onRunnerConnect(RUNNER_ID);

    const answered = 'evt_user_mgr_answered';
    runner.scriptTurn(answered, { lines: [{ type: 'mgr.a.1' }] });
    await store.append(WORKSPACE_ID, SESSION_ID, [
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        producedBy: 'client',
        idempotencyKey: '',
        input: {
          id: answered,
          type: 'user.message',
          content: [{ type: 'text', text: 'answer me' }],
        },
      }),
    ]);
    await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === answered));
    const turnsBeforeReconnect = runner.turns.length;

    // Newest-wins reconnect: a fresh bridge replaces the old one. It must NOT
    // re-forward the already-answered turn sitting in the transcript.
    await manager.onRunnerConnect(RUNNER_ID);
    try {
      // Give a from-beginning replay a chance to (wrongly) re-drive it.
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
      expect(runner.turns.length).toBe(turnsBeforeReconnect);
      expect(runner.turns.filter((t) => t.userEvent['id'] === answered).length).toBe(1);

      // A genuinely new turn after the reconnect IS still driven.
      const next = 'evt_user_mgr_next';
      runner.scriptTurn(next, { lines: [{ type: 'mgr.b.1' }] });
      await store.append(WORKSPACE_ID, SESSION_ID, [
        httpEventToProto({
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
          producedBy: 'client',
          idempotencyKey: '',
          input: { id: next, type: 'user.message', content: [{ type: 'text', text: 'new' }] },
        }),
      ]);
      await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === next));
      expect(runner.turns.filter((t) => t.userEvent['id'] === answered).length).toBe(1);
    } finally {
      await manager.stopAll();
    }
  });

  it('does not throw out of the connect hook when the resolver fails', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const logger = recordingLogger();
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    resolver.failNext = true;
    const manager = new SessionEventBridgeManager({ store, registry, resolver, logger });
    await manager.onRunnerConnect(RUNNER_ID); // must not reject
    expect(manager.size).toBe(0);
    expect(logger.errors.length).toBeGreaterThanOrEqual(1);
    await manager.stopAll();
  });
});

// ── manager — reverse-lookup recovery composed on (re)connect ──
//
// The owner pod's (re)connect sequence is: resolve → SERVE A REVERSE-LOOKUP RESUME
// REPLAY (SessionRecovery pushes the session's persisted events forward over the
// tunnel; the runner dedups by id) → START THE BRIDGE (the single writer + SOLE
// turn driver). These tests pin that the replay actually happens end-to-end (the
// critical wiring) AND that recovery + the bridge never both drive a turn — so a
// turn that streamed partial output before a drop is NOT re-driven and its
// persisted prefix is NOT duplicated (exactly-once at the system level).

describe('session-event-bridge manager — reverse-lookup recovery on (re)connect', () => {
  it('serves a reverse-lookup resume replay of the persisted events on connect', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    runner.connect();
    // History a prior generation persisted: a completed turn (user + agent + done).
    const u1 = await appendUserMessage(store, 'first');
    const a1 = await appendAgentEvent(store, 'response.created');
    const c1 = await appendAgentEvent(store, COMPLETED_TURN_EVENT_KIND);

    try {
      await manager.onRunnerConnect(RUNNER_ID);
      // The owner pod pushed exactly one resume replay (a fresh full replay — empty
      // cursor) carrying every persisted event, in seq order, with stable ids the
      // runner dedups by. This is the subject behaviour, end-to-end.
      expect(runner.replays.length).toBe(1);
      const replay = runner.replays[0]!;
      expect(replay.cursor).toBe('');
      expect(replay.lines.map((l) => String(l['id']))).toEqual([u1.id, a1.id, c1.id]);
      // The history ends at a completed turn → the bridge drives nothing.
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
      expect(runner.turns.length).toBe(0);
    } finally {
      await manager.stopAll();
    }
  });

  it('serves an INCREMENTAL after={cursor} replay when the runner presents a resume cursor for the bound session', async () => {
    // The end-to-end incremental-resume win: the runner presents its last-consumed
    // cursor per session in its hello (here passed straight to onRunnerConnect, as
    // the runner-tunnel route does after reading hello.resumeCursors). The owner pod
    // serves ONLY the events after that cursor — not a fresh full replay.
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    runner.connect();
    // The runner already consumed u1 + a1; everything after a1 must be replayed.
    const u1 = await appendUserMessage(store, 'old turn');
    const a1 = await appendAgentEvent(store, 'response.created');
    const a2 = await appendAgentEvent(store, 'response.output_text.delta');
    const c1 = await appendAgentEvent(store, COMPLETED_TURN_EVENT_KIND);

    try {
      await manager.onRunnerConnect(RUNNER_ID, { [SESSION_ID]: a1.id });
      expect(runner.replays.length).toBe(1);
      const replay = runner.replays[0]!;
      // The slice is keyed on the presented cursor and carries only what's after it.
      expect(replay.cursor).toBe(a1.id);
      expect(replay.lines.map((l) => String(l['id']))).toEqual([a2.id, c1.id]);
      expect(replay.lines.some((l) => l['id'] === u1.id)).toBe(false);
      expect(replay.lines.some((l) => l['id'] === a1.id)).toBe(false);
    } finally {
      await manager.stopAll();
    }
  });

  it('serves a FRESH full replay for a session absent from the presented resume-cursor map', async () => {
    // A cursor map that names a DIFFERENT session leaves the bound session without a
    // cursor → it degrades to a fresh full replay (the bound session is treated as
    // fresh, exactly as an empty map). Proves the per-session selection.
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    runner.connect();
    const u1 = await appendUserMessage(store, 'first');
    const c1 = await appendAgentEvent(store, COMPLETED_TURN_EVENT_KIND);

    try {
      await manager.onRunnerConnect(RUNNER_ID, { ses_some_other_session: 'evt_x' });
      expect(runner.replays.length).toBe(1);
      const replay = runner.replays[0]!;
      // No cursor for THIS session → fresh full replay (empty cursor, whole history).
      expect(replay.cursor).toBe('');
      expect(replay.lines.map((l) => String(l['id']))).toEqual([u1.id, c1.id]);
    } finally {
      await manager.stopAll();
    }
  });

  it('serves the replay BEFORE the bridge drives a pending turn', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    runner.connect();
    // A pre-connect first turn the runner never got to answer (no agent event).
    const pending = await appendUserMessage(store, 'pre-connect first turn');
    runner.scriptTurn(pending.id, { lines: [{ type: 'resp.1' }] });

    try {
      await manager.onRunnerConnect(RUNNER_ID);
      // The bridge catch-up drives the un-driven turn …
      await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === pending.id));
      // … and the resume replay was pushed (and recorded) BEFORE that turn — the
      // runner had its state rebuilt before the turn arrived.
      expect(runner.replays.length).toBeGreaterThanOrEqual(1);
      expect(runner.replays[0]!.lines.map((l) => String(l['id']))).toEqual([pending.id]);
    } finally {
      await manager.stopAll();
    }
  });

  it('does NOT re-drive (or duplicate) a partial turn that dropped before completing', async () => {
    // The latent exactly-once hazard the verifier flagged: recovery's pending rule
    // (keyed on agent.turn_completed) WOULD surface a turn that streamed partial
    // output then dropped (user → response.created, no completion). But recovery
    // only SERVES — the BRIDGE is the sole driver, and the bridge's answered-up-to
    // rule treats any agent event after the user turn as answered, so it does NOT
    // re-drive it. The persisted prefix must NOT be duplicated.
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    runner.connect();
    const partial = await appendUserMessage(store, 'partial then dropped');
    await appendAgentEvent(store, 'response.created'); // a persisted prefix, no completion
    const before = store.snapshot(WORKSPACE_ID, SESSION_ID).length;
    // Script a (wrong) re-drive so that IF the bridge re-drove it, the prefix would
    // visibly grow — the assertion below proves it does not.
    runner.scriptTurn(partial.id, { lines: [{ type: 'response.created' }, { type: 'duplicate' }] });

    try {
      await manager.onRunnerConnect(RUNNER_ID);
      // The replay still catches the runner up on the partial prefix …
      await waitFor(() => runner.replays.length >= 1);
      expect(runner.replays[0]!.lines.map((l) => String(l['id']))).toContain(partial.id);
      // … but the partial turn is NOT re-driven (the bridge skips an answered turn),
      // so no new agent events are appended and the prefix is not duplicated.
      await new Promise<void>((resolve) => setTimeout(resolve, 30));
      expect(runner.turns.filter((t) => t.userEvent['id'] === partial.id).length).toBe(0);
      expect(store.snapshot(WORKSPACE_ID, SESSION_ID).length).toBe(before);
    } finally {
      await manager.stopAll();
    }
  });

  it('waits for a live runner generation before starting the bridge', async () => {
    // An offline runner has no generation to pin. A later connect starts recovery.
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const logger = recordingLogger();
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver, logger });
    // Runner is bound but NOT connected — the replay push is offline.
    await appendUserMessage(store, 'while offline');
    try {
      await manager.onRunnerConnect(RUNNER_ID);
      expect(manager.size).toBe(0);
      const runner = new FakeRunner(registry, RUNNER_ID);
      runner.connect();
      await manager.onRunnerConnect(RUNNER_ID);
      expect(manager.size).toBe(1);
    } finally {
      await manager.stopAll();
    }
  });

  it('can be disabled via recoverOnConnect=false (bridge still starts, no replay)', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({
      store,
      registry,
      resolver,
      recoverOnConnect: false,
    });
    runner.connect();
    await appendUserMessage(store, 'turn');
    try {
      await manager.onRunnerConnect(RUNNER_ID);
      expect(manager.size).toBe(1);
      // No reverse-lookup replay was served when recovery is disabled.
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
      expect(runner.replays.length).toBe(0);
    } finally {
      await manager.stopAll();
    }
  });
});

describe('session-event-bridge manager — recovery observation (serving-side seam)', () => {
  it('records what recovery served on connect (replayed count + delivered + truncation)', async () => {
    // The serving-side observation is CONSUMED at the manager boundary, not
    // discarded inside a void method: the connect sequence keeps what the owner pod
    // replayed for the runner so ops / an explicit-resume seam can read it.
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    runner.connect();
    // A completed turn: 3 persisted events, nothing pending.
    await appendUserMessage(store, 'first');
    await appendAgentEvent(store, 'response.created');
    await appendAgentEvent(store, COMPLETED_TURN_EVENT_KIND);
    try {
      await manager.onRunnerConnect(RUNNER_ID);
      const obs = manager.recoveryObservation(RUNNER_ID);
      expect(obs).not.toBeNull();
      const seen = obs as RecoveryObservation;
      expect(seen.replayedCount).toBe(3);
      expect(seen.replayDelivered).toBe(true);
      expect(seen.replayTruncated).toBe(false);
      // History ended at a completed turn → nothing looked pending.
      expect(seen.pendingUserTurnId).toBeNull();
    } finally {
      await manager.stopAll();
    }
  });

  it('surfaces the pending-turn id as a serving-side observation while the bridge drives it (one driver)', async () => {
    // The field the verifier flagged: recovery surfaces a pending user.message
    // (past the last agent.turn_completed). It is exposed here as a diagnostic, and
    // the SAME turn is driven by the BRIDGE (the sole driver) — never by recovery.
    // We assert both: the observation names the pending id AND the bridge (not
    // recovery) is what actually drove the turn.
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    runner.connect();
    const pending = await appendUserMessage(store, 'pre-connect first turn');
    runner.scriptTurn(pending.id, {
      lines: [{ type: 'resp.1' }, { type: COMPLETED_TURN_EVENT_KIND }],
    });
    try {
      await manager.onRunnerConnect(RUNNER_ID);
      // The observation names the pending user turn recovery saw.
      const obs = manager.recoveryObservation(RUNNER_ID);
      expect(obs).not.toBeNull();
      expect((obs as RecoveryObservation).pendingUserTurnId).toBe(pending.id);
      // The bridge (the sole driver) drove exactly that turn — recovery did not. A
      // single turn POST for the pending id proves it was driven once, by the bridge.
      await waitFor(() => runner.turns.some((t) => t.userEvent['id'] === pending.id));
      expect(runner.turns.filter((t) => t.userEvent['id'] === pending.id).length).toBe(1);
    } finally {
      await manager.stopAll();
    }
  });

  it('returns null after the runner disconnects (observation cleared with the bridge)', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const manager = new SessionEventBridgeManager({ store, registry, resolver });
    runner.connect();
    await appendUserMessage(store, 'turn');
    await appendAgentEvent(store, COMPLETED_TURN_EVENT_KIND);
    await manager.onRunnerConnect(RUNNER_ID);
    expect(manager.recoveryObservation(RUNNER_ID)).not.toBeNull();
    // A true close clears the per-runner observation along with the bridge.
    runner.disconnect();
    await manager.onRunnerDisconnect(RUNNER_ID);
    expect(manager.recoveryObservation(RUNNER_ID)).toBeNull();
    await manager.stopAll();
  });

  it('returns null for a runner that has never connected, and when recovery is disabled', async () => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    // Never connected anyone yet.
    const idle = new SessionEventBridgeManager({ store, registry, resolver });
    expect(idle.recoveryObservation(RUNNER_ID)).toBeNull();
    // Recovery disabled: the bridge starts but no observation is recorded.
    const disabled = new SessionEventBridgeManager({
      store,
      registry,
      resolver,
      recoverOnConnect: false,
    });
    runner.connect();
    await appendUserMessage(store, 'turn');
    try {
      await disabled.onRunnerConnect(RUNNER_ID);
      expect(disabled.size).toBe(1);
      expect(disabled.recoveryObservation(RUNNER_ID)).toBeNull();
    } finally {
      await disabled.stopAll();
    }
  });

  it('replaces the observation on reconnect and does not leave a stale one when recovery later throws', async () => {
    // First connect records an observation; a reconnect whose recovery THROWS (a
    // read failure) must not leave the prior generation's observation behind — the
    // manager drops it when it replaces the bridge.
    const inner = new InMemoryTranscriptStore();
    let failReadNext = false;
    const store: TranscriptStore = {
      append: inner.append.bind(inner),
      read(ws, ses, opts): AsyncIterable<Event> {
        if (failReadNext) {
          failReadNext = false;
          return {
            [Symbol.asyncIterator](): AsyncIterator<Event> {
              return {
                next: (): Promise<IteratorResult<Event>> =>
                  Promise.reject(new Error('simulated read failure')),
              };
            },
          };
        }
        return inner.read(ws, ses, opts);
      },
      tail: inner.tail.bind(inner),
      archive: inner.archive.bind(inner),
      close: inner.close.bind(inner),
    };
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
    const logger = recordingLogger();
    const manager = new SessionEventBridgeManager({ store, registry, resolver, logger });
    runner.connect();
    await appendUserMessage(inner, 'first');
    await appendAgentEvent(inner, COMPLETED_TURN_EVENT_KIND);
    try {
      await manager.onRunnerConnect(RUNNER_ID);
      expect(manager.recoveryObservation(RUNNER_ID)).not.toBeNull();
      // Reconnect with the next read rigged to fail: recovery throws (logged), the
      // bridge still starts, and the stale observation is gone (not the old one).
      failReadNext = true;
      await manager.onRunnerConnect(RUNNER_ID);
      expect(manager.size).toBe(1);
      expect(manager.recoveryObservation(RUNNER_ID)).toBeNull();
      expect(logger.errors.length).toBeGreaterThanOrEqual(1);
    } finally {
      await manager.stopAll();
    }
  });
});

describe('private harness checkpoint control records', () => {
  it.each([false, true])(
    'persists privately before completion; persistence failure=%s',
    async (fail) => {
      const logger = recordingLogger();
      const saved: unknown[] = [];
      const checkpoint = { version: 1, threadId: 'thread-test', files: {} };
      const { bridge, store, runner } = buildBridge({
        logger,
        saveHarnessState: async (state) => {
          if (fail) throw new Error('lost session ownership');
          saved.push(state);
        },
      });
      runner.connect();
      const id = 'evt_checkpoint';
      runner.scriptTurn(id, {
        lines: [
          { type: 'orca.harness_checkpoint', provider: 'codex-sdk', state: checkpoint },
          { type: 'agent.turn_completed' },
        ],
      });
      bridge.start();
      try {
        await store.append(WORKSPACE_ID, SESSION_ID, [
          httpEventToProto({
            workspaceId: WORKSPACE_ID,
            sessionId: SESSION_ID,
            producedBy: 'client',
            idempotencyKey: '',
            input: { id, type: 'user.message', content: [{ type: 'text', text: 'run' }] },
          }),
        ]);
        if (fail) await appendUserMessage(store, 'queued behind failed checkpoint');
        await waitFor(() =>
          fail ? logger.errors.length > 0 : store.snapshot(WORKSPACE_ID, SESSION_ID).length > 1,
        );
        expect(saved).toEqual(fail ? [] : [checkpoint]);
        const kinds = store.snapshot(WORKSPACE_ID, SESSION_ID).map((event) => event.kind);
        expect(kinds).not.toContain('orca.harness_checkpoint');
        expect(kinds.includes('agent.turn_completed')).toBe(!fail);
        if (fail) {
          expect(bridge.running).toBe(false);
          await bridge.stop();
          bridge.start(); // A poisoned generation cannot be manually restarted.
          await appendUserMessage(store, 'next');
          await appendToolConfirmation(store, 'tool_after_failure', 'allow');
          await appendUserInterrupt(store);
          await new Promise<void>((resolve) => setTimeout(resolve, 20));
          expect(runner.confirmations).toEqual([]);
          expect(runner.interrupts).toEqual([]);
          expect(runner.turns).toHaveLength(1);
          expect(bridge.running).toBe(false);
        }
      } finally {
        await bridge.stop();
      }
    },
  );
});

describe('managed resource checkpoint controls', () => {
  it.each([false, true])('acknowledges privately before completion, failure=%s', async (fail) => {
    const logger = recordingLogger();
    const committed: unknown[][] = [];
    const { bridge, store, runner } = buildBridge({
      logger,
      resources: {
        commit: async (...args) => {
          if (fail) throw new Error('resource persistence unavailable');
          committed.push(args);
        },
      },
    });
    runner.connect();
    const user = await appendUserMessage(store, 'change memory');
    runner.scriptTurn(user.id, {
      lines: [
        { type: 'orca.resource_checkpoint', checkpoint_id: 'rchk_test', manifest_sha256: 'digest' },
        { type: 'agent.turn_completed' },
      ],
    });
    bridge.start();
    try {
      await waitFor(() =>
        fail ? logger.errors.length > 0 : store.snapshot(WORKSPACE_ID, SESSION_ID).length > 1,
      );
      const kinds = store.snapshot(WORKSPACE_ID, SESSION_ID).map((event) => event.kind);
      expect(kinds).not.toContain('orca.resource_checkpoint');
      expect(kinds.includes('agent.turn_completed')).toBe(!fail);
      expect(committed).toEqual(fail ? [] : [['rchk_test', 'digest']]);
      if (fail) {
        await appendUserMessage(store, 'must not run');
        await appendToolConfirmation(store, 'stale', 'allow');
        await new Promise<void>((resolve) => setTimeout(resolve, 20));
        expect(bridge.running).toBe(false);
        expect(runner.turns).toHaveLength(1);
        expect(runner.confirmations).toEqual([]);
      }
    } finally {
      await bridge.stop();
    }
  });

  it('does not acknowledge resource changes after a public event append failed', async () => {
    const store = new InMemoryTranscriptStore();
    const append = store.append.bind(store);
    store.append = async (workspace, session, events) => {
      if (events.some((event) => event.producedBy === 'harness'))
        throw new Error('transcript unavailable');
      return append(workspace, session, events);
    };
    let commits = 0;
    const logger = recordingLogger();
    const { bridge, runner } = buildBridge({
      store,
      logger,
      resources: {
        commit: async () => {
          commits++;
        },
      },
    });
    runner.connect();
    const user = await appendUserMessage(store, 'change memory');
    runner.scriptTurn(user.id, {
      lines: [
        { type: 'agent.tool_called', tool_use_id: 'tool_1' },
        { type: 'orca.resource_checkpoint', checkpoint_id: 'rchk_test', manifest_sha256: 'digest' },
      ],
    });
    bridge.start();
    try {
      await waitFor(() => logger.errors.length > 0);
      expect(commits).toBe(0);
      expect(bridge.running).toBe(false);
    } finally {
      await bridge.stop();
    }
  });
});

it('retires managed preparation failures before a later queued message can overtake the failed input', async () => {
  let preparations = 0;
  const logger = recordingLogger();
  const { bridge, store, runner } = buildBridge({
    logger,
    resources: { commit: async () => {} },
    beforeTurn: async () => {
      if (++preparations === 1) throw new Error('Skills refresh unavailable');
    },
  });
  runner.connect();
  await appendUserMessage(store, 'first must not be skipped');
  await appendUserMessage(store, 'second must not overtake');
  bridge.start();
  try {
    await waitFor(() => logger.errors.length > 0);
    expect(bridge.running).toBe(false);
    expect(preparations).toBe(1);
    expect(runner.turns).toEqual([]);
    bridge.start();
    expect(bridge.running).toBe(false);
  } finally {
    await bridge.stop();
  }
});

it('joins an in-flight checkpoint drain across repeated runner reconnects', async () => {
  const store = new InMemoryTranscriptStore();
  const registry = new TunnelRegistry();
  const first = new FakeRunner(registry, RUNNER_ID);
  first.connect();
  let release!: () => void;
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  let saving = false;
  let saved = false;
  const manager = new SessionEventBridgeManager({
    store,
    registry,
    recoverOnConnect: false,
    resolver: new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID),
    saveHarnessState: async () => {
      saving = true;
      await held;
      saved = true;
    },
  });
  try {
    await manager.onRunnerConnect(RUNNER_ID);
    const user = await appendUserMessage(store, 'checkpoint held');
    first.scriptTurn(user.id, {
      lines: [{ type: 'orca.harness_checkpoint', provider: 'codex-sdk', state: {} }],
    });
    await waitFor(() => saving);
    const second = new FakeRunner(registry, RUNNER_ID);
    second.connect();
    const connect2 = manager.onRunnerConnect(RUNNER_ID);
    await waitFor(() => manager.size === 0);
    const third = new FakeRunner(registry, RUNNER_ID);
    third.connect();
    let connected3 = false;
    const connect3 = manager.onRunnerConnect(RUNNER_ID).then(() => {
      connected3 = true;
    });
    await new Promise<void>((resolve) => setTimeout(resolve, 20));
    expect(connected3).toBe(false);
    expect(manager.size).toBe(0);
    expect(saved).toBe(false);
    release();
    await Promise.all([connect2, connect3]);
    expect(saved).toBe(true);
    expect(manager.size).toBe(1);
  } finally {
    release();
    await manager.stopAll();
  }
});

it('stops forwarding when the runner no longer resolves to the original session', async () => {
  const store = new InMemoryTranscriptStore();
  const registry = new TunnelRegistry();
  const runner = new FakeRunner(registry, RUNNER_ID);
  runner.connect();
  const resolver = new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID);
  const manager = new SessionEventBridgeManager({
    store,
    registry,
    resolver,
    recoverOnConnect: false,
  });
  try {
    await manager.onRunnerConnect(RUNNER_ID);
    resolver.bind(RUNNER_ID, WORKSPACE_ID, 'ses_replacement');
    await appendUserMessage(store, 'belongs to former binding');
    await appendToolConfirmation(store, 'old_tool', 'allow');
    await appendUserInterrupt(store);
    await new Promise<void>((resolve) => setTimeout(resolve, 20));
    expect(runner.turns).toEqual([]);
    expect(runner.confirmations).toEqual([]);
    expect(runner.interrupts).toEqual([]);
  } finally {
    await manager.stopAll();
  }
});

describe('Registry-owned request/usage bridge ordering', () => {
  it('does not cancel a later queued turn when the interrupt follower catches up during preparation', async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let preparing = false;
    const interrupt = vi.fn();
    const { bridge, store, runner } = buildBridge({
      accounting: {
        prepare: async () => {},
        usage: async (_, line) => line,
        checkpoint: async () => {},
        complete: async () => {},
        fail() {},
        interrupt,
      },
      beforeTurn: async () => {
        preparing = true;
        await gate;
      },
    });
    await appendUserInterrupt(store);
    const later = await appendUserMessage(store, 'run after the older interrupt');
    const read = store.read.bind(store);
    let reads = 0;
    let caughtUp = false;
    vi.spyOn(store, 'read').mockImplementation(async function* (...args) {
      const interruptFollower = ++reads === 3;
      if (interruptFollower) await waitFor(() => preparing);
      yield* read(...args);
      if (interruptFollower) caughtUp = true;
    });
    runner.connect();
    bridge.start();
    try {
      await waitFor(() => preparing && caughtUp);
      // Let catchUpInterrupts consume the read before releasing the slow preflight.
      await new Promise<void>((resolve) => setTimeout(resolve, 10));
      release();
      await waitFor(() => runner.turns.length === 1);
      expect(runner.turns[0]!.userEvent['id']).toBe(later.id);
      expect(runner.interrupts).toEqual([]);
      expect(interrupt).not.toHaveBeenCalled();
      expect(
        store.snapshot(WORKSPACE_ID, SESSION_ID).some((event) => event.kind === 'session.error'),
      ).toBe(false);
    } finally {
      release();
      await bridge.stop();
    }
  });

  it.each([
    { guarded: false, stage: 'resources' },
    { guarded: true, stage: 'resources' },
    { guarded: false, stage: 'accounting' },
    { guarded: true, stage: 'accounting' },
  ])(
    'honors interrupts during $stage preflight and retains guarded=$guarded accounting',
    async ({ guarded, stage }) => {
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      let preparing = false;
      let pending = false;
      let failed = false;
      let requests = 0;
      const accounting: RunnerTurnAccounting = {
        async prepare() {
          if (pending)
            throw new RunnerRequestDenied('pending usage', 'guardrail_usage_unavailable');
          // Snapshot resolution may initialize accounting AFTER the interrupt arrived.
          failed = false;
          pending = guarded;
        },
        async usage(_, line) {
          return line;
        },
        async checkpoint() {},
        async complete() {
          if (!failed) pending = false;
        },
        interrupt() {
          failed = true;
        },
        fail() {
          failed = true;
        },
      };
      const { bridge, store, runner } = buildBridge({
        resources: { commit: async () => {} },
        accounting,
        beforeTurn: async (event) => {
          if (requests++ === 0) {
            if (stage === 'accounting') await accounting.prepare(event, {} as never);
            preparing = true;
            await gate;
            if (stage === 'accounting') {
              if (failed)
                throw new RunnerRequestDenied(
                  'accounting interrupted',
                  'guardrail_usage_unavailable',
                );
              return;
            }
          }
          await accounting.prepare(event, {} as never);
        },
      });
      runner.connect();
      bridge.start();
      try {
        const first = await appendUserMessage(store, 'interrupt while preparing');
        await waitFor(() => preparing);
        await appendUserInterrupt(store);
        await waitFor(() => runner.interrupts.length === 1);
        release();
        await waitFor(() =>
          store
            .snapshot(WORKSPACE_ID, SESSION_ID)
            .some((event) => event.kind === 'agent.turn_completed'),
        );
        expect(runner.turns).toEqual([]);
        expect(failed).toBe(true);
        expect(pending).toBe(guarded);
        const emitted = store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .filter((event) => event.producedBy === AGENT_EVENT_PRODUCED_BY);
        expect(emitted.map((event) => event.kind)).toEqual([
          'session.error',
          'session.status_idle',
          'agent.turn_completed',
        ]);
        expect(JSON.parse(Buffer.from(emitted[0]!.payload).toString())).toMatchObject({
          error: { type: 'interrupted' },
          retry_status: { will_retry: false },
        });
        expect(JSON.parse(Buffer.from(emitted[2]!.payload).toString())).toMatchObject({
          turn_event_id: first.id,
        });
        await appendUserMessage(store, 'next request');
        await waitFor(() =>
          guarded
            ? store
                .snapshot(WORKSPACE_ID, SESSION_ID)
                .filter((event) => event.kind === 'agent.turn_completed').length === 2
            : runner.turns.length === 1,
        );
        expect(runner.turns).toHaveLength(guarded ? 0 : 1);
        expect(pending).toBe(guarded);
        await appendToolConfirmation(store, 'still_alive', 'allow');
        await appendUserInterrupt(store);
        await waitFor(() => runner.confirmations.length === 1 && runner.interrupts.length === 2);
        expect(bridge.running).toBe(true);
      } finally {
        release();
        await bridge.stop();
      }
    },
  );

  it('denies before the provider and keeps confirmation/interrupt followers alive', async () => {
    const { bridge, store, runner } = buildBridge({
      resources: { commit: async () => {} },
      beforeTurn: async (event) => {
        expect(event.producedBy).toBe('client');
        throw new RunnerRequestDenied('pending usage', 'guardrail_usage_unavailable');
      },
    });
    runner.connect();
    bridge.start();
    try {
      await appendUserMessage(store, 'blocked');
      await waitFor(() =>
        store.snapshot(WORKSPACE_ID, SESSION_ID).some((e) => e.kind === 'session.status_idle'),
      );
      expect(runner.turns).toHaveLength(0);
      expect(
        JSON.parse(
          Buffer.from(
            store.snapshot(WORKSPACE_ID, SESSION_ID).find((e) => e.kind === 'session.error')!
              .payload,
          ).toString(),
        ),
      ).toMatchObject({
        error: { type: 'guardrail_usage_unavailable' },
        retry_status: { will_retry: false },
      });
      await appendToolConfirmation(store, 'tool_pending', 'allow');
      await appendUserInterrupt(store);
      await waitFor(() => runner.confirmations.length === 1 && runner.interrupts.length === 1);
      expect(bridge.running).toBe(true);
    } finally {
      await bridge.stop();
    }
  });

  it.each(['none', 'usage', 'checkpoint', 'completion'])(
    'never forwards completion before durable accounting (failure=%s)',
    async (failure) => {
      const calls: string[] = [];
      let release!: () => void;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      const accounting: RunnerTurnAccounting = {
        prepare: async () => {},
        interrupt() {},
        fail() {},
        usage: async (_id, line) => {
          calls.push('usage');
          await held;
          if (failure === 'usage') throw new Error('usage ACK failed');
          return { ...line, id: 'evt_authoritative' };
        },
        checkpoint: async () => {
          calls.push('checkpoint');
          if (failure === 'checkpoint') throw new Error('checkpoint ACK failed');
        },
        complete: async () => {
          calls.push('completion');
          if (failure === 'completion') throw new Error('marker ACK failed');
        },
      };
      const logger = recordingLogger();
      const { bridge, store, runner } = buildBridge({ accounting, logger });
      runner.connect();
      const user = await appendUserMessage(store, 'account');
      runner.scriptTurn(user.id, {
        lines: [
          { type: 'agent.usage', usage: {} },
          { type: 'orca.harness_checkpoint', provider: 'codex-sdk', state: {} },
          { type: 'agent.turn_completed' },
        ],
      });
      bridge.start();
      try {
        await waitFor(() => calls.length === 1);
        expect(
          store.snapshot(WORKSPACE_ID, SESSION_ID).filter((e) => e.producedBy === 'harness'),
        ).toHaveLength(0);
        release();
        await waitFor(() =>
          failure === 'none'
            ? store
                .snapshot(WORKSPACE_ID, SESSION_ID)
                .some((e) => e.kind === 'agent.turn_completed')
            : logger.errors.length + logger.warns.length > 0,
        );
        expect(calls).toEqual(
          failure === 'usage'
            ? ['usage']
            : failure === 'checkpoint'
              ? ['usage', 'checkpoint']
              : ['usage', 'checkpoint', 'completion'],
        );
        if (failure !== 'none') {
          await waitFor(() => !bridge.running);
          const queued = await appendUserMessage(store, 'must not overtake failed accounting');
          runner.scriptTurn(queued.id, { lines: [{ type: 'agent.turn_completed' }] });
          bridge.start(); // This generation cannot be revived after persistence failure.
          expect(bridge.running).toBe(false);
          expect(runner.turns).toHaveLength(1);
        }
        const all = store.snapshot(WORKSPACE_ID, SESSION_ID);
        expect(all.some((e) => e.kind === 'orca.harness_checkpoint')).toBe(false);
        expect(all.some((e) => e.kind === 'agent.turn_completed')).toBe(failure === 'none');
      } finally {
        release();
        await bridge.stop();
      }
    },
  );
});
it.each(['session.resource_mounted', 'session.output_indexed'])(
  'does not treat preparation receipt %s as an answered user message',
  async (kind) => {
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    const user = await appendUserMessage(store, 'queued before preparation');
    await appendAgentEvent(store, kind);
    bridge.start();
    try {
      await waitFor(() => runner.turns.length === 1);
      expect(runner.turns[0]!.userEvent.id).toBe(user.id);
    } finally {
      await bridge.stop();
    }
  },
);

it.each([false, true])(
  'publishes mounts only after acknowledged preparation and recovery (failed ACK=%s)',
  async (fail) => {
    const store = new InMemoryTranscriptStore(),
      registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    if (fail) runner.preparationStatus = 503;
    const published: string[] = [];
    const refreshed = vi.fn(async () => {});
    const logger = recordingLogger();
    const manager = new SessionEventBridgeManager({
      store,
      registry,
      resolver: new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID),
      logger,
      resourcesFactory: async () => ({
        deliver: async () => ({ version: 1, revision: 'a'.repeat(64) }),
        commit: async () => {},
        refreshGitCapabilities: refreshed,
        publishMounted: async () => {
          expect(runner.preparations.slice(-2)).toEqual([
            '/v1/runner/skills',
            '/v1/runner/snapshot',
          ]);
          expect(runner.replays.length).toBe(1);
          published.push('mounted');
        },
      }),
      skillsProvider: { resolveSkillBundles: async () => [] },
      skillStore: new InMemorySkillStore(),
      snapshotProvider: {
        resolve: async () => ({
          provider: 'codex-sdk',
          model: { provider: 'openai', id: 'gpt-5.4' },
          system: '',
          allowed_tool_names: [],
          allowed_mcp_server_names: [],
          egress: {
            mode: 'gateway',
            gateway: { mcp_base_url: 'http://gateway', session_jwt: 'a.b.c', mcp_servers: {} },
          },
        }),
      },
    });
    try {
      await manager.onRunnerConnect(RUNNER_ID);
      if (!fail) expect(logger.errors).toEqual([]);
      expect(published).toHaveLength(fail ? 0 : 1);
      if (!fail) {
        await appendUserMessage(store, 'refresh');
        await waitFor(() => runner.turns.length === 1);
        expect(published).toHaveLength(2);
        expect(refreshed).toHaveBeenCalledTimes(3);
      } else expect(manager.size).toBe(0);
    } finally {
      await manager.stopAll();
    }
  },
);

it.each([false, true])(
  'drains initial mount publication across rapid reconnects (append fails=%s)',
  async (fail) => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    new FakeRunner(registry, RUNNER_ID).connect();
    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    let publishing = false;
    let durable = false;
    let prepares = 0;
    let appends = 0;
    const manager = new SessionEventBridgeManager({
      store,
      registry,
      resolver: new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID),
      recoverOnConnect: false,
      skillsProvider: { resolveSkillBundles: async () => [] },
      skillStore: new InMemorySkillStore(),
      snapshotProvider: {
        resolve: async () => ({
          provider: 'codex-sdk',
          model: { provider: 'openai', id: 'gpt-5.4' },
          system: '',
          allowed_tool_names: [],
          allowed_mcp_server_names: [],
          egress: {
            mode: 'gateway',
            gateway: { mcp_base_url: 'http://gateway', session_jwt: 'a.b.c', mcp_servers: {} },
          },
        }),
      },
      resourcesFactory: async () => {
        const first = ++prepares === 1;
        return {
          deliver: async () => ({ version: 1 as const, revision: 'a'.repeat(64) }),
          commit: async () => {},
          publishMounted: async () => {
            if (durable) return;
            appends++;
            if (first) {
              publishing = true;
              await held;
            }
            durable = true;
            if (first && fail) throw new Error('append ACK lost');
          },
        };
      },
    });
    const first = manager.onRunnerConnect(RUNNER_ID);
    await waitFor(() => publishing);
    new FakeRunner(registry, RUNNER_ID).connect();
    const second = manager.onRunnerConnect(RUNNER_ID);
    new FakeRunner(registry, RUNNER_ID).connect();
    const third = manager.onRunnerConnect(RUNNER_ID);
    try {
      await new Promise((resolve) => setTimeout(resolve, 25));
      expect(prepares).toBe(1);
      expect(appends).toBe(1);
      release();
      await Promise.all([first, second, third]);
      expect(prepares).toBe(2);
      expect(appends).toBe(1);
      expect(manager.size).toBe(1);
    } finally {
      release();
      await manager.stopAll();
    }
  },
);

it.each(['disconnect', 'shutdown'] as const)(
  'drains pending mount publication on %s',
  async (end) => {
    const store = new InMemoryTranscriptStore();
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    let publishing = false;
    let stopped = false;
    const manager = new SessionEventBridgeManager({
      store,
      registry,
      recoverOnConnect: false,
      resolver: new FakeResolver().bind(RUNNER_ID, WORKSPACE_ID, SESSION_ID),
      skillsProvider: { resolveSkillBundles: async () => [] },
      skillStore: new InMemorySkillStore(),
      snapshotProvider: {
        resolve: async () => ({
          provider: 'codex-sdk',
          model: { provider: 'openai', id: 'gpt-5.4' },
          system: '',
          allowed_tool_names: [],
          allowed_mcp_server_names: [],
          egress: {
            mode: 'gateway',
            gateway: { mcp_base_url: 'http://gateway', session_jwt: 'a.b.c', mcp_servers: {} },
          },
        }),
      },
      resourcesFactory: async () => ({
        deliver: async () => ({ version: 1, revision: 'a'.repeat(64) }),
        commit: async () => {},
        publishMounted: async () => {
          publishing = true;
          await held;
        },
      }),
    });
    const preparation = manager.onRunnerConnect(RUNNER_ID);
    await waitFor(() => publishing);
    if (end === 'disconnect') runner.disconnect();
    const stopping = (
      end === 'disconnect' ? manager.onRunnerDisconnect(RUNNER_ID) : manager.stopAll()
    ).then(() => {
      stopped = true;
    });
    try {
      await new Promise((resolve) => setTimeout(resolve, 25));
      expect(stopped).toBe(false);
      release();
      await Promise.all([preparation, stopping]);
      expect(manager.size).toBe(0);
    } finally {
      release();
      await manager.stopAll();
    }
  },
);

async function appendCustomResult(
  store: TranscriptStore,
  id: string,
  extra: Record<string, unknown> = {},
): Promise<void> {
  await store.append(WORKSPACE_ID, SESSION_ID, [
    httpEventToProto({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      producedBy: 'client',
      idempotencyKey: '',
      input: {
        type: 'user.custom_tool_result',
        custom_tool_use_id: id,
        content: [{ type: 'text', text: 'ticket' }],
        is_error: true,
        ...extra,
      },
    }),
  ]);
}

describe('custom callback control delivery and recovery', () => {
  it('delivers a result while the turn is parked; never creates a new turn', async () => {
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    const user = await appendUserMessage(store, 'lookup');
    runner.scriptTurnGatedOnConfirmation(user.id, 'evt_custom', [
      { type: 'agent.message', content: [{ type: 'text', text: 'Ticket found' }] },
      { type: 'agent.turn_completed' },
    ]);
    bridge.start();
    try {
      await waitFor(() => runner.turns.length === 1);
      await appendCustomResult(store, 'evt_custom');
      await waitFor(() =>
        store.snapshot(WORKSPACE_ID, SESSION_ID).some((e) => e.kind === 'agent.turn_completed'),
      );
      expect(runner.confirmations).toHaveLength(1);
      expect(runner.confirmations[0]!.frame.path).toBe(RUNNER_CUSTOM_TOOL_RESULT_PATH);
      expect(runner.confirmations[0]!.body).toMatchObject({
        custom_tool_use_id: 'evt_custom',
        content: [{ type: 'text', text: 'ticket' }],
        is_error: true,
      });
      expect(runner.turns).toHaveLength(1);
    } finally {
      await bridge.stop();
    }
  });

  it('catches up a result despite unrelated agent output and retains client/primary filters', async () => {
    const { bridge, store, runner } = buildBridge();
    runner.connect();
    await appendCustomResult(store, 'evt_custom');
    await appendCustomResult(store, 'evt_child', { subpath: 'subagents/child' });
    const forged = httpEventToProto({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      producedBy: 'harness',
      idempotencyKey: '',
      input: { type: 'user.custom_tool_result', custom_tool_use_id: 'evt_forged', content: [] },
    });
    await store.append(WORKSPACE_ID, SESSION_ID, [forged]);
    await appendAgentEvent(store, 'agent.message');
    bridge.start();
    try {
      await waitFor(() => runner.confirmations.length === 1);
      expect(runner.confirmations[0]!.body.custom_tool_use_id).toBe('evt_custom');
      expect(runner.turns).toHaveLength(0);
      await bridge.stop();
      bridge.start();
      await waitFor(() => runner.confirmations.length === 2);
      expect(runner.confirmations.every((c) => c.body.custom_tool_use_id === 'evt_custom')).toBe(
        true,
      );
    } finally {
      await bridge.stop();
    }
  });

  it.each([false, true])(
    'classifies a disconnected callback as abandoned (persisted result=%s), without redriving it',
    async (persistedResult) => {
      const { bridge, store, runner } = buildBridge();
      runner.connect();
      const user = await appendUserMessage(store, 'lookup');
      runner.scriptTurn(user.id, {
        lines: [
          {
            id: 'evt_custom',
            type: 'agent.custom_tool_use',
            name: 'lookup_ticket',
            input: { id: 'T1' },
            source_event_id: 'evt_forged_source',
          },
          {
            type: 'session.status_idle',
            stop_reason: { type: 'requires_action', event_ids: ['evt_custom'] },
          },
          { type: 'agent.turn_completed' },
        ],
        dropAfter: 2,
      });
      bridge.start();
      try {
        await waitFor(() =>
          store.snapshot(WORKSPACE_ID, SESSION_ID).some((e) => e.kind === 'agent.custom_tool_use'),
        );
        const callback = store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .find((e) => e.kind === 'agent.custom_tool_use')!;
        expect(payloadJson(callback).source_event_id).toBe(user.id);
        expect(toPublicHttpEvent(protoToHttpEvent(callback), false)).not.toHaveProperty(
          'source_event_id',
        );
        await new Promise((resolve) => setTimeout(resolve, 20));
        await bridge.stop();
        if (persistedResult) await appendCustomResult(store, 'evt_custom');
        runner.connect();
        bridge.start();
        await waitFor(() =>
          store.snapshot(WORKSPACE_ID, SESSION_ID).some((e) => e.kind === 'session.error'),
        );
        const error = store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .find((e) => e.kind === 'session.error')!;
        expect(payloadJson(error)).toMatchObject({
          error: {
            type: 'custom_tool_callback_abandoned',
            message: expect.stringContaining('evt_custom'),
          },
        });
        expect(runner.turns).toHaveLength(1);
        const completed = store
          .snapshot(WORKSPACE_ID, SESSION_ID)
          .filter((e) => e.kind === 'agent.turn_completed');
        expect(completed).toHaveLength(1);
        expect(payloadJson(completed[0]!)).toMatchObject({ turn_event_id: user.id });
        await bridge.stop();
        bridge.start();
        await appendUserMessage(store, 'New independent turn');
        await waitFor(() => runner.turns.length === 2);
        expect(
          store.snapshot(WORKSPACE_ID, SESSION_ID).filter((e) => e.kind === 'session.error'),
        ).toHaveLength(1);
      } finally {
        await bridge.stop();
      }
    },
  );

  it('does not diagnose completed callbacks or append an orphan failure after ownership is lost', async () => {
    let owned = true;
    const { bridge, store, runner } = buildBridge({ ownsSession: async () => owned });
    runner.connect();
    await appendUserMessage(store, 'old');
    await appendAgentEvent(store, 'agent.custom_tool_use');
    await appendAgentEvent(store, 'agent.turn_completed');
    bridge.start();
    await new Promise((resolve) => setTimeout(resolve, 20));
    await bridge.stop();
    expect(store.snapshot(WORKSPACE_ID, SESSION_ID).some((e) => e.kind === 'session.error')).toBe(
      false,
    );
    await appendUserMessage(store, 'orphan');
    await appendAgentEvent(store, 'agent.custom_tool_use');
    owned = false;
    bridge.start();
    try {
      await appendCustomResult(store, 'evt_custom');
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(runner.confirmations).toHaveLength(0);
      expect(store.snapshot(WORKSPACE_ID, SESSION_ID).some((e) => e.kind === 'session.error')).toBe(
        false,
      );
    } finally {
      await bridge.stop();
    }
  });
});

it('does not lose a custom result appended between catch-up and live follow', async () => {
  class GapStore extends InMemoryTranscriptStore {
    private injected = false;
    override async *tail(
      workspaceId: string,
      sessionId: string,
      opts: TailOptions,
    ): AsyncIterable<Event> {
      if (!this.injected) {
        this.injected = true;
        await appendCustomResult(this, 'evt_gap');
      }
      yield* super.tail(workspaceId, sessionId, opts);
    }
  }
  const { bridge, runner } = buildBridge({ store: new GapStore() });
  runner.connect();
  bridge.start();
  try {
    await waitFor(() => runner.confirmations.length === 1);
    expect(runner.confirmations[0]!.body.custom_tool_use_id).toBe('evt_gap');
    expect(runner.turns).toHaveLength(0);
  } finally {
    await bridge.stop();
  }
});

it('keeps a queued new message after abandoning the callback of the preceding turn', async () => {
  const { bridge, store, runner } = buildBridge();
  runner.connect();
  const original = await appendUserMessage(store, 'Start callback');
  const queued = await appendUserMessage(store, 'Independent next turn');
  const callback = httpEventToProto({
    workspaceId: WORKSPACE_ID,
    sessionId: SESSION_ID,
    producedBy: 'harness',
    idempotencyKey: '',
    input: {
      type: 'agent.custom_tool_use',
      name: 'lookup_ticket',
      input: {},
      source_event_id: original.id,
    },
  });
  await store.append(WORKSPACE_ID, SESSION_ID, [callback]);
  await appendCustomResult(store, callback.id);
  bridge.start();
  try {
    await waitFor(() => runner.turns.length === 1);
    expect(runner.turns[0]!.userEvent.id).toBe(queued.id);
    const marker = store
      .snapshot(WORKSPACE_ID, SESSION_ID)
      .find((e) => e.kind === 'agent.turn_completed')!;
    expect(payloadJson(marker).turn_event_id).toBe(original.id);
  } finally {
    await bridge.stop();
  }
});
