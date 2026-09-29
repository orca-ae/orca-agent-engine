// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the owner-pod reverse-lookup session recovery.
//
// Recovery is the registry/owner-pod side of an exactly-once resume when a runner
// (re)connects: a catch-up scan plus resume-from-last-item, performed by the
// OWNER POD, which is the single writer of the session's agent events and can
// read the (cross-pod) transcript any pod persisted.
//
// The approved recovery decision this spec pins:
//
//   1. CURSOR. On (re)connect the runner presents its cursor — the id of the last
//      transcript event it consumed. Recovery serves events `after={cursor}` IN
//      ORDER from the transcript and PUSHES them down the runner tunnel so the
//      runner can rebuild from where it left off. The runner dedups by event id.
//   2. FRESH REPLAY. A fresh runner presents an EMPTY cursor and gets a FULL replay
//      from the start of the transcript, to rebuild its whole state.
//   3. START-TURN-IF-PENDING. Recovery starts a turn ONLY IF a `user.message` is
//      pending PAST the last completed `agent.turn_completed` — it never re-runs a
//      turn that already completed. The pending user turn it surfaces is what the
//      caller drives (via the owner-pod event bridge).
//   4. EXACTLY-ONCE. Recovery never appends to the transcript; the replay push is
//      idempotent on the runner (dedup by id) and a turn is driven at most once
//      (the pending-turn rule), so a re-connect / re-run replays state without
//      duplicating it.
//
// Everything runs IN-PROCESS, with no DB / Kafka / real sockets:
//   * the REAL `TunnelRegistry` + `TunnelTransport` (network-free by design),
//   * a FAKE in-process runner — a ws peer speaking the tunnel frame protocol that
//     records the replay POST the owner pushes (decoding its NDJSON body) and
//     answers it, driven the same way the bridge spec drives its fake runner,
//   * an in-memory `TranscriptStore` with REAL append/read ordering so the spec can
//     assert the `after={cursor}` slice + IN-ORDER replay against persisted seqs.

import { describe, it, expect } from 'vitest';
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
import { httpEventToProto } from '../../src/domain/events.js';
import {
  SessionRecovery,
  RUNNER_REPLAY_PATH,
  RUNNER_SESSION_HEADER,
  RUNNER_RESUME_CURSOR_HEADER,
  COMPLETED_TURN_EVENT_KIND,
  type SessionRecoveryLogger,
} from '../../src/tunnel/session-recovery.js';
import { sessionRecoveryReplayedTotal } from '../../src/metrics.js';

const WORKSPACE_ID = 'ws_acme';
const SESSION_ID = 'ses_recover_1';
const RUNNER_ID = 'runner_token_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

// ── In-memory transcript store with REAL append/read ordering ──

/**
 * An in-memory {@link TranscriptStore} honouring the contract recovery relies on:
 * `append` assigns a strictly increasing 1-based `seq` per (workspace, session)
 * and returns each event id; `read` with an empty `fromCursor` walks from the
 * beginning to the high-watermark at call time, and an explicit numeric cursor
 * yields `seq >= fromCursor`. A faithful in-process stand-in for the
 * Kafka/Postgres/Pulsar backends — enough to assert the `after={cursor}` replay
 * slice + IN-ORDER delivery. `tail` is provided for interface completeness (the
 * recovery path under test only `read`s); it follows from an explicit cursor or
 * from-now, like the real backends.
 */
class InMemoryTranscriptStore implements TranscriptStore {
  private readonly logs = new Map<string, Event[]>();
  private readonly waiters = new Map<string, Set<() => void>>();
  /** Records every read call so a spec can assert the cursor recovery read with. */
  readonly readCalls: Array<{ key: string; opts: ReadOptions }> = [];

  private key(workspaceId: string, sessionId: string): string {
    return `${workspaceId}/${sessionId}`;
  }

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    const key = this.key(workspaceId, sessionId);
    const log = this.logs.get(key) ?? [];
    this.logs.set(key, log);
    const ids: string[] = [];
    for (const event of events) {
      const stored: Event = { ...event, seq: log.length + 1 };
      log.push(stored);
      ids.push(stored.id);
    }
    this.wake(key);
    return ids;
  }

  async *read(workspaceId: string, sessionId: string, opts: ReadOptions): AsyncIterable<Event> {
    this.readCalls.push({ key: this.key(workspaceId, sessionId), opts: { ...opts } });
    const log = this.logs.get(this.key(workspaceId, sessionId)) ?? [];
    const from = opts.fromCursor === '' ? 0 : Number(opts.fromCursor);
    let count = 0;
    for (const event of log) {
      if (event.seq < from) continue;
      if (!subpathMatches(opts.subpath, event.subpath)) continue;
      if (opts.maxEvents > 0 && count++ >= opts.maxEvents) break;
      yield { ...event };
    }
  }

  async *tail(workspaceId: string, sessionId: string, opts: TailOptions): AsyncIterable<Event> {
    const key = this.key(workspaceId, sessionId);
    let cursor =
      opts.fromCursor === '' ? (this.logs.get(key)?.length ?? 0) : Number(opts.fromCursor) - 1;
    const signal = opts.signal;
    for (;;) {
      if (signal?.aborted) return;
      const log = this.logs.get(key) ?? [];
      let advanced = false;
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
 * A fake runner that owns the runner end of a tunnel and records what the owner
 * pod PUSHES to it during recovery. It registers in the REAL `TunnelRegistry`
 * exactly as the production runner-tunnel route does, decodes the `request`
 * frames the owner sends, and answers each replay POST with `response.head` →
 * `response.end` (a 200 ack with no body — the runner consumed the replay).
 *
 * The replay POST's NDJSON body (one persisted-event JSON per line, each with its
 * stable `id`) is decoded and recorded so the spec can assert the exact `after=
 * {cursor}` slice + ORDER the owner served, and that the runner can dedup by id.
 */
class FakeRunner {
  /** Each replay POST the owner pushed: its decoded header cursor + event lines. */
  readonly replays: Array<{
    cursor: string;
    sessionId: string;
    lines: Array<Record<string, unknown>>;
  }> = [];
  /** Raw request frames seen, for asserting the route/method/headers contract. */
  readonly requests: RequestFrame[] = [];
  /** Optional override of the status the runner answers a replay POST with. */
  replayStatus = 200;
  /** When true, the runner DROPS the tunnel instead of acking a replay POST. */
  dropOnReplay = false;
  private readonly socket: RunnerSocket;
  private connected = false;

  constructor(
    private readonly registry: TunnelRegistry,
    private readonly runnerId: string,
  ) {
    this.socket = new RunnerSocket((raw) => this.onOwnerFrame(raw));
  }

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

  disconnect(): void {
    if (!this.connected) return;
    this.registry.deregister(this.runnerId);
    this.connected = false;
  }

  private startOutboundDrain(session: RegistrySession): void {
    void (async () => {
      for (;;) {
        const data = await session.outboundQueue.get();
        if (data === null) {
          return;
        }
        await this.socket.sendText(data);
      }
    })();
  }

  private onOwnerFrame(raw: string): void {
    let frame: Frame;
    try {
      frame = decodeFrame(raw);
    } catch {
      return;
    }
    if (frame.kind === FrameKind.Ping) {
      void this.registry.get(this.runnerId);
      this.route(encodeFrame({ kind: FrameKind.Pong, ts: frame.ts }));
      return;
    }
    if (frame.kind === FrameKind.RequestCancel) {
      this.route(encodeFrame({ kind: FrameKind.ResponseEnd, id: frame.id }));
      return;
    }
    if (frame.kind !== FrameKind.Request) {
      return;
    }
    this.handleReplay(frame);
  }

  /** Record a replay POST (decoding its NDJSON body) and ack it (or drop). */
  private handleReplay(frame: RequestFrame): void {
    this.requests.push(frame);
    const headers = new Map((frame.headers ?? []).map(([k, v]) => [k.toLowerCase(), v]));
    const cursor = headers.get(RUNNER_RESUME_CURSOR_HEADER.toLowerCase()) ?? '';
    const sessionId = headers.get(RUNNER_SESSION_HEADER.toLowerCase()) ?? '';
    const lines = decodeNdjsonBody(frame);
    this.replays.push({ cursor, sessionId, lines });
    if (this.dropOnReplay) {
      // Simulate a runner that died before acking — drop the tunnel without an
      // end frame, deferred so the owner's send completes first.
      setTimeout(() => this.disconnect(), 5);
      return;
    }
    this.route(
      encodeFrame({
        kind: FrameKind.ResponseHead,
        id: frame.id,
        status: this.replayStatus,
        headers: [['content-type', 'application/json']],
      }),
    );
    this.route(encodeFrame({ kind: FrameKind.ResponseEnd, id: frame.id }));
  }

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

/** The runner side of a tunnel as a {@link RegistryWebSocketLike}. */
class RunnerSocket implements RegistryWebSocketLike {
  closed: { code: number; reason: string } | undefined;
  constructor(private readonly onFrame: (raw: string) => void) {}

  async sendText(data: string): Promise<void> {
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

/** Decode a `request` frame's NDJSON body into the per-line JSON objects. */
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

// ── Helpers to seed transcript events ──

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

let agentEventCounter = 0;

/** Append one agent (`producedBy=harness`) event of a given kind. */
async function appendAgentEvent(store: TranscriptStore, kind: string): Promise<Event> {
  agentEventCounter += 1;
  const proto = httpEventToProto({
    workspaceId: WORKSPACE_ID,
    sessionId: SESSION_ID,
    producedBy: 'harness',
    idempotencyKey: '',
    input: { id: `evt_agent_${agentEventCounter}`, type: kind },
  });
  const [id] = await store.append(WORKSPACE_ID, SESSION_ID, [proto]);
  return { ...proto, id: id ?? proto.id };
}

/** Append an `agent.turn_completed` marker — the completed-turn boundary. */
function appendTurnCompleted(store: TranscriptStore): Promise<Event> {
  return appendAgentEvent(store, COMPLETED_TURN_EVENT_KIND);
}

/** One recorded log call: the structured object and the optional message. */
interface LogCall {
  obj: unknown;
  msg: string | undefined;
}

/** A logger that records every call so the spec can assert error reporting. */
function recordingLogger(): SessionRecoveryLogger & {
  infos: LogCall[];
  warns: LogCall[];
  errors: LogCall[];
} {
  const infos: LogCall[] = [];
  const warns: LogCall[] = [];
  const errors: LogCall[] = [];
  return {
    infos,
    warns,
    errors,
    info(obj, msg) {
      infos.push({ obj, msg });
    },
    warn(obj, msg) {
      warns.push({ obj, msg });
    },
    error(obj, msg) {
      errors.push({ obj, msg });
    },
  };
}

/** Build a recovery + its collaborators over a (default fresh) store + runner. */
function buildRecovery(
  over: {
    store?: InMemoryTranscriptStore;
    logger?: SessionRecoveryLogger;
    maxReplayEvents?: number;
    replayChunkSize?: number;
  } = {},
): {
  recovery: SessionRecovery;
  store: InMemoryTranscriptStore;
  registry: TunnelRegistry;
  runner: FakeRunner;
} {
  const store = over.store ?? new InMemoryTranscriptStore();
  const registry = new TunnelRegistry();
  const runner = new FakeRunner(registry, RUNNER_ID);
  const recovery = new SessionRecovery({
    workspaceId: WORKSPACE_ID,
    sessionId: SESSION_ID,
    runnerId: RUNNER_ID,
    store,
    registry,
    ...(over.logger !== undefined ? { logger: over.logger } : {}),
    ...(over.maxReplayEvents !== undefined ? { maxReplayEvents: over.maxReplayEvents } : {}),
    ...(over.replayChunkSize !== undefined ? { replayChunkSize: over.replayChunkSize } : {}),
  });
  return { recovery, store, registry, runner };
}

/** Read the `id`s the runner saw in a replay push, in order. */
function replayIds(replay: { lines: Array<Record<string, unknown>> }): string[] {
  return replay.lines.map((l) => String(l['id']));
}

/** Concatenate the lines across every replay push, in push order. */
function allReplayedLines(runner: FakeRunner): Array<Record<string, unknown>> {
  return runner.replays.flatMap((r) => r.lines);
}

/** Current value of the `sessionRecoveryReplayedTotal` counter for a `mode`. */
async function replayedMetric(mode: 'fresh' | 'resume'): Promise<number> {
  const metric = (await sessionRecoveryReplayedTotal.get()).values.find(
    (v) => v.labels['mode'] === mode,
  );
  return metric?.value ?? 0;
}

// ── 1. fresh runner: full replay from the start ──

it('keeps a queued message pending when the preceding turn completes after it', async () => {
  const { recovery, store, runner } = buildRecovery();
  runner.connect();
  for (const input of [
    { id: 'evt_a', type: 'user.message', content: [{ type: 'text', text: 'A' }] },
    { id: 'evt_b', type: 'user.message', content: [{ type: 'text', text: 'B' }] },
    { id: 'evt_completed_a', type: 'agent.turn_completed', turn_event_id: 'evt_a' },
  ])
    await store.append(WORKSPACE_ID, SESSION_ID, [
      httpEventToProto({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        idempotencyKey: '',
        producedBy: input.type === 'user.message' ? 'client' : 'harness',
        input,
      }),
    ]);
  expect((await recovery.recover('')).pendingUserTurn?.id).toBe('evt_b');
});

describe('session-recovery — fresh runner (empty cursor) → full replay', () => {
  it('pushes every transcript event from the start, in order, to a fresh runner', async () => {
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    // History the prior generation persisted: a completed turn (user + agent).
    const u1 = await appendUserMessage(store, 'first');
    const a1 = await appendAgentEvent(store, 'response.created');
    const c1 = await appendTurnCompleted(store);

    const outcome = await recovery.recover('');

    // A fresh runner gets a FULL replay: every event from the start, in seq order.
    expect(runner.replays.length).toBe(1);
    const replay = runner.replays[0]!;
    expect(replay.cursor).toBe('');
    expect(replay.sessionId).toBe(SESSION_ID);
    expect(replayIds(replay)).toEqual([u1.id, a1.id, c1.id]);
    // The pushed lines carry the stable transcript event ids the runner dedups by,
    // and the event kinds (so the runner can rebuild its state).
    expect(replay.lines.map((l) => l['type'])).toEqual([
      'user.message',
      'response.created',
      COMPLETED_TURN_EVENT_KIND,
    ]);
    // The history ends at a completed turn — nothing pending — so no turn starts.
    expect(outcome.replayedCount).toBe(3);
    expect(outcome.pendingUserTurn).toBeNull();
    expect(outcome.fresh).toBe(true);
    expect(recovery.lastReplayCursor).toBe('');
  });

  it('reads the transcript from the beginning (empty read cursor) for a fresh runner', async () => {
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    await appendUserMessage(store, 'one');
    await recovery.recover('');
    // Recovery reads from the start (read cursor "") so the replay slice is the
    // whole transcript; the after-cursor slice is computed in-process over it.
    expect(store.readCalls.length).toBeGreaterThanOrEqual(1);
    expect(store.readCalls[0]!.opts.fromCursor).toBe('');
    expect(store.readCalls[0]!.opts.subpath).toBe('');
  });

  it('pushes an empty full replay when the transcript is empty (brand-new session)', async () => {
    const { recovery, runner } = buildRecovery();
    runner.connect();
    const outcome = await recovery.recover('');
    // A fresh runner on an empty transcript: the owner still acks the resume (a
    // zero-line replay) and nothing is pending.
    expect(runner.replays.length).toBe(1);
    expect(runner.replays[0]!.lines).toEqual([]);
    expect(outcome.replayedCount).toBe(0);
    expect(outcome.pendingUserTurn).toBeNull();
    expect(outcome.fresh).toBe(true);
  });
});

// ── 2. reconnect at a cursor: serve events AFTER it, in order ──

describe('session-recovery — reconnect at cursor → after={cursor} replay', () => {
  it("pushes only the events AFTER the runner's cursor, in order", async () => {
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    // The runner already consumed u1 + a1; its cursor is a1. Everything after a1
    // must be replayed (and only that), in order.
    const u1 = await appendUserMessage(store, 'old turn');
    const a1 = await appendAgentEvent(store, 'response.created');
    const a2 = await appendAgentEvent(store, 'response.output_text.delta');
    const c1 = await appendTurnCompleted(store);

    const outcome = await recovery.recover(a1.id);

    expect(runner.replays.length).toBe(1);
    const replay = runner.replays[0]!;
    expect(replay.cursor).toBe(a1.id);
    // Strictly AFTER the cursor: a2, c1 — NOT u1 or a1 (already consumed).
    expect(replayIds(replay)).toEqual([a2.id, c1.id]);
    expect(replay.lines.some((l) => l['id'] === u1.id)).toBe(false);
    expect(replay.lines.some((l) => l['id'] === a1.id)).toBe(false);
    expect(outcome.replayedCount).toBe(2);
    // History ended at a completed turn → nothing pending.
    expect(outcome.pendingUserTurn).toBeNull();
    expect(outcome.fresh).toBe(false);
    expect(recovery.lastReplayCursor).toBe(a1.id);
  });

  it('pushes an empty replay when the cursor is already at the head (caught up)', async () => {
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    await appendUserMessage(store, 'turn');
    const last = await appendTurnCompleted(store);
    // The runner already consumed everything: cursor is the head event.
    const outcome = await recovery.recover(last.id);
    expect(runner.replays.length).toBe(1);
    expect(runner.replays[0]!.lines).toEqual([]);
    expect(outcome.replayedCount).toBe(0);
    expect(outcome.pendingUserTurn).toBeNull();
  });

  it('pushes an empty replay (NOT a full replay) when the cursor id is unknown', async () => {
    // A cursor that names an id not in the transcript (e.g. the runner is ahead of
    // what THIS pod can see, or a stale/foreign id): `after={unknown}` yields
    // nothing — recovery must NOT silently fall back to a from-beginning replay,
    // which would re-push (and let the runner re-apply) the whole history. A
    // non-empty cursor is never treated as fresh.
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    await appendUserMessage(store, 'a');
    await appendTurnCompleted(store);
    const outcome = await recovery.recover('evt_does_not_exist');
    expect(runner.replays.length).toBe(1);
    expect(runner.replays[0]!.lines).toEqual([]);
    expect(outcome.replayedCount).toBe(0);
    expect(outcome.fresh).toBe(false);
    expect(outcome.pendingUserTurn).toBeNull();
  });
});

// ── 3. start-turn-if-pending: pending user.message past last turn_completed ──

describe('session-recovery — start-turn-if-pending (past last agent.turn_completed)', () => {
  it('surfaces a pending user.message that arrived AFTER the last completed turn (mid-session)', async () => {
    // A completed turn, then a fresh user.message with no completed turn after it.
    // That message is pending — recovery surfaces it as the turn to drive.
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    await appendUserMessage(store, 'answered');
    await appendTurnCompleted(store);
    const pending = await appendUserMessage(store, 'not yet answered');

    const outcome = await recovery.recover('');
    expect(outcome.pendingUserTurn).not.toBeNull();
    expect(outcome.pendingUserTurn!.id).toBe(pending.id);
    // The pending turn is the LATEST user.message past the completed boundary, and
    // the full history (including it) was replayed.
    expect(replayIds(runner.replays[0]!)).toContain(pending.id);
  });

  it('surfaces the FIRST-EVER user.message as pending when no turn has completed yet', async () => {
    // No agent.turn_completed anywhere: a single user.message is pending (the
    // pre-connect first turn the runner never got to answer).
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    const pending = await appendUserMessage(store, 'first turn ever');
    const outcome = await recovery.recover('');
    expect(outcome.pendingUserTurn).not.toBeNull();
    expect(outcome.pendingUserTurn!.id).toBe(pending.id);
    expect(replayIds(runner.replays[0]!)).toEqual([pending.id]);
  });

  it('does NOT surface a pending turn when the history ends at a completed turn', async () => {
    // user → turn_completed: the turn finished. Nothing is pending; recovery must
    // NOT re-run the finished turn.
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    await appendUserMessage(store, 'done');
    await appendTurnCompleted(store);
    const outcome = await recovery.recover('');
    expect(outcome.pendingUserTurn).toBeNull();
    expect(runner.replays.length).toBe(1);
  });

  it('treats a user.message followed only by NON-completion agent events as pending', async () => {
    // A turn that streamed partial output (response.created) but never reached
    // agent.turn_completed (a mid-turn drop): the user.message is still pending —
    // only the COMPLETED marker closes a turn, not any agent event.
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    const pending = await appendUserMessage(store, 'partial then dropped');
    await appendAgentEvent(store, 'response.created');
    await appendAgentEvent(store, 'response.output_text.delta');
    const outcome = await recovery.recover('');
    expect(outcome.pendingUserTurn).not.toBeNull();
    expect(outcome.pendingUserTurn!.id).toBe(pending.id);
  });

  it('treats a turn that ended in an INTERRUPTED tool exchange (no completion) as pending', async () => {
    // The pending-detection parity nuance: a turn whose last events are a tool call
    // + its output but with NO agent.turn_completed after (an interrupted tool turn)
    // is STILL pending here, because we key on the user.message vs the completed
    // marker — not on the trailing event's kind. So the "trailing tool exchange ⇒
    // pending" case is subsumed by the user-message rule; no special-casing needed.
    // (Documented in registry-service.md — the pending-detection nuance.)
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    const pending = await appendUserMessage(store, 'asked, tool ran, then dropped');
    await appendAgentEvent(store, 'response.output_item.added'); // tool call
    await appendAgentEvent(store, 'response.function_call_arguments.delta'); // tool output, no completion
    const outcome = await recovery.recover('');
    expect(outcome.pendingUserTurn).not.toBeNull();
    expect(outcome.pendingUserTurn!.id).toBe(pending.id);
  });

  it('picks the LATEST pending user.message when several arrived past the boundary', async () => {
    // Two user messages queued past the last completed turn. The latest is the one
    // to drive (the reference resumes the last item); both are replayed so the
    // runner rebuilds the full pending context.
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    await appendUserMessage(store, 'older answered');
    await appendTurnCompleted(store);
    const p1 = await appendUserMessage(store, 'pending one');
    const p2 = await appendUserMessage(store, 'pending two');
    const outcome = await recovery.recover('');
    expect(outcome.pendingUserTurn!.id).toBe(p2.id);
    expect(replayIds(runner.replays[0]!)).toEqual(expect.arrayContaining([p1.id, p2.id]));
  });

  it('does NOT surface a pending turn for a session with no user messages at all', async () => {
    // Only agent events (e.g. a system-seeded transcript): nothing to drive.
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    await appendAgentEvent(store, 'response.created');
    await appendTurnCompleted(store);
    const outcome = await recovery.recover('');
    expect(outcome.pendingUserTurn).toBeNull();
    expect(runner.replays.length).toBe(1);
  });
});

// ── 4. the pending-turn rule is independent of the resume cursor ──

describe('session-recovery — pending decision spans the whole transcript, not just the replay slice', () => {
  it('surfaces a pending turn even when the cursor skips PAST the completed boundary', async () => {
    // The runner already consumed up to the pending user.message (cursor = the
    // pending message itself): the replay slice after it is EMPTY, but the turn is
    // STILL pending and must be driven. The pending decision is computed over the
    // full transcript, not only the after-cursor slice — so an already-consumed
    // pending message is not dropped on the floor.
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    await appendUserMessage(store, 'answered');
    await appendTurnCompleted(store);
    const pending = await appendUserMessage(store, 'pending, already seen by runner');

    const outcome = await recovery.recover(pending.id);
    // Nothing AFTER the cursor → empty replay.
    expect(runner.replays[0]!.lines).toEqual([]);
    // …but the turn is still pending and surfaced for driving.
    expect(outcome.pendingUserTurn).not.toBeNull();
    expect(outcome.pendingUserTurn!.id).toBe(pending.id);
  });

  it('does NOT surface a pending turn when the cursor is past a since-completed turn', async () => {
    // The runner's cursor sits on the pending user.message, but the owner pod's
    // transcript shows the turn has SINCE completed (another generation answered
    // it). The replay carries the new completion; no turn is pending — recovery
    // must not re-run it.
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    await appendUserMessage(store, 'answered');
    await appendTurnCompleted(store);
    const seen = await appendUserMessage(store, 'was pending when runner left');
    const completion = await appendTurnCompleted(store);

    const outcome = await recovery.recover(seen.id);
    // The replay catches the runner up with the completion it missed.
    expect(replayIds(runner.replays[0]!)).toEqual([completion.id]);
    // The turn is now complete → nothing pending.
    expect(outcome.pendingUserTurn).toBeNull();
  });
});

// ── 5. exactly-once: recovery never writes, replay is idempotent ──

describe('session-recovery — exactly-once (no transcript writes; idempotent replay)', () => {
  it('never appends to the transcript during recovery', async () => {
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    await appendUserMessage(store, 'turn');
    await appendTurnCompleted(store);
    const before = store.snapshot(WORKSPACE_ID, SESSION_ID).length;
    await recovery.recover('');
    const after = store.snapshot(WORKSPACE_ID, SESSION_ID).length;
    // Recovery is read-only: it pushes replay + surfaces a decision, it does not
    // write. Exactly-once append is the bridge/store's job, not recovery's.
    expect(after).toBe(before);
  });

  it('is idempotent across repeated recover() calls (same cursor → same replay, no new state)', async () => {
    // Two recover() calls at the same cursor (e.g. a flapping reconnect) push the
    // SAME after-cursor slice each time. The runner dedups by event id, so re-
    // pushing the same ids does not duplicate its state — exactly-once across
    // reconnects.
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    const u1 = await appendUserMessage(store, 'old');
    const a1 = await appendAgentEvent(store, 'response.created');
    const c1 = await appendTurnCompleted(store);

    await recovery.recover(u1.id);
    await recovery.recover(u1.id);
    expect(runner.replays.length).toBe(2);
    // Identical id slices both times — the runner's dedup makes the second a no-op.
    expect(replayIds(runner.replays[0]!)).toEqual([a1.id, c1.id]);
    expect(replayIds(runner.replays[1]!)).toEqual([a1.id, c1.id]);
    // The transcript is unchanged by either replay.
    expect(store.snapshot(WORKSPACE_ID, SESSION_ID).length).toBe(3);
  });

  it('advances the served slice as the runner advances its cursor across reconnects', async () => {
    // First reconnect at a1 replays a2,c1; the runner consumes them and reconnects
    // at c1 — the second replay is empty (nothing left). No event is served twice
    // for a monotonically advancing cursor.
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    await appendUserMessage(store, 'turn');
    const a1 = await appendAgentEvent(store, 'response.created');
    const a2 = await appendAgentEvent(store, 'response.output_text.delta');
    const c1 = await appendTurnCompleted(store);

    await recovery.recover(a1.id);
    expect(replayIds(runner.replays[0]!)).toEqual([a2.id, c1.id]);
    await recovery.recover(c1.id);
    expect(runner.replays[1]!.lines).toEqual([]);
  });
});

// ── 6. error / edge paths ──

describe('session-recovery — error paths', () => {
  it('throws ConnectError-flavoured failure (logged) when the runner is offline at recover time', async () => {
    const logger = recordingLogger();
    const { recovery, store } = buildRecovery({ logger });
    // Runner NEVER connects — the replay push cannot be delivered.
    await appendUserMessage(store, 'while offline');
    const outcome = await recovery.recover('');
    // Recovery reports the offline runner and surfaces it as a non-delivered
    // replay, but still computes the pending decision so the caller can act once
    // the runner returns (and never throws out of the connect hook).
    expect(outcome.replayDelivered).toBe(false);
    expect(outcome.pendingUserTurn).not.toBeNull();
    expect(logger.warns.length + logger.errors.length).toBeGreaterThanOrEqual(1);
  });

  it('reports a non-2xx replay ack from the runner and still returns the decision', async () => {
    const logger = recordingLogger();
    const { recovery, store, runner } = buildRecovery({ logger });
    runner.connect();
    runner.replayStatus = 500;
    const u1 = await appendUserMessage(store, 'turn');
    const outcome = await recovery.recover('');
    // The replay was sent but the runner refused it (non-2xx): reported, marked
    // not-delivered, but the pending decision still stands.
    expect(outcome.replayDelivered).toBe(false);
    expect(outcome.pendingUserTurn!.id).toBe(u1.id);
    expect(logger.warns.length + logger.errors.length).toBeGreaterThanOrEqual(1);
  });

  it('reports a mid-replay tunnel drop and still returns the decision', async () => {
    const logger = recordingLogger();
    const { recovery, store, runner } = buildRecovery({ logger });
    runner.connect();
    runner.dropOnReplay = true;
    const u1 = await appendUserMessage(store, 'turn');
    const outcome = await recovery.recover('');
    expect(outcome.replayDelivered).toBe(false);
    expect(outcome.pendingUserTurn!.id).toBe(u1.id);
    expect(logger.warns.length + logger.errors.length).toBeGreaterThanOrEqual(1);
  });

  it('propagates a transcript read failure as a thrown error (caller decides retry)', async () => {
    // Unlike a delivery failure (recoverable when the runner returns), a read
    // failure means recovery could not even compute the replay/decision — it must
    // surface, not silently claim success. The owning connect hook decides retry.
    const inner = new InMemoryTranscriptStore();
    const readFails: TranscriptStore = {
      append: inner.append.bind(inner),
      read(): AsyncIterable<Event> {
        return {
          [Symbol.asyncIterator](): AsyncIterator<Event> {
            return {
              next(): Promise<IteratorResult<Event>> {
                return Promise.reject(new Error('simulated read failure'));
              },
            };
          },
        };
      },
      tail: inner.tail.bind(inner),
      archive: inner.archive.bind(inner),
      close: inner.close.bind(inner),
    };
    const registry = new TunnelRegistry();
    const runner = new FakeRunner(registry, RUNNER_ID);
    runner.connect();
    const recovery = new SessionRecovery({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      runnerId: RUNNER_ID,
      store: readFails,
      registry,
    });
    await expect(recovery.recover('')).rejects.toThrow(/read failure/);
    // No replay was pushed — the read failed before any slice could be served.
    expect(runner.replays.length).toBe(0);
  });

  it('serves the replay POST to the Orca-native runner replay route with the session + cursor headers', async () => {
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    const u1 = await appendUserMessage(store, 'turn');
    await recovery.recover(u1.id);
    expect(runner.requests.length).toBe(1);
    const req = runner.requests[0]!;
    expect(req.method).toBe('POST');
    expect(req.path).toBe(RUNNER_REPLAY_PATH);
    const headers = new Map((req.headers ?? []).map(([k, v]) => [k.toLowerCase(), v]));
    expect(headers.get(RUNNER_SESSION_HEADER.toLowerCase())).toBe(SESSION_ID);
    expect(headers.get(RUNNER_RESUME_CURSOR_HEADER.toLowerCase())).toBe(u1.id);
    expect(headers.get('content-type')).toBe('application/x-ndjson');
  });
});

// ── 7. subagent scoping ──

describe('session-recovery — parent-agent scope', () => {
  it('replays only parent-agent events, not subagent-subpath events', async () => {
    // Recovery resumes the PARENT agent's transcript; subagent traces ride their
    // own subpath and are recovered by their own runner. A subagent event must not
    // leak into the parent replay.
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    const u1 = await appendUserMessage(store, 'parent turn');
    // A subagent-subpath event interleaved into the same session log.
    const subProto = httpEventToProto({
      workspaceId: WORKSPACE_ID,
      sessionId: SESSION_ID,
      producedBy: 'harness',
      idempotencyKey: '',
      input: { id: 'evt_sub_1', type: 'response.created', subpath: 'subagents/x' },
    });
    await store.append(WORKSPACE_ID, SESSION_ID, [subProto]);
    const c1 = await appendTurnCompleted(store);

    await recovery.recover('');
    const ids = replayIds(runner.replays[0]!);
    // Parent events only: the user turn + the completed marker, NOT the subagent.
    expect(ids).toEqual([u1.id, c1.id]);
    expect(ids).not.toContain('evt_sub_1');
  });
});

// ── 8. observability: the replayed-events counter ──

describe('session-recovery — sessionRecoveryReplayedTotal metric', () => {
  it('increments mode=fresh by the replayed count for a fresh full replay', async () => {
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    await appendUserMessage(store, 'one');
    await appendAgentEvent(store, 'response.created');
    await appendTurnCompleted(store);
    const before = await replayedMetric('fresh');
    const outcome = await recovery.recover('');
    const after = await replayedMetric('fresh');
    // The fresh-mode counter advanced by exactly the events replayed (3).
    expect(after - before).toBe(3);
    expect(outcome.replayedCount).toBe(3);
  });

  it('increments mode=resume by the after-cursor slice size for a reconnect', async () => {
    const { recovery, store, runner } = buildRecovery();
    runner.connect();
    await appendUserMessage(store, 'turn');
    const a1 = await appendAgentEvent(store, 'response.created');
    await appendAgentEvent(store, 'response.output_text.delta');
    await appendTurnCompleted(store);
    const before = await replayedMetric('resume');
    const outcome = await recovery.recover(a1.id);
    const after = await replayedMetric('resume');
    // Resume-mode counter advanced by the slice after a1 (a2, c1 = 2).
    expect(after - before).toBe(2);
    expect(outcome.replayedCount).toBe(2);
  });
});

// ── 9. bounded pagination: chunked push + read cap ──

describe('session-recovery — bounded replay (chunked push, capped read)', () => {
  it('pushes the slice in bounded chunks (multiple frames), preserving order + ids', async () => {
    // A small chunk size forces pagination: the owner serves the after-cursor slice
    // as several RUNNER_REPLAY_PATH POSTs (each the runner dedups + acks), not one
    // unbounded frame — matching the reference catch-up's paged fetch.
    const { recovery, store, runner } = buildRecovery({ replayChunkSize: 2 });
    runner.connect();
    const seeded: string[] = [];
    seeded.push((await appendUserMessage(store, 'turn')).id);
    seeded.push((await appendAgentEvent(store, 'response.created')).id);
    seeded.push((await appendAgentEvent(store, 'response.output_text.delta')).id);
    seeded.push((await appendAgentEvent(store, 'response.output_text.delta')).id);
    seeded.push((await appendTurnCompleted(store)).id);

    const outcome = await recovery.recover('');
    // 5 events at chunk size 2 → 3 frames (2 + 2 + 1).
    expect(runner.replays.length).toBe(3);
    expect(runner.replays.map((r) => r.lines.length)).toEqual([2, 2, 1]);
    // Concatenated across frames, the full slice is delivered in order, by id.
    expect(allReplayedLines(runner).map((l) => String(l['id']))).toEqual(seeded);
    expect(outcome.replayedCount).toBe(5);
    expect(outcome.replayDelivered).toBe(true);
    // Each later frame's cursor names where the runner is now caught up to (the
    // last id of the prior chunk), so the runner sees contiguous paging.
    expect(runner.replays[0]!.cursor).toBe('');
    expect(runner.replays[1]!.cursor).toBe(seeded[1]);
    expect(runner.replays[2]!.cursor).toBe(seeded[3]);
  });

  it('stops pushing remaining chunks once a chunk fails to deliver (non-2xx)', async () => {
    // A partial replay is harmless (recovery is read-only; the runner dedups), so a
    // failed chunk stops the push and reports not-delivered — the caller retries the
    // whole recovery when the runner returns.
    const logger = recordingLogger();
    const { recovery, store, runner } = buildRecovery({ replayChunkSize: 2, logger });
    runner.connect();
    runner.replayStatus = 500; // every chunk is refused
    await appendUserMessage(store, 'turn');
    await appendAgentEvent(store, 'response.created');
    await appendAgentEvent(store, 'response.output_text.delta');
    const outcome = await recovery.recover('');
    // The FIRST chunk failed → no further chunks were sent.
    expect(runner.replays.length).toBe(1);
    expect(outcome.replayDelivered).toBe(false);
    expect(logger.warns.length + logger.errors.length).toBeGreaterThanOrEqual(1);
  });

  it('streams past the former total cap in bounded acknowledged pages', async () => {
    // A long transcript is streamed to its head without buffering the whole history.
    const logger = recordingLogger();
    const { recovery, store, runner } = buildRecovery({
      maxReplayEvents: 3,
      replayChunkSize: 10,
      logger,
    });
    runner.connect();
    const ids: string[] = [];
    ids.push((await appendUserMessage(store, 'turn')).id);
    for (let i = 0; i < 5; i += 1) {
      ids.push((await appendAgentEvent(store, 'response.output_text.delta')).id);
    }
    const outcome = await recovery.recover('');
    expect(outcome.replayedCount).toBe(6);
    expect(outcome.replayTruncated).toBe(false);
    expect(allReplayedLines(runner).map((l) => String(l['id']))).toEqual(ids);
    expect(runner.replays).toHaveLength(2);
    const resumed = await recovery.recover(ids[3]!);
    expect(resumed.replayedCount).toBe(2);
    expect(resumed.replayTruncated).toBe(false);
  });

  it('does not flag truncation when the transcript fits within the cap', async () => {
    const { recovery, store, runner } = buildRecovery({ maxReplayEvents: 100 });
    runner.connect();
    await appendUserMessage(store, 'turn');
    await appendTurnCompleted(store);
    const outcome = await recovery.recover('');
    expect(outcome.replayTruncated).toBe(false);
    expect(outcome.replayedCount).toBe(2);
    expect(runner.replays.length).toBe(1);
  });
});

it('streams private events without losing the visible suffix', async () => {
  const { recovery, store, runner } = buildRecovery({ maxReplayEvents: 2 });
  runner.connect();
  await appendAgentEvent(store, 'harness.resource_deletions');
  const visible = await appendUserMessage(store, 'visible prefix');
  const completed = await appendTurnCompleted(store);
  const outcome = await recovery.recover('');
  expect(store.readCalls[0]!.opts.maxEvents).toBe(0);
  expect(outcome.replayTruncated).toBe(false);
  expect(outcome.replayedCount).toBe(2);
  expect(outcome.pendingUserTurn).toBeNull();
  expect(allReplayedLines(runner).map((line) => line.id)).toEqual([visible.id, completed.id]);
});

it('recovers beyond 10,000 raw events and resumes from a cursor beyond the old cap', async () => {
  const { recovery, store, runner } = buildRecovery();
  runner.connect();
  for (let i = 0; i < 10_005; i += 1) await appendAgentEvent(store, 'harness.private');
  const user = await appendUserMessage(store, 'long-lived session');
  const completed = await appendTurnCompleted(store);
  const fresh = await recovery.recover('');
  expect(fresh).toMatchObject({
    replayDelivered: true,
    replayTruncated: false,
    replayedCount: 2,
    pendingUserTurn: null,
  });
  expect(allReplayedLines(runner).map((line) => line.id)).toEqual([user.id, completed.id]);
  const pending = await appendUserMessage(store, 'next turn');
  const resumed = await recovery.recover(completed.id);
  expect(resumed).toMatchObject({
    replayDelivered: true,
    replayTruncated: false,
    replayedCount: 1,
    pendingUserTurn: { id: pending.id },
  });
});
