// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the runner core loop — the heart that consumes the snapshot,
// drives the harness per turn, emits agent events up the tunnel, and applies the
// pushed recovery replay.
//
// Everything here runs IN-PROCESS against a FAKE AgentHarness (scripted events) +
// a ProviderRegistry that hands it back — no claude SDK, no tunnel socket. The
// assertions pin: snapshot CONSUME (construct + start the
// provider's harness), the TURN LOOP (drive the harness, stream its events as
// NDJSON, end on the turn boundary, synthesize the completed marker), EVENT
// EMISSION ordering + the `{...payload, type, id}` line shape, LIFECYCLE (stop /
// newest-wins re-delivery), and RECOVERY APPLY (dedup by id + resume-cursor
// advance — the client half of registry recovery).

import { describe, it, expect } from 'vitest';
import type { Event } from '@orca/transcript-store-types';
import { ProviderRegistry } from '../../src/harness/provider.js';
import {
  SessionLoop,
  TURN_COMPLETED_EVENT_KIND,
  parseUserEvent,
  UserEventParseError,
  BoundedIdSet,
  DEFAULT_DEDUP_WINDOW,
  type TranscriptSink,
} from '../../src/session-loop.js';
import { FakeAgentHarness, type ScriptedTurn } from './support/fake-agent-harness.js';

const WS = 'ws_loop';
const SES = 'ses_loop';

/** Build a loop whose single `claude` provider returns `harness`. */
function loopWith(harness: FakeAgentHarness): SessionLoop {
  const providers = new ProviderRegistry();
  providers.register('claude', () => harness);
  return new SessionLoop({ workspaceId: WS, providers });
}

/** A delivered snapshot body (one NDJSON line) for the `claude` provider. */
function snapshotBody(overrides: Record<string, unknown> = {}): Uint8Array {
  return new TextEncoder().encode(
    `${JSON.stringify({
      model: { provider: 'anthropic', id: 'claude-sonnet-4' },
      provider: 'claude',
      system: 'sys',
      allowed_tool_names: ['bash'],
      allowed_mcp_server_names: [],
      egress: { mode: 'gateway' },
      ...overrides,
    })}\n`,
  );
}

/** A user-turn body (the public user.* event payload). */
function turnBody(text: string): Uint8Array {
  return new TextEncoder().encode(
    JSON.stringify({ type: 'user.message', id: 'evt_user', content: [{ type: 'text', text }] }),
  );
}

/** Drain the turn stream into the parsed NDJSON lines it emitted. */
async function drainLines(
  stream: AsyncIterable<Uint8Array>,
): Promise<Array<Record<string, unknown>>> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  const text = Buffer.concat(chunks.map((c) => Buffer.from(c))).toString('utf8');
  return text
    .split('\n')
    .filter((l) => l.trim().length > 0)
    .map((l) => JSON.parse(l) as Record<string, unknown>);
}

describe('SessionLoop — snapshot consume', () => {
  it('constructs + starts the provider harness from the delivered snapshot', async () => {
    const harness = new FakeAgentHarness();
    const loop = loopWith(harness);
    expect(loop.hasHarness()).toBe(false);

    await loop.applySnapshot(SES, snapshotBody());

    expect(loop.hasHarness()).toBe(true);
    expect(harness.startCount).toBe(1);
    // The snapshot was projected into the harness boot context.
    expect(harness.startInput).toMatchObject({
      workspaceId: WS,
      sessionId: SES,
      agentSnapshot: { model_id: 'claude-sonnet-4', system: 'sys', allowed_tool_names: ['bash'] },
    });
    await loop.stop();
  });

  it('dispatches to the provider named by the snapshot (capability mismatch is fatal)', async () => {
    const providers = new ProviderRegistry();
    providers.register('claude', () => new FakeAgentHarness());
    const loop = new SessionLoop({ workspaceId: WS, providers });
    await expect(loop.applySnapshot(SES, snapshotBody({ provider: 'codex' }))).rejects.toThrow(
      /no harness provider registered/,
    );
    expect(loop.hasHarness()).toBe(false);
  });

  it('rejects a malformed snapshot body (the handler maps it to a non-2xx ack)', async () => {
    const loop = loopWith(new FakeAgentHarness());
    await expect(loop.applySnapshot(SES, new TextEncoder().encode('not json'))).rejects.toThrow();
    expect(loop.hasHarness()).toBe(false);
  });

  it('newest-wins: a re-delivered snapshot stops the old harness and builds a fresh one', async () => {
    const first = new FakeAgentHarness();
    const second = new FakeAgentHarness();
    const queue = [first, second];
    const providers = new ProviderRegistry();
    providers.register('claude', () => queue.shift()!);
    const loop = new SessionLoop({ workspaceId: WS, providers });

    await loop.applySnapshot(SES, snapshotBody());
    expect(first.startCount).toBe(1);

    await loop.applySnapshot(SES, snapshotBody());
    // The prior harness was stopped (newest-wins), the new one started.
    expect(first.stopReason).toBe('replica.shutting_down');
    expect(second.startCount).toBe(1);
    await loop.stop();
  });
});

describe('SessionLoop — turn loop + event emission', () => {
  it('drives the harness and streams its agent events as NDJSON, in order', async () => {
    const harness = new FakeAgentHarness([
      {
        events: [
          { kind: 'response.created', payload: { foo: 1 } },
          { kind: 'agent.message', payload: { content: 'hi' } },
          { kind: TURN_COMPLETED_EVENT_KIND, payload: {} },
        ],
      } satisfies ScriptedTurn,
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const lines = await drainLines(
      loop.runTurn(SES, turnBody('hello'), new AbortController().signal),
    );

    expect(lines.map((l) => l.type)).toEqual([
      'response.created',
      'agent.message',
      TURN_COMPLETED_EVENT_KIND,
    ]);
    // The user event reached the harness with its kind + payload.
    expect(harness.submitted).toHaveLength(1);
    expect(harness.submitted[0]!.kind).toBe('user.message');
    await loop.stop();
  });

  it('stamps type (over any payload type) and the optional stable id on each line', async () => {
    const harness = new FakeAgentHarness([
      {
        // The payload carries its own `type: assistant`; the agent kind must win.
        events: [
          { kind: 'agent.message', payload: { type: 'assistant', text: 'x' }, id: 'evt_a1' },
          { kind: TURN_COMPLETED_EVENT_KIND, payload: {} },
        ],
      },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const lines = await drainLines(loop.runTurn(SES, turnBody('hi'), new AbortController().signal));
    expect(lines[0]).toEqual({ type: 'agent.message', text: 'x', id: 'evt_a1' });
    await loop.stop();
  });

  it('carries an event subpath onto the wire line (so a subagent-thread event routes)', async () => {
    // A coordinator's subagent-thread events are emitted with a transcript `subpath`
    // of `subagents/<id>`; the bridge persists each line under that subpath so it
    // lands on the child thread's stream. The loop must therefore surface the harness
    // event's `subpath` as a top-level field on the NDJSON line (alongside type/id),
    // matching the `{ type, id?, subpath?, ... }` shape the bridge reads. A default
    // (empty-subpath) event carries no subpath field, so single-agent lines are
    // unchanged.
    const harness = new FakeAgentHarness([
      {
        events: [
          { kind: 'agent.message', payload: { text: 'primary' } },
          {
            kind: 'agent.message',
            payload: { text: 'child' },
            id: 'evt_c1',
            subpath: 'subagents/sth_child',
          },
          { kind: TURN_COMPLETED_EVENT_KIND, payload: {} },
        ],
      },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const lines = await drainLines(loop.runTurn(SES, turnBody('hi'), new AbortController().signal));
    // The primary-thread line carries NO subpath field (single-agent shape unchanged).
    expect(lines[0]).toEqual({ type: 'agent.message', text: 'primary' });
    expect('subpath' in lines[0]!).toBe(false);
    // The child-thread line carries its `subpath` (and id) as top-level fields.
    expect(lines[1]).toEqual({
      type: 'agent.message',
      text: 'child',
      id: 'evt_c1',
      subpath: 'subagents/sth_child',
    });
    await loop.stop();
  });

  it('synthesizes a terminal completed marker when the harness emits none', async () => {
    const harness = new FakeAgentHarness([
      { events: [{ kind: 'agent.message', payload: { text: 'done' } }] },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const lines = await drainLines(loop.runTurn(SES, turnBody('hi'), new AbortController().signal));
    // The harness emitted only agent.message; the loop appended the completed marker
    // so the owner pod sees the turn as answered.
    expect(lines.map((l) => l.type)).toEqual(['agent.message', TURN_COMPLETED_EVENT_KIND]);
    await loop.stop();
  });

  it('does NOT double-synthesize when the harness already emitted the completed marker', async () => {
    const harness = new FakeAgentHarness([
      { events: [{ kind: TURN_COMPLETED_EVENT_KIND, payload: { stop_reason: 'end_turn' } }] },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const lines = await drainLines(loop.runTurn(SES, turnBody('hi'), new AbortController().signal));
    expect(lines).toHaveLength(1);
    expect(lines[0]).toEqual({ type: TURN_COMPLETED_EVENT_KIND, stop_reason: 'end_turn' });
    await loop.stop();
  });

  it('a mid-turn stream end WHILE submit is in flight ends the turn with agent.error + the marker', async () => {
    // The phase-1 bypass: the harness's event stream ends (its fault path calls
    // `endEventStream()`) while `submit` is still pending, so the loop's pull returns
    // done mid-turn. Returning silently there left the turn with NO marker at all —
    // pending forever from the owner pod's point of view.
    let release: () => void = () => {};
    const hold = new Promise<void>((resolve) => {
      release = resolve;
    });
    const harness = new FakeAgentHarness([
      { events: [{ kind: 'agent.message', payload: { text: 'partial' } }], endStream: true, hold },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const stream = loop.runTurn(SES, turnBody('hi'), new AbortController().signal);
    const drained = drainLines(stream);
    // The stream is already ended; releasing the hold settles `submit` so the turn ends.
    release();
    const lines = await drained;

    expect(lines.map((l) => l.type)).toEqual([
      'agent.message',
      'agent.error',
      TURN_COMPLETED_EVENT_KIND,
    ]);
    // The fault is on the WIRE (and so in the durable transcript), not only on stderr.
    expect(lines[1]!.message).toBe('harness event stream ended before the turn completed');
    await loop.stop();
  });

  it('a stream end during the flush-drain ends the turn with agent.error + the marker', async () => {
    // The phase-2 bypass: `submit` resolved first (so the runner parked a pending pull
    // and moved to the flush-drain), and the stream ends a tick later — the drain's pull
    // comes back done. Same silent-empty-turn bug, reached through the other loop.
    const harness = new FakeAgentHarness([
      {
        events: [{ kind: 'agent.message', payload: { text: 'partial' } }],
        endStreamAfterSubmit: true,
      },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const lines = await drainLines(loop.runTurn(SES, turnBody('hi'), new AbortController().signal));

    expect(lines.map((l) => l.type)).toEqual([
      'agent.message',
      'agent.error',
      TURN_COMPLETED_EVENT_KIND,
    ]);
    expect(lines[1]!.message).toBe('harness event stream ended before the turn completed');
    await loop.stop();
  });

  it('a TERMINAL harness error that is NOT the turn`s last word does not silence the loop', async () => {
    // The suppression flag tracks the LAST primary event, not a sticky "saw an error
    // somewhere". A harness that reported a fault MID-turn and then kept talking ends with
    // an ordinary message as its last word, so the turn is cut short with NO explanation
    // unless the loop speaks — the harness's earlier error is buried up-stream where an
    // operator reading the turn's tail never sees it.
    //
    // Every other fault cell in this package puts the harness's error LAST, which makes
    // `lastPrimary = …` and `lastPrimary ||= …` indistinguishable: flipping the assignment
    // to `||=` left the whole package green. This case is the one that separates them.
    let release: () => void = () => {};
    const hold = new Promise<void>((resolve) => {
      release = resolve;
    });
    const harness = new FakeAgentHarness([
      {
        events: [
          { kind: 'agent.error', terminal: true, payload: { message: 'model backend reset' } },
          { kind: 'agent.message', payload: { text: 'retrying…' } },
        ],
        endStream: true,
        hold,
      },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const drained = drainLines(loop.runTurn(SES, turnBody('hi'), new AbortController().signal));
    release();
    const lines = await drained;

    expect(
      lines.map((l) => l.type),
      'the truncation must still be explained: the harness`s error was not the turn`s last word',
    ).toEqual(['agent.error', 'agent.message', 'agent.error', TURN_COMPLETED_EVENT_KIND]);
    expect(lines[0]!.message).toBe('model backend reset');
    expect(lines[2]!.message).toBe('harness event stream ended before the turn completed');
    await loop.stop();
  });

  it('a NON-terminal agent.error as the turn`s last word does not silence the loop either', async () => {
    // NOT every `agent.error` ends a turn. The SDK's `system/mirror_error` — the
    // transcript-mirror DATA-LOSS diagnostic, emitted whenever a store/Kafka append batch
    // is dropped — maps to a primary `agent.error` that says nothing about the turn ending.
    // Keying the suppression on KIND let one of those disarm the loop: a turn cut short
    // right after a dropped batch reported the DROP and never mentioned the truncation, so
    // the client got an accurate-sounding cause for the wrong thing.
    let release: () => void = () => {};
    const hold = new Promise<void>((resolve) => {
      release = resolve;
    });
    const harness = new FakeAgentHarness([
      {
        events: [
          {
            kind: 'agent.error',
            payload: { message: 'transcript mirror append failed (batch dropped): kafka timeout' },
          },
        ],
        endStream: true,
        hold,
      },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const drained = drainLines(loop.runTurn(SES, turnBody('hi'), new AbortController().signal));
    release();
    const lines = await drained;

    expect(
      lines.map((l) => l.type),
      'a mid-turn diagnostic is not the turn`s terminal explanation — the truncation is owed',
    ).toEqual(['agent.error', 'agent.error', TURN_COMPLETED_EVENT_KIND]);
    expect(lines[1]!.message).toBe('harness event stream ended before the turn completed');
    await loop.stop();
  });

  it('a TERMINAL harness error AS the turn`s last word DOES silence the loop`s generic one', async () => {
    // The other side of the same rule, so the two tests together pin the behavior rather
    // than one of them alone: when the harness's terminal error IS the last word, the
    // loop's generic "harness event stream ended…" would be SECOND and, being last, the
    // one an operator reads — burying the precise cause.
    let release: () => void = () => {};
    const hold = new Promise<void>((resolve) => {
      release = resolve;
    });
    const harness = new FakeAgentHarness([
      {
        events: [
          { kind: 'agent.error', terminal: true, payload: { message: 'model backend reset' } },
        ],
        endStream: true,
        hold,
      },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const drained = drainLines(loop.runTurn(SES, turnBody('hi'), new AbortController().signal));
    release();
    const lines = await drained;

    expect(lines.map((l) => l.type)).toEqual(['agent.error', TURN_COMPLETED_EVENT_KIND]);
    expect(lines[0]!.message).toBe('model backend reset');
    await loop.stop();
  });

  it('a submit REJECTION ends the turn with agent.error + the marker (never a clean empty turn)', async () => {
    // A turn whose `submit` merely rejects used to reach the synthesis and emit a BARE
    // `agent.turn_completed` — the transcript then recorded the turn as answered with no
    // answer, and the failure existed only on stderr. The rejection's own message rides
    // out (it is the root cause, so it wins over the generic stream-ended text).
    const harness = new FakeAgentHarness([{ events: [], fail: new Error('sdk subprocess died') }]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const lines = await drainLines(loop.runTurn(SES, turnBody('hi'), new AbortController().signal));

    expect(lines.map((l) => l.type)).toEqual(['agent.error', TURN_COMPLETED_EVENT_KIND]);
    expect(lines[0]!.message).toBe('harness turn failed: sdk subprocess died');
    await loop.stop();
  });

  it('drives two turns serially, each a fresh bounded NDJSON stream', async () => {
    const harness = new FakeAgentHarness([
      {
        events: [
          { kind: 'agent.message', payload: { n: 1 } },
          { kind: TURN_COMPLETED_EVENT_KIND, payload: {} },
        ],
      },
      {
        events: [
          { kind: 'agent.message', payload: { n: 2 } },
          { kind: TURN_COMPLETED_EVENT_KIND, payload: {} },
        ],
      },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const first = await drainLines(
      loop.runTurn(SES, turnBody('one'), new AbortController().signal),
    );
    const second = await drainLines(
      loop.runTurn(SES, turnBody('two'), new AbortController().signal),
    );
    expect(first.map((l) => l.n).filter((n) => n !== undefined)).toEqual([1]);
    expect(second.map((l) => l.n).filter((n) => n !== undefined)).toEqual([2]);
    expect(harness.submitted).toHaveLength(2);
    await loop.stop();
  });

  it('a cancel mid-turn ends the stream early WITHOUT tearing down the harness', async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const harness = new FakeAgentHarness([
      {
        gate,
        events: [
          { kind: 'agent.message', payload: { late: true } },
          { kind: TURN_COMPLETED_EVENT_KIND, payload: {} },
        ],
      },
      // A SECOND turn proves the harness survived the cancel and is reused.
      {
        events: [
          { kind: 'agent.message', payload: { second: true } },
          { kind: TURN_COMPLETED_EVENT_KIND, payload: {} },
        ],
      },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const ac = new AbortController();
    const stream = loop.runTurn(SES, turnBody('slow'), ac.signal);
    const iterator = stream[Symbol.asyncIterator]();
    // Cancel before the gated turn emits anything.
    ac.abort();
    release();
    const first = await iterator.next();
    // The stream ended with no lines (cancelled before any event), and crucially
    // no synthetic completed marker (the partial turn stays re-promptable).
    expect(first.done).toBe(true);
    // The harness was NOT stopped — the next turn drives it.
    expect(harness.stopReason).toBeUndefined();
    const second = await drainLines(
      loop.runTurn(SES, turnBody('again'), new AbortController().signal),
    );
    expect(second.some((l) => l.second === true)).toBe(true);
    await loop.stop();
  });

  it('runTurn before a snapshot yields an empty stream (the handler surfaces 503)', async () => {
    const loop = loopWith(new FakeAgentHarness());
    const lines = await drainLines(loop.runTurn(SES, turnBody('hi'), new AbortController().signal));
    expect(lines).toEqual([]);
    expect(loop.hasHarness()).toBe(false);
  });

  it('a cancel FORWARDS the abort to the harness so a BLOCKED turn unwinds (no manual gate release)', async () => {
    // The Major-finding fix: when a turn is BLOCKED on the model, request.cancel must
    // forward the abort to the harness — otherwise the loop's discard would hang on
    // `await submit` forever. Here the gated turn is NEVER released by the test; only
    // the loop's harness.interrupt() (wired into the cancel path) can unblock it.
    let releasedByTest = false;
    const gate = new Promise<void>((resolve) => {
      // Intentionally only resolved if the test were to call it — it never does.
      void resolve;
      releasedByTest = false;
    });
    const harness = new FakeAgentHarness([
      {
        gate,
        events: [
          { kind: 'agent.message', payload: { late: true } },
          { kind: TURN_COMPLETED_EVENT_KIND, payload: {} },
        ],
      },
      {
        events: [
          { kind: 'agent.message', payload: { second: true } },
          { kind: TURN_COMPLETED_EVENT_KIND, payload: {} },
        ],
      },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    const ac = new AbortController();
    const stream = loop.runTurn(SES, turnBody('blocked'), ac.signal);
    const iterator = stream[Symbol.asyncIterator]();
    // Cancel while the turn is blocked on its (never-released) gate.
    ac.abort();
    // The stream MUST end (it would hang if the cancel did not interrupt the harness).
    const first = await iterator.next();
    expect(first.done).toBe(true);
    expect(releasedByTest).toBe(false); // the test never released the gate
    // The cancel interrupted the harness exactly once, and did NOT tear it down.
    expect(harness.interruptCount).toBe(1);
    expect(harness.stopReason).toBeUndefined();
    // The harness survived: the next turn drives it normally.
    const second = await drainLines(
      loop.runTurn(SES, turnBody('again'), new AbortController().signal),
    );
    expect(second.some((l) => l.second === true)).toBe(true);
    await loop.stop();
  });
});

describe('SessionLoop — interrupt (out-of-band)', () => {
  it('interruptTurn() aborts the in-flight turn via the harness and keeps it alive', async () => {
    // A turn blocked on its gate; interruptTurn() (the interrupt route's effect) must
    // unwind it WITHOUT tearing the harness down — the next turn reuses it.
    const gate = new Promise<void>(() => {
      /* never resolved by the test — only interrupt unblocks it */
    });
    const harness = new FakeAgentHarness([
      { gate, events: [{ kind: TURN_COMPLETED_EVENT_KIND, payload: {} }] },
      {
        events: [
          { kind: 'agent.message', payload: { after: true } },
          { kind: TURN_COMPLETED_EVENT_KIND, payload: {} },
        ],
      },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    // Drive the blocked turn WITHOUT awaiting (it parks on the gate).
    const drained = drainLines(loop.runTurn(SES, turnBody('block'), new AbortController().signal));
    // Let the turn reach the harness's gate.
    await new Promise((r) => setTimeout(r, 5));

    // The interrupt route fires: abort the in-flight turn.
    expect(loop.interruptTurn()).toBe(true);
    expect(harness.interruptCount).toBe(1);

    // The interrupted turn's stream ends (re-promptable, no synthetic marker bleed).
    await drained;
    // The harness was NOT stopped — a subsequent turn drives it.
    expect(harness.stopReason).toBeUndefined();
    const next = await drainLines(
      loop.runTurn(SES, turnBody('next'), new AbortController().signal),
    );
    expect(next.some((l) => l.after === true)).toBe(true);
    await loop.stop();
  });

  it('interruptTurn() before a snapshot is a harmless no-op (returns false)', async () => {
    const loop = loopWith(new FakeAgentHarness());
    expect(loop.interruptTurn()).toBe(false);
  });

  it('interruptTurn() between turns (no turn in flight) is an idempotent no-op on the harness', async () => {
    const harness = new FakeAgentHarness([
      { events: [{ kind: TURN_COMPLETED_EVENT_KIND, payload: {} }] },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());
    // A completed turn, then an interrupt with nothing in flight.
    await drainLines(loop.runTurn(SES, turnBody('done'), new AbortController().signal));
    expect(loop.interruptTurn()).toBe(true); // a harness is present…
    expect(harness.interruptCount).toBe(1); // …interrupt() was called (a no-op mid-idle)
    expect(harness.stopReason).toBeUndefined(); // and it did not tear the harness down
  });
});

describe('SessionLoop — recovery apply (replay)', () => {
  it('applies replayed events, deduping by stable id, and advances the resume cursor', async () => {
    const loop = loopWith(new FakeAgentHarness());
    await loop.applySnapshot(SES, snapshotBody());

    const replay = ndjson([
      { id: 'evt_1', type: 'user.message' },
      { id: 'evt_2', type: 'agent.message' },
      { id: 'evt_3', type: 'agent.turn_completed' },
    ]);
    const applied = loop.applyReplay(SES, replay);
    expect(applied).toBe(3);
    // The resume cursor advanced to the LAST id in the slice.
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_3' });
    await loop.stop();
  });

  it('a re-pushed (overlapping) slice is idempotent — known ids apply zero times', async () => {
    const loop = loopWith(new FakeAgentHarness());
    await loop.applySnapshot(SES, snapshotBody());

    loop.applyReplay(SES, ndjson([{ id: 'evt_1' }, { id: 'evt_2' }]));
    // Re-push evt_2 (already held) + a new evt_3: only evt_3 is newly applied.
    const applied = loop.applyReplay(SES, ndjson([{ id: 'evt_2' }, { id: 'evt_3' }]));
    expect(applied).toBe(1);
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_3' });
    await loop.stop();
  });

  it('an empty caught-up replay frame applies nothing and leaves the cursor unset', async () => {
    const loop = loopWith(new FakeAgentHarness());
    await loop.applySnapshot(SES, snapshotBody());
    const applied = loop.applyReplay(SES, new Uint8Array(0));
    expect(applied).toBe(0);
    expect(loop.resumeCursors()).toEqual({});
    await loop.stop();
  });

  it('skips malformed / id-less replay lines without aborting the apply', async () => {
    const loop = loopWith(new FakeAgentHarness());
    await loop.applySnapshot(SES, snapshotBody());
    const body = new TextEncoder().encode(
      ['{"id":"evt_1"}', 'not json', '{"no":"id"}', '{"id":"evt_2"}'].join('\n') + '\n',
    );
    const applied = loop.applyReplay(SES, body);
    expect(applied).toBe(2);
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_2' });
    await loop.stop();
  });

  it('advances the cursor even when every event in the slice was a duplicate', async () => {
    const loop = loopWith(new FakeAgentHarness());
    await loop.applySnapshot(SES, snapshotBody());
    loop.applyReplay(SES, ndjson([{ id: 'evt_1' }]));
    // Re-push only evt_1 — applied 0, but the cursor still names evt_1.
    const applied = loop.applyReplay(SES, ndjson([{ id: 'evt_1' }]));
    expect(applied).toBe(0);
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_1' });
    await loop.stop();
  });

  it('NEVER drives a turn — applying a replay is state-rebuild only (the runner is not the turn driver)', async () => {
    // The runner-side half of "never re-run a finished turn": recovery on the
    // runner is STATE-REBUILD ONLY. The owner pod's recovery SERVES the replay and
    // the owner pod's event bridge DRIVES turns (the single driver, the one
    // authority on "answered"); the runner applies the replay to advance its dedup
    // + cursor and MUST NOT itself start a turn. This is the exactly-once guarantee
    // at the runner boundary: even a replay slice that ENDS with a pending
    // `user.message` (the very shape that would otherwise look like an unanswered
    // turn to start) drives ZERO turns here — the harness is never `submit`ted.
    const harness = new FakeAgentHarness();
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());

    // A slice whose LAST event is a bare user.message past the last completed turn —
    // the "trailing pending user turn" case. Applying it rebuilds state + advances
    // the cursor, but drives no turn.
    const applied = loop.applyReplay(
      SES,
      ndjson([
        { id: 'evt_1', type: 'user.message' },
        { id: 'evt_2', type: 'agent.message' },
        { id: 'evt_3', type: 'agent.turn_completed' },
        { id: 'evt_4', type: 'user.message' }, // trailing PENDING user turn
      ]),
    );
    expect(applied).toBe(4);
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_4' });
    // The load-bearing assertion: the harness was NEVER driven. No `submit` ⇒ no
    // turn ⇒ the runner cannot re-run a finished (or start an unfinished) turn from
    // a replay. The bridge owns that decision.
    expect(harness.submitted).toHaveLength(0);
    expect(loop.hasActiveWork()).toBe(false);

    // Give any (incorrectly) scheduled async turn a macrotask to surface — it must
    // still be zero. Recovery drives nothing, ever.
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
    expect(harness.submitted).toHaveLength(0);
    await loop.stop();
  });

  it('applying a replay then a real turn drives EXACTLY that one turn (replay did not pre-drive)', async () => {
    // Pairs with the above: after a replay that ends on a pending user.message, the
    // genuine turn — when the bridge drives it via runTurn — is the FIRST and ONLY
    // submit. Proves the replay did not silently consume / pre-drive the turn.
    const harness = new FakeAgentHarness([
      { events: [{ kind: TURN_COMPLETED_EVENT_KIND, payload: {} }] },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());
    loop.applyReplay(
      SES,
      ndjson([
        { id: 'evt_3', type: 'agent.turn_completed' },
        { id: 'evt_4', type: 'user.message' },
      ]),
    );
    expect(harness.submitted).toHaveLength(0);

    // Now the bridge drives the pending turn for real (a turn POST → runTurn).
    await drainLines(loop.runTurn(SES, turnBody('go'), new AbortController().signal));
    expect(harness.submitted).toHaveLength(1);
    await loop.stop();
  });
});

describe('SessionLoop — recovery apply feeds the tunnel-fed transcript sink', () => {
  /** A recording TranscriptSink — the in-memory store's `ingest` seam, captured. */
  class RecordingSink implements TranscriptSink {
    readonly ingested: Event[] = [];
    ingest(event: Event): void {
      this.ingested.push(event);
    }
  }

  /** A loop whose providers serve `harness` and whose replay feeds `sink`. */
  function loopWithSink(harness: FakeAgentHarness, sink: TranscriptSink): SessionLoop {
    const providers = new ProviderRegistry();
    providers.register('claude', () => harness);
    return new SessionLoop({ workspaceId: WS, providers, transcriptSink: sink });
  }

  it('ingests each NEWLY-applied replay event into the sink, reconstructing the Event shape', async () => {
    const sink = new RecordingSink();
    const loop = loopWithSink(new FakeAgentHarness(), sink);
    await loop.applySnapshot(SES, snapshotBody());

    loop.applyReplay(
      SES,
      ndjson([
        {
          id: 'evt_1',
          type: 'user.message',
          produced_by: 'client',
          produced_at: '2026-01-01T00:00:00.000Z',
          seq: '0',
          content: [{ type: 'text', text: 'hi' }],
        },
        { id: 'evt_2', type: 'agent.message', produced_by: 'harness', subpath: '', seq: '1' },
      ]),
    );

    expect(sink.ingested.map((e) => e.id)).toEqual(['evt_1', 'evt_2']);
    // The reconstructed Event lifts the registry's serialized fields back into shape:
    const first = sink.ingested[0]!;
    expect(first.workspaceId).toBe(WS);
    expect(first.sessionId).toBe(SES);
    expect(first.kind).toBe('user.message');
    expect(first.producedBy).toBe('client');
    expect(first.producedAt).toBe('2026-01-01T00:00:00.000Z');
    // The whole line rides the payload so a local-log reader sees the same body.
    expect(JSON.parse(Buffer.from(first.payload).toString('utf8'))).toMatchObject({
      id: 'evt_1',
      type: 'user.message',
      content: [{ type: 'text', text: 'hi' }],
    });
    await loop.stop();
  });

  it('does NOT re-ingest a duplicate (re-pushed) replay event — only fresh applies feed the sink', async () => {
    const sink = new RecordingSink();
    const loop = loopWithSink(new FakeAgentHarness(), sink);
    await loop.applySnapshot(SES, snapshotBody());

    loop.applyReplay(SES, ndjson([{ id: 'evt_1', type: 'user.message' }]));
    // Re-push evt_1 (already held) + a new evt_2: only evt_2 newly feeds the sink.
    loop.applyReplay(
      SES,
      ndjson([
        { id: 'evt_1', type: 'user.message' },
        { id: 'evt_2', type: 'agent.message' },
      ]),
    );

    expect(sink.ingested.map((e) => e.id)).toEqual(['evt_1', 'evt_2']);
    await loop.stop();
  });

  it('skips malformed / id-less replay lines for the sink too (defensive, never throws)', async () => {
    const sink = new RecordingSink();
    const loop = loopWithSink(new FakeAgentHarness(), sink);
    await loop.applySnapshot(SES, snapshotBody());

    const body = new TextEncoder().encode(
      [
        '{"id":"evt_1","type":"user.message"}',
        'not json',
        '{"no":"id"}',
        '{"id":"evt_2","type":"agent.message"}',
      ].join('\n') + '\n',
    );
    const applied = loop.applyReplay(SES, body);
    expect(applied).toBe(2);
    expect(sink.ingested.map((e) => e.id)).toEqual(['evt_1', 'evt_2']);
    await loop.stop();
  });

  it('a loop with NO sink still applies the replay (dedup + cursor) — the sink is additive', async () => {
    // The unit-spec default: no sink wired. The replay bookkeeping is unchanged.
    const loop = loopWith(new FakeAgentHarness());
    await loop.applySnapshot(SES, snapshotBody());
    const applied = loop.applyReplay(SES, ndjson([{ id: 'evt_1' }, { id: 'evt_2' }]));
    expect(applied).toBe(2);
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_2' });
    await loop.stop();
  });
});

describe('SessionLoop — lifecycle', () => {
  it('stop() before any snapshot is a no-op', async () => {
    const loop = loopWith(new FakeAgentHarness());
    await expect(loop.stop()).resolves.toBeUndefined();
  });

  it('stop() tears down the harness and drains the pump', async () => {
    const harness = new FakeAgentHarness();
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());
    await loop.stop('idle.timeout');
    expect(harness.stopReason).toBe('idle.timeout');
    expect(loop.hasHarness()).toBe(false);
  });

  it('exposes the provider names it can serve (for the tunnel hello advertise)', () => {
    const providers = new ProviderRegistry();
    providers.register('claude', () => new FakeAgentHarness());
    const loop = new SessionLoop({ workspaceId: WS, providers });
    expect(loop.providerNames()).toEqual(['claude']);
  });
});

describe('SessionLoop — idle activity tracking (drives the inactivity watchdog)', () => {
  /** A loop with an injected clock so activity stamps are deterministic. */
  function loopWithClock(harness: FakeAgentHarness, clock: { t: number }): SessionLoop {
    const providers = new ProviderRegistry();
    providers.register('claude', () => harness);
    return new SessionLoop({ workspaceId: WS, providers, now: () => clock.t });
  }

  it('touches last-activity on snapshot, turn, and replay', async () => {
    const clock = { t: 100 };
    const harness = new FakeAgentHarness([
      { events: [{ kind: TURN_COMPLETED_EVENT_KIND, payload: {} }] },
    ]);
    const loop = loopWithClock(harness, clock);
    expect(loop.lastActivity()).toBe(100);

    clock.t = 200;
    await loop.applySnapshot(SES, snapshotBody());
    expect(loop.lastActivity()).toBe(200);

    clock.t = 300;
    await drainLines(loop.runTurn(SES, turnBody('hi'), new AbortController().signal));
    // The turn touches activity at end (>= the start touch).
    expect(loop.lastActivity()).toBe(300);

    clock.t = 400;
    loop.applyReplay(SES, ndjson([{ id: 'evt_1' }]));
    expect(loop.lastActivity()).toBe(400);
    await loop.stop();
  });

  it('reports active work only WHILE a turn is in flight', async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const harness = new FakeAgentHarness([
      { gate, events: [{ kind: TURN_COMPLETED_EVENT_KIND, payload: {} }] },
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody());
    expect(loop.hasActiveWork()).toBe(false);

    // Start draining the turn (drives submit, which parks on the gate).
    const drained = drainLines(loop.runTurn(SES, turnBody('slow'), new AbortController().signal));
    // Let the generator advance to the in-flight point.
    await Promise.resolve();
    expect(loop.hasActiveWork()).toBe(true);

    release();
    await drained;
    expect(loop.hasActiveWork()).toBe(false);
    await loop.stop();
  });
});

describe('parseUserEvent', () => {
  it('reads the kind from the payload type and carries the payload', () => {
    const ev = parseUserEvent(new TextEncoder().encode('{"type":"user.message","content":[]}'));
    expect(ev.kind).toBe('user.message');
    expect(ev.payload).toEqual({ type: 'user.message', content: [] });
  });

  it('defaults a typeless body to user.message (the only chat-only kind)', () => {
    const ev = parseUserEvent(new TextEncoder().encode('{"content":[]}'));
    expect(ev.kind).toBe('user.message');
  });

  it('rejects a non-object body', () => {
    expect(() => parseUserEvent(new TextEncoder().encode('[]'))).toThrow(UserEventParseError);
    expect(() => parseUserEvent(new TextEncoder().encode(''))).toThrow(UserEventParseError);
  });
});

describe('BoundedIdSet — the replay dedup window has a memory ceiling', () => {
  it('dedups membership like a Set (has after add; re-add is a no-op)', () => {
    const set = new BoundedIdSet(10);
    expect(set.has('a')).toBe(false);
    set.add('a');
    expect(set.has('a')).toBe(true);
    set.add('a'); // no churn
    expect(set.size).toBe(1);
  });

  it('evicts the OLDEST-inserted id past capacity (FIFO), bounding memory', () => {
    const set = new BoundedIdSet(3);
    set.add('a');
    set.add('b');
    set.add('c');
    expect(set.size).toBe(3);
    // Adding a 4th evicts the oldest ('a'); the window holds the 3 most recent.
    set.add('d');
    expect(set.size).toBe(3);
    expect(set.has('a')).toBe(false); // evicted
    expect(set.has('b')).toBe(true);
    expect(set.has('c')).toBe(true);
    expect(set.has('d')).toBe(true);
  });

  it('never exceeds capacity across many inserts (the hard ceiling)', () => {
    const set = new BoundedIdSet(100);
    for (let i = 0; i < 10_000; i += 1) {
      set.add(`evt_${i}`);
      expect(set.size).toBeLessThanOrEqual(100);
    }
    expect(set.size).toBe(100);
    // The most-recent window is retained; far-older ids are gone.
    expect(set.has('evt_9999')).toBe(true);
    expect(set.has('evt_0')).toBe(false);
  });

  it('a non-positive capacity falls back to the default window (never unbounded)', () => {
    const set = new BoundedIdSet(0);
    set.add('a');
    expect(set.has('a')).toBe(true);
    // The default cap is the large recovery-replay-safe window.
    expect(DEFAULT_DEDUP_WINDOW).toBeGreaterThan(10_000);
  });
});

/** Build an NDJSON body (one JSON object per line + trailing newline). */
function ndjson(objs: Array<Record<string, unknown>>): Uint8Array {
  return new TextEncoder().encode(objs.map((o) => JSON.stringify(o)).join('\n') + '\n');
}

// ── Colocated Skill materialization (applySkills → applySnapshot injection) ──

describe('SessionLoop — skills materialization', () => {
  /** A skills push body (manifest + one SKILL.md file) for one skill. */
  function skillsBody(skill: string, content: string): Uint8Array {
    return ndjson([
      { type: 'skills_manifest', dir: 'skills-plugin', skills: [skill] },
      {
        type: 'skill_file',
        skill,
        path: 'SKILL.md',
        mode: 0o644,
        mime_type: null,
        content_base64: Buffer.from(content).toString('base64'),
      },
    ]);
  }

  async function skillsWorkspace(): Promise<string> {
    const { mkdtemp } = await import('node:fs/promises');
    const { tmpdir } = await import('node:os');
    const { join } = await import('node:path');
    return mkdtemp(join(tmpdir(), 'orca-loop-skills-'));
  }

  function loopWithSkills(harness: FakeAgentHarness, skillsWorkspaceDir: string): SessionLoop {
    const providers = new ProviderRegistry();
    providers.register('claude', () => harness);
    return new SessionLoop({ workspaceId: WS, providers, skillsWorkspaceDir });
  }

  it('materializes the pushed skills and injects the plugin dir onto the snapshot', async () => {
    const { join } = await import('node:path');
    const dir = await skillsWorkspace();
    const harness = new FakeAgentHarness();
    const loop = loopWithSkills(harness, dir);

    // Skills push arrives BEFORE the snapshot.
    await loop.applySkills(SES, skillsBody('alpha', '# Alpha\n'));
    await loop.applySnapshot(SES, snapshotBody());

    // The runner-owned plugin dir is projected onto the boot context as `--plugin-dir`.
    const snap = (harness.startInput as { agentSnapshot: { skills_plugin_dir?: string } })
      .agentSnapshot;
    expect(snap.skills_plugin_dir).toBe(join(dir, 'skills-plugin'));
    await loop.stop();
  });

  it('does not override a snapshot that already carries a skills_plugin_dir', async () => {
    const dir = await skillsWorkspace();
    const harness = new FakeAgentHarness();
    const loop = loopWithSkills(harness, dir);
    await loop.applySkills(SES, skillsBody('alpha', '# Alpha\n'));
    // A snapshot that pinned its own plugin dir keeps it (the runner's `??=` yields).
    await loop.applySnapshot(SES, snapshotBody({ skills_plugin_dir: '/pinned/dir' }));
    const snap = (harness.startInput as { agentSnapshot: { skills_plugin_dir?: string } })
      .agentSnapshot;
    expect(snap.skills_plugin_dir).toBe('/pinned/dir');
    await loop.stop();
  });

  it('leaves skills_plugin_dir absent when no skills were pushed', async () => {
    const dir = await skillsWorkspace();
    const harness = new FakeAgentHarness();
    const loop = loopWithSkills(harness, dir);
    await loop.applySnapshot(SES, snapshotBody());
    const snap = (harness.startInput as { agentSnapshot: { skills_plugin_dir?: string } })
      .agentSnapshot;
    expect(snap.skills_plugin_dir).toBeUndefined();
    await loop.stop();
  });

  it('re-delivery replaces the materialized tree (newest-wins)', async () => {
    const { readFile } = await import('node:fs/promises');
    const { join } = await import('node:path');
    const dir = await skillsWorkspace();
    const harness = new FakeAgentHarness();
    const loop = loopWithSkills(harness, dir);
    await loop.applySkills(SES, skillsBody('alpha', 'first\n'));
    await loop.applySkills(SES, skillsBody('alpha', 'second\n'));
    const skillMd = join(dir, 'skills-plugin', 'skills', 'alpha', 'SKILL.md');
    expect(await readFile(skillMd, 'utf8')).toBe('second\n');
    await loop.stop();
  });
});

describe('SessionLoop — request-phase guardrails', () => {
  const denyRule = {
    id: 'grd_req',
    name: 'No turns here',
    tier: 'workspace',
    phases: ['request'],
    rule: {
      kind: 'expression',
      expression: `event.session.id == 'ses_allowed'`,
      onFalse: 'deny',
      reason: 'This session may not start a turn.',
    },
    stateful: false,
  };

  it('never submits a turn the request phase denied', async () => {
    // The model must not see a message a guardrail refused. Asserting on the
    // terminal line alone would not catch a turn that ran and was then
    // narrated as denied — `submitted` is what proves nothing reached the
    // harness.
    const harness = new FakeAgentHarness();
    const loop = loopWith(harness);
    await loop.applySnapshot(SES, snapshotBody({ guardrails: [denyRule] }));

    const lines = await drainLines(
      loop.runTurn(SES, turnBody('hello'), new AbortController().signal),
    );

    expect(harness.submitted).toEqual([]);
    // The turn still terminates: a denied turn is answered, not abandoned, so
    // the owner pod's pending-turn rule does not leave it hanging.
    expect(lines.map((l) => l.type)).toContain(TURN_COMPLETED_EVENT_KIND);
    expect(JSON.stringify(lines)).toContain('This session may not start a turn.');
    await loop.stop();
  });

  it('submits normally when the request phase allows', async () => {
    const harness = new FakeAgentHarness([
      {
        events: [
          { kind: 'agent.message', payload: { content: 'hi' } },
          { kind: TURN_COMPLETED_EVENT_KIND, payload: {} },
        ],
      } satisfies ScriptedTurn,
    ]);
    const loop = loopWith(harness);
    await loop.applySnapshot(
      SES,
      snapshotBody({
        guardrails: [
          { ...denyRule, rule: { ...denyRule.rule, expression: `event.session.id != ''` } },
        ],
      }),
    );

    const lines = await drainLines(
      loop.runTurn(SES, turnBody('hello'), new AbortController().signal),
    );

    expect(harness.submitted).toHaveLength(1);
    expect(lines.map((l) => l.type)).toContain('agent.message');
    await loop.stop();
  });

  it('refuses a snapshot carrying any phase the runner cannot enforce', async () => {
    const loop = loopWith(new FakeAgentHarness());
    for (const phase of ['tool_call', 'tool_result', 'llm_request', 'llm_response', 'response']) {
      await expect(
        loop.applySnapshot(SES, snapshotBody({ guardrails: [{ ...denyRule, phases: [phase] }] })),
      ).rejects.toThrow(/cannot enforce guardrail/);
    }
    await loop.stop();
  });

  it('refuses stateful request rules until their writes are durable', async () => {
    const loop = loopWith(new FakeAgentHarness());
    await expect(
      loop.applySnapshot(
        SES,
        snapshotBody({
          guardrails: [{ ...denyRule, stateful: true, state_scope: 'session' }],
        }),
      ),
    ).rejects.toThrow(/durable Registry write-through/);
    await loop.stop();
  });
});
