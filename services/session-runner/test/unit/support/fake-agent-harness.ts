// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// A scripted fake {@link AgentHarness} for the runner core-loop specs.
//
// The runner CONSTRUCTS a harness from the snapshot and DRIVES it per turn; these
// specs assert the runner's turn-drive / event-emission / lifecycle behavior, so
// the harness itself is a test double, not the real claude SDK (real claude runs in
// the self-hosted e2e). The fake records what the runner did to it (the captured
// start input, each submitted user event, the stop reason) and lets a test SCRIPT
// the agent events a turn emits — honoring the load-bearing harness contract that
// `submit` RESOLVES only after the turn's events are all emitted through
// `events()`, so the runner's "end the stream when submit resolves" turn boundary
// is exercised against a faithful peer.

import type {
  AgentEvent,
  AgentHarness,
  SessionStartInput,
  TerminationReason,
  UserEvent,
} from '../../../src/harness/agent-harness.js';

/** How one scripted turn behaves. */
export interface ScriptedTurn {
  /** The agent events this turn emits, in order. */
  events: AgentEvent[];
  /**
   * When set, `submit` waits on this promise BEFORE emitting the turn's events —
   * so a test can hold a turn open (e.g. to assert a `request.cancel` mid-turn)
   * and release it deterministically.
   */
  gate?: Promise<void>;
  /**
   * When true, the turn ENDS the shared `events()` stream after emitting its events —
   * the shape of a harness that FAULTED mid-turn. Every real harness ends its stream on
   * the way out of a fault (the claude harness's mid-turn catch calls `endEventStream()`;
   * each native-CLI read loop's `finally` does `terminated = true; endEventStream()`), so
   * this is the normal manifestation of a fault on the runner's shared iterator.
   */
  endStream?: boolean;
  /**
   * When true, the turn ends the shared `events()` stream one MACROTASK after `submit`
   * resolved — a harness whose read loop unwinds just past the turn boundary. The runner
   * has by then left its phase-1 race (it broke on `submit` resolving) and PARKED a
   * pending pull, so the stream end surfaces in the phase-2 flush-drain instead.
   */
  endStreamAfterSubmit?: boolean;
  /**
   * When set, `submit` waits on this promise AFTER emitting the turn's events (and after
   * an `endStream`), before resolving — so a test can hold the TURN BOUNDARY open while
   * the stream has already ended, pinning the runner in its phase-1 pull race.
   */
  hold?: Promise<void>;
  /**
   * When set, `submit` REJECTS with this error instead of resolving — the turn-setup /
   * mid-turn failure the runner must surface rather than end as a clean empty turn.
   */
  fail?: Error;
}

/**
 * A fake harness whose turns are scripted per-call.
 *
 * Construct with a queue of {@link ScriptedTurn}s (one per expected `submit`); the
 * nth `submit` plays the nth turn. `submit` emits the turn's events through the
 * shared `events()` stream, then RESOLVES (the contract the runner relies on for
 * its turn boundary). `events()` ends when `stop` is called — or mid-turn, when a turn
 * is scripted to FAULT (`endStream` / `fail`), the shape the runner must not read as a
 * successful empty turn.
 */
export class FakeAgentHarness implements AgentHarness {
  /** The boot input the runner passed to {@link start}; `undefined` until started. */
  startInput: SessionStartInput | undefined;
  /** Each user event the runner submitted, in order. */
  readonly submitted: UserEvent[] = [];
  /** The reason the runner stopped the harness; `undefined` until stopped. */
  stopReason: TerminationReason | undefined;
  /** How many times {@link start} was called (newest-wins snapshot re-delivery). */
  startCount = 0;
  /** How many times {@link interrupt} was called (the out-of-band abort). */
  interruptCount = 0;

  private readonly turns: ScriptedTurn[];
  private turnIndex = 0;
  private readonly outQueue: AgentEvent[] = [];
  private outResolvers: Array<(r: IteratorResult<AgentEvent>) => void> = [];
  private terminated = false;
  /**
   * Fires when the in-flight turn is interrupted, so a gated turn's `await gate`
   * unblocks (the fake analog of aborting the SDK `query()`); reset per turn. The
   * harness stays alive across an interrupt — only the in-flight turn is aborted.
   */
  private interruptInFlight: (() => void) | undefined;
  /** `true` once the in-flight turn was interrupted, so it skips its remaining events. */
  private turnInterrupted = false;

  constructor(turns: ScriptedTurn[] = []) {
    this.turns = turns;
  }

  /** Append another scripted turn (for tests that script lazily). */
  scriptTurn(turn: ScriptedTurn): void {
    this.turns.push(turn);
  }

  async start(input: SessionStartInput): Promise<void> {
    this.startInput = input;
    this.startCount += 1;
    this.terminated = false;
  }

  async submit(event: UserEvent): Promise<void> {
    this.submitted.push(event);
    if (this.terminated) {
      return;
    }
    const turn = this.turns[this.turnIndex];
    this.turnIndex += 1;
    if (turn === undefined) {
      return; // no script for this turn: a no-op turn (emits nothing).
    }
    this.turnInterrupted = false;
    if (turn.gate !== undefined) {
      // Race the gate against an interrupt so an interrupted gated turn unblocks
      // (the fake analog of the real harness aborting its blocked `query()`).
      const interrupted = new Promise<void>((resolve) => {
        this.interruptInFlight = resolve;
      });
      await Promise.race([turn.gate, interrupted]);
      this.interruptInFlight = undefined;
    }
    if (!this.turnInterrupted) {
      for (const event of turn.events) {
        if (this.terminated) {
          break;
        }
        this.emit(event);
      }
    }
    // A faulting turn ends the shared stream on the way out — before the boundary is
    // held or the rejection is raised, exactly as a real harness's fault path does.
    if (turn.endStream === true) {
      this.endEventStream();
    }
    if (turn.endStreamAfterSubmit === true) {
      setImmediate(() => this.endEventStream());
    }
    if (turn.hold !== undefined) {
      await turn.hold;
    }
    if (turn.fail !== undefined) {
      throw turn.fail;
    }
    // `submit` resolves here — AFTER the turn's events were emitted (or the turn was
    // interrupted) — honoring the runner's turn-boundary contract. An interrupt does
    // NOT terminate the harness: the next `submit` plays the next scripted turn.
  }

  /**
   * End the shared `events()` stream: mark the harness terminated and resolve every
   * waiting consumer as done. What a real harness does when its turn faults; exposed so
   * a spec can also end the stream out-of-band, mid-turn.
   */
  endEventStream(): void {
    this.terminated = true;
    while (this.outResolvers.length > 0) {
      const resolver = this.outResolvers.shift();
      if (resolver !== undefined) {
        resolver({ value: undefined, done: true });
      }
    }
  }

  /**
   * Abort the in-flight turn WITHOUT terminating the harness — the out-of-band
   * `user.interrupt`. Unblocks a gated turn's `await gate` and marks the turn so it
   * emits no further events; `events()` keeps flowing for the next turn. A no-op when
   * no turn is in flight.
   */
  interrupt(): void {
    if (this.terminated) {
      return;
    }
    this.interruptCount += 1;
    this.turnInterrupted = true;
    const release = this.interruptInFlight;
    this.interruptInFlight = undefined;
    if (release !== undefined) {
      release();
    }
  }

  async stop(reason: TerminationReason): Promise<void> {
    this.stopReason = reason;
    this.endEventStream();
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.terminated || this.outQueue.length > 0) {
      const head = this.outQueue.shift();
      if (head !== undefined) {
        yield head;
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

  private emit(event: AgentEvent): void {
    const resolver = this.outResolvers.shift();
    if (resolver !== undefined) {
      resolver({ value: event, done: false });
      return;
    }
    this.outQueue.push(event);
  }
}
