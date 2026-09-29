// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// A fake {@link AgentHarness} whose turn GATES a tool call through the runner's
// confirmation callback — the test peer for the approval round-trip.
//
// On `start` it captures the `confirmTool` gate the SessionLoop binds onto the boot
// context (the runner's `canUseTool`). Its single turn calls that gate keyed by a
// fixed tool-use id, PARKS until the verdict lands (an allow/deny delivered over the
// confirmation route), records whether the tool was allowed or denied, then emits a
// terminal `agent.turn_completed` so the runner's turn boundary fires. `submit`
// resolves only after the turn's events are emitted — honoring the harness contract.
//
// This is a deliberately minimal hand-rolled harness (not a subclass of the scripted
// FakeAgentHarness) because the gating is the point: the turn must block on the gate
// between submit and completion, which the scripted-events model does not express.

import type {
  AgentEvent,
  AgentHarness,
  SessionStartInput,
  TerminationReason,
  ToolConfirmer,
  ToolPermissionDecision,
  UserEvent,
} from '../../../src/harness/agent-harness.js';

export class GatedToolHarness implements AgentHarness {
  /** `true` once a tool call's gate resolved ALLOW. */
  toolAllowed = false;
  /** `true` once a tool call's gate resolved DENY. */
  toolDenied = false;
  /** `true` once the in-flight turn was interrupted (the gate released as a denial). */
  toolInterrupted = false;
  /** Each user event the runner submitted, in order. */
  readonly submitted: UserEvent[] = [];
  /** The reason the runner stopped the harness; `undefined` until stopped. */
  stopReason: TerminationReason | undefined;

  private confirm: ToolConfirmer | undefined;
  private readonly outQueue: AgentEvent[] = [];
  private outResolvers: Array<(r: IteratorResult<AgentEvent>) => void> = [];
  private terminated = false;
  /**
   * Releases the parked tool gate as a denial when the in-flight turn is interrupted
   * (the fake analog of the real harness aborting its turn's `AbortController`, which
   * fires `canUseTool`'s abort race). Set while parked; cleared once resolved.
   */
  private releaseOnInterrupt: ((decision: ToolPermissionDecision) => void) | undefined;

  constructor(private readonly toolUseId: string) {}

  async start(input: SessionStartInput): Promise<void> {
    this.confirm = input.confirmTool;
    this.terminated = false;
  }

  async submit(event: UserEvent): Promise<void> {
    this.submitted.push(event);
    if (this.terminated) {
      return;
    }
    // Gate a tool call: park on the runner confirmation gate keyed by the tool-use id.
    // This is exactly what the real claude harness's `canUseTool` does. The park is
    // raced against an interrupt so an interrupted turn releases the gate as a denial
    // (mirroring the real harness's abort signal), without a delivered verdict.
    if (this.confirm !== undefined) {
      const interrupted = new Promise<ToolPermissionDecision>((resolve) => {
        this.releaseOnInterrupt = resolve;
      });
      const result = await Promise.race([
        this.confirm('Bash', { command: 'ls' }, { toolUseId: this.toolUseId }),
        interrupted,
      ]);
      this.releaseOnInterrupt = undefined;
      if (this.toolInterrupted) {
        this.toolDenied = true;
      } else if (result.behavior === 'allow') {
        this.toolAllowed = true;
      } else {
        this.toolDenied = true;
      }
    }
    if (this.terminated || this.toolInterrupted) {
      // An interrupted turn ends without a completed marker (the partial turn stays
      // re-promptable, exactly like the real cancel/interrupt path); the harness stays
      // alive for the next turn.
      return;
    }
    // End the turn with the completed marker (the runner would synthesize one
    // otherwise; emitting it here keeps the wire assertion explicit).
    this.emit({ kind: 'agent.turn_completed', payload: {} });
    // `submit` resolves here — after the turn's events were emitted.
  }

  /**
   * Abort the in-flight turn WITHOUT terminating the harness — the out-of-band
   * `user.interrupt`. Releases the parked tool gate as a denial; `events()` keeps
   * flowing for the next turn. A no-op when no turn is in flight.
   */
  interrupt(): void {
    if (this.terminated) {
      return;
    }
    this.toolInterrupted = true;
    const release = this.releaseOnInterrupt;
    this.releaseOnInterrupt = undefined;
    if (release !== undefined) {
      release({ behavior: 'deny', message: 'Tool use denied: turn interrupted.' });
    }
  }

  async stop(reason: TerminationReason): Promise<void> {
    this.stopReason = reason;
    this.terminated = true;
    while (this.outResolvers.length > 0) {
      const resolver = this.outResolvers.shift();
      if (resolver !== undefined) {
        resolver({ value: undefined, done: true });
      }
    }
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
