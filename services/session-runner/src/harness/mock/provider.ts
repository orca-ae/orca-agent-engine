// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The first-class `mock` provider — a deterministic, LLM-free {@link AgentHarness}.
//
// This is a legitimate runner feature, not a stub. Selected when a snapshot carries
// `provider: "mock"`, it drives each user turn with a DETERMINISTIC scripted
// response instead of calling any model — so the full worker → runner → registry →
// SSE plumbing can be validated in CI with NO API key. It implements the same
// {@link AgentHarness} seam the claude providers do (the runner's turn loop drives
// it identically), and emits Orca-native agent events: per `user.message` it emits
// one `agent.message` (answering the user's text, in the SDK-native content shape)
// followed by a terminal `agent.turn_completed` marker — exactly the "answered"
// shape the owner pod's pending-turn rule keys on.
//
// It honors the load-bearing harness contract: `submit` RESOLVES only after the
// turn's events are all emitted through `events()` (the runner ends the tunneled
// agent-event stream when `submit` returns). The scripted turn emits synchronously,
// so the events are queued before `submit` resolves — the same buffered-emit shape
// the claude harness uses, so the runner's flush-drain turn boundary applies
// uniformly.

import { ProviderRegistry, type ProviderFactory } from '../provider.js';
import type {
  AgentEvent,
  AgentHarness,
  SessionStartInput,
  TerminationReason,
  UserEvent,
} from '../agent-harness.js';

/** The provider name the mock harness registers under (matches the snapshot's). */
export const MOCK_PROVIDER_NAME = 'mock';

/** The agent-event kind that marks a turn complete (mirrors the loop's constant). */
const TURN_COMPLETED_EVENT_KIND = 'agent.turn_completed';

/** The deterministic prefix the mock answer wraps the user's text with. */
const MOCK_ANSWER_PREFIX = 'mock: ';

/** The deterministic answer body when a turn carries no extractable user text. */
const MOCK_NO_MESSAGE_BODY = '(no message)';

/**
 * Register the `mock` provider on `registry` and return it (for chaining).
 *
 * The factory needs no boot collaborators (no transcript store, no LLM egress): the
 * mock harness answers from the user message alone. A `provider: "mock"` snapshot
 * therefore resolves to a live harness with no external wiring.
 *
 * @throws Error if a `mock` provider is already registered (a wiring bug).
 */
export function registerMockProvider(registry: ProviderRegistry): ProviderRegistry {
  const factory: ProviderFactory = () => new MockAgentHarness();
  registry.register(MOCK_PROVIDER_NAME, factory);
  return registry;
}

/**
 * A deterministic, LLM-free {@link AgentHarness}. One per session; the runner
 * `start`s it from the snapshot then `submit`s each user turn, which emits a
 * scripted `agent.message` + `agent.turn_completed`.
 */
export class MockAgentHarness implements AgentHarness {
  private terminated = false;
  private readonly outQueue: AgentEvent[] = [];
  private outResolvers: Array<(r: IteratorResult<AgentEvent>) => void> = [];

  // The snapshot boot context is captured for symmetry with the real harnesses
  // (and so a future scripted behavior could read the model/system); the scripted
  // answer itself depends only on the user message, keeping it deterministic.
  private input: SessionStartInput | undefined;

  async start(input: SessionStartInput): Promise<void> {
    this.input = input;
    this.terminated = false;
  }

  async submit(event: UserEvent): Promise<void> {
    if (this.terminated) {
      return;
    }
    // Only `user.message` drives a turn; other kinds emit nothing (the runner still
    // synthesizes a terminal completed marker, so the turn is answered) — matching
    // the claude harness's chat-only turn handling.
    if (event.kind !== 'user.message') {
      return;
    }
    const answer = `${MOCK_ANSWER_PREFIX}${userTextOf(event.payload) ?? MOCK_NO_MESSAGE_BODY}`;
    // Emit the scripted answer + the terminal marker. Both are queued synchronously
    // here, so they are all in the stream before `submit` resolves (the load-bearing
    // "submit resolves after the turn's events are emitted" contract).
    this.emit({ kind: 'agent.message', payload: { content: [{ type: 'text', text: answer }] } });
    this.emit({ kind: TURN_COMPLETED_EVENT_KIND, payload: { stop_reason: 'end_turn' } });
    // `submit` resolves here — the turn's events are all emitted.
  }

  /**
   * Abort the in-flight turn WITHOUT terminating the harness. The mock turn emits
   * synchronously and completes within `submit`, so there is never a turn in flight
   * to abort: this is a harmless no-op that keeps the harness alive (the next turn
   * drives normally), satisfying the out-of-band interrupt contract.
   */
  interrupt(): void {
    // No-op: the scripted turn already completed; nothing is in flight.
  }

  async stop(reason: TerminationReason): Promise<void> {
    void reason; // Informational; the runner logs it.
    this.terminated = true;
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

/** Extract the first text part from a `user.message` payload's content array. */
function userTextOf(payload: unknown): string | null {
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
