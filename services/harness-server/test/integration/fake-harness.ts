// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type {
  AgentEvent,
  AgentEventInput,
  AgentHarness,
  SessionStartInput,
  TerminationReason,
  UserEvent,
} from '../../src/harness/agent-harness.js';
import { withCanonicalAgentEventEnvelope } from '../../src/harness/agent-harness.js';

export interface FakeHarnessOptions {
  /** Delay before emitting the agent.message reply (simulates a slow LLM). */
  replyDelayMs?: number;
}

/**
 * Test double for `AgentHarness` that echoes each `user.message` back as
 * `agent.message`. Used by the end-to-end and crash-recovery integration
 * tests so we don't need a real Anthropic API key or network access.
 */
export class FakeHarness implements AgentHarness {
  private readonly replyDelayMs: number;
  private q: AgentEvent[] = [];
  private resolvers: Array<(v: IteratorResult<AgentEvent>) => void> = [];
  private done = false;

  constructor(opts: FakeHarnessOptions = {}) {
    this.replyDelayMs = opts.replyDelayMs ?? 0;
  }

  async start(_: SessionStartInput): Promise<void> {
    void _;
  }

  async submit(ev: UserEvent): Promise<void> {
    if (this.done) return;
    if (ev.kind !== 'user.message') return;
    const text = (() => {
      const arr = (ev.payload as { content?: unknown })?.content;
      if (!Array.isArray(arr)) return '';
      const first = arr.find(
        (p: unknown) =>
          typeof p === 'object' && p !== null && (p as { type?: unknown }).type === 'text',
      ) as { text?: unknown } | undefined;
      return typeof first?.text === 'string' ? first.text : '';
    })();
    if (this.replyDelayMs > 0) await new Promise((r) => setTimeout(r, this.replyDelayMs));
    if (this.done) return;
    this.emit({
      kind: 'agent.message',
      payload: { type: 'assistant', content: [{ type: 'text', text: `echo: ${text}` }] },
    });
  }

  async stop(reason: TerminationReason): Promise<void> {
    void reason;
    this.done = true;
    for (const r of this.resolvers.splice(0)) {
      r({ value: undefined, done: true });
    }
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.done || this.q.length > 0) {
      if (this.q.length > 0) {
        const head = this.q.shift();
        if (head !== undefined) {
          yield head;
        }
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        this.resolvers.push(resolve);
      });
      if (next.done) return;
      yield next.value;
    }
  }

  private emit(e: AgentEventInput): void {
    const event = withCanonicalAgentEventEnvelope(e);
    const r = this.resolvers.shift();
    if (r) r({ value: event, done: false });
    else this.q.push(event);
  }
}
