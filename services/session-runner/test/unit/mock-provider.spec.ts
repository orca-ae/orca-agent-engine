// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the first-class `mock` provider — a deterministic, LLM-free
// {@link AgentHarness} the runner can serve.
//
// The mock provider is a legitimate runner feature, not a stub: it lets the e2e
// validate the full worker → runner → registry → SSE plumbing in CI with NO API
// key, by emitting a deterministic scripted response per user turn instead of
// calling a model. Asserted here: it registers under `mock`; a `provider: "mock"`
// snapshot resolves to a real harness (not an unknown-provider 422); each
// `user.message` yields an `agent.message` (answering the user text) followed by a
// terminal `agent.turn_completed`, with NOTHING calling out to an LLM; and it
// honors the load-bearing harness contract (`submit` resolves only after the turn's
// events are all emitted) plus the lifecycle surface (interrupt / stop / serial
// turns) the runner's turn loop drives.

import { describe, expect, it } from 'vitest';
import { ProviderRegistry, buildSessionStartInput } from '../../src/harness/provider.js';
import {
  registerMockProvider,
  MockAgentHarness,
  MOCK_PROVIDER_NAME,
} from '../../src/harness/mock/provider.js';
import type { RunnerSnapshot } from '../../src/snapshot.js';
import type { AgentEvent, UserEvent } from '../../src/harness/agent-harness.js';
import { SessionLoop, TURN_COMPLETED_EVENT_KIND } from '../../src/session-loop.js';

const SNAPSHOT: RunnerSnapshot = {
  model: { provider: 'mock', id: 'mock-1' },
  provider: 'mock',
  system: 'be helpful',
  allowed_tool_names: [],
  allowed_mcp_server_names: [],
  tool_permissions: {},
  egress: null,
};

const CTX = { workspaceId: 'ws_mock', sessionId: 'ses_mock' };

function userMessage(text: string): UserEvent {
  return { kind: 'user.message', payload: { content: [{ type: 'text', text }] } };
}

/** Drain one turn: submit, then pull the buffered events without blocking. */
async function drainOneTurn(harness: MockAgentHarness, user: UserEvent): Promise<AgentEvent[]> {
  const events: AgentEvent[] = [];
  const iter = harness.events()[Symbol.asyncIterator]();
  await harness.submit(user);
  for (;;) {
    const next = await Promise.race([
      iter.next(),
      new Promise<'idle'>((r) => setImmediate(() => r('idle'))),
    ]);
    if (next === 'idle' || next.done) {
      break;
    }
    events.push(next.value);
  }
  await harness.stop('replica.shutting_down');
  return events;
}

describe('registerMockProvider', () => {
  it('registers the mock provider name on the registry', () => {
    const registry = new ProviderRegistry();
    registerMockProvider(registry);
    expect(registry.has(MOCK_PROVIDER_NAME)).toBe(true);
    expect(registry.providerNames()).toEqual(['mock']);
  });

  it('builds a real MockAgentHarness for a provider:mock snapshot (no 422)', () => {
    const registry = new ProviderRegistry();
    registerMockProvider(registry);
    const harness = registry.build(SNAPSHOT, CTX);
    expect(harness).toBeInstanceOf(MockAgentHarness);
  });

  it('co-exists with another provider under its own key', () => {
    const registry = new ProviderRegistry();
    registry.register('claude', () => new MockAgentHarness());
    registerMockProvider(registry);
    expect(registry.providerNames()).toEqual(['claude', 'mock']);
  });
});

describe('MockAgentHarness — scripted turn (no LLM)', () => {
  it('emits an agent.message answering the user text, then a terminal turn_completed', async () => {
    const registry = new ProviderRegistry();
    registerMockProvider(registry);
    const harness = registry.build(SNAPSHOT, CTX) as MockAgentHarness;
    await harness.start(buildSessionStartInput(SNAPSHOT, CTX));

    const events = await drainOneTurn(harness, userMessage('hello world'));

    expect(events.map((e) => e.kind)).toEqual(['agent.message', TURN_COMPLETED_EVENT_KIND]);
    // The agent.message echoes/answers the user's text, in the SDK-native content shape.
    const message = events[0]!;
    expect(message.payload).toEqual({ content: [{ type: 'text', text: 'mock: hello world' }] });
    // The terminal marker carries a deterministic stop reason.
    expect(events[1]!.payload).toEqual({ stop_reason: 'end_turn' });
  });

  it('is DETERMINISTIC: the same user text yields the same events every turn', async () => {
    const registry = new ProviderRegistry();
    registerMockProvider(registry);
    const first = registry.build(SNAPSHOT, CTX) as MockAgentHarness;
    await first.start(buildSessionStartInput(SNAPSHOT, CTX));
    const a = await drainOneTurn(first, userMessage('ping'));

    const second = registry.build(SNAPSHOT, CTX) as MockAgentHarness;
    await second.start(buildSessionStartInput(SNAPSHOT, CTX));
    const b = await drainOneTurn(second, userMessage('ping'));

    expect(a).toEqual(b);
  });

  it('answers a turn with no extractable text using a deterministic fallback', async () => {
    const registry = new ProviderRegistry();
    registerMockProvider(registry);
    const harness = registry.build(SNAPSHOT, CTX) as MockAgentHarness;
    await harness.start(buildSessionStartInput(SNAPSHOT, CTX));

    // A user.message with no text content still produces a well-formed answered turn.
    const events = await drainOneTurn(harness, { kind: 'user.message', payload: {} });
    expect(events.map((e) => e.kind)).toEqual(['agent.message', TURN_COMPLETED_EVENT_KIND]);
    expect(events[0]!.payload).toEqual({ content: [{ type: 'text', text: 'mock: (no message)' }] });
  });

  it('drives multiple turns serially, each a complete scripted answer', async () => {
    // The mock turn is deterministic (exactly one agent.message + one terminal
    // marker), so each turn is drained by pulling exactly those two events off the
    // harness's single shared stream — the same single-iterator discipline the
    // runner's loop uses (it never abandons a pull mid-stream). Proves the harness
    // answers two successive turns, each with its own user text.
    const registry = new ProviderRegistry();
    registerMockProvider(registry);
    const harness = registry.build(SNAPSHOT, CTX) as MockAgentHarness;
    await harness.start(buildSessionStartInput(SNAPSHOT, CTX));

    const iter = harness.events()[Symbol.asyncIterator]();
    const turn = async (user: UserEvent): Promise<AgentEvent[]> => {
      await harness.submit(user);
      const first = await iter.next();
      const second = await iter.next();
      return [first, second]
        .filter((r): r is IteratorYieldResult<AgentEvent> => !r.done)
        .map((r) => r.value);
    };

    const first = await turn(userMessage('one'));
    const second = await turn(userMessage('two'));
    await harness.stop('replica.shutting_down');

    expect(first.map((e) => e.kind)).toEqual(['agent.message', TURN_COMPLETED_EVENT_KIND]);
    expect((first[0]!.payload as { content: Array<{ text: string }> }).content[0]!.text).toBe(
      'mock: one',
    );
    expect(second.map((e) => e.kind)).toEqual(['agent.message', TURN_COMPLETED_EVENT_KIND]);
    expect((second[0]!.payload as { content: Array<{ text: string }> }).content[0]!.text).toBe(
      'mock: two',
    );
  });

  it('ignores a non-user.message event (emits nothing for it)', async () => {
    const registry = new ProviderRegistry();
    registerMockProvider(registry);
    const harness = registry.build(SNAPSHOT, CTX) as MockAgentHarness;
    await harness.start(buildSessionStartInput(SNAPSHOT, CTX));
    const events = await drainOneTurn(harness, { kind: 'user.tool_result', payload: {} });
    expect(events).toEqual([]);
  });

  it('submit after stop emits nothing (terminated)', async () => {
    const registry = new ProviderRegistry();
    registerMockProvider(registry);
    const harness = registry.build(SNAPSHOT, CTX) as MockAgentHarness;
    await harness.start(buildSessionStartInput(SNAPSHOT, CTX));
    await harness.stop('replica.shutting_down');
    await harness.submit(userMessage('after stop')); // no throw, no events
  });

  it('interrupt() is a harmless no-op (the synchronous turn already completed) and keeps the harness alive', async () => {
    const registry = new ProviderRegistry();
    registerMockProvider(registry);
    const harness = registry.build(SNAPSHOT, CTX) as MockAgentHarness;
    await harness.start(buildSessionStartInput(SNAPSHOT, CTX));
    expect(() => harness.interrupt()).not.toThrow();
    // The harness is still alive: a turn after the interrupt drives normally.
    const events = await drainOneTurn(harness, userMessage('still here'));
    expect(events.map((e) => e.kind)).toEqual(['agent.message', TURN_COMPLETED_EVENT_KIND]);
  });
});

describe('MockAgentHarness — events() stream lifecycle', () => {
  it('ends events() on stop so a consumer is not stranded', async () => {
    const registry = new ProviderRegistry();
    registerMockProvider(registry);
    const harness = registry.build(SNAPSHOT, CTX) as MockAgentHarness;
    await harness.start(buildSessionStartInput(SNAPSHOT, CTX));
    const iter = harness.events()[Symbol.asyncIterator]();
    await harness.stop('replica.shutting_down');
    const next = await Promise.race([
      iter.next(),
      new Promise<'hang'>((r) => setTimeout(() => r('hang'), 50)),
    ]);
    expect(next).not.toBe('hang');
    expect((next as IteratorResult<AgentEvent>).done).toBe(true);
  });
});

describe('mock provider through the REAL SessionLoop', () => {
  /** Drain a runner turn stream into the parsed NDJSON lines it emitted. */
  async function drainLines(
    stream: AsyncIterable<Uint8Array>,
  ): Promise<Array<Record<string, unknown>>> {
    const chunks: Uint8Array[] = [];
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    return Buffer.concat(chunks.map((c) => Buffer.from(c)))
      .toString('utf8')
      .split('\n')
      .filter((l) => l.trim().length > 0)
      .map((l) => JSON.parse(l) as Record<string, unknown>);
  }

  it('a provider:mock snapshot drives a turn end-to-end through the loop (no LLM, no 422)', async () => {
    // The genuine plumbing the e2e relies on: the loop dispatches on snapshot.provider
    // to the registered mock provider, builds + starts it, drives the turn, and streams
    // the scripted agent.message + turn_completed as NDJSON — no API key anywhere.
    const providers = new ProviderRegistry();
    registerMockProvider(providers);
    const loop = new SessionLoop({ workspaceId: 'ws_mock', providers });

    await loop.applySnapshot('ses_mock', new TextEncoder().encode(`${JSON.stringify(SNAPSHOT)}\n`));
    expect(loop.hasHarness()).toBe(true);

    const lines = await drainLines(
      loop.runTurn(
        'ses_mock',
        new TextEncoder().encode(
          JSON.stringify({
            type: 'user.message',
            id: 'evt_u',
            content: [{ type: 'text', text: 'hi mock' }],
          }),
        ),
        new AbortController().signal,
      ),
    );

    expect(lines.map((l) => l.type)).toEqual(['agent.message', TURN_COMPLETED_EVENT_KIND]);
    expect(lines[0]!['content']).toEqual([{ type: 'text', text: 'mock: hi mock' }]);
    await loop.stop();
  });
});
