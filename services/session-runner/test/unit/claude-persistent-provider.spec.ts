// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the `claude-sdk-persistent` provider — provider B, the persistent
// live-client {@link AgentHarness} (the streaming-input `query()` kept alive across
// turns), as opposed to provider A's stateless one-shot `query()` per turn.
//
// What this spec pins (the persistent model, Anthropic-native):
//   - a LIVE session reused across turns: one `query()` is created on the first turn
//     and the SAME live handle drives every subsequent turn (no fresh `query()` per
//     turn) — the persistent live-session lifecycle;
//   - PER-TURN model selection: the model the snapshot/turn pins is applied via the
//     live handle's `setModel`, and only when it CHANGES (no redundant set);
//   - INTERRUPT = interrupt-then-rebuild-history: an interrupt fires the live handle's
//     `interrupt()` then CLOSES the live session, so the next turn rebuilds the full
//     conversation history (from the transcript) into a fresh live session — replayed as
//     STRUCTURED prior messages (role + every block preserved) so thinking / tool_use /
//     tool_result context survives; the abandoned turn is visible-but-superseded;
//   - CRASHED-SESSION recovery: a turn-stream fault records the session as crashed +
//     closes the live handle; the next turn SELF-HEALS — it emits a `session_recovered`
//     status diagnostic, rebuilds a fresh live session from the transcript, and drives
//     the turn (recovery is in-harness; the dead handle is never driven);
//   - RETRY observation: an `api_retry` system frame surfaces as an Anthropic-native
//     diagnostic event (and an auth / not-found retry status is terminal);
//   - the `canUseTool` approval hook wired to the EXISTING pending-approvals gate
//     (park on `always_ask`, auto-approve `always_allow`, fail-closed with no policy);
//   - thinking blocks (streamed + settled) + tool-use/result pairing by `tool_use_id`
//     (the shared mapper), identical to provider A;
//   - robust FORCE-CLOSE cleanup on interrupt / stop / fault.
//
// The SDK is driven through a faithful FAKE persistent `Query` (an async-generator
// that also exposes `interrupt`/`setModel`/`close`/`streamInput`, like the real
// streaming-input `Query`) so the whole persistent lifecycle is exercised in-process
// with no network. The transcript store is an in-memory fake; the approvals gate is
// the REAL `PendingApprovals` (driven through the real `SessionLoop` for the full
// chain).

import { describe, it, expect } from 'vitest';
import type {
  Event,
  ReadOptions,
  TailOptions,
  TranscriptStore,
} from '@orca/transcript-store-types';
import { ProviderRegistry, buildSessionStartInput } from '../../src/harness/provider.js';
import {
  registerClaudePersistentProvider,
  CLAUDE_PERSISTENT_PROVIDER_NAME,
} from '../../src/harness/claude/persistent-provider.js';
import { registerClaudeProvider, CLAUDE_PROVIDER_NAME } from '../../src/harness/claude/provider.js';
import { ClaudeAgentSdkHarness } from '../../src/harness/claude/index.js';
import {
  ClaudePersistentSdkHarness,
  type PersistentClaudeQuery,
  type PersistentQueryHandle,
} from '../../src/harness/claude/persistent.js';
import { SessionLoop } from '../../src/session-loop.js';
import { CLAUDE_SESSION_ENTRY_EVENT_KIND } from '../../src/harness/claude/event-mapper.js';
import type { RunnerSnapshot } from '../../src/snapshot.js';
import type { AgentEvent, SessionStartInput } from '../../src/harness/agent-harness.js';

/** A minimal in-memory TranscriptStore: append accumulates, read replays in order. */
class FakeTranscriptStore implements TranscriptStore {
  readonly appended: Array<{ workspaceId: string; sessionId: string; events: Event[] }> = [];
  private readonly byKey = new Map<string, Event[]>();

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    this.appended.push({ workspaceId, sessionId, events });
    const key = `${workspaceId}/${sessionId}`;
    const bucket = this.byKey.get(key) ?? [];
    for (const e of events) {
      bucket.push(e);
    }
    this.byKey.set(key, bucket);
    return events.map((e) => e.id);
  }

  async *read(workspaceId: string, sessionId: string, _opts: ReadOptions): AsyncIterable<Event> {
    const bucket = this.byKey.get(`${workspaceId}/${sessionId}`) ?? [];
    for (const e of bucket) {
      yield e;
    }
  }

  async *tail(_workspaceId: string, _sessionId: string, _opts: TailOptions): AsyncIterable<Event> {
    // not used by the adapter
  }

  async archive(): Promise<void> {
    // not used
  }

  async close(): Promise<void> {
    // not used
  }
}

/** A scripted mid-turn tool call: the fake invokes canUseTool with these, then captures the verdict. */
interface CallToolSpec {
  name: string;
  input: Record<string, unknown>;
  toolUseId: string;
  capture: (outcome: 'allow' | 'deny', message?: string) => void;
}

function isCallToolMarker(item: unknown): item is { __callTool: CallToolSpec } {
  return item !== null && typeof item === 'object' && '__callTool' in (item as object);
}

function isThrowMarker(item: unknown): item is { __throw: unknown } {
  return item !== null && typeof item === 'object' && '__throw' in (item as object);
}

/**
 * A faithful fake of the SDK's streaming-input `Query`: an async generator over the
 * SDK messages, that ALSO exposes the persistent-control surface (`interrupt`,
 * `setModel`, `close`, `streamInput`) the real `Query` exposes in streaming-input
 * mode. One live instance per session — the harness drives many turns through it.
 *
 * The model: the harness pushes a turn's user message in via the prompt iterable
 * (the constructor argument), then reads this generator until the turn's `result`
 * frame. The fake's `scriptTurn(messages)` enqueues the SDK messages the NEXT turn
 * should yield; the generator yields them in order, blocking between turns until the
 * next turn is scripted (mirroring a live CLI that idles between prompts).
 */
class FakePersistentQuery implements AsyncIterableIterator<unknown> {
  /** Models passed to `setModel`, in call order (asserts per-turn model selection). */
  readonly setModelCalls: Array<string | undefined> = [];
  /** Count of `interrupt()` calls (best-effort interrupt before close). */
  interruptCalls = 0;
  /** Count of `close()` calls (the force-close of the live subprocess). */
  closeCalls = 0;
  /** User messages streamed in via the prompt iterable (the turns submitted). */
  readonly promptsSeen: unknown[] = [];
  /** Whether the live handle has been closed (ends the output generator). */
  closed = false;

  /** Per-turn scripted message queues, FIFO; each entry is one turn's messages. */
  private readonly turnQueues: unknown[][] = [];
  /** A waiter resolved when a new turn is scripted while the generator idles. */
  private wake: (() => void) | undefined;
  /** The currently-draining turn's remaining messages. */
  private current: unknown[] = [];

  constructor(
    readonly promptIterable: AsyncIterable<unknown>,
    readonly options: Record<string, unknown>,
  ) {
    // Drain the prompt iterable in the background, recording each submitted user
    // message (the harness pushes one per turn). A real Query consumes the prompt
    // stream the same way.
    void this.drainPrompts();
  }

  private async drainPrompts(): Promise<void> {
    try {
      for await (const m of this.promptIterable) {
        this.promptsSeen.push(m);
      }
    } catch {
      // The harness may abort/close the prompt iterable on teardown — fine.
    }
  }

  /** Enqueue the SDK messages the next turn should yield (then wake the generator). */
  scriptTurn(messages: unknown[]): void {
    this.turnQueues.push(messages);
    this.wake?.();
    this.wake = undefined;
  }

  async setModel(model?: string): Promise<void> {
    this.setModelCalls.push(model);
  }

  async interrupt(): Promise<void> {
    this.interruptCalls += 1;
  }

  close(): void {
    this.closeCalls += 1;
    this.closed = true;
    // Unblock a generator parked between turns so it ends.
    this.wake?.();
    this.wake = undefined;
  }

  async streamInput(_stream: AsyncIterable<unknown>): Promise<void> {
    // The harness uses the prompt-iterable path; streamInput is unused here.
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<unknown> {
    return this;
  }

  async next(): Promise<IteratorResult<unknown>> {
    for (;;) {
      if (this.closed) {
        return { value: undefined, done: true };
      }
      if (this.current.length > 0) {
        const item = this.current.shift();
        // Marker: a mid-turn tool call. The real CLI invokes options.canUseTool
        // before running a tool; the fake does the same here (driving the gate),
        // then continues to the next scripted message. It does NOT yield the marker.
        if (isCallToolMarker(item)) {
          await this.driveToolCall(item.__callTool);
          continue;
        }
        // Marker: a mid-turn stream fault (the live subprocess died). The real
        // generator surfaces this by throwing out of next(); the fake mirrors that.
        if (isThrowMarker(item)) {
          this.closed = true;
          throw item.__throw;
        }
        return { value: item, done: false };
      }
      const nextTurn = this.turnQueues.shift();
      if (nextTurn !== undefined) {
        this.current = [...nextTurn];
        continue;
      }
      // No scripted turn: park until one is scripted or the handle closes.
      await new Promise<void>((resolve) => {
        this.wake = resolve;
      });
    }
  }

  /** Invoke the harness's `canUseTool` the way the real SDK does for a gated tool. */
  private async driveToolCall(spec: CallToolSpec): Promise<void> {
    const canUseTool = this.options['canUseTool'] as
      | ((
          name: string,
          input: Record<string, unknown>,
          opts: { signal: AbortSignal; toolUseID: string },
        ) => Promise<{ behavior: string; message?: string }>)
      | undefined;
    if (typeof canUseTool !== 'function') {
      spec.capture('allow');
      return;
    }
    const ac = new AbortController();
    const verdict = await canUseTool(spec.name, spec.input, {
      signal: ac.signal,
      toolUseID: spec.toolUseId,
    });
    if (verdict.behavior === 'allow') {
      spec.capture('allow');
    } else {
      spec.capture('deny', verdict.message);
    }
  }

  async return(): Promise<IteratorResult<unknown>> {
    this.closed = true;
    return { value: undefined, done: true };
  }

  async throw(err?: unknown): Promise<IteratorResult<unknown>> {
    this.closed = true;
    throw err;
  }
}

/**
 * Build a `PersistentClaudeQuery` fake that records each created live handle. The
 * factory creates a `FakePersistentQuery` per `query()` call; tests assert how many
 * live handles were created (one per live session — a reuse keeps it at 1; an
 * interrupt-rebuild bumps it to 2) and script each handle's turns.
 */
function fakePersistentQueryFactory(): {
  query: PersistentClaudeQuery;
  handles: FakePersistentQuery[];
} {
  const handles: FakePersistentQuery[] = [];
  const query: PersistentClaudeQuery = ({ prompt, options }) => {
    const handle = new FakePersistentQuery(
      prompt as AsyncIterable<unknown>,
      options as unknown as Record<string, unknown>,
    );
    handles.push(handle);
    return handle as unknown as PersistentQueryHandle;
  };
  return { query, handles };
}

const SNAPSHOT: RunnerSnapshot = {
  model: { provider: 'anthropic', id: 'claude-sonnet-4' },
  provider: 'claude-sdk-persistent',
  system: 'be helpful',
  allowed_tool_names: [],
  allowed_mcp_server_names: [],
  tool_permissions: {},
  egress: {
    mode: 'gateway',
    gateway: {
      mcp_base_url: 'https://gw.example/mcp',
      session_jwt: 'mcp-jwt',
      llm_base_url: 'https://gw.example/llm',
      llm_jwt: 'llm-jwt-123',
      mcp_servers: {},
    },
  },
};

function userMessage(text: string): { kind: string; payload: unknown } {
  return { kind: 'user.message', payload: { content: [{ type: 'text', text }] } };
}

/** A lean (one-shot) `query` shape that yields nothing — for the coexistence test. */
function emptyOneShotQuery(): AsyncIterable<never> {
  return (async function* () {
    /* no messages */
  })();
}

/** A `result` SDK frame ending a turn with the given usage. */
function resultFrame(input = 1, output = 1): unknown {
  return {
    type: 'result',
    subtype: 'success',
    usage: { input_tokens: input, output_tokens: output },
  };
}

/** An assistant text message SDK frame. */
function assistantText(text: string): unknown {
  return { type: 'assistant', message: { role: 'assistant', content: [{ type: 'text', text }] } };
}

/** Poll `cond` on each macrotask until true or the bounded budget elapses. */
async function waitUntil(cond: () => boolean, tries = 300): Promise<void> {
  for (let i = 0; i < tries; i += 1) {
    if (cond()) {
      return;
    }
    await new Promise<void>((r) => setTimeout(r, 2));
  }
  throw new Error('waitUntil: condition not met in time');
}

/** Drain a runner turn's NDJSON byte stream to completion (the bytes are ignored). */
async function drainStream(stream: AsyncIterable<Uint8Array>): Promise<void> {
  for await (const _chunk of stream) {
    void _chunk;
  }
}

/**
 * Drive one turn against the harness and collect its agent events. Submits the turn,
 * waits for the live handle to be created (it is opened inside `submit` after an
 * `await`, so it appears a microtask later — a fresh handle is appended to `handles`),
 * then scripts that handle's turn (the `result` frame is what resolves `submit`).
 */
async function driveTurn(
  harness: ClaudePersistentSdkHarness,
  user: { kind: string; payload: unknown },
  handles: FakePersistentQuery[],
  script: () => unknown[],
): Promise<AgentEvent[]> {
  const before = handles.length;
  const events: AgentEvent[] = [];
  const iter = harness.events()[Symbol.asyncIterator]();
  const pump = (async () => {
    for (;;) {
      const next = await iter.next();
      if (next.done) {
        return;
      }
      events.push(next.value);
    }
  })();
  const submitDone = harness.submit(user);
  await waitUntil(() => handles.length > before);
  handles[handles.length - 1]?.scriptTurn(script());
  await submitDone;
  // Give the pump a macrotask to flush the buffered events, then stop.
  await new Promise<void>((r) => setImmediate(r));
  await harness.stop('replica.shutting_down');
  await pump;
  return events;
}

function startInputFor(sessionId: string): SessionStartInput {
  return buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId });
}

describe('registerClaudePersistentProvider', () => {
  it('registers the claude-sdk-persistent provider name on the registry', () => {
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
    });
    expect(registry.has(CLAUDE_PERSISTENT_PROVIDER_NAME)).toBe(true);
    expect(CLAUDE_PERSISTENT_PROVIDER_NAME).toBe('claude-sdk-persistent');
    expect(registry.providerNames()).toEqual(['claude-sdk-persistent']);
  });

  it('builds a real ClaudePersistentSdkHarness for a provider:claude-sdk-persistent snapshot (no 422)', () => {
    const { query } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_1' });
    expect(harness).toBeInstanceOf(ClaudePersistentSdkHarness);
  });

  it('COEXISTS with the lean claude provider on one registry (both names advertised, each builds its own harness)', () => {
    // The way main.ts wires them: both providers on ONE registry. They must not collide
    // and each must dispatch to its OWN harness type by `snapshot.provider`.
    const store = new FakeTranscriptStore();
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store,
      modelDefault: 'm',
      query: () => emptyOneShotQuery(),
    });
    registerClaudePersistentProvider(registry, {
      store,
      modelDefault: 'm',
      query: fakePersistentQueryFactory().query,
    });

    expect(registry.has(CLAUDE_PROVIDER_NAME)).toBe(true);
    expect(registry.has(CLAUDE_PERSISTENT_PROVIDER_NAME)).toBe(true);
    expect(new Set(registry.providerNames())).toEqual(new Set(['claude', 'claude-sdk-persistent']));

    const lean = registry.build(
      { ...SNAPSHOT, provider: 'claude' },
      { workspaceId: 'ws', sessionId: 'ses' },
    );
    const persistent = registry.build(
      { ...SNAPSHOT, provider: 'claude-sdk-persistent' },
      { workspaceId: 'ws', sessionId: 'ses' },
    );
    expect(lean).toBeInstanceOf(ClaudeAgentSdkHarness);
    expect(persistent).toBeInstanceOf(ClaudePersistentSdkHarness);
  });

  it('passes the gateway LLM base URL + scoped JWT + model into the live query options.env', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'claude-default',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_1',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_1'));
    await driveTurn(harness, userMessage('hi'), handles, () => [resultFrame()]);

    const env = handles[0]?.options['env'] as Record<string, string> | undefined;
    expect(env?.['ANTHROPIC_BASE_URL']).toBe('https://gw.example/llm');
    expect(env?.['ANTHROPIC_API_KEY']).toBe('llm-jwt-123');
    // The model the snapshot pinned rides options.model on the live query.
    expect(handles[0]?.options['model']).toBe('claude-sonnet-4');
    // Streaming partial messages requested so thinking can stream live.
    expect(handles[0]?.options['includePartialMessages']).toBe(true);
  });
});

describe('claude-sdk-persistent — persistent live session reused across turns', () => {
  it('creates the live query ONCE and reuses the SAME handle for the second turn', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_live',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_live'));

    // Turn 1.
    const events1: AgentEvent[] = [];
    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        const next = await iter.next();
        if (next.done) return;
        events1.push(next.value);
      }
    })();
    const submit1 = harness.submit(userMessage('first'));
    await waitUntil(() => handles.length === 1);
    handles[0]?.scriptTurn([assistantText('one'), resultFrame()]);
    await submit1;

    // Turn 2 — must reuse the SAME live handle (no new query() created).
    const submit2 = harness.submit(userMessage('second'));
    await waitUntil(() => (handles[0]?.promptsSeen.length ?? 0) >= 2);
    handles[0]?.scriptTurn([assistantText('two'), resultFrame()]);
    await submit2;

    expect(handles).toHaveLength(1); // ONE live session across both turns
    // Both user turns were streamed into the same live handle's prompt.
    expect(handles[0]?.promptsSeen.length).toBe(2);

    await harness.stop('replica.shutting_down');
    await pump;
    // Both assistant texts surfaced as agent.message events.
    const texts = events1
      .filter((e) => e.kind === 'agent.message')
      .map((e) => (e.payload as { content: Array<{ text?: string }> }).content[0]?.text);
    expect(texts).toContain('one');
    expect(texts).toContain('two');
  });

  it('on a live session, sends ONLY the latest user message (history is internal to the live client)', async () => {
    const store = new FakeTranscriptStore();
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, { store, modelDefault: 'm', query });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_only',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_only'));

    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        const n = await iter.next();
        if (n.done) return;
      }
    })();
    const s1 = harness.submit(userMessage('alpha'));
    await waitUntil(() => handles.length === 1);
    handles[0]?.scriptTurn([resultFrame()]);
    await s1;
    const s2 = harness.submit(userMessage('beta'));
    await waitUntil(() => (handles[0]?.promptsSeen.length ?? 0) >= 2);
    handles[0]?.scriptTurn([resultFrame()]);
    await s2;

    // The SECOND prompt carried only "beta" (the latest), NOT a rebuilt history blob.
    const second = handles[0]?.promptsSeen[1];
    const text = userPromptText(second);
    expect(text).toBe('beta');
    expect(text).not.toContain('alpha');

    await harness.stop('replica.shutting_down');
    await pump;
  });
});

describe('claude-sdk-persistent — per-turn model selection (setModel)', () => {
  it('applies the live handle setModel only when the per-turn model CHANGES', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_model',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_model'));

    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        const n = await iter.next();
        if (n.done) return;
      }
    })();

    // Turn 1 with the snapshot model (claude-sonnet-4) — set as options.model, no setModel needed.
    const s1 = harness.submit(userMessage('one'));
    await waitUntil(() => handles.length === 1);
    handles[0]?.scriptTurn([resultFrame()]);
    await s1;
    expect(handles[0]?.setModelCalls).toEqual([]); // initial model rode options.model

    // Turn 2 keeps the SAME model — still no setModel call.
    const s2 = harness.submit(userMessage('two'));
    await waitUntil(() => (handles[0]?.promptsSeen.length ?? 0) >= 2);
    handles[0]?.scriptTurn([resultFrame()]);
    await s2;
    expect(handles[0]?.setModelCalls).toEqual([]);

    // Turn 3 selects a DIFFERENT model via a per-turn override — setModel fires once.
    const s3 = harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'three' }], model: 'claude-opus-4' },
    });
    await waitUntil(() => (handles[0]?.promptsSeen.length ?? 0) >= 3);
    handles[0]?.scriptTurn([resultFrame()]);
    await s3;
    expect(handles[0]?.setModelCalls).toEqual(['claude-opus-4']);

    await harness.stop('replica.shutting_down');
    await pump;
  });

  it('applies a per-turn model override on a FRESH session (post-interrupt) via options.model — not setModel — and the seed suppresses a redundant setModel next turn', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_freshmodel',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_freshmodel'));

    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        const n = await iter.next();
        if (n.done) return;
      }
    })();

    // Turn 1 on the snapshot model (claude-sonnet-4) — rides handle #1's options.model.
    const s1 = harness.submit(userMessage('one'));
    await waitUntil(() => handles.length === 1);
    handles[0]?.scriptTurn([resultFrame()]);
    await s1;

    // Interrupt closes the live session: the NEXT turn opens a fresh handle #2.
    harness.interrupt();
    await waitUntil(() => (handles[0]?.closeCalls ?? 0) >= 1);

    // Turn 2 carries a per-turn model override. Because the session is FRESH (rebuilt
    // after the interrupt), the override must ride the NEW handle's options.model — NOT
    // a setModel call (there is no live model to change yet).
    const s2 = harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'two' }], model: 'claude-opus-4' },
    });
    await waitUntil(() => handles.length === 2);
    handles[1]?.scriptTurn([resultFrame()]);
    await s2;

    expect(handles).toHaveLength(2);
    // The override seeded the fresh live query's options.model directly…
    expect(handles[1]?.options['model']).toBe('claude-opus-4');
    // …and was NOT applied via setModel (a fresh session has no prior model to switch).
    expect(handles[1]?.setModelCalls).toEqual([]);

    // Turn 3 keeps the SAME override model. Since the fresh session was SEEDED to it,
    // no redundant setModel fires (the change-detection holds across the rebuild).
    const s3 = harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'three' }], model: 'claude-opus-4' },
    });
    await waitUntil(() => (handles[1]?.promptsSeen.length ?? 0) >= 2);
    handles[1]?.scriptTurn([resultFrame()]);
    await s3;
    expect(handles[1]?.setModelCalls).toEqual([]);

    await harness.stop('replica.shutting_down');
    await pump;
  });
});

describe('claude-sdk-persistent — per-turn model validation', () => {
  it('REJECTS a malformed per-turn model override (control chars) back to the snapshot model with an agent.status diagnostic', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_badmodel',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_badmodel'));

    // A per-turn override carrying a newline (injection-shaped) is rejected; the turn
    // falls back to the snapshot model, and NO unvetted id reaches the live session.
    const events = await collectTurnScripted(
      harness,
      {
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'hi' }], model: 'evil\nmodel' },
      },
      handles,
      () => [resultFrame()],
    );

    // The fresh live session opened on the snapshot model (claude-sonnet-4), not the junk id.
    expect(handles[0]?.options['model']).toBe('claude-sonnet-4');
    expect(handles[0]?.setModelCalls).toEqual([]);
    const rejected = events.find(
      (e) =>
        e.kind === 'agent.status' &&
        (e.payload as { status?: string }).status === 'model_override_rejected',
    );
    expect(rejected).toBeDefined();
    expect((rejected?.payload as { reason?: string }).reason).toBe('malformed_model_id');
    expect((rejected?.payload as { effective_model?: string }).effective_model).toBe(
      'claude-sonnet-4',
    );
  });

  it('REJECTS an over-long per-turn model override (length cap)', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_longmodel',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_longmodel'));

    const events = await collectTurnScripted(
      harness,
      {
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'hi' }], model: 'x'.repeat(5000) },
      },
      handles,
      () => [resultFrame()],
    );

    expect(handles[0]?.options['model']).toBe('claude-sonnet-4'); // fell back to the snapshot model
    const rejected = events.find(
      (e) =>
        e.kind === 'agent.status' &&
        (e.payload as { status?: string }).status === 'model_override_rejected',
    );
    expect(rejected).toBeDefined();
    expect((rejected?.payload as { reason?: string }).reason).toBe('malformed_model_id');
  });

  it('with an allowedModels allow-list set, REJECTS an override outside it (model_not_allowed) but ADMITS one inside it', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
      // Only these ids may be selected per-turn (the snapshot model is implicitly allowed).
      allowedModels: ['claude-opus-4'],
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_allow',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_allow'));

    const iter = harness.events()[Symbol.asyncIterator]();
    const collected: AgentEvent[] = [];
    const pump = (async () => {
      for (;;) {
        const n = await iter.next();
        if (n.done) return;
        collected.push(n.value);
      }
    })();

    // Turn 1: an override OUTSIDE the allow-list (and not the snapshot model) is rejected.
    const s1 = harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'one' }], model: 'claude-haiku-3' },
    });
    await waitUntil(() => handles.length === 1);
    handles[0]?.scriptTurn([resultFrame()]);
    await s1;
    expect(handles[0]?.options['model']).toBe('claude-sonnet-4'); // snapshot fallback, not the rejected id
    expect(handles[0]?.setModelCalls).toEqual([]);
    const blocked = collected.find(
      (e) =>
        e.kind === 'agent.status' &&
        (e.payload as { status?: string }).status === 'model_override_rejected',
    );
    expect(blocked).toBeDefined();
    expect((blocked?.payload as { reason?: string }).reason).toBe('model_not_allowed');

    // Turn 2: an override INSIDE the allow-list is admitted via setModel (it changed).
    const s2 = harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'two' }], model: 'claude-opus-4' },
    });
    await waitUntil(() => (handles[0]?.promptsSeen.length ?? 0) >= 2);
    handles[0]?.scriptTurn([resultFrame()]);
    await s2;
    expect(handles[0]?.setModelCalls).toEqual(['claude-opus-4']);

    await harness.stop('replica.shutting_down');
    await pump;
  });

  it('ADMITS a well-formed per-turn override when NO allow-list is configured (format-only; gateway is the policy point)', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_fmtok',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_fmtok'));

    const events = await collectTurnScripted(
      harness,
      {
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'hi' }], model: 'claude-opus-4-1' },
      },
      handles,
      () => [resultFrame()],
    );

    // The well-formed override seeded the fresh live query's options.model directly.
    expect(handles[0]?.options['model']).toBe('claude-opus-4-1');
    // No rejection diagnostic was emitted (the override was admitted on format).
    expect(
      events.some(
        (e) =>
          e.kind === 'agent.status' &&
          (e.payload as { status?: string }).status === 'model_override_rejected',
      ),
    ).toBe(false);
  });
});

describe('claude-sdk-persistent — interrupt = interrupt-then-rebuild-history', () => {
  it('interrupt() interrupts + closes the live session; the NEXT turn rebuilds full history as STRUCTURED prior messages into a fresh session', async () => {
    // Seed prior history into the transcript so the rebuild has something to replay.
    const store = new FakeTranscriptStore();
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, { store, modelDefault: 'm', query });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_int',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_int'));

    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        const n = await iter.next();
        if (n.done) return;
      }
    })();

    // Turn 1: a normal completed turn on the live session. The live CLI persists the
    // conversation through the session-store adapter (the SDK's `sessionStore.append`);
    // here the fake handle persists the user + assistant entries during the turn so the
    // post-interrupt rebuild can read them back from the transcript.
    const s1 = harness.submit(userMessage('remember apples'));
    await waitUntil(() => handles.length === 1);
    await persistTurnEntries(handles[0], [
      { type: 'user', uuid: 'u-rem', message: { role: 'user', content: 'remember apples' } },
      {
        type: 'assistant',
        uuid: 'a-rem',
        message: { role: 'assistant', content: [{ type: 'text', text: 'noted apples' }] },
      },
    ]);
    handles[0]?.scriptTurn([assistantText('noted apples'), resultFrame()]);
    await s1;

    // Interrupt: must call the live handle's interrupt() THEN close it.
    harness.interrupt();
    expect(handles[0]?.interruptCalls).toBe(1);
    await waitUntil(() => (handles[0]?.closeCalls ?? 0) >= 1);

    // Turn 2 after the interrupt: a NEW live session is created (handle #2). It is PRIMED
    // with the prior turns replayed as STRUCTURED, append-only (`shouldQuery:false`)
    // messages, THEN the latest user message (the only one that drives the turn).
    const s2 = harness.submit(userMessage('what fruit?'));
    await waitUntil(() => handles.length === 2);
    handles[1]?.scriptTurn([assistantText('apples'), resultFrame()]);
    await s2;

    expect(handles).toHaveLength(2); // a fresh live session after the interrupt
    const seen = handles[1]?.promptsSeen ?? [];
    // Prior history rode as append-only priming messages (role preserved, shouldQuery:false)…
    const priming = seen.filter((m) => promptShouldQuery(m) === false);
    expect(priming.length).toBe(2);
    expect(priming.map(promptRole)).toEqual(['user', 'assistant']);
    expect(userPromptText(priming[0])).toContain('remember apples');
    expect(userPromptText(priming[1])).toContain('noted apples'); // prior assistant content replayed
    // …and the LAST message is the latest user turn (the one that drives the turn).
    const last = seen[seen.length - 1];
    expect(promptShouldQuery(last)).not.toBe(false); // drives a turn (shouldQuery default)
    expect(userPromptText(last)).toBe('what fruit?');
    expect(userPromptText(last)).not.toContain('apples'); // latest message is NOT a flattened blob

    await harness.stop('replica.shutting_down');
    await pump;
  });

  it('rebuilds prior history as STRUCTURED messages with FULL fidelity: text, thinking, tool_use and tool_result blocks all survive verbatim', async () => {
    // This pins the faithful rebuild: a fresh live session is primed with the prior turns
    // replayed VERBATIM as structured `SDKUserMessage`s (each role preserved, each block
    // array carried as-is, `shouldQuery:false` so it is append-only). Every block type —
    // text, thinking, tool_use, tool_result — survives, so the rebuilt session retains the
    // same thinking / tool context a live session would have held (no plain-text flatten,
    // no dropped tool/thinking history).
    const store = new FakeTranscriptStore();
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, { store, modelDefault: 'm', query });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_fidelity',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_fidelity'));

    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        const n = await iter.next();
        if (n.done) return;
      }
    })();

    // Turn 1: persist a MIXED prior history through the adapter (the way the SDK would),
    // then interrupt so turn 2 rebuilds it.
    const s1 = harness.submit(userMessage('use a tool then summarize'));
    await waitUntil(() => handles.length === 1);
    await persistTurnEntries(handles[0], [
      // Plain user text — replayed as a `user` message.
      {
        type: 'user',
        uuid: 'u-1',
        message: { role: 'user', content: 'use a tool then summarize' },
      },
      // Assistant turn mixing thinking + a tool_use + text — ALL three blocks survive
      // verbatim on the replayed assistant message (full thinking + tool context retained).
      {
        type: 'assistant',
        uuid: 'a-1',
        message: {
          role: 'assistant',
          content: [
            { type: 'thinking', thinking: 'THOUGHT_kept_intact', signature: 'sig' },
            {
              type: 'tool_use',
              id: 'toolu_1',
              name: 'Bash',
              input: { command: 'CMD_kept_intact' },
            },
            { type: 'text', text: 'Running the command now.' },
          ],
        },
      },
      // Tool RESULT comes back as a user message with a tool_result block — preserved as a
      // structured `user` message so the rebuilt session sees the tool output.
      {
        type: 'user',
        uuid: 'u-2',
        message: {
          role: 'user',
          content: [
            {
              type: 'tool_result',
              tool_use_id: 'toolu_1',
              content: 'RESULT_kept_intact',
              is_error: false,
            },
          ],
        },
      },
      // Assistant turn with ONLY a tool_use (no text) — preserved verbatim (a real tool
      // turn carries context even with no human-readable text).
      {
        type: 'assistant',
        uuid: 'a-2',
        message: {
          role: 'assistant',
          content: [
            {
              type: 'tool_use',
              id: 'toolu_2',
              name: 'Read',
              input: { path: 'PATH_kept_intact' },
            },
          ],
        },
      },
      // Final assistant text summary — replayed as an `assistant` message.
      {
        type: 'assistant',
        uuid: 'a-3',
        message: { role: 'assistant', content: [{ type: 'text', text: 'Here is the summary.' }] },
      },
      // A genuinely-empty entry (no recoverable content) is the only thing dropped.
      { type: 'assistant', uuid: 'a-empty', message: { role: 'assistant', content: [] } },
    ]);
    handles[0]?.scriptTurn([assistantText('ok'), resultFrame()]);
    await s1;

    harness.interrupt();
    await waitUntil(() => (handles[0]?.closeCalls ?? 0) >= 1);

    // Turn 2: the fresh session is primed with the structured prior history, then the
    // latest user message drives the turn.
    const s2 = harness.submit(userMessage('and the result?'));
    await waitUntil(() => handles.length === 2);
    handles[1]?.scriptTurn([assistantText('done'), resultFrame()]);
    await s2;

    const seen = handles[1]?.promptsSeen ?? [];
    const priming = seen.filter((m) => promptShouldQuery(m) === false);
    // FIVE prior entries replayed (the empty assistant entry is the only one dropped);
    // each carries its role + full block array verbatim, append-only.
    expect(priming.length).toBe(5);
    expect(priming.map(promptRole)).toEqual([
      'user',
      'assistant',
      'user',
      'assistant',
      'assistant',
    ]);
    // The mixed assistant turn kept ALL its blocks (thinking + tool_use + text), in order.
    const mixed = promptBlocks(priming[1]);
    expect(mixed.map((b) => (b as { type: string }).type)).toEqual([
      'thinking',
      'tool_use',
      'text',
    ]);
    // The tool_result-only user turn and the tool_use-only assistant turn are preserved.
    expect((promptBlocks(priming[2])[0] as { type: string }).type).toBe('tool_result');
    expect((promptBlocks(priming[3])[0] as { type: string }).type).toBe('tool_use');
    // The full structured content survives verbatim — thinking / tool input / tool result
    // are all retained (the whole point of the structured rebuild; nothing flattened away).
    const replayed = JSON.stringify(priming);
    expect(replayed).toContain('THOUGHT_kept_intact');
    expect(replayed).toContain('CMD_kept_intact');
    expect(replayed).toContain('RESULT_kept_intact');
    expect(replayed).toContain('PATH_kept_intact');
    // The latest user message is the LAST prompt and drives the turn (shouldQuery default).
    const last = seen[seen.length - 1];
    expect(promptShouldQuery(last)).not.toBe(false);
    expect(userPromptText(last)).toBe('and the result?');

    await harness.stop('replica.shutting_down');
    await pump;
  });

  it('a user.interrupt submitted as a turn also interrupts + closes the live session', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_int2',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_int2'));

    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        const n = await iter.next();
        if (n.done) return;
      }
    })();
    const s1 = harness.submit(userMessage('go'));
    await waitUntil(() => handles.length === 1);
    handles[0]?.scriptTurn([resultFrame()]);
    await s1;

    // user.interrupt as a turn delegates to interrupt().
    await harness.submit({ kind: 'user.interrupt', payload: { type: 'user.interrupt' } });
    expect(handles[0]?.interruptCalls).toBe(1);
    await waitUntil(() => (handles[0]?.closeCalls ?? 0) >= 1);

    await harness.stop('replica.shutting_down');
    await pump;
  });

  it('interrupt() force-closes a turn BLOCKED on the model, and the harness STAYS ALIVE for the next turn', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_block',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_block'));

    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        const n = await iter.next();
        if (n.done) return;
      }
    })();

    // Turn 1 blocks on the model (no result frame scripted → submit does not resolve).
    const blockedTurn = harness.submit(userMessage('block me'));
    await waitUntil(() => handles.length === 1);
    // Interrupt unwinds the blocked turn (interrupt + close ends the live generator).
    harness.interrupt();
    await blockedTurn; // resolves because the close ended the live stream
    expect(handles[0]?.interruptCalls).toBe(1);
    expect(handles[0]?.closeCalls).toBeGreaterThanOrEqual(1);

    // Turn 2: a fresh live session drives normally — the harness survived the interrupt.
    const s2 = harness.submit(userMessage('next'));
    await waitUntil(() => handles.length === 2);
    handles[1]?.scriptTurn([resultFrame()]);
    await s2;
    expect(handles).toHaveLength(2);

    await harness.stop('replica.shutting_down');
    await pump;
  });

  it('interrupt() is a no-op after the harness is stopped (terminated)', async () => {
    const { query } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_noop',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_noop'));
    await harness.stop('replica.shutting_down');
    expect(() => harness.interrupt()).not.toThrow();
  });
});

describe('claude-sdk-persistent — crashed-session recovery (self-heal)', () => {
  it('a turn-stream fault records the session crashed + closes the live handle; the NEXT turn self-heals (status diagnostic + fresh rebuilt session) and drives', async () => {
    const store = new FakeTranscriptStore();
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, { store, modelDefault: 'm', query });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_crash',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_crash'));

    // ONE shared events() consumer across BOTH turns — mirroring SessionLoop's single
    // shared iterator (a crashed harness STAYS ALIVE; the stream is not per-turn).
    const events: AgentEvent[] = [];
    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        let n: IteratorResult<AgentEvent>;
        try {
          n = await iter.next();
        } catch {
          return;
        }
        if (n.done) return;
        events.push(n.value);
      }
    })();

    // Turn 1 faults mid-stream (the live subprocess died): script a throwing turn. The
    // live handle persists the user turn through the adapter first, so the post-crash
    // rebuild has prior history to replay.
    const s1 = harness.submit(userMessage('go')).catch((e: unknown) => {
      void e; // the fault propagates out of submit; the loop contains it in production
    });
    await waitUntil(() => handles.length === 1);
    await persistTurnEntries(handles[0], [
      { type: 'user', uuid: 'u-go', message: { role: 'user', content: 'go' } },
    ]);
    handles[0]?.scriptTurn([
      assistantText('partial'),
      { __throw: new Error('sdk subprocess died') },
    ]);
    await s1;

    // The live handle was force-closed on the fault; the session is marked crashed and a
    // terminal error surfaced for turn 1.
    await waitUntil(() => (handles[0]?.closeCalls ?? 0) >= 1);
    expect(harness.isCrashed()).toBe(true);
    await waitUntil(() => events.some((e) => e.kind === 'agent.error'));

    // Turn 2 for the crashed session SELF-HEALS: it emits a `session_recovered`
    // agent.status diagnostic, rebuilds a FRESH live session (handle #2) from the
    // transcript, and drives the turn — recovery is in-harness, no snapshot re-push.
    const s2 = harness.submit(userMessage('again'));
    await waitUntil(() => handles.length === 2);
    handles[1]?.scriptTurn([assistantText('recovered'), resultFrame()]);
    await s2;

    expect(handles).toHaveLength(2); // a fresh live session rebuilt for the crashed session
    expect(harness.isCrashed()).toBe(false); // the crash cleared on self-heal

    // The recovery diagnostic surfaced as Anthropic-native agent.status (not a dead-end error).
    const recovered = events.find(
      (e) =>
        e.kind === 'agent.status' &&
        (e.payload as { status?: string }).status === 'session_recovered',
    );
    expect(recovered).toBeDefined();
    expect(JSON.stringify(recovered?.payload)).toContain('sdk subprocess died'); // carries the cause

    // The rebuilt session replayed prior history (append-only) then drove the latest turn,
    // which produced its assistant message.
    const priming = (handles[1]?.promptsSeen ?? []).filter((m) => promptShouldQuery(m) === false);
    expect(priming.length).toBeGreaterThanOrEqual(1);
    const texts = events
      .filter((e) => e.kind === 'agent.message')
      .map((e) => (e.payload as { content: Array<{ text?: string }> }).content[0]?.text);
    expect(texts).toContain('recovered');

    await harness.stop('replica.shutting_down');
    await pump;
  });
});

describe('claude-sdk-persistent — api_retry observation', () => {
  it('surfaces an api_retry system frame as an Anthropic-native agent.status diagnostic', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_retry',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_retry'));

    const events = await collectTurnScripted(harness, userMessage('go'), handles, () => [
      {
        type: 'system',
        subtype: 'api_retry',
        attempt: 1,
        max_retries: 3,
        retry_delay_ms: 500,
        error_status: 529,
        error: 'server_error',
      },
      assistantText('recovered'),
      resultFrame(),
    ]);

    const status = events.find((e) => e.kind === 'agent.status');
    expect(status).toBeDefined();
    expect((status?.payload as { status?: string }).status).toBe('api_retry');
    expect((status?.payload as { attempt?: number }).attempt).toBe(1);
    // The turn still completed (a non-terminal retry status is observed, not fatal).
    expect(events.some((e) => e.kind === 'agent.message')).toBe(true);
  });

  it('an auth-failed api_retry (status 401) is TERMINAL: yields an agent.error and ends the turn', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_auth',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_auth'));

    const events = await collectTurnScripted(harness, userMessage('go'), handles, () => [
      {
        type: 'system',
        subtype: 'api_retry',
        attempt: 1,
        max_retries: 3,
        retry_delay_ms: 0,
        error_status: 401,
        error: 'authentication_failed',
      },
      // (a real CLI would keep retrying; the harness treats 401 as terminal and stops)
    ]);

    const err = events.find((e) => e.kind === 'agent.error');
    expect(err).toBeDefined();
    expect(JSON.stringify(err?.payload).toLowerCase()).toContain('auth');
  });

  it('an endpoint-not-found api_retry (status 404) is TERMINAL: yields an agent.error pointing at ANTHROPIC_BASE_URL', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_nf',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_nf'));

    const events = await collectTurnScripted(harness, userMessage('go'), handles, () => [
      {
        type: 'system',
        subtype: 'api_retry',
        attempt: 1,
        max_retries: 3,
        retry_delay_ms: 0,
        error_status: 404,
        error: 'not_found_error',
      },
      // A 404 means the configured base URL has no Anthropic endpoint — the model can
      // make no progress, so (unlike a 529) the harness treats it as terminal and stops.
    ]);

    // The diagnostic still surfaces as agent.status (the retry is observed)…
    const status = events.find((e) => e.kind === 'agent.status');
    expect(status).toBeDefined();
    expect((status?.payload as { status?: string }).status).toBe('api_retry');
    expect((status?.payload as { error_status?: number }).error_status).toBe(404);
    // …and the terminal error names the base-URL misconfiguration to surface.
    const err = events.find((e) => e.kind === 'agent.error');
    expect(err).toBeDefined();
    const msg = (err?.payload as { message?: string }).message ?? '';
    expect(msg).toContain('endpoint was not found');
    expect(msg).toContain('ANTHROPIC_BASE_URL');
    // The turn ended at the terminal retry: no assistant message followed it.
    expect(events.some((e) => e.kind === 'agent.message')).toBe(false);
  });
});

describe('claude-sdk-persistent — thinking blocks + tool pairing', () => {
  it('streams thinking_delta as partial thinking blocks then the settled blocks (shared mapper)', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_think',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_think'));

    const events = await collectTurnScripted(harness, userMessage('hello'), handles, () => [
      { type: 'system', subtype: 'init', mcp_servers: [] },
      {
        type: 'stream_event',
        event: {
          type: 'content_block_delta',
          index: 0,
          delta: { type: 'thinking_delta', thinking: 'I should ' },
        },
      },
      {
        type: 'stream_event',
        event: {
          type: 'content_block_delta',
          index: 0,
          delta: { type: 'thinking_delta', thinking: 'check.' },
        },
      },
      {
        type: 'assistant',
        message: {
          role: 'assistant',
          content: [
            { type: 'thinking', thinking: 'I should check.', signature: 'sig' },
            { type: 'text', text: 'Let me look.' },
          ],
        },
      },
      resultFrame(5, 9),
    ]);

    const messages = events.filter((e) => e.kind === 'agent.message');
    const partials = messages.filter((e) => (e.payload as { partial?: boolean }).partial === true);
    expect(partials).toHaveLength(2);
    expect((partials[0]?.payload as { content: unknown[] }).content).toEqual([
      { type: 'thinking', thinking: 'I should ' },
    ]);
    const settled = messages.filter((e) => (e.payload as { partial?: boolean }).partial !== true);
    expect(
      settled.map((e) => (e.payload as { content: Array<{ type: string }> }).content[0]?.type),
    ).toEqual(['thinking', 'text']);
  });

  it('emits paired agent.tool_use / agent.tool_result events keyed by tool_use_id', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_pair',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_pair'));

    const events = await collectTurnScripted(harness, userMessage('go'), handles, () => [
      {
        type: 'assistant',
        message: {
          role: 'assistant',
          content: [{ type: 'tool_use', id: 'toolu_42', name: 'Bash', input: { command: 'ls' } }],
        },
      },
      {
        type: 'user',
        message: {
          role: 'user',
          content: [
            { type: 'tool_result', tool_use_id: 'toolu_42', content: 'a\nb', is_error: false },
          ],
        },
      },
      resultFrame(),
    ]);

    const use = events.find((e) => e.kind === 'agent.tool_use');
    const result = events.find((e) => e.kind === 'agent.tool_result');
    expect(use?.id).toBe('toolu_42');
    expect(use?.payload).toEqual({
      name: 'Bash',
      input: { command: 'ls' },
      tool_use_id: 'toolu_42',
    });
    expect(result?.id).toBe('toolu_42');
    expect(result?.payload).toEqual({ tool_use_id: 'toolu_42', content: 'a\nb', is_error: false });
  });
});

describe('claude-sdk-persistent — usage accounting', () => {
  it("emits the result frame's usage as an agent.usage event (same shared mapper as provider A)", async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_usage',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_usage'));

    // A result frame carrying token usage flows through the SAME SdkMessageMapper before
    // isResultFrame() ends the turn, so its cumulative usage surfaces as agent.usage on
    // the persistent path exactly as it does on provider A's one-shot path.
    const events = await collectTurnScripted(harness, userMessage('go'), handles, () => [
      assistantText('done'),
      {
        type: 'result',
        subtype: 'success',
        usage: {
          input_tokens: 12,
          output_tokens: 7,
          cache_read_input_tokens: 2,
          cache_creation: { ephemeral_1h_input_tokens: 3, ephemeral_5m_input_tokens: 4 },
        },
      },
    ]);

    const kinds = events.map((e) => e.kind);
    expect(kinds).toContain('agent.usage');
    const usageEvent = events.find((e) => e.kind === 'agent.usage');
    expect(usageEvent?.payload).toEqual({
      usage: {
        cache_creation: { ephemeral_1h_input_tokens: 3, ephemeral_5m_input_tokens: 4 },
        cache_read_input_tokens: 2,
        input_tokens: 12,
        output_tokens: 7,
      },
    });
  });
});

describe('claude-sdk-persistent — canUseTool approval gate (wired to PendingApprovals)', () => {
  it('wires canUseTool when start carries confirmTool (no bypassPermissions)', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_gate',
    }) as ClaudePersistentSdkHarness;
    const startInput: SessionStartInput = {
      ...startInputFor('ses_gate'),
      confirmTool: async (_n, input) => ({ behavior: 'allow', updatedInput: input }),
    };
    await harness.start(startInput);
    await driveTurn(harness, userMessage('go'), handles, () => [resultFrame()]);

    expect(typeof handles[0]?.options['canUseTool']).toBe('function');
    expect(handles[0]?.options['permissionMode']).toBeUndefined();
  });

  it('FULL CHAIN: SDK invokes canUseTool mid-turn → real SessionLoop gate parks → delivered ALLOW proceeds', async () => {
    let toolOutcome: 'allow' | 'deny' | undefined;
    const { query, handles } = fakePersistentQueryFactory();
    const providers = new ProviderRegistry();
    registerClaudePersistentProvider(providers, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const loop = new SessionLoop({ workspaceId: 'ws_full', providers });
    await loop.applySnapshot('ses_full', new TextEncoder().encode(`${JSON.stringify(SNAPSHOT)}\n`));

    const turnStream = loop.runTurn(
      'ses_full',
      new TextEncoder().encode(
        JSON.stringify({ type: 'user.message', content: [{ type: 'text', text: 'go' }] }),
      ),
      new AbortController().signal,
    );
    const drained = drainStream(turnStream);
    // Once the live handle exists, script a turn that CALLS canUseTool then completes.
    await waitUntil(() => handles.length === 1);
    handles[0]?.scriptTurn([
      {
        __callTool: {
          name: 'Bash',
          input: { command: 'ls' },
          toolUseId: 'toolu_full',
          capture: (o: 'allow' | 'deny') => {
            toolOutcome = o;
          },
        },
      },
      resultFrame(),
    ]);

    await waitUntil(() => loop.hasPendingApproval('toolu_full'));
    expect(loop.resolveToolConfirmation('toolu_full', true)).toBe(true);
    await drained;
    expect(toolOutcome).toBe('allow');
    expect(loop.hasPendingApproval()).toBe(false);
    await loop.stop();
  });

  it('FULL CHAIN: a delivered DENY returns a clean denial to the model mid-turn', async () => {
    let toolOutcome: 'allow' | 'deny' | undefined;
    let denyMessage: string | undefined;
    const { query, handles } = fakePersistentQueryFactory();
    const providers = new ProviderRegistry();
    registerClaudePersistentProvider(providers, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const loop = new SessionLoop({ workspaceId: 'ws_full', providers });
    await loop.applySnapshot('ses_deny', new TextEncoder().encode(`${JSON.stringify(SNAPSHOT)}\n`));

    const drained = drainStream(
      loop.runTurn(
        'ses_deny',
        new TextEncoder().encode(
          JSON.stringify({ type: 'user.message', content: [{ type: 'text', text: 'go' }] }),
        ),
        new AbortController().signal,
      ),
    );
    await waitUntil(() => handles.length === 1);
    handles[0]?.scriptTurn([
      {
        __callTool: {
          name: 'Bash',
          input: { command: 'rm -rf /' },
          toolUseId: 'toolu_deny',
          capture: (o: 'allow' | 'deny', msg?: string) => {
            toolOutcome = o;
            denyMessage = msg;
          },
        },
      },
      resultFrame(),
    ]);

    await waitUntil(() => loop.hasPendingApproval('toolu_deny'));
    expect(loop.resolveToolConfirmation('toolu_deny', false)).toBe(true);
    await drained;
    expect(toolOutcome).toBe('deny');
    expect(denyMessage).toBeTruthy();
    await loop.stop();
  });

  it('emits agent.requires_action (keyed by tool_use_id) before parking an always_ask tool', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_sig',
    }) as ClaudePersistentSdkHarness;

    const parked: string[] = [];
    let release: (() => void) | undefined;
    const startInput: SessionStartInput = {
      ...startInputFor('ses_sig'),
      confirmTool: (_n, input, opts) => {
        parked.push(opts.toolUseId);
        return new Promise((resolve) => {
          release = () => resolve({ behavior: 'allow', updatedInput: input });
        });
      },
    };
    await harness.start(startInput);

    const collected: AgentEvent[] = [];
    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        const n = await iter.next();
        if (n.done) return;
        collected.push(n.value);
      }
    })();
    const submitDone = harness.submit(userMessage('go'));
    await waitUntil(() => handles.length === 1);
    handles[0]?.scriptTurn([
      {
        __callTool: {
          name: 'Bash',
          input: { command: 'x' },
          toolUseId: 'toolu_sig',
          capture: () => {},
        },
      },
      resultFrame(),
    ]);

    await waitUntil(() => collected.some((e) => e.kind === 'agent.requires_action'));
    const sig = collected.find((e) => e.kind === 'agent.requires_action');
    expect(sig?.payload).toEqual({
      action: 'tool_confirmation',
      tool_use_id: 'toolu_sig',
      tool_name: 'Bash',
    });
    expect(parked).toEqual(['toolu_sig']);

    release?.();
    await submitDone;
    await harness.stop('replica.shutting_down');
    await pump;
  });

  it('AUTO-APPROVES an always_allow tool without parking or emitting requires_action', async () => {
    let parkCount = 0;
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_aa',
    }) as ClaudePersistentSdkHarness;
    let outcome: 'allow' | 'deny' | undefined;
    const startInput: SessionStartInput = {
      ...startInputFor('ses_aa'),
      confirmTool: async () => {
        parkCount += 1;
        return { behavior: 'allow', updatedInput: {} };
      },
      toolPermissions: { policyFor: (name) => (name === 'Read' ? 'always_allow' : 'always_ask') },
    };
    await harness.start(startInput);

    const events = await collectTurnScripted(harness, userMessage('go'), handles, () => [
      {
        __callTool: {
          name: 'Read',
          input: { path: '/x' },
          toolUseId: 'toolu_aa',
          capture: (o: 'allow' | 'deny') => {
            outcome = o;
          },
        },
      },
      resultFrame(),
    ]);

    expect(outcome).toBe('allow');
    expect(parkCount).toBe(0); // never parked — pre-authorized
    expect(events.some((e) => e.kind === 'agent.requires_action')).toBe(false);
  });
});

describe('claude-sdk-persistent — process-cleanup robustness', () => {
  it('stop() force-closes the live query (calls close())', async () => {
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_fc',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_fc'));

    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        const n = await iter.next();
        if (n.done) return;
      }
    })();
    const s1 = harness.submit(userMessage('go'));
    await waitUntil(() => handles.length === 1);
    handles[0]?.scriptTurn([resultFrame()]);
    await s1;

    await harness.stop('error');
    expect(handles[0]?.closeCalls).toBeGreaterThanOrEqual(1);
    await pump;
  });

  it('persists SDK-appended history through the transcript-store adapter (for the rebuild)', async () => {
    const store = new FakeTranscriptStore();
    const { query, handles } = fakePersistentQueryFactory();
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, { store, modelDefault: 'm', query });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_9',
      sessionId: 'ses_z',
    }) as ClaudePersistentSdkHarness;
    await harness.start(startInputFor('ses_z'));

    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        const n = await iter.next();
        if (n.done) return;
      }
    })();
    const s1 = harness.submit(userMessage('hi'));
    await waitUntil(() => handles.length === 1);
    // The live handle persists a user entry through the adapter (mirrors the SDK
    // persisting a turn) before completing.
    const adapter = handles[0]?.options['sessionStore'] as
      | { append?: (k: unknown, e: unknown[]) => Promise<void> }
      | undefined;
    await adapter?.append?.(
      { sessionId: (handles[0]?.options['sessionId'] as string) ?? '', projectKey: 'orca' },
      [{ type: 'user', uuid: 'u-1' }],
    );
    handles[0]?.scriptTurn([resultFrame()]);
    await s1;

    expect(store.appended.length).toBeGreaterThanOrEqual(1);
    expect(store.appended[0]?.workspaceId).toBe('ws_9');
    expect(store.appended[0]?.sessionId).toBe('ses_z');
    expect(store.appended[0]?.events[0]?.kind).toBe(CLAUDE_SESSION_ENTRY_EVENT_KIND);

    await harness.stop('replica.shutting_down');
    await pump;
  });
});

// ── helpers that depend on the fake's scripting protocol ────────────────────────

/**
 * Persist conversation entries through the live handle's session-store adapter — the
 * way the real SDK persists a turn via `sessionStore.append`. Used by the rebuild test
 * to seed the transcript with what turn 1 wrote, so the post-interrupt rebuild reads it.
 */
async function persistTurnEntries(
  handle: FakePersistentQuery | undefined,
  entries: Array<Record<string, unknown>>,
): Promise<void> {
  const adapter = handle?.options['sessionStore'] as
    | { append?: (k: unknown, e: unknown[]) => Promise<void> }
    | undefined;
  const sessionId = (handle?.options['sessionId'] as string) ?? '';
  await adapter?.append?.({ sessionId, projectKey: 'orca' }, entries);
}

/** The `shouldQuery` flag on a streamed prompt message (`false` ⇒ append-only priming). */
function promptShouldQuery(message: unknown): boolean | undefined {
  if (message === null || typeof message !== 'object') {
    return undefined;
  }
  const v = (message as { shouldQuery?: unknown }).shouldQuery;
  return typeof v === 'boolean' ? v : undefined;
}

/** The inner conversation role of a streamed prompt message (`user` / `assistant`). */
function promptRole(message: unknown): string | undefined {
  if (message === null || typeof message !== 'object') {
    return undefined;
  }
  const inner = (message as { message?: unknown }).message;
  if (inner === null || typeof inner !== 'object') {
    return undefined;
  }
  const role = (inner as { role?: unknown }).role;
  return typeof role === 'string' ? role : undefined;
}

/** The inner content block array of a streamed prompt message (empty when not an array). */
function promptBlocks(message: unknown): unknown[] {
  if (message === null || typeof message !== 'object') {
    return [];
  }
  const inner = (message as { message?: unknown }).message;
  if (inner === null || typeof inner !== 'object') {
    return [];
  }
  const content = (inner as { content?: unknown }).content;
  return Array.isArray(content) ? content : [];
}

/** Extract the user prompt text out of a streamed SDKUserMessage (or string). */
function userPromptText(message: unknown): string {
  if (typeof message === 'string') {
    return message;
  }
  if (message === null || typeof message !== 'object') {
    return '';
  }
  const inner = (message as { message?: unknown }).message;
  if (inner === null || typeof inner !== 'object') {
    // Some emitters carry `content` at the top level.
    const top = (message as { content?: unknown }).content;
    return contentToText(top);
  }
  return contentToText((inner as { content?: unknown }).content);
}

function contentToText(content: unknown): string {
  if (typeof content === 'string') {
    return content;
  }
  if (!Array.isArray(content)) {
    return '';
  }
  return content
    .filter(
      (b): b is { type: string; text: string } =>
        b !== null &&
        typeof b === 'object' &&
        (b as { type?: unknown }).type === 'text' &&
        typeof (b as { text?: unknown }).text === 'string',
    )
    .map((b) => b.text)
    .join('\n');
}

/** Submit a turn, script the (about-to-exist) live handle's messages, collect events. */
async function collectTurnScripted(
  harness: ClaudePersistentSdkHarness,
  user: { kind: string; payload: unknown },
  handles: FakePersistentQuery[],
  script: () => unknown[],
): Promise<AgentEvent[]> {
  const before = handles.length;
  const events: AgentEvent[] = [];
  const iter = harness.events()[Symbol.asyncIterator]();
  const pump = (async () => {
    for (;;) {
      let n: IteratorResult<AgentEvent>;
      try {
        n = await iter.next();
      } catch {
        return;
      }
      if (n.done) return;
      events.push(n.value);
    }
  })();
  const submitDone = harness.submit(user).catch(() => {});
  await waitUntil(() => handles.length > before);
  handles[handles.length - 1]?.scriptTurn(script());
  await submitDone;
  await new Promise<void>((r) => setImmediate(r));
  await harness.stop('replica.shutting_down');
  await pump;
  return events;
}
