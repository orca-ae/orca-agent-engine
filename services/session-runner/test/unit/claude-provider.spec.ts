// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the default `claude` provider — the real harness the runner drives.
//
// Proves the end-to-end machinery a `provider: "claude"` snapshot exercises without
// the network: registerClaudeProvider wires the `claude` key; the factory reads the
// gateway egress (LLM base URL + scoped JWT) off the snapshot and builds a real
// ClaudeAgentSdkHarness over a transcript-store adapter; the harness drives the
// (injected) SDK `query()` loop, persists history through the adapter, and maps SDK
// messages → agent events with the load-bearing "submit resolves only after the
// turn's events are emitted" contract. The SDK `query` is a scripted fake; the
// transcript store is an in-memory fake — both faithful peers, no real I/O.

import { describe, it, expect } from 'vitest';
import type {
  Event,
  ReadOptions,
  TailOptions,
  TranscriptStore,
} from '@orca/transcript-store-types';
import { ProviderRegistry, buildSessionStartInput } from '../../src/harness/provider.js';
import {
  registerClaudeProvider,
  readGatewayLlmEgress,
  CLAUDE_PROVIDER_NAME,
} from '../../src/harness/claude/provider.js';
import { ClaudeAgentSdkHarness, type ClaudeQuery } from '../../src/harness/claude/index.js';
import { SessionLoop } from '../../src/session-loop.js';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import {
  entryToEvent,
  eventToEntry,
  isClaudeSessionEntryEvent,
  CLAUDE_SESSION_ENTRY_EVENT_KIND,
} from '../../src/harness/claude/event-mapper.js';
import type { RunnerSnapshot } from '../../src/snapshot.js';
import type { AgentEvent } from '../../src/harness/agent-harness.js';

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

/** Build a scripted SDK `query` yielding the given messages in order. */
function scriptedQuery(messages: unknown[], spy?: (args: { prompt: string }) => void): ClaudeQuery {
  return ({ prompt }) => {
    spy?.({ prompt });
    return (async function* () {
      for (const m of messages) {
        yield m as never;
      }
    })();
  };
}

/**
 * A blocking SDK query handle: its `next()` awaits `block` then ends the stream. Models
 * an in-flight `query()` loop stalled on the model — an interrupt / stop / fault can
 * land while it is pending. A plain async-iterable (not a generator) so it can stall
 * without a meaningless `yield`. The optional `close` augments it like the real SDK
 * `Query` (force-terminate the subprocess); calling it resolves the block.
 */
function blockingQueryHandle(
  block: Promise<void>,
  withClose?: { onClose: () => void },
): AsyncIterable<never> & {
  close?: () => void;
} {
  const handle: AsyncIterable<never> & { close?: () => void } = {
    [Symbol.asyncIterator](): AsyncIterator<never> {
      return {
        async next(): Promise<IteratorResult<never>> {
          await block;
          return { value: undefined, done: true };
        },
      };
    },
  };
  if (withClose !== undefined) {
    handle.close = withClose.onClose;
  }
  return handle;
}

const SNAPSHOT: RunnerSnapshot = {
  model: { provider: 'anthropic', id: 'claude-sonnet-4' },
  provider: 'claude',
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

/** Drain a runner turn's NDJSON byte stream to completion (the bytes are ignored). */
async function drainStream(stream: AsyncIterable<Uint8Array>): Promise<void> {
  for await (const _chunk of stream) {
    void _chunk;
  }
}

/** Poll `cond` on each macrotask until true or the bounded budget elapses. */
async function waitUntil(cond: () => boolean, tries = 200): Promise<void> {
  for (let i = 0; i < tries; i += 1) {
    if (cond()) {
      return;
    }
    await new Promise<void>((r) => setTimeout(r, 2));
  }
  throw new Error('waitUntil: condition not met in time');
}

async function drainOneTurn(
  harness: ClaudeAgentSdkHarness,
  user: { kind: string; payload: unknown },
): Promise<AgentEvent[]> {
  const events: AgentEvent[] = [];
  const iter = harness.events()[Symbol.asyncIterator]();
  await harness.submit(user);
  // submit has resolved → all the turn's events are buffered; pull them without
  // blocking by racing each pull against a macrotask.
  for (;;) {
    const next = await Promise.race([
      iter.next(),
      new Promise<'idle'>((r) => setImmediate(() => r('idle'))),
    ]);
    if (next === 'idle') {
      break;
    }
    if (next.done) {
      break;
    }
    events.push(next.value);
  }
  await harness.stop('replica.shutting_down');
  return events;
}

describe('registerClaudeProvider', () => {
  it('registers the claude provider name on the registry', () => {
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, { store: new FakeTranscriptStore(), modelDefault: 'm' });
    expect(registry.has(CLAUDE_PROVIDER_NAME)).toBe(true);
    expect(registry.providerNames()).toEqual(['claude']);
  });

  it('builds a real ClaudeAgentSdkHarness for a provider:claude snapshot (no 422)', () => {
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query: scriptedQuery([]),
    });
    const harness = registry.build(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_1' });
    expect(harness).toBeInstanceOf(ClaudeAgentSdkHarness);
  });

  it('drives the SDK query loop and maps assistant/result messages to agent events', async () => {
    let seenPrompt: string | undefined;
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'claude-default',
      query: scriptedQuery(
        [
          { type: 'system', subtype: 'init', mcp_servers: [] },
          // The real SDK assistant message carries a structured content-block array
          // (BetaMessage.content); the provider maps each block to an event.
          {
            type: 'assistant',
            message: { role: 'assistant', content: [{ type: 'text', text: 'hi there' }] },
          },
          {
            type: 'result',
            usage: { input_tokens: 12, output_tokens: 7, cache_read_input_tokens: 2 },
          },
        ],
        (args) => {
          seenPrompt = args.prompt;
        },
      ),
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_abc',
    }) as ClaudeAgentSdkHarness;
    await harness.start(
      buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_abc' }),
    );

    const events = await drainOneTurn(harness, userMessage('hello'));

    expect(seenPrompt).toBe('hello');
    const kinds = events.map((e) => e.kind);
    expect(kinds).toContain('system');
    expect(kinds).toContain('agent.message');
    expect(kinds).toContain('agent.usage');
    const usageEvent = events.find((e) => e.kind === 'agent.usage');
    expect(usageEvent?.payload).toEqual({
      usage: {
        cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 0 },
        cache_read_input_tokens: 2,
        input_tokens: 12,
        output_tokens: 7,
      },
    });
  });

  it('passes the gateway LLM base URL + scoped JWT into the SDK options.env', async () => {
    let seenOptions: { env?: Record<string, string>; model?: string } | undefined;
    const query: ClaudeQuery = ({ options }) => {
      seenOptions = options as never;
      return (async function* () {
        /* no messages */
      })();
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'claude-default',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_1',
    }) as ClaudeAgentSdkHarness;
    await harness.start(
      buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_1' }),
    );
    await harness.submit(userMessage('hi'));
    await harness.stop('replica.shutting_down');

    expect(seenOptions?.env?.['ANTHROPIC_BASE_URL']).toBe('https://gw.example/llm');
    expect(seenOptions?.env?.['ANTHROPIC_API_KEY']).toBe('llm-jwt-123');
    expect(seenOptions?.model).toBe('claude-sonnet-4');
  });

  it('falls back to fallbackApiKey + modelDefault when the snapshot carries neither', async () => {
    let seenOptions: { env?: Record<string, string>; model?: string } | undefined;
    const query: ClaudeQuery = ({ options }) => {
      seenOptions = options as never;
      return (async function* () {})();
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'claude-default',
      fallbackApiKey: 'env-key',
      query,
    });
    const noEgress: RunnerSnapshot = {
      ...SNAPSHOT,
      model: { provider: 'anthropic', id: '' },
      egress: null,
    };
    const harness = registry.build(noEgress, {
      workspaceId: 'ws_1',
      sessionId: 'ses_1',
    }) as ClaudeAgentSdkHarness;
    await harness.start(
      buildSessionStartInput(noEgress, { workspaceId: 'ws_1', sessionId: 'ses_1' }),
    );
    await harness.submit(userMessage('hi'));
    await harness.stop('replica.shutting_down');

    expect(seenOptions?.env?.['ANTHROPIC_API_KEY']).toBe('env-key');
    expect(seenOptions?.env?.['ANTHROPIC_BASE_URL']).toBeUndefined();
    expect(seenOptions?.model).toBe('claude-default');
  });

  it('persists SDK-appended history through the transcript-store adapter', async () => {
    const store = new FakeTranscriptStore();
    // A query that writes an entry through the adapter (simulating the SDK persisting
    // a turn) before yielding its assistant message.
    const query: ClaudeQuery = ({ options }) => {
      const sdkStore = (options as { sessionStore?: ClaudeAgentSdkAdapter }).sessionStore;
      return (async function* () {
        await sdkStore?.append(
          { sessionId: (options as { sessionId: string }).sessionId, projectKey: 'orca' },
          [{ type: 'user', uuid: 'u-1' }],
        );
        yield { type: 'assistant', message: { role: 'assistant', content: 'ok' } } as never;
      })();
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, { store, modelDefault: 'm', query });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_9',
      sessionId: 'ses_z',
    }) as ClaudeAgentSdkHarness;
    await harness.start(
      buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_9', sessionId: 'ses_z' }),
    );
    await harness.submit(userMessage('hi'));
    await harness.stop('replica.shutting_down');

    // The adapter rewrote the SDK UUID key back to the orca session id + workspace.
    expect(store.appended.length).toBe(1);
    expect(store.appended[0]?.workspaceId).toBe('ws_9');
    expect(store.appended[0]?.sessionId).toBe('ses_z');
    expect(store.appended[0]?.events[0]?.kind).toBe(CLAUDE_SESSION_ENTRY_EVENT_KIND);
  });

  it('wires canUseTool to the runner confirmation gate when start carries confirmTool', async () => {
    let seenOptions: { canUseTool?: unknown; permissionMode?: unknown } | undefined;
    const query: ClaudeQuery = ({ options }) => {
      seenOptions = options as never;
      return (async function* () {})();
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_1',
    }) as ClaudeAgentSdkHarness;
    // A start input that carries the runner's confirmation gate (what SessionLoop binds).
    const parked: Array<{ toolUseId: string }> = [];
    let releaseVerdict: (v: {
      behavior: 'allow' | 'deny';
      updatedInput?: Record<string, unknown>;
      message?: string;
    }) => void = () => {};
    const startInput = {
      ...buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_1' }),
      confirmTool: async (
        _toolName: string,
        input: Record<string, unknown>,
        opts: { toolUseId: string },
      ) => {
        parked.push({ toolUseId: opts.toolUseId });
        return new Promise<
          | { behavior: 'allow'; updatedInput: Record<string, unknown> }
          | { behavior: 'deny'; message: string }
        >((resolve) => {
          releaseVerdict = (v) =>
            resolve(
              v.behavior === 'allow'
                ? { behavior: 'allow', updatedInput: v.updatedInput ?? input }
                : { behavior: 'deny', message: v.message ?? 'denied' },
            );
        });
      },
    };
    await harness.start(startInput);
    await harness.submit(userMessage('hi'));
    await harness.stop('replica.shutting_down');

    // The SDK options carried a canUseTool (gated) and NOT bypassPermissions.
    expect(typeof seenOptions?.canUseTool).toBe('function');
    expect(seenOptions?.permissionMode).toBeUndefined();

    // Invoking the wired canUseTool parks on the gate keyed by the SDK toolUseID and
    // maps the verdict back to the SDK PermissionResult.
    const canUseTool = seenOptions?.canUseTool as (
      name: string,
      input: Record<string, unknown>,
      options: { signal: AbortSignal; toolUseID: string },
    ) => Promise<{ behavior: string; message?: string; updatedInput?: Record<string, unknown> }>;
    const ac = new AbortController();
    const pending = canUseTool(
      'Bash',
      { command: 'ls' },
      { signal: ac.signal, toolUseID: 'toolu_99' },
    );
    await Promise.resolve();
    expect(parked).toEqual([{ toolUseId: 'toolu_99' }]);
    releaseVerdict({ behavior: 'allow' });
    const result = await pending;
    expect(result.behavior).toBe('allow');
    expect(result.updatedInput).toEqual({ command: 'ls' });
  });

  it('canUseTool denies immediately when the turn is already aborted (no park)', async () => {
    let seenOptions: { canUseTool?: unknown } | undefined;
    const query: ClaudeQuery = ({ options }) => {
      seenOptions = options as never;
      return (async function* () {})();
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_1',
    }) as ClaudeAgentSdkHarness;
    let parkCount = 0;
    const startInput = {
      ...buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_1' }),
      confirmTool: async () => {
        parkCount += 1;
        return { behavior: 'allow' as const, updatedInput: {} };
      },
    };
    await harness.start(startInput);
    await harness.submit(userMessage('hi'));
    await harness.stop('replica.shutting_down');

    const canUseTool = seenOptions?.canUseTool as (
      name: string,
      input: Record<string, unknown>,
      options: { signal: AbortSignal; toolUseID: string },
    ) => Promise<{ behavior: string; message?: string }>;
    const ac = new AbortController();
    ac.abort();
    const result = await canUseTool('Bash', {}, { signal: ac.signal, toolUseID: 'toolu_aborted' });
    expect(result.behavior).toBe('deny');
    expect(parkCount).toBe(0); // never parked — the abort short-circuited
  });

  it('FULL CHAIN: the SDK invokes canUseTool mid-turn → real SessionLoop gate parks → delivered verdict ALLOWS the tool', async () => {
    // The complete gated-tool seam through the REAL pieces, with ONLY the SDK query
    // mocked (the genuine model boundary): the real claude harness wires the real
    // makeCanUseTool onto canUseTool; the real SessionLoop binds its real
    // PendingApprovals gate as confirmTool; the scripted query CALLS canUseTool the
    // way the SDK does when the model wants a tool, parks, and only proceeds once the
    // verdict — delivered through loop.resolveToolConfirmation (the confirmation
    // route's effect) — lands. Proves the chain SDK.canUseTool → makeCanUseTool →
    // loop.confirmTool → park → delivered verdict → allow back to the model end to end.
    let toolOutcome: 'allow' | 'deny' | undefined;
    const query: ClaudeQuery = ({ options }) => {
      const canUseTool = options.canUseTool!;
      return (async function* () {
        // The SDK decides to use a tool mid-turn: invoke the gate with a stable id.
        const ac = new AbortController();
        const verdict = await canUseTool('Bash', { command: 'ls' }, {
          signal: ac.signal,
          toolUseID: 'toolu_full',
        } as never);
        toolOutcome = verdict.behavior === 'allow' ? 'allow' : 'deny';
        // Reflect the outcome as a result message so the turn produces an event.
        yield {
          type: 'result',
          subtype: 'success',
          usage: { input_tokens: 1, output_tokens: 1 },
        } as never;
      })();
    };
    const providers = new ProviderRegistry();
    registerClaudeProvider(providers, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const loop = new SessionLoop({ workspaceId: 'ws_full', providers });
    // The snapshot is delivered through the loop, which binds its real confirmTool gate.
    await loop.applySnapshot('ses_full', new TextEncoder().encode(`${JSON.stringify(SNAPSHOT)}\n`));

    // Drive the turn WITHOUT awaiting — it parks inside canUseTool on the loop gate.
    const turnStream = loop.runTurn(
      'ses_full',
      new TextEncoder().encode(
        JSON.stringify({ type: 'user.message', content: [{ type: 'text', text: 'go' }] }),
      ),
      new AbortController().signal,
    );
    const drained = drainStream(turnStream);

    // Wait until the harness is parked on the gate (the loop reports it pending).
    await waitUntil(() => loop.hasPendingApproval('toolu_full'));
    // Deliver the user's ALLOW verdict the way the confirmation route does.
    expect(loop.resolveToolConfirmation('toolu_full', true)).toBe(true);

    await drained;
    expect(toolOutcome).toBe('allow');
    expect(loop.hasPendingApproval()).toBe(false);
    await loop.stop();
  });

  it('FULL CHAIN: a delivered DENY verdict returns a clean denial to the model mid-turn', async () => {
    let toolOutcome: 'allow' | 'deny' | undefined;
    let denyMessage: string | undefined;
    const query: ClaudeQuery = ({ options }) => {
      const canUseTool = options.canUseTool!;
      return (async function* () {
        const ac = new AbortController();
        const verdict = await canUseTool('Bash', { command: 'rm -rf /' }, {
          signal: ac.signal,
          toolUseID: 'toolu_full_deny',
        } as never);
        toolOutcome = verdict.behavior === 'allow' ? 'allow' : 'deny';
        if (verdict.behavior === 'deny') {
          denyMessage = (verdict as { message?: string }).message;
        }
        yield {
          type: 'result',
          subtype: 'success',
          usage: { input_tokens: 1, output_tokens: 1 },
        } as never;
      })();
    };
    const providers = new ProviderRegistry();
    registerClaudeProvider(providers, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const loop = new SessionLoop({ workspaceId: 'ws_full', providers });
    await loop.applySnapshot(
      'ses_full_deny',
      new TextEncoder().encode(`${JSON.stringify(SNAPSHOT)}\n`),
    );

    const drained = drainStream(
      loop.runTurn(
        'ses_full_deny',
        new TextEncoder().encode(
          JSON.stringify({ type: 'user.message', content: [{ type: 'text', text: 'go' }] }),
        ),
        new AbortController().signal,
      ),
    );
    await waitUntil(() => loop.hasPendingApproval('toolu_full_deny'));
    expect(loop.resolveToolConfirmation('toolu_full_deny', false)).toBe(true);

    await drained;
    expect(toolOutcome).toBe('deny');
    expect(denyMessage).toBeTruthy(); // a human-readable denial reason reached the model
    await loop.stop();
  });

  it('ignores non-user.message turns (chat-only) and emits no events', async () => {
    let called = false;
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query: scriptedQuery([], () => {
        called = true;
      }),
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_1',
    }) as ClaudeAgentSdkHarness;
    await harness.start(
      buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_1' }),
    );
    const events = await drainOneTurn(harness, { kind: 'user.tool_result', payload: {} });
    expect(called).toBe(false);
    expect(events).toEqual([]);
  });

  it('requests partial messages from the SDK so thinking can stream live', async () => {
    let seenOptions: { includePartialMessages?: unknown } | undefined;
    const query: ClaudeQuery = ({ options }) => {
      seenOptions = options as never;
      return (async function* () {})();
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_1',
    }) as ClaudeAgentSdkHarness;
    await harness.start(
      buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_1' }),
    );
    await harness.submit(userMessage('hi'));
    await harness.stop('replica.shutting_down');
    expect(seenOptions?.includePartialMessages).toBe(true);
  });
});

describe('claude provider — thinking + tool pairing through a turn', () => {
  it('streams thinking_delta as partial agent.message thinking blocks, then the settled assistant message', async () => {
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query: scriptedQuery([
        { type: 'system', subtype: 'init', mcp_servers: [] },
        // The SDK streams the extended-thinking deltas first…
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
            delta: { type: 'thinking_delta', thinking: 'check the files.' },
          },
        },
        // …then the settled assistant message carries the whole thinking block + text.
        {
          type: 'assistant',
          message: {
            role: 'assistant',
            content: [
              { type: 'thinking', thinking: 'I should check the files.', signature: 'sig' },
              { type: 'text', text: 'Let me look.' },
            ],
          },
        },
        { type: 'result', subtype: 'success', usage: { input_tokens: 5, output_tokens: 9 } },
      ]),
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_t',
    }) as ClaudeAgentSdkHarness;
    await harness.start(
      buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_t' }),
    );

    const events = await drainOneTurn(harness, userMessage('hello'));

    const messages = events.filter((e) => e.kind === 'agent.message');
    // Two partial thinking deltas + the settled thinking block + the settled text = 4.
    const partials = messages.filter((e) => (e.payload as { partial?: boolean }).partial === true);
    expect(partials).toHaveLength(2);
    expect((partials[0]?.payload as { content: unknown[] }).content).toEqual([
      { type: 'thinking', thinking: 'I should ' },
    ]);
    // The settled (non-partial) messages carry the thinking block then the text block.
    const settled = messages.filter((e) => (e.payload as { partial?: boolean }).partial !== true);
    expect(
      settled.map((e) => (e.payload as { content: Array<{ type: string }> }).content[0]?.type),
    ).toEqual(['thinking', 'text']);
  });

  it('emits paired agent.tool_use / agent.tool_result events keyed by the SDK tool_use_id', async () => {
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query: scriptedQuery([
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
        { type: 'result', subtype: 'success', usage: { input_tokens: 1, output_tokens: 1 } },
      ]),
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_p',
    }) as ClaudeAgentSdkHarness;
    await harness.start(
      buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_p' }),
    );

    const events = await drainOneTurn(harness, userMessage('go'));

    const use = events.find((e) => e.kind === 'agent.tool_use');
    const result = events.find((e) => e.kind === 'agent.tool_result');
    expect(use?.id).toBe('toolu_42');
    expect(use?.payload).toEqual({
      name: 'Bash',
      input: { command: 'ls' },
      tool_use_id: 'toolu_42',
    });
    expect(result?.id).toBe('toolu_42'); // paired by call_id
    expect(result?.payload).toEqual({ tool_use_id: 'toolu_42', content: 'a\nb', is_error: false });
  });
});

describe('claude provider — requires_action signal on a gated tool', () => {
  it('emits an agent.requires_action signal (keyed by tool_use_id) when a tool parks on the gate', async () => {
    // The scripted query invokes canUseTool the way the SDK does when the model wants a
    // tool; the harness must surface the tool-confirmation-required signal as an
    // Anthropic-native `agent.requires_action` event BEFORE it parks, so a client knows
    // to deliver a verdict.
    let releaseVerdict: (() => void) | undefined;
    const query: ClaudeQuery = ({ options }) => {
      const canUseTool = options.canUseTool!;
      return (async function* () {
        const ac = new AbortController();
        await canUseTool('Bash', { command: 'rm x' }, {
          signal: ac.signal,
          toolUseID: 'toolu_sig',
        } as never);
        yield {
          type: 'result',
          subtype: 'success',
          usage: { input_tokens: 1, output_tokens: 1 },
        } as never;
      })();
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_sig',
    }) as ClaudeAgentSdkHarness;

    const parked: string[] = [];
    const startInput = {
      ...buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_sig' }),
      confirmTool: (_n: string, input: Record<string, unknown>, opts: { toolUseId: string }) => {
        parked.push(opts.toolUseId);
        return new Promise<{ behavior: 'allow'; updatedInput: Record<string, unknown> }>(
          (resolve) => {
            releaseVerdict = () => resolve({ behavior: 'allow', updatedInput: input });
          },
        );
      },
    };
    await harness.start(startInput);

    // Collect events live; the turn will block on the parked gate.
    const collected: AgentEvent[] = [];
    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        const next = await iter.next();
        if (next.done) {
          return;
        }
        collected.push(next.value);
      }
    })();
    const submitDone = harness.submit(userMessage('go'));

    // The signal is emitted as soon as the gate parks (before the verdict).
    await waitUntil(() => collected.some((e) => e.kind === 'agent.requires_action'));
    const sig = collected.find((e) => e.kind === 'agent.requires_action');
    expect(sig?.payload).toEqual({
      action: 'tool_confirmation',
      tool_use_id: 'toolu_sig',
      tool_name: 'Bash',
    });
    expect(parked).toEqual(['toolu_sig']);

    // Release the verdict so the turn finishes and the pump drains.
    releaseVerdict?.();
    await submitDone;
    await harness.stop('replica.shutting_down');
    await pump;
  });
});

describe('claude provider — per-tool permission policy (always_ask vs always_allow)', () => {
  /** A resolver mapping fixed tool names to policies, with a fail-closed default. */
  function resolver(map: Record<string, 'always_ask' | 'always_allow'>) {
    return {
      policyFor: (name: string): 'always_ask' | 'always_allow' => map[name] ?? 'always_ask',
    };
  }

  it.each(['always_allow', 'always_deny'] as const)(
    'enforces %s without parking or emitting requires_action',
    async (policy) => {
      let toolBehavior: string | undefined;
      const parked: string[] = [];
      const query: ClaudeQuery = ({ options }) => {
        const canUseTool = options.canUseTool!;
        return (async function* () {
          const ac = new AbortController();
          const verdict = await canUseTool('Read', { path: '/x' }, {
            signal: ac.signal,
            toolUseID: 'toolu_allow_auto',
          } as never);
          toolBehavior = verdict.behavior;
          yield {
            type: 'result',
            subtype: 'success',
            usage: { input_tokens: 1, output_tokens: 1 },
          } as never;
        })();
      };
      const registry = new ProviderRegistry();
      registerClaudeProvider(registry, {
        store: new FakeTranscriptStore(),
        modelDefault: 'm',
        query,
      });
      const harness = registry.build(SNAPSHOT, {
        workspaceId: 'ws_1',
        sessionId: 'ses_aa',
      }) as ClaudeAgentSdkHarness;
      const startInput = {
        ...buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_aa' }),
        // The gate is wired (it would park), but Read's policy is always_allow.
        confirmTool: async (
          _n: string,
          _i: Record<string, unknown>,
          opts: { toolUseId: string },
        ) => {
          parked.push(opts.toolUseId);
          return { behavior: 'allow' as const, updatedInput: {} };
        },
        toolPermissions: { policyFor: () => policy },
      };
      await harness.start(startInput);

      const events = await drainOneTurn(harness, userMessage('go'));

      // The tool proceeded WITHOUT ever calling the confirmation gate…
      expect(toolBehavior).toBe(policy === 'always_allow' ? 'allow' : 'deny');
      expect(parked).toEqual([]);
      // …and WITHOUT emitting an agent.requires_action signal (no human verdict awaited).
      expect(events.some((e) => e.kind === 'agent.requires_action')).toBe(false);
    },
  );

  it('PARKS an always_ask tool on the gate (emits requires_action) even with a resolver present', async () => {
    const parked: string[] = [];
    let releaseVerdict: (() => void) | undefined;
    const query: ClaudeQuery = ({ options }) => {
      const canUseTool = options.canUseTool!;
      return (async function* () {
        const ac = new AbortController();
        await canUseTool('Bash', { command: 'ls' }, {
          signal: ac.signal,
          toolUseID: 'toolu_ask',
        } as never);
        yield {
          type: 'result',
          subtype: 'success',
          usage: { input_tokens: 1, output_tokens: 1 },
        } as never;
      })();
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_ak',
    }) as ClaudeAgentSdkHarness;
    const collected: AgentEvent[] = [];
    const startInput = {
      ...buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_ak' }),
      confirmTool: (_n: string, input: Record<string, unknown>, opts: { toolUseId: string }) => {
        parked.push(opts.toolUseId);
        return new Promise<{ behavior: 'allow'; updatedInput: Record<string, unknown> }>(
          (resolve) => {
            releaseVerdict = () => resolve({ behavior: 'allow', updatedInput: input });
          },
        );
      },
      // Bash → always_ask; Read (unused here) → always_allow.
      toolPermissions: resolver({ Bash: 'always_ask', Read: 'always_allow' }),
    };
    await harness.start(startInput);
    const iter = harness.events()[Symbol.asyncIterator]();
    const pump = (async () => {
      for (;;) {
        const next = await iter.next();
        if (next.done) return;
        collected.push(next.value);
      }
    })();
    const submitDone = harness.submit(userMessage('go'));
    await waitUntil(() => parked.length > 0);
    // The always_ask tool parked AND surfaced the requires_action signal.
    expect(parked).toEqual(['toolu_ask']);
    await waitUntil(() => collected.some((e) => e.kind === 'agent.requires_action'));
    releaseVerdict?.();
    await submitDone;
    await harness.stop('replica.shutting_down');
    await pump;
  });

  it('FAILS CLOSED with no resolver: every gated tool parks (existing behavior)', async () => {
    const parked: string[] = [];
    let releaseVerdict: (() => void) | undefined;
    const query: ClaudeQuery = ({ options }) => {
      const canUseTool = options.canUseTool!;
      return (async function* () {
        const ac = new AbortController();
        await canUseTool('Read', { path: '/x' }, {
          signal: ac.signal,
          toolUseID: 'toolu_fc',
        } as never);
        yield {
          type: 'result',
          subtype: 'success',
          usage: { input_tokens: 1, output_tokens: 1 },
        } as never;
      })();
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_fc',
    }) as ClaudeAgentSdkHarness;
    const startInput = {
      ...buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_fc' }),
      confirmTool: (_n: string, input: Record<string, unknown>, opts: { toolUseId: string }) => {
        parked.push(opts.toolUseId);
        return new Promise<{ behavior: 'allow'; updatedInput: Record<string, unknown> }>(
          (resolve) => {
            releaseVerdict = () => resolve({ behavior: 'allow', updatedInput: input });
          },
        );
      },
      // No toolPermissions: even a normally-innocuous Read must park (fail-closed).
    };
    await harness.start(startInput);
    const pump = (async () => {
      const iter = harness.events()[Symbol.asyncIterator]();
      for (;;) {
        const next = await iter.next();
        if (next.done) return;
      }
    })();
    const submitDone = harness.submit(userMessage('go'));
    await waitUntil(() => parked.length > 0);
    expect(parked).toEqual(['toolu_fc']); // parked despite Read being innocuous — fail-closed
    releaseVerdict?.();
    await submitDone;
    await harness.stop('replica.shutting_down');
    await pump;
  });
});

describe('claude provider — user.interrupt aborts the in-flight turn', () => {
  it('a user.interrupt submitted during a turn aborts the SDK abort signal', async () => {
    let capturedSignal: AbortSignal | undefined;
    let releaseTurn: (() => void) | undefined;
    // Block until released (simulating an in-flight turn) so an interrupt can land
    // mid-flight. The harness's abort fires the signal we captured.
    const block = new Promise<void>((resolve) => {
      releaseTurn = resolve;
    });
    const query: ClaudeQuery = ({ options }) => {
      capturedSignal = options.abortController?.signal;
      return blockingQueryHandle(block) as never;
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_int',
    }) as ClaudeAgentSdkHarness;
    await harness.start(
      buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_int' }),
    );

    // Start the message turn (does not resolve — the query is blocked).
    const turn = harness.submit(userMessage('long running'));
    await waitUntil(() => capturedSignal !== undefined);
    expect(capturedSignal?.aborted).toBe(false);

    // Submit a user.interrupt — it must abort the in-flight turn's signal.
    await harness.submit({ kind: 'user.interrupt', payload: { type: 'user.interrupt' } });
    expect(capturedSignal?.aborted).toBe(true);

    // Let the (now-aborted) turn finish so the harness settles.
    releaseTurn?.();
    await turn;
    await harness.stop('replica.shutting_down');
  });

  it('interrupt() force-closes the blocked query, keeps the harness ALIVE, and the next turn drives', async () => {
    // The out-of-band interrupt path (the runner interrupt route's effect): a turn is
    // BLOCKED on the model; interrupt() must abort + force-close it so submit resolves,
    // WITHOUT terminating the harness — the next turn must drive a fresh query().
    let turnIndex = 0;
    let closedFirst = false;
    let releaseFirst: (() => void) | undefined;
    const block = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const query: ClaudeQuery = () => {
      turnIndex += 1;
      if (turnIndex === 1) {
        // First turn blocks until force-closed (close() resolves the block).
        return blockingQueryHandle(block, {
          onClose: () => {
            closedFirst = true;
            releaseFirst?.();
          },
        }) as never;
      }
      // Second turn: a normal completed turn, proving the harness survived the interrupt.
      return (async function* () {
        yield {
          type: 'result',
          subtype: 'success',
          usage: { input_tokens: 1, output_tokens: 1 },
        } as never;
      })();
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_ialive',
    }) as ClaudeAgentSdkHarness;
    await harness.start(
      buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_ialive' }),
    );

    // First turn blocks; interrupt() unwinds it (force-close) without terminating.
    const firstTurn = harness.submit(userMessage('block me'));
    await waitUntil(() => turnIndex === 1);
    harness.interrupt();
    await firstTurn; // resolves because the force-close unblocked the query
    expect(closedFirst).toBe(true);

    // The harness is STILL ALIVE: a second turn drives a fresh query and completes.
    const events = await drainOneTurn(harness, userMessage('next'));
    expect(turnIndex).toBe(2);
    expect(events.some((e) => e.kind === 'agent.usage')).toBe(true);
  });

  it('interrupt() is a no-op after the harness is stopped (terminated)', async () => {
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query: scriptedQuery([]),
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_inoop',
    }) as ClaudeAgentSdkHarness;
    await harness.start(
      buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_inoop' }),
    );
    await harness.stop('replica.shutting_down');
    // No turn in flight + terminated: interrupt must not throw and must stay a no-op.
    expect(() => harness.interrupt()).not.toThrow();
  });
});

describe('claude provider — process-cleanup robustness', () => {
  it('stop() force-closes the SDK query (calls close()) and aborts the turn', async () => {
    let closed = false;
    let aborted = false;
    let releaseTurn: (() => void) | undefined;
    const block = new Promise<void>((resolve) => {
      releaseTurn = resolve;
    });
    // A query handle shaped like the real SDK Query: a stalled stream WITH a close()
    // that force-terminates the subprocess (here: records the close + unblocks).
    const query: ClaudeQuery = ({ options }) => {
      options.abortController?.signal.addEventListener('abort', () => {
        aborted = true;
      });
      return blockingQueryHandle(block, {
        onClose: () => {
          closed = true;
          releaseTurn?.();
        },
      }) as never;
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_fc',
    }) as ClaudeAgentSdkHarness;
    await harness.start(
      buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_fc' }),
    );

    const turn = harness.submit(userMessage('go'));
    await waitUntil(() => releaseTurn !== undefined);

    await harness.stop('error');
    expect(aborted).toBe(true); // the turn's abort controller fired
    expect(closed).toBe(true); // the SDK query was force-closed (subprocess terminated)
    await turn;
  });

  it('an SDK query that throws mid-turn emits a terminal agent.error, then ends the stream', async () => {
    // A query whose stream faults (the subprocess died). The harness must clean up:
    // end the event stream (so a consumer is not stranded) and abort the in-flight
    // controller — and, BEFORE ending it, emit the fault as an `agent.error` so the
    // reason reaches the wire (and the durable transcript). Rethrowing alone is not
    // enough: the runner's submit handler is a stderr log, so a silent fault would reach
    // the client as a successful EMPTY turn. A plain async-iterable whose next() rejects
    // models the fault.
    let aborted = false;
    const query: ClaudeQuery = ({ options }) => {
      options.abortController?.signal.addEventListener('abort', () => {
        aborted = true;
      });
      const faulting: AsyncIterable<never> = {
        [Symbol.asyncIterator]: () => ({
          next: () => Promise.reject(new Error('sdk subprocess died')),
        }),
      };
      return faulting as never;
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const harness = registry.build(SNAPSHOT, {
      workspaceId: 'ws_1',
      sessionId: 'ses_err',
    }) as ClaudeAgentSdkHarness;
    await harness.start(
      buildSessionStartInput(SNAPSHOT, { workspaceId: 'ws_1', sessionId: 'ses_err' }),
    );

    // The events stream must carry the fault, then terminate (not hang).
    const iter = harness.events()[Symbol.asyncIterator]();
    await expect(harness.submit(userMessage('go'))).rejects.toThrow('sdk subprocess died');
    expect(aborted).toBe(true);
    const errored = await Promise.race([
      iter.next(),
      new Promise<'hang'>((r) => setTimeout(() => r('hang'), 50)),
    ]);
    expect(errored).not.toBe('hang');
    const errorEvent = (errored as IteratorResult<AgentEvent>).value;
    expect(errorEvent?.kind).toBe('agent.error');
    expect(errorEvent?.payload).toEqual({ message: 'Claude SDK error: sdk subprocess died' });
    const next = await Promise.race([
      iter.next(),
      new Promise<'hang'>((r) => setTimeout(() => r('hang'), 50)),
    ]);
    expect(next).not.toBe('hang');
    expect((next as IteratorResult<AgentEvent>).done).toBe(true);
  });
});

describe('readGatewayLlmEgress', () => {
  it('reads the gateway LLM base URL + JWT from a gateway snapshot', () => {
    expect(readGatewayLlmEgress(SNAPSHOT)).toEqual({
      baseURL: 'https://gw.example/llm',
      apiKey: 'llm-jwt-123',
    });
  });

  it('returns empty for a non-gateway / malformed / absent egress', () => {
    expect(readGatewayLlmEgress({ ...SNAPSHOT, egress: null })).toEqual({});
    expect(readGatewayLlmEgress({ ...SNAPSHOT, egress: { mode: 'sidecar' } })).toEqual({});
    expect(readGatewayLlmEgress({ ...SNAPSHOT, egress: { mode: 'gateway' } })).toEqual({});
  });

  it('omits fields the gateway block does not carry (MCP-only gateway)', () => {
    const mcpOnly: RunnerSnapshot = {
      ...SNAPSHOT,
      egress: {
        mode: 'gateway',
        gateway: { mcp_base_url: 'x', session_jwt: 'y', mcp_servers: {} },
      },
    };
    expect(readGatewayLlmEgress(mcpOnly)).toEqual({});
  });
});

describe('claude event-mapper', () => {
  it('round-trips an SDK entry through entryToEvent → eventToEntry', () => {
    const event = entryToEvent({
      workspaceId: 'ws',
      sessionId: 'ses',
      subpath: '',
      producedBy: 'harness',
      entry: { type: 'assistant', uuid: 'e-1', text: 'hi' },
    });
    expect(event.id).toBe('e-1'); // entry uuid becomes the event id (store dedup key)
    expect(event.kind).toBe(CLAUDE_SESSION_ENTRY_EVENT_KIND);
    expect(isClaudeSessionEntryEvent(event)).toBe(true);
    expect(eventToEntry(event)).toEqual({ type: 'assistant', uuid: 'e-1', text: 'hi' });
  });
});
