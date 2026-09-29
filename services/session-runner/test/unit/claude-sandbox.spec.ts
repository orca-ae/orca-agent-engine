// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Sandbox wiring for the claude providers — the real built-in tools.
//
// The runner's claude providers were LLM-only (no bash/read/write). This spec pins
// the fix: when the runner hands a per-session `SandboxHandle` to a claude provider,
// the provider builds the in-process `orca` MCP tool server bound to that sandbox,
// merges it with the snapshot's gateway MCP servers, and anchors the SDK `cwd` at the
// sandbox root — so both the `orca` tools AND the SDK's own built-ins land in the
// sandbox. It also proves the loop ACQUIRES + THREADS + DESTROYS the handle per
// session for the real providers, and that the mock provider gets NO sandbox.
//
// The SDK `query()` is a scripted fake (no network); the sandbox is a real
// `InMemorySandboxRuntime` from `@orca/sandbox-runtime`, so a tool call genuinely
// executes in the per-session tmpdir.

import { describe, it, expect } from 'vitest';
import { FakeAgentHarness } from './support/fake-agent-harness.js';
import type {
  Event,
  ReadOptions,
  TailOptions,
  TranscriptStore,
} from '@orca/transcript-store-types';
import {
  InMemorySandboxRuntime,
  type EnvironmentSpec,
  type SandboxCapabilities,
} from '@orca/sandbox-runtime';
import { ProviderRegistry, buildSessionStartInput } from '../../src/harness/provider.js';
import { registerClaudeProvider } from '../../src/harness/claude/provider.js';
import { registerClaudePersistentProvider } from '../../src/harness/claude/persistent-provider.js';
import { registerMockProvider } from '../../src/harness/mock/provider.js';
import { ClaudeAgentSdkHarness, type ClaudeQuery } from '../../src/harness/claude/index.js';
import {
  ClaudePersistentSdkHarness,
  type PersistentClaudeQuery,
  type PersistentQueryHandle,
} from '../../src/harness/claude/persistent.js';
import { buildOrcaSdkTools, ORCA_MCP_SERVER_NAME } from '../../src/harness/claude/mcp-tools.js';
import { SessionLoop } from '../../src/session-loop.js';
import type { SandboxHandle, SandboxRuntime } from '../../src/sandbox/seam.js';
import type { RunnerSnapshot } from '../../src/snapshot.js';
import type { SessionStartInput } from '../../src/harness/agent-harness.js';

/** A minimal in-memory TranscriptStore: append accumulates, read replays in order. */
class FakeTranscriptStore implements TranscriptStore {
  private readonly byKey = new Map<string, Event[]>();
  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    const key = `${workspaceId}/${sessionId}`;
    const bucket = this.byKey.get(key) ?? [];
    for (const e of events) bucket.push(e);
    this.byKey.set(key, bucket);
    return events.map((e) => e.id);
  }
  async *read(workspaceId: string, sessionId: string, _opts: ReadOptions): AsyncIterable<Event> {
    for (const e of this.byKey.get(`${workspaceId}/${sessionId}`) ?? []) yield e;
  }
  async *tail(_w: string, _s: string, _o: TailOptions): AsyncIterable<Event> {}
  async archive(): Promise<void> {}
  async close(): Promise<void> {}
}

/**
 * A recording runtime that wraps `InMemorySandboxRuntime` and captures every handle it
 * hands out (so a test can assert the loop acquired + destroyed the per-session sandbox
 * AND that the provider bound that exact handle). `spawn`/`runPrivileged`/… pass through.
 */
class RecordingSandboxRuntime implements SandboxRuntime {
  readonly acquired: SandboxHandle[] = [];
  readonly destroyed: SandboxHandle[] = [];
  private readonly inner = new InMemorySandboxRuntime();
  get capabilities(): SandboxCapabilities {
    return this.inner.capabilities;
  }
  async acquire(env: EnvironmentSpec): Promise<SandboxHandle> {
    const handle = await this.inner.acquire(env);
    // Capture the destroyed-log directly (no `this`-alias) so the destroy trap records
    // the release without closing over the instance.
    const destroyedLog = this.destroyed;
    const wrapped: SandboxHandle = new Proxy(handle, {
      get(target, prop, receiver) {
        if (prop === 'destroy') {
          return async (): Promise<void> => {
            destroyedLog.push(wrapped);
            await (target as SandboxHandle).destroy();
          };
        }
        return Reflect.get(target, prop, receiver);
      },
    }) as SandboxHandle;
    this.acquired.push(wrapped);
    return wrapped;
  }
}

const GATEWAY_SNAPSHOT: RunnerSnapshot = {
  model: { provider: 'anthropic', id: 'claude-sonnet-4' },
  provider: 'claude',
  system: 'be helpful',
  allowed_tool_names: [],
  allowed_mcp_server_names: ['github'],
  tool_permissions: {},
  egress: {
    mode: 'gateway',
    gateway: {
      mcp_base_url: 'https://gw.example/mcp',
      session_jwt: 'mcp-jwt',
      llm_base_url: 'https://gw.example/llm',
      llm_jwt: 'llm-jwt-123',
      mcp_servers: {
        github: {
          type: 'http',
          url: 'https://gw.example/mcp',
          headers: { Authorization: 'Bearer mcp-jwt' },
        },
      },
    },
  },
};

/** The same gateway snapshot but selecting the persistent provider (provider B). */
const PERSISTENT_SNAPSHOT: RunnerSnapshot = {
  ...GATEWAY_SNAPSHOT,
  provider: 'claude-sdk-persistent',
};

function userMessage(text: string): { kind: string; payload: unknown } {
  return { kind: 'user.message', payload: { content: [{ type: 'text', text }] } };
}

/** Drain a runner turn's NDJSON byte stream, returning the parsed lines. */
async function drainLines(
  stream: AsyncIterable<Uint8Array>,
): Promise<Array<Record<string, unknown>>> {
  const chunks: Uint8Array[] = [];
  for await (const c of stream) chunks.push(c);
  return Buffer.concat(chunks.map((c) => Buffer.from(c)))
    .toString('utf8')
    .split('\n')
    .filter((l) => l.trim().length > 0)
    .map((l) => JSON.parse(l) as Record<string, unknown>);
}

// ── the lean `claude` provider (A) ───────────────────────────────────────────────

describe('lean claude provider — sandbox tools + cwd', () => {
  it('binds the orca MCP server to the sandbox and sets cwd to the sandbox root', async () => {
    let seen: { cwd?: string; mcpServers?: Record<string, unknown> } | undefined;
    const query: ClaudeQuery = ({ options }) => {
      seen = options as never;
      return (async function* () {})();
    };
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    try {
      const registry = new ProviderRegistry();
      registerClaudeProvider(registry, {
        store: new FakeTranscriptStore(),
        modelDefault: 'm',
        query,
      });
      const ctx = { workspaceId: 'ws_1', sessionId: 'ses_1', sandbox };
      const harness = registry.build(GATEWAY_SNAPSHOT, ctx) as ClaudeAgentSdkHarness;
      await harness.start(buildSessionStartInput(GATEWAY_SNAPSHOT, ctx));
      await harness.submit(userMessage('hi'));
      await harness.stop('replica.shutting_down');

      // The orca sandbox tool server is merged in alongside the gateway server…
      const servers = seen?.mcpServers ?? {};
      expect(Object.keys(servers).sort()).toEqual([ORCA_MCP_SERVER_NAME, 'github'].sort());
      expect((servers[ORCA_MCP_SERVER_NAME] as { type?: string }).type).toBe('sdk');
      // …and the SDK cwd anchors at the per-session sandbox root.
      expect(seen?.cwd).toBe((sandbox as unknown as { rootDir(): string }).rootDir());
    } finally {
      await sandbox.destroy();
    }
  });

  it('a tool call bound to the sandbox executes INSIDE the sandbox (InMemory)', async () => {
    // The provider binds THIS exact handle into options.mcpServers.orca. Running a tool
    // built from the same handle proves the bound surface dispatches into the sandbox:
    // `pwd` returns the sandbox root and a written file lands on the sandbox FS.
    let boundServer: { name?: string } | undefined;
    const query: ClaudeQuery = ({ options }) => {
      boundServer = (options.mcpServers as Record<string, { name?: string }>)[ORCA_MCP_SERVER_NAME];
      return (async function* () {})();
    };
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    try {
      const registry = new ProviderRegistry();
      registerClaudeProvider(registry, {
        store: new FakeTranscriptStore(),
        modelDefault: 'm',
        query,
      });
      const ctx = { workspaceId: 'ws_1', sessionId: 'ses_tool', sandbox };
      const harness = registry.build(GATEWAY_SNAPSHOT, ctx) as ClaudeAgentSdkHarness;
      await harness.start(buildSessionStartInput(GATEWAY_SNAPSHOT, ctx));
      await harness.submit(userMessage('go'));
      await harness.stop('replica.shutting_down');

      expect(boundServer?.name).toBe(ORCA_MCP_SERVER_NAME);
      // Drive a tool from the SAME sandbox handle the provider bound.
      const tools = buildOrcaSdkTools(sandbox);
      const bash = tools.find((t) => t.name === 'bash')!;
      const pwd = await bash.handler({ command: 'pwd' }, {});
      const root = (sandbox as unknown as { rootDir(): string }).rootDir();
      expect((pwd as { content: Array<{ text: string }> }).content[0]!.text).toContain(root);

      await tools
        .find((t) => t.name === 'write')!
        .handler({ path: '/in-sbx.txt', content: 'yes' }, {});
      expect((await sandbox.files.read('/in-sbx.txt')).toString('utf8')).toBe('yes');
    } finally {
      await sandbox.destroy();
    }
  });

  it('does NOT set cwd or an orca server when no sandbox is wired (chat-only)', async () => {
    let seen: { cwd?: string; mcpServers?: Record<string, unknown> } | undefined;
    const query: ClaudeQuery = ({ options }) => {
      seen = options as never;
      return (async function* () {})();
    };
    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    // No `sandbox` in the ctx → the provider stays LLM-only (existing behavior).
    const ctx = { workspaceId: 'ws_1', sessionId: 'ses_ns' };
    const harness = registry.build(GATEWAY_SNAPSHOT, ctx) as ClaudeAgentSdkHarness;
    await harness.start(buildSessionStartInput(GATEWAY_SNAPSHOT, ctx));
    await harness.submit(userMessage('hi'));
    await harness.stop('replica.shutting_down');

    expect(seen?.cwd).toBeUndefined();
    // Only the gateway server remains — no orca server without a sandbox.
    expect(Object.keys(seen?.mcpServers ?? {})).toEqual(['github']);
  });

  it('binds the ctx sandbox even when the start input omits it (ctx is a sufficient source)', async () => {
    // The coupling this pins: the provider factory captures `ctx.sandbox`, so a caller
    // that builds the harness with a sandboxed context but hands `start` a
    // SessionStartInput WITHOUT `.sandbox` STILL gets sandbox-bound tools — the sandbox
    // binding does not depend on `buildSessionStartInput` being the sole start-input builder.
    let seen: { cwd?: string; mcpServers?: Record<string, unknown> } | undefined;
    const query: ClaudeQuery = ({ options }) => {
      seen = options as never;
      return (async function* () {})();
    };
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    try {
      const registry = new ProviderRegistry();
      registerClaudeProvider(registry, {
        store: new FakeTranscriptStore(),
        modelDefault: 'm',
        query,
      });
      const ctx = { workspaceId: 'ws_1', sessionId: 'ses_ctx', sandbox };
      const harness = registry.build(GATEWAY_SNAPSHOT, ctx) as ClaudeAgentSdkHarness;
      // A HAND-BUILT start input that deliberately OMITS `.sandbox` (not via
      // buildSessionStartInput), but still carries the gateway MCP servers a real caller
      // would project. The ctx-captured sandbox must still take effect and merge alongside.
      const startInput: SessionStartInput = {
        workspaceId: 'ws_1',
        sessionId: 'ses_ctx',
        agentSnapshot: { model_id: 'm', system: 'be helpful', allowed_tool_names: [] },
        mcpServers: {
          github: {
            type: 'http',
            url: 'https://gw.example/mcp',
            headers: { Authorization: 'Bearer mcp-jwt' },
          },
        },
      };
      await harness.start(startInput);
      await harness.submit(userMessage('hi'));
      await harness.stop('replica.shutting_down');

      const servers = seen?.mcpServers ?? {};
      // The ctx sandbox bound the orca server (merged with the start input's gateway server)…
      expect(Object.keys(servers).sort()).toEqual([ORCA_MCP_SERVER_NAME, 'github'].sort());
      // …and anchored the SDK cwd at the ctx sandbox root.
      expect(seen?.cwd).toBe((sandbox as unknown as { rootDir(): string }).rootDir());
    } finally {
      await sandbox.destroy();
    }
  });
});

// ── the persistent `claude-sdk-persistent` provider (B) ──────────────────────────

/** A faithful streaming-input `Query` fake that also exposes interrupt/setModel/close. */
function fakePersistentQuery(
  capture: (options: { cwd?: string; mcpServers?: Record<string, unknown> }) => void,
): PersistentClaudeQuery {
  return ({ options }) => {
    capture(options as never);
    let done = false;
    const handle: PersistentQueryHandle = {
      [Symbol.asyncIterator](): AsyncIterator<never> {
        return {
          next(): Promise<IteratorResult<never>> {
            if (done) return Promise.resolve({ value: undefined, done: true });
            done = true;
            // One turn-ending result frame so `submit` resolves promptly.
            return Promise.resolve({
              value: {
                type: 'result',
                subtype: 'success',
                usage: { input_tokens: 1, output_tokens: 1 },
              } as never,
              done: false,
            });
          },
        };
      },
      async interrupt(): Promise<void> {},
      async setModel(): Promise<void> {},
      close(): void {},
    };
    return handle;
  };
}

describe('persistent claude provider — sandbox tools + cwd', () => {
  it('binds the orca MCP server to the sandbox and sets cwd to the sandbox root', async () => {
    let seen: { cwd?: string; mcpServers?: Record<string, unknown> } | undefined;
    const query = fakePersistentQuery((o) => {
      seen = o;
    });
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    try {
      const registry = new ProviderRegistry();
      registerClaudePersistentProvider(registry, {
        store: new FakeTranscriptStore(),
        modelDefault: 'm',
        query,
      });
      const ctx = { workspaceId: 'ws_1', sessionId: 'ses_p', sandbox };
      const harness = registry.build(PERSISTENT_SNAPSHOT, ctx) as ClaudePersistentSdkHarness;
      const startInput: SessionStartInput = buildSessionStartInput(PERSISTENT_SNAPSHOT, ctx);
      await harness.start(startInput);
      await harness.submit(userMessage('hi'));
      await harness.stop('replica.shutting_down');

      const servers = seen?.mcpServers ?? {};
      expect(Object.keys(servers).sort()).toEqual([ORCA_MCP_SERVER_NAME, 'github'].sort());
      expect((servers[ORCA_MCP_SERVER_NAME] as { type?: string }).type).toBe('sdk');
      expect(seen?.cwd).toBe((sandbox as unknown as { rootDir(): string }).rootDir());
    } finally {
      await sandbox.destroy();
    }
  });

  it('stays LLM-only (no cwd, no orca server) with no sandbox', async () => {
    let seen: { cwd?: string; mcpServers?: Record<string, unknown> } | undefined;
    const query = fakePersistentQuery((o) => {
      seen = o;
    });
    const registry = new ProviderRegistry();
    registerClaudePersistentProvider(registry, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const ctx = { workspaceId: 'ws_1', sessionId: 'ses_pns' };
    const harness = registry.build(PERSISTENT_SNAPSHOT, ctx) as ClaudePersistentSdkHarness;
    await harness.start(buildSessionStartInput(PERSISTENT_SNAPSHOT, ctx));
    await harness.submit(userMessage('hi'));
    await harness.stop('replica.shutting_down');

    expect(seen?.cwd).toBeUndefined();
    expect(Object.keys(seen?.mcpServers ?? {})).toEqual(['github']);
  });

  it('binds the ctx sandbox even when the start input omits it (ctx is a sufficient source)', async () => {
    let seen: { cwd?: string; mcpServers?: Record<string, unknown> } | undefined;
    const query = fakePersistentQuery((o) => {
      seen = o;
    });
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    try {
      const registry = new ProviderRegistry();
      registerClaudePersistentProvider(registry, {
        store: new FakeTranscriptStore(),
        modelDefault: 'm',
        query,
      });
      const ctx = { workspaceId: 'ws_1', sessionId: 'ses_pctx', sandbox };
      const harness = registry.build(PERSISTENT_SNAPSHOT, ctx) as ClaudePersistentSdkHarness;
      // Hand-built start input WITHOUT `.sandbox` — the ctx-captured sandbox must still bind.
      const startInput: SessionStartInput = {
        workspaceId: 'ws_1',
        sessionId: 'ses_pctx',
        agentSnapshot: { model_id: 'm', system: 'be helpful', allowed_tool_names: [] },
        mcpServers: {
          github: {
            type: 'http',
            url: 'https://gw.example/mcp',
            headers: { Authorization: 'Bearer mcp-jwt' },
          },
        },
      };
      await harness.start(startInput);
      await harness.submit(userMessage('hi'));
      await harness.stop('replica.shutting_down');

      const servers = seen?.mcpServers ?? {};
      expect(Object.keys(servers).sort()).toEqual([ORCA_MCP_SERVER_NAME, 'github'].sort());
      expect(seen?.cwd).toBe((sandbox as unknown as { rootDir(): string }).rootDir());
    } finally {
      await sandbox.destroy();
    }
  });
});

// ── the loop acquires + threads + destroys the sandbox per session ───────────────

describe('SessionLoop acquires a per-session sandbox for real providers', () => {
  it('acquires a handle, threads it to the claude provider (cwd), and destroys it on teardown', async () => {
    let seenCwd: string | undefined;
    const query: ClaudeQuery = ({ options }) => {
      seenCwd = (options as { cwd?: string }).cwd;
      return (async function* () {
        yield {
          type: 'result',
          subtype: 'success',
          usage: { input_tokens: 1, output_tokens: 1 },
        } as never;
      })();
    };
    const runtime = new RecordingSandboxRuntime();
    const providers = new ProviderRegistry();
    registerClaudeProvider(providers, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const loop = new SessionLoop({ workspaceId: 'ws_loop', providers, sandboxRuntime: runtime });

    await loop.applySnapshot(
      'ses_loop',
      new TextEncoder().encode(`${JSON.stringify(GATEWAY_SNAPSHOT)}\n`),
    );
    // Exactly one sandbox acquired for the session.
    expect(runtime.acquired.length).toBe(1);
    const acquiredRoot = (runtime.acquired[0] as unknown as { rootDir(): string }).rootDir();

    await drainLines(
      loop.runTurn(
        'ses_loop',
        new TextEncoder().encode(
          JSON.stringify({ type: 'user.message', content: [{ type: 'text', text: 'go' }] }),
        ),
        new AbortController().signal,
      ),
    );
    // The provider was configured with the acquired sandbox's root as cwd.
    expect(seenCwd).toBe(acquiredRoot);

    // Teardown releases the per-session sandbox.
    await loop.stop();
    expect(runtime.destroyed.length).toBe(1);
    expect(runtime.destroyed[0]).toBe(runtime.acquired[0]);
  });

  it('does NOT acquire a sandbox for the mock provider (sandbox is only for real providers)', async () => {
    const runtime = new RecordingSandboxRuntime();
    const providers = new ProviderRegistry();
    registerMockProvider(providers);
    const loop = new SessionLoop({ workspaceId: 'ws_mock', providers, sandboxRuntime: runtime });

    const mockSnapshot: RunnerSnapshot = { ...GATEWAY_SNAPSHOT, provider: 'mock' };
    await loop.applySnapshot(
      'ses_mock',
      new TextEncoder().encode(`${JSON.stringify(mockSnapshot)}\n`),
    );
    expect(runtime.acquired.length).toBe(0);

    const lines = await drainLines(
      loop.runTurn(
        'ses_mock',
        new TextEncoder().encode(
          JSON.stringify({ type: 'user.message', content: [{ type: 'text', text: 'ping' }] }),
        ),
        new AbortController().signal,
      ),
    );
    // The mock provider still answers deterministically with no sandbox.
    expect(lines.map((l) => l.type)).toEqual(['agent.message', 'agent.turn_completed']);
    await loop.stop();
    expect(runtime.acquired.length).toBe(0);
  });

  it('re-acquires a fresh sandbox on a re-delivered snapshot and destroys the prior one', async () => {
    const query: ClaudeQuery = () =>
      (async function* () {
        yield {
          type: 'result',
          subtype: 'success',
          usage: { input_tokens: 1, output_tokens: 1 },
        } as never;
      })();
    const runtime = new RecordingSandboxRuntime();
    const providers = new ProviderRegistry();
    registerClaudeProvider(providers, {
      store: new FakeTranscriptStore(),
      modelDefault: 'm',
      query,
    });
    const loop = new SessionLoop({ workspaceId: 'ws_re', providers, sandboxRuntime: runtime });

    await loop.applySnapshot(
      'ses_re',
      new TextEncoder().encode(`${JSON.stringify(GATEWAY_SNAPSHOT)}\n`),
    );
    await loop.applySnapshot(
      'ses_re',
      new TextEncoder().encode(`${JSON.stringify(GATEWAY_SNAPSHOT)}\n`),
    );
    // Two acquisitions total; the first handle was destroyed when the snapshot was replaced.
    expect(runtime.acquired.length).toBe(2);
    expect(runtime.destroyed).toContain(runtime.acquired[0]);
    await loop.stop();
    // Both handles released after stop.
    expect(runtime.destroyed).toContain(runtime.acquired[1]);
  });
});

it('preserves the session filesystem when a native-resume harness refreshes credentials', async () => {
  const runtime = new RecordingSandboxRuntime();
  const providers = new ProviderRegistry();
  providers.register('codex-sdk', () =>
    Object.assign(new FakeAgentHarness(), { preserveSandboxOnRefresh: true }),
  );
  const loop = new SessionLoop({ workspaceId: 'ws_refresh', providers, sandboxRuntime: runtime });
  const snapshot = {
    ...GATEWAY_SNAPSHOT,
    provider: 'codex-sdk',
    model: { provider: 'openai', id: 'gpt-5.4' },
  };
  const body = new TextEncoder().encode(JSON.stringify(snapshot) + '\n');
  try {
    await loop.applySnapshot('ses_refresh', body);
    const original = runtime.acquired[0]!;
    await original.files.write('proof.txt', Buffer.from('keep me'));
    await loop.applySnapshot('ses_refresh', body);
    expect(runtime.acquired).toHaveLength(1);
    expect(runtime.destroyed).toHaveLength(0);
    expect(new TextDecoder().decode(await original.files.read('proof.txt'))).toBe('keep me');
  } finally {
    await loop.stop();
  }
  expect(runtime.destroyed).toHaveLength(1);
});
