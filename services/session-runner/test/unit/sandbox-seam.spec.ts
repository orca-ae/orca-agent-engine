// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The session-runner is a LIVE consumer of `@orca/sandbox-runtime`.
//
// The shared package was extracted so both the harness-server AND this runner
// consume ONE definition of the sandbox boundary. This spec proves the runner
// side is real, not merely capability-shaped:
//   1. the runner constructs a concrete `SandboxRuntime` from the package via its
//      seam and drives the extracted streaming `SandboxHandle.spawn` primitive
//      (round-trip stdin → stdout) — the exact primitive the native-CLI providers
//      launch a CLI with;
//   2. runtime selection matches the harness-server's `SANDBOX_RUNTIME` contract
//      (default InMemory; `local` only when a manager is wired);
//   3. the runner's `SessionLoop` HANDS the runtime to a provider through the
//      provider context, so a native-CLI provider reaches `spawn` through the same
//      snapshot → build path the in-process providers use.

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it, expect } from 'vitest';
import {
  InMemorySandboxRuntime,
  LocalSandboxRuntime,
  type SandboxManagerInitConfig,
  type SandboxManagerLike,
} from '@orca/sandbox-runtime';
import {
  createRunnerSandboxRuntime,
  resolveRunnerLocalSandbox,
  type SandboxHandle,
  type SandboxRuntime,
} from '../../src/sandbox/seam.js';
import { SessionLoop } from '../../src/session-loop.js';
import { ProviderRegistry, type ProviderSessionContext } from '../../src/harness/provider.js';
import type {
  AgentEvent,
  AgentHarness,
  SessionStartInput,
  TerminationReason,
  UserEvent,
} from '../../src/harness/agent-harness.js';
import type { RunnerSnapshot } from '../../src/snapshot.js';

/** Read a readable stream to EOF, returning the accumulated utf8 string. */
function collect(stream: NodeJS.ReadableStream): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (b: Buffer) => chunks.push(Buffer.isBuffer(b) ? b : Buffer.from(b)));
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    stream.on('error', reject);
  });
}

/** Verbatim `SandboxManager` fake — `wrapWithSandbox` returns the command unchanged. */
class FakeSandboxManager implements SandboxManagerLike {
  async initialize(_config: SandboxManagerInitConfig): Promise<void> {}
  async wrapWithSandbox(command: string): Promise<string> {
    return command;
  }
}

describe('createRunnerSandboxRuntime — the runner links @orca/sandbox-runtime', () => {
  it('defaults to an InMemorySandboxRuntime (no external dependency)', () => {
    expect(createRunnerSandboxRuntime({ kind: undefined })).toBeInstanceOf(InMemorySandboxRuntime);
    expect(createRunnerSandboxRuntime({ kind: '' })).toBeInstanceOf(InMemorySandboxRuntime);
    expect(createRunnerSandboxRuntime({ kind: 'in-memory' })).toBeInstanceOf(
      InMemorySandboxRuntime,
    );
  });

  it('selects LocalSandboxRuntime for SANDBOX_RUNTIME=local ONLY when a manager is wired', () => {
    const baseDir = mkdtempSync(join(tmpdir(), 'orca-runner-seam-local-'));
    try {
      const local = createRunnerSandboxRuntime({
        kind: 'local',
        local: {
          harnessWorkDir: baseDir,
          allowedNetworkHosts: [],
          manager: new FakeSandboxManager(),
        },
      });
      expect(local).toBeInstanceOf(LocalSandboxRuntime);
      // Case-insensitive, matching the harness-server's parse.
      expect(
        createRunnerSandboxRuntime({
          kind: 'LOCAL',
          local: {
            harnessWorkDir: baseDir,
            allowedNetworkHosts: [],
            manager: new FakeSandboxManager(),
          },
        }),
      ).toBeInstanceOf(LocalSandboxRuntime);
    } finally {
      rmSync(baseDir, { recursive: true, force: true });
    }
  });

  it('falls back to InMemory when local is requested but not wired (never boot-fails)', () => {
    // The runner has no `srt` manager of its own yet; an unwired `local` must not
    // fail the whole runner boot — it degrades to the always-constructible InMemory.
    expect(createRunnerSandboxRuntime({ kind: 'local' })).toBeInstanceOf(InMemorySandboxRuntime);
  });

  it('drives the extracted SandboxHandle.spawn primitive: stdin → streamed stdout', async () => {
    const runtime: SandboxRuntime = createRunnerSandboxRuntime();
    const sandbox: SandboxHandle = await runtime.acquire({});
    try {
      // `spawn` is optional on the interface (cloud runtimes may omit it); the two
      // reusable runtimes implement it, so a runner-constructed default has it.
      expect(sandbox.spawn).toBeTypeOf('function');
      const proc = await sandbox.spawn!('cat');
      proc.stdin.write('from-runner\n');
      proc.stdin.end();
      const out = await collect(proc.stdout);
      expect(out).toBe('from-runner\n');
    } finally {
      await sandbox.destroy();
    }
  });
});

describe('resolveRunnerLocalSandbox — the runner wires its own srt Local runtime', () => {
  it('resolves the Local wiring (work-dir + srt-derived allow-list) when srt is present', () => {
    let probed = false;
    const resolved = resolveRunnerLocalSandbox({
      harnessWorkDir: '/var/run/orca/ws',
      env: {
        AI_GATEWAY_URL: 'https://gw.example:8443/mcp',
        S3_ENDPOINT: 'https://minio.example:9000',
      },
      probeSrt: () => {
        probed = true; // a passing (non-throwing) probe stands in for `srt --version`.
      },
    });
    expect(probed).toBe(true);
    expect(resolved).toBeDefined();
    expect(resolved!.harnessWorkDir).toBe('/var/run/orca/ws');
    // The allow-list contains the provider API hosts + the hosts parsed out of the two URLs (no wildcards).
    expect([...resolved!.allowedNetworkHosts].sort()).toEqual(
      ['api.anthropic.com', 'api.openai.com', 'gw.example', 'minio.example'].sort(),
    );
    // The real SandboxManager (re-exported by @orca/sandbox-runtime) is wired as the manager.
    expect(resolved!.manager).toBeDefined();
    // The resolved wiring actually constructs a LocalSandboxRuntime through the seam.
    expect(createRunnerSandboxRuntime({ kind: 'local', local: resolved! })).toBeInstanceOf(
      LocalSandboxRuntime,
    );
  });

  it('keeps only provider API hosts when egress URLs are malformed', () => {
    const resolved = resolveRunnerLocalSandbox({
      harnessWorkDir: '/w',
      env: { AI_GATEWAY_URL: 'not-a-url', S3_ENDPOINT: '' },
      probeSrt: () => undefined,
    });
    // A malformed AI_GATEWAY_URL and a blank S3_ENDPOINT contribute no hosts; the
    // explicit provider API hosts remain.
    expect(resolved!.allowedNetworkHosts).toEqual(['api.anthropic.com', 'api.openai.com']);
  });

  it('returns undefined and warns when srt is not reachable (never throws / boot-fails)', () => {
    // Explicit `| undefined` (matching register-handlers.spec.ts's LogCall) so a
    // logger call whose `msg?` arg is omitted stays assignable under
    // `exactOptionalPropertyTypes`.
    const warnings: Array<{ obj: unknown; msg?: string | undefined }> = [];
    const resolved = resolveRunnerLocalSandbox({
      harnessWorkDir: '/w',
      env: {},
      probeSrt: () => {
        throw new Error('spawn srt ENOENT');
      },
      logger: { warn: (obj: unknown, msg?: string): void => void warnings.push({ obj, msg }) },
    });
    // Best-effort: an unreachable `srt` degrades to InMemory (undefined), not a throw.
    expect(resolved).toBeUndefined();
    // The fallback is explained (never silent): a warning names the missing binary.
    expect(warnings).toHaveLength(1);
    expect(warnings[0]!.msg).toContain('srt');
    // And the seam then degrades a local request to InMemory using that undefined wiring.
    expect(
      createRunnerSandboxRuntime({
        kind: 'local',
        ...(resolved !== undefined ? { local: resolved } : {}),
      }),
    ).toBeInstanceOf(InMemorySandboxRuntime);
  });
});

// A minimal native-CLI-style provider: instead of running a model in-process (like
// the claude/mock providers), it launches a child through `ctx.sandboxRuntime` and
// streams its stdout — the shape the native-CLI providers take. Used here to
// prove the loop hands the sandbox runtime to a provider via the context.
const SNAPSHOT: RunnerSnapshot = {
  model: { provider: 'anthropic', id: 'claude-sonnet-4' },
  provider: 'native-cli',
  system: 'sys',
  allowed_tool_names: ['bash'],
  allowed_mcp_server_names: [],
  tool_permissions: {},
  egress: { mode: 'gateway' },
};

class SpawnEchoHarness implements AgentHarness {
  readonly capturedCtx: ProviderSessionContext;
  /** The bytes the spawned child streamed back (proves `spawn` actually ran). */
  spawnedOutput = '';
  private terminated = false;
  private readonly outQueue: AgentEvent[] = [];
  private outResolvers: Array<(r: IteratorResult<AgentEvent>) => void> = [];

  constructor(ctx: ProviderSessionContext) {
    this.capturedCtx = ctx;
  }

  async start(_input: SessionStartInput): Promise<void> {
    this.terminated = false;
  }

  async submit(_event: UserEvent): Promise<void> {
    if (this.terminated) {
      return;
    }
    const runtime = this.capturedCtx.sandboxRuntime;
    if (runtime === undefined) {
      throw new Error('native-cli provider requires a sandbox runtime but got none');
    }
    // The native-CLI launch: acquire a sandbox from the runner's runtime and drive
    // the extracted streaming spawn primitive to run a CLI-shaped command.
    const sandbox = await runtime.acquire({});
    try {
      const proc = await sandbox.spawn!('printf "cli-ran"');
      proc.stdin.end();
      this.spawnedOutput = await collect(proc.stdout);
    } finally {
      await sandbox.destroy();
    }
    this.emit({
      kind: 'agent.message',
      payload: { content: [{ type: 'text', text: this.spawnedOutput }] },
    });
    this.emit({ kind: 'agent.turn_completed', payload: { stop_reason: 'end_turn' } });
  }

  interrupt(): void {
    /* no in-flight turn to abort in this test double */
  }

  async stop(_reason: TerminationReason): Promise<void> {
    this.terminated = true;
    while (this.outResolvers.length > 0) {
      this.outResolvers.shift()?.({ value: undefined, done: true });
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

describe('SessionLoop hands the sandbox runtime to a provider via the context', () => {
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

  it('a native-cli provider acquires the runner sandbox and spawns through the loop', async () => {
    const runtime = createRunnerSandboxRuntime();
    let built: SpawnEchoHarness | undefined;
    const providers = new ProviderRegistry();
    providers.register('native-cli', (_snapshot, ctx) => {
      built = new SpawnEchoHarness(ctx);
      return built;
    });
    // The loop is constructed WITH the runner's sandbox runtime — the production wiring.
    const loop = new SessionLoop({ workspaceId: 'ws_cli', providers, sandboxRuntime: runtime });

    await loop.applySnapshot('ses_cli', new TextEncoder().encode(`${JSON.stringify(SNAPSHOT)}\n`));

    // The loop passed the SAME runtime instance onto the provider's context.
    expect(built).toBeDefined();
    expect(built!.capturedCtx.sandboxRuntime).toBe(runtime);

    const lines = await drainLines(
      loop.runTurn(
        'ses_cli',
        new TextEncoder().encode(
          JSON.stringify({
            type: 'user.message',
            id: 'evt_u',
            content: [{ type: 'text', text: 'go' }],
          }),
        ),
        new AbortController().signal,
      ),
    );

    // The provider actually ran a child via SandboxHandle.spawn and streamed its output.
    expect(built!.spawnedOutput).toBe('cli-ran');
    expect(lines.map((l) => l.type)).toEqual(['agent.message', 'agent.turn_completed']);
    expect(lines[0]!['content']).toEqual([{ type: 'text', text: 'cli-ran' }]);
    await loop.stop();
  });

  it('omits sandboxRuntime from the context when the loop wired none (backward-compatible)', async () => {
    let seenCtx: ProviderSessionContext | undefined;
    const providers = new ProviderRegistry();
    providers.register('native-cli', (_snapshot, ctx) => {
      seenCtx = ctx;
      // A no-op harness: this case only asserts the context shape.
      return new SpawnEchoHarness(ctx);
    });
    const loop = new SessionLoop({ workspaceId: 'ws_none', providers });

    await loop.applySnapshot('ses_none', new TextEncoder().encode(`${JSON.stringify(SNAPSHOT)}\n`));
    expect(seenCtx).toBeDefined();
    expect(seenCtx!.sandboxRuntime).toBeUndefined();
    await loop.stop();
  });
});
