// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the `codex` native-CLI provider — driven through a FAKE `codex app-server`
// that speaks the SAME JSON-RPC-over-stdio protocol the real binary does.
//
// The provider boots `codex app-server` via the native-CLI launcher ({@link launchNativeCli})
// inside a per-session {@link SandboxHandle}, wires the native-CLI tool-bridge as the
// app-server's `orca` MCP server (so the model's orca tools resolve INSIDE the sandbox),
// drives each `user.message` by sending `thread/start` (first turn) + `turn/start` over the
// JSON-RPC stdio, and normalizes the app-server's `item/*` + `turn/*` notifications into
// Orca-native {@link AgentEvent}s. Codex's `on-request` approvals (server→client requests)
// are routed to the uniform transcript approval (`confirmTool`) and answered over stdin.
//
// This suite proves that end to end with NO real codex/claude binary:
//   1. registration under the `codex` key + a driven turn's event normalization
//      (assistant text → usage → turn_completed),
//   2. an MCP tool call that resolves THROUGH the bridge inside the sandbox (the file the
//      tool writes lands under the sandbox root),
//   3. approval routing: codex's built-in `exec` raises an `on-request` approval that parks
//      on `confirmTool`; an ALLOW verdict lets the command run, a DENY verdict blocks it,
//   4. the launch argv the provider emitted actually reached the CLI (`app-server` +
//      on-request approval policy + the bridge mcp-config),
//   5. `stop()` tears the app-server down and ends the event stream,
//   6. the harness resolves through the registry factory + buildSessionStartInput.
//
// The bridge child codex spawns reconstructs a handle over the SAME session root, so a tool
// call's write is observable on the host under that root.

import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { afterEach, beforeEach, describe, it, expect } from 'vitest';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import { ProviderRegistry, buildSessionStartInput } from '../../src/harness/provider.js';
import { registerCodexProvider, CODEX_PROVIDER_NAME } from '../../src/harness/codex/provider.js';
import { CodexCliHarness } from '../../src/harness/codex/index.js';
import type { SandboxHandle } from '../../src/sandbox/seam.js';
import type { AgentEvent, ToolPermissionDecision } from '../../src/harness/agent-harness.js';
import type { RunnerSnapshot } from '../../src/snapshot.js';
import { failedNativeCli, muteNativeCli } from './support/failed-native-cli.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const FAKE_CLI = join(HERE, 'support', 'fake-codex-cli.mjs');
const TSX = join(HERE, '..', '..', 'node_modules', '.bin', 'tsx');
// The provider's bridge entrypoint (shared with claude-code), run via tsx against TS source.
const BRIDGE_ENTRY = join(HERE, '..', '..', 'src', 'harness', 'codex', 'bridge-entry.ts');

function tmuxAvailable(): boolean {
  try {
    execFileSync('tmux', ['-V'], { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

/** Collect agent events off a harness into an array until the stream ends. */
function collectEvents(harness: { events(): AsyncIterable<AgentEvent> }): {
  events: AgentEvent[];
  done: Promise<void>;
} {
  const events: AgentEvent[] = [];
  const done = (async () => {
    for await (const e of harness.events()) {
      events.push(e);
    }
  })();
  return { events, done };
}

/** Wait until `cond()` is true or the timeout elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() >= deadline) {
      throw new Error('waitFor timed out');
    }
    await new Promise((r) => setTimeout(r, 25));
  }
}

/** A `user.message` UserEvent carrying one text block. */
function userMessage(text: string): { kind: string; payload: unknown } {
  return { kind: 'user.message', payload: { content: [{ type: 'text', text }] } };
}

/** Read the host-side work-dir root off a reusable-runtime handle (for on-disk asserts). */
function rootDirOf(sandbox: SandboxHandle): string {
  return (sandbox as unknown as { rootDir(): string }).rootDir();
}

/** Build the harness options that run the FAKE cli (via node) + the TS bridge entry. */
function fakeHarnessOptions(
  sandbox: SandboxHandle,
  extra: Partial<ConstructorParameters<typeof CodexCliHarness>[0]> = {},
): ConstructorParameters<typeof CodexCliHarness>[0] {
  return {
    modelDefault: 'gpt-5-codex',
    workspaceId: 'ws_1',
    sessionId: 'ses_cx',
    sandbox,
    // Run the FAKE cli under node instead of the real `codex` binary.
    cliCommand: process.execPath,
    cliPrefixArgs: [FAKE_CLI],
    // The bridge entry runs under tsx so the CLI child speaks MCP to the TS source.
    bridgeCommand: TSX,
    bridgePrefixArgs: [BRIDGE_ENTRY],
    ...extra,
  };
}

describe('codex provider — headless codex app-server over JSON-RPC (fake CLI)', () => {
  let workdir: string;

  beforeEach(() => {
    workdir = mkdtempSync(join(tmpdir(), 'orca-cx-spec-'));
  });

  afterEach(() => {
    rmSync(workdir, { recursive: true, force: true });
  });

  it('registers under the `codex` key on the provider registry', () => {
    const registry = new ProviderRegistry();
    registerCodexProvider(registry, { modelDefault: 'gpt-5-codex' });
    expect(CODEX_PROVIDER_NAME).toBe('codex');
    expect(registry.has('codex')).toBe(true);
    expect(registry.providerNames()).toContain('codex');
  });

  it('drives a chat turn: normalizes assistant text → usage → turn_completed', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const harness = new CodexCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cx', agentSnapshot: {} });
      await harness.submit(userMessage('say hi there'));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      const kinds = sink.events.map((e) => e.kind);
      const message = sink.events.find((e) => e.kind === 'agent.message');
      expect(message?.payload).toEqual({ content: [{ type: 'text', text: 'hi there' }] });
      expect(kinds).toContain('agent.usage');
      expect(kinds[kinds.length - 1]).toBe('agent.turn_completed');
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('runs an MCP tool THROUGH the bridge: the write lands under the sandbox root', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const sandboxRoot = rootDirOf(sandbox);
    const harness = new CodexCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cx', agentSnapshot: {} });
      const marker = 'orca-cx-bridge-marker';
      await harness.submit(userMessage(`run echo ${marker} > tool-out.txt && echo ${marker}`));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      const use = sink.events.find((e) => e.kind === 'agent.tool_use');
      const result = sink.events.find((e) => e.kind === 'agent.tool_result');
      expect(use?.payload).toMatchObject({ name: 'mcp__orca__bash' });
      expect(result).toBeDefined();
      expect((result?.payload as { is_error?: boolean }).is_error).toBe(false);
      // The file the bash tool wrote landed IN THE SANDBOX ROOT (proof the tool executed
      // inside the sandbox via the bridge, not on the runner host cwd).
      const onDisk = join(sandboxRoot, 'tool-out.txt');
      expect(existsSync(onDisk)).toBe(true);
      expect(readFileSync(onDisk, 'utf8')).toContain(marker);
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('routes an on-request approval to confirmTool and lets an ALLOW verdict run the command', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const sandboxRoot = rootDirOf(sandbox);

    let gatedToolName: string | undefined;
    let gatedToolUseId: string | undefined;
    const confirmTool = async (
      toolName: string,
      input: Record<string, unknown>,
      opts: { toolUseId: string },
    ): Promise<ToolPermissionDecision> => {
      gatedToolName = toolName;
      gatedToolUseId = opts.toolUseId;
      return { behavior: 'allow', updatedInput: input };
    };

    const harness = new CodexCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cx',
        agentSnapshot: {},
        confirmTool,
        // No toolPermissions resolver → fail-closed always_ask (the call parks).
      });
      const marker = 'gated-allow-marker';
      await harness.submit(userMessage(`exec echo ${marker} > gated.txt && echo ${marker}`));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      // The gate saw the codex built-in shell call, keyed by a tool_use_id…
      expect(gatedToolName).toBe('bash');
      expect(gatedToolUseId).toBeTruthy();
      // …a requires_action signal was emitted for it…
      const requires = sink.events.find((e) => e.kind === 'agent.requires_action');
      expect(requires?.payload).toMatchObject({ action: 'tool_confirmation', tool_name: 'bash' });
      // …the ALLOW verdict let the command run through the bridge (result not an error)…
      const result = sink.events.find((e) => e.kind === 'agent.tool_result');
      expect((result?.payload as { is_error?: boolean }).is_error).toBe(false);
      // …and the write landed in the sandbox.
      expect(readFileSync(join(sandboxRoot, 'gated.txt'), 'utf8')).toContain(marker);
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('a DENY verdict blocks the command and surfaces an error result', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const sandboxRoot = rootDirOf(sandbox);

    const confirmTool = async (): Promise<ToolPermissionDecision> => ({
      behavior: 'deny',
      message: 'not allowed',
    });

    const harness = new CodexCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cx',
        agentSnapshot: {},
        confirmTool,
      });
      await harness.submit(userMessage('exec echo should-not-run > denied.txt'));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      // The DENY verdict produced an error tool_result…
      const result = sink.events.find((e) => e.kind === 'agent.tool_result');
      expect((result?.payload as { is_error?: boolean }).is_error).toBe(true);
      // …and the command never wrote its file (it was blocked before running).
      expect(existsSync(join(sandboxRoot, 'denied.txt'))).toBe(false);
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('LOGS the gate`s REASON rather than discarding it', async () => {
    // codex`s approval result is a bare decision string, so the deny carries no message the cause could ride out on — a bare
    // `catch` did not even BIND the error and it reached nothing at all. The realistic
    // trigger is a backing-store outage, which denies EVERY tool call in the session, so a
    // dropped cause means an operator sees a session where the model is refused everything
    // and nothing anywhere says why. The logger is the only channel left; it must be used.
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const confirmTool = (): Promise<ToolPermissionDecision> =>
      Promise.reject(new Error('permission store unreachable'));
    const logged: string[] = [];
    const logger = {
      error: (obj: unknown, msg?: string): void => {
        const err = (obj as { err?: unknown }).err;
        logged.push(`${String(msg)} ${err instanceof Error ? err.message : String(err)}`);
      },
    };

    const harness = new CodexCliHarness(fakeHarnessOptions(sandbox, { logger }));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cx',
        agentSnapshot: {},
        confirmTool,
      });
      await harness.submit(userMessage('exec echo should-not-run > gate-reason.txt'));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));
      await waitFor(() => logged.length > 0);

      expect(
        logged.join(' | '),
        'the gate`s cause must reach the log — this protocol gives it nowhere else to go',
      ).toContain('permission store unreachable');
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('a THROWING gate is answered as a DENY rather than wedging the CLI', async () => {
    // The gate park is dispatched FIRE-AND-FORGET off the read loop (so a later cancel /
    // interrupt stays reachable). A throw out of it is therefore doubly bad: it becomes an
    // unhandled rejection, which is fatal to this process, and it skips the decision frame
    // the CLI is blocked on — parking that tool call for the rest of the session. A gate
    // that faults must be answered exactly like a DENY.
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const sandboxRoot = rootDirOf(sandbox);

    // A gate whose backing store is unreachable: it rejects instead of returning a verdict.
    const confirmTool = (): Promise<ToolPermissionDecision> =>
      Promise.reject(new Error('permission store unreachable'));

    const harness = new CodexCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cx',
        agentSnapshot: {},
        confirmTool,
      });
      await harness.submit(userMessage('exec echo should-not-run > gate-threw.txt'));
      // The turn COMPLETES: the CLI got its decision frame. Without the catch it never
      // does, and this wait is what times out.
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      // The faulted gate produced an error tool_result (fail-closed, exactly like a deny)…
      const result = sink.events.find((e) => e.kind === 'agent.tool_result');
      expect((result?.payload as { is_error?: boolean }).is_error).toBe(true);
      // …and the command never ran.
      expect(existsSync(join(sandboxRoot, 'gate-threw.txt'))).toBe(false);
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('reports a FAILED CLI launch as an agent.error instead of an empty, silent turn', async () => {
    // A missing / misnamed `codex` on the sandbox PATH is the likeliest production
    // misconfiguration here, and it is invisible from the stream alone: the launcher hands
    // back a process whose stdout is already ended, which is byte-for-byte what a CLI that
    // started and exited immediately looks like. The read loop must read the launcher's
    // `failure` and put it on the wire.
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const launchError = new Error('spawn ENOENT');

    const harness = new CodexCliHarness(
      fakeHarnessOptions(sandbox, { launch: () => failedNativeCli(launchError) }),
    );
    const sink = collectEvents(harness);
    try {
      // codex blocks `start` on its `initialize` handshake. With the CLI dead that response
      // never arrives, so `start` must FAIL — left pending it hangs the session's snapshot
      // apply forever, `activeTurns` never drops, and the idle watchdog never fires.
      await expect(
        harness.start({
          workspaceId: 'ws_1',
          sessionId: 'ses_cx',
          agentSnapshot: {},
        }),
      ).rejects.toThrow(/codex initialize failed/);
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.error'));

      const failure = sink.events.find((e) => e.kind === 'agent.error');
      expect((failure?.payload as { message?: string }).message).toMatch(
        /codex CLI failed to launch: spawn ENOENT/,
      );
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('bounds a JSON-RPC request codex never answers, so start FAILS instead of hanging', async () => {
    // codex can accept a frame and simply go quiet — a protocol mismatch, a wedged child.
    // No stream event reports that, and the reader's own settle only fires when stdout
    // ENDS, which a live-but-mute process never does. Left unbounded, `start()`'s
    // `initialize` parks forever: the snapshot apply never returns, `activeTurns` never
    // drops, and the idle watchdog can never shut the runner down.
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const mute = muteNativeCli();

    const harness = new CodexCliHarness(
      fakeHarnessOptions(sandbox, { launch: () => mute, requestTimeoutMs: 50 }),
    );
    const sink = collectEvents(harness);
    try {
      await expect(
        harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cx', agentSnapshot: {} }),
      ).rejects.toThrow(/codex initialize failed: .*timed out after 50ms/);
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('emits the launch argv: app-server + on-request approval policy + bridge mcp-config', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const argvOut = join(workdir, 'argv.json');

    const harness = new CodexCliHarness(
      fakeHarnessOptions(sandbox, { cliExtraArgs: ['--argv-out', argvOut] }),
    );
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cx',
        agentSnapshot: {},
        // A gate is wired → the argv must use approval_policy=on-request.
        confirmTool: async () => ({ behavior: 'allow', updatedInput: {} }),
      });
      await harness.submit(userMessage('say ok'));
      await waitFor(() => existsSync(argvOut));
      const argv = JSON.parse(readFileSync(argvOut, 'utf8')) as string[];

      // The app-server subcommand + the on-request approval policy override are present.
      expect(argv).toContain('app-server');
      expect(argv).toContain('approval_policy="on-request"');
      // The bridge was wired as the orca MCP server.
      expect(argv.some((a) => a.startsWith('mcp_servers.orca.command='))).toBe(true);
      expect(argv.some((a) => a.startsWith('mcp_servers.orca.args='))).toBe(true);
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('stop() tears the app-server down and ends the event stream', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const harness = new CodexCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cx', agentSnapshot: {} });
    await harness.submit(userMessage('say hello'));
    await harness.stop('client.archived');
    await Promise.race([
      sink.done,
      new Promise<void>((_, reject) =>
        setTimeout(() => reject(new Error('events() did not end after stop()')), 8000),
      ),
    ]);
    await sandbox.destroy();
  }, 30_000);

  it('interrupt() unwinds a turn parked on the approval gate WITHOUT ending the harness', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});

    // A gate that PARKS until released — mimics a turn awaiting a human verdict.
    let release: ((d: ToolPermissionDecision) => void) | undefined;
    const confirmTool = (): Promise<ToolPermissionDecision> =>
      new Promise<ToolPermissionDecision>((resolve) => {
        release = resolve;
      });

    const harness = new CodexCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cx',
        agentSnapshot: {},
        confirmTool,
      });
      const turn = harness.submit(userMessage('exec echo blocked > blocked.txt'));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.requires_action'));
      harness.interrupt();
      await Promise.race([
        turn,
        new Promise<void>((_, reject) =>
          setTimeout(() => reject(new Error('interrupt did not unwind the parked turn')), 8000),
        ),
      ]);
      // The harness is still alive: a follow-up chat turn drives normally.
      release?.({ behavior: 'deny', message: 'released' });
      await harness.submit(userMessage('say after-interrupt'));
      const isFollowUp = (e: AgentEvent): boolean =>
        e.kind === 'agent.message' &&
        JSON.stringify(e.payload) ===
          JSON.stringify({ content: [{ type: 'text', text: 'after-interrupt' }] });
      await waitFor(() => sink.events.some(isFollowUp));
      expect(sink.events.some(isFollowUp)).toBe(true);
    } finally {
      release?.({ behavior: 'deny', message: 'cleanup' });
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('resolves the harness through the registry factory + buildSessionStartInput projection', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const registry = new ProviderRegistry();
    registerCodexProvider(registry, {
      modelDefault: 'gpt-5-codex',
      cliCommand: process.execPath,
      cliPrefixArgs: [FAKE_CLI],
      bridgeCommand: TSX,
      bridgePrefixArgs: [BRIDGE_ENTRY],
    });
    const snapshot = {
      provider: 'codex',
      model: { provider: 'openai', id: 'gpt-5-codex' },
      system: 'You are Orca.',
      allowed_tool_names: ['bash'],
      allowed_mcp_server_names: [],
      tool_permissions: {},
      egress: { mode: 'gateway', gateway: { llm_base_url: 'https://gw/openai', llm_jwt: 'jwt' } },
    } as unknown as RunnerSnapshot;

    const harness = registry.build(snapshot, {
      workspaceId: 'ws_1',
      sessionId: 'ses_cx',
      sandbox,
    });
    const input = buildSessionStartInput(snapshot, {
      workspaceId: 'ws_1',
      sessionId: 'ses_cx',
      sandbox,
    });
    const sink = collectEvents(harness);
    try {
      await harness.start(input);
      await harness.submit(userMessage('say via-registry'));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));
      const message = sink.events.find((e) => e.kind === 'agent.message');
      expect(message?.payload).toEqual({ content: [{ type: 'text', text: 'via-registry' }] });
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('fails fast on a root-less (cloud-only) sandbox handle rather than emitting a --root-less bridge', async () => {
    const cloudHandle: SandboxHandle = {
      id: 'sbx_cloud_only',
      async run() {
        return { output: [] };
      },
      files: {
        async write() {},
        async read() {
          return Buffer.from('');
        },
        async readUtf8Page() {
          return {
            content: '',
            metadata: {
              offset: 0,
              limit: 0,
              bytes_read: 0,
              total_bytes: 0,
              offset_unit: 'utf8_bytes' as const,
              truncation: false,
              next_offset: null,
            },
          };
        },
        async list() {
          return [];
        },
        async chmod() {},
        async delete() {},
      },
      async runPrivileged() {
        throw new Error('unsupported');
      },
      async pause() {},
      async resume() {},
      async destroy() {},
      // NOTE: no `rootDir()` and no `spawn()`.
    };

    const harness = new CodexCliHarness(fakeHarnessOptions(cloudHandle));
    await expect(
      harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cx', agentSnapshot: {} }),
    ).rejects.toThrow(/root-dir-backed sandbox|sbx_cloud_only|rootDir/);
    await harness.stop('client.archived');
  }, 15_000);
});

// The tmux leg proves the same provider over the local-attach transport (the real target):
// the bridge child reconstructs a tmux-backed handle over the shared socket. Self-skips
// without `tmux`.
const maybeTmux = tmuxAvailable() ? describe : describe.skip;
maybeTmux('codex provider — over the tmux local-attach transport', () => {
  it('runs a tool through the bridge on a tmux-backed sandbox', async () => {
    const { TmuxSandboxRuntime } = await import('../../src/sandbox/tmux-sandbox.js');
    const runtime = new TmuxSandboxRuntime();
    const sandbox = await runtime.acquire({});
    const sandboxRoot = rootDirOf(sandbox);
    const harness = new CodexCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cx', agentSnapshot: {} });
      const marker = 'tmux-cx-bridge-marker';
      await harness.submit(userMessage(`run echo ${marker} > tmux-tool.txt && echo ${marker}`));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));
      const result = sink.events.find((e) => e.kind === 'agent.tool_result');
      expect(result).toBeDefined();
      expect(readFileSync(join(sandboxRoot, 'tmux-tool.txt'), 'utf8')).toContain(marker);
    } finally {
      await harness.stop('client.archived');
      await sink.done;
      await sandbox.destroy();
    }
  }, 40_000);
});
