// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the generic `custom` native-CLI provider — driven through TWO FAKE CLIs that
// stand in for arbitrary operator-registered coding CLIs (a PLAIN-TEXT one and a JSON-LINE one).
//
// The `custom` provider lets an operator register ANY CLI via a declarative spec carried on the
// snapshot (command / argv-with-placeholders / env / cwd / a stdout→AgentEvent mapping, plus an
// optional approvals opt-in). It boots that CLI via the native-CLI launcher ({@link launchNativeCli})
// inside a per-session {@link SandboxHandle}, wires the native-CLI tool-bridge as the CLI's `orca`
// MCP server (so the model's orca tools resolve INSIDE the sandbox), drives each `user.message` by
// writing the turn to the CLI's stdin (per the spec's stdin template), and normalizes the CLI's
// stdout into Orca-native {@link AgentEvent}s per the spec's stdout mapping. When the spec opts in,
// an approval request the CLI raises parks on the uniform transcript approval (`confirmTool`).
//
// This suite proves that end to end with NO real binary:
//   1. registration under the `custom` key;
//   2. a PLAIN-TEXT CLI: stdout lines → agent text → turn_completed (the simplest mapping);
//   3. a JSON-LINE CLI: assistant/tool_use/tool_result/usage/done frames → Anthropic-native events;
//   4. a JSON-LINE MCP tool call resolves THROUGH the bridge inside the sandbox (the file lands
//      under the sandbox root);
//   5. approval routing: the JSON-LINE CLI's gated `exec` raises an approval that parks on
//      `confirmTool`; an ALLOW verdict runs the command, a DENY blocks it;
//   6. the launch argv the provider emitted (placeholder substitution) reached the CLI;
//   7. `stop()` tears the CLI down and ends the event stream;
//   8. the harness resolves through the registry factory + buildSessionStartInput.

import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { afterEach, beforeEach, describe, it, expect } from 'vitest';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import { ProviderRegistry, buildSessionStartInput } from '../../src/harness/provider.js';
import { registerCustomProvider, CUSTOM_PROVIDER_NAME } from '../../src/harness/custom/provider.js';
import { CustomCliHarness } from '../../src/harness/custom/index.js';
import type { SandboxHandle } from '../../src/sandbox/seam.js';
import type { AgentEvent, ToolPermissionDecision } from '../../src/harness/agent-harness.js';
import type { RunnerSnapshot } from '../../src/snapshot.js';
import { failedNativeCli } from './support/failed-native-cli.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const FAKE_TEXT_CLI = join(HERE, 'support', 'fake-custom-text-cli.mjs');
const FAKE_JSON_CLI = join(HERE, 'support', 'fake-custom-json-cli.mjs');
const TSX = join(HERE, '..', '..', 'node_modules', '.bin', 'tsx');
// The provider's bridge entrypoint (shared across native-CLI providers), run via tsx against source.
const BRIDGE_ENTRY = join(HERE, '..', '..', 'src', 'harness', 'custom', 'bridge-entry.ts');

/** The declarative spec for the PLAIN-TEXT fake CLI (stdout lines → agent text). */
const TEXT_SPEC = {
  command: 'my-text-agent',
  // Raw user text on stdin (the default); print assistant text lines terminated by the sentinel.
  argv: [],
  stdout: { mode: 'text', end_sentinel: '<<TURN_END>>' },
};

/** The declarative spec for the JSON-LINE fake CLI (JSON frames → typed events + approvals). */
const JSON_SPEC = {
  command: 'my-json-agent',
  // Wire the orca bridge into the CLI argv via the bridge placeholders.
  argv: ['--bridge-cmd', '{bridgeCommand}', '--bridge-args', '{bridgeArgsJson}'],
  // Drive each turn by writing a JSON prompt frame to stdin.
  stdin: { template: { prompt: '{userText}' } },
  stdout: {
    mode: 'jsonLine',
    text: { type_equals: 'assistant', text_field: 'text' },
    tool_use: { type_equals: 'tool_call', name_field: 'tool', input_field: 'args', id_field: 'id' },
    tool_result: {
      type_equals: 'tool_result',
      id_field: 'id',
      content_field: 'output',
      error_field: 'is_error',
    },
    usage: { type_equals: 'usage', input_tokens_field: 'in', output_tokens_field: 'out' },
    turn_completed: { type_equals: 'done' },
    approval_request: {
      type_equals: 'approval',
      id_field: 'request_id',
      name_field: 'tool',
      input_field: 'args',
    },
  },
  approvals: {
    response: { type: 'approval_response', request_id: '{requestId}', decision: '{decision}' },
    allow_value: 'allow',
    deny_value: 'deny',
  },
};

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

/** Harness options that run the PLAIN-TEXT fake cli (via node). */
function textHarnessOptions(
  sandbox: SandboxHandle,
  spec: unknown = TEXT_SPEC,
  extra: Partial<ConstructorParameters<typeof CustomCliHarness>[0]> = {},
): ConstructorParameters<typeof CustomCliHarness>[0] {
  return {
    spec,
    modelDefault: 'my-model',
    workspaceId: 'ws_1',
    sessionId: 'ses_cu',
    sandbox,
    cliCommand: process.execPath,
    cliPrefixArgs: [FAKE_TEXT_CLI],
    bridgeCommand: TSX,
    bridgePrefixArgs: [BRIDGE_ENTRY],
    ...extra,
  };
}

/** Harness options that run the JSON-LINE fake cli (via node) + the TS bridge entry. */
function jsonHarnessOptions(
  sandbox: SandboxHandle,
  spec: unknown = JSON_SPEC,
  extra: Partial<ConstructorParameters<typeof CustomCliHarness>[0]> = {},
): ConstructorParameters<typeof CustomCliHarness>[0] {
  return {
    spec,
    modelDefault: 'my-model',
    workspaceId: 'ws_1',
    sessionId: 'ses_cu',
    sandbox,
    cliCommand: process.execPath,
    cliPrefixArgs: [FAKE_JSON_CLI],
    bridgeCommand: TSX,
    bridgePrefixArgs: [BRIDGE_ENTRY],
    ...extra,
  };
}

describe('custom provider — generic operator CLI over the native-CLI foundation (fake CLIs)', () => {
  let workdir: string;

  beforeEach(() => {
    workdir = mkdtempSync(join(tmpdir(), 'orca-cu-spec-'));
  });

  afterEach(() => {
    rmSync(workdir, { recursive: true, force: true });
  });

  it('registers under the `custom` key on the provider registry', () => {
    const registry = new ProviderRegistry();
    registerCustomProvider(registry, { modelDefault: 'my-model' });
    expect(CUSTOM_PROVIDER_NAME).toBe('custom');
    expect(registry.has('custom')).toBe(true);
    expect(registry.providerNames()).toContain('custom');
  });

  it('PLAIN-TEXT CLI: normalizes stdout lines → agent text → turn_completed', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const harness = new CustomCliHarness(textHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cu', agentSnapshot: {} });
      await harness.submit(userMessage('two hello'));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      const messages = sink.events.filter((e) => e.kind === 'agent.message');
      // The "two" verb prints two text lines; each stdout line became an agent.message.
      expect(messages[0]?.payload).toEqual({ content: [{ type: 'text', text: 'hello' }] });
      expect(messages[1]?.payload).toEqual({ content: [{ type: 'text', text: 'hello!' }] });
      const kinds = sink.events.map((e) => e.kind);
      expect(kinds[kinds.length - 1]).toBe('agent.turn_completed');
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('JSON-LINE CLI: normalizes assistant text → usage → turn_completed', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const harness = new CustomCliHarness(jsonHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cu', agentSnapshot: {} });
      await harness.submit(userMessage('say hi there'));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      const message = sink.events.find((e) => e.kind === 'agent.message');
      expect(message?.payload).toEqual({ content: [{ type: 'text', text: 'hi there' }] });
      const kinds = sink.events.map((e) => e.kind);
      expect(kinds).toContain('agent.usage');
      expect(kinds[kinds.length - 1]).toBe('agent.turn_completed');
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('JSON-LINE CLI: runs an MCP tool THROUGH the bridge — the write lands under the sandbox root', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const sandboxRoot = rootDirOf(sandbox);
    const harness = new CustomCliHarness(jsonHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cu', agentSnapshot: {} });
      const marker = 'orca-cu-bridge-marker';
      await harness.submit(userMessage(`run echo ${marker} > tool-out.txt && echo ${marker}`));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      const use = sink.events.find((e) => e.kind === 'agent.tool_use');
      const result = sink.events.find((e) => e.kind === 'agent.tool_result');
      expect(use?.payload).toMatchObject({ name: 'bash' });
      expect(result).toBeDefined();
      expect((result?.payload as { is_error?: boolean }).is_error).toBe(false);
      // The file the bash tool wrote landed IN THE SANDBOX ROOT (proof the tool executed inside the
      // sandbox via the bridge, not on the runner host cwd).
      const onDisk = join(sandboxRoot, 'tool-out.txt');
      expect(existsSync(onDisk)).toBe(true);
      expect(readFileSync(onDisk, 'utf8')).toContain(marker);
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('routes an approval to confirmTool and lets an ALLOW verdict run the command', async () => {
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

    const harness = new CustomCliHarness(jsonHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cu',
        agentSnapshot: {},
        confirmTool,
        // No toolPermissions resolver → fail-closed always_ask (the call parks).
      });
      const marker = 'gated-allow-marker';
      await harness.submit(userMessage(`exec echo ${marker} > gated.txt && echo ${marker}`));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      // The gate saw the CLI's gated call, keyed by a tool_use_id…
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

    const harness = new CustomCliHarness(jsonHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cu',
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
    // the spec`s approval response frame carries a bare allow/deny value, so the deny carries no message the cause could ride out on — a bare
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

    const harness = new CustomCliHarness(jsonHarnessOptions(sandbox, JSON_SPEC, { logger }));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cu',
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

    const harness = new CustomCliHarness(jsonHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cu',
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
    // A missing / misnamed `the spec CLI` on the sandbox PATH is the likeliest production
    // misconfiguration here, and it is invisible from the stream alone: the launcher hands
    // back a process whose stdout is already ended, which is byte-for-byte what a CLI that
    // started and exited immediately looks like. The read loop must read the launcher's
    // `failure` and put it on the wire.
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const launchError = new Error('spawn ENOENT');

    const harness = new CustomCliHarness(
      jsonHarnessOptions(sandbox, JSON_SPEC, { launch: () => failedNativeCli(launchError) }),
    );
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cu',
        agentSnapshot: {},
      });
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.error'));

      const failure = sink.events.find((e) => e.kind === 'agent.error');
      expect((failure?.payload as { message?: string }).message).toMatch(
        /custom CLI failed to launch: spawn ENOENT/,
      );
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('emits the launch argv: spec argv with placeholders substituted (bridge wired)', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const argvOut = join(workdir, 'argv.json');

    const harness = new CustomCliHarness(
      jsonHarnessOptions(sandbox, JSON_SPEC, { cliExtraArgs: ['--argv-out', argvOut] }),
    );
    const sink = collectEvents(harness);
    try {
      await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cu', agentSnapshot: {} });
      await harness.submit(userMessage('say ok'));
      await waitFor(() => existsSync(argvOut));
      const argv = JSON.parse(readFileSync(argvOut, 'utf8')) as string[];

      // The spec's `--bridge-cmd {bridgeCommand}` was substituted with the real bridge command…
      const idx = argv.indexOf('--bridge-cmd');
      expect(idx).toBeGreaterThanOrEqual(0);
      expect(argv[idx + 1]).toBe(TSX);
      // …and `{bridgeArgsJson}` resolved to a JSON array of the bridge args (pointing at --root).
      const argsIdx = argv.indexOf('--bridge-args');
      const bridgeArgs = JSON.parse(argv[argsIdx + 1] ?? 'null') as string[];
      expect(bridgeArgs).toContain('--root');
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('stop() tears the CLI down and ends the event stream', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const harness = new CustomCliHarness(jsonHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cu', agentSnapshot: {} });
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

    const harness = new CustomCliHarness(jsonHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cu',
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
    registerCustomProvider(registry, {
      modelDefault: 'my-model',
      cliCommand: process.execPath,
      cliPrefixArgs: [FAKE_JSON_CLI],
      bridgeCommand: TSX,
      bridgePrefixArgs: [BRIDGE_ENTRY],
    });
    const snapshot = {
      provider: 'custom',
      model: { provider: 'acme', id: 'my-model' },
      system: 'You are Orca.',
      allowed_tool_names: ['bash'],
      allowed_mcp_server_names: [],
      tool_permissions: {},
      custom_spec: JSON_SPEC,
      egress: {
        mode: 'gateway',
        gateway: { llm_base_url: 'https://gw/anthropic', llm_jwt: 'jwt' },
      },
    } as unknown as RunnerSnapshot;

    const harness = registry.build(snapshot, {
      workspaceId: 'ws_1',
      sessionId: 'ses_cu',
      sandbox,
    });
    const input = buildSessionStartInput(snapshot, {
      workspaceId: 'ws_1',
      sessionId: 'ses_cu',
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

    const harness = new CustomCliHarness(jsonHarnessOptions(cloudHandle));
    await expect(
      harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cu', agentSnapshot: {} }),
    ).rejects.toThrow(/root-dir-backed sandbox|sbx_cloud_only|rootDir/);
    await harness.stop('client.archived');
  }, 15_000);

  it('fails fast when the snapshot carries no custom spec', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    // Construct a harness with NO spec (a snapshot that forgot the custom_spec block).
    const harness = new CustomCliHarness({
      modelDefault: 'my-model',
      workspaceId: 'ws_1',
      sessionId: 'ses_cu',
      sandbox,
      cliCommand: process.execPath,
      cliPrefixArgs: [FAKE_JSON_CLI],
      bridgeCommand: TSX,
      bridgePrefixArgs: [BRIDGE_ENTRY],
    } as ConstructorParameters<typeof CustomCliHarness>[0]);
    await expect(
      harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cu', agentSnapshot: {} }),
    ).rejects.toThrow(/custom.*spec/i);
    await harness.stop('client.archived');
    await sandbox.destroy();
  }, 15_000);
});

// The tmux leg proves the same provider over the local-attach transport (the real target): the
// bridge child reconstructs a tmux-backed handle over the shared socket. Self-skips without `tmux`.
const maybeTmux = tmuxAvailable() ? describe : describe.skip;
maybeTmux('custom provider — over the tmux local-attach transport', () => {
  it('runs a tool through the bridge on a tmux-backed sandbox', async () => {
    const { TmuxSandboxRuntime } = await import('../../src/sandbox/tmux-sandbox.js');
    const runtime = new TmuxSandboxRuntime();
    const sandbox = await runtime.acquire({});
    const sandboxRoot = rootDirOf(sandbox);
    const harness = new CustomCliHarness(jsonHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cu', agentSnapshot: {} });
      const marker = 'tmux-cu-bridge-marker';
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
