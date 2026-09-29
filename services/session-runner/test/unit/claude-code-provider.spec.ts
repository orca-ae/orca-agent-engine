// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the `claude-code` native-CLI provider — driven through a FAKE
// `claude`-like CLI that speaks the SAME stream-json protocol the real binary does.
//
// The provider boots the `claude` binary HEADLESS via the native-CLI launcher
// ({@link launchNativeCli}) inside a per-session {@link SandboxHandle}, wires the
// native-CLI tool-bridge as the CLI's MCP server (so the model's orca tools resolve
// INSIDE the sandbox), drives each `user.message` by writing a stream-json user frame
// to the CLI's stdin, and normalizes the CLI's stream-json stdout into Orca-native
// {@link AgentEvent}s. Tool permission requests the CLI raises over the stream-json
// control channel are routed to the uniform transcript approval (`confirmTool`) and
// answered over stdin.
//
// This suite proves that end to end with NO real claude/codex binary:
//   1. registration under the `claude-code` key + a driven turn's event normalization
//      (system → assistant text → usage → turn_completed),
//   2. a tool call that resolves THROUGH the bridge inside the sandbox (the file the
//      tool writes lands under the sandbox root),
//   3. approval routing: a gated tool call parks on `confirmTool`; an ALLOW verdict
//      lets the tool run, a DENY verdict surfaces an error tool_result,
//   4. the launch argv the provider emitted actually reached the CLI (stream-json
//      flags + the bridge mcp-config + the permission wiring),
//   5. `stop()` tears the CLI down and ends the event stream,
//   6. a snapshot's `skills_plugin_dir` reaches the CLI as `--plugin-dir` end-to-end
//      (snapshot → buildSessionStartInput → harness → argv), and is omitted when the
//      snapshot stages no skills bundle,
//   7. a `control_cancel_request` the CLI raises releases the matching parked gate as a
//      deny (so a withdrawn permission does not hang the turn until interrupt/stop),
//   8. a root-less (cloud-only) sandbox handle fails `start` FAST with an actionable
//      error, rather than emitting a --root-less bridge the entrypoint rejects opaquely.
//
// The bridge child the CLI spawns reconstructs a handle over the SAME session root
// (the runner and the CLI share the sandbox tree), so a `bash` tool call's write is
// observable on the host under that root.

import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { afterEach, beforeEach, describe, it, expect, vi } from 'vitest';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import { ProviderRegistry, buildSessionStartInput } from '../../src/harness/provider.js';
import {
  registerClaudeCodeProvider,
  CLAUDE_CODE_PROVIDER_NAME,
} from '../../src/harness/claude-code/provider.js';
import { ClaudeCodeCliHarness } from '../../src/harness/claude-code/index.js';
import type { SandboxHandle } from '../../src/sandbox/seam.js';
import type {
  AgentEvent,
  SessionStartInput,
  ToolPermissionDecision,
} from '../../src/harness/agent-harness.js';
import type { RunnerSnapshot } from '../../src/snapshot.js';
import {
  failedNativeCli,
  muteNativeCli,
  scriptedNativeCliWithDeath,
} from './support/failed-native-cli.js';
import { DEFAULT_TURN_DEADLINE_MS } from '../../src/sandbox/native-cli-launcher.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const FAKE_CLI = join(HERE, 'support', 'fake-claude-code-cli.mjs');
const TSX = join(HERE, '..', '..', 'node_modules', '.bin', 'tsx');
// The provider's bridge entrypoint, run via tsx so the spec exercises the TS source.
const BRIDGE_ENTRY = join(HERE, '..', '..', 'src', 'harness', 'claude-code', 'bridge-entry.ts');

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
  extra: Partial<ConstructorParameters<typeof ClaudeCodeCliHarness>[0]> = {},
): ConstructorParameters<typeof ClaudeCodeCliHarness>[0] {
  return {
    modelDefault: 'claude-sonnet-4-5',
    workspaceId: 'ws_1',
    sessionId: 'ses_cc',
    sandbox,
    // Run the FAKE cli under node instead of the real `claude` binary.
    cliCommand: process.execPath,
    cliPrefixArgs: [FAKE_CLI],
    // The bridge entry runs under tsx so the CLI child speaks MCP to the TS source.
    bridgeCommand: TSX,
    bridgePrefixArgs: [BRIDGE_ENTRY],
    ...extra,
  };
}

describe('claude-code provider — headless claude over stream-json (fake CLI)', () => {
  let workdir: string;

  beforeEach(() => {
    workdir = mkdtempSync(join(tmpdir(), 'orca-cc-spec-'));
  });

  afterEach(() => {
    rmSync(workdir, { recursive: true, force: true });
  });

  it('registers under the `claude-code` key on the provider registry', () => {
    const registry = new ProviderRegistry();
    registerClaudeCodeProvider(registry, { modelDefault: 'claude-sonnet-4-5' });
    expect(CLAUDE_CODE_PROVIDER_NAME).toBe('claude-code');
    expect(registry.has('claude-code')).toBe(true);
    expect(registry.providerNames()).toContain('claude-code');
  });

  it('drives a chat turn: normalizes system → assistant text → usage → turn_completed', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const harness = new ClaudeCodeCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cc', agentSnapshot: {} });
      await harness.submit(userMessage('say hi there'));
      // `submit` resolves once the turn's events are EMITTED; wait for the external
      // events() consumer to drain them (delivery is async) before asserting.
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      const kinds = sink.events.map((e) => e.kind);
      // The init frame surfaced as a `system` event…
      expect(kinds).toContain('system');
      // …the assistant text as an `agent.message`…
      const message = sink.events.find((e) => e.kind === 'agent.message');
      expect(message?.payload).toEqual({ content: [{ type: 'text', text: 'hi there' }] });
      // …the result frame usage as `agent.usage`…
      expect(kinds).toContain('agent.usage');
      // …and the turn boundary as a terminal `agent.turn_completed` (submit resolved).
      expect(kinds[kinds.length - 1]).toBe('agent.turn_completed');
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('runs a tool THROUGH the bridge: the write lands under the sandbox root', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const sandboxRoot = rootDirOf(sandbox);
    // No gate wired → the CLI is launched with --dangerously-skip-permissions and runs
    // the tool without parking.
    const harness = new ClaudeCodeCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cc', agentSnapshot: {} });
      const marker = 'orca-cc-bridge-marker';
      await harness.submit(userMessage(`run echo ${marker} > tool-out.txt && echo ${marker}`));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      // The tool_use + its paired tool_result were normalized…
      const use = sink.events.find((e) => e.kind === 'agent.tool_use');
      const result = sink.events.find((e) => e.kind === 'agent.tool_result');
      expect(use?.payload).toMatchObject({ name: 'mcp__orca__bash' });
      expect(result).toBeDefined();
      expect((result?.payload as { is_error?: boolean }).is_error).toBe(false);
      // …and the file the bash tool wrote landed IN THE SANDBOX ROOT (proof the tool
      // executed inside the sandbox via the bridge, not on the runner host cwd).
      const onDisk = join(sandboxRoot, 'tool-out.txt');
      expect(existsSync(onDisk)).toBe(true);
      expect(readFileSync(onDisk, 'utf8')).toContain(marker);
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('routes a gated tool call to confirmTool and lets an ALLOW verdict run the tool', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const sandboxRoot = rootDirOf(sandbox);

    // A confirmation gate that records the parked call and resolves ALLOW.
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

    const harness = new ClaudeCodeCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      const input: SessionStartInput = {
        workspaceId: 'ws_1',
        sessionId: 'ses_cc',
        agentSnapshot: {},
        confirmTool,
        // No toolPermissions resolver → fail-closed always_ask (the call parks).
      };
      await harness.start(input);
      const marker = 'gated-allow-marker';
      await harness.submit(userMessage(`run echo ${marker} > gated.txt && echo ${marker}`));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      // The gate saw the tool call keyed by its tool_use_id…
      expect(gatedToolName).toBe('mcp__orca__bash');
      expect(gatedToolUseId).toBeTruthy();
      // …a requires_action signal was emitted for it…
      const requires = sink.events.find((e) => e.kind === 'agent.requires_action');
      expect(requires?.payload).toMatchObject({
        action: 'tool_confirmation',
        tool_name: 'mcp__orca__bash',
      });
      // …the ALLOW verdict let the tool run through the bridge (result not an error)…
      const result = sink.events.find((e) => e.kind === 'agent.tool_result');
      expect((result?.payload as { is_error?: boolean }).is_error).toBe(false);
      // …and the write landed in the sandbox.
      expect(readFileSync(join(sandboxRoot, 'gated.txt'), 'utf8')).toContain(marker);
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('a DENY verdict blocks the tool and surfaces an error tool_result', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const sandboxRoot = rootDirOf(sandbox);

    const confirmTool = async (): Promise<ToolPermissionDecision> => ({
      behavior: 'deny',
      message: 'not allowed',
    });

    const harness = new ClaudeCodeCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cc',
        agentSnapshot: {},
        confirmTool,
      });
      await harness.submit(userMessage('run echo should-not-run > denied.txt'));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      // The DENY verdict produced an error tool_result…
      const result = sink.events.find((e) => e.kind === 'agent.tool_result');
      expect((result?.payload as { is_error?: boolean }).is_error).toBe(true);
      // …and the tool never wrote its file (it was blocked before running).
      expect(existsSync(join(sandboxRoot, 'denied.txt'))).toBe(false);
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('carries the gate`s REASON out with the deny rather than discarding it', async () => {
    // A fail-closed deny that says only "the permission gate failed" is indistinguishable
    // from an ordinary user denial. The realistic trigger is a backing-store outage, which
    // denies EVERY tool call in the session — so if the cause is dropped here, an operator
    // sees a session where the model is refused everything and nothing anywhere says why.
    // This protocol carries a deny MESSAGE, so that is where the reason goes.
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const confirmTool = (): Promise<ToolPermissionDecision> =>
      Promise.reject(new Error('permission store unreachable'));

    const harness = new ClaudeCodeCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cc',
        agentSnapshot: {},
        confirmTool,
      });
      await harness.submit(userMessage('run echo should-not-run > gate-reason.txt'));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      const result = sink.events.find((e) => e.kind === 'agent.tool_result');
      expect(
        JSON.stringify(result?.payload ?? {}),
        'the gate`s cause must ride out on the deny — otherwise a store outage denies the ' +
          'whole session with no explanation anywhere',
      ).toContain('permission store unreachable');
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('reports a CLI death BETWEEN turns as a NON-terminal error, in the idle wording', async () => {
    // Two things are being separated here, and conflating them cost real information.
    //
    // The read loop's `finally` used to report an abnormal exit only when a turn was parked,
    // because its message says "…before the turn completed" and (at the time) ANY trailing
    // `agent.error` disarmed the runner loop's own terminal fault for the NEXT turn — so a
    // stale one would be read as that turn's cause and suppress its real one. Dropping the
    // fault fixed the suppression by DESTROYING the evidence: an OOM-kill between turns was
    // then reported nowhere at all, in all five native-CLI harnesses (the three with a
    // logger included — the guard short-circuited before the message was ever built).
    //
    // `AgentEvent.terminal` made the gate unnecessary. Suppression now keys on the FLAG, not
    // on the kind, so an unflagged `agent.error` disarms nothing. The fault is therefore
    // always computed and always emitted; only the flag is withheld, and the wording says
    // `while the session was idle` because the other sentence would be false.
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});

    // Turn 1 succeeds: the CLI answers the user frame with its terminal `result`.
    const { cli, die } = scriptedNativeCliWithDeath({
      respond: (frame) =>
        frame.includes('"user"')
          ? [JSON.stringify({ type: 'result', subtype: 'success' })]
          : undefined,
      // …and when it later dies, it dies ABNORMALLY (nobody asked it to).
      exit: { code: 137, signal: null },
    });

    const harness = new ClaudeCodeCliHarness(fakeHarnessOptions(sandbox, { launch: () => cli }));
    const sink = collectEvents(harness);
    try {
      await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cc', agentSnapshot: {} });
      await harness.submit(userMessage('say hi'));
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      // The turn is over and nothing is parked. NOW the child dies of an OOM.
      die();
      // The read loop unwinds and ends the event stream; the collector settles with it.
      await sink.done;

      const errors = sink.events.filter((e) => e.kind === 'agent.error');
      expect(
        errors.map((e) => (e.payload as { message?: string }).message ?? ''),
        'an idle death must still be REPORTED — dropping it was the only record that the ' +
          'child had died at all',
      ).toEqual(['claude-code CLI exited with code 137 while the session was idle']);
      // The load-bearing half: NOT flagged terminal. The flag is what suppresses the runner
      // loop's own terminal fault, so a flagged stale error would become the NEXT turn's
      // stated cause and bury that turn's real one — which is what the old, information-
      // destroying gate was there to prevent. Asserted on the flag directly, because
      // nothing else reads it here: the conformance gate detects a WRONG flag only
      // indirectly, by counting a doubled `agent.error` on a turn the loop also faulted.
      expect(
        errors[0]?.terminal,
        'an idle death is not why any turn ended — flagging it terminal makes it the NEXT ' +
          'turn`s stated cause and suppresses the real one',
      ).toBeUndefined();
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('bounds a turn by DEFAULT_TURN_DEADLINE_MS when no turnTimeoutMs is configured', async () => {
    // The other half of the bound nothing checked: that the DEFAULT is applied at all.
    // Every cell that reaches a deadline supplies its own `turnTimeoutMs` in milliseconds,
    // so replacing the `??` fallback with `Infinity` — a harness whose turns are once again
    // unbounded in production — left the whole suite green. This is the only case that
    // constructs the harness WITHOUT an override, so it is the only one that can see it.
    vi.useFakeTimers();
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const harness = new ClaudeCodeCliHarness(
      fakeHarnessOptions(sandbox, { launch: () => muteNativeCli() }),
    );
    const sink = collectEvents(harness);
    try {
      await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cc', agentSnapshot: {} });
      let settled = false;
      const turn = harness.submit(userMessage('say hi')).then(() => {
        settled = true;
      });

      // Just short of the default the turn is still parked — the bound is a real WAIT on the
      // CLI, not a give-up that would truncate every turn the moment it started.
      await vi.advanceTimersByTimeAsync(DEFAULT_TURN_DEADLINE_MS - 1);
      expect(settled, 'the default bound must not expire early').toBe(false);

      // Past it the turn ends, naming the bound it blew — so the value, and the fact that it
      // was applied, are both observable.
      await vi.advanceTimersByTimeAsync(2);
      expect(
        settled,
        'a turn with no override must be bounded by the DEFAULT — still parked past it means ' +
          'the fallback is some other value, or none',
      ).toBe(true);
      await turn;
      expect(
        sink.events
          .filter((e) => e.kind === 'agent.error')
          .map((e) => (e.payload as { message?: string }).message ?? ''),
      ).toEqual([`claude-code CLI did not complete the turn within ${DEFAULT_TURN_DEADLINE_MS}ms`]);
    } finally {
      await harness.stop('client.archived');
      vi.useRealTimers();
      await sink.done;
    }
  });

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

    const harness = new ClaudeCodeCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cc',
        agentSnapshot: {},
        confirmTool,
      });
      await harness.submit(userMessage('run echo should-not-run > gate-threw.txt'));
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
    // A missing / misnamed `claude` on the sandbox PATH is the likeliest production
    // misconfiguration here, and it is invisible from the stream alone: the launcher hands
    // back a process whose stdout is already ended, which is byte-for-byte what a CLI that
    // started and exited immediately looks like. The read loop must read the launcher's
    // `failure` and put it on the wire.
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const launchError = new Error('spawn ENOENT');

    const harness = new ClaudeCodeCliHarness(
      fakeHarnessOptions(sandbox, { launch: () => failedNativeCli(launchError) }),
    );
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cc',
        agentSnapshot: {},
      });
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.error'));

      const failure = sink.events.find((e) => e.kind === 'agent.error');
      expect((failure?.payload as { message?: string }).message).toMatch(
        /claude-code CLI failed to launch: spawn ENOENT/,
      );
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('emits the launch argv the CLI was booted with: stream-json + bridge mcp-config + stdio permissions', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const argvOut = join(workdir, 'argv.json');

    const harness = new ClaudeCodeCliHarness(
      fakeHarnessOptions(sandbox, {
        // Append a probe flag so the fake CLI records the full argv it received.
        cliExtraArgs: ['--argv-out', argvOut],
      }),
    );
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cc',
        agentSnapshot: {},
        // A gate is wired → the argv must use --permission-prompt-tool stdio.
        confirmTool: async () => ({ behavior: 'allow', updatedInput: {} }),
      });
      await harness.submit(userMessage('say ok'));
      await waitFor(() => existsSync(argvOut));
      const argv = JSON.parse(readFileSync(argvOut, 'utf8')) as string[];

      expect(argv).toContain('--output-format');
      expect(argv[argv.indexOf('--output-format') + 1]).toBe('stream-json');
      expect(argv).toContain('--input-format');
      expect(argv).toContain('--verbose');
      // The bridge was wired as the orca MCP server.
      const mcpRaw = argv[argv.indexOf('--mcp-config') + 1];
      const mcp = JSON.parse(mcpRaw ?? '{}') as { mcpServers?: Record<string, unknown> };
      expect(mcp.mcpServers?.['orca']).toBeDefined();
      // A gate → stdio permission routing (not skip-permissions).
      expect(argv[argv.indexOf('--permission-prompt-tool') + 1]).toBe('stdio');
      expect(argv).not.toContain('--dangerously-skip-permissions');
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('stop() tears the CLI down and ends the event stream', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const harness = new ClaudeCodeCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cc', agentSnapshot: {} });
    await harness.submit(userMessage('say hello'));
    await harness.stop('client.archived');
    // The events() async-iterable completes (no hang) after stop.
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

    // A gate that PARKS forever until released — mimics a turn awaiting a human verdict
    // that never arrives, so the turn is unwound only by interrupt (the SDK abort race in
    // production; here the harness settles the parked turn locally).
    let release: ((d: ToolPermissionDecision) => void) | undefined;
    const confirmTool = (): Promise<ToolPermissionDecision> =>
      new Promise<ToolPermissionDecision>((resolve) => {
        release = resolve;
      });

    const harness = new ClaudeCodeCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cc',
        agentSnapshot: {},
        confirmTool,
      });
      // Drive a tool turn that will park on the gate; do NOT await (it blocks on the park).
      const turn = harness.submit(userMessage('run echo blocked > blocked.txt'));
      // Wait until the gate is reached (the requires_action signal surfaced).
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.requires_action'));
      // Interrupt: the blocked turn must unwind (submit resolves) and the harness stays alive.
      harness.interrupt();
      await Promise.race([
        turn,
        new Promise<void>((_, reject) =>
          setTimeout(() => reject(new Error('interrupt did not unwind the parked turn')), 8000),
        ),
      ]);
      // The harness is still alive: a follow-up chat turn drives normally and produces its
      // assistant message (identified by its unique text, so it can't be confused with any
      // trailing event from the interrupted turn).
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

  it('releases a parked gate when the CLI withdraws the request (control_cancel_request → deny)', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});

    // A gate that PARKS FOREVER — so the ONLY thing that can release this park is the
    // CLI's `control_cancel_request` (no verdict ever arrives). If the cancel were
    // ignored (the pre-fix behavior), the park would hang and the turn would never end.
    const confirmTool = (): Promise<ToolPermissionDecision> =>
      new Promise<ToolPermissionDecision>(() => {
        /* never resolves */
      });

    const harness = new ClaudeCodeCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({
        workspaceId: 'ws_1',
        sessionId: 'ses_cc',
        agentSnapshot: {},
        confirmTool,
      });
      // `cancelrun` raises can_use_tool then immediately withdraws it.
      await harness.submit(userMessage('cancelrun echo hi'));
      // The turn COMPLETES only because the harness released the parked gate as a deny on
      // the cancel and replied — otherwise the fake CLI never gets its control_response.
      await waitFor(() => sink.events.some((e) => e.kind === 'agent.turn_completed'));

      // The requires_action was emitted (the gate was reached)…
      expect(sink.events.some((e) => e.kind === 'agent.requires_action')).toBe(true);
      // …and the withdrawal surfaced as a DENY tool_result (the release wrote a deny back).
      const result = sink.events.find((e) => e.kind === 'agent.tool_result');
      expect((result?.payload as { is_error?: boolean }).is_error).toBe(true);
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('resolves the harness through the registry factory + buildSessionStartInput projection', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const registry = new ProviderRegistry();
    registerClaudeCodeProvider(registry, {
      modelDefault: 'claude-sonnet-4-5',
      cliCommand: process.execPath,
      cliPrefixArgs: [FAKE_CLI],
      bridgeCommand: TSX,
      bridgePrefixArgs: [BRIDGE_ENTRY],
    });
    const snapshot = {
      provider: 'claude-code',
      model: { provider: 'anthropic', id: 'claude-sonnet-4-5' },
      system: 'You are Orca.',
      allowed_tool_names: ['bash'],
      allowed_mcp_server_names: [],
      tool_permissions: {},
      egress: {
        mode: 'gateway',
        gateway: { llm_base_url: 'https://gw/anthropic', llm_jwt: 'jwt' },
      },
    } as unknown as RunnerSnapshot;

    const harness = registry.build(snapshot, {
      workspaceId: 'ws_1',
      sessionId: 'ses_cc',
      sandbox,
    });
    const input = buildSessionStartInput(snapshot, {
      workspaceId: 'ws_1',
      sessionId: 'ses_cc',
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

  it("wires a snapshot's skills_plugin_dir to the CLI as --plugin-dir (snapshot → argv, end-to-end)", async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const argvOut = join(workdir, 'argv-skills.json');
    const skillsDir = '/snap/skills';

    // A snapshot that STAGED a skills bundle (carries `skills_plugin_dir`) — the field
    // must flow snapshot → buildSessionStartInput → harness → the CLI's `--plugin-dir`.
    const snapshot = {
      provider: 'claude-code',
      model: { provider: 'anthropic', id: 'claude-sonnet-4-5' },
      system: 'You are Orca.',
      skills_plugin_dir: skillsDir,
      allowed_tool_names: ['bash'],
      allowed_mcp_server_names: [],
      tool_permissions: {},
      egress: null,
    } as unknown as RunnerSnapshot;

    const harness = new ClaudeCodeCliHarness(
      fakeHarnessOptions(sandbox, { cliExtraArgs: ['--argv-out', argvOut] }),
    );
    const input = buildSessionStartInput(snapshot, {
      workspaceId: 'ws_1',
      sessionId: 'ses_cc',
      sandbox,
    });
    // The projection carried the plugin-dir onto the boot context (not swallowed).
    expect(input.agentSnapshot.skills_plugin_dir).toBe(skillsDir);

    const sink = collectEvents(harness);
    try {
      await harness.start(input);
      await harness.submit(userMessage('say ok'));
      await waitFor(() => existsSync(argvOut));
      const argv = JSON.parse(readFileSync(argvOut, 'utf8')) as string[];
      // The staged skills dir reached the headless CLI as `--plugin-dir <dir>`.
      expect(argv[argv.indexOf('--plugin-dir') + 1]).toBe(skillsDir);
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('omits --plugin-dir when the snapshot stages no skills bundle', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const argvOut = join(workdir, 'argv-no-skills.json');

    // A snapshot with NO `skills_plugin_dir` (skills delivered only as the composed
    // prompt + allowlist) → the projection omits it and the CLI gets no `--plugin-dir`.
    const snapshot = {
      provider: 'claude-code',
      model: { provider: 'anthropic', id: 'claude-sonnet-4-5' },
      system: 'You are Orca.',
      allowed_tool_names: ['bash'],
      allowed_mcp_server_names: [],
      tool_permissions: {},
      egress: null,
    } as unknown as RunnerSnapshot;

    const harness = new ClaudeCodeCliHarness(
      fakeHarnessOptions(sandbox, { cliExtraArgs: ['--argv-out', argvOut] }),
    );
    const input = buildSessionStartInput(snapshot, {
      workspaceId: 'ws_1',
      sessionId: 'ses_cc',
      sandbox,
    });
    expect(input.agentSnapshot.skills_plugin_dir).toBeUndefined();

    const sink = collectEvents(harness);
    try {
      await harness.start(input);
      await harness.submit(userMessage('say ok'));
      await waitFor(() => existsSync(argvOut));
      const argv = JSON.parse(readFileSync(argvOut, 'utf8')) as string[];
      expect(argv).not.toContain('--plugin-dir');
    } finally {
      await harness.stop('client.archived');
      await sink.done;
    }
  }, 30_000);

  it('fails fast on a root-less (cloud-only) sandbox handle rather than emitting a --root-less bridge', async () => {
    // A cloud-only handle is a valid SandboxHandle that exposes NO `rootDir()`: its
    // work-dir is not a host path the bridge child can bind `node:fs`/`child_process` to.
    // `start` must reject with an actionable error at THIS layer, not emit a --root-less
    // bridge command the bridge entrypoint would reject opaquely deep in the CLI child.
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
      // NOTE: no `rootDir()` and no `spawn()` — a cloud handle exposes neither.
    };

    const harness = new ClaudeCodeCliHarness(fakeHarnessOptions(cloudHandle));
    await expect(
      harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cc', agentSnapshot: {} }),
    ).rejects.toThrow(/root-dir-backed sandbox|sbx_cloud_only|rootDir/);
    // Nothing was launched — teardown is a clean no-op (no CLI child to kill).
    await harness.stop('client.archived');
  }, 15_000);
});

// The tmux leg proves the same provider over the local-attach transport (the real
// target): the bridge child reconstructs a tmux-backed handle over the shared socket,
// so sys_terminal_* would also resolve. Self-skips without `tmux`.
const maybeTmux = tmuxAvailable() ? describe : describe.skip;
maybeTmux('claude-code provider — over the tmux local-attach transport', () => {
  it('runs a tool through the bridge on a tmux-backed sandbox', async () => {
    const { TmuxSandboxRuntime } = await import('../../src/sandbox/tmux-sandbox.js');
    const runtime = new TmuxSandboxRuntime();
    const sandbox = await runtime.acquire({});
    const sandboxRoot = rootDirOf(sandbox);
    const harness = new ClaudeCodeCliHarness(fakeHarnessOptions(sandbox));
    const sink = collectEvents(harness);
    try {
      await harness.start({ workspaceId: 'ws_1', sessionId: 'ses_cc', agentSnapshot: {} });
      const marker = 'tmux-bridge-marker';
      await harness.submit(userMessage(`run echo ${marker} > tmux-tool.txt && echo ${marker}`));
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
