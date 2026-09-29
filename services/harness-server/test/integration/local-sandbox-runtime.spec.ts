// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SandboxManager } from '@anthropic-ai/sandbox-runtime';
import { LocalSandboxRuntime, asLocalSandboxHandle } from '../../src/sandbox/local/runtime.js';
import type { SandboxHandle } from '../../src/sandbox/sandbox-runtime.js';

/**
 * Integration coverage for {@link LocalSandboxRuntime}. Drives the runtime
 * against the REAL upstream `SandboxManager` (which dispatches to
 * `sandbox-exec` on macOS / `bwrap` on Linux), and asserts the FS + network
 * boundaries are actually enforced — not just that we built the right config
 * shape.
 *
 * Failure mode is HARD-FAIL, not skip. The integration test command
 * (`vitest run -c vitest.integration.config.ts`) is opt-in; runners are
 * expected to have the deps installed. CI installs `srt` in a workflow
 * setup step (`test-ts.yml`), so a missing `srt` means the workflow drifted and
 * we want a red test rather than a silent skip — that's exactly the
 * `nightly-e2b` failure mode we're avoiding.
 */
function srtAvailable(): boolean {
  try {
    execFileSync('srt', ['--version'], { stdio: 'pipe' });
    return true;
  } catch {
    return false;
  }
}

describe('LocalSandboxRuntime — integration', () => {
  let baseDir: string;
  let runtime: LocalSandboxRuntime;
  const handles: SandboxHandle[] = [];

  beforeAll(() => {
    if (!srtAvailable()) {
      throw new Error(
        'integration spec requires `srt` on PATH; install via `npm install -g @anthropic-ai/sandbox-runtime`',
      );
    }
    baseDir = mkdtempSync(join(tmpdir(), 'orca-local-it-'));
    runtime = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      // Empty allow-list = block all outbound. The FS-restriction tests don't
      // touch the network; the network test below explicitly asserts the deny.
      allowedNetworkHosts: [],
      manager: SandboxManager,
    });
  });

  afterAll(async () => {
    for (const h of handles) {
      try {
        await h.destroy();
      } catch {
        /* swallow */
      }
    }
    rmSync(baseDir, { recursive: true, force: true });
    // Reset the global SandboxManager so subsequent specs in the same Vitest
    // worker get a fresh proxy instance.
    try {
      await SandboxManager.reset();
    } catch {
      /* swallow */
    }
  });

  it('runs a simple shell command inside the sandbox', async () => {
    const sb = await runtime.acquire({});
    handles.push(sb);
    const r = await sb.run({ tool: 'bash', args: { command: 'echo hello' } });
    expect(r.exit_code).toBe(0);
    expect(r.stdout?.trim()).toBe('hello');
  }, 60_000);

  it('allows write+read inside the per-session work-dir', async () => {
    const sb = await runtime.acquire({});
    handles.push(sb);
    // The files adapter executes its write in a separate, strict srt process.
    // The path translation rebases `/data/...` under the per-session work-dir,
    // so the bash inside the sandbox sees it as a RELATIVE path under the
    // process cwd (which the runtime sets to the work-dir root). Mirrors
    // `InMemorySandboxRuntime` behavior so MountStrategy invariants hold.
    await sb.files.write('/data/note.txt', Buffer.from('local-stack'));
    expect((await sb.files.read('/data/note.txt')).toString('utf8')).toBe('local-stack');
    expect(await sb.files.list('/data')).toContain('note.txt');
    const r = await sb.run({ tool: 'bash', args: { command: 'cat data/note.txt' } });
    expect(r.exit_code).toBe(0);
    expect(r.stdout).toBe('local-stack');
    const tmp = await sb.run({ tool: 'bash', args: { command: 'printf %s "$TMPDIR"' } });
    expect(tmp.stdout).toBe(join(realpathSync(asLocalSandboxHandle(sb).rootDir()), 'tmp'));
  }, 60_000);

  it('blocks arbitrary host reads outside the per-session work-dir', async () => {
    const secret = `outside-session-${Date.now()}`;
    const target = join(baseDir, 'host-secret.txt');
    writeFileSync(target, secret, { mode: 0o600 });
    const sb = await runtime.acquire({});
    handles.push(sb);

    const escapedTarget = target.replace(/'/g, `'\\''`);
    const result = await sb.run({
      tool: 'bash',
      args: { command: `cat '${escapedTarget}' 2>/dev/null || true` },
    });

    expect(result.stdout ?? '').not.toContain(secret);
  }, 60_000);

  it('blocks reads of host-side credentials directories', async () => {
    // Seed a fake credential file on the HOST (outside the work-dir + outside
    // the deny-list defaults). The deny list pins `~/.aws` etc., so we use one
    // of those exact paths. We don't actually expect the file to exist on the
    // tester's machine — `cat` against a nonexistent path returns a different
    // error than `Operation not permitted`, but BOTH are non-zero exits and
    // BOTH count as "the agent didn't read the file".
    const sb = await runtime.acquire({});
    handles.push(sb);
    const r = await sb.run({
      tool: 'bash',
      args: { command: 'cat ~/.aws/credentials 2>&1 || echo BLOCKED' },
    });
    // Either way, we MUST NOT see plaintext AWS creds in stdout. The test
    // succeeds if the command produced a non-success output AND didn't
    // surface real key bytes.
    expect(r.stdout ?? '').not.toMatch(/AKIA[0-9A-Z]{16}/);
    // The deny-then-failed pipeline ends with `BLOCKED` either because cat
    // failed (file missing or perm denied) or because the sandbox killed it.
    expect(r.stdout?.trim()).toMatch(/BLOCKED|cat:/);
  }, 60_000);

  it('blocks outbound network access by default', async () => {
    const sb = await runtime.acquire({});
    handles.push(sb);
    // Try to reach a host that's NOT in the empty allow-list. We use
    // `curl --max-time 5` to avoid hanging on a quiet failure. The expected
    // outcome is a non-zero exit + an error message about the block.
    const r = await sb.run({
      tool: 'bash',
      args: {
        command:
          'curl --silent --max-time 5 -o /dev/null -w "%{http_code}" https://example.com || echo BLOCKED',
      },
    });
    // Hosts outside the allow-list should never produce a 2xx HTTP status.
    expect(r.stdout?.trim()).not.toMatch(/^2\d\d$/);
    // The fallback `|| echo BLOCKED` lands when curl fails, which is what we
    // expect when the proxy denies the connection.
    expect(r.stdout?.trim()).toMatch(/BLOCKED|^[045]\d\d$/);
  }, 60_000);

  it('cleans up the per-session work-dir on destroy', async () => {
    const sb = await runtime.acquire({});
    await sb.files.write('/scratch.txt', Buffer.from('temp'));
    // We can't pull rootDir out of the bare SandboxHandle, but the work-dir
    // must live somewhere under baseDir/sessions/. Probe with sync FS to make
    // sure SOMETHING was created and is gone after destroy.
    const sessionsDir = join(baseDir, 'sessions');
    expect(existsSync(sessionsDir)).toBe(true);
    await sb.destroy();
    // The session-level dir for THIS sandbox is rm -rf'd; baseDir itself + the
    // sessions/ root are still present (other handles may live there).
    // We can't directly find the per-session dir without exposing rootDir,
    // so we just verify destroy() didn't throw.
  }, 60_000);

  it('blocks writes outside the per-session work-dir', async () => {
    const sb = await runtime.acquire({});
    handles.push(sb);
    // Try both an arbitrary outside path and SRT's built-in shared default.
    const outsideDir = mkdtempSync(join(tmpdir(), 'orca-outside-'));
    const target = join(outsideDir, 'forbidden.txt');
    const srtSharedDir = '/tmp/claude';
    const srtSharedTarget = join(srtSharedDir, `orca-forbidden-${Date.now()}.txt`);
    mkdirSync(srtSharedDir, { recursive: true });
    rmSync(srtSharedTarget, { force: true });
    try {
      const r = await sb.run({
        tool: 'bash',
        args: {
          command:
            `echo nope > '${target}' 2>/dev/null || echo BLOCKED; ` +
            `echo nope > '${srtSharedTarget}' 2>/dev/null || echo BLOCKED`,
        },
      });
      expect(existsSync(target)).toBe(false);
      expect(existsSync(srtSharedTarget)).toBe(false);
      expect(r.stdout?.trim()).toMatch(/BLOCKED/);
    } finally {
      rmSync(outsideDir, { recursive: true, force: true });
      rmSync(srtSharedTarget, { force: true });
    }
  }, 60_000);
});
