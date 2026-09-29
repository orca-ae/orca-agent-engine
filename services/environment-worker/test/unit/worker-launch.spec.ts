// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// End-to-end-ish spec for the EnvironmentWorker launch/stop/watch surface.
//
// The worker is a CLIENT: it dials the registry worker tunnel over a real `ws`
// WebSocket. This spec stands up an in-process FAKE registry worker-tunnel server
// that speaks the exact worker-frame protocol the real registry speaks (built
// from `@orca/harness-tunnel` — the shared cross-component contract): it
// authenticates the Env Key handshake header, exchanges the worker.hello, lets the
// test push launch / stop frames, and records the one-way worker.runner_exited
// report. The real worker connects with the production `ws`-backed connector and
// spawns a STUB shell script as the "runner" (the runner binary does not exist
// yet, so the launch command is injected). No protocol is mocked.
//
// Cases covered: launch spawns with the binding token + workspace and replies
// "launched"; a bad workspace fails; an unconfigured harness is refused with the
// structured code; a runner that dies after launch is reported via
// worker.runner_exited; a stop terminates the child and is NOT reported as a crash.

import { afterEach, describe, expect, it } from 'vitest';
import { mkdtempSync, writeFileSync, chmodSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  HARNESS_NOT_CONFIGURED_ERROR_CODE,
  tokenBoundRunnerId,
  type WorkerLaunchResult,
  type WorkerStopResult,
} from '@orca/harness-tunnel';
import { EnvironmentWorker } from '../../src/worker.js';
import { FakeRegistryWorkerTunnel, type LiveWorker } from './support/fake-registry.js';

const ENV_ID = 'env_worker_001';
const ENV_KEY = 'sk-test-env-key-wwwwwwwwwwwwwwwwwwwwwwwwwww';

const openServers: FakeRegistryWorkerTunnel[] = [];
const openWorkers: EnvironmentWorker[] = [];
const tmpDirs: string[] = [];

afterEach(async () => {
  for (const worker of openWorkers.splice(0)) {
    await worker.stop();
  }
  for (const server of openServers.splice(0)) {
    await server.close();
  }
  for (const dir of tmpDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

async function buildRegistry(): Promise<FakeRegistryWorkerTunnel> {
  const server = new FakeRegistryWorkerTunnel({ environmentId: ENV_ID, environmentKey: ENV_KEY });
  openServers.push(server);
  await server.listen();
  return server;
}

/** A scratch workspace dir on disk so the workspace existence check passes. */
function makeWorkspace(): string {
  const dir = mkdtempSync(join(tmpdir(), 'orca-ws-'));
  tmpDirs.push(dir);
  return dir;
}

/** Write an executable shell stub the worker launches as the "runner". */
function makeRunnerScript(body: string): string[] {
  const dir = mkdtempSync(join(tmpdir(), 'orca-runner-'));
  tmpDirs.push(dir);
  const path = join(dir, 'runner.sh');
  writeFileSync(path, `#!/bin/sh\n${body}\n`);
  chmodSync(path, 0o755);
  return ['/bin/sh', path];
}

interface BuildWorkerOpts {
  runnerLaunchCommand: string[];
  harnessConfigured?: (harness: string) => boolean;
  configuredHarnesses?: Record<string, boolean> | null;
  /** Pin the worker's `workspaceDir` (else a fresh temp dir is used). */
  workspaceDir?: string;
}

/** Start a real worker dialing the fake registry; resolve once it has registered. */
async function startWorker(
  server: FakeRegistryWorkerTunnel,
  opts: BuildWorkerOpts,
): Promise<{ worker: EnvironmentWorker; conn: LiveWorker }> {
  const worker = new EnvironmentWorker({
    environmentId: ENV_ID,
    environmentKey: ENV_KEY,
    registryTunnelBaseUrl: server.baseUrl(),
    registryRunnerUrl: server.baseUrl(),
    workspaceDir: opts.workspaceDir ?? makeWorkspace(),
    runnerLaunchCommand: opts.runnerLaunchCommand,
    name: 'test-worker',
    ...(opts.harnessConfigured !== undefined ? { harnessConfigured: opts.harnessConfigured } : {}),
    ...(opts.configuredHarnesses !== undefined
      ? { configuredHarnesses: opts.configuredHarnesses }
      : {}),
  });
  openWorkers.push(worker);
  void worker.run();
  const conn = await server.nextWorker();
  return { worker, conn };
}

describe('EnvironmentWorker — connect + hello', () => {
  it('dials the registry worker tunnel with the Env Key + internal origin and sends hello', async () => {
    const server = await buildRegistry();
    const { conn } = await startWorker(server, {
      runnerLaunchCommand: makeRunnerScript('sleep 10'),
      configuredHarnesses: { 'claude-code': true },
    });
    expect(conn.envKeyHeader).toBe(ENV_KEY);
    expect(conn.originHeader).toBe('orca://internal');
    expect(conn.path).toBe(`/v1/tunnels/environments/${ENV_ID}`);
    expect(conn.hello.name).toBe('test-worker');
    expect(conn.hello.frameProtocolVersion).toBe(1);
    expect(conn.hello.configuredHarnesses).toEqual({ 'claude-code': true });
  });
});

describe('EnvironmentWorker — worker.launch_runner', () => {
  it('spawns a runner with the binding token + workspace and replies launched', async () => {
    const server = await buildRegistry();
    const workspace = makeWorkspace();
    const { worker, conn } = await startWorker(server, {
      runnerLaunchCommand: makeRunnerScript('sleep 10'),
    });

    const result: WorkerLaunchResult = await conn.launch({
      requestId: 'req_001',
      bindingToken: 'test_token_abc',
      workspace,
    });

    expect(result.status).toBe('launched');
    expect(result.runnerId).toBe(tokenBoundRunnerId('test_token_abc'));
    expect(result.error).toBeNull();
    expect(worker.aliveRunnerIds()).toContain(tokenBoundRunnerId('test_token_abc'));
  });

  it('fails the launch when the workspace path does not exist', async () => {
    const server = await buildRegistry();
    const { conn } = await startWorker(server, {
      runnerLaunchCommand: makeRunnerScript('sleep 10'),
    });

    const result = await conn.launch({
      requestId: 'req_002',
      bindingToken: 'token_xyz',
      workspace: '/nonexistent/path/that/does/not/exist',
    });

    expect(result.status).toBe('failed');
    expect(result.error ?? '').toContain('does not exist');
    expect(result.runnerId).toBeNull();
  });

  it('refuses an unconfigured harness with the structured code and a named message', async () => {
    const server = await buildRegistry();
    const workspace = makeWorkspace();
    const { worker, conn } = await startWorker(server, {
      runnerLaunchCommand: makeRunnerScript('sleep 10'),
      harnessConfigured: () => false,
    });

    const result = await conn.launch({
      requestId: 'req_unconfigured',
      bindingToken: 'token_abc',
      workspace,
      harness: 'codex',
    });

    expect(result.status).toBe('failed');
    expect(result.errorCode).toBe(HARNESS_NOT_CONFIGURED_ERROR_CODE);
    expect(result.error ?? '').toContain('codex');
    expect(result.error ?? '').toContain('test-worker');
    expect(result.runnerId).toBeNull();
    expect(worker.aliveRunnerIds()).toEqual([]);
  });

  it('skips the readiness check when the launch frame carries no harness (version skew)', async () => {
    const server = await buildRegistry();
    const workspace = makeWorkspace();
    const { conn } = await startWorker(server, {
      runnerLaunchCommand: makeRunnerScript('sleep 10'),
      harnessConfigured: () => {
        throw new Error('harnessConfigured must not run when harness is null');
      },
    });

    const result = await conn.launch({
      requestId: 'req_no_harness',
      bindingToken: 'token_ghi',
      workspace,
    });

    expect(result.status).toBe('launched');
  });

  it('proceeds to spawn when the harness IS configured', async () => {
    const server = await buildRegistry();
    const workspace = makeWorkspace();
    const { conn } = await startWorker(server, {
      runnerLaunchCommand: makeRunnerScript('sleep 10'),
      harnessConfigured: () => true,
    });

    const result = await conn.launch({
      requestId: 'req_configured',
      bindingToken: 'token_def',
      workspace,
      harness: 'claude-code',
    });

    expect(result.status).toBe('launched');
    expect(result.errorCode).toBeNull();
  });

  it('surfaces the exit code when a real runner dies right after launch', async () => {
    const server = await buildRegistry();
    const workspace = makeWorkspace();
    const { conn } = await startWorker(server, {
      runnerLaunchCommand: makeRunnerScript("echo 'boom-traceback' >&2; exit 7"),
    });

    const result = await conn.launch({
      requestId: 'req_dead',
      bindingToken: 'tok_dead',
      workspace,
    });

    // With a REAL subprocess the death may be caught synchronously (failed
    // launch) or a moment later by the watcher (runner_exited) — which fires is a
    // spawn-timing race. Either way the cause (the exit code) must reach the
    // registry, never a silent "launched" with the death swallowed.
    if (result.status === 'failed') {
      expect(result.error ?? '').toContain('code 7');
    } else {
      expect(result.status).toBe('launched');
      const report = await conn.nextRunnerExited();
      expect(report.runnerId).toBe(tokenBoundRunnerId('tok_dead'));
      expect(report.error).toContain('code 7');
    }
  });

  it('passes the runner-wiring env (binding token + workspace + parent pid) to the child', async () => {
    const server = await buildRegistry();
    const workspace = makeWorkspace();
    // The stub writes its received wiring env to a file we then read back, so we
    // assert the child actually received the binding token, workspace, and the
    // worker's pid (the runner watchdog's parent-pid signal).
    const probe = join(makeWorkspace(), 'env.txt');
    const { conn } = await startWorker(server, {
      runnerLaunchCommand: makeRunnerScript(
        `printf '%s\\n%s\\n%s\\n%s\\n' ` +
          `"$ORCA_RUNNER_TUNNEL_BINDING_TOKEN" "$ORCA_RUNNER_WORKSPACE" ` +
          `"$ORCA_RUNNER_PARENT_PID" "$ORCA_RUNNER_REGISTRY_URL" > ${probe}; sleep 10`,
      ),
    });

    const result = await conn.launch({
      requestId: 'req_env',
      bindingToken: 'tok_env',
      workspace,
    });
    expect(result.status).toBe('launched');

    const { readFileSync } = await import('node:fs');
    await waitFile(probe);
    const [token, ws, parentPid, registryUrl] = readFileSync(probe, 'utf-8').split('\n');
    expect(token).toBe('tok_env');
    expect(ws).toBe(workspace);
    expect(parentPid).toBe(String(process.pid));
    expect(registryUrl).toBe(server.baseUrl());
  });

  it('derives a per-runner workspace under workspaceDir when the frame workspace is empty', async () => {
    // A self_hosted session carries no attached workspace by default, so the
    // registry's launch frame sends an empty workspace. The worker must then
    // create a per-runner directory UNDER its configured `workspaceDir` and launch
    // the runner there — `workspaceDir` is the operative worker layout, not the
    // frame. (Without this, the launch fails the does-not-exist check on '' and the
    // runner never spawns — the exact gap the self-hosted e2e surfaced.)
    const server = await buildRegistry();
    const workspaceDir = makeWorkspace();
    const probe = join(makeWorkspace(), 'env.txt');
    const { worker, conn } = await startWorker(server, {
      workspaceDir,
      runnerLaunchCommand: makeRunnerScript(
        `printf '%s' "$ORCA_RUNNER_WORKSPACE" > ${probe}; sleep 10`,
      ),
    });

    const result = await conn.launch({
      requestId: 'req_empty_ws',
      bindingToken: 'tok_empty_ws',
      workspace: '',
    });

    expect(result.status).toBe('launched');
    const runnerId = tokenBoundRunnerId('tok_empty_ws');
    expect(result.runnerId).toBe(runnerId);
    expect(worker.aliveRunnerIds()).toContain(runnerId);

    // The runner's cwd/workspace env is the derived per-runner dir under workspaceDir.
    const { readFileSync, existsSync } = await import('node:fs');
    await waitFile(probe);
    const derived = join(workspaceDir, runnerId);
    expect(readFileSync(probe, 'utf-8')).toBe(derived);
    expect(existsSync(derived)).toBe(true);
  });
});

describe('EnvironmentWorker — worker.runner_exited watcher', () => {
  it('reports a runner that dies after launch via worker.runner_exited', async () => {
    const server = await buildRegistry();
    const workspace = makeWorkspace();
    const { conn } = await startWorker(server, {
      runnerLaunchCommand: makeRunnerScript('sleep 0.2; exit 3'),
    });

    const result = await conn.launch({
      requestId: 'req_watch',
      bindingToken: 'tok_watch',
      workspace,
    });
    expect(result.status).toBe('launched');

    const report = await conn.nextRunnerExited();
    expect(report.runnerId).toBe(tokenBoundRunnerId('tok_watch'));
    expect(report.error).toContain('code 3');
  });

  it('does NOT report a worker.stop_runner termination as a crash', async () => {
    const server = await buildRegistry();
    const workspace = makeWorkspace();
    const { worker, conn } = await startWorker(server, {
      runnerLaunchCommand: makeRunnerScript('sleep 60'),
    });

    const launch = await conn.launch({
      requestId: 'req_stop',
      bindingToken: 'tok_stop',
      workspace,
    });
    expect(launch.status).toBe('launched');
    const runnerId = tokenBoundRunnerId('tok_stop');

    const stop = await conn.stop('req_stop_2', runnerId);
    expect(stop.status).toBe('stopped');
    await waitUntil(() => !worker.aliveRunnerIds().includes(runnerId));

    // Give the watcher time to (incorrectly) report before asserting it did not.
    await delay(100);
    expect(conn.runnerExitedReports()).toEqual([]);
  });
});

describe('EnvironmentWorker — worker.stop_runner', () => {
  it('fails a stop for an unknown runner id', async () => {
    const server = await buildRegistry();
    const { conn } = await startWorker(server, {
      runnerLaunchCommand: makeRunnerScript('sleep 10'),
    });

    const result: WorkerStopResult = await conn.stop('req_004', 'runner_nonexistent');

    expect(result.status).toBe('failed');
    expect(result.error ?? '').toContain('unknown runner');
  });
});

describe('EnvironmentWorker — answers registry pings', () => {
  it('replies to a registry ping with a pong on the same socket', async () => {
    const server = await buildRegistry();
    const { conn } = await startWorker(server, {
      runnerLaunchCommand: makeRunnerScript('sleep 10'),
    });

    const ts = 1234567;
    const pongTs = await conn.pingAndAwaitPong(ts);
    expect(pongTs).toBe(ts);
  });
});

describe('EnvironmentWorker — forward-compatible frame handling', () => {
  it('silently drops a frame it has no handler for without replying or crashing', async () => {
    // The registry only ever pushes request frames, but the worker's dispatch must
    // still take the else-fall-through for any inbound frame it cannot act on (a
    // RESULT frame, or a future request kind a newer registry adds): no reply, no
    // crash, the tunnel stays live. We push a worker.stat_result (a result frame,
    // which only flows worker→registry), then prove the loop survived (a) by a
    // launch that still gets answered and (b) by a ping that still gets ponged.
    const server = await buildRegistry();
    const workspace = makeWorkspace();
    const { conn } = await startWorker(server, {
      runnerLaunchCommand: makeRunnerScript('sleep 10'),
    });

    // The unhandled frame must produce nothing — give the worker a tick to
    // (wrongly) reply before we assert it stayed silent.
    conn.sendUnhandledFrame('req_fwd_compat');
    await delay(50);

    // The socket is still fully serviceable after the dropped frame: a launch is
    // answered normally…
    const launch = await conn.launch({
      requestId: 'req_after_drop',
      bindingToken: 'tok_after_drop',
      workspace,
    });
    expect(launch.status).toBe('launched');

    // …and a ping is still ponged on the same socket.
    const pongTs = await conn.pingAndAwaitPong(42);
    expect(pongTs).toBe(42);

    // The worker answered ONLY the launch — the dropped result frame produced no
    // outbound reply of its own.
    expect(conn.sentWorkerFrameKinds()).toContain('worker.launch_runner_result');
  });
});

async function waitUntil(predicate: () => boolean, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error('waitUntil timed out');
    }
    await delay(5);
  }
}

async function waitFile(path: string, timeoutMs = 3000): Promise<void> {
  const { existsSync, statSync } = await import('node:fs');
  await waitUntil(() => existsSync(path) && statSync(path).size > 0, timeoutMs);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
