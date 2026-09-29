// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// LocalEnvironmentLauncher — lifecycle contract + startWorker wiring, driven
// through an injectable FAKE process spawner so no real process is ever
// spawned (mirrors services/environment-worker's own fake-spawner unit style
// in test/unit/worker-runners.spec.ts).
//
// Two concerns:
//  - Lifecycle contract: provision returns a stable id; after
//    startWorker, isRunning is true; after terminate, isRunning is false;
//    double-terminate is safe; resume is unsupported (Local environments are
//    not resumable).
//  - startWorker wiring: the spawned command + the exact env
//    services/environment-worker/src/config.ts requires to dial back
//    (registry tunnel URL, environment id, token, identity) are all present
//    and correct — proving the worker *would* connect, without connecting one.

import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  ENVIRONMENT_ID_ENV_VAR,
  ENVIRONMENT_WORKER_ID_ENV_VAR,
  ENVIRONMENT_WORKER_NAME_ENV_VAR,
  LocalEnvironmentLauncher,
  ORCA_ENVIRONMENT_TOKEN_ENV_VAR,
  REGISTRY_TUNNEL_BASE_URL_ENV_VAR,
  RUNNER_LAUNCH_COMMAND_ENV_VAR,
  WORKSPACE_DIR_ENV_VAR,
} from '../../src/environment/launcher/local-environment-launcher.js';
import type {
  ProcessSpawner,
  SpawnedProcess,
  SpawnRequest,
} from '../../src/environment/launcher/process-spawner.js';
import type { StartWorkerOptions } from '../../src/environment/launcher/types.js';

/** A fake spawned process whose exit is driven by the test, never a real OS process. */
class FakeSpawnedProcess implements SpawnedProcess {
  readonly pid = Math.floor(Math.random() * 1_000_000) + 2;
  exitCode: number | null = null;
  terminateCalls = 0;
  killCalls = 0;

  poll(): number | null {
    return this.exitCode;
  }

  terminate(): void {
    this.terminateCalls += 1;
    // A real SIGTERM ends the child; reflect that immediately so terminate()'s
    // await-exit resolves without needing a fake clock.
    this.exitCode ??= 143;
  }

  kill(): void {
    this.killCalls += 1;
    this.exitCode ??= 137;
  }

  wait(): Promise<void> {
    return Promise.resolve();
  }
}

/** A spawner that hands back pre-seeded fake processes in order and records every request. */
class FakeSpawner implements ProcessSpawner {
  readonly spawned: Array<{ request: SpawnRequest; proc: FakeSpawnedProcess }> = [];
  constructor(private readonly queue: FakeSpawnedProcess[] = []) {}

  spawn(request: SpawnRequest): SpawnedProcess {
    const proc = this.queue.shift() ?? new FakeSpawnedProcess();
    this.spawned.push({ request, proc });
    return proc;
  }
}

const WORKER_LAUNCH_COMMAND = ['node', '/opt/orca/services/environment-worker/dist/main.js'];
const RUNNER_LAUNCH_COMMAND = ['node', '/opt/orca/services/session-runner/dist/main.js'];

const START_OPTS: StartWorkerOptions = {
  token: 'sk-env-token-abc',
  registryTunnelUrl: 'wss://registry.example.com',
  environmentId: 'env_test123456789',
  identity: { workerId: 'worker_id_001', workerName: 'test-worker-name' },
};

let baseDir: string;

beforeEach(() => {
  baseDir = mkdtempSync(join(tmpdir(), 'orca-local-launcher-'));
});

afterEach(() => {
  rmSync(baseDir, { recursive: true, force: true });
});

function buildLauncher(spawner: ProcessSpawner): LocalEnvironmentLauncher {
  return new LocalEnvironmentLauncher({
    baseDir,
    workerLaunchCommand: WORKER_LAUNCH_COMMAND,
    runnerLaunchCommand: RUNNER_LAUNCH_COMMAND,
    spawner,
  });
}

describe('LocalEnvironmentLauncher — lifecycle contract', () => {
  it('provision returns a non-empty id and creates the environment work-dir on disk', async () => {
    const launcher = buildLauncher(new FakeSpawner());
    const id = await launcher.provision('test-env');
    expect(typeof id).toBe('string');
    expect(id.length).toBeGreaterThan(0);
    expect(existsSync(join(baseDir, id))).toBe(true);
  });

  it('returns a distinct id for each provision call', async () => {
    const launcher = buildLauncher(new FakeSpawner());
    const first = await launcher.provision('a');
    const second = await launcher.provision('b');
    expect(first).not.toBe(second);
  });

  it('is not running before startWorker is called', async () => {
    const launcher = buildLauncher(new FakeSpawner());
    const id = await launcher.provision('test-env');
    expect(await launcher.isRunning(id)).toBe(false);
  });

  it('is running after startWorker, and stops running after terminate', async () => {
    const launcher = buildLauncher(new FakeSpawner());
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);
    expect(await launcher.isRunning(id)).toBe(true);

    await launcher.terminate(id);
    expect(await launcher.isRunning(id)).toBe(false);
  });

  it('is safe to terminate twice', async () => {
    const launcher = buildLauncher(new FakeSpawner());
    const id = await launcher.provision('test-env');
    await launcher.startWorker(id, START_OPTS);

    await launcher.terminate(id);
    await expect(launcher.terminate(id)).resolves.toBeUndefined();
    expect(await launcher.isRunning(id)).toBe(false);
  });

  it('is safe to terminate an environment that was never started', async () => {
    const launcher = buildLauncher(new FakeSpawner());
    const id = await launcher.provision('test-env');
    await expect(launcher.terminate(id)).resolves.toBeUndefined();
  });

  it('rejects resume — local environments are not resumable', async () => {
    const launcher = buildLauncher(new FakeSpawner());
    const id = await launcher.provision('test-env');
    await expect(launcher.resume?.(id)).rejects.toThrow(/resum/i);
  });

  it('rejects startWorker for an id that was never provisioned', async () => {
    const launcher = buildLauncher(new FakeSpawner());
    await expect(launcher.startWorker('nonexistent-id', START_OPTS)).rejects.toThrow(
      /nonexistent-id/,
    );
  });
});

describe('LocalEnvironmentLauncher — startWorker wiring (fake spawner)', () => {
  it('spawns environment-worker with the configured launch command', async () => {
    const spawner = new FakeSpawner();
    const launcher = buildLauncher(spawner);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    expect(spawner.spawned).toHaveLength(1);
    expect(spawner.spawned[0]!.request.command).toEqual(WORKER_LAUNCH_COMMAND);
  });

  it('spawns the child with cwd set to the provisioned environment directory', async () => {
    const spawner = new FakeSpawner();
    const launcher = buildLauncher(spawner);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    expect(spawner.spawned[0]!.request.cwd).toBe(join(baseDir, id));
  });

  it('wires the exact env environment-worker/config.ts requires to dial back', async () => {
    // registry tunnel URL, environment id, and the token — the three
    // dial-back essentials the spawned worker needs to connect back.
    const spawner = new FakeSpawner();
    const launcher = buildLauncher(spawner);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    const env = spawner.spawned[0]!.request.env;
    expect(env[REGISTRY_TUNNEL_BASE_URL_ENV_VAR]).toBe(START_OPTS.registryTunnelUrl);
    expect(env[ENVIRONMENT_ID_ENV_VAR]).toBe(START_OPTS.environmentId);
  });

  it('wires the per-launch token as ORCA_ENVIRONMENT_TOKEN — the managed-auth path, not the self-hosted ENVIRONMENT_KEY', async () => {
    // The token must NOT land in ENVIRONMENT_KEY_ENV_VAR (the self-hosted
    // operator-provisioned secret slot). This launcher always runs the MANAGED
    // path (StartWorkerOptions always carries the per-launch Environment Token
    // the launch lifecycle mints — it is never a self-hosted Env Key), so
    // environment-worker/config.ts's managed-auth
    // fork must see it via ORCA_ENVIRONMENT_TOKEN_ENV_VAR: absent that,
    // config.ts falls through to REQUIRING ENVIRONMENT_KEY (never set here),
    // and the worker fails to boot.
    const spawner = new FakeSpawner();
    const launcher = buildLauncher(spawner);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    const env = spawner.spawned[0]!.request.env;
    expect(env[ORCA_ENVIRONMENT_TOKEN_ENV_VAR]).toBe(START_OPTS.token);
    // Never leak the token into the self-hosted Env-Key slot — a worker that
    // reads BOTH `ORCA_ENVIRONMENT_TOKEN` (managed) and a same-valued
    // `ENVIRONMENT_KEY` would be tolerated by config.ts's fork today by
    // accident, not by design; asserting its absence keeps the two auth
    // paths from silently blurring together.
    expect(env['ENVIRONMENT_KEY']).toBeUndefined();
  });

  it('wires the identity (workerName visible to environment-worker, workerId not dropped)', async () => {
    const spawner = new FakeSpawner();
    const launcher = buildLauncher(spawner);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    const env = spawner.spawned[0]!.request.env;
    // environment-worker/config.ts's ENVIRONMENT_WORKER_NAME_ENV_VAR is the
    // identity field it actually reads today.
    expect(env[ENVIRONMENT_WORKER_NAME_ENV_VAR]).toBe(START_OPTS.identity.workerName);
    // workerId has no environment-worker/config.ts var yet, but the launcher
    // must still carry it through rather than silently drop it.
    expect(env[ENVIRONMENT_WORKER_ID_ENV_VAR]).toBe(START_OPTS.identity.workerId);
  });

  it('wires WORKSPACE_DIR to the provisioned environment directory', async () => {
    const spawner = new FakeSpawner();
    const launcher = buildLauncher(spawner);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    expect(spawner.spawned[0]!.request.env[WORKSPACE_DIR_ENV_VAR]).toBe(join(baseDir, id));
  });

  it('wires RUNNER_LAUNCH_COMMAND as the configured argv space-joined (config.ts parses on whitespace)', async () => {
    const spawner = new FakeSpawner();
    const launcher = buildLauncher(spawner);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    expect(spawner.spawned[0]!.request.env[RUNNER_LAUNCH_COMMAND_ENV_VAR]).toBe(
      RUNNER_LAUNCH_COMMAND.join(' '),
    );
  });

  it('does not drop an inherited base-env var (the worker still needs PATH etc.)', async () => {
    const spawner = new FakeSpawner();
    const launcher = new LocalEnvironmentLauncher({
      baseDir,
      workerLaunchCommand: WORKER_LAUNCH_COMMAND,
      runnerLaunchCommand: RUNNER_LAUNCH_COMMAND,
      spawner,
      baseEnv: { PATH: '/usr/bin:/bin', HOME: '/home/orca' },
    });
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    const env = spawner.spawned[0]!.request.env;
    expect(env.PATH).toBe('/usr/bin:/bin');
    expect(env.HOME).toBe('/home/orca');
  });
});
