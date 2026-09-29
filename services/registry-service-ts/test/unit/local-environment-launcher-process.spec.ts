// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// LocalEnvironmentLauncher — real ChildProcessSpawner smoke test.
//
// The rest of the launcher's unit suite (local-environment-launcher.spec.ts)
// drives the launcher through a FAKE spawner so command/env wiring is
// deterministic and no real process is ever spawned. This file is the
// complementary real-process proof (mirrors environment-worker's own split:
// test/unit/worker-runners.spec.ts uses a fake spawner, test/unit/worker-launch.spec.ts
// spawns a real stub subprocess) — it exercises the PRODUCTION
// `ChildProcessSpawner` seam end to end against a real, harmless, short-lived
// OS process, proving the launcher provisions + starts + terminates for real.
// No registry or environment-worker connection is involved (that is A3's
// end-to-end concern) — the "worker" here is a trivial idle Node process
// standing in for environment-worker's process lifecycle only.

import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { LocalEnvironmentLauncher } from '../../src/environment/launcher/local-environment-launcher.js';
import type { StartWorkerOptions } from '../../src/environment/launcher/types.js';

const tmpDirs: string[] = [];

afterEach(() => {
  for (const dir of tmpDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function makeBaseDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'orca-local-launcher-real-'));
  tmpDirs.push(dir);
  return dir;
}

const START_OPTS: StartWorkerOptions = {
  token: 'sk-env-token-real',
  registryTunnelUrl: 'wss://registry.example.com',
  environmentId: 'env_real_test',
  identity: { workerId: 'worker_real_001', workerName: 'real-spawn-test-worker' },
};

describe('LocalEnvironmentLauncher — real ChildProcessSpawner', () => {
  it('provisions a work-dir, starts a real idle process, and reports it running', async () => {
    const baseDir = makeBaseDir();
    // The production spawner defaults in when no `spawner` option is given —
    // this is the ChildProcessSpawner itself under test, not a fake.
    const launcher = new LocalEnvironmentLauncher({
      baseDir,
      workerLaunchCommand: [process.execPath, '-e', 'setInterval(() => {}, 1000)'],
      runnerLaunchCommand: ['node', '/opt/orca/services/session-runner/dist/main.js'],
    });

    const id = await launcher.provision('real-spawn-env');
    expect(existsSync(join(baseDir, id))).toBe(true);
    expect(await launcher.isRunning(id)).toBe(false);

    await launcher.startWorker(id, START_OPTS);
    expect(await launcher.isRunning(id)).toBe(true);

    await launcher.terminate(id);
    expect(await launcher.isRunning(id)).toBe(false);
  });

  it('is safe to terminate a real process twice', async () => {
    const baseDir = makeBaseDir();
    const launcher = new LocalEnvironmentLauncher({
      baseDir,
      workerLaunchCommand: [process.execPath, '-e', 'setInterval(() => {}, 1000)'],
      runnerLaunchCommand: ['node', '/opt/orca/services/session-runner/dist/main.js'],
    });
    const id = await launcher.provision('real-spawn-env-2');
    await launcher.startWorker(id, START_OPTS);

    await launcher.terminate(id);
    await expect(launcher.terminate(id)).resolves.toBeUndefined();
  });
});
