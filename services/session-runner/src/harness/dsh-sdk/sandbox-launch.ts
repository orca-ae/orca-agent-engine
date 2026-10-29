// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import {
  DshSdkWorker,
  inspectDshInstallation,
  type DshWorkerLaunchOptions,
} from '@orca/dsh-harness';
import { LocalSandboxRuntime, type SandboxRuntime } from '../../sandbox/seam.js';
import type { RunnerSnapshot } from '../../snapshot.js';
import { validateDshBootstrapSnapshot } from './snapshot.js';

export function assertDshBootstrapHost(): void {
  if (
    process.platform !== 'linux' ||
    process.arch !== 'x64' ||
    process.versions.node !== '22.23.2' ||
    process.getuid?.() === undefined ||
    process.getuid() === 0
  )
    throw new Error('DSH bootstrap requires Linux amd64, Node 22.23.2 and a non-root account');
  const require = createRequire(import.meta.url);
  const sandboxRequire = createRequire(require.resolve('@orca/sandbox-runtime'));
  const entry = sandboxRequire.resolve('@anthropic-ai/sandbox-runtime');
  const installed = JSON.parse(readFileSync(join(dirname(dirname(entry)), 'package.json'), 'utf8'));
  if (installed.version !== '0.0.50') throw new Error('DSH bootstrap requires SRT npm 0.0.50');
  // Runner selection probes PATH srt availability; execution imports the pinned npm SandboxManager.
  execFileSync('srt', ['--version'], { stdio: 'pipe', timeout: 10000 });
  execFileSync('bwrap', ['--version'], { stdio: 'pipe', timeout: 10000 });
}

/** Initialize and retire the real native profile only. No submit/tool dispatch is exposed. */
export async function probeDshSandboxBootstrap(
  snapshot: RunnerSnapshot,
  runtime: SandboxRuntime,
  sessionId: string,
): Promise<void> {
  validateDshBootstrapSnapshot(snapshot);
  assertDshBootstrapHost();
  if (!(runtime instanceof LocalSandboxRuntime))
    throw new Error('DSH bootstrap requires Local+SRT; fallback is disabled');
  const installedPaths = inspectDshInstallation();
  const sandbox = await runtime
    .withReadOnlyRuntimePaths([dirname(process.execPath), ...installedPaths])
    .acquire({ target: 'self_hosted' });
  let worker: DshSdkWorker | undefined;
  let failed = false;
  let failure: unknown;
  try {
    const root = (sandbox as typeof sandbox & { rootDir?(): string }).rootDir?.();
    if (!root || !sandbox.spawn) throw new Error('DSH bootstrap requires native sandbox spawn');
    const privateRoot = join(root, '.orca-dsh');
    await mkdir(privateRoot, { mode: 0o700 });
    const launch: DshWorkerLaunchOptions['launch'] = async (input) => {
      if (input.cwd !== root) throw new Error('DSH bootstrap cwd must equal the sandbox root');
      // env -i also strips any operator passthrough before the native process starts.
      const quote = (value: string): string => "'" + value.replaceAll("'", "'\\''") + "'";
      const command = [
        '/usr/bin/env',
        '-i',
        ...Object.entries(input.env).map(([key, value]) => `${key}=${value}`),
        input.executable,
        ...input.args,
      ]
        .map(quote)
        .join(' ');
      const child = await sandbox.spawn!(command, {
        // Sandbox virtual / maps to the acquired host root, not the host filesystem root.
        cwd: '/',
        env: { BASH_ENV: '/dev/null', ENV: '/dev/null' },
      });
      if (!(child.stdout instanceof Readable) || !(child.stdin instanceof Writable)) {
        child.kill('SIGKILL');
        await child.exited;
        throw new Error('DSH bootstrap requires Node stdio streams');
      }
      return { ...child, stdout: child.stdout, stdin: child.stdin };
    };
    worker = new DshSdkWorker(() => {}, { homeRoot: privateRoot, launch });
    const gateway = (snapshot.egress as { gateway: { llm_base_url: string; llm_jwt: string } })
      .gateway;
    await worker.handle({
      type: 'start',
      root,
      sessionId,
      modelProvider: 'deepseek',
      model: snapshot.model.id,
      system: snapshot.system,
      apiKey: gateway.llm_jwt,
      dshGatewayUrl: gateway.llm_base_url,
      tools: [],
    });
  } catch (error) {
    failed = true;
    failure = error;
  }
  try {
    // destroy only removes the workdir; preserve it if worker retirement is unconfirmed.
    if (worker) await worker.close();
    await sandbox.destroy();
  } catch (cleanupError) {
    if (failed)
      throw new AggregateError([failure, cleanupError], 'DSH bootstrap and cleanup failed');
    throw cleanupError;
  }
  if (failed) throw failure;
}
