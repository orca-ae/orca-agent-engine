// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Entry point for @orca/environment-worker.
//
// The environment-worker is a long-running CLIENT process (it does NOT expose an
// HTTP server). A self-hosted operator runs it; it dials the registry host
// tunnel using the environment id + key and spawns session runners on demand.
//
// This wires the resolved configuration to the {@link EnvironmentWorker} host
// loop and runs it: print the startup banner, run the worker, and on a PERMANENT
// connection failure (`EnvironmentConnectError`: the upgrade rejected with a
// permanent `4xx`, such as an outdated registry without the route) print the
// actionable cause and exit non-zero instead of hanging. A credential the registry
// refuses after the upgrade (a `4004` close) is an ordinary disconnect and is
// retried with backoff.
// SIGINT / SIGTERM stop the worker cleanly (its runners are terminated on the
// way out).

import { loadConfig, type EnvironmentWorkerConfig } from './config.js';
import { EnvironmentConnectError } from './errors.js';
import { EnvironmentWorker } from './worker.js';

/**
 * Construct and run the environment worker for the resolved `config`.
 *
 * The returned promise resolves when the worker shuts down cleanly (a signal
 * stops it) and rejects if it terminates with an error. A permanent connection
 * failure surfaces as an {@link EnvironmentConnectError}.
 */
export async function startWorker(config: EnvironmentWorkerConfig): Promise<void> {
  const worker = new EnvironmentWorker({
    environmentId: config.environmentId,
    ...(config.environmentKey !== undefined ? { environmentKey: config.environmentKey } : {}),
    ...(config.environmentToken !== undefined ? { environmentToken: config.environmentToken } : {}),
    ...(config.workerId !== undefined ? { workerId: config.workerId } : {}),
    registryTunnelBaseUrl: config.registryTunnelBaseUrl,
    registryRunnerUrl: config.registryRunnerUrl,
    workspaceDir: config.workspaceDir,
    runnerLaunchCommand: config.runnerLaunchCommand,
    name: config.name,
    logger: {
      info: (obj, msg) => console.log('[environment-worker]', msg ?? '', obj),
      warn: (obj, msg) => console.warn('[environment-worker]', msg ?? '', obj),
      error: (obj, msg) => console.error('[environment-worker]', msg ?? '', obj),
    },
  });

  // A signal stops the worker: cancel the run loop's next reconnect by closing
  // out, then terminate live runners. `run()` resolves once stopped.
  let stopping = false;
  const stop = (): void => {
    if (stopping) {
      return;
    }
    stopping = true;
    // A rejection here is a teardown failure (a runner that would not die); it
    // must be reported, not become an unhandled rejection that Node turns into a
    // crash with no context.
    void worker.stop().catch((error: unknown) => {
      console.error('[environment-worker] error while stopping:', error);
      process.exitCode = 1;
    });
  };
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);

  console.log(
    `[environment-worker] connecting to ${config.registryTunnelBaseUrl} as ` +
      `${JSON.stringify(config.name)} (environment ${config.environmentId})`,
  );
  await worker.run();
}

export async function main(): Promise<void> {
  const config = loadConfig();
  try {
    await startWorker(config);
  } catch (error) {
    if (error instanceof EnvironmentConnectError) {
      // Fail loud: a permanent connection failure must not look like the process
      // is still working. Print the cause + fix, then exit non-zero.
      console.error(
        `\n[environment-worker] could not connect to ${config.registryTunnelBaseUrl}.\n${error.message}`,
      );
      process.exitCode = 1;
      return;
    }
    throw error;
  }
}

// Run when invoked directly (e.g. `node dist/main.js`), but not when imported by
// tests. `import.meta.url` resolves to this module's file URL.
const isEntrypoint =
  process.argv[1] !== undefined && import.meta.url === `file://${process.argv[1]}`;
if (isEntrypoint) {
  main().catch((error: unknown) => {
    console.error('[environment-worker] fatal:', error);
    process.exitCode = 1;
  });
}
