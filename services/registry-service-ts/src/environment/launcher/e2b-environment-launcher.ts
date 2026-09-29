// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// E2B environment-launcher backend.
//
// An Environment is an E2B sandbox (micro-VM); its worker is
// `environment-worker` exec'd detached inside that same sandbox. All of the
// actual lifecycle (provision/startWorker/terminate/isRunning/resume) is the
// shared `CloudEnvironmentLauncher` — see that module's doc for why one
// implementation backs every cloud provider. This file only supplies the
// E2B-specific config surface (`E2BSandboxRuntimeOptions` plumbing) and the
// exported class name `launcherFactory` selects on `'e2b'`.

import { E2BSandboxRuntime, type E2BSandboxRuntimeOptions } from '@orca/cloud-sandbox';
import type { SandboxRuntime } from '@orca/sandbox-runtime';
import { CloudEnvironmentLauncher } from './cloud-environment-launcher.js';
import type { EnvironmentLauncher, StartWorkerOptions } from './types.js';

/** Construction options for {@link E2BEnvironmentLauncher}. */
export interface E2BEnvironmentLauncherOptions {
  /** E2B API key (`E2B_API_KEY`). */
  readonly apiKey: string;
  /**
   * Custom E2B template id the Environment box boots from
   * (`E2B_ENVIRONMENT_TEMPLATE_ID` — distinct from harness-server's own
   * `E2B_TEMPLATE_ID`; see `launcher-factory.ts`); SDK default if unset.
   */
  readonly templateId?: string;
  /** Optional E2B server URL override (e.g. for self-hosted E2B). */
  readonly baseURL?: string;
  /** Argv used to exec `environment-worker` INSIDE the box. */
  readonly workerLaunchCommand: readonly string[];
  /** Argv handed to the exec'd worker as its own `RUNNER_LAUNCH_COMMAND`. */
  readonly runnerLaunchCommand: readonly string[];
  /** Workspace dir inside the box. Defaults to {@link DEFAULT_CLOUD_WORKSPACE_DIR} (see cloud-environment-launcher.ts). */
  readonly workspaceDir?: string;
  /**
   * Injectable `SandboxRuntime` seam for tests — bypasses the real E2B SDK
   * entirely (mirrors `LocalEnvironmentLauncherOptions.spawner`). Defaults
   * to a real `E2BSandboxRuntime` built from `apiKey`/`templateId`/`baseURL`.
   */
  readonly runtime?: SandboxRuntime;
}

/**
 * Build the `E2BSandboxRuntimeOptions` the production `E2BSandboxRuntime` is
 * constructed with. Exported as a pure function so the apiKey/templateId/
 * baseURL plumbing is unit-testable without ever touching the E2B SDK (the
 * SDK is only imported — dynamically — inside `E2BSandboxRuntime.acquire`).
 */
export function e2bSandboxRuntimeOptions(
  opts: Pick<E2BEnvironmentLauncherOptions, 'apiKey' | 'templateId' | 'baseURL'>,
): E2BSandboxRuntimeOptions {
  return {
    apiKey: opts.apiKey,
    ...(opts.templateId !== undefined ? { templateId: opts.templateId } : {}),
    ...(opts.baseURL !== undefined ? { baseURL: opts.baseURL } : {}),
  };
}

/**
 * E2B backend: an Environment is an E2B sandbox, and its worker is
 * `environment-worker` exec'd detached inside that sandbox. Delegates the
 * entire `EnvironmentLauncher` contract to {@link CloudEnvironmentLauncher}.
 */
export class E2BEnvironmentLauncher implements EnvironmentLauncher {
  private readonly inner: CloudEnvironmentLauncher;

  constructor(opts: E2BEnvironmentLauncherOptions) {
    const runtime = opts.runtime ?? new E2BSandboxRuntime(e2bSandboxRuntimeOptions(opts));
    this.inner = new CloudEnvironmentLauncher({
      runtime,
      workerLaunchCommand: opts.workerLaunchCommand,
      runnerLaunchCommand: opts.runnerLaunchCommand,
      ...(opts.workspaceDir !== undefined ? { workspaceDir: opts.workspaceDir } : {}),
      label: 'E2B',
    });
  }

  provision(name: string): Promise<string> {
    return this.inner.provision(name);
  }

  startWorker(environmentId: string, opts: StartWorkerOptions): Promise<void> {
    return this.inner.startWorker(environmentId, opts);
  }

  terminate(environmentId: string): Promise<void> {
    return this.inner.terminate(environmentId);
  }

  isRunning(environmentId: string): Promise<boolean> {
    return this.inner.isRunning(environmentId);
  }

  resume(environmentId: string): Promise<void> {
    return this.inner.resume(environmentId);
  }
}
