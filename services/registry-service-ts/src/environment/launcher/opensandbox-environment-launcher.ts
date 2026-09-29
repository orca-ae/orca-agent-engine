// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// OpenSandbox environment-launcher backend.
//
// An Environment is an OpenSandbox sandbox (container); its worker is
// `environment-worker` exec'd detached inside that same sandbox. All of the
// actual lifecycle (provision/startWorker/terminate/isRunning/resume) is the
// shared `CloudEnvironmentLauncher` — see that module's doc for why one
// implementation backs every cloud provider. This file only supplies the
// OpenSandbox-specific config surface (`OpenSandboxRuntimeOptions` plumbing,
// with the same defaults harness-server's config.ts already uses for the
// SAME env var names) and the exported class name `launcherFactory` selects
// on `'opensandbox'`.

import { OpenSandboxRuntime, type OpenSandboxRuntimeOptions } from '@orca/cloud-sandbox';
import type { SandboxRuntime } from '@orca/sandbox-runtime';
import { CloudEnvironmentLauncher } from './cloud-environment-launcher.js';
import type { EnvironmentLauncher, StartWorkerOptions } from './types.js';

/** Default sandbox lifetime, seconds. Mirrors harness-server's `OPEN_SANDBOX_TIMEOUT_SECONDS` default. */
export const DEFAULT_TIMEOUT_SECONDS = 1800;
/** Default `useServerProxy`. Mirrors harness-server's `OPEN_SANDBOX_USE_SERVER_PROXY` default. */
export const DEFAULT_USE_SERVER_PROXY = true;
/** Default lifecycle/execd request timeout, seconds. Mirrors harness-server's `OPEN_SANDBOX_REQUEST_TIMEOUT_SECONDS` default. */
export const DEFAULT_REQUEST_TIMEOUT_SECONDS = 30;

/** Construction options for {@link OpenSandboxEnvironmentLauncher}. */
export interface OpenSandboxEnvironmentLauncherOptions {
  /** OpenSandbox server host[:port] or full URL (`OPEN_SANDBOX_DOMAIN`). */
  readonly domain: string;
  /** Protocol used when `domain` has no scheme (`OPEN_SANDBOX_PROTOCOL`). */
  readonly protocol: 'http' | 'https';
  /** Optional OpenSandbox API key (`OPEN_SANDBOX_API_KEY`). */
  readonly apiKey?: string;
  /** Sandbox image the Environment box boots from (`OPEN_SANDBOX_ENVIRONMENT_IMAGE` — distinct from harness-server's own `OPEN_SANDBOX_IMAGE`; see `launcher-factory.ts`). */
  readonly image: string;
  /** Sandbox lifetime, seconds. Defaults to {@link DEFAULT_TIMEOUT_SECONDS}. */
  readonly timeoutSeconds?: number;
  /** Whether to reach the sandbox through the OpenSandbox server proxy. Defaults to {@link DEFAULT_USE_SERVER_PROXY}. */
  readonly useServerProxy?: boolean;
  /** Lifecycle/execd request timeout, seconds. Defaults to {@link DEFAULT_REQUEST_TIMEOUT_SECONDS}. */
  readonly requestTimeoutSeconds?: number;
  /** Argv used to exec `environment-worker` INSIDE the box. */
  readonly workerLaunchCommand: readonly string[];
  /** Argv handed to the exec'd worker as its own `RUNNER_LAUNCH_COMMAND`. */
  readonly runnerLaunchCommand: readonly string[];
  /** Workspace dir inside the box. Defaults to {@link DEFAULT_CLOUD_WORKSPACE_DIR} (see cloud-environment-launcher.ts). */
  readonly workspaceDir?: string;
  /**
   * Injectable `SandboxRuntime` seam for tests — bypasses the real
   * OpenSandbox HTTP client entirely (mirrors
   * `LocalEnvironmentLauncherOptions.spawner`). Defaults to a real
   * `OpenSandboxRuntime` built from the fields above.
   */
  readonly runtime?: SandboxRuntime;
}

/**
 * Build the `OpenSandboxRuntimeOptions` the production `OpenSandboxRuntime`
 * is constructed with. Exported as a pure function so the domain/protocol/
 * image/timeout plumbing is unit-testable without a real OpenSandbox server.
 */
export function openSandboxRuntimeOptions(
  opts: Pick<
    OpenSandboxEnvironmentLauncherOptions,
    | 'domain'
    | 'protocol'
    | 'apiKey'
    | 'image'
    | 'timeoutSeconds'
    | 'useServerProxy'
    | 'requestTimeoutSeconds'
  >,
): OpenSandboxRuntimeOptions {
  return {
    domain: opts.domain,
    protocol: opts.protocol,
    image: opts.image,
    timeoutSeconds: opts.timeoutSeconds ?? DEFAULT_TIMEOUT_SECONDS,
    useServerProxy: opts.useServerProxy ?? DEFAULT_USE_SERVER_PROXY,
    requestTimeoutSeconds: opts.requestTimeoutSeconds ?? DEFAULT_REQUEST_TIMEOUT_SECONDS,
    ...(opts.apiKey !== undefined ? { apiKey: opts.apiKey } : {}),
  };
}

/**
 * OpenSandbox backend: an Environment is an OpenSandbox sandbox, and its
 * worker is `environment-worker` exec'd detached inside that sandbox.
 * Delegates the entire `EnvironmentLauncher` contract to
 * {@link CloudEnvironmentLauncher}.
 */
export class OpenSandboxEnvironmentLauncher implements EnvironmentLauncher {
  private readonly inner: CloudEnvironmentLauncher;

  constructor(opts: OpenSandboxEnvironmentLauncherOptions) {
    const runtime = opts.runtime ?? new OpenSandboxRuntime(openSandboxRuntimeOptions(opts));
    this.inner = new CloudEnvironmentLauncher({
      runtime,
      workerLaunchCommand: opts.workerLaunchCommand,
      runnerLaunchCommand: opts.runnerLaunchCommand,
      ...(opts.workspaceDir !== undefined ? { workspaceDir: opts.workspaceDir } : {}),
      label: 'OpenSandbox',
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
