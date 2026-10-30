// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { EnvironmentSpec } from './sandbox-runtime.js';
import {
  ENVIRONMENT_PACKAGES_EXTENSION,
  ENVIRONMENT_PACKAGES_ENV,
  ENVIRONMENT_PACKAGE_TIMEOUT_MS,
  prepareEnvironmentPackagePlan,
  environmentPackageFailureMessage,
} from './environment-packages.js';

const DEFAULT_RESOURCE_LIMITS: Record<string, string> = { cpu: '1', memory: '2Gi' };
const DEFAULT_ENTRYPOINT = ['tail', '-f', '/dev/null'];
const EXECD_ISOLATION_EXTENSION = 'bootstrap.execd.isolation';
const FUSE_DEVICE_EXTENSION = 'orca.fuse.device';
export const OPENSANDBOX_READY_TIMEOUT_MS = 60_000;
const READY_POLL_MS = 500;
// Each package lifecycle read also inspects Pod init status. Keep persistent
// failure detection prompt without listing Pods twice per second per acquire.
const PACKAGE_READY_POLL_MS = 2_000;

/**
 * Build the POST /sandboxes request body for OpenSandbox `acquire`. Exported as
 * a pure function so it can be unit-tested without provisioning a real sandbox
 * (the production `acquire` calls this so test + prod share one implementation).
 *
 * Merges `env.harnessEnv` (gateway LLM vars) into
 * the container env map, honors a per-acquire `env.image` / `env.exposePorts`
 * override (falling back to the constructor image), and treats undefined
 * `harnessEnv` as `{}`. A per-acquire entrypoint travels with an in-sandbox
 * harness image instead of inheriting the constructor image's entrypoint.
 */
export function buildOpenSandboxAcquireRequest(
  env: EnvironmentSpec,
  opts: {
    image: string;
    entrypoint?: string[];
    timeoutSeconds: number;
    resourceLimits?: Record<string, string>;
    enableFuse?: boolean;
    execdAccessToken?: string;
  },
): {
  body: Record<string, unknown>;
  packagePlan: ReturnType<typeof prepareEnvironmentPackagePlan>;
} {
  // Merge harnessEnv (gateway LLM vars, etc.) into the container env map.
  // Treat undefined as {} per the backward-compat contract.
  const containerEnv: Record<string, string> = { ...(env.harnessEnv ?? {}) };
  const packagePlan = prepareEnvironmentPackagePlan(env.packages, env.networking);
  delete containerEnv['EXECD_ACCESS_TOKEN'];
  if (opts.execdAccessToken) containerEnv['EXECD_ACCESS_TOKEN'] = opts.execdAccessToken;
  if (packagePlan && !opts.execdAccessToken)
    throw new Error('environment packages require authenticated execd access');
  delete containerEnv[ENVIRONMENT_PACKAGES_ENV];
  if (packagePlan) containerEnv[ENVIRONMENT_PACKAGES_ENV] = JSON.stringify(packagePlan.packages);
  // Per-acquire image override takes precedence over the constructor image.
  const image = env.image ?? opts.image;
  const body: Record<string, unknown> = {
    image: { uri: image },
    entrypoint: env.entrypoint ?? opts.entrypoint ?? DEFAULT_ENTRYPOINT,
    resourceLimits: opts.resourceLimits ?? DEFAULT_RESOURCE_LIMITS,
    secureAccess: false,
    timeout: opts.timeoutSeconds + (packagePlan ? ENVIRONMENT_PACKAGE_TIMEOUT_MS / 1000 : 0),
    metadata: { owner: 'orca-managed-agents' },
    env: containerEnv,
    // OpenSandbox grants only this acquired workload the namespace/security
    // context needed by bubblewrap. FUSE additionally needs the Orca-patched
    // server to admit /dev/fuse for this workload; a device mount alone is
    // insufficient on runtimes that enforce the container device cgroup.
    extensions: {
      [EXECD_ISOLATION_EXTENSION]: 'enable',
      ...(packagePlan ? { [ENVIRONMENT_PACKAGES_EXTENSION]: packagePlan.extension } : {}),
      ...(opts.enableFuse ? { [FUSE_DEVICE_EXTENSION]: 'enable' } : {}),
    },
  };
  if (env.exposePorts?.length) {
    body['exposePorts'] = env.exposePorts;
  }
  return { body, packagePlan };
}

/** Shared by both adapters; callbacks retain their endpoint and HTTP ownership. */
export async function waitUntilOpenSandboxReady(opts: {
  id: string;
  timeoutMs?: number;
  getLifecycle?: () => Promise<unknown>;
  ping: () => Promise<{ ok: boolean; status: number }>;
}): Promise<void> {
  const deadline = Date.now() + (opts.timeoutMs ?? OPENSANDBOX_READY_TIMEOUT_MS);
  const pollMs = opts.getLifecycle ? PACKAGE_READY_POLL_MS : READY_POLL_MS;
  const pause = () =>
    new Promise<void>((resolve) =>
      setTimeout(resolve, Math.max(0, Math.min(pollMs, deadline - Date.now()))),
    );
  let lastError: unknown;
  while (Date.now() < deadline) {
    if (opts.getLifecycle) {
      let lifecycle: unknown;
      try {
        lifecycle = await opts.getLifecycle();
      } catch (error) {
        lastError = error;
        await pause();
        continue;
      }
      const failure = environmentPackageFailureMessage(lifecycle);
      if (failure) throw new Error(failure);
    }
    try {
      const response = await opts.ping();
      if (response.ok) return;
      lastError = new Error(`execd ping returned ${response.status}`);
    } catch (error) {
      lastError = error;
    }
    await pause();
  }
  throw new Error(
    `OpenSandbox sandbox ${opts.id} did not become ready: ${lastError instanceof Error ? lastError.message : String(lastError)}`,
  );
}
