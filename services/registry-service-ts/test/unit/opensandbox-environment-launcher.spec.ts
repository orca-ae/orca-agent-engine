// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// OpenSandboxEnvironmentLauncher — thin OpenSandbox-specific wiring over the
// shared CloudEnvironmentLauncher (see cloud-environment-launcher.spec.ts for
// the full lifecycle + startWorker-wiring contract, which this backend
// inherits unchanged). Two concerns here:
//  - `openSandboxRuntimeOptions` — the domain/protocol/image/timeout
//    plumbing into OpenSandboxRuntimeOptions (with the same defaults
//    harness-server's config.ts uses), unit-tested as a pure function so it
//    never needs a real OpenSandbox server.
//  - The launcher delegates every EnvironmentLauncher method to an injected
//    fake SandboxRuntime — proving the composition wiring is correct without
//    ever touching a real OpenSandbox sandbox.

import { describe, expect, it } from 'vitest';
import type {
  EnvironmentSpec,
  SandboxHandle,
  SandboxRuntime,
  ToolCall,
  ToolResult,
} from '@orca/sandbox-runtime';
import {
  OpenSandboxEnvironmentLauncher,
  openSandboxRuntimeOptions,
} from '../../src/environment/launcher/opensandbox-environment-launcher.js';
import {
  ENVIRONMENT_ID_ENV_VAR,
  ORCA_ENVIRONMENT_TOKEN_ENV_VAR,
} from '../../src/environment/launcher/worker-env.js';
import type { StartWorkerOptions } from '../../src/environment/launcher/types.js';

class FakeSandboxHandle implements SandboxHandle {
  readonly runCalls: ToolCall[] = [];
  destroyCalls = 0;
  readonly files: SandboxHandle['files'] = {
    write: () => Promise.reject(new Error('not implemented')),
    read: () => Promise.reject(new Error('not implemented')),
    list: () => Promise.reject(new Error('not implemented')),
    delete: () => Promise.reject(new Error('not implemented')),
    readUtf8Page: () => Promise.reject(new Error('not implemented')),
    chmod: () => Promise.reject(new Error('not implemented')),
  };
  constructor(readonly id: string) {}
  async run(call: ToolCall): Promise<ToolResult> {
    this.runCalls.push(call);
    return { stdout: '', stderr: '', exit_code: 0 };
  }
  async runPrivileged(): Promise<ToolResult> {
    throw new Error('not implemented');
  }
  async pause(): Promise<void> {}
  async resume(): Promise<void> {}
  async destroy(): Promise<void> {
    this.destroyCalls += 1;
  }
}

class FakeSandboxRuntime implements SandboxRuntime {
  readonly capabilities = { supportsFuse: false };
  readonly acquireCalls: EnvironmentSpec[] = [];
  readonly handles: FakeSandboxHandle[] = [];
  private counter = 0;
  async acquire(env: EnvironmentSpec): Promise<SandboxHandle> {
    this.acquireCalls.push(env);
    this.counter += 1;
    const handle = new FakeSandboxHandle(`fake-opensandbox-${this.counter}`);
    this.handles.push(handle);
    return handle;
  }
}

const WORKER_LAUNCH_COMMAND = ['node', '/opt/orca/environment-worker/dist/main.js'];
const RUNNER_LAUNCH_COMMAND = ['node', '/opt/orca/session-runner/dist/main.js'];
const START_OPTS: StartWorkerOptions = {
  token: 'sk-env-token-opensandbox',
  registryTunnelUrl: 'wss://registry.example.com',
  environmentId: 'env_opensandbox_test',
  identity: { workerId: 'worker_os_001', workerName: 'opensandbox-test-worker' },
};

describe('openSandboxRuntimeOptions', () => {
  it('applies the same defaults as harness-server (1800s timeout, server-proxy on, 30s request timeout)', () => {
    const opts = openSandboxRuntimeOptions({
      domain: 'opensandbox.internal:8080',
      protocol: 'http',
      image: 'ghcr.io/orca-ae/orca-environment:latest',
    });
    expect(opts).toEqual({
      domain: 'opensandbox.internal:8080',
      protocol: 'http',
      image: 'ghcr.io/orca-ae/orca-environment:latest',
      timeoutSeconds: 1800,
      useServerProxy: true,
      requestTimeoutSeconds: 30,
    });
    expect('apiKey' in opts).toBe(false);
  });

  it('honors caller overrides for timeout/proxy/apiKey', () => {
    const opts = openSandboxRuntimeOptions({
      domain: 'opensandbox.internal:8080',
      protocol: 'https',
      apiKey: 'os_test_key',
      image: 'ghcr.io/orca-ae/orca-environment:latest',
      timeoutSeconds: 600,
      useServerProxy: false,
      requestTimeoutSeconds: 10,
    });
    expect(opts).toEqual({
      domain: 'opensandbox.internal:8080',
      protocol: 'https',
      apiKey: 'os_test_key',
      image: 'ghcr.io/orca-ae/orca-environment:latest',
      timeoutSeconds: 600,
      useServerProxy: false,
      requestTimeoutSeconds: 10,
    });
  });
});

describe('OpenSandboxEnvironmentLauncher', () => {
  it('does not construct the real OpenSandboxRuntime when a runtime override is supplied (never touches the network)', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = new OpenSandboxEnvironmentLauncher({
      domain: 'unused.internal',
      protocol: 'http',
      image: 'unused:latest',
      workerLaunchCommand: WORKER_LAUNCH_COMMAND,
      runnerLaunchCommand: RUNNER_LAUNCH_COMMAND,
      runtime,
    });
    const id = await launcher.provision('test-env');
    expect(id).toBe(runtime.handles[0]!.id);
    expect(runtime.acquireCalls).toHaveLength(1);
  });

  it('constructing without a runtime override does not throw (no I/O happens until acquire())', () => {
    expect(
      () =>
        new OpenSandboxEnvironmentLauncher({
          domain: 'unused.internal',
          protocol: 'http',
          image: 'unused:latest',
          workerLaunchCommand: WORKER_LAUNCH_COMMAND,
          runnerLaunchCommand: RUNNER_LAUNCH_COMMAND,
        }),
    ).not.toThrow();
  });

  it('delegates the full lifecycle to the injected runtime', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = new OpenSandboxEnvironmentLauncher({
      domain: 'unused.internal',
      protocol: 'http',
      image: 'unused:latest',
      workerLaunchCommand: WORKER_LAUNCH_COMMAND,
      runnerLaunchCommand: RUNNER_LAUNCH_COMMAND,
      runtime,
    });
    const id = await launcher.provision('test-env');
    expect(await launcher.isRunning(id)).toBe(false);

    await launcher.startWorker(id, START_OPTS);
    expect(await launcher.isRunning(id)).toBe(true);

    await launcher.terminate(id);
    expect(await launcher.isRunning(id)).toBe(false);
    expect(runtime.handles[0]!.destroyCalls).toBe(1);
  });

  it('startWorker wires the dial-back token and environment id (critical: managed auth, not a placeholder)', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = new OpenSandboxEnvironmentLauncher({
      domain: 'unused.internal',
      protocol: 'http',
      image: 'unused:latest',
      workerLaunchCommand: WORKER_LAUNCH_COMMAND,
      runnerLaunchCommand: RUNNER_LAUNCH_COMMAND,
      runtime,
    });
    const id = await launcher.provision('test-env');
    await launcher.startWorker(id, START_OPTS);

    const args = runtime.handles[0]!.runCalls[0]!.args as { command: string };
    expect(args.command).toContain(`${ORCA_ENVIRONMENT_TOKEN_ENV_VAR}='sk-env-token-opensandbox'`);
    expect(args.command).toContain(`${ENVIRONMENT_ID_ENV_VAR}='env_opensandbox_test'`);
  });

  it('rejects resume, mirroring the shared cloud launcher default', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = new OpenSandboxEnvironmentLauncher({
      domain: 'unused.internal',
      protocol: 'http',
      image: 'unused:latest',
      workerLaunchCommand: WORKER_LAUNCH_COMMAND,
      runnerLaunchCommand: RUNNER_LAUNCH_COMMAND,
      runtime,
    });
    const id = await launcher.provision('test-env');
    await expect(launcher.resume(id)).rejects.toThrow(/resum/i);
  });
});
