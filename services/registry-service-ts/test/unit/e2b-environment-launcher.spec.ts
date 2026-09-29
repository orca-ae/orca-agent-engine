// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// E2BEnvironmentLauncher — thin E2B-specific wiring over the shared
// CloudEnvironmentLauncher (see cloud-environment-launcher.spec.ts for the
// full lifecycle + startWorker-wiring contract, which this backend inherits
// unchanged). Two concerns here:
//  - `e2bSandboxRuntimeOptions` — the apiKey/templateId/baseURL plumbing
//    into E2BSandboxRuntimeOptions, unit-tested as a pure function so it
//    never needs the real E2B SDK.
//  - The launcher delegates every EnvironmentLauncher method to an injected
//    fake SandboxRuntime — proving the composition wiring is correct without
//    ever touching a real E2B sandbox.

import { describe, expect, it } from 'vitest';
import type {
  EnvironmentSpec,
  SandboxHandle,
  SandboxRuntime,
  ToolCall,
  ToolResult,
} from '@orca/sandbox-runtime';
import {
  E2BEnvironmentLauncher,
  e2bSandboxRuntimeOptions,
} from '../../src/environment/launcher/e2b-environment-launcher.js';
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
  readonly capabilities = { supportsFuse: true };
  readonly acquireCalls: EnvironmentSpec[] = [];
  readonly handles: FakeSandboxHandle[] = [];
  private counter = 0;
  async acquire(env: EnvironmentSpec): Promise<SandboxHandle> {
    this.acquireCalls.push(env);
    this.counter += 1;
    const handle = new FakeSandboxHandle(`fake-e2b-${this.counter}`);
    this.handles.push(handle);
    return handle;
  }
}

const WORKER_LAUNCH_COMMAND = ['node', '/opt/orca/environment-worker/dist/main.js'];
const RUNNER_LAUNCH_COMMAND = ['node', '/opt/orca/session-runner/dist/main.js'];
const START_OPTS: StartWorkerOptions = {
  token: 'sk-env-token-e2b',
  registryTunnelUrl: 'wss://registry.example.com',
  environmentId: 'env_e2b_test',
  identity: { workerId: 'worker_e2b_001', workerName: 'e2b-test-worker' },
};

describe('e2bSandboxRuntimeOptions', () => {
  it('passes apiKey through and omits templateId/baseURL when unset', () => {
    const opts = e2bSandboxRuntimeOptions({ apiKey: 'e2b_test_key' });
    expect(opts).toEqual({ apiKey: 'e2b_test_key' });
    expect('templateId' in opts).toBe(false);
    expect('baseURL' in opts).toBe(false);
  });

  it('includes templateId and baseURL when configured', () => {
    const opts = e2bSandboxRuntimeOptions({
      apiKey: 'e2b_test_key',
      templateId: 'orca-default',
      baseURL: 'https://e2b.internal.example.com',
    });
    expect(opts).toEqual({
      apiKey: 'e2b_test_key',
      templateId: 'orca-default',
      baseURL: 'https://e2b.internal.example.com',
    });
  });
});

describe('E2BEnvironmentLauncher', () => {
  it('does not construct the real E2B SDK runtime when a runtime override is supplied (never touches the network)', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = new E2BEnvironmentLauncher({
      apiKey: 'unused-test-key',
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
        new E2BEnvironmentLauncher({
          apiKey: 'unused-test-key',
          templateId: 'orca-default',
          workerLaunchCommand: WORKER_LAUNCH_COMMAND,
          runnerLaunchCommand: RUNNER_LAUNCH_COMMAND,
        }),
    ).not.toThrow();
  });

  it('delegates the full lifecycle to the injected runtime', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = new E2BEnvironmentLauncher({
      apiKey: 'unused-test-key',
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
    const launcher = new E2BEnvironmentLauncher({
      apiKey: 'unused-test-key',
      workerLaunchCommand: WORKER_LAUNCH_COMMAND,
      runnerLaunchCommand: RUNNER_LAUNCH_COMMAND,
      runtime,
    });
    const id = await launcher.provision('test-env');
    await launcher.startWorker(id, START_OPTS);

    const args = runtime.handles[0]!.runCalls[0]!.args as { command: string };
    expect(args.command).toContain(`${ORCA_ENVIRONMENT_TOKEN_ENV_VAR}='sk-env-token-e2b'`);
    expect(args.command).toContain(`${ENVIRONMENT_ID_ENV_VAR}='env_e2b_test'`);
  });

  it('rejects resume, mirroring the shared cloud launcher default', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = new E2BEnvironmentLauncher({
      apiKey: 'unused-test-key',
      workerLaunchCommand: WORKER_LAUNCH_COMMAND,
      runnerLaunchCommand: RUNNER_LAUNCH_COMMAND,
      runtime,
    });
    const id = await launcher.provision('test-env');
    await expect(launcher.resume(id)).rejects.toThrow(/resum/i);
  });
});
