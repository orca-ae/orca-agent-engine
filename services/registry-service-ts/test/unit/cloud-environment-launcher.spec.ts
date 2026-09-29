// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// CloudEnvironmentLauncher — the shared EnvironmentLauncher core E2B and
// OpenSandbox both delegate to (see cloud-environment-launcher.ts's module
// doc for why one implementation backs both provider-specific classes).
// Driven through an injectable FAKE SandboxRuntime/SandboxHandle so no real
// cloud sandbox is ever touched — mirrors LocalEnvironmentLauncher's fake
// ProcessSpawner seam in local-environment-launcher.spec.ts.
//
// Two concerns:
//  - Lifecycle contract: provision returns the sandbox id; after
//    startWorker, isRunning is true; after terminate, the sandbox is
//    destroyed and isRunning is false; double-terminate is safe; resume is
//    unsupported (this launcher only ever destroys, never pauses).
//  - startWorker wiring: the exec'd command backgrounds
//    `workerLaunchCommand` inside the box with the exact dial-back env
//    (registry tunnel URL, environment id, token, identity) environment-worker
//    needs to connect back — proving the worker *would* dial the registry,
//    without dialing one.

import { describe, expect, it } from 'vitest';
import type {
  EnvironmentSpec,
  SandboxHandle,
  SandboxRuntime,
  ToolCall,
  ToolResult,
} from '@orca/sandbox-runtime';
import {
  buildWorkerExecCommand,
  CloudEnvironmentLauncher,
} from '../../src/environment/launcher/cloud-environment-launcher.js';
import {
  ENVIRONMENT_ID_ENV_VAR,
  ENVIRONMENT_WORKER_ID_ENV_VAR,
  ENVIRONMENT_WORKER_NAME_ENV_VAR,
  ORCA_ENVIRONMENT_TOKEN_ENV_VAR,
  REGISTRY_TUNNEL_BASE_URL_ENV_VAR,
  RUNNER_LAUNCH_COMMAND_ENV_VAR,
  WORKSPACE_DIR_ENV_VAR,
} from '../../src/environment/launcher/worker-env.js';
import type { StartWorkerOptions } from '../../src/environment/launcher/types.js';

/** A fake sandbox handle that records every bash exec + destroy call. */
class FakeSandboxHandle implements SandboxHandle {
  readonly runCalls: ToolCall[] = [];
  destroyCalls = 0;
  nextRunResult: ToolResult = { stdout: 'ok', stderr: '', exit_code: 0 };

  readonly files: SandboxHandle['files'] = {
    write: () => Promise.reject(new Error('FakeSandboxHandle.files.write not implemented')),
    read: () => Promise.reject(new Error('FakeSandboxHandle.files.read not implemented')),
    list: () => Promise.reject(new Error('FakeSandboxHandle.files.list not implemented')),
    delete: () => Promise.reject(new Error('FakeSandboxHandle.files.delete not implemented')),
    readUtf8Page: () =>
      Promise.reject(new Error('FakeSandboxHandle.files.readUtf8Page not implemented')),
    chmod: () => Promise.reject(new Error('FakeSandboxHandle.files.chmod not implemented')),
  };

  constructor(readonly id: string) {}

  async run(call: ToolCall): Promise<ToolResult> {
    this.runCalls.push(call);
    return this.nextRunResult;
  }

  async runPrivileged(): Promise<ToolResult> {
    throw new Error('FakeSandboxHandle.runPrivileged not implemented');
  }

  async pause(): Promise<void> {}

  async resume(): Promise<void> {}

  async destroy(): Promise<void> {
    this.destroyCalls += 1;
  }
}

/** A fake runtime that hands back fresh FakeSandboxHandles and records every acquire() call. */
class FakeSandboxRuntime implements SandboxRuntime {
  readonly capabilities = { supportsFuse: false };
  readonly acquireCalls: EnvironmentSpec[] = [];
  readonly handles: FakeSandboxHandle[] = [];
  private counter = 0;

  async acquire(env: EnvironmentSpec): Promise<SandboxHandle> {
    this.acquireCalls.push(env);
    this.counter += 1;
    const handle = new FakeSandboxHandle(`fake-sandbox-${this.counter}`);
    this.handles.push(handle);
    return handle;
  }
}

const WORKER_LAUNCH_COMMAND = ['node', '/opt/orca/environment-worker/dist/main.js'];
const RUNNER_LAUNCH_COMMAND = ['node', '/opt/orca/session-runner/dist/main.js'];

const START_OPTS: StartWorkerOptions = {
  token: 'sk-env-token-abc',
  registryTunnelUrl: 'wss://registry.example.com',
  environmentId: 'env_test123456789',
  identity: { workerId: 'worker_id_001', workerName: 'test-worker-name' },
};

function buildLauncher(runtime: FakeSandboxRuntime, label = 'Fake'): CloudEnvironmentLauncher {
  return new CloudEnvironmentLauncher({
    runtime,
    workerLaunchCommand: WORKER_LAUNCH_COMMAND,
    runnerLaunchCommand: RUNNER_LAUNCH_COMMAND,
    label,
  });
}

describe('CloudEnvironmentLauncher — lifecycle contract', () => {
  it('provision returns the acquired sandbox id', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');
    expect(id).toBe(runtime.handles[0]!.id);
  });

  it('returns a distinct id for each provision call', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const first = await launcher.provision('a');
    const second = await launcher.provision('b');
    expect(first).not.toBe(second);
  });

  it('acquires with an empty EnvironmentSpec (environment-level box, not a per-session sandbox)', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    await launcher.provision('test-env');
    expect(runtime.acquireCalls).toEqual([{}]);
  });

  it('is not running before startWorker is called', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');
    expect(await launcher.isRunning(id)).toBe(false);
  });

  it('is running after startWorker, and stops running after terminate', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);
    expect(await launcher.isRunning(id)).toBe(true);

    await launcher.terminate(id);
    expect(await launcher.isRunning(id)).toBe(false);
  });

  it('terminate destroys the underlying sandbox handle', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');
    await launcher.startWorker(id, START_OPTS);

    await launcher.terminate(id);
    expect(runtime.handles[0]!.destroyCalls).toBe(1);
  });

  it('is safe to terminate twice', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');
    await launcher.startWorker(id, START_OPTS);

    await launcher.terminate(id);
    await expect(launcher.terminate(id)).resolves.toBeUndefined();
    // The second terminate() is a no-op — it must not re-destroy the handle.
    expect(runtime.handles[0]!.destroyCalls).toBe(1);
  });

  it('is safe to terminate an environment that was never started', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');
    await expect(launcher.terminate(id)).resolves.toBeUndefined();
    expect(runtime.handles[0]!.destroyCalls).toBe(1);
  });

  it('rejects resume — this launcher only ever destroys, never pauses', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');
    await expect(launcher.resume?.(id)).rejects.toThrow(/resum/i);
  });

  it('rejects startWorker for an id that was never provisioned', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    await expect(launcher.startWorker('nonexistent-id', START_OPTS)).rejects.toThrow(
      /nonexistent-id/,
    );
  });

  it('throws when the exec-launch bash command reports a non-zero exit', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');
    runtime.handles[0]!.nextRunResult = { stdout: '', stderr: 'command not found', exit_code: 127 };

    await expect(launcher.startWorker(id, START_OPTS)).rejects.toThrow(/127/);
    expect(await launcher.isRunning(id)).toBe(false);
  });
});

describe('CloudEnvironmentLauncher — startWorker wiring (fake runtime)', () => {
  it('execs a single bash tool call on the provisioned sandbox handle', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    expect(runtime.handles[0]!.runCalls).toHaveLength(1);
    expect(runtime.handles[0]!.runCalls[0]!.tool).toBe('bash');
  });

  it('backgrounds the configured worker launch command', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    const args = runtime.handles[0]!.runCalls[0]!.args as { command: string };
    expect(args.command).toContain("'node' '/opt/orca/environment-worker/dist/main.js'");
    expect(args.command.trimEnd()).toMatch(/&$/);
  });

  it('wires the exact env environment-worker/config.ts requires to dial back', async () => {
    // registry tunnel URL, environment id, and the token — the three
    // dial-back essentials the exec'd worker needs to connect back.
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    const args = runtime.handles[0]!.runCalls[0]!.args as { command: string };
    expect(args.command).toContain(
      `${REGISTRY_TUNNEL_BASE_URL_ENV_VAR}='wss://registry.example.com'`,
    );
    expect(args.command).toContain(`${ENVIRONMENT_ID_ENV_VAR}='env_test123456789'`);
  });

  it('wires the per-launch token as ORCA_ENVIRONMENT_TOKEN — the managed-auth path, not the self-hosted ENVIRONMENT_KEY', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    const args = runtime.handles[0]!.runCalls[0]!.args as { command: string };
    expect(args.command).toContain(`${ORCA_ENVIRONMENT_TOKEN_ENV_VAR}='sk-env-token-abc'`);
    // Never leak the token into the self-hosted Env-Key slot.
    expect(args.command).not.toContain('ENVIRONMENT_KEY=');
  });

  it('wires the identity (workerName visible to environment-worker, workerId not dropped)', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    const args = runtime.handles[0]!.runCalls[0]!.args as { command: string };
    expect(args.command).toContain(`${ENVIRONMENT_WORKER_NAME_ENV_VAR}='test-worker-name'`);
    expect(args.command).toContain(`${ENVIRONMENT_WORKER_ID_ENV_VAR}='worker_id_001'`);
  });

  it('wires RUNNER_LAUNCH_COMMAND as the configured argv space-joined (config.ts parses on whitespace)', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    const args = runtime.handles[0]!.runCalls[0]!.args as { command: string };
    expect(args.command).toContain(
      `${RUNNER_LAUNCH_COMMAND_ENV_VAR}='node /opt/orca/session-runner/dist/main.js'`,
    );
  });

  it('wires WORKSPACE_DIR to a non-empty default when unset', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = buildLauncher(runtime);
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    const args = runtime.handles[0]!.runCalls[0]!.args as { command: string };
    expect(args.command).toMatch(new RegExp(`${WORKSPACE_DIR_ENV_VAR}='[^']+'`));
  });

  it('wires WORKSPACE_DIR to a configured override', async () => {
    const runtime = new FakeSandboxRuntime();
    const launcher = new CloudEnvironmentLauncher({
      runtime,
      workerLaunchCommand: WORKER_LAUNCH_COMMAND,
      runnerLaunchCommand: RUNNER_LAUNCH_COMMAND,
      workspaceDir: '/srv/orca-env',
    });
    const id = await launcher.provision('test-env');

    await launcher.startWorker(id, START_OPTS);

    const args = runtime.handles[0]!.runCalls[0]!.args as { command: string };
    expect(args.command).toContain(`${WORKSPACE_DIR_ENV_VAR}='/srv/orca-env'`);
  });
});

describe('buildWorkerExecCommand (pure function)', () => {
  it('quotes each argv element and each env value', () => {
    const command = buildWorkerExecCommand(['node', 'main.js'], { FOO: 'bar baz' }, '/tmp/log');
    expect(command).toBe(
      "env FOO='bar baz' nohup 'node' 'main.js' < /dev/null > '/tmp/log' 2>&1 &",
    );
  });

  it('escapes a single quote embedded in an env value (e.g. a token)', () => {
    const command = buildWorkerExecCommand(['node'], { TOKEN: "it's-a-token" }, '/tmp/log');
    expect(command).toContain(String.raw`TOKEN='it'\''s-a-token'`);
  });

  it('escapes a single quote embedded in an argv element', () => {
    const command = buildWorkerExecCommand(["can't"], {}, '/tmp/log');
    expect(command).toContain(String.raw`'can'\''t'`);
  });
});
