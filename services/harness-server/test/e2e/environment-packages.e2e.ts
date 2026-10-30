// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  buildSandboxWritePolicy,
  createPolicyEnforcedSandbox,
  hasWritePolicyEnforcement,
} from '@orca/sandbox-runtime';
import { OpenSandboxRuntime as CloudRuntime } from '../../../../packages/cloud-sandbox/src/opensandbox/runtime.js';
import { OpenSandboxRuntime } from '../../src/sandbox/opensandbox/runtime.js';

// No transport or Kubernetes mocks: both clients use the live patched server.
class TrackedHarnessRuntime extends OpenSandboxRuntime {
  createdId: string | undefined;
  override async requestJson<T>(
    ...args: Parameters<OpenSandboxRuntime['requestJson']>
  ): Promise<T> {
    const value = await super.requestJson<T>(...args);
    if (
      args[0] === '/sandboxes' &&
      typeof value === 'object' &&
      value !== null &&
      'id' in value &&
      typeof value.id === 'string'
    )
      this.createdId = value.id;
    return value;
  }
}
class TrackedCloudRuntime extends CloudRuntime {
  createdId: string | undefined;
  override async requestJson<T>(...args: Parameters<CloudRuntime['requestJson']>): Promise<T> {
    const value = await super.requestJson<T>(...args);
    if (
      args[0] === '/sandboxes' &&
      typeof value === 'object' &&
      value !== null &&
      'id' in value &&
      typeof value.id === 'string'
    )
      this.createdId = value.id;
    return value;
  }
}

const domain = process.env['OPEN_SANDBOX_DOMAIN'];
const image = process.env['OPEN_SANDBOX_IMAGE'];
const cluster = process.env['KIND_CLUSTER_NAME'];
const clients: Array<TrackedHarnessRuntime | TrackedCloudRuntime> = [];
const packages = {
  apt: ['jq'],
  pip: ['idna==3.10'],
  npm: ['is-number@7.0.0', 'file:/opt/orca-package-fixtures/malicious'],
  cargo: ['ripgrep@14.1.1'],
  gem: ['rake:13.2.1'],
  go: ['github.com/rakyll/hey@v0.1.4'],
};

function kubectl(...args: string[]): string {
  // Never use the caller's ambient (possibly shared) current context.
  return execFileSync('kubectl', ['--context', `kind-${cluster}`, '-n', 'opensandbox', ...args], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    timeout: 15_000,
  });
}

function runtimeOptions() {
  return {
    domain: domain!,
    protocol: (process.env['OPEN_SANDBOX_PROTOCOL'] ?? 'http') as 'http' | 'https',
    apiKey: process.env['OPEN_SANDBOX_API_KEY'],
    image: image!,
    entrypoint: ['/usr/local/bin/orca-sandbox-harness'],
    timeoutSeconds: 1800,
    requestTimeoutSeconds: 60,
    useServerProxy: true,
    resourceLimits: { cpu: '2', memory: '2Gi' },
  };
}

beforeAll(() => {
  if (!domain || !image || !cluster)
    throw new Error(
      'Live package E2E requires OPEN_SANDBOX_DOMAIN, the package fixture OPEN_SANDBOX_IMAGE, and KIND_CLUSTER_NAME',
    );
});

afterEach(async () => {
  for (const client of clients.splice(0)) {
    if (client.createdId)
      await client
        .lifecycleRequest(`/sandboxes/${client.createdId}`, { method: 'DELETE' })
        .catch(() => {});
  }
});

for (const [name, construct] of [
  ['harness-server', () => new TrackedHarnessRuntime(runtimeOptions())],
  ['cloud Environment launcher', () => new TrackedCloudRuntime(runtimeOptions())],
] as const) {
  describe(`${name}: packages through live OpenSandbox and gVisor`, () => {
    it('installs six managers while isolating installer hooks, resources and control credentials', async () => {
      const runtime = construct();
      clients.push(runtime);
      const sandbox = await runtime.acquire({
        packages,
        networking: { type: 'unrestricted' },
        requiresFuse: true,
        fileUploadOwnership: { owner: 'node', group: 'node' },
        harnessEnv: { ORCA_PACKAGE_CREDENTIAL_SENTINEL: randomUUID() },
      });
      try {
        const pod = JSON.parse(kubectl('get', 'pod', `${sandbox.id}-0`, '-o', 'json'));
        const init = pod.spec.initContainers.find(
          (item: { name: string }) => item.name === 'orca-environment-packages',
        );
        const main = pod.spec.containers.find((item: { name: string }) => item.name === 'sandbox');
        const completed = pod.status.initContainerStatuses.find(
          (item: { name: string }) => item.name === 'orca-environment-packages',
        );
        // Project the Pod before asserting: a failure must not print its secret env values.
        expect({
          runtime: pod.spec.runtimeClassName,
          controllerOwner: pod.metadata.labels['batch-sandbox.sandbox.opensandbox.io/name'],
          sharedPids: pod.spec.shareProcessNamespace,
          serviceAccount: pod.spec.automountServiceAccountToken,
          installerEnv: (init.env ?? []).length + (init.envFrom ?? []).length,
          installerMounts: init.volumeMounts.map((mount: { name: string }) => mount.name),
          artifactReadonly: main.volumeMounts.find(
            (mount: { name: string }) => mount.name === 'orca-environment-packages',
          )?.readOnly,
          artifactLimit: pod.spec.volumes.find(
            (volume: { name: string }) => volume.name === 'orca-environment-packages',
          )?.emptyDir?.sizeLimit,
          completed: completed.state.terminated?.exitCode,
          installerPullPolicy: init.imagePullPolicy,
          runtimePullPolicy: main.imagePullPolicy,
        }).toEqual({
          runtime: 'gvisor',
          controllerOwner: sandbox.id,
          sharedPids: false,
          serviceAccount: false,
          installerEnv: 0,
          installerMounts: ['orca-environment-packages'],
          artifactReadonly: true,
          artifactLimit: '8Gi',
          completed: 0,
          installerPullPolicy: 'IfNotPresent',
          runtimePullPolicy: 'IfNotPresent',
        });
        const running = pod.status.containerStatuses.find(
          (item: { name: string }) => item.name === 'sandbox',
        );
        expect(completed.imageID).toBe(running.imageID);
        expect(
          (await sandbox.files.read('/opt/orca-environment-packages/init-saw-token')).toString(),
        ).toBe('absent');
        if (!sandbox.prepareFilesystemRoots) throw new Error('live runtime lacks setup preflight');
        await sandbox.prepareFilesystemRoots([
          '/workspace/skills',
          '/mnt/package-input.txt',
          '/mnt/session/outputs',
        ]);
        const setup = await sandbox.run({
          tool: 'bash',
          args: {
            command: 'mkdir -p /mnt/session/outputs && chown 1000:1000 /mnt/session/outputs',
          },
        });
        expect(setup.exit_code, setup.stderr).toBe(0);
        await sandbox.files.write('/workspace/skills/SKILL.md', Buffer.from('pinned'));
        await sandbox.files.write('/mnt/package-input.txt', Buffer.from('read-only input'));
        if (!hasWritePolicyEnforcement(sandbox))
          throw new Error('live runtime lacks write-policy enforcement');
        const agent = await createPolicyEnforcedSandbox(
          sandbox,
          buildSandboxWritePolicy(
            [{ path: '/mnt/package-input.txt', kind: 'file', access: 'read_only' }],
            { includeSkillsRoot: true, environmentPackages: true },
          ),
        );
        const result = await agent.run({
          tool: 'bash',
          args: {
            command: [
              'set -eu',
              'test "$(id)" = poisoned-id',
              'test "$(cat /workspace/skills/SKILL.md)" = pinned',
              'test "$(cat /mnt/package-input.txt)" = "read-only input"',
              'jq -n -e true',
              'python -c "import idna; assert idna.__version__ == \'3.10\'"',
              'node -e "if(!require(\'is-number\')(42))process.exit(1)"',
              'rg --version | head -n 1 | grep -F "14.1.1"',
              "ruby -e \"require 'rake'; abort unless Rake::VERSION == '13.2.1'\"",
              'hey -h >/dev/null 2>&1 || test "$?" = 2',
              'test -z "${EXECD_ACCESS_TOKEN:-}"',
              "node -e \"if(require('fs').readFileSync('/proc/1/environ','utf8').includes('EXECD_ACCESS_TOKEN='))process.exit(1)\"",
              'node -e "fetch(\'http://127.0.0.1:44772/ping\').then(r=>{if(r.status!==401)process.exit(1)})"',
              'if printf denied >> /mnt/package-input.txt; then exit 81; fi',
              'if printf denied >> /workspace/skills/SKILL.md; then exit 82; fi',
              'if printf denied > /etc/orca-package-escape; then exit 83; fi',
              'printf verified > /mnt/session/outputs/packages.txt',
            ].join('\n'),
            timeout_ms: 30_000,
          },
        });
        expect(result.exit_code, result.stderr).toBe(0);
        expect((await sandbox.files.read('/mnt/session/outputs/packages.txt')).toString()).toBe(
          'verified',
        );
        expect((await sandbox.files.read('/mnt/package-input.txt')).toString()).toBe(
          'read-only input',
        );
        const controller = await sandbox.run({
          tool: 'bash',
          args: {
            command:
              'sleep 2; test "$(id -u)" = 0 && test ! -e /opt/orca-environment-packages/late-write',
          },
        });
        expect(controller.exit_code, controller.stderr).toBe(0);
        await expect(agent.files.read('/proc/1/environ')).rejects.toThrow(/read denied/);
      } finally {
        await sandbox.destroy();
      }
      await expect
        .poll(async () => (await runtime.lifecycleRequest(`/sandboxes/${sandbox.id}`)).status, {
          timeout: 60_000,
          interval: 500,
        })
        .toBe(404);
      await expect
        .poll(() => kubectl('get', 'pod', `${sandbox.id}-0`, '--ignore-not-found', '-o', 'name'), {
          timeout: 60_000,
          interval: 500,
        })
        .toBe('');
    });

    it('reports a failed installer promptly without its logs and deletes the sandbox', async () => {
      const runtime = construct();
      clients.push(runtime);
      const before = new Set(
        JSON.parse(kubectl('get', 'pod,batchsandbox', '-o', 'json')).items.map(
          (item: { metadata: { uid: string } }) => item.metadata.uid,
        ),
      );
      const started = Date.now();
      let completedAt: number | undefined;
      let failedAt: number | undefined;
      const acquisition = runtime
        .acquire({
          packages: { npm: ['file:/opt/orca-package-fixtures/failing'] },
          networking: { type: 'unrestricted' },
          requiresFuse: true,
        })
        .then(
          () => {
            completedAt = Date.now();
            return undefined;
          },
          (error: unknown) => {
            completedAt = Date.now();
            return error;
          },
        );
      // Measure failure propagation after init fails, independently of cold
      // gVisor/container startup. No Pod specs or installer messages are logged.
      while (completedAt === undefined && Date.now() - started < 240_000) {
        const pods = JSON.parse(kubectl('get', 'pod', '-o', 'json')).items;
        for (const pod of pods) {
          if (before.has(pod.metadata.uid)) continue;
          for (const init of pod.status.initContainerStatuses ?? []) {
            if (init.name !== 'orca-environment-packages') continue;
            if (
              (init.state.terminated && init.state.terminated.exitCode !== 0) ||
              init.state.waiting?.reason === 'CrashLoopBackOff'
            )
              failedAt ??= Date.now();
          }
        }
        if (failedAt !== undefined && Date.now() - failedAt >= 30_000) break;
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
      expect(completedAt !== undefined, 'installer failure was not reported promptly').toBe(true);
      const error = await acquisition;
      expect(error instanceof Error, 'installer unexpectedly completed').toBe(true);
      expect((error as Error).message).toMatch(/ENVIRONMENT_PACKAGES_(INSTALL|START)_FAILED/);
      expect((error as Error).message).not.toContain('installer-private-output');
      if (failedAt !== undefined && completedAt !== undefined)
        expect(completedAt - failedAt).toBeLessThan(30_000);
      await expect
        .poll(
          () => {
            const resources = JSON.parse(kubectl('get', 'pod,batchsandbox', '-o', 'json')).items;
            return resources.filter(
              (item: { metadata: { uid: string } }) => !before.has(item.metadata.uid),
            ).length;
          },
          { timeout: 60_000, interval: 500 },
        )
        .toBe(0);
    }, 300_000);

    it('rejects an aliased installer root and deletes the failed sandbox', async () => {
      const runtime = construct();
      clients.push(runtime);
      await expect(
        runtime.acquire({
          packages: { npm: ['file:/opt/orca-package-fixtures/invalid-rootfs'] },
          networking: { type: 'unrestricted' },
          requiresFuse: true,
        }),
      ).rejects.toThrow(/environment package preparation failed/);
      const id = runtime.createdId;
      expect(id).toBeDefined();
      await expect
        .poll(async () => (await runtime.lifecycleRequest(`/sandboxes/${id}`)).status, {
          timeout: 60_000,
          interval: 500,
        })
        .toBe(404);
      await expect
        .poll(() => kubectl('get', 'pod', `${id}-0`, '--ignore-not-found', '-o', 'name'), {
          timeout: 60_000,
          interval: 500,
        })
        .toBe('');
    });
  });
}
