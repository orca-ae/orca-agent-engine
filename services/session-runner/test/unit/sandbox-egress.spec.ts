// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RunnerResources } from '../../src/resources.js';
import { SessionLoop } from '../../src/session-loop.js';
import { ProviderRegistry } from '../../src/harness/provider.js';
import type { AgentHarness, AgentEvent } from '../../src/harness/agent-harness.js';
import {
  createRunnerSandboxRuntime,
  resourceManifestDigest,
  type SandboxHandle,
  type ResourceManifest,
} from '../../src/sandbox/seam.js';

vi.mock('node:fs/promises', async (original) => {
  const actual = await original<typeof import('node:fs/promises')>();
  return {
    ...actual,
    readFile: async (...args: Parameters<typeof actual.readFile>) => {
      if (
        args[0] === '/etc/ssl/certs/ca-certificates.crt' ||
        args[0] === '/run/orca/sandbox-network/ca-bundle.crt'
      ) {
        return Buffer.from('-----BEGIN CERTIFICATE-----public-fixture');
      }
      if (args[0] === '/run/orca/sandbox-network/context.json')
        return JSON.stringify({
          binding_id: 'network',
          generation: 'network-one',
          proxy_url: 'https://proxy.example:8443',
          proxy_capability: 'base-network-only',
          ca_bundle_path: '/run/orca/sandbox-network/ca-bundle.crt',
          internal_bypass: ['registry.example'],
        });
      return actual.readFile(...args);
    },
  };
});

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const fn of cleanup.splice(0).reverse()) await fn();
});
const manifest: ResourceManifest = { version: 1, revision: 'a'.repeat(64), resources: [] };
const delivery = (generation: string, capability: string) => ({
  network_binding_id: 'network',
  binding_id: 'binding',
  generation,
  proxy_capability: capability,
  placeholders: {},
});

async function fixture(required = true, kind: 'local' | 'in-memory' = 'local') {
  const workspaceDir = await mkdtemp(join(tmpdir(), 'orca-egress-runner-'));
  const runtime = createRunnerSandboxRuntime({
    kind,
    local: {
      harnessWorkDir: workspaceDir,
      allowedNetworkHosts: [],
      manager: { initialize: async () => {}, wrapWithSandbox: async (command) => command },
    },
  });
  let raw: SandboxHandle | undefined;
  const acquire = runtime.acquire.bind(runtime);
  runtime.acquire = async (env) => {
    raw = await acquire(env);
    return raw;
  };
  const controller = new RunnerResources({
    workspaceDir,
    sessionSandbox: runtime,
    ...(required ? { sandboxEgressRequired: true } : {}),
  });
  cleanup.push(async () => {
    await controller.close();
    await rm(workspaceDir, { recursive: true, force: true });
  });
  const push = (body: unknown) => controller.push('ses_fixture', body);
  const begin = (session_egress?: unknown) =>
    push({
      type: 'manifest',
      placement: 'session_sandbox',
      manifest,
      ...(session_egress !== undefined ? { session_egress } : {}),
    });
  const commit = () =>
    push({
      type: 'commit',
      revision: manifest.revision,
      manifest_sha256: resourceManifestDigest(manifest),
    });
  return { controller, raw: () => raw!, begin, commit };
}

describe('managed sandbox egress delivery', () => {
  it('reuses native credentials only within one binding generation and restarts on rotation', async () => {
    const { controller, begin, commit } = await fixture();
    await begin(delivery('one', 'first-capability'));
    await commit();
    const start = vi.fn(async () => {});
    const stop = vi.fn(async () => {});
    const refreshCredentials = vi.fn(async () => {});
    const harness: AgentHarness = {
      start,
      stop,
      refreshCredentials,
      submit: async () => {},
      interrupt: () => {},
      async *events(): AsyncIterable<AgentEvent> {
        /* no active turn */
      },
    };
    const providers = new ProviderRegistry().register('pi', () => harness, {
      sessionSandboxReadPaths: [],
    });
    const loop = new SessionLoop({ workspaceId: 'ws_fixture', providers, resources: controller });
    cleanup.push(() => loop.stop());
    const snapshot = Buffer.from(
      JSON.stringify({
        provider: 'pi',
        model: { provider: 'anthropic', id: 'model' },
        system: '',
        allowed_tool_names: [],
        allowed_mcp_server_names: [],
        tool_permissions: {},
        egress: {
          mode: 'gateway',
          gateway: { llm_base_url: 'http://fixture.invalid', llm_jwt: 'llm-token' },
        },
        sandbox_resources: {
          version: 1,
          revision: manifest.revision,
          placement_id: controller.placement!.id,
        },
      }),
    );
    await loop.applySnapshot('ses_fixture', snapshot);
    await loop.applySnapshot('ses_fixture', snapshot);
    expect(start).toHaveBeenCalledTimes(1);
    expect(refreshCredentials).toHaveBeenCalledTimes(1);
    await begin(delivery('two', 'second-capability'));
    await loop.applySnapshot('ses_fixture', snapshot);
    expect(stop).toHaveBeenCalledTimes(1);
    expect(start).toHaveBeenCalledTimes(2);
  });
  it('applies before placement commit, rotates on identical resource revision and persists only public trust', async () => {
    const { controller, raw, begin, commit } = await fixture();
    await begin(delivery('one', 'capability-one'));
    expect(controller.readyForSnapshot).toBe(false);
    await commit();
    expect(controller.readyForSnapshot).toBe(true);
    const command = 'printf "%s" "$HTTPS_PROXY"';
    expect((await raw().run({ tool: 'bash', args: { command } })).stdout).toContain(
      'capability-one',
    );
    expect(controller.egressGeneration).toBe('binding:one');
    expect(await begin(delivery('two', 'capability-two'))).toEqual({ committed: true });
    expect((await raw().run({ tool: 'bash', args: { command } })).stdout).toContain(
      'capability-two',
    );
    expect(controller.egressGeneration).toBe('binding:two');
    expect(controller.placement!.policy.networkUnrestricted).toBe(true);
    const root = (raw() as SandboxHandle & { rootDir(): string }).rootDir();
    expect(
      (await raw().run({ tool: 'bash', args: { command: 'cat "$CURL_HOME/.curlrc"' } })).stdout,
    ).toContain(root + '/run/orca/sandbox-network/ca-bundle.crt');
    const scan = async (path: string): Promise<string> =>
      (
        await Promise.all(
          (await readdir(path, { withFileTypes: true })).map(async (entry) =>
            entry.isDirectory()
              ? scan(join(path, entry.name))
              : (await readFile(join(path, entry.name))).toString(),
          ),
        )
      ).join('');
    expect(await scan(root)).not.toContain('capability-');
    const clear = vi.spyOn(raw(), 'configureSessionEgress');
    await controller.close();
    expect(clear).toHaveBeenLastCalledWith({});
  });

  it('keeps session capabilities isolated and blocks missing redelivery until repaired', async () => {
    const first = await fixture();
    const second = await fixture();
    await first.begin(delivery('one', 'first-secret'));
    await first.commit();
    await second.begin(delivery('one', 'second-secret'));
    await second.commit();
    await expect(first.begin()).rejects.toThrow('invalid sandbox network value');
    expect(first.controller.readyForSnapshot).toBe(false);
    expect(second.controller.readyForSnapshot).toBe(true);
    await first.begin(delivery('three', 'third-secret'));
    expect(first.controller.readyForSnapshot).toBe(true);
    expect(
      (await second.raw().run({ tool: 'bash', args: { command: 'printf "%s" "$HTTPS_PROXY"' } }))
        .stdout,
    ).toContain('second-secret');
  });

  it('rejects disabled, non-native and in-memory fallback topologies', async () => {
    const disabled = await fixture(false);
    await expect(disabled.begin(delivery('one', 'secret'))).rejects.toThrow('managed cloud');
    const unsupported = new RunnerResources({
      workspaceDir: '/unused',
      sandboxEgressRequired: true,
    });
    await expect(
      unsupported.push('ses_fixture', {
        type: 'manifest',
        manifest,
        session_egress: delivery('one', 'secret'),
      }),
    ).rejects.toThrow('native placement');
    const fallback = await fixture(true, 'in-memory');
    await fallback.begin(delivery('one', 'secret'));
    await expect(fallback.commit()).rejects.toThrow('cannot install sandbox egress');
    expect(fallback.controller.readyForSnapshot).toBe(false);
    await expect(
      fallback.controller.push('ses_fixture', { type: 'git_capabilities', capabilities: [] }),
    ).rejects.toThrow('rejects legacy Git');
    expect(
      () =>
        new SessionLoop({
          workspaceId: 'ws',
          providers: new ProviderRegistry(),
          sandboxEgressRequired: true,
        }),
    ).toThrow('managed Linux');
  });
});
