// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer } from 'node:http';
import { once } from 'node:events';
import { beforeEach, expect, it, vi } from 'vitest';
import { inspectDshInstallation } from '@orca/dsh-harness';
import { ProviderRegistry, buildSessionStartInput } from '../../src/harness/provider.js';
import {
  registerDormantDshSdkProvider,
  DSH_DORMANT_REASON,
} from '../../src/harness/dsh-sdk/provider.js';
import { validateDshBootstrapSnapshot } from '../../src/harness/dsh-sdk/snapshot.js';
import { assertDshBootstrapHost } from '../../src/harness/dsh-sdk/sandbox-launch.js';
import { parseSnapshotBody, type RunnerSnapshot } from '../../src/snapshot.js';
import { createRunnerSandboxRuntime } from '../../src/sandbox/seam.js';

vi.mock('@orca/dsh-harness', () => ({ inspectDshInstallation: vi.fn() }));
vi.mock('../../src/harness/dsh-sdk/sandbox-launch.js', () => ({ assertDshBootstrapHost: vi.fn() }));

function snapshot(overrides: Partial<RunnerSnapshot> = {}): RunnerSnapshot {
  return parseSnapshotBody(
    Buffer.from(
      JSON.stringify({
        provider: 'dsh-sdk',
        model: { provider: 'deepseek', id: 'deepseek-v4-flash' },
        system: 'Bootstrap only.',
        allowed_tool_names: [],
        allowed_mcp_server_names: [],
        tool_permissions: {},
        egress: {
          mode: 'gateway',
          gateway: { llm_base_url: 'http://127.0.0.1:1', llm_jwt: 'fixture-only' },
        },
        ...overrides,
      }),
    ),
  );
}

beforeEach(() => vi.resetAllMocks());

it.each([
  { model: { provider: 'anthropic', id: 'claude-sonnet-4-6' } },
  { model: { provider: 'deepseek', id: 'deepseek-v4-flash', effort: 'high' } },
  { allowed_tool_names: ['web_fetch'], default_tool_permission: 'always_allow' as const },
  { allowed_tool_names: ['unknown'], default_tool_permission: 'always_deny' as const },
  { allowed_mcp_server_names: ['remote'] },
  { skills_plugin_dir: '/skills' },
  { custom_spec: {} },
  { harness_state: {} },
  { guardrail_state: { budget: 1 } },
  { managed_resources: { version: 1 as const, revision: 'a'.repeat(64) } },
  { sandbox_resources: { version: 1 as const, revision: 'a'.repeat(64), placement_id: 'p' } },
  { egress: { mode: 'sidecar' } },
  {
    egress: {
      mode: 'gateway',
      gateway: { llm_base_url: 'https://provider.invalid/v1', llm_jwt: 'x' },
    },
  },
  {
    egress: {
      mode: 'gateway',
      gateway: {
        llm_base_url: 'https://gateway.invalid',
        llm_jwt: 'x',
        mcp_servers: { hidden: {} },
      },
    },
  },
])('rejects unsupported snapshot before dependency/host probes: %j', (overrides) => {
  const registry = new ProviderRegistry();
  registerDormantDshSdkProvider(registry);
  expect(() => registry.build(snapshot(overrides), { workspaceId: 'w', sessionId: 's' })).toThrow();
  expect(assertDshBootstrapHost).not.toHaveBeenCalled();
  expect(inspectDshInstallation).not.toHaveBeenCalled();
});

it('preserves deny-all and requires effective deny for enabled web_fetch', () => {
  expect(() => validateDshBootstrapSnapshot(snapshot())).not.toThrow();
  const input = snapshot({
    allowed_tool_names: ['read', 'bash', 'web_fetch'],
    tool_permissions: { web_fetch: 'always_deny' },
  });
  expect(() => validateDshBootstrapSnapshot(input)).not.toThrow();
  expect(input.allowed_tool_names).toEqual(['read', 'bash', 'web_fetch']);
});

it('sanitizes malformed Gateway URLs without retaining the input or cause', () => {
  const input = snapshot({
    egress: {
      mode: 'gateway',
      gateway: { llm_base_url: 'invalid-url-with-secret', llm_jwt: 'fixture-only' },
    },
  });
  let failure: unknown;
  try {
    validateDshBootstrapSnapshot(input);
  } catch (error) {
    failure = error;
  }
  expect(failure).toBeInstanceOf(Error);
  expect((failure as Error).message).toBe('DSH bootstrap requires a Gateway origin');
  expect((failure as Error).cause).toBeUndefined();
  expect(String((failure as Error).stack)).not.toContain('invalid-url-with-secret');
});

it('keeps missing dependencies and even a prepared fixture dormant with zero HTTP/tools', async () => {
  let requests = 0;
  const server = createServer((_request, response) => {
    requests++;
    response.end('{}');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('missing fixture port');
  const sandbox = await createRunnerSandboxRuntime({ kind: 'in-memory' }).acquire({});
  const tool = vi.spyOn(sandbox, 'run');
  const spawn = vi.spyOn(sandbox, 'spawn');
  const input = snapshot({
    egress: {
      mode: 'gateway',
      gateway: { llm_base_url: `http://127.0.0.1:${address.port}`, llm_jwt: 'fixture-only' },
    },
  });
  const context = { workspaceId: 'w', sessionId: 's', sandbox };
  const registry = new ProviderRegistry();
  registerDormantDshSdkProvider(registry);
  const harness = registry.build(input, context);
  try {
    vi.mocked(assertDshBootstrapHost).mockImplementationOnce(() => {
      throw new Error('missing SRT');
    });
    await expect(harness.start(buildSessionStartInput(input, context))).rejects.toThrow(
      'missing SRT',
    );
    expect(inspectDshInstallation).not.toHaveBeenCalled();
    vi.mocked(inspectDshInstallation).mockImplementationOnce(() => {
      throw new Error('missing DSH');
    });
    await expect(harness.start(buildSessionStartInput(input, context))).rejects.toThrow(
      'missing DSH',
    );
    await expect(harness.start(buildSessionStartInput(input, context))).rejects.toThrow(
      DSH_DORMANT_REASON,
    );
    await expect(
      harness.submit({ kind: 'user.message', payload: { text: 'never execute' } }),
    ).rejects.toThrow(DSH_DORMANT_REASON);
    await harness.stop('error');
    expect(await harness.events()[Symbol.asyncIterator]().next()).toMatchObject({ done: true });
    expect(requests).toBe(0);
    expect(tool).not.toHaveBeenCalled();
    expect(spawn).not.toHaveBeenCalled();
  } finally {
    await sandbox.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
