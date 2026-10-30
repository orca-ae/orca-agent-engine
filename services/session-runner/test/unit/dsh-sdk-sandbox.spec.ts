// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { expect, it } from 'vitest';
import {
  assertDshBootstrapHost,
  probeDshSandboxBootstrap,
} from '../../src/harness/dsh-sdk/sandbox-launch.js';
import { createRunnerSandboxRuntime, resolveRunnerLocalSandbox } from '../../src/sandbox/seam.js';

const candidate =
  process.platform === 'linux' &&
  process.arch === 'x64' &&
  process.versions.node === '22.23.2' &&
  process.getuid?.() !== 0;

it.skipIf(candidate)('rejects the actual unqualified host before sandbox launch', () => {
  expect(() => assertDshBootstrapHost()).toThrow(
    'Linux amd64, Node 22.23.2 and a non-root account',
  );
});

// This is initialization only, not the confinement/cancellation qualification matrix.
it.skipIf(!candidate)(
  'initializes and retires the native profile through real Local+SRT',
  async () => {
    const workspace = await mkdtemp(join(tmpdir(), 'runner-dsh-bootstrap-'));
    let requests = 0;
    const server = createServer((_request, response) => {
      requests++;
      response.end('{}');
    });
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('missing fixture port');
    const origin = `http://127.0.0.1:${address.port}`;
    const runtime = createRunnerSandboxRuntime({
      kind: 'local',
      local: resolveRunnerLocalSandbox({
        harnessWorkDir: workspace,
        env: { AI_GATEWAY_URL: origin },
      }),
    });
    try {
      await probeDshSandboxBootstrap(
        {
          provider: 'dsh-sdk',
          model: { provider: 'deepseek', id: 'deepseek-v4-flash' },
          system: 'Bootstrap only.',
          allowed_tool_names: [],
          allowed_mcp_server_names: [],
          tool_permissions: {},
          egress: { mode: 'gateway', gateway: { llm_base_url: origin, llm_jwt: 'fixture-only' } },
        },
        runtime,
        'ses_bootstrap',
      );
      expect(requests).toBe(0);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await rm(workspace, { recursive: true, force: true });
    }
  },
  45000,
);
