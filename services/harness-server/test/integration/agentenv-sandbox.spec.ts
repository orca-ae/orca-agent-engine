// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { AgentEnvRuntime } from '../../src/sandbox/agentenv/runtime.js';
import {
  buildSandboxWritePolicy,
  createPolicyEnforcedSandbox,
} from '../../src/sandbox/write-policy.js';

const baseUrl = process.env['AGENTENV_BASE_URL'];
const apiKey = process.env['AGENTENV_API_KEY'];
const image = process.env['AGENTENV_IMAGE'];
const skip = !baseUrl || !apiKey || !image;

describe.skipIf(skip)(`AgentEnvRuntime (live ${baseUrl})`, () => {
  if (skip) {
    console.warn(
      'agentenv-sandbox: skipping — AGENTENV_BASE_URL, AGENTENV_API_KEY, and AGENTENV_IMAGE are required',
    );
  }

  it('round-trips files, enforces write policy, and preserves state across pause/resume', async () => {
    const runtime = new AgentEnvRuntime({
      baseUrl: baseUrl!,
      apiKey: apiKey!,
      image: image!,
      timeoutSeconds: 600,
      requestTimeoutSeconds: 180,
    });
    const sandbox = await runtime.acquire({});
    try {
      await sandbox.files.write('/tmp/orca-agentenv.txt', Buffer.from('hello agentenv'));
      const readBack = await sandbox.files.read('/tmp/orca-agentenv.txt');
      expect(readBack.toString('utf8')).toBe('hello agentenv');

      const entries = await sandbox.files.list('/tmp');
      expect(entries).toContain('orca-agentenv.txt');

      const agentSandbox = await createPolicyEnforcedSandbox(sandbox, buildSandboxWritePolicy([]));
      const isolation = await agentSandbox.run({
        tool: 'bash',
        args: {
          command: [
            'test "$(id -u):$(id -g)" = 1000:1000',
            "printf '%s' persisted > /mnt/session/outputs/agentenv.txt",
            'test ! -w /etc',
            `awk '$1 == "CapEff:" { exit $2 == "0000000000000000" ? 0 : 1 }' /proc/self/status`,
            `awk '$1 == "NoNewPrivs:" { exit $2 == "1" ? 0 : 1 }' /proc/self/status`,
          ].join(' && '),
        },
      });
      expect(isolation.exit_code, isolation.stderr).toBe(0);

      await sandbox.pause();
      await sandbox.resume();
      const persisted = await sandbox.files.read('/mnt/session/outputs/agentenv.txt');
      expect(persisted.toString('utf8')).toBe('persisted');
    } finally {
      await sandbox.destroy();
    }
  }, 180_000);
});
