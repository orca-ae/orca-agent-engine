// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';
import { E2BSandboxRuntime } from '../../src/sandbox/e2b/runtime.js';
import { OpenSandboxRuntime } from '../../src/sandbox/opensandbox/runtime.js';
import { AgentEnvRuntime } from '../../src/sandbox/agentenv/runtime.js';

describe('SandboxRuntime capabilities', () => {
  describe('InMemorySandboxRuntime', () => {
    it('advertises supportsFuse=false', () => {
      const rt = new InMemorySandboxRuntime();
      expect(rt.capabilities.supportsFuse).toBe(false);
      expect(rt.capabilities.supportsLocalMemory).toBe(true);
      expect(rt.capabilities.supportsWritePolicy).toBe(true);
    });

    it('runPrivileged on the acquired handle throws with a clear message', async () => {
      const rt = new InMemorySandboxRuntime();
      const sb = await rt.acquire({});
      try {
        await expect(sb.runPrivileged('whatever')).rejects.toThrowError(
          'InMemorySandboxRuntime does not support privileged operations',
        );
      } finally {
        await sb.destroy();
      }
    });

    it('runPrivileged still throws when envs option is supplied', async () => {
      const rt = new InMemorySandboxRuntime();
      const sb = await rt.acquire({});
      try {
        await expect(
          sb.runPrivileged('mkdir -p /mnt/x', { envs: { AWS_ACCESS_KEY_ID: 'x' } }),
        ).rejects.toThrow(/InMemorySandboxRuntime/);
      } finally {
        await sb.destroy();
      }
    });

    it('accepts filesystem-root preparation inside its private workdir', async () => {
      const sb = await new InMemorySandboxRuntime().acquire({});
      try {
        await expect(
          sb.prepareFilesystemRoots!(['/mnt/inputs', '/workspace/skills']),
        ).resolves.toBeUndefined();
      } finally {
        await sb.destroy();
      }
    });
  });

  describe('E2BSandboxRuntime', () => {
    it('advertises supportsFuse=true on the runtime instance (no acquire required)', () => {
      // Constructing the runtime is safe — `Sandbox.create` is only called
      // inside `acquire()`, and we don't acquire here. This lets us assert
      // the static capability flag without an E2B API key.
      const rt = new E2BSandboxRuntime({ apiKey: 'unused-test-key' });
      expect(rt.capabilities.supportsFuse).toBe(true);
      expect(rt.capabilities.supportsWritePolicy).toBe(true);
    });
  });

  describe('OpenSandboxRuntime', () => {
    it('always advertises FUSE and no local-memory fallback', () => {
      const rt = new OpenSandboxRuntime({
        domain: 'localhost:18080',
        protocol: 'http',
        image: 'opensandbox/code-interpreter:v1.0.2',
        timeoutSeconds: 1800,
        useServerProxy: true,
        requestTimeoutSeconds: 30,
      });
      expect(rt.capabilities.supportsFuse).toBe(true);
      expect(rt.capabilities.supportsLocalMemory).toBeUndefined();
      expect(rt.capabilities.supportsWritePolicy).toBe(true);
    });
  });

  describe('AgentEnvRuntime', () => {
    it('uses Files API fallback without claiming guest FUSE support', () => {
      const rt = new AgentEnvRuntime({
        baseUrl: 'http://agentenv-gateway.agentenv-system.svc:8080',
        apiKey: 'unused-test-key',
        image: 'registry.example/orca-agentenv:v1',
        timeoutSeconds: 1800,
        requestTimeoutSeconds: 180,
      });
      expect(rt.capabilities.supportsFuse).toBe(false);
      expect(rt.capabilities.supportsLocalMemory).toBe(true);
      expect(rt.capabilities.supportsWritePolicy).toBe(true);
    });
  });
});
