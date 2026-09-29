// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { buildRuntimeSecretResolver } from '../../src/secrets/runtime-resolver.js';

describe('buildRuntimeSecretResolver', () => {
  it('resolves SecretStore-owned local references before legacy providers', async () => {
    const secretStore = {
      resolve: vi.fn().mockResolvedValue('stored-token'),
      put: vi.fn(),
      delete: vi.fn(),
    };
    const secretProvider = { resolve: vi.fn().mockResolvedValue('legacy-token') };

    const resolver = buildRuntimeSecretResolver(secretProvider, secretStore);

    await expect(resolver.resolve('local:git_credentials/ws/cred/token')).resolves.toBe(
      'stored-token',
    );
    expect(secretProvider.resolve).not.toHaveBeenCalled();
  });

  it('does not query SecretStore for external provider references', async () => {
    const secretStore = {
      resolve: vi.fn().mockRejectedValue(new Error('store unavailable')),
      put: vi.fn(),
      delete: vi.fn(),
    };
    const secretProvider = { resolve: vi.fn().mockResolvedValue('external-token') };

    const resolver = buildRuntimeSecretResolver(secretProvider, secretStore);

    await expect(resolver.resolve('awssm://us-east-1/github-token')).resolves.toBe(
      'external-token',
    );
    expect(secretStore.resolve).not.toHaveBeenCalled();
  });

  it('never treats an unresolved local reference as literal secret material', async () => {
    const secretStore = {
      resolve: vi.fn().mockResolvedValue(null),
      put: vi.fn(),
      delete: vi.fn(),
    };
    const secretProvider = {
      resolve: vi.fn(async (reference: string) => reference),
    };

    const resolver = buildRuntimeSecretResolver(secretProvider, secretStore);

    await expect(resolver.resolve('local:missing')).resolves.toBeNull();
  });
});
