// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import type { AzureClientFactory } from '../../src/secrets/azure.js';
import { AzureKeyVaultSecretProvider, azureIsSupported } from '../../src/secrets/azure.js';

describe('AzureKeyVaultSecretProvider', () => {
  const fakeClient = { getSecret: vi.fn() };
  const factory = vi.fn().mockReturnValue(fakeClient) as unknown as AzureClientFactory;
  const p = new AzureKeyVaultSecretProvider(factory);

  it('isSupported recognizes azurekv://', () => {
    expect(azureIsSupported('azurekv://myvault.vault.azure.net/secrets/my-secret')).toBe(true);
    expect(azureIsSupported('azurekv://myvault.vault.azure.net/secrets/my-secret/abc123')).toBe(
      true,
    );
  });

  it('resolves a secret without explicit version', async () => {
    fakeClient.getSecret.mockResolvedValueOnce({ value: 'val' });
    expect(await p.resolve('azurekv://myvault.vault.azure.net/secrets/my-secret')).toBe('val');
    expect(factory).toHaveBeenCalledWith('https://myvault.vault.azure.net');
    expect(fakeClient.getSecret).toHaveBeenCalledWith('my-secret', undefined);
  });

  it('resolves with explicit version', async () => {
    fakeClient.getSecret.mockResolvedValueOnce({ value: 'val' });
    await p.resolve('azurekv://myvault.vault.azure.net/secrets/my-secret/abc123');
    expect(fakeClient.getSecret).toHaveBeenCalledWith('my-secret', { version: 'abc123' });
  });

  it('extracts a JSON key when fragment present', async () => {
    fakeClient.getSecret.mockResolvedValueOnce({ value: '{"username":"u","password":"p"}' });
    expect(await p.resolve('azurekv://myvault.vault.azure.net/secrets/db#password')).toBe('p');
  });

  it('returns null when value is missing', async () => {
    fakeClient.getSecret.mockResolvedValueOnce({ value: undefined });
    expect(await p.resolve('azurekv://myvault.vault.azure.net/secrets/empty')).toBeNull();
  });
});
