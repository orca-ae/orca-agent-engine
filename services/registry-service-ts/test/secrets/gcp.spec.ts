// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import type { GcpClientFactory } from '../../src/secrets/gcp.js';
import { GcpSecretManagerSecretProvider, gcpIsSupported } from '../../src/secrets/gcp.js';

describe('GcpSecretManagerSecretProvider', () => {
  const fakeClient = { accessSecretVersion: vi.fn() };
  const factory: GcpClientFactory = () => fakeClient as unknown as ReturnType<GcpClientFactory>;
  const p = new GcpSecretManagerSecretProvider(factory);

  it('isSupported recognizes gcpsm://', () => {
    expect(gcpIsSupported('gcpsm://my-project/my-secret')).toBe(true);
    expect(gcpIsSupported('gcpsm://my-project/my-secret/versions/3#k')).toBe(true);
  });

  it('resolves with default version', async () => {
    fakeClient.accessSecretVersion.mockResolvedValueOnce([
      { payload: { data: Buffer.from('val') } },
    ]);
    expect(await p.resolve('gcpsm://my-project/my-secret')).toBe('val');
    expect(fakeClient.accessSecretVersion).toHaveBeenCalledWith({
      name: 'projects/my-project/secrets/my-secret/versions/latest',
    });
  });

  it('resolves with explicit version + JSON key', async () => {
    fakeClient.accessSecretVersion.mockResolvedValueOnce([
      { payload: { data: Buffer.from('{"k":"v"}') } },
    ]);
    expect(await p.resolve('gcpsm://my-project/my-secret/versions/5#k')).toBe('v');
  });
});
