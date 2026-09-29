// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import type { SecretsClientFactory } from '../../src/secrets/aws.js';
import { AwsSecretsManagerSecretProvider, awsIsSupported } from '../../src/secrets/aws.js';

describe('AwsSecretsManagerSecretProvider', () => {
  const fakeClient = { send: vi.fn() };
  const factory = vi.fn().mockReturnValue(fakeClient) as unknown as SecretsClientFactory;
  const p = new AwsSecretsManagerSecretProvider(factory);

  it('isSupported recognizes awssm://', () => {
    expect(awsIsSupported('awssm://us-east-1/my-secret')).toBe(true);
    expect(awsIsSupported('awssm://us-east-1/my-secret#k')).toBe(true);
    expect(awsIsSupported('something-else')).toBe(false);
  });

  it('resolves a plain secret', async () => {
    fakeClient.send.mockResolvedValueOnce({ SecretString: 'plainvalue' });
    expect(await p.resolve('awssm://us-west-2/api-token')).toBe('plainvalue');
    expect(factory).toHaveBeenCalledWith('us-west-2');
  });

  it('extracts a JSON key when fragment present', async () => {
    fakeClient.send.mockResolvedValueOnce({ SecretString: '{"username":"u","password":"p"}' });
    expect(await p.resolve('awssm://us-east-1/db#password')).toBe('p');
  });

  it('returns null on unsupported scheme', async () => {
    expect(await p.resolve('not-aws://x')).toBeNull();
  });

  it('falls back to SecretBinary when SecretString is missing', async () => {
    fakeClient.send.mockResolvedValueOnce({
      SecretBinary: new Uint8Array(Buffer.from('binary-secret', 'utf8')),
    });
    expect(await p.resolve('awssm://us-east-1/binary')).toBe('binary-secret');
  });
});
