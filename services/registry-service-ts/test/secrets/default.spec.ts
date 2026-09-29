// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import { DefaultSecretProvider } from '../../src/secrets/default.js';
import { EnvSecretProvider } from '../../src/secrets/env.js';

describe('DefaultSecretProvider', () => {
  it('routes env: to EnvSecretProvider', async () => {
    const env = new EnvSecretProvider(() => 'envvalue');
    const def = new DefaultSecretProvider(env, []);
    expect(await def.resolve('env:FOO')).toBe('envvalue');
  });

  it('falls through to a delegated provider if its scheme matches', async () => {
    const env = new EnvSecretProvider(() => null);
    const fake = { resolve: vi.fn().mockResolvedValue('fakevalue') };
    const def = new DefaultSecretProvider(env, [
      { provider: fake, isSupported: (r) => r.startsWith('fake:') },
    ]);
    expect(await def.resolve('fake:bar')).toBe('fakevalue');
  });

  it('returns input as-is if no provider matches', async () => {
    const env = new EnvSecretProvider(() => null);
    const def = new DefaultSecretProvider(env, []);
    expect(await def.resolve('plain')).toBe('plain');
  });

  it('returns null on empty input', async () => {
    const env = new EnvSecretProvider(() => null);
    const def = new DefaultSecretProvider(env, []);
    expect(await def.resolve('')).toBeNull();
    expect(await def.resolve('   ')).toBeNull();
  });
});
