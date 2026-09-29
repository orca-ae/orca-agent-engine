// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { EnvSecretProvider, envIsSupported } from '../../src/secrets/env.js';

describe('EnvSecretProvider', () => {
  const lookup = (k: string) => ({ FOO: 'bar', PASSWORD: 'p@ss' })[k] ?? null;
  const p = new EnvSecretProvider(lookup);

  it('resolves env: prefix', async () => {
    expect(await p.resolve('env:FOO')).toBe('bar');
    expect(await p.resolve('env:MISSING')).toBeNull();
  });

  it('resolves ${VAR} pattern', async () => {
    expect(await p.resolve('${PASSWORD}')).toBe('p@ss');
  });

  it('returns the input verbatim if not a recognized pattern', async () => {
    expect(await p.resolve('plain-string')).toBe('plain-string');
  });

  it('isSupported recognizes both forms', () => {
    expect(envIsSupported('env:X')).toBe(true);
    expect(envIsSupported('${X}')).toBe(true);
    expect(envIsSupported('plain')).toBe(false);
  });
});
