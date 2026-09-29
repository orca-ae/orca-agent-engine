// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { buildInternalServiceTokenProvider } from '../../src/auth/internal-service-token.js';

const TOKEN_A = 'harness-internal-service-token-at-least-32-chars';
const TOKEN_B = 'rotated-internal-service-token-at-least-32-chars';

describe('Harness internal service token provider', () => {
  it('requires exactly one sufficiently long token source', async () => {
    expect(() => buildInternalServiceTokenProvider({})).toThrow(/exactly one/);
    expect(() =>
      buildInternalServiceTokenProvider({ token: TOKEN_A, tokenFile: '/tmp/token' }),
    ).toThrow(/exactly one/);
    expect(() => buildInternalServiceTokenProvider({ token: 'short' })).toThrow(/at least 32/);
    await expect(buildInternalServiceTokenProvider({ token: TOKEN_A })()).resolves.toBe(TOKEN_A);
  });

  it('rereads token files so projected ServiceAccount JWTs can rotate', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'orca-harness-token-'));
    const path = join(dir, 'token');
    try {
      await writeFile(path, `${TOKEN_A}\n`);
      const provider = buildInternalServiceTokenProvider({ tokenFile: path });
      await expect(provider()).resolves.toBe(TOKEN_A);
      await writeFile(path, `${TOKEN_B}\n`);
      await expect(provider()).resolves.toBe(TOKEN_B);
      await rm(path);
      await expect(provider()).rejects.toThrow();
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
