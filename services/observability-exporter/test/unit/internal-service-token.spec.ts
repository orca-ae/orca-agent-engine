// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { buildInternalServiceTokenProvider } from '../../src/internal-service-token.js';

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(temporaryDirectories.splice(0).map((path) => rm(path, { recursive: true })));
});

describe('buildInternalServiceTokenProvider', () => {
  it('reads the token file for every resolver call so projected service-account tokens can rotate', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'orca-observability-token-'));
    temporaryDirectories.push(directory);
    const path = join(directory, 'token');
    const firstToken = 'a'.repeat(32);
    const secondToken = 'b'.repeat(32);
    await writeFile(path, `${firstToken}\n`);
    const provider = buildInternalServiceTokenProvider({ tokenFile: path });

    await expect(provider()).resolves.toBe(firstToken);
    await writeFile(path, `${secondToken}\n`);
    await expect(provider()).resolves.toBe(secondToken);
  });

  it('rejects missing, dual, and malformed sources', () => {
    expect(() => buildInternalServiceTokenProvider({})).toThrow('exactly one');
    expect(() =>
      buildInternalServiceTokenProvider({ token: 'token', tokenFile: '/var/run/token' }),
    ).toThrow('exactly one');
    expect(() => buildInternalServiceTokenProvider({ token: 'bad token' })).toThrow('invalid');
    expect(() => buildInternalServiceTokenProvider({ token: 'x'.repeat(31) })).toThrow('invalid');
  });
});
