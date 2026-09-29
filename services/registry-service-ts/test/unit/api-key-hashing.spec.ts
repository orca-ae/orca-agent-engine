// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyRequest } from 'fastify';
import { describe, it, expect, vi } from 'vitest';
import {
  buildApiKeyAuth,
  createLegacyApiKeyFallbackLimiter,
  fingerprintApiKey,
  generateApiKey,
  hashApiKey,
  isLegacyApiKeyFingerprint,
  verifyApiKey,
} from '../../src/auth/api-key.js';
import {
  fingerprintAdminApiKey,
  generateAdminApiKey,
  hashAdminApiKey,
  isAdminApiKey,
  parseAdminApiKeyScopes,
} from '../../src/auth/admin-api-key.js';
import {
  fingerprintPlatformApiKey,
  generatePlatformApiKey,
  hashPlatformApiKey,
  isPlatformApiKey,
} from '../../src/auth/platform-api-key.js';
import * as argon2 from 'argon2';
import type { DbClient } from '../../src/persistence/postgres/client.js';

describe('api-key hashing', () => {
  it('round-trips: hash then verify with the same plaintext returns true', async () => {
    const plaintext = 'orca_test_abc123def456';
    const hashed = await hashApiKey(plaintext);
    expect(hashed).toMatch(/^\$argon2id\$/);
    expect(await verifyApiKey(plaintext, hashed)).toBe(true);
  });

  it('verify with wrong plaintext returns false', async () => {
    const hashed = await hashApiKey('orca_correct');
    expect(await verifyApiKey('orca_wrong', hashed)).toBe(false);
  });

  it('builds a deterministic domain-separated lookup fingerprint', () => {
    expect(fingerprintApiKey('orca_same')).toBe(fingerprintApiKey('orca_same'));
    expect(fingerprintApiKey('orca_same')).not.toBe(fingerprintApiKey('orca_other'));
    expect(fingerprintApiKey('orca_same')).toMatch(/^[0-9a-f]{64}$/);
  });

  it('detects only legacy api-key fingerprint placeholders', () => {
    expect(isLegacyApiKeyFingerprint('legacy:apikey_seed')).toBe(true);
    expect(isLegacyApiKeyFingerprint(fingerprintApiKey('orca_same'))).toBe(false);
    expect(isLegacyApiKeyFingerprint('not-legacy:apikey_seed')).toBe(false);
  });

  it('bounds legacy fallback by concurrency, source, and fixed window', () => {
    let now = 0;
    const limiter = createLegacyApiKeyFallbackLimiter({
      now: () => now,
      windowMs: 10_000,
      maxAttemptsPerWindow: 2,
      maxAttemptsPerSource: 1,
      maxConcurrent: 1,
    });

    const first = limiter.acquire('192.0.2.1');
    expect(first.acquired).toBe(true);
    expect(limiter.acquire('192.0.2.2')).toEqual({
      acquired: false,
      retryAfterSeconds: 1,
    });
    if (first.acquired) first.release();

    expect(limiter.acquire('192.0.2.1')).toEqual({
      acquired: false,
      retryAfterSeconds: 10,
    });
    const second = limiter.acquire('192.0.2.2');
    expect(second.acquired).toBe(true);
    if (second.acquired) second.release();
    expect(limiter.acquire('192.0.2.3')).toEqual({
      acquired: false,
      retryAfterSeconds: 10,
    });

    now = 10_000;
    const afterReset = limiter.acquire('192.0.2.1');
    expect(afterReset.acquired).toBe(true);
    if (afterReset.acquired) afterReset.release();
  });

  it('generateApiKey returns a string with the orca_ prefix', () => {
    const key = generateApiKey();
    expect(key).toMatch(/^orca_[A-Za-z0-9_-]{32,}$/);
  });

  it('uses a separate prefix and fingerprint domain for organization admin keys', async () => {
    const key = generateAdminApiKey();
    const hash = await hashAdminApiKey(key);

    expect(key).toMatch(/^orca_admin_[A-Za-z0-9_-]{32,}$/);
    expect(isAdminApiKey(key)).toBe(true);
    expect(isAdminApiKey('orca_admin_too-short')).toBe(false);
    expect(await argon2.verify(hash, key)).toBe(true);
    expect(fingerprintAdminApiKey(key)).not.toBe(fingerprintApiKey(key));
  });

  it('uses a separate prefix and fingerprint domain for platform keys', async () => {
    const key = generatePlatformApiKey();
    const hash = await hashPlatformApiKey(key);

    expect(key).toMatch(/^orca_platform_[A-Za-z0-9_-]{32,}$/);
    expect(isPlatformApiKey(key)).toBe(true);
    expect(isPlatformApiKey('orca_platform_too-short')).toBe(false);
    expect(await argon2.verify(hash, key)).toBe(true);
    expect(fingerprintPlatformApiKey(key)).not.toBe(fingerprintAdminApiKey(key));
    expect(fingerprintPlatformApiKey(key)).not.toBe(fingerprintApiKey(key));
  });

  it('parses and validates delegated organization admin scopes', () => {
    expect(parseAdminApiKeyScopes(undefined)).toEqual(['org:admin']);
    expect(
      parseAdminApiKeyScopes(
        'workspaces:read, observability:read, observability:write, observability:rotate, api_keys:write, observability:read',
      ),
    ).toEqual([
      'workspaces:read',
      'observability:read',
      'observability:write',
      'observability:rotate',
      'api_keys:write',
    ]);
    expect(() => parseAdminApiKeyScopes('')).toThrow(/at least one admin scope/);
    expect(() => parseAdminApiKeyScopes('workspaces:read,unknown')).toThrow(
      /unsupported scopes: unknown/,
    );
  });

  it('includes the stable API key id in the authenticated principal', async () => {
    const plaintext = 'orca_actor_key';
    const row = {
      id: 'key_actor',
      workspaceId: 'ws_actor',
      hashedKey: await hashApiKey(plaintext),
      keyFingerprint: fingerprintApiKey(plaintext),
      principal: 'actor-principal',
      scopes: ['memory:write'],
      status: 'active',
      revokedAt: null,
      expiresAt: null,
    };
    const limit = vi.fn(async () => [{ key: row }]);
    const db = {
      select: () => ({
        from: () => ({
          innerJoin: () => ({
            where: () => ({ limit }),
          }),
        }),
      }),
      update: () => ({
        set: () => ({
          where: vi.fn(async () => undefined),
        }),
      }),
    } as unknown as DbClient;

    const result = await buildApiKeyAuth(db)({
      headers: { 'x-api-key': plaintext },
    } as unknown as FastifyRequest);

    expect(result).toMatchObject({
      outcome: 'authenticated',
      principal: {
        workspaceId: 'ws_actor',
        principal: 'actor-principal',
        authMethod: 'api-key',
        apiKeyId: 'key_actor',
      },
    });
  });

  /**
   * `absent` is the only outcome that lets `buildAuth` run OIDC, so which
   * inputs land there is the whole security boundary. These pin it at the unit
   * level with a database that throws if touched: every case below must be
   * decided on the header alone, before any lookup.
   */
  const unreachableDb = () =>
    ({
      select: () => {
        throw new Error('must not reach the database');
      },
    }) as unknown as DbClient;

  it.each([
    ['a malformed key', 'not-orca-shaped'],
    ['an empty key', ''],
    ['a whitespace-only key', '   '],
    ['an admin key on the public listener', 'orca_admin_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'],
    ['a platform key on the public listener', 'orca_platform_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'],
  ])('rejects %s before any database lookup', async (_label, header) => {
    await expect(
      buildApiKeyAuth(unreachableDb())({ headers: { 'x-api-key': header } } as FastifyRequest),
    ).resolves.toEqual({ outcome: 'rejected' });
  });

  /**
   * Cannot arise on a real listener — Node joins duplicate `x-api-key` lines
   * into one comma-separated string, which the integration suite confirms over
   * a real socket. Pinned here because the branch decides on *presence* rather
   * than on string-ness precisely so a value we cannot interpret denies instead
   * of delegating to OIDC.
   */
  it('rejects rather than delegating when x-api-key is not a string', async () => {
    await expect(
      buildApiKeyAuth(unreachableDb())({
        headers: { 'x-api-key': ['orca_one', 'orca_two'] },
      } as unknown as FastifyRequest),
    ).resolves.toEqual({ outcome: 'rejected' });
  });

  /**
   * What a real listener actually presents for duplicate headers. The joined
   * value keeps the `orca_` prefix, so it is looked up like any other key and
   * misses — `rejected`, never `absent`.
   */
  it('rejects duplicate x-api-key headers once Node has joined them', async () => {
    const emptyDb = {
      select: () => ({
        from: () => ({
          innerJoin: () => ({
            where: () => ({
              limit: async () => [],
              orderBy: () => ({ limit: async () => [] }),
            }),
          }),
        }),
      }),
    } as unknown as DbClient;

    await expect(
      buildApiKeyAuth(emptyDb)({
        headers: { 'x-api-key': 'orca_one, orca_two' },
        ip: '198.51.100.9',
      } as FastifyRequest),
    ).resolves.toEqual({ outcome: 'rejected' });
  });

  it('reports an absent x-api-key as absent so OIDC may run', async () => {
    await expect(
      buildApiKeyAuth(unreachableDb())({ headers: {} } as FastifyRequest),
    ).resolves.toEqual({ outcome: 'absent' });
  });
});
