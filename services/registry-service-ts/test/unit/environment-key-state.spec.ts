// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  armEnvKeyColumns,
  revokeEnvKeyColumns,
  verifyEnvKeyForEnvironment,
} from '../../src/domain/environment-key-state.js';
import {
  hashEnvKey,
  verifyEnvKey,
  ENV_KEY_PREFIX,
  ENV_KEY_TTL_MS,
} from '../../src/domain/environment-key.js';

/**
 * Route-level credential-lifecycle invariants, unit-tested without a DB. These
 * cover the column patches the create / rotate / revoke / archive handlers
 * persist — the "raw returned once, digest-only stored, revoke clears, archived
 * never authenticates" guarantees that were previously only asserted in the
 * infra-gated integration suite.
 */

describe('environment-key-state — armEnvKeyColumns (create + rotate)', () => {
  it('persists only the digest + a future expiry, and surfaces the raw once', () => {
    const now = new Date('2026-06-18T00:00:00.000Z');
    const armed = armEnvKeyColumns(now);

    // The raw `sk-` key is the credential the route returns exactly once.
    expect(armed.raw.startsWith(ENV_KEY_PREFIX)).toBe(true);
    // The persisted column is the digest of that raw key — never the raw key.
    expect(armed.envKeyDigest).toBe(hashEnvKey(armed.raw));
    expect(armed.envKeyDigest).not.toBe(armed.raw);
    // Expiry is now + the policy TTL.
    expect(armed.envKeyExpiresAt.getTime()).toBe(now.getTime() + ENV_KEY_TTL_MS);
  });

  it('arms a state the raw key verifies against on a live environment', () => {
    const now = new Date('2026-06-18T00:00:00.000Z');
    const armed = armEnvKeyColumns(now);
    expect(verifyEnvKeyForEnvironment(armed.raw, armed, false, now)).toBe(true);
  });

  it('rotates to a fresh key whose digest revokes the prior key', () => {
    const now = new Date('2026-06-18T00:00:00.000Z');
    const first = armEnvKeyColumns(now);
    const second = armEnvKeyColumns(now);
    // A fresh draw each call — the prior raw no longer verifies against the new
    // stored digest, so persisting `second` atomically revokes `first`.
    expect(second.raw).not.toBe(first.raw);
    expect(second.envKeyDigest).not.toBe(first.envKeyDigest);
    expect(verifyEnvKey(first.raw, second.envKeyDigest, second.envKeyExpiresAt, now)).toBe(false);
  });

  it('defaults `now` to the current time when omitted', () => {
    const before = Date.now();
    const armed = armEnvKeyColumns();
    const after = Date.now();
    expect(armed.envKeyExpiresAt.getTime()).toBeGreaterThanOrEqual(before + ENV_KEY_TTL_MS);
    expect(armed.envKeyExpiresAt.getTime()).toBeLessThanOrEqual(after + ENV_KEY_TTL_MS);
  });
});

describe('environment-key-state — revokeEnvKeyColumns (explicit revoke + archive)', () => {
  it('clears both the digest and the expiry', () => {
    expect(revokeEnvKeyColumns()).toEqual({ envKeyDigest: null, envKeyExpiresAt: null });
  });

  it('produces a state that no raw key authenticates against', () => {
    const armed = armEnvKeyColumns(new Date());
    const cleared = revokeEnvKeyColumns();
    // After revoke, even the just-issued raw key fails closed.
    expect(verifyEnvKeyForEnvironment(armed.raw, cleared, false, new Date())).toBe(false);
  });
});

describe('environment-key-state — verifyEnvKeyForEnvironment (the verify/auth path)', () => {
  const now = new Date('2026-06-18T00:00:00.000Z');

  it('authenticates the right key against a live, armed environment', () => {
    const armed = armEnvKeyColumns(now);
    expect(verifyEnvKeyForEnvironment(armed.raw, armed, false, now)).toBe(true);
  });

  it('rejects the wrong key', () => {
    const armed = armEnvKeyColumns(now);
    expect(verifyEnvKeyForEnvironment('sk-not-the-key', armed, false, now)).toBe(false);
  });

  it('rejects once the key has expired', () => {
    const armed = armEnvKeyColumns(now);
    const past = new Date(armed.envKeyExpiresAt.getTime() + 1);
    expect(verifyEnvKeyForEnvironment(armed.raw, armed, false, past)).toBe(false);
  });

  it('rejects a revoked (unarmed) environment', () => {
    const armed = armEnvKeyColumns(now);
    expect(
      verifyEnvKeyForEnvironment(
        armed.raw,
        { envKeyDigest: null, envKeyExpiresAt: null },
        false,
        now,
      ),
    ).toBe(false);
  });

  it('rejects an archived environment even with a still-matching digest', () => {
    const armed = armEnvKeyColumns(now);
    // Same valid, unexpired credential — archive alone must fail the auth, since
    // archive revokes the credential as teardown.
    expect(verifyEnvKeyForEnvironment(armed.raw, armed, true, now)).toBe(false);
  });

  it('treats the expiry boundary as still valid at exactly the expiry instant', () => {
    const armed = armEnvKeyColumns(now);
    expect(verifyEnvKeyForEnvironment(armed.raw, armed, false, armed.envKeyExpiresAt)).toBe(true);
  });
});
