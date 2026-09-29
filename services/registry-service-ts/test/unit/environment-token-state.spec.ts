// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  armEnvironmentTokenColumns,
  revokeEnvironmentTokenColumns,
  verifyEnvironmentTokenForEnvironment,
} from '../../src/domain/environment-token-state.js';
import {
  hashEnvironmentToken,
  verifyEnvironmentToken,
  ENVIRONMENT_TOKEN_PREFIX,
  ENVIRONMENT_TOKEN_TTL_MS,
} from '../../src/domain/environment-token.js';

/**
 * Route-level credential-lifecycle invariants for the per-launch Environment
 * Token, unit-tested without a DB — mirrors `environment-key-state.spec.ts`.
 */

describe('environment-token-state — armEnvironmentTokenColumns (mint)', () => {
  it('persists only the digest + a future expiry, and surfaces the raw once', () => {
    const now = new Date('2026-06-18T00:00:00.000Z');
    const armed = armEnvironmentTokenColumns(undefined, now);

    expect(armed.raw.startsWith(ENVIRONMENT_TOKEN_PREFIX)).toBe(true);
    expect(armed.environmentTokenDigest).toBe(hashEnvironmentToken(armed.raw));
    expect(armed.environmentTokenDigest).not.toBe(armed.raw);
    expect(armed.environmentTokenExpiresAt.getTime()).toBe(
      now.getTime() + ENVIRONMENT_TOKEN_TTL_MS,
    );
  });

  it('honors an explicit ttlMs, e.g. a shorter per-launch window', () => {
    const now = new Date('2026-06-18T00:00:00.000Z');
    const armed = armEnvironmentTokenColumns(120_000, now);
    expect(armed.environmentTokenExpiresAt.getTime()).toBe(now.getTime() + 120_000);
  });

  it('arms a state the raw token verifies against on a live environment', () => {
    const now = new Date('2026-06-18T00:00:00.000Z');
    const armed = armEnvironmentTokenColumns(undefined, now);
    expect(verifyEnvironmentTokenForEnvironment(armed.raw, armed, false, now)).toBe(true);
  });

  it('mints a fresh token each call whose digest revokes the prior one (relaunch semantics)', () => {
    const now = new Date('2026-06-18T00:00:00.000Z');
    const first = armEnvironmentTokenColumns(undefined, now);
    const second = armEnvironmentTokenColumns(undefined, now);
    expect(second.raw).not.toBe(first.raw);
    expect(second.environmentTokenDigest).not.toBe(first.environmentTokenDigest);
    // Persisting `second` in place (a relaunch re-crediting the same row)
    // atomically revokes `first` — its raw no longer verifies.
    expect(
      verifyEnvironmentToken(
        first.raw,
        second.environmentTokenDigest,
        second.environmentTokenExpiresAt,
        now,
      ),
    ).toBe(false);
  });

  it('defaults `now` to the current time when omitted', () => {
    const before = Date.now();
    const armed = armEnvironmentTokenColumns();
    const after = Date.now();
    expect(armed.environmentTokenExpiresAt.getTime()).toBeGreaterThanOrEqual(
      before + ENVIRONMENT_TOKEN_TTL_MS,
    );
    expect(armed.environmentTokenExpiresAt.getTime()).toBeLessThanOrEqual(
      after + ENVIRONMENT_TOKEN_TTL_MS,
    );
  });
});

describe('environment-token-state — revokeEnvironmentTokenColumns (the launch-lifecycle cleanup path)', () => {
  it('clears both the digest and expiry, keeping the environment row', () => {
    expect(revokeEnvironmentTokenColumns()).toEqual({
      environmentTokenDigest: null,
      environmentTokenExpiresAt: null,
    });
  });

  it('produces a state an armed raw token no longer verifies against', () => {
    const now = new Date('2026-06-18T00:00:00.000Z');
    const armed = armEnvironmentTokenColumns(undefined, now);
    const revoked = revokeEnvironmentTokenColumns();
    expect(verifyEnvironmentTokenForEnvironment(armed.raw, revoked, false, now)).toBe(false);
  });
});

describe('environment-token-state — verifyEnvironmentTokenForEnvironment (the resolve/auth path)', () => {
  const now = new Date('2026-06-18T00:00:00.000Z');

  it('authenticates the right token against a live, armed environment', () => {
    const armed = armEnvironmentTokenColumns(undefined, now);
    expect(verifyEnvironmentTokenForEnvironment(armed.raw, armed, false, now)).toBe(true);
  });

  it('rejects the wrong token', () => {
    const armed = armEnvironmentTokenColumns(undefined, now);
    expect(verifyEnvironmentTokenForEnvironment('et-not-the-token', armed, false, now)).toBe(false);
  });

  it('rejects once the token has expired', () => {
    const armed = armEnvironmentTokenColumns(undefined, now);
    const past = new Date(armed.environmentTokenExpiresAt.getTime() + 1);
    expect(verifyEnvironmentTokenForEnvironment(armed.raw, armed, false, past)).toBe(false);
  });

  it('rejects an unarmed (never-minted / revoked) environment', () => {
    const armed = armEnvironmentTokenColumns(undefined, now);
    expect(
      verifyEnvironmentTokenForEnvironment(
        armed.raw,
        { environmentTokenDigest: null, environmentTokenExpiresAt: null },
        false,
        now,
      ),
    ).toBe(false);
  });

  it('rejects an archived environment even with a still-matching digest', () => {
    const armed = armEnvironmentTokenColumns(undefined, now);
    // Same valid, unexpired credential — archive alone must fail the auth.
    expect(verifyEnvironmentTokenForEnvironment(armed.raw, armed, true, now)).toBe(false);
  });

  it('treats the expiry boundary as still valid at exactly the expiry instant', () => {
    const armed = armEnvironmentTokenColumns(undefined, now);
    expect(
      verifyEnvironmentTokenForEnvironment(
        armed.raw,
        armed,
        false,
        armed.environmentTokenExpiresAt,
      ),
    ).toBe(true);
  });

  it('is id-scoped: a token armed for one environment does not resolve against another', () => {
    // Two independently armed environments — env A's raw token must not verify
    // against env B's stored digest, even though both are live and unexpired.
    const envA = armEnvironmentTokenColumns(undefined, now);
    const envB = armEnvironmentTokenColumns(undefined, now);
    expect(verifyEnvironmentTokenForEnvironment(envA.raw, envB, false, now)).toBe(false);
    expect(verifyEnvironmentTokenForEnvironment(envB.raw, envA, false, now)).toBe(false);
  });

  it('does not throw on a corrupt/short stored digest (constant-time compare guard)', () => {
    const armed = armEnvironmentTokenColumns(undefined, now);
    expect(
      verifyEnvironmentTokenForEnvironment(
        armed.raw,
        {
          environmentTokenDigest: 'deadbeef',
          environmentTokenExpiresAt: armed.environmentTokenExpiresAt,
        },
        false,
        now,
      ),
    ).toBe(false);
  });
});
