// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { createHash } from 'node:crypto';
import {
  generateEnvironmentToken,
  hashEnvironmentToken,
  verifyEnvironmentToken,
  armEnvironmentToken,
  revokeEnvironmentToken,
  ENVIRONMENT_TOKEN_PREFIX,
  ENVIRONMENT_TOKEN_TTL_MS,
} from '../../src/domain/environment-token.js';
import { ENV_KEY_TTL_MS } from '../../src/domain/environment-key.js';

describe('environment token — ENVIRONMENT_TOKEN_TTL_MS', () => {
  it('is the environment-lifetime TTL (reuses ENV_KEY_TTL_MS), not the old 15-minute bootstrap window', () => {
    // C2 fix: the token must outlive a single bootstrap dial — it
    // authenticates every reconnect for the environment's whole life, so it
    // shares the Env Key's long TTL rather than a short, launch-only one.
    expect(ENVIRONMENT_TOKEN_TTL_MS).toBe(ENV_KEY_TTL_MS);
    expect(ENVIRONMENT_TOKEN_TTL_MS).toBeGreaterThan(15 * 60 * 1000);
  });

  it('resolves a reconnect more than 15 minutes after launch (the old TTL would have expired it)', () => {
    const launchedAt = new Date('2026-06-17T00:00:00.000Z');
    const armed = armEnvironmentToken(undefined, launchedAt);
    const reconnectAttempt = new Date(launchedAt.getTime() + 20 * 60 * 1000); // +20 min
    expect(
      verifyEnvironmentToken(
        armed.raw,
        armed.environmentTokenDigest,
        armed.environmentTokenExpiresAt,
        reconnectAttempt,
      ),
    ).toBe(true);
  });
});

describe('environment token — generateEnvironmentToken', () => {
  it('returns an et- prefixed opaque token', () => {
    const token = generateEnvironmentToken();
    expect(token.startsWith(ENVIRONMENT_TOKEN_PREFIX)).toBe(true);
    expect(token).toMatch(/^et-[A-Za-z0-9_-]{32,}$/);
  });

  it('is unique across calls (randomness, not a constant)', () => {
    const tokens = new Set(Array.from({ length: 100 }, () => generateEnvironmentToken()));
    // No collisions across 100 draws — proves real entropy, not a fixed value.
    expect(tokens.size).toBe(100);
  });

  it('carries at least 32 bytes of url-safe randomness after the prefix', () => {
    const body = generateEnvironmentToken().slice(ENVIRONMENT_TOKEN_PREFIX.length);
    // base64url of 32 bytes is 43 chars; the body must clear that bar so the
    // token carries a full 256-bit secret.
    expect(body.length).toBeGreaterThanOrEqual(43);
    expect(body).toMatch(/^[A-Za-z0-9_-]+$/);
  });
});

describe('environment token — hashEnvironmentToken', () => {
  it('is the sha256 hex digest of the raw token (64 lowercase hex chars)', () => {
    const raw = 'et-example-raw-token';
    const digest = hashEnvironmentToken(raw);
    expect(digest).toMatch(/^[0-9a-f]{64}$/);
    expect(digest).toBe(createHash('sha256').update(raw).digest('hex'));
  });

  it('is deterministic — the same raw token always hashes to the same digest', () => {
    const raw = generateEnvironmentToken();
    expect(hashEnvironmentToken(raw)).toBe(hashEnvironmentToken(raw));
  });

  it('is collision-distinct — different tokens hash to different digests', () => {
    expect(hashEnvironmentToken('et-aaa')).not.toBe(hashEnvironmentToken('et-bbb'));
  });

  it('never returns the raw token (the digest is not the plaintext)', () => {
    const raw = generateEnvironmentToken();
    expect(hashEnvironmentToken(raw)).not.toBe(raw);
  });
});

describe('environment token — verifyEnvironmentToken', () => {
  const now = new Date('2026-06-17T00:00:00.000Z');
  const future = new Date(now.getTime() + 3600_000);
  const past = new Date(now.getTime() - 1);

  it('accepts the right token before expiry', () => {
    const raw = generateEnvironmentToken();
    const digest = hashEnvironmentToken(raw);
    expect(verifyEnvironmentToken(raw, digest, future, now)).toBe(true);
  });

  it('rejects the wrong token even before expiry', () => {
    const digest = hashEnvironmentToken(generateEnvironmentToken());
    expect(verifyEnvironmentToken('et-some-other-token', digest, future, now)).toBe(false);
  });

  it('rejects the right token once expired', () => {
    const raw = generateEnvironmentToken();
    const digest = hashEnvironmentToken(raw);
    // Expiry semantics: once expiresAt < now the token no longer authenticates.
    expect(verifyEnvironmentToken(raw, digest, past, now)).toBe(false);
  });

  it('treats the expiry boundary as still-valid at exactly now', () => {
    const raw = generateEnvironmentToken();
    const digest = hashEnvironmentToken(raw);
    // expires_at == now is not yet "< now", so the token is still live — the
    // off-by-one guard that keeps a token from dying a tick early.
    expect(verifyEnvironmentToken(raw, digest, now, now)).toBe(true);
  });

  it('rejects when no digest is stored (revoked / never armed)', () => {
    const raw = generateEnvironmentToken();
    expect(verifyEnvironmentToken(raw, null, future, now)).toBe(false);
  });

  it('rejects when no expiry is stored, mirroring fail-closed resolution', () => {
    const raw = generateEnvironmentToken();
    const digest = hashEnvironmentToken(raw);
    // Fail closed when no expiry is stored even if the digest matched.
    expect(verifyEnvironmentToken(raw, digest, null, now)).toBe(false);
  });

  it('defaults `now` to the current time when omitted', () => {
    const raw = generateEnvironmentToken();
    const digest = hashEnvironmentToken(raw);
    const farFuture = new Date(Date.now() + 3600_000);
    expect(verifyEnvironmentToken(raw, digest, farFuture)).toBe(true);
    const longGone = new Date(Date.now() - 3600_000);
    expect(verifyEnvironmentToken(raw, digest, longGone)).toBe(false);
  });

  it('does not throw on digests of differing length (constant-time compare guard)', () => {
    const raw = generateEnvironmentToken();
    // A stored digest that is not a 64-hex string must yield false, not an
    // exception from a length-mismatched timing-safe compare.
    expect(verifyEnvironmentToken(raw, 'deadbeef', new Date(Date.now() + 1000))).toBe(false);
  });
});

describe('environment token — armEnvironmentToken', () => {
  it('mints a token, stores only its digest, and sets a future expiry using the default TTL', () => {
    const now = new Date('2026-06-17T00:00:00.000Z');
    const armed = armEnvironmentToken(undefined, now);

    expect(armed.raw.startsWith(ENVIRONMENT_TOKEN_PREFIX)).toBe(true);
    // What gets persisted is the digest of that raw token — never the raw token.
    expect(armed.environmentTokenDigest).toBe(hashEnvironmentToken(armed.raw));
    expect(armed.environmentTokenDigest).not.toBe(armed.raw);
    expect(armed.environmentTokenExpiresAt.getTime()).toBe(
      now.getTime() + ENVIRONMENT_TOKEN_TTL_MS,
    );
  });

  it('honors an explicit ttlMs override, distinct from the default policy TTL', () => {
    const now = new Date('2026-06-17T00:00:00.000Z');
    const armed = armEnvironmentToken(60_000, now);
    expect(armed.environmentTokenExpiresAt.getTime()).toBe(now.getTime() + 60_000);
  });

  it('produces a state that the raw token verifies against before expiry', () => {
    const now = new Date('2026-06-17T00:00:00.000Z');
    const armed = armEnvironmentToken(undefined, now);
    expect(
      verifyEnvironmentToken(
        armed.raw,
        armed.environmentTokenDigest,
        armed.environmentTokenExpiresAt,
        now,
      ),
    ).toBe(true);
  });

  it('rotates to a fresh token whose digest replaces the prior one', () => {
    const now = new Date('2026-06-17T00:00:00.000Z');
    const first = armEnvironmentToken(undefined, now);
    const second = armEnvironmentToken(undefined, now);
    // A new draw each time — the old digest no longer matches the new token,
    // so re-arming atomically revokes the previously issued token.
    expect(second.raw).not.toBe(first.raw);
    expect(second.environmentTokenDigest).not.toBe(first.environmentTokenDigest);
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
    const armed = armEnvironmentToken();
    const after = Date.now();
    expect(armed.environmentTokenExpiresAt.getTime()).toBeGreaterThanOrEqual(
      before + ENVIRONMENT_TOKEN_TTL_MS,
    );
    expect(armed.environmentTokenExpiresAt.getTime()).toBeLessThanOrEqual(
      after + ENVIRONMENT_TOKEN_TTL_MS,
    );
  });
});

describe('environment token — revokeEnvironmentToken', () => {
  it('clears both the digest and the expiry', () => {
    expect(revokeEnvironmentToken()).toEqual({
      environmentTokenDigest: null,
      environmentTokenExpiresAt: null,
    });
  });

  it('produces a state that verifyEnvironmentToken rejects', () => {
    const raw = generateEnvironmentToken();
    const cleared = revokeEnvironmentToken();
    expect(
      verifyEnvironmentToken(
        raw,
        cleared.environmentTokenDigest,
        cleared.environmentTokenExpiresAt,
      ),
    ).toBe(false);
  });
});
