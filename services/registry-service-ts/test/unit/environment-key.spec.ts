// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { createHash } from 'node:crypto';
import {
  generateEnvKey,
  hashEnvKey,
  verifyEnvKey,
  revokeEnvKey,
  armEnvKey,
  ENV_KEY_PREFIX,
  ENV_KEY_TTL_MS,
} from '../../src/domain/environment-key.js';

describe('environment key — generateEnvKey', () => {
  it('returns an sk- prefixed opaque key', () => {
    const key = generateEnvKey();
    expect(key.startsWith(ENV_KEY_PREFIX)).toBe(true);
    expect(key).toMatch(/^sk-[A-Za-z0-9_-]{32,}$/);
  });

  it('is unique across calls (randomness, not a constant)', () => {
    const keys = new Set(Array.from({ length: 100 }, () => generateEnvKey()));
    // No collisions across 100 draws — proves real entropy, not a fixed value.
    expect(keys.size).toBe(100);
  });

  it('carries at least 32 bytes of url-safe randomness after the prefix', () => {
    const body = generateEnvKey().slice(ENV_KEY_PREFIX.length);
    // base64url of 32 bytes is 43 chars; the body must clear that bar so the
    // key carries a full 256-bit secret.
    expect(body.length).toBeGreaterThanOrEqual(43);
    expect(body).toMatch(/^[A-Za-z0-9_-]+$/);
  });
});

describe('environment key — hashEnvKey', () => {
  it('is the sha256 hex digest of the raw key (64 lowercase hex chars)', () => {
    const raw = 'sk-example-raw-key';
    const digest = hashEnvKey(raw);
    expect(digest).toMatch(/^[0-9a-f]{64}$/);
    // The digest is the sha256 hex of the raw key's UTF-8 bytes.
    expect(digest).toBe(createHash('sha256').update(raw).digest('hex'));
  });

  it('is deterministic — the same raw key always hashes to the same digest', () => {
    const raw = generateEnvKey();
    expect(hashEnvKey(raw)).toBe(hashEnvKey(raw));
  });

  it('is collision-distinct — different keys hash to different digests', () => {
    expect(hashEnvKey('sk-aaa')).not.toBe(hashEnvKey('sk-bbb'));
  });

  it('never returns the raw key (the digest is not the plaintext)', () => {
    const raw = generateEnvKey();
    expect(hashEnvKey(raw)).not.toBe(raw);
  });
});

describe('environment key — verifyEnvKey', () => {
  const now = new Date('2026-06-17T00:00:00.000Z');
  const future = new Date(now.getTime() + 3600_000);
  const past = new Date(now.getTime() - 1);

  it('accepts the right key before expiry', () => {
    const raw = generateEnvKey();
    const digest = hashEnvKey(raw);
    expect(verifyEnvKey(raw, digest, future, now)).toBe(true);
  });

  it('rejects the wrong key even before expiry', () => {
    const digest = hashEnvKey(generateEnvKey());
    expect(verifyEnvKey('sk-some-other-key', digest, future, now)).toBe(false);
  });

  it('rejects the right key once expired', () => {
    const raw = generateEnvKey();
    const digest = hashEnvKey(raw);
    // Expiry semantics: once expiresAt < now the key no longer authenticates.
    expect(verifyEnvKey(raw, digest, past, now)).toBe(false);
  });

  it('treats the expiry boundary as still-valid at exactly now', () => {
    const raw = generateEnvKey();
    const digest = hashEnvKey(raw);
    // expires_at == now is not yet "< now", so the key is still live — the
    // off-by-one guard that keeps a key from dying a tick early.
    expect(verifyEnvKey(raw, digest, now, now)).toBe(true);
  });

  it('rejects when no digest is stored (revoked / never armed)', () => {
    const raw = generateEnvKey();
    expect(verifyEnvKey(raw, null, future, now)).toBe(false);
  });

  it('rejects when no expiry is stored, mirroring fail-closed resolution', () => {
    const raw = generateEnvKey();
    const digest = hashEnvKey(raw);
    // Fail closed when no expiry is stored even if the digest matched: a
    // stored digest without an expiry must not authenticate.
    expect(verifyEnvKey(raw, digest, null, now)).toBe(false);
  });

  it('defaults `now` to the current time when omitted', () => {
    const raw = generateEnvKey();
    const digest = hashEnvKey(raw);
    const farFuture = new Date(Date.now() + 3600_000);
    expect(verifyEnvKey(raw, digest, farFuture)).toBe(true);
    const longGone = new Date(Date.now() - 3600_000);
    expect(verifyEnvKey(raw, digest, longGone)).toBe(false);
  });

  it('does not throw on digests of differing length (constant-time compare guard)', () => {
    const raw = generateEnvKey();
    // A stored digest that is not a 64-hex string must yield false, not an
    // exception from a length-mismatched timing-safe compare.
    expect(verifyEnvKey(raw, 'deadbeef', new Date(Date.now() + 1000))).toBe(false);
  });
});

describe('environment key — armEnvKey', () => {
  it('mints a key, stores only its digest, and sets a future expiry', () => {
    const now = new Date('2026-06-17T00:00:00.000Z');
    const armed = armEnvKey(now);

    // The raw key is the sk- credential the caller returns once.
    expect(armed.raw.startsWith(ENV_KEY_PREFIX)).toBe(true);
    // What gets persisted is the digest of that raw key — never the raw key.
    expect(armed.envKeyDigest).toBe(hashEnvKey(armed.raw));
    expect(armed.envKeyDigest).not.toBe(armed.raw);
    // Expiry is now + the policy TTL.
    expect(armed.envKeyExpiresAt.getTime()).toBe(now.getTime() + ENV_KEY_TTL_MS);
  });

  it('produces a state that the raw key verifies against before expiry', () => {
    const now = new Date('2026-06-17T00:00:00.000Z');
    const armed = armEnvKey(now);
    expect(verifyEnvKey(armed.raw, armed.envKeyDigest, armed.envKeyExpiresAt, now)).toBe(true);
  });

  it('rotates to a fresh key whose digest replaces the prior one', () => {
    const now = new Date('2026-06-17T00:00:00.000Z');
    const first = armEnvKey(now);
    const second = armEnvKey(now);
    // A new draw each time — the old digest no longer matches the new key,
    // so re-arming atomically revokes the previously issued key.
    expect(second.raw).not.toBe(first.raw);
    expect(second.envKeyDigest).not.toBe(first.envKeyDigest);
    expect(verifyEnvKey(first.raw, second.envKeyDigest, second.envKeyExpiresAt, now)).toBe(false);
  });

  it('defaults `now` to the current time when omitted', () => {
    const before = Date.now();
    const armed = armEnvKey();
    const after = Date.now();
    expect(armed.envKeyExpiresAt.getTime()).toBeGreaterThanOrEqual(before + ENV_KEY_TTL_MS);
    expect(armed.envKeyExpiresAt.getTime()).toBeLessThanOrEqual(after + ENV_KEY_TTL_MS);
  });
});

describe('environment key — revokeEnvKey', () => {
  it('clears both the digest and the expiry', () => {
    expect(revokeEnvKey()).toEqual({ envKeyDigest: null, envKeyExpiresAt: null });
  });

  it('produces a state that verifyEnvKey rejects', () => {
    const raw = generateEnvKey();
    const cleared = revokeEnvKey();
    expect(verifyEnvKey(raw, cleared.envKeyDigest, cleared.envKeyExpiresAt)).toBe(false);
  });
});
