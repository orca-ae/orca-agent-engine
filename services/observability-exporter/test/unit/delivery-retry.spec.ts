// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  deliveryRetryDelayMs,
  MAX_DELIVERY_RETRY_DELAY_MS,
  parseRetryAfterMs,
} from '../../src/delivery-retry.js';

describe('delivery retry scheduling', () => {
  it('backs off from durable attempt count and applies bounded jitter', () => {
    expect(deliveryRetryDelayMs(0, undefined, () => 0)).toBe(1_000);
    expect(deliveryRetryDelayMs(1, undefined, () => 0)).toBe(2_000);
    expect(deliveryRetryDelayMs(2, undefined, () => 0.5)).toBe(4_500);
    expect(deliveryRetryDelayMs(60, undefined, () => 0.5)).toBe(262_500);
  });

  it('parses delta-seconds and HTTP-date and rejects invalid Retry-After', () => {
    const now = Date.parse('2026-09-05T12:00:00.000Z');
    expect(parseRetryAfterMs('12', now)).toBe(12_000);
    expect(parseRetryAfterMs(new Date(now + 45_000).toUTCString(), now)).toBe(45_000);
    expect(parseRetryAfterMs('not-a-date', now)).toBeUndefined();
    expect(parseRetryAfterMs('1.5', now)).toBeUndefined();
    expect(parseRetryAfterMs('9'.repeat(200), now)).toBeUndefined();
    expect(parseRetryAfterMs('999999999999', now)).toBe(MAX_DELIVERY_RETRY_DELAY_MS);
  });

  it('accepts all three HTTP-date wire formats', () => {
    const now = Date.parse('1994-11-06T08:49:00.000Z');
    for (const value of [
      'Sun, 06 Nov 1994 08:49:37 GMT',
      'Sunday, 06-Nov-94 08:49:37 GMT',
      'Sun Nov  6 08:49:37 1994',
    ]) {
      expect(parseRetryAfterMs(value, now)).toBe(37_000);
    }
  });

  it('applies the rolling 50-year rule to RFC 850 dates', () => {
    const now = Date.parse('2026-09-05T12:00:00.000Z');
    expect(parseRetryAfterMs('Wednesday, 06-Nov-75 08:49:37 GMT', now)).toBe(
      MAX_DELIVERY_RETRY_DELAY_MS,
    );
    expect(parseRetryAfterMs('Friday, 04-Sep-76 12:00:00 GMT', now)).toBe(
      MAX_DELIVERY_RETRY_DELAY_MS,
    );
    expect(parseRetryAfterMs('Saturday, 06-Nov-76 08:49:37 GMT', now)).toBe(0);
    expect(parseRetryAfterMs('Sunday, 06-Nov-94 08:49:37 GMT', now)).toBe(0);
  });

  it('rejects normalized or internally inconsistent HTTP dates', () => {
    const now = Date.parse('2026-09-05T12:00:00.000Z');
    expect(parseRetryAfterMs('Sun, 31 Feb 2027 08:49:37 GMT', now)).toBeUndefined();
    expect(parseRetryAfterMs('Sunday, 31-Feb-27 08:49:37 GMT', now)).toBeUndefined();
    expect(parseRetryAfterMs('Sun Feb 31 08:49:37 2027', now)).toBeUndefined();
    expect(parseRetryAfterMs('Mon, 06 Nov 1994 08:49:37 GMT', now)).toBeUndefined();
  });

  it('treats Retry-After as a bounded minimum without allowing a zero-delay retry', () => {
    expect(deliveryRetryDelayMs(0, 30_000, () => 0)).toBe(30_000);
    expect(deliveryRetryDelayMs(0, 0, () => 0)).toBe(1_000);
    expect(deliveryRetryDelayMs(0, MAX_DELIVERY_RETRY_DELAY_MS, () => 0)).toBe(
      MAX_DELIVERY_RETRY_DELAY_MS,
    );
  });
});
