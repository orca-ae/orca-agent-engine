// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  isTraceSampled,
  parseTraceSamplingPolicy,
  TRACE_SAMPLING_VERSION,
  traceSamplingThreshold,
  type TraceSamplingPolicy,
} from '../../src/sampling.js';

const traceId = '0123456789abcdef0123456789abcdef';
const policy: TraceSamplingPolicy = {
  algorithmVersion: TRACE_SAMPLING_VERSION,
  bindingId: 'aob_test',
  bindingVersion: 1,
  sampleRate: 0.5,
};

describe('versioned deterministic trace sampling', () => {
  it.each([0, -0, 1])('implements endpoint rate %s without dropping hash bits', (rate) => {
    expect(traceSamplingThreshold(rate)).toBe(rate === 0 ? 0n : 1n << 64n);
    expect(isTraceSampled({ ...policy, sampleRate: rate }, traceId)).toBe(rate === 1);
  });

  it('compares first64 strictly against the exact fractional cutoff', () => {
    const cutoff = traceSamplingThreshold(0.5);
    expect(cutoff).toBe(0x8000000000000000n);
    expect(0x7fffffffffffffffn < cutoff).toBe(true);
    expect(0x8000000000000000n < cutoff).toBe(false);
    expect(traceSamplingThreshold(0.1)).toBe(1844674407370955264n);
    expect(traceSamplingThreshold(2 ** -64)).toBe(1n);
    expect(traceSamplingThreshold(2 ** -65)).toBe(1n);
    expect(traceSamplingThreshold(Number.MIN_VALUE)).toBe(1n);
    expect(traceSamplingThreshold(1 - 2 ** -53)).toBe((1n << 64n) - 2048n);
  });

  it('pins an independently computed SHA-256/domain/JSON tuple/big-endian boundary vector', () => {
    // SHA256(domain + NUL + ["aob_test",1,traceId]) starts c22709287e049800.
    const boundary = 0.7584081386430789;
    expect(traceSamplingThreshold(boundary)).toBe(0xc22709287e049800n);
    expect(isTraceSampled({ ...policy, sampleRate: boundary }, traceId)).toBe(false);
    expect(isTraceSampled({ ...policy, sampleRate: boundary + 2 ** -53 }, traceId)).toBe(true);
    expect(isTraceSampled({ ...policy, sampleRate: boundary - 2 ** -53 }, traceId)).toBe(false);
  });

  it('does not round a near-boundary 64-bit hash into a false exclusion', () => {
    // Version 2 hashes to f1ace080b201e755, 171 below the exact binary64 cutoff.
    const boundary = 0.9440441431233989;
    expect(traceSamplingThreshold(boundary)).toBe(0xf1ace080b201e800n);
    expect(isTraceSampled({ ...policy, bindingVersion: 2, sampleRate: boundary }, traceId)).toBe(
      true,
    );
    expect(
      isTraceSampled({ ...policy, bindingVersion: 2, sampleRate: boundary - 2 ** -53 }, traceId),
    ).toBe(false);
  });

  it('is stable on replay and separates binding, config version, and trace identities', () => {
    const pinned = { ...policy, sampleRate: 0.85 };
    expect(isTraceSampled(pinned, traceId)).toBe(true);
    expect(isTraceSampled(JSON.parse(JSON.stringify(pinned)) as TraceSamplingPolicy, traceId)).toBe(
      true,
    );
    expect(isTraceSampled({ ...pinned, bindingVersion: 2 }, traceId)).toBe(false);
    expect(isTraceSampled(policy, traceId)).toBe(false);
    expect(isTraceSampled({ ...policy, bindingId: 'aob_other' }, traceId)).toBe(true);
    expect(isTraceSampled({ ...policy, sampleRate: 0.6 }, 'fedcba9876543210fedcba9876543210')).toBe(
      true,
    );
  });

  it.each([NaN, Infinity, -Infinity, -0.1, 1.1])('rejects invalid rate %s', (sampleRate) => {
    expect(() => traceSamplingThreshold(sampleRate)).toThrow('invalid trace sample rate');
    expect(() => isTraceSampled({ ...policy, sampleRate }, traceId)).toThrow(
      'invalid trace sampling policy',
    );
  });

  it('rejects unsupported algorithm versions and malformed policies without retaining data', () => {
    for (const value of [
      null,
      {},
      { ...policy, algorithmVersion: 'v2' },
      { ...policy, bindingVersion: 0 },
      { ...policy, bindingVersion: 1.5 },
      { ...policy, bindingId: 'aob_bad\0id' },
      { ...policy, sampleRate: '0.5' },
    ]) {
      expect(() => parseTraceSamplingPolicy(value)).toThrow('invalid trace sampling policy');
    }
    expect(parseTraceSamplingPolicy({ ...policy, content: 'not retained' })).toEqual(policy);
  });
});
