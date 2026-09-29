// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';

export const TRACE_SAMPLING_VERSION = 'orca.observability.trace-sampling.v1' as const;
const HASH_SPACE = 1n << 64n;

export interface TraceSamplingPolicy {
  algorithmVersion: typeof TRACE_SAMPLING_VERSION;
  bindingId: string;
  bindingVersion: number;
  sampleRate: number;
}

export function isValidSampleRate(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1;
}

/** Reconstruct only the immutable, non-secret sampling policy. */
export function parseTraceSamplingPolicy(value: unknown): TraceSamplingPolicy {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('invalid trace sampling policy');
  }
  const policy = value as Record<string, unknown>;
  if (
    policy.algorithmVersion !== TRACE_SAMPLING_VERSION ||
    typeof policy.bindingId !== 'string' ||
    policy.bindingId.length === 0 ||
    policy.bindingId.length > 512 ||
    [...policy.bindingId].some(
      (character) => character.charCodeAt(0) <= 32 || character.charCodeAt(0) === 127,
    ) ||
    typeof policy.bindingVersion !== 'number' ||
    !Number.isSafeInteger(policy.bindingVersion) ||
    policy.bindingVersion <= 0 ||
    !isValidSampleRate(policy.sampleRate)
  ) {
    throw new Error('invalid trace sampling policy');
  }
  return {
    algorithmVersion: TRACE_SAMPLING_VERSION,
    bindingId: policy.bindingId,
    bindingVersion: policy.bindingVersion,
    sampleRate: policy.sampleRate,
  };
}

/**
 * Integer exclusive cutoff equivalent to h < rate * 2^64 for the exact
 * binary64 rate received from Registry. Never round a 64-bit hash to Number.
 * Ceil preserves the strict comparison even for subnormal positive rates.
 */
export function traceSamplingThreshold(rate: number): bigint {
  if (!isValidSampleRate(rate)) throw new Error('invalid trace sample rate');
  if (rate === 0) return 0n;
  if (rate === 1) return HASH_SPACE;
  const bytes = Buffer.alloc(8);
  bytes.writeDoubleBE(rate);
  const bits = bytes.readBigUInt64BE();
  const exponent = Number((bits >> 52n) & 0x7ffn);
  const fraction = bits & ((1n << 52n) - 1n);
  const significand = exponent === 0 ? fraction : fraction | (1n << 52n);
  const shift = (exponent === 0 ? -1022 : exponent - 1023) - 52 + 64;
  if (shift >= 0) return significand << BigInt(shift);
  const denominator = 1n << BigInt(-shift);
  return (significand + denominator - 1n) / denominator;
}

/** SHA-256 UTF-8 domain + NUL + unambiguous JSON tuple; first 64 bits, big-endian. */
export function isTraceSampled(policy: TraceSamplingPolicy, traceId: string): boolean {
  const pinned = parseTraceSamplingPolicy(policy);
  if (!/^[a-f0-9]{32}$/u.test(traceId)) throw new Error('invalid sampling trace identity');
  const hash = createHash('sha256')
    .update(TRACE_SAMPLING_VERSION)
    .update('\0')
    .update(JSON.stringify([pinned.bindingId, pinned.bindingVersion, traceId]))
    .digest();
  return hash.readBigUInt64BE() < traceSamplingThreshold(pinned.sampleRate);
}
