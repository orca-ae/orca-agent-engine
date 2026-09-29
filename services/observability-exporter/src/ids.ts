// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';

function hashParts(domain: string, parts: readonly string[]): Uint8Array {
  const hash = createHash('sha256');
  hash.update('orca-managed-agents-observability');
  hash.update('\0');
  hash.update(domain);
  for (const part of parts) {
    hash.update('\0');
    hash.update(part);
  }
  return hash.digest();
}

function nonZeroHex(bytes: Uint8Array): string {
  if (bytes.some((value) => value !== 0)) return Buffer.from(bytes).toString('hex');
  const copy = Buffer.from(bytes);
  copy[copy.length - 1] = 1;
  return copy.toString('hex');
}

/** Valid, non-zero 128-bit W3C trace ID. Stable across source replay. */
export function deterministicTraceId(
  workspaceId: string,
  sessionId: string,
  anchorEventId: string,
): string {
  return nonZeroHex(hashParts('trace-v1', [workspaceId, sessionId, anchorEventId]).subarray(0, 16));
}

/** Root span seed is exactly trace identity plus its fixed root role. */
export function deterministicRootSpanId(traceId: string): string {
  return nonZeroHex(hashParts('root-span-v1', [traceId, 'root']).subarray(0, 8));
}

/** Child seed includes type, canonical subpath, and source identity. */
export function deterministicChildSpanId(
  traceId: string,
  observationType: string,
  subpath: string,
  sourceEventId: string,
): string {
  return nonZeroHex(
    hashParts('child-span-v1', [traceId, observationType, subpath, sourceEventId]).subarray(0, 8),
  );
}
