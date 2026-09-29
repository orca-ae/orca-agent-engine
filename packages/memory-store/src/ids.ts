// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { customAlphabet } from 'nanoid';

/**
 * Library-internal id generator. Callers (the registry, harness) don't
 * supply ids — `LocalMemoryStore` mints them via this helper.
 *
 * Alphabet matches NanoID's URL-safe lowercase + digit set so the resulting
 * ids are safe in URLs, S3 keys, and shell args without escaping. 16 chars
 * gives ~10^25 combinations — plenty for the per-store id space.
 */
const nano = customAlphabet('abcdefghijklmnopqrstuvwxyz0123456789', 16);

/** Memory store ids: `mems_…`, memory ids: `mem_…`, version ids: `memver_…`. */
export type MemoryIdPrefix = 'mems' | 'mem' | 'memver';

export function newId(prefix: MemoryIdPrefix): string {
  return `${prefix}_${nano()}`;
}
