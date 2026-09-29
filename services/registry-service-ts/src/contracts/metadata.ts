// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { z } from 'zod';

export const MAX_METADATA_PAIRS = 16;
export const MAX_METADATA_KEY_LENGTH = 64;
export const MAX_METADATA_VALUE_LENGTH = 512;

const metadataKey = z.string().min(1).max(MAX_METADATA_KEY_LENGTH);
const metadataValue = z.string().max(MAX_METADATA_VALUE_LENGTH);

function pairCountWithinLimit(value: Record<string, unknown>): boolean {
  return Object.keys(value).length <= MAX_METADATA_PAIRS;
}

const metadataRecord = z.record(metadataKey, metadataValue).refine(pairCountWithinLimit, {
  message: `metadata must contain at most ${MAX_METADATA_PAIRS} pairs`,
});

/**
 * `metadata` in a **response**.
 *
 * Every mapper that emits one runs the stored value through
 * `normalizeStoredMetadata`, which always returns an object — so the key is
 * never absent from a body, and the schema says so. Anthropic marks it required
 * too. Split from {@link Metadata} because a `.default()` renders as *optional*
 * in the generated OpenAPI, which is truthful about a request and a lie about a
 * response.
 */
export const MetadataOutput = metadataRecord;

/**
 * `metadata` in a **request**: absent means `{}`.
 *
 * Do not use in a response position — see {@link MetadataOutput}.
 */
export const Metadata = metadataRecord.default({});

/**
 * A patch clears a key by setting it to null.
 *
 * `.nullable()` on the value, not a `z.null()` branch inside a union — the same
 * distinction `model-wire.ts` documents, and for a sharper reason here. The
 * renderer spells `z.null()` as `{ type: string, format: null, nullable: true }`,
 * which accepts *any* string, so the union published two branches that both
 * matched every ordinary value: `"abc"` satisfied neither (a `oneOf` demands
 * exactly one) while a 600-character value satisfied the unbounded branch and
 * escaped `maxLength` entirely. Identical to zod, correct on the wire.
 */
export const MetadataPatch = z.record(metadataKey, metadataValue.nullable());
