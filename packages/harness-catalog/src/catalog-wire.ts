// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Parse and validate an upstream model-price catalog payload.
 *
 * The payload arrives over the network from a configured refresh URL, so it is
 * untrusted: this module validates rather than trusts, returns a discriminated
 * result rather than throwing, and drops anything it cannot read.
 *
 * Dropping matters more than it looks. A model whose rates are missing or
 * unreadable must come back **unpriced**, never priced at zero — a zero rate
 * would let a cost guardrail silently pass a session it never measured.
 */

import { hasSupportedPricePrecision, type ModelPriceEntry } from './pricing.js';

/** Major version this parser understands. Anything else is rejected wholesale. */
const SUPPORTED_SCHEMA_MAJOR = 1;
export const MAX_CATALOG_MODEL_ID_LENGTH = 200;
export const DEFAULT_MAX_UPSTREAM_PRICE_DELTA_FACTOR = 10;
const ADDRESSABLE_MODEL_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/;

export interface ModelPriceCatalogParseOptions {
  /** Provider whose configured feed produced this catalog. */
  provider: string;
  /** Persisted upstream rows plus seed fallback rows visible to the refresher. */
  baseline?: readonly ModelPriceEntry[];
  /** Largest accepted multiplicative change in either direction. */
  maxPriceDeltaFactor?: number;
}

export interface CatalogParseSuccess {
  ok: true;
  entries: ModelPriceEntry[];
  /**
   * How many models in the payload were dropped as unreadable or unpriced. A
   * non-zero count on a successful parse is a signal worth logging, not a
   * failure — but a parse where *every* model is skipped fails instead (see
   * `parseModelPriceCatalog`), so a bad upstream never silently blanks prices.
   */
  skipped: number;
}

export interface CatalogParseFailure {
  ok: false;
  error: string;
}

export type CatalogParseResult = CatalogParseSuccess | CatalogParseFailure;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Major version of `schema_version`. Accepts a bare number (`1`) or a dotted
 * string (`"1.0"`); returns `null` for anything it cannot read.
 */
function schemaMajor(raw: unknown): number | null {
  if (typeof raw === 'number') {
    return Number.isFinite(raw) ? Math.trunc(raw) : null;
  }
  if (typeof raw === 'string') {
    const major = raw.split('.')[0];
    return major !== undefined && /^\d+$/.test(major) ? Number.parseInt(major, 10) : null;
  }
  return null;
}

/**
 * A usable published rate: a finite, strictly positive number. Zero is not a
 * real token price — it computes a `$0` cost that reads as measured and silently
 * disables the budget — so a `0`, negative, or non-finite rate is `null`.
 */
function positiveRate(raw: unknown): number | null {
  return typeof raw === 'number' &&
    Number.isFinite(raw) &&
    raw > 0 &&
    hasSupportedPricePrecision(raw)
    ? raw
    : null;
}

/**
 * A usable cache rate: a finite, non-negative number. A published zero cache
 * rate is legitimate (unlike input/output), so only a negative or non-finite
 * value is `null`.
 */
function cacheRate(raw: unknown): number | null {
  return typeof raw === 'number' &&
    Number.isFinite(raw) &&
    raw >= 0 &&
    hasSupportedPricePrecision(raw)
    ? raw
    : null;
}

function validModelId(modelId: string): boolean {
  return modelId.length <= MAX_CATALOG_MODEL_ID_LENGTH && ADDRESSABLE_MODEL_ID.test(modelId);
}

function baselineFor(
  provider: string,
  modelId: string,
  entries: readonly ModelPriceEntry[],
): ModelPriceEntry | undefined {
  const matching = entries.filter(
    (entry) => entry.provider === provider && entry.modelId === modelId,
  );
  return (
    matching.find((entry) => entry.source === 'upstream') ??
    matching.find((entry) => entry.source === 'seed')
  );
}

function rateWithinDelta(value: number | undefined, baseline: number | undefined, factor: number) {
  if (value === undefined || baseline === undefined) return true;
  if (value === 0 || baseline === 0) return value === baseline;
  const ratio = value / baseline;
  return ratio >= 1 / factor && ratio <= factor;
}

function priceWithinDelta(
  entry: ModelPriceEntry,
  baseline: ModelPriceEntry | undefined,
  factor: number,
): boolean {
  if (baseline === undefined) return true;
  return (
    rateWithinDelta(entry.inputPerMillionTokens, baseline.inputPerMillionTokens, factor) &&
    rateWithinDelta(entry.outputPerMillionTokens, baseline.outputPerMillionTokens, factor) &&
    rateWithinDelta(entry.cacheReadPerMillionTokens, baseline.cacheReadPerMillionTokens, factor) &&
    rateWithinDelta(entry.cacheWritePerMillionTokens, baseline.cacheWritePerMillionTokens, factor)
  );
}

/**
 * Parse an upstream catalog payload into entries at `upstream` precedence.
 *
 * Never throws. Rejects the whole payload when it is structurally unusable
 * (wrong major version, not an object, no models); skips individual models that
 * carry no readable, positive input and output rate. A parse where every model
 * is skipped is itself a failure — a non-empty payload that yields nothing would
 * otherwise replace the entire price set with silence, reading as "all free" —
 * so it is rejected like the empty-models case, and the count of dropped models
 * rides along on a successful parse as `skipped`.
 */
export function parseModelPriceCatalog(
  payload: unknown,
  options: ModelPriceCatalogParseOptions,
): CatalogParseResult {
  try {
    const { provider, baseline = [] } = options;
    const maxPriceDeltaFactor =
      options.maxPriceDeltaFactor ?? DEFAULT_MAX_UPSTREAM_PRICE_DELTA_FACTOR;
    if (!validModelId(provider)) {
      return { ok: false, error: 'catalog provider is missing or unreadable' };
    }
    if (!Number.isFinite(maxPriceDeltaFactor) || maxPriceDeltaFactor < 1) {
      return {
        ok: false,
        error: 'maxPriceDeltaFactor must be a finite number greater than or equal to 1',
      };
    }
    if (!isPlainObject(payload)) {
      return { ok: false, error: 'catalog payload must be a JSON object' };
    }

    const major = schemaMajor(payload['schema_version']);
    if (major === null) {
      return { ok: false, error: 'catalog payload has a missing or unreadable schema_version' };
    }
    if (major !== SUPPORTED_SCHEMA_MAJOR) {
      return {
        ok: false,
        error: `unsupported catalog schema_version major ${major}; expected ${SUPPORTED_SCHEMA_MAJOR}`,
      };
    }

    const models = payload['models'];
    if (!isPlainObject(models)) {
      return { ok: false, error: 'catalog payload is missing a models object' };
    }

    const modelEntries = Object.entries(models);
    if (modelEntries.length === 0) {
      return { ok: false, error: 'catalog payload has an empty models object' };
    }

    const entries: ModelPriceEntry[] = [];
    let skipped = 0;
    for (const [modelId, model] of modelEntries) {
      if (!validModelId(modelId) || !isPlainObject(model)) {
        skipped++;
        continue;
      }

      const pricing = model['pricing'];
      if (!isPlainObject(pricing)) {
        skipped++;
        continue; // unpriced, not zero
      }

      const input = positiveRate(pricing['input_per_million_tokens']);
      const output = positiveRate(pricing['output_per_million_tokens']);
      if (input === null || output === null) {
        skipped++;
        continue; // unpriced, not zero
      }

      const cacheRead = cacheRate(pricing['cache_read_per_million_tokens']);
      const cacheWrite = cacheRate(pricing['cache_write_per_million_tokens']);
      if (
        (Object.hasOwn(pricing, 'cache_read_per_million_tokens') && cacheRead === null) ||
        (Object.hasOwn(pricing, 'cache_write_per_million_tokens') && cacheWrite === null)
      ) {
        skipped++;
        continue;
      }

      const entry: ModelPriceEntry = {
        provider,
        modelId,
        source: 'upstream',
        inputPerMillionTokens: input,
        outputPerMillionTokens: output,
        ...(cacheRead !== null ? { cacheReadPerMillionTokens: cacheRead } : {}),
        ...(cacheWrite !== null ? { cacheWritePerMillionTokens: cacheWrite } : {}),
      };
      if (!priceWithinDelta(entry, baselineFor(provider, modelId, baseline), maxPriceDeltaFactor)) {
        skipped++;
        continue;
      }
      entries.push(entry);
    }

    // A non-empty payload that parsed to nothing is a failure, not an empty
    // success: replacing the whole price set with zero entries reads as "every
    // model is free", the same silent-$0 hazard the empty-models check guards.
    if (entries.length === 0) {
      return {
        ok: false,
        error: `catalog payload has ${modelEntries.length} model(s) but none carried a usable price; all ${skipped} were skipped as unpriced`,
      };
    }

    return { ok: true, entries, skipped };
  } catch (cause) {
    const detail = cause instanceof Error ? cause.message : String(cause);
    return { ok: false, error: `catalog payload could not be read: ${detail}` };
  }
}
