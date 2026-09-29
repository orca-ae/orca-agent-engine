// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Model pricing: what a token costs, and which price applies to a model id.
 *
 * This module is pure — no I/O, no clock, no network. Callers supply the price
 * entries (from Postgres, from the seed file below, or both) and get back either
 * a rate or `null`. `null` means **unpriced**, which is deliberately distinct
 * from "costs nothing": a cost value exists only when it was actually computed.
 * Nothing here ever defaults a price to zero.
 *
 * See `docs/managed-agents/pricing.md`.
 */

// Inside `src/` deliberately. Three service images copy this package's `src`
// and nothing else, so a data file beside it would be missing from every one of
// them — and from any image added later. Keeping it here means the seed travels
// wherever the code does.
import seedCatalog from './model-prices.seed.json' with { type: 'json' };

const TOKENS_PER_MILLION = 1_000_000;
const NANO_USD_PER_USD = 1_000_000_000;
export const MAX_PRICE_DECIMAL_PLACES = 3;

/** USD per single token. */
export interface ModelPricing {
  inputPerToken: number;
  outputPerToken: number;
  cacheReadPerToken?: number;
  cacheWritePerToken?: number; // the short-lived (5m) cache-creation rate
}

/** Token counts for one usage delta. `input` must EXCLUDE cached tokens. */
export interface TokenUsage {
  inputTokens?: number;
  outputTokens?: number;
  cacheReadInputTokens?: number;
  cacheCreationEphemeral5mInputTokens?: number;
  cacheCreationEphemeral1hInputTokens?: number;
}

/**
 * Industry-standard ratios, applied to the input rate when a catalog entry
 * omits the corresponding rate. Dropping the bucket entirely would under-report
 * a cache-heavy session; billing it at the full input rate would over-report one
 * by roughly ten times.
 */
export const CACHE_READ_INPUT_RATIO = 0.1;
export const CACHE_WRITE_5M_INPUT_RATIO = 1.25;
export const CACHE_WRITE_1H_INPUT_RATIO = 2.0;
export const CACHE_WRITE_1H_FROM_5M_RATIO = 1.6;

/**
 * Cost in integer nano-USD. Buckets are additive; cached tokens are never
 * subtracted. Converting each effective rate once, before multiplying by token
 * counts, makes addition independent of how a model call is split into deltas.
 *
 * No catalog publishes a long-lived (1h) cache-creation rate. When the catalog
 * publishes a 5m rate, the 1h bucket derives from it; otherwise both cache-write
 * rates derive from the input rate at their standard multiples.
 */
export function computeCostNanoUsd(usage: TokenUsage, pricing: ModelPricing): bigint {
  const inputRate = pricing.inputPerToken;
  const cacheReadRate = pricing.cacheReadPerToken ?? inputRate * CACHE_READ_INPUT_RATIO;
  const cacheWrite5mRate = pricing.cacheWritePerToken ?? inputRate * CACHE_WRITE_5M_INPUT_RATIO;
  const cacheWrite1hRate =
    pricing.cacheWritePerToken === undefined
      ? inputRate * CACHE_WRITE_1H_INPUT_RATIO
      : cacheWrite5mRate * CACHE_WRITE_1H_FROM_5M_RATIO;

  return (
    tokenCount(usage.inputTokens, 'inputTokens') * nanoUsdRate(inputRate, 'input') +
    tokenCount(usage.outputTokens, 'outputTokens') * nanoUsdRate(pricing.outputPerToken, 'output') +
    tokenCount(usage.cacheReadInputTokens, 'cacheReadInputTokens') *
      nanoUsdRate(cacheReadRate, 'cache read') +
    tokenCount(usage.cacheCreationEphemeral5mInputTokens, 'cacheCreationEphemeral5mInputTokens') *
      nanoUsdRate(cacheWrite5mRate, 'cache write 5m') +
    tokenCount(usage.cacheCreationEphemeral1hInputTokens, 'cacheCreationEphemeral1hInputTokens') *
      nanoUsdRate(cacheWrite1hRate, 'cache write 1h')
  );
}

function tokenCount(value: number | undefined, name: string): bigint {
  const count = value ?? 0;
  if (!Number.isSafeInteger(count) || count < 0) {
    throw new RangeError(`${name} must be a non-negative safe integer`);
  }
  return BigInt(count);
}

function nanoUsdRate(perTokenUsd: number, name: string): bigint {
  const scaled = perTokenUsd * NANO_USD_PER_USD;
  const rounded = Math.round(scaled);
  if (!Number.isFinite(scaled) || scaled < 0 || !Number.isSafeInteger(rounded)) {
    throw new RangeError(`${name} price cannot be represented in nano-USD`);
  }
  return BigInt(rounded);
}

export type PriceSource = 'operator' | 'upstream' | 'seed';
export const SEED_PRICE_PROVIDER = 'anthropic';

/** Rates are quoted per million tokens, matching how vendors publish them. */
export interface ModelPriceEntry {
  provider: string;
  modelId: string;
  source: PriceSource;
  /** Required for operator rows; seed and upstream rows are deployment-global. */
  organizationId?: string;
  inputPerMillionTokens: number;
  outputPerMillionTokens: number;
  cacheReadPerMillionTokens?: number;
  cacheWritePerMillionTokens?: number;
}

/**
 * Strip a trailing dated-snapshot suffix (for example `-20251101`), so a pinned
 * snapshot resolves to the same catalog entry as its base alias.
 */
export function stripDatedSnapshotSuffix(modelId: string): string {
  return modelId.replace(/-\d{8}$/, '');
}

function sourceRank(source: PriceSource): number {
  switch (source) {
    case 'operator':
      return 3;
    case 'upstream':
      return 2;
    case 'seed':
      return 1;
    default:
      return 0;
  }
}

/** A published token rate: finite and strictly positive. Zero is not a real price. */
export function hasSupportedPricePrecision(value: number): boolean {
  const scaled = value * 10 ** MAX_PRICE_DECIMAL_PLACES;
  return (
    Number.isSafeInteger(Math.round(scaled)) &&
    Math.abs(scaled - Math.round(scaled)) <= Number.EPSILON * Math.max(1, Math.abs(scaled)) * 4
  );
}

function isPositiveRate(value: number | undefined): value is number {
  return (
    typeof value === 'number' &&
    Number.isFinite(value) &&
    value > 0 &&
    hasSupportedPricePrecision(value)
  );
}

/** A cache rate: finite and non-negative. A published zero is legitimate. */
function isNonNegativeRate(value: number | undefined): value is number {
  return (
    typeof value === 'number' &&
    Number.isFinite(value) &&
    value >= 0 &&
    hasSupportedPricePrecision(value)
  );
}

/**
 * Whether an entry carries a usable base price. Input and output must be finite
 * and strictly positive: a zero, negative, or non-finite rate is not a real
 * price, and letting it through would compute a `0`/`NaN` cost that reads as a
 * real number and silently disables every budget. Such an entry is UNPRICED —
 * the resolver drops it rather than pricing a token at zero.
 *
 * Cache rates are validated separately, in `toPricing`: a corrupt cache rate is
 * dropped and derived from the input rate, exactly as an absent one is, without
 * discarding an otherwise-valid entry.
 */
function isPricableEntry(entry: ModelPriceEntry): boolean {
  return (
    isPositiveRate(entry.inputPerMillionTokens) && isPositiveRate(entry.outputPerMillionTokens)
  );
}

/**
 * Convert an entry to per-token rates. Callers must pass an entry that already
 * satisfies `isPricableEntry`, so the base rates are known good; a cache rate is
 * carried only when it is itself finite and non-negative, and otherwise dropped
 * so `computeCost` derives it rather than multiplying by a `NaN`/negative rate.
 */
function toPricing(entry: ModelPriceEntry): ModelPricing {
  return {
    inputPerToken: entry.inputPerMillionTokens / TOKENS_PER_MILLION,
    outputPerToken: entry.outputPerMillionTokens / TOKENS_PER_MILLION,
    ...(isNonNegativeRate(entry.cacheReadPerMillionTokens)
      ? { cacheReadPerToken: entry.cacheReadPerMillionTokens / TOKENS_PER_MILLION }
      : {}),
    ...(isNonNegativeRate(entry.cacheWritePerMillionTokens)
      ? { cacheWritePerToken: entry.cacheWritePerMillionTokens / TOKENS_PER_MILLION }
      : {}),
  };
}

/** A published rate and an absent one count as a disagreement, not a match. */
function samePricing(a: ModelPricing, b: ModelPricing): boolean {
  return (
    a.inputPerToken === b.inputPerToken &&
    a.outputPerToken === b.outputPerToken &&
    a.cacheReadPerToken === b.cacheReadPerToken &&
    a.cacheWritePerToken === b.cacheWritePerToken
  );
}

/**
 * Collapse the candidates a single match tier produced.
 *
 * The highest-precedence source present wins outright. If several entries tie at
 * that precedence and disagree on any rate, the result is unpriced rather than
 * one of them — guessing would silently mis-bill, refusing surfaces it.
 */
function collapseCandidates(candidates: readonly ModelPriceEntry[]): ModelPricing | null {
  let bestRank = -1;
  for (const candidate of candidates) {
    const rank = sourceRank(candidate.source);
    if (rank > bestRank) bestRank = rank;
  }

  const winners = candidates.filter((candidate) => sourceRank(candidate.source) === bestRank);
  const first = winners[0];
  if (first === undefined) return null;

  const pricing = toPricing(first);
  for (const winner of winners) {
    if (!samePricing(pricing, toPricing(winner))) return null;
  }
  return pricing;
}

/**
 * Resolve a provider/model identity against `entries`. Returns `null` when unpriced.
 *
 * Entries that are not pricable — a non-positive or non-finite input or output
 * rate — are dropped first, so a corrupt rate resolves UNPRICED rather than to a
 * `0`/`NaN` price. Operator rows are first narrowed to the resolving
 * organization. Match specificity is then the outer loop — exact id, a trailing
 * dated-snapshot suffix stripped, then the final hyphen-delimited segment
 * stripped for a guarded family match. Source precedence (operator > upstream >
 * seed) settles ties within whichever tier matched first. A tier that matches is
 * the answer: a same-precedence disagreement yields `null` rather than guessing.
 */
export function resolveModelPricing(
  provider: string,
  modelId: string,
  entries: readonly ModelPriceEntry[],
  organizationId?: string,
): ModelPricing | null {
  const pricable = entries.filter(
    (entry) =>
      entry.provider === provider &&
      isPricableEntry(entry) &&
      (entry.source !== 'operator' ||
        (organizationId !== undefined && entry.organizationId === organizationId)),
  );
  const base = stripDatedSnapshotSuffix(modelId);

  const exact = pricable.filter((entry) => entry.modelId === modelId);
  if (exact.length > 0) return collapseCandidates(exact);

  if (base !== modelId) {
    const snapshot = pricable.filter((entry) => entry.modelId === base);
    if (snapshot.length > 0) return collapseCandidates(snapshot);
  }

  const separator = base.lastIndexOf('-');
  if (separator > 0) {
    const family = base.slice(0, separator);
    const familyMatches = pricable.filter((entry) => {
      const candidate = stripDatedSnapshotSuffix(entry.modelId);
      const candidateSeparator = candidate.lastIndexOf('-');
      return candidateSeparator > 0 && candidate.slice(0, candidateSeparator) === family;
    });
    if (familyMatches.length > 0) return collapseCandidates(familyMatches);
  }

  return null;
}

/**
 * The shape of one entry in `model-prices.seed.json`. Read through an explicit
 * view rather than the inferred literal type so that omitting a cache rate from
 * a future entry stays a data edit, not a compile break.
 */
export interface SeedRates {
  input_per_million_tokens: number;
  output_per_million_tokens: number;
  cache_read_per_million_tokens?: number;
  cache_write_per_million_tokens?: number;
}

const seedModels = seedCatalog.models as unknown as Record<string, SeedRates>;
export const SEED_GENERATED_AT = seedCatalog.generated_at;

/**
 * The seed entries, loaded from `model-prices.seed.json`. A row with a
 * non-positive or non-finite input or output rate is dropped rather than seeded
 * as a `0`/`NaN` price — an unpriced model is supported; a wrong price is not.
 */
/** One seed row as a `ModelPriceEntry`; a cache rate is carried only when present. */
export function toSeedEntry(
  modelId: string,
  rates: SeedRates,
  provider = SEED_PRICE_PROVIDER,
): ModelPriceEntry {
  return {
    provider,
    modelId,
    source: 'seed',
    inputPerMillionTokens: rates.input_per_million_tokens,
    outputPerMillionTokens: rates.output_per_million_tokens,
    ...(rates.cache_read_per_million_tokens !== undefined
      ? { cacheReadPerMillionTokens: rates.cache_read_per_million_tokens }
      : {}),
    ...(rates.cache_write_per_million_tokens !== undefined
      ? { cacheWritePerMillionTokens: rates.cache_write_per_million_tokens }
      : {}),
  };
}

export const SEED_MODEL_PRICES: readonly ModelPriceEntry[] = Object.freeze(
  Object.entries(seedModels)
    .map(([modelId, rates]) => toSeedEntry(modelId, rates))
    .filter(isPricableEntry),
);
