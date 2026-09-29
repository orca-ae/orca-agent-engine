// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import {
  CACHE_READ_INPUT_RATIO,
  CACHE_WRITE_5M_INPUT_RATIO,
  SEED_PRICE_PROVIDER,
  hasSupportedPricePrecision,
  type ModelPricing,
  type PriceSource,
} from '@orca/harness-catalog';
import { ClaudeErrorResponse, isoTimestamp, pagination } from './common.js';
import type { ModelPriceWrite, StoredModelPrice } from '../pricing/store.js';

const c = initContract();

/**
 * Model prices are their own API group rather than part of the policy group:
 * they evolve on a vendor-repricing cadence rather than a policy-design one,
 * and they are useful for cost reporting on a deployment that runs no
 * guardrails at all.
 *
 * See `docs/managed-agents/api-groups-and-extensions.md` for the rule that
 * groups split by who owns an API's evolution.
 */
export const PRICING_API_PREFIX = '/apis/pricing.runorca.ai/v1';

const TOKENS_PER_MILLION = 1_000_000;

/**
 * Enough for any published model identifier and short enough that an operator
 * typo cannot become a row nobody notices.
 */
export const MODEL_PRICE_ID_MAX_LENGTH = 200;

/**
 * The punctuation vendors actually use in a model identifier, and nothing that
 * would stop the id being addressed as a path segment. An entry the API cannot
 * name is an entry no operator can update or delete, so it is refused at the
 * door rather than stored and stranded.
 */
const MODEL_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/;

export const ModelPriceId = z
  .string()
  .min(1)
  .max(MODEL_PRICE_ID_MAX_LENGTH)
  .regex(MODEL_ID_PATTERN, 'model_id must be an addressable model identifier');

/**
 * Which vendor serves the model. Part of the identity, not an attribute: the
 * same id served through two providers can carry different rates, so
 * `(provider, model_id)` is what resolves to a price.
 *
 * Defaulted rather than required. A deployment pricing only Anthropic models —
 * every seed row, and every entry written before this field existed — never has
 * to name it, and the default is the identity those rows already had.
 */
export const ModelProviderId = z
  .string()
  .min(1)
  .max(MODEL_PRICE_ID_MAX_LENGTH)
  .regex(MODEL_ID_PATTERN, 'provider must be an addressable identifier');

/**
 * Base rates must be positive: zero means unpriced in the resolver. Cache
 * rates may be zero because vendors can explicitly make a cache bucket free.
 * All rates use the catalog's fixed three-decimal per-million precision so the
 * nano-USD accumulator can remain exact.
 */
const BaseRate = z.number().finite().positive().refine(hasSupportedPricePrecision, {
  message: 'rate supports at most three decimal places',
});
const CacheRate = z.number().finite().nonnegative().refine(hasSupportedPricePrecision, {
  message: 'rate supports at most three decimal places',
});

export const ModelPriceSource = z.enum(['operator', 'upstream', 'seed']);

/**
 * `source` is read-only. It is accepted and dropped rather than rejected so a
 * client can PATCH back the object it just read without a 400 for echoing a
 * field the server itself sent — but a value supplied here never reaches
 * storage. Every write on this API is an operator write.
 */
const ReadOnlySource = z.unknown().optional();

const ModelPriceRates = z.object({
  input_per_million_tokens: BaseRate,
  output_per_million_tokens: BaseRate,
  /** `null` clears the published rate, returning the bucket to its derived rate. */
  cache_read_per_million_tokens: CacheRate.nullable().optional(),
  cache_write_per_million_tokens: CacheRate.nullable().optional(),
});

export const ModelPriceCreate = ModelPriceRates.extend({
  provider: ModelProviderId.default(SEED_PRICE_PROVIDER),
  model_id: ModelPriceId,
  source: ReadOnlySource,
}).strict();

export const ModelPriceUpdate = ModelPriceRates.partial()
  // `provider` is identity, not a rate: moving a row between providers would be
  // a different entry, so it is accepted and dropped exactly like `source`.
  .extend({ source: ReadOnlySource, provider: z.unknown().optional() })
  .strict()
  .refine(
    (body) =>
      body.input_per_million_tokens !== undefined ||
      body.output_per_million_tokens !== undefined ||
      body.cache_read_per_million_tokens !== undefined ||
      body.cache_write_per_million_tokens !== undefined,
    // A body whose only field is ignored would otherwise report success for a
    // request that changed nothing.
    { message: 'at least one rate is required' },
  );

export type ModelPriceCreateBody = z.infer<typeof ModelPriceCreate>;
export type ModelPriceUpdateBody = z.infer<typeof ModelPriceUpdate>;

/** One stored entry, at one source. What the admin listener reads and writes. */
export const ModelPriceEntryWire = z.object({
  type: z.literal('model_price_entry'),
  provider: ModelProviderId,
  model_id: ModelPriceId,
  source: ModelPriceSource,
  input_per_million_tokens: BaseRate,
  output_per_million_tokens: BaseRate,
  cache_read_per_million_tokens: CacheRate.nullable(),
  cache_write_per_million_tokens: CacheRate.nullable(),
  fetched_at: isoTimestamp.nullable(),
  created_at: isoTimestamp,
  updated_at: isoTimestamp,
});

/**
 * The effective price of a model, after precedence and family fallback. What
 * the workspace listener serves — an operator reading this is asking what
 * their spend is measured against, not which row supplied it.
 */
export const ResolvedModelPriceWire = z.object({
  type: z.literal('model_price'),
  provider: ModelProviderId,
  model_id: ModelPriceId,
  input_per_million_tokens: BaseRate,
  output_per_million_tokens: BaseRate,
  cache_read_per_million_tokens: CacheRate,
  cache_write_per_million_tokens: CacheRate,
});

export const ModelPriceEntryDeleted = z.object({
  provider: ModelProviderId,
  model_id: ModelPriceId,
  type: z.literal('model_price_entry_deleted'),
});

export const modelPricesContract = c.router({
  list: {
    method: 'GET',
    path: `${PRICING_API_PREFIX}/modelprices`,
    query: pagination,
    responses: {
      200: z.object({ data: z.array(ResolvedModelPriceWire), next_page: z.string().nullable() }),
      400: ClaudeErrorResponse,
    },
  },
  get: {
    method: 'GET',
    path: `${PRICING_API_PREFIX}/modelprices/:model_id`,
    pathParams: z.object({ model_id: ModelPriceId }),
    // The provider half of the identity. Optional, defaulting to `anthropic`,
    // so a single-vendor deployment addresses an entry by model id alone.
    query: z.object({ provider: ModelProviderId.optional() }),
    // 400 is reachable: an unreadable `provider` is refused rather than quietly
    // resolved as the default, which would answer about a different entry.
    responses: {
      200: ResolvedModelPriceWire,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
  },
});

/** Strip the read-only fields and normalize the optional rates to explicit nulls. */
export function modelPriceWriteFromCreate(body: ModelPriceCreateBody): ModelPriceWrite {
  return {
    provider: body.provider,
    modelId: body.model_id,
    inputPerMillionTokens: body.input_per_million_tokens,
    outputPerMillionTokens: body.output_per_million_tokens,
    cacheReadPerMillionTokens: body.cache_read_per_million_tokens ?? null,
    cacheWritePerMillionTokens: body.cache_write_per_million_tokens ?? null,
  };
}

/**
 * Apply a patch to an existing entry.
 *
 * An omitted rate keeps its stored value; an explicit `null` clears a cache
 * rate back to derived. The two are different requests, so `undefined` and
 * `null` are read differently rather than collapsed.
 */
export function modelPriceWriteFromUpdate(
  body: ModelPriceUpdateBody,
  existing: StoredModelPrice,
): ModelPriceWrite {
  return {
    provider: existing.provider,
    modelId: existing.modelId,
    inputPerMillionTokens: body.input_per_million_tokens ?? existing.inputPerMillionTokens,
    outputPerMillionTokens: body.output_per_million_tokens ?? existing.outputPerMillionTokens,
    cacheReadPerMillionTokens:
      body.cache_read_per_million_tokens !== undefined
        ? body.cache_read_per_million_tokens
        : existing.cacheReadPerMillionTokens,
    cacheWritePerMillionTokens:
      body.cache_write_per_million_tokens !== undefined
        ? body.cache_write_per_million_tokens
        : existing.cacheWritePerMillionTokens,
  };
}

export function modelPriceEntryToWire(row: StoredModelPrice): z.infer<typeof ModelPriceEntryWire> {
  return {
    type: 'model_price_entry',
    provider: row.provider,
    model_id: row.modelId,
    source: row.source,
    input_per_million_tokens: row.inputPerMillionTokens,
    output_per_million_tokens: row.outputPerMillionTokens,
    cache_read_per_million_tokens: row.cacheReadPerMillionTokens,
    cache_write_per_million_tokens: row.cacheWritePerMillionTokens,
    fetched_at: row.fetchedAt?.toISOString() ?? null,
    created_at: row.createdAt.toISOString(),
    updated_at: row.updatedAt.toISOString(),
  };
}

/**
 * Twelve significant digits: far more precision than any published rate needs,
 * and enough to drop the representation noise a per-token round trip leaves
 * behind (a derived rate of `0.08` arrives as `0.08000000000000002`). Nothing
 * here changes a rate a caller would notice; it changes how it prints.
 */
function quote(perTokenRate: number): number {
  return Number((perTokenRate * TOKENS_PER_MILLION).toPrecision(12));
}

/**
 * Quote a resolved price per million tokens.
 *
 * The cache rates reported are the *effective* ones: when a source publishes
 * none, the engine derives them from the input rate rather than dropping the
 * bucket, so those derived rates are what a session is actually billed at.
 * Reporting `null` here would read as free.
 */
export function resolvedModelPriceToWire(
  provider: string,
  modelId: string,
  pricing: ModelPricing,
): z.infer<typeof ResolvedModelPriceWire> {
  return {
    type: 'model_price',
    provider,
    model_id: modelId,
    input_per_million_tokens: quote(pricing.inputPerToken),
    output_per_million_tokens: quote(pricing.outputPerToken),
    cache_read_per_million_tokens: quote(
      pricing.cacheReadPerToken ?? pricing.inputPerToken * CACHE_READ_INPUT_RATIO,
    ),
    cache_write_per_million_tokens: quote(
      pricing.cacheWritePerToken ?? pricing.inputPerToken * CACHE_WRITE_5M_INPUT_RATIO,
    ),
  };
}

export function parseModelPriceSource(raw: unknown): PriceSource | null {
  const parsed = ModelPriceSource.safeParse(raw);
  return parsed.success ? parsed.data : null;
}
