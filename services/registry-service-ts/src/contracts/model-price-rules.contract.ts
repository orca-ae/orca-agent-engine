// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import { hasSupportedPricePrecision } from '@orca/harness-catalog';
import { ClaudeErrorResponse, isoTimestamp } from './common.js';
import { ModelProviderId } from './model-prices.contract.js';

export const PRICING_V2_PREFIX = '/apis/pricing.runorca.ai/v2';
const c = initContract();
const count = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
const rate = z.number().finite().nonnegative().refine(hasSupportedPricePrecision);
const baseRate = rate.refine((n) => n > 0);
export const PriceConditionsSchema = z
  .object({
    service_tier: z.string().min(1).max(64),
    request_input_tokens: z
      .object({ min: count, max: count.nullable() })
      .strict()
      .refine((b) => b.max === null || b.min < b.max, 'invalid integer interval'),
  })
  .strict();
export const ConditionalRatesSchema = z
  .object({
    input_per_million_tokens: baseRate,
    output_per_million_tokens: baseRate,
    cache_read_per_million_tokens: rate.nullable(),
    cache_write_per_million_tokens: rate.nullable(),
    cache_write_1h_per_million_tokens: rate.nullable(),
  })
  .strict();
export const PriceRuleInputSchema = z
  .object({
    conditions: PriceConditionsSchema,
    rates: ConditionalRatesSchema,
  })
  .strict();
export const PriceRuleSetCreateSchema = z
  .object({
    provider: ModelProviderId,
    model_id: z
      .string()
      .min(1)
      .max(200)
      .regex(/^[A-Za-z0-9][A-Za-z0-9._:/-]*$/),
    profile_version: z.enum(['literal-v1', 'openai-v1', 'anthropic-v1']),
    usage_profile: z.enum(['exclusive-v1', 'anthropic-cache-v1']),
    rules: z.array(PriceRuleInputSchema).min(1).max(10_000),
    provider_effective_at: isoTimestamp.nullable().default(null),
    /** Explicit reviewed conversion. Dormant publications leave legacy accounting unchanged. */
    activate: z.boolean().default(false),
  })
  .strict();
export const PriceRuleSetReplaceSchema = PriceRuleSetCreateSchema;
export const RequestPriceFactsSchema = z
  .object({
    version: z.literal(1),
    request_id: z.string().min(1).max(200),
    service_tier: z.string().min(1).max(64).nullable(),
    request_input_tokens: count.nullable(),
    usage_profile: z.string().min(1).max(64),
    evidence_source: z.enum(['provider_response', 'legacy_adapter']),
    unsupported_usage: z.boolean().optional(),
    observed_at: isoTimestamp.optional(),
  })
  .strict();
export const CatalogRuleSchema = PriceRuleInputSchema.extend({
  rule_set_id: z.string(),
  rule_id: z.string(),
  revision: z.number().int().positive(),
  provider: z.string(),
  model_id: z.string(),
  source: z.enum(['operator', 'upstream', 'seed']),
  organization_id: z.string().optional(),
  usage_profile: z.enum(['exclusive-v1', 'anthropic-cache-v1']),
  profile_version: z.string(),
  provenance: z.object({
    source_id: z.string(),
    document_sha256: z.string(),
    adapter_version: z.string(),
    observed_at: isoTimestamp,
  }),
  provider_effective_at: isoTimestamp.nullable(),
  active_from: isoTimestamp,
  retired_at: isoTimestamp.nullable(),
  valid_until: isoTimestamp.nullable(),
});
export const PricingCatalogPageSchema = z.object({
  catalog_id: z.string(),
  generation: count,
  published_at: isoTimestamp,
  digest: z.string(),
  required_features: z.array(z.string()),
  conditional_models: z.array(z.object({ provider: z.string(), model_id: z.string() })),
  expires_at: isoTimestamp,
  data: z.array(CatalogRuleSchema),
  next_page: z.string().nullable(),
});
export const modelPriceRulesContract = c.router({
  list: {
    method: 'GET',
    path: `${PRICING_V2_PREFIX}/modelpricerules`,
    query: z.object({
      limit: z.coerce.number().int().min(1).max(100).optional(),
      page: z.string().min(1).max(500).optional(),
    }),
    responses: {
      200: PricingCatalogPageSchema,
      400: ClaudeErrorResponse,
      410: ClaudeErrorResponse,
    },
  },
});
