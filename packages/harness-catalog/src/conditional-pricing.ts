// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  CACHE_READ_INPUT_RATIO,
  CACHE_WRITE_5M_INPUT_RATIO,
  hasSupportedPricePrecision,
  type ModelPriceEntry,
  type ModelPricing,
  type PriceSource,
  type TokenUsage,
} from './pricing.js';

export const PRICING_V2_FEATURES = [
  'exact-identity-v1',
  'actual-service-tier-v1',
  'whole-request-input-band-v1',
  'explicit-cache-rates-v1',
  'source-precedence-v1',
  'per-rule-validity-v1',
] as const;

export interface PriceConditions {
  service_tier: string;
  request_input_tokens: { min: number; max: number | null };
}

/** null means unsupported, never an instruction to derive a rate. */
export interface ConditionalPriceRates {
  input_per_million_tokens: number;
  output_per_million_tokens: number;
  cache_read_per_million_tokens: number | null;
  cache_write_per_million_tokens: number | null;
  cache_write_1h_per_million_tokens: number | null;
}

export interface PriceRule {
  rule_set_id: string;
  rule_id: string;
  revision: number;
  provider: string;
  model_id: string;
  source: PriceSource;
  /** Internal authority; stripped from workspace snapshots. */
  organization_id?: string;
  conditions: PriceConditions;
  rates: ConditionalPriceRates;
  /** Only this admitted profile supports the existing 5m/1h cache interpretation. */
  usage_profile: 'exclusive-v1' | 'anthropic-cache-v1';
  profile_version: string;
  provenance: {
    source_id: string;
    document_sha256: string;
    adapter_version: string;
    observed_at: string;
  };
  provider_effective_at: string | null;
}

/** Freshness/activation attestations belong to a catalog, not to a mutable rule. */
export interface CatalogPriceRule extends PriceRule {
  active_from: string;
  retired_at: string | null;
  valid_until: string | null;
}

export interface PricingCatalog {
  catalog_id: string;
  generation: number;
  published_at: string;
  required_features: readonly string[];
  rules: readonly CatalogPriceRule[];
}

export interface RequestPriceFacts {
  version: 1;
  request_id: string;
  service_tier: string | null;
  request_input_tokens: number | null;
  usage_profile: string;
  evidence_source: 'provider_response' | 'legacy_adapter';
  /** Writers flag additional billable dimensions instead of silently dropping them. */
  unsupported_usage?: boolean | undefined;
  observed_at?: string | undefined;
}

export type UnpricedReason =
  | 'unsupported_features'
  | 'catalog_not_published'
  | 'unknown_service_tier'
  | 'unknown_request_input'
  | 'invalid_usage'
  | 'input_count_mismatch'
  | 'unsupported_usage'
  | 'no_matching_rule'
  | 'conflicting_rules';

export type ConditionalPriceResolution =
  | { status: 'unpriced'; reason: UnpricedReason }
  | { status: 'priced'; rule: CatalogPriceRule; cost_nano_usd: bigint };

/** Profiles are reviewed code. Unknown profile versions never acquire aliases. */
export function canonicalPriceTier(
  provider: string,
  tier: string | null,
  profileVersion: string,
): string | null {
  if (!tier || tier === 'auto' || tier === 'unknown') return null;
  if (profileVersion === 'openai-v1' && provider === 'openai') {
    return (
      (
        {
          default: 'standard',
          standard: 'standard',
          fast: 'priority',
          priority: 'priority',
          flex: 'flex',
          batch: 'batch',
          ultrafast: 'ultrafast',
        } as Record<string, string>
      )[tier] ?? null
    );
  }
  if (profileVersion === 'anthropic-v1' && provider === 'anthropic') {
    return (
      (
        { standard: 'standard', priority: 'priority', batch: 'batch', fast: 'fast' } as Record<
          string,
          string
        >
      )[tier] ?? null
    );
  }
  return profileVersion === 'literal-v1' && /^[a-z][a-z0-9_-]{0,63}$/.test(tier) ? tier : null;
}

const rank: Record<PriceSource, number> = { operator: 3, upstream: 2, seed: 1 };
const count = (n: unknown): n is number => Number.isSafeInteger(n) && (n as number) >= 0;
const time = (s: string): number => Date.parse(s);
const unpriced = (reason: UnpricedReason): ConditionalPriceResolution => ({
  status: 'unpriced',
  reason,
});

export function priceConditionKey(conditions: PriceConditions): string {
  return JSON.stringify([
    conditions.service_tier,
    conditions.request_input_tokens.min,
    conditions.request_input_tokens.max,
  ]);
}

/** Check a complete model/source/scope set before publication. Adjacent bands are valid. */
export function validatePriceRuleSet(rules: readonly PriceRule[]): string | null {
  if (rules.length === 0 || rules.length > 10_000) return 'a rule set needs 1 to 10000 rules';
  const first = rules[0]!;
  const ids = new Set<string>();
  for (const rule of rules) {
    if (
      rule.provider !== first.provider ||
      rule.model_id !== first.model_id ||
      rule.source !== first.source ||
      rule.organization_id !== first.organization_id
    ) {
      return 'a rule set must have one provider, model, source and scope';
    }
    if (rule.source === 'operator' ? !rule.organization_id : !!rule.organization_id)
      return 'invalid rule scope';
    if (
      !rule.rule_id ||
      ids.has(rule.rule_id) ||
      !Number.isSafeInteger(rule.revision) ||
      rule.revision < 1
    )
      return 'invalid or duplicate rule identity';
    ids.add(rule.rule_id);
    const { min, max } = rule.conditions.request_input_tokens;
    if (!count(min) || (max !== null && (!count(max) || min >= max)))
      return 'invalid integer interval';
    if (
      canonicalPriceTier(rule.provider, rule.conditions.service_tier, rule.profile_version) !==
      rule.conditions.service_tier
    )
      return 'rule tier must be canonical for its profile';
    if (
      !['exclusive-v1', 'anthropic-cache-v1'].includes(rule.usage_profile) ||
      (rule.usage_profile === 'anthropic-cache-v1' && rule.provider !== 'anthropic')
    )
      return 'unsupported usage profile';
    const rateKeys = [
      'input_per_million_tokens',
      'output_per_million_tokens',
      'cache_read_per_million_tokens',
      'cache_write_per_million_tokens',
      'cache_write_1h_per_million_tokens',
    ];
    if (
      Object.keys(rule.rates).length !== rateKeys.length ||
      rateKeys.some((k) => !(k in rule.rates))
    )
      return 'incomplete rate tuple';
    for (const [key, value] of Object.entries(rule.rates)) {
      const base = key === 'input_per_million_tokens' || key === 'output_per_million_tokens';
      if (value === null && !base) continue;
      if (
        typeof value !== 'number' ||
        !Number.isFinite(value) ||
        (base ? value <= 0 : value < 0) ||
        !hasSupportedPricePrecision(value)
      )
        return 'invalid rate or precision';
    }
    if (rule.provider_effective_at !== null && !Number.isFinite(time(rule.provider_effective_at)))
      return 'invalid effective time';
  }
  const byTier = new Map<string, PriceRule[]>();
  for (const rule of rules) {
    const tier = rule.conditions.service_tier;
    const group = byTier.get(tier) ?? [];
    group.push(rule);
    byTier.set(tier, group);
  }
  for (const group of byTier.values()) {
    group.sort(
      (a, b) => a.conditions.request_input_tokens.min - b.conditions.request_input_tokens.min,
    );
    for (let i = 1; i < group.length; i++) {
      if (
        (group[i - 1]!.conditions.request_input_tokens.max ?? Infinity) >
        group[i]!.conditions.request_input_tokens.min
      )
        return 'overlapping price intervals';
    }
  }
  return null;
}

/** Pure whole-request resolution. The caller supplies authoritative acceptance time. */
export function resolveConditionalPrice(input: {
  catalog: PricingCatalog;
  organization_id?: string | undefined;
  provider: string;
  model_id: string;
  facts: RequestPriceFacts | null;
  usage: TokenUsage;
  pricing_time: string;
}): ConditionalPriceResolution {
  const { catalog, facts, usage } = input;
  const at = time(input.pricing_time);
  if (
    catalog.required_features.some((f) => !(PRICING_V2_FEATURES as readonly string[]).includes(f))
  )
    return unpriced('unsupported_features');
  if (
    !Number.isFinite(at) ||
    !Number.isFinite(time(catalog.published_at)) ||
    time(catalog.published_at) > at
  )
    return unpriced('catalog_not_published');
  if (!facts?.service_tier || facts.service_tier === 'auto')
    return unpriced('unknown_service_tier');
  if (!count(facts.request_input_tokens)) return unpriced('unknown_request_input');
  if (
    facts.version !== 1 ||
    !facts.request_id ||
    facts.unsupported_usage ||
    !['provider_response', 'legacy_adapter'].includes(facts.evidence_source)
  )
    return unpriced('unsupported_usage');
  const buckets = [
    usage.inputTokens,
    usage.outputTokens,
    usage.cacheReadInputTokens,
    usage.cacheCreationEphemeral5mInputTokens,
    usage.cacheCreationEphemeral1hInputTokens,
  ];
  if (buckets.some((n) => !count(n))) return unpriced('invalid_usage');
  const fullInput =
    usage.inputTokens! +
    usage.cacheReadInputTokens! +
    usage.cacheCreationEphemeral5mInputTokens! +
    usage.cacheCreationEphemeral1hInputTokens!;
  if (!count(fullInput) || fullInput !== facts.request_input_tokens)
    return unpriced('input_count_mismatch');
  const candidates = catalog.rules.filter((r) => {
    const band = r.conditions.request_input_tokens;
    return (
      r.provider === input.provider &&
      r.model_id === input.model_id &&
      (r.source !== 'operator' ||
        (!!input.organization_id && r.organization_id === input.organization_id)) &&
      canonicalPriceTier(r.provider, facts.service_tier, r.profile_version) ===
        r.conditions.service_tier &&
      band.min <= fullInput &&
      (band.max === null || fullInput < band.max) &&
      Number.isFinite(time(r.active_from)) &&
      time(r.active_from) <= at &&
      (r.retired_at === null || at < time(r.retired_at)) &&
      (r.valid_until === null || at < time(r.valid_until)) &&
      (r.provider_effective_at === null || time(r.provider_effective_at) <= at)
    );
  });
  const best = Math.max(...candidates.map((r) => rank[r.source]));
  const winners = candidates.filter((r) => rank[r.source] === best);
  if (winners.length === 0) return unpriced('no_matching_rule');
  if (winners.length !== 1) return unpriced('conflicting_rules');
  const rule = winners[0]!;
  if (validatePriceRuleSet([rule])) return unpriced('unsupported_usage');
  if (
    facts.usage_profile !== rule.usage_profile ||
    (facts.evidence_source === 'legacy_adapter' && facts.service_tier !== 'standard')
  )
    return unpriced('unsupported_usage');
  const rates = rule.rates;
  const write1h =
    rule.usage_profile === 'anthropic-cache-v1' ? rates.cache_write_1h_per_million_tokens : null;
  const quoted = [
    rates.input_per_million_tokens,
    rates.output_per_million_tokens,
    rates.cache_read_per_million_tokens,
    rates.cache_write_per_million_tokens,
    write1h,
  ];
  let cost = 0n;
  for (let i = 0; i < buckets.length; i++) {
    if (buckets[i] === 0) continue;
    const rate = quoted[i];
    if (
      rate === null ||
      rate === undefined ||
      !Number.isFinite(rate) ||
      rate < 0 ||
      !Number.isSafeInteger(Math.round(rate * 1000))
    )
      return unpriced('unsupported_usage');
    cost += BigInt(buckets[i]!) * BigInt(Math.round(rate * 1000));
  }
  return { status: 'priced', rule, cost_nano_usd: cost };
}

/** Conversion requires reviewed context coverage; it never invents an unbounded domain. */
export function materializeLegacyPriceRates(entry: ModelPriceEntry): ConditionalPriceRates {
  return {
    input_per_million_tokens: entry.inputPerMillionTokens,
    output_per_million_tokens: entry.outputPerMillionTokens,
    cache_read_per_million_tokens:
      Math.round(
        (entry.cacheReadPerMillionTokens ?? entry.inputPerMillionTokens * CACHE_READ_INPUT_RATIO) *
          1000,
      ) / 1000,
    cache_write_per_million_tokens:
      Math.round(
        (entry.cacheWritePerMillionTokens ??
          entry.inputPerMillionTokens * CACHE_WRITE_5M_INPUT_RATIO) * 1000,
      ) / 1000,
    cache_write_1h_per_million_tokens:
      Math.round(
        (entry.cacheWritePerMillionTokens === undefined
          ? entry.inputPerMillionTokens * 2
          : entry.cacheWritePerMillionTokens * 1.6) * 1000,
      ) / 1000,
  };
}

/** Safe v1 projection: identical effective standard rates over the complete integer domain. */
export function projectConditionalPriceToV1(
  catalog: PricingCatalog,
  provider: string,
  modelId: string,
  organizationId: string,
  at: string,
): ModelPricing | null {
  const rules = catalog.rules.filter((r) => r.provider === provider && r.model_id === modelId);
  const boundaries = [
    ...new Set([
      0,
      ...rules.flatMap((r) => [
        r.conditions.request_input_tokens.min,
        ...(r.conditions.request_input_tokens.max === null
          ? []
          : [r.conditions.request_input_tokens.max]),
      ]),
    ]),
  ].sort((a, b) => a - b);
  let first: ConditionalPriceRates | undefined;
  for (const n of boundaries) {
    const result = resolveConditionalPrice({
      catalog,
      provider,
      model_id: modelId,
      organization_id: organizationId,
      pricing_time: at,
      facts: {
        version: 1,
        request_id: 'projection',
        service_tier: 'standard',
        request_input_tokens: n,
        usage_profile: rules[0]?.usage_profile ?? 'exclusive-v1',
        evidence_source: 'provider_response',
      },
      usage: {
        inputTokens: n,
        outputTokens: 0,
        cacheReadInputTokens: 0,
        cacheCreationEphemeral5mInputTokens: 0,
        cacheCreationEphemeral1hInputTokens: 0,
      },
    });
    if (result.status !== 'priced') return null;
    const rates = result.rule.rates;
    if (
      rates.cache_read_per_million_tokens === null ||
      rates.cache_write_per_million_tokens === null
    )
      return null;
    if (
      result.rule.usage_profile !== 'anthropic-cache-v1' ||
      rates.cache_write_1h_per_million_tokens === null ||
      Math.round(rates.cache_write_per_million_tokens * 1600) !==
        Math.round(rates.cache_write_1h_per_million_tokens * 1000)
    )
      return null;
    if (
      first &&
      Object.keys(first).some(
        (key) =>
          first![key as keyof ConditionalPriceRates] !== rates[key as keyof ConditionalPriceRates],
      )
    )
      return null;
    first = rates;
  }
  return first
    ? {
        inputPerToken: first.input_per_million_tokens / 1e6,
        outputPerToken: first.output_per_million_tokens / 1e6,
        cacheReadPerToken: first.cache_read_per_million_tokens! / 1e6,
        cacheWritePerToken: first.cache_write_per_million_tokens! / 1e6,
      }
    : null;
}
