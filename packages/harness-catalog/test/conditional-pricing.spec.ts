// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import {
  PRICING_V2_FEATURES,
  canonicalPriceTier,
  priceConditionKey,
  resolveConditionalPrice,
  validatePriceRuleSet,
  materializeLegacyPriceRates,
  projectConditionalPriceToV1,
  type CatalogPriceRule,
  type PricingCatalog,
  type RequestPriceFacts,
} from '../src/conditional-pricing.js';
import { computeCostNanoUsd } from '../src/pricing.js';

const at = '2026-10-29T00:00:00.000Z';
const usage = (inputTokens = 100) => ({
  inputTokens,
  outputTokens: 10,
  cacheReadInputTokens: 0,
  cacheCreationEphemeral5mInputTokens: 0,
  cacheCreationEphemeral1hInputTokens: 0,
});
const facts = (n = 100, tier: string | null = 'standard'): RequestPriceFacts => ({
  version: 1,
  request_id: 'response-1',
  service_tier: tier,
  request_input_tokens: n,
  usage_profile: 'anthropic-cache-v1',
  evidence_source: 'provider_response',
});
function rule(extra: Partial<CatalogPriceRule> = {}): CatalogPriceRule {
  return {
    rule_set_id: 'set-1',
    rule_id: 'rule-1',
    revision: 1,
    provider: 'anthropic',
    model_id: 'model/one',
    source: 'seed',
    conditions: { service_tier: 'standard', request_input_tokens: { min: 0, max: null } },
    rates: {
      input_per_million_tokens: 2,
      output_per_million_tokens: 8,
      cache_read_per_million_tokens: 0.2,
      cache_write_per_million_tokens: 2.5,
      cache_write_1h_per_million_tokens: 4,
    },
    usage_profile: 'anthropic-cache-v1',
    profile_version: 'anthropic-v1',
    provenance: {
      source_id: 'test',
      document_sha256: 'a'.repeat(64),
      adapter_version: 'test-v1',
      observed_at: at,
    },
    active_from: at,
    retired_at: null,
    valid_until: null,
    provider_effective_at: null,
    ...extra,
  };
}
function catalog(rules = [rule()]): PricingCatalog {
  return {
    catalog_id: 'catalog-1',
    generation: 1,
    published_at: at,
    required_features: PRICING_V2_FEATURES,
    rules,
  };
}
function resolve(rules = [rule()], n = 100, f = facts(n), atTime = at) {
  return resolveConditionalPrice({
    catalog: catalog(rules),
    provider: 'anthropic',
    model_id: 'model/one',
    organization_id: 'org-1',
    facts: f,
    usage: usage(n),
    pricing_time: atTime,
  });
}

describe('conditional price conformance', () => {
  const shared = JSON.parse(
    readFileSync(new URL('./fixtures/conditional-pricing-v1.json', import.meta.url), 'utf8'),
  ) as {
    vectors: {
      name: string;
      input: Parameters<typeof resolveConditionalPrice>[0];
      expected: { status: string; reason?: string; cost_nano_usd?: string; rule_id?: string };
    }[];
  };
  for (const vector of shared.vectors)
    it(`shared TS/Rust vector: ${vector.name}`, () => {
      const result = resolveConditionalPrice(vector.input);
      expect(result.status).toBe(vector.expected.status);
      if (result.status === 'unpriced') expect(result.reason).toBe(vector.expected.reason);
      else {
        expect(result.cost_nano_usd.toString()).toBe(vector.expected.cost_nano_usd);
        expect(result.rule.rule_id).toBe(vector.expected.rule_id);
      }
    });
  it('uses whole-request half-open integer bands, including exactly 100000 and 100001', () => {
    const low = rule({
      conditions: { service_tier: 'standard', request_input_tokens: { min: 0, max: 100001 } },
    });
    const high = rule({
      rule_id: 'high',
      conditions: { service_tier: 'standard', request_input_tokens: { min: 100001, max: null } },
      rates: { ...low.rates, input_per_million_tokens: 4 },
    });
    expect(resolve([low, high], 100000)).toMatchObject({
      status: 'priced',
      cost_nano_usd: 200080000n,
    });
    expect(resolve([low, high], 100001)).toMatchObject({
      status: 'priced',
      rule: { rule_id: 'high' },
      cost_nano_usd: 400084000n,
    });
    expect(validatePriceRuleSet([low, high])).toBeNull();
    expect(validatePriceRuleSet([low, rule({ rule_id: 'overlap' })])).toMatch(/overlap/);
  });
  it('never guesses identity, tier, context or a standard price for priority', () => {
    for (const tier of [null, 'auto', 'unknown', 'priority'])
      expect(resolve([rule()], 100, facts(100, tier)).status).toBe('unpriced');
    expect(resolve([rule()], 100, { ...facts(), request_input_tokens: null })).toMatchObject({
      reason: 'unknown_request_input',
    });
    expect(
      resolveConditionalPrice({
        catalog: catalog(),
        provider: 'anthropic',
        model_id: 'model/one-20261029',
        facts: facts(),
        usage: usage(),
        pricing_time: at,
      }),
    ).toMatchObject({ reason: 'no_matching_rule' });
  });
  it('normalizes only reviewed OpenAI actual tier aliases', () => {
    for (const tier of ['fast', 'priority']) {
      const r = rule({
        provider: 'openai',
        profile_version: 'openai-v1',
        usage_profile: 'exclusive-v1',
        conditions: { service_tier: 'priority', request_input_tokens: { min: 0, max: null } },
      });
      expect(
        resolveConditionalPrice({
          catalog: catalog([r]),
          provider: 'openai',
          model_id: r.model_id,
          facts: { ...facts(100, tier), usage_profile: 'exclusive-v1' },
          usage: usage(),
          pricing_time: at,
        }).status,
      ).toBe('priced');
    }
  });
  it('applies scope and source precedence independently of array order, refusing any winning overlap', () => {
    const upstream = rule({ rule_id: 'upstream', source: 'upstream' });
    const operator = rule({ rule_id: 'operator', source: 'operator', organization_id: 'org-1' });
    for (const rows of [
      [operator, upstream],
      [upstream, operator],
    ])
      expect(resolve(rows)).toMatchObject({ rule: { rule_id: 'operator' } });
    expect(resolve([upstream, { ...operator, organization_id: 'org-other' }])).toMatchObject({
      rule: { rule_id: 'upstream' },
    });
    expect(resolve([operator, { ...operator, rule_id: 'other' }])).toMatchObject({
      reason: 'conflicting_rules',
    });
    expect(
      resolve([
        rule({
          conditions: { service_tier: 'priority', request_input_tokens: { min: 0, max: null } },
        }),
        upstream,
      ]),
    ).toMatchObject({ rule: { rule_id: 'upstream' } });
  });
  it('counts cached input exactly once and refuses unsupported nonzero buckets', () => {
    const u = {
      ...usage(1000),
      outputTokens: 100,
      cacheReadInputTokens: 200,
      cacheCreationEphemeral5mInputTokens: 300,
    };
    const resolveUsage = (r: CatalogPriceRule, f = facts(1500)) =>
      resolveConditionalPrice({
        catalog: catalog([r]),
        provider: 'anthropic',
        model_id: r.model_id,
        facts: f,
        usage: u,
        pricing_time: at,
      });
    expect(resolveUsage(rule())).toMatchObject({ cost_nano_usd: 3590000n });
    expect(resolveUsage(rule(), facts(1000))).toMatchObject({ reason: 'input_count_mismatch' });
    expect(
      resolveUsage(rule({ rates: { ...rule().rates, cache_read_per_million_tokens: null } })),
    ).toMatchObject({ reason: 'unsupported_usage' });
    expect(resolveUsage(rule(), { ...facts(1500), unsupported_usage: true })).toMatchObject({
      reason: 'unsupported_usage',
    });
    expect(resolve([rule()], 0, facts(0))).toMatchObject({ status: 'priced' });
  });
  it('enforces publication, activation, effective time, retirement and exact expiry', () => {
    const later = '2026-10-29T01:00:00.000Z';
    const r = rule({ provider_effective_at: later, active_from: later });
    expect(resolve([r])).toMatchObject({ reason: 'no_matching_rule' });
    expect(resolve([r], 100, facts(), later).status).toBe('priced');
    expect(resolve([rule({ valid_until: later })], 100, facts(), later).status).toBe('unpriced');
    expect(resolve([rule({ retired_at: later })], 100, facts(), later).status).toBe('unpriced');
    expect(resolve([rule()], 100, facts(), '2026-10-28T23:59:59Z')).toMatchObject({
      reason: 'catalog_not_published',
    });
    expect(
      resolve([rule({ source: 'upstream', valid_until: at }), rule({ rule_id: 'fallback' })]),
    ).toMatchObject({ rule: { rule_id: 'fallback' } });
  });
  it('refuses unknown features and preserves legacy per-rate rounding, including the 1h bucket', () => {
    expect(
      resolveConditionalPrice({
        catalog: { ...catalog(), required_features: ['future'] },
        provider: 'anthropic',
        model_id: 'model/one',
        facts: facts(),
        usage: usage(),
        pricing_time: at,
      }),
    ).toMatchObject({ reason: 'unsupported_features' });
    for (const input of [2, 0.002, 0.333])
      for (const override of [undefined, 0.123, 0]) {
        const rates = materializeLegacyPriceRates({
          provider: 'anthropic',
          modelId: 'model/one',
          source: 'seed',
          inputPerMillionTokens: input,
          outputPerMillionTokens: 8,
          ...(override === undefined ? {} : { cacheWritePerMillionTokens: override }),
        });
        const u = {
          ...usage(1000),
          outputTokens: 100,
          cacheReadInputTokens: 200,
          cacheCreationEphemeral5mInputTokens: 300,
          cacheCreationEphemeral1hInputTokens: 400,
        };
        const actual = resolveConditionalPrice({
          catalog: catalog([rule({ rates })]),
          provider: 'anthropic',
          model_id: 'model/one',
          facts: facts(1900),
          usage: u,
          pricing_time: at,
        });
        expect(actual).toMatchObject({
          status: 'priced',
          cost_nano_usd: computeCostNanoUsd(u, {
            inputPerToken: input / 1e6,
            outputPerToken: 8 / 1e6,
            ...(override === undefined ? {} : { cacheWritePerToken: override / 1e6 }),
          }),
        });
        expect(validatePriceRuleSet([rule({ rates })])).toBeNull();
      }
  });
  it('projects only complete flat standard prices into v1', () => {
    expect(
      projectConditionalPriceToV1(catalog(), 'anthropic', 'model/one', 'org-1', at)?.inputPerToken,
    ).toBe(0.000002);
    expect(
      projectConditionalPriceToV1(
        catalog([
          rule({
            conditions: { service_tier: 'standard', request_input_tokens: { min: 0, max: 100001 } },
          }),
        ]),
        'anthropic',
        'model/one',
        'org-1',
        at,
      ),
    ).toBeNull();
  });
});

describe('conditional price rule admission', () => {
  it.each<[string, string, string, string | null]>([
    ['openai', 'default', 'openai-v1', 'standard'],
    ['openai', 'unrecognized', 'openai-v1', null],
    ['anthropic', 'fast', 'anthropic-v1', 'fast'],
    ['anthropic', 'default', 'anthropic-v1', null],
    ['custom', 'priority_1-fast', 'literal-v1', 'priority_1-fast'],
    ['custom', 'Priority', 'literal-v1', null],
    ['custom', 'a'.repeat(65), 'literal-v1', null],
    ['openai', 'fast', 'future-v1', null],
    ['custom', 'fast', 'openai-v1', null],
  ])('keeps %s tier %s bound to profile %s', (provider, tier, profile, expected) => {
    expect(canonicalPriceTier(provider, tier, profile)).toBe(expected);
  });

  it('keeps condition identity stable across field order and distinct across tiers and bands', () => {
    const standard = {
      service_tier: 'standard',
      request_input_tokens: { min: 0, max: 100001 },
    };
    expect(
      priceConditionKey({
        request_input_tokens: { max: 100001, min: 0 },
        service_tier: 'standard',
      }),
    ).toBe(priceConditionKey(standard));
    expect(
      new Set([
        priceConditionKey(standard),
        priceConditionKey({ ...standard, service_tier: 'priority' }),
        priceConditionKey({ ...standard, request_input_tokens: { min: 1, max: 100001 } }),
        priceConditionKey({ ...standard, request_input_tokens: { min: 0, max: null } }),
      ]).size,
    ).toBe(4);
  });

  it('bounds the number of rules before accepting a set', () => {
    expect(validatePriceRuleSet([])).toMatch(/1 to 10000/);
    expect(validatePriceRuleSet(Array.from({ length: 10001 }, () => rule()))).toMatch(/1 to 10000/);
  });

  it.each([
    { provider: 'openai' },
    { model_id: 'another-model' },
    { source: 'upstream' as const },
    { organization_id: 'another-organization' },
  ])('rejects mixed rule-set authority: %j', (change) => {
    expect(validatePriceRuleSet([rule(), rule({ rule_id: 'other', ...change })])).toMatch(
      /one provider, model, source and scope/,
    );
  });

  const invalidRules: {
    name: string;
    change: Partial<CatalogPriceRule>;
    error: RegExp;
  }[] = [
    { name: 'operator without an organization', change: { source: 'operator' }, error: /scope/ },
    {
      name: 'shared rule with an organization',
      change: { organization_id: 'org-1' },
      error: /scope/,
    },
    { name: 'empty rule identity', change: { rule_id: '' }, error: /identity/ },
    { name: 'zero revision', change: { revision: 0 }, error: /identity/ },
    { name: 'fractional revision', change: { revision: 1.5 }, error: /identity/ },
    {
      name: 'negative input boundary',
      change: {
        conditions: { service_tier: 'standard', request_input_tokens: { min: -1, max: null } },
      },
      error: /interval/,
    },
    {
      name: 'fractional input boundary',
      change: {
        conditions: { service_tier: 'standard', request_input_tokens: { min: 0, max: 1.5 } },
      },
      error: /interval/,
    },
    {
      name: 'empty input band',
      change: {
        conditions: { service_tier: 'standard', request_input_tokens: { min: 1, max: 1 } },
      },
      error: /interval/,
    },
    {
      name: 'noncanonical tier alias',
      change: {
        provider: 'openai',
        profile_version: 'openai-v1',
        conditions: { service_tier: 'fast', request_input_tokens: { min: 0, max: null } },
      },
      error: /canonical/,
    },
    {
      name: 'unreviewed usage profile',
      change: { usage_profile: 'future-v1' as CatalogPriceRule['usage_profile'] },
      error: /usage profile/,
    },
    {
      name: 'Anthropic cache semantics for another provider',
      change: { provider: 'openai', profile_version: 'openai-v1' },
      error: /usage profile/,
    },
    {
      name: 'missing cache rate declaration',
      change: {
        rates: {
          input_per_million_tokens: 2,
          output_per_million_tokens: 8,
          cache_write_per_million_tokens: 2.5,
          cache_write_1h_per_million_tokens: 4,
        } as CatalogPriceRule['rates'],
      },
      error: /rate tuple/,
    },
    {
      name: 'unknown rate replacing a required dimension',
      change: {
        rates: {
          input_per_million_tokens: 2,
          output_per_million_tokens: 8,
          cache_write_per_million_tokens: 2.5,
          cache_write_1h_per_million_tokens: 4,
          future_rate: 1,
        } as unknown as CatalogPriceRule['rates'],
      },
      error: /rate tuple/,
    },
    {
      name: 'zero base rate',
      change: { rates: { ...rule().rates, output_per_million_tokens: 0 } },
      error: /rate or precision/,
    },
    {
      name: 'negative cache rate',
      change: { rates: { ...rule().rates, cache_read_per_million_tokens: -1 } },
      error: /rate or precision/,
    },
    {
      name: 'nonfinite rate',
      change: { rates: { ...rule().rates, input_per_million_tokens: Infinity } },
      error: /rate or precision/,
    },
    {
      name: 'unsupported rate precision',
      change: { rates: { ...rule().rates, input_per_million_tokens: 0.0001 } },
      error: /rate or precision/,
    },
    { name: 'invalid effective time', change: { provider_effective_at: 'invalid' }, error: /time/ },
  ];
  it.each(invalidRules)('rejects $name', ({ change, error }) => {
    expect(validatePriceRuleSet([rule(change)])).toMatch(error);
  });

  it('rejects duplicate identities and overlap after an unbounded band', () => {
    expect(validatePriceRuleSet([rule(), rule()])).toMatch(/duplicate rule identity/);
    expect(
      validatePriceRuleSet([
        rule(),
        rule({
          rule_id: 'later-band',
          conditions: {
            service_tier: 'standard',
            request_input_tokens: { min: 100001, max: null },
          },
        }),
      ]),
    ).toMatch(/overlapping/);
  });
});

describe('conditional price refusal and v1 compatibility', () => {
  it('refuses invalid winning rules and mismatched usage evidence', () => {
    expect(resolve([rule({ revision: 0 })])).toMatchObject({ reason: 'unsupported_usage' });
    expect(resolve([rule()], 100, { ...facts(), usage_profile: 'exclusive-v1' })).toMatchObject({
      reason: 'unsupported_usage',
    });
    expect(
      resolve(
        [
          rule({
            conditions: { service_tier: 'priority', request_input_tokens: { min: 0, max: null } },
          }),
        ],
        100,
        { ...facts(100, 'priority'), evidence_source: 'legacy_adapter' },
      ),
    ).toMatchObject({ reason: 'unsupported_usage' });
  });

  it('refuses an input total that overflows even when every individual bucket is safe', () => {
    expect(
      resolveConditionalPrice({
        catalog: catalog(),
        provider: 'anthropic',
        model_id: 'model/one',
        facts: facts(Number.MAX_SAFE_INTEGER),
        usage: { ...usage(Number.MAX_SAFE_INTEGER), cacheReadInputTokens: 1 },
        pricing_time: at,
      }),
    ).toMatchObject({ reason: 'input_count_mismatch' });
  });

  const project = (rules: CatalogPriceRule[]) =>
    projectConditionalPriceToV1(catalog(rules), 'anthropic', 'model/one', 'org-1', at);
  const bands = (): [CatalogPriceRule, CatalogPriceRule] => [
    rule({
      conditions: { service_tier: 'standard', request_input_tokens: { min: 0, max: 100001 } },
    }),
    rule({
      rule_id: 'high',
      conditions: { service_tier: 'standard', request_input_tokens: { min: 100001, max: null } },
    }),
  ];
  it('projects identical adjacent bands regardless of their order, but refuses changing rates', () => {
    const [low, high] = bands();
    expect(project([high, low])).toEqual({
      inputPerToken: 0.000002,
      outputPerToken: 0.000008,
      cacheReadPerToken: expect.closeTo(0.0000002, 18),
      cacheWritePerToken: 0.0000025,
    });
    expect(
      project([low, { ...high, rates: { ...high.rates, output_per_million_tokens: 9 } }]),
    ).toBeNull();
  });

  it.each([
    ['cache read', { ...rule().rates, cache_read_per_million_tokens: null }],
    ['cache write', { ...rule().rates, cache_write_per_million_tokens: null }],
    ['1h cache write', { ...rule().rates, cache_write_1h_per_million_tokens: null }],
    ['different 1h cache semantics', { ...rule().rates, cache_write_1h_per_million_tokens: 5 }],
  ] as const)('refuses a v1 projection with unsupported %s', (_name, rates) => {
    expect(project([rule({ rates })])).toBeNull();
  });

  it('refuses v1 projection without a matching model or reviewed cache profile', () => {
    expect(project([])).toBeNull();
    expect(project([rule({ model_id: 'another-model' })])).toBeNull();
    expect(project([rule({ usage_profile: 'exclusive-v1' })])).toBeNull();
  });
});
