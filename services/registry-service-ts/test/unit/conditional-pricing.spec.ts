// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import Fastify from 'fastify';
import { describe, it, expect, vi } from 'vitest';
import { resolveConditionalPrice } from '@orca/harness-catalog';
import {
  InMemoryPricingCatalogStore,
  PricingConflictError,
} from '../../src/pricing/catalog-store.js';
import { publishOperatorRuleSet, retireOperatorRuleSet } from '../../src/pricing/rule-sets.js';
import { PriceRuleSetCreateSchema } from '../../src/contracts/model-price-rules.contract.js';
import { createConditionalPriceRefresher } from '../../src/pricing/conditional-refresher.js';
import type { PriceSourceResponse } from '../../src/pricing/source-transport.js';
import {
  PriceSourceConfigSchema,
  parsePriceSources,
  parseSourcePage,
} from '../../src/pricing/provider-sources.js';
import { registerModelPriceRulesRoutes } from '../../src/api/model-price-rules.routes.js';

const now = new Date('2026-10-29T00:00:00Z');
const rates = {
  input_per_million_tokens: 2,
  output_per_million_tokens: 8,
  cache_read_per_million_tokens: 0.2,
  cache_write_per_million_tokens: 2.5,
  cache_write_1h_per_million_tokens: 4,
};
const rule = {
  conditions: { service_tier: 'standard', request_input_tokens: { min: 0, max: null } },
  rates,
};
const body = (extra = {}) =>
  PriceRuleSetCreateSchema.parse({
    provider: 'anthropic',
    model_id: 'model/one',
    usage_profile: 'anthropic-cache-v1',
    profile_version: 'anthropic-v1',
    provider_effective_at: null,
    rules: [rule],
    ...extra,
  });
const source = (extra = {}) =>
  PriceSourceConfigSchema.parse({
    id: 'test',
    provider: 'anthropic',
    adapter: 'normalized-v1',
    url: 'https://prices.example/catalog',
    ...extra,
  });
const model = (id = 'model/one', rules: unknown[] = [rule]) => ({
  model_id: id,
  profile_version: 'anthropic-v1',
  usage_profile: 'anthropic-cache-v1',
  provider_effective_at: null,
  rules,
});
const feed = (models: unknown[] = [model()], extra = {}) =>
  JSON.stringify({
    schema_version: 1,
    price_scope: 'public_list',
    provider: 'anthropic',
    currency: 'USD',
    unit: 'per_million_tokens',
    complete: true,
    upstream_document_sha256: 'a'.repeat(64),
    models,
    withdrawn_models: [],
    next_page: null,
    ...extra,
  });
const response = (text: string, status = 200) => ({
  status,
  body: text,
  etag: 'etag-1',
  last_modified: null,
});

describe('immutable pricing publications', () => {
  it('keeps dormant publications separate, guards activation, and refuses stale revisions', async () => {
    const store = new InMemoryPricingCatalogStore();
    await expect(
      store.publish('org', now, (doc) =>
        publishOperatorRuleSet(doc, 'org', body({ activate: true }), now, false),
      ),
    ).rejects.toThrow(/disabled/);
    const set = await store.publish('org', now, (doc) =>
      publishOperatorRuleSet(doc, 'org', body(), now, false),
    );
    expect((await store.read('org')).conditional_models).toEqual([]);
    await expect(
      store.publish('org', now, (doc) =>
        publishOperatorRuleSet(doc, 'org', body(), now, false, set.id, 99),
      ),
    ).rejects.toThrow(/stale/);
    expect((await store.read('org')).generation).toBe(1);
    await expect(
      store.publish('org', now, (doc) => {
        doc.rules[0]!.rates.input_per_million_tokens = 3;
      }),
    ).rejects.toThrow(/different content/);
  });
  it('switches complete scheduled sets at equality and retains old snapshot contents', async () => {
    const store = new InMemoryPricingCatalogStore();
    const set = await store.publish('org', now, (doc) =>
      publishOperatorRuleSet(doc, 'org', body({ activate: true }), now, true),
    );
    const original = await store.read('org');
    const later = new Date(now.getTime() + 3600000);
    const revised = await store.publish('org', now, (doc) =>
      publishOperatorRuleSet(
        doc,
        'org',
        body({
          provider_effective_at: later.toISOString(),
          rules: [{ ...rule, rates: { ...rates, input_per_million_tokens: 4 } }],
        }),
        now,
        true,
        set.id,
        1,
      ),
    );
    const doc = await store.read('org');
    const resolve = (at: Date) =>
      resolveConditionalPrice({
        catalog: doc,
        organization_id: 'org',
        provider: 'anthropic',
        model_id: 'model/one',
        facts: {
          version: 1,
          request_id: 'response',
          service_tier: 'standard',
          request_input_tokens: 10,
          usage_profile: 'anthropic-cache-v1',
          evidence_source: 'provider_response',
        },
        usage: {
          inputTokens: 10,
          outputTokens: 0,
          cacheReadInputTokens: 0,
          cacheCreationEphemeral5mInputTokens: 0,
          cacheCreationEphemeral1hInputTokens: 0,
        },
        pricing_time: at.toISOString(),
      });
    expect(resolve(now)).toMatchObject({ status: 'priced', cost_nano_usd: 20000n });
    expect(resolve(later)).toMatchObject({ status: 'priced', cost_nano_usd: 40000n });
    expect(original.rules).toHaveLength(1);
    expect(original.rules[0]!.retired_at).toBeNull();
    await store.publish('org', now, (draft) =>
      retireOperatorRuleSet(draft, set.id, revised.revision, now),
    );
    expect((await store.read('org')).rules).toEqual([]);
    expect((await store.read('org')).conditional_models).toEqual([
      { provider: 'anthropic', model_id: 'model/one' },
    ]);
  });
  it('fences old replica publications and binds stable snapshots to workspace and organization', async () => {
    const store = new InMemoryPricingCatalogStore();
    const first = (await store.acquireLease('anthropic', 'test', now, 1000))!;
    expect(await store.acquireLease('anthropic', 'test', now, 1000)).toBeNull();
    const later = new Date(now.getTime() + 1001);
    const second = (await store.acquireLease('anthropic', 'test', later, 1000))!;
    await expect(store.publish('', later, () => {}, first)).rejects.toBeInstanceOf(
      PricingConflictError,
    );
    await store.publish('', later, () => {}, second);
    const snap = await store.snapshot('ws', 'org', later);
    await store.publish('org', later, (doc) =>
      publishOperatorRuleSet(doc, 'org', body(), later, false),
    );
    expect((await store.fetchSnapshot('ws', 'org', snap.catalog_id, later)).rules).toEqual([]);
    await expect(store.fetchSnapshot('other', 'org', snap.catalog_id, later)).rejects.toThrow(
      /expired/,
    );
    await expect(store.fetchSnapshot('ws', 'other', snap.catalog_id, later)).rejects.toThrow(
      /expired/,
    );
    await expect(
      store.fetchSnapshot('ws', 'org', snap.catalog_id, new Date(later.getTime() + 3600000)),
    ).rejects.toThrow(/expired/);
  });
  it('keeps all pages on one immutable generation during concurrent publication', async () => {
    const store = new InMemoryPricingCatalogStore();
    await store.publish('org', now, (doc) =>
      publishOperatorRuleSet(
        doc,
        'org',
        body({
          rules: [rule, { ...rule, conditions: { ...rule.conditions, service_tier: 'priority' } }],
        }),
        now,
        false,
      ),
    );
    const app = Fastify();
    app.addHook('preHandler', async (req) => {
      req.auth = { workspaceId: 'ws' } as NonNullable<typeof req.auth>;
    });
    registerModelPriceRulesRoutes(
      app,
      store,
      async () => 'org',
      () => now,
    );
    try {
      const first = (
        await app.inject('/apis/pricing.runorca.ai/v2/modelpricerules?limit=1')
      ).json();
      await store.publish('org', now, (doc) => {
        doc.rules = [];
      });
      const second = (
        await app.inject(`/apis/pricing.runorca.ai/v2/modelpricerules?page=${first.next_page}`)
      ).json();
      expect(second.catalog_id).toBe(first.catalog_id);
      expect(second.data).toHaveLength(1);
      expect(second.next_page).toBeNull();
      expect(
        (await app.inject('/apis/pricing.runorca.ai/v2/modelpricerules?limit=0')).statusCode,
      ).toBe(400);
      expect(
        (await app.inject('/apis/pricing.runorca.ai/v2/modelpricerules?page=bad')).statusCode,
      ).toBe(400);
    } finally {
      await app.close();
    }
  });
});

describe('bounded provider refresh', () => {
  it('requires an expected source owner for cutover and fences an older approved publisher', async () => {
    const store = new InMemoryPricingCatalogStore();
    const payload = feed();
    const digest = createHash('sha256').update(payload).digest('hex');
    const first = source({ approved_change_digests: [digest] });
    const second = source({ id: 'replacement', approved_change_digests: [digest] });
    const refresh = (config: ReturnType<typeof source>) =>
      createConditionalPriceRefresher({
        store,
        sources: [config],
        now: () => now,
        request: async () => response(payload),
      }).refreshNow(config.id);
    expect(await refresh(first)).toBe('accepted');
    expect(await refresh(second)).toBe('failed');
    second.replaces_source_id = first.id;
    expect(await refresh(second)).toBe('accepted');
    expect((await store.read('')).rules[0]!.provenance.source_id).toBe(second.id);
    expect(await refresh(first)).toBe('failed');
    expect((await store.read('')).upstream_sources.anthropic).toBe(second.id);
  });
  it('parses recorded official tables without importing unsupported cache lifetimes', () => {
    const openai = source({
      id: 'openai',
      provider: 'openai',
      adapter: 'openai-public-v1',
      url: 'https://developers.openai.com/api/docs/pricing',
    });
    const anthropic = source({
      adapter: 'anthropic-public-v1',
      url: 'https://platform.claude.com/docs/en/about-claude/pricing',
    });
    const read = (name: string) =>
      readFileSync(new URL(`../fixtures/pricing/${name}-public-v1.md`, import.meta.url), 'utf8');
    const models = parseSourcePage(read('openai'), openai, 'a'.repeat(64)).models;
    const sol = models.find((m) => m.model_id === 'gpt-6.1-sol')!.value!;
    expect(sol.rules).toHaveLength(10);
    expect(
      sol.rules.find(
        (r) =>
          r.conditions.service_tier === 'priority' &&
          r.conditions.request_input_tokens.min === 272001,
      )?.rates,
    ).toMatchObject({
      input_per_million_tokens: 8,
      output_per_million_tokens: 30,
      cache_write_per_million_tokens: null,
    });
    expect(
      parseSourcePage(read('anthropic'), anthropic, 'a'.repeat(64)).models.find(
        (m) => m.model_id === 'claude-sonnet-4-6',
      )!.value!.rules[0]!.rates,
    ).toMatchObject({ input_per_million_tokens: 3, cache_write_1h_per_million_tokens: 6 });
    expect(() =>
      parseSourcePage(read('openai').replace('≤272K', '≤200K'), openai, 'a'.repeat(64)),
    ).toThrow(/semantics/);
    expect(() =>
      parseSourcePage(
        read('anthropic').replace('Base input tokens', 'Input'),
        anthropic,
        'a'.repeat(64),
      ),
    ).toThrow(/table/);
  });
  it('finishes every page before entering the accounting-shared publication lock', async () => {
    const store = new InMemoryPricingCatalogStore();
    const publish = vi.spyOn(store, 'publish');
    let finishPage!: (value: PriceSourceResponse) => void;
    const lastPage = new Promise<PriceSourceResponse>((resolve) => {
      finishPage = resolve;
    });
    const request = vi
      .fn()
      .mockResolvedValueOnce(
        response(feed([model()], { next_page: 'https://prices.example/page2' })),
      )
      .mockImplementationOnce(() => lastPage);
    const refresher = createConditionalPriceRefresher({
      store,
      sources: [source()],
      now: () => now,
      request,
    });
    const pending = refresher.refreshNow('test');
    await vi.waitFor(() => expect(request).toHaveBeenCalledTimes(2));
    expect(publish).not.toHaveBeenCalled();
    expect((await store.read('')).rules).toEqual([]);
    finishPage(response(feed([model('model/two')])));
    expect(await pending).toBe('accepted');
    expect(publish).toHaveBeenCalledTimes(1);
    expect((await store.read('')).rule_sets.map((set) => set.model_id).sort()).toEqual([
      'model/one',
      'model/two',
    ]);
  });

  it('uses one deadline across pages rather than restarting it for each response', async () => {
    vi.useFakeTimers();
    const store = new InMemoryPricingCatalogStore();
    const signals: AbortSignal[] = [];
    const request = vi.fn((url: string, { signal }: { signal: AbortSignal }) => {
      signals.push(signal);
      return new Promise<PriceSourceResponse>((resolve, reject) => {
        const abort = () => {
          clearTimeout(timer);
          reject(new Error('fetch aborted'));
        };
        const timer = setTimeout(() => {
          signal.removeEventListener('abort', abort);
          resolve(
            response(
              feed([model(url.endsWith('/page2') ? 'model/two' : 'model/one')], {
                next_page: url.endsWith('/page2') ? null : 'https://prices.example/page2',
              }),
            ),
          );
        }, 60);
        signal.addEventListener('abort', abort, { once: true });
      });
    });
    const refresher = createConditionalPriceRefresher({
      store,
      sources: [source({ timeout_ms: 100 })],
      now: () => now,
      request,
    });
    try {
      const pending = refresher.refreshNow('test');
      await vi.advanceTimersByTimeAsync(60);
      expect(request).toHaveBeenCalledTimes(2);
      expect(signals[1]).toBe(signals[0]);
      expect(signals[1]!.aborted).toBe(false);
      await vi.advanceTimersByTimeAsync(40);
      expect(await pending).toBe('failed');
      expect(signals[1]!.aborted).toBe(true);
      expect((await store.read('')).rules).toEqual([]);
    } finally {
      refresher.stop();
      vi.useRealTimers();
    }
  });

  it('quarantines whole bad model sets without renewing them while accepting healthy models', async () => {
    let clock = now;
    const store = new InMemoryPricingCatalogStore();
    let payload = feed([model(), model('model/two')]);
    const request = vi.fn(async () => response(payload));
    const refresher = createConditionalPriceRefresher({
      store,
      sources: [source()],
      now: () => clock,
      request,
    });
    expect(await refresher.refreshNow('test')).toBe('accepted');
    const old = (await store.read('')).rules.find((r) => r.model_id === 'model/one')!;
    clock = new Date(now.getTime() + 3600000);
    payload = feed([
      model('model/one', [{ ...rule, conditions: { ...rule.conditions, unsupported: true } }]),
      model('model/two'),
    ]);
    expect(await refresher.refreshNow('test')).toBe('accepted');
    const doc = await store.read('');
    expect(doc.rules.find((r) => r.model_id === 'model/one')!.valid_until).toBe(old.valid_until);
    expect(doc.rules.find((r) => r.model_id === 'model/two')!.valid_until).not.toBe(
      old.valid_until,
    );
    expect(doc.refresh.test!.quarantined_models).toEqual(['model/one']);
    request.mockImplementation(async () => response('', 304));
    expect(await refresher.refreshNow('test')).toBe('failed');
    expect((await store.read('')).rules.find((r) => r.model_id === 'model/one')!.valid_until).toBe(
      old.valid_until,
    );
  });
  it('revalidates only a complete 304 baseline, preserving immutable rule revisions', async () => {
    const store = new InMemoryPricingCatalogStore();
    let clock = now;
    const request = vi.fn(async () => response(feed()));
    const refresher = createConditionalPriceRefresher({
      store,
      sources: [source()],
      request,
      now: () => clock,
    });
    await refresher.refreshNow('test');
    const before = (await store.read('')).rules[0]!;
    clock = new Date(now.getTime() + 10000);
    request.mockImplementation(async () => response('', 304));
    expect(await refresher.refreshNow('test')).toBe('accepted');
    const after = (await store.read('')).rules[0]!;
    expect(after.rule_id).toBe(before.rule_id);
    expect(after.revision).toBe(before.revision);
    expect(after.valid_until).not.toBe(before.valid_until);
  });
  it('guards rate and partition changes; approved complete snapshots alone withdraw models', async () => {
    const store = new InMemoryPricingCatalogStore();
    let payload = feed([model(), model('model/two')]);
    const config = source();
    const refresher = createConditionalPriceRefresher({
      store,
      sources: [config],
      now: () => now,
      request: async () => response(payload),
    });
    await refresher.refreshNow('test');
    payload = feed([
      model('model/one', [{ ...rule, rates: { ...rates, input_per_million_tokens: 21 } }]),
    ]);
    expect(await refresher.refreshNow('test')).toBe('failed');
    expect((await store.read('')).rule_sets).toHaveLength(2);
    config.approved_change_digests.push(createHash('sha256').update(payload).digest('hex'));
    expect(await refresher.refreshNow('test')).toBe('accepted');
    expect((await store.read('')).rule_sets).toHaveLength(1);
    payload = feed([], { complete: false });
    expect(await refresher.refreshNow('test')).toBe('failed');
    expect((await store.read('')).rule_sets).toHaveLength(1);
  });
  it('fails whole refreshes on pagination loss, version changes and resource limits', async () => {
    for (const config of [
      source({ max_rules: 1 }),
      source({ max_pages: 1 }),
      source({ max_bytes: 1024 }),
    ]) {
      const store = new InMemoryPricingCatalogStore();
      const payload = feed([model(), model('model/two')], {
        next_page: 'https://prices.example/page2',
      });
      const refresher = createConditionalPriceRefresher({
        store,
        sources: [config],
        request: async () =>
          response(payload + (config.max_bytes === 1024 ? ' '.repeat(1024) : '')),
        now: () => now,
      });
      expect(await refresher.refreshNow('test')).toBe('failed');
      expect((await store.read('')).rules).toEqual([]);
    }
    expect(() => parsePriceSources(JSON.stringify([source()]), 'https://legacy.example')).toThrow(
      /cannot/,
    );
    expect(() =>
      parsePriceSources(JSON.stringify([source(), source({ id: 'second' })]), undefined),
    ).toThrow(/one source/);
  });
});
