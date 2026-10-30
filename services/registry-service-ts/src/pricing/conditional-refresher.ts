// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { Counter, Gauge } from 'prom-client';
import {
  priceConditionKey,
  validatePriceRuleSet,
  type CatalogPriceRule,
} from '@orca/harness-catalog';
import { registry } from '../metrics.js';
import { newId } from '../domain/versioning.js';
import {
  priceDigest,
  type CatalogDocument,
  type PricingCatalogStore,
  type PriceRefreshRecord,
  type PublicationFence,
} from './catalog-store.js';
import { parseSourcePage, type PriceSourceConfig, type SourceModel } from './provider-sources.js';
import { requestPriceSource, type PriceSourceRequest } from './source-transport.js';

const refreshes = new Counter({
  name: 'registry_service_conditional_price_refresh_total',
  help: 'Conditional price source attempts by configured source and result.',
  labelNames: ['source', 'result'],
  registers: [registry],
});
const acceptedAt = new Gauge({
  name: 'registry_service_conditional_price_accepted_timestamp_seconds',
  help: 'Last accepted source observation; per-model validity remains authoritative.',
  labelNames: ['source'],
  registers: [registry],
});
const quarantines = new Counter({
  name: 'registry_service_conditional_price_quarantine_total',
  help: 'Quarantined model sets by configured source.',
  labelNames: ['source'],
  registers: [registry],
});
const activeRules = new Gauge({
  name: 'registry_service_conditional_price_rules',
  help: 'Accepted current rules by configured source.',
  labelNames: ['source'],
  registers: [registry],
});
const generation = new Gauge({
  name: 'registry_service_conditional_price_generation',
  help: 'Accepted catalog generation by configured source.',
  labelNames: ['source'],
  registers: [registry],
});

const sha = (text: string) => createHash('sha256').update(text).digest('hex');
function samePartition(old: CatalogPriceRule[], model: SourceModel): boolean {
  const keys = (rules: { conditions: CatalogPriceRule['conditions'] }[]) =>
    rules
      .map((r) => priceConditionKey(r.conditions))
      .sort()
      .join('\n');
  return (
    keys(old) === keys(model.rules) &&
    old.every(
      (r) => r.profile_version === model.profile_version && r.usage_profile === model.usage_profile,
    )
  );
}
function suspicious(old: CatalogPriceRule[], model: SourceModel): boolean {
  if (!samePartition(old, model)) return true;
  return model.rules.some((r) => {
    const before = old.find(
      (p) => priceConditionKey(p.conditions) === priceConditionKey(r.conditions),
    )!.rates;
    return Object.entries(r.rates).some(([key, value]) => {
      const prior = before[key as keyof typeof before];
      if (prior === value) return false;
      if (prior === null || value === null || prior === 0 || value === 0) return true;
      return value / prior > 10 || value / prior < 0.1;
    });
  });
}

/** Every decision changes a whole model set or retains it with its original expiry. */
export function applySourceSnapshot(
  doc: CatalogDocument,
  source: PriceSourceConfig,
  models: { model_id: string; value: SourceModel | null }[],
  withdrawn: string[],
  digest: string,
  upstreamDigest: string,
  now: Date,
): string[] {
  const approved = source.approved_change_digests.includes(digest);
  const owner = doc.upstream_sources[source.provider];
  if (owner && owner !== source.id && (!approved || source.replaces_source_id !== owner))
    throw new Error(
      'upstream source replacement requires an approved document and expected source owner',
    );
  if (models.length === 0 && !source.approve_empty)
    throw new Error('empty source requires approval');
  const ids = models.map((m) => m.model_id);
  if (
    new Set(ids).size !== ids.length ||
    new Set(withdrawn).size !== withdrawn.length ||
    withdrawn.some((id) => ids.includes(id))
  )
    throw new Error('duplicate source model identity');
  const prior = doc.rule_sets.filter(
    (s) => s.source === 'upstream' && s.provider === source.provider,
  );
  const quarantined: string[] = [];
  let valid = 0;
  const activeFrom = now.toISOString();
  for (const entry of models) {
    const model = entry.value;
    const set = prior.find((s) => s.model_id === entry.model_id);
    const old = doc.rules.filter((r) => r.rule_set_id === set?.id && r.retired_at === null);
    if (
      !model ||
      (old.length &&
        !approved &&
        (suspicious(old, model) || old.some((r) => r.provenance.source_id !== source.id)))
    ) {
      quarantined.push(entry.model_id);
      continue;
    }
    if (old.some((r) => Date.parse(r.active_from) > now.getTime())) {
      quarantined.push(entry.model_id);
      continue;
    }
    const setId = set?.id ?? newId('prs');
    const revision = (set?.revision ?? 0) + 1;
    const effective = new Date(
      Math.max(
        now.getTime(),
        model.provider_effective_at ? Date.parse(model.provider_effective_at) : 0,
      ),
    ).toISOString();
    const expires = new Date(now.getTime() + source.max_age_ms).toISOString();
    const sameModel =
      old.length === model.rules.length &&
      old.every((r) => r.provenance.source_id === source.id) &&
      samePartition(old, model) &&
      model.rules.every((r) =>
        old.some(
          (p) =>
            priceConditionKey(p.conditions) === priceConditionKey(r.conditions) &&
            priceDigest(p.rates) === priceDigest(r.rates) &&
            p.provider_effective_at === model.provider_effective_at,
        ),
      );
    const rules = model.rules.map((input): CatalogPriceRule => {
      const previous = old.find(
        (r) => priceConditionKey(r.conditions) === priceConditionKey(input.conditions),
      );
      const unchanged = sameModel && previous;
      if (unchanged) return { ...previous, valid_until: expires };
      return {
        ...input,
        rule_set_id: setId,
        rule_id: previous?.rule_id ?? newId('prule'),
        revision,
        provider: source.provider,
        model_id: model.model_id,
        source: 'upstream',
        usage_profile: model.usage_profile,
        profile_version: model.profile_version,
        provenance: {
          source_id: source.id,
          document_sha256: upstreamDigest,
          adapter_version: source.adapter,
          observed_at: activeFrom,
        },
        provider_effective_at: model.provider_effective_at,
        active_from: effective,
        retired_at: null,
        valid_until: expires,
      };
    });
    if (validatePriceRuleSet(rules)) {
      quarantined.push(entry.model_id);
      continue;
    }
    valid++;
    const unchanged = rules.every((r) =>
      old.some((p) => p.rule_id === r.rule_id && p.revision === r.revision),
    );
    if (unchanged) {
      doc.rules = doc.rules.map(
        (r) => rules.find((p) => p.rule_id === r.rule_id && p.revision === r.revision) ?? r,
      );
    } else {
      for (const rule of old) rule.retired_at = effective;
      doc.rules.push(...rules);
    }
    doc.rule_sets = [
      ...doc.rule_sets.filter((s) => s.id !== setId),
      {
        id: setId,
        revision: unchanged ? set!.revision : revision,
        provider: source.provider,
        model_id: model.model_id,
        source: 'upstream',
        organization_id: '',
        rule_ids: rules.map((r) => r.rule_id),
      },
    ];
  }
  if (models.length > 0 && valid === 0) throw new Error('all source model sets were quarantined');
  doc.upstream_sources[source.provider] = source.id;
  // This is called only after every complete page has been validated and bounded.
  for (const set of prior)
    if (!ids.includes(set.model_id) || withdrawn.includes(set.model_id)) {
      for (const rule of doc.rules)
        if (
          rule.rule_set_id === set.id &&
          (rule.retired_at === null || Date.parse(rule.retired_at) > now.getTime())
        )
          rule.retired_at = activeFrom;
      doc.rule_sets = doc.rule_sets.filter((s) => s.id !== set.id);
    }
  return quarantined;
}

export function createConditionalPriceRefresher(options: {
  store: PricingCatalogStore;
  sources: PriceSourceConfig[];
  request?: PriceSourceRequest;
  now?: () => Date;
  onFailure?: (sourceId: string, message: string) => void;
}) {
  const now = options.now ?? (() => new Date());
  const request = options.request ?? requestPriceSource;
  const inFlight = new Set<string>();
  const timers = new Map<string, ReturnType<typeof setTimeout>>();
  const controllers = new Set<AbortController>();
  let stopped = true;
  async function refreshNow(id: string): Promise<'accepted' | 'failed' | 'skipped'> {
    const source = options.sources.find((s) => s.id === id);
    if (!source || inFlight.has(id) || inFlight.size >= 2) return 'skipped';
    inFlight.add(id);
    const controller = new AbortController();
    controllers.add(controller);
    // One deadline covers every page through its complete response body, not just connection time.
    const timeout = setTimeout(() => controller.abort(), source.timeout_ms);
    timeout.unref();
    let lease: PublicationFence | null = null;
    try {
      lease = await options.store.acquireLease(
        source.provider,
        id,
        now(),
        source.timeout_ms + 5000,
      );
      if (!lease) return 'skipped';
      const prior = (await options.store.read('')).refresh[id];
      const headers: Record<string, string> = {
        accept: source.adapter === 'normalized-v1' ? 'application/json' : 'text/markdown',
      };
      if (prior?.complete && prior.status === 'accepted') {
        if (prior.etag) headers['if-none-match'] = prior.etag;
        if (prior.last_modified) headers['if-modified-since'] = prior.last_modified;
      }
      let url: string | null = source.url,
        bytes = 0,
        ruleCount = 0;
      const pages = new Set<string>();
      const models: { model_id: string; value: SourceModel | null }[] = [],
        withdrawn: string[] = [];
      const digests: string[] = [],
        upstream: string[] = [];
      let etag: string | null = null,
        modified: string | null = null;
      while (url !== null) {
        if (
          pages.size >= source.max_pages ||
          pages.has(url) ||
          new URL(url).origin !== new URL(source.url).origin ||
          new URL(url).username ||
          new URL(url).password
        )
          throw new Error('source pagination refused');
        pages.add(url);
        const response = await request(url, {
          headers: pages.size === 1 ? headers : { accept: headers.accept! },
          signal: controller.signal,
          maxBytes: source.max_bytes - bytes,
        });
        if (response.status === 304) {
          if (
            pages.size !== 1 ||
            !prior?.complete ||
            prior.status !== 'accepted' ||
            (!headers['if-none-match'] && !headers['if-modified-since'])
          )
            throw new Error('304 without a complete accepted baseline');
          const observed = now();
          await options.store.publish(
            '',
            observed,
            (doc) => {
              if (
                doc.refresh[id]?.document_sha256 !== prior.document_sha256 ||
                !doc.refresh[id]?.complete
              )
                throw new Error('304 baseline changed');
              for (const rule of doc.rules)
                if (rule.provenance.source_id === id && rule.retired_at === null)
                  rule.valid_until = new Date(observed.getTime() + source.max_age_ms).toISOString();
              doc.refresh[id] = {
                ...prior,
                observed_at: observed.toISOString(),
                accepted_at: observed.toISOString(),
              };
            },
            lease,
          );
          acceptedAt.set({ source: id }, observed.getTime() / 1000);
          refreshes.inc({ source: id, result: 'accepted' });
          return 'accepted';
        }
        if (response.status !== 200) throw new Error('source HTTP response refused');
        bytes += Buffer.byteLength(response.body);
        if (bytes > source.max_bytes) throw new Error('source byte limit exceeded');
        const digest = sha(response.body);
        const page = parseSourcePage(response.body, source, digest);
        models.push(...page.models);
        withdrawn.push(...page.withdrawn_models);
        ruleCount += page.rule_count;
        if (ruleCount > source.max_rules || models.length > source.max_rules)
          throw new Error('source rule limit exceeded');
        digests.push(digest);
        upstream.push(page.upstream_digest);
        if (pages.size === 1) {
          etag = response.etag;
          modified = response.last_modified;
        }
        url = page.next_page;
      }
      const observed = now(),
        digest = digests.length === 1 ? digests[0]! : priceDigest(digests);
      if (new Set(upstream).size !== 1) throw new Error('source pages changed document version');
      // All network reads and page parsing are complete before taking the publication scope lock.
      const publication = await options.store.publish(
        '',
        observed,
        (doc) => {
          const priorModels = doc.rule_sets
            .filter((s) => s.source === 'upstream' && s.provider === source.provider)
            .map((s) => s.model_id);
          const quarantine = applySourceSnapshot(
            doc,
            source,
            models,
            withdrawn,
            digest,
            upstream.length === 1 ? upstream[0]! : priceDigest(upstream),
            observed,
          );
          doc.refresh[id] = {
            source_id: id,
            provider: source.provider,
            status: quarantine.length ? 'quarantined' : 'accepted',
            observed_at: observed.toISOString(),
            accepted_at: observed.toISOString(),
            document_sha256: digest,
            etag,
            last_modified: modified,
            quarantined_models: quarantine,
            complete: quarantine.length === 0 && pages.size === 1,
            error: null,
          };
          const current = doc.rule_sets
            .filter((s) => s.source === 'upstream' && s.provider === source.provider)
            .map((s) => s.model_id);
          doc.refresh[id]!.diff = {
            added: current.filter((m) => !priorModels.includes(m)),
            changed: current.filter((m) => priorModels.includes(m) && !quarantine.includes(m)),
            withdrawn: priorModels.filter((m) => !current.includes(m)),
            quarantined: quarantine,
          };
          return {
            quarantined: quarantine.length,
            rules: doc.rules.filter((r) => r.provenance.source_id === id && r.retired_at === null)
              .length,
            generation: doc.generation + 1,
          };
        },
        lease,
      );
      quarantines.inc({ source: id }, publication.quarantined);
      activeRules.set({ source: id }, publication.rules);
      generation.set({ source: id }, publication.generation);
      acceptedAt.set({ source: id }, observed.getTime() / 1000);
      refreshes.inc({ source: id, result: 'accepted' });
      return 'accepted';
    } catch {
      const message = 'price source refresh failed; previous model validity is unchanged';
      options.onFailure?.(id, message);
      refreshes.inc({ source: id, result: 'failed' });
      // Failure evidence is a new immutable generation, not a renewed tariff.
      if (lease)
        await options.store
          .publish(
            '',
            now(),
            (doc) => {
              const prior = doc.refresh[id];
              doc.refresh[id] = {
                source_id: id,
                provider: source.provider,
                status: 'failed',
                observed_at: now().toISOString(),
                accepted_at: prior?.accepted_at ?? null,
                document_sha256: prior?.document_sha256 ?? null,
                etag: null,
                last_modified: null,
                quarantined_models: prior?.quarantined_models ?? [],
                complete: false,
                error: message,
              } satisfies PriceRefreshRecord;
            },
            lease,
          )
          .catch(() => undefined);
      return 'failed';
    } finally {
      if (lease) await options.store.releaseLease(lease, now()).catch(() => undefined);
      clearTimeout(timeout);
      inFlight.delete(id);
      controllers.delete(controller);
    }
  }
  function schedule(source: PriceSourceConfig, failures = 0, delay = 0): void {
    const timer = setTimeout(() => {
      void refreshNow(source.id).then((result) => {
        if (stopped) return;
        const n = result === 'failed' ? failures + 1 : 0;
        const wait =
          result === 'skipped'
            ? 1000
            : n
              ? Math.min(source.interval_ms, 1000 * 2 ** Math.min(n, 10)) *
                (0.75 + Math.random() * 0.5)
              : source.interval_ms;
        schedule(source, n, wait);
      });
    }, delay);
    timer.unref();
    timers.set(source.id, timer);
  }
  return {
    refreshNow,
    start() {
      if (!stopped) return;
      stopped = false;
      options.sources.forEach((s) => schedule(s));
    },
    stop() {
      stopped = true;
      timers.forEach(clearTimeout);
      timers.clear();
      controllers.forEach((c) => c.abort());
    },
  };
}
