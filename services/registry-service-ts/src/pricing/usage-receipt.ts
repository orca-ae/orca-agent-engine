// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { eq, sql } from 'drizzle-orm';
import { alias } from 'drizzle-orm/pg-core';
import { Counter } from 'prom-client';
import { registry } from '../metrics.js';
import {
  resolveConditionalPrice,
  type PricingCatalog,
  type RequestPriceFacts,
} from '@orca/harness-catalog';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import { pricingCatalogHeads, workspaces } from '../persistence/postgres/schema.js';
import { emptyPriceCatalog, type CatalogDocument } from './catalog-store.js';
import { priceUsageDelta, type UsageDelta } from '../domain/usage-accounting.js';

const failures = new Counter({
  name: 'registry_service_conditional_price_resolution_failures_total',
  help: 'Conditional pricing failures by bounded reason and provider.',
  labelNames: ['reason', 'provider'],
  registers: [registry],
});

export interface UsagePricingContext {
  provider: string;
  modelId: string;
  facts: RequestPriceFacts | null;
}
export interface UsagePriceReceipt {
  version: 1;
  provider: string;
  model_id: string;
  facts: RequestPriceFacts | null;
  usage: UsageDelta;
  pricing_time: string;
  global_catalog_id: string | null;
  org_catalog_id: string | null;
  /** Opaque compatibility identifier; query the explicit scope IDs instead of parsing this. */
  catalog_id: string | null;
  rule_id: string | null;
  rule_revision: number | null;
  rates: unknown;
  cost_nano_usd: string | null;
  reason: string | null;
  price_basis: 'api_equivalent_estimate';
}

export async function loadWorkspacePricingCatalog(
  db: Pick<DbClient, 'select'>,
  workspaceId: string,
) {
  const global = alias(pricingCatalogHeads, 'global_pricing');
  const operator = alias(pricingCatalogHeads, 'operator_pricing');
  const [row] = await db
    .select({
      organizationId: workspaces.organizationId,
      global: global.document,
      operator: operator.document,
    })
    .from(workspaces)
    .leftJoin(global, eq(global.organizationId, ''))
    .leftJoin(operator, eq(operator.organizationId, workspaces.organizationId))
    .where(eq(workspaces.id, workspaceId))
    .limit(1);
  const globalDoc = (row?.global as CatalogDocument | null) ?? emptyPriceCatalog();
  const orgDoc = (row?.operator as CatalogDocument | null) ?? emptyPriceCatalog();
  return { organizationId: row?.organizationId, globalDoc, orgDoc };
}

/** Called after report deduplication, inside the authoritative accounting transaction. */
export async function priceUsageReceipt(
  db: DbTransaction,
  workspaceId: string,
  context: UsagePricingContext,
  usage: UsageDelta,
  clock: () => Date,
) {
  // Publication takes the exclusive forms of these locks. Hold both scopes
  // through accounting commit so the clock cannot select a retired catalog
  // between the head read and first authoritative acceptance.
  await db.execute(sql`select pg_advisory_xact_lock_shared(hashtextextended('pricing:', 0))`);
  await db.execute(sql`select pg_advisory_xact_lock_shared(hashtextextended('pricing:' || organization_id, 0))
    from workspaces where id = ${workspaceId}`);
  // Legacy reports need the activation markers, not the potentially large
  // rule documents. Both reads share the publication locks above.
  const global = alias(pricingCatalogHeads, 'global_pricing');
  const operator = alias(pricingCatalogHeads, 'operator_pricing');
  const [head] = await db
    .select({
      organizationId: workspaces.organizationId,
      globalCatalogId: global.catalogId,
      orgCatalogId: operator.catalogId,
      globalModels: sql<
        CatalogDocument['conditional_models']
      >`coalesce(${global.document}->'conditional_models', '[]'::jsonb)`,
      orgModels: sql<
        CatalogDocument['conditional_models']
      >`coalesce(${operator.document}->'conditional_models', '[]'::jsonb)`,
    })
    .from(workspaces)
    .leftJoin(global, eq(global.organizationId, ''))
    .leftJoin(operator, eq(operator.organizationId, workspaces.organizationId))
    .where(eq(workspaces.id, workspaceId))
    .limit(1);
  const models = [...(head?.globalModels ?? []), ...(head?.orgModels ?? [])];
  const conditional = models.some(
    (model) => model.provider === context.provider && model.model_id === context.modelId,
  );
  const catalogs = conditional ? await loadWorkspacePricingCatalog(db, workspaceId) : null;
  const now = clock();
  const receipt: UsagePriceReceipt = {
    version: 1,
    provider: context.provider,
    model_id: context.modelId,
    facts: context.facts,
    usage,
    pricing_time: now.toISOString(),
    global_catalog_id: null,
    org_catalog_id: null,
    catalog_id: null,
    rule_id: null,
    rule_revision: null,
    rates: null,
    cost_nano_usd: null,
    reason: null,
    price_basis: 'api_equivalent_estimate',
  };
  if (catalogs === null) {
    const costNanoUsd = await priceUsageDelta(
      db as unknown as DbClient,
      workspaceId,
      context.provider,
      context.modelId,
      usage,
      models,
    );
    receipt.cost_nano_usd = costNanoUsd?.toString() ?? null;
    receipt.reason = costNanoUsd === null ? 'legacy_unpriced' : null;
    return { costNanoUsd, receipt };
  }
  const { globalDoc, orgDoc } = catalogs;
  // The receipt names the immutable scope catalogs, not a disposable fetch snapshot.
  receipt.global_catalog_id = head?.globalCatalogId || null;
  receipt.org_catalog_id = head?.orgCatalogId || null;
  const catalogId = JSON.stringify([head?.globalCatalogId ?? '', head?.orgCatalogId ?? '']);
  receipt.catalog_id = catalogId;
  // Resolution does not consume fetch digests, expiry or rule order. Avoid
  // sorting and hashing every rule while the accounting locks are held.
  const catalog: PricingCatalog = {
    catalog_id: catalogId,
    generation: globalDoc.generation + orgDoc.generation,
    published_at: new Date(
      Math.max(Date.parse(globalDoc.published_at), Date.parse(orgDoc.published_at)),
    ).toISOString(),
    required_features: [...new Set([...globalDoc.required_features, ...orgDoc.required_features])],
    rules: [...globalDoc.rules, ...orgDoc.rules],
  };
  const result = resolveConditionalPrice({
    catalog,
    ...(head?.organizationId ? { organization_id: head.organizationId } : {}),
    provider: context.provider,
    model_id: context.modelId,
    facts: context.facts,
    usage,
    pricing_time: receipt.pricing_time,
  });
  if (result.status === 'unpriced') {
    failures.inc({
      reason: result.reason,
      provider: ['openai', 'anthropic'].includes(context.provider) ? context.provider : 'other',
    });
    receipt.reason = result.reason;
    return { costNanoUsd: null, receipt };
  }
  receipt.rule_id = result.rule.rule_id;
  receipt.rule_revision = result.rule.revision;
  receipt.rates = result.rule.rates;
  receipt.cost_nano_usd = result.cost_nano_usd.toString();
  return { costNanoUsd: result.cost_nano_usd, receipt };
}
