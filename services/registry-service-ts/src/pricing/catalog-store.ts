// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { and, desc, eq, inArray, lt, notInArray, sql } from 'drizzle-orm';
import {
  PRICING_V2_FEATURES,
  type PricingCatalog,
  type PriceSource,
  type CatalogPriceRule,
} from '@orca/harness-catalog';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  pricingCatalogHeads,
  pricingCatalogVersions,
  pricingFetchSnapshots,
  pricingSourceLeases,
} from '../persistence/postgres/schema.js';
import { newId } from '../domain/versioning.js';

export interface StoredPriceRuleSet {
  id: string;
  revision: number;
  provider: string;
  model_id: string;
  source: PriceSource;
  organization_id: string;
  rule_ids: string[];
}
export interface PriceRefreshRecord {
  diff?: { added: string[]; changed: string[]; withdrawn: string[]; quarantined: string[] };
  source_id: string;
  provider: string;
  status: 'accepted' | 'quarantined' | 'failed';
  observed_at: string;
  accepted_at: string | null;
  document_sha256: string | null;
  etag: string | null;
  last_modified: string | null;
  quarantined_models: string[];
  /** Whether a 304 may attest every model in this exact source document. */
  complete: boolean;
  error: string | null;
}
export interface CatalogDocument extends PricingCatalog {
  upstream_sources: Record<string, string>;
  rules: CatalogPriceRule[];
  rule_sets: StoredPriceRuleSet[];
  conditional_models: { provider: string; model_id: string }[];
  refresh: Record<string, PriceRefreshRecord>;
}
export interface FetchCatalog extends PricingCatalog {
  rules: CatalogPriceRule[];
  digest: string;
  conditional_models: { provider: string; model_id: string }[];
  expires_at: string;
}
export interface PublicationFence {
  provider: string;
  source_id: string;
  token: number;
}
export class PricingConflictError extends Error {}
export class PricingCursorExpiredError extends Error {}

/** Stable structural encoding, also used to refuse same-identity content changes. */
export function priceDigest(value: unknown): string {
  function stable(v: unknown): unknown {
    if (Array.isArray(v)) return v.map(stable);
    if (v !== null && typeof v === 'object')
      return Object.fromEntries(
        Object.entries(v)
          .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
          .map(([k, x]) => [k, stable(x)]),
      );
    return v;
  }
  return createHash('sha256')
    .update(JSON.stringify(stable(value)))
    .digest('hex');
}
export function emptyPriceCatalog(): CatalogDocument {
  return {
    catalog_id: '',
    generation: 0,
    published_at: new Date(0).toISOString(),
    required_features: [...PRICING_V2_FEATURES],
    rules: [],
    rule_sets: [],
    conditional_models: [],
    refresh: {},
    upstream_sources: {},
  };
}

export interface PricingCatalogStore {
  read(organizationId: string): Promise<CatalogDocument>;
  /**
   * Serialize publication per scope; mutation, audit and snapshot commit together.
   * The change callback is synchronous. Fetch and parse complete pages before calling publish;
   * accounting shares this scope lock and must never wait for network I/O inside it.
   */
  publish<T>(
    organizationId: string,
    now: Date,
    change: (draft: CatalogDocument) => T,
    fence?: PublicationFence,
  ): Promise<T>;
  acquireLease(
    provider: string,
    sourceId: string,
    now: Date,
    durationMs: number,
  ): Promise<PublicationFence | null>;
  releaseLease(fence: PublicationFence, now: Date): Promise<void>;
  snapshot(workspaceId: string, organizationId: string, now: Date): Promise<FetchCatalog>;
  fetchSnapshot(
    workspaceId: string,
    organizationId: string,
    id: string,
    now: Date,
  ): Promise<FetchCatalog>;
}

function finishPublication(draft: CatalogDocument, previous: CatalogDocument, now: Date): void {
  if (!Number.isFinite(now.getTime()) || now.getTime() < Date.parse(previous.published_at))
    throw new PricingConflictError('publication clock precedes the active catalog');
  // Freshness may change in a new catalog, but a rule's immutable content may not.
  const bindingFields = ['active_from', 'retired_at', 'valid_until'];
  const immutable = (r: CatalogPriceRule) =>
    Object.fromEntries(Object.entries(r).filter(([k]) => !bindingFields.includes(k)));
  const seen = new Map<string, string>();
  for (const r of [...previous.rules, ...draft.rules]) {
    const key = JSON.stringify([r.organization_id ?? '', r.rule_id, r.revision]);
    const digest = priceDigest(immutable(r));
    if (seen.has(key) && seen.get(key) !== digest)
      throw new PricingConflictError('same rule revision has different content');
    seen.set(key, digest);
  }
  draft.catalog_id = newId('pcat');
  draft.rules = draft.rules.filter(
    (r) => r.retired_at === null || Date.parse(r.retired_at) > now.getTime(),
  );
  if (draft.rules.length > 10_000) throw new PricingConflictError('catalog rule budget exceeded');
  draft.generation = previous.generation + 1;
  draft.published_at = now.toISOString();
}

export function combinePriceCatalogs(
  global: CatalogDocument,
  org: CatalogDocument,
  now: Date,
): FetchCatalog {
  const content = {
    generation: global.generation + org.generation,
    published_at: new Date(
      Math.max(Date.parse(global.published_at), Date.parse(org.published_at)),
    ).toISOString(),
    required_features: [...new Set([...global.required_features, ...org.required_features])],
    rules: [...global.rules, ...org.rules].sort(
      (a, b) => a.rule_id.localeCompare(b.rule_id) || a.revision - b.revision,
    ),
    conditional_models: [
      ...new Map(
        [...global.conditional_models, ...org.conditional_models].map((m) => [
          JSON.stringify(m),
          m,
        ]),
      ).values(),
    ],
  };
  return {
    ...content,
    catalog_id: newId('pcat'),
    digest: priceDigest(content),
    expires_at: new Date(now.getTime() + 60 * 60_000).toISOString(),
  };
}

type CatalogDb = Pick<
  DbClient,
  'select' | 'insert' | 'delete' | 'update' | 'execute' | 'transaction'
>;
export function createPostgresPricingCatalogStore(db: CatalogDb): PricingCatalogStore {
  async function read(organizationId: string): Promise<CatalogDocument> {
    const [row] = await db
      .select()
      .from(pricingCatalogHeads)
      .where(eq(pricingCatalogHeads.organizationId, organizationId));
    return row ? structuredClone(row.document as CatalogDocument) : emptyPriceCatalog();
  }
  return {
    read,
    async publish(organizationId, now, change, fence) {
      return db.transaction(async (tx) => {
        // A lock exists even before the first head row, avoiding two first writers.
        await tx.execute(
          sql`select pg_advisory_xact_lock(hashtextextended(${`pricing:${organizationId}`}, 0))`,
        );
        if (fence) {
          const [lease] = await tx
            .select()
            .from(pricingSourceLeases)
            .where(
              and(
                eq(pricingSourceLeases.organizationId, ''),
                eq(pricingSourceLeases.provider, fence.provider),
                eq(pricingSourceLeases.sourceId, fence.source_id),
              ),
            )
            .for('update');
          if (!lease || lease.fence !== fence.token || lease.expiresAt <= now)
            throw new PricingConflictError('source publication lease expired');
        }
        const store = createPostgresPricingCatalogStore(tx);
        const previous = await store.read(organizationId);
        const draft = structuredClone(previous);
        const result = change(draft);
        finishPublication(draft, previous, now);
        await tx.insert(pricingCatalogVersions).values({
          organizationId,
          catalogId: draft.catalog_id,
          generation: draft.generation,
          document: draft,
          digest: priceDigest(draft),
          publishedAt: now,
        });
        await tx
          .insert(pricingCatalogHeads)
          .values({
            organizationId,
            catalogId: draft.catalog_id,
            generation: draft.generation,
            document: draft,
          })
          .onConflictDoUpdate({
            target: pricingCatalogHeads.organizationId,
            set: { catalogId: draft.catalog_id, generation: draft.generation, document: draft },
          });
        return result;
      });
    },
    async acquireLease(provider, sourceId, now, durationMs) {
      const [lease] = await db
        .insert(pricingSourceLeases)
        .values({
          organizationId: '',
          provider,
          sourceId,
          fence: 1,
          expiresAt: new Date(now.getTime() + durationMs),
        })
        .onConflictDoUpdate({
          target: [
            pricingSourceLeases.organizationId,
            pricingSourceLeases.provider,
            pricingSourceLeases.sourceId,
          ],
          set: {
            fence: sql`${pricingSourceLeases.fence} + 1`,
            expiresAt: new Date(now.getTime() + durationMs),
          },
          setWhere: lt(pricingSourceLeases.expiresAt, now),
        })
        .returning();
      return lease ? { provider, source_id: sourceId, token: lease.fence } : null;
    },
    async snapshot(workspaceId, organizationId, now) {
      return db.transaction(async (tx) => {
        await tx.execute(
          sql`select pg_advisory_xact_lock(hashtextextended(${JSON.stringify(['pricing-fetch', workspaceId, organizationId])}, 0))`,
        );
        const heads = await tx
          .select()
          .from(pricingCatalogHeads)
          .where(inArray(pricingCatalogHeads.organizationId, ['', organizationId]));
        const document = combinePriceCatalogs(
          (heads.find((h) => h.organizationId === '')?.document as CatalogDocument | undefined) ??
            emptyPriceCatalog(),
          (heads.find((h) => h.organizationId === organizationId)?.document as
            | CatalogDocument
            | undefined) ?? emptyPriceCatalog(),
          now,
        );
        await tx
          .delete(pricingFetchSnapshots)
          .where(
            and(
              eq(pricingFetchSnapshots.workspaceId, workspaceId),
              eq(pricingFetchSnapshots.organizationId, organizationId),
              lt(pricingFetchSnapshots.expiresAt, now),
            ),
          );
        await tx.insert(pricingFetchSnapshots).values({
          workspaceId,
          organizationId,
          catalogId: document.catalog_id,
          document,
          expiresAt: new Date(document.expires_at),
        });
        const keep = await tx
          .select({ id: pricingFetchSnapshots.catalogId })
          .from(pricingFetchSnapshots)
          .where(
            and(
              eq(pricingFetchSnapshots.workspaceId, workspaceId),
              eq(pricingFetchSnapshots.organizationId, organizationId),
            ),
          )
          .orderBy(desc(pricingFetchSnapshots.expiresAt), desc(pricingFetchSnapshots.catalogId))
          .limit(16);
        if (!keep.some((r) => r.id === document.catalog_id)) {
          keep.pop();
          keep.push({ id: document.catalog_id });
        }
        await tx.delete(pricingFetchSnapshots).where(
          and(
            eq(pricingFetchSnapshots.workspaceId, workspaceId),
            eq(pricingFetchSnapshots.organizationId, organizationId),
            notInArray(
              pricingFetchSnapshots.catalogId,
              keep.map((r) => r.id),
            ),
          ),
        );
        return document;
      });
    },
    async releaseLease(fence, now) {
      await db
        .update(pricingSourceLeases)
        .set({ expiresAt: new Date(now.getTime() - 1) })
        .where(
          and(
            eq(pricingSourceLeases.organizationId, ''),
            eq(pricingSourceLeases.provider, fence.provider),
            eq(pricingSourceLeases.sourceId, fence.source_id),
            eq(pricingSourceLeases.fence, fence.token),
          ),
        );
    },
    async fetchSnapshot(workspaceId, organizationId, id, now) {
      const [row] = await db
        .select()
        .from(pricingFetchSnapshots)
        .where(
          and(
            eq(pricingFetchSnapshots.workspaceId, workspaceId),
            eq(pricingFetchSnapshots.organizationId, organizationId),
            eq(pricingFetchSnapshots.catalogId, id),
          ),
        );
      if (!row || row.expiresAt <= now)
        throw new PricingCursorExpiredError('pricing snapshot expired; restart the complete fetch');
      return structuredClone(row.document as FetchCatalog);
    },
  };
}

export class InMemoryPricingCatalogStore implements PricingCatalogStore {
  private heads = new Map<string, CatalogDocument>();
  readonly history: CatalogDocument[] = [];
  private snapshots = new Map<string, FetchCatalog>();
  private leases = new Map<string, { fence: number; expires: number }>();
  async read(scope: string): Promise<CatalogDocument> {
    return structuredClone(this.heads.get(scope) ?? emptyPriceCatalog());
  }
  async publish<T>(
    scope: string,
    now: Date,
    change: (draft: CatalogDocument) => T,
    fence?: PublicationFence,
  ): Promise<T> {
    if (fence) {
      const lease = this.leases.get(JSON.stringify([fence.provider, fence.source_id]));
      if (!lease || lease.fence !== fence.token || lease.expires <= now.getTime())
        throw new PricingConflictError('source publication lease expired');
    }
    const previous = this.heads.get(scope) ?? emptyPriceCatalog();
    const draft = structuredClone(previous);
    const result = change(draft);
    finishPublication(draft, previous, now);
    this.heads.set(scope, draft);
    this.history.push(structuredClone(draft));
    return result;
  }
  async acquireLease(
    provider: string,
    sourceId: string,
    now: Date,
    durationMs: number,
  ): Promise<PublicationFence | null> {
    const key = JSON.stringify([provider, sourceId]);
    const prior = this.leases.get(key);
    if (prior && prior.expires >= now.getTime()) return null;
    const token = (prior?.fence ?? 0) + 1;
    this.leases.set(key, { fence: token, expires: now.getTime() + durationMs });
    return { provider, source_id: sourceId, token };
  }
  async snapshot(workspaceId: string, organizationId: string, now: Date): Promise<FetchCatalog> {
    const doc = combinePriceCatalogs(await this.read(''), await this.read(organizationId), now);
    for (const [key, value] of this.snapshots)
      if (Date.parse(value.expires_at) <= now.getTime()) this.snapshots.delete(key);
    this.snapshots.set(
      JSON.stringify([workspaceId, organizationId, doc.catalog_id]),
      structuredClone(doc),
    );
    const scoped = [...this.snapshots.keys()].filter((key) => {
      const [workspace, org] = JSON.parse(key) as string[];
      return workspace === workspaceId && org === organizationId;
    });
    for (const key of scoped.slice(0, -16)) this.snapshots.delete(key);
    return doc;
  }
  async releaseLease(fence: PublicationFence, now: Date): Promise<void> {
    const lease = this.leases.get(JSON.stringify([fence.provider, fence.source_id]));
    if (lease?.fence === fence.token) lease.expires = now.getTime() - 1;
  }
  async fetchSnapshot(
    workspaceId: string,
    organizationId: string,
    id: string,
    now: Date,
  ): Promise<FetchCatalog> {
    const doc = this.snapshots.get(JSON.stringify([workspaceId, organizationId, id]));
    if (!doc || Date.parse(doc.expires_at) <= now.getTime())
      throw new PricingCursorExpiredError('pricing snapshot expired; restart the complete fetch');
    return structuredClone(doc);
  }
}
