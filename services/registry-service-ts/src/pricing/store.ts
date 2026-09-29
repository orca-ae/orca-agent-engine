// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Where model prices live, and how a model id becomes a rate.
 *
 * Prices are stored in Postgres rather than cached per process, so every
 * replica resolves identically and a restart keeps the last successful rows (a
 * configured refresher still fetches once at startup). Precedence and
 * family fallback are not implemented here: `@orca/harness-catalog` owns them,
 * and this module's whole job is to hand it every candidate row and pass the
 * answer back.
 *
 * See `docs/managed-agents/pricing.md`.
 */

import { isNull, and, eq, notInArray, or, sql } from 'drizzle-orm';
import {
  SEED_MODEL_PRICES,
  SEED_PRICE_PROVIDER,
  resolveModelPricing,
  type ModelPriceEntry,
  type ModelPricing,
  type PriceSource,
} from '@orca/harness-catalog';
import type { DbClient } from '../persistence/postgres/client.js';
import { modelPrices } from '../persistence/postgres/schema.js';

/** Empty is reserved for deployment-global seed/upstream rows. */
export const GLOBAL_MODEL_PRICE_SCOPE = '';

/** One stored row: a single source's entry for a single model. */
export interface StoredModelPrice {
  provider: string;
  organizationId: string;
  modelId: string;
  source: PriceSource;
  inputPerMillionTokens: number;
  outputPerMillionTokens: number;
  /**
   * `null` means the source published no cache rate, which is not the same as
   * publishing zero — the engine derives an absent rate from the input rate
   * rather than billing the bucket at nothing.
   */
  cacheReadPerMillionTokens: number | null;
  cacheWritePerMillionTokens: number | null;
  /** When the upstream catalog carrying this row was fetched; `null` otherwise. */
  fetchedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

/** The mutable half of a row. The source is a parameter of the write, never a field of it. */
export interface ModelPriceWrite {
  provider: string;
  modelId: string;
  inputPerMillionTokens: number;
  outputPerMillionTokens: number;
  cacheReadPerMillionTokens?: number | null;
  cacheWritePerMillionTokens?: number | null;
  fetchedAt?: Date | null;
}

/**
 * The persistence port.
 *
 * Deliberately tiny, and deliberately without a resolve method: resolution is
 * a pure function of the rows, so keeping it out here means the refresher, the
 * routes and the seed loader can all be exercised without Postgres while the
 * one behaviour that must match production — the SQL — stays in a single
 * implementation covered by the integration suite.
 */
export interface ModelPriceStore {
  /**
   * Every stored row. Resolution needs the whole set because a family match is
   * a prefix match: narrowing in SQL would mean re-implementing the library's
   * matching rules in a second place, where they could drift. The table holds
   * one row per (organization scope, model, source) for the models a deployment
   * prices, so this stays a small read.
   */
  list(organizationId: string): Promise<StoredModelPrice[]>;
  get(
    provider: string,
    modelId: string,
    source: PriceSource,
    organizationId: string,
  ): Promise<StoredModelPrice | null>;
  /** Insert or replace each write at `source`. Rows at other sources are untouched. */
  upsert(
    source: PriceSource,
    writes: readonly ModelPriceWrite[],
    now: Date,
    organizationId: string,
  ): Promise<void>;
  /**
   * Atomically replace all rows at one provider/source/scope after a successful
   * full refresh.
   *
   * Scoped to a provider because a refresh fetches one provider's catalog: a
   * replace that spanned providers would delete every other provider's rows at
   * that source as collateral, reading as "the vendor withdrew them".
   */
  replace(
    provider: string,
    source: PriceSource,
    writes: readonly ModelPriceWrite[],
    now: Date,
    organizationId: string,
  ): Promise<void>;
  /** `false` when there was nothing to delete, so a caller can 404 rather than lie. */
  delete(
    provider: string,
    modelId: string,
    source: PriceSource,
    organizationId: string,
  ): Promise<boolean>;
}

/** Project stored rows onto the library's entry shape. */
export function toPriceEntries(rows: readonly StoredModelPrice[]): ModelPriceEntry[] {
  return rows.map((row) => ({
    provider: row.provider,
    modelId: row.modelId,
    source: row.source,
    // Carried so the library can narrow operator rows to their own
    // organization. Global rows keep the empty scope, which never matches an
    // organization id and so is only ever reached through seed/upstream.
    organizationId: row.organizationId,
    inputPerMillionTokens: row.inputPerMillionTokens,
    outputPerMillionTokens: row.outputPerMillionTokens,
    // Spread-omitted rather than passed as null: the library reads an absent
    // cache rate as "derive it", and a null would type-error into a zero.
    ...(row.cacheReadPerMillionTokens !== null
      ? { cacheReadPerMillionTokens: row.cacheReadPerMillionTokens }
      : {}),
    ...(row.cacheWritePerMillionTokens !== null
      ? { cacheWritePerMillionTokens: row.cacheWritePerMillionTokens }
      : {}),
  }));
}

/**
 * The rate a model resolves to, or `null` when it is unpriced.
 *
 * `null` is load-bearing: a cost guardrail cannot enforce a budget it cannot
 * measure, so it must be able to tell "no price" from "free".
 */
export async function resolveStoredModelPricing(
  store: ModelPriceStore,
  provider: string,
  modelId: string,
  organizationId: string,
): Promise<ModelPricing | null> {
  return resolveModelPricing(
    provider,
    modelId,
    toPriceEntries(await store.list(organizationId)),
    organizationId,
  );
}

/**
 * Load the checked-in seed catalog at `seed` precedence.
 *
 * Safe to run on every boot. Because the table is keyed on (model, source), a
 * seed write cannot reach an operator or upstream row at all — the isolation
 * is structural rather than a condition someone has to remember. Re-running
 * does refresh the seed rows themselves, so a repository update to the
 * checked-in rates takes effect on the next restart.
 */
export async function loadSeedModelPrices(
  store: ModelPriceStore,
  now: Date = new Date(),
): Promise<number> {
  const writes = SEED_MODEL_PRICES.map((entry): ModelPriceWrite => {
    return {
      provider: entry.provider,
      modelId: entry.modelId,
      inputPerMillionTokens: entry.inputPerMillionTokens,
      outputPerMillionTokens: entry.outputPerMillionTokens,
      cacheReadPerMillionTokens: entry.cacheReadPerMillionTokens ?? null,
      cacheWritePerMillionTokens: entry.cacheWritePerMillionTokens ?? null,
      // Checked in, not fetched.
      fetchedAt: null,
    };
  });
  // Every seed row is one provider's catalog, so one scoped replace covers them.
  await store.replace(SEED_PRICE_PROVIDER, 'seed', writes, now, GLOBAL_MODEL_PRICE_SCOPE);
  return writes.length;
}

type ModelPriceRow = typeof modelPrices.$inferSelect;

function fromRow(row: ModelPriceRow): StoredModelPrice {
  return {
    provider: row.provider,
    organizationId: row.organizationId,
    modelId: row.modelId,
    source: row.source as PriceSource,
    inputPerMillionTokens: row.inputPerMillionTokens,
    outputPerMillionTokens: row.outputPerMillionTokens,
    cacheReadPerMillionTokens: row.cacheReadPerMillionTokens,
    cacheWritePerMillionTokens: row.cacheWritePerMillionTokens,
    fetchedAt: row.fetchedAt,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

/**
 * The database surface this store needs, structurally rather than nominally.
 *
 * A drizzle transaction handle satisfies it, which is what lets a caller bind a
 * store to an open transaction and commit a price change together with its
 * audit event. `writeAudit` narrows the same way for the same reason.
 */
export type ModelPriceDb = Pick<DbClient, 'select' | 'insert' | 'update' | 'transaction'>;

export function createPostgresModelPriceStore(db: ModelPriceDb): ModelPriceStore {
  return {
    async list(organizationId) {
      const rows = await db
        .select()
        .from(modelPrices)
        .where(
          and(
            isNull(modelPrices.deletedAt),
            organizationId === GLOBAL_MODEL_PRICE_SCOPE
              ? eq(modelPrices.organizationId, GLOBAL_MODEL_PRICE_SCOPE)
              : or(
                  eq(modelPrices.organizationId, GLOBAL_MODEL_PRICE_SCOPE),
                  eq(modelPrices.organizationId, organizationId),
                ),
          ),
        );
      return rows.map(fromRow);
    },

    async get(provider, modelId, source, organizationId) {
      assertPriceScope(source, organizationId);
      const rows = await db
        .select()
        .from(modelPrices)
        .where(
          and(
            isNull(modelPrices.deletedAt),
            eq(modelPrices.provider, provider),
            eq(modelPrices.modelId, modelId),
            eq(modelPrices.source, source),
            eq(modelPrices.organizationId, organizationId),
          ),
        )
        .limit(1);
      const row = rows[0];
      return row ? fromRow(row) : null;
    },

    async upsert(source, writes, now, organizationId) {
      assertPriceScope(source, organizationId);
      if (writes.length === 0) return;
      await db
        .insert(modelPrices)
        .values(
          writes.map((write) => ({
            provider: write.provider,
            modelId: write.modelId,
            source,
            organizationId,
            inputPerMillionTokens: write.inputPerMillionTokens,
            outputPerMillionTokens: write.outputPerMillionTokens,
            cacheReadPerMillionTokens: write.cacheReadPerMillionTokens ?? null,
            cacheWritePerMillionTokens: write.cacheWritePerMillionTokens ?? null,
            fetchedAt: write.fetchedAt ?? null,
            createdAt: now,
            updatedAt: now,
          })),
        )
        .onConflictDoUpdate({
          target: [
            modelPrices.provider,
            modelPrices.modelId,
            modelPrices.source,
            modelPrices.organizationId,
          ],
          set: {
            deletedAt: null,
            inputPerMillionTokens: sql`excluded.input_per_million_tokens`,
            outputPerMillionTokens: sql`excluded.output_per_million_tokens`,
            cacheReadPerMillionTokens: sql`excluded.cache_read_per_million_tokens`,
            cacheWritePerMillionTokens: sql`excluded.cache_write_per_million_tokens`,
            fetchedAt: sql`excluded.fetched_at`,
            // `created_at` is deliberately absent: it records when this
            // (model, source) pair was first priced and does not move.
            updatedAt: now,
          },
        });
    },

    async replace(provider, source, writes, now, organizationId) {
      assertPriceScope(source, organizationId);
      await db.transaction(async (tx) => {
        const base = and(
          eq(modelPrices.provider, provider),
          eq(modelPrices.source, source),
          eq(modelPrices.organizationId, organizationId),
        );
        await tx
          .update(modelPrices)
          .set({ deletedAt: new Date() })
          .where(
            and(
              isNull(modelPrices.deletedAt),
              writes.length === 0
                ? base
                : and(
                    base,
                    notInArray(
                      modelPrices.modelId,
                      writes.map((write) => write.modelId),
                    ),
                  ),
            ),
          );
        if (writes.length === 0) return;
        await tx
          .insert(modelPrices)
          .values(
            writes.map((write) => ({
              provider: write.provider,
              modelId: write.modelId,
              source,
              organizationId,
              inputPerMillionTokens: write.inputPerMillionTokens,
              outputPerMillionTokens: write.outputPerMillionTokens,
              cacheReadPerMillionTokens: write.cacheReadPerMillionTokens ?? null,
              cacheWritePerMillionTokens: write.cacheWritePerMillionTokens ?? null,
              fetchedAt: write.fetchedAt ?? null,
              createdAt: now,
              updatedAt: now,
            })),
          )
          .onConflictDoUpdate({
            target: [
              modelPrices.provider,
              modelPrices.modelId,
              modelPrices.source,
              modelPrices.organizationId,
            ],
            set: {
              deletedAt: null,
              inputPerMillionTokens: sql`excluded.input_per_million_tokens`,
              outputPerMillionTokens: sql`excluded.output_per_million_tokens`,
              cacheReadPerMillionTokens: sql`excluded.cache_read_per_million_tokens`,
              cacheWritePerMillionTokens: sql`excluded.cache_write_per_million_tokens`,
              fetchedAt: sql`excluded.fetched_at`,
              updatedAt: now,
            },
          });
      });
    },

    async delete(provider, modelId, source, organizationId) {
      assertPriceScope(source, organizationId);
      const deleted = await db
        .update(modelPrices)
        .set({ deletedAt: new Date() })
        .where(
          and(
            isNull(modelPrices.deletedAt),
            eq(modelPrices.provider, provider),
            eq(modelPrices.modelId, modelId),
            eq(modelPrices.source, source),
            eq(modelPrices.organizationId, organizationId),
          ),
        )
        .returning({ modelId: modelPrices.modelId });
      return deleted.length > 0;
    },
  };
}

/** The same semantics without Postgres, for unit tests. */
export class InMemoryModelPriceStore implements ModelPriceStore {
  private readonly deletedAt = new Map<string, Date>();
  private readonly rows = new Map<string, StoredModelPrice>();

  private static key(
    provider: string,
    modelId: string,
    source: PriceSource,
    organizationId: string,
  ): string {
    return `${organizationId}\0${source}\0${provider}\0${modelId}`;
  }

  async list(organizationId: string): Promise<StoredModelPrice[]> {
    return [...this.rows.values()]
      .filter(
        (row) =>
          !this.deletedAt.has(
            InMemoryModelPriceStore.key(row.provider, row.modelId, row.source, row.organizationId),
          ) &&
          (row.organizationId === GLOBAL_MODEL_PRICE_SCOPE ||
            row.organizationId === organizationId),
      )
      .map((row) => ({ ...row }));
  }

  async get(
    provider: string,
    modelId: string,
    source: PriceSource,
    organizationId: string,
  ): Promise<StoredModelPrice | null> {
    assertPriceScope(source, organizationId);
    const row = this.rows.get(
      InMemoryModelPriceStore.key(provider, modelId, source, organizationId),
    );
    return row &&
      !this.deletedAt.has(InMemoryModelPriceStore.key(provider, modelId, source, organizationId))
      ? { ...row }
      : null;
  }

  async upsert(
    source: PriceSource,
    writes: readonly ModelPriceWrite[],
    now: Date,
    organizationId: string,
  ): Promise<void> {
    assertPriceScope(source, organizationId);
    for (const write of writes) {
      const key = InMemoryModelPriceStore.key(
        write.provider,
        write.modelId,
        source,
        organizationId,
      );
      const existing = this.rows.get(key);
      this.deletedAt.delete(key);
      this.rows.set(key, {
        provider: write.provider,
        organizationId,
        modelId: write.modelId,
        source,
        inputPerMillionTokens: write.inputPerMillionTokens,
        outputPerMillionTokens: write.outputPerMillionTokens,
        cacheReadPerMillionTokens: write.cacheReadPerMillionTokens ?? null,
        cacheWritePerMillionTokens: write.cacheWritePerMillionTokens ?? null,
        fetchedAt: write.fetchedAt ?? null,
        createdAt: existing?.createdAt ?? now,
        updatedAt: now,
      });
    }
  }

  async replace(
    provider: string,
    source: PriceSource,
    writes: readonly ModelPriceWrite[],
    now: Date,
    organizationId: string,
  ): Promise<void> {
    assertPriceScope(source, organizationId);
    const keep = new Set(writes.map((write) => write.modelId));
    for (const row of this.rows.values()) {
      if (
        row.provider === provider &&
        row.source === source &&
        row.organizationId === organizationId &&
        !keep.has(row.modelId)
      ) {
        const key = InMemoryModelPriceStore.key(provider, row.modelId, source, organizationId);
        if (!this.deletedAt.has(key)) this.deletedAt.set(key, now);
      }
    }
    await this.upsert(source, writes, now, organizationId);
  }

  async delete(
    provider: string,
    modelId: string,
    source: PriceSource,
    organizationId: string,
  ): Promise<boolean> {
    assertPriceScope(source, organizationId);
    const key = InMemoryModelPriceStore.key(provider, modelId, source, organizationId);
    if (!this.rows.has(key) || this.deletedAt.has(key)) return false;
    this.deletedAt.set(key, new Date());
    return true;
  }
}

function assertPriceScope(source: PriceSource, organizationId: string): void {
  if (source === 'operator' ? organizationId.length === 0 : organizationId.length !== 0) {
    throw new Error(
      source === 'operator'
        ? 'operator model prices require an organization scope'
        : `${source} model prices must use the deployment-global scope`,
    );
  }
}
