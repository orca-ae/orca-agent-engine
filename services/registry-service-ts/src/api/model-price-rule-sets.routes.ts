// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance, FastifyRequest } from 'fastify';
import { and, eq, lt, sql } from 'drizzle-orm';
import { requireAdminScope } from '../auth/admin-auth.js';
import { PriceRuleSetCreateSchema } from '../contracts/model-price-rules.contract.js';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import { pricingAdminIdempotency } from '../persistence/postgres/schema.js';
import {
  createPostgresPricingCatalogStore,
  priceDigest,
  PricingConflictError,
  type CatalogDocument,
  type StoredPriceRuleSet,
} from '../pricing/catalog-store.js';
import {
  InvalidPriceRuleSetError,
  publishOperatorRuleSet,
  retireOperatorRuleSet,
} from '../pricing/rule-sets.js';

type Audit = (
  db: DbTransaction,
  req: FastifyRequest,
  action: string,
  targetType: string,
  targetId: string,
  workspaceId: null,
  metadata: Record<string, unknown>,
) => Promise<void>;

function wire(doc: CatalogDocument, set: StoredPriceRuleSet) {
  return {
    ...set,
    rules: doc.rules.filter((r) => r.rule_set_id === set.id),
    catalog_id: doc.catalog_id,
    generation: doc.generation,
  };
}

export function registerAdminModelPriceRuleSetRoutes(
  app: FastifyInstance,
  db: DbClient,
  writeAudit: Audit,
  activationEnabled = false,
): void {
  const path = '/v1/organizations/modelpricerulesets';
  app.get(path, async (req, reply) => {
    if (!requireAdminScope(req, reply, 'model_prices:read')) return;
    const doc = await createPostgresPricingCatalogStore(db).read(req.adminAuth!.organizationId);
    const global = await createPostgresPricingCatalogStore(db).read('');
    return {
      data: [
        ...global.rule_sets.map((set) => wire(global, set)),
        ...doc.rule_sets.map((set) => wire(doc, set)),
      ],
      refresh: global.refresh,
    };
  });
  app.get(`${path}/:id`, async (req, reply) => {
    if (!requireAdminScope(req, reply, 'model_prices:read')) return;
    let doc = await createPostgresPricingCatalogStore(db).read(req.adminAuth!.organizationId);
    let set = doc.rule_sets.find((s) => s.id === (req.params as { id: string }).id);
    if (!set) {
      doc = await createPostgresPricingCatalogStore(db).read('');
      set = doc.rule_sets.find((s) => s.id === (req.params as { id: string }).id);
    }
    if (!set) return reply.code(404).send({ error: 'model price rule set not found' });
    reply.header('etag', `"${set.revision}"`);
    return wire(doc, set);
  });
  for (const method of ['POST', 'PATCH', 'DELETE'] as const) {
    app.route({
      method,
      url: method === 'POST' ? path : `${path}/:id`,
      handler: async (req, reply) => {
        if (!requireAdminScope(req, reply, 'model_prices:write')) return;
        const org = req.adminAuth!.organizationId;
        const id = method === 'POST' ? undefined : (req.params as { id: string }).id;
        let revision: number | undefined;
        if (id) {
          const match = /^"([1-9][0-9]*)"$/.exec(String(req.headers['if-match'] ?? ''));
          if (!match || !Number.isSafeInteger(Number(match[1])))
            return reply
              .code(400)
              .send({ error: 'If-Match must contain the quoted rule set revision' });
          revision = Number(match[1]);
        }
        const body = method === 'DELETE' ? undefined : PriceRuleSetCreateSchema.safeParse(req.body);
        if (body && !body.success)
          return reply
            .code(400)
            .send({ error: body.error.issues[0]?.message ?? 'invalid rule set' });
        const key = req.headers['idempotency-key'];
        if (key !== undefined && (typeof key !== 'string' || !/^[\x21-\x7e]{1,200}$/.test(key)))
          return reply.code(400).send({ error: 'invalid idempotency key' });
        try {
          const now = new Date();
          const result = await db.transaction(async (tx) => {
            const operation = `${method}:${req.url.split('?')[0]}`;
            const principal = req.adminAuth!.principal;
            const hash = priceDigest([req.body ?? null, req.headers['if-match'] ?? null]);
            const match = and(
              eq(pricingAdminIdempotency.organizationId, org),
              eq(pricingAdminIdempotency.principal, principal),
              eq(pricingAdminIdempotency.operation, operation),
              eq(pricingAdminIdempotency.key, key ?? ''),
            );
            if (key) {
              await tx.execute(
                sql`select pg_advisory_xact_lock(hashtextextended(${JSON.stringify(['price-mutation', org, principal, operation, key])}, 0))`,
              );
              const [cached] = await tx.select().from(pricingAdminIdempotency).where(match);
              if (cached && cached.expiresAt > now) {
                if (cached.bodyHash !== hash)
                  throw new PricingConflictError('idempotency key reused with different request');
                return cached.response as { status: number; body: unknown; etag: string | null };
              }
            }
            const store = createPostgresPricingCatalogStore(tx);
            if (id && !(await store.read(org)).rule_sets.some((s) => s.id === id))
              return {
                status: 404,
                body: { error: 'model price rule set not found' },
                etag: null,
              };
            const set = await store.publish(org, now, (doc) => {
              if (method === 'DELETE') {
                retireOperatorRuleSet(doc, id!, revision!, now);
                return null;
              }
              return publishOperatorRuleSet(
                doc,
                org,
                body!.data!,
                now,
                activationEnabled,
                id,
                revision,
              );
            });
            await writeAudit(
              tx,
              req,
              `model_price_rule_set.${method === 'POST' ? 'created' : method === 'PATCH' ? 'updated' : 'retired'}`,
              'model_price_rule_set',
              set?.id ?? id!,
              null,
              { revision: set?.revision ?? revision },
            );
            const response = {
              status: method === 'POST' ? 201 : 200,
              body: set
                ? wire(await store.read(org), set)
                : { id, type: 'model_price_rule_set_deleted' },
              etag: set ? `"${set.revision}"` : null,
            };
            if (key) {
              await tx
                .delete(pricingAdminIdempotency)
                .where(
                  and(
                    eq(pricingAdminIdempotency.organizationId, org),
                    lt(pricingAdminIdempotency.expiresAt, now),
                  ),
                );
              await tx
                .insert(pricingAdminIdempotency)
                .values({
                  organizationId: org,
                  principal,
                  operation,
                  key,
                  bodyHash: hash,
                  response,
                  expiresAt: new Date(now.getTime() + 24 * 3600000),
                })
                .onConflictDoUpdate({
                  target: [
                    pricingAdminIdempotency.organizationId,
                    pricingAdminIdempotency.principal,
                    pricingAdminIdempotency.operation,
                    pricingAdminIdempotency.key,
                  ],
                  set: {
                    bodyHash: hash,
                    response,
                    expiresAt: new Date(now.getTime() + 24 * 3600000),
                  },
                });
            }
            return response;
          });
          if (result.etag) reply.header('etag', result.etag);
          if (result.status === 201) return reply.code(201).send(result.body);
          if (result.status === 404) return reply.code(404).send(result.body);
          return reply.code(200).send(result.body);
        } catch (error) {
          if (error instanceof PricingConflictError)
            return reply.code(409).send({ error: error.message });
          if (error instanceof InvalidPriceRuleSetError)
            return reply.code(400).send({ error: error.message });
          throw error;
        }
      },
    });
  }
}
