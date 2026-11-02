// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance } from 'fastify';
import { buildClaudeErrorResponse } from '../contracts/common.js';
import {
  PRICING_V2_PREFIX,
  modelPriceRulesContract,
} from '../contracts/model-price-rules.contract.js';
import { PricingCursorExpiredError, type PricingCatalogStore } from '../pricing/catalog-store.js';
import type { ModelPriceOrganizationResolver } from './model-prices.routes.js';

export function registerModelPriceRulesRoutes(
  app: FastifyInstance,
  store: PricingCatalogStore,
  organizationForWorkspace: ModelPriceOrganizationResolver,
  now = () => new Date(),
): void {
  app.get(`${PRICING_V2_PREFIX}/modelpricerules`, async (req, reply) => {
    const query = modelPriceRulesContract.list.query.safeParse(req.query);
    const bad = (message: string) =>
      reply.code(400).send(buildClaudeErrorResponse(req.id, 'invalid_request_error', message));
    if (!query.success) return bad('invalid pricing page or limit');
    const scope = req.auth!.workspaceId;
    const organization = await organizationForWorkspace(scope);
    const limit = query.data.limit ?? 100;
    try {
      let offset = 0;
      let catalog;
      if (query.data.page) {
        const match = /^(pcat_[A-Z0-9]+):([1-9][0-9]*)$/.exec(query.data.page);
        if (!match) return bad('invalid pricing cursor');
        offset = Number(match[2]);
        if (!Number.isSafeInteger(offset)) return bad('invalid pricing cursor');
        catalog = await store.fetchSnapshot(scope, organization, match[1]!, now());
        if (offset >= catalog.rules.length) return bad('invalid pricing cursor offset');
      } else catalog = await store.snapshot(scope, organization, now());
      const { rules, ...metadata } = catalog;
      const end = Math.min(offset + limit, rules.length);
      reply.header('cache-control', 'private, no-store');
      return {
        ...metadata,
        data: rules.slice(offset, end),
        next_page: end < rules.length ? `${catalog.catalog_id}:${end}` : null,
      };
    } catch (error) {
      if (error instanceof PricingCursorExpiredError)
        return reply
          .code(410)
          .send(buildClaudeErrorResponse(req.id, 'not_found_error', error.message));
      throw error;
    }
  });
}
