// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { SEED_PRICE_PROVIDER, resolveModelPricing } from '@orca/harness-catalog';
import { buildClaudeErrorResponse, type ClaudeErrorType } from '../contracts/common.js';
import {
  ModelProviderId,
  PRICING_API_PREFIX,
  resolvedModelPriceToWire,
} from '../contracts/model-prices.contract.js';
import { toPriceEntries, type ModelPriceStore } from '../pricing/store.js';
import { parsePositiveIntQueryParam } from './query-params.js';

const MODEL_PRICE_LIST_PAGE_DEFAULT = 100;
const MODEL_PRICE_LIST_PAGE_MAX = 100;

/**
 * The one sentence a workspace principal gets for attempting a write.
 * Identical across every method so the boundary reads as one rule.
 */
export const PRICE_WRITE_TIER_MESSAGE =
  'model prices are managed by the organization; see /v1/organizations/modelprices';

export type ModelPriceOrganizationResolver = (workspaceId: string) => Promise<string>;

/**
 * Workspace-facing model prices: read-only, and resolved.
 *
 * Reads are served here because an operator must be able to see the rates
 * their spend is measured against. Writes are not, and the asymmetry is the
 * point: a workspace that could set its own prices could set them to zero and
 * walk through every budget applied to it. Price authority sits with the tier
 * that owns organization guardrails, for the same reason.
 *
 * Every response is the *effective* price after precedence and family
 * fallback, because that is the number a session is billed at. A model nothing
 * prices is a 404 rather than an object full of zeroes: unpriced and free are
 * different facts, and only one of them a cost guardrail can enforce against.
 *
 * See `docs/managed-agents/pricing.md`.
 */
export function registerModelPricesRoutes(
  app: FastifyInstance,
  store: ModelPriceStore,
  organizationForWorkspace: ModelPriceOrganizationResolver,
): void {
  app.get(`${PRICING_API_PREFIX}/modelprices`, async (req, reply) => {
    const organizationId = await organizationForWorkspace(req.auth!.workspaceId);
    const q = req.query as { limit?: string; page?: string };
    const parsedLimit = parsePositiveIntQueryParam(q.limit, 'limit', {
      defaultValue: MODEL_PRICE_LIST_PAGE_DEFAULT,
      max: MODEL_PRICE_LIST_PAGE_MAX,
    });
    if (!parsedLimit.ok) return badRequest(reply, req, parsedLimit.error!);
    const limit = parsedLimit.value!;

    // One page of *models*, not of rows: a model priced by all three sources is
    // one price, and paging the rows would split it across pages. The page key
    // is the whole identity — the same id under two providers is two prices, so
    // paging on the id alone would collapse them into one entry.
    const entries = toPriceEntries(await store.list(organizationId));
    const keys = [
      ...new Set(entries.map((entry) => modelKey(entry.provider, entry.modelId))),
    ].sort();
    const after = q.page;
    if (after !== undefined && !keys.includes(after)) {
      return badRequest(reply, req, 'invalid page');
    }
    const start = after === undefined ? 0 : keys.indexOf(after) + 1;
    const page = keys.slice(start, start + limit);

    const data = page.flatMap((key) => {
      const { provider, modelId } = splitModelKey(key);
      const pricing = resolveModelPricing(provider, modelId, entries, organizationId);
      // A stored row whose family match is ambiguous resolves to unpriced. It
      // is omitted rather than served at some invented rate.
      return pricing ? [resolvedModelPriceToWire(provider, modelId, pricing)] : [];
    });
    const last = page[page.length - 1];
    return reply.send({
      data,
      next_page: last !== undefined && start + limit < keys.length ? last : null,
    });
  });

  app.get(`${PRICING_API_PREFIX}/modelprices/:model_id`, async (req, reply) => {
    const organizationId = await organizationForWorkspace(req.auth!.workspaceId);
    const { model_id: modelId } = req.params as { model_id: string };
    // The provider rides in the query rather than the path: it is part of the
    // identity, but a deployment pricing one vendor should not have to spell it
    // out, and the id stays a single addressable path segment.
    const provider = parseProvider((req.query as { provider?: string }).provider);
    if (provider === null) return badRequest(reply, req, 'invalid provider');
    const pricing = resolveModelPricing(
      provider,
      modelId,
      toPriceEntries(await store.list(organizationId)),
      organizationId,
    );
    if (!pricing) return unpriced(reply, req, provider, modelId);
    return reply.send(resolvedModelPriceToWire(provider, modelId, pricing));
  });

  // No write methods are registered here, so a write to this listener 404s.
  //
  // An earlier revision answered them with a 403 naming the admin listener,
  // which reads better to a human but registers eight public routes that no
  // contract declares. `test/unit/route-contract-parity.spec.ts` exists to stop
  // exactly that: a served operation the published spec and the conformance
  // matrix know nothing about. Declaring them instead would be worse — a
  // generated client would offer price writes against the workspace base URL
  // that can only ever fail. `GET /apis/pricing.runorca.ai/v1` and the pricing
  // doc both say where writes live; the served surface stays honest.
}

/**
 * This API group sits outside `/v1`, where the Claude edge adapter rewrites
 * error payloads, so every failure here builds its own envelope. Clients get
 * one error shape across both groups.
 */
function fail(
  reply: FastifyReply,
  req: FastifyRequest,
  status: number,
  type: ClaudeErrorType,
  message: string,
) {
  return reply.code(status).send(buildClaudeErrorResponse(req.id, type, message));
}

function badRequest(reply: FastifyReply, req: FastifyRequest, message: string) {
  return fail(reply, req, 400, 'invalid_request_error', message);
}

function unpriced(reply: FastifyReply, req: FastifyRequest, provider: string, modelId: string) {
  return fail(
    reply,
    req,
    404,
    'not_found_error',
    `no price is configured for model ${modelId} from provider ${provider}`,
  );
}

/** `undefined` means "the default provider"; an unreadable value is a 400, never a silent default. */
function parseProvider(raw: string | undefined): string | null {
  if (raw === undefined) return SEED_PRICE_PROVIDER;
  const parsed = ModelProviderId.safeParse(raw);
  return parsed.success ? parsed.data : null;
}

/**
 * A page cursor that survives a round trip through a query parameter. `/` is
 * excluded from both halves by `MODEL_ID_PATTERN`, so the split is unambiguous
 * and the cursor stays a printable string a client can echo back.
 */
function modelKey(provider: string, modelId: string): string {
  return `${provider}/${modelId}`;
}

function splitModelKey(key: string): { provider: string; modelId: string } {
  const separator = key.indexOf('/');
  return { provider: key.slice(0, separator), modelId: key.slice(separator + 1) };
}
