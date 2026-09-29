// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract, isAppRoute, type AppRouter } from '@ts-rest/core';
import { z } from 'zod';
import { agentsContract } from './agents.contract.js';
import { discoveryContract } from './discovery.contract.js';
import { environmentsContract } from './environments.contract.js';
import { filesContract } from './files.contract.js';
import { gitCredsContract } from './git-creds.contract.js';
import { gitProxyContract } from './git-proxy.contract.js';
import { guardrailsContract } from './guardrails.contract.js';
import { healthContract } from './health.contract.js';
import { memoryStoresContract } from './memory-stores.contract.js';
import { modelPricesContract } from './model-prices.contract.js';
import { outcomesContract } from './outcomes.contract.js';
import { sessionsContract } from './sessions.contract.js';
import { skillsContract } from './skills.contract.js';
import { triggersContract } from './triggers.contract.js';
import { vaultsContract } from './vaults.contract.js';

const c = initContract();

/**
 * Headers every public operation accepts and does nothing with.
 *
 * `anthropic-beta` is declared because Anthropic declares it on every operation
 * and their SDK sends it on every call, so any client built against their spec
 * will send it here too. This engine accepts it and ignores it — deliberately.
 * See `docs/managed-agents/overview.md`: Orca is Anthropic-compatible but is not
 * Anthropic, and must not let an SDK conclude otherwise. Declaring the header
 * does not change that; it makes the existing choice visible instead of leaving
 * a silently-swallowed header undocumented.
 *
 * Typed `string`, not Anthropic's enum of beta value names, on purpose:
 * publishing the enum would advertise that we recognise those values. We parse
 * none of them, and nothing in this service branches on this header.
 */
const acceptedAndIgnoredHeaders = z.object({
  'anthropic-beta': z
    .string()
    .optional()
    .describe(
      'Accepted for Anthropic SDK compatibility and ignored. No value of this header changes ' +
        'request handling. Typed as a free-form string rather than an enum because this service ' +
        'recognises no specific beta value.',
    ),
});

/**
 * Extend every route's `headers` with {@link acceptedAndIgnoredHeaders}.
 *
 * Applied once here rather than repeated on ~80 route definitions: "the public
 * surface accepts and ignores this header" is one fact about the whole surface,
 * and stating it once is the only version that cannot drift. `.extend()` rather
 * than `.merge()` so a route's existing `.passthrough()` survives.
 */
function withAcceptedAndIgnoredHeaders<T extends AppRouter>(router: T): T {
  const composed: AppRouter = {};
  for (const [key, value] of Object.entries(router)) {
    if (isAppRoute(value)) {
      const declared = value.headers;
      composed[key] = {
        ...value,
        headers:
          declared instanceof z.ZodObject
            ? declared.extend(acceptedAndIgnoredHeaders.shape)
            : acceptedAndIgnoredHeaders,
      };
    } else {
      composed[key] = withAcceptedAndIgnoredHeaders(value);
    }
  }
  return composed as T;
}

/**
 * The public HTTP surface, composed from the per-resource routers.
 *
 * This is what `pnpm openapi:gen` renders into `openapi/managed-agents.yaml`
 * and what the conformance differ compares against Anthropic's spec. A route
 * that is not reachable from here does not exist as far as either is concerned.
 *
 * `internal.contract.ts` is deliberately absent: `/internal/*` is a workload API
 * spoken between the registry and the harness, not public surface, and including
 * it would report a pile of Orca-only operations that no external client can
 * reach.
 *
 * `discovery` and `health` are here for the opposite reason: both are served on
 * the public listener, so a document that omitted them would describe a smaller
 * surface than the one that exists. Anthropic publishes
 * neither, so both classify as extensions and are tagged as such — which is the
 * classification working, not a problem to suppress.
 */
export const publicContract = withAcceptedAndIgnoredHeaders(
  c.router({
    agents: agentsContract,
    discovery: discoveryContract,
    environments: environmentsContract,
    files: filesContract,
    gitCreds: gitCredsContract,
    gitProxy: gitProxyContract,
    guardrails: guardrailsContract,
    health: healthContract,
    memoryStores: memoryStoresContract,
    modelPrices: modelPricesContract,
    outcomes: outcomesContract,
    sessions: sessionsContract,
    skills: skillsContract,
    triggers: triggersContract,
    vaults: vaultsContract,
  }),
);
