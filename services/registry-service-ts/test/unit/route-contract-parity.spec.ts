// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FileStore } from '@orca/file-store';
import type { MemoryStore } from '@orca/memory-store';
import { InMemorySkillStore } from '@orca/skill-store';
import type { TranscriptStore } from '@orca/transcript-store';
import { isAppRoute, type AppRouter } from '@ts-rest/core';
import { afterEach, describe, expect, it } from 'vitest';
import type { SessionJwtMinter } from '../../src/auth/session-jwt.js';
import { publicContract } from '../../src/contracts/index.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import type { SecretStore } from '../../src/secrets/secret-provider.js';
import {
  buildPublicApp,
  registeredRoutes,
  type BuildAppOptions,
  type RouteRecord,
} from '../../src/server.js';
import { operationKey } from '../../scripts/lib/normalize-operation.mjs';

/**
 * The public contract and the Fastify route table must describe the same
 * surface.
 *
 * `src/contracts/*.contract.ts` is what `pnpm openapi:gen` publishes and what
 * the conformance differ compares against Anthropic's spec. The route table is
 * what clients can actually call. Nothing generates one from the other, so
 * before this test a handler added or deleted on its own changed the served API
 * without moving either generated artifact — and both drift gates stayed green.
 *
 * That is not hypothetical: an undeclared PATCH alias was once served and
 * covered by the integration suite while the published spec and conformance
 * matrix showed only GET, POST and DELETE on that path.
 *
 * Both sides are reduced with the differ's own `operationKey`, so "the same
 * operation" means here exactly what it means in the conformance matrix.
 */

/**
 * Routes that are deliberately served without being contract surface.
 *
 * Asserted as an exact set, not consulted as an allowlist: a new route outside
 * `/v1` has to be added here consciously, which is the moment to ask whether it
 * belongs in the published contract instead.
 *
 * The probes and discovery routes are NOT here — `health.contract.ts` and
 * `discovery.contract.ts` publish them. What remains is the colocated execution
 * path's transport surface: the WebSocket tunnel upgrades and the mesh-internal
 * runner-management routes the registry serves to the environment worker and
 * session-runner. These carry no request/response body an OpenAPI document could
 * describe, so they are served without contract surface, like the probes were.
 */
const NON_CONTRACT_ROUTES: string[] = [
  'GET /internal/runners',
  'GET /internal/runners/{1}/status',
  'GET /v1/sessions/{1}/terminals/{2}/attach',
  'GET /v1/tunnels/environments/{1}',
  'GET /v1/tunnels/runners/{1}',
];

const RETIRED_PUBLIC_OPERATIONS = [
  operationKey('POST', '/v1/files/:id/archive'),
  operationKey('PATCH', '/v1/memory_stores/:id/memories/:memory_id'),
  operationKey('GET', '/v1/memory_stores/:id/memories/:memory_id/content'),
  operationKey('GET', '/v1/sessions/:id/stream'),
  operationKey('POST', '/v1/skills/:id'),
  operationKey('POST', '/v1/skills/:id/archive'),
];

/**
 * The complete public surface needs every optional dependency: memory-store
 * routes mount only with a `memoryStore`, and the Git credential helper only
 * with a `secretStore` or `secretProvider`. Omitting one shrinks the comparison
 * instead of failing it, so all of them are supplied. The stubs are never
 * called — route registration does not touch them.
 */
function optionsWithEveryRouteFamily(): BuildAppOptions {
  return {
    db: {} as DbClient,
    oidc: { allowedIssuers: [], audience: 'test' },
    store: {} as TranscriptStore,
    sse: { bufferSize: 1, dropAgeMs: 1, heartbeatMs: 1 },
    jwtMinter: {} as SessionJwtMinter,
    fileStore: {} as FileStore,
    skillStore: new InMemorySkillStore(),
    memoryStore: {} as MemoryStore,
    secretStore: {} as SecretStore,
  };
}

function contractOperations(router: AppRouter, into: Set<string> = new Set()): Set<string> {
  for (const value of Object.values(router)) {
    if (isAppRoute(value)) into.add(operationKey(value.method, value.path));
    else contractOperations(value as AppRouter, into);
  }
  return into;
}

function servedOperations(routes: readonly RouteRecord[]): Set<string> {
  return new Set(routes.map((route) => operationKey(route.method, route.url)));
}

const apps: Array<ReturnType<typeof buildPublicApp>> = [];

afterEach(async () => {
  await Promise.all(apps.splice(0).map(async (app) => app.close()));
});

async function publicApp(): Promise<ReturnType<typeof buildPublicApp>> {
  const app = buildPublicApp(optionsWithEveryRouteFamily());
  apps.push(app);
  await app.ready();
  return app;
}

describe('the public route table and the published contract', () => {
  it('serves every operation the contract declares', async () => {
    const served = servedOperations(registeredRoutes(await publicApp()));

    const declaredButNotServed = [...contractOperations(publicContract)]
      .filter((operation) => !served.has(operation))
      .sort();

    expect(
      declaredButNotServed,
      'the contract publishes operations no handler answers; add the route or drop the contract entry',
    ).toEqual([]);
  });

  it('declares every operation it serves, apart from the probes', async () => {
    const declared = contractOperations(publicContract);

    const servedButNotDeclared = [...servedOperations(registeredRoutes(await publicApp()))]
      .filter((operation) => !declared.has(operation))
      .sort();

    expect(
      servedButNotDeclared,
      'a handler answers an operation the published spec and conformance matrix do not know about',
    ).toEqual([...NON_CONTRACT_ROUTES].sort());
  });

  /**
   * Guards the comparison itself. Both assertions above pass trivially over an
   * empty or truncated route table, and the families below are the ones that
   * mount conditionally — exactly the ones a refactor could stop mounting here
   * without anything else noticing.
   */
  it('compares the whole surface, including the conditionally mounted families', async () => {
    const served = servedOperations(registeredRoutes(await publicApp()));

    expect(served.size).toBeGreaterThan(50);
    expect(served).toContain('POST /v1/memory_stores/{1}/memories/{2}');
    expect(served).toContain('POST /v1/git-creds');
    expect(served).toContain('GET /v1/git-proxy/{1}/info/refs');
    expect(served).toContain('POST /v1/git-proxy/{1}/git-upload-pack');
  });

  it('does not serve or publish retired Orca-only operations', async () => {
    const served = servedOperations(registeredRoutes(await publicApp()));
    const declared = contractOperations(publicContract);

    for (const operation of RETIRED_PUBLIC_OPERATIONS) {
      expect(served).not.toContain(operation);
      expect(declared).not.toContain(operation);
    }
  });
});
