// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FileStore } from '@orca/file-store';
import type { MemoryStore } from '@orca/memory-store';
import { InMemorySkillStore } from '@orca/skill-store';
import type { TranscriptStore } from '@orca/transcript-store';
import { isAppRoute, type AppRouter } from '@ts-rest/core';
import Fastify, { type FastifyInstance } from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';
import { registerDiscoveryRoutes } from '../../src/api/discovery.routes.js';
import type { SessionJwtMinter } from '../../src/auth/session-jwt.js';
import { publicContract } from '../../src/contracts/index.js';
import {
  ApiGroupList,
  ApiResourceList,
  ApiVersions,
} from '../../src/contracts/discovery.contract.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import type { SecretStore } from '../../src/secrets/secret-provider.js';
import { buildPublicApp, registeredRoutes, type BuildAppOptions } from '../../src/server.js';
import { operationKey } from '../../scripts/lib/normalize-operation.mjs';

const apps: Array<ReturnType<typeof buildPublicApp>> = [];
const bareApps: FastifyInstance[] = [];

afterEach(async () => {
  await Promise.all([...apps.splice(0), ...bareApps.splice(0)].map(async (app) => app.close()));
});

/**
 * Every optional dependency supplied, so the app registers every route it is
 * capable of registering. A drift test run against a partially-wired app would
 * report the routes it happened to omit as contract-only entries.
 */
function options(extra: Partial<BuildAppOptions> = {}): BuildAppOptions {
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
    ...extra,
  };
}

describe('discovery', () => {
  /**
   * The handlers alone, with no auth hook in front of them.
   *
   * The payload and the auth boundary are two separate claims, and the real app
   * cannot prove the first one: its `db` is `{}`, so authenticating far enough to
   * reach a 200 would throw. Asserting the body here keeps that assertion in a
   * unit test; the authenticated 200 is proven end to end in
   * `test/integration/auth.spec.ts`.
   */
  function buildBareDiscoveryApp(): FastifyInstance {
    const app = Fastify();
    registerDiscoveryRoutes(app);
    bareApps.push(app);
    return app;
  }

  it('discovers every managed SDK, including the persistent Claude variant', async () => {
    const response = await buildBareDiscoveryApp().inject({
      method: 'GET',
      url: '/apis/runtime.runorca.ai/v1/harnesses',
    });
    expect(response.statusCode).toBe(200);
    const entries = response.json().data;
    expect(entries.map((entry: { id: string }) => entry.id)).toEqual([
      'claude_agent_sdk',
      'claude_agent_sdk_persistent',
      'codex_sdk',
      'pi_sdk',
    ]);
    const piProviders = entries[3].models.map((model: { provider: string }) => model.provider);
    expect(piProviders).toEqual(
      expect.arrayContaining(['anthropic', 'openai', 'deepseek', 'google', 'zai']),
    );
    expect(piProviders).not.toContain('openai-codex');
    expect(piProviders).not.toContain('google-vertex');
    expect(entries[3].models).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ provider: 'anthropic', id: 'claude-sonnet-4-6' }),
        expect.objectContaining({ provider: 'deepseek', id: 'deepseek-flash' }),
      ]),
    );
    expect(entries[1]).toMatchObject({
      provider: 'claude-sdk-persistent',
      modes: ['separate'],
      models: entries[0].models,
      capabilities: { native_resume: false },
    });
  });

  it('advertises the core API version', async () => {
    const response = await buildBareDiscoveryApp().inject({ method: 'GET', url: '/api' });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      kind: 'APIVersions',
      versions: ['v1'],
      preferred_version: 'v1',
    });
    expect(ApiVersions.safeParse(response.json()).success).toBe(true);
  });

  it('advertises the groups this engine ships', async () => {
    // Both ship in the engine, so both are present on every deployment
    // including a self-hosted one. A distribution appends its own; the list is
    // the only supported way for a client to learn which groups it can call.
    const response = await buildBareDiscoveryApp().inject({ method: 'GET', url: '/apis' });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      kind: 'APIGroupList',
      groups: [
        {
          name: 'runtime.runorca.ai',
          versions: [{ group_version: 'runtime.runorca.ai/v1', version: 'v1' }],
          preferred_version: { group_version: 'runtime.runorca.ai/v1', version: 'v1' },
        },
        {
          name: 'policy.runorca.ai',
          versions: [{ group_version: 'policy.runorca.ai/v1', version: 'v1' }],
          preferred_version: { group_version: 'policy.runorca.ai/v1', version: 'v1' },
        },
        {
          name: 'pricing.runorca.ai',
          versions: [{ group_version: 'pricing.runorca.ai/v1', version: 'v1' }],
          preferred_version: { group_version: 'pricing.runorca.ai/v1', version: 'v1' },
        },
      ],
    });
    expect(ApiGroupList.safeParse(response.json()).success).toBe(true);
  });

  it('lists the resources in each advertised group', async () => {
    // The second half of discovery: a client that has just read `/apis` can
    // enumerate a group without consulting documentation.
    const app = buildBareDiscoveryApp();

    const policy = await app.inject({ method: 'GET', url: '/apis/policy.runorca.ai/v1' });
    expect(policy.statusCode).toBe(200);
    expect(policy.json()).toEqual({
      kind: 'APIResourceList',
      group_version: 'policy.runorca.ai/v1',
      resources: [
        // Guardrails belong to a workspace; the type catalog is identical for
        // every caller. The workspace never appears in the path either way —
        // `namespaced` is about ownership, not URL shape.
        { name: 'guardrails', namespaced: true, kind: 'Guardrail' },
        { name: 'guardrailtypes', namespaced: false, kind: 'GuardrailType' },
      ],
    });
    expect(ApiResourceList.safeParse(policy.json()).success).toBe(true);

    const pricing = await app.inject({ method: 'GET', url: '/apis/pricing.runorca.ai/v1' });
    expect(pricing.statusCode).toBe(200);
    expect(pricing.json()).toEqual({
      kind: 'APIResourceList',
      group_version: 'pricing.runorca.ai/v1',
      resources: [{ name: 'modelprices', namespaced: false, kind: 'ModelPrice' }],
    });
    expect(ApiResourceList.safeParse(pricing.json()).success).toBe(true);
  });

  it('404s a group version it does not serve', async () => {
    // Registered per group rather than behind `/apis/:group/:version`. A
    // wildcard would answer 200 with an empty resource list for any group name
    // a client invented, turning "not served here" into "served, and empty" —
    // the exact confusion discovery exists to remove.
    const app = buildBareDiscoveryApp();

    for (const url of [
      '/apis/policy.runorca.ai/v2',
      '/apis/cloud.example.com/v1',
      '/apis/policy.example.com/v1',
    ]) {
      expect((await app.inject({ method: 'GET', url })).statusCode, url).toBe(404);
    }
  });

  it('lists every advertised group version as a served route', async () => {
    // Ties the two documents together: anything `/apis` advertises must have a
    // resource list behind it. A group added to the table without a route, or a
    // route without an entry, fails here rather than at a client.
    const app = buildBareDiscoveryApp();
    const groups = (await app.inject({ method: 'GET', url: '/apis' })).json() as {
      groups: Array<{ preferred_version: { group_version: string } }>;
    };

    for (const group of groups.groups) {
      const url = `/apis/${group.preferred_version.group_version}`;
      const response = await app.inject({ method: 'GET', url });
      expect(response.statusCode, url).toBe(200);
      expect((response.json() as { group_version: string }).group_version, url).toBe(
        group.preferred_version.group_version,
      );
    }
  });

  it('requires credentials, like every other route on this listener', async () => {
    // Discovery describes what this deployment serves. That is an answer about
    // the deployment, not a liveness signal, so a caller with no key does not
    // get it — the same line Kubernetes draws between `system:public-info-viewer`
    // and `system:discovery`. The 401 arrives Claude-shaped because `/api` and
    // `/apis` are in `ENVELOPE_PREFIXES`.
    const app = buildPublicApp(options());
    apps.push(app);
    await app.ready();

    for (const url of ['/api', '/apis']) {
      const response = await app.inject({ method: 'GET', url });
      expect(response.statusCode, url).toBe(401);
      expect(response.json(), url).toEqual({
        type: 'error',
        error: { type: 'authentication_error', message: 'unauthenticated' },
        request_id: expect.any(String),
      });
    }
  });

  it('leaves the probes as the whole unauthenticated surface', async () => {
    // The other half of the claim above. Removing `/api` and `/apis` from the
    // allowlist must not have taken the probes with them: a kubelet and a
    // compose healthcheck cannot present a credential.
    const app = buildPublicApp(options());
    apps.push(app);
    await app.ready();

    for (const url of ['/healthz', '/readyz']) {
      expect((await app.inject({ method: 'GET', url })).statusCode, url).toBe(200);
    }
  });

  it('requires credentials for everything else', async () => {
    const app = buildPublicApp(options());
    apps.push(app);
    await app.ready();

    for (const url of ['/v1/agents', '/apis/example.orca.dev/v1/things', '/api/v1/agents']) {
      const response = await app.inject({ method: 'GET', url });
      expect([401, 404], `${url} → ${response.statusCode}`).toContain(response.statusCode);
      expect(response.statusCode, url).not.toBe(200);
    }
  });

  it('is not registered on the internal listener', async () => {
    const { buildInternalApp } = await import('../../src/server.js');
    const { StaticInternalAuthVerifier, staticTokenSource } =
      await import('../../src/auth/internal-auth.js');
    const token = 'test-internal-service-token-at-least-32-chars';
    const app = buildInternalApp(
      options(),
      new StaticInternalAuthVerifier(staticTokenSource(token)),
    );
    apps.push(app);
    await app.ready();

    const response = await app.inject({
      method: 'GET',
      url: '/api',
      headers: { authorization: `Bearer ${token}` },
    });
    expect(response.statusCode).toBe(404);
  });
});

/** Every operation an AppRouter declares, keyed the way the differ keys them. */
function contractOperations(router: AppRouter, into: Set<string> = new Set()): Set<string> {
  for (const value of Object.values(router)) {
    if (isAppRoute(value)) into.add(operationKey(value.method, value.path));
    else contractOperations(value as AppRouter, into);
  }
  return into;
}

describe('contract/route drift', () => {
  /**
   * `route-contract-parity.spec.ts` compares the whole route table against the
   * whole contract. This is the discovery-shaped corner of that comparison,
   * named explicitly so a regression in the set comparison there cannot pass by
   * both sides losing discovery at once.
   *
   * The route side comes from `registeredRoutes`, which reports what Fastify
   * actually registered. Rebuilding the expectation from the same contract the
   * test is checking would make it agree with itself.
   */
  it('serves the discovery routes the contract declares', async () => {
    const app = buildPublicApp(options());
    apps.push(app);
    await app.ready();

    const urls = registeredRoutes(app).map((route) => route.url);
    const declared = contractOperations(publicContract);

    for (const path of [
      '/api',
      '/apis',
      '/apis/policy.runorca.ai/v1',
      '/apis/pricing.runorca.ai/v1',
    ]) {
      expect(urls, path).toContain(path);
      expect(declared, path).toContain(operationKey('GET', path));
    }
  });
});
