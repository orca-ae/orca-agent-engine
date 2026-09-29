// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FileStore } from '@orca/file-store';
import { InMemorySkillStore } from '@orca/skill-store';
import type { TranscriptStore } from '@orca/transcript-store';
import { isAppRoute } from '@ts-rest/core';
import { afterEach, describe, expect, it } from 'vitest';
import type { SessionJwtMinter } from '../../src/auth/session-jwt.js';
import { adminAgentObservabilityContract } from '../../src/contracts/agent-observability.contract.js';
import { platformAgentObservabilityContract } from '../../src/contracts/platform-agent-observability.contract.js';
import { StaticInternalAuthVerifier, staticTokenSource } from '../../src/auth/internal-auth.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  buildAdminApp,
  buildInternalApp,
  buildPublicApp,
  registeredRoutes,
  type BuildAppOptions,
} from '../../src/server.js';

const apps: Array<ReturnType<typeof buildPublicApp>> = [];
const INTERNAL_TOKEN = 'test-internal-service-token-at-least-32-chars';

afterEach(async () => {
  await Promise.all(apps.splice(0).map(async (app) => app.close()));
});

function options(): BuildAppOptions {
  return {
    db: {} as DbClient,
    oidc: { allowedIssuers: [], audience: 'test' },
    store: {} as TranscriptStore,
    sse: { bufferSize: 1, dropAgeMs: 1, heartbeatMs: 1 },
    jwtMinter: {} as SessionJwtMinter,
    fileStore: {} as FileStore,
    skillStore: new InMemorySkillStore(),
  };
}

describe('admin agent observability route-contract parity', () => {
  it('serves exactly the standalone contract paths on admin listener only', async () => {
    const publicApp = buildPublicApp(options());
    const internalApp = buildInternalApp(
      options(),
      new StaticInternalAuthVerifier(staticTokenSource(INTERNAL_TOKEN)),
    );
    const adminApp = buildAdminApp({
      db: options().db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: options().store,
    });
    apps.push(publicApp, internalApp, adminApp);
    await Promise.all([publicApp.ready(), internalApp.ready(), adminApp.ready()]);

    const declared = [
      ...Object.values(adminAgentObservabilityContract),
      ...Object.values(platformAgentObservabilityContract),
    ]
      .filter(isAppRoute)
      .map((route) => ({ method: route.method, url: route.path }))
      .sort((left, right) => left.url.localeCompare(right.url));
    const served = registeredRoutes(adminApp)
      .filter((route) => route.url.includes('agent_observability'))
      .sort((left, right) => left.url.localeCompare(right.url));

    expect(declared).toEqual([
      { method: 'GET', url: '/v1/organizations/agent_observability' },
      { method: 'PUT', url: '/v1/organizations/agent_observability' },
      {
        method: 'POST',
        url: '/v1/organizations/agent_observability:disable',
      },
      {
        method: 'POST',
        url: '/v1/organizations/agent_observability:rotate_credentials',
      },
      { method: 'PUT', url: '/v1/organizations/agent_observability/capture_ceiling' },
      {
        method: 'GET',
        url: '/v1/organizations/workspaces/:workspaceId/agent_observability',
      },
      {
        method: 'PUT',
        url: '/v1/organizations/workspaces/:workspaceId/agent_observability',
      },
      {
        method: 'POST',
        url: '/v1/organizations/workspaces/:workspaceId/agent_observability:rotate_credentials',
      },
      { method: 'GET', url: '/v1/platform/agent_observability' },
      { method: 'PUT', url: '/v1/platform/agent_observability' },
    ]);
    expect(served).toEqual(declared);
    for (const route of declared) {
      expect(publicApp.hasRoute(route)).toBe(false);
      expect(internalApp.hasRoute(route)).toBe(false);
    }
  });
});
