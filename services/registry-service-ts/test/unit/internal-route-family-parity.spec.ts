// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FileStore } from '@orca/file-store';
import { InMemorySkillStore } from '@orca/skill-store';
import type { TranscriptStore } from '@orca/transcript-store';
import { afterEach, describe, expect, it } from 'vitest';
import { internalRouteCallers, type InternalAuthVerifier } from '../../src/auth/internal-auth.js';
import type { SessionJwtMinter } from '../../src/auth/session-jwt.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  buildInternalApp,
  registeredRoutes,
  type BuildAppOptions,
  type RouteRecord,
} from '../../src/server.js';

const apps: Array<ReturnType<typeof buildInternalApp>> = [];

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

const verifier: InternalAuthVerifier = { verify: async () => null };

function operation(route: RouteRecord): string {
  return `${route.method} ${route.url}`;
}

type WorkloadCaller = 'harness' | 'ai-gateway' | 'observability-exporter';

const EXPECTED_ROUTE_CALLERS: Record<string, readonly WorkloadCaller[]> = {
  'GET /internal/v1/guardrails/effective': ['ai-gateway'],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/context/resolve':
    ['observability-exporter'],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/secret/resolve':
    ['observability-exporter'],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/mcp-destination/resolve': [
    'ai-gateway',
  ],
  'GET /internal/v1/workspaces/:workspaceId/sessions/:sessionId/execution-owner': ['harness'],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/executions:prepare': ['harness'],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/harness-state': ['harness'],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/harness-turn': ['harness'],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/git-credentials/:id/resolve': [
    'harness',
  ],
  'GET /internal/environments/:id': ['harness'],
  'POST /internal/environments/:id/verify-key': ['harness'],
  'POST /internal/environments/claims/reap': ['harness'],
  'PUT /internal/environments/:id/claim': ['harness'],
  'POST /internal/environments/:id/claim/heartbeat': ['harness'],
  'POST /internal/environments/:id/claim/release': ['harness'],
  'GET /internal/environments/:id/claim': ['harness'],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/vault-credentials/:id/resolve': [
    'ai-gateway',
  ],
  'PATCH /internal/v1/workspaces/:workspaceId/sessions/:id/state': ['harness'],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:id/usage': ['harness', 'ai-gateway'],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:id/guardrail-subject-window': ['harness'],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:id/guardrail-state': [
    'harness',
    'ai-gateway',
  ],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:id/mint-jwt': ['harness'],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/files': ['harness'],
  'GET /internal/v1/workspaces/:workspaceId/sessions/:sessionId/memory-stores/:storeId/memories': [
    'harness',
  ],
  'GET /internal/v1/workspaces/:workspaceId/sessions/:sessionId/memory-stores/:storeId/memories/:memoryId/content':
    ['harness'],
  'GET /internal/v1/workspaces/:workspaceId/sessions/:sessionId/memory-stores/:storeId/memory-versions':
    ['harness'],
  'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/memory-stores/:storeId/memory-versions':
    ['harness'],
};

describe('internal workload route-capability parity', () => {
  it('classifies every registered non-probe internal route exactly once', async () => {
    const app = buildInternalApp(options(), verifier);
    apps.push(app);
    await app.ready();

    const routes = registeredRoutes(app).filter((route) => route.url.startsWith('/internal/'));
    expect(routes).toHaveLength(Object.keys(EXPECTED_ROUTE_CALLERS).length);

    const classified = routes.map((route) => ({
      ...route,
      callers: internalRouteCallers(route.url, route.method),
    }));
    expect(classified.filter((route) => route.callers === null).map(operation)).toEqual([]);
    expect(
      Object.fromEntries(classified.map((route) => [operation(route), route.callers])),
    ).toEqual(EXPECTED_ROUTE_CALLERS);

    const exporter = classified
      .filter((route) => route.callers?.includes('observability-exporter'))
      .map(operation)
      .sort();
    expect(exporter).toEqual([
      'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/context/resolve',
      'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/secret/resolve',
    ]);

    const aiGateway = classified
      .filter((route) => route.callers?.includes('ai-gateway'))
      .map(operation)
      .sort();
    expect(aiGateway).toEqual([
      'GET /internal/v1/guardrails/effective',
      'POST /internal/v1/workspaces/:workspaceId/sessions/:id/guardrail-state',
      'POST /internal/v1/workspaces/:workspaceId/sessions/:id/usage',
      'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/mcp-destination/resolve',
      'POST /internal/v1/workspaces/:workspaceId/sessions/:sessionId/vault-credentials/:id/resolve',
    ]);

    const harness = classified
      .filter((route) => route.callers?.includes('harness'))
      .map(operation)
      .sort();
    expect(harness).toEqual(
      Object.entries(EXPECTED_ROUTE_CALLERS)
        .filter(([, callers]) => callers.includes('harness'))
        .map(([route]) => route)
        .sort(),
    );

    for (const route of routes) {
      expect(internalRouteCallers(route.url, 'DELETE'), operation(route)).toBeNull();
    }
  });

  it('fails closed for unknown routes and known paths with a wrong method', () => {
    expect(
      internalRouteCallers(
        '/internal/v1/workspaces/ws_context/sessions/ses_context/not-a-real-route',
        'POST',
      ),
    ).toBeNull();
    for (const method of ['GET', 'PUT'] as const) {
      expect(
        internalRouteCallers(
          '/internal/v1/workspaces/ws_context/sessions/ses_context/agent-observability/context/resolve',
          method,
        ),
      ).toBeNull();
    }
  });
});
