// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { TranscriptStore } from '@orca/transcript-store';
import Fastify from 'fastify';
import { describe, expect, it, vi } from 'vitest';
import { registerAdminRoutes } from '../../src/api/admin.routes.js';
import { executeOrganizationAgentObservabilityPut } from '../../src/domain/agent-observability-organization-service.js';
import { executeWorkspaceAgentObservabilityPut } from '../../src/domain/agent-observability-workspace-service.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';

vi.mock('../../src/domain/agent-observability-organization-service.js', () => ({
  executeOrganizationAgentObservabilityPut: vi.fn(async () => ({ kind: 'unavailable' })),
}));
vi.mock('../../src/domain/agent-observability-workspace-service.js', () => ({
  executeWorkspaceAgentObservabilityPut: vi.fn(async () => ({ kind: 'unavailable' })),
}));

describe('raw IO admin authority', () => {
  it.each([[], ['observability:read'], ['observability:write'], ['org:admin']])(
    'retains the existing organization/workspace write scope gate: %j',
    async (...scopes: string[]) => {
      vi.mocked(executeOrganizationAgentObservabilityPut).mockClear();
      vi.mocked(executeWorkspaceAgentObservabilityPut).mockClear();
      const app = Fastify();
      app.addHook('preHandler', async (req) => {
        req.adminAuth = {
          organizationId: 'org_raw',
          principal: 'admin',
          authMethod: 'admin-api-key',
          scopes,
        };
      });
      registerAdminRoutes(app, {} as DbClient, {} as TranscriptStore);
      try {
        const body = {
          target: {
            adapter_type: 'otlp_http',
            endpoint_kind: 'traces_endpoint',
            endpoint_class: 'public',
            endpoint_url: 'https://collector.example/v1/traces',
            external_project_id: null,
          },
          config: {
            semantic_profile: 'otel_genai',
            protocol: 'http/json',
            compression: 'none',
            timeout_ms: 5000,
            environment: null,
            release: null,
            capture_mode: 'raw_io',
            sample_rate: 1,
          },
          capture_ceiling: 'raw_io',
        };
        for (const [url, payload, execute] of [
          ['/v1/organizations/agent_observability', body, executeOrganizationAgentObservabilityPut],
          [
            '/v1/organizations/workspaces/ws_raw/agent_observability',
            { ...body, mode: 'custom' },
            executeWorkspaceAgentObservabilityPut,
          ],
        ] as const) {
          const response = await app.inject({
            method: 'PUT',
            url,
            payload,
            headers: { 'idempotency-key': 'raw-io-test', 'if-match': '"orca-aos-v1-current"' },
          });
          if (scopes.includes('observability:write') || scopes.includes('org:admin')) {
            // Reaching the executor proves parsing succeeded; its failure still fails closed.
            expect(response.statusCode).toBe(503);
            expect(execute).toHaveBeenCalledWith(
              expect.objectContaining({
                organizationId: 'org_raw',
                request: expect.objectContaining({
                  captureCeiling: 'raw_io',
                  config: expect.objectContaining({ captureMode: 'raw_io' }),
                }),
              }),
            );
          } else {
            expect(response.statusCode).toBe(403);
            expect(execute).not.toHaveBeenCalled();
          }
        }
      } finally {
        await app.close();
      }
    },
  );
});
