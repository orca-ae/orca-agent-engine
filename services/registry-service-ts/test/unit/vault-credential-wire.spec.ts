// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { toCredentialApi } from '../../src/api/vault-credentials.routes.js';

type CredentialRow = Parameters<typeof toCredentialApi>[0];

function row(overrides: Partial<CredentialRow>): CredentialRow {
  return {
    id: 'vcrd_test',
    workspaceId: 'ws_test',
    vaultId: 'vlt_test',
    displayName: null,
    authType: 'static_bearer',
    provider: null,
    scheme: null,
    logicalId: null,
    resolutionVersion: null,
    mcpServerUrl: 'https://mcp.example.com',
    secretName: null,
    networking: {},
    accessSecretRef: 'secret:access',
    refreshSecretRef: null,
    tokenEndpoint: null,
    clientId: null,
    tokenEndpointAuthType: null,
    clientSecretRef: null,
    authConfig: {},
    metadata: {},
    archivedAt: null,
    createdAt: new Date('2026-07-23T00:00:00Z'),
    updatedAt: new Date('2026-07-23T00:00:00Z'),
    ...overrides,
  } as CredentialRow;
}

describe('vault credential public projection', () => {
  it('returns provider binding metadata without secret material or its reference', () => {
    const projected = toCredentialApi(
      row({
        authType: 'provider',
        provider: 'vertex',
        scheme: 'gcp-service-account',
        logicalId: 'llm:vertex',
        resolutionVersion: 'revision-1',
        mcpServerUrl: null,
      }),
    );

    expect(projected.auth).toEqual({
      type: 'provider',
      provider: 'vertex',
      scheme: 'gcp-service-account',
      logical_id: 'llm:vertex',
      version: 'revision-1',
    });
    expect(JSON.stringify(projected)).not.toContain('secret:access');
    expect(JSON.stringify(projected)).not.toContain('mcp_server_url');
  });

  it('returns required environment-variable injection booleans without secrets', () => {
    const projected = toCredentialApi(
      row({
        authType: 'environment_variable',
        mcpServerUrl: null,
        secretName: 'OPENAI_API_KEY',
        networking: { type: 'limited', allowed_hosts: ['api.openai.com'] },
        authConfig: { injection_location: { header: false, body: true } },
      }),
    );

    expect(projected.auth).toEqual({
      type: 'environment_variable',
      secret_name: 'OPENAI_API_KEY',
      networking: { type: 'limited', allowed_hosts: ['api.openai.com'] },
      injection_location: { header: false, body: true },
    });
    expect(JSON.stringify(projected)).not.toContain('secret:access');
  });

  it('round-trips non-secret OAuth expiry, resource, scope, and endpoint auth type', () => {
    const projected = toCredentialApi(
      row({
        authType: 'mcp_oauth',
        refreshSecretRef: 'secret:refresh',
        tokenEndpoint: 'https://auth.example.com/token',
        clientId: 'client-id',
        tokenEndpointAuthType: 'client_secret_post',
        clientSecretRef: 'secret:client',
        authConfig: {
          expires_at: '2099-12-31T23:59:59.000Z',
          resource: 'https://mcp.example.com',
          scope: 'read write',
        },
      }),
    );

    expect(projected.auth).toEqual({
      type: 'mcp_oauth',
      mcp_server_url: 'https://mcp.example.com',
      expires_at: '2099-12-31T23:59:59.000Z',
      refresh: {
        token_endpoint: 'https://auth.example.com/token',
        client_id: 'client-id',
        token_endpoint_auth: { type: 'client_secret_post' },
        resource: 'https://mcp.example.com',
        scope: 'read write',
      },
    });
    expect(JSON.stringify(projected)).not.toContain('secret:refresh');
    expect(JSON.stringify(projected)).not.toContain('secret:client');
  });
});
