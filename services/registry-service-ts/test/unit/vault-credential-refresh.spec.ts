// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  exchangeRefreshToken,
  type CredentialRow,
} from '../../src/api/vault-credentials.routes.js';

const row: CredentialRow = {
  id: 'vcrd_refresh_test',
  deletedAt: null,
  workspaceId: 'ws_refresh_test',
  vaultId: 'vlt_refresh_test',
  displayName: null,
  authType: 'mcp_oauth',
  mcpServerUrl: 'https://mcp.example.test',
  secretName: null,
  networking: {},
  accessSecretRef: 'local:access',
  refreshSecretRef: 'local:refresh',
  tokenEndpoint: 'https://slack.com/api/oauth.v2.access',
  clientId: 'client-id',
  tokenEndpointAuthType: 'client_secret_basic',
  clientSecretRef: 'local:client-secret',
  oauthRefreshLeaseOwner: null,
  oauthRefreshLeaseExpiresAt: null,
  authConfig: {},
  metadata: {},
  archivedAt: null,
  createdAt: new Date('2026-01-01T00:00:00.000Z'),
  updatedAt: new Date('2026-01-01T00:00:00.000Z'),
};

describe('exchangeRefreshToken Slack application errors', () => {
  it('keeps internal_error retryable', async () => {
    const result = await exchangeRefreshToken(
      async () => jsonResponse({ ok: false, error: 'internal_error' }),
      row,
      'refresh-token',
      'client-secret',
      'access-token',
    );

    expect(result.status).toBe('connect_error');
    expect(result.httpResponse).toMatchObject({ status_code: 200, body: '[redacted]' });
  });

  it('treats invalid_refresh_token as definitive', async () => {
    const result = await exchangeRefreshToken(
      async () => jsonResponse({ ok: false, error: 'invalid_refresh_token' }),
      row,
      'refresh-token',
      'client-secret',
      'access-token',
    );

    expect(result.status).toBe('failed');
    expect(result.httpResponse).toMatchObject({ status_code: 200, body: '[redacted]' });
  });
});

function jsonResponse(payload: unknown): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
}
