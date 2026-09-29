// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { beforeAll, describe, expect, it } from 'vitest';
import { seedOrganizationAdminApiKey } from '../src/seed.js';

const here = dirname(fileURLToPath(import.meta.url));
const defaultTokenFile = resolve(here, '../../../services/dev/run/internal-service-token');
const internalBaseUrl = process.env['REGISTRY_INTERNAL_BASE_URL'] ?? 'http://localhost:8081';
const internalPath = '/internal/v1/workspaces/invalid/sessions/invalid/state';

let internalToken: string;
let adminKey: string;

async function callInternal(headers: Record<string, string> = {}) {
  return fetch(`${internalBaseUrl}${internalPath}`, {
    method: 'PATCH',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify({ status: 'idle' }),
  });
}

describe('Registry internal API static-token auth', () => {
  beforeAll(async () => {
    const tokenFile = process.env['INTERNAL_SERVICE_TOKEN_FILE'] ?? defaultTokenFile;
    const [token, seededAdmin] = await Promise.all([
      readFile(tokenFile, 'utf8'),
      seedOrganizationAdminApiKey(),
    ]);
    internalToken = token.trim();
    adminKey = seededAdmin.apiKey;
    expect(internalToken.length).toBeGreaterThanOrEqual(32);
  });

  it('keeps health probes unauthenticated', async () => {
    const response = await fetch(`${internalBaseUrl}/healthz`);
    expect(response.status).toBe(200);
  });

  it('rejects missing, workspace, admin, and invalid bearer credentials', async () => {
    const workspaceKey = process.env['ORCA_E2E_PLAINTEXT_KEY'] ?? 'orca_dev_e2e_local_stack';
    await expect(callInternal()).resolves.toMatchObject({ status: 401 });
    await expect(callInternal({ 'x-api-key': workspaceKey })).resolves.toMatchObject({
      status: 401,
    });
    await expect(callInternal({ authorization: `Bearer ${adminKey}` })).resolves.toMatchObject({
      status: 401,
    });
    await expect(callInternal({ authorization: `Bearer ${workspaceKey}` })).resolves.toMatchObject({
      status: 401,
    });
  });

  it('accepts the generated internal service token', async () => {
    const response = await callInternal({ authorization: `Bearer ${internalToken}` });
    // Invalid route identifiers prove the request reached the handler after
    // authentication without mutating any real session.
    expect(response.status).toBe(400);
  });
});
