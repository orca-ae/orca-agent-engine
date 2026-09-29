// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { apiCall, buildClientFromConfig, type OrcaClientConfig } from '../src/client.js';

export async function createTestEnvironment(
  cfg: OrcaClientConfig,
  prefix: string,
): Promise<string> {
  const response = await apiCall(cfg, '/v1/environments', {
    method: 'POST',
    body: JSON.stringify({
      name: `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      config: { type: 'cloud' },
    }),
  });
  if (response.status !== 200) {
    throw new Error(`environment create failed: ${response.status} ${response.text}`);
  }
  return response.json<{ id: string }>().id;
}

export async function deleteTestEnvironment(
  cfg: OrcaClientConfig,
  environmentId: string,
): Promise<void> {
  await apiCall(cfg, `/v1/environments/${environmentId}`, {
    method: 'DELETE',
    body: JSON.stringify({}),
  });
}

export interface ProvisionedWorkspace {
  id: string;
  keyId: string;
  cfg: OrcaClientConfig;
}

/**
 * Create a real workspace plus its own runtime API key, through the live admin
 * API.
 *
 * `seedWorkspaceApiKey()` is not a substitute when a spec needs *two*
 * workspaces: it writes a fixed workspace id and rotates that one key, so
 * calling it twice yields two credentials for the same workspace and
 * invalidates the first. Any isolation assertion built on it would pass while
 * proving nothing.
 */
export async function provisionWorkspace(
  adminCfg: OrcaClientConfig,
  name: string,
): Promise<ProvisionedWorkspace> {
  const workspaceRes = await apiCall(adminCfg, '/v1/organizations/workspaces', {
    method: 'POST',
    body: JSON.stringify({ name }),
  });
  if (workspaceRes.status !== 200) {
    throw new Error(`workspace create failed: ${workspaceRes.status} ${workspaceRes.text}`);
  }
  const workspace = workspaceRes.json<{ id: string }>();
  try {
    const keyRes = await apiCall(adminCfg, `/v1/organizations/workspaces/${workspace.id}/api_keys`, {
      method: 'POST',
      body: JSON.stringify({ name: `${name} runtime` }),
    });
    if (keyRes.status !== 201) {
      throw new Error(`workspace key create failed: ${keyRes.status} ${keyRes.text}`);
    }
    if (keyRes.headers.get('cache-control') !== 'no-store') {
      throw new Error('workspace key create response must set Cache-Control: no-store');
    }
    const key = keyRes.json<{ id: string; key?: string }>();
    if (!key.key) throw new Error('workspace key create response did not return plaintext key');
    return {
      id: workspace.id,
      keyId: key.id,
      cfg: buildClientFromConfig({ apiKey: key.key }),
    };
  } catch (error) {
    await archiveWorkspace(adminCfg, workspace.id).catch(() => undefined);
    throw error;
  }
}

export async function archiveWorkspace(
  adminCfg: OrcaClientConfig,
  workspaceId: string,
): Promise<void> {
  await apiCall(adminCfg, `/v1/organizations/workspaces/${workspaceId}/archive`, {
    method: 'POST',
    body: JSON.stringify({}),
  });
}
