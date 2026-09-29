// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { customAlphabet } from 'nanoid';
import { fingerprintApiKey, hashApiKey } from '../../src/auth/api-key.js';
import {
  newOrganizationObservabilitySettings,
  newWorkspaceObservabilitySettings,
} from '../../src/domain/agent-observability-settings.js';
import {
  agentObservabilityOrganizationSettings,
  agentObservabilityWorkspaceSettings,
  apiKeys,
  organizations,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';

const nano = customAlphabet('abcdefghijklmnopqrstuvwxyz0123456789', 12);
export const TEST_ORGANIZATION_ID = 'org_registry_integration_tests';

export function uniqueWorkspace(prefix: string): string {
  return `ws_${prefix}_${nano()}`;
}

export async function createTestWorkspace(db: DbClient, workspaceId: string): Promise<void> {
  const now = new Date();
  await db
    .insert(organizations)
    .values({
      id: TEST_ORGANIZATION_ID,
      name: 'Registry integration tests',
      status: 'active',
    })
    .onConflictDoNothing();
  await db
    .insert(agentObservabilityOrganizationSettings)
    .values(newOrganizationObservabilitySettings(TEST_ORGANIZATION_ID, now))
    .onConflictDoNothing();
  await db
    .insert(workspaces)
    .values({
      id: workspaceId,
      organizationId: TEST_ORGANIZATION_ID,
      name: workspaceId,
      status: 'active',
      createdBy: 'integration-test',
    })
    .onConflictDoUpdate({
      target: workspaces.id,
      set: { status: 'active', archivedAt: null, updatedAt: new Date() },
    });
  await db
    .insert(agentObservabilityWorkspaceSettings)
    .values(newWorkspaceObservabilitySettings(TEST_ORGANIZATION_ID, workspaceId, now))
    .onConflictDoNothing();
}

export async function createTestApiKey(db: DbClient, workspaceId: string): Promise<string> {
  await createTestWorkspace(db, workspaceId);
  const plaintext = `orca_test_${nano()}`;
  const hashed = await hashApiKey(plaintext);
  await db.insert(apiKeys).values({
    id: `apikey_${nano()}`,
    workspaceId,
    hashedKey: hashed,
    keyFingerprint: fingerprintApiKey(plaintext),
    principal: 'test-principal',
    scopes: [
      'workspace.agents.create',
      'workspace.agents.alter',
      'workspace.agents.describe',
      'workspace.agents.delete',
      'workspace.sessions.create',
      'workspace.sessions.alter',
      'workspace.sessions.describe',
      'workspace.sessions.delete',
      'workspace.agentTriggers.create',
      'workspace.agentTriggers.alter',
      'workspace.agentTriggers.describe',
      'workspace.agentTriggers.delete',
    ],
    revokedAt: null,
  });
  return plaintext;
}

export async function createTestAgent(baseURL: string, apiKey: string): Promise<string> {
  const res = await fetch(`${baseURL}/v1/agents`, {
    method: 'POST',
    headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
    body: JSON.stringify({
      name: `agt-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
      tools: [],
      mcp_servers: [],
      skills: [],
      metadata: {},
    }),
  });
  if (res.status !== 200) throw new Error(`agent create failed: ${res.status} ${await res.text()}`);
  return ((await res.json()) as { id: string }).id;
}

export async function createTestEnvironment(baseURL: string, apiKey: string): Promise<string> {
  const res = await fetch(`${baseURL}/v1/environments`, {
    method: 'POST',
    headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
    body: JSON.stringify({
      name: `env-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      config: { type: 'cloud' },
    }),
  });
  if (res.status !== 200) {
    throw new Error(`environment create failed: ${res.status} ${await res.text()}`);
  }
  return ((await res.json()) as { id: string }).id;
}

export async function createTestSession(
  baseURL: string,
  apiKey: string,
  agentId: string,
  environmentId?: string,
): Promise<string> {
  const resolvedEnvironmentId = environmentId ?? (await createTestEnvironment(baseURL, apiKey));
  const res = await fetch(`${baseURL}/v1/sessions`, {
    method: 'POST',
    headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
    body: JSON.stringify({ agent_id: agentId, environment_id: resolvedEnvironmentId }),
  });
  if (res.status !== 200)
    throw new Error(`session create failed: ${res.status} ${await res.text()}`);
  return ((await res.json()) as { id: string }).id;
}
