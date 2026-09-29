// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Layer A multi-workspace isolation against the live three-listener Registry.
 *
 * The organization-admin credential is seeded as an offline-bootstrap
 * prerequisite. Everything under that authority boundary—workspace creation,
 * workspace-key issuance, data-plane resource creation, and archive—is driven
 * through real HTTP listeners with real Postgres, S3, and transcript backends.
 */
import { createHash, randomUUID } from 'node:crypto';
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type OrcaClientConfig,
} from '../src/client.js';
import { seedOrganizationAdminApiKey } from '../src/seed.js';
import { createTestEnvironment } from './environment-helpers.js';

interface WorkspaceResponse {
  id: string;
  name: string;
  archived_at: string | null;
}

interface WorkspaceApiKeyResponse {
  id: string;
  workspace_id: string;
  status: 'active' | 'inactive' | 'archived' | 'expired';
  key?: string;
}

interface ProvisionedWorkspace {
  id: string;
  keyId: string;
  cfg: OrcaClientConfig;
}

interface ResourceResponse {
  id: string;
}

interface SkillResponse extends ResourceResponse {
  latest_version: string;
}

interface AgentResponse extends ResourceResponse {
  name: string;
  version: number;
}

type FileResponse = ResourceResponse;

interface MemoryResponse extends ResourceResponse {
  content_sha256: string;
}

const publicBaseURL = process.env['ORCA_BASE_URL'] ?? 'http://localhost:8080';
const adminBaseURL = process.env['ORCA_ADMIN_BASE_URL'] ?? 'http://localhost:8082';
const s3Bucket = process.env['S3_BUCKET'] ?? 'orca-files';
const s3KeyPrefix = normalizeKeyPrefix(process.env['S3_KEY_PREFIX'] ?? 'managed-agents/');
const s3 = new S3Client({
  endpoint: process.env['S3_ENDPOINT'] ?? 'http://localhost:9000',
  region: process.env['S3_REGION'] ?? 'us-east-1',
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env['S3_ACCESS_KEY_ID'] ?? process.env['S3_ACCESS_KEY'] ?? 'minioadmin',
    secretAccessKey:
      process.env['S3_SECRET_ACCESS_KEY'] ?? process.env['S3_SECRET_KEY'] ?? 'minioadmin',
  },
});

describe('Layer A: multi-workspace isolation (live Registry + stores)', () => {
  let adminCfg: OrcaClientConfig;
  let workspaceA: ProvisionedWorkspace | undefined;
  let workspaceB: ProvisionedWorkspace | undefined;
  const runId = randomUUID().replaceAll('-', '').slice(0, 16);

  beforeAll(async () => {
    const seededAdmin = await seedOrganizationAdminApiKey();
    adminCfg = buildClientFromConfig({ baseURL: adminBaseURL, apiKey: seededAdmin.apiKey });
    await ensureStackReachable(adminCfg);
    workspaceA = await provisionWorkspace(adminCfg, `Isolation A ${runId}`);
    workspaceB = await provisionWorkspace(adminCfg, `Isolation B ${runId}`);
    await Promise.all([ensureStackReachable(workspaceA.cfg), ensureStackReachable(workspaceB.cfg)]);
  });

  afterAll(async () => {
    await Promise.all(
      [workspaceA, workspaceB]
        .filter((workspace): workspace is ProvisionedWorkspace => workspace !== undefined)
        .map((workspace) =>
          apiCall(adminCfg, `/v1/organizations/workspaces/${workspace.id}/archive`, {
            method: 'POST',
            body: JSON.stringify({}),
          }).catch(() => undefined),
        ),
    );
  });

  it('keeps organization-admin and workspace credentials on separate listeners', async () => {
    const a = requiredWorkspace(workspaceA);
    const b = requiredWorkspace(workspaceB);
    const adminOnPublic = buildClientFromConfig({
      baseURL: publicBaseURL,
      apiKey: adminCfg.apiKey,
    });
    const workspaceOnAdmin = buildClientFromConfig({
      baseURL: adminBaseURL,
      apiKey: a.cfg.apiKey,
    });

    const [adminDataPlane, workspaceAdminPlane, publicInternalRoute] = await Promise.all([
      apiCall(adminOnPublic, '/v1/agents'),
      apiCall(workspaceOnAdmin, '/v1/organizations/me'),
      apiCall(a.cfg, `/internal/v1/workspaces/${a.id}/sessions/not-a-session/executions:prepare`, {
        method: 'POST',
        body: JSON.stringify({}),
      }),
    ]);
    expect(adminDataPlane.status).toBe(401);
    expect(workspaceAdminPlane.status).toBe(401);
    expect(publicInternalRoute.status).toBe(404);

    const explicitQuery = await apiCall(a.cfg, `/v1/agents?workspace_id=${b.id}`);
    expect(explicitQuery.status).toBe(400);
    const explicitBody = await apiCall(a.cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({ workspace_id: b.id }),
    });
    expect(explicitBody.status).toBe(400);
  });

  it('isolates resource lookup, relationships, file metadata, and memory bytes', async () => {
    const a = requiredWorkspace(workspaceA);
    const b = requiredWorkspace(workspaceB);
    const logicalName = `shared-${runId}`;

    const skill = await createSkill(a.cfg, logicalName, 'workspace A only');

    const agentA = await createAgent(a.cfg, logicalName, [
      { type: 'custom', skill_id: skill.id, version: skill.latest_version },
    ]);
    const agentB = await createAgent(b.cfg, logicalName, []);
    const [environmentA, environmentB] = await Promise.all([
      createTestEnvironment(a.cfg, `isolation-a-${runId}`),
      createTestEnvironment(b.cfg, `isolation-b-${runId}`),
    ]);

    const vault = await createResource(a.cfg, '/v1/vaults', {
      display_name: logicalName,
      metadata: { workspace: 'A' },
    });

    const sharedFileBytes = Buffer.from(`same sha across workspaces ${runId}`, 'utf8');
    const [fileA, fileB] = await Promise.all([
      uploadFile(a.cfg, `${logicalName}.txt`, sharedFileBytes),
      uploadFile(b.cfg, `${logicalName}.txt`, sharedFileBytes),
    ]);
    expect(fileA.id).not.toBe(fileB.id);
    const sharedFileSha = createHash('sha256').update(sharedFileBytes).digest('hex');
    const fileKeyA = fileObjectKey(a.id, sharedFileSha);
    const fileKeyB = fileObjectKey(b.id, sharedFileSha);
    expect(fileKeyA).not.toBe(fileKeyB);
    const [storedFileA, storedFileB] = await Promise.all([
      readObject(fileKeyA),
      readObject(fileKeyB),
    ]);
    expect(storedFileA).toEqual(sharedFileBytes);
    expect(storedFileB).toEqual(sharedFileBytes);

    const [memoryStoreA, memoryStoreB] = await Promise.all([
      createResource(a.cfg, '/v1/memory_stores', { name: logicalName }),
      createResource(b.cfg, '/v1/memory_stores', { name: logicalName }),
    ]);

    const memoryABytes = Buffer.from(`workspace-A-${runId}`, 'utf8');
    const memoryBBytes = Buffer.from(`workspace-B-${runId}`, 'utf8');
    const [memoryA, memoryB] = await Promise.all([
      writeMemory(a.cfg, memoryStoreA.id, 'shared.txt', memoryABytes),
      writeMemory(b.cfg, memoryStoreB.id, 'shared.txt', memoryBBytes),
    ]);

    const memoryLiveKeyA = memoryObjectKey(a.id, memoryStoreA.id, 'live/shared.txt');
    const memoryLiveKeyB = memoryObjectKey(b.id, memoryStoreB.id, 'live/shared.txt');
    const memoryVersionKeyA = memoryObjectKey(
      a.id,
      memoryStoreA.id,
      `versions/${memoryA.content_sha256}`,
    );
    const memoryVersionKeyB = memoryObjectKey(
      b.id,
      memoryStoreB.id,
      `versions/${memoryB.content_sha256}`,
    );
    expect(memoryLiveKeyA).not.toBe(memoryLiveKeyB);
    const [storedMemoryLiveA, storedMemoryLiveB, storedMemoryVersionA, storedMemoryVersionB] =
      await Promise.all([
        readObject(memoryLiveKeyA),
        readObject(memoryLiveKeyB),
        readObject(memoryVersionKeyA),
        readObject(memoryVersionKeyB),
      ]);
    expect(storedMemoryLiveA).toEqual(memoryABytes);
    expect(storedMemoryLiveB).toEqual(memoryBBytes);
    expect(storedMemoryVersionA).toEqual(memoryABytes);
    expect(storedMemoryVersionB).toEqual(memoryBBytes);

    const [memoryAContent, memoryBContent] = await Promise.all([
      apiCall(a.cfg, `/v1/memory_stores/${memoryStoreA.id}/memories/${memoryA.id}?view=full`),
      apiCall(b.cfg, `/v1/memory_stores/${memoryStoreB.id}/memories/${memoryB.id}?view=full`),
    ]);
    expect(memoryAContent.status).toBe(200);
    expect(memoryBContent.status).toBe(200);
    expect(memoryAContent.json<{ content: string | null }>().content).toBe(
      memoryABytes.toString('utf8'),
    );
    expect(memoryBContent.json<{ content: string | null }>().content).toBe(
      memoryBBytes.toString('utf8'),
    );

    const session = await createResource(a.cfg, '/v1/sessions', {
      agent_id: agentA.id,
      environment_id: environmentA,
      vault_ids: [vault.id],
      resources: [
        { type: 'file', file_id: fileA.id, mount_path: '/mnt/shared.txt' },
        { type: 'memory_store', memory_store_id: memoryStoreA.id },
      ],
    });

    const hiddenResources = [
      ['/v1/agents', `/v1/agents/${agentA.id}`, agentA.id],
      ['/v1/skills', `/v1/skills/${skill.id}`, skill.id],
      ['/v1/vaults', `/v1/vaults/${vault.id}`, vault.id],
      ['/v1/files', `/v1/files/${fileA.id}`, fileA.id],
      ['/v1/memory_stores', `/v1/memory_stores/${memoryStoreA.id}`, memoryStoreA.id],
      ['/v1/environments', `/v1/environments/${environmentA}`, environmentA],
      ['/v1/sessions', `/v1/sessions/${session.id}`, session.id],
    ] as const;
    for (const [listPath, getPath, id] of hiddenResources) {
      const [list, get] = await Promise.all([apiCall(b.cfg, listPath), apiCall(b.cfg, getPath)]);
      expect(list.status, `GET ${listPath}`).toBe(200);
      expect(list.json<{ data: ResourceResponse[] }>().data.map((item) => item.id)).not.toContain(
        id,
      );
      expect(get.status, `GET ${getPath}`).toBe(404);
    }

    const [agentBFromA, fileBFromA, memoryBFromA] = await Promise.all([
      apiCall(a.cfg, `/v1/agents/${agentB.id}`),
      apiCall(a.cfg, `/v1/files/${fileB.id}`),
      apiCall(a.cfg, `/v1/memory_stores/${memoryStoreB.id}`),
    ]);
    expect(agentBFromA.status).toBe(404);
    expect(fileBFromA.status).toBe(404);
    expect(memoryBFromA.status).toBe(404);

    const crossSkillAgent = await apiCall(b.cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify(
        agentBody(`cross-skill-${runId}`, [
          { type: 'custom', skill_id: skill.id, version: skill.latest_version },
        ]),
      ),
    });
    expect(crossSkillAgent.status).toBe(400);

    const crossBindings = [
      { agent_id: agentA.id, environment_id: environmentB },
      {
        agent_id: agentB.id,
        environment_id: environmentB,
        resources: [{ type: 'file', file_id: fileA.id, mount_path: '/mnt/cross-file.txt' }],
      },
      {
        agent_id: agentB.id,
        environment_id: environmentB,
        resources: [{ type: 'memory_store', memory_store_id: memoryStoreA.id }],
      },
      { agent_id: agentB.id, environment_id: environmentB, vault_ids: [vault.id] },
      { agent_id: agentB.id, environment_id: environmentA },
    ];
    for (const body of crossBindings) {
      const response = await apiCall(b.cfg, '/v1/sessions', {
        method: 'POST',
        body: JSON.stringify(body),
      });
      expect([400, 404]).toContain(response.status);
    }
    const sessionsB = await apiCall(b.cfg, '/v1/sessions');
    expect(sessionsB.status).toBe(200);
    expect(sessionsB.json<{ data: ResourceResponse[] }>().data).toHaveLength(0);
  });

  it('archives a workspace terminally without affecting its sibling workspace', async () => {
    const a = requiredWorkspace(workspaceA);
    const b = requiredWorkspace(workspaceB);
    const archive = await apiCall(adminCfg, `/v1/organizations/workspaces/${a.id}/archive`, {
      method: 'POST',
      body: JSON.stringify({}),
    });
    expect(archive.status).toBe(200);
    expect(archive.json<WorkspaceResponse>()).toMatchObject({
      id: a.id,
    });
    expect(archive.json<WorkspaceResponse>().archived_at).toBeTypeOf('string');

    const [archivedDataPlane, siblingDataPlane, archivedKey] = await Promise.all([
      apiCall(a.cfg, '/v1/agents'),
      apiCall(b.cfg, '/v1/agents'),
      apiCall(adminCfg, `/v1/organizations/api_keys/${a.keyId}`),
    ]);
    expect(archivedDataPlane.status).toBe(401);
    expect(siblingDataPlane.status).toBe(200);
    expect(archivedKey.status).toBe(200);
    expect(archivedKey.json<WorkspaceApiKeyResponse>().status).toBe('archived');

    const replacementKey = await apiCall(
      adminCfg,
      `/v1/organizations/workspaces/${a.id}/api_keys`,
      {
        method: 'POST',
        body: JSON.stringify({ name: 'must not be created' }),
      },
    );
    expect(replacementKey.status).toBe(409);
  });
});

function requiredWorkspace(workspace: ProvisionedWorkspace | undefined): ProvisionedWorkspace {
  if (!workspace) throw new Error('workspace fixture was not provisioned');
  return workspace;
}

async function provisionWorkspace(
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
  const workspace = workspaceRes.json<WorkspaceResponse>();
  try {
    const keyRes = await apiCall(
      adminCfg,
      `/v1/organizations/workspaces/${workspace.id}/api_keys`,
      {
        method: 'POST',
        body: JSON.stringify({ name: `${name} runtime` }),
      },
    );
    if (keyRes.status !== 201) {
      throw new Error(`workspace key create failed: ${keyRes.status} ${keyRes.text}`);
    }
    if (keyRes.headers.get('cache-control') !== 'no-store') {
      throw new Error('workspace key create response must set Cache-Control: no-store');
    }
    const key = keyRes.json<WorkspaceApiKeyResponse>();
    if (!key.key) throw new Error('workspace key create response did not return plaintext key');
    return {
      id: workspace.id,
      keyId: key.id,
      cfg: buildClientFromConfig({ baseURL: publicBaseURL, apiKey: key.key }),
    };
  } catch (error) {
    await apiCall(adminCfg, `/v1/organizations/workspaces/${workspace.id}/archive`, {
      method: 'POST',
      body: JSON.stringify({}),
    }).catch(() => undefined);
    throw error;
  }
}

async function createResource(
  cfg: OrcaClientConfig,
  path: string,
  body: Record<string, unknown>,
): Promise<ResourceResponse> {
  const response = await apiCall(cfg, path, {
    method: 'POST',
    body: JSON.stringify(body),
  });
  if (response.status !== 200) {
    throw new Error(`resource create ${path} failed: ${response.status} ${response.text}`);
  }
  return response.json<ResourceResponse>();
}

async function createSkill(
  cfg: OrcaClientConfig,
  directory: string,
  instructions: string,
): Promise<SkillResponse> {
  const form = new FormData();
  form.set(
    'files[]',
    new Blob(
      [
        [
          '---',
          `name: ${directory}`,
          'description: multi-workspace isolation fixture',
          '---',
          '',
          instructions,
        ].join('\n'),
      ],
      { type: 'text/markdown' },
    ),
    `${directory}/SKILL.md`,
  );
  const response = await apiCall(cfg, '/v1/skills', { method: 'POST', body: form });
  if (response.status !== 200) {
    throw new Error(`skill create failed: ${response.status} ${response.text}`);
  }
  return response.json<SkillResponse>();
}

async function createAgent(
  cfg: OrcaClientConfig,
  name: string,
  skills: Array<{ type: 'custom'; skill_id: string; version: string }>,
): Promise<AgentResponse> {
  const response = await apiCall(cfg, '/v1/agents', {
    method: 'POST',
    body: JSON.stringify(agentBody(name, skills)),
  });
  if (response.status !== 200) {
    throw new Error(`agent create failed: ${response.status} ${response.text}`);
  }
  return response.json<AgentResponse>();
}

function agentBody(
  name: string,
  skills: Array<{ type: 'custom'; skill_id: string; version: string }>,
): Record<string, unknown> {
  return {
    name,
    model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
    system: '',
    tools: [],
    mcp_servers: [],
    skills,
    metadata: {},
  };
}

async function uploadFile(
  cfg: OrcaClientConfig,
  filename: string,
  content: Buffer,
): Promise<FileResponse> {
  const form = new FormData();
  form.set('file', new Blob([content.toString('utf8')], { type: 'text/plain' }), filename);
  form.set('purpose', 'agent');
  const response = await apiCall(cfg, '/v1/files', { method: 'POST', body: form });
  if (response.status !== 200) {
    throw new Error(`file upload failed: ${response.status} ${response.text}`);
  }
  return response.json<FileResponse>();
}

async function writeMemory(
  cfg: OrcaClientConfig,
  storeId: string,
  path: string,
  content: Buffer,
): Promise<MemoryResponse> {
  const response = await apiCall(cfg, `/v1/memory_stores/${storeId}/memories`, {
    method: 'POST',
    body: JSON.stringify({
      path: path.startsWith('/') ? path : `/${path}`,
      content: content.toString('utf8'),
    }),
  });
  if (response.status !== 200) {
    throw new Error(`memory write failed: ${response.status} ${response.text}`);
  }
  return response.json<MemoryResponse>();
}

function normalizeKeyPrefix(prefix: string): string {
  if (prefix === '') return '';
  return prefix.endsWith('/') ? prefix : `${prefix}/`;
}

function fileObjectKey(workspaceId: string, sha256: string): string {
  return (
    `${s3KeyPrefix}workspaces/${workspaceId}/files/blobs/` +
    `${sha256.slice(0, 2)}/${sha256.slice(2, 4)}/${sha256}/content`
  );
}

function memoryObjectKey(workspaceId: string, storeId: string, suffix: string): string {
  return `${s3KeyPrefix}workspaces/${workspaceId}/memory-stores/${storeId}/${suffix}`;
}

async function readObject(key: string): Promise<Buffer> {
  const response = await s3.send(new GetObjectCommand({ Bucket: s3Bucket, Key: key }));
  if (!response.Body) throw new Error(`S3 object ${key} has no body`);
  return Buffer.from(await response.Body.transformToByteArray());
}
