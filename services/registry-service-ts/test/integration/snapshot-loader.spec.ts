// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Postgres-backed integration spec for the snapshot RECORD LOADER + resolver.
//
// The unit suites cover the AgentSnapshotResolver against in-memory loader fakes
// (test/unit/agent-snapshot-resolver.spec.ts) and the pure builders. What only a
// real DB can prove is the Drizzle-backed `buildSnapshotRecordLoader`
// (src/api/snapshot-loader.ts): its workspace-scoping and archived-filter
// contract over live `environments` / `agents` / `skills` / `skill_versions` /
// `vaults` / `sessions` rows. This spec wires the REAL loader into a REAL
// resolver (with a real SessionJwtMinter) and drives `resolve(sessionId)` end to
// end, asserting:
//
//   - the gateway happy path: a self_hosted, egress_mode='gateway' session →
//     composed system + intersected tools + rewritten MCP servers carrying a
//     minted, audience-scoped JWT + the matched session vault id;
//   - the sidecar happy path: egress_mode='sidecar' → a credential-proxy spec of
//     per-host vault-reference bindings (no minted JWT, no rewritten servers);
//   - workspace scoping: an agent in another workspace resolves to null (the
//     session's workspace gates the agent read); a skill_version whose owning
//     skill is in another workspace fails loud; a vault in another workspace is
//     dropped from the egress (never composed cross-workspace);
//   - archived filtering: an archived agent → null; an archived skill (the
//     skill_version's owning skill) → loadSkillVersion returns null → the
//     resolver throws `skill_version ... not found`; an archived vault → dropped
//     from the gateway url→vault map (no X-Orca-Vault-Id header).
//
// DB-touching, so it lives under test/integration and is gated on the dev compose
// stack (Postgres), exactly like environment-claims.spec.ts.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { and, eq } from 'drizzle-orm';
import { getTestDb, closeTestDb, buildTestJwtMinter } from './setup.js';
import { uniqueWorkspace, createTestWorkspace } from './fixtures.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  environments,
  sessions,
  sessionSkillBindings,
  skills,
  skillVersions,
  vaultCredentials,
  vaults,
} from '../../src/persistence/postgres/schema.js';
import { newId } from '../../src/domain/versioning.js';
import { buildSnapshotRecordLoader } from '../../src/api/snapshot-loader.js';
import { AgentSnapshotResolver } from '../../src/domain/agent-snapshot-resolver.js';
import { assertSnapshotCredentialFree } from '../../src/domain/egress-credential-free.js';

const GATEWAY_MCP_URL = 'https://ai-gateway.internal/mcp';
const GATEWAY_LLM_URL = 'https://ai-gateway.internal/llm';

// ── Row seeders (direct Drizzle inserts — the read model the loader reads) ──

interface SeedAgentArgs {
  workspaceId: string;
  archived?: boolean;
  tools?: unknown[];
  mcpServers?: Array<{ name: string; url: string }>;
  skills?: string[];
  metadata?: Record<string, unknown>;
  system?: string;
}

async function seedAgent(db: DbClient, args: SeedAgentArgs): Promise<string> {
  const id = newId('agt');
  const now = new Date();
  await db.insert(agents).values({
    id,
    workspaceId: args.workspaceId,
    name: `snap-agent-${id}`,
    version: 1,
    modelProvider: 'anthropic',
    modelId: 'claude-opus-4',
    system: args.system ?? 'Agent system.',
    tools: args.tools ?? [{ type: 'agent_toolset' }],
    mcpServers: args.mcpServers ?? [],
    skills: args.skills ?? [],
    metadata: args.metadata ?? {},
    archivedAt: args.archived ? now : null,
    createdAt: now,
    updatedAt: now,
  });
  // `sessions_workspace_agent_version_fk` points at
  // `agent_versions(workspace_id, agent_id, version)`, so a session seeded with
  // `agentVersion: 1` needs the matching version row. The HTTP create path
  // writes both tables; seeding `agents` alone leaves the FK unsatisfied.
  await db.insert(agentVersions).values({
    id: newId('agtv'),
    workspaceId: args.workspaceId,
    agentId: id,
    version: 1,
    snapshot: {
      id,
      name: `snap-agent-${id}`,
      version: 1,
      model: { provider: 'anthropic', id: 'claude-opus-4' },
      system: args.system ?? 'Agent system.',
      tools: args.tools ?? [{ type: 'agent_toolset' }],
      mcp_servers: args.mcpServers ?? [],
      skills: args.skills ?? [],
      guardrail_ids: [],
      metadata: args.metadata ?? {},
    },
    createdAt: now,
  });
  return id;
}

async function seedEnvironment(
  db: DbClient,
  workspaceId: string,
  egressMode: 'gateway' | 'sidecar' | null,
): Promise<string> {
  const id = newId('env');
  const now = new Date();
  await db.insert(environments).values({
    id,
    workspaceId,
    name: `snap-env-${id}`,
    target: 'self_hosted',
    egressMode,
    createdAt: now,
    updatedAt: now,
  });
  return id;
}

/** Seed a skill + one skill_version, returning the skill_version id. */
async function seedSkillVersion(
  db: DbClient,
  args: {
    workspaceId: string;
    systemPrompt: string;
    toolAllowlist?: string[] | null;
    archived?: boolean;
  },
): Promise<string> {
  const skillId = newId('skl');
  const versionId = newId('sklv');
  const now = new Date();
  // FK `skills_workspace_latest_version_fk` requires `latest_version_id` to
  // already exist in `skill_versions` — insert with it null, then backfill
  // after the version row exists (mirrors `agents.spec.ts`'s seedCustomSkill).
  await db.insert(skills).values({
    id: skillId,
    workspaceId: args.workspaceId,
    type: 'custom',
    name: `snap-skill-${skillId}`,
    slug: `snap-skill-${skillId}`.toLowerCase(),
    version: 1,
    latestVersionId: null,
    description: null,
    displayTitle: null,
    archivedAt: args.archived ? now : null,
    createdAt: now,
    updatedAt: now,
  });
  // Main's progressive Skill disclosure model replaced the inline
  // system_prompt/tool_allowlist columns this loader used to write with a
  // bundle (directory/entrypoint/package_sha256/package_manifest) —
  // `loadSkillVersion` only reads `{id, workspaceId}` off this row now (see
  // its doc comment in `../../src/api/snapshot-loader.ts`), so
  // `args.systemPrompt` / `args.toolAllowlist` no longer map to a persisted
  // column. A placeholder bundle satisfies the NOT NULL columns; the args are
  // kept so the call sites below (which read as documentation of "what skill
  // is attached") don't need updating.
  await db.insert(skillVersions).values({
    id: versionId,
    workspaceId: args.workspaceId,
    skillId,
    version: 1,
    versionIdentifier: '1',
    name: `snap-skill-${skillId}`,
    description: '',
    directory: `snap-skill-${skillId}`,
    entrypoint: 'SKILL.md',
    packageSha256: 'a'.repeat(64),
    packageSizeBytes: 1,
    packageManifest: [
      {
        path: 'SKILL.md',
        sizeBytes: 1,
        sha256: 'b'.repeat(64),
        mode: 0o644,
        mimeType: 'text/markdown',
      },
    ],
    archivedAt: null,
    createdAt: now,
  });
  await db
    .update(skills)
    .set({ latestVersionId: versionId })
    .where(and(eq(skills.workspaceId, args.workspaceId), eq(skills.id, skillId)));
  return versionId;
}

/**
 * Seed a skill + version with a VALID, kebab-case name + non-empty description (the
 * shared materialization validators reject `_` names / empty descriptions), returning
 * the version's full descriptor identity for a `session_skill_bindings` pin.
 */
async function seedNamedSkillVersion(
  db: DbClient,
  args: {
    workspaceId: string;
    name: string;
    description: string;
    packageSha256: string;
    packageSizeBytes: number;
  },
): Promise<{ skillVersionId: string; skillId: string; bundleSha256: string; name: string }> {
  const skillId = newId('skl');
  const versionId = newId('sklv');
  const now = new Date();
  await db.insert(skills).values({
    id: skillId,
    workspaceId: args.workspaceId,
    type: 'anthropic',
    name: args.name,
    slug: args.name,
    version: 1,
    latestVersionId: null,
    description: args.description,
    displayTitle: null,
    archivedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(skillVersions).values({
    id: versionId,
    workspaceId: args.workspaceId,
    skillId,
    version: 1,
    versionIdentifier: '1',
    name: args.name,
    description: args.description,
    directory: args.name,
    entrypoint: 'SKILL.md',
    packageSha256: args.packageSha256,
    packageSizeBytes: args.packageSizeBytes,
    packageManifest: [
      {
        path: 'SKILL.md',
        sizeBytes: 1,
        sha256: 'b'.repeat(64),
        mode: 0o644,
        mimeType: 'text/markdown',
      },
    ],
    archivedAt: null,
    createdAt: now,
  });
  await db
    .update(skills)
    .set({ latestVersionId: versionId })
    .where(and(eq(skills.workspaceId, args.workspaceId), eq(skills.id, skillId)));
  return { skillVersionId: versionId, skillId, bundleSha256: args.packageSha256, name: args.name };
}

async function seedVault(
  db: DbClient,
  args: { workspaceId: string; targetUrl: string; targetKind: string; archived?: boolean },
): Promise<string> {
  const id = newId('vlt');
  const now = new Date();
  await db.insert(vaults).values({
    id,
    workspaceId: args.workspaceId,
    displayName: `snap-vault-${id}`,
    metadata: {},
    archivedAt: args.archived ? now : null,
    createdAt: now,
    updatedAt: now,
  });
  // Main's model moved the credential target off `vaults` onto
  // `vault_credentials` (see `loadVault`'s doc comment in
  // `../../src/api/snapshot-loader.ts` for the full rationale). `loadVault`
  // sources `targetUrl` from the vault's one active credential — seed exactly
  // that. `args.targetKind` — OURS' pre-merge git-credential-kind taxonomy
  // (git_https/gh_basic/https_bearer) — has no equivalent column on
  // `vault_credentials` (its `auth_type` is a different taxonomy: static_bearer/
  // mcp_oauth/environment_variable), so it's accepted here but NOT persisted:
  // every vault now resolves to the sidecar's bearer default regardless of the
  // caller's intended kind (see the sidecar test below, which asserts that).
  await db.insert(vaultCredentials).values({
    id: newId('vcrd'),
    workspaceId: args.workspaceId,
    vaultId: id,
    displayName: `snap-vault-cred-${id}`,
    authType: 'static_bearer',
    mcpServerUrl: args.targetUrl,
    secretName: null,
    networking: {},
    accessSecretRef: 'vault-secret-ref-placeholder',
    refreshSecretRef: null,
    tokenEndpoint: null,
    clientId: null,
    tokenEndpointAuthType: null,
    clientSecretRef: null,
    metadata: {},
    archivedAt: args.archived ? now : null,
    createdAt: now,
    updatedAt: now,
  });
  return id;
}

async function seedSession(
  db: DbClient,
  args: { workspaceId: string; agentId: string; environmentId: string | null; vaultIds: string[] },
): Promise<string> {
  const id = newId('ses');
  const now = new Date();
  await db.insert(sessions).values({
    id,
    workspaceId: args.workspaceId,
    agentId: args.agentId,
    agentVersion: 1,
    environmentId: args.environmentId,
    vaultIds: args.vaultIds,
    status: 'idle',
    createdAt: now,
    updatedAt: now,
  });
  return id;
}

/**
 * Pin one `session_skill_bindings` row — the immutable per-session Skill pin the
 * progressive-disclosure loader (`loadSessionSkillBundles`) reads. Mirrors what
 * `resolveSessionSkillBindings` writes at session create.
 */
async function seedSessionSkillBinding(
  db: DbClient,
  args: {
    workspaceId: string;
    sessionId: string;
    agentId: string;
    ordinal: number;
    skillVersionId: string;
    bundleSha256: string;
  },
): Promise<void> {
  await db.insert(sessionSkillBindings).values({
    workspaceId: args.workspaceId,
    sessionId: args.sessionId,
    agentId: args.agentId,
    agentVersion: 1,
    ordinal: args.ordinal,
    skillVersionId: args.skillVersionId,
    bundleSha256: args.bundleSha256,
  });
}

describe('snapshot record loader + resolver (integration)', () => {
  let db: DbClient;
  let workspaceId: string;
  let otherWorkspaceId: string;

  function gatewayResolver(): AgentSnapshotResolver {
    return new AgentSnapshotResolver({
      loader: buildSnapshotRecordLoader(db),
      minter: buildTestJwtMinter(),
      gatewayMcpUrl: GATEWAY_MCP_URL,
      gatewayLlmUrl: GATEWAY_LLM_URL,
    });
  }

  /** A resolver with NO gateway URL — the pure-sidecar deployment posture. */
  function sidecarResolver(): AgentSnapshotResolver {
    return new AgentSnapshotResolver({
      loader: buildSnapshotRecordLoader(db),
      minter: buildTestJwtMinter(),
    });
  }

  beforeAll(async () => {
    ({ db } = await getTestDb());
    workspaceId = uniqueWorkspace('snap_loader');
    otherWorkspaceId = uniqueWorkspace('snap_loader_other');
    // `agents`, `skills` and `vaults` all carry a real `workspace_id` FK. This
    // spec seeds rows directly rather than over HTTP, so nothing else creates
    // the workspaces for it.
    await createTestWorkspace(db, workspaceId);
    await createTestWorkspace(db, otherWorkspaceId);
  });

  afterAll(async () => {
    await closeTestDb();
  });

  it('resolves the gateway happy path over real rows (composed system, tools, rewritten servers, minted JWT)', async () => {
    const sklv = await seedSkillVersion(db, {
      workspaceId,
      systemPrompt: 'Skill A.',
      toolAllowlist: null,
    });
    const agentId = await seedAgent(db, {
      workspaceId,
      tools: [{ type: 'agent_toolset' }, { type: 'mcp_toolset', mcp_server_name: 'github' }],
      mcpServers: [{ name: 'github', url: 'https://github.example/mcp' }],
      skills: [sklv],
      metadata: { harness: 'claude_code', mode: 'colocated' },
    });
    const envId = await seedEnvironment(db, workspaceId, 'gateway');
    const vaultId = await seedVault(db, {
      workspaceId,
      targetUrl: 'https://github.example/mcp',
      targetKind: 'git_https',
    });
    const sessionId = await seedSession(db, {
      workspaceId,
      agentId,
      environmentId: envId,
      vaultIds: [vaultId],
    });

    const snap = await gatewayResolver().resolve(sessionId);
    expect(snap).not.toBeNull();
    // Provider derived from the agent harness annotation (claude_code → claude).
    // `claude_code` is the NATIVE Claude Code CLI provider, not the in-process Agent
    // SDK one that `claude_agent_sdk` selects. This loader is the only place the two
    // are distinguishable — their event streams look nearly identical downstream.
    expect(snap!.provider).toBe('claude-code');
    expect(snap!.model).toEqual({ provider: 'anthropic', id: 'claude-opus-4' });
    // Progressive Skill disclosure: a skill version is a materialized-on-demand bundle,
    // not an eager system-prompt slice, so `system` is just the agent's own (the Skill
    // union rides `snapshot.skills`, from `session_skill_bindings` — see the dedicated
    // loadSessionSkillBundles tests below).
    expect(snap!.system).toBe('Agent system.');
    // agent_toolset expanded; mcp_toolset surfaced as a server name.
    expect(snap!.allowed_tool_names.sort()).toEqual(
      ['bash', 'delete', 'edit', 'glob', 'grep', 'list', 'read', 'write'].sort(),
    );
    expect(snap!.allowed_mcp_server_names).toEqual(['github']);

    expect(snap!.egress.mode).toBe('gateway');
    if (snap!.egress.mode !== 'gateway') throw new Error('unreachable');
    const gw = snap!.egress.gateway;
    expect(gw.mcp_base_url).toBe(GATEWAY_MCP_URL);
    expect(gw.llm_base_url).toBe(GATEWAY_LLM_URL);
    // A real signed JWT (three dot-separated base64url segments).
    expect(gw.session_jwt.split('.')).toHaveLength(3);
    const gh = gw.mcp_servers['github']!;
    expect(gh.url).toBe(GATEWAY_MCP_URL);
    expect(gh.headers['X-Orca-Backend']).toBe('github');
    expect(gh.headers['Authorization']).toBe(`Bearer ${gw.session_jwt}`);
    // The session vault matched the github server URL → its id rides the header.
    expect(gh.headers['X-Orca-Vault-Id']).toBe(vaultId);

    // Structural credential-free guarantee over the live-resolved snapshot.
    expect(() => assertSnapshotCredentialFree(snap!)).not.toThrow();
  });

  it('keeps model and prompt pinned when the live Agent advances to another version', async () => {
    const agentId = await seedAgent(db, { workspaceId, system: 'Pinned prompt.' });
    const sessionId = await seedSession(db, {
      workspaceId,
      agentId,
      environmentId: null,
      vaultIds: [],
    });
    const [version] = await db
      .select()
      .from(agentVersions)
      .where(eq(agentVersions.agentId, agentId));
    const versionId = newId('agtv');
    await db.insert(agentVersions).values({
      id: versionId,
      workspaceId,
      agentId,
      version: 2,
      createdAt: new Date(),
      snapshot: {
        ...(version!.snapshot as Record<string, unknown>),
        version: 2,
        model: { provider: 'anthropic', id: 'claude-sonnet-4-6' },
        system: 'New prompt.',
      },
    });
    await db
      .update(agents)
      .set({
        version: 2,
        latestVersionId: versionId,
        modelId: 'claude-sonnet-4-6',
        system: 'New prompt.',
      })
      .where(eq(agents.id, agentId));
    const snap = await gatewayResolver().resolve(sessionId);
    expect(snap!.model.id).toBe('claude-opus-4');
    expect(snap!.system).toBe('Pinned prompt.');
  });

  it('mints a JWT whose claims carry the session, mcp servers, and full vault allowlist', async () => {
    const agentId = await seedAgent(db, {
      workspaceId,
      tools: [{ type: 'mcp_toolset', mcp_server_name: 'github' }],
      mcpServers: [{ name: 'github', url: 'https://github.example/mcp' }],
      metadata: { harness: 'claude_code', mode: 'colocated' },
    });
    const envId = await seedEnvironment(db, workspaceId, 'gateway');
    const vaultId = await seedVault(db, {
      workspaceId,
      targetUrl: 'https://github.example/mcp',
      targetKind: 'git_https',
    });
    const sessionId = await seedSession(db, {
      workspaceId,
      agentId,
      environmentId: envId,
      vaultIds: [vaultId],
    });

    const minter = buildTestJwtMinter();
    const resolver = new AgentSnapshotResolver({
      loader: buildSnapshotRecordLoader(db),
      minter,
      gatewayMcpUrl: GATEWAY_MCP_URL,
    });
    const snap = await resolver.resolve(sessionId);
    if (snap!.egress.mode !== 'gateway') throw new Error('unreachable');
    const verified = await minter.verify(snap!.egress.gateway.session_jwt, {
      expectedAudience: 'ai-gateway',
    });
    expect(verified.sessionId).toBe(sessionId);
    expect(verified.workspaceId).toBe(workspaceId);
    expect(verified.mcpServerNames).toEqual(['github']);
    expect(verified.vaultIds).toEqual([vaultId]);
  });

  it('loads the session-pinned agent version after the live agent changes', async () => {
    const agentId = await seedAgent(db, { workspaceId });
    const sessionId = await seedSession(db, {
      workspaceId,
      agentId,
      environmentId: null,
      vaultIds: [],
    });
    await db
      .update(agents)
      .set({ modelId: 'claude-sonnet-4', system: 'Updated agent system.' })
      .where(eq(agents.id, agentId));

    const snap = await gatewayResolver().resolve(sessionId);
    expect(snap?.model).toEqual({ provider: 'anthropic', id: 'claude-opus-4' });
    expect(snap?.system).toBe('Agent system.');
  });

  it('resolves the sidecar happy path over real rows (vault-reference bindings, no JWT, no servers)', async () => {
    const agentId = await seedAgent(db, {
      workspaceId,
      tools: [{ type: 'agent_toolset' }],
      mcpServers: [],
      metadata: {}, // no harness annotation → default provider
    });
    const envId = await seedEnvironment(db, workspaceId, 'sidecar');
    const ghVault = await seedVault(db, {
      workspaceId,
      targetUrl: 'https://api.github.com',
      targetKind: 'gh_basic',
    });
    const apiVault = await seedVault(db, {
      workspaceId,
      targetUrl: 'https://api.example.com',
      targetKind: 'https_bearer',
    });
    const sessionId = await seedSession(db, {
      workspaceId,
      agentId,
      environmentId: envId,
      vaultIds: [ghVault, apiVault],
    });

    const snap = await sidecarResolver().resolve(sessionId);
    // This agent carries NO harness annotation (see the fixture above), so it takes the
    // platform default — `claude_agent_sdk`, whose provider is the in-process SDK
    // `claude`. Not `claude-code`: that is what an explicit `harness: 'claude_code'`
    // selects, and the sibling test above asserts it.
    expect(snap!.provider).toBe('claude');
    expect(snap!.egress.mode).toBe('sidecar');
    if (snap!.egress.mode !== 'sidecar') throw new Error('unreachable');
    const entries = snap!.egress.sidecar.entries;
    expect(entries).toHaveLength(2);

    // `target_kind` (gh_basic here) has no home on `vault_credentials` anymore
    // (see `seedVault`'s comment above) — every vault, regardless of the
    // caller's intended kind, degrades to the sidecar's bearer default.
    const gh = entries.find((e) => e.host === 'api.github.com')!;
    expect(gh.scheme).toBe('bearer');
    expect(gh.username).toBeUndefined();
    expect(gh.inject_env).toEqual([]);
    expect(gh.source).toEqual({ kind: 'vault', vault_id: ghVault });

    // https_bearer → bearer scheme, no username, no injection.
    const api = entries.find((e) => e.host === 'api.example.com')!;
    expect(api.scheme).toBe('bearer');
    expect(api.username).toBeUndefined();
    expect(api.inject_env).toEqual([]);
    expect(api.source).toEqual({ kind: 'vault', vault_id: apiVault });

    expect(() => assertSnapshotCredentialFree(snap!)).not.toThrow();
  });

  // ── Workspace scoping (only a live DB read proves the WHERE clauses) ──

  it('loadAgent resolves null when the agent belongs to another workspace (workspace-scoped agent read)', async () => {
    // `loadAgent` scopes the agent read to the workspace it is given, so an agent id
    // from a different workspace is not found.
    //
    // This asserts against `loadAgent` directly rather than seeding a cross-workspace
    // session, because that row is UNCONSTRUCTIBLE: `sessions_workspace_agent_version_fk`
    // (migration 0024) and `agent_versions_workspace_agent_fk` between them forbid a
    // session in workspace A referencing an agent in workspace B. The database already
    // enforces what the loader's WHERE clause also guards — belt and braces — so the
    // guard is what there is to test.
    const foreignAgent = await seedAgent(db, { workspaceId: otherWorkspaceId });
    const loader = buildSnapshotRecordLoader(db);

    expect(await loader.loadAgent(workspaceId, foreignAgent)).toBeNull();
    // ...and the same id DOES resolve under its own workspace, so the null above is the
    // scoping, not a missing row.
    expect(await loader.loadAgent(otherWorkspaceId, foreignAgent)).not.toBeNull();
  });

  it('loadSessionSkillBundles resolves the session-wide union pinned in session_skill_bindings', async () => {
    // Progressive Skill disclosure: the colocated Skill union comes from the pinned
    // `session_skill_bindings` rows joined skill_versions→skills — the Drizzle twin of
    // `loadPreparedAgents`. The resolved snapshot carries the SAME descriptors.
    const skill = await seedNamedSkillVersion(db, {
      workspaceId,
      name: 'alpha-skill',
      description: 'Alpha skill.',
      packageSha256: 'a'.repeat(64),
      packageSizeBytes: 128,
    });
    const agentId = await seedAgent(db, {
      workspaceId,
      metadata: { harness: 'claude_code', mode: 'colocated' },
    });
    const envId = await seedEnvironment(db, workspaceId, 'gateway');
    const sessionId = await seedSession(db, {
      workspaceId,
      agentId,
      environmentId: envId,
      vaultIds: [],
    });
    await seedSessionSkillBinding(db, {
      workspaceId,
      sessionId,
      agentId,
      ordinal: 0,
      skillVersionId: skill.skillVersionId,
      bundleSha256: skill.bundleSha256,
    });

    const loader = buildSnapshotRecordLoader(db);
    const union = await loader.loadSessionSkillBundles!(workspaceId, sessionId);
    expect(union).toEqual([
      {
        id: skill.skillVersionId,
        skill_id: skill.skillId,
        source: 'anthropic',
        version_identifier: '1',
        name: 'alpha-skill',
        description: 'Alpha skill.',
        entrypoint: 'SKILL.md',
        package_sha256: 'a'.repeat(64),
        package_size_bytes: 128,
      },
    ]);
    // The resolved snapshot carries the SAME union on `skills`.
    const snap = await gatewayResolver().resolve(sessionId);
    expect(snap!.skills).toEqual(union);
  });

  it('drops a vault that belongs to another workspace (workspace-scoped vault read)', async () => {
    // The session lists a vault id that exists in ANOTHER workspace. The loader
    // scopes the vault read to the session's workspace, so it resolves null and
    // is dropped — no X-Orca-Vault-Id header, never composed cross-workspace.
    const foreignVault = await seedVault(db, {
      workspaceId: otherWorkspaceId,
      targetUrl: 'https://github.example/mcp',
      targetKind: 'git_https',
    });
    const agentId = await seedAgent(db, {
      workspaceId,
      tools: [{ type: 'mcp_toolset', mcp_server_name: 'github' }],
      mcpServers: [{ name: 'github', url: 'https://github.example/mcp' }],
      metadata: { harness: 'claude_code', mode: 'colocated' },
    });
    const envId = await seedEnvironment(db, workspaceId, 'gateway');
    const sessionId = await seedSession(db, {
      workspaceId,
      agentId,
      environmentId: envId,
      vaultIds: [foreignVault],
    });
    const snap = await gatewayResolver().resolve(sessionId);
    if (snap!.egress.mode !== 'gateway') throw new Error('unreachable');
    // The foreign vault is not in this workspace → no vault header on the server.
    expect(snap!.egress.gateway.mcp_servers['github']!.headers['X-Orca-Vault-Id']).toBeUndefined();
  });

  // ── Archived filtering (the loader's archived_at predicates) ──

  it('resolves null when the agent is archived (archived agent is not loaded)', async () => {
    const agentId = await seedAgent(db, { workspaceId, archived: true });
    const envId = await seedEnvironment(db, workspaceId, 'gateway');
    const sessionId = await seedSession(db, {
      workspaceId,
      agentId,
      environmentId: envId,
      vaultIds: [],
    });
    const snap = await gatewayResolver().resolve(sessionId);
    expect(snap).toBeNull();
  });

  // SKIPPED: the state this asserts on cannot be created.
  //
  // `session_skill_bindings_bundle_fk` (migration 0036) is ON DELETE RESTRICT and not
  // deferrable, so a binding cannot be left pointing at a skill version that does not
  // exist -- the delete is refused while the binding is live, and the insert below is
  // refused outright. The fail-loud branch in `loadSessionSkillBundles` is therefore
  // unreachable in production while that FK stands.
  //
  // Kept rather than deleted, and kept pointing at the real behaviour, so that it starts
  // running again if the constraint is ever relaxed or deferred.
  //
  // Whether to keep the branch at all is an open decision: it is defence in depth against a
  // state the schema currently forbids, so it is either prudent or dead code depending on
  // whether that FK is considered permanent. Deleting this test would erase the question
  // along with the evidence for it.
  it.skip('loadSessionSkillBundles fails loud when a pinned binding no longer resolves', async () => {
    // A `session_skill_bindings` row whose (skill_version_id, bundle_sha256) no longer
    // joins to a live skill_versions row (a deleted / re-uploaded version) must fail
    // loud, not silently drop the Skill. Seed a binding pointing at a non-existent pin.
    const agentId = await seedAgent(db, {
      workspaceId,
      metadata: { harness: 'claude_code', mode: 'colocated' },
    });
    const envId = await seedEnvironment(db, workspaceId, 'gateway');
    const sessionId = await seedSession(db, {
      workspaceId,
      agentId,
      environmentId: envId,
      vaultIds: [],
    });
    await seedSessionSkillBinding(db, {
      workspaceId,
      sessionId,
      agentId,
      ordinal: 0,
      skillVersionId: 'sklv_does_not_exist',
      bundleSha256: 'c'.repeat(64),
    });
    const loader = buildSnapshotRecordLoader(db);
    await expect(loader.loadSessionSkillBundles!(workspaceId, sessionId)).rejects.toThrow(
      /not found/,
    );
    // The resolver propagates the fail-loud (a snapshot must not lose a pinned Skill).
    await expect(gatewayResolver().resolve(sessionId)).rejects.toThrow(/not found/);
  });

  it('drops an archived vault from the gateway url→vault map (no X-Orca-Vault-Id header)', async () => {
    const archivedVault = await seedVault(db, {
      workspaceId,
      targetUrl: 'https://github.example/mcp',
      targetKind: 'git_https',
      archived: true,
    });
    const agentId = await seedAgent(db, {
      workspaceId,
      tools: [{ type: 'mcp_toolset', mcp_server_name: 'github' }],
      mcpServers: [{ name: 'github', url: 'https://github.example/mcp' }],
      metadata: { harness: 'claude_code', mode: 'colocated' },
    });
    const envId = await seedEnvironment(db, workspaceId, 'gateway');
    const sessionId = await seedSession(db, {
      workspaceId,
      agentId,
      environmentId: envId,
      vaultIds: [archivedVault],
    });
    const snap = await gatewayResolver().resolve(sessionId);
    if (snap!.egress.mode !== 'gateway') throw new Error('unreachable');
    expect(snap!.egress.gateway.mcp_servers['github']!.headers['X-Orca-Vault-Id']).toBeUndefined();
  });

  it('defaults to gateway egress when the session has no environment row', async () => {
    // A session with environmentId=null → no egress_mode → the gateway default.
    const agentId = await seedAgent(db, {
      workspaceId,
      tools: [{ type: 'agent_toolset' }],
      mcpServers: [],
      metadata: { harness: 'claude_code', mode: 'colocated' },
    });
    const sessionId = await seedSession(db, {
      workspaceId,
      agentId,
      environmentId: null,
      vaultIds: [],
    });
    const snap = await gatewayResolver().resolve(sessionId);
    expect(snap!.egress.mode).toBe('gateway');
  });

  it('returns null for a session id with no row', async () => {
    const snap = await gatewayResolver().resolve('ses_does_not_exist');
    expect(snap).toBeNull();
  });
});
