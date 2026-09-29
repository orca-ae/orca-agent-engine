// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { nativeHarnessState } from '../domain/harness-state.js';
import { bindStoredHarness } from '@orca/harness-catalog';
// Drizzle-backed {@link SnapshotRecordLoader} over the registry tables.
//
// The {@link AgentSnapshotResolver} reads the records it composes a snapshot from
// through this seam. Keeping the loader here (next to the other table-backed
// builders like `buildDistributionSessionStore`) keeps the resolver itself pure
// of Drizzle, so its orchestration stays unit-testable against an in-memory fake.
//
// Each read is workspace-correct: the session resolves the owning workspace
// (looked up unscoped by id — the runner-tunnel connect path has no api key), and
// the agent / skill-version / vault reads are scoped to that workspace so a
// snapshot can never compose a cross-workspace record.

import { and, eq, isNull, or } from 'drizzle-orm';
import { uniqueSkillMaterializations } from '@orca/sandbox-runtime';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  environments,
  guardrails,
  guardrailState,
  sessions,
  sessionHarnessStates,
  sessionSkillBindings,
  skills,
  skillVersions,
  vaultCredentials,
  vaults,
  workspaces,
} from '../persistence/postgres/schema.js';
import type {
  ResolverAgentRecord,
  ResolverEnvironmentRecord,
  ResolverSessionRecord,
  ResolverSkillVersionRecord,
  ResolverVaultRecord,
  SnapshotRecordLoader,
} from '../domain/agent-snapshot-resolver.js';
import type { AgentToolEntry } from '../domain/agent-snapshot.js';
import type { PreparedSkillDescriptor } from '../contracts/internal.contract.js';

/**
 * Build a {@link SnapshotRecordLoader} over the registry tables.
 *
 * The loader resolves:
 *   - `loadSession` — by id, unscoped (the runner-tunnel connect path has no api
 *     key); archived sessions resolve normally (a live runner for an archived
 *     session still gets its snapshot).
 *   - `loadAgent` — the live agent row scoped to the workspace + non-archived.
 *   - `loadEnvironment` — by id (the egress mode + target).
 *   - `loadSkillVersion` — joined to `skills` for the owning workspace (the
 *     resolver workspace-checks it), excluding archived skills.
 *   - `loadVault` — scoped to the workspace; the resolver drops archived vaults,
 *     so the row's `archived_at` is surfaced as a boolean.
 */
export function buildSnapshotRecordLoader(db: DbClient): SnapshotRecordLoader {
  return {
    async loadSession(sessionId: string): Promise<ResolverSessionRecord | null> {
      // Joined to `workspaces` for `organization_id` — the session JWT's required
      // `org_id` claim (see `SessionJwtMinter.mint`, `src/auth/session-jwt.ts`).
      // Mirrors `loadActiveRuntimeSession` in `internal.routes.ts`, minus its
      // active-session/active-workspace filters: a snapshot resolves normally for
      // an archived session (see the module doc above), so this stays an unscoped,
      // unfiltered lookup by session id, only widened to pull the organization id
      // through the (non-nullable) workspace foreign key.
      const rows = await db
        .select({
          id: sessions.id,
          workspaceId: sessions.workspaceId,
          agentId: sessions.agentId,
          agentVersion: sessions.agentVersion,
          runtimeRevision: sessions.runtimeRevision,
          harnessState: sessionHarnessStates.state,
          tools: sessions.tools,
          mcpServers: sessions.mcpServers,
          environmentId: sessions.environmentId,
          vaultIds: sessions.vaultIds,
          agentOverrides: sessions.agentOverrides,
          organizationId: workspaces.organizationId,
        })
        .from(sessions)
        .leftJoin(
          sessionHarnessStates,
          and(
            eq(sessionHarnessStates.workspaceId, sessions.workspaceId),
            eq(sessionHarnessStates.sessionId, sessions.id),
          ),
        )
        .innerJoin(workspaces, eq(workspaces.id, sessions.workspaceId))
        .where(and(isNull(sessions.deletedAt), eq(sessions.id, sessionId)))
        .limit(1);
      const row = rows[0];
      if (!row) return null;
      return {
        id: row.id,
        workspaceId: row.workspaceId,
        organizationId: row.organizationId,
        agentId: row.agentId,
        agentVersion: row.agentVersion,
        runtimeRevision: row.runtimeRevision,
        harnessState: nativeHarnessState(row.harnessState),
        agentOverrides: row.agentOverrides as Record<string, unknown> | null,
        tools: row.tools as AgentToolEntry[] | null,
        mcpServers: row.mcpServers as Array<{ name: string; url: string }> | null,
        environmentId: row.environmentId ?? null,
        vaultIds: row.vaultIds ?? [],
        guardrailIds: guardrailIdsFromOverrides(row.agentOverrides),
      };
    },

    async loadAgent(workspaceId: string, agentId: string): Promise<ResolverAgentRecord | null> {
      const rows = await db
        .select()
        .from(agents)
        .where(and(isNull(agents.deletedAt), eq(agents.id, agentId)))
        .limit(1);
      const row = rows[0];
      if (!row || row.workspaceId !== workspaceId || row.archivedAt !== null) return null;
      return {
        id: row.id,
        name: row.name,
        model: { provider: row.modelProvider, id: row.modelId },
        system: row.system ?? '',
        tools: (row.tools as AgentToolEntry[]) ?? [],
        mcpServers: (row.mcpServers as Array<{ name: string; url: string }>) ?? [],
        skills: (row.skills as string[]) ?? [],
        metadata: bindStoredHarness(
          row.metadata as Record<string, unknown>,
          row.harnessType,
          row.harnessType,
        ),
        guardrailIds: (row.guardrailIds as string[] | null) ?? [],
      };
    },

    async loadAgentVersion(
      workspaceId: string,
      agentId: string,
      version: number,
    ): Promise<ResolverAgentRecord | null> {
      // Resolve a version-PINNED roster member from `agent_versions.snapshot` (the
      // immutable JSONB written at agent create/update). The agent row is still
      // read for the workspace scope + archived check — a member whose agent is
      // archived or cross-workspace does not resolve.
      const agentRows = await db
        .select()
        .from(agents)
        .where(and(isNull(agents.deletedAt), eq(agents.id, agentId)))
        .limit(1);
      const agentRow = agentRows[0];
      if (!agentRow || agentRow.workspaceId !== workspaceId || agentRow.archivedAt !== null) {
        return null;
      }
      const versionRows = await db
        .select()
        .from(agentVersions)
        .where(and(eq(agentVersions.agentId, agentId), eq(agentVersions.version, version)))
        .limit(1);
      const versionRow = versionRows[0];
      if (!versionRow) return null;
      const snap = versionRow.snapshot as Record<string, unknown>;
      const model = (snap['model'] as { provider?: string; id?: string } | undefined) ?? {};
      return {
        id: agentId,
        // The version snapshot carries the name at the pinned version; fall back to the
        // live agent name (the display name is not versioned in practice).
        name: (snap['name'] as string | undefined) ?? agentRow.name,
        model: { ...model, provider: model.provider ?? '', id: model.id ?? '' },
        system: (snap['system'] as string | undefined) ?? '',
        tools: (snap['tools'] as AgentToolEntry[]) ?? [],
        mcpServers: (snap['mcp_servers'] as Array<{ name: string; url: string }>) ?? [],
        skills: (snap['skills'] as string[]) ?? [],
        metadata: bindStoredHarness(
          snap['metadata'] as Record<string, unknown>,
          agentRow.harnessType,
          snap['harness_type'],
        ),
        guardrailIds: (snap['guardrail_ids'] as string[] | undefined) ?? [],
      };
    },

    async loadEnvironment(environmentId: string): Promise<ResolverEnvironmentRecord | null> {
      const rows = await db
        .select()
        .from(environments)
        .where(and(isNull(environments.deletedAt), eq(environments.id, environmentId)))
        .limit(1);
      const row = rows[0];
      if (!row) return null;
      return {
        id: row.id,
        target: (row.target as 'cloud' | 'self_hosted' | null) ?? null,
        egressMode: (row.egressMode as 'gateway' | 'sidecar' | null) ?? null,
      };
    },

    async loadSkillVersion(skillVersionId: string): Promise<ResolverSkillVersionRecord | null> {
      // Progressive Skill disclosure (main's model): a skill version is a bundle,
      // not an inline system-prompt/tool-allowlist row. The resolver only needs the
      // identity fields for existence + workspace-correctness validation. A version
      // whose parent skill is archived is excluded via the skills join.
      const rows = await db
        .select({
          id: skillVersions.id,
          workspaceId: skills.workspaceId,
          skillArchivedAt: skills.archivedAt,
          versionArchivedAt: skillVersions.archivedAt,
        })
        .from(skillVersions)
        .innerJoin(skills, and(isNull(skills.deletedAt), eq(skillVersions.skillId, skills.id)))
        .where(and(eq(skillVersions.id, skillVersionId), isNull(skillVersions.deletedAt)))
        .limit(1);
      const row = rows[0];
      if (!row || row.skillArchivedAt !== null || row.versionArchivedAt !== null) return null;
      return {
        id: row.id,
        workspaceId: row.workspaceId,
      };
    },

    async loadSessionSkillBundles(
      workspaceId: string,
      sessionId: string,
    ): Promise<PreparedSkillDescriptor[]> {
      // The pinned bindings for this session — the EXPECTED set. Read first so a
      // binding whose skill version no longer resolves through the join below (a
      // deleted / re-uploaded / archived version) fails LOUD rather than silently
      // dropping the Skill (mirrors `loadPreparedAgents`'s per-agent count check).
      const bindingRows = await db
        .select({
          skillVersionId: sessionSkillBindings.skillVersionId,
          bundleSha256: sessionSkillBindings.bundleSha256,
          agentId: sessionSkillBindings.agentId,
          ordinal: sessionSkillBindings.ordinal,
        })
        .from(sessionSkillBindings)
        .where(
          and(
            eq(sessionSkillBindings.workspaceId, workspaceId),
            eq(sessionSkillBindings.sessionId, sessionId),
          ),
        );
      if (bindingRows.length === 0) return [];

      // The Drizzle twin of `loadPreparedAgents`'s join: bindings → the exact pinned
      // `skill_versions` row (matched on both the version id AND the pinned bundle
      // digest, so a re-uploaded version cannot masquerade) → its `skills` (for
      // `type`/archived scope). Workspace-scoped end to end.
      const rows = await db
        .select({
          agentId: sessionSkillBindings.agentId,
          ordinal: sessionSkillBindings.ordinal,
          id: skillVersions.id,
          skillId: skillVersions.skillId,
          source: skills.type,
          versionIdentifier: skillVersions.versionIdentifier,
          name: skillVersions.name,
          description: skillVersions.description,
          entrypoint: skillVersions.entrypoint,
          packageSha256: skillVersions.packageSha256,
          packageSizeBytes: skillVersions.packageSizeBytes,
        })
        .from(sessionSkillBindings)
        .innerJoin(
          skillVersions,
          and(
            eq(sessionSkillBindings.workspaceId, skillVersions.workspaceId),
            eq(sessionSkillBindings.skillVersionId, skillVersions.id),
            eq(sessionSkillBindings.bundleSha256, skillVersions.packageSha256),
          ),
        )
        .innerJoin(
          skills,
          and(
            eq(skillVersions.workspaceId, skills.workspaceId),
            eq(skillVersions.skillId, skills.id),
          ),
        )
        .where(
          and(
            eq(sessionSkillBindings.workspaceId, workspaceId),
            eq(sessionSkillBindings.sessionId, sessionId),
          ),
        );

      // Fail loud: every distinct pinned binding must have resolved through the join.
      const resolved = new Set(rows.map((row) => `${row.id}\0${row.packageSha256}`));
      for (const binding of bindingRows) {
        if (!resolved.has(`${binding.skillVersionId}\0${binding.bundleSha256}`)) {
          throw new Error(
            `snapshot: session skill binding ${binding.skillVersionId} (bundle ${binding.bundleSha256}) not found in workspace`,
          );
        }
      }

      // Build descriptors in a stable (agentId, ordinal) order, narrowing the two
      // string columns the descriptor type pins. The shared `uniqueSkillMaterializations`
      // then validates every descriptor and dedupes the SESSION-WIDE union by name —
      // throwing on an invalid descriptor or a name that resolves to two bundle digests,
      // exactly as the harness materialization does — so both paths agree by construction.
      const ordered = [...rows].sort(
        (a, b) => a.agentId.localeCompare(b.agentId) || a.ordinal - b.ordinal,
      );
      const descriptors: PreparedSkillDescriptor[] = ordered.map((row) => {
        if (row.source !== 'anthropic' && row.source !== 'custom') {
          throw new Error(`snapshot: skill version ${row.id} has an invalid source`);
        }
        if (row.entrypoint !== 'SKILL.md') {
          throw new Error(`snapshot: skill version ${row.id} has an unsupported entrypoint`);
        }
        return {
          id: row.id,
          skill_id: row.skillId,
          source: row.source,
          version_identifier: row.versionIdentifier,
          name: row.name,
          description: row.description,
          entrypoint: 'SKILL.md',
          package_sha256: row.packageSha256,
          package_size_bytes: row.packageSizeBytes,
        };
      });
      return uniqueSkillMaterializations(descriptors);
    },

    async loadGuardrailContext(workspaceId, organizationId, sessionId) {
      const visibleRows = await db
        .select({
          id: guardrails.id,
          name: guardrails.name,
          enabled: guardrails.enabled,
          phases: guardrails.phases,
          scope: guardrails.scope,
          rule: guardrails.rule,
        })
        .from(guardrails)
        .where(
          and(
            isNull(guardrails.deletedAt),
            isNull(guardrails.archivedAt),
            or(
              eq(guardrails.workspaceId, workspaceId),
              and(
                eq(guardrails.organizationId, organizationId),
                eq(guardrails.scope, 'organization'),
              ),
            ),
          ),
        )
        .orderBy(guardrails.id);
      const stateRows = await db
        .select({
          key: guardrailState.key,
          valueNum: guardrailState.valueNum,
          valueJson: guardrailState.valueJson,
        })
        .from(guardrailState)
        .where(
          and(eq(guardrailState.workspaceId, workspaceId), eq(guardrailState.sessionId, sessionId)),
        );
      const state: Record<string, unknown> = {};
      for (const row of stateRows) state[row.key] = row.valueNum ?? row.valueJson;
      return {
        visible: visibleRows.map((row) => ({
          id: row.id,
          name: row.name,
          enabled: row.enabled,
          phases: Array.isArray(row.phases) ? (row.phases as string[]) : [],
          scope: row.scope as 'organization' | 'workspace' | 'explicit',
          rule: row.rule,
        })),
        state,
      };
    },

    async loadVault(workspaceId: string, vaultId: string): Promise<ResolverVaultRecord | null> {
      const rows = await db
        .select()
        .from(vaults)
        .where(
          and(
            isNull(vaults.deletedAt),
            eq(vaults.id, vaultId),
            eq(vaults.workspaceId, workspaceId),
          ),
        )
        .limit(1);
      const row = rows[0];
      if (!row) return null;

      // Main's model moved the credential target OFF `vaults` (now just
      // display/workspace identity) and ONTO `vault_credentials` — and, unlike
      // OURS' pre-merge 1:1 vault-to-target design (`vaults.target_url` /
      // `target_kind`), one vault may hold SEVERAL active credentials, each
      // bound to its own `mcp_server_url` (`vault_credentials_active_url_idx`
      // is unique per (workspace, vault, url), not per vault). Main's own
      // per-request resolution (`resolveMcpDestination` in
      // `domain/mcp-destination.ts`) matches a specific MCP server URL against
      // a session's vaults at USE time — there is no longer a single "the
      // vault's URL" in general.
      //
      // The snapshot resolver's egress binding (`buildGatewayConfig`'s
      // `vaultByUrl` map, `vaultsToBindings`'s sidecar host binding), though,
      // composes a snapshot with no per-request context: it needs ONE URL per
      // `ResolverVaultRecord` (a documented, pre-existing gap in that same
      // resolver — see the `TODO(follow-up)` on `credential_ids: []` in
      // `AgentSnapshotResolver.buildGatewayConfig`, `domain/agent-snapshot-
      // resolver.ts`, noting the loader seam is vault-level only). Source it
      // correctly for the common, unambiguous case: a vault with exactly one
      // active credential carrying a URL — the same shape OURS' pre-merge
      // design assumed. A vault with zero or multiple distinct
      // active-credential URLs has no single correct answer here; resolve to
      // an inert empty target rather than guessing. That degrades safely:
      // `hostOf('')` fails to parse and the vault is skipped by
      // `vaultsToBindings` (documented "vault whose URL can't be parsed is
      // skipped"), and an empty string never collides with a real
      // `mcp_servers[].url` in `vaultByUrl`.
      const credentialRows = await db
        .select({ mcpServerUrl: vaultCredentials.mcpServerUrl })
        .from(vaultCredentials)
        .where(
          and(
            isNull(vaultCredentials.deletedAt),
            eq(vaultCredentials.workspaceId, workspaceId),
            eq(vaultCredentials.vaultId, vaultId),
            isNull(vaultCredentials.archivedAt),
          ),
        );
      const activeUrls = [
        ...new Set(
          credentialRows.map((c) => c.mcpServerUrl).filter((url): url is string => url !== null),
        ),
      ];
      const targetUrl = activeUrls.length === 1 ? (activeUrls[0] ?? '') : '';

      return {
        id: row.id,
        targetUrl,
        // INTENTIONAL under main's model, not a gap: vault sidecar egress is
        // bearer-only. Main dropped the git-credential-kind taxonomy
        // (`git_https` / `gh_basic` / `https_basic`) from vaults entirely —
        // git-repo credentials now live in the separate `git_credentials`
        // table (`api/git-creds.routes.ts`, `mintGitCredsJwt`), NOT vaults.
        // `vault_credentials.auth_type` (`static_bearer` / `mcp_oauth` /
        // `environment_variable`) answers a different question (how the
        // secret is obtained/refreshed, not what wire scheme the sidecar
        // should speak to the host), so there's no `target_kind` to source
        // from it. `targetKind` is left undefined so the sidecar applies its
        // bearer default — safe, since `bindingForKind`
        // (`domain/agent-snapshot-resolver.ts`) already treats every
        // unrecognized/absent kind as the bearer swap-on-access default that
        // `https_bearer` also produced pre-merge (see the matching seam
        // comment on `EgressAuthScheme` in `domain/credential-egress.ts`). Do
        // NOT reintroduce a per-vault-credential-kind (basic/token) taxonomy
        // here to "fix" this — that would contradict adopt-main.
        archived: row.archivedAt !== null,
      };
    },
  };
}

function guardrailIdsFromOverrides(raw: unknown): string[] {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) return [];
  const ids = (raw as Record<string, unknown>)['guardrailIds'];
  return Array.isArray(ids)
    ? ids.filter((value): value is string => typeof value === 'string')
    : [];
}
