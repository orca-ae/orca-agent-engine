// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, isNull, or, type SQL } from 'drizzle-orm';
import { CustomSkillVersionSelector, MAX_AGENT_SKILL_REFS } from '../contracts/agents.contract.js';
import { PreparedAgentSnapshotSchema } from '../contracts/internal.contract.js';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  skills,
  skillVersions,
  sessionSkillBindings,
} from '../persistence/postgres/schema.js';
import { findSkillReferenceConflict } from './skill-reference-conflicts.js';

const MAX_SESSION_SKILL_BINDINGS = MAX_AGENT_SKILL_REFS;

type SessionSkillBindingInsert = typeof sessionSkillBindings.$inferInsert;
type SessionSkillBindingReadDb = Pick<DbClient, 'select'>;

interface RequestedSkillRef {
  source: 'anthropic' | 'custom';
  skillId: string;
  version: string;
}

interface AgentSkillRefs {
  agentId: string;
  agentVersion: number;
  refs: RequestedSkillRef[];
}

interface ResolvedSkillVersion {
  id: string;
  skillId: string;
  source: string;
  latestVersionId: string | null;
  version: number;
  versionIdentifier: string;
  name: string;
  description: string;
  entrypoint: string;
  packageSha256: string;
  packageSizeBytes: number;
}

export class SessionSkillBindingError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SessionSkillBindingError';
  }
}

/**
 * Resolves every skill ref on the primary agent and its direct coordinator
 * roster, returning immutable rows for insertion in the same transaction as
 * the session. Nested coordinators are not executable by the Harness and are
 * rejected by both Agent validation and this persistence boundary.
 * `latest` is resolved here—not when the agent version is saved.
 */
export async function resolveSessionSkillBindings(
  db: SessionSkillBindingReadDb,
  input: {
    workspaceId: string;
    sessionId: string;
    primaryAgentId: string;
    primaryAgentVersion: number;
    primarySkillRefsOverride?: unknown[];
  },
): Promise<SessionSkillBindingInsert[]> {
  const primary = await loadAgentSkillRefs(
    db,
    input.workspaceId,
    input.primaryAgentId,
    input.primaryAgentVersion,
    input.primarySkillRefsOverride,
  );
  const graph = new Map<string, AgentSkillRefs>([[agentKey(primary), primary]]);
  for (const ref of primary.subagents) {
    const key = agentKey(ref);
    if (graph.has(key)) continue;
    const loaded = await loadAgentSkillRefs(db, input.workspaceId, ref.agentId, ref.agentVersion);
    if (loaded.subagents.length > 0) {
      throw new SessionSkillBindingError(
        `nested coordinator agent ${loaded.agentId}@${loaded.agentVersion} is not supported`,
      );
    }
    graph.set(key, loaded);
  }

  const requestedCount = [...graph.values()].reduce((total, agent) => total + agent.refs.length, 0);
  if (requestedCount > MAX_SESSION_SKILL_BINDINGS) {
    throw new SessionSkillBindingError(
      `session agent roster references ${requestedCount} skills; maximum is ${MAX_SESSION_SKILL_BINDINGS}`,
    );
  }
  if (requestedCount === 0) return [];

  const requested = [...graph.values()].flatMap((agent) => agent.refs);
  const predicates = requested.map((ref) => skillVersionPredicate(ref));
  const rows = await db
    .select({
      id: skillVersions.id,
      skillId: skillVersions.skillId,
      source: skills.type,
      latestVersionId: skills.latestVersionId,
      version: skillVersions.version,
      versionIdentifier: skillVersions.versionIdentifier,
      name: skillVersions.name,
      description: skillVersions.description,
      entrypoint: skillVersions.entrypoint,
      packageSha256: skillVersions.packageSha256,
      packageSizeBytes: skillVersions.packageSizeBytes,
    })
    .from(skillVersions)
    .innerJoin(
      skills,
      and(
        isNull(skills.deletedAt),
        eq(skillVersions.workspaceId, skills.workspaceId),
        eq(skillVersions.skillId, skills.id),
      ),
    )
    .where(
      and(
        eq(skills.workspaceId, input.workspaceId),
        eq(skillVersions.workspaceId, input.workspaceId),
        isNull(skills.archivedAt),
        isNull(skillVersions.archivedAt),
        isNull(skillVersions.deletedAt),
        or(...predicates),
      ),
    )
    .for('share');

  const resolvedRows = rows as ResolvedSkillVersion[];
  for (const row of resolvedRows) {
    if (
      !/^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$/.test(row.name) ||
      row.description.length === 0 ||
      row.versionIdentifier.length === 0 ||
      row.entrypoint !== 'SKILL.md' ||
      !/^[0-9a-f]{64}$/.test(row.packageSha256) ||
      row.packageSizeBytes <= 0
    ) {
      throw new SessionSkillBindingError(`skill version ${row.id} has invalid package metadata`);
    }
  }
  const rowsByRequest = new Map<string, ResolvedSkillVersion>();
  for (const ref of requested) {
    const matches = resolvedRows.filter((row) => matchesRequestedSkill(row, ref));
    if (matches.length !== 1) {
      throw new SessionSkillBindingError(
        `skill ${ref.skillId} version ${String(ref.version)} not found in workspace`,
      );
    }
    rowsByRequest.set(requestKey(ref), matches[0]!);
  }

  const packageByName = new Map<string, string>();
  const bindings: SessionSkillBindingInsert[] = [];
  for (const agent of graph.values()) {
    const resolvedForAgent = agent.refs.map((ref) => rowsByRequest.get(requestKey(ref))!);
    const conflict = findSkillReferenceConflict(resolvedForAgent, packageByName);
    if (conflict?.type === 'duplicate_version') {
      throw new SessionSkillBindingError(
        `agent ${agent.agentId}@${agent.agentVersion} references skill version ${conflict.skillVersionId} more than once`,
      );
    }
    if (conflict?.type === 'different_packages') {
      throw new SessionSkillBindingError(
        `skill name ${conflict.name} resolves to different packages in the session agent graph`,
      );
    }

    for (const [ordinal, ref] of agent.refs.entries()) {
      const resolved = rowsByRequest.get(requestKey(ref))!;
      bindings.push({
        workspaceId: input.workspaceId,
        sessionId: input.sessionId,
        agentId: agent.agentId,
        agentVersion: agent.agentVersion,
        ordinal,
        skillVersionId: resolved.id,
        bundleSha256: resolved.packageSha256,
      });
    }
  }
  return bindings;
}

async function loadAgentSkillRefs(
  db: SessionSkillBindingReadDb,
  workspaceId: string,
  agentId: string,
  agentVersion: number,
  skillRefsOverride?: unknown[],
): Promise<AgentSkillRefs & { subagents: Array<{ agentId: string; agentVersion: number }> }> {
  const rows = await db
    .select({ snapshot: agentVersions.snapshot })
    .from(agentVersions)
    .innerJoin(
      agents,
      and(
        isNull(agents.deletedAt),
        eq(agentVersions.workspaceId, agents.workspaceId),
        eq(agentVersions.agentId, agents.id),
      ),
    )
    .where(
      and(
        eq(agentVersions.workspaceId, workspaceId),
        eq(agentVersions.agentId, agentId),
        eq(agentVersions.version, agentVersion),
        eq(agents.workspaceId, workspaceId),
        isNull(agents.archivedAt),
      ),
    )
    .for('share')
    .limit(1);
  const parsed = PreparedAgentSnapshotSchema.safeParse(rows[0]?.snapshot);
  if (!parsed.success || parsed.data.id !== agentId || parsed.data.version !== agentVersion) {
    throw new SessionSkillBindingError(`agent ${agentId} version ${agentVersion} not found`);
  }

  const rawRefs = skillRefsOverride ?? parsed.data.skills;
  return {
    agentId,
    agentVersion,
    refs: rawRefs.map(parseRequestedSkillRef),
    subagents: (parsed.data.multiagent?.agents ?? []).map((ref) => ({
      agentId: ref.id,
      agentVersion: ref.version,
    })),
  };
}

function parseRequestedSkillRef(value: unknown): RequestedSkillRef {
  if (!isRecord(value) || (value.type !== 'anthropic' && value.type !== 'custom')) {
    throw new SessionSkillBindingError('agent contains an invalid skill reference');
  }
  if (typeof value.skill_id !== 'string' || value.skill_id.length === 0) {
    throw new SessionSkillBindingError('agent contains a skill reference without a skill_id');
  }
  const version = value.version == null ? 'latest' : value.version;
  if (
    typeof version !== 'string' ||
    (value.type === 'custom'
      ? !CustomSkillVersionSelector.safeParse(version).success
      : version.length === 0)
  ) {
    throw new SessionSkillBindingError(`skill ${value.skill_id} has an invalid version reference`);
  }
  return {
    source: value.type,
    skillId: value.skill_id,
    version,
  };
}

function skillVersionPredicate(ref: RequestedSkillRef): SQL {
  const versionPredicate =
    ref.version === 'latest'
      ? eq(skillVersions.id, skills.latestVersionId)
      : ref.source === 'custom'
        ? eq(skillVersions.versionIdentifier, ref.version)
        : or(eq(skillVersions.versionIdentifier, ref.version), eq(skillVersions.id, ref.version));
  return and(eq(skills.id, ref.skillId), eq(skills.type, ref.source), versionPredicate)!;
}

function matchesRequestedSkill(row: ResolvedSkillVersion, ref: RequestedSkillRef): boolean {
  if (row.skillId !== ref.skillId || row.source !== ref.source) return false;
  if (ref.version === 'latest') return row.id === row.latestVersionId;
  if (ref.source === 'custom') return row.versionIdentifier === ref.version;
  return row.id === ref.version || row.versionIdentifier === ref.version;
}

function requestKey(ref: RequestedSkillRef): string {
  return `${ref.source}\0${ref.skillId}\0${ref.version}`;
}

function agentKey(agent: { agentId: string; agentVersion: number }): string {
  return `${agent.agentId}@${agent.agentVersion}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
