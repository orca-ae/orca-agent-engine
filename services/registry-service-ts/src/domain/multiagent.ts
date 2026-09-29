// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Multiagent (Anthropic thread-model) contract + snapshot resolution for the
 * registry.
 *
 * Anthropic's thread model runs ONE session with MULTIPLE threads. An agent with
 * a `multiagent` block of `{ type: 'coordinator', agents: [...] }` is a
 * COORDINATOR: it delegates (via the agent_toolset) to a fixed ROSTER of agents,
 * each of which runs in its own session THREAD (a context-isolated event stream +
 * its own history) inside the SAME session — one sandbox, one filesystem, one set
 * of vault credentials, one runner. Isolated per roster agent: its own
 * model/system/tools/MCP/skills + its own thread.
 *
 * The roster is a contract field on the agent, snapshotted AT AGENT CREATE with
 * each roster member's version PINNED, so a session started from a given agent
 * version always delegates to the exact roster snapshot that existed when the
 * coordinator was created. A `{ type: 'self' }` member is the coordinator agent
 * delegating to itself (a recursion into its own definition); it carries no id and
 * is never version-pinned (its version is the coordinator's own).
 *
 * Constraints enforced here (mirrors Anthropic's published limits):
 *   - the roster is non-empty and holds at most {@link MAX_ROSTER_SIZE} members;
 *   - delegation is ONE LEVEL only — a roster member may not itself be a
 *     coordinator (enforced at create against the resolved roster agents);
 *   - a roster `{ type: 'agent' }` member references an `agt_…` id that must
 *     resolve to a live, non-archived agent in the SAME workspace.
 *
 * This module is pure (no DB): {@link parseMultiagent} validates the request
 * shape, and {@link snapshotMultiagent} folds resolved roster versions into the
 * immutable snapshot. The routes do the DB lookups (roster resolution +
 * one-level check) and call these.
 */

/** Anthropic's published cap on coordinator roster size. */
export const MAX_ROSTER_SIZE = 20;

/** A roster member referencing another agent by id (optionally version-pinned). */
export interface MultiagentRosterAgentRef {
  type: 'agent';
  /** The `agt_…` id of the roster agent. */
  id: string;
  /**
   * Optional pinned version. When omitted at create, the snapshot pins the
   * roster agent's CURRENT version so the roster is immutable for the
   * coordinator version.
   */
  version?: number;
}

/** A roster member that is the coordinator delegating to itself. */
export interface MultiagentRosterSelfRef {
  type: 'self';
}

export type MultiagentRosterMember = MultiagentRosterAgentRef | MultiagentRosterSelfRef;

/** The `multiagent` block on an agent contract. */
export interface Multiagent {
  type: 'coordinator';
  agents: MultiagentRosterMember[];
}

/**
 * The snapshotted `multiagent` block persisted into an agent version. Identical
 * shape to {@link Multiagent} except every `{ type: 'agent' }` member carries a
 * RESOLVED, pinned `version` (never optional), so the roster is fully immutable
 * for the coordinator version.
 */
export interface MultiagentSnapshotAgentRef {
  type: 'agent';
  id: string;
  version: number;
}

export type MultiagentSnapshotMember = MultiagentSnapshotAgentRef | MultiagentRosterSelfRef;

export interface MultiagentSnapshot {
  type: 'coordinator';
  agents: MultiagentSnapshotMember[];
}

const AGENT_ID_PREFIX = 'agt_';

/**
 * Parse + shape-validate a request's `multiagent` value (from the agent
 * create/update body). Returns a normalized {@link Multiagent} or an `{ error }`.
 *
 * `undefined`/`null` is NOT handled here (a single-agent agent has no
 * `multiagent`); callers gate on presence first. All structural rules that do
 * NOT need the DB are enforced here:
 *   - `type` must be `'coordinator'` (the only multiagent kind);
 *   - `agents` is a non-empty array with at most {@link MAX_ROSTER_SIZE} members;
 *   - each member is `{ type: 'agent', id: 'agt_…', version? }` or
 *     `{ type: 'self' }`;
 *   - a member id must be the `agt_` form; a pinned `version` (if present) is a
 *     positive integer.
 *
 * Roster resolution (the ids exist, are non-archived, one-level) needs the DB and
 * is done by the caller against the returned refs.
 */
export function parseMultiagent(input: unknown): Multiagent | { error: string } {
  if (input === null || typeof input !== 'object' || Array.isArray(input)) {
    return { error: 'multiagent must be an object' };
  }
  const obj = input as Record<string, unknown>;
  if (obj['type'] !== 'coordinator') {
    return { error: "multiagent.type must be 'coordinator'" };
  }
  const rawAgents = obj['agents'];
  if (!Array.isArray(rawAgents)) {
    return { error: 'multiagent.agents must be an array' };
  }
  if (rawAgents.length === 0) {
    return { error: 'multiagent.agents must not be empty' };
  }
  if (rawAgents.length > MAX_ROSTER_SIZE) {
    return { error: `multiagent.agents exceeds the max roster size of ${MAX_ROSTER_SIZE}` };
  }

  const members: MultiagentRosterMember[] = [];
  for (const raw of rawAgents) {
    const member = parseRosterMember(raw);
    if ('error' in member) return member;
    members.push(member);
  }
  return { type: 'coordinator', agents: members };
}

function parseRosterMember(raw: unknown): MultiagentRosterMember | { error: string } {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    return { error: 'multiagent.agents[] entries must be objects' };
  }
  const m = raw as Record<string, unknown>;
  const type = m['type'];
  if (type === 'self') {
    return { type: 'self' };
  }
  if (type !== 'agent') {
    return { error: "multiagent.agents[].type must be 'agent' or 'self'" };
  }
  const id = m['id'];
  if (typeof id !== 'string' || !id.startsWith(AGENT_ID_PREFIX)) {
    return { error: 'multiagent.agents[].id must be an agt_… identifier' };
  }
  if ('version' in m && m['version'] !== undefined) {
    const version = m['version'];
    if (typeof version !== 'number' || !Number.isInteger(version) || version <= 0) {
      return { error: 'multiagent.agents[].version must be a positive integer' };
    }
    return { type: 'agent', id, version };
  }
  return { type: 'agent', id };
}

/**
 * The distinct roster-agent ids referenced by a parsed {@link Multiagent} (the
 * `{ type: 'agent' }` members). `{ type: 'self' }` members contribute no id. The
 * caller resolves these against the workspace for the one-level + existence
 * checks.
 */
export function rosterAgentIds(multiagent: Multiagent): string[] {
  const ids = new Set<string>();
  for (const member of multiagent.agents) {
    if (member.type === 'agent') ids.add(member.id);
  }
  return [...ids];
}

/** A resolved roster agent, as looked up by the caller from the DB. */
export interface ResolvedRosterAgent {
  id: string;
  /** The agent's current version — the pin used when the member omits `version`. */
  currentVersion: number;
  /** Whether this roster agent is itself a coordinator (one-level violation). */
  isCoordinator: boolean;
}

/**
 * Fold the resolved roster agents into the immutable {@link MultiagentSnapshot}.
 *
 * For each `{ type: 'agent' }` member: pin the member's explicit `version` when
 * present, else the resolved `currentVersion`. `{ type: 'self' }` members pass
 * through unchanged (no id, no version — the coordinator's own version applies).
 *
 * Returns `{ error }` when a referenced id is missing from `resolved` (a roster
 * agent not found in the workspace) or when a resolved roster agent is itself a
 * coordinator (delegation is ONE LEVEL only). Both are create-time 400s.
 */
export function snapshotMultiagent(
  multiagent: Multiagent,
  resolved: ReadonlyMap<string, ResolvedRosterAgent>,
): MultiagentSnapshot | { error: string } {
  const members: MultiagentSnapshotMember[] = [];
  for (const member of multiagent.agents) {
    if (member.type === 'self') {
      members.push({ type: 'self' });
      continue;
    }
    const agent = resolved.get(member.id);
    if (agent === undefined) {
      return { error: `multiagent roster agent ${member.id} not found in workspace` };
    }
    if (agent.isCoordinator) {
      return {
        error: `multiagent roster agent ${member.id} is itself a coordinator (delegation is one level only)`,
      };
    }
    members.push({ type: 'agent', id: member.id, version: member.version ?? agent.currentVersion });
  }
  return { type: 'coordinator', agents: members };
}

/**
 * Read a persisted agent snapshot's `multiagent` field back into a
 * {@link MultiagentSnapshot}, or `null` when the agent is single-agent (no
 * `multiagent`). Tolerant of the stored JSONB shape — used by the load path and
 * the distributor to tell a coordinator from a plain agent.
 */
export function readMultiagentSnapshot(value: unknown): MultiagentSnapshot | null {
  if (value === null || value === undefined || typeof value !== 'object' || Array.isArray(value)) {
    return null;
  }
  const obj = value as Record<string, unknown>;
  if (obj['type'] !== 'coordinator' || !Array.isArray(obj['agents'])) {
    return null;
  }
  const members: MultiagentSnapshotMember[] = [];
  for (const raw of obj['agents']) {
    if (raw === null || typeof raw !== 'object') continue;
    const m = raw as Record<string, unknown>;
    if (m['type'] === 'self') {
      members.push({ type: 'self' });
    } else if (
      m['type'] === 'agent' &&
      typeof m['id'] === 'string' &&
      typeof m['version'] === 'number'
    ) {
      members.push({ type: 'agent', id: m['id'], version: m['version'] });
    }
  }
  return { type: 'coordinator', agents: members };
}

/** Whether a persisted agent snapshot describes a coordinator (multiagent) agent. */
export function isCoordinatorSnapshot(value: unknown): boolean {
  return readMultiagentSnapshot(value) !== null;
}
