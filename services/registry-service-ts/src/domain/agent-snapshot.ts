// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The credential-free runtime agent snapshot — PURE builder.
//
// At session start the registry composes everything a runner needs to run turns
// for one (agent, environment, session) triple and delivers it over the tunnel.
// This module is the pure heart of that composition: given already-resolved
// records it produces the snapshot. Record resolution (DB reads, JWT mint, vault
// matching) lives in the resolver that calls this; keeping the composition pure
// makes the skills/tool/egress logic exhaustively unit-testable.
//
// The snapshot is CREDENTIAL-FREE — the egress config carries an opaque scoped
// JWT + vault-id references (gateway) or vault references only (sidecar), never
// an upstream secret.
//
// PROVIDER carriage. The self-hosted launch frame deliberately omits the
// provider (the worker spawns a generic runner shell), so the snapshot MUST carry
// the harness/provider selection: a multi-provider runner reads it to spin up the
// right agent harness. This is the one field that cannot be reconstructed from the
// launch path and is therefore load-bearing in the snapshot.

import {
  composeSystemPrompt,
  intersectToolAllowlists,
  canonicalAgentToolNames,
  expandAllowedToolNames,
  enabledMcpToolsetServerNames,
  toolsetPermissionPolicies,
  type ManagedToolPermissionPolicy,
  type SkillSlice,
  type AgentToolEntry,
} from '@orca/harness-catalog';
import { ToolDef } from '../contracts/agents.contract.js';
import type { z } from 'zod';
import type { EgressConfig } from './credential-egress.js';
import type { PreparedSkillDescriptor } from '../contracts/internal.contract.js';

// The composition core (compose system prompt, intersect allowlists, canonicalize
// + expand tool names, enable mcp servers) lives in `@orca/harness-catalog` so the
// registry build and the harness-server build share ONE implementation. The
// snapshot SHAPE — model + provider + egress — is registry-specific and stays
// here. `AgentToolEntry` + `enabledMcpToolsetServerNames` are re-exported so the
// registry's existing consumers (snapshot loader, tests) keep their import path.
export { type AgentToolEntry, enabledMcpToolsetServerNames };

/** The model selection carried verbatim into the snapshot. */
export interface AgentModel {
  effort?: string;
  provider: string;
  id: string;
}

/** The resolved agent fields the snapshot is composed from. */
export interface SnapshotAgent {
  model: AgentModel;
  system: string;
  tools: AgentToolEntry[];
}

/**
 * The RUNTIME multiagent block folded into the delivered snapshot — the wire shape
 * the runner parses into its coordinator roster ({@link parseMultiagent} in the
 * runner's `snapshot.ts`).
 *
 * Distinct from the CONTRACT `MultiagentSnapshot` persisted on the agent version
 * (which stores members as `{ type: 'agent', id, version }` REFERENCES): here each
 * `{ type: 'agent' }` member is RESOLVED to its own credential-free sub-snapshot (its
 * own model/system/tools/MCP/egress) so the runner can construct a subagent harness
 * for a delegated turn WITHOUT another registry round-trip. A `{ type: 'self' }`
 * member carries no embedded snapshot — the runner derives it from the coordinator's
 * own snapshot (with the multiagent block stripped), so one-level delegation holds.
 */
export interface AgentSnapshotMultiagent {
  type: 'coordinator';
  /**
   * The session's primary thread id (`sth_…`), stamped as the `parent_thread_id` on
   * each child thread. The registry creates the primary thread row lazily (its
   * `session.thread_created` projection), so it is typically NOT known at
   * snapshot-delivery time — the resolver emits an empty string, and the runner
   * coordinator then emits a null parent + uses its `'primary'` message fallback.
   */
  primary_thread_id: string;
  /** The resolved roster the coordinator may delegate to. */
  agents: AgentSnapshotMultiagentMember[];
}

/** One resolved roster member on the runtime multiagent block. */
export type AgentSnapshotMultiagentMember =
  | {
      /** The roster agent's display name (announced on `session.thread_created`). */
      agent_name: string;
      /** The roster agent's own resolved credential-free sub-snapshot. */
      snapshot: AgentSnapshot;
    }
  | {
      /** A self member — the coordinator delegating to itself (runner derives its snapshot). */
      type: 'self';
      /** The self member's display name (the coordinator's own name). */
      agent_name: string;
    };

/** Input to {@link buildAgentSnapshot}. Every field is already resolved + secret-free. */
export interface AgentSnapshotInput {
  /** The resolved agent (model, system, tools). */
  agent: SnapshotAgent;
  /** The agent's resolved skill slices, in agent-declared order. */
  skills: SkillSlice[];
  /**
   * The SESSION-WIDE union of exact Skill-bundle pins for a COLOCATED session — the
   * descriptors the owner pod PUSHES to the runner over the skills route + carries on
   * the snapshot so the runner materializes them as a native `--plugin-dir` plugin.
   * Distinct from the eager `skills` slices above (which stay empty under progressive
   * disclosure): these are the bundle refs, not a system-prompt/tool fragment. Absent
   * for a session with no Skills, so a skill-free snapshot is byte-for-byte unchanged.
   */
  skillBundles?: PreparedSkillDescriptor[];
  /**
   * A runner-relative directory the runner will stage the Skill plugin under. The
   * COLOCATED runner owns this path (it materializes the pushed bundles there and
   * fills the field in itself), so the registry normally leaves it UNSET; carried on
   * the input only so a producer that already knows the target dir can pin it.
   */
  skillsPluginDir?: string;
  /**
   * The harness/provider the runner must run (e.g. `'claude'`, `'codex'`).
   * Resolved from the agent's harness annotation; carried because the launch
   * frame omits it by design.
   */
  provider: string;
  /** The credential-free egress config (gateway or sidecar). */
  egress: EgressConfig;
  /**
   * The declarative CLI-agent spec for the generic `custom` native-CLI provider,
   * lifted VERBATIM from the agent's `metadata.custom_spec`. Opaque here (never
   * inspected by the pure builder): only the runner's `custom` harness parses +
   * validates it (fail-fast at session start). Present ONLY for a `custom`-annotated
   * agent that declared one; absent for every other provider. It carries NO secret
   * (the spec references credentials by env-var name, resolved on the runner via the
   * env passthrough), so it does not weaken the credential-free guarantee.
   */
  customSpec?: unknown;
  /**
   * The RESOLVED runtime multiagent block, present ONLY for a coordinator agent (an
   * agent created with a `multiagent` roster). Absent for a single-agent agent — the
   * overwhelming majority — so the field is purely additive and a single-agent
   * snapshot is byte-for-byte unchanged. The resolver folds the persisted roster into
   * this shape (resolving each member's sub-snapshot) before calling the builder.
   */
  multiagent?: AgentSnapshotMultiagent;
  /**
   * Guardrails already composed across session / agent / workspace /
   * organization authority, plus the durable session state restored for them.
   * Resolution belongs to the Registry resolver; the pure builder only carries
   * the credential-free values onto the runner wire.
   */
  guardrails?: SnapshotGuardrail[];
  guardrailState?: Record<string, unknown>;
}

/** One prepared guardrail on the Registry -> session-runner snapshot wire. */
export interface SnapshotGuardrail {
  id: string;
  name: string;
  tier: string;
  phases: string[];
  rule: unknown;
  stateful: boolean;
  state_scope?: string;
  subagent_id?: string;
}

export type SnapshotCustomTool = Pick<
  Extract<z.infer<typeof ToolDef>, { type: 'custom' }>,
  'name' | 'description' | 'input_schema'
>;

/** The credential-free snapshot delivered to the runner over the tunnel. */
export interface AgentSnapshot {
  /** Declared client callbacks surviving the Skill tool intersection. */
  custom_tools?: SnapshotCustomTool[];
  tool_permissions?: Record<string, ManagedToolPermissionPolicy>;
  default_tool_permission?: ManagedToolPermissionPolicy;
  /** Private request enforcement delegation, valid only with managed Codex resources. */
  request_guardrails_owner?: 'registry';
  /** Committed resource revision acknowledged before this snapshot. */
  managed_resources?: { version: 1; revision: string };
  harness_state?: unknown;
  /** The LLM the agent runs (provider + model id). */
  model: AgentModel;
  /** The harness/provider the runner must run (launch frame omits it). */
  provider: string;
  /** The agent system prompt composed with each skill's system prompt, in order. */
  system: string;
  /** The concrete Orca tool names allowed, after the skill-allowlist intersection. */
  allowed_tool_names: string[];
  /** The enabled `mcp_toolset` server names surviving the intersection. */
  allowed_mcp_server_names: string[];
  /** The credential-free egress config (gateway or sidecar). */
  egress: EgressConfig;
  /**
   * The SESSION-WIDE union of exact Skill-bundle pins, present ONLY for a COLOCATED
   * session that has Skills. The owner pod PUSHES these bundles' bytes to the runner
   * over the skills route (before this snapshot) and the runner materializes them as a
   * native `--plugin-dir` plugin; this list carries the descriptors verbatim so the
   * snapshot is self-describing. Absent (omitted entirely) for a skill-free session, so
   * the separate/chat/single-agent snapshot shape is byte-for-byte unchanged.
   */
  skills?: PreparedSkillDescriptor[];
  /**
   * The runner-relative directory the Skill plugin is staged under, surfaced to a
   * native-CLI provider as `--plugin-dir`. The COLOCATED runner materializes the pushed
   * bundles into its own workspace and fills this in itself (its `applySnapshot` injects
   * it when the snapshot left it unset), so the registry normally OMITS it. Present only
   * when a producer pinned the target dir up front; absent otherwise.
   */
  skills_plugin_dir?: string;
  /**
   * The declarative CLI-agent spec for the generic `custom` native-CLI provider
   * (from the agent's `metadata.custom_spec`). Emitted on the wire as `custom_spec`,
   * which the runner's snapshot parser carries verbatim into the `custom` harness.
   * Present ONLY for a `custom` agent that declared one; a snapshot for any other
   * provider omits it entirely, so the field is purely additive.
   */
  custom_spec?: unknown;
  /**
   * The RESOLVED runtime multiagent block — present ONLY for a coordinator agent.
   * Its presence is what turns the runner's session into the Anthropic thread model
   * (the runner's `maybeWrapCoordinator` wraps the base harness in a coordinator).
   * Absent for a single-agent agent, so the field is purely additive and a
   * single-agent snapshot's wire shape is unchanged.
   */
  multiagent?: AgentSnapshotMultiagent;
  /** The Registry-composed guardrails for this session. */
  guardrails?: SnapshotGuardrail[];
  /** Durable session-scoped guardrail state restored by Registry. */
  guardrail_state?: Record<string, unknown>;
}

/**
 * Build the credential-free runtime agent snapshot.
 *
 * Composition order:
 *   1. system — the agent system prompt joined with each skill slice's system
 *      prompt (in agent-declared order), blank parts dropped.
 *   2. tools — the agent's tool names, INTERSECTED with each skill slice's tool
 *      allowlist (a null allowlist does not narrow), then expanded to the
 *      concrete Orca tool names (`agent_toolset` → the file/shell logical tools).
 *   3. mcp servers — the `mcp_toolset` server names, kept only when `mcp_toolset`
 *      itself survives the intersection.
 *   4. egress — embedded verbatim (already credential-free).
 *
 * The resolver passes no skill slices. Skills are disclosed progressively, as
 * bundles on the snapshot's `skills` field that the runner materializes, so a Skill
 * contributes nothing to `system` or `allowed_tool_names`: steps 1 and 2 reduce to
 * the agent's own prompt and tools.
 *
 * Pure: no I/O, no secrets. The provider + model + egress are carried verbatim
 * from the resolved input.
 */
export function buildAgentSnapshot(input: AgentSnapshotInput): AgentSnapshot {
  const {
    agent,
    skills,
    skillBundles,
    skillsPluginDir,
    provider,
    egress,
    customSpec,
    multiagent,
    guardrails,
    guardrailState,
  } = input;

  const system = composeSystemPrompt(agent.system ?? '', skills);

  // The agent's declared tool names, canonicalized (the dated agent_toolset alias
  // maps to its canonical name) so the intersection + expansion key on one form.
  const agentToolNames = canonicalAgentToolNames(agent.tools ?? []);
  const intersectedToolNames = intersectToolAllowlists(agentToolNames, skills);

  const customTools: SnapshotCustomTool[] = [];
  const customNames = new Set<string>();
  for (const entry of agent.tools ?? []) {
    if (entry.type !== 'custom') continue;
    const tool = ToolDef.parse(entry);
    if (tool.type !== 'custom') continue;
    if (
      !tool.name ||
      ['agent_toolset', 'agent_toolset_20260401', 'mcp_toolset'].includes(tool.name) ||
      tool.name.startsWith('mcp__') ||
      tool.name.startsWith('sys_terminal_') ||
      customNames.has(tool.name)
    )
      throw new Error(`invalid or duplicate custom tool: ${tool.name}`);
    customNames.add(tool.name);
    if (intersectedToolNames.includes(tool.name))
      customTools.push({
        name: tool.name,
        description: tool.description,
        input_schema: tool.input_schema,
      });
  }

  const allowedMcpServers = enabledMcpToolsetServerNames(agent.tools, intersectedToolNames);
  const permissions: Record<string, ManagedToolPermissionPolicy> = {};
  if (intersectedToolNames.includes('agent_toolset')) {
    permissions['mcp__orca__*'] = 'always_allow';
    Object.assign(
      permissions,
      toolsetPermissionPolicies(
        agent.tools,
        'orca',
        (tool) => tool.type === 'agent_toolset' || tool.type === 'agent_toolset_20260401',
      ),
    );
  }
  for (const name of allowedMcpServers) {
    permissions[`mcp__${name}__*`] = 'always_ask';
    Object.assign(
      permissions,
      toolsetPermissionPolicies(
        agent.tools,
        name,
        (tool) => tool.type === 'mcp_toolset' && tool.mcp_server_name === name,
      ),
    );
  }

  return {
    tool_permissions: permissions,
    default_tool_permission: 'always_ask',
    ...(customTools.length ? { custom_tools: customTools } : {}),
    model: { ...agent.model },
    provider,
    system,
    allowed_tool_names: expandAllowedToolNames(intersectedToolNames),
    allowed_mcp_server_names: [...allowedMcpServers],
    egress,
    // The session-wide Skill-bundle union rides the snapshot ONLY for a colocated
    // session that has Skills. Spread in only when non-empty — a skill-free snapshot
    // omits it entirely (byte-for-byte unchanged), and under `exactOptionalPropertyTypes`
    // an explicit `undefined` is not assignable to the optional `skills?` slot.
    ...(skillBundles !== undefined && skillBundles.length > 0 ? { skills: skillBundles } : {}),
    // The staged plugin dir rides the snapshot ONLY when a producer pinned it up front;
    // the colocated runner normally fills this in itself, so it is usually omitted.
    ...(skillsPluginDir !== undefined && skillsPluginDir.length > 0
      ? { skills_plugin_dir: skillsPluginDir }
      : {}),
    // The custom-CLI spec rides the snapshot ONLY for a `custom` agent that declared
    // one. Spread in only when present — under `exactOptionalPropertyTypes` an explicit
    // `undefined` is not assignable to the optional `custom_spec?` slot, and every
    // non-custom snapshot omits it entirely (byte-for-byte unchanged).
    ...(customSpec !== undefined ? { custom_spec: customSpec } : {}),
    // The multiagent block rides the snapshot ONLY for a coordinator agent. Spread in
    // only when present — under `exactOptionalPropertyTypes` an explicit `undefined`
    // is not assignable to the optional `multiagent?` slot, and a single-agent snapshot
    // omits it entirely (keeping that path byte-for-byte unchanged).
    ...(multiagent !== undefined ? { multiagent } : {}),
    ...(guardrails !== undefined ? { guardrails } : {}),
    ...(guardrailState !== undefined ? { guardrail_state: guardrailState } : {}),
  };
}
