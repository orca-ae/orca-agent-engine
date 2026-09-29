// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { parseCustomTools, type CustomTool } from './custom-tools.js';
import {
  validateSkillDescriptor,
  MAX_SKILL_BINDINGS,
  type MaterializableSkillDescriptor,
} from './sandbox/seam.js';
// The credential-free runtime agent snapshot the registry delivers over the
// tunnel, as the runner CONSUMES it.
//
// The owner pod composes the snapshot in the registry and pushes it to
// `POST /v1/runner/snapshot` as a SINGLE NDJSON line (one JSON object) before any
// turn is driven. This module is the runner-side mirror of the
// registry's `AgentSnapshot` SHAPE plus the body parser. The runner does not
// re-derive the snapshot (no skills/tool composition here) — the registry already
// did that; the runner reads the resolved fields and configures the provider.
//
// The snapshot is CREDENTIAL-FREE by construction (the registry asserts this
// before it leaves the wire): the egress block carries opaque scoped JWTs +
// vault-id references, never an upstream secret. The runner therefore reaches
// credentialed upstreams via the egress config (gateway base URL + JWT, or the
// sidecar), never by holding a secret.

/** The model selection carried verbatim in the snapshot. */
export interface SnapshotModel {
  effort?: string;
  /** Model provider, e.g. `"anthropic"`. */
  provider: string;
  /** Model id, e.g. `"claude-sonnet-4"`. */
  id: string;
}

/**
 * The credential-free snapshot delivered to the runner. Mirrors the registry's
 * `AgentSnapshot` wire shape (model + provider + composed system + tool/mcp
 * allowlists + egress). The `egress` block is left as an opaque object: the runner
 * core loop passes it to the provider, which reads the base URL / auth it needs;
 * the structural credential-free guarantee is the registry's (enforced before
 * delivery), so the runner does not re-validate the egress structure here.
 */
export interface RunnerSnapshot {
  custom_tools?: CustomTool[];
  /** Private Registry request enforcement delegation for managed Codex snapshots. */
  request_guardrails_owner?: 'registry';
  skills?: MaterializableSkillDescriptor[];
  managed_resources?: { version: 1; revision: string };
  harness_state?: unknown;
  /** The LLM the agent runs (provider + model id). */
  model: SnapshotModel;
  /** The harness/provider the runner must run (the launch frame omits it). */
  provider: string;
  /**
   * The agent system prompt. Skills add nothing to it on the wire: a Skill is a
   * bundle, not a prompt fragment. For managed resources the session loop appends
   * the progressive-disclosure Skill catalog before the harness starts.
   */
  system: string;
  /**
   * A sandbox-relative directory the skills are staged under as a native plugin
   * bundle for the CLI to discover (the `claude-code` provider passes it to the
   * headless binary as `--plugin-dir`, so the CLI loads the bundled skills natively).
   *
   * The registry does not set it: the session loop fills it in after materializing
   * the Skill bundles the owner pod pushes, and removes it for managed resources,
   * which use the catalog in `system` instead. A session with no Skills leaves it
   * absent rather than empty. A provider that has no native plugin surface (the
   * in-process claude providers) ignores it. A producer that does pin a dir up
   * front keeps it.
   */
  skills_plugin_dir?: string;
  /**
   * The concrete Orca tool names the agent's tools expand to. Skills never narrow
   * them.
   *
   * `[]` means DENY-ALL, not "no restriction": an mcp-only agent declares no
   * directly-runnable tool and legitimately composes to an empty list. Every
   * provider must treat the empty array the way `buildOrcaSdkTools` does — an
   * allowlist that permits nothing — and only an ABSENT allowlist as unrestricted.
   *
   * REQUIRED ON THE WIRE, and {@link parseSnapshotBody} enforces it: a snapshot whose
   * `allowed_tool_names` is missing, `null`, or not an array is a
   * {@link SnapshotParseError}, never a defaulted value. The two readings of a missing
   * field are one boolean apart and read OPPOSITELY — deny every tool, or grant the
   * whole built-in surface — so guessing either way silently mis-scopes the agent with
   * nothing in the logs. Only the producer knows which it meant, so the runner refuses
   * to decide: the push acks non-2xx and the owner pod records it undelivered, exactly
   * as an unknown `provider` or a malformed `multiagent` block already does.
   *
   * The unrestricted state is still expressible where it is actually reachable — a
   * caller that constructs a `SessionStartInput` directly leaves
   * `agentSnapshot.allowed_tool_names` absent, and every provider reads that as "no
   * restriction". It just cannot arrive on this wire by omission.
   */
  allowed_tool_names: string[];
  /** The enabled `mcp_toolset` server names surviving the intersection. */
  allowed_mcp_server_names: string[];
  /**
   * Per-tool confirmation permission policy (`tool name → "always_ask" |
   * "always_allow" | "always_deny"`), composed by the registry from the agent's per-tool config.
   * Mirrors the Managed Agents `permission_policy`: an `always_ask` tool parks on the
   * human gate; an `always_allow` tool auto-approves. A tool absent from this map
   * falls back to {@link default_tool_permission}. Empty / absent when the snapshot
   * carries no per-tool policy (the provider is then fail-closed — see
   * {@link default_tool_permission}).
   */
  tool_permissions: Record<string, ToolPermissionPolicy>;
  /**
   * Default confirmation permission policy for a tool NOT named in
   * {@link tool_permissions}. Absent → the provider is FAIL-CLOSED: an unclassified
   * tool is treated as `always_ask` (parks on the human gate), so a snapshot that
   * stamps no policy keeps the gate engaged for every tool. The registry sets this
   * explicitly to `always_allow` when the agent's default policy auto-approves.
   */
  default_tool_permission?: ToolPermissionPolicy;
  /** The credential-free egress config (gateway or sidecar) — opaque to the loop. */
  egress: unknown;
  /**
   * The declarative CLI-agent spec for the generic `custom` provider (command / argv-with-
   * placeholders / env / cwd / a stdout→AgentEvent mapping / an optional approvals opt-in). Opaque
   * to the loop and every other provider — only the `custom` harness reads + validates it (fail-fast
   * at start). Carried verbatim so a self-hosted operator can register ANY CLI agent by dropping the
   * spec on the snapshot, with no runner change. Absent for every non-`custom` provider (and for a
   * `custom` snapshot that forgot it, which the harness surfaces as a clean capability error).
   */
  custom_spec?: unknown;
  /**
   * The multiagent COORDINATOR block — present ONLY when this snapshot describes a
   * coordinator agent (an agent created with a `multiagent` roster). Its presence is
   * what turns the session into the Anthropic thread model: ONE session, MULTIPLE
   * threads, sharing the SAME sandbox / filesystem / vault credentials, with each
   * roster subagent isolated in its own thread (own model/system/tools/MCP + own
   * history). When present the runner wraps the coordinator's own harness so it can
   * DELEGATE to the roster (see the coordinator harness); when ABSENT the runner
   * drives the harness as a plain single agent — so the whole feature is purely
   * additive (a single-agent snapshot never carries this and its path is unchanged).
   *
   * The registry composes it credential-free and snapshots the roster at agent
   * create (each member version-pinned), so a coordinator always delegates to the
   * exact roster that existed when it was created.
   */
  multiagent?: MultiagentSnapshot;
  /**
   * The guardrails composed for this session, already resolved and ordered by
   * the registry across all four authority tiers, plus the state restored for
   * them. Carried beside {@link tool_permissions} because they are the same
   * kind of decision from the same producer: the permission policy is the seed
   * of the guardrail fold, not a separate mechanism.
   *
   * Credential-free like everything else here — declarative rules the registry
   * already validated, and counters.
   *
   * Absent on a snapshot from a producer that predates guardrails, which reads
   * as "no guardrails" and leaves that path byte-for-byte unchanged.
   */
  guardrails?: SnapshotGuardrail[];
  /** Session-scoped guardrail counters restored from the previous runner. */
  guardrail_state?: Record<string, unknown>;
}

/**
 * The runner-side view of a coordinator's `multiagent` roster, parsed from the
 * snapshot. Mirrors the registry's snapshotted coordinator block but carries each
 * roster member's FULLY-RESOLVED sub-snapshot (its own model/system/tools/MCP/egress
 * — the same {@link RunnerSnapshot} shape) so the runner can construct a subagent
 * harness for a delegated turn WITHOUT another round-trip: the coordinator delegates
 * ONE LEVEL, in-process, over the SAME sandbox.
 */
export interface MultiagentSnapshot {
  /** The only multiagent kind. */
  type: 'coordinator';
  /**
   * The session's PRIMARY thread id (`sth_…`) — the registry-created thread row with
   * `parent_thread_id = null`. Carried so the coordinator stamps it as the
   * `parent_thread_id` on every child thread it spawns (the read-model projection
   * keys the child to its parent). Empty string when the registry did not carry one
   * (older snapshots / tests) — the coordinator then emits a null parent.
   */
  primaryThreadId: string;
  /** The roster the coordinator may delegate to (non-empty; at most the roster cap). */
  agents: MultiagentRosterMember[];
}

/**
 * One resolved roster member the coordinator may delegate to: the roster agent's
 * display name (announced on `session.thread_created` + used for delegation
 * addressing) plus its fully-resolved sub-snapshot (the config the runner builds a
 * subagent harness from). A `{ type: 'self' }` member's snapshot is the coordinator's
 * OWN snapshot with the multiagent block stripped (so a self subagent is never itself
 * a coordinator — one-level delegation).
 */
export interface MultiagentRosterMember {
  /** The roster agent's name (delegation target + `session.thread_created` agent_name). */
  agentName: string;
  /** The roster agent's own resolved snapshot (its model/system/tools/MCP/egress). */
  snapshot: RunnerSnapshot;
}

/**
 * A tool-confirmation permission policy on the snapshot — whether a tool call PARKS
 * on the human gate (`always_ask`) or AUTO-APPROVES (`always_allow`). Mirrors the
 * Managed Agents `permission_policy` type. Re-declared here (not imported from the
 * harness seam) so the snapshot shape stays self-contained; the value space is
 * identical, so the projection into `SessionStartInput` is a direct pass-through.
 */
export type ToolPermissionPolicy = 'always_ask' | 'always_allow' | 'always_deny';

/**
 * One composed guardrail as the registry delivers it. Mirrors the internal
 * contract's `PreparedGuardrail` field for field; `rule` is carried opaque
 * because `@orca/guardrails` owns its shape and re-declaring it here would be a
 * second definition to keep in step.
 */
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

/** Thrown when the snapshot body is not a single well-formed snapshot object. */
export class SnapshotParseError extends Error {
  constructor(message: string) {
    super(`snapshot parse failed: ${message}`);
    this.name = 'SnapshotParseError';
  }
}

/**
 * Parse the snapshot push body (the registry's single NDJSON line) into a
 * {@link RunnerSnapshot}.
 *
 * The delivery body is exactly `JSON.stringify(snapshot) + "\n"`, so a tolerant
 * parse trims trailing whitespace/newline and requires a single JSON object with
 * a string `provider` (the load-bearing field that selects the harness — the one
 * field that cannot be reconstructed from the launch path). Missing optional
 * fields default to empty so a minimal snapshot (chat-only, no tools) still
 * configures a provider. A non-object body, multiple lines, or a non-string
 * provider is a {@link SnapshotParseError} (the snapshot handler maps it to a
 * non-2xx ack so the owner pod's delivery records it as undelivered and retries
 * on the next reconnect).
 */
export function parseSnapshotBody(body: Uint8Array): RunnerSnapshot {
  const text = Buffer.from(body).toString('utf8').trim();
  if (text.length === 0) {
    throw new SnapshotParseError('empty body');
  }
  if (text.includes('\n')) {
    throw new SnapshotParseError('expected a single NDJSON line');
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (err) {
    throw new SnapshotParseError(err instanceof Error ? err.message : 'invalid JSON');
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new SnapshotParseError('body is not a JSON object');
  }
  const obj = parsed as Record<string, unknown>;
  const provider = obj.provider;
  if (typeof provider !== 'string' || provider.length === 0) {
    throw new SnapshotParseError('missing or non-string provider');
  }
  const model = normalizeModel(obj.model);
  const snapshot: RunnerSnapshot = {
    model,
    provider,
    system: typeof obj.system === 'string' ? obj.system : '',
    // NOT `stringArray` — that coerces absent/`null`/non-array to `[]`, and `[]` is
    // DENY-ALL. Coercing here would turn a producer that merely forgot the field into
    // an agent with zero tools on every provider, silently. See the field's doc.
    allowed_tool_names: requiredStringArray(obj.allowed_tool_names, 'allowed_tool_names'),
    allowed_mcp_server_names: stringArray(obj.allowed_mcp_server_names),
    tool_permissions: toolPermissionMap(obj.tool_permissions),
    egress: obj.egress ?? null,
    ...(obj.harness_state ? { harness_state: obj.harness_state } : {}),
  };
  if (obj.custom_tools !== undefined) {
    try {
      snapshot.custom_tools = parseCustomTools(obj.custom_tools, snapshot.allowed_tool_names);
    } catch {
      throw new SnapshotParseError('invalid custom_tools');
    }
  }
  if (obj.managed_resources !== undefined) {
    const resources = obj.managed_resources as Record<string, unknown> | null;
    if (
      !resources ||
      typeof resources !== 'object' ||
      Array.isArray(resources) ||
      resources.version !== 1 ||
      typeof resources.revision !== 'string' ||
      !/^[a-f0-9]{64}$/.test(resources.revision)
    ) {
      throw new SnapshotParseError('invalid managed_resources');
    }
    snapshot.managed_resources = { version: 1, revision: resources.revision };
  }
  if (obj.request_guardrails_owner !== undefined) {
    if (
      obj.request_guardrails_owner !== 'registry' ||
      (provider !== 'codex-sdk' && provider !== 'pi-sdk') ||
      !snapshot.managed_resources ||
      obj.multiagent !== undefined
    )
      throw new SnapshotParseError('invalid request_guardrails_owner');
    snapshot.request_guardrails_owner = 'registry';
  }
  if (obj.skills !== undefined) {
    if (!Array.isArray(obj.skills) || obj.skills.length > MAX_SKILL_BINDINGS)
      throw new SnapshotParseError('invalid skills descriptors');
    try {
      for (const skill of obj.skills)
        validateSkillDescriptor(skill as MaterializableSkillDescriptor);
    } catch {
      throw new SnapshotParseError('invalid skills descriptor');
    }
    snapshot.skills = obj.skills as MaterializableSkillDescriptor[];
  }
  const defaultPolicy = toolPermissionPolicy(obj.default_tool_permission);
  if (defaultPolicy !== undefined) {
    snapshot.default_tool_permission = defaultPolicy;
  }
  // The skills plugin-dir rides the snapshot ONLY when the producer staged the
  // skills as a native bundle; a non-string / empty value is treated as absent (the
  // session loop then fills in the dir it materialized from the Skills push, if any).
  // Spread in only when present — under `exactOptionalPropertyTypes` an explicit
  // `undefined` is not assignable to the optional `skills_plugin_dir?` slot.
  const skillsPluginDir = obj.skills_plugin_dir;
  if (typeof skillsPluginDir === 'string' && skillsPluginDir.length > 0) {
    snapshot.skills_plugin_dir = skillsPluginDir;
  }
  // The declarative custom-CLI spec rides the snapshot ONLY for the `custom` provider. It is kept
  // OPAQUE here (carried verbatim, like `egress`) — the `custom` harness's spec parser validates it
  // fail-fast at start. Spread in only when present — under `exactOptionalPropertyTypes` an explicit
  // `undefined` is not assignable to the optional `custom_spec?` slot, and every non-`custom`
  // snapshot omits it entirely.
  if (obj.custom_spec !== undefined) {
    snapshot.custom_spec = obj.custom_spec;
  }
  // The multiagent coordinator block rides the snapshot ONLY for a coordinator agent. A
  // block that is PRESENT but malformed is a {@link SnapshotParseError}, not a silent
  // degrade: dropping it would run a coordinator as a plain single agent — no roster, no
  // delegation tool, every delegation the model was configured for simply gone — and the
  // only trace would be `coordinator: false` in a routine info line. Throwing acks the
  // push non-2xx, so the owner pod records it undelivered and retries, exactly as an
  // unknown `provider` already does. Spread in only when present; a single-agent snapshot
  // omits it entirely, keeping that path byte-for-byte unchanged.
  const multiagent = parseMultiagent(obj.multiagent, snapshot);
  if (multiagent !== undefined) {
    snapshot.multiagent = multiagent;
  }
  // Guardrails follow the `multiagent` precedent, not the `custom_spec` one: a
  // block that is PRESENT but malformed throws rather than degrading. Dropping
  // it would run the session with a rule the operator believes is applied and
  // the runtime never sees — the exact failure the whole feature exists to
  // prevent — and the only trace would be its absence from a log line.
  // Throwing acks the push non-2xx, so the owner pod retries.
  const guardrails = parseGuardrails(obj.guardrails);
  if (guardrails !== undefined) {
    snapshot.guardrails = guardrails;
  }
  const guardrailState = obj.guardrail_state;
  if (guardrailState !== undefined) {
    if (
      typeof guardrailState !== 'object' ||
      guardrailState === null ||
      Array.isArray(guardrailState)
    ) {
      throw new SnapshotParseError('guardrail_state is not a JSON object');
    }
    snapshot.guardrail_state = guardrailState as Record<string, unknown>;
  }
  return snapshot;
}

/**
 * Parse the snapshot's `multiagent` block into a {@link MultiagentSnapshot}, or
 * `undefined` when the block is ABSENT (the ordinary single-agent snapshot).
 *
 * A well-formed block is `{ type: 'coordinator', primary_thread_id?, agents: [...] }`
 * with a NON-EMPTY roster whose members each resolve to a usable sub-snapshot:
 *   - `{ agent_name?, snapshot }` — an ordinary roster agent; its `snapshot` is parsed
 *     as a full {@link RunnerSnapshot} (its own model/system/provider/egress);
 *   - `{ type: 'self', agent_name? }` — the coordinator delegating to itself; its
 *     snapshot is the COORDINATOR's own snapshot (`coordinator`) with the multiagent
 *     block stripped, so a self subagent is never itself a coordinator (one-level).
 *
 * Anything else — a non-object block, a `type` other than `coordinator` (the
 * version-skew case), an empty roster, or ANY unusable member — is a
 * {@link SnapshotParseError}. Recovering a PARTIAL roster is never an option (it would
 * silently under-delegate), and dropping the whole block is no better: the coordinator
 * would run as a plain single agent with no roster and no delegation tool, and the only
 * trace would be `coordinator: false` in a routine info line. Throwing makes the handler
 * ack non-2xx, so the owner pod records the push undelivered and retries — the same path
 * an unknown `provider` already takes (the handler maps that one to a 422).
 *
 * The self-snapshot is derived from `coordinator` (the already-parsed outer snapshot) so
 * `self` needs no embedded copy on the wire.
 */
/**
 * Parse the snapshot's `guardrails` array, or `undefined` when absent.
 *
 * Every field is required except the two the registry omits rather than
 * optional fields (`state_scope`, `subagent_id`). A malformed entry throws for
 * the reason the whole array does: a guardrail silently changed or dropped is
 * worse than a session that refuses to start, because the operator has no way
 * to tell.
 */
function parseGuardrails(raw: unknown): SnapshotGuardrail[] | undefined {
  if (raw === undefined) return undefined;
  if (!Array.isArray(raw)) {
    throw new SnapshotParseError('guardrails is not an array');
  }
  return raw.map((entry, index) => {
    if (typeof entry !== 'object' || entry === null || Array.isArray(entry)) {
      throw new SnapshotParseError(`guardrails[${index}] is not a JSON object`);
    }
    const obj = entry as Record<string, unknown>;
    const id = obj.id;
    const name = obj.name;
    const tier = obj.tier;
    if (typeof id !== 'string' || id.length === 0) {
      throw new SnapshotParseError(`guardrails[${index}].id must be a non-empty string`);
    }
    if (typeof name !== 'string') {
      throw new SnapshotParseError(`guardrails[${index}].name must be a string`);
    }
    if (
      typeof tier !== 'string' ||
      !['session', 'agent', 'workspace', 'organization'].includes(tier)
    ) {
      throw new SnapshotParseError(`guardrails[${index}].tier is not recognized`);
    }
    if (
      !Array.isArray(obj.phases) ||
      obj.phases.length === 0 ||
      obj.phases.some((p) => typeof p !== 'string')
    ) {
      throw new SnapshotParseError(
        `guardrails[${index}].phases must be a non-empty array of strings`,
      );
    }
    if (obj.rule === undefined || obj.rule === null) {
      throw new SnapshotParseError(`guardrails[${index}].rule is required`);
    }
    if (typeof obj.stateful !== 'boolean') {
      throw new SnapshotParseError(`guardrails[${index}].stateful must be a boolean`);
    }
    const guardrail: SnapshotGuardrail = {
      id,
      name,
      tier,
      phases: obj.phases as string[],
      rule: obj.rule,
      stateful: obj.stateful,
    };
    if (obj.state_scope !== undefined) {
      if (
        typeof obj.state_scope !== 'string' ||
        !['turn', 'session', 'subject_window'].includes(obj.state_scope)
      ) {
        throw new SnapshotParseError(`guardrails[${index}].state_scope is not recognized`);
      }
      guardrail.state_scope = obj.state_scope;
    }
    if (obj.subagent_id !== undefined) {
      if (typeof obj.subagent_id !== 'string' || obj.subagent_id.length === 0) {
        throw new SnapshotParseError(`guardrails[${index}].subagent_id must be a non-empty string`);
      }
      guardrail.subagent_id = obj.subagent_id;
    }
    return guardrail;
  });
}

function parseMultiagent(
  raw: unknown,
  coordinator: RunnerSnapshot,
): MultiagentSnapshot | undefined {
  if (raw === undefined || raw === null) {
    return undefined; // absent — a single-agent snapshot, by far the common case.
  }
  if (typeof raw !== 'object' || Array.isArray(raw)) {
    throw new SnapshotParseError('multiagent is not a JSON object');
  }
  const obj = raw as Record<string, unknown>;
  if (obj.type !== 'coordinator') {
    throw new SnapshotParseError(
      `multiagent.type must be 'coordinator', got ${JSON.stringify(obj.type)}`,
    );
  }
  if (!Array.isArray(obj.agents) || obj.agents.length === 0) {
    throw new SnapshotParseError('multiagent.agents must be a non-empty array');
  }
  const primaryThreadId = typeof obj.primary_thread_id === 'string' ? obj.primary_thread_id : '';
  const agents: MultiagentRosterMember[] = obj.agents.map((rawMember, index) =>
    parseRosterMember(rawMember, coordinator, index),
  );
  return { type: 'coordinator', primaryThreadId, agents };
}

/**
 * Parse one roster member, throwing a {@link SnapshotParseError} naming its index when it
 * is unusable. An `agent_name` falls back to the member snapshot's own hint or the stable
 * `'agent'` placeholder (the projector's own default), so a `session.thread_created`
 * always carries a name.
 */
function parseRosterMember(
  raw: unknown,
  coordinator: RunnerSnapshot,
  index: number,
): MultiagentRosterMember {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new SnapshotParseError(`multiagent.agents[${index}] is not a JSON object`);
  }
  const obj = raw as Record<string, unknown>;
  const explicitName =
    typeof obj.agent_name === 'string' && obj.agent_name.length > 0 ? obj.agent_name : undefined;
  if (obj.type === 'self') {
    // A self member reuses the coordinator's own config, with the multiagent block
    // stripped so the self subagent gets no delegation tool (one-level delegation).
    return { agentName: explicitName ?? 'agent', snapshot: stripMultiagent(coordinator) };
  }
  // An ordinary roster agent carries its own resolved sub-snapshot. It is parsed as a
  // full RunnerSnapshot; a snapshot that is itself a coordinator is defended against by
  // stripping its multiagent block (one-level delegation is enforced here too).
  if (obj.snapshot === undefined) {
    throw new SnapshotParseError(
      `multiagent.agents[${index}] has neither a snapshot nor type 'self'`,
    );
  }
  let snapshot: RunnerSnapshot;
  try {
    snapshot = parseSnapshotObject(obj.snapshot);
  } catch (err) {
    // Re-wrapped with the member's index: the inner message names the offending FIELD,
    // and the index names WHICH roster agent carried it.
    throw new SnapshotParseError(
      `multiagent.agents[${index}] snapshot: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  return { agentName: explicitName ?? 'agent', snapshot: stripMultiagent(snapshot) };
}

/**
 * A shallow COPY of a snapshot with its multiagent block removed (the one-level
 * guard). Always a fresh object — never the input by reference — because a `self`
 * member derives its snapshot from the coordinator's own snapshot, which the caller
 * ({@link parseSnapshotBody}) mutates AFTER this returns (it assigns `.multiagent`
 * onto it). Returning a copy keeps the self subagent's snapshot from aliasing (and
 * thus re-acquiring) the coordinator's multiagent block.
 */
function stripMultiagent(snapshot: RunnerSnapshot): RunnerSnapshot {
  const { multiagent: _dropped, ...rest } = snapshot;
  return rest;
}

/**
 * Parse an ALREADY-DECODED snapshot object (a roster member's embedded sub-snapshot)
 * into a {@link RunnerSnapshot}. Shares the exact field logic of {@link parseSnapshotBody}
 * by re-encoding to the single-line body form the parser consumes — so a member snapshot
 * is validated by the same rules the top-level snapshot is (load-bearing `provider`,
 * required allowlists, opaque egress passthrough, and — recursively — its own
 * multiagent handling, which {@link stripMultiagent} then flattens for one-level).
 */
function parseSnapshotObject(value: unknown): RunnerSnapshot {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new SnapshotParseError('member snapshot is not a JSON object');
  }
  return parseSnapshotBody(new TextEncoder().encode(JSON.stringify(value)));
}

/** Narrow a raw value to a {@link ToolPermissionPolicy}, or `undefined` when not one. */
function toolPermissionPolicy(raw: unknown): ToolPermissionPolicy | undefined {
  if (raw === undefined) return undefined;
  if (raw === 'always_ask' || raw === 'always_allow' || raw === 'always_deny') return raw;
  throw new SnapshotParseError('invalid tool permission policy');
}

/**
 * Validate the per-tool map before applying a snapshot. Reject malformed entries
 * instead of dropping them: falling back to an allow default could weaken a deny.
 */
function toolPermissionMap(raw: unknown): Record<string, ToolPermissionPolicy> {
  if (raw === undefined) return {};
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new SnapshotParseError('tool permissions must be an object');
  }
  const out: Record<string, ToolPermissionPolicy> = {};
  for (const [name, value] of Object.entries(raw as Record<string, unknown>)) {
    const policy = toolPermissionPolicy(value);
    if (policy !== undefined) {
      out[name] = policy;
    }
  }
  return out;
}

/** Normalize the `model` field; absent / malformed yields empty provider + id. */
function normalizeModel(raw: unknown): SnapshotModel {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    return { provider: '', id: '' };
  }
  const obj = raw as Record<string, unknown>;
  return {
    provider: typeof obj.provider === 'string' ? obj.provider : '',
    id: typeof obj.id === 'string' ? obj.id : '',
    ...(typeof obj.effort === 'string' ? { effort: obj.effort } : {}),
  };
}

/**
 * Read a REQUIRED string-array field: `[]` and any populated array are accepted (with
 * non-string entries dropped, as {@link stringArray} does), and ABSENT / `null` / any
 * non-array value is a {@link SnapshotParseError} naming the field.
 *
 * Used for `allowed_tool_names` and nothing else. Defaulting a missing allowlist to `[]`
 * is not a safe fallback but a silent decision: `[]` denies every tool, so a producer
 * that merely omitted the field ships an agent that can run nothing, with no diagnostic
 * anywhere. Defaulting it to "unrestricted" is the same mistake pointing the other way —
 * it grants the full built-in surface to a snapshot that never asked for it. Throwing is
 * the only reading that cannot be wrong.
 */
function requiredStringArray(raw: unknown, field: string): string[] {
  if (!Array.isArray(raw)) {
    throw new SnapshotParseError(`missing or non-array ${field}`);
  }
  return raw.filter((v): v is string => typeof v === 'string');
}

/** Coerce a field to a string array, dropping non-string entries. */
function stringArray(raw: unknown): string[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw.filter((v): v is string => typeof v === 'string');
}
