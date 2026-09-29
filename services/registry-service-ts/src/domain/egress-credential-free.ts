// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { isManagedToolPermissionPolicy, validateSdkCheckpoint } from '@orca/harness-catalog';
// Structural credential-free guarantee for the {@link EgressConfig}.
//
// The egress config is part of the snapshot delivered over the tunnel, so it MUST
// be secret-free. The {@link EgressConfig} types are secret-free BY CONSTRUCTION
// (vault-id references + caller-minted JWTs only), but a type-level guarantee
// erases at runtime. A pure serialize-and-string-match guard (look for
// `real_secret` / `password` / `ghp_` / `sk-`) is heuristic: it would not catch a
// NOVEL secret-shaped field name (e.g. a future `api_key` / `client_secret` field
// accidentally threaded into a rewritten server or a sidecar entry).
//
// This module closes that gap with a STRUCTURAL walk: it asserts the egress
// config conforms EXACTLY to the known secret-free shape — every object key is in
// an allow-list of fields that are secret-free by design, and there are NO extra
// keys. An unexpected key fails loud regardless of its string contents, so a
// novel secret-shaped field is caught by its STRUCTURE, not by a pattern on its
// value. The two credential-shaped values that ARE allowed — the opaque scoped
// JWTs (`session_jwt` / `llm_jwt` and the `Authorization: Bearer <jwt>` header) —
// are additionally shape-checked to be JWT-shaped (or empty), so a raw upstream
// secret can't masquerade as one.
//
// The check is intended both as a hard test oracle (the snapshot/egress specs
// assert against it instead of string-matching) and as an optional runtime
// invariant the delivery path can enforce before a snapshot leaves the registry.

import type { EgressConfig } from './credential-egress.js';
import type { AgentSnapshot } from './agent-snapshot.js';
import { ToolDef } from '../contracts/agents.contract.js';

/** A path step in the egress tree, for a precise violation message. */
type PathStep = string;

/**
 * The exact key set the credential-free {@link AgentSnapshot} envelope may carry.
 * The non-egress fields are plain, non-credential data by design (model id +
 * provider + composed system text + tool/mcp name lists); the only place a secret
 * could structurally hide is the egress block, delegated to
 * {@link assertEgressCredentialFree}. An unexpected top-level key (a smuggled
 * `api_key` / `credentials` field) fails loud here.
 */
const ALLOWED_SNAPSHOT_KEYS = new Set([
  'model',
  'provider',
  'system',
  'allowed_tool_names',
  'allowed_mcp_server_names',
  'egress',
  // The colocated Skill-bundle union + its staged plugin dir. The descriptors are
  // plain, non-credential metadata (id / name / digest / size / description); the
  // bundle BYTES travel on a separate push, never in the snapshot. Listing the keys
  // keeps a skills-carrying snapshot passing the structural credential-free walk.
  'skills',
  'skills_plugin_dir',
  // Declarative policy + counters are data, not credentials. Their nested
  // shapes are validated by the guardrail authoring/compiler path and by the
  // runner parser; admitting the envelope keys here lets the structural egress
  // guard continue policing the actual credential boundary.
  'guardrails',
  'guardrail_state',
  'harness_state',
  'managed_resources',
  'custom_tools',
  'tool_permissions',
  'default_tool_permission',
  'request_guardrails_owner',
  // The coordinator roster block. Its members carry NESTED sub-snapshots, each of
  // which is itself walked credential-free (so a secret smuggled into a roster
  // member's snapshot is still caught by its structure) — see {@link assertMultiagent}.
  'multiagent',
]);

/** The exact key sets allowed on the runtime multiagent block + its members. */
const ALLOWED_MULTIAGENT_KEYS = {
  root: new Set(['type', 'primary_thread_id', 'agents']),
  /** An `{ agent_name, snapshot }` roster member (its snapshot is walked recursively). */
  agentMember: new Set(['agent_name', 'snapshot']),
  /** A `{ type: 'self', agent_name }` roster member (no embedded snapshot). */
  selfMember: new Set(['type', 'agent_name']),
} as const;

/**
 * The exact key sets allowed at each node of the egress tree. Any key NOT listed
 * is a structural violation — this is what makes a novel secret-shaped field
 * (whatever it is named) fail loud. Optional keys are allowed-but-not-required;
 * required keys are checked for presence separately where it matters.
 */
const ALLOWED_KEYS = {
  /** Top-level discriminated union: `{ mode, gateway }` or `{ mode, sidecar }`. */
  root: new Set(['mode', 'gateway', 'sidecar']),
  gateway: new Set(['mcp_base_url', 'llm_base_url', 'session_jwt', 'llm_jwt', 'mcp_servers']),
  rewrittenServer: new Set(['type', 'url', 'headers', 'alwaysLoad']),
  sidecar: new Set(['entries']),
  sidecarEntry: new Set(['host', 'scheme', 'source', 'username', 'inject_env']),
  sidecarSource: new Set(['kind', 'vault_id']),
} as const;

/**
 * The ONLY header keys a rewritten MCP server may carry. `Authorization` is the
 * single credential-shaped header (a `Bearer <jwt>`, shape-checked below); the
 * `X-Orca-*` routing/audit headers are plain references. A header key outside
 * this set is a structural violation (e.g. an `X-Api-Key` smuggling a secret).
 */
const ALLOWED_HEADER_KEYS = new Set([
  'X-Orca-Backend',
  'X-Orca-Session-Id',
  'X-Orca-Vault-Id',
  'Authorization',
]);

/** Error thrown when the egress config violates the credential-free structure. */
export class EgressNotCredentialFreeError extends Error {
  constructor(message: string) {
    super(`egress is not credential-free: ${message}`);
    this.name = 'EgressNotCredentialFreeError';
  }
}

/**
 * A token is JWT-shaped iff it is three non-empty `base64url` segments joined by
 * dots (`header.payload.signature`). The empty string is allowed too: the gateway
 * path deliberately leaves `session_jwt=''` (and omits the `Authorization` header)
 * when an agent has zero MCP servers, so "" is a valid no-token sentinel — but a
 * raw secret (which is neither empty nor three base64url segments) is rejected.
 */
function isJwtShapedOrEmpty(value: string): boolean {
  if (value === '') return true;
  const segs = value.split('.');
  if (segs.length !== 3) return false;
  return segs.every((s) => s.length > 0 && /^[A-Za-z0-9_-]+$/.test(s));
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Reject any key on `obj` that is not in `allowed`, at `path`. */
function assertNoExtraKeys(
  obj: Record<string, unknown>,
  allowed: ReadonlySet<string>,
  path: PathStep,
): void {
  for (const key of Object.keys(obj)) {
    if (!allowed.has(key)) {
      throw new EgressNotCredentialFreeError(
        `unexpected field '${path}.${key}' (not a known secret-free field)`,
      );
    }
  }
}

/**
 * Structurally assert that `egress` is credential-free: it conforms exactly to
 * the known secret-free shape, with no extra fields anywhere, and the only
 * credential-shaped values (the scoped JWTs) are JWT-shaped (or the empty
 * sentinel). Throws {@link EgressNotCredentialFreeError} on the first violation.
 *
 * This is a STRUCTURAL guarantee, not a string heuristic: a field named anything
 * outside the allow-list is rejected by its presence, so a novel secret-shaped
 * field is caught whatever it is called and whatever it contains.
 */
export function assertEgressCredentialFree(egress: EgressConfig): void {
  if (!isPlainObject(egress)) {
    throw new EgressNotCredentialFreeError('egress is not an object');
  }
  assertNoExtraKeys(egress, ALLOWED_KEYS.root, 'egress');

  if (egress.mode === 'gateway') {
    assertGateway((egress as { gateway: unknown }).gateway);
    if ('sidecar' in egress) {
      throw new EgressNotCredentialFreeError("gateway egress must not carry a 'sidecar' block");
    }
    return;
  }
  if (egress.mode === 'sidecar') {
    assertSidecar((egress as { sidecar: unknown }).sidecar);
    if ('gateway' in egress) {
      throw new EgressNotCredentialFreeError("sidecar egress must not carry a 'gateway' block");
    }
    return;
  }
  throw new EgressNotCredentialFreeError(
    `unrecognized egress mode '${String((egress as { mode: unknown }).mode)}'`,
  );
}

function assertGateway(gateway: unknown): void {
  if (!isPlainObject(gateway)) {
    throw new EgressNotCredentialFreeError('egress.gateway is not an object');
  }
  assertNoExtraKeys(gateway, ALLOWED_KEYS.gateway, 'egress.gateway');

  // The scoped JWTs are the only credential-shaped top-level gateway values —
  // assert they are opaque JWTs (or the empty sentinel), never a raw secret.
  assertJwtField(gateway.session_jwt, 'egress.gateway.session_jwt');
  if (gateway.llm_jwt !== undefined) {
    assertJwtField(gateway.llm_jwt, 'egress.gateway.llm_jwt');
  }

  const servers = gateway.mcp_servers;
  if (!isPlainObject(servers)) {
    throw new EgressNotCredentialFreeError('egress.gateway.mcp_servers is not an object');
  }
  for (const [name, server] of Object.entries(servers)) {
    assertRewrittenServer(server, `egress.gateway.mcp_servers.${name}`);
  }
}

function assertRewrittenServer(server: unknown, path: PathStep): void {
  if (!isPlainObject(server)) {
    throw new EgressNotCredentialFreeError(`${path} is not an object`);
  }
  assertNoExtraKeys(server, ALLOWED_KEYS.rewrittenServer, path);

  const headers = server.headers;
  if (!isPlainObject(headers)) {
    throw new EgressNotCredentialFreeError(`${path}.headers is not an object`);
  }
  for (const [key, value] of Object.entries(headers)) {
    if (!ALLOWED_HEADER_KEYS.has(key)) {
      throw new EgressNotCredentialFreeError(
        `unexpected header '${path}.headers.${key}' (not a known secret-free header)`,
      );
    }
    // `Authorization` is the one credential-shaped header: it must be a
    // `Bearer <jwt>`, with the token JWT-shaped — not a raw secret.
    if (key === 'Authorization') {
      if (typeof value !== 'string' || !value.startsWith('Bearer ')) {
        throw new EgressNotCredentialFreeError(
          `${path}.headers.Authorization must be a 'Bearer <jwt>' value`,
        );
      }
      const token = value.slice('Bearer '.length);
      if (!isJwtShapedOrEmpty(token)) {
        throw new EgressNotCredentialFreeError(
          `${path}.headers.Authorization carries a non-JWT-shaped token (possible raw secret)`,
        );
      }
    }
  }
}

function assertSidecar(sidecar: unknown): void {
  if (!isPlainObject(sidecar)) {
    throw new EgressNotCredentialFreeError('egress.sidecar is not an object');
  }
  assertNoExtraKeys(sidecar, ALLOWED_KEYS.sidecar, 'egress.sidecar');

  const entries = sidecar.entries;
  if (!Array.isArray(entries)) {
    throw new EgressNotCredentialFreeError('egress.sidecar.entries is not an array');
  }
  entries.forEach((entry, i) => assertSidecarEntry(entry, `egress.sidecar.entries[${i}]`));
}

function assertSidecarEntry(entry: unknown, path: PathStep): void {
  if (!isPlainObject(entry)) {
    throw new EgressNotCredentialFreeError(`${path} is not an object`);
  }
  assertNoExtraKeys(entry, ALLOWED_KEYS.sidecarEntry, path);

  const source = entry.source;
  if (!isPlainObject(source)) {
    throw new EgressNotCredentialFreeError(`${path}.source is not an object`);
  }
  // The source is a vault REFERENCE only: `{ kind: 'vault', vault_id }`. Any
  // other key here (e.g. an inlined `secret` / `token`) is a structural
  // violation — this is the heart of the sidecar's secret-free posture.
  assertNoExtraKeys(source, ALLOWED_KEYS.sidecarSource, `${path}.source`);
  if (source.kind !== 'vault') {
    throw new EgressNotCredentialFreeError(
      `${path}.source.kind must be 'vault' (a reference, not an inlined secret)`,
    );
  }
  if (typeof source.vault_id !== 'string' || source.vault_id.length === 0) {
    throw new EgressNotCredentialFreeError(
      `${path}.source.vault_id must be a non-empty vault reference`,
    );
  }
}

function assertJwtField(value: unknown, path: PathStep): void {
  if (typeof value !== 'string') {
    throw new EgressNotCredentialFreeError(`${path} must be a string token`);
  }
  if (!isJwtShapedOrEmpty(value)) {
    throw new EgressNotCredentialFreeError(`${path} is not JWT-shaped (possible raw secret)`);
  }
}

/**
 * Structurally assert that a whole {@link AgentSnapshot} is credential-free: its
 * envelope carries only the known non-credential fields (an unexpected top-level
 * key fails loud) and its egress block passes {@link assertEgressCredentialFree}.
 * Throws {@link EgressNotCredentialFreeError} on the first violation.
 *
 * This is the structural oracle the snapshot/resolver specs assert against in
 * place of a serialize-and-string-match heuristic.
 */
export function assertSnapshotCredentialFree(snapshot: AgentSnapshot): void {
  assertSnapshotCredentialFreeAt(snapshot, 'snapshot');
}

/** Walk a snapshot (top-level or a nested roster-member) at `path`. */
function assertSnapshotCredentialFreeAt(snapshot: AgentSnapshot, path: PathStep): void {
  if (!isPlainObject(snapshot)) {
    throw new EgressNotCredentialFreeError(`${path} is not an object`);
  }
  assertNoExtraKeys(snapshot as Record<string, unknown>, ALLOWED_SNAPSHOT_KEYS, path);
  assertEgressCredentialFree(snapshot.egress);
  if (snapshot.managed_resources !== undefined) {
    const managed = snapshot.managed_resources;
    if (
      !isPlainObject(managed) ||
      managed.version !== 1 ||
      typeof managed.revision !== 'string' ||
      !/^[a-f0-9]{64}$/.test(managed.revision)
    )
      throw new EgressNotCredentialFreeError('invalid managed resource revision');
    assertNoExtraKeys(managed, new Set(['version', 'revision']), `${path}.managed_resources`);
  }
  if (snapshot.tool_permissions !== undefined) {
    if (
      !isPlainObject(snapshot.tool_permissions) ||
      Object.values(snapshot.tool_permissions).some(
        (policy) => !isManagedToolPermissionPolicy(policy),
      )
    )
      throw new EgressNotCredentialFreeError('invalid tool permissions');
  }
  if (
    snapshot.default_tool_permission !== undefined &&
    !isManagedToolPermissionPolicy(snapshot.default_tool_permission)
  )
    throw new EgressNotCredentialFreeError('invalid default tool permission');
  if (snapshot.request_guardrails_owner !== undefined) {
    if (
      snapshot.request_guardrails_owner !== 'registry' ||
      (snapshot.provider !== 'codex-sdk' && snapshot.provider !== 'pi-sdk') ||
      snapshot.managed_resources === undefined ||
      snapshot.multiagent !== undefined
    )
      throw new EgressNotCredentialFreeError('invalid request guardrails owner');
  }
  if (snapshot.custom_tools !== undefined) {
    if (!Array.isArray(snapshot.custom_tools))
      throw new EgressNotCredentialFreeError('custom tools must be an array');
    const names = new Set<string>();
    for (const [index, tool] of snapshot.custom_tools.entries()) {
      const toolPath = `${path}.custom_tools[${index}]`;
      if (!isPlainObject(tool))
        throw new EgressNotCredentialFreeError(`${toolPath} must be an object`);
      assertNoExtraKeys(tool, new Set(['name', 'description', 'input_schema']), toolPath);
      if (
        !ToolDef.safeParse({ ...tool, type: 'custom' }).success ||
        typeof tool.name !== 'string' ||
        !tool.name ||
        tool.name.startsWith('mcp__') ||
        tool.name.startsWith('sys_terminal_') ||
        names.has(tool.name) ||
        !snapshot.allowed_tool_names.includes(tool.name)
      )
        throw new EgressNotCredentialFreeError(`${toolPath} is invalid or disallowed`);
      names.add(tool.name);
    }
  }
  if (snapshot.harness_state !== undefined) {
    if (snapshot.provider !== 'codex-sdk' && snapshot.provider !== 'pi-sdk')
      throw new EgressNotCredentialFreeError('native checkpoint belongs to another harness');
    validateSdkCheckpoint(
      snapshot.harness_state,
      snapshot.provider === 'pi-sdk' ? 'pi_sdk' : 'codex_sdk',
    );
  }
  const multiagent = (snapshot as { multiagent?: unknown }).multiagent;
  if (multiagent !== undefined) {
    assertMultiagent(multiagent, `${path}.multiagent`);
  }
}

/**
 * Structurally walk the runtime multiagent block: only the known keys, and each
 * `{ agent_name, snapshot }` member's NESTED snapshot is walked recursively — so a
 * secret smuggled into a roster member's sub-snapshot fails loud exactly like one on
 * the top-level snapshot. A `{ type: 'self' }` member carries no embedded snapshot.
 */
function assertMultiagent(multiagent: unknown, path: PathStep): void {
  if (!isPlainObject(multiagent)) {
    throw new EgressNotCredentialFreeError(`${path} is not an object`);
  }
  assertNoExtraKeys(multiagent, ALLOWED_MULTIAGENT_KEYS.root, path);
  const agents = multiagent.agents;
  if (!Array.isArray(agents)) {
    throw new EgressNotCredentialFreeError(`${path}.agents is not an array`);
  }
  agents.forEach((member, i) => assertMultiagentMember(member, `${path}.agents[${i}]`));
}

function assertMultiagentMember(member: unknown, path: PathStep): void {
  if (!isPlainObject(member)) {
    throw new EgressNotCredentialFreeError(`${path} is not an object`);
  }
  // A self member is `{ type: 'self', agent_name }` — no embedded snapshot to walk.
  if (member.type === 'self') {
    assertNoExtraKeys(member, ALLOWED_MULTIAGENT_KEYS.selfMember, path);
    return;
  }
  // Otherwise an `{ agent_name, snapshot }` member — recurse into the sub-snapshot.
  assertNoExtraKeys(member, ALLOWED_MULTIAGENT_KEYS.agentMember, path);
  assertSnapshotCredentialFreeAt(member.snapshot as AgentSnapshot, `${path}.snapshot`);
}
