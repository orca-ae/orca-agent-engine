// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { HarnessTurnRequest, HarnessTurnSnapshot } from '@orca/harness-catalog';
/**
 * Registry HTTP client used by the harness-server dispatcher.
 *
 * Every call made by the harness targets the mesh-internal registry listener.
 * A shared harness is a workload identity, not a workspace principal, so it
 * must never authenticate with a public workspace api-key. Tenant scope is
 * explicit in the workspace/session composite paths below.
 *
 * Returned types intentionally model only the fields harness-server reads.
 */

import type { MemoryRecord, MemoryVersionRecord } from '@orca/memory-store';
import type { StateUpdate } from '@orca/guardrails';
import type { Packages } from '../sandbox/sandbox-runtime.js';
import type { InternalServiceTokenProvider } from '../auth/internal-service-token.js';
import type { ModelEffort, ModelSpeed } from '../harness/agent-harness.js';

/**
 * Per Anthropic Managed Agents semantics, credentials bind to MCP servers via
 * `Credential.auth.mcp_server_url`. A credential id field is intentionally NOT
 * on the agent's mcp_servers entries — at session start, harness-server matches
 * each entry's `url` against session credential URLs (first match wins).
 */
export interface AgentMcpServerEntry {
  name: string;
  url: string;
}

export interface AgentToolEntry {
  type: string;
  [key: string]: unknown;
}

export interface AgentMultiagentRef {
  type: 'agent';
  id: string;
  version: number;
}

export interface AgentMultiagentConfig {
  type: 'coordinator';
  agents: AgentMultiagentRef[];
}

/** Exact immutable Skill bundle pinned by Registry for one agent in a Session. */
export interface SkillDescriptor {
  id: string;
  skill_id: string;
  source: 'anthropic' | 'custom';
  version_identifier: string;
  name: string;
  description: string;
  entrypoint: 'SKILL.md';
  package_sha256: string;
  package_size_bytes: number;
}

export interface AgentRecord {
  id: string;
  name: string;
  workspace_id: string;
  version: number;
  model?: {
    provider: string;
    id: string;
    speed?: ModelSpeed;
    effort?: ModelEffort;
  };
  model_provider?: string;
  model_id?: string;
  model_speed?: ModelSpeed;
  model_effort?: ModelEffort;
  system: string | null;
  tools: AgentToolEntry[];
  mcp_servers: AgentMcpServerEntry[];
  /** Ordered catalog scoped to this agent only. */
  skills: SkillDescriptor[];
  metadata?: Record<string, unknown>;
  multiagent?: AgentMultiagentConfig | null;
}

export interface SessionResourceEntry {
  id: string;
  type: 'file' | 'memory_store' | 'github_repository';
  file_id: string | null;
  memory_store_id: string | null;
  repo_ref: Record<string, unknown> | null;
  mount_path: string;
  access: 'read_only' | 'read_write';
  instructions: string | null;
  attached_at: string;
  detached_at: string | null;
  /** Immutable file metadata resolved by the prepared-execution endpoint. */
  file?: {
    filename: string;
    mime_type: string;
    size_bytes: number;
    sha256: string;
    purpose: 'agent' | 'agent_output';
  } | null;
  /** Present on prepared execution resources of type `memory_store`. */
  memory_store?: { id: string; name: string; workspace_id: string } | null;
  /**
   * Public file resources are always materialized host-side. This is kept on
   * the internal snapshot so the harness can fail closed if a future Registry
   * accidentally reintroduces a sandbox-visible file S3 grant.
   */
  mount_strategy?: 'tarball_prefetch' | null;
}

export interface SessionRecord {
  id: string;
  /** Session-scoped Orca extensions, including the optional LLM egress selection. */
  metadata?: Record<string, unknown>;
  agent_id: string;
  agent_version: number;
  status?: 'idle' | 'running' | 'rescheduling' | 'terminated';
  sandbox_handle_id?: string | null;
  started_at?: string | null;
  last_active_at?: string | null;
  stats?: {
    active_seconds?: number;
    duration_seconds?: number;
  };
  usage?: SessionUsageDelta;
  /** Authoritative usage-backed shared guardrail keys returned by Registry. */
  guardrail_usage_state?: Record<string, unknown>;
  /** Authoritative per-principal UTC-window counters returned by Registry. */
  guardrail_subject_window_state?: Record<string, unknown>;
  /**
   * Present in prepared-execution responses and checked against the event's
   * workspace before any runner work begins.
   */
  workspace_id: string;
  /** Single producer selected by Registry for authoritative LLM usage writes. */
  usage_writer?: 'harness' | 'ai-gateway';
  /** Monotonic Registry revision for fields that affect runner boot. */
  runtime_revision?: number;
  /** Private Codex recovery state and compare-and-swap revision from preparation. */
  harness_state?: unknown;
  harness_state_revision?: string | null;
  harness_ownership_revision?: number;
  vault_ids?: string[];
  /**
   * Session-local override of the agent's tools (managed-agents-2026-04-01
   * UpdateSession). `null`/absent means "no override — use the pinned agent
   * version's tools". A non-null array is a FULL REPLACEMENT.
   */
  tools?: AgentToolEntry[] | null;
  /** Session-local override of the agent's mcp_servers. Same semantics as `tools`. */
  mcp_servers?: AgentMcpServerEntry[] | null;
  environment_id?: string | null;
  resources?: SessionResourceEntry[];
}

export interface SessionUsageDelta {
  cache_creation?: {
    ephemeral_1h_input_tokens?: number;
    ephemeral_5m_input_tokens?: number;
  };
  cache_read_input_tokens?: number;
  input_tokens?: number;
  output_tokens?: number;
}

export interface VaultCredentialRuntimeRecord {
  credential_id: string;
  vault_id: string;
  /** Null for environment_variable credentials, which are not MCP-server bound. */
  mcp_server_url?: string | null;
  auth_type: 'static_bearer' | 'mcp_oauth' | 'environment_variable' | string;
  secret_name?: string | null;
  networking?: unknown;
}

export interface EnvironmentRecord {
  id: string;
  workspace_id: string;
  name: string;
  packages?: Packages;
  networking?: Record<string, unknown>;
  image?: string | null;
  target?: 'cloud' | 'self_hosted' | null;
}

/**
 * Immutable control-plane input for one runner generation. Registry validates
 * every referenced resource against `(workspace_id, session_id)` before
 * returning this envelope. Each agent carries its own ordered, pinned Skill
 * descriptors; Harness never flattens or re-resolves those bindings.
 */
export interface PreparedExecutionV2 {
  schema_version: 2;
  workspace_id: string;
  session: SessionRecord;
  primary_agent: AgentRecord;
  subagents: AgentRecord[];
  environment: EnvironmentRecord | null;
  vault_credentials: VaultCredentialRuntimeRecord[];
  resources: SessionResourceEntry[];
  /** Guardrails applying to this session, ordered by authority and compiled. */
  guardrails?: PreparedGuardrailRecord[];
  /** Session-scoped guardrail counters, restored at every preparation. */
  guardrail_state?: Record<string, unknown>;
}

/**
 * A guardrail as the runtime receives it. Mirrors the registry contract by
 * hand — these two definitions must stay in lock-step, as the rest of this
 * file's records already do.
 */
export interface PreparedGuardrailRecord {
  id: string;
  name: string;
  tier: 'session' | 'agent' | 'workspace' | 'organization';
  phases: string[];
  rule: unknown;
  stateful: boolean;
  state_scope?: 'turn' | 'session' | 'subject_window';
  subagent_id?: string;
}

type PreparedExecutionWire = Omit<PreparedExecutionV2, 'schema_version'> & {
  schema_version?: 2;
};

export class RegistryInvalidRuntimeBindingError extends Error {
  constructor(
    readonly resourceType: string,
    readonly resourceId: string,
  ) {
    super(`invalid runtime binding: ${resourceType} ${resourceId}`);
    this.name = 'RegistryInvalidRuntimeBindingError';
  }
}

export class RegistryClient {
  constructor(
    private readonly internalBaseUrl: string,
    private readonly tokenProvider: InternalServiceTokenProvider,
  ) {}

  async getExecutionOwner(input: {
    workspaceId: string;
    sessionId: string;
  }): Promise<'registry' | 'harness-server' | null> {
    const res = await this.request(`${this.sessionBase(input)}/execution-owner`, { method: 'GET' });
    if (res.status === 404) return null;
    if (!res.ok) throw new Error(`getExecutionOwner failed: ${res.status}`);
    const body = (await res.json()) as { owner?: unknown };
    if (body.owner !== 'registry' && body.owner !== 'harness-server') {
      throw new Error('getExecutionOwner returned an invalid owner');
    }
    return body.owner;
  }

  async prepareExecution(input: {
    workspaceId: string;
    sessionId: string;
    signal?: AbortSignal;
  }): Promise<PreparedExecutionV2 | null> {
    const res = await this.request(`${this.sessionBase(input)}/executions:prepare`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}',
      ...(input.signal ? { signal: input.signal } : {}),
    });
    if (res.status === 404) return null;
    if (!res.ok) {
      const responseText = await res.text().catch(() => '');
      if (res.status === 409) {
        const body = parseJsonRecord(responseText);
        if (
          body?.['error'] === 'invalid_runtime_binding' &&
          typeof body['resource_type'] === 'string' &&
          typeof body['resource_id'] === 'string'
        ) {
          throw new RegistryInvalidRuntimeBindingError(body['resource_type'], body['resource_id']);
        }
      }
      throw new Error(
        `prepareExecution ${input.workspaceId}/${input.sessionId} failed: ${res.status} ${responseText}`,
      );
    }
    const wire = (await res.json()) as PreparedExecutionWire;
    if (wire.schema_version !== 2) {
      throw new Error('prepareExecution returned an unsupported schema version');
    }
    const usageWriter = wire.session.usage_writer;
    if (usageWriter !== undefined && usageWriter !== 'harness' && usageWriter !== 'ai-gateway') {
      throw new Error('prepareExecution returned an unsupported usage writer');
    }
    return {
      schema_version: 2,
      workspace_id: wire.workspace_id,
      session: {
        ...wire.session,
        // Backward-compatible with Registry versions predating explicit usage
        // authority: Harness remains the safe writer until Gateway sink
        // enablement is positively confirmed by Registry.
        usage_writer: usageWriter ?? 'harness',
        resources: wire.resources,
      },
      primary_agent: wire.primary_agent,
      subagents: wire.subagents,
      environment: wire.environment,
      vault_credentials: wire.vault_credentials,
      resources: wire.resources,
      ...(wire.guardrails ? { guardrails: wire.guardrails } : {}),
      ...(wire.guardrail_state ? { guardrail_state: wire.guardrail_state } : {}),
    };
  }

  async harnessTurn(input: {
    workspaceId: string;
    sessionId: string;
    request: HarnessTurnRequest;
  }): Promise<HarnessTurnSnapshot | null> {
    const response = await this.request(`${this.sessionBase(input)}/harness-turn`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(input.request),
    });
    if (!response.ok) {
      if (response.status === 409) {
        const body = parseJsonRecord(await response.text().catch(() => ''));
        if (
          body?.['error'] === 'invalid_runtime_binding' &&
          typeof body['resource_type'] === 'string' &&
          typeof body['resource_id'] === 'string'
        )
          throw new RegistryInvalidRuntimeBindingError(body['resource_type'], body['resource_id']);
      }
      throw new Error(`harnessTurn ${input.sessionId} failed: ${response.status}`);
    }
    const value = (await response.json()) as HarnessTurnSnapshot | null;
    if (
      value !== null &&
      (!Number.isSafeInteger(value.ownershipRevision) ||
        !Number.isSafeInteger(value.runtimeRevision))
    )
      throw new Error('invalid harness turn acknowledgment');
    return value;
  }

  async saveHarnessState(input: {
    workspaceId: string;
    sessionId: string;
    runtimeRevision: number;
    expectedCheckpointRevision: string | null;
    state: unknown;
  }): Promise<string> {
    const res = await this.request(`${this.sessionBase(input)}/harness-state`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        runtime_revision: input.runtimeRevision,
        expected_checkpoint_revision: input.expectedCheckpointRevision,
        state: input.state,
      }),
    });
    if (!res.ok) {
      // Native rollout data may contain user content; never echo a response/body in errors.
      throw new Error(`saveHarnessState ${input.sessionId} failed: ${res.status}`);
    }
    const result = (await res.json()) as { checkpoint_revision?: unknown };
    if (
      typeof result.checkpoint_revision !== 'string' ||
      !/^[a-f0-9]{64}$/.test(result.checkpoint_revision)
    ) {
      throw new Error('saveHarnessState returned an invalid checkpoint revision');
    }
    return result.checkpoint_revision;
  }

  async updateSessionStateInternal(input: {
    workspaceId: string;
    sessionId: string;
    status: 'idle' | 'running' | 'rescheduling' | 'terminated';
    sandboxHandleId?: string | null;
  }): Promise<SessionRecord | null> {
    const body: { status: string; sandbox_handle_id?: string | null } = {
      status: input.status,
    };
    if (Object.prototype.hasOwnProperty.call(input, 'sandboxHandleId')) {
      body.sandbox_handle_id = input.sandboxHandleId ?? null;
    }
    const res = await this.request(`${this.sessionBase(input)}/state`, {
      method: 'PATCH',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (res.status === 404) return null;
    if (!res.ok) {
      throw new Error(
        `updateSessionStateInternal ${input.sessionId} failed: ${res.status} ${await res.text().catch(() => '')}`,
      );
    }
    return (await res.json()) as SessionRecord;
  }

  async recordSessionUsageInternal(input: {
    workspaceId: string;
    sessionId: string;
    usage: SessionUsageDelta;
    model?: string;
    provider?: string;
    subagentId?: string;
    turnEventId?: string;
    usageEventId?: string;
  }): Promise<SessionRecord | null> {
    const res = await this.request(`${this.sessionBase(input)}/usage`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        usage: input.usage,
        ...(input.model ? { model: input.model } : {}),
        ...(input.provider ? { provider: input.provider } : {}),
        ...(input.subagentId ? { subagent_id: input.subagentId } : {}),
        ...(input.turnEventId ? { turn_event_id: input.turnEventId } : {}),
        ...(input.usageEventId ? { usage_event_id: input.usageEventId } : {}),
      }),
    });
    if (res.status === 404) return null;
    if (!res.ok) {
      throw new Error(
        `recordSessionUsageInternal ${input.sessionId} failed: ${res.status} ${await res.text().catch(() => '')}`,
      );
    }
    return (await res.json()) as SessionRecord;
  }

  async refreshGuardrailSubjectWindowInternal(input: {
    workspaceId: string;
    sessionId: string;
    turnEventId: string;
  }): Promise<Record<string, unknown>> {
    const res = await this.request(`${this.sessionBase(input)}/guardrail-subject-window`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ turn_event_id: input.turnEventId }),
    });
    if (!res.ok) {
      throw new Error(
        `refreshGuardrailSubjectWindowInternal ${input.sessionId} failed: ${res.status} ${await res.text().catch(() => '')}`,
      );
    }
    const body = (await res.json()) as { guardrail_subject_window_state?: unknown };
    return isRecord(body.guardrail_subject_window_state) ? body.guardrail_subject_window_state : {};
  }

  async applyGuardrailStateInternal(input: {
    workspaceId: string;
    sessionId: string;
    updates: readonly (Omit<StateUpdate, 'scope'> & {
      scope: 'session' | 'subject_window';
    })[];
    subject?: string;
    window?: string;
    signal?: AbortSignal;
  }): Promise<number> {
    const res = await this.request(`${this.sessionBase(input)}/guardrail-state`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        updates: input.updates,
        ...(input.subject ? { subject: input.subject } : {}),
        ...(input.window ? { window: input.window } : {}),
      }),
      ...(input.signal ? { signal: input.signal } : {}),
    });
    if (!res.ok) {
      throw new Error(
        `applyGuardrailStateInternal ${input.sessionId} failed: ${res.status} ${await res.text().catch(() => '')}`,
      );
    }
    const body = (await res.json()) as { applied: number };
    return body.applied;
  }

  async mintSessionJwt(
    workspaceId: string,
    sessionId: string,
    mcpServerNames: string[],
    vaultIds: string[],
    signal: AbortSignal,
  ): Promise<{ token: string; expiresAt: number }> {
    const url = `${this.sessionBase({ workspaceId, sessionId })}/mint-jwt`;
    // The session JWT provider owns the refresh deadline. This layer only
    // observes its signal, including while acquiring the workload identity.
    const abortError = () => new DOMException('mintSessionJwt request cancelled', 'AbortError');
    if (signal.aborted) throw abortError();
    let onAbort!: () => void;
    const aborted = new Promise<never>((_resolve, reject) => {
      onAbort = () => reject(abortError());
      signal.addEventListener('abort', onAbort, { once: true });
    });
    // The race also bounds workload-token acquisition and response-body reading.
    // Token providers cannot be cancelled; a late token must never start a fetch.
    const request = (async () => {
      let res: Response;
      try {
        const token = await this.tokenProvider();
        signal.throwIfAborted();
        res = await fetch(url, {
          method: 'POST',
          headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
          body: JSON.stringify({ mcp_server_names: mcpServerNames, vault_ids: vaultIds }),
          signal,
        });
        signal.throwIfAborted();
      } catch {
        if (signal.aborted) throw abortError();
        throw new Error('mintSessionJwt request failed');
      }
      if (!res.ok) {
        // Cancel rather than read an error body that may contain sensitive material.
        void res.body?.cancel().catch(() => {});
        throw new Error(`mintSessionJwt failed: HTTP ${res.status}`);
      }
      let body: unknown;
      try {
        body = await res.json();
      } catch {
        throw new Error('mintSessionJwt returned an invalid response');
      }
      if (signal.aborted) throw abortError();
      if (
        !isRecord(body) ||
        typeof body.token !== 'string' ||
        body.token.length === 0 ||
        /\s/.test(body.token) ||
        typeof body.expires_at !== 'number' ||
        !Number.isSafeInteger(body.expires_at) ||
        body.expires_at <= Date.now() / 1000
      ) {
        throw new Error('mintSessionJwt returned an invalid response');
      }
      return { token: body.token, expiresAt: body.expires_at };
    })();
    try {
      // Consumes late rejections even if a token provider or mocked fetch ignores abort.
      return await Promise.race([request, aborted]);
    } finally {
      signal.removeEventListener('abort', onAbort);
    }
  }

  /**
   * Mint a session-scoped JWT with `aud='git-creds'` and a pinned
   * `repo_urls` allowlist. Hits the same workspace/session-scoped `mint-jwt`
   * route as {@link mintSessionJwt}. The minter encodes them into the JWT
   * payload (as the `aud` claim and `repo_urls` custom claim respectively) so
   * the in-sandbox `orca-git-creds` helper can call `/v1/git-creds` on behalf
   * of the agent. Returns the same `{ token, expires_at }` envelope.
   */
  async mintGitCredsJwt(input: {
    workspaceId: string;
    sessionId: string;
    repoUrls: string[];
  }): Promise<{ token: string; expiresAt: number }> {
    const res = await this.request(`${this.sessionBase(input)}/mint-jwt`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        audience: 'git-creds',
        repo_urls: input.repoUrls,
      }),
    });
    if (!res.ok) {
      throw new Error(`mintGitCredsJwt failed: ${res.status} ${await res.text().catch(() => '')}`);
    }
    const body = (await res.json()) as { token: string; expires_at: number };
    return { token: body.token, expiresAt: body.expires_at };
  }

  /**
   * Mint a session JWT for the ai-gateway LLM route. Hits the same scoped
   * route as {@link mintSessionJwt} and
   * {@link mintGitCredsJwt}. It requests no MCP servers, so `mcp_server_names`
   * stays empty; Registry ignores the body's `vault_ids` and derives
   * `vault_ids`/`credential_ids` from the session's persisted Vault bindings,
   * and LLM route/model claims from its trusted startup policy.
   */
  async mintLlmGatewayJwt(
    workspaceId: string,
    sessionId: string,
    signal?: AbortSignal,
  ): Promise<{ token: string; expiresAt: number }> {
    const res = await this.request(`${this.sessionBase({ workspaceId, sessionId })}/mint-jwt`, {
      method: 'POST',
      signal,
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        audience: 'ai-gateway',
        mcp_server_names: [],
        vault_ids: [],
      }),
    });
    if (!res.ok) {
      throw new Error(
        `mintLlmGatewayJwt failed: ${res.status} ${await res.text().catch(() => '')}`,
      );
    }
    const body = (await res.json()) as { token: string; expires_at: number };
    return { token: body.token, expiresAt: body.expires_at };
  }

  /**
   * Resolve the dedicated repo credential model's PAT for host-side clone.
   * Secret bytes stay in harness memory only for the clone operation.
   */
  async resolveGitCredentialSecret(input: {
    workspaceId: string;
    sessionId: string;
    gitCredentialId: string;
  }): Promise<string> {
    assertInternalId('git credential', input.gitCredentialId, /^gitcred_[A-Za-z0-9_-]+$/);
    const res = await this.request(
      `${this.sessionBase(input)}/git-credentials/${encodeURIComponent(input.gitCredentialId)}/resolve`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{}',
      },
    );
    if (!res.ok) {
      throw new Error(
        `resolveGitCredentialSecret failed: ${res.status} ${await res.text().catch(() => '')}`,
      );
    }
    const body = (await res.json()) as { secret_value: string };
    return body.secret_value;
  }

  /**
   * Register an output through the workspace/session-scoped internal route.
   * Registry derives workspace, purpose, scope, and downloadability from the
   * path and accepts only the file bytes from the harness.
   *
   * The bytes are buffered into a `Blob` rather than streamed — the indexer
   * caps each file at `maxBytesPerFile` (default 500 MB, matching Anthropic's
   * Files API contract) before calling here, so there's a hard upper bound
   * on memory pressure.
   */
  async createFile(input: {
    workspaceId: string;
    sessionId: string;
    content: Buffer;
    filename: string;
    mimeType: string;
    signal?: AbortSignal;
  }): Promise<RegistryFileRecord> {
    const form = new FormData();
    form.append(
      'file',
      new Blob([new Uint8Array(input.content)], { type: input.mimeType }),
      input.filename,
    );

    const res = await this.request(`${this.sessionBase(input)}/files`, {
      method: 'POST',
      body: form,
      ...(input.signal ? { signal: input.signal } : {}),
    });
    if (!res.ok) {
      throw new Error(
        `createFile ${input.filename} failed: ${res.status} ${await res.text().catch(() => '')}`,
      );
    }
    return (await res.json()) as RegistryFileRecord;
  }

  /**
   * List all memories in a store that is attached to this session. Registry
   * validates the full workspace/session/store relationship.
   */
  async listSessionMemories(input: {
    workspaceId: string;
    sessionId: string;
    storeId: string;
    signal?: AbortSignal;
  }): Promise<MemoryRecord[]> {
    const base = this.sessionMemoryBase(input);
    const res = await this.request(`${base}/memories`, {
      ...(input.signal ? { signal: input.signal } : {}),
    });
    if (!res.ok) {
      throw new Error(
        `listSessionMemories ${input.storeId} failed: ${res.status} ${await res.text().catch(() => '')}`,
      );
    }
    const body = (await res.json()) as { data?: ApiMemory[]; memories?: ApiMemory[] };
    const memories = Array.isArray(body.data) ? body.data : body.memories;
    if (!Array.isArray(memories)) {
      throw new Error(`listSessionMemories ${input.storeId} returned invalid list envelope`);
    }
    return memories.map(apiToMemory);
  }

  /**
   * Read the live bytes of a memory by id. Used by the dispatcher to seed
   * the InMemory sandbox FS with each existing memory's
   * bytes at session-spawn — a Session B that reads `/mnt/memory/.../foo.txt`
   * must see what Session A wrote, even though the InMemory runtime's tmpdir
   * is fresh per acquire(). The S3 / FUSE path doesn't need this seeding
   * because the bytes are already in the bucket; the local-tmpdir path does.
   *
   * Returns `null` on 404 so the dispatcher's seeding loop can tolerate a
   * raced delete between `listSessionMemories` and this fetch (treat-as-empty).
   */
  async getSessionMemoryContent(input: {
    workspaceId: string;
    sessionId: string;
    storeId: string;
    memoryId: string;
    signal?: AbortSignal;
  }): Promise<Buffer | null> {
    assertInternalId('memory', input.memoryId, /^mem_[A-Za-z0-9_-]+$/);
    const base = this.sessionMemoryBase(input);
    const res = await this.request(
      `${base}/memories/${encodeURIComponent(input.memoryId)}/content`,
      input.signal ? { signal: input.signal } : undefined,
    );
    if (res.status === 404) return null;
    if (!res.ok) {
      throw new Error(
        `getSessionMemoryContent ${input.memoryId} failed: ${res.status} ${await res.text().catch(() => '')}`,
      );
    }
    const arrayBuf = await res.arrayBuffer();
    return Buffer.from(arrayBuf);
  }

  async listSessionMemoryVersions(input: {
    workspaceId: string;
    sessionId: string;
    storeId: string;
    memoryId: string;
    signal?: AbortSignal;
  }): Promise<MemoryVersionRecord[]> {
    assertInternalId('memory', input.memoryId, /^mem_[A-Za-z0-9_-]+$/);
    const params = new URLSearchParams({ memory_id: input.memoryId });
    const base = this.sessionMemoryBase(input);
    const res = await this.request(`${base}/memory-versions?${params.toString()}`, {
      ...(input.signal ? { signal: input.signal } : {}),
    });
    if (!res.ok) {
      throw new Error(
        `listSessionMemoryVersions ${input.memoryId} failed: ${res.status} ${await res.text().catch(() => '')}`,
      );
    }
    const body = (await res.json()) as { data: ApiMemoryVersion[]; next_page: string | null };
    return body.data.map(apiToMemoryVersion);
  }

  /**
   * Register a memory version observed by the harness watcher. Hits the
   * scoped mesh-internal memory-version route (last-writer-wins). Unlike `POST /v1/memory_stores/:id/memories/:memory_id`, this
   * route falls back to a non-CAS write when `previous_sha256` no longer
   * matches the live state and returns `conflict: true` so the watcher can
   * emit a `session.memory_conflict` event on the transcript stream.
   */
  async recordSessionMemoryVersion(input: {
    workspaceId: string;
    sessionId: string;
    storeId: string;
    path: string;
    contentBase64: string;
    contentSha256: string;
    previousSha256: string | null;
    versionId?: string;
    writtenByEventId?: string;
    signal?: AbortSignal;
  }): Promise<{ memory: MemoryRecord; version: MemoryVersionRecord; conflict: boolean }> {
    const body: Record<string, unknown> = {
      path: input.path,
      content_base64: input.contentBase64,
      content_sha256: input.contentSha256,
      previous_sha256: input.previousSha256,
      ...(input.versionId !== undefined ? { version_id: input.versionId } : {}),
    };
    if (input.writtenByEventId !== undefined) body.written_by_event_id = input.writtenByEventId;
    const res = await this.request(`${this.sessionMemoryBase(input)}/memory-versions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
      ...(input.signal ? { signal: input.signal } : {}),
    });
    if (!res.ok) {
      throw new Error(
        `recordSessionMemoryVersion failed: ${res.status} ${await res.text().catch(() => '')}`,
      );
    }
    const json = (await res.json()) as {
      memory: ApiMemory;
      version: ApiMemoryVersion;
      conflict: boolean;
    };
    return {
      memory: apiToMemory(json.memory),
      version: apiToMemoryVersion(json.version),
      conflict: json.conflict,
    };
  }

  private sessionMemoryBase(input: {
    workspaceId: string;
    sessionId: string;
    storeId: string;
  }): string {
    assertInternalId('memory store', input.storeId, /^mems_[A-Za-z0-9_-]+$/);
    return `${this.sessionBase(input)}/memory-stores/${encodeURIComponent(input.storeId)}`;
  }

  private async request(url: string, init?: RequestInit): Promise<Response> {
    const headers = new Headers(init?.headers);
    headers.set('authorization', `Bearer ${await this.tokenProvider()}`);
    return fetch(url, { ...init, headers });
  }

  private sessionBase(input: { workspaceId: string; sessionId: string }): string {
    assertInternalId('workspace', input.workspaceId, /^[A-Za-z0-9_-]{1,128}$/);
    assertInternalId('session', input.sessionId, /^ses_[A-Za-z0-9_-]+$/);
    return (
      `${this.internalBaseUrl}/internal/v1/workspaces/${encodeURIComponent(input.workspaceId)}` +
      `/sessions/${encodeURIComponent(input.sessionId)}`
    );
  }
}

function parseJsonRecord(value: string): Record<string, unknown> | null {
  try {
    const parsed: unknown = JSON.parse(value);
    return parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

function assertInternalId(label: string, value: string, pattern: RegExp): void {
  if (!pattern.test(value)) {
    throw new Error(`RegistryClient: invalid ${label} id "${value}"`);
  }
}

/**
 * Snake-case JSON shapes returned by the scoped memory-store routes. The
 * harness translates these to camelCase via
 * the `apiTo*` converters below so downstream code can use the
 * `@orca/memory-store` library types directly.
 */
interface ApiMemory {
  id: string;
  store_id: string;
  path: string;
  current_sha256: string;
  size_bytes: number;
  updated_at: string;
  updated_by_session_id: string | null;
  updated_by_event_id: string | null;
}

interface ApiMemoryVersion {
  id: string;
  store_id: string;
  memory_id: string;
  path: string;
  sha256: string;
  size_bytes: number;
  written_by_session_id: string | null;
  written_by_event_id: string | null;
  written_at: string;
  redacted_at: string | null;
}

function apiToMemory(r: ApiMemory): MemoryRecord {
  return {
    id: r.id,
    storeId: r.store_id,
    path: r.path,
    currentSha256: r.current_sha256,
    sizeBytes: r.size_bytes,
    updatedAt: new Date(r.updated_at),
    updatedBySessionId: r.updated_by_session_id,
    updatedByEventId: r.updated_by_event_id,
  };
}

function apiToMemoryVersion(r: ApiMemoryVersion): MemoryVersionRecord {
  return {
    id: r.id,
    storeId: r.store_id,
    memoryId: r.memory_id,
    path: r.path,
    sha256: r.sha256,
    sizeBytes: r.size_bytes,
    writtenBySessionId: r.written_by_session_id,
    writtenByEventId: r.written_by_event_id,
    writtenAt: new Date(r.written_at),
    redactedAt: r.redacted_at ? new Date(r.redacted_at) : null,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/**
 * Mirrors the registry's `File` zod (see `files.contract.ts`). We model only
 * the fields the harness reads after `createFile`.
 */
export interface RegistryFileRecord {
  id: string;
  filename: string;
  mime_type: string;
  size_bytes: number;
  sha256: string;
  metadata: Record<string, string>;
  purpose: 'agent' | 'agent_output';
  scope_id: string | null;
  downloadable: boolean;
  archived_at: string | null;
  created_at: string;
  updated_at: string;
}
