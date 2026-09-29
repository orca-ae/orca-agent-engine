// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the agent-snapshot RESOLVER.
//
// The resolver is the impure orchestrator that turns a session id into a
// credential-free snapshot: it resolves the records (session, agent, skills,
// environment, vaults), mints the scoped session JWT IN-PROCESS (the registry
// owns the minter — no HTTP round-trip), selects the egress strategy by the
// environment's `egress_mode`, and calls the pure snapshot builder.
//
// Everything is driven against in-memory fakes for the record loaders + a real
// `SessionJwtMinter` (the registry's own minter, network-free), so the resolver's
// orchestration is unit-testable without a DB. The assertions pin:
//   - PROVIDER resolved from the agent's harness annotation (metadata.harness);
//   - skills resolved + composed, tool allowlists intersected;
//   - the vault→url map built first-match-wins from the session vaults;
//   - gateway mode embeds the rewritten servers + a minted, audience-scoped JWT;
//   - sidecar mode embeds vault-reference bindings (NO minted gateway JWT, NO
//     rewritten servers, NO raw secret);
//   - the whole snapshot is credential-free.

import { describe, it, expect } from 'vitest';
import { decodeJwt, importSPKI, jwtVerify } from 'jose';
import { SessionJwtMinter } from '../../src/auth/session-jwt.js';
import {
  AgentSnapshotResolver,
  DEFAULT_LLM_PROXY_JWT_TTL_SECS,
  LLM_PROXY_AUDIENCE,
  resolveAgentMode,
  type SnapshotRecordLoader,
  type ResolverAgentRecord,
  type ResolverSessionRecord,
  type ResolverEnvironmentRecord,
  type ResolverVaultRecord,
  type ResolverSkillVersionRecord,
} from '../../src/domain/agent-snapshot-resolver.js';
import type { PreparedSkillDescriptor } from '../../src/contracts/internal.contract.js';
import { assertSnapshotCredentialFree } from '../../src/domain/egress-credential-free.js';

// A throwaway RS256 keypair for the minter (PKCS8). Generated once for the spec.
import { generateKeyPairSync } from 'node:crypto';

const { privateKey, publicKey } = generateKeyPairSync('rsa', {
  modulusLength: 2048,
  publicKeyEncoding: { type: 'spki', format: 'pem' },
  privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
});

function makeMinter(): SessionJwtMinter {
  return new SessionJwtMinter({
    privateKeyPem: privateKey,
    issuer: 'orca-registry',
    audience: 'ai-gateway',
    ttlSecs: 300,
  });
}

const WORKSPACE_ID = 'ws_acme';
const SESSION_ID = 'ses_resolver_1';
// The owning organization id — required on every `ResolverSessionRecord` since
// it is threaded straight through to the minted JWTs' required `org_id` claim
// (see `src/auth/session-jwt.ts`'s `SessionJwtMinter.mint`).
const ORGANIZATION_ID = 'org_acme';

/** An in-memory record loader the resolver reads through. */
class FakeRecordLoader implements SnapshotRecordLoader {
  constructor(
    private readonly data: {
      session: ResolverSessionRecord | null;
      agent: ResolverAgentRecord | null;
      environment: ResolverEnvironmentRecord | null;
      skillVersions: Record<string, ResolverSkillVersionRecord>;
      vaults: Record<string, ResolverVaultRecord>;
      /** The SESSION-WIDE Skill-bundle union the loader returns (progressive disclosure). */
      sessionSkillBundles?: PreparedSkillDescriptor[];
      /** When set, `loadSessionSkillBundles` throws it (a missing/foreign binding fails loud). */
      sessionSkillBundlesError?: Error;
    },
  ) {}

  async loadSession(sessionId: string): Promise<ResolverSessionRecord | null> {
    return sessionId === this.data.session?.id ? this.data.session : null;
  }
  async loadSessionSkillBundles(
    workspaceId: string,
    sessionId: string,
  ): Promise<PreparedSkillDescriptor[]> {
    if (this.data.sessionSkillBundlesError) throw this.data.sessionSkillBundlesError;
    return workspaceId === WORKSPACE_ID && sessionId === this.data.session?.id
      ? (this.data.sessionSkillBundles ?? [])
      : [];
  }
  async loadAgent(workspaceId: string, agentId: string): Promise<ResolverAgentRecord | null> {
    return this.data.agent && this.data.agent.id === agentId && workspaceId === WORKSPACE_ID
      ? this.data.agent
      : null;
  }
  async loadEnvironment(environmentId: string): Promise<ResolverEnvironmentRecord | null> {
    return environmentId === this.data.environment?.id ? this.data.environment : null;
  }
  async loadSkillVersion(skillVersionId: string): Promise<ResolverSkillVersionRecord | null> {
    return this.data.skillVersions[skillVersionId] ?? null;
  }
  async loadVault(workspaceId: string, vaultId: string): Promise<ResolverVaultRecord | null> {
    const v = this.data.vaults[vaultId];
    return v && workspaceId === WORKSPACE_ID ? v : null;
  }
}

function gatewayResolver(loaderData: ConstructorParameters<typeof FakeRecordLoader>[0]) {
  return new AgentSnapshotResolver({
    loader: new FakeRecordLoader(loaderData),
    minter: makeMinter(),
    gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
    gatewayLlmUrl: 'https://ai-gateway.internal/llm',
  });
}

/**
 * A resolver constructed with NO gateway URL — the pure-sidecar deployment
 * posture (no ai-gateway at all). Sidecar egress must resolve end to end against
 * this; gateway egress against it is a configuration error that throws.
 */
function sidecarResolver(loaderData: ConstructorParameters<typeof FakeRecordLoader>[0]) {
  return new AgentSnapshotResolver({
    loader: new FakeRecordLoader(loaderData),
    minter: makeMinter(),
  });
}

describe('resolveAgentMode', () => {
  it('resolves colocated from an explicit metadata.mode', () => {
    expect(resolveAgentMode({ harness: 'mock', mode: 'colocated' })).toBe('colocated');
  });

  it('resolves separate from an explicit metadata.mode', () => {
    expect(resolveAgentMode({ mode: 'separate' })).toBe('separate');
  });

  it('resolves the default mode when metadata is empty/undefined/null', () => {
    // The platform DEFAULT_MODE is 'separate' — an unannotated agent's loop
    // stays in harness-server, matching resolveAgentProvider's own default
    // (claude_agent_sdk, a 'separate'-only harness).
    expect(resolveAgentMode({})).toBe('separate');
    expect(resolveAgentMode(undefined)).toBe('separate');
    expect(resolveAgentMode(null)).toBe('separate');
  });

  it('rejects an invalid mode instead of selecting a different harness', () => {
    expect(() => resolveAgentMode({ mode: 'not-a-real-mode' })).toThrow('metadata.mode');
  });

  it('rejects an unsupported harness and mode combination', () => {
    // 'mock' only supports colocated (see the catalog) — asking it to run
    // separate is an invalid (harness, mode) combination.
    expect(() => resolveAgentMode({ harness: 'mock', mode: 'separate' })).toThrow(
      'does not support',
    );
  });

  it("resolves a harness-only annotation to that harness's own default (first supported) mode", () => {
    // 'mock' supports only colocated, so omitting `mode` resolves to it —
    // the same derivation resolveAgentProvider relies on for its own default.
    expect(resolveAgentMode({ harness: 'mock' })).toBe('colocated');
  });
});

describe('AgentSnapshotResolver (gateway egress)', () => {
  const loaderData = {
    session: {
      id: SESSION_ID,
      workspaceId: WORKSPACE_ID,
      organizationId: ORGANIZATION_ID,
      agentId: 'agt_1',
      environmentId: 'env_1',
      vaultIds: ['vlt_gh'],
    },
    agent: {
      id: 'agt_1',
      model: { provider: 'anthropic', id: 'claude-opus-4' },
      system: 'Agent system.',
      tools: [{ type: 'agent_toolset' }, { type: 'mcp_toolset', mcp_server_name: 'github' }],
      mcpServers: [{ name: 'github', url: 'https://github.example/mcp' }],
      skills: ['sklv_a'],
      metadata: { harness: 'claude_code', mode: 'colocated' },
    },
    environment: {
      id: 'env_1',
      target: 'self_hosted' as const,
      egressMode: 'gateway' as const,
    },
    skillVersions: {
      sklv_a: {
        id: 'sklv_a',
        workspaceId: WORKSPACE_ID,
        systemPrompt: 'Skill A.',
        toolAllowlist: null,
      },
    },
    vaults: {
      vlt_gh: {
        id: 'vlt_gh',
        targetUrl: 'https://github.example/mcp',
        archived: false,
      },
    },
  };

  it.each([
    ['codex_sdk', 'openai', 'gpt-5.4', 'llm-responses'],
    ['pi_sdk', 'openai', 'gpt-5.4', 'llm-pi-openai-openai-responses'],
    ['pi_sdk', 'anthropic', 'claude-sonnet-4-6', 'llm-pi-anthropic-anthropic-messages'],
    ['pi_sdk', 'deepseek', 'deepseek-flash', 'llm-pi-deepseek-openai-completions'],
  ] as const)(
    'pins %s and narrows gateway claims to its overridden model',
    async (harness, provider, model, route) => {
      const data = {
        ...loaderData,
        session: {
          ...loaderData.session,
          agentVersion: 2,
          agentOverrides: { model: { provider, id: model, effort: 'high' } },
        },
        agent: {
          ...loaderData.agent,
          metadata: { harness, mode: 'colocated' },
          skills: [],
          model: { provider, id: model },
        },
      };
      const loader = new FakeRecordLoader(data);
      let requestedVersion: number | undefined;
      const pinnedLoader: SnapshotRecordLoader = {
        loadSession: loader.loadSession.bind(loader),
        loadAgent: async () => {
          throw new Error('must use pinned version');
        },
        loadEnvironment: loader.loadEnvironment.bind(loader),
        loadSkillVersion: loader.loadSkillVersion.bind(loader),
        loadVault: loader.loadVault.bind(loader),
        loadAgentVersion: async (_workspace, _agent, version) => {
          requestedVersion = version;
          return data.agent;
        },
      };
      const minter = makeMinter();
      const resolver = new AgentSnapshotResolver({
        loader: pinnedLoader,
        minter,
        gatewayMcpUrl: 'https://gateway/v1/mcp',
        gatewayLlmUrl: 'https://gateway/v1',
        llmPolicy: {
          routes: [
            'llm-responses',
            'llm-messages',
            'llm-chat',
            'llm-pi-openai-openai-responses',
            'llm-pi-anthropic-anthropic-messages',
            'llm-pi-deepseek-openai-completions',
          ],
          models: ['gpt-*', 'claude-*', 'deepseek-*'],
        },
      });
      const snap = await resolver.resolve(SESSION_ID);
      expect(requestedVersion).toBe(2);
      expect(snap?.provider).toBe(harness.replace('_', '-'));
      expect(snap?.model).toMatchObject({ provider, id: model, effort: 'high' });
      if (snap?.egress.mode !== 'gateway') throw new Error('expected gateway');
      const claims = await minter.verify(snap.egress.gateway.llm_jwt!);
      expect(claims.llmRoutes).toEqual([route]);
      expect(claims.llmModels).toEqual([model]);
      expect(claims.mcpServerNames).toEqual([]);
      const decoded = decodeJwt(snap.egress.gateway.llm_jwt!);
      expect(decoded.exp! - decoded.iat!).toBe(660);
      // The MCP client is rebuilt from the fresh snapshot before every turn;
      // its credential must also cover the full bounded ten-minute turn.
      const mcpDecoded = decodeJwt(snap.egress.gateway.session_jwt);
      expect(mcpDecoded.exp! - mcpDecoded.iat!).toBe(660);
      await expect(
        jwtVerify(snap.egress.gateway.session_jwt, await importSPKI(publicKey, 'RS256'), {
          issuer: 'orca-registry',
          audience: 'ai-gateway',
          currentDate: new Date((mcpDecoded.iat! + 600) * 1000),
        }),
      ).resolves.toMatchObject({ payload: { mcp_server_names: ['github'] } });
      await expect(
        new AgentSnapshotResolver({
          loader: pinnedLoader,
          minter,
          gatewayMcpUrl: 'https://gateway/v1/mcp',
          gatewayLlmUrl: 'https://gateway/v1',
          llmPolicy: { routes: ['llm-responses'], models: ['not-allowed-*'] },
        }).resolve(SESSION_ID),
      ).rejects.toThrow('SESSION_JWT_LLM');
    },
  );

  it('resolves the provider from the agent harness annotation', async () => {
    const snap = await gatewayResolver(loaderData).resolve(SESSION_ID);
    // The fixture agent is annotated `harness: 'claude_code'`, which the catalog maps
    // to the NATIVE Claude Code CLI provider — not to the in-process Agent SDK
    // `claude` provider that `claude_agent_sdk` selects. This resolver is the single
    // writer of `snapshot.provider`, and it is the only place the two are told apart:
    // their event streams look nearly identical, so a substitution here is invisible
    // downstream.
    expect(snap!.provider).toBe('claude-code');
  });

  it.each([undefined, 'separate'])(
    'rejects persisted self-hosted Codex mode %s before minting runner credentials',
    async (mode) => {
      const data = {
        ...loaderData,
        agent: {
          ...loaderData.agent,
          metadata: { harness: 'codex_sdk', ...(mode ? { mode } : {}) },
          skills: [],
          model: { provider: 'openai', id: 'gpt-5.4' },
        },
      };
      await expect(gatewayResolver(data).resolve(SESSION_ID)).rejects.toThrow(
        /codex_sdk\/separate.*self_hosted/,
      );
    },
  );

  it('carries the agent system prompt (skills are bundles, not eagerly composed in)', async () => {
    // Under progressive Skill disclosure a skill version is a materialized-on-demand
    // bundle, not an inline system-prompt slice: the eager composition gets NO slices,
    // so `composeSystemPrompt` (packages/harness-catalog) adds nothing and the delivered
    // `system` is just the agent's own. The Skill union rides `snapshot.skills` instead
    // (sourced from `session_skill_bindings`, tested above), for the runner to materialize.
    const snap = await gatewayResolver(loaderData).resolve(SESSION_ID);
    expect(snap!.system).toBe('Agent system.');
  });

  it('carries the model and expands the agent toolset', async () => {
    const snap = await gatewayResolver(loaderData).resolve(SESSION_ID);
    expect(snap!.model).toEqual({ provider: 'anthropic', id: 'claude-opus-4' });
    expect(snap!.allowed_tool_names.sort()).toEqual(
      ['bash', 'delete', 'edit', 'glob', 'grep', 'list', 'read', 'write'].sort(),
    );
    expect(snap!.allowed_mcp_server_names).toEqual(['github']);
  });

  it('composes guardrails and restored state onto the colocated snapshot', async () => {
    const loader: SnapshotRecordLoader = new FakeRecordLoader({
      ...loaderData,
      session: { ...loaderData.session, guardrailIds: ['grd_session'] },
      agent: { ...loaderData.agent, guardrailIds: ['grd_agent'] },
    });
    loader.loadGuardrailContext = async () => ({
      visible: [
        {
          id: 'grd_workspace',
          name: 'Workspace request screen',
          enabled: true,
          phases: ['request'],
          scope: 'workspace',
          rule: {
            kind: 'expression',
            expression: `event.session.id != ''`,
            onFalse: 'deny',
          },
        },
        {
          id: 'grd_agent',
          name: 'Agent request screen',
          enabled: true,
          phases: ['request'],
          scope: 'explicit',
          rule: {
            kind: 'expression',
            expression: `event.session.id != ''`,
            onFalse: 'deny',
          },
        },
        {
          id: 'grd_session',
          name: 'Session request screen',
          enabled: true,
          phases: ['request'],
          scope: 'explicit',
          rule: {
            kind: 'expression',
            expression: `event.session.id != ''`,
            onFalse: 'deny',
          },
        },
      ],
      state: { 'g:grd_session:seen': 2 },
    });
    const resolver = new AgentSnapshotResolver({
      loader,
      minter: makeMinter(),
      gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
      gatewayLlmUrl: 'https://ai-gateway.internal/llm',
    });

    const snap = await resolver.resolve(SESSION_ID);

    expect(snap!.guardrails?.map(({ id, tier }) => ({ id, tier }))).toEqual([
      { id: 'grd_session', tier: 'session' },
      { id: 'grd_agent', tier: 'agent' },
      { id: 'grd_workspace', tier: 'workspace' },
    ]);
    expect(snap!.guardrail_state).toEqual({ 'g:grd_session:seen': 2 });
    expect(() => assertSnapshotCredentialFree(snap!)).not.toThrow();
  });

  it('builds gateway egress with a minted, audience-scoped JWT and rewritten servers', async () => {
    const snap = await gatewayResolver(loaderData).resolve(SESSION_ID);
    expect(snap!.egress.mode).toBe('gateway');
    if (snap!.egress.mode !== 'gateway') throw new Error('unreachable');
    const gw = snap!.egress.gateway;
    expect(gw.mcp_base_url).toBe('https://ai-gateway.internal/mcp');
    expect(gw.llm_base_url).toBe('https://ai-gateway.internal/llm');
    // The JWT is a real signed token (three dot-separated base64url segments).
    expect(gw.session_jwt.split('.')).toHaveLength(3);
    expect(gw.llm_jwt!.split('.')).toHaveLength(3);
    // The matched session vault is attached to the github server (first-match-wins).
    const gh = gw.mcp_servers['github']!;
    expect(gh.url).toBe('https://ai-gateway.internal/mcp');
    expect(gh.headers['X-Orca-Vault-Id']).toBe('vlt_gh');
    expect(gh.headers['Authorization']).toBe(`Bearer ${gw.session_jwt}`);
  });

  it('mints a JWT whose claims carry the session, mcp servers, and vault allowlist', async () => {
    const minter = makeMinter();
    const resolver = new AgentSnapshotResolver({
      loader: new FakeRecordLoader(loaderData),
      minter,
      gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
      gatewayLlmUrl: 'https://ai-gateway.internal/llm',
    });
    const snap = await resolver.resolve(SESSION_ID);
    if (snap!.egress.mode !== 'gateway') throw new Error('unreachable');
    const verified = await minter.verify(snap!.egress.gateway.session_jwt, {
      expectedAudience: 'ai-gateway',
    });
    expect(verified.sessionId).toBe(SESSION_ID);
    expect(verified.workspaceId).toBe(WORKSPACE_ID);
    expect(verified.mcpServerNames).toEqual(['github']);
    expect(verified.vaultIds).toEqual(['vlt_gh']);
  });

  it('mints Gateway JWTs with agent and guardrail identity from resolved records', async () => {
    const minter = makeMinter();
    const resolver = new AgentSnapshotResolver({
      loader: new FakeRecordLoader({
        ...loaderData,
        session: { ...loaderData.session, guardrailIds: ['grd_session'], runtimeRevision: 7 },
        agent: { ...loaderData.agent, guardrailIds: ['grd_agent'] },
      }),
      minter,
      gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
      gatewayLlmUrl: 'https://ai-gateway.internal/llm',
    });
    const snap = await resolver.resolve(SESSION_ID);
    if (snap!.egress.mode !== 'gateway') throw new Error('unreachable');
    for (const [token, audience] of [
      [snap!.egress.gateway.session_jwt, 'ai-gateway'],
      [snap!.egress.gateway.llm_jwt!, LLM_PROXY_AUDIENCE],
    ]) {
      const verified = await minter.verify(token!, { expectedAudience: audience! });
      expect(verified.agentId).toBe('agt_1');
      expect(verified.guardrailIds).toEqual(['grd_session', 'grd_agent']);
      expect(verified.runtimeConfigRevision).toBe('7');
    }
  });

  it('mints the LLM-proxy JWT with a DISTINCT, end-to-end-verified audience', async () => {
    // The LLM token is an independently-scoped credential. Its
    // audience is verified END TO END (not just at the minter unit level): the
    // gateway's LLM route accepts it only as `aud='llm-proxy'`.
    const minter = makeMinter();
    const resolver = new AgentSnapshotResolver({
      loader: new FakeRecordLoader(loaderData),
      minter,
      gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
      gatewayLlmUrl: 'https://ai-gateway.internal/llm',
    });
    const snap = await resolver.resolve(SESSION_ID);
    if (snap!.egress.mode !== 'gateway') throw new Error('unreachable');
    const llmJwt = snap!.egress.gateway.llm_jwt!;
    // The gateway's LLM route verifies the token against the `llm-proxy` audience.
    const verified = await minter.verify(llmJwt, { expectedAudience: LLM_PROXY_AUDIENCE });
    expect(verified.audience).toBe(LLM_PROXY_AUDIENCE);
    expect(verified.sessionId).toBe(SESSION_ID);
    expect(verified.workspaceId).toBe(WORKSPACE_ID);
    // The LLM token carries no MCP-server / vault scope — it reaches only the LLM.
    expect(verified.mcpServerNames).toEqual([]);
    expect(verified.vaultIds).toEqual([]);
  });

  it('rejects the LLM-proxy JWT against the MCP audience (no cross-audience replay)', async () => {
    // The MCP/session JWT carries `aud='ai-gateway'`; the LLM JWT carries
    // `aud='llm-proxy'`. Neither may be replayed against the other route's
    // audience — verified end to end in the egress path, not just at the minter.
    const minter = makeMinter();
    const resolver = new AgentSnapshotResolver({
      loader: new FakeRecordLoader(loaderData),
      minter,
      gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
      gatewayLlmUrl: 'https://ai-gateway.internal/llm',
    });
    const snap = await resolver.resolve(SESSION_ID);
    if (snap!.egress.mode !== 'gateway') throw new Error('unreachable');
    const gw = snap!.egress.gateway;
    // The LLM token is NOT accepted as an ai-gateway (MCP) token.
    await expect(minter.verify(gw.llm_jwt!, { expectedAudience: 'ai-gateway' })).rejects.toThrow();
    // …and the MCP token is NOT accepted as an llm-proxy token.
    await expect(
      minter.verify(gw.session_jwt, { expectedAudience: LLM_PROXY_AUDIENCE }),
    ).rejects.toThrow();
  });

  it('mints the LLM-proxy JWT with a DISTINCT, shorter TTL than the session/MCP JWT', async () => {
    // The LLM token is independently-scoped and SHORT-lived. It
    // does NOT inherit the session/MCP JWT's TTL (here the minter default, 300s);
    // it carries its own shorter lifetime so a leaked LLM token expires fast.
    const snap = await gatewayResolver(loaderData).resolve(SESSION_ID);
    if (snap!.egress.mode !== 'gateway') throw new Error('unreachable');
    const gw = snap!.egress.gateway;
    const ttlOf = (jwt: string): number => {
      const { iat, exp } = decodeJwt(jwt);
      if (typeof iat !== 'number' || typeof exp !== 'number') {
        throw new Error('jwt missing iat/exp');
      }
      return exp - iat;
    };
    const llmTtl = ttlOf(gw.llm_jwt!);
    const sessionTtl = ttlOf(gw.session_jwt);
    // The default LLM TTL is the short-lived 120s; the session/MCP TTL is 300s.
    expect(llmTtl).toBe(DEFAULT_LLM_PROXY_JWT_TTL_SECS);
    expect(sessionTtl).toBe(300);
    expect(llmTtl).toBeLessThan(sessionTtl);
  });

  it('honors an explicit llmJwtTtlSecs override for the LLM-proxy JWT only', async () => {
    // The LLM TTL is independently tunable (wired from AI_GATEWAY_LLM_JWT_TTL_SECS)
    // without touching the session/MCP JWT lifetime.
    const resolver = new AgentSnapshotResolver({
      loader: new FakeRecordLoader(loaderData),
      minter: makeMinter(),
      gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
      gatewayLlmUrl: 'https://ai-gateway.internal/llm',
      llmJwtTtlSecs: 45,
    });
    const snap = await resolver.resolve(SESSION_ID);
    if (snap!.egress.mode !== 'gateway') throw new Error('unreachable');
    const gw = snap!.egress.gateway;
    const ttlOf = (jwt: string): number => {
      const { iat, exp } = decodeJwt(jwt);
      if (typeof iat !== 'number' || typeof exp !== 'number') {
        throw new Error('jwt missing iat/exp');
      }
      return exp - iat;
    };
    expect(ttlOf(gw.llm_jwt!)).toBe(45);
    // The session/MCP JWT keeps the minter's default TTL (unaffected by the override).
    expect(ttlOf(gw.session_jwt)).toBe(300);
  });

  it('returns null when the session does not exist', async () => {
    const snap = await gatewayResolver(loaderData).resolve('ses_missing');
    expect(snap).toBeNull();
  });

  it('returns null when the agent does not exist (deleted / cross-workspace)', async () => {
    const data = { ...loaderData, agent: null };
    const snap = await gatewayResolver(data).resolve(SESSION_ID);
    expect(snap).toBeNull();
  });

  it('defaults to gateway egress when the session has no environment', async () => {
    const data = {
      ...loaderData,
      session: { ...loaderData.session, environmentId: null },
      environment: null,
    };
    const snap = await gatewayResolver(data).resolve(SESSION_ID);
    // A session with no environment has no egress_mode → gateway (the default).
    expect(snap!.egress.mode).toBe('gateway');
  });

  it('attaches the session-wide skill-bundle union (from sessionSkillBindings) to the snapshot', async () => {
    // Progressive Skill disclosure: the resolver sources the colocated Skill union from
    // the pinned `session_skill_bindings` rows (via the loader seam), NOT `agents.skills`,
    // and attaches it to the top-level snapshot for the runner to materialize.
    const bundle: PreparedSkillDescriptor = {
      id: 'sklv_a',
      skill_id: 'skl_a',
      source: 'anthropic',
      version_identifier: '1',
      name: 'alpha',
      description: 'Alpha skill.',
      entrypoint: 'SKILL.md',
      package_sha256: 'a'.repeat(64),
      package_size_bytes: 128,
    };
    const snap = await gatewayResolver({ ...loaderData, sessionSkillBundles: [bundle] }).resolve(
      SESSION_ID,
    );
    expect(snap!.skills).toEqual([bundle]);
    // The union is credential-free metadata (no bytes) — the structural guard accepts it.
    expect(() => assertSnapshotCredentialFree(snap!)).not.toThrow();
  });

  it('omits skills entirely when the session has no skill bindings (skill-free unchanged)', async () => {
    const snap = await gatewayResolver(loaderData).resolve(SESSION_ID);
    // No `session_skill_bindings` → the field is absent (byte-for-byte unchanged path).
    expect(snap!.skills).toBeUndefined();
  });

  it('fails loud when a session skill binding is missing/foreign (loader throws)', async () => {
    const data = {
      ...loaderData,
      sessionSkillBundlesError: new Error(
        'snapshot: session skill binding sklv_a not found in workspace',
      ),
    };
    await expect(gatewayResolver(data).resolve(SESSION_ID)).rejects.toThrow(
      /not found in workspace/,
    );
  });

  it('skips an archived session vault when building the url→vault map', async () => {
    const data = {
      ...loaderData,
      vaults: {
        vlt_gh: { id: 'vlt_gh', targetUrl: 'https://github.example/mcp', archived: true },
      },
    };
    const snap = await gatewayResolver(data).resolve(SESSION_ID);
    if (snap!.egress.mode !== 'gateway') throw new Error('unreachable');
    // Archived vault is excluded → no vault header on the server.
    expect(snap!.egress.gateway.mcp_servers['github']!.headers['X-Orca-Vault-Id']).toBeUndefined();
  });

  it('is credential-free end to end (STRUCTURALLY, against the real minted JWTs)', async () => {
    // The resolver mints REAL signed JWTs and embeds them; the structural check
    // accepts them as JWT-shaped opaque tokens while rejecting any field outside
    // the secret-free allow-list — so a novel secret-shaped field anywhere in the
    // resolved snapshot would fail loud (a string heuristic would miss it).
    const snap = await gatewayResolver(loaderData).resolve(SESSION_ID);
    expect(snap).not.toBeNull();
    expect(() => assertSnapshotCredentialFree(snap!)).not.toThrow();
  });

  it('does NOT mint a session JWT when the agent has zero MCP servers', async () => {
    // The resolver's `serverNames.length > 0` guard: with no servers,
    // `rewriteMcpServers` returns {} so no rewritten server would carry the token
    // — minting one would put an unused short-lived credential in the snapshot.
    const data = {
      ...loaderData,
      agent: {
        ...loaderData.agent,
        tools: [{ type: 'agent_toolset' }],
        mcpServers: [],
      },
    };
    const snap = await gatewayResolver(data).resolve(SESSION_ID);
    if (snap!.egress.mode !== 'gateway') throw new Error('unreachable');
    const gw = snap!.egress.gateway;
    // Gateway base URL is still pinned; no session JWT is minted; no servers.
    expect(gw.mcp_base_url).toBe('https://ai-gateway.internal/mcp');
    expect(gw.session_jwt).toBe('');
    expect(gw.mcp_servers).toEqual({});
    // No `Bearer <session jwt>` anywhere (no server carries the empty token).
    expect(JSON.stringify(gw.mcp_servers)).not.toContain('Bearer ');
    // The LLM-proxy JWT is independent of the MCP-server guard and is still minted.
    expect(gw.llm_jwt!.split('.')).toHaveLength(3);
  });
});

describe('AgentSnapshotResolver (sidecar egress)', () => {
  const loaderData = {
    session: {
      id: SESSION_ID,
      workspaceId: WORKSPACE_ID,
      organizationId: ORGANIZATION_ID,
      agentId: 'agt_1',
      environmentId: 'env_1',
      vaultIds: ['vlt_gh', 'vlt_api'],
    },
    agent: {
      id: 'agt_1',
      model: { provider: 'anthropic', id: 'claude-opus-4' },
      system: 'Agent system.',
      tools: [{ type: 'agent_toolset' }],
      mcpServers: [],
      skills: [],
      // No harness annotation → the default provider.
      metadata: {},
    },
    environment: {
      id: 'env_1',
      target: 'self_hosted' as const,
      egressMode: 'sidecar' as const,
    },
    skillVersions: {},
    vaults: {
      // A GitHub-CLI vault bound to the API host → token scheme + GH_TOKEN/
      // GITHUB_TOKEN injection (the gh_basic api-host preset).
      vlt_gh: {
        id: 'vlt_gh',
        targetUrl: 'https://api.github.com',
        targetKind: 'gh_basic',
        archived: false,
      },
      // A plain bearer SaaS vault → bearer scheme, swap-on-access, no injection.
      vlt_api: {
        id: 'vlt_api',
        targetUrl: 'https://api.example.com',
        targetKind: 'https_bearer',
        archived: false,
      },
    },
  };

  it('builds a sidecar credential-proxy spec from the session vaults — no minted gateway JWT', async () => {
    // Resolved against a resolver with NO gateway URL — the pure-sidecar posture.
    const snap = await sidecarResolver(loaderData).resolve(SESSION_ID);
    expect(snap!.egress.mode).toBe('sidecar');
    if (snap!.egress.mode !== 'sidecar') throw new Error('unreachable');
    const entries = snap!.egress.sidecar.entries;
    expect(entries).toHaveLength(2);

    // gh_basic on an api.* host → token scheme + gh env injection, no username.
    const gh = entries.find((e) => e.host === 'api.github.com')!;
    expect(gh.scheme).toBe('token');
    expect(gh.username).toBeUndefined();
    expect(gh.inject_env).toEqual(['GH_TOKEN', 'GITHUB_TOKEN']);
    expect(gh.source).toEqual({ kind: 'vault', vault_id: 'vlt_gh' });

    // https_bearer → bearer scheme, no username, no injection.
    const api = entries.find((e) => e.host === 'api.example.com')!;
    expect(api.scheme).toBe('bearer');
    expect(api.username).toBeUndefined();
    expect(api.inject_env).toEqual([]);
    expect(api.source).toEqual({ kind: 'vault', vault_id: 'vlt_api' });
  });

  it("resolves a vault credential with no target_kind (loadVault's real shape under main's model) to the bearer sidecar scheme", async () => {
    // `loadVault` (`api/snapshot-loader.ts`) never sources a `target_kind`
    // under main's model — vault_credentials carries no equivalent column
    // (see its doc comment), so every REAL vault record the resolver sees
    // omits `targetKind` entirely. The fixtures above pin explicit-but-
    // synthetic kinds; this one pins the loader's actual output shape, so a
    // future change to the bearer default is deliberate, not accidental.
    const data = {
      ...loaderData,
      session: { ...loaderData.session, vaultIds: ['vlt_bearer'] },
      vaults: {
        vlt_bearer: {
          id: 'vlt_bearer',
          targetUrl: 'https://api.example.com',
          archived: false,
          // targetKind intentionally omitted — matches loadVault's real output.
        },
      },
    };
    const snap = await sidecarResolver(data).resolve(SESSION_ID);
    if (snap!.egress.mode !== 'sidecar') throw new Error('unreachable');
    const entries = snap!.egress.sidecar.entries;
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({
      host: 'api.example.com',
      scheme: 'bearer',
      source: { kind: 'vault', vault_id: 'vlt_bearer' },
    });
    expect(entries[0]!.username).toBeUndefined();
    expect(entries[0]!.inject_env).toEqual([]);
  });

  it('maps a git_https vault to a basic swap-on-access binding (x-access-token, no injection)', async () => {
    const data = {
      ...loaderData,
      session: { ...loaderData.session, vaultIds: ['vlt_git'] },
      vaults: {
        vlt_git: {
          id: 'vlt_git',
          targetUrl: 'https://gitlab.example/group/repo.git',
          targetKind: 'git_https',
          archived: false,
        },
      },
    };
    const snap = await sidecarResolver(data).resolve(SESSION_ID);
    if (snap!.egress.mode !== 'sidecar') throw new Error('unreachable');
    const e = snap!.egress.sidecar.entries[0]!;
    expect(e.host).toBe('gitlab.example');
    expect(e.scheme).toBe('basic');
    expect(e.username).toBe('x-access-token');
    expect(e.inject_env).toEqual([]);
  });

  it('dedups two session vaults on the same host (first-match-wins, parity with the gateway URL map)', async () => {
    // The credential-proxy enforces ONE exact-host rewrite rule per
    // host. Two session vaults resolving to the same host — even with different
    // kinds — must collapse to a SINGLE entry, the first one listed (matching the
    // gateway path's first-match-wins `vaultByUrl` map). A later same-host vault is
    // dropped, never emitting a second ambiguous entry the proxy would never reach.
    const data = {
      ...loaderData,
      session: { ...loaderData.session, vaultIds: ['vlt_first', 'vlt_second'] },
      vaults: {
        // First vault for github.com — git_https → basic swap-on-access. Wins.
        vlt_first: {
          id: 'vlt_first',
          targetUrl: 'https://github.com/org/repo.git',
          targetKind: 'git_https',
          archived: false,
        },
        // Second vault, SAME host, different kind (https_bearer). Dropped.
        vlt_second: {
          id: 'vlt_second',
          targetUrl: 'https://github.com/api',
          targetKind: 'https_bearer',
          archived: false,
        },
      },
    };
    const snap = await sidecarResolver(data).resolve(SESSION_ID);
    if (snap!.egress.mode !== 'sidecar') throw new Error('unreachable');
    const entries = snap!.egress.sidecar.entries;
    // Exactly one entry for github.com — the FIRST vault wins.
    expect(entries).toHaveLength(1);
    const e = entries[0]!;
    expect(e.host).toBe('github.com');
    expect(e.scheme).toBe('basic'); // git_https preset, not the dropped bearer
    expect(e.source).toEqual({ kind: 'vault', vault_id: 'vlt_first' });
    // The dropped second vault must not appear anywhere in the spec.
    expect(JSON.stringify(snap!.egress.sidecar)).not.toContain('vlt_second');
  });

  it('dedups same-host vaults case-insensitively (canonical lower-cased host match)', async () => {
    // Host comparison is canonical: `GitHub.com` and `github.com` are the same
    // host, so the second is dropped even though the raw URL casing differs.
    const data = {
      ...loaderData,
      session: { ...loaderData.session, vaultIds: ['vlt_first', 'vlt_second'] },
      vaults: {
        vlt_first: {
          id: 'vlt_first',
          targetUrl: 'https://GitHub.com/org/repo.git',
          targetKind: 'git_https',
          archived: false,
        },
        vlt_second: {
          id: 'vlt_second',
          targetUrl: 'https://github.com/other.git',
          targetKind: 'git_https',
          archived: false,
        },
      },
    };
    const snap = await sidecarResolver(data).resolve(SESSION_ID);
    if (snap!.egress.mode !== 'sidecar') throw new Error('unreachable');
    const entries = snap!.egress.sidecar.entries;
    expect(entries).toHaveLength(1);
    expect(entries[0]!.host).toBe('github.com');
    expect(entries[0]!.source.vault_id).toBe('vlt_first');
  });

  it('defaults the provider when the agent has no harness annotation', async () => {
    const snap = await sidecarResolver(loaderData).resolve(SESSION_ID);
    expect(snap!.provider).toBe('claude');
  });

  it('lifts metadata.custom_spec onto the snapshot for a custom native-CLI agent', async () => {
    // The generic `custom` provider selects its harness via `metadata.harness: 'custom'`
    // and carries its declarative CLI spec in `metadata.custom_spec`; the resolver lifts
    // that spec verbatim onto the snapshot so the runner's custom harness can parse it.
    const spec = {
      command: 'my-cli',
      argv: ['run', '--session', '{sessionId}'],
      stdout: { mode: 'text' },
    };
    const data = {
      ...loaderData,
      agent: {
        ...loaderData.agent,
        metadata: { harness: 'custom', custom_spec: spec },
      },
    };
    const snap = await sidecarResolver(data).resolve(SESSION_ID);
    expect(snap!.provider).toBe('custom');
    expect(snap!.custom_spec).toEqual(spec);
  });

  it('omits custom_spec when the agent declared none (purely additive)', async () => {
    const snap = await sidecarResolver(loaderData).resolve(SESSION_ID);
    expect(snap!.custom_spec).toBeUndefined();
    expect('custom_spec' in snap!).toBe(false);
  });

  it('embeds no minted JWT and no rewritten MCP servers in sidecar mode', async () => {
    const snap = await sidecarResolver(loaderData).resolve(SESSION_ID);
    const serialized = JSON.stringify(snap);
    // No gateway block, hence no JWT, hence no Bearer.
    expect(serialized).not.toContain('Bearer ');
    expect(serialized).not.toContain('mcp_base_url');
  });

  it('is credential-free (STRUCTURALLY vault-reference-only)', async () => {
    const snap = await sidecarResolver(loaderData).resolve(SESSION_ID);
    expect(snap).not.toBeNull();
    // Structural guarantee over the resolved sidecar snapshot: every source is a
    // vault reference, no inlined-secret field anywhere.
    expect(() => assertSnapshotCredentialFree(snap!)).not.toThrow();
    // The vault reference IS present (a reference, never a secret).
    expect(JSON.stringify(snap)).toContain('vlt_gh');
  });

  it('delivers a full snapshot (model/provider/system/egress) with NO gateway configured', async () => {
    // The headline sidecar deliverable: a pure-sidecar deployment (no
    // AI_GATEWAY_MCP_URL) must still get model + provider + composed system +
    // tools + the sidecar egress spec. Regression guard for the gap where the
    // resolver required a gateway URL and a gateway-less boot delivered nothing.
    const snap = await sidecarResolver(loaderData).resolve(SESSION_ID);
    expect(snap).not.toBeNull();
    expect(snap!.model).toEqual({ provider: 'anthropic', id: 'claude-opus-4' });
    expect(snap!.provider).toBe('claude');
    expect(snap!.system).toBe('Agent system.');
    expect(snap!.allowed_tool_names.sort()).toEqual(
      ['bash', 'delete', 'edit', 'glob', 'grep', 'list', 'read', 'write'].sort(),
    );
    expect(snap!.egress.mode).toBe('sidecar');
  });
});

describe('AgentSnapshotResolver (gateway egress without a configured gateway URL)', () => {
  const loaderData = {
    session: {
      id: SESSION_ID,
      workspaceId: WORKSPACE_ID,
      organizationId: ORGANIZATION_ID,
      agentId: 'agt_1',
      environmentId: 'env_1',
      vaultIds: [],
    },
    agent: {
      id: 'agt_1',
      model: { provider: 'anthropic', id: 'claude-opus-4' },
      system: 'Agent system.',
      tools: [{ type: 'agent_toolset' }, { type: 'mcp_toolset', mcp_server_name: 'github' }],
      mcpServers: [{ name: 'github', url: 'https://github.example/mcp' }],
      skills: [],
      metadata: {},
    },
    environment: {
      id: 'env_1',
      target: 'self_hosted' as const,
      egressMode: 'gateway' as const,
    },
    skillVersions: {},
    vaults: {},
  };

  it('throws a loud configuration error (does not emit a broken snapshot)', async () => {
    // Gateway egress genuinely needs the gateway URL (every MCP server is
    // rewritten to it). Selecting gateway egress in a deployment with no gateway
    // URL is a misconfiguration — fail loud rather than deliver servers pointed
    // at `undefined`.
    await expect(sidecarResolver(loaderData).resolve(SESSION_ID)).rejects.toThrow(
      /gateway egress selected but no ai-gateway MCP URL/,
    );
  });
});

// The COORDINATOR FOLD — the registry-side half of runner-thread-orchestration
// reachability. A coordinator agent carries a persisted `multiagent` roster
// (`metadata.multiagent`); the resolver must resolve each member's OWN sub-snapshot
// and fold the runtime `multiagent` block onto the delivered snapshot, so the runner's
// `maybeWrapCoordinator` fires. Previously the resolver never set it, so the runner's
// snapshot.multiagent was always absent in production and the whole thread choreography
// was unreachable end to end.
describe('AgentSnapshotResolver (multiagent coordinator fold)', () => {
  /**
   * A loader over MANY agents keyed by id, with version-pinned reads — the shape the
   * coordinator fold needs (a coordinator + its roster members). `loadAgentVersion`
   * resolves a pinned member; `loadAgent` resolves the current one.
   */
  class MultiAgentLoader implements SnapshotRecordLoader {
    constructor(
      private readonly data: {
        session: ResolverSessionRecord;
        environment: ResolverEnvironmentRecord | null;
        agents: Record<string, ResolverAgentRecord>;
        agentVersions?: Record<string, ResolverAgentRecord>; // key: `${id}@${version}`
        skillVersions?: Record<string, ResolverSkillVersionRecord>;
        vaults?: Record<string, ResolverVaultRecord>;
      },
    ) {}
    async loadSession(sessionId: string): Promise<ResolverSessionRecord | null> {
      return sessionId === this.data.session.id ? this.data.session : null;
    }
    async loadAgent(workspaceId: string, agentId: string): Promise<ResolverAgentRecord | null> {
      const a = this.data.agents[agentId];
      return a && workspaceId === WORKSPACE_ID ? a : null;
    }
    async loadAgentVersion(
      workspaceId: string,
      agentId: string,
      version: number,
    ): Promise<ResolverAgentRecord | null> {
      if (workspaceId !== WORKSPACE_ID) return null;
      return this.data.agentVersions?.[`${agentId}@${version}`] ?? null;
    }
    async loadEnvironment(environmentId: string): Promise<ResolverEnvironmentRecord | null> {
      return environmentId === this.data.environment?.id ? this.data.environment : null;
    }
    async loadSkillVersion(id: string): Promise<ResolverSkillVersionRecord | null> {
      return this.data.skillVersions?.[id] ?? null;
    }
    async loadVault(workspaceId: string, vaultId: string): Promise<ResolverVaultRecord | null> {
      const v = this.data.vaults?.[vaultId];
      return v && workspaceId === WORKSPACE_ID ? v : null;
    }
  }

  function agentRecord(over: Partial<ResolverAgentRecord> & { id: string }): ResolverAgentRecord {
    return {
      model: { provider: 'anthropic', id: 'claude-opus-4' },
      system: '',
      tools: [{ type: 'agent_toolset' }],
      mcpServers: [],
      skills: [],
      metadata: {},
      ...over,
    };
  }

  const environment = {
    id: 'env_1',
    target: 'self_hosted' as const,
    egressMode: 'gateway' as const,
  };

  function resolverOver(loader: SnapshotRecordLoader): AgentSnapshotResolver {
    return new AgentSnapshotResolver({
      loader,
      minter: makeMinter(),
      gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
      gatewayLlmUrl: 'https://ai-gateway.internal/llm',
    });
  }

  it('mints primary-agent guardrail claims from the Session-pinned version', async () => {
    const minter = makeMinter();
    const current = agentRecord({
      id: 'agt_coord',
      guardrailIds: ['grd_current'],
      mcpServers: [{ name: 'github', url: 'https://github.example/mcp' }],
    });
    const pinned = agentRecord({
      id: 'agt_coord',
      guardrailIds: ['grd_pinned'],
      mcpServers: [{ name: 'github', url: 'https://github.example/mcp' }],
    });
    const loader = new MultiAgentLoader({
      session: {
        id: SESSION_ID,
        workspaceId: WORKSPACE_ID,
        organizationId: ORGANIZATION_ID,
        agentId: 'agt_coord',
        agentVersion: 2,
        environmentId: 'env_1',
        vaultIds: [],
        guardrailIds: ['grd_session'],
        runtimeRevision: 7,
      },
      environment,
      agents: { agt_coord: current },
      agentVersions: { 'agt_coord@2': pinned },
    });
    const resolver = new AgentSnapshotResolver({
      loader,
      minter,
      gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
      gatewayLlmUrl: 'https://ai-gateway.internal/llm',
    });
    const snapshot = await resolver.resolve(SESSION_ID);
    if (snapshot?.egress.mode !== 'gateway') throw new Error('expected gateway egress');
    const verified = await minter.verify(snapshot.egress.gateway.session_jwt, {
      expectedAudience: 'ai-gateway',
    });
    expect(verified.guardrailIds).toEqual(['grd_session', 'grd_pinned']);
  });

  it('folds a resolved roster (each member carries its OWN resolved sub-snapshot)', async () => {
    // The coordinator's persisted roster pins a member agent at a version; the resolver
    // resolves that member's OWN snapshot (its own model/system/tools) and folds it.
    const loader = new MultiAgentLoader({
      session: {
        id: SESSION_ID,
        workspaceId: WORKSPACE_ID,
        organizationId: ORGANIZATION_ID,
        agentId: 'agt_coord',
        environmentId: 'env_1',
        vaultIds: [],
      },
      environment,
      agents: {
        agt_coord: agentRecord({
          id: 'agt_coord',
          name: 'Coordinator',
          system: 'coordinate',
          metadata: {
            harness: 'claude_agent_sdk',
            multiagent: {
              type: 'coordinator',
              agents: [{ type: 'agent', id: 'agt_r', version: 3 }],
            },
          },
        }),
        agt_r: agentRecord({ id: 'agt_r', name: 'Researcher-current', system: 'CURRENT' }),
      },
      agentVersions: {
        // The PINNED version (v3) has different system text than the current row —
        // proving the fold reads the version-pinned member, not the live one.
        'agt_r@3': agentRecord({
          id: 'agt_r',
          name: 'Researcher',
          system: 'research pinned',
          model: { provider: 'anthropic', id: 'claude-haiku-4' },
        }),
      },
    });

    const snap = await resolverOver(loader).resolve(SESSION_ID);
    expect(snap).not.toBeNull();
    expect(snap!.multiagent).toBeDefined();
    expect(snap!.multiagent!.type).toBe('coordinator');
    // primary_thread_id is empty at delivery (created lazily by the registry).
    expect(snap!.multiagent!.primary_thread_id).toBe('');
    expect(snap!.multiagent!.agents).toHaveLength(1);
    const member = snap!.multiagent!.agents[0]!;
    if (!('snapshot' in member)) throw new Error('expected a resolved agent member');
    expect(member.agent_name).toBe('Researcher');
    // The member's OWN pinned snapshot (its own model + system), not the coordinator's.
    expect(member.snapshot.system).toBe('research pinned');
    expect(member.snapshot.model.id).toBe('claude-haiku-4');
    expect(member.snapshot.provider).toBe('claude');
    // The coordinator's own fields are still delivered on the top-level snapshot.
    expect(snap!.system).toBe('coordinate');
  });

  it('resolves a self member as { type: self } carrying the coordinator name (runner derives its snapshot)', async () => {
    const loader = new MultiAgentLoader({
      session: {
        id: SESSION_ID,
        workspaceId: WORKSPACE_ID,
        organizationId: ORGANIZATION_ID,
        agentId: 'agt_coord',
        environmentId: 'env_1',
        vaultIds: [],
      },
      environment,
      agents: {
        agt_coord: agentRecord({
          id: 'agt_coord',
          name: 'Coordinator',
          metadata: {
            multiagent: { type: 'coordinator', agents: [{ type: 'self' }] },
          },
        }),
      },
    });
    const snap = await resolverOver(loader).resolve(SESSION_ID);
    expect(snap!.multiagent!.agents).toEqual([{ type: 'self', agent_name: 'Coordinator' }]);
  });

  it('drops a roster member whose agent no longer resolves (degrade, do not fail)', async () => {
    // A member references a deleted/cross-workspace agent → dropped. The surviving
    // member still folds; the coordinator degrades to the roster it can reach.
    const loader = new MultiAgentLoader({
      session: {
        id: SESSION_ID,
        workspaceId: WORKSPACE_ID,
        organizationId: ORGANIZATION_ID,
        agentId: 'agt_coord',
        environmentId: 'env_1',
        vaultIds: [],
      },
      environment,
      agents: {
        agt_coord: agentRecord({
          id: 'agt_coord',
          name: 'Coordinator',
          metadata: {
            multiagent: {
              type: 'coordinator',
              agents: [
                { type: 'agent', id: 'agt_gone', version: 1 },
                { type: 'agent', id: 'agt_ok', version: 1 },
              ],
            },
          },
        }),
        agt_ok: agentRecord({ id: 'agt_ok', name: 'OK' }),
      },
      agentVersions: {
        // Only agt_ok has a pinned version; agt_gone resolves to neither (dropped).
        'agt_ok@1': agentRecord({ id: 'agt_ok', name: 'OK', system: 'ok' }),
      },
    });
    const snap = await resolverOver(loader).resolve(SESSION_ID);
    expect(snap!.multiagent).toBeDefined();
    // The missing member was dropped; only the resolvable one folded.
    expect(snap!.multiagent!.agents).toHaveLength(1);
    const m = snap!.multiagent!.agents[0]!;
    if (!('snapshot' in m)) throw new Error('expected a resolved member');
    expect(m.agent_name).toBe('OK');
  });

  it('falls back to the member current version when no version-pinned loader is available', async () => {
    // An older loader without loadAgentVersion still delivers a working roster from the
    // member's CURRENT definition (best-effort). Build the loader then strip the method.
    const loader = new MultiAgentLoader({
      session: {
        id: SESSION_ID,
        workspaceId: WORKSPACE_ID,
        organizationId: ORGANIZATION_ID,
        agentId: 'agt_coord',
        environmentId: 'env_1',
        vaultIds: [],
      },
      environment,
      agents: {
        agt_coord: agentRecord({
          id: 'agt_coord',
          name: 'Coordinator',
          metadata: {
            multiagent: {
              type: 'coordinator',
              agents: [{ type: 'agent', id: 'agt_r', version: 9 }],
            },
          },
        }),
        agt_r: agentRecord({ id: 'agt_r', name: 'Researcher', system: 'current research' }),
      },
    });
    // Simulate a loader that predates the version-pinned read.
    (loader as { loadAgentVersion?: unknown }).loadAgentVersion = undefined;

    const snap = await resolverOver(loader).resolve(SESSION_ID);
    const m = snap!.multiagent!.agents[0]!;
    if (!('snapshot' in m)) throw new Error('expected a resolved member');
    // Fell back to the CURRENT member definition.
    expect(m.snapshot.system).toBe('current research');
  });

  it('does NOT fold a multiagent block for a single-agent coordinator-less agent', async () => {
    const loader = new MultiAgentLoader({
      session: {
        id: SESSION_ID,
        workspaceId: WORKSPACE_ID,
        organizationId: ORGANIZATION_ID,
        agentId: 'agt_solo',
        environmentId: 'env_1',
        vaultIds: [],
      },
      environment,
      agents: {
        agt_solo: agentRecord({ id: 'agt_solo', name: 'Solo', system: 'solo', metadata: {} }),
      },
    });
    const snap = await resolverOver(loader).resolve(SESSION_ID);
    expect(snap!.multiagent).toBeUndefined();
  });

  it('skips the fold (single-agent) when no roster member resolves', async () => {
    // Every member references a missing agent → the resolved roster is empty → the
    // coordinator runs as a plain agent (no multiagent block).
    const loader = new MultiAgentLoader({
      session: {
        id: SESSION_ID,
        workspaceId: WORKSPACE_ID,
        organizationId: ORGANIZATION_ID,
        agentId: 'agt_coord',
        environmentId: 'env_1',
        vaultIds: [],
      },
      environment,
      agents: {
        agt_coord: agentRecord({
          id: 'agt_coord',
          name: 'Coordinator',
          metadata: {
            multiagent: {
              type: 'coordinator',
              agents: [{ type: 'agent', id: 'agt_gone', version: 1 }],
            },
          },
        }),
      },
    });
    const snap = await resolverOver(loader).resolve(SESSION_ID);
    expect(snap!.multiagent).toBeUndefined();
  });

  it('keeps the folded coordinator snapshot credential-free', async () => {
    // The member sub-snapshots each carry their own minted egress; the whole folded
    // snapshot must still pass the structural credential-free check.
    const loader = new MultiAgentLoader({
      session: {
        id: SESSION_ID,
        workspaceId: WORKSPACE_ID,
        organizationId: ORGANIZATION_ID,
        agentId: 'agt_coord',
        environmentId: 'env_1',
        vaultIds: [],
      },
      environment,
      agents: {
        agt_coord: agentRecord({
          id: 'agt_coord',
          name: 'Coordinator',
          metadata: {
            multiagent: {
              type: 'coordinator',
              agents: [{ type: 'agent', id: 'agt_r', version: 1 }],
            },
          },
        }),
      },
      agentVersions: {
        'agt_r@1': agentRecord({
          id: 'agt_r',
          name: 'Researcher',
          // The member declares its own MCP server → its own minted, scoped JWT.
          tools: [{ type: 'agent_toolset' }, { type: 'mcp_toolset', mcp_server_name: 'github' }],
          mcpServers: [{ name: 'github', url: 'https://github.example/mcp' }],
        }),
      },
    });
    const snap = await resolverOver(loader).resolve(SESSION_ID);
    expect(snap).not.toBeNull();
    expect(() => assertSnapshotCredentialFree(snap!)).not.toThrow();
    // The member's own gateway egress was minted (its own scoped JWT).
    const m = snap!.multiagent!.agents[0]!;
    if (!('snapshot' in m)) throw new Error('expected a resolved member');
    expect(m.snapshot.egress.mode).toBe('gateway');
  });
});
