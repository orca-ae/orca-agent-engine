// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { SignJWT, jwtVerify, importPKCS8, importSPKI, type KeyLike, type JWTPayload } from 'jose';
import { createPublicKey } from 'node:crypto';
import { isWorkspaceId } from './workspace-id.js';

export interface SessionJwtClaims {
  org_id: string;
  workspace_id: string;
  session_id: string;
  mcp_server_names: string[];
  vault_ids: string[];
  credential_ids: string[];
  llm_routes?: string[];
  llm_models?: string[];
  agent_id?: string;
  guardrail_ids?: string[];
  runtime_config_revision?: string;
  user_id?: string;
}

export interface GitProxyScope {
  resourceId: string;
  repoUrl: string;
  credentialId: string;
  credentialRevision: string;
}

/**
 * Optional overrides applied per-mint. The minter's default audience comes
 * from its `SessionJwtConfig` (`'ai-gateway'` unless configured otherwise).
 * Other audiences (e.g. `'git-creds'` for the in-sandbox credential helper)
 * share the same signing key and issuer but need a different `aud` claim and,
 * for `'git-creds'`, a `repo_urls` allowlist. Callers that omit this argument
 * mint with the configured defaults.
 */
export interface MintOptions {
  /** Override the configured `aud` claim. Defaults to `cfg.audience`. */
  audience?: string;
  /**
   * Custom claim attached when minting `aud='git-creds'` JWTs. The
   * `/v1/git-creds` route compares the incoming repo URL against this
   * allowlist before resolving a PAT — so a leaked token can only fetch
   * credentials for repos the registry explicitly authorized.
   */
  repoUrls?: string[];
  /** Exact resource/repository/credential binding for the read-only Git proxy. */
  gitProxy?: GitProxyScope;
  /**
   * Override the token lifetime (seconds) for THIS mint. Defaults to
   * `cfg.ttlSecs`. The snapshot/egress design treats distinct audiences as
   * independently-scoped credentials, so a caller minting a different-audience
   * token (e.g. `aud='llm-proxy'`) can give it a distinct — typically shorter —
   * lifetime than the default session/MCP token without changing the global
   * config. Must be a positive number; a non-positive override falls back to
   * `cfg.ttlSecs` (a zero/negative TTL would mint an already-expired token,
   * which is never the intent).
   */
  ttlSecs?: number;
}

export interface VerifyOptions {
  /** When set, reject tokens whose `aud` claim doesn't match. */
  expectedAudience?: string;
}

export interface SessionJwtConfig {
  privateKeyPem: string;
  issuer: string;
  audience: string;
  ttlSecs: number;
}

export interface VerifiedSessionJwt {
  orgId: string;
  workspaceId: string;
  sessionId: string;
  mcpServerNames: string[];
  vaultIds: string[];
  credentialIds: string[];
  llmRoutes: string[];
  llmModels: string[];
  agentId?: string;
  guardrailIds: string[];
  runtimeConfigRevision?: string;
  userId?: string;
  audience: string;
  repoUrls: string[];
  gitProxy?: GitProxyScope;
  expiresAt: number;
}

export class SessionJwtMinter {
  private keyPromise: Promise<KeyLike> | null = null;
  private publicKeyPromise: Promise<KeyLike> | null = null;

  constructor(private readonly cfg: SessionJwtConfig) {}

  private async key(): Promise<KeyLike> {
    if (!this.keyPromise) {
      this.keyPromise = importPKCS8(this.cfg.privateKeyPem, 'RS256');
    }
    return this.keyPromise;
  }

  /**
   * Public-key handle used by `verify`. Derived from the configured private
   * key so callers don't have to thread the SPKI in separately — the minter
   * already owns the keypair conceptually. Cached after first use.
   */
  private async publicKey(): Promise<KeyLike> {
    if (!this.publicKeyPromise) {
      this.publicKeyPromise = (async (): Promise<KeyLike> => {
        const priv = await this.key();
        // `KeyLike` covers both `KeyObject` (Node) and `CryptoKey` (Web). In
        // Node, jose's importPKCS8 returns a KeyObject which `createPublicKey`
        // accepts directly; we then re-import via importSPKI to get a
        // jose-compatible KeyLike with the correct alg metadata.
        const pubPem = createPublicKey(priv as Parameters<typeof createPublicKey>[0]).export({
          format: 'pem',
          type: 'spki',
        }) as string;
        return importSPKI(pubPem, 'RS256');
      })();
    }
    return this.publicKeyPromise;
  }

  /** Resolve the final audience exactly as {@link mint} will encode it. */
  audienceFor(options?: MintOptions): string {
    return options?.audience ?? this.cfg.audience;
  }

  async mint(
    claims: SessionJwtClaims,
    options?: MintOptions,
  ): Promise<{ token: string; expiresAt: number }> {
    if (typeof claims.org_id !== 'string' || claims.org_id.trim().length === 0) {
      throw new Error('org_id must be a non-empty string');
    }
    const now = Math.floor(Date.now() / 1000);
    // A per-mint TTL override scopes a distinct-audience token (e.g.
    // `aud='llm-proxy'`) to its own — typically shorter — lifetime. A
    // non-positive override is ignored (a zero/negative TTL would mint an
    // already-expired token); we fall back to the configured default.
    const ttlSecs =
      options?.ttlSecs !== undefined && options.ttlSecs > 0 ? options.ttlSecs : this.cfg.ttlSecs;
    const exp = now + ttlSecs;
    const key = await this.key();
    const audience = this.audienceFor(options);

    const payload: JWTPayload = {
      org_id: claims.org_id,
      workspace_id: claims.workspace_id,
      session_id: claims.session_id,
      mcp_server_names: claims.mcp_server_names,
      vault_ids: claims.vault_ids,
      credential_ids: claims.credential_ids,
    };
    if (claims.llm_routes !== undefined) {
      payload['llm_routes'] = claims.llm_routes;
    }
    if (claims.llm_models !== undefined) {
      payload['llm_models'] = claims.llm_models;
    }
    if (claims.agent_id !== undefined) payload['agent_id'] = claims.agent_id;
    if (claims.guardrail_ids !== undefined) payload['guardrail_ids'] = claims.guardrail_ids;
    if (claims.runtime_config_revision !== undefined) {
      payload['runtime_config_revision'] = claims.runtime_config_revision;
    }
    if (claims.user_id !== undefined) payload['user_id'] = claims.user_id;
    if (options?.repoUrls && options.repoUrls.length > 0) {
      payload['repo_urls'] = options.repoUrls;
    }
    if (options?.gitProxy) payload['git_proxy'] = options.gitProxy;

    const token = await new SignJWT(payload)
      .setProtectedHeader({ alg: 'RS256' })
      .setIssuer(this.cfg.issuer)
      .setAudience(audience)
      .setSubject(claims.session_id)
      .setIssuedAt(now)
      .setExpirationTime(exp)
      .sign(key);
    return { token, expiresAt: exp };
  }

  /**
   * Verify a token against the configured issuer and (optionally) a caller-
   * provided audience. Throws on signature/issuer/audience/expiry mismatch.
   *
   * Backwards compat: callers that don't pass `expectedAudience` accept any
   * audience the minter could have produced — but in practice every caller
   * that cares about scope MUST pass an explicit `expectedAudience` so a
   * `'ai-gateway'` token can't be replayed against the `'git-creds'` route.
   */
  async verify(token: string, options?: VerifyOptions): Promise<VerifiedSessionJwt> {
    const pub = await this.publicKey();
    const verifyOpts: { issuer: string; audience?: string } = { issuer: this.cfg.issuer };
    if (options?.expectedAudience !== undefined) {
      verifyOpts.audience = options.expectedAudience;
    }
    const { payload } = await jwtVerify(token, pub, verifyOpts);

    const aud = Array.isArray(payload.aud) ? payload.aud[0] : payload.aud;
    if (typeof aud !== 'string') {
      throw new Error('JWT missing audience claim');
    }
    if (typeof payload['org_id'] !== 'string' || payload['org_id'].trim().length === 0) {
      throw new Error('JWT missing or invalid org_id claim');
    }
    if (!isWorkspaceId(payload['workspace_id'])) {
      throw new Error('JWT missing or invalid workspace_id claim');
    }
    if (typeof payload['session_id'] !== 'string') {
      throw new Error('JWT missing session_id claim');
    }
    if (typeof payload.exp !== 'number') {
      throw new Error('JWT missing exp claim');
    }

    const repoUrls = Array.isArray(payload['repo_urls'])
      ? (payload['repo_urls'] as unknown[]).filter((v): v is string => typeof v === 'string')
      : [];
    const mcpServerNames = Array.isArray(payload['mcp_server_names'])
      ? (payload['mcp_server_names'] as unknown[]).filter((v): v is string => typeof v === 'string')
      : [];
    const vaultIds = Array.isArray(payload['vault_ids'])
      ? (payload['vault_ids'] as unknown[]).filter((v): v is string => typeof v === 'string')
      : [];
    const credentialIds = Array.isArray(payload['credential_ids'])
      ? (payload['credential_ids'] as unknown[]).filter((v): v is string => typeof v === 'string')
      : [];
    const llmRoutes = Array.isArray(payload['llm_routes'])
      ? (payload['llm_routes'] as unknown[]).filter((v): v is string => typeof v === 'string')
      : [];
    const llmModels = Array.isArray(payload['llm_models'])
      ? (payload['llm_models'] as unknown[]).filter((v): v is string => typeof v === 'string')
      : [];
    const guardrailIds = Array.isArray(payload['guardrail_ids'])
      ? (payload['guardrail_ids'] as unknown[]).filter((v): v is string => typeof v === 'string')
      : [];
    const gitProxy = payload['git_proxy'];
    if (
      gitProxy !== undefined &&
      (!gitProxy ||
        typeof gitProxy !== 'object' ||
        Array.isArray(gitProxy) ||
        !['resourceId', 'repoUrl', 'credentialId', 'credentialRevision'].every(
          (key) =>
            typeof (gitProxy as Record<string, unknown>)[key] === 'string' &&
            (gitProxy as Record<string, string>)[key]!.length > 0,
        ))
    )
      throw new Error('JWT invalid git_proxy claim');

    return {
      orgId: payload['org_id'],
      workspaceId: payload['workspace_id'],
      sessionId: payload['session_id'],
      mcpServerNames,
      vaultIds,
      credentialIds,
      llmRoutes,
      llmModels,
      ...(typeof payload['agent_id'] === 'string' ? { agentId: payload['agent_id'] } : {}),
      guardrailIds,
      ...(typeof payload['runtime_config_revision'] === 'string'
        ? { runtimeConfigRevision: payload['runtime_config_revision'] }
        : {}),
      ...(typeof payload['user_id'] === 'string' ? { userId: payload['user_id'] } : {}),
      audience: aud,
      repoUrls,
      ...(gitProxy ? { gitProxy: gitProxy as unknown as GitProxyScope } : {}),
      expiresAt: payload.exp,
    };
  }
}
