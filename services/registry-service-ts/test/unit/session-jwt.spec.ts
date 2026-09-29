// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import fs from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { generateKeyPairSync } from 'node:crypto';
import { describe, it, expect } from 'vitest';
import { SignJWT, importPKCS8, decodeJwt } from 'jose';
import { SessionJwtMinter, type SessionJwtConfig } from '../../src/auth/session-jwt.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PRIVATE_PEM = fs.readFileSync(
  resolve(__dirname, '../fixtures/session-jwt-private.pem'),
  'utf8',
);

const BASE_CLAIMS = {
  org_id: 'org_test',
  workspace_id: 'ws_test',
  session_id: 'ses_test_abc',
  mcp_server_names: ['github'],
  vault_ids: ['vlt_1'],
  credential_ids: ['vcrd_1'],
  llm_routes: ['managed-agent-llm-openai'],
  llm_models: ['gpt-4o', 'gpt-4o-mini'],
};

function buildMinter(overrides: Partial<SessionJwtConfig> = {}): SessionJwtMinter {
  return new SessionJwtMinter({
    privateKeyPem: PRIVATE_PEM,
    issuer: 'orca-registry',
    audience: 'ai-gateway',
    ttlSecs: 300,
    ...overrides,
  });
}

describe('SessionJwtMinter', () => {
  describe('default audience (Phase 4 backwards compat)', () => {
    it('mints with the configured audience when no override is supplied', async () => {
      const minter = buildMinter();
      const { token } = await minter.mint(BASE_CLAIMS);
      const decoded = decodeJwt(token);
      expect(decoded.aud).toBe('ai-gateway');
      expect(decoded['repo_urls']).toBeUndefined();
    });

    it.each(['', '   '])('rejects invalid org_id before signing (%j)', async (orgId) => {
      const minter = buildMinter();
      await expect(minter.mint({ ...BASE_CLAIMS, org_id: orgId })).rejects.toThrow(
        'org_id must be a non-empty string',
      );
    });

    it('verify with no expectedAudience accepts the default audience', async () => {
      const minter = buildMinter();
      const { token } = await minter.mint(BASE_CLAIMS);
      const result = await minter.verify(token);
      expect(result.audience).toBe('ai-gateway');
      expect(result.orgId).toBe(BASE_CLAIMS.org_id);
      expect(result.workspaceId).toBe(BASE_CLAIMS.workspace_id);
      expect(result.sessionId).toBe(BASE_CLAIMS.session_id);
      expect(result.mcpServerNames).toEqual(['github']);
      expect(result.vaultIds).toEqual(['vlt_1']);
      expect(result.credentialIds).toEqual(['vcrd_1']);
      expect(result.llmRoutes).toEqual(['managed-agent-llm-openai']);
      expect(result.llmModels).toEqual(['gpt-4o', 'gpt-4o-mini']);
      expect(result.repoUrls).toEqual([]);
    });

    it.each([undefined, '', '   '])(
      'rejects a token with missing or invalid org_id (%j)',
      async (orgId) => {
        const minter = buildMinter();
        const key = await importPKCS8(PRIVATE_PEM, 'RS256');
        const now = Math.floor(Date.now() / 1000);
        const token = await new SignJWT({
          ...(orgId !== undefined ? { org_id: orgId } : {}),
          workspace_id: BASE_CLAIMS.workspace_id,
          session_id: BASE_CLAIMS.session_id,
          mcp_server_names: BASE_CLAIMS.mcp_server_names,
          vault_ids: BASE_CLAIMS.vault_ids,
          credential_ids: BASE_CLAIMS.credential_ids,
        })
          .setProtectedHeader({ alg: 'RS256' })
          .setIssuer('orca-registry')
          .setAudience('git-creds')
          .setSubject(BASE_CLAIMS.session_id)
          .setIssuedAt(now)
          .setExpirationTime(now + 300)
          .sign(key);

        await expect(minter.verify(token, { expectedAudience: 'git-creds' })).rejects.toThrow(
          'JWT missing or invalid org_id claim',
        );
      },
    );

    it('encodes LLM authorization claims for ai-gateway tokens', async () => {
      const minter = buildMinter();
      const { token } = await minter.mint(BASE_CLAIMS);
      const decoded = decodeJwt(token);
      expect(decoded['llm_routes']).toEqual(['managed-agent-llm-openai']);
      expect(decoded['llm_models']).toEqual(['gpt-4o', 'gpt-4o-mini']);
    });

    it('round-trips the guardrail identity and revision claims', async () => {
      const minter = buildMinter();
      const { token } = await minter.mint({
        ...BASE_CLAIMS,
        agent_id: 'agt_test',
        guardrail_ids: ['grd_one', 'grd_two'],
        runtime_config_revision: '7',
        user_id: 'usr_test',
      });
      const decoded = decodeJwt(token);
      expect(decoded['agent_id']).toBe('agt_test');
      expect(decoded['guardrail_ids']).toEqual(['grd_one', 'grd_two']);
      expect(decoded['runtime_config_revision']).toBe('7');
      expect(decoded['user_id']).toBe('usr_test');
      const verified = await minter.verify(token, { expectedAudience: 'ai-gateway' });
      expect(verified.agentId).toBe('agt_test');
      expect(verified.guardrailIds).toEqual(['grd_one', 'grd_two']);
      expect(verified.runtimeConfigRevision).toBe('7');
      expect(verified.userId).toBe('usr_test');
    });

    it('verify with explicit expectedAudience matching the default succeeds', async () => {
      const minter = buildMinter();
      const { token } = await minter.mint(BASE_CLAIMS);
      const result = await minter.verify(token, { expectedAudience: 'ai-gateway' });
      expect(result.audience).toBe('ai-gateway');
    });
  });

  describe("audience='git-creds' (Phase 7)", () => {
    it('resolves the configured and overridden audiences consistently', () => {
      const minter = buildMinter();
      expect(minter.audienceFor()).toBe('ai-gateway');
      expect(minter.audienceFor({ audience: 'git-creds' })).toBe('git-creds');
    });

    it("mints a token whose aud claim is 'git-creds' when overridden", async () => {
      const minter = buildMinter();
      const { token } = await minter.mint(BASE_CLAIMS, { audience: 'git-creds' });
      const decoded = decodeJwt(token);
      expect(decoded.aud).toBe('git-creds');
    });

    it("verify(token, { expectedAudience: 'git-creds' }) succeeds for a git-creds token", async () => {
      const minter = buildMinter();
      const { token } = await minter.mint(BASE_CLAIMS, { audience: 'git-creds' });
      const result = await minter.verify(token, { expectedAudience: 'git-creds' });
      expect(result.audience).toBe('git-creds');
      expect(result.sessionId).toBe(BASE_CLAIMS.session_id);
    });

    it('rejects a git-creds token when verify expects ai-gateway (no cross-audience replay)', async () => {
      const minter = buildMinter();
      const { token } = await minter.mint(BASE_CLAIMS, { audience: 'git-creds' });
      await expect(minter.verify(token, { expectedAudience: 'ai-gateway' })).rejects.toThrow();
    });

    it('rejects a default-audience token when verify expects git-creds', async () => {
      const minter = buildMinter();
      const { token } = await minter.mint(BASE_CLAIMS);
      await expect(minter.verify(token, { expectedAudience: 'git-creds' })).rejects.toThrow();
    });
  });

  describe('repo_urls custom claim', () => {
    it("encodes repoUrls as a 'repo_urls' claim and round-trips through verify", async () => {
      const minter = buildMinter();
      const repoUrls = ['https://github.com/orca-ae/orca-agent-engine', 'https://github.com/x/y'];
      const { token } = await minter.mint(BASE_CLAIMS, {
        audience: 'git-creds',
        repoUrls,
      });
      const decoded = decodeJwt(token);
      expect(decoded['repo_urls']).toEqual(repoUrls);

      const result = await minter.verify(token, { expectedAudience: 'git-creds' });
      expect(result.repoUrls).toEqual(repoUrls);
    });

    it('omits repo_urls when not provided', async () => {
      const minter = buildMinter();
      const { token } = await minter.mint(BASE_CLAIMS, { audience: 'git-creds' });
      const decoded = decodeJwt(token);
      expect(decoded['repo_urls']).toBeUndefined();

      const result = await minter.verify(token, { expectedAudience: 'git-creds' });
      expect(result.repoUrls).toEqual([]);
    });
  });

  describe('ttlSecs override (per-mint lifetime)', () => {
    it('mints a token whose lifetime is the per-mint override, not the configured TTL', async () => {
      // A distinct-audience credential (e.g. aud='llm-proxy') is independently
      // scoped to its own shorter lifetime without changing the global config.
      const minter = buildMinter({ ttlSecs: 300 });
      const { token, expiresAt } = await minter.mint(BASE_CLAIMS, { ttlSecs: 60 });
      const decoded = decodeJwt(token);
      const iat = decoded.iat as number;
      const exp = decoded.exp as number;
      expect(exp - iat).toBe(60);
      expect(expiresAt).toBe(exp);
    });

    it('ignores a non-positive ttl override and falls back to the configured TTL', async () => {
      // A zero/negative override would mint an already-expired token; fall back.
      const minter = buildMinter({ ttlSecs: 300 });
      const zero = await minter.mint(BASE_CLAIMS, { ttlSecs: 0 });
      const negative = await minter.mint(BASE_CLAIMS, { ttlSecs: -10 });
      for (const { token } of [zero, negative]) {
        const decoded = decodeJwt(token);
        expect((decoded.exp as number) - (decoded.iat as number)).toBe(300);
      }
    });

    it('composes a shorter ttl with an audience override', async () => {
      const minter = buildMinter({ ttlSecs: 300 });
      const { token } = await minter.mint(BASE_CLAIMS, { audience: 'llm-proxy', ttlSecs: 120 });
      const decoded = decodeJwt(token);
      expect(decoded.aud).toBe('llm-proxy');
      expect((decoded.exp as number) - (decoded.iat as number)).toBe(120);
      // The shorter-lived llm-proxy token still verifies against its own audience.
      const result = await minter.verify(token, { expectedAudience: 'llm-proxy' });
      expect(result.audience).toBe('llm-proxy');
    });
  });

  describe('LLM authorization claims', () => {
    it('omits LLM authorization claims when they are not supplied', async () => {
      const minter = buildMinter();
      const { token } = await minter.mint({
        org_id: BASE_CLAIMS.org_id,
        workspace_id: BASE_CLAIMS.workspace_id,
        session_id: BASE_CLAIMS.session_id,
        mcp_server_names: BASE_CLAIMS.mcp_server_names,
        vault_ids: BASE_CLAIMS.vault_ids,
        credential_ids: BASE_CLAIMS.credential_ids,
      });
      const decoded = decodeJwt(token);
      expect(decoded['llm_routes']).toBeUndefined();
      expect(decoded['llm_models']).toBeUndefined();
    });

    it('defaults missing LLM claims to empty lists on verify', async () => {
      const minter = buildMinter();
      const key = await importPKCS8(PRIVATE_PEM, 'RS256');
      const now = Math.floor(Date.now() / 1000);
      const token = await new SignJWT({
        org_id: BASE_CLAIMS.org_id,
        workspace_id: BASE_CLAIMS.workspace_id,
        session_id: BASE_CLAIMS.session_id,
        mcp_server_names: BASE_CLAIMS.mcp_server_names,
        vault_ids: BASE_CLAIMS.vault_ids,
        credential_ids: BASE_CLAIMS.credential_ids,
      })
        .setProtectedHeader({ alg: 'RS256' })
        .setIssuer('orca-registry')
        .setAudience('ai-gateway')
        .setSubject(BASE_CLAIMS.session_id)
        .setIssuedAt(now)
        .setExpirationTime(now + 300)
        .sign(key);

      const decoded = decodeJwt(token);
      expect(decoded['llm_routes']).toBeUndefined();
      expect(decoded['llm_models']).toBeUndefined();

      const result = await minter.verify(token, { expectedAudience: 'ai-gateway' });
      expect(result.llmRoutes).toEqual([]);
      expect(result.llmModels).toEqual([]);
    });
  });

  describe('expiry', () => {
    it('rejects a token whose exp is in the past', async () => {
      const minter = buildMinter({ ttlSecs: 1 });
      const { token } = await minter.mint(BASE_CLAIMS);
      // jose enforces a small clock-skew tolerance; sleep past it.
      await new Promise((r) => setTimeout(r, 1500));
      await expect(minter.verify(token, { expectedAudience: 'ai-gateway' })).rejects.toThrow();
    }, 10000);
  });

  describe('issuer mismatch', () => {
    it('rejects a token signed with the right key but the wrong issuer', async () => {
      const minter = buildMinter();
      // Sign a hand-crafted JWT with `iss='attacker'` — same key, wrong issuer.
      const key = await importPKCS8(PRIVATE_PEM, 'RS256');
      const now = Math.floor(Date.now() / 1000);
      const rogue = await new SignJWT({
        org_id: BASE_CLAIMS.org_id,
        workspace_id: BASE_CLAIMS.workspace_id,
        session_id: BASE_CLAIMS.session_id,
        mcp_server_names: BASE_CLAIMS.mcp_server_names,
        vault_ids: BASE_CLAIMS.vault_ids,
      })
        .setProtectedHeader({ alg: 'RS256' })
        .setIssuer('attacker')
        .setAudience('ai-gateway')
        .setSubject(BASE_CLAIMS.session_id)
        .setIssuedAt(now)
        .setExpirationTime(now + 300)
        .sign(key);
      await expect(minter.verify(rogue, { expectedAudience: 'ai-gateway' })).rejects.toThrow();
    });
  });

  describe('signature mismatch', () => {
    it('rejects a token signed with a different key', async () => {
      const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
      const otherPem = privateKey.export({ format: 'pem', type: 'pkcs8' }) as string;
      const otherMinter = new SessionJwtMinter({
        privateKeyPem: otherPem,
        issuer: 'orca-registry',
        audience: 'ai-gateway',
        ttlSecs: 300,
      });
      const verifyMinter = buildMinter();
      const { token } = await otherMinter.mint(BASE_CLAIMS);
      await expect(
        verifyMinter.verify(token, { expectedAudience: 'ai-gateway' }),
      ).rejects.toThrow();
    });
  });
});
