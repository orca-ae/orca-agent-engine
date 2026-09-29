// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { internalContract } from '../../src/contracts/internal.contract.js';

const resolveContract = internalContract.resolveVaultCredential;
const mintSessionJwtContract = internalContract.mintSessionJwt;

describe('internal vault credential resolve contract', () => {
  it.each(['vcrd_contract', 'llm:anthropic'])(
    'accepts the requested credential id %s in path and body',
    (requestedId) => {
      expect(
        resolveContract.pathParams.safeParse({
          workspaceId: 'ws_contract',
          sessionId: 'ses_contract',
          id: requestedId,
        }).success,
      ).toBe(true);
      expect(
        resolveContract.body.safeParse({
          credential_id: requestedId,
          vault_id: requestedId,
        }).success,
      ).toBe(true);
    },
  );

  it('requires the gateway credential and vault binding body', () => {
    expect(
      resolveContract.body.safeParse({
        credential_id: 'vcrd_contract',
        vault_id: 'vcrd_contract',
      }).success,
    ).toBe(true);
    expect(
      resolveContract.body.safeParse({
        credential_id: 'vcrd_contract',
        vault_id: 'vcrd_contract',
        force_refresh: true,
      }).success,
    ).toBe(true);
    expect(resolveContract.body.safeParse({}).success).toBe(false);
    expect(
      resolveContract.body.safeParse({
        credential_id: 'vcrd_contract',
        vault_id: 'vcrd_contract',
        force_refresh: 'yes',
      }).success,
    ).toBe(false);
  });

  it('enforces the llm alias namespace and URL-path-safe boundaries', () => {
    for (const value of ['llm:a', `llm:${'a'.repeat(124)}`]) {
      expect(
        resolveContract.body.safeParse({ credential_id: value, vault_id: value }).success,
        value,
      ).toBe(true);
    }
    for (const value of [
      '',
      'anthropic',
      'llm:',
      `llm:${'a'.repeat(125)}`,
      'llm/anthropic',
      'llm?anthropic',
      'llm#anthropic',
      'llm%2Fanthropic',
      'llm anthropic',
      'llm:模型',
    ]) {
      expect(
        resolveContract.body.safeParse({ credential_id: value, vault_id: value }).success,
        value,
      ).toBe(false);
    }
  });

  it.each(['api_key', 'bearer', 'gcp-service-account', 'aws-sig-v4'])(
    'accepts canonical %s provider responses with a concrete selected id',
    (scheme) => {
      expect(
        resolveContract.responses[200].safeParse({
          credential_id: 'vcrd_selected',
          vault_id: 'vlt_provider',
          version: 'opaque-version',
          scheme,
          secret_value: 'redacted-in-real-responses',
          ttl_seconds: 300,
        }).success,
      ).toBe(true);
    },
  );

  it('rejects a logical alias as the selected response id', () => {
    expect(
      resolveContract.responses[200].safeParse({
        credential_id: 'llm:anthropic',
        vault_id: 'vlt_provider',
        version: 'opaque-version',
        scheme: 'api_key',
        secret_value: 'redacted-in-real-responses',
        ttl_seconds: 300,
      }).success,
    ).toBe(false);
  });

  it('uses a workspace/session-scoped path', () => {
    expect(resolveContract.path).toBe(
      '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/vault-credentials/:id/resolve',
    );
  });
});

describe('internal tenant route architecture', () => {
  /**
   * Environment-management routes are intentionally ENVIRONMENT-scoped, not
   * workspace/session-scoped. An environment id is managed-infra identity (the
   * sandbox-launch + worker-tunnel claim lifecycle — see
   * `docs/managed-agents/architecture.md`), not session/tenant data, so these
   * routes are keyed by `:id` (the environment) rather than
   * `:workspaceId/sessions/:...`. This is a deliberate allowlist, not a tenancy
   * leak: every route below still requires internal-mesh auth (mesh-only, Istio
   * mTLS in prod — see docs/operation/internal-traffic-auth.md), and no
   * session/vault/credential route is exempted from the workspace/session-scoped
   * invariant.
   */
  const ENVIRONMENT_SCOPED_ROUTES = new Set<string>([
    'verifyEnvKey',
    'claimEnvironment',
    'heartbeatEnvironmentClaim',
    'releaseEnvironmentClaim',
    'getEnvironmentClaim',
    'reapEnvironmentClaims',
  ]);
  // AI Gateway's RegistryGuardrailSource uses this fixed path and sends the
  // verified scope as query parameters. Its handler checks every tenant and
  // Session dimension against Registry rows before returning a bundle.
  const GATEWAY_SCOPE_QUERY_ROUTES = new Set<string>(['effectiveGuardrails']);

  it('keeps internal contract routes tenant scoped with explicit infra exceptions', () => {
    const entries = Object.entries(internalContract) as Array<[string, { path: string }]>;
    expect(entries.length).toBeGreaterThan(0);
    let environmentScopedCount = 0;
    let gatewayQueryScopedCount = 0;
    for (const [name, route] of entries) {
      if (ENVIRONMENT_SCOPED_ROUTES.has(name)) {
        environmentScopedCount += 1;
        expect(route.path).toMatch(/^\/internal\/environments(\/|$)/);
        continue;
      }
      if (GATEWAY_SCOPE_QUERY_ROUTES.has(name)) {
        gatewayQueryScopedCount += 1;
        expect(route.path).toBe('/internal/v1/guardrails/effective');
        continue;
      }
      expect(route.path).toMatch(/^\/internal\/v1\/workspaces\/:workspaceId\/sessions\/:/);
    }
    // The allowlist itself stays honest: every named route must still exist on
    // the contract (a route rename here would otherwise silently narrow the
    // invariant back down without failing).
    expect(environmentScopedCount).toBe(ENVIRONMENT_SCOPED_ROUTES.size);
    expect(gatewayQueryScopedCount).toBe(GATEWAY_SCOPE_QUERY_ROUTES.size);
  });
});

describe('internal session JWT mint contract', () => {
  it('does not expose caller-controlled LLM allowlists', () => {
    expect(
      mintSessionJwtContract.body.safeParse({
        mcp_server_names: [],
      }).success,
    ).toBe(true);
    const parsed = mintSessionJwtContract.body.parse({
      mcp_server_names: [],
      llm_routes: ['caller-route'],
      llm_models: ['caller-model'],
    });
    expect(parsed).not.toHaveProperty('llm_routes');
    expect(parsed).not.toHaveProperty('llm_models');
  });
});
