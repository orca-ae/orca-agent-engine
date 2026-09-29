// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the pluggable credential-egress strategy.
//
// An environment declares HOW the agent reaches credentialed upstreams via
// `egress_mode`:
//   - `gateway` — the agent's MCP/LLM traffic is proxied through the ai-gateway.
//     The egress config embeds the gateway base URL + a scoped per-session JWT +
//     the rewritten MCP servers. The gateway holds the real upstream credentials;
//     the runner only ever carries the opaque JWT.
//   - `sidecar` — a credential-proxy sidecar runs alongside the runner and swaps
//     real secrets onto outbound requests. The egress config embeds a SPEC only:
//     per-host bindings of { host, scheme, vault reference, optional injected env
//     names }. The real secret is resolved by the sidecar (out of band) from the
//     vault reference; it is NEVER embedded in the spec.
//
// Either way the strategy output is credential-FREE: gateway carries a JWT (an
// opaque, audience-scoped, short-lived token — not an upstream secret) and a
// vault-id reference; sidecar carries only vault references. The two modes
// produce structurally different configs, asserted here, and the selector maps
// `egress_mode` → strategy (with an explicit default + error path).

import { describe, it, expect } from 'vitest';
import {
  selectEgressStrategy,
  buildGatewayEgress,
  buildSidecarEgress,
  EGRESS_MODE_GATEWAY,
  EGRESS_MODE_SIDECAR,
  type GatewayEgressInput,
  type SidecarEgressInput,
} from '../../src/domain/credential-egress.js';
import { assertEgressCredentialFree } from '../../src/domain/egress-credential-free.js';

const SESSION_ID = 'ses_egress_1';

const GATEWAY_INPUT: GatewayEgressInput = {
  sessionId: SESSION_ID,
  gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
  gatewayLlmUrl: 'https://ai-gateway.internal/llm',
  sessionJwt: 'eyJ.gateway.jwt',
  llmJwt: 'eyJ.llm.jwt',
  mcpServers: [{ name: 'github', url: 'https://github.example/mcp' }],
  vaultByUrl: new Map([['https://github.example/mcp', 'vlt_gh']]),
};

const SIDECAR_INPUT: SidecarEgressInput = {
  bindings: [
    {
      host: 'github.com',
      scheme: 'basic',
      vaultId: 'vlt_gh',
      username: 'x-access-token',
      injectEnv: ['GH_TOKEN', 'GITHUB_TOKEN'],
    },
    {
      host: 'api.example.com',
      scheme: 'bearer',
      vaultId: 'vlt_api',
    },
  ],
};

describe('selectEgressStrategy', () => {
  it('maps gateway mode to the gateway strategy', () => {
    const strategy = selectEgressStrategy(EGRESS_MODE_GATEWAY);
    expect(strategy.mode).toBe('gateway');
  });

  it('maps sidecar mode to the sidecar strategy', () => {
    const strategy = selectEgressStrategy(EGRESS_MODE_SIDECAR);
    expect(strategy.mode).toBe('sidecar');
  });

  it('defaults a null/absent egress_mode to gateway (the cloud-equivalent default)', () => {
    expect(selectEgressStrategy(null).mode).toBe('gateway');
    expect(selectEgressStrategy(undefined).mode).toBe('gateway');
  });

  it('rejects an unrecognized egress_mode', () => {
    expect(() => selectEgressStrategy('carrier-pigeon' as never)).toThrow(/egress_mode/);
  });
});

describe('gateway strategy', () => {
  it('builds an egress config with the gateway URLs, the scoped JWT, and rewritten servers', () => {
    const strategy = selectEgressStrategy(EGRESS_MODE_GATEWAY);
    // Narrow the strategy union by mode before invoking its `build` — the two
    // strategies take structurally distinct inputs.
    if (strategy.mode !== 'gateway') throw new Error('expected gateway strategy');
    const egress = strategy.build(GATEWAY_INPUT);

    expect(egress.mode).toBe('gateway');
    if (egress.mode !== 'gateway') throw new Error('unreachable');
    expect(egress.gateway.mcp_base_url).toBe('https://ai-gateway.internal/mcp');
    expect(egress.gateway.llm_base_url).toBe('https://ai-gateway.internal/llm');
    expect(egress.gateway.session_jwt).toBe('eyJ.gateway.jwt');
    expect(egress.gateway.llm_jwt).toBe('eyJ.llm.jwt');
    // The rewritten servers point at the gateway and carry the JWT + routing.
    const gh = egress.gateway.mcp_servers['github']!;
    expect(gh.url).toBe('https://ai-gateway.internal/mcp');
    expect(gh.headers['X-Orca-Backend']).toBe('github');
    expect(gh.headers['X-Orca-Session-Id']).toBe(SESSION_ID);
    expect(gh.headers['Authorization']).toBe('Bearer eyJ.gateway.jwt');
    expect(gh.headers['X-Orca-Vault-Id']).toBe('vlt_gh');
    // No sidecar spec in gateway mode.
    expect('sidecar' in egress).toBe(false);
  });

  it('omits the LLM fields when no LLM gateway is configured', () => {
    const input: GatewayEgressInput = {
      sessionId: SESSION_ID,
      gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
      sessionJwt: 'eyJ.gateway.jwt',
      mcpServers: [],
      vaultByUrl: new Map(),
    };
    const egress = buildGatewayEgress(input);
    expect(egress.gateway.llm_base_url).toBeUndefined();
    expect(egress.gateway.llm_jwt).toBeUndefined();
    expect(egress.gateway.mcp_servers).toEqual({});
  });

  it('embeds no raw secret — only the opaque JWT and vault-id references', () => {
    const egress = buildGatewayEgress(GATEWAY_INPUT);
    // Structural credential-free guarantee: the config conforms exactly to the
    // secret-free gateway shape (no field outside the allow-list, JWTs JWT-shaped),
    // so a novel secret-shaped field would fail loud — stronger than string-match.
    expect(() => assertEgressCredentialFree(egress)).not.toThrow();
    // The opaque token + the vault reference ARE present (allowed, not secrets).
    const serialized = JSON.stringify(egress);
    expect(serialized).toContain('eyJ.gateway.jwt'); // the opaque token
    expect(serialized).toContain('vlt_gh'); // a reference, not a secret
  });
});

describe('sidecar strategy', () => {
  it('builds a credential-proxy SPEC of per-host vault bindings — no raw secrets', () => {
    const strategy = selectEgressStrategy(EGRESS_MODE_SIDECAR);
    // Narrow the strategy union by mode before invoking its `build`.
    if (strategy.mode !== 'sidecar') throw new Error('expected sidecar strategy');
    const egress = strategy.build(SIDECAR_INPUT);

    expect(egress.mode).toBe('sidecar');
    if (egress.mode !== 'sidecar') throw new Error('unreachable');
    expect(egress.sidecar.entries).toHaveLength(2);

    const gh = egress.sidecar.entries[0]!;
    expect(gh.host).toBe('github.com');
    expect(gh.scheme).toBe('basic');
    expect(gh.username).toBe('x-access-token');
    expect(gh.inject_env).toEqual(['GH_TOKEN', 'GITHUB_TOKEN']);
    // The secret is referenced by vault id; the real value is resolved by the
    // sidecar out of band and is NEVER present in the spec.
    expect(gh.source.kind).toBe('vault');
    expect(gh.source.vault_id).toBe('vlt_gh');
    const ghSource = gh.source as unknown as Record<string, unknown>;
    expect('secret' in ghSource).toBe(false);
    expect('real_secret' in ghSource).toBe(false);

    const api = egress.sidecar.entries[1]!;
    expect(api.host).toBe('api.example.com');
    expect(api.scheme).toBe('bearer');
    // bearer/token schemes carry no username and default to no env injection.
    expect(api.username).toBeUndefined();
    expect(api.inject_env).toEqual([]);
    expect(api.source.vault_id).toBe('vlt_api');

    // No gateway block in sidecar mode.
    expect('gateway' in egress).toBe(false);

    // Structural credential-free guarantee: every entry is a known secret-free
    // shape with a vault-REFERENCE source (no inlined secret field anywhere).
    expect(() => assertEgressCredentialFree(egress)).not.toThrow();
  });

  it('lower-cases the host so the proxy host match is canonical', () => {
    const egress = buildSidecarEgress({
      bindings: [{ host: 'GitHub.COM', scheme: 'bearer', vaultId: 'vlt_gh' }],
    });
    expect(egress.sidecar.entries[0]!.host).toBe('github.com');
  });

  it('rejects an unrecognized auth scheme', () => {
    expect(() =>
      buildSidecarEgress({
        bindings: [{ host: 'github.com', scheme: 'oauth2' as never, vaultId: 'vlt_gh' }],
      }),
    ).toThrow(/scheme/);
  });

  it('embeds no raw secret anywhere — STRUCTURALLY vault-reference-only', () => {
    const egress = buildSidecarEgress(SIDECAR_INPUT);
    // Structural guarantee: a novel secret-shaped field on any entry/source would
    // fail loud, where a string-match for known patterns would not.
    expect(() => assertEgressCredentialFree(egress)).not.toThrow();
    // The vault reference IS present (a reference, never a secret).
    expect(JSON.stringify(egress)).toContain('vlt_gh');
  });
});
