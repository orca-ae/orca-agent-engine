// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Pluggable credential-egress strategy, selected by an environment's
// `egress_mode`. The strategy decides HOW the runner reaches credentialed
// upstreams — and, crucially, produces a CREDENTIAL-FREE config either way:
//
//   - `gateway` — MCP + LLM traffic is proxied through the ai-gateway, which
//     holds the real upstream credentials. The config embeds the gateway base
//     URLs, a scoped per-session JWT (opaque, audience-bound, short-lived — not
//     an upstream secret), and the rewritten MCP servers (each pointing at the
//     gateway, carrying the JWT + routing headers + a vault-id reference). The
//     runner never holds an upstream secret; it holds a token the gateway swaps.
//
//   - `sidecar` — a credential-proxy sidecar runs alongside the runner and swaps
//     real secrets onto outbound requests (swap-on-access; optional synthetic
//     env injection for credential-gating clients). The config embeds a SPEC
//     ONLY: per-host bindings of { host, scheme, vault REFERENCE, optional
//     injected env names }. The sidecar resolves the real secret out of band from
//     the vault reference; the spec never carries the secret. This mirrors the
//     established credential-proxy spec shape (a per-host source binding) while
//     keeping the secret resolution on the trusted side.
//
// The strategy output is part of the snapshot delivered over the tunnel, so it
// must be PURE + serializable + secret-free. Both `build*` functions are pure;
// secrets are resolved elsewhere (the gateway / the sidecar) — never here.

import { rewriteMcpServers, type AgentMcpServer, type RewrittenServer } from './mcp-rewrite.js';

/** `egress_mode` value: proxy via the ai-gateway (holds the real credentials). */
export const EGRESS_MODE_GATEWAY = 'gateway' as const;
/** `egress_mode` value: a credential-proxy sidecar swaps real secrets out of band. */
export const EGRESS_MODE_SIDECAR = 'sidecar' as const;

/** The recognized egress modes — the environment contract's `egress_mode` enum. */
export type EgressMode = typeof EGRESS_MODE_GATEWAY | typeof EGRESS_MODE_SIDECAR;

/**
 * Auth schemes the sidecar emits upstream. Mirrors the credential-proxy entry's
 * scheme set: `basic` carries a username (secret in the password field), `bearer`
 * / `token` carry the secret as the bare credential.
 *
 * Seam note: this module only ENCODES a scheme it is handed — it has no
 * opinion on where a `SidecarBinding.scheme` comes from. Today the only
 * producer is `vaultsToBindings`/`bindingForKind`
 * (`domain/agent-snapshot-resolver.ts`), fed by `loadVault`
 * (`api/snapshot-loader.ts`). Under main's model every vault-sourced binding
 * resolves to `'bearer'`: main dropped OURS' pre-merge vault
 * credential-kind taxonomy (`git_https` / `gh_basic` / `https_basic`), so
 * `loadVault` never sources a `target_kind`, and `bindingForKind` treats the
 * resulting absent/unrecognized kind as `'bearer'`. That is intentional, not
 * a gap. `'basic'` / `'token'` stay in this union for the credential-proxy
 * spec's general shape (and `bindingForKind`'s still-present presets), but no
 * live vault resolves to them today. Basic-auth GIT credentials are not a
 * vault concern at all — they flow through the separate `git_credentials`
 * table (`api/git-creds.routes.ts`, `mintGitCredsJwt`), outside this sidecar
 * spec entirely. Do not reintroduce a per-vault-credential-kind taxonomy to
 * route more vaults through `'basic'`/`'token'` — that would contradict
 * adopt-main.
 */
export type EgressAuthScheme = 'basic' | 'bearer' | 'token';

const VALID_AUTH_SCHEMES: ReadonlySet<string> = new Set<EgressAuthScheme>([
  'basic',
  'bearer',
  'token',
]);

// ── Gateway egress ───────────────────────────────────────

/** Input to {@link buildGatewayEgress}. All credential-shaped values are caller-minted JWTs. */
export interface GatewayEgressInput {
  /** Session id stamped on each rewritten MCP server (audit + vault scoping). */
  sessionId: string;
  /** Public ai-gateway MCP base URL each rewritten server points at. */
  gatewayMcpUrl: string;
  /** Public ai-gateway LLM-proxy base URL, when an LLM gateway is configured. */
  gatewayLlmUrl?: string;
  /** Scoped session JWT (`aud='ai-gateway'`) the rewritten MCP servers carry. */
  sessionJwt: string;
  /** Scoped LLM-proxy JWT (`aud='llm-proxy'`), when an LLM gateway is configured. */
  llmJwt?: string;
  /** The agent's MCP servers (logical name + original URL) to rewrite. */
  mcpServers: AgentMcpServer[];
  /** url → vault_id for servers whose URL matched a session vault (first-match-wins). */
  vaultByUrl: Map<string, string>;
}

/** The gateway egress block embedded in the snapshot. Credential-free (JWTs only). */
export interface GatewayEgressConfig {
  /** ai-gateway MCP base URL. */
  mcp_base_url: string;
  /** ai-gateway LLM-proxy base URL, when configured. */
  llm_base_url?: string;
  /** Scoped session JWT for MCP egress. */
  session_jwt: string;
  /** Scoped LLM-proxy JWT, when an LLM gateway is configured. */
  llm_jwt?: string;
  /** Rewritten MCP servers, keyed by logical name (each points at the gateway). */
  mcp_servers: Record<string, RewrittenServer>;
}

/** The discriminated gateway egress result. */
export interface GatewayEgress {
  mode: typeof EGRESS_MODE_GATEWAY;
  gateway: GatewayEgressConfig;
}

/**
 * Build the gateway egress config: rewrite the MCP servers to the gateway URL
 * (carrying the session JWT + routing headers + vault-id references) and pin the
 * gateway base URLs + scoped JWTs. Pure + credential-free — the only
 * credential-shaped values are the caller-minted JWTs.
 */
export function buildGatewayEgress(input: GatewayEgressInput): GatewayEgress {
  const gateway: GatewayEgressConfig = {
    mcp_base_url: input.gatewayMcpUrl,
    session_jwt: input.sessionJwt,
    mcp_servers: rewriteMcpServers(
      input.mcpServers,
      input.sessionId,
      input.gatewayMcpUrl,
      input.sessionJwt,
      input.vaultByUrl,
    ),
  };
  if (input.gatewayLlmUrl !== undefined) gateway.llm_base_url = input.gatewayLlmUrl;
  if (input.llmJwt !== undefined) gateway.llm_jwt = input.llmJwt;
  return { mode: EGRESS_MODE_GATEWAY, gateway };
}

// ── Sidecar egress ───────────────────────────────────────

/** One per-host credential binding the caller asks the sidecar to enforce. */
export interface SidecarBinding {
  /** Hostname the binding applies to; lower-cased into the spec. */
  host: string;
  /** Auth scheme the sidecar emits upstream. */
  scheme: EgressAuthScheme;
  /** Vault id the sidecar resolves the real secret from (out of band). */
  vaultId: string;
  /** Basic-auth username emitted upstream when `scheme='basic'`. */
  username?: string;
  /**
   * Env-var names set to a synthetic placeholder inside the runner so a
   * credential-gating client (e.g. a CLI that refuses to issue a request without
   * a local credential) emits a request the sidecar can rewrite. Empty (default)
   * means pure swap-on-access — nothing is injected.
   */
  injectEnv?: string[];
}

/** Input to {@link buildSidecarEgress}. Carries vault REFERENCES — never secrets. */
export interface SidecarEgressInput {
  bindings: SidecarBinding[];
}

/**
 * Where the sidecar resolves the real secret from. Only a `vault` reference is
 * embedded; the secret value is resolved out of band by the sidecar and never
 * appears in the spec.
 */
export interface CredentialProxySource {
  kind: 'vault';
  vault_id: string;
}

/**
 * One normalized per-host credential-proxy binding. Mirrors the established
 * credential-proxy entry shape (host + scheme + source + optional username +
 * opt-in env injection) but with a vault-REFERENCE source so no secret is
 * embedded.
 */
export interface CredentialProxyEntry {
  /** Exact (lower-cased) hostname this binding applies to. */
  host: string;
  /** Auth scheme the sidecar emits upstream. */
  scheme: EgressAuthScheme;
  /** Where the sidecar resolves the real secret from (a vault reference). */
  source: CredentialProxySource;
  /** Basic-auth username emitted upstream when `scheme='basic'`; absent otherwise. */
  username?: string;
  /** Opt-in env-var names that receive a synthetic placeholder; empty by default. */
  inject_env: string[];
}

/** The secretless credential-proxy spec the sidecar enforces. */
export interface CredentialProxySpec {
  entries: CredentialProxyEntry[];
}

/** The discriminated sidecar egress result. */
export interface SidecarEgress {
  mode: typeof EGRESS_MODE_SIDECAR;
  sidecar: CredentialProxySpec;
}

/**
 * Build the sidecar egress config: a credential-proxy spec of per-host vault
 * bindings. Pure + credential-free — each entry references a vault id; the
 * sidecar resolves the real secret out of band. Hosts are lower-cased so the
 * proxy's host match is canonical; an unrecognized scheme throws.
 */
export function buildSidecarEgress(input: SidecarEgressInput): SidecarEgress {
  const entries: CredentialProxyEntry[] = input.bindings.map((b) => {
    if (!VALID_AUTH_SCHEMES.has(b.scheme)) {
      throw new Error(`credential-egress: unsupported auth scheme '${b.scheme}'`);
    }
    const entry: CredentialProxyEntry = {
      host: b.host.toLowerCase(),
      scheme: b.scheme,
      source: { kind: 'vault', vault_id: b.vaultId },
      inject_env: b.injectEnv ? [...b.injectEnv] : [],
    };
    if (b.username !== undefined) entry.username = b.username;
    return entry;
  });
  return { mode: EGRESS_MODE_SIDECAR, sidecar: { entries } };
}

// ── Strategy selection ───────────────────────────────────

/** The egress config embedded in the snapshot — a discriminated union on `mode`. */
export type EgressConfig = GatewayEgress | SidecarEgress;

/**
 * A credential-egress strategy: knows its `mode` and builds the matching egress
 * config from the matching input. The two strategies are structurally distinct
 * (different `build` input), so the union is narrowed by `mode` at the call site.
 */
export type CredentialEgressStrategy =
  | { mode: typeof EGRESS_MODE_GATEWAY; build(input: GatewayEgressInput): GatewayEgress }
  | { mode: typeof EGRESS_MODE_SIDECAR; build(input: SidecarEgressInput): SidecarEgress };

/**
 * Select the egress strategy for an environment's `egress_mode`.
 *
 *   - `'gateway'` → the gateway strategy.
 *   - `'sidecar'` → the sidecar strategy.
 *   - `null` / `undefined` → defaults to `gateway` (the cloud-equivalent default:
 *     a session with no explicit egress mode behaves like a cloud session whose
 *     traffic is proxied through the gateway).
 *   - anything else → throws (an unrecognized mode is a configuration error, not a
 *     silent fall-through to an insecure default).
 */
export function selectEgressStrategy(
  mode: EgressMode | null | undefined,
): CredentialEgressStrategy {
  const resolved = mode ?? EGRESS_MODE_GATEWAY;
  if (resolved === EGRESS_MODE_GATEWAY) {
    return { mode: EGRESS_MODE_GATEWAY, build: buildGatewayEgress };
  }
  if (resolved === EGRESS_MODE_SIDECAR) {
    return { mode: EGRESS_MODE_SIDECAR, build: buildSidecarEgress };
  }
  throw new Error(`credential-egress: unrecognized egress_mode '${String(resolved)}'`);
}
