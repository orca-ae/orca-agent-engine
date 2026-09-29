// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Runner-tunnel authentication posture, derived from service config.
//
// The runner tunnel is mounted on the PUBLIC `/v1/tunnels/runners/:runnerId`
// path (a self-hosted runner dials it from outside the mesh, so it cannot reach
// `/internal/*`). The global app-auth pre-handler bypasses `/v1/tunnels/*`
// (see `auth.ts`), so the WS handler is the ONLY thing standing between the open
// internet and a registered runner. This module turns the operator's config into
// the route options that make that handler fail closed by default:
//
//   - `allowedTunnelTokens` — the binding-token allow-list. When the operator
//     provisions one (`RUNNER_TUNNEL_TOKENS`), only those exact tokens may open a
//     remote tunnel, and each registers under a stable (not token-derived) id.
//   - `authProvider` — a {@link TunnelAuthProvider} that resolves the tunnel owner
//     from the handshake. Wiring ANY provider flips the route out of single-user
//     mode and makes the route's owner fail-closed gate reachable: an
//     unauthenticated NON-loopback peer is refused before accept instead of being
//     registered owner-less (and thus visible to / bindable by every tenant).
//
// The shipped default (no `RUNNER_TUNNEL_TOKENS`, no external identity provider)
// must still be safe even though the listener binds `0.0.0.0`: we wire a
// loopback-only auth provider so a LOCAL runner (the single-user laptop flow)
// registers as the reserved local owner, while every remote peer resolves to no
// identity and is refused by the fail-closed gate. Opening the tunnel to remote
// runners is therefore an explicit opt-in (provision `RUNNER_TUNNEL_TOKENS`),
// never the silent default.

import type { FastifyRequest } from 'fastify';
import { LOCAL_TUNNEL_OWNER, type TunnelAuthProvider } from '../api/runner-tunnel.routes.js';

/**
 * Resolve the peer host used for the loopback decision. Mirrors the runner-tunnel
 * route's `resolvePeerHost` seam so the provider and the route agree on what
 * "loopback" means even behind a trusted L4 proxy. Defaults to the upgrade
 * request's TCP remote address.
 */
export type PeerHostResolver = (req: FastifyRequest) => string | undefined;

/** Default peer-host source: the upgrade request's TCP remote address. */
const defaultPeerHost: PeerHostResolver = (req) => req.socket?.remoteAddress ?? undefined;

/**
 * Config slice this module reads. Mirrors the runner-tunnel fields on
 * {@link import('../config.js').ServiceConfig} so the builder can be unit-tested
 * without constructing a whole config.
 */
export interface RunnerTunnelAuthConfig {
  /**
   * Operator-provisioned binding-token allow-list. When non-empty, a remote
   * runner's `X-Orca-Runner-Tunnel-Token` must be one of these exact tokens and
   * the runner may use a stable id; loopback runners bypass it. Empty keeps the
   * tunnel closed to remote runners (loopback only).
   */
  runnerTunnelTokens: ReadonlySet<string>;
  /**
   * Extra permitted WS `Origin` values for the CSWSH guard, beyond the internal
   * sentinel + loopback. Always honored.
   */
  runnerTunnelAllowedOrigins: ReadonlySet<string>;
  /**
   * CSWSH local-mode flag. In local mode an `Origin` is allowed only when its
   * hostname is loopback (the single-user posture with no cookie/proxy auth).
   * Defaults to `true` for the self-hosted single-user deployment.
   */
  runnerTunnelLocalMode: boolean;
  /**
   * Optional peer-host resolver shared with the route's `resolvePeerHost` seam.
   * Behind a trusted L4 proxy a deployment supplies the same resolver to both the
   * route and the loopback-only provider so their loopback decisions stay in
   * lock-step. Defaults to the request's TCP remote address.
   */
  resolvePeerHost?: PeerHostResolver;
}

/**
 * Route options that pin the public runner tunnel's auth posture.
 *
 * Spread into {@link import('../api/runner-tunnel.routes.js').RunnerTunnelRouteOptions}
 * (and onto {@link import('../server.js').BuildAppOptions}) so the wiring is one
 * call. `allowedTunnelTokens` is omitted entirely when no allow-list is
 * configured (rather than passed as an empty set) so the route's
 * "loopback-only / fail-closed-remote" default is selected by the auth provider,
 * not accidentally turned into "remote token required" by an empty allow-list.
 */
export interface RunnerTunnelAuthWiring {
  authProvider: TunnelAuthProvider;
  allowedTunnelTokens?: ReadonlySet<string>;
  allowedOrigins: ReadonlySet<string>;
  localMode: boolean;
}

/**
 * Auth provider that admits ONLY loopback peers, used as the safe default for a
 * publicly-bound runner tunnel with no operator-provisioned credentials.
 *
 * Returns the reserved local owner for a loopback handshake (so the single-user
 * laptop runner registers and ownership checks stay coherent) and `null` for
 * every non-loopback handshake (so the route's owner fail-closed gate refuses a
 * remote peer before accept). Presenting an allow-listed binding token is the
 * supported way to admit a remote runner; without one, remote dials are refused.
 *
 * The loopback decision is taken through the SAME peer-host resolver the route
 * uses (defaulting to `req.socket.remoteAddress`), so this provider and the
 * route's own loopback gate never disagree — including behind a trusted L4 proxy
 * that supplies a custom resolver.
 */
export class LoopbackOnlyTunnelAuthProvider implements TunnelAuthProvider {
  private readonly resolvePeerHost: PeerHostResolver;

  constructor(resolvePeerHost: PeerHostResolver = defaultPeerHost) {
    this.resolvePeerHost = resolvePeerHost;
  }

  getUserId(req: FastifyRequest): string | null {
    const host = this.resolvePeerHost(req);
    return host !== undefined && hostnameIsLoopback(host) ? LOCAL_TUNNEL_OWNER : null;
  }
}

/**
 * Auth provider for an allow-list deployment: every allow-listed remote runner
 * is one tenant (the operator who holds the token), and loopback stays the
 * reserved local owner.
 *
 * With an allow-list configured the binding token IS the credential, so a
 * successful remote dial is attributed to a single shared operator owner (the
 * constructor `owner`, defaulting to the reserved local id). The point of wiring a
 * provider at all in this mode is to keep the route OUT of single-user mode so the
 * owner fail-closed gate stays reachable; the owner value itself is uniform
 * because all allow-listed tokens belong to the same operator.
 */
export class AllowlistTunnelAuthProvider implements TunnelAuthProvider {
  constructor(private readonly owner: string = LOCAL_TUNNEL_OWNER) {}

  getUserId(req: FastifyRequest): string {
    // Loopback or remote, an allow-list deployment attributes every accepted
    // runner to the one operator owner. (A non-loopback peer that is NOT
    // allow-listed never reaches here — the route's token-binding gate refuses it
    // first.)
    void req;
    return this.owner;
  }
}

/**
 * Build the runner-tunnel auth wiring from config.
 *
 * - No allow-list → {@link LoopbackOnlyTunnelAuthProvider}: loopback runners
 *   register, remote runners are refused (safe public default).
 * - Allow-list present → the allow-list is forwarded AND an
 *   {@link AllowlistTunnelAuthProvider} keeps the route out of single-user mode so
 *   the fail-closed gate stays live for any peer whose token is not allow-listed.
 */
export function buildRunnerTunnelAuth(config: RunnerTunnelAuthConfig): RunnerTunnelAuthWiring {
  const hasAllowlist = config.runnerTunnelTokens.size > 0;
  const wiring: RunnerTunnelAuthWiring = {
    authProvider: hasAllowlist
      ? new AllowlistTunnelAuthProvider()
      : new LoopbackOnlyTunnelAuthProvider(config.resolvePeerHost ?? defaultPeerHost),
    allowedOrigins: config.runnerTunnelAllowedOrigins,
    localMode: config.runnerTunnelLocalMode,
  };
  if (hasAllowlist) {
    wiring.allowedTunnelTokens = config.runnerTunnelTokens;
  }
  return wiring;
}

/**
 * Return whether a bare hostname / IP literal is a loopback host.
 *
 * `localhost`, the IPv4 loopback block `127.0.0.0/8`, IPv6 `::1`, and IPv4-mapped
 * loopback (`::ffff:127.0.0.1`) all count; anything else (including a missing
 * host) does not. Kept here — rather than imported from the route — so this
 * foundational auth seam has no dependency on route internals; the rules match
 * the route's own loopback predicate.
 */
function hostnameIsLoopback(host: string): boolean {
  if (host === 'localhost') {
    return true;
  }
  if (isIpv4LoopbackLiteral(host)) {
    return true;
  }
  if (host === '::1') {
    return true;
  }
  const mapped = host.toLowerCase().match(/:ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
  if (mapped !== null && isIpv4LoopbackLiteral(mapped[1]!)) {
    return true;
  }
  return false;
}

/** Whether `host` is a dotted-quad IPv4 literal inside `127.0.0.0/8`. */
function isIpv4LoopbackLiteral(host: string): boolean {
  const parts = host.split('.');
  if (parts.length !== 4) {
    return false;
  }
  const octets = parts.map((p) => Number(p));
  if (octets.some((n) => !Number.isInteger(n) || n < 0 || n > 255)) {
    return false;
  }
  if (parts.some((p) => p.length === 0 || (p.length > 1 && p.startsWith('0')))) {
    return false;
  }
  return octets[0] === 127;
}
