// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyRequest } from 'fastify';
import { LOCAL_TUNNEL_OWNER } from '@orca/harness-tunnel';
import { hostnameIsLoopback } from './tunnel-origin.js';

export interface RunnerTunnelAuthConfig {
  runnerTunnelTokens: ReadonlySet<string>;
  runnerTunnelAllowedOrigins: ReadonlySet<string>;
  runnerTunnelLocalMode: boolean;
  /** Use the same trusted peer source as the WebSocket route. */
  resolvePeerHost?: (req: FastifyRequest) => string | undefined;
}

/** Local/static fallback; durable Session assignments are authorized separately. */
export function buildRunnerTunnelAuth(config: RunnerTunnelAuthConfig) {
  const hasAllowlist = config.runnerTunnelTokens.size > 0;
  const peerHost = config.resolvePeerHost ?? ((req: FastifyRequest) => req.socket?.remoteAddress);
  return {
    authProvider: {
      getUserId(req: FastifyRequest): string | null {
        // Static tokens belong to one operator; without them only loopback has an identity.
        return hasAllowlist || hostnameIsLoopback(peerHost(req)) ? LOCAL_TUNNEL_OWNER : null;
      },
    },
    ...(hasAllowlist ? { allowedTunnelTokens: config.runnerTunnelTokens } : {}),
    allowedOrigins: config.runnerTunnelAllowedOrigins,
    localMode: config.runnerTunnelLocalMode,
  };
}
