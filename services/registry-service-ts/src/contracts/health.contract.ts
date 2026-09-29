// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import { openApiSecurity } from './openapi-security.js';

const c = initContract();

/**
 * Liveness and readiness probes.
 *
 * Published because they are part of the surface: they are served on the public
 * listener, reachable without credentials, and already relied on by the local
 * stack, the compose healthchecks, and every deployment manifest in
 * `charts/`. A route a client can call and a document that omits it is exactly
 * the kind of undocumented boundary the API-groups work exists to remove.
 *
 * Anthropic publishes no probes, so these classify as Orca extensions and are
 * tagged as such in the generated spec.
 *
 * They are *not* wrapped in the Claude error envelope — see
 * `src/middleware/claude-edge.ts` — because an orchestrator reads the status
 * code and nothing else, and a probe is not an API call.
 */

const SERVICE_NAME = z.string().describe('The service answering the probe.');

export const healthContract = c.router({
  healthz: {
    method: 'GET',
    path: '/healthz',
    summary: 'Liveness probe. Unauthenticated.',
    metadata: openApiSecurity([]),
    responses: {
      200: z.object({ status: z.literal('ok'), service: SERVICE_NAME }),
    },
  },
  readyz: {
    method: 'GET',
    path: '/readyz',
    summary: 'Readiness probe. Unauthenticated.',
    metadata: openApiSecurity([]),
    responses: {
      200: z.object({ status: z.literal('ready'), service: SERVICE_NAME }),
    },
  },
});
