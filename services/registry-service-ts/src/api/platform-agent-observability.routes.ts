// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance } from 'fastify';
import {
  platformAgentObservabilityContract,
  PlatformAgentObservabilityPolicySchema,
  PlatformAgentObservabilityPutHeadersSchema,
  PlatformAgentObservabilityPutRequestSchema,
} from '../contracts/platform-agent-observability.contract.js';
import {
  loadPlatformAgentObservabilityPolicy,
  platformAgentObservabilityPolicyEtag,
  replacePlatformAgentObservabilityPolicy,
} from '../domain/agent-observability-platform-policy.js';
import type { DbClient } from '../persistence/postgres/client.js';
import { runPlatformMutation, writePlatformAudit } from './platform-mutations.js';

const unavailable = { error: 'agent observability platform policy unavailable' };

export function registerPlatformAgentObservabilityRoutes(app: FastifyInstance, db: DbClient): void {
  const contract = platformAgentObservabilityContract;
  app.get(contract.getPolicy.path, async (req, reply) => {
    try {
      const policy = await loadPlatformAgentObservabilityPolicy(db);
      return reply.header('etag', platformAgentObservabilityPolicyEtag(policy)).send(policy);
    } catch {
      req.log.error('agent observability platform policy read unavailable');
      return reply.code(503).send(unavailable);
    }
  });

  app.put(contract.putPolicy.path, async (req, reply) => {
    const body = PlatformAgentObservabilityPutRequestSchema.safeParse(req.body);
    if (!body.success) {
      return reply.code(400).send({ error: 'invalid agent observability platform policy request' });
    }
    if (req.headers['if-match'] === undefined) {
      return reply.code(428).send({ error: 'if-match is required' });
    }
    const headers = PlatformAgentObservabilityPutHeadersSchema.safeParse(req.headers);
    if (!headers.success) {
      return reply.code(400).send({ error: 'invalid agent observability platform policy request' });
    }
    try {
      const result = await runPlatformMutation(
        db,
        req,
        'PUT ' + contract.putPolicy.path,
        headers.data['idempotency-key'],
        // Include the precondition in the normalized request identity. A retry
        // with the original ETag replays before checking current singleton state.
        { policy: body.data, if_match: headers.data['if-match'] },
        async (tx) => {
          const replacement = await replacePlatformAgentObservabilityPolicy(
            tx,
            body.data,
            headers.data['if-match'],
          );
          if (replacement.kind === 'stale') {
            return { status: 412, body: { error: 'agent observability platform policy is stale' } };
          }
          if (replacement.changed) {
            await writePlatformAudit(
              tx,
              req,
              'agent_observability.platform_policy.updated',
              'agent_observability_platform_policy',
              'default',
              null,
              null,
              { before: replacement.before, after: replacement.after },
            );
          }
          return { status: 200, body: replacement.after };
        },
      );
      if (result.status === 200) {
        // The replayed representation, not today's policy, owns this ETag.
        const policy = PlatformAgentObservabilityPolicySchema.parse(
          JSON.parse(result.serializedBody),
        );
        reply.header('etag', platformAgentObservabilityPolicyEtag(policy));
        return reply.code(200).type('application/json').send(result.serializedBody);
      }
      if (result.status === 409) {
        return reply.code(409).send({ error: 'idempotency-key reused with different request' });
      }
      if (result.status === 412) {
        return reply.code(412).send({ error: 'agent observability platform policy is stale' });
      }
      throw new Error('unexpected platform policy mutation status');
    } catch {
      req.log.error('agent observability platform policy mutation unavailable');
      return reply.code(503).send(unavailable);
    }
  });
}
