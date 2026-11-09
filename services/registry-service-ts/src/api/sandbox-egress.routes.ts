// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance, FastifyRequest } from 'fastify';
import { sandboxEgressContract as contract } from '../contracts/sandbox-egress.contract.js';
import { SandboxEgressError, type SandboxEgressService } from '../domain/sandbox-egress.js';

export function registerSandboxEgressRoutes(
  app: FastifyInstance,
  service: SandboxEgressService,
): void {
  const params = (req: FastifyRequest) => req.params as { workspaceId: string; sessionId: string };
  const requireCaller = (req: FastifyRequest, caller: 'harness' | 'ai-gateway') => {
    if (req.internalAuth?.caller !== caller && req.internalAuth?.caller !== 'shared')
      throw new SandboxEgressError('permission_denied');
  };
  app.register(async (scope) => {
    scope.addHook('onRequest', async (_req, reply) => {
      reply.header('cache-control', 'private, no-store');
    });
    // Never log request bodies or resolver replacements; errors use fixed non-sensitive text.
    scope.setErrorHandler((error, _req, reply) => {
      const statusCode =
        error instanceof Error && 'statusCode' in error ? error.statusCode : undefined;
      const type =
        error instanceof SandboxEgressError
          ? error.type
          : typeof statusCode === 'number' && [400, 413].includes(statusCode)
            ? 'invalid_request'
            : 'dependency_unavailable';
      switch (type) {
        case 'invalid_capability':
          reply.code(401);
          break;
        case 'permission_denied':
          reply.code(403);
          break;
        case 'invalid_request':
          reply.code(400);
          break;
        case 'dependency_unavailable':
          reply.code(503);
          break;
      }
      return reply.send({ error: { type, message: type.replaceAll('_', ' ') } });
    });
    scope.post(contract.authorize.path, { bodyLimit: 65536 }, async (req) => {
      requireCaller(req, 'ai-gateway');
      const body = contract.authorize.body.safeParse(req.body);
      if (!body.success) throw new SandboxEgressError('invalid_request');
      return service.authorizeProxy(body.data.capability, body.data.target);
    });
    scope.post(contract.resolve.path, { bodyLimit: 65536 }, async (req) => {
      requireCaller(req, 'ai-gateway');
      const body = contract.resolve.body.safeParse(req.body);
      if (!body.success) throw new SandboxEgressError('invalid_request');
      return service.resolveRequest(body.data);
    });
    scope.post(contract.issue.path, { bodyLimit: 8192 }, async (req) => {
      requireCaller(req, 'harness');
      const body = contract.issue.body.safeParse(req.body);
      // Workload-authenticated Harness is trusted to supply platform downward-API identity.
      if (!body.success) throw new SandboxEgressError('invalid_request');
      const { workspaceId, sessionId } = params(req);
      return service.issueHarnessEgress(
        workspaceId,
        sessionId,
        body.data.sandbox_id,
        body.data.owner,
      );
    });
    scope.post(contract.revoke.path, { bodyLimit: 8192 }, async (req) => {
      requireCaller(req, 'harness');
      const body = contract.revoke.body.safeParse(req.body);
      if (!body.success) throw new SandboxEgressError('invalid_request');
      const { workspaceId, sessionId } = params(req);
      return service.revokeEgress(
        workspaceId,
        sessionId,
        body.data.binding_id,
        body.data.generation,
      );
    });
    scope.post(contract.registerNetwork.path, { bodyLimit: 8192 }, async (req) => {
      if (req.internalAuth?.caller !== 'harness') throw new SandboxEgressError('permission_denied');
      const body = contract.registerNetwork.body.safeParse(req.body);
      if (!body.success) throw new SandboxEgressError('invalid_request');
      const { workspaceId, sessionId } = params(req);
      return service.networks.registerHarnessNetwork(workspaceId, sessionId, body.data.owner);
    });
    scope.post(contract.revokeNetwork.path, { bodyLimit: 8192 }, async (req) => {
      if (req.internalAuth?.caller !== 'harness') throw new SandboxEgressError('permission_denied');
      const body = contract.revokeNetwork.body.safeParse(req.body);
      if (!body.success) throw new SandboxEgressError('invalid_request');
      const { workspaceId, sessionId } = params(req);
      return service.networks.revokeHarnessNetwork(
        workspaceId,
        sessionId,
        body.data.binding_id,
        body.data.generation,
      );
    });
    scope.post(contract.bootstrapNetwork.path, { bodyLimit: 8192 }, async (req) => {
      const body = contract.bootstrapNetwork.body.safeParse(req.body);
      const token = /^Bearer ([^\s]+)$/i.exec(req.headers.authorization ?? '')?.[1];
      if (!body.success) throw new SandboxEgressError('invalid_request');
      if (!token) throw new SandboxEgressError('invalid_capability');
      return service.networks.bootstrap(
        (req.params as { bindingId: string }).bindingId,
        body.data.generation,
        token,
      );
    });
  });
}
