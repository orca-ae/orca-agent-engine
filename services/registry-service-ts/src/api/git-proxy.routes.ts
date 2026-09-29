// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { validateGitUploadPack } from '../domain/git-upload-pack.js';
import type { FastifyError, FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { promisify } from 'node:util';
import { gunzip } from 'node:zlib';
import type { GitCredsRouteDeps } from './git-creds.routes.js';
import { buildClaudeErrorResponse } from '../contracts/common.js';
import {
  gitProxyContract,
  gitProxyDiscoveryQuery,
  gitProxyHeaders,
  gitProxyParams,
  gitProxyUploadQuery,
  GIT_ADVERTISEMENT_TYPE,
  GIT_UPLOAD_REQUEST_TYPE,
  GIT_UPLOAD_RESULT_TYPE,
} from '../contracts/git-proxy.contract.js';
import { loadGitProxyBinding } from '../domain/git-proxy-capability.js';
import {
  createGitProxyRequest,
  GIT_PROXY_REQUEST_MAX_BYTES,
  GIT_PROXY_TIMEOUT_MS,
  type GitProxyRequest,
} from '../domain/git-proxy-transport.js';
import { buildRuntimeSecretResolver } from '../secrets/runtime-resolver.js';

const inflateGitRequest = promisify(gunzip);

export function registerGitProxyRoutes(
  app: FastifyInstance,
  deps: GitCredsRouteDeps & { request?: GitProxyRequest },
): void {
  const secrets = buildRuntimeSecretResolver(deps.secretProvider, deps.secretStore);
  const upstream = deps.request ?? createGitProxyRequest();
  app.addContentTypeParser(
    GIT_UPLOAD_REQUEST_TYPE,
    { parseAs: 'buffer', bodyLimit: GIT_PROXY_REQUEST_MAX_BYTES },
    (_req, body, done) => done(null, body),
  );

  function beginRequest(req: FastifyRequest, reply: FastifyReply, done: () => void) {
    // Start before body parsing so a slow upload cannot bypass the deadline.
    // An extra five seconds covers bounded local auth/response work around the
    // upstream's own 60-second deadline. Never cache parser/auth failures either.
    reply.header('cache-control', 'no-store');
    const timeout = setTimeout(() => req.raw.destroy(), GIT_PROXY_TIMEOUT_MS + 5_000);
    timeout.unref();
    const cleanup = () => {
      clearTimeout(timeout);
      reply.raw.off('finish', cleanup);
      reply.raw.off('close', cleanup);
    };
    reply.raw.once('finish', cleanup);
    reply.raw.once('close', cleanup);
    done();
  }

  function parserError(error: FastifyError, req: FastifyRequest, reply: FastifyReply) {
    // Parser failures precede the handler. Keep these routes' binary request
    // errors in the same fixed envelope as their declared contract, without
    // changing the behavior of unrelated API routes or exposing parser details.
    reply.header('cache-control', 'no-store');
    if (error.statusCode === 413)
      return reply
        .code(413)
        .send(
          buildClaudeErrorResponse(req.id, 'request_too_large', 'Git request body is too large'),
        );
    if (error.statusCode === 415)
      return reply
        .code(415)
        .send(
          buildClaudeErrorResponse(
            req.id,
            'invalid_request_error',
            'Git upload-pack bytes are required',
          ),
        );
    if (error.statusCode === 400)
      return reply
        .code(400)
        .send(
          buildClaudeErrorResponse(req.id, 'invalid_request_error', 'Invalid Git proxy request'),
        );
    return app.errorHandler(error, req, reply);
  }

  async function handle(req: FastifyRequest, reply: FastifyReply) {
    reply.header('cache-control', 'no-store');
    const fail = (code: 400 | 401 | 404 | 413 | 415 | 502, message: string) =>
      reply
        .code(code)
        .send(
          buildClaudeErrorResponse(
            req.id,
            code === 401
              ? 'authentication_error'
              : code === 404
                ? 'not_found_error'
                : code === 413
                  ? 'request_too_large'
                  : code === 502
                    ? 'api_error'
                    : 'invalid_request_error',
            message,
          ),
        );
    const authorization = req.headers.authorization;
    if (!authorization?.startsWith('Bearer ') || authorization.length > 16_384)
      return fail(401, 'Invalid Git proxy authorization');
    let verified;
    try {
      verified = await deps.jwtMinter.verify(authorization.slice(7).trim(), {
        expectedAudience: 'git-proxy',
      });
    } catch {
      return fail(401, 'Invalid Git proxy authorization');
    }
    if (!verified.gitProxy) return fail(401, 'Invalid Git proxy authorization');
    const encoding = req.headers['content-encoding'];
    if (encoding !== undefined && encoding !== 'identity' && encoding !== 'gzip')
      return fail(415, 'Unsupported Git request encoding');
    const params = gitProxyParams.safeParse(req.params);
    const headers = gitProxyHeaders.safeParse(req.headers);
    const query = (req.method === 'GET' ? gitProxyDiscoveryQuery : gitProxyUploadQuery).safeParse(
      req.query,
    );
    if (!params.success || !headers.success || !query.success)
      return fail(400, 'Invalid Git proxy request');
    if (verified.gitProxy.resourceId !== params.data.resourceId)
      return fail(404, 'Git resource is unavailable');
    if (
      req.method === 'POST' &&
      (!Buffer.isBuffer(req.body) ||
        req.headers['content-type']?.split(';')[0] !== GIT_UPLOAD_REQUEST_TYPE)
    )
      return fail(415, 'Git upload-pack bytes are required');
    let requestBody = req.method === 'POST' ? (req.body as Buffer) : undefined;
    if (requestBody && encoding === 'gzip') {
      // Native Git compresses larger smart-HTTP requests. The parser already
      // bounds compressed bytes; zlib's output cap independently bounds inflated
      // bytes before the transport sets their Content-Length and forwards them.
      try {
        requestBody = await inflateGitRequest(requestBody, {
          maxOutputLength: GIT_PROXY_REQUEST_MAX_BYTES,
        });
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ERR_BUFFER_TOO_LARGE')
          return fail(413, 'Git request body is too large');
        return fail(400, 'Invalid Git upload-pack encoding');
      }
    }
    if (requestBody) {
      try {
        validateGitUploadPack(requestBody, headers.data['git-protocol']);
      } catch {
        return fail(400, 'Invalid Git upload-pack request');
      }
    }
    const binding = await loadGitProxyBinding(deps.db, {
      workspaceId: verified.workspaceId,
      sessionId: verified.sessionId,
      resourceId: params.data.resourceId,
    });
    if (
      !binding ||
      binding.organizationId !== verified.orgId ||
      binding.scope.repoUrl !== verified.gitProxy.repoUrl ||
      binding.scope.credentialId !== verified.gitProxy.credentialId ||
      binding.scope.credentialRevision !== verified.gitProxy.credentialRevision
    )
      return fail(404, 'Git resource is unavailable');
    const controller = new AbortController();
    const abort = () => controller.abort();
    req.raw.once('aborted', abort);
    reply.raw.once('close', abort);
    if (req.raw.aborted || reply.raw.destroyed) abort();
    try {
      const pat = await secrets.resolve(binding.secretRef);
      if (!pat) return fail(404, 'Git resource is unavailable');
      const bytes = await upstream({
        repoUrl: binding.scope.repoUrl,
        pat,
        method: req.method === 'GET' ? 'GET' : 'POST',
        ...(requestBody ? { body: requestBody } : {}),
        ...(headers.data['git-protocol'] ? { gitProtocol: headers.data['git-protocol'] } : {}),
        signal: controller.signal,
      });
      return reply
        .type(req.method === 'GET' ? GIT_ADVERTISEMENT_TYPE : GIT_UPLOAD_RESULT_TYPE)
        .send(bytes);
    } catch {
      return fail(502, 'Git upstream request failed');
    } finally {
      req.raw.off('aborted', abort);
      reply.raw.off('close', abort);
    }
  }
  app.get(
    gitProxyContract.advertise.path,
    {
      exposeHeadRoute: false,
      bodyLimit: GIT_PROXY_REQUEST_MAX_BYTES,
      onRequest: beginRequest,
      errorHandler: parserError,
    },
    handle,
  );
  app.post(
    gitProxyContract.uploadPack.path,
    { bodyLimit: GIT_PROXY_REQUEST_MAX_BYTES, onRequest: beginRequest, errorHandler: parserError },
    handle,
  );
}
