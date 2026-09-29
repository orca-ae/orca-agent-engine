// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyReply, FastifyRequest } from 'fastify';
import { buildClaudeErrorResponse } from '../contracts/common.js';

export function requireWorkspaceScopes(
  req: FastifyRequest,
  reply: FastifyReply,
  required: string | string[],
): boolean {
  const scopes = req.auth?.scopes ?? [];
  const requiredScopes = Array.isArray(required) ? required : [required];
  if (
    scopes.includes('workspace.full_access') ||
    requiredScopes.every((scope) => scopes.includes(scope))
  ) {
    return true;
  }
  reply
    .code(403)
    .send(
      buildClaudeErrorResponse(
        req.id,
        'permission_error',
        `missing required scope: ${requiredScopes.join(', ')}`,
      ),
    );
  return false;
}
