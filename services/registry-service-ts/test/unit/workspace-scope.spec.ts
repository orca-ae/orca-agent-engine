// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyReply, FastifyRequest } from 'fastify';
import { describe, expect, it } from 'vitest';
import { requireWorkspaceScopes } from '../../src/auth/workspace-scope.js';

describe('Trigger workspace scopes', () => {
  it('accepts full access or every requested operation scope', () => {
    expect(check(['workspace.full_access'], ['workspace.agentTriggers.delete']).allowed).toBe(true);
    expect(
      check(
        ['workspace.agentTriggers.describe', 'workspace.sessions.describe'],
        ['workspace.agentTriggers.describe', 'workspace.sessions.describe'],
      ).allowed,
    ).toBe(true);
  });

  it('returns a Claude permission error when any requested scope is absent', () => {
    const result = check(
      ['workspace.agentTriggers.describe'],
      ['workspace.agentTriggers.describe', 'workspace.sessions.describe'],
    );
    expect(result).toMatchObject({
      allowed: false,
      status: 403,
      body: {
        type: 'error',
        error: {
          type: 'permission_error',
          message:
            'missing required scope: workspace.agentTriggers.describe, workspace.sessions.describe',
        },
        request_id: 'req_scope',
      },
    });
  });
});

function check(scopes: string[], required: string | string[]) {
  let status: number | undefined;
  let body: unknown;
  const req = {
    id: 'req_scope',
    auth: {
      workspaceId: 'ws_scope',
      principal: 'test',
      scopes,
      authMethod: 'api-key',
    },
  } as unknown as FastifyRequest;
  const reply = {
    code(value: number) {
      status = value;
      return this;
    },
    send(value: unknown) {
      body = value;
      return this;
    },
  } as unknown as FastifyReply;
  return { allowed: requireWorkspaceScopes(req, reply, required), status, body };
}
