// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import { isIP } from 'node:net';

export const egressHost = z
  .string()
  .max(253)
  .regex(/^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/)
  .refine(
    (host) =>
      !isIP(host) &&
      host
        .split('.')
        .every(
          (label) =>
            label.length > 0 &&
            label.length <= 63 &&
            !label.startsWith('-') &&
            !label.endsWith('-'),
        ),
  );
export const egressTarget = z
  .object({ host: egressHost, port: z.union([z.literal(80), z.literal(443)]) })
  .strict();
const pod = z
  .object({
    namespace: z.string().min(1).max(253),
    name: z.string().min(1).max(253),
    uid: z.string().min(1).max(128),
  })
  .strict();
export const egressOwnerAuthority = z.discriminatedUnion('kind', [
  z
    .object({
      kind: z.literal('harness'),
      owner_pod: pod,
      sandbox_pod: pod,
      sandbox_handle_id: z.string().min(1).max(256),
    })
    .strict(),
  z
    .object({
      kind: z.literal('registry'),
      runner_id: z.string().min(1),
      environment_id: z.string().min(1),
      owner_pod: z.string().min(1),
      worker_conn_id: z.string().min(1),
      launch_digest: z.string().min(1),
      sandbox_handle_id: z.string().min(1).max(253),
      sandbox_pod: pod,
    })
    .strict(),
]);
export type EgressOwnerAuthority = z.infer<typeof egressOwnerAuthority>;
export const egressAuthorizeRequest = z
  .object({ capability: z.string().min(32).max(256), target: egressTarget })
  .strict();
export const egressResolveRequest = z
  .object({
    binding_id: z.string().min(1).max(128),
    generation: z.string().min(1).max(128),
    scope: z.enum(['sandbox', 'session']),
    target: egressTarget,
    method: z.string().regex(/^[A-Z]{1,16}$/),
    path_and_query: z.string().min(1).max(8192),
    candidate_headers: z
      .array(
        z
          .object({
            name: z.string().regex(/^[a-z0-9!#$%&'*+.^_`|~-]{1,128}$/),
            value: z
              .string()
              .max(16384)
              .refine((value) => !/[\r\n\0]/.test(value)),
          })
          .strict(),
      )
      .max(32),
  })
  .strict();
export const egressIssueRequest = z
  .object({
    sandbox_id: z
      .string()
      .min(1)
      .max(253)
      .regex(/^[a-z0-9][a-z0-9.-]*$/),
    owner: pod,
  })
  .strict();
export const egressRevokeRequest = z
  .object({ binding_id: z.string().min(1), generation: z.string().min(1) })
  .strict();
export const egressResolveResponse = z.object({
  replacements: z.array(z.object({ header: z.string(), value: z.string() })),
  credential_version: z.string().nullable(),
  request_body_policy: z.enum(['git_upload_pack', 'stream']),
});
export type EgressResolveResponse = z.infer<typeof egressResolveResponse>;
const error = z.object({
  error: z.object({
    type: z.enum([
      'invalid_capability',
      'permission_denied',
      'dependency_unavailable',
      'invalid_request',
    ]),
    message: z.string(),
  }),
});
const errors = { 400: error, 401: error, 403: error, 503: error };
export const sandboxEgressContract = initContract().router({
  authorize: {
    method: 'POST',
    path: '/internal/v1/sandbox-egress/authorize',
    body: egressAuthorizeRequest,
    responses: {
      200: z.object({
        binding_id: z.string(),
        generation: z.string(),
        scope: z.enum(['sandbox', 'session']),
        decision: z.enum(['mitm', 'passthrough']),
        managed_auth: z.boolean(),
      }),
      ...errors,
    },
  },
  resolve: {
    method: 'POST',
    path: '/internal/v1/sandbox-egress/resolve',
    body: egressResolveRequest,
    responses: {
      200: egressResolveResponse,
      ...errors,
    },
  },
  issue: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/session-egress/issue',
    body: egressIssueRequest,
    responses: {
      200: z.object({
        network_binding_id: z.string(),
        binding_id: z.string(),
        generation: z.string(),
        proxy_capability: z.string(),
        placeholders: z.record(z.string()),
        git: z.object({ marker: z.string(), username: z.string() }).optional(),
      }),
      ...errors,
    },
  },
  revoke: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/session-egress/revoke',
    body: egressRevokeRequest,
    responses: { 200: z.object({ revoked: z.literal(true) }), ...errors },
  },
  registerNetwork: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/sandbox-networks',
    body: z.object({ owner: pod }).strict(),
    responses: { 200: z.object({ binding_id: z.string(), generation: z.string() }), ...errors },
  },
  revokeNetwork: {
    method: 'POST',
    path: '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/sandbox-networks/revoke',
    body: egressRevokeRequest,
    responses: { 200: z.object({ revoked: z.literal(true) }), ...errors },
  },
  bootstrapNetwork: {
    method: 'POST',
    path: '/internal/v1/sandbox-networks/:bindingId/bootstrap',
    body: z.object({ generation: z.string().min(1).max(128) }).strict(),
    responses: {
      200: z.object({
        binding_id: z.string(),
        generation: z.string(),
        proxy_url: z.string(),
        proxy_capability: z.string(),
        internal_bypass: z.array(z.string()),
      }),
      ...errors,
    },
  },
});
