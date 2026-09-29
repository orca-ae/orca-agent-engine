// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import { ClaudeErrorResponse, idString } from './common.js';
import { binaryDownload, openApiMedia } from './openapi-media.js';
import { openApiSecurity } from './openapi-security.js';

const c = initContract();
export const GIT_UPLOAD_REQUEST_TYPE = 'application/x-git-upload-pack-request';
export const GIT_UPLOAD_RESULT_TYPE = 'application/x-git-upload-pack-result';
export const GIT_ADVERTISEMENT_TYPE = 'application/x-git-upload-pack-advertisement';
export const gitProxyParams = z.object({ resourceId: idString('sesrsc') });
export const gitProxyDiscoveryQuery = z.object({ service: z.literal('git-upload-pack') }).strict();
export const gitProxyUploadQuery = z.object({}).strict();
export const gitProxyHeaders = z
  .object({
    authorization: z.string(),
    'git-protocol': z.enum(['version=0', 'version=1', 'version=2']).optional(),
    'content-encoding': z.enum(['identity', 'gzip']).optional(),
  })
  .passthrough();
const errors = {
  400: ClaudeErrorResponse,
  401: ClaudeErrorResponse,
  404: ClaudeErrorResponse,
  413: ClaudeErrorResponse,
  415: ClaudeErrorResponse,
  502: ClaudeErrorResponse,
};

/** Git smart HTTP reads only. These capabilities cannot resolve raw credentials. */
export const gitProxyContract = c.router({
  advertise: {
    method: 'GET',
    path: '/v1/git-proxy/:resourceId/info/refs',
    pathParams: gitProxyParams,
    query: gitProxyDiscoveryQuery,
    headers: gitProxyHeaders,
    responses: { 200: z.unknown(), ...errors },
    metadata: {
      ...openApiSecurity([{ gitProxyJwt: [] }]),
      ...openApiMedia({
        responses: { 200: binaryDownload(GIT_ADVERTISEMENT_TYPE, 'Git upload-pack advertisement') },
      }),
    },
  },
  uploadPack: {
    method: 'POST',
    path: '/v1/git-proxy/:resourceId/git-upload-pack',
    pathParams: gitProxyParams,
    query: gitProxyUploadQuery,
    headers: gitProxyHeaders,
    body: z.unknown(),
    responses: { 200: z.unknown(), ...errors },
    metadata: {
      ...openApiSecurity([{ gitProxyJwt: [] }]),
      ...openApiMedia({
        requestBody: binaryDownload(GIT_UPLOAD_REQUEST_TYPE, 'Git upload-pack request bytes'),
        responses: { 200: binaryDownload(GIT_UPLOAD_RESULT_TYPE, 'Git upload-pack result bytes') },
      }),
    },
  },
});
