// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import {
  LOGICAL_CREDENTIAL_ID_PATTERN,
  PROVIDER_CREDENTIAL_PROVIDERS,
  PROVIDER_CREDENTIAL_SCHEMES,
  isCompatibleProviderCredentialScheme,
  isLogicalCredentialId,
} from '../domain/provider-credential.js';
import { ClaudeErrorResponse, idString, isoTimestamp } from './common.js';

const c = initContract();
const vaultId = idString('vlt');
const logicalCredentialId = z.string().refine(isLogicalCredentialId, {
  message: `must match ${LOGICAL_CREDENTIAL_ID_PATTERN.source}`,
});
const Metadata = z
  .record(z.string().min(1).max(64), z.string().max(512))
  .refine((value) => Object.keys(value).length <= 16, 'metadata must contain at most 16 pairs');
const MetadataPatch = z.record(z.string().min(1).max(64), z.string().max(512).nullable());
const vaultPagination = z.object({
  limit: z.number().int().min(1).optional(),
  page: z.string().optional(),
  include_archived: z.boolean().optional(),
});
const DISPLAY_NAME_ERROR = 'display_name must be a non-empty string up to 255 characters';
const displayName = z.string().min(1).max(255);
const displayNameInput = z
  .string({
    required_error: DISPLAY_NAME_ERROR,
    invalid_type_error: DISPLAY_NAME_ERROR,
  })
  .min(1, { message: DISPLAY_NAME_ERROR })
  .max(255, { message: DISPLAY_NAME_ERROR })
  .refine((value) => value.trim().length > 0, {
    message: DISPLAY_NAME_ERROR,
  });

const Vault = z.object({
  id: vaultId,
  type: z.literal('vault'),
  display_name: displayName,
  metadata: Metadata,
  archived_at: isoTimestamp.nullable(),
  created_at: isoTimestamp,
  updated_at: isoTimestamp,
});

export const VaultCreate = z
  .object({
    display_name: displayNameInput,
    metadata: Metadata.optional(),
  })
  .strict();

export const VaultUpdate = z
  .object({
    display_name: displayName.nullable().optional(),
    metadata: MetadataPatch.nullable().optional(),
  })
  .strict();

const StaticBearerAuthResponse = z.object({
  type: z.literal('static_bearer'),
  mcp_server_url: z.string(),
});

const McpOauthAuthResponse = z.object({
  type: z.literal('mcp_oauth'),
  mcp_server_url: z.string(),
  expires_at: isoTimestamp.nullable().optional(),
  refresh: z
    .object({
      token_endpoint: z.string(),
      client_id: z.string(),
      token_endpoint_auth: z.object({
        type: z.enum(['none', 'client_secret_basic', 'client_secret_post']),
      }),
      resource: z.string().nullable().optional(),
      scope: z.string().nullable().optional(),
    })
    .nullable()
    .optional(),
});

const credentialHost = z
  .string()
  .min(1)
  .max(253)
  .refine((value) => {
    if (
      /\s/.test(value) ||
      value.includes('/') ||
      value.includes(':') ||
      value.includes('[') ||
      value.includes(']')
    ) {
      return false;
    }
    const host = value.startsWith('*.') ? value.slice(2) : value;
    if (!host || host.startsWith('*.')) return false;
    if (/^\d+(?:\.\d+){3}$/.test(host)) {
      return !value.startsWith('*.') && host.split('.').every((part) => Number(part) <= 255);
    }
    return host
      .split('.')
      .every((label) => /^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$/.test(label));
  }, 'must be a bare hostname, IPv4 address, or *. wildcard');

const EnvVarNetworking = z.discriminatedUnion('type', [
  z.object({
    type: z.literal('limited'),
    allowed_hosts: z.array(credentialHost).max(16),
  }),
  z.object({ type: z.literal('unrestricted') }),
]);
const InjectionLocation = z.object({
  header: z.boolean().optional(),
  body: z.boolean().optional(),
});
const InjectionLocationCreate = InjectionLocation.refine(
  (value) => (value.header ?? false) || (value.body ?? false),
  'must enable header or body',
);

// secret_value is intentionally absent — it is write-only and never echoed.
const EnvironmentVariableAuthResponse = z.object({
  type: z.literal('environment_variable'),
  secret_name: z.string(),
  networking: EnvVarNetworking,
  injection_location: z.object({ header: z.boolean(), body: z.boolean() }),
});

const ProviderAuthResponse = z.object({
  type: z.literal('provider'),
  provider: z.enum(PROVIDER_CREDENTIAL_PROVIDERS),
  scheme: z.enum(PROVIDER_CREDENTIAL_SCHEMES),
  logical_id: logicalCredentialId,
  version: z.string().min(1),
});

const VaultCredential = z.object({
  id: idString('vcrd'),
  type: z.literal('vault_credential'),
  vault_id: vaultId,
  display_name: z.string().nullable(),
  auth: z.discriminatedUnion('type', [
    StaticBearerAuthResponse,
    McpOauthAuthResponse,
    EnvironmentVariableAuthResponse,
    ProviderAuthResponse,
  ]),
  metadata: Metadata,
  archived_at: isoTimestamp.nullable(),
  created_at: isoTimestamp,
  updated_at: isoTimestamp,
});

const StaticBearerAuthCreate = z.object({
  type: z.literal('static_bearer'),
  token: z.string().min(1),
  mcp_server_url: z.string().url(),
});

const McpOauthAuthCreate = z.object({
  type: z.literal('mcp_oauth'),
  access_token: z.string().min(1),
  mcp_server_url: z.string().url(),
  expires_at: isoTimestamp.nullable().optional(),
  refresh: z
    .object({
      refresh_token: z.string().min(1),
      token_endpoint: z.string().min(1),
      client_id: z.string().min(1),
      token_endpoint_auth: z.discriminatedUnion('type', [
        z.object({ type: z.literal('none') }),
        z.object({ type: z.literal('client_secret_basic'), client_secret: z.string().min(1) }),
        z.object({ type: z.literal('client_secret_post'), client_secret: z.string().min(1) }),
      ]),
      resource: z.string().nullable().optional(),
      scope: z.string().nullable().optional(),
    })
    .nullable()
    .optional(),
});

const EnvironmentVariableAuthCreate = z.object({
  type: z.literal('environment_variable'),
  secret_name: z.string().min(1).max(255),
  secret_value: z.string().min(1),
  networking: EnvVarNetworking,
  injection_location: InjectionLocationCreate.optional(),
});

const ProviderAuthCreate = z
  .object({
    type: z.literal('provider'),
    provider: z.enum(PROVIDER_CREDENTIAL_PROVIDERS),
    scheme: z.enum(PROVIDER_CREDENTIAL_SCHEMES),
    logical_id: logicalCredentialId,
    secret_value: z.string().min(1),
  })
  .superRefine((value, ctx) => {
    if (!isCompatibleProviderCredentialScheme(value.provider, value.scheme)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['scheme'],
        message: `${value.scheme} is not compatible with provider ${value.provider}`,
      });
    }
  });

const credentialDisplayName = z.string().min(1).max(255).nullable().optional();

const VaultCredentialCreate = z.object({
  display_name: credentialDisplayName,
  metadata: Metadata.optional(),
  auth: z.union([
    StaticBearerAuthCreate,
    McpOauthAuthCreate,
    EnvironmentVariableAuthCreate,
    ProviderAuthCreate,
  ]),
});

const StaticBearerAuthUpdate = z.object({
  type: z.literal('static_bearer'),
  token: z.string().nullable().optional(),
});

const McpOauthAuthUpdate = z.object({
  type: z.literal('mcp_oauth'),
  access_token: z.string().nullable().optional(),
  expires_at: isoTimestamp.nullable().optional(),
  refresh: z
    .object({
      refresh_token: z.string().nullable().optional(),
      scope: z.string().nullable().optional(),
      token_endpoint_auth: z
        .discriminatedUnion('type', [
          z.object({
            type: z.literal('client_secret_basic'),
            client_secret: z.string().nullable().optional(),
          }),
          z.object({
            type: z.literal('client_secret_post'),
            client_secret: z.string().nullable().optional(),
          }),
        ])
        .optional(),
    })
    .nullable()
    .optional(),
});

const EnvironmentVariableAuthUpdate = z.object({
  type: z.literal('environment_variable'),
  injection_location: InjectionLocation.optional(),
  networking: EnvVarNetworking.nullable().optional(),
  secret_value: z.string().nullable().optional(),
});

const ProviderAuthUpdate = z.object({
  type: z.literal('provider'),
  logical_id: logicalCredentialId.optional(),
  secret_value: z.string().nullable().optional(),
});

const VaultCredentialUpdate = z.object({
  display_name: credentialDisplayName,
  metadata: MetadataPatch.nullable().optional(),
  auth: z
    .discriminatedUnion('type', [
      StaticBearerAuthUpdate,
      McpOauthAuthUpdate,
      EnvironmentVariableAuthUpdate,
      ProviderAuthUpdate,
    ])
    .optional(),
});

const HttpResponseCapture = z.object({
  status_code: z.number().int(),
  content_type: z.string(),
  body: z.string(),
  body_truncated: z.boolean(),
});

const McpProbe = z.object({
  method: z.literal('initialize'),
  http_response: HttpResponseCapture.nullable(),
});

const RefreshOutcome = z.object({
  status: z.enum(['succeeded', 'connect_error', 'failed', 'no_refresh_token']),
  http_response: HttpResponseCapture.nullable(),
});

const VaultCredentialValidation = z.object({
  type: z.literal('vault_credential_validation'),
  credential_id: idString('vcrd'),
  vault_id: vaultId,
  validated_at: isoTimestamp,
  has_refresh_token: z.boolean(),
  status: z.enum(['valid', 'invalid', 'unknown']),
  mcp_probe: McpProbe,
  refresh: RefreshOutcome,
});

const VaultDeleted = z.object({ id: vaultId, type: z.literal('vault_deleted') });
const VaultCredentialDeleted = z.object({
  id: idString('vcrd'),
  type: z.literal('vault_credential_deleted'),
});

const vaultCredentialHeaders = z.object({ 'orca-beta': z.string().optional() }).passthrough();
const vaultCredentialMutationHeaders = z
  .object({
    'idempotency-key': z.string().optional(),
    'orca-beta': z.string().optional(),
  })
  .passthrough();

export const vaultsContract = c.router({
  create: {
    method: 'POST',
    path: '/v1/vaults',
    body: VaultCreate,
    responses: {
      200: Vault,
      400: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
    },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
  },
  get: {
    method: 'GET',
    path: '/v1/vaults/:id',
    pathParams: z.object({ id: vaultId }),
    responses: { 200: Vault, 404: ClaudeErrorResponse },
  },
  list: {
    method: 'GET',
    path: '/v1/vaults',
    query: vaultPagination,
    responses: {
      200: z.object({ data: z.array(Vault), next_page: z.string().nullable() }),
      400: ClaudeErrorResponse,
    },
  },
  update: {
    method: 'POST',
    path: '/v1/vaults/:id',
    pathParams: z.object({ id: vaultId }),
    body: VaultUpdate,
    responses: {
      200: Vault,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
  },
  archive: {
    method: 'POST',
    path: '/v1/vaults/:id/archive',
    pathParams: z.object({ id: vaultId }),
    body: z.object({}).strict(),
    responses: { 200: Vault, 404: ClaudeErrorResponse },
    headers: z.object({ 'idempotency-key': z.string().optional() }).passthrough(),
  },
  delete: {
    method: 'DELETE',
    path: '/v1/vaults/:id',
    pathParams: z.object({ id: vaultId }),
    body: z.object({}).strict(),
    responses: { 200: VaultDeleted, 404: ClaudeErrorResponse },
  },
  listCredentials: {
    method: 'GET',
    path: '/v1/vaults/:vault_id/credentials',
    pathParams: z.object({ vault_id: vaultId }),
    query: vaultPagination,
    responses: {
      200: z.object({ data: z.array(VaultCredential), next_page: z.string().nullable() }),
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
    headers: vaultCredentialHeaders,
  },
  createCredential: {
    method: 'POST',
    path: '/v1/vaults/:vault_id/credentials',
    pathParams: z.object({ vault_id: vaultId }),
    body: VaultCredentialCreate,
    responses: {
      200: VaultCredential,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
      503: ClaudeErrorResponse,
    },
    headers: vaultCredentialMutationHeaders,
  },
  getCredential: {
    method: 'GET',
    path: '/v1/vaults/:vault_id/credentials/:credential_id',
    pathParams: z.object({ vault_id: vaultId, credential_id: idString('vcrd') }),
    responses: { 200: VaultCredential, 404: ClaudeErrorResponse },
    headers: vaultCredentialHeaders,
  },
  updateCredential: {
    method: 'POST',
    path: '/v1/vaults/:vault_id/credentials/:credential_id',
    pathParams: z.object({ vault_id: vaultId, credential_id: idString('vcrd') }),
    body: VaultCredentialUpdate,
    responses: {
      200: VaultCredential,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      409: ClaudeErrorResponse,
      503: ClaudeErrorResponse,
    },
    headers: vaultCredentialMutationHeaders,
  },
  archiveCredential: {
    method: 'POST',
    path: '/v1/vaults/:vault_id/credentials/:credential_id/archive',
    pathParams: z.object({ vault_id: vaultId, credential_id: idString('vcrd') }),
    body: z.object({}).strict(),
    responses: { 200: VaultCredential, 404: ClaudeErrorResponse },
    headers: vaultCredentialMutationHeaders,
  },
  mcpOauthValidateCredential: {
    method: 'POST',
    path: '/v1/vaults/:vault_id/credentials/:credential_id/mcp_oauth_validate',
    pathParams: z.object({ vault_id: vaultId, credential_id: idString('vcrd') }),
    body: z.object({}).strict(),
    responses: {
      200: VaultCredentialValidation,
      400: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
      // Concurrent token rotation lost the compare-and-swap; safe to retry.
      409: ClaudeErrorResponse,
      503: ClaudeErrorResponse,
    },
    headers: vaultCredentialMutationHeaders,
  },
  deleteCredential: {
    method: 'DELETE',
    path: '/v1/vaults/:vault_id/credentials/:credential_id',
    pathParams: z.object({ vault_id: vaultId, credential_id: idString('vcrd') }),
    body: z.object({}).strict(),
    responses: { 200: VaultCredentialDeleted, 404: ClaudeErrorResponse },
    headers: vaultCredentialHeaders,
  },
});
