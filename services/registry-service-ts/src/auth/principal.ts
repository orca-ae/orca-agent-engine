// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export interface AuthenticatedPrincipal {
  workspaceId: string;
  principal: string;
  scopes: string[];
  authMethod: 'api-key' | 'oidc';
  /** Stable public identifier of the credential that authenticated the request. */
  apiKeyId?: string;
  /** Raw verified standard OIDC subject used for public `user_actor` attribution. */
  userId?: string;
  /** Successfully verified OIDC issuer paired with `userId`; internal use only. */
  oidcIssuer?: string;
}

export interface AdminPrincipal {
  organizationId: string;
  principal: string;
  scopes: string[];
  authMethod: 'admin-api-key' | 'oidc';
}

export interface PlatformPrincipal {
  principal: string;
  scopes: string[];
  authMethod: 'platform-api-key' | 'oidc';
}

export interface InternalServicePrincipal {
  caller: 'shared' | 'harness' | 'ai-gateway' | 'observability-exporter';
  subject: string;
}

declare module 'fastify' {
  interface FastifyRequest {
    auth?: AuthenticatedPrincipal;
    adminAuth?: AdminPrincipal;
    platformAuth?: PlatformPrincipal;
    internalAuth?: InternalServicePrincipal;
  }
}
