// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { AppRoute } from '@ts-rest/core';
import { generateOpenApi } from '@ts-rest/open-api';
import { adminAgentObservabilityContract } from './agent-observability.contract.js';
import { platformAgentObservabilityContract } from './platform-agent-observability.contract.js';
import { normalizeExclusiveBounds, normalizeNullableForOas30 } from './openapi-normalize.js';

/** Keep action colons literal: ts-rest otherwise turns :disable into {disable}. */
function protectActions(routes: Record<string, AppRoute>): Record<string, AppRoute> {
  return Object.fromEntries(
    Object.entries(routes).map(([name, route]) => [
      name,
      { ...route, path: route.path.replace(/(agent_observability):/g, '$1%3A') },
    ]),
  );
}

function markCredentialsWriteOnly(value: unknown): void {
  if (!value || typeof value !== 'object') return;
  const object = value as Record<string, unknown>;
  const properties = object.properties as Record<string, Record<string, unknown>> | undefined;
  if (properties?.credentials) properties.credentials.writeOnly = true;
  for (const child of Object.values(object)) markCredentialsWriteOnly(child);
}

/** Standalone management contract; never composed into the public OpenAPI. */
export function buildObservabilityOpenApiDocument(options: { version: string }) {
  const document = generateOpenApi(
    {
      organization: protectActions(adminAgentObservabilityContract),
      platform: protectActions(platformAgentObservabilityContract),
    },
    {
      info: {
        title: 'Observability administration in Orca Agent Engine',
        version: options.version,
        description:
          'Admin-listener-only operations. Organization and platform credentials are separate. ' +
          'Configuration acceptance does not establish exporter capability or remote delivery. ' +
          'Generated from standalone Registry contracts; not part of the public Managed Agents API.',
      },
      tags: ['Organization observability', 'Workspace observability', 'Platform observability'].map(
        (name) => ({ name }),
      ),
      servers: [
        {
          url: 'https://{admin-host}',
          variables: { 'admin-host': { default: 'admin.example.com' } },
          description: 'Registry admin listener origin, not the public Workspace endpoint',
        },
      ],
      components: {
        securitySchemes: {
          organizationApiKey: {
            type: 'apiKey',
            in: 'header',
            name: 'x-api-key',
            description:
              'Organization admin key (orca_admin_...). Not a Workspace or platform key.',
          },
          organizationOidc: {
            type: 'http',
            scheme: 'bearer',
            description:
              'Admin OIDC token with org:admin and an organization claim. Scoped OIDC roles are not supported.',
          },
          platformApiKey: {
            type: 'apiKey',
            in: 'header',
            name: 'x-api-key',
            description: 'Independent platform key (orca_platform_...) with platform:admin.',
          },
          platformOidc: {
            type: 'http',
            scheme: 'bearer',
            description:
              'Platform OIDC token with platform:admin from the configured platform issuer/audience.',
          },
        },
      },
    },
    {
      setOperationId: 'concatenated-path',
      operationMapper: (operation, route) => {
        const platform = route.path.startsWith('/v1/platform/');
        const workspace = route.path.includes('/workspaces/');
        const scope = platform
          ? 'platform:admin'
          : route.path.endsWith('/capture_ceiling')
            ? 'org:admin'
            : route.method === 'GET'
              ? 'observability:read'
              : route.path.endsWith('rotate_credentials')
                ? 'observability:rotate'
                : 'observability:write';
        operation.tags = [
          platform
            ? 'Platform observability'
            : workspace
              ? 'Workspace observability'
              : 'Organization observability',
        ];
        operation.summary = operation
          .operationId!.split('.')
          .pop()!
          .replace(/([a-z])([A-Z])/g, '$1 $2')
          .toLowerCase();
        operation.summary = operation.summary[0]!.toUpperCase() + operation.summary.slice(1);
        operation.description =
          'Admin listener only. Required API-key scope: ' +
          scope +
          '.' +
          (platform
            ? ' Platform credentials cannot call organization routes.'
            : ' Organization keys with org:admin also qualify; OIDC requires org:admin.') +
          ' See the observability administration guide for replacement, precondition and exporter restrictions.';
        operation.security = platform
          ? [{ platformApiKey: [] }, { platformOidc: [] }]
          : [{ organizationApiKey: [] }, { organizationOidc: [] }];
        if (operation.requestBody && !('$ref' in operation.requestBody)) {
          // Unlike public archive placeholders, emergency disable requires {}.
          operation.requestBody.required = true;
          markCredentialsWriteOnly(operation.requestBody);
        }
        for (const [status, response] of Object.entries(operation.responses)) {
          if ('$ref' in response) continue;
          response.headers = {
            'Cache-Control': {
              schema: { type: 'string' },
              description: 'No-store. Reads and platform responses are private, no-store.',
            },
          };
          if (status === '200' && (route.method === 'GET' || platform)) {
            response.headers.ETag = {
              schema: { type: 'string' },
              description:
                'Opaque strong entity tag. Scoped mutations require a fresh GET for the current ETag.',
            };
          }
        }
        return operation;
      },
    },
  );
  document.paths = Object.fromEntries(
    Object.entries(document.paths)
      .map(([path, item]) => [path.replace(/%3A/g, ':'), item])
      .sort(([a], [b]) => String(a).localeCompare(String(b))),
  );
  return normalizeNullableForOas30(normalizeExclusiveBounds(document));
}
