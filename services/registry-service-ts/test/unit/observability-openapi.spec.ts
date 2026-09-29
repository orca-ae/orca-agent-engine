// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import SwaggerParser from '@apidevtools/swagger-parser';
import { load } from 'js-yaml';
import { describe, expect, it } from 'vitest';
import { buildObservabilityOpenApiDocument } from '../../src/contracts/observability-openapi-schema.js';

interface PublishedOperation {
  operationId?: string;
  parameters?: { in?: string; name?: string }[];
  security?: unknown;
  requestBody?: unknown;
}

describe('observability admin OpenAPI', () => {
  const document = buildObservabilityOpenApiDocument({ version: 'test' });
  const operations = Object.entries(document.paths).flatMap(([path, item]) =>
    Object.entries(item).map(([method, operation]) => ({
      path,
      method,
      operation: operation as PublishedOperation,
    })),
  );

  it('publishes exactly the ten admin operations, without public or internal routes', () => {
    expect(operations.map(({ path, method }) => `${method.toUpperCase()} ${path}`).sort()).toEqual(
      [
        'GET /v1/organizations/agent_observability',
        'GET /v1/organizations/workspaces/{workspaceId}/agent_observability',
        'GET /v1/platform/agent_observability',
        'POST /v1/organizations/agent_observability:disable',
        'POST /v1/organizations/agent_observability:rotate_credentials',
        'POST /v1/organizations/workspaces/{workspaceId}/agent_observability:rotate_credentials',
        'PUT /v1/organizations/agent_observability',
        'PUT /v1/organizations/agent_observability/capture_ceiling',
        'PUT /v1/organizations/workspaces/{workspaceId}/agent_observability',
        'PUT /v1/platform/agent_observability',
      ].sort(),
    );
    expect(new Set(operations.map(({ operation }) => operation.operationId)).size).toBe(10);
  });

  it('preserves literal actions instead of inventing path parameters', () => {
    for (const { path, operation } of operations) {
      expect(
        operation.parameters
          ?.filter((parameter) => 'in' in parameter && parameter.in === 'path')
          .map((parameter) => 'name' in parameter && parameter.name),
      ).toEqual(path.includes('{workspaceId}') ? ['workspaceId'] : []);
    }
  });

  it('requires mutation bodies, including the empty emergency-disable object', () => {
    for (const { method, operation } of operations) {
      if (method !== 'get') expect(operation.requestBody).toMatchObject({ required: true });
    }
    const headers = (path: string) => document.paths[path]!.put!.parameters;
    expect(headers('/v1/platform/agent_observability')).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: 'if-match', in: 'header', required: true }),
        expect.objectContaining({ name: 'idempotency-key', in: 'header', required: true }),
      ]),
    );
  });

  it('keeps platform and organization credentials separate and marks secrets write-only', () => {
    for (const { path, operation } of operations) {
      expect(operation.security).toEqual(
        path.startsWith('/v1/platform/')
          ? [{ platformApiKey: [] }, { platformOidc: [] }]
          : [{ organizationApiKey: [] }, { organizationOidc: [] }],
      );
    }
    const body = document.paths['/v1/organizations/agent_observability']!.put!.requestBody;
    expect(body).toMatchObject({
      content: {
        'application/json': {
          schema: {
            properties: { credentials: { writeOnly: true } },
          },
        },
      },
    });
    expect(document.paths['/v1/platform/agent_observability']!.put!.responses['200']).toMatchObject(
      { headers: { ETag: { schema: { type: 'string' } } } },
    );
  });

  it('is a valid standalone OpenAPI document', async () => {
    await expect(
      SwaggerParser.validate(JSON.parse(JSON.stringify(document))),
    ).resolves.toBeDefined();
  });

  it('keeps the checked-in artifact in sync, including tags required by docs generation', () => {
    const published = load(
      readFileSync(new URL('../../openapi/observability-admin.yaml', import.meta.url), 'utf8'),
    ) as ReturnType<typeof buildObservabilityOpenApiDocument> & { 'x-orca-source'?: unknown };
    delete published['x-orca-source'];
    published.info.version = 'test';
    expect(published).toEqual(document);
    expect(published.tags?.map((tag) => tag.name)).toEqual([
      'Organization observability',
      'Workspace observability',
      'Platform observability',
    ]);
  });
});
