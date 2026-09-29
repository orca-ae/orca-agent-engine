// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { generateOpenApi } from '@ts-rest/open-api';
import type { AppRoute } from '@ts-rest/core';
import { publicContract } from './index.js';
import { normalizeExclusiveBounds, normalizeNullableForOas30 } from './openapi-normalize.js';
import { readOpenApiMedia, type MediaOverride } from './openapi-media.js';
import { DEFAULT_SECURITY, SECURITY_SCHEMES, readOpenApiSecurity } from './openapi-security.js';

type Operation = Record<string, unknown>;

/**
 * Apply the corrections that need the route in hand.
 *
 * **`requestBody.required`.** ts-rest emits every request body as optional.
 * None of them are: a route that declares a body rejects a request without one.
 * Left as generated, a client may omit the body and believe the spec allowed it.
 *
 * **Media overrides.** Routes that move bytes rather than JSON, or frames rather
 * than a document, declare their real media type in `metadata` — see
 * `openapi-media.ts` for why they cannot be derived from the zod schema.
 *
 * **Security overrides.** One route is authenticated differently from the rest;
 * see `openapi-security.ts`.
 */
function applyRouteOverrides(operation: Operation, route: AppRoute): Operation {
  const result: Operation = { ...operation };
  const media = readOpenApiMedia(route.metadata);
  const security = readOpenApiSecurity(route.metadata);
  if (security) result.security = security;

  const requestBody = result.requestBody as Record<string, unknown> | undefined;
  if (requestBody) {
    if (media?.requestBody) {
      result.requestBody = {
        ...requestBody,
        required: true,
        content: contentFor(media.requestBody, requestBody.content),
      };
    } else if (bodyIsEmptyPlaceholder(requestBody)) {
      // `z.object({}).strict()` on an archive or delete route is a placeholder
      // for "no body", not a body that happens to be empty. Publishing it as a
      // required `{}` gives generated clients a mandatory argument for a request
      // both sides send bodyless — and 19 of these overlap Anthropic operations
      // that declare no request body at all.
      delete result.requestBody;
    } else {
      result.requestBody = { ...requestBody, required: true };
    }
  }

  if (media?.responses) {
    const responses = { ...(result.responses as Record<string, unknown> | undefined) };
    for (const [status, override] of Object.entries(media.responses)) {
      const existing = (responses[status] ?? {}) as Record<string, unknown>;
      responses[status] = {
        ...existing,
        ...(override.description ? { description: override.description } : {}),
        content: contentFor(override, existing.content),
      };
    }
    result.responses = responses;
  }

  return result;
}

/**
 * Build the `content` map for an override, reusing the generated schema when the
 * override supplies none — see {@link MediaOverride.schema}.
 */
function contentFor(override: MediaOverride, generated: unknown): Record<string, unknown> {
  const schema = override.schema ?? generatedSchema(generated);
  if (!schema) {
    throw new Error(
      `media override for \`${override.contentType}\` supplies no schema and the route ` +
        'generated none to keep',
    );
  }
  return { [override.contentType]: { schema } };
}

/** The single schema ts-rest generated for a body, whatever media type it keyed it under. */
function generatedSchema(content: unknown): unknown {
  if (!content || typeof content !== 'object') return undefined;
  const entries = Object.values(content as Record<string, { schema?: unknown } | undefined>);
  return entries.length === 1 ? entries[0]?.schema : undefined;
}

/** An object schema with no properties and nothing required — i.e. "no body". */
function bodyIsEmptyPlaceholder(requestBody: Record<string, unknown>): boolean {
  const content = requestBody.content as Record<string, { schema?: unknown }> | undefined;
  const schema = content && (Object.values(content)[0]?.schema as Record<string, unknown>);
  if (!schema || schema.type !== 'object') return false;

  const properties = (schema.properties ?? {}) as Record<string, unknown>;
  const required = (schema.required ?? []) as unknown[];
  return Object.keys(properties).length === 0 && required.length === 0;
}

/**
 * Render {@link publicContract} as an OpenAPI document.
 *
 * Kept next to the contract rather than inside the generator script so the
 * document's shape — title, description, operationId scheme — is reviewed as
 * part of the API surface, and so a test can build the document without
 * touching the filesystem.
 *
 * Declares `security` / `securitySchemes` from `openapi-security.ts`, which
 * transcribes what `src/auth/` enforces. An omitted `security` is not a silence
 * — OpenAPI reads it as "no authentication required" — and this document is
 * published as the contract a client is generated from. The narrative behind
 * each scheme stays in `docs/managed-agents/auth-and-vaults.md`.
 */
export function buildOpenApiDocument(options: { version: string }) {
  const document = generateOpenApi(
    publicContract,
    {
      info: {
        title: 'Orca Managed Agents API',
        version: options.version,
        description: [
          'The public HTTP surface of the Orca Managed Agents registry.',
          '',
          'Generated from `src/contracts/*.contract.ts` by `pnpm openapi:gen`. Do not edit by',
          'hand — CI regenerates this document and fails on drift.',
          '',
          'This API is Anthropic-compatible, not Anthropic. `docs/managed-agents/conformance-matrix.md`',
          'records every operation-level difference from Anthropic’s published spec together with',
          'an explicit decision about it.',
        ].join('\n'),
      },
      components: { securitySchemes: SECURITY_SCHEMES },
      security: DEFAULT_SECURITY,
    },
    // `concatenated-path` yields `agents.create` rather than a bare `create`,
    // which would collide across resources the moment two routers share a key.
    {
      setOperationId: 'concatenated-path',
      operationMapper: (operation, route) =>
        applyRouteOverrides(operation as Operation, route) as typeof operation,
    },
  );
  // Both run last, over the whole document, because they have to reach schemas
  // nested anywhere — parameters, bodies, responses and every `$ref`-free
  // branch. Neither depends on the other; the order is only the order they were
  // written in.
  return normalizeNullableForOas30(normalizeExclusiveBounds(document));
}
