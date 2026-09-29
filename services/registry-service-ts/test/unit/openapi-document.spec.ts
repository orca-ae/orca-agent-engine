// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import SwaggerParser from '@apidevtools/swagger-parser';
import { load } from 'js-yaml';
import { describe, expect, it } from 'vitest';
import { normalizePath } from '../../scripts/lib/normalize-operation.mjs';

/**
 * The published document is the contract a regenerated client is built from.
 *
 * Every assertion here is written from a defect that shipped, not derived from
 * the generator's own output — a test that regenerates its expectations would
 * have passed happily on all of them. Each one describes something a client
 * author would have discovered the hard way:
 *
 *   - the file upload published `{ nullable: true }` as its entire body, so a
 *     generated SDK had no parameter to put the bytes in;
 *   - both download routes published nullable JSON instead of their real media
 *     types;
 *   - no request body was marked required, so clients could omit them and
 *     believe the spec allowed it;
 *   - `exclusiveMinimum` was emitted in 3.1's numeric form inside a 3.0.2
 *     document, which validators reject outright;
 *   - `effort` unions overlapped, so every valid value failed `oneOf`;
 *   - two resource paths differed only by parameter name, which OAS forbids.
 */

const document = load(
  readFileSync(new URL('../../openapi/managed-agents.yaml', import.meta.url), 'utf8'),
) as {
  openapi: string;
  paths: Record<string, Record<string, Operation>>;
  security?: unknown;
  components?: { securitySchemes?: unknown };
};

interface Operation {
  requestBody?: { required?: boolean; content?: Record<string, { schema?: unknown }> };
  responses?: Record<string, { content?: Record<string, { schema?: unknown }> }>;
  security?: unknown;
}

const HTTP_METHODS = ['get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace'];
const operations = () =>
  Object.entries(document.paths).flatMap(([path, item]) =>
    Object.entries(item)
      .filter(([method]) => HTTP_METHODS.includes(method))
      .map(([method, operation]) => ({ label: `${method.toUpperCase()} ${path}`, operation })),
  );

function walk(node: unknown, visit: (value: Record<string, unknown>) => void): void {
  if (Array.isArray(node)) {
    for (const item of node) walk(item, visit);
    return;
  }
  if (!node || typeof node !== 'object') return;
  visit(node as Record<string, unknown>);
  for (const value of Object.values(node)) walk(value, visit);
}

/** Keywords that describe a schema without ruling any value out. */
const ANNOTATIONS = new Set([
  'nullable',
  'description',
  'readOnly',
  'writeOnly',
  'deprecated',
  'example',
  'examples',
  'title',
  'default',
  'externalDocs',
  'xml',
  // Annotation-only in practice: validators ignore formats they do not know,
  // and this is the keyword that let `z.null()` masquerade as a bounded string.
  'format',
]);

const required = (schema: Record<string, unknown>): string[] =>
  Array.isArray(schema.required) ? (schema.required as string[]) : [];

const constrainsNothingBeyondType = (schema: Record<string, unknown>): boolean =>
  Object.keys(schema).every((key) => key === 'type' || ANNOTATIONS.has(key));

/** The literal values a schema pins, if it pins any — an enum or a const. */
function literals(schema: unknown): Set<string> | null {
  if (!schema || typeof schema !== 'object') return null;
  const node = schema as Record<string, unknown>;
  if (Array.isArray(node.enum)) return new Set(node.enum.map((value) => JSON.stringify(value)));
  if ('const' in node) return new Set([JSON.stringify(node.const)]);
  return null;
}

/** True when some shared property pins disjoint literals — a discriminator. */
function discriminatedApart(a: Record<string, unknown>, b: Record<string, unknown>): boolean {
  const aProps = (a.properties ?? {}) as Record<string, unknown>;
  const bProps = (b.properties ?? {}) as Record<string, unknown>;
  for (const key of Object.keys(aProps)) {
    if (!(key in bProps)) continue;
    const aValues = literals(aProps[key]);
    const bValues = literals(bProps[key]);
    if (aValues && bValues && ![...aValues].some((value) => bValues.has(value))) return true;
  }
  return false;
}

/** True when every object `b` accepts, `a` accepts too. */
function subsumesObject(a: Record<string, unknown>, b: Record<string, unknown>): boolean {
  if (a.type !== 'object' || b.type !== 'object') return false;
  if (a.additionalProperties === false) return false;
  return required(a).every((key) => required(b).includes(key));
}

describe('the published OpenAPI document', () => {
  it('is a valid document by its own declared version', async () => {
    // Mirrors the generator's gate. The generator refuses to write an invalid
    // document; this fails the far more frequently-run test suite too.
    await expect(SwaggerParser.validate(structuredClone(document) as never)).resolves.toBeDefined();
  });

  it('describes the file upload as a required binary part', () => {
    const body = document.paths['/v1/files']!.post!.requestBody!;
    const schema = body.content!['multipart/form-data']!.schema as {
      required?: string[];
      properties?: Record<string, { type?: string; format?: string }>;
    };

    expect(Object.keys(body.content!)).toEqual(['multipart/form-data']);
    expect(schema.required).toContain('file');
    expect(schema.properties?.file).toMatchObject({ type: 'string', format: 'binary' });
  });

  it.each([
    ['/v1/files/{id}/content', 'application/octet-stream'],
    ['/v1/skills/{id}/versions/{version}/content', 'application/zip'],
  ])('describes %s as returning %s', (path, mediaType) => {
    const content = document.paths[path]!.get!.responses!['200']!.content!;

    expect(Object.keys(content)).toEqual([mediaType]);
    expect(content[mediaType]!.schema).toMatchObject({ type: 'string', format: 'binary' });
  });

  it('marks every request body required', () => {
    const optional = operations()
      .filter(({ operation }) => operation.requestBody && operation.requestBody.required !== true)
      .map(({ label }) => label);

    expect(optional).toEqual([]);
  });

  it('states exclusive bounds in the form its own OpenAPI version defines', () => {
    expect(document.openapi).toMatch(/^3\.0\./);

    const numeric: string[] = [];
    walk(document.paths, (node) => {
      for (const keyword of ['exclusiveMinimum', 'exclusiveMaximum']) {
        if (typeof node[keyword] === 'number') numeric.push(`${keyword}: ${String(node[keyword])}`);
      }
    });

    expect(numeric).toEqual([]);
  });

  it('never lets two branches of a oneOf accept the same value', () => {
    // A `oneOf` demands exactly one match, so any pair of branches where one
    // accepts everything the other does makes every such value invalid.
    //
    // The earlier version of this test only recognised an enum branch beside an
    // unconstrained branch of the same type. Two live overlaps sat underneath
    // it: `ModelInput` published `{id}` and `{provider, id}` as separate
    // passthrough objects, and a metadata patch value published a bounded string
    // beside `z.null()`'s rendering, which is an *unbounded* string. Both are
    // caught by asking the general question instead — does one branch subsume
    // another — while skipping pairs a literal property tells apart, which is
    // how every discriminated union in the document distinguishes its members.
    const overlaps: string[] = [];
    walk(document.paths, (node) => {
      const branches = (node.oneOf as unknown[] | undefined)?.filter(
        (branch): branch is Record<string, unknown> => !!branch && typeof branch === 'object',
      );
      if (!branches) return;
      for (const branch of branches) {
        for (const other of branches) {
          if (branch === other || discriminatedApart(branch, other)) continue;
          if (subsumesObject(branch, other)) {
            overlaps.push(
              `object requiring [${required(branch).join(', ')}] also accepts everything ` +
                `requiring [${required(other).join(', ')}]`,
            );
          } else if (
            branch.type &&
            branch.type === other.type &&
            constrainsNothingBeyondType(branch) &&
            !constrainsNothingBeyondType(other)
          ) {
            overlaps.push(`unconstrained \`${String(branch.type)}\` beside a constrained one`);
          }
        }
      }
    });

    expect([...new Set(overlaps)]).toEqual([]);
  });

  it('never states `nullable` where OpenAPI 3.0 gives it no meaning', () => {
    // 3.0 defines `nullable` only as a modifier on a declared `type`. Without
    // one it is inert to a reader and a hard error to a validator — AJV refuses
    // to compile the schema at all. `openapi-instances.spec.ts` is the other
    // half of this: hoisting a `type` to satisfy the rule passes here and still
    // rejects `null`.
    const offenders: string[] = [];
    walk(document.paths, (node) => {
      if (node.nullable === true && node.type === undefined) {
        offenders.push(`{ ${Object.keys(node).join(', ')} }`);
      }
    });

    expect([...new Set(offenders)]).toEqual([]);
  });

  it.each(['/v1/sessions/{id}/events/stream', '/v1/sessions/{id}/threads/{thread_id}/stream'])(
    'describes %s as an event stream carrying the session event',
    (path) => {
      // These published `application/json` with `{ readOnly: true }` — `z.never()`
      // rendered, and a body no generated client could type — while the handler
      // wrote `text/event-stream`. Pinned per route because the same fix was once
      // applied to Files and missed on both Skill uploads.
      const content = document.paths[path]!.get!.responses!['200']!.content!;

      expect(Object.keys(content)).toEqual(['text/event-stream']);
      expect(content['text/event-stream']!.schema).toMatchObject({
        type: 'object',
        required: ['id', 'type'],
        properties: { id: { type: 'string' }, type: { type: 'string' } },
      });
    },
  );

  it('declares the authentication the public listener enforces', () => {
    expect(document.components?.securitySchemes).toMatchObject({
      apiKey: { type: 'apiKey', in: 'header', name: 'x-api-key' },
      oidcBearer: { type: 'http', scheme: 'bearer' },
      gitCredsJwt: { type: 'http', scheme: 'bearer' },
      gitProxyJwt: { type: 'http', scheme: 'bearer' },
    });
    // A list of requirements is an OR, matching `src/auth/auth.ts`: the api key
    // is authoritative when present, OIDC runs only when none was.
    expect(document.security).toEqual([{ apiKey: [] }, { oidcBearer: [] }]);
  });

  it('declares exactly the operation-level security overrides the public surface needs', () => {
    // An exact set, not an allowlist to append to. The probes clear the default
    // because the public auth hook exempts them; `/v1/git-creds` replaces it
    // with its handler-enforced JWT. A second override has to be argued for here
    // rather than absorbed silently, the same discipline
    // `route-contract-parity.spec.ts` applies to served-but-uncontracted routes.
    const exceptions = operations()
      .filter(({ operation }) => operation.security !== undefined)
      .map(({ label }) => label)
      .sort();

    expect(exceptions).toEqual([
      'GET /healthz',
      'GET /readyz',
      'GET /v1/git-proxy/{resourceId}/info/refs',
      'POST /v1/git-creds',
      'POST /v1/git-proxy/{resourceId}/git-upload-pack',
    ]);
    expect(document.paths['/healthz']!.get!.security).toEqual([]);
    expect(document.paths['/readyz']!.get!.security).toEqual([]);
    expect(document.paths['/v1/git-creds']!.post!.security).toEqual([{ gitCredsJwt: [] }]);
    expect(document.paths['/v1/git-proxy/{resourceId}/info/refs']!.get!.security).toEqual([
      { gitProxyJwt: [] },
    ]);
    expect(document.paths['/v1/git-proxy/{resourceId}/git-upload-pack']!.post!.security).toEqual([
      { gitProxyJwt: [] },
    ]);
  });

  it('declares no two paths that OpenAPI would treat as one', () => {
    // Path templates differing only in parameter names are the same path to OAS,
    // and declaring both is forbidden.
    const byTemplate = new Map<string, string>();
    const collisions: string[] = [];
    for (const path of Object.keys(document.paths)) {
      const normalized = normalizePath(path);
      const previous = byTemplate.get(normalized);
      if (previous) collisions.push(`${previous} vs ${path}`);
      else byTemplate.set(normalized, path);
    }

    expect(collisions).toEqual([]);
  });
});
