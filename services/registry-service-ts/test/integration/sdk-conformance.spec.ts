// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import type { AddressInfo } from 'node:net';
import type { FastifyInstance } from 'fastify';
import { load } from 'js-yaml';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  HTTP_METHODS,
  isBetaPathKey,
  operationKey,
} from '../../scripts/lib/normalize-operation.mjs';
import { buildCombinedTestApp } from '../../src/server.js';
import { createTestApiKey, uniqueWorkspace } from './fixtures.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import {
  createResponseSchemaCompiler,
  describeSchemaErrors,
} from '../helpers/openapi-response-validation.js';

/**
 * The real `@anthropic-ai/sdk` against a real listener, in both dialects, with
 * every default response checked directionally against Anthropic's vendored
 * response schema, and every `orca-beta` or Orca-only response checked against
 * `openapi/managed-agents.yaml` for the operation the request actually routed to.
 *
 * ## What this proves, and what it does not
 *
 * **The default target is independent.** One side is the status and bytes a live
 * Fastify listener put on the wire, driven by a client this repository did not
 * write. The other is the response schema in Anthropic's pinned OpenAPI document.
 * Orca's generated spec remains the target for `orca-beta` and Orca-only
 * operations, where Anthropic has no contract for the behavior being exercised.
 *
 * **Both dialects, because one would hide most of it.** Every operation runs
 * twice, once plain and once with `orca-beta`. The suite originally found most
 * response divergences on the `orca-beta` path, so a default-only run would not
 * exercise the fixes or prevent them from regressing.
 *
 * **The SDK is a test client, not the standard.** The vendored Anthropic spec is
 * the sole definition of Anthropic-compatible; `0.113.0` is an implementation
 * detail of this harness. An operation the SDK has no method for is a limit of
 * the harness, never a finding about the server, and never a reason to bump it.
 *
 * **Coverage is measured, not claimed.** Which operation a call exercised comes
 * from Fastify's own router (`req.routeOptions.url`), recorded by a hook — not
 * from a table mapping SDK methods to paths, which would be a second guess
 * rather than a second source. {@link DRIVEN_OPERATIONS} pins the result, so
 * coverage cannot shrink without saying so.
 *
 * ## Reach — read this before reading a green run as "spec and wire agree"
 *
 * **44 of the 72 `core` operations, once per dialect.** Not rows, not
 * completeness: 44 operations. The 28 not driven are `/v1/files/*` and
 * `/v1/memory_stores/*` (both need S3/MinIO, which this spec deliberately does
 * not require), `/v1/vaults/{id}/credentials/*` (needs a configured
 * `SecretStore`), and the two SSE stream operations, whose response never
 * completes and so has no status for the hook to record.
 *
 * **Full response schemas, directionally.** The schema check resolves component
 * refs and composition and enforces required fields, types, literals, and
 * nullability at every depth. Default responses may contain additive Orca
 * fields, but every field Anthropic defines must conform. `orca-beta` and
 * Orca-only responses are checked exactly against Orca's published schema. The
 * older key-presence assertions remain as focused diagnostics and counterweights
 * for the five omissions this suite originally found.
 */

const orcaSpec = load(
  readFileSync(new URL('../../openapi/managed-agents.yaml', import.meta.url), 'utf8'),
) as SpecDocument;
const anthropicSpec = JSON.parse(
  readFileSync(new URL('../../vendor/anthropic/openapi.json', import.meta.url), 'utf8'),
) as SpecDocument;

interface SpecSchema extends Record<string, unknown> {
  required?: string[];
  properties?: Record<string, SpecSchema>;
  items?: SpecSchema;
}

interface SpecResponse {
  content?: Record<string, { schema?: SpecSchema }>;
}

interface SpecOperation {
  responses?: Record<string, SpecResponse>;
}

interface SpecDocument {
  paths: Record<string, Record<string, unknown>>;
  components?: { schemas?: Record<string, unknown> };
}

/** `(method, normalized path)` → what our own published spec declares for it. */
const declared = new Map<
  string,
  {
    successCodes: string[];
    requiredKeys: Map<string, string[]>;
    requiredSessionAgentKeys: Map<string, string[]>;
  }
>();
for (const [pathKey, pathItem] of Object.entries(orcaSpec.paths)) {
  for (const method of HTTP_METHODS) {
    const operation = pathItem[method] as SpecOperation | undefined;
    if (!operation) continue;
    const codes = Object.keys(operation.responses ?? {});
    declared.set(operationKey(method, pathKey), {
      successCodes: codes.filter((code) => /^2\d\d$/.test(code)),
      requiredKeys: new Map(
        codes.map((code) => [
          code,
          operation.responses?.[code]?.content?.['application/json']?.schema?.required ?? [],
        ]),
      ),
      requiredSessionAgentKeys: new Map(
        codes.map((code) => {
          const schema = operation.responses?.[code]?.content?.['application/json']?.schema;
          const directAgent = schema?.properties?.agent;
          const listedAgent = schema?.properties?.data?.items?.properties?.agent;
          return [code, directAgent?.required ?? listedAgent?.required ?? []];
        }),
      ),
    });
  }
}

interface ResponseContract {
  beta: boolean;
  responses: Record<string, SpecResponse>;
}

function collectResponseContracts(spec: SpecDocument): Map<string, ResponseContract> {
  const contracts = new Map<string, ResponseContract>();
  for (const [pathKey, pathItem] of Object.entries(spec.paths)) {
    if ('$ref' in pathItem) {
      throw new Error(`response contract collector does not support path-item $ref at ${pathKey}`);
    }
    for (const method of HTTP_METHODS) {
      const operation = pathItem[method] as SpecOperation | undefined;
      if (!operation) continue;
      const key = operationKey(method, pathKey);
      const next = { beta: isBetaPathKey(pathKey), responses: operation.responses ?? {} };
      const existing = contracts.get(key);
      if (!existing || (next.beta && !existing.beta)) contracts.set(key, next);
      else if (next.beta === existing.beta) {
        throw new Error(`multiple response contracts collapse to ${key}`);
      }
    }
  }
  return contracts;
}

const orcaContracts = collectResponseContracts(orcaSpec);
const anthropicContracts = collectResponseContracts(anthropicSpec);
const compileOrcaResponse = createResponseSchemaCompiler(orcaSpec, {
  allowAdditionalProperties: false,
});
const compileAnthropicResponse = createResponseSchemaCompiler(anthropicSpec, {
  allowAdditionalProperties: true,
});
const orcaValidators = new Map<unknown, ReturnType<typeof compileOrcaResponse>>();
const anthropicValidators = new Map<unknown, ReturnType<typeof compileAnthropicResponse>>();

type Dialect = 'default' | 'orca-beta';

/** One response as the listener produced it. */
interface Observation {
  key: string;
  operation: string;
  dialect: Dialect;
  status: number;
  mediaType: string | null;
  responseBody: unknown;
  responseParseError: string | null;
  /** Top-level keys of the JSON body, or `null` when the body was not a JSON object. */
  bodyKeys: string[] | null;
  /** Keys of every Session `agent` snapshot found in a direct or list response. */
  sessionAgentKeySets: string[][];
}

const observations: Observation[] = [];
const callFailures: string[] = [];

function validateResponseObservations(
  selected: Observation[],
  contracts: Map<string, ResponseContract>,
  compile: typeof compileOrcaResponse,
  validators: Map<unknown, ReturnType<typeof compileOrcaResponse>>,
): string[] {
  const problems: string[] = [];
  for (const observation of selected) {
    if (observation.responseParseError) {
      problems.push(`${describe_(observation)}: ${observation.responseParseError}`);
      continue;
    }
    const contract = contracts.get(observation.key);
    if (!contract) {
      problems.push(`${describe_(observation)}: target spec has no operation`);
      continue;
    }
    const response =
      contract.responses[String(observation.status)] ??
      contract.responses[`${Math.floor(observation.status / 100)}XX`] ??
      contract.responses.default;
    if (!response) {
      problems.push(`${describe_(observation)}: target spec has no response for this status`);
      continue;
    }
    const content = response.content ?? {};
    if (Object.keys(content).length === 0) {
      if (
        observation.responseBody !== undefined &&
        observation.responseBody !== null &&
        observation.responseBody !== ''
      ) {
        problems.push(`${describe_(observation)}: target declares no response body`);
      }
      continue;
    }
    const mediaType = observation.mediaType;
    const media = mediaType ? content[mediaType] : undefined;
    if (!media) {
      problems.push(
        `${describe_(observation)}: target declares ${Object.keys(content).join(', ')}, got ${mediaType ?? 'no content-type'}`,
      );
      continue;
    }
    if (media.schema === undefined) {
      problems.push(`${describe_(observation)}: target media type has no schema`);
      continue;
    }

    try {
      let validate = validators.get(media.schema);
      if (!validate) {
        validate = compile(media.schema);
        validators.set(media.schema, validate);
      }
      if (!validate(observation.responseBody)) {
        problems.push(`${describe_(observation)}: ${describeSchemaErrors(validate.errors)}`);
      }
    } catch (error) {
      problems.push(`${describe_(observation)}: cannot compile target schema: ${String(error)}`);
    }
  }
  return problems;
}

/**
 * The operations this suite drives, pinned.
 *
 * Two-sided on purpose. An operation that quietly stops being exercised — a call
 * that starts failing and gets deleted, an SDK method that moves — fails here
 * rather than shrinking the suite's reach in silence. Adding one fails too,
 * which costs a line and buys the guarantee that this list is what ran.
 */
const DRIVEN_OPERATIONS = [
  'DELETE /v1/environments/{1}',
  'DELETE /v1/sessions/{1}',
  'DELETE /v1/sessions/{1}/resources/{2}',
  'DELETE /v1/skills/{1}',
  'DELETE /v1/skills/{1}/versions/{2}',
  'DELETE /v1/vaults/{1}',
  'GET /v1/agents',
  'GET /v1/agents/{1}',
  'GET /v1/agents/{1}/versions',
  'GET /v1/environments',
  'GET /v1/environments/{1}',
  'GET /v1/sessions',
  'GET /v1/sessions/{1}',
  'GET /v1/sessions/{1}/events',
  'GET /v1/sessions/{1}/resources',
  'GET /v1/sessions/{1}/resources/{2}',
  'GET /v1/sessions/{1}/threads',
  'GET /v1/sessions/{1}/threads/{2}',
  'GET /v1/sessions/{1}/threads/{2}/events',
  'GET /v1/skills',
  'GET /v1/skills/{1}',
  'GET /v1/skills/{1}/versions',
  'GET /v1/skills/{1}/versions/{2}',
  'GET /v1/skills/{1}/versions/{2}/content',
  'GET /v1/vaults',
  'GET /v1/vaults/{1}',
  'POST /v1/agents',
  'POST /v1/agents/{1}',
  'POST /v1/agents/{1}/archive',
  'POST /v1/environments',
  'POST /v1/environments/{1}',
  'POST /v1/environments/{1}/archive',
  'POST /v1/sessions',
  'POST /v1/sessions/{1}',
  'POST /v1/sessions/{1}/archive',
  'POST /v1/sessions/{1}/events',
  'POST /v1/sessions/{1}/resources',
  'POST /v1/sessions/{1}/resources/{2}',
  'POST /v1/sessions/{1}/threads/{2}/archive',
  'POST /v1/skills',
  'POST /v1/skills/{1}/versions',
  'POST /v1/vaults',
  'POST /v1/vaults/{1}',
  'POST /v1/vaults/{1}/archive',
];

const SKILL_MD = [
  '---',
  'name: confskill',
  'description: A skill used only to exercise the skills operations',
  '---',
  '',
  'Body.',
].join('\n');

describe('SDK conformance in both dialects (integration)', () => {
  let app: FastifyInstance;

  beforeAll(async () => {
    const { db } = await getTestDb();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      // `get` answers so `POST /v1/sessions/{id}/resources` can attach a file
      // without an S3 round-trip. The file *store* is not what this suite is
      // about; the HTTP contract of the resource operations is.
      fileStore: {
        ...buildStubFileStore(),
        async get(workspaceId: string, fileId: string) {
          return {
            id: fileId,
            workspaceId,
            filename: 'conformance.txt',
            mimeType: 'text/plain',
            sizeBytes: 1,
            sha256: 'a'.repeat(64),
            metadata: {},
            purpose: 'agent',
            scopeId: null,
            createdAt: new Date(),
            updatedAt: new Date(),
            archivedAt: null,
          };
        },
      } as never,
    });

    // The seam that makes coverage measured rather than asserted. Fastify says
    // which route matched, so a call that quietly hit a different operation than
    // its author intended is recorded as the operation it actually hit. `onSend`
    // rather than `onResponse` because the body is only available here.
    // Callback form, and it does not hand the payload back: returning a payload
    // from `onSend` — even the identical one — makes Fastify re-derive the
    // headers it has already written. This hook observes; it must not be able to
    // change what the client receives, or the suite would be testing a listener
    // nobody runs.
    app.addHook('onSend', (req, reply, payload, done) => {
      const url = req.routeOptions.url;
      if (url) {
        let bodyKeys: string[] | null = null;
        const sessionAgentKeySets: string[][] = [];
        const contentType = reply.getHeader('content-type')?.toString() ?? '';
        const mediaType = contentType.split(';', 1)[0]?.trim().toLowerCase() || null;
        let responseBody: unknown = payload;
        let responseParseError: string | null = null;
        if (mediaType === 'application/json' || mediaType?.endsWith('+json')) {
          const encoded =
            typeof payload === 'string'
              ? payload
              : Buffer.isBuffer(payload)
                ? payload.toString('utf8')
                : null;
          try {
            if (encoded === null) throw new Error(`JSON payload has type ${typeof payload}`);
            responseBody = JSON.parse(encoded);
            if (responseBody && typeof responseBody === 'object' && !Array.isArray(responseBody)) {
              bodyKeys = Object.keys(responseBody);
              const agent = (responseBody as Record<string, unknown>).agent;
              if (agent && typeof agent === 'object' && !Array.isArray(agent)) {
                sessionAgentKeySets.push(Object.keys(agent));
              }
              const data = (responseBody as Record<string, unknown>).data;
              if (Array.isArray(data)) {
                for (const item of data) {
                  if (!item || typeof item !== 'object' || Array.isArray(item)) continue;
                  const listedAgent = (item as Record<string, unknown>).agent;
                  if (
                    listedAgent &&
                    typeof listedAgent === 'object' &&
                    !Array.isArray(listedAgent)
                  ) {
                    sessionAgentKeySets.push(Object.keys(listedAgent));
                  }
                }
              }
            }
          } catch (error) {
            bodyKeys = null;
            responseParseError = String(error);
          }
        } else if (Buffer.isBuffer(payload)) {
          // OpenAPI models binary response bytes as a string. Preserve every
          // byte while presenting the value in that JSON Schema type.
          responseBody = payload.toString('latin1');
        }
        observations.push({
          key: operationKey(req.method, url),
          operation: `${req.method} ${url}`,
          dialect: req.headers['orca-beta'] ? 'orca-beta' : 'default',
          status: reply.statusCode,
          mediaType,
          responseBody,
          responseParseError,
          bodyKeys,
          sessionAgentKeySets,
        });
      }
      done();
    });

    await app.listen({ host: '127.0.0.1', port: 0 });
    const baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;

    for (const dialect of ['default', 'orca-beta'] as const) {
      const workspace = uniqueWorkspace(dialect === 'default' ? 'confplain' : 'confbeta');
      await driveEveryOperation(baseURL, await createTestApiKey(db, workspace), dialect);
    }
  }, 180000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestDb();
  });

  it('completed every call it made', () => {
    // A call that threw is a call that did not exercise its operation. Reported
    // before anything else so a cascade of downstream failures is read as one
    // broken call rather than a wall of conformance findings.
    expect(callFailures).toEqual([]);
  });

  it('drove every operation it claims to, in both dialects', () => {
    for (const dialect of ['default', 'orca-beta'] as const) {
      const driven = [
        ...new Set(observations.filter((o) => o.dialect === dialect).map((o) => o.key)),
      ].sort();
      expect(driven, dialect).toEqual(DRIVEN_OPERATIONS);
    }
  });

  it('routed every call to an operation the published spec declares', () => {
    // Guards the join. If `req.routeOptions.url` stopped lining up with the
    // spec's path keys, every assertion below would look up an empty declaration
    // and pass by vacuum.
    expect(observations.filter((o) => !declared.has(o.key)).map((o) => o.operation)).toEqual([]);
  });

  it('answered a success status on every call, so nothing passed by failing', () => {
    // Without this the suite is theatre: `404` and `400` are *declared* statuses
    // on nearly every operation, so a call that never reached the code it meant
    // to exercise would satisfy everything below.
    expect(observations.filter((o) => o.status < 200 || o.status >= 300).map(describe_)).toEqual(
      [],
    );
  });

  it('answered a status the spec declares for that operation', () => {
    // The SDK does not throw on an unexpected 2xx, so a test that only checked
    // the call succeeded would go green while the wire and the spec disagreed.
    //
    // This used to report `DELETE …/resources/:resource_id` returning an
    // Orca-only 204. The route and contract now expose the same Anthropic 200
    // tombstone in both dialects. Any undeclared status fails here.
    const undeclared = observations.filter(
      (o) => !declared.get(o.key)!.successCodes.includes(String(o.status)),
    );
    expect([...new Set(undeclared.map(describe_))].sort()).toEqual([]);
  });

  it('answered 200 from every create, as Anthropic does', () => {
    // The counterweight to the empty list above, and the wire-side check on the
    // five create operations this suite reaches. Read off the observations
    // rather than the spec: the spec is what the assertion above already
    // compares against, so re-deriving from it would be one source, not two.
    const creates = [
      'POST /v1/agents',
      'POST /v1/environments',
      'POST /v1/sessions',
      'POST /v1/sessions/{1}/resources',
      'POST /v1/vaults',
    ];
    const statuses = observations
      .filter((o) => creates.includes(o.key))
      .map((o) => `${describe_(o)}`);
    expect(statuses.length).toBeGreaterThanOrEqual(creates.length * 2);
    expect(statuses.filter((s) => !s.endsWith('→ 200'))).toEqual([]);
  });

  it('matched every default core response against Anthropic schemas directionally', () => {
    const selected = observations.filter(
      (observation) => observation.dialect === 'default' && anthropicContracts.has(observation.key),
    );

    expect(selected.length).toBeGreaterThanOrEqual(DRIVEN_OPERATIONS.length);
    expect(
      validateResponseObservations(
        selected,
        anthropicContracts,
        compileAnthropicResponse,
        anthropicValidators,
      ),
    ).toEqual([]);
  });

  it('matched every orca-beta and Orca-only response against the published Orca schema', () => {
    const selected = observations.filter(
      (observation) =>
        observation.dialect === 'orca-beta' || !anthropicContracts.has(observation.key),
    );

    expect(selected.length).toBeGreaterThanOrEqual(DRIVEN_OPERATIONS.length);
    expect(
      validateResponseObservations(selected, orcaContracts, compileOrcaResponse, orcaValidators),
    ).toEqual([]);
  });

  it('returned every top-level response key the spec marks required', () => {
    // Top-level keys only. A nested object can be arbitrarily wrong and pass
    // here; so can a key present with the wrong type or an unexpected null.
    // This checks presence, and nothing else.
    //
    // It used to report five findings, every one on the `orca-beta` path — the
    // reason this suite runs both dialects rather than only the default one.
    // They were also precisely what the status scan structurally cannot see:
    // they branched inside `loadSession(…, orcaBeta)` and the list-response
    // mapper, not at a `reply.code` call.
    //
    // All five are fixed rather than declared away: the schema marked `type` and
    // `prev_page` required and a generated client trusts that, so the wire is
    // what was wrong. Empty in both directions — a new omission fails here, and
    // so does a schema that stopped requiring a key to make one go away.
    const missing = observations.flatMap((o) => {
      if (o.bodyKeys === null) return [];
      const required = declared.get(o.key)!.requiredKeys.get(String(o.status)) ?? [];
      const absent = required.filter((key) => !o.bodyKeys!.includes(key));
      return absent.length ? [`${describe_(o)} omits ${absent.join(', ')}`] : [];
    });
    expect([...new Set(missing)].sort()).toEqual([]);
  });

  it('still requires `type` and `prev_page` where the five findings were', () => {
    // The counterweight. An empty `missing` list is satisfied just as well by a
    // spec that requires nothing, which is the cheap way to make the assertion
    // above go green. This names the five keys the fix was about and checks the
    // spec still asks for them, so the check above cannot pass by vacuum.
    const requiredAt = (key: string, code: string) =>
      declared.get(key)?.requiredKeys.get(code) ?? [];
    expect(requiredAt('POST /v1/sessions', '200')).toContain('type');
    expect(requiredAt('GET /v1/sessions/{1}', '200')).toContain('type');
    expect(requiredAt('POST /v1/sessions/{1}', '200')).toContain('type');
    expect(requiredAt('POST /v1/sessions/{1}/archive', '200')).toContain('type');
    expect(requiredAt('GET /v1/sessions', '200')).toContain('prev_page');
  });

  it('returned every nested Session agent key the spec marks required', () => {
    const missing = observations.flatMap((o) => {
      const required = declared.get(o.key)!.requiredSessionAgentKeys.get(String(o.status)) ?? [];
      if (required.length === 0) return [];
      if (o.sessionAgentKeySets.length === 0) return [`${describe_(o)} has no object agent`];
      return o.sessionAgentKeySets.flatMap((keys) => {
        const absent = required.filter((key) => !keys.includes(key));
        return absent.length ? [`${describe_(o)} agent omits ${absent.join(', ')}`] : [];
      });
    });
    expect([...new Set(missing)].sort()).toEqual([]);
  });

  it('still requires the complete Agent snapshot inside a Session', () => {
    const required = declared.get('POST /v1/sessions')?.requiredSessionAgentKeys.get('200') ?? [];
    expect([...required].sort()).toEqual(
      [
        'description',
        'id',
        'mcp_servers',
        'model',
        'multiagent',
        'name',
        'skills',
        'system',
        'tools',
        'type',
        'version',
      ].sort(),
    );
  });

  it('checked a response body against a non-empty required list often enough to mean something', () => {
    // Reach, measured. `required` is empty for some operations, and for those
    // the assertion above is vacuous — it would pass on any body at all. This
    // records how many observations were actually constrained, so a regression
    // that emptied every `required` list shows up as this number collapsing
    // rather than as a suite that still reports success.
    //
    // 79 of the 90 observations today. The floor leaves room for an operation
    // or two to change shape without a spurious failure, and none for the
    // check quietly ceasing to apply.
    const constrained = observations.filter(
      (o) =>
        o.bodyKeys !== null &&
        (declared.get(o.key)!.requiredKeys.get(String(o.status)) ?? []).length > 0,
    );
    expect(constrained.length).toBeGreaterThanOrEqual(70);
  });
});

const describe_ = (o: Observation) => `${o.operation} [${o.dialect}] → ${o.status}`;

/**
 * Drive every operation in {@link DRIVEN_OPERATIONS} once, in one dialect,
 * against a workspace of its own.
 *
 * Ordering is dictated by the resources: an agent and an environment before a
 * session, a session before its resources and events, every skill version
 * deleted before its skill. Each call is recorded rather than thrown so one
 * broken call reports as one broken call.
 */
async function driveEveryOperation(
  baseURL: string,
  apiKey: string,
  dialect: Dialect,
): Promise<void> {
  const sdk = await import('@anthropic-ai/sdk');
  const client = new sdk.default({
    baseURL,
    apiKey,
    // One SDK call must be one HTTP request, or an observation cannot be
    // attributed to the call that produced it.
    maxRetries: 0,
    ...(dialect === 'orca-beta' ? { defaultHeaders: { 'orca-beta': 'true' } } : {}),
  });
  const beta = client.beta;

  const call = async <T>(label: string, fn: () => Promise<T>): Promise<T | null> => {
    try {
      return await fn();
    } catch (error) {
      callFailures.push(`[${dialect}] ${label}: ${String(error).slice(0, 300)}`);
      return null;
    }
  };
  const skillFile = () =>
    sdk.toFile(Buffer.from(SKILL_MD), 'confskill/SKILL.md', { type: 'text/markdown' });
  const id = (value: unknown) => (value as { id?: string } | null)?.id as string;

  const agent = await call('agents.create', () =>
    beta.agents.create({
      name: `conf-${dialect}-${Date.now()}`,
      model: 'claude-sonnet-4-6',
      tools: [],
    }),
  );
  const agentId = id(agent);
  await call('agents.retrieve', () => beta.agents.retrieve(agentId));
  await call('agents.list', () => beta.agents.list());
  await call('agents.update', () => beta.agents.update(agentId, { description: 'updated' }));
  await call('agents.versions.list', () => beta.agents.versions.list(agentId));

  const environment = await call('environments.create', () =>
    beta.environments.create({
      name: `conf-${dialect}-${Date.now()}`,
      config: { type: 'cloud' },
    } as never),
  );
  const environmentId = id(environment);
  await call('environments.retrieve', () => beta.environments.retrieve(environmentId));
  await call('environments.list', () => beta.environments.list());
  await call('environments.update', () =>
    beta.environments.update(environmentId, { description: 'updated' } as never),
  );

  const session = await call('sessions.create', () =>
    beta.sessions.create({ agent_id: agentId, environment_id: environmentId } as never),
  );
  const sessionId = id(session);
  await call('sessions.retrieve', () => beta.sessions.retrieve(sessionId));
  await call('sessions.list', () => beta.sessions.list());
  await call('sessions.update', () =>
    beta.sessions.update(sessionId, { title: 'conformance' } as never),
  );

  const resource = await call('sessions.resources.add', () =>
    beta.sessions.resources.add(sessionId, {
      type: 'file',
      file_id: 'file_conformance01',
    } as never),
  );
  await call('sessions.resources.list', () => beta.sessions.resources.list(sessionId));
  const resourceId = id(resource);
  await call('sessions.resources.retrieve', () =>
    beta.sessions.resources.retrieve(resourceId, { session_id: sessionId } as never),
  );
  await call('sessions.resources.update', () =>
    beta.sessions.resources.update(resourceId, {
      session_id: sessionId,
      instructions: 'updated',
    } as never),
  );
  await call('sessions.resources.delete', () =>
    beta.sessions.resources.delete(resourceId, { session_id: sessionId } as never),
  );

  await call('sessions.events.send', () =>
    beta.sessions.events.send(sessionId, {
      events: [{ type: 'user.message', content: [{ type: 'text', text: 'hello' }] }],
    } as never),
  );
  await call('sessions.events.list', () => beta.sessions.events.list(sessionId));

  const threads = await call('sessions.threads.list', () => beta.sessions.threads.list(sessionId));
  const threadId = (threads as { data?: { id: string }[] } | null)?.data?.[0]?.id as string;
  await call('sessions.threads.retrieve', () =>
    beta.sessions.threads.retrieve(threadId, { session_id: sessionId } as never),
  );
  await call('sessions.threads.events.list', () =>
    beta.sessions.threads.events.list(threadId, { session_id: sessionId } as never),
  );
  await call('sessions.threads.archive', () =>
    beta.sessions.threads.archive(threadId, { session_id: sessionId } as never),
  );

  const skill = await call('skills.create', async () =>
    beta.skills.create({ display_title: `conf-${dialect}`, files: [await skillFile()] } as never),
  );
  const skillId = id(skill);
  const firstVersion = (skill as { latest_version?: string } | null)?.latest_version as string;
  await call('skills.retrieve', () => beta.skills.retrieve(skillId));
  await call('skills.list', () => beta.skills.list());
  const version = await call('skills.versions.create', async () =>
    beta.skills.versions.create(skillId, { files: [await skillFile()] } as never),
  );
  const versionName = (version as { version?: string } | null)?.version as string;
  await call('skills.versions.list', () => beta.skills.versions.list(skillId));
  await call('skills.versions.retrieve', () =>
    beta.skills.versions.retrieve(versionName, { skill_id: skillId } as never),
  );
  await call('skills.versions.download', () =>
    beta.skills.versions.download(versionName, { skill_id: skillId } as never),
  );
  await call('skills.versions.delete', () =>
    beta.skills.versions.delete(versionName, { skill_id: skillId } as never),
  );
  await call('skills.versions.delete(first)', () =>
    beta.skills.versions.delete(firstVersion, { skill_id: skillId } as never),
  );
  await call('skills.delete', () => beta.skills.delete(skillId));

  const vault = await call('vaults.create', () =>
    beta.vaults.create({ display_name: `conf-${dialect}` } as never),
  );
  const vaultId = id(vault);
  await call('vaults.retrieve', () => beta.vaults.retrieve(vaultId));
  await call('vaults.list', () => beta.vaults.list());
  await call('vaults.update', () =>
    beta.vaults.update(vaultId, { display_name: 'updated' } as never),
  );
  await call('vaults.archive', () => beta.vaults.archive(vaultId));
  await call('vaults.delete', () => beta.vaults.delete(vaultId));

  await call('sessions.archive', () => beta.sessions.archive(sessionId));
  await call('sessions.delete', () => beta.sessions.delete(sessionId));
  await call('environments.archive', () => beta.environments.archive(environmentId));
  await call('environments.delete', () => beta.environments.delete(environmentId));
  await call('agents.archive', () => beta.agents.archive(agentId));
}
