// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  aggregateDeltaFingerprint,
  classify,
  collectOperations,
  matchDecisions,
  validateDecisions,
  validateProseInvariants,
  validateReferences,
} from '../../scripts/conformance-core.mjs';
import { normalizePath, operationKey } from '../../scripts/lib/normalize-operation.mjs';

/**
 * The differ is a pure module precisely so it can be exercised with tiny
 * hand-written specs. `conformance-matrix.spec.ts` covers the real artifacts.
 */

const operation = (extra: Record<string, unknown> = {}) => ({
  responses: { 200: {} },
  ...extra,
});

describe('normalizePath', () => {
  it("strips Anthropic's ?beta=true path-key suffix", () => {
    expect(normalizePath('/v1/agents?beta=true')).toBe('/v1/agents');
    expect(normalizePath('/v1/agents/{agent_id}/archive?beta=true')).toBe('/v1/agents/{1}/archive');
  });

  it('reduces path parameters to positions so differently-named params compare equal', () => {
    expect(normalizePath('/v1/agents/{agent_id}')).toBe(normalizePath('/v1/agents/{id}'));
    expect(normalizePath('/v1/skills/:id/versions/:version')).toBe(
      normalizePath('/v1/skills/{skill_id}/versions/{version}'),
    );
  });

  it('keeps distinct paths distinct after positional reduction', () => {
    expect(normalizePath('/v1/sessions/{id}/stream')).not.toBe(
      normalizePath('/v1/sessions/{id}/events/stream'),
    );
  });

  it('keys an operation on method and normalized path', () => {
    expect(operationKey('get', '/v1/agents/{agent_id}?beta=true')).toBe('GET /v1/agents/{1}');
  });
});

describe('collectOperations', () => {
  it('collapses the GA and beta forms of one operation, keeping the beta variant', () => {
    const collected = collectOperations({
      paths: {
        '/v1/messages': { post: operation({ operationId: 'messages_post' }) },
        '/v1/messages?beta=true': { post: operation({ operationId: 'beta_messages_post' }) },
      },
    });
    expect([...collected.keys()]).toEqual(['POST /v1/messages']);
    expect(collected.get('POST /v1/messages')?.operationId).toBe('beta_messages_post');
  });

  it('refuses to guess when two operations collide with no beta variant to prefer', () => {
    expect(() =>
      collectOperations({
        paths: {
          '/v1/agents/{id}': { get: operation() },
          '/v1/agents/{agent_id}': { get: operation() },
        },
      }),
    ).toThrow(/collapse to/);
  });

  it('splits response codes into success and error sets', () => {
    const collected = collectOperations({
      paths: { '/v1/agents': { post: { responses: { 200: {}, 201: {}, 400: {}, '4XX': {} } } } },
    });
    const record = collected.get('POST /v1/agents');
    expect(record?.successCodes).toEqual(['200', '201']);
    expect(record?.errorCodes).toEqual(['400', '4XX']);
  });

  it('resolves $ref request bodies when collecting required parameters', () => {
    const collected = collectOperations({
      paths: {
        '/v1/agents': {
          post: {
            responses: { 200: {} },
            parameters: [{ name: 'limit', in: 'query', required: true }],
            requestBody: {
              content: { 'application/json': { schema: { $ref: '#/components/schemas/Create' } } },
            },
          },
        },
      },
      components: { schemas: { Create: { required: ['name', 'model'] } } },
    });
    expect(collected.get('POST /v1/agents')?.requiredParams).toEqual([
      'body:model',
      'body:name',
      'query:limit',
    ]);
  });

  it('ignores required path parameters, whose names are cosmetic', () => {
    const collected = collectOperations({
      paths: {
        '/v1/agents/{agent_id}': {
          get: operation({ parameters: [{ name: 'agent_id', in: 'path', required: true }] }),
        },
      },
    });
    expect(collected.get('GET /v1/agents/{1}')?.requiredParams).toEqual([]);
  });
});

describe('classify', () => {
  const anthropic = {
    paths: {
      '/v1/agents?beta=true': {
        get: operation({ operationId: 'BetaListAgents' }),
        post: operation({ operationId: 'BetaCreateAgent', responses: { 200: {}, 400: {} } }),
      },
      '/v1/deployments?beta=true': { get: operation({ operationId: 'BetaListDeployments' }) },
    },
  };
  const orca = {
    paths: {
      '/v1/agents': {
        get: operation(),
        post: operation({ responses: { 201: {}, 404: {} } }),
      },
      '/v1/agents/{id}': { delete: operation() },
    },
  };

  it('sorts operations into core, missing and extension', () => {
    const result = classify(anthropic, orca);
    expect(result.counts).toMatchObject({ core: 2, missing: 1, extension: 1 });
    expect(result.missing.map((entry) => entry.display)).toEqual(['GET /v1/deployments']);
    expect(result.extension.map((entry) => entry.display)).toEqual(['DELETE /v1/agents/{id}']);
  });

  it('compares success codes strictly', () => {
    const difference = classify(anthropic, orca).differences.find(
      (entry) => entry.class === 'success-codes',
    );
    expect(difference?.operation).toBe('POST /v1/agents');
    expect(difference?.onlyAnthropic).toEqual(['200']);
    expect(difference?.onlyOrca).toEqual(['201']);
  });

  it('reports error-code differences without treating them as success differences', () => {
    const errorDifference = classify(anthropic, orca).differences.find(
      (entry) => entry.class === 'error-codes',
    );
    expect(errorDifference?.onlyAnthropic).toEqual(['400']);
    expect(errorDifference?.onlyOrca).toEqual(['404']);
  });

  it('throws rather than publish a matrix claiming the whole surface is absent', () => {
    // The failure this guard exists for: a normalization bug — historically,
    // forgetting Anthropic's `?beta=true` path-key suffix — makes nothing match,
    // and the matrix then reports every operation as both missing and an
    // extension while looking perfectly well-formed.
    const disjoint = { paths: { '/v2/agents': { get: operation() } } };
    expect(() => classify(disjoint, orca)).toThrow(/no operation matched/);
  });
});

describe('validateDecisions', () => {
  const wellFormed = {
    version: 1,
    decisions: [
      {
        id: 'a',
        match: { class: 'missing', path: '/v1/x*' },
        covers: 1,
        decision: 'not-implemented',
        reference: 'docs/x.md',
      },
    ],
  };

  it('accepts a well-formed register', () => {
    expect(validateDecisions(wellFormed)).toEqual([]);
  });

  it('rejects an unknown decision word', () => {
    const entry = { ...wellFormed.decisions[0], decision: 'probably-fine' };
    expect(validateDecisions({ version: 1, decisions: [entry] })).toContainEqual(
      expect.stringContaining('`decision` must be one of'),
    );
  });

  it('rejects an entry with no reference', () => {
    const entry = { ...wellFormed.decisions[0], reference: '   ' };
    expect(validateDecisions({ version: 1, decisions: [entry] })).toContainEqual(
      expect.stringContaining('`reference` is required'),
    );
  });

  it('rejects a match that is neither exactly-one of operation or path', () => {
    const both = {
      ...wellFormed.decisions[0],
      match: { class: 'missing', path: '/v1/x*', operation: 'GET /v1/x' },
    };
    expect(validateDecisions({ version: 1, decisions: [both] })).toContainEqual(
      expect.stringContaining('exactly one of'),
    );
    const neither = { ...wellFormed.decisions[0], match: { class: 'missing' } };
    expect(validateDecisions({ version: 1, decisions: [neither] })).toContainEqual(
      expect.stringContaining('exactly one of'),
    );
  });

  it('rejects duplicate ids', () => {
    expect(
      validateDecisions({
        version: 1,
        decisions: [...wellFormed.decisions, ...wellFormed.decisions],
      }),
    ).toContainEqual(expect.stringContaining('duplicate `id`'));
  });
});

describe('matchDecisions', () => {
  const difference = (extra: Record<string, unknown> = {}) => ({
    class: 'missing',
    operation: 'GET /v1/deployments',
    method: 'GET',
    path: '/v1/deployments',
    key: 'GET /v1/deployments',
    onlyAnthropic: [],
    onlyOrca: [],
    ...extra,
  });
  const rule = (id: string, match: Record<string, unknown>) => ({
    id,
    match,
    decision: 'not-implemented',
    reference: 'docs/x.md',
  });

  it('pins a difference to a glob rule', () => {
    const { resolved, unmatched, unused } = matchDecisions([difference()], {
      version: 1,
      decisions: [rule('glob', { class: 'missing', path: '/v1/deployments*' })],
    });
    expect(resolved[0]?.decisionId).toBe('glob');
    expect(unmatched).toEqual([]);
    expect(unused).toEqual([]);
  });

  it('lets an exact operation match beat a glob', () => {
    const { resolved } = matchDecisions([difference()], {
      version: 1,
      decisions: [
        rule('glob', { class: 'missing', path: '/v1/*' }),
        rule('exact', { class: 'missing', operation: 'GET /v1/deployments' }),
      ],
    });
    expect(resolved[0]?.decisionId).toBe('exact');
  });

  it('lets the longest glob win, so a family rule can be narrowed without reordering', () => {
    const { resolved } = matchDecisions([difference()], {
      version: 1,
      decisions: [
        rule('narrow', { class: 'missing', path: '/v1/deployments*' }),
        rule('broad', { class: 'missing', path: '/v1/*' }),
      ],
    });
    expect(resolved[0]?.decisionId).toBe('narrow');
  });

  it('never matches across difference classes', () => {
    const { unmatched } = matchDecisions([difference({ class: 'extension' })], {
      version: 1,
      decisions: [rule('glob', { class: 'missing', path: '/v1/*' })],
    });
    expect(unmatched).toHaveLength(1);
  });

  it('reports a difference nobody decided about', () => {
    const { unmatched, resolved } = matchDecisions([difference()], { version: 1, decisions: [] });
    expect(unmatched).toHaveLength(1);
    expect(resolved[0]?.decision).toBeNull();
  });

  it('reports a decision that matches nothing, so dead entries cannot read as coverage', () => {
    const { unused } = matchDecisions([difference()], {
      version: 1,
      decisions: [
        rule('live', { class: 'missing', path: '/v1/deployments*' }),
        rule('dead', { class: 'missing', path: '/v1/tunnels*' }),
      ],
    });
    expect(unused.map((entry: { id: string }) => entry.id)).toEqual(['dead']);
  });

  it('reports a tie between two equally specific rules rather than picking one', () => {
    const { ambiguous } = matchDecisions([difference()], {
      version: 1,
      // Same length, both matching: nothing distinguishes them.
      decisions: [
        rule('one', { class: 'missing', path: '/v1/deployment*' }),
        rule('two', { class: 'missing', path: '/v1/deployments' }),
      ],
    });
    expect(ambiguous).toHaveLength(1);
    expect(ambiguous[0]?.ids).toEqual(['one', 'two']);
  });
});

/**
 * A union body's *shared* requirement is a real requirement.
 *
 * The differ used to skip `oneOf`/`anyOf` entirely, which is why
 * `POST /v1/sessions/{id}/resources` reported no difference at all while
 * Anthropic required `file_id` and we did not.
 */
describe('required parameters from union request bodies', () => {
  const spec = (schema: unknown) => ({
    openapi: '3.0.2',
    paths: {
      '/v1/things': {
        post: {
          responses: { 200: {} },
          requestBody: { content: { 'application/json': { schema } } },
        },
      },
    },
  });

  const requiredParamsOf = (anthropic: unknown, orca: unknown) =>
    classify(spec(anthropic), spec(orca)).differences.find((d) => d.class === 'required-params');

  it('counts what every branch requires', () => {
    const single = { type: 'object', required: ['type', 'file_id'] };
    const three = {
      oneOf: [
        { type: 'object', required: ['type', 'file_id'] },
        { type: 'object', required: ['type', 'memory_store_id'] },
        { type: 'object', required: ['type', 'url'] },
      ],
    };

    const difference = requiredParamsOf(single, three);
    expect(difference?.onlyAnthropic).toEqual(['body:file_id']);
    expect(difference?.onlyOrca).toEqual([]);
  });

  it('does not count what only one branch requires', () => {
    const union = {
      oneOf: [
        { type: 'object', required: ['type', 'file_id'] },
        { type: 'object', required: ['type'] },
      ],
    };
    // `file_id` is required on one branch only, so a caller can satisfy the
    // request without it — treating it as required would invent a difference.
    expect(requiredParamsOf({ type: 'object', required: ['type'] }, union)).toBeUndefined();
  });

  it('resolves branches through $ref', () => {
    const withRefs = {
      openapi: '3.0.2',
      components: { schemas: { Branch: { type: 'object', required: ['type', 'file_id'] } } },
      paths: {
        '/v1/things': {
          post: {
            responses: { 200: {} },
            requestBody: {
              content: {
                'application/json': {
                  schema: { oneOf: [{ $ref: '#/components/schemas/Branch' }] },
                },
              },
            },
          },
        },
      },
    };
    const difference = classify(
      withRefs,
      spec({ type: 'object', required: ['type'] }),
    ).differences.find((d) => d.class === 'required-params');
    expect(difference?.onlyAnthropic).toEqual(['body:file_id']);
  });

  it('keeps cycle detection local when sibling branches share a base $ref', () => {
    const sharedBase = {
      openapi: '3.0.2',
      components: { schemas: { Common: { type: 'object', required: ['type'] } } },
      paths: {
        '/v1/things': {
          post: {
            responses: { 200: {} },
            requestBody: {
              content: {
                'application/json': {
                  schema: {
                    oneOf: [
                      {
                        allOf: [
                          { $ref: '#/components/schemas/Common' },
                          { type: 'object', required: ['left'] },
                        ],
                      },
                      {
                        allOf: [
                          { $ref: '#/components/schemas/Common' },
                          { type: 'object', required: ['right'] },
                        ],
                      },
                    ],
                  },
                },
              },
            },
          },
        },
      },
    };

    expect(
      classify(spec({ type: 'object', required: ['type'] }), sharedBase).differences.find(
        (d) => d.class === 'required-params',
      ),
    ).toBeUndefined();
  });
});

/**
 * A decision approves a *direction*, not just an operation.
 *
 * Without this, a rule written for "we return 201 where they return 200"
 * silently keeps approving the reverse — a narrowing that rejects input
 * Anthropic considers valid — with nothing unmatched and nothing unused.
 */
describe('direction-sensitive decisions', () => {
  const successDifference = (onlyAnthropic: string[], onlyOrca: string[]) => ({
    class: 'success-codes',
    operation: 'POST /v1/agents',
    path: '/v1/agents',
    onlyAnthropic,
    onlyOrca,
  });

  const pinned = {
    id: 'creates-201',
    match: {
      class: 'success-codes',
      operation: 'POST /v1/agents',
      onlyAnthropic: ['200'],
      onlyOrca: ['201'],
    },
    decision: 'fix-later',
    reference: 'docs/x.md',
  };

  it('matches the delta it was written for', () => {
    const { resolved, unmatched } = matchDecisions([successDifference(['200'], ['201'])], {
      version: 1,
      decisions: [pinned],
    });
    expect(unmatched).toEqual([]);
    expect(resolved[0]?.decisionId).toBe('creates-201');
  });

  it('stops matching when the delta reverses', () => {
    const { unmatched, unused } = matchDecisions([successDifference(['201'], ['200'])], {
      version: 1,
      decisions: [pinned],
    });
    expect(unmatched).toHaveLength(1);
    expect(unused.map((entry: { id: string }) => entry.id)).toEqual(['creates-201']);
  });

  it('matches any direction when a rule says so explicitly', () => {
    const broad = {
      ...pinned,
      match: { ...pinned.match, onlyAnthropic: undefined, onlyOrca: undefined, anyDelta: true },
    };
    const { unmatched } = matchDecisions([successDifference(['201'], ['200'])], {
      version: 1,
      decisions: [broad],
    });
    expect(unmatched).toEqual([]);
  });

  it('requires direction-sensitive rules to declare a delta', () => {
    const problems = validateDecisions({
      version: 1,
      decisions: [
        {
          id: 'vague',
          match: { class: 'success-codes', operation: 'POST /v1/agents' },
          decision: 'fix-later',
          reference: 'docs/x.md',
        },
      ],
    });
    expect(problems.join('\n')).toContain('direction-sensitive');
  });

  it('rejects a rule that both pins a delta and claims any', () => {
    const problems = validateDecisions({
      version: 1,
      decisions: [{ ...pinned, match: { ...pinned.match, anyDelta: true } }],
    });
    expect(problems.join('\n')).toContain('contradicts');
  });
});

describe('schema decision fingerprints', () => {
  const row = (operationName: string, onlyAnthropic: string[], onlyOrca: string[]) => ({
    class: 'request-schema',
    operation: operationName,
    path: '/v1/things',
    key: `request-schema ${operationName}`,
    onlyAnthropic,
    onlyOrca,
  });

  const original = [
    row('POST /v1/things', ['schema.type:string'], ['schema.type:integer']),
    row('POST /v1/things/archive', ['required:id'], []),
  ];
  const decision = {
    id: 'request-family',
    match: { class: 'request-schema', path: '/v1/things*' },
    covers: 2,
    deltaFingerprint: aggregateDeltaFingerprint(original),
    decision: 'accepted-deviation',
    reference: 'docs/x.md',
  };

  it('accepts the exact aggregate it was reviewed against', () => {
    expect(validateDecisions({ version: 1, decisions: [decision] })).toEqual([]);
    expect(
      matchDecisions(original, { version: 1, decisions: [decision] }).misfingerprinted,
    ).toEqual([]);
  });

  it('fails when an atom changes without changing the row count', () => {
    const changed = [original[0], row('POST /v1/things/archive', ['required:admin_secret'], [])];
    const { miscounted, misfingerprinted } = matchDecisions(changed, {
      version: 1,
      decisions: [decision],
    });
    expect(miscounted).toEqual([]);
    expect(misfingerprinted).toEqual([
      {
        id: 'request-family',
        expected: decision.deltaFingerprint,
        actual: aggregateDeltaFingerprint(changed),
      },
    ]);
  });

  it('requires content-sensitive rules to declare a fingerprint', () => {
    const { deltaFingerprint: _removed, ...withoutFingerprint } = decision;
    expect(validateDecisions({ version: 1, decisions: [withoutFingerprint] }).join('\n')).toContain(
      'deltaFingerprint',
    );
  });
});

/**
 * The prose-invariant file is the only hand-written input, so it is the one most
 * able to rot into something that reads authoritative and is not.
 */
describe('prose invariants', () => {
  const invariant = (extra: Record<string, unknown> = {}) => ({
    id: 'beta-header',
    family: 'beta-header-managed-agents',
    paths: ['/v1/*'],
    anthropicRequires: 'anthropic-beta: managed-agents-2026-04-01',
    orcaBehaviour: 'Accepted and ignored.',
    source: 'https://platform.claude.com/docs/en/api/beta',
    ...extra,
  });

  it('accepts a fully cited invariant', () => {
    expect(validateProseInvariants({ version: 1, invariants: [invariant()] })).toEqual([]);
  });

  it('refuses an invariant with no citation', () => {
    const problems = validateProseInvariants({
      version: 1,
      invariants: [invariant({ source: undefined })],
    });
    expect(problems.join('\n')).toContain('`source` is required');
  });

  it('refuses a citation that is not a URL', () => {
    const problems = validateProseInvariants({
      version: 1,
      invariants: [invariant({ source: 'the docs somewhere' })],
    });
    expect(problems.join('\n')).toContain('https URL');
  });

  it('emits one difference per invariant, carrying its source', () => {
    const empty = { openapi: '3.0.2', paths: { '/v1/a': { get: { responses: { 200: {} } } } } };
    const { differences } = classify(empty, empty, { version: 1, invariants: [invariant()] });
    const prose = differences.filter((d) => d.class === 'prose-invariant');
    expect(prose).toHaveLength(1);
    expect(prose[0]?.operation).toBe('beta-header');
    expect(prose[0]?.note).toBe('https://platform.claude.com/docs/en/api/beta');
  });
});

/**
 * Breadth gates.
 *
 * Each of these exists because a rule that matched more than it approved got
 * through review: a prose decision that pre-approved requirements nobody had
 * written yet, and family globs that would absorb a new difference in silence.
 */
describe('glob rules pin how much they cover', () => {
  const glob = (extra: Record<string, unknown> = {}) => ({
    id: 'family',
    match: { class: 'missing', path: '/v1/x*' },
    covers: 2,
    decision: 'not-implemented',
    reference: 'docs/x.md',
    ...extra,
  });
  const miss = (path: string) => ({ class: 'missing', operation: `GET ${path}`, path });

  it('accepts a glob whose count is right', () => {
    expect(validateDecisions({ version: 1, decisions: [glob()] })).toEqual([]);
    const { miscounted } = matchDecisions([miss('/v1/xa'), miss('/v1/xb')], {
      version: 1,
      decisions: [glob()],
    });
    expect(miscounted).toEqual([]);
  });

  it('fails when a new difference joins the family', () => {
    const { miscounted, unmatched } = matchDecisions(
      [miss('/v1/xa'), miss('/v1/xb'), miss('/v1/xc')],
      { version: 1, decisions: [glob()] },
    );
    // Not unmatched — the rule does match it. That is the point: without the
    // pinned count the newcomer would inherit a rationale nobody re-read.
    expect(unmatched).toEqual([]);
    expect(miscounted).toEqual([{ id: 'family', covers: 2, actual: 3 }]);
  });

  it('fails when the family shrinks', () => {
    const { miscounted } = matchDecisions([miss('/v1/xa')], { version: 1, decisions: [glob()] });
    expect(miscounted).toEqual([{ id: 'family', covers: 2, actual: 1 }]);
  });

  it('requires a glob to declare covers at all', () => {
    const bare = { ...glob() };
    delete (bare as { covers?: number }).covers;
    expect(validateDecisions({ version: 1, decisions: [bare] }).join('\n')).toContain('covers');
  });

  it('rejects covers on an exact rule, which already names one target', () => {
    const exact = glob({ match: { class: 'missing', operation: 'GET /v1/x' } });
    expect(validateDecisions({ version: 1, decisions: [exact] }).join('\n')).toContain(
      'meaningless',
    );
  });
});

describe('prose decisions cannot pre-approve', () => {
  it('rejects a prose rule matched by glob', () => {
    const problems = validateDecisions({
      version: 1,
      decisions: [
        {
          id: 'broad',
          match: { class: 'prose-invariant', path: '/v1/*' },
          covers: 3,
          decision: 'accepted-deviation',
          reference: 'docs/x.md',
        },
      ],
    });
    expect(problems.join('\n')).toContain('exact invariant');
  });

  it('leaves a newly introduced invariant unmatched', () => {
    // The reproduction from review: a brand-new prose requirement resolved to
    // the existing beta-header rationale with nothing unmatched or unused.
    const { unmatched } = matchDecisions(
      [{ class: 'prose-invariant', operation: 'some-future-rule', path: '/v1/agents' }],
      {
        version: 1,
        decisions: [
          {
            id: 'beta',
            match: { class: 'prose-invariant', operation: 'beta-header-managed-agents-version' },
            decision: 'accepted-deviation',
            reference: 'docs/x.md',
          },
        ],
      },
    );
    expect(unmatched).toHaveLength(1);
  });
});

describe('prose invariant families may not overlap', () => {
  const invariant = (extra: Record<string, unknown>) => ({
    id: 'x',
    family: 'f',
    paths: ['/v1/*'],
    anthropicRequires: 'a',
    orcaBehaviour: 'b',
    source: 'https://example.invalid/docs',
    ...extra,
  });

  it('rejects two families claiming the same path', () => {
    // Exactly the shipped contradiction: `/v1/*` asserting one beta value while
    // memory rows asserted another.
    const problems = validateProseInvariants({
      version: 1,
      invariants: [
        invariant({ id: 'all', family: 'managed', paths: ['/v1/*'] }),
        invariant({ id: 'mem', family: 'memory', paths: ['/v1/memory_stores*'] }),
      ],
    });
    expect(problems.join('\n')).toContain('both claim');
  });

  it('allows complementary rules within one family', () => {
    expect(
      validateProseInvariants({
        version: 1,
        invariants: [
          invariant({ id: 'version', family: 'memory', paths: ['/v1/memory_stores*'] }),
          invariant({
            id: 'exclusivity',
            family: 'memory',
            paths: ['/v1/memory_stores*'],
            anthropicRequires: 'both values together are rejected',
          }),
        ],
      }),
    ).toEqual([]);
  });

  it('allows families of different kinds to share a path', () => {
    // A beta-header rule and a response-ordering rule describe different
    // dimensions of the same operation — they cannot contradict each other.
    expect(
      validateProseInvariants({
        version: 1,
        invariants: [
          invariant({ id: 'all', family: 'managed', paths: ['/v1/sessions*'] }),
          invariant({
            id: 'ordering',
            family: 'threads-ordering',
            kind: 'response-ordering',
            paths: ['/v1/sessions/*/threads'],
          }),
        ],
      }),
    ).toEqual([]);
  });

  it('still rejects same-kind families claiming the same path', () => {
    const problems = validateProseInvariants({
      version: 1,
      invariants: [
        invariant({ id: 'a', family: 'fa', kind: 'response-ordering', paths: ['/v1/sessions*'] }),
        invariant({
          id: 'b',
          family: 'fb',
          kind: 'response-ordering',
          paths: ['/v1/sessions/*/threads'],
        }),
      ],
    });
    expect(problems.join('\n')).toContain('both claim');
    expect(problems.join('\n')).toContain('response-ordering');
  });

  it('allows disjoint families', () => {
    expect(
      validateProseInvariants({
        version: 1,
        invariants: [
          invariant({ id: 'a', family: 'managed', paths: ['/v1/agents*', '/v1/sessions*'] }),
          invariant({ id: 'm', family: 'memory', paths: ['/v1/memory_stores*'] }),
        ],
      }),
    ).toEqual([]);
  });
});

describe('validateReferences', () => {
  // `validateReferences` takes its file access as callbacks, so a whole doc
  // tree is a two-entry object. That is the reason for the injection: the
  // alternative is a fixture directory nobody reads.
  const docs = (files: Record<string, string>) => ({
    exists: (path: string) => Object.prototype.hasOwnProperty.call(files, path),
    read: (path: string) => files[path],
  });

  const rule = (reference: string) => ({
    version: 1,
    decisions: [
      {
        id: 'r',
        match: { class: 'missing', path: '/v1/x*' },
        covers: 1,
        decision: 'not-implemented',
        reference,
      },
    ],
  });

  it('accepts a reference to a file with no anchor', () => {
    expect(validateReferences(rule('docs/x.md'), docs({ 'docs/x.md': '# X' }))).toEqual([]);
  });

  // The regression this gate exists for: retiring `delivery-phases.md` left
  // four rules — 30 operations — pointing at a file that no longer existed,
  // and nothing failed.
  it('rejects a reference whose file was deleted', () => {
    expect(validateReferences(rule('docs/gone.md#anywhere'), docs({}))).toContainEqual(
      expect.stringContaining('which does not exist'),
    );
  });

  it('rejects an anchor no heading or tag declares', () => {
    expect(
      validateReferences(rule('docs/x.md#nope'), docs({ 'docs/x.md': '# X\n\n## Real heading\n' })),
    ).toContainEqual(expect.stringContaining('has no anchor `#nope`'));
  });

  it('accepts a heading-derived slug', () => {
    expect(
      validateReferences(
        rule('docs/x.md#anthropic-surface-not-implemented'),
        docs({ 'docs/x.md': '# X\n\n## Anthropic surface not implemented\n' }),
      ),
    ).toEqual([]);
  });

  // `api-groups-and-extensions.md` writes `### <a id="probes"></a>The health
  // probes`. A slug-only reader calls that broken — and a validator that cries
  // wolf is worse than none, because the next person routes around it.
  it('accepts an explicit <a id> anchor a slug would miss', () => {
    expect(
      validateReferences(
        rule('docs/x.md#probes'),
        docs({ 'docs/x.md': '# X\n\n### <a id="probes"></a>The health probes\n' }),
      ),
    ).toEqual([]);
  });

  it('strips inline anchor tags before slugging the heading text', () => {
    expect(
      validateReferences(
        rule('docs/x.md#extension-tagging'),
        docs({ 'docs/x.md': '# X\n\n## <a id="other"></a>Extension tagging\n' }),
      ),
    ).toEqual([]);
  });

  // An issue link is a legitimate place to park reasoning, and not ours to resolve.
  it('leaves an http reference alone', () => {
    expect(validateReferences(rule('https://github.com/orca-ae/x/issues/1'), docs({}))).toEqual([]);
  });

  // `validateDecisions` owns the empty case; duplicating it here would mean two
  // messages for one mistake.
  it('defers an empty reference to validateDecisions', () => {
    expect(validateReferences(rule('  '), docs({}))).toEqual([]);
  });
});
