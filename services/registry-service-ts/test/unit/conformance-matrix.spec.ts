// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { load } from 'js-yaml';
import { describe, expect, it } from 'vitest';
import { classify, matchDecisions, validateDecisions } from '../../scripts/conformance-core.mjs';
import { schemaDifferences } from '../../scripts/lib/schema-diff.mjs';

/**
 * Ground truth, asserted against the committed artifacts rather than a fixture.
 *
 * The error this whole apparatus exists to prevent was a *classification* error:
 * Orca-only operations were published as Anthropic core operations, and one
 * genuinely Anthropic operation was published as an Orca extension. The assertions
 * below are the ones that would have caught it, and they are deliberately
 * hand-written from Anthropic's spec rather than derived from the differ's own
 * output — a test that regenerates its expectations from the code under test
 * would have passed happily on the original mistake.
 */

const read = (relative: string) => readFileSync(new URL(relative, import.meta.url), 'utf8');
const readBytes = (relative: string) => readFileSync(new URL(relative, import.meta.url));

const anthropicSpecBytes = readBytes('../../vendor/anthropic/openapi.json');
const anthropicSpec = JSON.parse(anthropicSpecBytes.toString('utf8'));
const pinned = JSON.parse(read('../../vendor/anthropic/PINNED.json'));
const orcaSpec = load(read('../../openapi/managed-agents.yaml')) as Record<string, unknown>;
const register = load(read('../../conformance-decisions.yaml')) as Record<string, unknown>;
const proseInvariants = load(read('../../anthropic-prose-invariants.yaml')) as Record<
  string,
  unknown
>;

// Exactly the inputs the generator uses, including the oasdiff pass. Anything
// less and this file asserts against a smaller surface than the one that ships:
// dropping the prose invariants once made their decision look dead, and dropping
// the schema rows did the same to three more. Running the real comparison costs
// about a second and is the only version of this test that means anything.
const schemaRows = await schemaDifferences(anthropicSpec, orcaSpec);
const result = classify(anthropicSpec, orcaSpec, proseInvariants, schemaRows);
const displayOf = (entries: { display: string }[]) => entries.map((entry) => entry.display);

describe('the vendored Anthropic spec', () => {
  /**
   * Every classification below, and every digest the matrix cites, is a claim
   * about these exact bytes. `pnpm conformance:gen` verifies this too, but PR CI
   * runs the test suite on far more changes than it runs the generator, so an
   * altered or truncated vendored spec should fail here first.
   */
  it('matches the sha256 pinned alongside it', () => {
    expect(pinned.sha256_of_openapi_json_computed_locally).toMatch(/^[0-9a-f]{64}$/);
    expect(createHash('sha256').update(anthropicSpecBytes).digest('hex')).toBe(
      pinned.sha256_of_openapi_json_computed_locally,
    );
  });
});

describe('the published Orca spec', () => {
  it('exposes no internal workload routes', () => {
    // Enumerated rather than "everything starts with /v1/": since API groups
    // landed, the public listener also serves `/api`, `/apis` and the two
    // probes, and a rule that rejected them would have to be relaxed to
    // "anything" the first time one was added. The property worth pinning is
    // that `/internal/*` — the registry↔harness workload API — never appears.
    const publicRoots = ['/v1/', '/api', '/apis', '/healthz', '/readyz'];
    for (const path of Object.keys(orcaSpec.paths as object)) {
      expect(path.startsWith('/internal/'), path).toBe(false);
      expect(
        publicRoots.some((root) => path.startsWith(root)),
        path,
      ).toBe(true);
    }
  });

  it('declares anthropic-beta as an accepted header on every operation', () => {
    const paths = orcaSpec.paths as Record<string, Record<string, { parameters?: unknown[] }>>;
    for (const [path, item] of Object.entries(paths)) {
      for (const [method, operation] of Object.entries(item)) {
        const names = (operation.parameters ?? []).map((p) => (p as { name: string }).name);
        expect(names, `${method.toUpperCase()} ${path}`).toContain('anthropic-beta');
      }
    }
  });
});

describe('classification against Anthropic’s published spec', () => {
  it('matches enough of the surface to be a meaningful diff', () => {
    expect(result.counts.core).toBeGreaterThan(50);
  });

  it('classifies the deliberate business operations Anthropic does not publish as extensions', () => {
    // Verified by hand against vendor/anthropic/openapi.json. Anthropic archives
    // Agents but never deletes them, exposes outcomes as a Session field
    // rather than a standalone route, and has no Session-nested Files API.
    expect(displayOf(result.extension)).toEqual(
      expect.arrayContaining([
        'DELETE /v1/agents/{id}',
        'GET /v1/sessions/{id}/outcome',
        'GET /v1/sessions/{id}/files',
        'GET /v1/sessions/{id}/files/{file_id}',
        'GET /v1/sessions/{id}/files/{file_id}/content',
        'DELETE /v1/sessions/{id}/files/{file_id}',
      ]),
    );
  });

  it('classifies skill-version content download as core, because Anthropic publishes it', () => {
    // `beta_download_skill_version_content_v1_skills__skill_id__versions__version__content_get`.
    // Publishing this as an Orca extension is the specific mistake that started
    // this work.
    expect(displayOf(result.extension)).not.toContain(
      'GET /v1/skills/{id}/versions/{version}/content',
    );
    expect(result.core.map((entry) => entry.orca.display)).toContain(
      'GET /v1/skills/{id}/versions/{version}/content',
    );
  });

  it('reports Anthropic features we do not serve as missing rather than suppressing them', () => {
    expect(displayOf(result.missing)).toEqual(
      expect.arrayContaining([
        'GET /v1/deployments',
        'GET /v1/environments/{environment_id}/work',
        'GET /v1/tunnels',
        'GET /v1/dreams',
        'GET /v1/user_profiles',
      ]),
    );
  });

  it('does not report a managed-agents operation as missing merely because it is beta-only', () => {
    // Every managed-agents path key in Anthropic's spec carries `?beta=true`.
    // Leaving that suffix on made an earlier attempt report the whole surface as
    // absent, so pin the shape of the result, not just its non-emptiness.
    expect(displayOf(result.missing)).not.toContain('GET /v1/agents');
    expect(displayOf(result.missing)).not.toContain('POST /v1/sessions');
  });
});

describe('the decision register', () => {
  it('is well formed', () => {
    expect(validateDecisions(register)).toEqual([]);
  });

  it('pins every difference to a decision', () => {
    const { unmatched, ambiguous, miscounted, misfingerprinted } = matchDecisions(
      result.differences,
      register,
    );
    expect(unmatched.map((entry) => `[${entry.class}] ${entry.operation}`)).toEqual([]);
    expect(ambiguous).toEqual([]);
    expect(miscounted).toEqual([]);
    expect(misfingerprinted).toEqual([]);
  });

  it('carries no decision that matches nothing', () => {
    const { unused } = matchDecisions(result.differences, register);
    expect(unused.map((entry: { id: string }) => entry.id)).toEqual([]);
  });

  it('records no success-code divergence', () => {
    // All nine differences that existed on this axis now answer Anthropic's
    // published status. A new entry of any decision kind is a regression.
    const { resolved } = matchDecisions(result.differences, register);
    expect(
      resolved.filter((entry) => entry.class === 'success-codes').map((entry) => entry.operation),
    ).toEqual([]);
  });

  it('records a media-type divergence without letting it replace the body comparison', () => {
    // Declaring `text/event-stream` on the streaming routes is right — it is
    // what the handler writes — but oasdiff matches content by media type, so
    // the honest declaration would have silently ended the schema comparison on
    // those two operations. Both rows must be present, not one.
    const { resolved } = matchDecisions(result.differences, register);
    // Correlated on the normalized key, not the displayed operation: schema rows
    // are labelled with Anthropic's parameter names (`{session_id}`) and the
    // rest with ours (`{id}`), so comparing the display strings would match
    // nothing and read as a pass.
    const rowsFor = (path: string, cls: string) =>
      resolved.filter((entry) => entry.class === cls && entry.key.endsWith(`GET ${path}`));

    for (const path of ['/v1/sessions/{1}/events/stream', '/v1/sessions/{1}/threads/{2}/stream']) {
      expect(rowsFor(path, 'response-media')).toHaveLength(1);

      // Asserting the row *exists* proves nothing: without the alignment,
      // oasdiff still emits a `success-schema` row and fills it with the two
      // `content:` markers instead of the bodies. The first version of this test
      // did exactly that and passed with the alignment disabled. What has to
      // hold is that something other than the media type was compared.
      const [schemaRow] = rowsFor(path, 'success-schema');
      const leaves = [...(schemaRow?.onlyAnthropic ?? []), ...(schemaRow?.onlyOrca ?? [])];
      expect(leaves.filter((leaf: string) => !leaf.startsWith('content:')).length).toBeGreaterThan(
        0,
      );
    }
  });
});
