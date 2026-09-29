// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import { load } from 'js-yaml';
import { describe, expect, it } from 'vitest';
import {
  MUST_BE_CORE,
  MUST_BE_EXTENSIONS,
  ORCA_EXTENSION_TAG,
  publishedOperations,
  tagOrcaExtensions,
} from '../../scripts/orca-extension-tagging.mjs';

/**
 * The tag has to be *computed* from Anthropic's spec, and the computation has to
 * be pinned by something written independently of it. This file exercises both
 * halves: the tagging against the committed artifacts, and the tripwire against
 * deliberately corrupted copies of the vendored spec.
 */

const read = (relative: string) => readFileSync(new URL(relative, import.meta.url), 'utf8');

const anthropicSpec = JSON.parse(read('../../vendor/anthropic/openapi.json'));
const committedSpec = load(read('../../openapi/managed-agents.yaml')) as {
  paths: Record<string, Record<string, { tags?: string[] }>>;
};

/** A fresh, untagged copy of our document — `tagOrcaExtensions` mutates. */
const orcaSpec = () => structuredClone(committedSpec);

const AGENTS_BY_ID = '/v1/agents/{agent_id}?beta=true';
const SKILL_VERSION_CONTENT = '/v1/skills/{skill_id}/versions/{version}/content?beta=true';

describe('extension tagging', () => {
  it('tags every operation Anthropic does not publish, and only those', () => {
    const { tagged } = tagOrcaExtensions(orcaSpec(), anthropicSpec);
    expect(tagged).toEqual(expect.arrayContaining(MUST_BE_EXTENSIONS));
    for (const operation of MUST_BE_CORE) expect(tagged).not.toContain(operation);
  });

  it('changes nothing but labels', () => {
    // The published path set must survive tagging byte-identically: this is a
    // labelling pass, and a labelling pass that moved a path would be a
    // behaviour change wearing a documentation change's clothes.
    const document = orcaSpec();
    const pathsBefore = JSON.stringify(Object.keys(document.paths));
    const operationsBefore = JSON.stringify(publishedOperations(document));

    const result = tagOrcaExtensions(document, anthropicSpec);

    expect(result.pathsUnchanged).toBe(true);
    expect(result.operationsUnchanged).toBe(true);
    expect(JSON.stringify(Object.keys(document.paths))).toBe(pathsBefore);
    expect(JSON.stringify(publishedOperations(document))).toBe(operationsBefore);
  });

  it('leaves every operation identical apart from its tags array', () => {
    const before = orcaSpec();
    const after = orcaSpec();
    tagOrcaExtensions(after, anthropicSpec);

    for (const [path, item] of Object.entries(after.paths)) {
      for (const [method, operation] of Object.entries(item)) {
        const original = before.paths[path]![method]!;
        expect({ ...operation, tags: null }, `${method} ${path}`).toEqual({
          ...original,
          tags: null,
        });
        // Existing resource tags are appended to, never replaced.
        expect(operation.tags, `${method} ${path}`).toEqual(
          expect.arrayContaining(original.tags ?? []),
        );
      }
    }
  });

  it('is already applied to the committed artifact', () => {
    const tagsOf = (method: string, path: string) =>
      committedSpec.paths[path]?.[method]?.tags ?? [];
    expect(tagsOf('delete', '/v1/agents/{id}')).toContain(ORCA_EXTENSION_TAG);
    expect(tagsOf('get', '/v1/sessions/{id}/outcome')).toContain(ORCA_EXTENSION_TAG);
    expect(tagsOf('get', '/v1/sessions/{id}/files')).toContain(ORCA_EXTENSION_TAG);
    expect(tagsOf('get', '/v1/sessions/{id}/files/{file_id}')).toContain(ORCA_EXTENSION_TAG);
    expect(tagsOf('get', '/v1/sessions/{id}/files/{file_id}/content')).toContain(
      ORCA_EXTENSION_TAG,
    );
    expect(tagsOf('delete', '/v1/sessions/{id}/files/{file_id}')).toContain(ORCA_EXTENSION_TAG);
    expect(tagsOf('get', '/v1/skills/{id}/versions/{version}/content')).not.toContain(
      ORCA_EXTENSION_TAG,
    );
    expect(tagsOf('get', '/v1/agents')).not.toContain(ORCA_EXTENSION_TAG);
  });
});

describe('the tripwire', () => {
  it('fires when an operation in MUST_BE_EXTENSIONS stops being tagged', () => {
    // Anthropic publishing Delete Agent would reclassify it as core. That is a
    // legitimate upstream change, and it must arrive as a build failure that
    // names the operation rather than as a spec whose labels quietly moved.
    const upstream = structuredClone(anthropicSpec);
    upstream.paths[AGENTS_BY_ID].delete = {
      operationId: 'BetaDeleteAgent',
      responses: { 200: { description: 'ok' } },
    };

    expect(() => tagOrcaExtensions(orcaSpec(), upstream)).toThrowError(
      '`DELETE /v1/agents/{id}` is in MUST_BE_EXTENSIONS but was not tagged',
    );
  });

  it('fires when an operation in MUST_BE_CORE starts being tagged', () => {
    // The mistake this whole apparatus exists to prevent, reproduced: lose the
    // skill-version content path from Anthropic's spec and our identical
    // operation classifies as an Orca extension.
    const upstream = structuredClone(anthropicSpec);
    delete upstream.paths[SKILL_VERSION_CONTENT];

    expect(() => tagOrcaExtensions(orcaSpec(), upstream)).toThrowError(
      /MUST_BE_CORE but was tagged/,
    );
  });

  it('fires when a pinned operation stops being published at all', () => {
    // Without this direction MUST_BE_CORE could be satisfied by deletion: an
    // operation this API no longer serves is trivially "not tagged".
    const document = orcaSpec() as { paths: Record<string, unknown> };
    delete document.paths['/v1/skills/{id}/versions/{version}/content'];

    expect(() => tagOrcaExtensions(document as never, anthropicSpec)).toThrowError(
      /no longer publishes it/,
    );
  });

  it('does not fire on the committed pair', () => {
    expect(() => tagOrcaExtensions(orcaSpec(), anthropicSpec)).not.toThrow();
  });
});
