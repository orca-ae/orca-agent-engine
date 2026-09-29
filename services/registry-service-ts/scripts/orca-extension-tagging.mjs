// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Tag the operations Anthropic does not publish, so a consumer can filter them.
 *
 * A generated client built from `openapi/managed-agents.yaml` otherwise cannot
 * tell an Anthropic-compatible operation from an Orca-only one: both are just
 * paths. The `orca-extension` tag is the machine-readable form of the boundary
 * that `conformance-matrix.md` states in prose, and OpenAPI generators already
 * know how to include or exclude by tag.
 *
 * **The tag is computed, never hand-maintained.** The classification comes from
 * `conformance-core.mjs` — the same differ that produces the matrix — run
 * against the vendored Anthropic spec. A hand-maintained list of extensions goes
 * stale the moment Anthropic publishes an operation we already serve, and it
 * goes stale silently: nothing about a stale list looks different from a correct
 * one. A computed list cannot drift from the spec it is computed from.
 *
 * What *can* go wrong is the computation itself — a normalization change, a
 * malformed sync, a differ regression — and that is what the tripwire below is
 * for. Pure module, no I/O: `generate-openapi.mjs` is the shell.
 */
import { classify } from './conformance-core.mjs';
import { HTTP_METHODS, displayKey } from './lib/normalize-operation.mjs';

/** The tag applied to every operation Anthropic does not publish. */
export const ORCA_EXTENSION_TAG = 'orca-extension';

/** Top-level tag definition, so the string in an operation's `tags` is defined somewhere. */
export const ORCA_EXTENSION_TAG_DEFINITION = {
  name: ORCA_EXTENSION_TAG,
  description:
    'Orca-only operation: this API serves it and Anthropic does not publish it. Applied by ' +
    'scripts/orca-extension-tagging.mjs from a diff against Anthropic’s vendored OpenAPI spec, ' +
    'never by hand. A client that must stay portable across Anthropic and Orca can exclude this ' +
    'tag at generation time. See docs/managed-agents/api-groups-and-extensions.md.',
};

/**
 * Operations that must come out tagged.
 *
 * Hand-written from the audit against `vendor/anthropic/openapi.json`, and
 * deliberately not derived from the differ: the two sides of this check have to
 * come from different places or it is the diff agreeing with itself. Anthropic
 * archives Agents but never deletes them, exposes outcomes only as a Session
 * field, and has no Session-nested Files routes.
 */
export const MUST_BE_EXTENSIONS = [
  'DELETE /v1/agents/{id}',
  'GET /v1/sessions/{id}/outcome',
  'GET /v1/sessions/{id}/files',
  'GET /v1/sessions/{id}/files/{file_id}',
  'GET /v1/sessions/{id}/files/{file_id}/content',
  'DELETE /v1/sessions/{id}/files/{file_id}',
];

/**
 * Operations that must come out untagged.
 *
 * One entry, and it is the reason all of this exists: Anthropic publishes
 * `beta_download_skill_version_content_v1_skills__skill_id__versions__version__content_get`.
 * Calling it an Orca extension is the specific error that propagated into a
 * generated spec and a downstream repo, so it is pinned in the opposite
 * direction from everything above.
 */
export const MUST_BE_CORE = ['GET /v1/skills/{id}/versions/{version}/content'];

/** Every `METHOD path` an OpenAPI document publishes, in document order. */
export function publishedOperations(document) {
  const operations = [];
  for (const [pathKey, pathItem] of Object.entries(document.paths ?? {})) {
    for (const method of HTTP_METHODS) {
      if (pathItem?.[method]) operations.push(displayKey(method, pathKey));
    }
  }
  return operations;
}

/**
 * The tripwire, both directions.
 *
 * Returns a list of human-readable problems; empty means the computation agrees
 * with the audit. Three ways to fail, not two — an entry naming an operation
 * this API no longer publishes is the quiet one, because a deleted route
 * satisfies "is not tagged" for free and would let `MUST_BE_CORE` pass by
 * vacuum.
 */
export function checkTripwire(taggedOperations, publishedOperationList) {
  const tagged = new Set(taggedOperations);
  const published = new Set(publishedOperationList);
  const problems = [];

  for (const operation of [...MUST_BE_EXTENSIONS, ...MUST_BE_CORE]) {
    if (!published.has(operation)) {
      problems.push(
        `\`${operation}\` is pinned by the extension-tagging tripwire but this API no longer ` +
          'publishes it. If the operation was intentionally removed, delete its tripwire entry ' +
          'in the same change; leaving it here pins nothing.',
      );
    }
  }

  for (const operation of MUST_BE_EXTENSIONS) {
    if (published.has(operation) && !tagged.has(operation)) {
      problems.push(
        `\`${operation}\` is in MUST_BE_EXTENSIONS but was not tagged \`${ORCA_EXTENSION_TAG}\`. ` +
          'Either Anthropic now publishes this operation — in which case it is core, and the ' +
          'tripwire entry plus its `extension` decision in conformance-decisions.yaml both go — ' +
          'or the classification is broken.',
      );
    }
  }

  for (const operation of MUST_BE_CORE) {
    if (published.has(operation) && tagged.has(operation)) {
      problems.push(
        `\`${operation}\` is in MUST_BE_CORE but was tagged \`${ORCA_EXTENSION_TAG}\`. Anthropic ` +
          'publishes this operation; tagging it as Orca-only is the exact error the conformance ' +
          'framework exists to prevent. Check that vendor/anthropic/openapi.json is intact ' +
          '(`pnpm anthropic:sync`) before touching the tripwire.',
      );
    }
  }

  return problems;
}

/**
 * Tag every Orca-only operation in `document`, in place, and check the tripwire.
 *
 * Labelling only: paths, methods, parameters, bodies and responses are left
 * exactly as they were. `pathsUnchanged` in the return value is the caller's
 * evidence for that — a tagging pass that moved a path would be a behaviour
 * change wearing a documentation change's clothes.
 *
 * Throws when the tripwire fires, so a wrong classification fails the build
 * rather than shipping a spec that mislabels the boundary.
 */
export function tagOrcaExtensions(document, anthropicSpec) {
  const pathsBefore = JSON.stringify(Object.keys(document.paths ?? {}));
  const operationsBefore = JSON.stringify(publishedOperations(document));

  // `proseInvariants` is omitted deliberately, not overlooked: it only feeds
  // `differences`, and this module reads `extension` — the spec-derived half.
  // A hand-written invariant must not be able to move the published tag.
  const { extension } = classify(anthropicSpec, document, undefined);
  const tagged = extension.map((operation) => operation.display);

  const problems = checkTripwire(tagged, publishedOperations(document));
  if (problems.length > 0) {
    throw new Error(
      ['extension-tagging tripwire failed:', ...problems.map((problem) => `  - ${problem}`)].join(
        '\n',
      ),
    );
  }

  for (const operation of extension) {
    const pathItem = document.paths[operation.path];
    const target = pathItem?.[operation.method.toLowerCase()];
    if (!target) {
      // classify() derived this operation from `document` itself, so failing to
      // find it again means the two disagree about what an operation is.
      throw new Error(
        `\`${operation.display}\` was classified an extension but is not addressable in the ` +
          'document it was classified from; path normalization and document lookup disagree.',
      );
    }
    const existing = Array.isArray(target.tags) ? target.tags : [];
    if (!existing.includes(ORCA_EXTENSION_TAG)) {
      target.tags = [...existing, ORCA_EXTENSION_TAG];
    }
  }

  document.tags = [
    ...(Array.isArray(document.tags) ? document.tags : []).filter(
      (tag) => tag?.name !== ORCA_EXTENSION_TAG,
    ),
    ORCA_EXTENSION_TAG_DEFINITION,
  ];

  return {
    tagged,
    pathsUnchanged: JSON.stringify(Object.keys(document.paths ?? {})) === pathsBefore,
    operationsUnchanged: JSON.stringify(publishedOperations(document)) === operationsBefore,
  };
}
