// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * One home for "what counts as the same operation" across the two specs.
 *
 * Imported by both `generate-openapi.mjs` (which renders our spec) and
 * `conformance-core.mjs` (which diffs it against Anthropic's), so the two
 * generators cannot drift apart on the definition.
 *
 * Two facts about Anthropic's published spec drive this file:
 *
 *  1. Its path *keys* carry a `?beta=true` suffix — a Stainless convention for
 *     marking the beta variant of an operation, not part of the request path.
 *     Every managed-agents operation is beta-only, so failing to strip the
 *     suffix makes the entire surface read as absent.
 *  2. Path parameters are named for their resource (`{agent_id}`, `{skill_id}`)
 *     where ours are frequently just `{id}`. Names are cosmetic; position is
 *     what identifies the operation. Both sides are reduced to positional
 *     placeholders before comparison.
 */

/** HTTP methods an OpenAPI path item may carry, in the order we report them. */
export const HTTP_METHODS = ['get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace'];

/**
 * Drop the query-string suffix Stainless appends to a path key
 * (`/v1/agents?beta=true` → `/v1/agents`). Returns the path unchanged when
 * there is no suffix.
 */
export function stripPathKeySuffix(pathKey) {
  const queryStart = pathKey.indexOf('?');
  return queryStart === -1 ? pathKey : pathKey.slice(0, queryStart);
}

/** True when the path key carried Anthropic's `?beta=true` marker. */
export function isBetaPathKey(pathKey) {
  return pathKey.includes('?beta=true');
}

/**
 * Reduce a path to its comparable form: no query suffix, no trailing slash and
 * every path parameter replaced by its 1-based position, so `/v1/agents/{id}`
 * and `/v1/agents/{agent_id}` compare equal.
 *
 * Accepts both OpenAPI (`{id}`) and ts-rest (`:id`) parameter syntax.
 */
export function normalizePath(pathKey) {
  const withoutSuffix = stripPathKeySuffix(pathKey);
  const trimmed =
    withoutSuffix.length > 1 && withoutSuffix.endsWith('/')
      ? withoutSuffix.slice(0, -1)
      : withoutSuffix;
  let position = 0;
  return trimmed
    .split('/')
    .map((segment) => {
      const isParameter =
        (segment.startsWith('{') && segment.endsWith('}')) || segment.startsWith(':');
      if (!isParameter) return segment;
      position += 1;
      return `{${position}}`;
    })
    .join('/');
}

/**
 * The stable identity of an operation: method plus normalized path. Rows in the
 * conformance matrix and entries in the decision register are keyed on this.
 */
export function operationKey(method, pathKey) {
  return `${method.toUpperCase()} ${normalizePath(pathKey)}`;
}

/**
 * Human-facing form of an operation: method plus the path as its own spec spells
 * it, minus the Stainless suffix. Used for matrix rows and decision globs, where
 * `{agent_id}` is more informative than `{1}`.
 */
export function displayKey(method, pathKey) {
  return `${method.toUpperCase()} ${stripPathKeySuffix(pathKey)}`;
}
