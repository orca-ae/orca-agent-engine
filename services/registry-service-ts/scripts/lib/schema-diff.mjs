// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Wire-schema comparison, delegated to oasdiff.
 *
 * The rest of this pipeline answers "does the operation exist, and what did we
 * decide about it". This file answers "do the shapes agree" — request bodies,
 * success response bodies, and parameters — and it does not implement that
 * comparison itself.
 *
 * Hand-rolling it was the plan until the normalization list came out identical
 * to oasdiff's flag set (`--flatten-allof`, `--exclude-elements`). Two bugs had
 * already been found by review in our own schema traversal; a tool that many
 * people run is a better bet than a third attempt at the same traversal.
 *
 * What oasdiff does NOT do for us, and why each wrapper below exists:
 *
 *  1. Its JSON output is not deterministic — three runs over identical inputs
 *     produce three different byte sequences, because Go map iteration order
 *     leaks into the encoder. Our artifacts are committed and byte-compared, so
 *     nothing here consumes its bytes: we parse, canonicalize, and render our
 *     own sorted rows.
 *  2. It matches responses by literal status code. Before the create statuses
 *     were aligned, it reported "200 deleted, 201 added" and never compared the
 *     two bodies at all — hiding exactly the kind of difference this comparison
 *     exists to find. Aligning a divergent success code before the diff is our
 *     modelling decision, not something the tool can infer.
 *  3. It knows nothing of Anthropic's `?beta=true` path-key suffix.
 *  4. It reports every leaf. Raw output is thousands of atoms — far past what a
 *     decision register can carry — so rows are aggregated to
 *     `(operation, axis)` and the atoms become the row's detail.
 */
import { runOasdiffDiffFromSpecs } from '@oasdiff-js/oasdiff-js';
import { displayKey, normalizePath, stripPathKeySuffix } from './normalize-operation.mjs';

/** Axes this file compares. Named here because the coverage ledger reads them. */
export const SCHEMA_AXES = [
  'request-schema',
  'success-schema',
  'header-parameters',
  'query-parameters',
];

const HTTP_METHODS = ['get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace'];

/**
 * Collapse Anthropic's `?beta=true` path keys, preferring the beta variant.
 *
 * Same rule `collectOperations` applies, repeated here because oasdiff reads
 * the raw document rather than our collected view.
 */
function stripBetaPathKeys(spec) {
  const paths = {};
  for (const [key, item] of Object.entries(spec.paths ?? {})) {
    const base = stripPathKeySuffix(key);
    if (key.includes('?beta=true') || !(base in paths)) paths[base] = item;
  }
  return { ...spec, paths };
}

/**
 * Fold a lone `201` onto `200` so the two success bodies are compared.
 *
 * The status-code divergence itself is not hidden by this: it is reported by
 * the `success-codes` axis, from the untouched specs, and carries its own
 * decision. This copy exists only so that "we answer 201" stops doubling as
 * "nobody ever looked at the body".
 */
function alignSuccessCodes(spec, against) {
  const theirSuccess = new Map();
  for (const [key, item] of Object.entries(against.paths ?? {})) {
    for (const method of HTTP_METHODS) {
      const codes = new Set(
        Object.keys(item?.[method]?.responses ?? {}).filter((code) => code.startsWith('2')),
      );
      if (codes.size > 0) theirSuccess.set(`${method} ${normalizePath(key)}`, codes);
    }
  }

  const paths = {};
  for (const [key, item] of Object.entries(spec.paths ?? {})) {
    const next = { ...item };
    for (const method of HTTP_METHODS) {
      const operation = next[method];
      const responses = operation?.responses;
      if (!responses || !responses['201'] || responses['200']) continue;
      const counterpart = theirSuccess.get(`${method} ${normalizePath(key)}`);
      if (!counterpart?.has('200') || counterpart.has('201')) continue;
      const { 201: created, ...rest } = responses;
      next[method] = { ...operation, responses: { ...rest, 200: created } };
    }
    paths[key] = next;
  }
  return { ...spec, paths };
}

/**
 * Re-key a success body onto the media type the other side declares.
 *
 * oasdiff matches response content by media type exactly as it matches responses
 * by status code, so a media-type divergence does not merely add a row — it
 * *replaces* the schema comparison with `content:` leaves and the bodies are
 * never compared. Declaring `text/event-stream` on the two streaming operations,
 * which is what they actually send, would otherwise have turned a 36-leaf
 * schema difference into a one-leaf media difference that reads like progress.
 *
 * Same shape as {@link alignSuccessCodes}, for the same reason and with the same
 * guarantee: the divergence is reported by the `response-media` axis, from the
 * untouched specs, and carries its own decision.
 *
 * Only a single-media-type body is re-keyed. A body offering a choice of
 * representations is a different question, and guessing which one to line up
 * would be the kind of quiet decision this pipeline is built to refuse.
 */
function alignResponseMedia(spec, against) {
  // Keyed on the normalized path, not the literal one: their `{session_id}` is
  // our `{id}`, and a literal lookup silently matches nothing — which looks
  // exactly like "no media type ever differed".
  const theirMedia = new Map();
  for (const [key, item] of Object.entries(against.paths ?? {})) {
    for (const method of HTTP_METHODS) {
      for (const [code, response] of Object.entries(item?.[method]?.responses ?? {})) {
        const media = Object.keys(response?.content ?? {});
        if (code.startsWith('2') && media.length === 1) {
          theirMedia.set(`${method} ${normalizePath(key)} ${code}`, media[0]);
        }
      }
    }
  }

  const paths = {};
  for (const [key, item] of Object.entries(spec.paths ?? {})) {
    const next = { ...item };
    for (const method of HTTP_METHODS) {
      const operation = next[method];
      if (!operation?.responses) continue;
      const responses = { ...operation.responses };
      let changed = false;
      for (const [code, response] of Object.entries(responses)) {
        if (!code.startsWith('2')) continue;
        const ours = Object.keys(response?.content ?? {});
        const theirs = theirMedia.get(`${method} ${normalizePath(key)} ${code}`);
        if (ours.length !== 1 || !theirs || ours[0] === theirs) continue;
        responses[code] = { ...response, content: { [theirs]: response.content[ours[0]] } };
        changed = true;
      }
      if (changed) next[method] = { ...operation, responses };
    }
    paths[key] = next;
  }
  return { ...spec, paths };
}

/** All preprocessing steps, exported so a test can assert them without oasdiff. */
export function preprocessForDiff(anthropicSpec, orcaSpec) {
  const base = stripBetaPathKeys(anthropicSpec);
  const aligned = alignSuccessCodes(orcaSpec, base);
  return { base, revision: alignResponseMedia(aligned, base) };
}

/**
 * Recursively order keys and arrays.
 *
 * oasdiff's ordering is unstable, and an unstable artifact cannot be
 * drift-gated. Sorting is enough: canonicalized, repeated runs are identical.
 */
export function canonicalize(value) {
  if (Array.isArray(value)) {
    const items = value.map(canonicalize);
    return items.sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : 1));
  }
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonicalize(value[key])]),
    );
  }
  return value;
}

/**
 * Walk an oasdiff subtree and collect its leaves, tagged by direction.
 *
 * oasdiff is called with Anthropic as base and this API as revision, so
 * `deleted` means "Anthropic has it and we do not" and `added` the reverse —
 * the same directional convention every other difference class uses.
 */
function collectLeaves(node, trail, out) {
  if (Array.isArray(node)) {
    for (const item of node) collectLeaves(item, trail, out);
    return;
  }
  if (!node || typeof node !== 'object') return;

  for (const [key, value] of Object.entries(node)) {
    if (key === 'added' || key === 'deleted') {
      addPresence(value, trail, key === 'deleted' ? 'onlyAnthropic' : 'onlyOrca', out);
      continue;
    }
    // A scalar that moved. oasdiff writes `from` as the base (Anthropic) value
    // and `to` as the revision (ours), so the pair is directional on its own.
    if (isChangedScalar(value)) {
      if (value.from !== undefined && value.from !== null && value.from !== '') {
        out.onlyAnthropic.push(`${[...trail, key].join('.')}:${String(value.from)}`);
      }
      if (value.to !== undefined && value.to !== null && value.to !== '') {
        out.onlyOrca.push(`${[...trail, key].join('.')}:${String(value.to)}`);
      }
      continue;
    }
    collectLeaves(value, key === 'modified' ? trail : [...trail, key], out);
  }
}

function isChangedScalar(value) {
  return (
    value &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    ('from' in value || 'to' in value) &&
    Object.keys(value).every((k) => k === 'from' || k === 'to')
  );
}

/**
 * Record an `added` / `deleted` payload.
 *
 * oasdiff spells presence three ways depending on what is being reported: a
 * flat list of names, a list of `{component}` records, or — for parameters — an
 * object keyed by location (`{header: [...], query: [...]}`). Handling only the
 * first is a silent drop: the parameter differences on every operation
 * disappeared until this was fixed, which is precisely the failure this
 * comparison exists to catch.
 */
function addPresence(value, trail, direction, out) {
  const at = (name) => (trail.length ? `${trail.join('.')}:${name}` : String(name));

  if (Array.isArray(value)) {
    for (const item of value) {
      out[direction].push(
        at(typeof item === 'object' ? (item?.component ?? JSON.stringify(item)) : item),
      );
    }
    return;
  }
  if (value && typeof value === 'object') {
    for (const [group, names] of Object.entries(value)) {
      const list = Array.isArray(names) ? names : [names];
      for (const name of list) {
        out[direction].push(
          at(`${group}.${typeof name === 'object' ? JSON.stringify(name) : name}`),
        );
      }
    }
    return;
  }
  if (value !== undefined && value !== null) out[direction].push(at(value));
}

/** Select one parameter location while preserving oasdiff's directional shape. */
function parametersAtLocation(parameters, location) {
  if (!parameters || typeof parameters !== 'object') return undefined;
  const selected = {};
  for (const direction of ['added', 'deleted']) {
    const value = parameters[direction]?.[location];
    if (value !== undefined) selected[direction] = value;
  }
  const modified = parameters.modified?.[location];
  if (modified !== undefined) selected.modified = modified;
  return Object.keys(selected).length > 0 ? selected : undefined;
}

/** Summarize one operation's diff into at most one row per axis. */
function rowsForOperation(pathKey, method, operationDiff) {
  const rows = [];
  const emit = (axis, subtree) => {
    if (!subtree) return;
    const detail = { onlyAnthropic: [], onlyOrca: [] };
    collectLeaves(subtree, [], detail);
    // Path parameters are positional here and their names are cosmetic —
    // `{agent_id}` versus `{id}` is already collapsed by the operation key, and
    // `requiredParameters` excludes them for the same reason. oasdiff reports
    // the name and pattern differences anyway; keeping them would add a row to
    // every operation that says nothing a client can act on.
    detail.onlyAnthropic = detail.onlyAnthropic.filter((entry) => !entry.startsWith('path.'));
    detail.onlyOrca = detail.onlyOrca.filter((entry) => !entry.startsWith('path.'));

    if (!detail.onlyAnthropic.length && !detail.onlyOrca.length) return;
    rows.push({
      class: axis,
      operation: displayKey(method, pathKey),
      display: displayKey(method, pathKey),
      method: method.toUpperCase(),
      path: stripPathKeySuffix(pathKey),
      key: `${axis} ${method.toUpperCase()} ${normalizePath(pathKey)}`,
      note: '',
      onlyAnthropic: [...new Set(detail.onlyAnthropic)].sort(),
      onlyOrca: [...new Set(detail.onlyOrca)].sort(),
    });
  };

  emit('request-schema', operationDiff.requestBody);
  emit('header-parameters', parametersAtLocation(operationDiff.parameters, 'header'));
  emit('query-parameters', parametersAtLocation(operationDiff.parameters, 'query'));

  // Success bodies only. Error-response *shapes* are deliberately out of scope:
  // Anthropic enumerates the whole error taxonomy on nearly every operation,
  // which is already one recorded convention, and folding thousands of envelope
  // leaves in here would bury every other row.
  const responses = operationDiff.responses?.modified ?? {};
  for (const [code, subtree] of Object.entries(responses)) {
    if (code.startsWith('2')) emit('success-schema', subtree);
  }

  return rows;
}

/**
 * Compare wire schemas. Returns difference records in the same shape the rest
 * of the pipeline uses, so they flow into the register and matrix unchanged.
 */
export async function schemaDifferences(anthropicSpec, orcaSpec) {
  const { base, revision } = preprocessForDiff(anthropicSpec, orcaSpec);

  const result = await runOasdiffDiffFromSpecs(base, revision, {
    format: 'json',
    flattenAllOf: true,
    excludeElements: ['description', 'examples', 'extensions', 'summary', 'title'],
  });
  if (result.exitCode !== 0) {
    throw new Error(`oasdiff exited ${result.exitCode}: ${result.stderr || '(no stderr)'}`);
  }

  const diff = canonicalize(JSON.parse(result.stdout || '{}'));
  const rows = [];
  for (const [pathKey, pathDiff] of Object.entries(diff.paths?.modified ?? {})) {
    for (const [method, operationDiff] of Object.entries(pathDiff.operations?.modified ?? {})) {
      rows.push(...rowsForOperation(pathKey, method.toLowerCase(), operationDiff));
    }
  }
  rows.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  return rows;
}
