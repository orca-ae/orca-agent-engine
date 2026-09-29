// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The conformance differ, as a pure module.
 *
 * No file I/O, no top-level await, no process access: `generate-conformance.mjs`
 * is the shell that reads and writes, and a unit test imports this file directly
 * with hand-written specs. Everything here is a function of its arguments.
 *
 * What it answers: how does our published API differ from Anthropic's published
 * API, operation by operation, with each difference reduced to something a
 * decision can be pinned to.
 */
import { createHash } from 'node:crypto';
import {
  HTTP_METHODS,
  displayKey,
  isBetaPathKey,
  normalizePath,
  stripPathKeySuffix,
} from './lib/normalize-operation.mjs';

/** The difference classes a decision can be registered against. */
export const DIFFERENCE_CLASSES = [
  'missing',
  'extension',
  'success-codes',
  'response-media',
  'required-params',
  'request-schema',
  'success-schema',
  'header-parameters',
  'query-parameters',
  'error-codes',
  'prose-invariant',
];

/**
 * What this tool compares, and — just as importantly — what it does not.
 *
 * Published in the matrix and used to generate every completeness claim there,
 * because the failure this register kept repeating was prose asserting more
 * coverage than the mechanism delivered. A claim written by hand drifts from
 * the code; a claim generated from this list cannot.
 *
 * Adding an axis means adding it here. Removing one means the matrix says so.
 */
export const COVERAGE = {
  compared: [
    ['operation presence', 'whether each side serves the operation at all'],
    ['success status codes', '2xx codes, compared strictly'],
    ['error status codes', 'declared 4xx/5xx codes'],
    ['request body schema', 'types, required properties, enums, nullability (via oasdiff)'],
    [
      'success response schema',
      'the 2xx body, aligned across a 200/201 and a media-type divergence so neither hides it (via oasdiff)',
    ],
    ['success response media types', 'the media types a 2xx body is offered under'],
    ['header parameters', 'header parameters and their schemas (via oasdiff)'],
    ['query parameters', 'query parameters and their schemas (via oasdiff)'],
    ['documented beta-header rules', 'hand-transcribed; see the prose-invariant section'],
  ],
  notCompared: [
    [
      'error response schemas',
      'Anthropic enumerates the whole taxonomy on nearly every operation; the shape delta is recorded once as a convention rather than per operation',
    ],
    ['descriptions, examples, titles', 'prose about the API, not the wire'],
    [
      'security schemes',
      'ours are declared (`src/contracts/openapi-security.ts`) and pinned by a test against the one route `src/auth/auth.ts` exempts; Anthropic publishes none at all, so there is no upstream statement to diff against',
    ],
    ['servers', 'deployment addresses, not the wire contract'],
    ['webhooks, callbacks', 'neither side publishes any'],
  ],
};

/**
 * Classes where the *direction* of the difference is the thing being decided.
 *
 * For `missing` and `extension`, naming the operation says everything: it is
 * absent here or absent there. For the classes below, the same operation can
 * differ in opposite directions at different times, and the rationales are not
 * interchangeable — "we answer 201 where they answer 200" does not justify the
 * reverse, and "we accept a field they require" does not justify rejecting
 * input they consider valid.
 *
 * So a decision on these classes must state the delta it approves, and stops
 * matching when the delta changes. Otherwise a safe broadening that later
 * becomes a narrowing keeps sailing through the gate under the old reasoning,
 * with nothing unmatched and nothing unused to show for it.
 */
export const DIRECTION_SENSITIVE_CLASSES = ['success-codes', 'required-params', 'response-media'];

/** Classes whose row contents must remain pinned even when their row count does not change. */
export const CONTENT_FINGERPRINT_CLASSES = [
  'request-schema',
  'success-schema',
  'header-parameters',
  'query-parameters',
];

/** The decisions a difference may carry. */
export const DECISION_VOCABULARY = ['keep', 'not-implemented', 'accepted-deviation', 'fix-later'];

/** Canonical form of a difference's direction, for comparing rule to reality. */
function deltaFingerprint(onlyAnthropic, onlyOrca) {
  const side = (list) => [...(list ?? [])].map(String).sort().join(',');
  return `anthropic:[${side(onlyAnthropic)}] orca:[${side(onlyOrca)}]`;
}

/** Stable digest of every directional atom approved by one aggregate rule. */
export function aggregateDeltaFingerprint(differences) {
  const records = differences
    .map((difference) => ({
      key: String(difference.key ?? `${difference.class} ${difference.operation}`),
      onlyAnthropic: [...(difference.onlyAnthropic ?? [])].map(String).sort(),
      onlyOrca: [...(difference.onlyOrca ?? [])].map(String).sort(),
    }))
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
  return `sha256:${createHash('sha256').update(JSON.stringify(records)).digest('hex')}`;
}

/**
 * Whether a match rule still covers this difference's direction.
 *
 * Non-direction-sensitive classes and rules that declared `anyDelta` always do.
 * A rule that named a delta covers only that exact delta.
 */
function approvesDelta(match, difference) {
  if (!DIRECTION_SENSITIVE_CLASSES.includes(difference.class)) return true;
  if (match.anyDelta === true) return true;
  return (
    deltaFingerprint(match.onlyAnthropic, match.onlyOrca) ===
    deltaFingerprint(difference.onlyAnthropic, difference.onlyOrca)
  );
}

const isSuccessCode = (code) => /^2\d\d$/.test(code);

/**
 * Follow a local `$ref` chain to the schema it names.
 *
 * Anthropic's spec refs almost every request body out to `components/schemas`,
 * so without this the required-property comparison sees nothing on either side
 * and silently reports full agreement.
 */
function resolveRef(spec, schema, seen = new Set()) {
  let current = schema;
  while (current && typeof current.$ref === 'string') {
    const ref = current.$ref;
    if (seen.has(ref)) return {}; // self-referential schema; nothing more to learn
    seen.add(ref);
    if (!ref.startsWith('#/')) return {}; // external refs are not resolvable offline
    let target = spec;
    for (const segment of ref.slice(2).split('/')) {
      target = target?.[segment.replace(/~1/g, '/').replace(/~0/g, '~')];
    }
    if (!target) return {};
    current = target;
  }
  return current ?? {};
}

/**
 * Required top-level properties of a JSON request body, including those pulled
 * in through `allOf` composition.
 */
function requiredBodyProperties(spec, operation) {
  const media = operation.requestBody?.content?.['application/json'];
  if (!media?.schema) return [];
  return [...requiredOf(spec, media.schema)].sort();
}

/**
 * What a caller must send no matter which shape of the body they choose.
 *
 * `allOf` contributes everything each branch requires, because all of them
 * apply at once. A union contributes only the **intersection** of its branches:
 * a property required on one branch is not required of the request, but one
 * required on every branch is, and dropping the union entirely loses that.
 *
 * That distinction is not academic. Anthropic publishes a single `file` branch
 * on `POST /v1/sessions/{id}/resources`, requiring `file_id`; we publish three
 * branches whose only shared requirement is `type`. Ignoring unions reported no
 * difference at all for an endpoint where a caller written against their spec
 * sends a field ours does not insist on — precisely the kind of divergence the
 * register exists to surface.
 */
function requiredOf(spec, schemaOrRef, seen = new Set()) {
  const schema = resolveRef(spec, schemaOrRef);
  if (!schema || typeof schema !== 'object') return new Set();

  // Recursive schemas are rare here but cheap to guard, and a cycle would
  // otherwise hang the generator rather than fail it.
  if (seen.has(schema)) return new Set();
  seen.add(schema);

  const required = new Set(Array.isArray(schema.required) ? schema.required : []);

  for (const branch of Array.isArray(schema.allOf) ? schema.allOf : []) {
    for (const name of requiredOf(spec, branch, new Set(seen))) required.add(name);
  }

  for (const keyword of ['oneOf', 'anyOf']) {
    const branches = schema[keyword];
    if (!Array.isArray(branches) || branches.length === 0) continue;
    const perBranch = branches.map((branch) => requiredOf(spec, branch, new Set(seen)));
    const shared = perBranch.reduce(
      (accumulator, current) => new Set([...accumulator].filter((name) => current.has(name))),
    );
    for (const name of shared) required.add(name);
  }

  return required;
}

/**
 * The set of things a caller *must* send, in a form comparable across specs.
 *
 * Path parameters are excluded on purpose: their names are cosmetic
 * (`{agent_id}` vs `{id}`) and their positions are already part of the operation
 * key, so including them would report a difference on nearly every operation and
 * bury the real ones.
 */
function requiredParameters(spec, operation) {
  const required = [];
  for (const parameter of operation.parameters ?? []) {
    const resolved = resolveRef(spec, parameter);
    if (!resolved.required || resolved.in === 'path') continue;
    required.push(`${resolved.in}:${resolved.name}`);
  }
  for (const property of requiredBodyProperties(spec, operation)) {
    required.push(`body:${property}`);
  }
  return required.sort();
}

/**
 * Media types a 2xx response is offered under, as a set.
 *
 * Collected without the status code on purpose. If success statuses diverge,
 * keying media by code would report that difference a second time in a class
 * that is not about status codes.
 */
function successMediaTypes(operation) {
  const media = new Set();
  for (const [code, response] of Object.entries(operation.responses ?? {})) {
    if (!isSuccessCode(code)) continue;
    for (const mediaType of Object.keys(response?.content ?? {})) media.add(mediaType);
  }
  return [...media].sort();
}

/**
 * Flatten an OpenAPI document into operations keyed by `(method, normalized path)`.
 *
 * Anthropic publishes the same operation twice for the handful of endpoints that
 * exist in both GA and beta form — once bare and once under a `?beta=true` path
 * key. They collapse to one key here, and the beta variant wins: everything in
 * the managed-agents surface is beta, so the beta shape is the one a client of
 * this API would be built against.
 */
export function collectOperations(spec) {
  if (!spec?.paths || typeof spec.paths !== 'object') {
    throw new Error('collectOperations: argument is not an OpenAPI document (no `paths`)');
  }
  const operations = new Map();
  for (const [pathKey, pathItem] of Object.entries(spec.paths)) {
    if (!pathItem || typeof pathItem !== 'object') continue;
    const beta = isBetaPathKey(pathKey);
    for (const method of HTTP_METHODS) {
      const operation = pathItem[method];
      if (!operation || typeof operation !== 'object') continue;

      const key = `${method.toUpperCase()} ${normalizePath(pathKey)}`;
      const codes = Object.keys(operation.responses ?? {});
      const record = {
        key,
        beta,
        method: method.toUpperCase(),
        display: displayKey(method, pathKey),
        path: stripPathKeySuffix(pathKey),
        operationId: operation.operationId ?? null,
        summary: operation.summary ?? null,
        successCodes: codes.filter(isSuccessCode).sort(),
        successMedia: successMediaTypes(operation),
        errorCodes: codes.filter((code) => !isSuccessCode(code)).sort(),
        requiredParams: requiredParameters(spec, operation),
      };

      const existing = operations.get(key);
      if (!existing) {
        operations.set(key, record);
      } else if (record.beta && !existing.beta) {
        operations.set(key, record);
      } else if (!record.beta && existing.beta) {
        // keep the beta variant already stored
      } else {
        throw new Error(
          `two operations collapse to \`${key}\` with no beta variant to prefer: ` +
            `\`${existing.display}\` and \`${record.display}\``,
        );
      }
    }
  }
  return operations;
}

const setDifference = (a, b) => a.filter((entry) => !b.includes(entry));

/**
 * Compare the two specs and produce the classification plus a flat list of
 * differences, each one a thing a decision must be registered against.
 *
 * Success (2xx) codes are compared strictly — a create that answers `201` where
 * Anthropic answers `200` is a real, client-visible divergence. Error codes and
 * required parameters are reported informationally: Anthropic enumerates sixteen
 * error codes on Create Agent alone, and treating that as a hard difference
 * would bury everything else.
 */
export function classify(anthropicSpec, orcaSpec, proseInvariants, schemaRows = []) {
  const anthropic = collectOperations(anthropicSpec);
  const orca = collectOperations(orcaSpec);

  const core = [];
  const missing = [];
  const extension = [];
  const differences = [];

  for (const [key, upstream] of anthropic) {
    const ours = orca.get(key);
    if (!ours) {
      missing.push(upstream);
      differences.push({
        class: 'missing',
        operation: upstream.display,
        method: upstream.method,
        path: upstream.path,
        key,
        note: upstream.operationId ?? upstream.summary ?? '',
        onlyAnthropic: [],
        onlyOrca: [],
      });
      continue;
    }
    core.push({ key, anthropic: upstream, orca: ours });

    const successOnlyAnthropic = setDifference(upstream.successCodes, ours.successCodes);
    const successOnlyOrca = setDifference(ours.successCodes, upstream.successCodes);
    if (successOnlyAnthropic.length || successOnlyOrca.length) {
      differences.push({
        class: 'success-codes',
        operation: ours.display,
        method: ours.method,
        path: ours.path,
        key,
        note: `anthropic ${upstream.successCodes.join(', ') || '—'} / orca ${ours.successCodes.join(', ') || '—'}`,
        onlyAnthropic: successOnlyAnthropic,
        onlyOrca: successOnlyOrca,
      });
    }

    // Reported here, from the untouched specs, because `alignResponseMedia` in
    // `schema-diff.mjs` re-keys the body so oasdiff still compares the schemas —
    // the same division of labour `success-codes` has with `alignSuccessCodes`.
    const mediaOnlyAnthropic = setDifference(upstream.successMedia, ours.successMedia);
    const mediaOnlyOrca = setDifference(ours.successMedia, upstream.successMedia);
    if (mediaOnlyAnthropic.length || mediaOnlyOrca.length) {
      differences.push({
        class: 'response-media',
        operation: ours.display,
        method: ours.method,
        path: ours.path,
        key,
        note: `anthropic ${upstream.successMedia.join(', ') || '—'} / orca ${ours.successMedia.join(', ') || '—'}`,
        onlyAnthropic: mediaOnlyAnthropic,
        onlyOrca: mediaOnlyOrca,
      });
    }

    const paramsOnlyAnthropic = setDifference(upstream.requiredParams, ours.requiredParams);
    const paramsOnlyOrca = setDifference(ours.requiredParams, upstream.requiredParams);
    if (paramsOnlyAnthropic.length || paramsOnlyOrca.length) {
      differences.push({
        class: 'required-params',
        operation: ours.display,
        method: ours.method,
        path: ours.path,
        key,
        note: '',
        onlyAnthropic: paramsOnlyAnthropic,
        onlyOrca: paramsOnlyOrca,
      });
    }

    const errorsOnlyAnthropic = setDifference(upstream.errorCodes, ours.errorCodes);
    const errorsOnlyOrca = setDifference(ours.errorCodes, upstream.errorCodes);
    if (errorsOnlyAnthropic.length || errorsOnlyOrca.length) {
      differences.push({
        class: 'error-codes',
        operation: ours.display,
        method: ours.method,
        path: ours.path,
        key,
        note: '',
        onlyAnthropic: errorsOnlyAnthropic,
        onlyOrca: errorsOnlyOrca,
      });
    }
  }

  for (const [key, ours] of orca) {
    if (anthropic.has(key)) continue;
    extension.push(ours);
    differences.push({
      class: 'extension',
      operation: ours.display,
      method: ours.method,
      path: ours.path,
      key,
      note: ours.operationId ?? '',
      onlyAnthropic: [],
      onlyOrca: [],
    });
  }

  // Refuse to publish a nonsense diff. If nothing matched, path normalization is
  // broken — most likely Anthropic's `?beta=true` path-key suffix stopped being
  // stripped — and the matrix would confidently report the entire surface as
  // simultaneously missing and an extension.
  if (core.length === 0) {
    throw new Error(
      `no operation matched between the two specs (${anthropic.size} upstream, ${orca.size} ours). ` +
        'Path normalization is broken; refusing to publish a matrix that reports the whole ' +
        'surface as missing.',
    );
  }

  // Wire-schema rows arrive already computed by `scripts/lib/schema-diff.mjs`,
  // which shells out to oasdiff. They are appended rather than derived here so
  // this module stays synchronous and unit-testable without a binary.
  differences.push(...schemaRows);

  for (const invariant of proseInvariants?.invariants ?? []) {
    differences.push({
      class: 'prose-invariant',
      operation: invariant.id,
      display: invariant.id,
      method: '',
      path: invariant.paths.join(', '),
      key: `PROSE ${invariant.id}`,
      note: invariant.source,
      onlyAnthropic: [invariant.anthropicRequires],
      onlyOrca: [],
      orcaBehaviour: invariant.orcaBehaviour,
    });
  }

  const classOrder = new Map(DIFFERENCE_CLASSES.map((name, index) => [name, index]));
  differences.sort(
    (a, b) =>
      classOrder.get(a.class) - classOrder.get(b.class) ||
      (a.path < b.path ? -1 : a.path > b.path ? 1 : 0) ||
      (a.method < b.method ? -1 : a.method > b.method ? 1 : 0),
  );

  const byPath = (a, b) =>
    a.path < b.path
      ? -1
      : a.path > b.path
        ? 1
        : a.method < b.method
          ? -1
          : a.method > b.method
            ? 1
            : 0;

  return {
    counts: {
      anthropicOperations: anthropic.size,
      orcaOperations: orca.size,
      core: core.length,
      missing: missing.length,
      extension: extension.length,
      differences: differences.length,
    },
    core: core.sort((a, b) => byPath(a.orca, b.orca)),
    missing: missing.sort(byPath),
    extension: extension.sort(byPath),
    differences,
  };
}

/** Translate a decision-file glob (`*` matches any run of characters) to a regexp. */
function globToRegExp(glob) {
  const escaped = glob.replace(/[.+^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`^${escaped.replace(/\*/g, '.*')}$`);
}

/**
 * Validate the decision register's own shape.
 *
 * Returns a list of human-readable problems; empty means the file is well
 * formed. Separate from matching so a malformed file reports *why* it is
 * malformed rather than surfacing as a wall of unmatched differences.
 */
export function validateDecisions(register) {
  const problems = [];
  if (!register || typeof register !== 'object') {
    return ['conformance-decisions.yaml did not parse to an object'];
  }
  if (register.version !== 1) {
    problems.push(`unsupported decision-file version: ${JSON.stringify(register.version)}`);
  }
  if (!Array.isArray(register.decisions)) {
    return [...problems, '`decisions` must be a list'];
  }
  const ids = new Set();
  for (const [index, entry] of register.decisions.entries()) {
    const where = `decisions[${index}]${entry?.id ? ` (${entry.id})` : ''}`;
    if (!entry || typeof entry !== 'object') {
      problems.push(`${where}: not a mapping`);
      continue;
    }
    if (typeof entry.id !== 'string' || !entry.id) problems.push(`${where}: missing \`id\``);
    else if (ids.has(entry.id)) problems.push(`${where}: duplicate \`id\``);
    else ids.add(entry.id);

    if (!DECISION_VOCABULARY.includes(entry.decision)) {
      problems.push(
        `${where}: \`decision\` must be one of ${DECISION_VOCABULARY.join(', ')} (got ${JSON.stringify(entry.decision)})`,
      );
    }
    if (typeof entry.reference !== 'string' || !entry.reference.trim()) {
      problems.push(`${where}: \`reference\` is required — a doc anchor or issue`);
    }
    const match = entry.match;
    if (!match || typeof match !== 'object') {
      problems.push(`${where}: \`match\` is required`);
      continue;
    }
    if (!DIFFERENCE_CLASSES.includes(match.class)) {
      problems.push(
        `${where}: \`match.class\` must be one of ${DIFFERENCE_CLASSES.join(', ')} (got ${JSON.stringify(match.class)})`,
      );
    }
    const hasExact = typeof match.operation === 'string' && match.operation.length > 0;
    const hasGlob = typeof match.path === 'string' && match.path.length > 0;
    if (hasExact === hasGlob) {
      problems.push(
        `${where}: \`match\` needs exactly one of \`operation\` (exact) or \`path\` (glob)`,
      );
    }

    // A glob covers an open-ended set, so it must say how large that set is.
    // An exact-operation rule already names its single target.
    if (hasGlob && !Number.isInteger(entry.covers)) {
      problems.push(
        `${where}: a \`path\` glob must declare \`covers: <n>\` — how many differences it accounts ` +
          'for — so a new one joining the family fails the build instead of being absorbed',
      );
    }
    if (hasExact && entry.covers !== undefined) {
      problems.push(`${where}: \`covers\` is meaningless on an exact \`operation\` rule`);
    }

    // The prose class is hand-written input; a glob there would pre-approve
    // requirements nobody has read yet.
    if (match.class === 'prose-invariant' && !hasExact) {
      problems.push(
        `${where}: \`prose-invariant\` decisions must name an exact invariant \`operation\` id; a ` +
          'glob silently approves prose requirements that do not exist yet',
      );
    }

    if (CONTENT_FINGERPRINT_CLASSES.includes(match.class)) {
      if (!/^sha256:[0-9a-f]{64}$/.test(entry.deltaFingerprint ?? '')) {
        problems.push(
          `${where}: \`${match.class}\` decisions must declare \`deltaFingerprint: sha256:<64 hex>\` ` +
            'so a changed row cannot inherit an old rationale while `covers` stays constant',
        );
      }
    } else if (entry.deltaFingerprint !== undefined) {
      problems.push(
        `${where}: \`deltaFingerprint\` is only valid for ${CONTENT_FINGERPRINT_CLASSES.join(', ')}`,
      );
    }

    if (DIRECTION_SENSITIVE_CLASSES.includes(match.class)) {
      const statesDelta = Array.isArray(match.onlyAnthropic) || Array.isArray(match.onlyOrca);
      // `anyDelta` is how a rule says "any direction, on purpose" — a family
      // convention rather than a judgement about one delta. Requiring it to be
      // written out keeps breadth a claim someone made, not an omission.
      if (!statesDelta && match.anyDelta !== true) {
        problems.push(
          `${where}: \`${match.class}\` is direction-sensitive, so \`match\` must state the delta it ` +
            'approves (`onlyAnthropic` / `onlyOrca`) or declare `anyDelta: true`',
        );
      }
      if (statesDelta && match.anyDelta === true) {
        problems.push(`${where}: \`anyDelta: true\` contradicts an explicit delta`);
      }
    } else if (match.anyDelta !== undefined || match.onlyAnthropic || match.onlyOrca) {
      problems.push(
        `${where}: \`${match.class}\` is not direction-sensitive; drop \`onlyAnthropic\` / ` +
          '`onlyOrca` / `anyDelta`',
      );
    }
  }
  return problems;
}

/**
 * Every anchor a markdown file offers: explicit `<a id>` / `<a name>` tags, plus
 * the slug GitHub derives from each heading.
 *
 * Both forms are load-bearing here. `api-groups-and-extensions.md` writes
 * `### <a id="probes"></a>The health probes` — a slug-only reader calls that
 * reference broken, and a validator that cries wolf is worse than none, because
 * the next person routes around it.
 */
function markdownAnchors(body) {
  const found = new Set();
  for (const m of body.matchAll(/<a\s+(?:id|name)=["']([^"']+)["']/g)) found.add(m[1]);
  for (const m of body.matchAll(/^#{1,6}\s+(.*)$/gm)) {
    const text = m[1].replace(/<a\s+[^>]*>|<\/a>/g, '');
    found.add(
      text
        .trim()
        .toLowerCase()
        .replace(/[^\w\s-]/g, '')
        .replace(/\s+/g, '-')
        .replace(/^-+|-+$/g, ''),
    );
  }
  return found;
}

/**
 * Resolve every `reference:` in the register to a real doc, and a real anchor
 * within it.
 *
 * `validateDecisions` only checks that the string is non-empty, which is how a
 * reference survives the file it points at being deleted. That is not
 * hypothetical: retiring `delivery-phases.md` left four rules — 30 operations —
 * aimed at a file that no longer existed, and nothing failed. A decision whose
 * reasoning cannot be reached is the same as no decision.
 *
 * Takes its file access as callbacks so `conformance-core.mjs` stays pure and
 * unit-testable; the I/O shell passes the real ones.
 *
 * @param register parsed `conformance-decisions.yaml`
 * @param io `{ exists(relPath) -> boolean, read(relPath) -> string }`, both
 *   resolving paths relative to the repository root
 */
export function validateReferences(register, io) {
  const problems = [];
  if (!Array.isArray(register?.decisions)) return problems;

  const anchorCache = new Map();
  for (const [index, entry] of register.decisions.entries()) {
    const reference = entry?.reference;
    if (typeof reference !== 'string' || !reference.trim()) continue; // validateDecisions owns this
    const where = `decisions[${index}]${entry?.id ? ` (${entry.id})` : ''}`;

    // An issue URL is a legitimate reference and is not ours to resolve.
    if (/^https?:\/\//.test(reference)) continue;

    const [path, anchor] = reference.split('#', 2);
    if (!io.exists(path)) {
      problems.push(`${where}: \`reference\` points at ${path}, which does not exist`);
      continue;
    }
    if (!anchor) continue;

    if (!anchorCache.has(path)) anchorCache.set(path, markdownAnchors(io.read(path)));
    if (!anchorCache.get(path).has(anchor)) {
      problems.push(
        `${where}: ${path} has no anchor \`#${anchor}\` — no heading slugs to it and no ` +
          '`<a id>` declares it',
      );
    }
  }
  return problems;
}

/**
 * Check the hand-maintained prose invariants.
 *
 * Held to the same standard as the decision register, and for the same reason:
 * this is the one input a human writes, so it is the one most able to drift into
 * something that reads authoritative and is not. The citation is mandatory —
 * an invariant nobody can trace back to a page is a rumour.
 */
export function validateProseInvariants(file) {
  const problems = [];
  if (!file || typeof file !== 'object') {
    return ['anthropic-prose-invariants.yaml did not parse to an object'];
  }
  if (file.version !== 1) {
    problems.push(`unsupported invariant-file version: ${JSON.stringify(file.version)}`);
  }
  if (!Array.isArray(file.invariants)) {
    return [...problems, '`invariants` must be a list'];
  }
  const ids = new Set();
  for (const [index, invariant] of file.invariants.entries()) {
    const where = `invariants[${index}]${invariant?.id ? ` (${invariant.id})` : ''}`;
    if (!invariant || typeof invariant !== 'object') {
      problems.push(`${where}: not a mapping`);
      continue;
    }
    for (const field of ['id', 'family', 'anthropicRequires', 'orcaBehaviour', 'source']) {
      if (typeof invariant[field] !== 'string' || !invariant[field].trim()) {
        problems.push(`${where}: \`${field}\` is required`);
      }
    }
    if (!Array.isArray(invariant.paths) || invariant.paths.length === 0) {
      problems.push(`${where}: \`paths\` must be a non-empty list of globs`);
    }
    if (typeof invariant.source === 'string' && !invariant.source.startsWith('https://')) {
      problems.push(`${where}: \`source\` must be the https URL this was transcribed from`);
    }
    if (typeof invariant.id === 'string') {
      if (ids.has(invariant.id)) problems.push(`${where}: duplicate \`id\``);
      else ids.add(invariant.id);
    }
  }

  // Two *families* covering the same path cannot both be the rule for it. This
  // is not hypothetical: a `/v1/*` row asserting the managed-agents beta value
  // sat directly above rows saying memory endpoints require a different one, and
  // nothing noticed until review.
  //
  // Within a family, overlap is expected — the memory family states both which
  // value is required and that two values together are rejected. Those are
  // complementary facts about one rule, not competing claims.
  //
  // Competing claims only arise between families of the same `kind` of
  // requirement (default: 'beta-header'). A response-ordering rule and a
  // beta-header rule on the same path describe different dimensions of the
  // operation and cannot contradict each other, so they are exempt from the
  // pairwise check — while two rules of one kind on one path still clash.
  const entries = file.invariants.filter(
    (i) => Array.isArray(i?.paths) && typeof i?.family === 'string',
  );
  const kindOf = (entry) =>
    typeof entry.kind === 'string' && entry.kind.trim() ? entry.kind : 'beta-header';
  for (let i = 0; i < entries.length; i += 1) {
    for (let j = i + 1; j < entries.length; j += 1) {
      const [a, b] = [entries[i], entries[j]];
      if (a.family === b.family) continue;
      if (kindOf(a) !== kindOf(b)) continue;
      const clash = a.paths.find((x) => b.paths.some((y) => globsOverlap(x, y)));
      if (!clash) continue;
      problems.push(
        `families \`${a.family}\` and \`${b.family}\` both claim \`${clash}\` (via \`${a.id}\` and ` +
          `\`${b.id}\`); a path can only be governed by one \`${kindOf(a)}\` rule, so scope them apart`,
      );
    }
  }
  return problems;
}

/**
 * Whether two path globs can match the same path.
 *
 * Deliberately conservative: it reports an overlap whenever one glob's literal
 * prefix is a prefix of the other's. A false positive costs someone a rescope;
 * a false negative is the contradiction shipping again.
 */
function globsOverlap(a, b) {
  const literal = (glob) => glob.split('*')[0];
  const [x, y] = [literal(a), literal(b)];
  return x.startsWith(y) || y.startsWith(x);
}

/**
 * Pin every difference to a decision, and every decision to a difference.
 *
 * The pin cuts both ways on purpose. A difference with no decision is an
 * unrecorded divergence. A decision that matches nothing is a dead entry that
 * still reads like coverage — the failure mode that makes a register worthless
 * a year after it is written. Both fail the build.
 *
 * Exact `operation` matches beat `path` globs, and among globs the longest
 * pattern wins, so a broad family rule can be overridden for one operation
 * without reordering the file. Two rules that tie are an error rather than a
 * coin flip.
 */
export function matchDecisions(differences, register) {
  const entries = (register?.decisions ?? []).map((entry, index) => ({
    entry,
    index,
    used: 0,
    matched: [],
  }));
  const resolved = [];
  const unmatched = [];
  const ambiguous = [];

  for (const difference of differences) {
    const candidates = entries
      .filter(({ entry }) => entry.match?.class === difference.class)
      // A rule that pinned a delta only applies while that delta holds. When it
      // reverses, the rule drops out and the difference lands in `unmatched` —
      // the build fails asking for a decision about what is true now, rather
      // than accepting an old rationale for a new divergence.
      .filter(({ entry }) => approvesDelta(entry.match, difference));
    const exact = candidates.filter(({ entry }) => entry.match.operation === difference.operation);
    let winners = exact;
    if (winners.length === 0) {
      const globbed = candidates.filter(
        ({ entry }) =>
          typeof entry.match.path === 'string' &&
          globToRegExp(entry.match.path).test(difference.path),
      );
      if (globbed.length > 0) {
        const longest = Math.max(...globbed.map(({ entry }) => entry.match.path.length));
        winners = globbed.filter(({ entry }) => entry.match.path.length === longest);
      }
    }

    if (winners.length === 0) {
      unmatched.push(difference);
      resolved.push({ ...difference, decision: null, reference: null, decisionId: null });
      continue;
    }
    if (winners.length > 1) {
      ambiguous.push({
        difference,
        ids: winners.map(({ entry, index }) => entry.id ?? `decisions[${index}]`),
      });
    }
    const winner = winners[0];
    winner.used += 1;
    winner.matched.push(difference);
    resolved.push({
      ...difference,
      decision: winner.entry.decision,
      reference: winner.entry.reference,
      decisionId: winner.entry.id,
      rationale: winner.entry.rationale ?? null,
    });
  }

  // A glob rule that quietly grows is how a register stops meaning anything: the
  // family absorbs a difference nobody decided about, and the both-ways gate
  // stays green because the rule is neither unmatched nor unused. Pinning the
  // count turns "this family" into a claim with a number attached, so a new
  // member has to be looked at.
  const miscounted = entries
    .filter(({ entry }) => typeof entry.covers === 'number' && entry.covers !== undefined)
    .filter(({ entry, used }) => entry.covers !== used)
    .map(({ entry, used }) => ({ id: entry.id, covers: entry.covers, actual: used }));

  const misfingerprinted = entries
    .filter(({ entry, used }) => entry.deltaFingerprint !== undefined && used > 0)
    .map(({ entry, matched }) => ({
      id: entry.id,
      expected: entry.deltaFingerprint,
      actual: aggregateDeltaFingerprint(matched),
    }))
    .filter(({ expected, actual }) => expected !== actual);

  return {
    resolved,
    unmatched,
    ambiguous,
    miscounted,
    misfingerprinted,
    unused: entries.filter(({ used }) => used === 0).map(({ entry }) => entry),
    usage: new Map(entries.map(({ entry, used }) => [entry.id, used])),
  };
}
