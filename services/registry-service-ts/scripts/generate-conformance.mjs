// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Diff our published spec against Anthropic's, pin every difference to a
 * decision, and render `docs/managed-agents/conformance-matrix.md`.
 *
 * Run with `pnpm conformance:gen`, after `pnpm openapi:gen` — the matrix has to
 * be generated from the spec that run just produced, or it describes a surface
 * that no longer exists.
 *
 * This file is the I/O shell. All the logic lives in `conformance-core.mjs`,
 * which is pure and importable by a test.
 */
import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { readFile, writeFile } from 'node:fs/promises';
import { load } from 'js-yaml';
import {
  COVERAGE,
  DIFFERENCE_CLASSES,
  classify,
  matchDecisions,
  validateDecisions,
  validateProseInvariants,
  validateReferences,
} from './conformance-core.mjs';
import { SCHEMA_AXES, schemaDifferences } from './lib/schema-diff.mjs';

const anthropicSpecUrl = new URL('../vendor/anthropic/openapi.json', import.meta.url);
const pinnedUrl = new URL('../vendor/anthropic/PINNED.json', import.meta.url);
const orcaSpecUrl = new URL('../openapi/managed-agents.yaml', import.meta.url);
const decisionsUrl = new URL('../conformance-decisions.yaml', import.meta.url);
const proseInvariantsUrl = new URL('../anthropic-prose-invariants.yaml', import.meta.url);
const matrixUrl = new URL('../../../docs/managed-agents/conformance-matrix.md', import.meta.url);

/** Read a file as raw bytes. Callers decode; the vendored spec is also hashed. */
async function readOrExplain(url, hint) {
  try {
    return await readFile(url);
  } catch (cause) {
    throw new Error(`cannot read ${url.pathname}: ${hint}`, { cause });
  }
}

const anthropicSpecBytes = await readOrExplain(anthropicSpecUrl, 'run `pnpm anthropic:sync` first');
const pinned = JSON.parse(
  (await readOrExplain(pinnedUrl, 'run `pnpm anthropic:sync` first')).toString('utf8'),
);

// Verify the vendored bytes against the pin before anything is derived from
// them. Every digest this run prints, and every row of the matrix, is a claim
// about *these* bytes; PR CI never runs `anthropic:sync`, so without this check
// an altered or truncated vendored spec would be diffed and committed while the
// matrix went on citing the digest of a file that is no longer there.
const specDigest = pinned.sha256_of_openapi_json_computed_locally;
if (typeof specDigest !== 'string' || !/^[0-9a-f]{64}$/.test(specDigest)) {
  throw new Error(
    'PINNED.json has no locally computed sha256 (expected 64 lowercase hex characters); ' +
      're-run `pnpm anthropic:sync`',
  );
}
const actualDigest = createHash('sha256').update(anthropicSpecBytes).digest('hex');
if (actualDigest !== specDigest) {
  throw new Error(
    [
      'vendor/anthropic/openapi.json does not match its pin.',
      `  PINNED.json: ${specDigest}`,
      `  actual:      ${actualDigest}`,
      '',
      'Run `pnpm anthropic:sync` if upstream moved, or restore the file if it was',
      'edited locally. Refusing to publish a matrix that cites a digest of bytes it',
      'did not read.',
    ].join('\n'),
  );
}

const anthropicSpec = JSON.parse(anthropicSpecBytes.toString('utf8'));
const orcaSpec = load(
  (await readOrExplain(orcaSpecUrl, 'run `pnpm openapi:gen` first')).toString('utf8'),
);
const register = load(
  (await readOrExplain(decisionsUrl, 'the decision register is required')).toString('utf8'),
);
const proseInvariants = load(
  (await readOrExplain(proseInvariantsUrl, 'the prose-invariant file is required')).toString(
    'utf8',
  ),
);

const registerProblems = validateDecisions(register);
if (registerProblems.length > 0) {
  process.stderr.write(
    [
      'conformance-decisions.yaml is malformed:',
      ...registerProblems.map((p) => `  - ${p}`),
      '',
    ].join('\n'),
  );
  process.exit(1);
}

// A decision is only as good as the reasoning a reader can reach. Resolved
// against the repo root, since references are written repo-relative.
const repoRoot = new URL('../../../', import.meta.url);
const referenceProblems = validateReferences(register, {
  exists: (relPath) => existsSync(new URL(relPath, repoRoot)),
  read: (relPath) => readFileSync(new URL(relPath, repoRoot), 'utf8'),
});
if (referenceProblems.length > 0) {
  process.stderr.write(
    [
      'conformance-decisions.yaml has unreachable references:',
      ...referenceProblems.map((p) => `  - ${p}`),
      '',
      '  A `reference` is where a difference goes to be explained. If it does not',
      '  resolve, the decision is undocumented no matter what the matrix prints.',
      '',
    ].join('\n'),
  );
  process.exit(1);
}

const invariantProblems = validateProseInvariants(proseInvariants);
if (invariantProblems.length > 0) {
  process.stderr.write(
    [
      'anthropic-prose-invariants.yaml is malformed:',
      ...invariantProblems.map((p) => `  - ${p}`),
      '',
    ].join('\n'),
  );
  process.exit(1);
}

const schemaRows = await schemaDifferences(anthropicSpec, orcaSpec);
const result = classify(anthropicSpec, orcaSpec, proseInvariants, schemaRows);
const { resolved, unmatched, ambiguous, unused, miscounted, misfingerprinted } = matchDecisions(
  result.differences,
  register,
);

// The gate, both directions. Nothing is written when it fails: a matrix with
// blank Decision cells is worse than no matrix, because it still reads as an
// answer.
const failures = [];
if (unmatched.length > 0) {
  failures.push(
    `${unmatched.length} difference(s) have no decision in conformance-decisions.yaml:`,
    ...unmatched.map((d) => `  - [${d.class}] ${d.operation}`),
    '',
    '  Add a match rule for each. A difference nobody decided about is exactly what',
    '  this register exists to prevent.',
  );
}
if (unused.length > 0) {
  failures.push(
    `${unused.length} decision(s) in conformance-decisions.yaml match no difference:`,
    ...unused.map((entry) => `  - ${entry.id} (${JSON.stringify(entry.match)})`),
    '',
    '  Delete them. If the divergence was fixed, deleting its decision is part of the fix;',
    '  a dead entry still reads as coverage.',
  );
}
if (miscounted.length > 0) {
  failures.push(
    `${miscounted.length} glob rule(s) no longer cover the number of differences they claim:`,
    ...miscounted.map((m) => `  - ${m.id}: declares covers ${m.covers}, matched ${m.actual}`),
    '',
    '  A family grew or shrank. Confirm the recorded rationale still applies to every member,',
    '  then update `covers`. Absorbing a new difference into an old rule silently is how a',
    '  register stops describing anything.',
  );
}
if (misfingerprinted.length > 0) {
  failures.push(
    `${misfingerprinted.length} schema rule(s) no longer approve the same directional atoms:`,
    ...misfingerprinted.map((m) => `  - ${m.id}: declares ${m.expected}, actual ${m.actual}`),
    '',
    '  A row changed while the family count stayed constant. Re-read every changed atom and',
    '  update the rationale and `deltaFingerprint` together.',
  );
}
if (ambiguous.length > 0) {
  failures.push(
    `${ambiguous.length} difference(s) match more than one decision at the same specificity:`,
    ...ambiguous.map(({ difference, ids }) => `  - ${difference.operation} → ${ids.join(', ')}`),
  );
}
if (failures.length > 0) {
  process.stderr.write(['conformance gate failed.', '', ...failures, ''].join('\n'));
  process.exit(1);
}

const specCitation = `\`${specDigest.slice(0, 12)}\``;

/** One line per difference class, so the summary cannot omit one silently. */
const CLASS_MEANINGS = {
  missing: 'Anthropic publishes it, we do not serve it',
  extension: 'we serve it, Anthropic does not publish it',
  'success-codes': '2xx codes differ (compared strictly)',
  'response-media': 'the media types a 2xx body is offered under differ',
  'required-params': 'required query/header/body parameters differ',
  'request-schema': 'request body shapes differ (via oasdiff)',
  'success-schema': 'success response shapes differ (via oasdiff)',
  'header-parameters': 'declared header parameters or their schemas differ (via oasdiff)',
  'query-parameters': 'declared query parameters or their schemas differ (via oasdiff)',
  'error-codes': 'declared error statuses differ (informational)',
  'prose-invariant': 'documented behaviour their spec does not encode — hand-transcribed',
};
const undocumentedClass = DIFFERENCE_CLASSES.find((name) => !CLASS_MEANINGS[name]);
if (undocumentedClass) {
  throw new Error(`difference class \`${undocumentedClass}\` has no summary meaning; add one`);
}

const SCHEMA_SECTION = {
  'request-schema': 'Request body differences',
  'success-schema': 'Success response differences',
  'header-parameters': 'Header parameter differences',
  'query-parameters': 'Query parameter differences',
};
const SCHEMA_BLURB = {
  'request-schema':
    'Compared by oasdiff with `allOf` flattened and descriptions, examples and vendor extensions ' +
    'excluded, so what is reported is what a client can observe rather than how either spec is written.',
  'success-schema':
    'The 2xx body. Success codes are aligned before comparison so a status-code difference cannot ' +
    'hide every difference in the body underneath it. Any status-code difference is reported ' +
    'separately under success codes.',
  'header-parameters':
    'Header parameters and their schemas. Authentication representation is discussed separately ' +
    'from query constraints so equivalent security declarations cannot hide observable input rules.',
  'query-parameters':
    'Query parameters and their schemas. These rows are isolated from header representation and ' +
    'fingerprinted so a changed bound or accepted shape requires a new decision.',
};

/** Keep a row readable: differences run to dozens of leaves on the widest operations. */
function summarize(entries, limit = 4) {
  if (!entries.length) return '—';
  const shown = entries.slice(0, limit).map((entry) => `\`${entry.split('.').pop()}\``);
  return entries.length > limit
    ? `${shown.join(', ')} … +${entries.length - limit}`
    : shown.join(', ');
}

const byClass = (name) => resolved.filter((difference) => difference.class === name);
const codes = (list) => (list.length ? list.join(', ') : '—');
const cell = (value) => String(value).replace(/\|/g, '\\|');

function table(headers, rows) {
  if (rows.length === 0) return '_None._\n';
  return [
    `| ${headers.join(' | ')} |`,
    `| ${headers.map(() => '---').join(' | ')} |`,
    ...rows.map((row) => `| ${row.map(cell).join(' | ')} |`),
    '',
  ].join('\n');
}

// The summary is the page's headline claim, so it must account for every row.
// Before this check the table omitted a whole class and read 139 while the
// decisions read 142 — a discrepancy visible only to someone adding it up.
const classTotal = DIFFERENCE_CLASSES.reduce((sum, name) => sum + byClass(name).length, 0);
if (classTotal !== resolved.length) {
  throw new Error(
    `the summary accounts for ${classTotal} differences but ${resolved.length} were resolved; ` +
      'a difference class is missing from DIFFERENCE_CLASSES or from the rendered sections',
  );
}

const distinctErrorDeltas = new Set(
  byClass('error-codes').map((d) => `${d.onlyAnthropic.join(',')}|${d.onlyOrca.join(',')}`),
).size;

const decisionCounts = new Map();
for (const difference of resolved) {
  decisionCounts.set(difference.decision, (decisionCounts.get(difference.decision) ?? 0) + 1);
}

const lines = [
  '# Claude Managed Agents Conformance Matrix',
  '',
  '<!--',
  'Generated by services/registry-service-ts/scripts/generate-conformance.mjs.',
  'Run `pnpm openapi:gen && pnpm conformance:gen` to regenerate. Do not edit by hand:',
  'CI regenerates this file and fails the build if the result differs from what is committed.',
  '-->',
  '',
  `Almost every row below is derived by diffing two machine-readable specs. The ${byClass('prose-invariant').length} rows in`,
  '[Prose invariants](#prose-invariants) are the exception: they are transcribed by hand from',
  "Anthropic's documentation, are labelled as such, and are the only rows here that no mechanism",
  'rechecks. See [`conformance.md`](./conformance.md) for how this is produced and what each',
  'class means.',
  '',
  '## What is compared',
  '',
  'This list is the source of every completeness claim on this page — it is read from the code',
  'that does the comparing, so a claim here cannot outrun the mechanism behind it.',
  '',
  table(
    ['Axis', 'What is checked'],
    COVERAGE.compared.map(([axis, detail]) => [`**${axis}**`, detail]),
  ),
  '**Not compared.** Absence of a row below says nothing about these:',
  '',
  table(
    ['Axis', 'Why not'],
    COVERAGE.notCompared.map(([axis, detail]) => [`**${axis}**`, detail]),
  ),
  '## What was compared',
  '',
  table(
    ['', 'Source', 'Operations'],
    [
      [
        'Anthropic',
        `[\`${pinned.source_url.split('/').pop()}\`](${pinned.source_url})<br>sha256 \`${specDigest}\` (computed locally)`,
        result.counts.anthropicOperations,
      ],
      [
        'Orca',
        '`services/registry-service-ts/openapi/managed-agents.yaml`<br>generated from `src/contracts/*.contract.ts`',
        result.counts.orcaOperations,
      ],
    ],
  ),
  'Operation counts are distinct `(method, normalized path)` pairs. Anthropic publishes a handful',
  'of operations in both GA and beta form under the same path; those collapse to one, beta winning.',
  '',
  '## Summary',
  '',
  table(
    ['Class', 'Count', 'Meaning'],
    [
      ['`core`', result.counts.core, 'operation exists in both specs'],
      // Every difference class, enumerated from the differ itself. A class added
      // to the code and forgotten here is what made the visible counts total 139
      // while the decisions totalled 142.
      ...DIFFERENCE_CLASSES.map((name) => [
        `\`${name}\``,
        name === 'error-codes'
          ? `${byClass(name).length} (${distinctErrorDeltas} distinct deltas)`
          : byClass(name).length,
        CLASS_MEANINGS[name],
      ]),
    ],
  ),
  `Every difference carries a decision: **${resolved.length}** rows across ${DIFFERENCE_CLASSES.length} classes.`,
  '',
  'Decisions registered across all differences: ' +
    [...decisionCounts.entries()]
      .sort(([a], [b]) => (a < b ? -1 : 1))
      .map(([decision, count]) => `\`${decision}\` ${count}`)
      .join(', ') +
    '.',
  '',
  `## Core — served by both (${result.counts.core})`,
  '',
  'These operations exist in both specs. Differences *within* them are listed in the sections below.',
  '',
  table(
    ['Operation', 'Anthropic operationId', 'Anthropic spec'],
    result.core.map(({ anthropic, orca }) => [
      `\`${orca.display}\``,
      anthropic.operationId ? `\`${anthropic.operationId}\`` : '—',
      specCitation,
    ]),
  ),
  `## Missing — Anthropic publishes it, we do not (${result.counts.missing})`,
  '',
  table(
    ['Operation', 'Anthropic operationId', 'Decision', 'Reference', 'Anthropic spec'],
    byClass('missing').map((d) => [
      `\`${d.operation}\``,
      d.note ? `\`${d.note}\`` : '—',
      `\`${d.decision}\``,
      d.reference,
      specCitation,
    ]),
  ),
  `## Extension — we serve it, Anthropic does not publish it (${result.counts.extension})`,
  '',
  table(
    ['Operation', 'Decision', 'Reference', 'Anthropic spec'],
    byClass('extension').map((d) => [
      `\`${d.operation}\``,
      `\`${d.decision}\``,
      d.reference,
      specCitation,
    ]),
  ),
  `## Success-code differences (${byClass('success-codes').length})`,
  '',
  'Compared strictly: a client can observe the status of a successful call.',
  '',
  table(
    ['Operation', 'Anthropic 2xx', 'Orca 2xx', 'Decision', 'Reference', 'Anthropic spec'],
    byClass('success-codes').map((d) => {
      const [, anthropicCodes, orcaCodes] = d.note.match(/^anthropic (.*) \/ orca (.*)$/) ?? [
        null,
        '—',
        '—',
      ];
      return [
        `\`${d.operation}\``,
        anthropicCodes,
        orcaCodes,
        `\`${d.decision}\``,
        d.reference,
        specCitation,
      ];
    }),
  ),
  `## Response media-type differences (${byClass('response-media').length})`,
  '',
  'The media types a successful response is offered under. Reported separately because oasdiff',
  'matches response content by media type, so a difference here would otherwise *replace* the',
  'body comparison rather than add to it — the schemas are aligned onto one media type before',
  'diffing so both are visible.',
  '',
  table(
    ['Operation', 'Anthropic', 'Orca', 'Decision', 'Reference', 'Anthropic spec'],
    byClass('response-media').map((d) => {
      const [, anthropicMedia, orcaMedia] = d.note.match(/^anthropic (.*) \/ orca (.*)$/) ?? [
        null,
        '—',
        '—',
      ];
      return [
        `\`${d.operation}\``,
        `\`${anthropicMedia}\``,
        `\`${orcaMedia}\``,
        `\`${d.decision}\``,
        d.reference,
        specCitation,
      ];
    }),
  ),
  `## Required-parameter differences (${byClass('required-params').length})`,
  '',
  'Informational. Required query/header parameters and required top-level request-body properties.',
  'Path parameters are excluded: their names are cosmetic and their positions are already part of',
  'the operation identity.',
  '',
  table(
    [
      'Operation',
      'Required only by Anthropic',
      'Required only by Orca',
      'Decision',
      'Reference',
      'Anthropic spec',
    ],
    byClass('required-params').map((d) => [
      `\`${d.operation}\``,
      codes(d.onlyAnthropic),
      codes(d.onlyOrca),
      `\`${d.decision}\``,
      d.reference,
      specCitation,
    ]),
  ),
  `## Error-code differences (${byClass('error-codes').length} operations, ${distinctErrorDeltas} distinct deltas)`,
  '',
  'Informational. Anthropic enumerates the platform-wide error taxonomy on nearly every operation',
  '(sixteen statuses on Create Agent alone) or a single `4XX` wildcard on the older ones; this',
  'service declares only the statuses a route actually returns.',
  '',
  table(
    [
      'Operation',
      'Declared only by Anthropic',
      'Declared only by Orca',
      'Decision',
      'Reference',
      'Anthropic spec',
    ],
    byClass('error-codes').map((d) => [
      `\`${d.operation}\``,
      codes(d.onlyAnthropic),
      codes(d.onlyOrca),
      `\`${d.decision}\``,
      d.reference,
      specCitation,
    ]),
  ),
  ...SCHEMA_AXES.flatMap((axis) => [
    `## ${SCHEMA_SECTION[axis]} (${byClass(axis).length})`,
    '',
    SCHEMA_BLURB[axis],
    '',
    table(
      ['Operation', 'Only Anthropic', 'Only Orca', 'Decision', 'Reference'],
      byClass(axis).map((d) => [
        `\`${d.operation}\``,
        summarize(d.onlyAnthropic),
        summarize(d.onlyOrca),
        `\`${d.decision}\``,
        d.reference,
      ]),
    ),
  ]),
  // Explicit anchor: the heading carries its count, which GitHub slugs to
  // `#prose-invariants-6`, so the self-link above would not resolve. The
  // `markdownAnchors` helper recognises this form, so a future link check sees it too.
  '<a id="prose-invariants"></a>',
  '',
  `## Prose invariants (${byClass('prose-invariant').length})`,
  '',
  '**These rows are not derived from a spec diff.** Every other row on this page comes from',
  'comparing two machine-readable documents. These are transcribed by hand from Anthropic’s',
  'documentation into `services/registry-service-ts/anthropic-prose-invariants.yaml`, because',
  'their published OpenAPI does not encode them — it declares `anthropic-beta` as an optional',
  'free-form string on every operation, exactly as we do, so a diff reports conformance on',
  'behaviour that differs.',
  '',
  'They are held to the same both-directions gate as everything else, and each cites the page it',
  'came from. Treat the citation as the authority, not this table: unlike a derived row, nothing',
  'here re-checks itself when upstream changes.',
  '',
  table(
    ['Invariant', 'Applies to', 'Anthropic requires', 'Orca behaviour', 'Decision', 'Source'],
    byClass('prose-invariant').map((d) => [
      `\`${d.operation}\``,
      `\`${d.path}\``,
      d.onlyAnthropic.join('; '),
      d.orcaBehaviour ?? '',
      `\`${d.decision}\``,
      d.note,
    ]),
  ),
];

await writeFile(matrixUrl, `${lines.join('\n').replace(/\n{3,}/g, '\n\n')}`, 'utf8');

process.stdout.write(
  [
    `conformance-matrix.md: ${result.counts.core} core, ${result.counts.missing} missing, ` +
      `${result.counts.extension} extension, ${result.counts.differences} differences`,
    `  all ${result.counts.differences} differences carry a decision; all ${register.decisions.length} decisions match at least one`,
    '',
  ].join('\n'),
);
