// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Render the composed public contract into `openapi/managed-agents.yaml`.
 *
 * Run with `pnpm openapi:gen`. Executed under `tsx` because the contract it
 * renders is TypeScript; everything else follows the repo's generator
 * precedent (`packages/skill-store/scripts/generate-unicode-case-fold.mjs`):
 * plain `.mjs`, top-level await, `node:fs/promises`, paths resolved from
 * `import.meta.url`, and a loud failure in preference to a wrong artifact.
 *
 * The emitted file is a build artifact that is checked in. CI regenerates it
 * and fails on `git diff`, so editing it by hand only produces a red build.
 */
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import SwaggerParser from '@apidevtools/swagger-parser';
import { dump } from 'js-yaml';
import { buildOpenApiDocument } from '../src/contracts/openapi-schema.js';
import { schemaConstrainsNothing } from '../src/contracts/openapi-normalize.js';
import { HTTP_METHODS, normalizePath } from './lib/normalize-operation.mjs';
import { tagOrcaExtensions } from './orca-extension-tagging.mjs';

const packageJsonUrl = new URL('../package.json', import.meta.url);
const anthropicSpecUrl = new URL('../vendor/anthropic/openapi.json', import.meta.url);
const outputDirUrl = new URL('../openapi/', import.meta.url);
const outputUrl = new URL('managed-agents.yaml', outputDirUrl);

const { version } = JSON.parse(await readFile(packageJsonUrl, 'utf8'));
if (typeof version !== 'string' || version.length === 0) {
  throw new Error('package.json has no `version`; the OpenAPI document needs one');
}

const document = buildOpenApiDocument({ version });

const pathKeys = Object.keys(document.paths ?? {});
if (pathKeys.length === 0) {
  throw new Error('the composed contract produced no paths; refusing to write an empty spec');
}

// Two invariants worth failing on rather than publishing. Both would otherwise
// surface downstream as a mysteriously wrong conformance matrix.
for (const pathKey of pathKeys) {
  if (pathKey.startsWith('/internal/')) {
    throw new Error(
      `\`${pathKey}\` is an internal workload route and must not appear in the public spec`,
    );
  }
  if (pathKey.includes(':')) {
    throw new Error(`\`${pathKey}\` still carries ts-rest \`:param\` syntax; expected \`{param}\``);
  }
}

// OpenAPI treats two path templates that differ only in the *names* of their
// parameters as the same path, and forbids declaring both. The collision check
// below is keyed on (method, path) and so cannot see it: `GET .../{resource_id}`
// and `DELETE .../{rsc_id}` are distinct operations sitting on what OAS
// considers one path item.
const templates = new Map();
for (const pathKey of Object.keys(document.paths)) {
  const normalized = normalizePath(pathKey);
  const previous = templates.get(normalized);
  if (previous && previous !== pathKey) {
    throw new Error(
      `\`${pathKey}\` and \`${previous}\` differ only in path-parameter names; OpenAPI treats ` +
        'them as the same path and forbids declaring both. Give both routes the same parameter name.',
    );
  }
  templates.set(normalized, pathKey);
}

// Distinct (method, normalized path) pairs must stay distinct: two contract
// routes that collide after normalization would silently merge in the matrix.
const seen = new Map();
for (const [pathKey, pathItem] of Object.entries(document.paths)) {
  for (const method of HTTP_METHODS) {
    if (!pathItem[method]) continue;
    const key = `${method.toUpperCase()} ${normalizePath(pathKey)}`;
    const previous = seen.get(key);
    if (previous) {
      throw new Error(
        `\`${method.toUpperCase()} ${pathKey}\` and \`${previous}\` normalize to the same ` +
          `operation (${key}); the conformance matrix cannot tell them apart`,
      );
    }
    seen.set(key, `${method.toUpperCase()} ${pathKey}`);
  }
}

// Label the operations Anthropic does not publish. Computed from the vendored
// spec by the same differ that produces the conformance matrix, and pinned by an
// independently written tripwire that fails this script in both directions. The
// check runs before anything is written, so a tripped tripwire leaves the
// committed artifact untouched rather than replacing it with a mislabelled one.
let anthropicSpec;
try {
  anthropicSpec = JSON.parse(await readFile(anthropicSpecUrl, 'utf8'));
} catch (cause) {
  throw new Error(
    `cannot read ${anthropicSpecUrl.pathname}: run \`pnpm anthropic:sync\` first. The extension ` +
      'tag is computed from Anthropic’s spec and cannot be produced without it.',
    { cause },
  );
}

const tagging = tagOrcaExtensions(document, anthropicSpec);
if (!tagging.pathsUnchanged || !tagging.operationsUnchanged) {
  throw new Error('extension tagging altered the published operation set; it must only label');
}

// `paths` and `components.schemas` are lookup maps with no meaningful order, so
// sort them: a route added mid-file then produces a diff local to that route
// instead of shifting everything after it.
document.paths = Object.fromEntries(
  Object.entries(document.paths).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
);
if (document.components?.schemas) {
  document.components.schemas = Object.fromEntries(
    Object.entries(document.components.schemas).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
  );
}

// A schema that constrains nothing is worse than a missing one: it looks like a
// description and generates a client with no usable parameter. `z.unknown()`
// renders this way, which is how the File upload shipped with `{nullable: true}`
// as its entire multipart body. Checked over every published media schema rather
// than the routes we happen to know about, because the same fix was applied to
// Files and missed on both Skill uploads.
//
// The predicate is shared with the nullable rule in `openapi-normalize.ts`
// rather than restated here. Restated, it listed `nullable` and `description`
// and not `readOnly` — so `z.never()` on the three SSE routes rendered as
// `{ readOnly: true }` and walked straight through the check written to catch
// exactly that.
const vacuous = [];
for (const [pathKey, pathItem] of Object.entries(document.paths)) {
  for (const method of HTTP_METHODS) {
    const operation = pathItem[method];
    if (!operation) continue;
    const media = [
      ['request body', operation.requestBody?.content],
      ...Object.entries(operation.responses ?? {}).map(([code, response]) => [
        `response ${code}`,
        response?.content,
      ]),
    ];
    for (const [what, content] of media) {
      for (const [mediaType, entry] of Object.entries(content ?? {})) {
        if (schemaConstrainsNothing(entry?.schema)) {
          vacuous.push(`${method.toUpperCase()} ${pathKey} — ${what} (${mediaType})`);
        }
      }
    }
  }
}
if (vacuous.length > 0) {
  throw new Error(
    [
      'these operations publish a schema that constrains nothing:',
      ...vacuous.map((entry) => `  - ${entry}`),
      '',
      'A client generated from this document gets no usable parameter for them. Declare the real',
      'media type on the route with `openApiMedia(...)` in src/contracts/openapi-media.ts.',
    ].join('\n'),
  );
}

// Validate before writing, so an invalid document is never what gets committed.
// The published spec is what a regenerated SDK is built from; shipping one that
// standard tooling rejects hands every client author the same broken artifact.
// `validate` is given a deep clone because it dereferences `$ref`s in place, and
// the document still has to be serialized as authored.
try {
  await SwaggerParser.validate(structuredClone(document));
} catch (cause) {
  throw new Error(
    `the generated document is not a valid OpenAPI ${document.openapi} spec: ${cause.message}`,
    { cause },
  );
}

const yaml = dump(document, {
  // ts-rest reuses schema objects between operations. Without `noRefs` js-yaml
  // emits YAML anchors/aliases for them, which makes the artifact depend on
  // object identity and unreadable to anything that is not a YAML parser.
  noRefs: true,
  lineWidth: -1,
  sortKeys: false,
});

const header = [
  '# Orca Managed Agents — public HTTP API.',
  '#',
  '# Generated by scripts/generate-openapi.mjs from src/contracts/*.contract.ts.',
  '# Run `pnpm openapi:gen` to regenerate. Do not edit by hand: CI regenerates',
  '# this file and fails the build if the result differs from what is committed.',
  '',
].join('\n');

await mkdir(outputDirUrl, { recursive: true });
await writeFile(outputUrl, `${header}${yaml}`, 'utf8');

process.stdout.write(
  `openapi/managed-agents.yaml: ${pathKeys.length} paths, ${seen.size} operations, ` +
    `${tagging.tagged.length} tagged \`orca-extension\`\n`,
);
