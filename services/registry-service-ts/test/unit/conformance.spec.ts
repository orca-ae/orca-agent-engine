// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync, readdirSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import { load } from 'js-yaml';
import { describe, expect, it } from 'vitest';
import { classify } from '../../scripts/conformance-core.mjs';
import {
  MUST_BE_CORE,
  MUST_BE_EXTENSIONS,
  ORCA_EXTENSION_TAG,
} from '../../scripts/orca-extension-tagging.mjs';
import { scanOrcaBetaStatuses } from '../../scripts/orca-beta-status-scan.mjs';

/**
 * The invariants the conformance matrix depends on, enforced rather than
 * trusted. Offline: two committed spec files and the route sources, no network
 * and no database.
 *
 * Every assertion in this file names the **two independently sourced things**
 * it compares, because the failure this repository keeps repeating is not a
 * wrong assertion, it is an assertion with only one source. Three shipped tests
 * passed while asserting nothing, and three more were commissioned and caught
 * only in review. The three that would be easiest to write here, and are all
 * worthless, are called out where they would have gone:
 *
 *   - "every operation tagged `orca-extension` is absent from Anthropic's spec"
 *     — the tag is *computed from* that spec, so it is the diff agreeing with
 *     itself, forever.
 *   - "every operation lands in exactly one bucket" — `classify` partitions by
 *     construction; there is no input that fails it.
 *   - "`core + missing === 131`" — arithmetically false (131 raw method entries,
 *     121 unique operations), and the only way to make it pass is to import the
 *     normalizer, at which point both sides share the semantics the invariant
 *     exists to check independently.
 */

/**
 * The HTTP methods an OpenAPI path item may carry, spelled out here rather than
 * imported.
 *
 * This is the one assumption the raw walk below shares with the code it checks.
 * Importing `HTTP_METHODS` from `scripts/lib/normalize-operation.mjs` would make
 * a method the differ forgot invisible to the test that exists to catch a
 * forgotten operation.
 */
const HTTP_METHOD_KEYS = ['get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace'];

const readText = (relative: string) => readFileSync(new URL(relative, import.meta.url), 'utf8');

const anthropicSpec = JSON.parse(readText('../../vendor/anthropic/openapi.json'));
const orcaSpec = load(readText('../../openapi/managed-agents.yaml')) as {
  paths: Record<string, Record<string, { tags?: string[] }>>;
};

const result = classify(anthropicSpec, orcaSpec);

interface RawKey {
  method: string;
  pathKey: string;
  /**
   * `METHOD` plus the path key with any query suffix removed.
   *
   * The second — and last — assumption shared with the code under test: a `?` in
   * an OpenAPI *path key* begins a suffix (Stainless writes
   * `/v1/agents?beta=true` to mark a beta variant), not part of the request
   * path. That is a fact about the document format. The differ's actual
   * semantics — reducing `{agent_id}` and `{id}` to the same positional form,
   * preferring the beta variant of a collapsed pair, refusing a collision it
   * cannot resolve — is not reproduced here, which is the point.
   */
  display: string;
}

/** Every `(METHOD, path key)` an OpenAPI document declares, walked naively. */
function rawOperationKeys(spec: { paths?: Record<string, unknown> }): RawKey[] {
  const keys: RawKey[] = [];
  for (const [pathKey, pathItem] of Object.entries(spec.paths ?? {})) {
    if (!pathItem || typeof pathItem !== 'object') continue;
    if ('$ref' in pathItem) {
      throw new Error(
        `rawOperationKeys: path item ${pathKey} uses $ref; resolve it before classifying operations`,
      );
    }
    for (const method of HTTP_METHOD_KEYS) {
      const operation = (pathItem as Record<string, unknown>)[method];
      if (!operation || typeof operation !== 'object') continue;
      keys.push({
        method: method.toUpperCase(),
        pathKey,
        display: `${method.toUpperCase()} ${pathKey.split('?')[0]}`,
      });
    }
  }
  return keys;
}

const rawAnthropicKeys = rawOperationKeys(anthropicSpec);
const rawOrcaKeys = rawOperationKeys(orcaSpec);

/**
 * The audit lists in `orca-extension-tagging.mjs` against the tags in the
 * committed `openapi/managed-agents.yaml`.
 *
 * **Two sources.** One: `MUST_BE_EXTENSIONS` / `MUST_BE_CORE`, hand-written from
 * reading Anthropic's spec, which is what a human claims the boundary is. Two:
 * the `orca-extension` tags in the checked-in artifact, which is what the
 * generator last wrote. Both wrong together only if someone changed the audit
 * literal *and* the artifact in one edit to agree with each other, which is the
 * deliberate act this cannot and should not defend against.
 *
 * **The tags are read, never recomputed.** Recomputing them from Anthropic's
 * spec would collapse the two sides into one — that is the first tautology named
 * above. Reading the committed file also catches a hand-edited spec, which the
 * generator's own tripwire structurally cannot: it runs before the file is
 * written, against a freshly computed classification.
 */
describe('the extension boundary in the committed spec', () => {
  const published = new Set(rawOrcaKeys.map((key) => key.display));
  const tagged = new Set(
    rawOrcaKeys
      .filter((key) =>
        (orcaSpec.paths[key.pathKey]?.[key.method.toLowerCase()]?.tags ?? []).includes(
          ORCA_EXTENSION_TAG,
        ),
      )
      .map((key) => key.display),
  );

  it('still publishes every operation the audit pins, in either direction', () => {
    // The quiet failure: a deleted route satisfies "is not tagged" for free, so
    // `MUST_BE_CORE` would pass by vacuum. Checked first so the two assertions
    // below are known to be about operations that exist.
    expect([...MUST_BE_EXTENSIONS, ...MUST_BE_CORE].filter((op) => !published.has(op))).toEqual([]);
  });

  it('tags every operation the audit says Anthropic does not publish', () => {
    expect(MUST_BE_EXTENSIONS.filter((op) => !tagged.has(op))).toEqual([]);
  });

  it('leaves untagged the operation the audit says Anthropic does publish', () => {
    // `GET /v1/skills/{id}/versions/{version}/content`. Publishing it as an Orca
    // extension is the specific mistake that started this work, so it is pinned
    // in the opposite direction from everything above.
    expect(MUST_BE_CORE.filter((op) => tagged.has(op))).toEqual([]);
  });

  it('carries as many tags as a fresh classification would produce', () => {
    // A *staleness* check, and nothing more. It compares the committed
    // artifact's tag count against what the generator would compute right now,
    // so a hand-added or hand-deleted tag disagrees.
    //
    // Read it for exactly that much. It is not evidence the boundary is
    // *correct* — the two sides are the artifact and a rerun of the code that
    // wrote it, so a wrong classification agrees with itself. `MUST_BE_*` above
    // is the only assertion here with a human-sourced side. It is also weaker
    // than the `openapi:gen && git diff --exit-code` gate in CI, which catches
    // any staleness rather than a change in count; it earns its place by
    // failing in a plain `pnpm test`, where that gate does not run.
    expect(tagged.size).toBe(result.counts.extension);
  });
});

/**
 * Success codes, our spec against Anthropic's, for every operation both publish.
 *
 * **Two sources.** One: `vendor/anthropic/openapi.json`, whose sha256 is
 * recorded in `PINNED.json` and recomputed locally by `pnpm anthropic:sync`. Two:
 * `openapi/managed-agents.yaml`, generated from `src/contracts/*.contract.ts`.
 * Both wrong together would require the vendored upstream document to have been
 * replaced by something derived from our contracts, which the pin exists to
 * prevent.
 *
 * The permitted-divergence list is derived from the route sources, never
 * hardcoded — and that derivation is only non-circular because
 * `scanOrcaBetaStatuses` pins its site count. Without the pin, a developer
 * adding a conditional status would extend the exception list *by writing the
 * code*, and the exception list would justify itself. The two halves are one
 * mechanism; neither is worth keeping alone.
 */
describe('success codes on operations both specs publish', () => {
  const srcRoot = fileURLToPath(new URL('../../src/', import.meta.url));
  const collect = (dir: string, out: { file: string; text: string }[] = []) => {
    for (const name of readdirSync(dir)) {
      const path = join(dir, name);
      if (statSync(path).isDirectory()) collect(path, out);
      else if (path.endsWith('.ts'))
        out.push({ file: path.slice(srcRoot.length), text: readFileSync(path, 'utf8') });
    }
    return out;
  };
  const scan = scanOrcaBetaStatuses(collect(srcRoot));

  it('scans the route sources without finding anything it cannot account for', () => {
    expect(scan.problems).toEqual([]);
  });

  it('keeps every core success-code set aligned with Anthropic', () => {
    const differences = result.core
      .filter(
        ({ anthropic, orca }) => anthropic.successCodes.join(',') !== orca.successCodes.join(','),
      )
      .map(
        ({ anthropic, orca }) =>
          `${orca.display} — Anthropic ${anthropic.successCodes.join(', ') || '—'}, Orca ${orca.successCodes.join(', ') || '—'}`,
      );
    expect(differences).toEqual([]);
  });

  it('has no success status selected by `orca-beta`', () => {
    expect(scan.successSites).toEqual([]);
    expect([...scan.byOperation]).toEqual([]);
  });
});

/**
 * Every operation either spec declares is accounted for by the classification.
 *
 * **Two sources.** One: a naive walk of the two committed spec documents,
 * collecting `(METHOD, raw path key)` and stripping nothing but a query suffix.
 * Two: the `display` strings `classify` *recorded* for the entries it produced.
 * The test recomputes none of the differ's semantics, so a `collectOperations`
 * that dropped an operation — through a `$ref` path item, a swallowed query
 * suffix, or a collision it merged instead of reporting — leaves a raw key with
 * nothing recording it.
 *
 * A count assertion would not catch any of that: both sides shrink together.
 * Anthropic's spec declares 131 raw method entries and 121 unique operations,
 * because ten collapse into a `?beta=true` twin, so `core + missing === 131` is
 * false as well as useless.
 *
 * Buckets are checked by construction: the Anthropic-side lookup is built from
 * `core` and `missing` only, so an Anthropic operation recorded as an
 * `extension` is unaccounted for, and vice versa.
 */
describe('coverage of the raw operation keys in both specs', () => {
  it('rejects path-item refs instead of silently dropping their operations', () => {
    expect(() =>
      rawOperationKeys({
        paths: {
          '/v1/reused': { $ref: '#/components/pathItems/Reused' },
        },
      }),
    ).toThrow('path item /v1/reused uses $ref');
  });

  // Guard against the walk collapsing: `rawOperationKeys` returning nothing
  // would satisfy every assertion below by vacuum, which is precisely the
  // failure mode this file exists to eliminate. Merging only ever reduces, so
  // there is never less raw than classified.
  it('walks at least as many raw keys as the classification produced operations', () => {
    expect(rawAnthropicKeys.length).toBeGreaterThanOrEqual(result.counts.anthropicOperations);
    // Exact on our side, which is itself worth pinning: we publish no `?beta=true`
    // twins, so nothing of ours is ever merged away before it is classified.
    expect(rawOrcaKeys.length).toBe(result.counts.orcaOperations);
  });

  it('records every operation Anthropic declares as either core or missing', () => {
    const recorded = new Set([
      ...result.core.map((entry) => entry.anthropic.display),
      ...result.missing.map((entry) => entry.display),
    ]);
    expect(
      rawAnthropicKeys.filter((key) => !recorded.has(key.display)).map((k) => k.display),
    ).toEqual([]);
  });

  it('records every operation we declare as either core or an extension', () => {
    const recorded = new Set([
      ...result.core.map((entry) => entry.orca.display),
      ...result.extension.map((entry) => entry.display),
    ]);
    expect(rawOrcaKeys.filter((key) => !recorded.has(key.display)).map((k) => k.display)).toEqual(
      [],
    );
  });

  it('invents no operation that neither spec declares', () => {
    // The other direction. A classification entry with no raw key behind it
    // would be the differ describing a surface that does not exist — the
    // mirror-image of a dropped operation, and just as invisible to a count.
    const rawAnthropic = new Set(rawAnthropicKeys.map((key) => key.display));
    const rawOrca = new Set(rawOrcaKeys.map((key) => key.display));
    expect(
      result.core.filter((entry) => !rawAnthropic.has(entry.anthropic.display)).map((e) => e.key),
    ).toEqual([]);
    expect(
      result.core.filter((entry) => !rawOrca.has(entry.orca.display)).map((e) => e.key),
    ).toEqual([]);
    expect(
      result.missing.filter((entry) => !rawAnthropic.has(entry.display)).map((e) => e.key),
    ).toEqual([]);
    expect(
      result.extension.filter((entry) => !rawOrca.has(entry.display)).map((e) => e.key),
    ).toEqual([]);
  });
});
