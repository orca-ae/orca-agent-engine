// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  MIN_DECLARED,
  collectDeclared,
  collectDocumented,
  collectEnvAccessed,
  commentStyleFor,
  compare,
  indexExemptions,
  isClean,
  isWired,
  MIN_DOCUMENTED,
  isHiddenPath,
  isTestFile,
  isWiringEvidence,
  namesIdentifier,
  stripComments,
  validateExemptions,
  validateExemptionSites,
  validateScanCoverage,
  validateSourceFloors,
} from '../../../../scripts/env-docs-core.mjs';

/**
 * Every case below is a false negative a reviewer reproduced by hand against
 * the first version of this gate, which fused its matching logic to `fs` and so
 * could not be tested at all. The gate reported success while:
 *
 *   - ~39 helper-read variables were invisible to it
 *   - `INTERNAL_HTTP_PORT` in the docs satisfied a check for `HTTP_PORT`
 *   - a variable named in a comment counted as a live consumer
 *
 * The lesson these encode: proving a gate fails for the reasons you imagined is
 * not the same as proving it fails.
 */

/** A problems object with enough scaffolding to exercise one axis at a time. */
const run = ({
  sources = {},
  docs = {},
  runtimeText = '',
  exempt = [],
}: {
  sources?: Record<string, string>;
  docs?: Record<string, string>;
  runtimeText?: string;
  exempt?: { name: string; reason: string; site?: string; note?: string }[];
}) => {
  const declared = collectDeclared(sources);
  const { documented, families } = collectDocumented(docs);
  const exemptions = indexExemptions({ version: 1, exemptions: exempt });
  const envAccessed = collectEnvAccessed(sources);
  return {
    declared,
    ...compare({ declared, documented, families, runtimeText, exemptions, envAccessed }),
  };
};

/** An entry that passes `validateExemptions`, so a case can vary one field. */
const entry = (over: Record<string, unknown> = {}) => ({
  name: 'SOME_VAR',
  reason: 'not-an-env-var',
  note: 'A justification long enough to be a real sentence.',
  ...over,
});

/**
 * Pad BOTH floors so a case can assert on one axis alone.
 *
 * The same count on both sides on purpose: every filler name is declared and
 * documented, so the padding contributes no undocumented entries and no
 * phantoms of its own.
 */
const FILLER_COUNT = Math.max(MIN_DECLARED, MIN_DOCUMENTED);
const filler = Array.from({ length: FILLER_COUNT }, (_, i) => `env['FILLER_${i}']`).join('\n');
const fillerDocs = Array.from({ length: FILLER_COUNT }, (_, i) => `\`FILLER_${i}\``).join('\n');

describe('collectDeclared', () => {
  it('sees a direct env access', () => {
    expect([...collectDeclared({ 'c.ts': "env['HTTP_PORT']" }).keys()]).toEqual(['HTTP_PORT']);
  });

  // The finding: config reads through optionalEnv/requiredEnv/parsePoolMax/…
  // and a regex that knows only `env['X']` misses ~39 variables while
  // reporting success over a surface it cannot see.
  it.each([
    ["optionalEnv(env, 'TRUST_PROXY_CIDRS')", 'TRUST_PROXY_CIDRS'],
    ["requiredEnv(env, 'ORCA_SECRET_STORE_K8S_NAMESPACE')", 'ORCA_SECRET_STORE_K8S_NAMESPACE'],
    ["parsePoolMax(env, 'DATABASE_POOL_MAX', 10)", 'DATABASE_POOL_MAX'],
    ["parseBooleanEnv(env, 'SOME_FLAG', false)", 'SOME_FLAG'],
    ["readKafkaTlsFile(env, 'KAFKA_SSL_CA_PATH')", 'KAFKA_SSL_CA_PATH'],
  ])('collects a variable read through a helper: %s', (source, expected) => {
    expect([...collectDeclared({ 'c.ts': source }).keys()]).toContain(expected);
  });

  // Found by mutation: every fixture used single quotes, so deleting the
  // double-quoted pattern changed nothing any test could see.
  it.each([
    ['optionalEnv(env, "DOUBLE_QUOTED_VAR")', 'DOUBLE_QUOTED_VAR'],
    ['env["BRACKET_DOUBLE_VAR"]', 'BRACKET_DOUBLE_VAR'],
  ])('collects a double-quoted literal: %s', (source, expected) => {
    expect([...collectDeclared({ 'c.ts': source }).keys()]).toContain(expected);
  });

  it('collects a helper the pattern has never seen before', () => {
    // Open on the function name on purpose: pinning the helper list would let
    // the next helper silently shrink the checked surface.
    expect([...collectDeclared({ 'c.ts': "brandNewHelper(env, 'FUTURE_VAR')" }).keys()]).toContain(
      'FUTURE_VAR',
    );
  });
});

/**
 * The property, not the instances.
 *
 * "A mention is not a use" was reported six times, in six different places,
 * because each collector re-asked the question in its own words. These cases
 * pin the rule at every collector at once, so the seventh place cannot ship
 * without failing here.
 */
describe('no collector treats a mention as a use', () => {
  it('collectDeclared ignores a name that only a comment names', () => {
    const commented = "// see 'GHOST_VAR' for the old behaviour\nenv['REAL_VAR'];\n";
    const names = [...collectDeclared({ 'c.ts': stripComments(commented, 'c') }).keys()];
    expect(names).toContain('REAL_VAR');
    expect(names).not.toContain('GHOST_VAR');
  });

  it('isWired ignores a read that only a comment shows', () => {
    expect(isWired(stripComments("// process.env['GHOST_VAR']\n", 'c'), 'GHOST_VAR')).toBe(false);
    expect(isWired(stripComments("process.env['REAL_VAR'];\n", 'c'), 'REAL_VAR')).toBe(true);
  });

  it('namesIdentifier ignores a name that only a comment names', () => {
    expect(namesIdentifier('// GHOST_VAR was here', 'GHOST_VAR')).toBe(false);
    expect(namesIdentifier('const x = GHOST_VAR;', 'GHOST_VAR')).toBe(true);
  });
});

describe('stripComments', () => {
  // `#` is a comment in YAML/shell/Makefile, not in TypeScript — the caller says
  // which, because assuming both is what let a YAML path open a block comment.
  it('drops a hash comment in a hash-style file', () => {
    expect(stripComments('# MEMORY_WATCHER_POLL_INTERVAL_MS is gone', 'hash')).not.toContain(
      'MEMORY',
    );
  });

  it('drops a line comment', () => {
    expect(stripComments('// SOME_VAR was removed')).not.toContain('SOME_VAR');
  });

  it('drops a block comment', () => {
    expect(stripComments('/* SOME_VAR was removed */ const x = 1;')).not.toContain('SOME_VAR');
  });

  // Found by mutation: asserting only that the commented name is GONE passes
  // whether the comment closed or ate the rest of the input. This pins the
  // close — which is the exact failure that deleted 39% of the scanned source.
  it('closes a block comment at the terminator, not at EOF', () => {
    const out = stripComments('/* gone */ process.env.AFTER_COMMENT;\n');
    expect(out).not.toContain('gone');
    expect(out).toContain('AFTER_COMMENT');
  });

  // Found by mutation: nothing pinned the escape rule, so `\\"` closing a string
  // early would have gone unnoticed — and an early close turns the rest of the
  // line into "code", which is how a name in prose becomes a name in source.
  it('does not let an escaped quote close a string', () => {
    // The consequence of losing the escape rule is not the string's contents —
    // those are kept either way — it is that the parser thinks it is back in
    // code, so a `//` INSIDE the string starts a comment and eats the real read
    // that follows.
    const src = 'const m = "a \\" // still string"; process.env.AFTER_ESCAPE;\n';
    expect(stripComments(src, 'c')).toContain('AFTER_ESCAPE');
  });

  // Found by mutation: block comments must preserve their newlines, or every
  // line number after one shifts.
  it('keeps line count stable across a block comment', () => {
    const src = 'a\n/* one\ntwo\nthree */\nb\n';
    expect(stripComments(src, 'c').split('\n').length).toBe(src.split('\n').length);
  });

  it('keeps a hash inside a string, so a URL fragment survives', () => {
    expect(stripComments("const a = 'docs.md#probes';")).toContain('#probes');
  });

  // The block pass used to be a regex that ran BEFORE any quote awareness, so a
  // `/*` inside a string opened a comment that ran to the next `*/` — across
  // file boundaries, because the shell stripped the whole concatenation at once.
  // 26.8% of the scanned surface was being deleted this way.
  it('does not let a slash-star inside a string open a comment', () => {
    const src = "app.mount('/v1/*', handler);\nconst k = process.env['REAL_VAR'];\n";
    expect(stripComments(src)).toContain('REAL_VAR');
  });

  it('does not let one file comment out the next', () => {
    const a = "route('/api/*', h);\n";
    const b = "process.env['SECOND_FILE_VAR'];\n/** doc */\n";
    expect(stripComments([a, b].join('\n'))).toContain('SECOND_FILE_VAR');
  });

  // A JS-shaped scanner reading YAML treats `services/*/Dockerfile` as a comment
  // opener; that swallowed 87% of one workflow file.
  it.each([
    ['- services/*/Dockerfile\nFOO: ${{ secrets.FOO }}\n', 'FOO'],
    ['url: https://example.com/x\nBAR: 1\n', 'BAR'],
  ])('treats %j as data, not comments, in hash-style files', (src, name) => {
    expect(stripComments(src, 'hash')).toContain(name);
  });

  it('still strips the syntax each style really has', () => {
    expect(stripComments('/* GONE */ const x = 1;', 'c')).not.toContain('GONE');
    expect(stripComments('// GONE\n', 'c')).not.toContain('GONE');
    expect(stripComments('# GONE\n', 'hash')).not.toContain('GONE');
    // `#` is not a comment in TypeScript, and `//` is not one in YAML.
    expect(stripComments('const a = 1; # KEPT\n', 'c')).toContain('KEPT');
    expect(stripComments('path: a//KEPT\n', 'hash')).toContain('KEPT');
  });

  it('picks the style from the extension', () => {
    expect(commentStyleFor('services/x/src/a.ts')).toBe('c');
    expect(commentStyleFor('scripts/b.mjs')).toBe('c');
    expect(commentStyleFor('.github/workflows/c.yml')).toBe('hash');
    expect(commentStyleFor('Makefile')).toBe('hash');
    expect(commentStyleFor('charts/x/templates/d.tpl')).toBe('hash');
  });
});

describe('compare', () => {
  it('passes when every variable is documented and every doc has a consumer', () => {
    const r = run({
      sources: { 'c.ts': filler },
      docs: { 'r.md': fillerDocs },
      runtimeText: filler,
    });
    expect(isClean(r)).toBe(true);
  });

  // The finding: `docText.includes('HTTP_PORT')` is satisfied by
  // `INTERNAL_HTTP_PORT`, so deleting both real HTTP_PORT rows still passed.
  it('does not let a longer identifier document a shorter one', () => {
    const r = run({
      sources: { 'c.ts': `${filler}\nenv['HTTP_PORT']` },
      docs: { 'r.md': `${fillerDocs}\n\`INTERNAL_HTTP_PORT\`` },
      runtimeText: filler,
    });
    expect(r.undocumented.map((u) => u.name)).toContain('HTTP_PORT');
  });

  // The finding: `sourceText.includes(v)` counted a mention in a comment as a
  // consumer, so a documented-but-dead knob stayed green.
  it('does not accept a comment as a consumer', () => {
    const source = '# MEMORY_WATCHER_POLL_INTERVAL_MS is no longer read';
    const r = run({
      sources: { 'c.ts': filler },
      docs: { 'r.md': `${fillerDocs}\n\`MEMORY_WATCHER_POLL_INTERVAL_MS\`` },
      runtimeText: stripComments(source),
    });
    expect(r.phantom.map((p) => p.name)).toContain('MEMORY_WATCHER_POLL_INTERVAL_MS');
  });

  it('accepts a real consumer', () => {
    const r = run({
      sources: { 'c.ts': filler },
      docs: { 'r.md': `${fillerDocs}\n\`REAL_VAR\`` },
      runtimeText: stripComments('const x = process.env.REAL_VAR;'),
    });
    expect(r.phantom).toEqual([]);
  });

  // Found by mutation: the family prefix is `name.slice(0, -1)` — dropping only
  // the `*` and keeping the underscore. One char further and `KAFKA_SASL` would
  // also swallow `KAFKA_SASLESQUE`.
  it('derives the family prefix exactly, keeping the underscore', () => {
    const { families } = collectDocumented({ 'r.md': '`KAFKA_SASL_*`' });
    expect([...families]).toEqual(['KAFKA_SASL_']);
  });

  it('honours a documented wildcard family', () => {
    const r = run({
      sources: { 'c.ts': `${filler}\nenv['KAFKA_SASL_USERNAME']` },
      docs: { 'r.md': `${fillerDocs}\n\`KAFKA_SASL_*\`` },
      runtimeText: filler,
    });
    expect(r.undocumented).toEqual([]);
  });

  // An empty `declared` makes the undocumented check vacuously pass — which is
  // precisely the state the broken collector left the gate in.
  it('fails when too few variables were collected to be measuring anything', () => {
    const r = run({ sources: { 'c.ts': "env['ONLY_ONE']" }, docs: {}, runtimeText: '' });
    expect(r.floor).toContain('not measuring anything');
    expect(isClean(r)).toBe(false);
  });

  // The doc side had no floor at all, so six of twelve hand-listed doc files
  // could vanish and the run stayed byte-identically green — the phantom check
  // simply stopped covering whatever they held.
  it('fails when too few documented identifiers were collected', () => {
    const r = run({
      sources: { 'c.ts': filler },
      docs: { 'r.md': '`ONLY_ONE_DOCUMENTED`' },
      runtimeText: filler,
    });
    expect(r.floor).toContain('documented identifiers were collected');
    expect(isClean(r)).toBe(false);
  });

  it('reports BOTH floors when both sides collapse', () => {
    const r = run({ sources: { 'c.ts': "env['ONLY_ONE']" }, docs: {}, runtimeText: '' });
    expect(r.floor).toContain('environment variables were collected');
    expect(r.floor).toContain('documented identifiers were collected');
  });

  // `undocumented` honoured wildcard families; the dead-exemption check did not,
  // so collapsing rows into a `KAFKA_SASL_*` row reported a live exemption dead.
  it('lets a wildcard family satisfy the dead-exemption check too', () => {
    const r = run({
      sources: { 'c.ts': filler },
      docs: { 'r.md': `${fillerDocs}\n\`KAFKA_SASL_*\`` },
      runtimeText: filler,
      exempt: [
        entry({ name: 'KAFKA_SASL_USERNAME', reason: 'wired-elsewhere', site: 'package.json' }),
      ],
    });
    expect(r.dead).toEqual([]);
  });

  // `multi-workspace-isolation.md` names HARNESS_REGISTRY_API_KEY to assert it
  // does not exist. Absence is the invariant, so the gate enforces it rather
  // than flagging the doc.
  it('does not flag a variable documented as intentionally absent', () => {
    const r = run({
      sources: { 'c.ts': filler },
      docs: { 'r.md': `${fillerDocs}\n\`HARNESS_REGISTRY_API_KEY\`` },
      runtimeText: filler,
      exempt: [entry({ name: 'HARNESS_REGISTRY_API_KEY', reason: 'intentionally-absent' })],
    });
    expect(r.phantom).toEqual([]);
    expect(isClean(r)).toBe(true);
  });

  it('fails if a variable documented as absent reappears in config', () => {
    const r = run({
      sources: { 'c.ts': `${filler}\nenv['HARNESS_REGISTRY_API_KEY']` },
      docs: { 'r.md': `${fillerDocs}\n\`HARNESS_REGISTRY_API_KEY\`` },
      runtimeText: filler,
      exempt: [entry({ name: 'HARNESS_REGISTRY_API_KEY', reason: 'intentionally-absent' })],
    });
    expect(r.resurrected.map((x) => x.name)).toEqual(['HARNESS_REGISTRY_API_KEY']);
    expect(isClean(r)).toBe(false);
  });

  // A design doc can land ahead of the code it describes, so it can name a real
  // knob whose reader is not in this tree yet. The reason exists for exactly
  // that, and unlike the others it has no `site` to check — the reading file
  // does not exist in this tree. What keeps it honest is that it expires on its
  // own, in both directions, asserted below.
  it('accepts a variable documented ahead of its implementation', () => {
    const r = run({
      sources: { 'c.ts': filler },
      docs: { 'r.md': `${fillerDocs}\n\`RUNNER_TUNNEL_TOKENS\`` },
      runtimeText: filler,
      exempt: [entry({ name: 'RUNNER_TUNNEL_TOKENS', reason: 'ahead-of-implementation' })],
    });
    expect(r.phantom).toEqual([]);
    expect(isClean(r)).toBe(true);
  });

  it('kills an ahead-of-implementation entry once the reader lands', () => {
    const r = run({
      sources: { 'c.ts': `${filler}\nenv['RUNNER_TUNNEL_TOKENS']` },
      docs: { 'r.md': `${fillerDocs}\n\`RUNNER_TUNNEL_TOKENS\`` },
      runtimeText: filler,
      exempt: [entry({ name: 'RUNNER_TUNNEL_TOKENS', reason: 'ahead-of-implementation' })],
    });
    expect(r.dead.map((x) => x.name)).toEqual(['RUNNER_TUNNEL_TOKENS']);
    expect(isClean(r)).toBe(false);
  });

  it('kills an ahead-of-implementation entry once the docs stop naming it', () => {
    const r = run({
      sources: { 'c.ts': filler },
      docs: { 'r.md': fillerDocs },
      runtimeText: filler,
      exempt: [entry({ name: 'RUNNER_TUNNEL_TOKENS', reason: 'ahead-of-implementation' })],
    });
    expect(r.dead.map((x) => x.name)).toEqual(['RUNNER_TUNNEL_TOKENS']);
    expect(isClean(r)).toBe(false);
  });

  it('rejects a site on an ahead-of-implementation entry', () => {
    const problems = validateExemptions({
      version: 1,
      exemptions: [
        entry({
          name: 'RUNNER_TUNNEL_TOKENS',
          reason: 'ahead-of-implementation',
          site: 'services/registry-service-ts/src/auth/tunnel-auth.ts',
        }),
      ],
    });
    expect(problems.join('\n')).toMatch(/`site` is only meaningful for `wired-elsewhere`/);
  });

  it('accepts a variable wired outside the scanned configs, when declared', () => {
    const r = run({
      sources: { 'c.ts': filler },
      docs: { 'r.md': `${fillerDocs}\n\`SANDBOX_HARNESS_DEFAULT_CWD\`` },
      runtimeText: filler,
      exempt: [
        entry({
          name: 'SANDBOX_HARNESS_DEFAULT_CWD',
          reason: 'wired-elsewhere',
          site: 'services/sandbox-harness/src/subprocess-entry.ts',
        }),
      ],
    });
    expect(r.phantom).toEqual([]);
    expect(isClean(r)).toBe(true);
  });

  it('flags the same variable when nothing declares it', () => {
    const r = run({
      sources: { 'c.ts': filler },
      docs: { 'r.md': `${fillerDocs}\n\`SANDBOX_HARNESS_DEFAULT_CWD\`` },
      runtimeText: filler,
    });
    expect(r.phantom.map((x: { name: string }) => x.name)).toEqual(['SANDBOX_HARNESS_DEFAULT_CWD']);
  });
});

/**
 * The register only earns its place if it shrinks as well as grows. Without
 * these two, an entry outlives the reason it was added and the file starts
 * reading as coverage it no longer provides — which is how four conformance
 * rules ended up pointing at a deleted `delivery-phases.md`.
 */
describe('compare — dead register entries', () => {
  it('fails when a config starts reading an exempted variable', () => {
    const r = run({
      sources: { 'c.ts': `${filler}\nenv['LATE_ARRIVAL']` },
      docs: { 'r.md': `${fillerDocs}\n\`LATE_ARRIVAL\`` },
      runtimeText: filler,
      exempt: [entry({ name: 'LATE_ARRIVAL', reason: 'wired-elsewhere', site: 'package.json' })],
    });
    expect(r.dead.map((x: { name: string }) => x.name)).toEqual(['LATE_ARRIVAL']);
    expect(isClean(r)).toBe(false);
  });

  it('fails when the docs stop mentioning an exempted variable', () => {
    const r = run({
      sources: { 'c.ts': filler },
      docs: { 'r.md': fillerDocs },
      runtimeText: filler,
      exempt: [entry({ name: 'FORGOTTEN_VAR' })],
    });
    expect(r.dead.map((x: { name: string }) => x.name)).toEqual(['FORGOTTEN_VAR']);
    expect(r.dead[0].why).toMatch(/mention it any more/);
  });

  it('does not call an intentionally-absent entry dead just for being absent', () => {
    const r = run({
      sources: { 'c.ts': filler },
      docs: { 'r.md': `${fillerDocs}\n\`HTTP_PROXY\`` },
      runtimeText: filler,
      exempt: [entry({ name: 'HTTP_PROXY', reason: 'intentionally-absent' })],
    });
    expect(r.dead).toEqual([]);
    expect(isClean(r)).toBe(true);
  });
});

describe('validateExemptions — the register must be well formed', () => {
  const shape = (over: Record<string, unknown>) =>
    validateExemptions({ version: 1, exemptions: [entry(over)] });

  it('accepts a well-formed entry', () => {
    expect(validateExemptions({ version: 1, exemptions: [entry()] })).toEqual([]);
  });

  it('rejects an unsupported version', () => {
    expect(validateExemptions({ version: 2, exemptions: [] }).join()).toContain('version');
  });

  it('rejects a reason outside the enum', () => {
    expect(shape({ reason: 'because-i-said-so' }).join()).toContain('must be one of');
  });

  // The note is the entire difference between this register and the loose regex
  // it replaced: both let a name through, only one says why.
  it('rejects an entry with no stated justification', () => {
    expect(shape({ note: 'x' }).join()).toContain('must say why');
  });

  it('rejects wired-elsewhere with no site', () => {
    expect(shape({ reason: 'wired-elsewhere' }).join()).toContain('requires `site`');
  });

  it('rejects a site on a reason that cannot have one', () => {
    expect(shape({ site: 'package.json' }).join()).toContain('only meaningful');
  });

  it('rejects a duplicate name', () => {
    const problems = validateExemptions({ version: 1, exemptions: [entry(), entry()] });
    expect(problems.join()).toContain('duplicate');
  });

  it('rejects a name that is not an identifier', () => {
    expect(shape({ name: 'lower_case' }).join()).toContain('SCREAMING_SNAKE');
  });
});

describe('validateExemptionSites — a justification must be reachable', () => {
  const io = (files: Record<string, string>) => ({
    exists: (p: string) => p in files,
    read: (p: string) => files[p],
  });

  const wired = entry({
    name: 'ORCA_GIT_CREDS_URL',
    reason: 'wired-elsewhere',
    site: 'src/dispatcher.ts',
  });

  it('passes when the site exists and still names the variable', () => {
    const problems = validateExemptionSites(
      { version: 1, exemptions: [wired] },
      io({ 'src/dispatcher.ts': 'ORCA_GIT_CREDS_URL: url' }),
    );
    expect(problems).toEqual([]);
  });

  it('fails when the site was deleted', () => {
    const problems = validateExemptionSites({ version: 1, exemptions: [wired] }, io({}));
    expect(problems.join()).toContain('does not exist');
  });

  // The failure mode that actually happens: the file survives a refactor but
  // the consumer moves out of it, and the entry keeps vouching for nothing.
  it('fails when the site no longer mentions the variable', () => {
    const problems = validateExemptionSites(
      { version: 1, exemptions: [wired] },
      io({ 'src/dispatcher.ts': 'const unrelated = 1;' }),
    );
    expect(problems.join()).toContain('no longer names it outside a comment');
  });

  // Found by the adversarial review, inside the check written to retire exactly
  // this category. Deleting both real uses of ORCA_GIT_CREDS_URL from
  // dispatcher.ts left the doc comment on line 380, and a raw `.includes()`
  // took that as justification — so the exemption kept vouching for a consumer
  // that no longer existed. Fifth appearance of "a mention is not a use".
  it('fails when only a comment in the site names the variable', () => {
    const problems = validateExemptionSites(
      { version: 1, exemptions: [wired] },
      io({ 'src/dispatcher.ts': '/** injected as `ORCA_GIT_CREDS_URL` */\nconst x = 1;' }),
    );
    expect(problems.join()).toContain('no longer names it outside a comment');
  });

  it('accepts a hash-commented YAML file that still has the real entry', () => {
    const problems = validateExemptionSites(
      { version: 1, exemptions: [wired] },
      io({ 'src/dispatcher.ts': '# sets ORCA_GIT_CREDS_URL below\n- name: ORCA_GIT_CREDS_URL\n' }),
    );
    expect(problems).toEqual([]);
  });

  // The second bug this one function shipped with, an hour after the first.
  // Renaming a consumer to a LONGER identifier containing the name kept the
  // exemption valid, because the check used `.includes()` — the substring bug a
  // reviewer had already reported against the doc side, re-acquired by a new
  // place asking the same question in its own words.
  it('does not accept a longer identifier that contains the name', () => {
    const problems = validateExemptionSites(
      { version: 1, exemptions: [wired] },
      io({ 'src/dispatcher.ts': 'const x = process.env.ORCA_GIT_CREDS_URL_LEGACY;' }),
    );
    expect(problems.join()).toContain('no longer names it outside a comment');
  });

  it('ignores reasons that carry no site', () => {
    expect(validateExemptionSites({ version: 1, exemptions: [entry()] }, io({}))).toEqual([]);
  });
});

describe('collectDeclared — shapes the agent review found missing', () => {
  // Live in the tree when this was reported: readS3StaticCredentialPair(env,
  // 'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY') hid a *secret* because the
  // pattern captured only the first group.
  it('collects every literal in a helper call, not just the first', () => {
    const d = collectDeclared({
      'c.ts': "readS3StaticCredentialPair(env, 'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY')",
    });
    expect([...d.keys()]).toEqual(
      expect.arrayContaining(['S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY']),
    );
  });

  it('collects dot access', () => {
    expect([...collectDeclared({ 'c.ts': 'const a = env.SHADOW_TIMEOUT_MS;' }).keys()]).toContain(
      'SHADOW_TIMEOUT_MS',
    );
  });

  it('collects destructuring', () => {
    const d = collectDeclared({ 'c.ts': 'const { SHADOW_MODE, OTHER_FLAG } = env;' });
    expect([...d.keys()]).toEqual(expect.arrayContaining(['SHADOW_MODE', 'OTHER_FLAG']));
  });
});

describe('isWired — a mention is not a use', () => {
  it('counts an env read in runtime code', () => {
    expect(isWired("const x = process.env['REAL_VAR'];", 'REAL_VAR')).toBe(true);
  });

  // These used to pass, via "setting" shapes broad enough that any quoted or
  // colon-adjacent token counted. That is how `MUST_BE_CORE` — an exported
  // const, not a variable — satisfied the phantom check through its own
  // `export const MUST_BE_CORE =`. Wiring outside a scanned config is now
  // declared in `env-exemptions.yaml` instead of guessed at here.
  it('does not count a chart entry or a CI secret as a read', () => {
    expect(
      isWired('- name: ORCA_TRUSTED_SANDBOX_WORKLOADS', 'ORCA_TRUSTED_SANDBOX_WORKLOADS'),
    ).toBe(false);
    expect(isWired('password: ${{ secrets.DOCKERHUB_TOKEN }}', 'DOCKERHUB_TOKEN')).toBe(false);
  });

  it('does not count an unrelated identifier that shares the name', () => {
    expect(isWired("const KNOWN_KEYS = ['SOME_VAR', 'OTHER'];", 'SOME_VAR')).toBe(false);
    expect(isWired('export const MUST_BE_CORE = [];', 'MUST_BE_CORE')).toBe(false);
    expect(isWired('interface Row { MAX_PATH_LENGTH: number }', 'MAX_PATH_LENGTH')).toBe(false);
  });

  it('does not count a quoted name that is really prose', () => {
    expect(isWired("log('# SOME_KNOB is no longer read');", 'SOME_KNOB')).toBe(false);
  });
});

/**
 * The sixth appearance of this category, found by the adversarial review inside
 * the narrower rule meant to contain it.
 *
 * Test files used to be scanned, accepting only a real `process.env.X` and never
 * a name in an assertion. That rule cannot survive a file whose subject is this
 * gate: the string below was, verbatim, an argument in this suite, and no
 * textual scan separates a read from a picture of one. Renaming the two real
 * consumers of `ORCA_E2E_SANDBOX_HARNESS` left the fixture as its only
 * read-shaped occurrence and the gate stayed green — proven by mutation.
 *
 * So `isWired` takes no test text, and the shell never builds any.
 */
describe('isWired — a depiction of a read is not a read', () => {
  it('cannot tell a depicted read from a real one, which is why tests are excluded', () => {
    const depiction = `isWired('', "if (process.env['ORCA_E2E_SANDBOX_HARNESS'])", 'X')`;
    expect(isWired(depiction, 'ORCA_E2E_SANDBOX_HARNESS')).toBe(true);
  });

  it('takes only runtime text and a name', () => {
    expect(isWired.length).toBe(2);
  });
});

/**
 * The shell's file-selection used to be three inline patterns, and one of them
 * — an explicit `.env.example` exclusion — sat after an extension test that
 * already rejected it, so it could never fire. It was deleted with a comment
 * claiming "the invariant is covered by a break-test instead"; the break-test
 * was in a scratchpad, not the repo. A gate asserting a test that does not exist
 * is the exact defect this PR removes from the docs, written in code.
 *
 * So the predicate moved into the pure core and the break-test is these cases.
 */
describe('isWiringEvidence — which files count as a consumer', () => {
  it.each([
    'services/harness-server/src/config.ts',
    'charts/orca-managed-agents/templates/configmap-harness.yaml',
    'services/dev/scripts/start-services.sh',
    '.github/workflows/e2e-stack.yml',
    'Makefile',
  ])('counts %s', (p) => expect(isWiringEvidence(p)).toBe(true));

  // An operator template is documentation. Adding one line to it used to flip
  // this gate green.
  it('does not count .env.example', () => {
    expect(isWiringEvidence('services/dev/.env.example')).toBe(false);
  });

  // Tests hold depictions of reads, which no scan separates from real ones.
  it.each([
    'services/registry-service-ts/test/unit/env-docs-core.spec.ts',
    'packages/e2e-tests/test/wire-conformance.spec.ts',
    'services/harness-server/test/integration/session-adapter.spec.ts',
  ])('does not count %s', (p) => expect(isWiringEvidence(p)).toBe(false));

  // Templates are excluded by the extension allowlist, not by a guard: no path
  // can end in both `.env.example` and an allowed extension, so a dedicated
  // guard is unreachable by construction — it was written three times before
  // that was noticed. These cases pin the outcome; the mechanism is the
  // allowlist.
  it('does not count an operator template', () => {
    expect(isWiringEvidence('services/dev/.env.example')).toBe(false);
    expect(isWiringEvidence('charts/x/values.env.example')).toBe(false);
  });

  // This exclusion IS load-bearing, and this case proves it: the path matches
  // the allowlist, so only the test-file check can reject it. Delete that check
  // and this fails.
  it('excludes a spec whose extension would otherwise qualify', () => {
    expect(isWiringEvidence('services/x/test/unit/a.spec.ts')).toBe(false);
    expect(isWiringEvidence('packages/y/src/b.ts')).toBe(true);
  });

  it('does not count a file with no recognised extension', () => {
    expect(isWiringEvidence('docs/managed-agents/roadmap.md')).toBe(false);
    expect(isWiringEvidence('services/sandbox-harness/Dockerfile')).toBe(false);
  });
});

describe('the two floors', () => {
  it('are both meaningful numbers', () => {
    expect(MIN_DECLARED).toBeGreaterThan(50);
    expect(MIN_DOCUMENTED).toBeGreaterThan(50);
  });
});

describe('namesIdentifier — one implementation of "does this text name this"', () => {
  it('matches an exact occurrence', () => {
    expect(namesIdentifier('- name: HTTP_PORT', 'HTTP_PORT')).toBe(true);
  });

  // `\b` alone is not enough: `_` is a word character, so `\bHTTP_PORT\b`
  // matches inside `INTERNAL_HTTP_PORT`.
  it.each([
    ['INTERNAL_HTTP_PORT', 'HTTP_PORT'],
    ['HTTP_PORT_LEGACY', 'HTTP_PORT'],
    ['XHTTP_PORT', 'HTTP_PORT'],
  ])('rejects %s as evidence for %s', (haystack, needle) => {
    expect(namesIdentifier(`const a = ${haystack};`, needle)).toBe(false);
  });

  it('ignores an occurrence that is only in a comment', () => {
    expect(namesIdentifier('// sets HTTP_PORT somewhere', 'HTTP_PORT')).toBe(false);
    expect(namesIdentifier('# sets HTTP_PORT somewhere', 'HTTP_PORT', 'hash')).toBe(false);
  });
});

/**
 * Everything below was found by mutating the module and watching all 84 tests
 * pass. Each case kills a specific mutant, named in its comment — a behaviour
 * that was correct, deliberate, explained in a comment, and unpinned.
 */
describe('not-an-env-var holds in both directions', () => {
  // Mutant: delete the `not-an-env-var` skip in the undocumented loop.
  // Survived 84/84; only the real gate caught it, and only because BEGIN,
  // SIGINT and SIGTERM happen to be in the tree today.
  it('does not demand documentation for a name it says is not a variable', () => {
    const r = run({
      sources: { 'c.ts': `${filler}\nconst guard = 'BEGIN';` },
      docs: { 'r.md': fillerDocs },
      runtimeText: filler,
      exempt: [entry({ name: 'BEGIN' })],
    });
    expect(r.undocumented).toHaveLength(0);
    expect(isClean(r)).toBe(true);
  });

  it('still demands documentation for a name with no entry', () => {
    const r = run({
      sources: { 'c.ts': `${filler}\nconst guard = 'BEGIN';` },
      docs: { 'r.md': fillerDocs },
      runtimeText: filler,
    });
    expect(r.undocumented.map((u: { name: string }) => u.name)).toEqual(['BEGIN']);
  });
});

describe('collectEnvAccessed — a literal is not a read', () => {
  it('collects a name reached through env, in either access shape', () => {
    const accessed = collectEnvAccessed({
      'a.ts': "const a = env.FIRST_ONE;\nconst b = env['SECOND_ONE'];",
    });
    expect([...accessed].sort()).toEqual(['FIRST_ONE', 'SECOND_ONE']);
  });

  it('ignores a quoted literal that is not an env access', () => {
    // The whole point: `collectDeclared` must see these (it over-collects on
    // purpose), and `collectEnvAccessed` must not.
    const sources = { 'a.ts': "child.kill('SIGKILL');\nawait pool.query('COMMIT');" };
    expect([...collectDeclared(sources).keys()].sort()).toEqual(['COMMIT', 'SIGKILL']);
    expect(collectEnvAccessed(sources).size).toBe(0);
  });

  it('sees a destructured read', () => {
    expect([...collectEnvAccessed({ 'a.ts': 'const { PULLED_OUT } = env;' })]).toEqual([
      'PULLED_OUT',
    ]);
  });
});

describe('a dead not-an-env-var entry is one the config actually reads', () => {
  // The rule this replaced asked for documented AND declared. That fired on
  // `GET` and `POST` — HTTP methods in a route table and HTTP methods in a
  // fixture's request options — which are not variables in either place.
  it('does not call an entry dead for being quoted in a config', () => {
    const r = run({
      sources: { 'c.ts': `${filler}\nawait fetch(url, { method: 'PATCH' });` },
      docs: { 'r.md': `${fillerDocs}\n\`PATCH\`` },
      runtimeText: filler,
      exempt: [entry({ name: 'PATCH' })],
    });
    expect(r.dead).toEqual([]);
    expect(isClean(r)).toBe(true);
  });

  it('calls it dead when a config reads it through env', () => {
    const r = run({
      sources: { 'c.ts': `${filler}\nenv['PATCH']` },
      docs: { 'r.md': `${fillerDocs}\n\`PATCH\`` },
      runtimeText: filler,
      exempt: [entry({ name: 'PATCH' })],
    });
    expect(r.dead.map((x: { name: string }) => x.name)).toEqual(['PATCH']);
    expect(r.dead[0].why).toMatch(/read from process\.env/);
  });

  it('calls it dead even when no doc mentions it', () => {
    // Strictly stronger than the rule it replaced, which needed the name
    // documented before it would look.
    const r = run({
      sources: { 'c.ts': `${filler}\nenv['UNDOCUMENTED_BUT_REAL']` },
      docs: { 'r.md': fillerDocs },
      runtimeText: filler,
      exempt: [entry({ name: 'UNDOCUMENTED_BUT_REAL' })],
    });
    expect(r.dead.map((x: { name: string }) => x.name)).toEqual(['UNDOCUMENTED_BUT_REAL']);
  });
});

describe('a documented family anchors at the prefix', () => {
  // Mutant: `name.startsWith(f)` -> `name.includes(f)`. Survived 84/84 and the
  // real gate. The existing family test pins how the prefix is DERIVED; this
  // pins how it is APPLIED.
  it('does not let a family cover a name that merely contains it', () => {
    const r = run({
      sources: { 'c.ts': `${filler}\nenv['ORCA_KAFKA_SASL_USERNAME']` },
      docs: { 'r.md': `${fillerDocs}\n\`KAFKA_SASL_*\`` },
      runtimeText: filler,
    });
    expect(r.undocumented.map((u: { name: string }) => u.name)).toEqual([
      'ORCA_KAFKA_SASL_USERNAME',
    ]);
  });

  it('still covers a name that starts with it', () => {
    const r = run({
      sources: { 'c.ts': `${filler}\nenv['KAFKA_SASL_USERNAME']` },
      docs: { 'r.md': `${fillerDocs}\n\`KAFKA_SASL_*\`` },
      runtimeText: filler,
    });
    expect(r.undocumented).toHaveLength(0);
  });
});

describe('stripComments recovers at a newline', () => {
  // Mutant: drop the newline arm that closes an unterminated quote. Survived
  // 84/84 and the real gate — latent, which is exactly when a guard gets
  // deleted as unnecessary, as the `.env.example` guard was three times.
  it('does not let an apostrophe swallow the following comment', () => {
    const src = "allowed = it's fine\n# process.env.DEAD_KNOB was removed\n";
    expect(stripComments(src, 'hash')).not.toContain('DEAD_KNOB');
  });

  it('still lets a template literal span lines', () => {
    const src = 'const q = `line one\nline two`;\n// GONE\n';
    const out = stripComments(src, 'c');
    expect(out).toContain('line two');
    expect(out).not.toContain('GONE');
  });
});

describe('validateScanCoverage — no reader may be omitted silently', () => {
  const scanned = ['services/a/src/config.ts'];

  it('passes when every reader is scanned', () => {
    expect(validateScanCoverage({ candidates: scanned, scanned })).toEqual([]);
  });

  it('fails on a reader that is neither scanned nor excused', () => {
    const problems = validateScanCoverage({
      candidates: [...scanned, 'packages/e2e/scripts/driver.ts'],
      scanned,
    });
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain('packages/e2e/scripts/driver.ts');
  });

  it('accepts a reader that carries a written excuse', () => {
    expect(
      validateScanCoverage({
        candidates: [...scanned, 'packages/e2e/scripts/driver.ts'],
        scanned,
        excused: new Map([['packages/e2e/scripts/driver.ts', 'a reason']]),
      }),
    ).toEqual([]);
  });

  it('rejects an excuse for a file that is also scanned', () => {
    const problems = validateScanCoverage({
      candidates: scanned,
      scanned,
      excused: new Map([[scanned[0], 'a reason']]),
    });
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain('both scanned and excused');
  });

  it('rejects an excuse for a file that no longer reads the environment', () => {
    const problems = validateScanCoverage({
      candidates: scanned,
      scanned,
      excused: new Map([['services/a/src/gone.ts', 'a reason']]),
    });
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain('no longer reads the environment');
  });
});

describe('validateSourceFloors — one file may not go quiet behind the others', () => {
  it('passes when every source holds its line', () => {
    expect(validateSourceFloors({ 'a.ts': 9, 'b.ts': 77 }, { 'a.ts': 9, 'b.ts': 77 })).toEqual([]);
  });

  it('fails the file that dropped, not the total', () => {
    // The case a global floor could not see: `a.ts` goes to zero while the sum
    // still clears any total-based bar.
    const problems = validateSourceFloors({ 'a.ts': 0, 'b.ts': 77 }, { 'a.ts': 9, 'b.ts': 77 });
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain('a.ts');
    expect(problems[0]).toContain('expected at least 9');
  });

  it('fails a floor whose file was dropped from the scan set', () => {
    const problems = validateSourceFloors({ 'b.ts': 77 }, { 'a.ts': 9, 'b.ts': 77 });
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain('has a floor but was not scanned');
  });

  it('allows a source to declare more than its floor', () => {
    expect(validateSourceFloors({ 'a.ts': 12 }, { 'a.ts': 9 })).toEqual([]);
  });

  // The map is read in both directions for the same reason the scan set is: an
  // inclusion list cannot fail loudly, because the failure is a line nobody
  // wrote. Reading it only forwards let a newly-scanned source carry no floor
  // and go to zero silently — the bug this gate was changed to fix, reproduced
  // one function over, in the commit that fixed it.
  it('fails a scanned source that nobody pinned a floor for', () => {
    const problems = validateSourceFloors({ 'a.ts': 9, 'unpinned.ts': 0 }, { 'a.ts': 9 });
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain('unpinned.ts');
    expect(problems[0]).toContain('no floor');
  });

  it('accepts a deliberate floor of 0', () => {
    // Three sources pass `process.env` wholesale and name nothing. "Declares
    // nothing" has to be sayable, or the rule above would force a false number.
    expect(validateSourceFloors({ 'passthrough.ts': 0 }, { 'passthrough.ts': 0 })).toEqual([]);
  });
});

/**
 * The suite above leans on message substrings. Rewording all thirteen
 * operator-facing strings, with no behaviour change, turned fifteen tests red —
 * and twelve of those had a substring as their only assertion, so once it is
 * updated they pin nothing. Two mutations proved the gap: collapsing the `where`
 * attribution to a constant, and emitting every problem twice, both survived the
 * whole suite. `where` and the site line numbers ARE the operator-facing output
 * of a failing run; a report that cannot say which of 61 entries is malformed is
 * not a report.
 *
 * These assert shape — how many problems, and which entry — so a reword costs a
 * string and a real regression still costs a test.
 */
describe('a failing register report identifies the entry', () => {
  const malformed = { name: 'SECOND_ONE', reason: 'not-an-env-var' };

  it('names the offending index and name, not just the problem', () => {
    const problems = validateExemptions({
      version: 1,
      exemptions: [entry({ name: 'FIRST_ONE' }), malformed],
    });
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain('exemptions[1]');
    expect(problems[0]).toContain('SECOND_ONE');
  });

  it('reports one problem per malformed entry, not one per rule', () => {
    const problems = validateExemptions({
      version: 1,
      exemptions: [malformed, { name: 'THIRD_ONE', reason: 'not-an-env-var' }],
    });
    expect(problems).toHaveLength(2);
    expect(problems.filter((p: string) => p.includes('SECOND_ONE'))).toHaveLength(1);
    expect(problems.filter((p: string) => p.includes('THIRD_ONE'))).toHaveLength(1);
  });

  it('reports a well-formed register as exactly zero problems', () => {
    expect(
      validateExemptions({
        version: 1,
        exemptions: [entry({ name: 'FIRST_ONE' }), entry({ name: 'SECOND_ONE' })],
      }),
    ).toHaveLength(0);
  });
});

describe('a failing site report identifies the site', () => {
  it('names the entry whose site is unreachable, and only that one', () => {
    const problems = validateExemptionSites(
      {
        version: 1,
        exemptions: [
          entry({ name: 'ALIVE', reason: 'wired-elsewhere', site: 'live.ts' }),
          entry({ name: 'ORPHANED', reason: 'wired-elsewhere', site: 'gone.ts' }),
        ],
      },
      { exists: (p: string) => p === 'live.ts', read: () => 'env.ALIVE' },
    );
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain('ORPHANED');
    expect(problems[0]).toContain('gone.ts');
  });

  it('names the entry whose site no longer mentions it', () => {
    const problems = validateExemptionSites(
      {
        version: 1,
        exemptions: [entry({ name: 'MOVED_AWAY', reason: 'wired-elsewhere', site: 'still.ts' })],
      },
      { exists: () => true, read: () => 'nothing relevant here' },
    );
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain('MOVED_AWAY');
    expect(problems[0]).toContain('still.ts');
  });
});

describe('collectDocumented attributes a name to its own line', () => {
  // Mutant: `${i + 1}` -> `${i}` in the site line number. Survived the whole
  // suite. A site is a file:line an operator is told to go read; off by one it
  // points at the previous line, which is how a correction gets applied to the
  // wrong row.
  it('reports the line the name is actually on', () => {
    const { documented } = collectDocumented({
      'r.md': 'first line\nsecond line\n`ON_THIRD_LINE`\n',
    });
    expect(documented.get('ON_THIRD_LINE')).toEqual(['r.md:3']);
  });

  it('reports every line a name appears on, in order', () => {
    const { documented } = collectDocumented({ 'r.md': '`TWICE_OVER`\nfiller\n`TWICE_OVER`\n' });
    expect(documented.get('TWICE_OVER')).toEqual(['r.md:1', 'r.md:3']);
  });
});

describe('both collectors see the idiomatic access shapes', () => {
  // Found by enumerating access shapes against both collectors rather than by
  // reading the regexes. Six of fourteen were missed; four are closed here.
  // None was live — no file in the tree writes them — but a bare destructure
  // leaves no quoted literal for the broad net, so that miss would have been
  // total and silent, which is the direction this gate exists to prevent.
  const shapes: [string, string][] = [
    ['process.env.PLAIN_DOT', 'PLAIN_DOT'],
    ["process.env['PLAIN_IDX']", 'PLAIN_IDX'],
    ['process.env["DOUBLE_QUOTED"]', 'DOUBLE_QUOTED'],
    ["process.env[ 'SPACED_INDEX' ]", 'SPACED_INDEX'],
    ['process.env?.OPTIONAL_DOT', 'OPTIONAL_DOT'],
    ["process.env?.['OPTIONAL_INDEX']", 'OPTIONAL_INDEX'],
    ['const { FROM_BARE_ENV } = env;', 'FROM_BARE_ENV'],
    ['const { FROM_PROCESS_ENV } = process.env;', 'FROM_PROCESS_ENV'],
    ['const { SECOND_OF_TWO, OTHER_ONE } = process.env;', 'SECOND_OF_TWO'],
    ['const { RENAMED_TO: local } = env;', 'RENAMED_TO'],
    ['const { WITH_A_DEFAULT = "x" } = env;', 'WITH_A_DEFAULT'],
  ];

  it.each(shapes)('collectDeclared sees %s', (src, name) => {
    expect([...collectDeclared({ 'a.ts': src }).keys()]).toContain(name);
  });

  it.each(shapes)('collectEnvAccessed sees %s', (src, name) => {
    expect(collectEnvAccessed({ 'a.ts': src }).has(name)).toBe(true);
  });

  // The two that stay out of reach, pinned so they are a known limit rather
  // than an untested assumption. Aliasing to an arbitrary identifier needs
  // dataflow analysis, not a regex.
  it('does not follow process.env aliased to another name', () => {
    const src = 'const e = process.env;\nconst v = e.ALIASED_AWAY;';
    expect(collectEnvAccessed({ 'a.ts': src }).has('ALIASED_AWAY')).toBe(false);
  });

  it('now sees a Reflect.get read too', () => {
    // This used to pin a limitation. Widening the helper shape to accept
    // `process.env` as the first argument closed it as a side effect, and a
    // reflective read IS an env read, so the wider behaviour is the correct one.
    const src = "Reflect.get(process.env, 'VIA_REFLECT');";
    expect(collectEnvAccessed({ 'a.ts': src }).has('VIA_REFLECT')).toBe(true);
    expect([...collectDeclared({ 'a.ts': src }).keys()]).toContain('VIA_REFLECT');
  });
});

describe('validateScanCoverage describes the right kind of file', () => {
  // It guards two different inclusion lists now — config sources and
  // documentation. Reusing one implementation is deliberate: "is this file in
  // the list" was the question, and asking it in two places is how this gate
  // acquired the same bug repeatedly. But the message was written for configs,
  // so a markdown file was reported as "reads process.env".
  it('defaults to wording that fits a shell script as well as a module', () => {
    const problems = validateScanCoverage({ candidates: ['a.sh'], scanned: [] });
    expect(problems[0]).toContain('reads the environment');
  });

  it('uses the caller’s phrase in both directions', () => {
    const missing = validateScanCoverage({
      candidates: ['a.md'],
      scanned: [],
      what: 'names environment identifiers',
    });
    expect(missing).toHaveLength(1);
    expect(missing[0]).toContain('names environment identifiers');
    expect(missing[0]).not.toContain('reads the environment');

    const stale = validateScanCoverage({
      candidates: [],
      scanned: [],
      excused: new Map([['gone.md', 'a reason']]),
      what: 'names environment identifiers',
    });
    expect(stale).toHaveLength(1);
    expect(stale[0]).toContain('no longer names environment identifiers');
  });
});

/**
 * A second mutation campaign, run by a reviewer against the suite the first one
 * produced: 65 mutants, 13 survivors. The pattern in them is that the previous
 * round fixed attribution testing *instance-wise* — for the register and its
 * sites — and left `compare()`, which produces every list the gate actually
 * prints.
 */
describe('compare reports which file and which line, not just which name', () => {
  // All five interpolated attribution fields in compare() collapsed to
  // constants simultaneously with the whole suite green. They are the entire
  // operator-facing payload: the gate prints origin, sites, and both `why`
  // sentences verbatim.
  it('names the origin file of an undocumented variable', () => {
    const r = run({
      sources: { 'owner.ts': `${filler}\nenv['ORPHAN_KNOB']` },
      docs: { 'r.md': fillerDocs },
      runtimeText: filler,
    });
    expect(r.undocumented).toHaveLength(1);
    expect(r.undocumented[0].origin).toBe('owner.ts');
  });

  it('names the file and line of a phantom', () => {
    const r = run({
      sources: { 'c.ts': filler },
      docs: { 'r.md': `${fillerDocs}\n\`GHOST_KNOB\`` },
      runtimeText: filler,
    });
    expect(r.phantom).toHaveLength(1);
    expect(r.phantom[0].sites).toEqual([`r.md:${fillerDocs.split('\n').length + 1}`]);
  });

  it('names both sides for a resurrected variable', () => {
    const r = run({
      sources: { 'owner.ts': `${filler}\nenv['GONE_KNOB']` },
      docs: { 'r.md': `${fillerDocs}\n\`GONE_KNOB\`` },
      runtimeText: filler,
      exempt: [entry({ name: 'GONE_KNOB', reason: 'intentionally-absent' })],
    });
    expect(r.resurrected).toHaveLength(1);
    expect(r.resurrected[0].origin).toBe('owner.ts');
    expect(r.resurrected[0].sites[0]).toMatch(/^r\.md:\d+$/);
  });

  it('names the origin in the dead-entry sentence', () => {
    const r = run({
      sources: { 'owner.ts': `${filler}\nenv['MISLABELLED']` },
      docs: { 'r.md': `${fillerDocs}\n\`MISLABELLED\`` },
      runtimeText: filler,
      exempt: [entry({ name: 'MISLABELLED' })],
    });
    expect(r.dead).toHaveLength(1);
    expect(r.dead[0].why).toContain('owner.ts');
  });

  it('names the origin in the obsolete-exemption sentence', () => {
    const r = run({
      sources: { 'owner.ts': `${filler}\nenv['NOW_READ']` },
      docs: { 'r.md': `${fillerDocs}\n\`NOW_READ\`` },
      runtimeText: filler,
      exempt: [entry({ name: 'NOW_READ', reason: 'wired-elsewhere', site: 'package.json' })],
    });
    expect(r.dead).toHaveLength(1);
    expect(r.dead[0].why).toContain('owner.ts');
  });
});

describe('an excuse covers one file, not the whole check', () => {
  // Mutant: `excused.has(rel)` -> `excused.size > 0`. Survived the whole suite,
  // because no case combined a non-empty excuse map with an unexcused reader.
  // Latent while the map was empty — and the map exists so that somebody adds
  // the first entry. Three now have.
  it('still fails an unexcused reader when another file is excused', () => {
    const problems = validateScanCoverage({
      candidates: ['scanned.ts', 'excused.ts', 'orphan.ts'],
      scanned: ['scanned.ts'],
      excused: new Map([['excused.ts', 'a written reason']]),
    });
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain('orphan.ts');
  });
});

describe('isWired anchors at both ends', () => {
  // The substring category, on its third implementation. `namesIdentifier` and
  // `validateExemptionSites` each pin it; `isWired` — which asks the same
  // question — did not, and this commit rewrote its pattern list.
  it('does not let a longer variable satisfy a shorter one', () => {
    expect(isWired('const p = process.env.HTTP_PORT_EXTRA;', 'HTTP_PORT')).toBe(false);
    expect(isWired("const p = process.env['HTTP_PORT_EXTRA'];", 'HTTP_PORT')).toBe(false);
    expect(
      isWired('process.env.ORCA_E2E_SANDBOX_HARNESS_ENABLED', 'ORCA_E2E_SANDBOX_HARNESS'),
    ).toBe(false);
  });

  it('does not treat an identifier merely ending in env as the environment', () => {
    expect(isWired('const x = myenv.FOO_BAR;', 'FOO_BAR')).toBe(false);
    expect(isWired("const x = denv['FOO_BAR'];", 'FOO_BAR')).toBe(false);
  });

  it('still matches the real shapes', () => {
    expect(isWired('const p = process.env.HTTP_PORT;', 'HTTP_PORT')).toBe(true);
    expect(isWired("const p = env['HTTP_PORT'];", 'HTTP_PORT')).toBe(true);
  });
});

describe('both collectors anchor where the shape may start', () => {
  // Found by mis-aiming a mutant: the same pattern appears in both collectors,
  // and replacing only the first occurrence hit `collectDeclared`, which turned
  // out to be the unpinned one. Both are pinned now.
  it('collectEnvAccessed ignores an identifier that merely ends in env', () => {
    expect(collectEnvAccessed({ 'a.ts': 'const x = myenv.FOO_BAR;' }).size).toBe(0);
    expect(collectEnvAccessed({ 'a.ts': "const x = denv['FOO_BAR'];" }).size).toBe(0);
  });

  it('collectDeclared ignores an identifier that merely ends in env', () => {
    // No quotes here, so the broad literal net cannot rescue it — the dot
    // pattern's leading boundary is the only thing keeping `myenv.FOO_BAR` out.
    expect([...collectDeclared({ 'a.ts': 'const x = myenv.FOO_BAR;' }).keys()]).toEqual([]);
  });

  it('collectDeclared still sees a real bare env read', () => {
    expect([...collectDeclared({ 'a.ts': 'const x = env.REAL_ONE;' }).keys()]).toEqual([
      'REAL_ONE',
    ]);
  });
});

describe('every floor is checked, including the last', () => {
  // Mutant: `Object.entries(floors)` -> `.slice(0, -1)`. Survived, because all
  // four cases put the failing entry first in a two-key object.
  it('fails the last file in the map, not only the first', () => {
    const problems = validateSourceFloors({ 'a.ts': 9, 'b.ts': 0 }, { 'a.ts': 9, 'b.ts': 77 });
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain('b.ts');
  });
});

describe('the dead-entry diagnosis matches the reason it contradicts', () => {
  // Mutant: drop the `not-an-env-var` reason guard. A wired-elsewhere entry then
  // gets "it is an env var after all" — a rebuttal of a claim nobody made.
  it('tells a wired-elsewhere entry its exemption is obsolete, not that it is a variable', () => {
    const r = run({
      sources: { 'c.ts': `${filler}\nenv['MOVED_HOME']` },
      docs: { 'r.md': `${fillerDocs}\n\`MOVED_HOME\`` },
      runtimeText: filler,
      exempt: [entry({ name: 'MOVED_HOME', reason: 'wired-elsewhere', site: 'package.json' })],
    });
    expect(r.dead).toHaveLength(1);
    expect(r.dead[0].why).toMatch(/obsolete/);
    expect(r.dead[0].why).not.toMatch(/env var after all/);
  });

  it('reports an env-accessed intentionally-absent entry once, as resurrected', () => {
    const r = run({
      sources: { 'c.ts': `${filler}\nenv['SHOULD_BE_ABSENT']` },
      docs: { 'r.md': `${fillerDocs}\n\`SHOULD_BE_ABSENT\`` },
      runtimeText: filler,
      exempt: [entry({ name: 'SHOULD_BE_ABSENT', reason: 'intentionally-absent' })],
    });
    expect(r.resurrected).toHaveLength(1);
    expect(r.dead).toEqual([]);
  });
});

describe('collectEnvAccessed sees the shapes this repo actually uses', () => {
  // The blocker: direct access saw 106 of 189 declared names. The other 83 —
  // including TRUST_PROXY_CIDRS, S3_SECRET_ACCESS_KEY, KAFKA_SASL_PASSWORD —
  // reach the environment through a helper, so no evidence could ever
  // contradict a `not-an-env-var` entry claiming they are not variables.
  it('sees a helper call taking env and the name', () => {
    expect(collectEnvAccessed({ 'a.ts': "optionalEnv(env, 'VIA_HELPER')" }).has('VIA_HELPER')).toBe(
      true,
    );
  });

  it('sees every name in a multi-argument helper call', () => {
    // Capturing only the first argument is what once hid a secret from the gate.
    const seen = collectEnvAccessed({
      'a.ts': "readS3StaticCredentialPair(env, 'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY')",
    });
    expect([...seen].sort()).toEqual(['S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY']);
  });

  it('sees the name-constant indirection', () => {
    const src = "const REPLAY_ENV_VAR = 'SANDBOX_HARNESS_REPLAY';\nconst v = env[REPLAY_ENV_VAR];";
    expect(collectEnvAccessed({ 'a.ts': src }).has('SANDBOX_HARNESS_REPLAY')).toBe(true);
  });

  it('does not treat an unrelated call taking a quoted name as an env read', () => {
    expect(collectEnvAccessed({ 'a.ts': "logger.warn(ctx, 'NOT_A_VARIABLE')" }).size).toBe(0);
  });
});

/**
 * A reviewer pointed at the shell widening and the helper shape found seven
 * more. These pin the four that were code defects rather than prose.
 */
describe('shell comments start at a word boundary', () => {
  // `#` opens a shell comment only at the start of a word. Treating every
  // unquoted `#` as one truncated its line on `${#arr[@]}` and `${VAR##*/}`, so
  // an unrelated shell idiom decided whether a read on that line was visible.
  // Harmless while `.sh` was only wiring evidence; load-bearing once shell files
  // became scanned sources.
  it('does not treat parameter length as a comment', () => {
    const src = 'if [ ${#items} -gt 0 ]; then A="${ORCA_STILL_VISIBLE:-x}"; fi';
    expect(stripComments(src, 'hash')).toContain('ORCA_STILL_VISIBLE');
  });

  it('does not treat prefix strip as a comment', () => {
    const src = 'tag=${spec##*/}; B="${ORCA_ALSO_VISIBLE:-y}"';
    expect(stripComments(src, 'hash')).toContain('ORCA_ALSO_VISIBLE');
  });

  it('still strips a comment at the start of a line', () => {
    expect(stripComments('# ORCA_OBITUARY was removed\nA=1\n', 'hash')).not.toContain(
      'ORCA_OBITUARY',
    );
  });

  it('still strips a trailing comment', () => {
    expect(stripComments('A=1  # ORCA_TRAILING is gone\n', 'hash')).not.toContain('ORCA_TRAILING');
  });
});

describe('the helper shape survives an ordinary argument list', () => {
  // `[^()]*` died at the first inner paren, so a token in an unrelated third
  // argument decided whether a false `not-an-env-var` entry could hide a real
  // variable. Reproduced end to end by a reviewer against DATABASE_POOL_MAX.
  it('sees a name when a later argument contains a call', () => {
    const src = "parsePoolMax(env, 'DATABASE_POOL_MAX', Number(DEFAULT_POOL_MAX))";
    expect(collectEnvAccessed({ 'a.ts': src }).has('DATABASE_POOL_MAX')).toBe(true);
  });

  it('accepts process.env or a member as the first argument', () => {
    expect(
      collectEnvAccessed({ 'a.ts': "optionalEnv(process.env, 'VIA_PROCESS')" }).has('VIA_PROCESS'),
    ).toBe(true);
    expect(
      collectEnvAccessed({ 'a.ts': "optionalEnv(cfg.env, 'VIA_MEMBER')" }).has('VIA_MEMBER'),
    ).toBe(true);
  });

  it('tolerates a space before the parenthesis', () => {
    expect(
      collectEnvAccessed({ 'a.ts': "optionalEnv (env, 'SPACED_CALL')" }).has('SPACED_CALL'),
    ).toBe(true);
  });

  it('still takes every name in a multi-argument helper', () => {
    const seen = collectEnvAccessed({
      'a.ts': "readS3StaticCredentialPair(env, 'S3_A_KEY', 'S3_B_KEY')",
    });
    expect([...seen].sort()).toEqual(['S3_A_KEY', 'S3_B_KEY']);
  });

  it('does not fire on a first argument that merely ends in env', () => {
    expect(collectEnvAccessed({ 'a.ts': "logger.info(notenv, 'NOT_A_VARIABLE')" }).size).toBe(0);
  });
});

describe('isTestFile — one predicate, shared by the shell and the core', () => {
  // The shell and `isWiringEvidence` each had their own, and they disagreed.
  // `envReaders` excluded `[cm]?[jt]s` while `isWiringEvidence` excluded only
  // `(ts|js|mjs)`, so a `foo.spec.cjs` was not a candidate — never demanded into
  // CONFIG_SOURCES — yet still counted as proof a documented name was live. It
  // could silence a phantom while never being asked to justify one, which is the
  // shape this gate keeps reproducing.
  const testFiles = [
    'a.spec.ts',
    'a.test.ts',
    'a.spec.js',
    'a.test.js',
    'a.spec.mjs',
    'a.spec.cjs',
    'a.spec.mts',
    'a.spec.cts',
    'a.d.ts',
    'a.d.mts',
    'a.d.cts',
  ];
  const realFiles = ['a.ts', 'a.mjs', 'a.specs.ts', 'a.dspec.ts', 'ad.ts', 'spec.ts', 'test.ts'];

  it.each(testFiles)('treats %s as a test or declaration file', (name) => {
    expect(isTestFile(name)).toBe(true);
  });

  it.each(realFiles)('treats %s as real source', (name) => {
    expect(isTestFile(name)).toBe(false);
  });

  it('is what isWiringEvidence uses, so the two cannot disagree', () => {
    // Every name isTestFile rejects must also be refused as wiring evidence,
    // whatever its extension.
    for (const name of testFiles) {
      expect(isWiringEvidence(`services/x/src/${name}`)).toBe(false);
    }
    expect(isWiringEvidence('services/x/src/a.ts')).toBe(true);
    expect(isWiringEvidence('services/x/src/a.mjs')).toBe(true);
  });
});

/**
 * An adversarial verifier mutation-proved that neither headline change in
 * `d3b0a520` had any test at all: reverting the name minimum left the suite AND
 * the real gate green, and reverting the shell terminator left the suite green.
 * The commit's "Verified: 168 gate tests" therefore read as evidence for those
 * changes and was not — the count is identical in every direction.
 *
 * This is the file's own "a check is unverified until you break what it guards"
 * rule, applied to the checks themselves.
 */
describe('names are two characters and up', () => {
  it('collects a two-character variable', () => {
    // `TZ` is real: the local runtime forwards it into every sandbox. Under the
    // old three-character minimum its documentation could be deleted silently.
    expect([...collectDeclared({ 'a.ts': "env['TZ']" }).keys()]).toContain('TZ');
    expect(collectEnvAccessed({ 'a.ts': "env['TZ']" }).has('TZ')).toBe(true);
  });

  it('documents a two-character variable', () => {
    const { documented } = collectDocumented({ 'r.md': 'Forwarded: `TZ`.' });
    expect([...documented.keys()]).toContain('TZ');
  });

  it('lets a two-character prefix declare a family', () => {
    // The subtler half: `LC_*` cannot match a three-character minimum at all, so
    // the family declaration in the harness README was inert until the minimum
    // dropped. A family that silently does not exist is worse than none.
    const { families } = collectDocumented({ 'r.md': 'Forwarded: `LC_*` and `LC_ALL`.' });
    expect([...families]).toContain('LC_');
  });

  it('still requires at least two characters', () => {
    expect(collectDeclared({ 'a.ts': "env['X']" }).size).toBe(0);
  });
});

describe('shell expansion covers suffix and prefix operators', () => {
  const suffix: [string, string][] = [
    ['${SOME_NAME}', 'braces'],
    ['${SOME_NAME:-d}', 'colon-dash'],
    ['${SOME_NAME-d}', 'uncolonized dash'],
    ['${SOME_NAME+w}', 'plus'],
    ['${SOME_NAME?w}', 'question'],
    ['${SOME_NAME:=d}', 'colon-equals'],
    ['${SOME_NAME%suf}', 'percent'],
    ['${SOME_NAME##pre}', 'double hash'],
    ['${SOME_NAME/a/b}', 'substitution'],
    ['${SOME_NAME^^}', 'upper'],
    ['$SOME_NAME', 'bare'],
  ];

  it.each(suffix)('sees %s (%s)', (form) => {
    expect([...collectDeclared({ 'a.sh': `x="${form}"` }).keys()]).toContain('SOME_NAME');
  });

  // These sit BEFORE the name, so no terminator can anchor on them — the
  // terminator rule handles whatever POSIX adds *after* a name, which is not the
  // same as whatever POSIX adds.
  it('sees ${#NAME}, the string-length prefix', () => {
    expect([...collectDeclared({ 'a.sh': 'n=${#SOME_NAME}' }).keys()]).toContain('SOME_NAME');
  });

  it('sees ${!NAME}, the indirect-expansion prefix', () => {
    expect([...collectDeclared({ 'a.sh': 'v=${!SOME_NAME}' }).keys()]).toContain('SOME_NAME');
  });

  it('does not let a longer name satisfy a shorter one', () => {
    expect([...collectDeclared({ 'a.sh': 'x="${SOME_NAME_SUFFIX}"' }).keys()]).not.toContain(
      'SOME_NAME',
    );
  });

  it('applies shell patterns only to shell files', () => {
    // Run against TypeScript these match `${CONST}` in an ordinary template
    // literal, which once reported five imported constants as undocumented.
    expect([...collectDeclared({ 'a.ts': 'const s = `${SOME_NAME}`;' }).keys()]).not.toContain(
      'SOME_NAME',
    );
  });
});

describe('isHiddenPath — per component, at every depth', () => {
  /**
   * Three rounds on one filter: deleted from two sites, restored to one, then
   * restored in a form covering only depth 0. Each fix was right about the case
   * in front of it.
   *
   * So the cases that matter here are the NESTED ones. A top-level `.vscode/`
   * passes under all three spellings, including the two that were wrong — it has
   * no power to tell them apart, and a test without discriminating power is how
   * this got to a third round.
   */
  it('skips a hidden directory nested under a scanned one', () => {
    expect(isHiddenPath('packages/e2e-tests/.vscode/NOTES.md')).toBe(true);
    expect(isHiddenPath('services/harness-server/.zed/README.md')).toBe(true);
    expect(isHiddenPath('services/.devcontainer/README.md')).toBe(true);
  });

  it('skips a hidden directory at the top level', () => {
    expect(isHiddenPath('.vscode/NOTES.md')).toBe(true);
    expect(isHiddenPath('.claude/worktrees/x/README.md')).toBe(true);
  });

  it('keeps .github, but only as the first component', () => {
    expect(isHiddenPath('.github/workflows/test-ts.yml')).toBe(false);
    expect(isHiddenPath('.github/NOTES.md')).toBe(false);
    // A nested `.github` is somebody's tooling directory, not the workflows dir.
    expect(isHiddenPath('packages/x/.github/NOTES.md')).toBe(true);
  });

  it('does not treat a name merely starting with .github as the exception', () => {
    expect(isHiddenPath('.githubfoo/a.md')).toBe(true);
  });

  it('leaves ordinary paths alone', () => {
    expect(isHiddenPath('packages/e2e-tests/README.md')).toBe(false);
    expect(isHiddenPath('services/harness-server/src/config.ts')).toBe(false);
    // A dot inside a filename is not a hidden directory.
    expect(isHiddenPath('packages/x/vitest.config.ts')).toBe(false);
  });
});
