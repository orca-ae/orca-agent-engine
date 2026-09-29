// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Env-doc drift detection, as a pure module.
 *
 * No file I/O: `check-env-docs.mjs` is the shell that reads, and a unit test
 * imports this file directly with hand-written inputs. That split exists
 * because the first version of this gate fused its matching logic to `fs`,
 * which made the logic untestable — and it shipped with three independent
 * false negatives that two reviewers found by hand:
 *
 *   1. it saw only `env['X']`, missing ~39 variables read through helpers
 *   2. it compared docs by substring, so `INTERNAL_HTTP_PORT` satisfied `HTTP_PORT`
 *   3. it compared source by substring, so a variable named in a *comment*
 *      looked like a live consumer
 *
 * Each is a case below.
 */

/**
 * Every environment variable a config file reads.
 *
 * Two shapes, because the config files use both and a gate that knows only one
 * is worse than no gate — it reports success over a surface it cannot see:
 *
 *   env['NAME']                  direct access
 *   anyHelper(env, 'NAME')       optionalEnv, requiredEnv, parsePoolMax,
 *                                parseBooleanEnv, readKafkaTlsFile,
 *                                readS3StaticCredentialPair, …
 *
 * The helper pattern is deliberately open on the function name. Pinning the
 * list would mean the next helper silently shrinks the checked surface, which
 * is exactly how the first version failed.
 */
/**
 * How a shell script reads the environment: `${VAR}`, `${VAR:=d}`, `${VAR:-d}`,
 * `${VAR:?msg}`, and bare `$VAR`.
 *
 * Applied only to `.sh` sources, per file, and that restriction is the whole
 * point rather than tidiness. Run against TypeScript these match `${CONST}`
 * inside an ordinary template literal, which is how a first attempt reported
 * `REPLAY_ENV_VAR` and four sibling constants as undocumented variables.
 *
 * Without them, putting a shell file in `CONFIG_SOURCES` bought nothing: the
 * collectors were TypeScript-shaped, so a `.sh` file was *believed* covered
 * while every variable it read as shell stayed invisible. That is worse than
 * not scanning it — `REGISTRY_INTERNAL_HTTP_PORT` and `REGISTRY_ADMIN_HTTP_PORT`
 * are printed to the operator by `start-services.sh:260` as knobs to set, in the
 * same sentence as two documented siblings, and were documented nowhere.
 */
// The terminator is "anything that cannot continue a name", not a list of
// operators. Enumerating `[:}]` covered `${NAME}` and the colon forms and
// silently missed every uncolonized one — `${NAME-default}`, `${NAME+word}`,
// `${NAME?word}`, `${NAME%suffix}` — which this repo already uses at
// start-services.sh:73, :172 and :173. Guessing which operators exist is the
// same mistake as guessing which access shapes exist.
const SHELL_PATTERNS = [
  // Suffix operators: the terminator is "anything that cannot continue a name",
  // which covers `-`, `+`, `?`, `=`, `%`, `#`, `/`, `^`, `,`, `:` and `}` without
  // enumerating them.
  /\$\{([A-Z][A-Z0-9_]{1,})(?![A-Za-z0-9_])/g,
  /\$([A-Z][A-Z0-9_]{1,})(?![A-Za-z0-9_])/g,
  // PREFIX operators, which that rule structurally cannot reach — the operator
  // sits between the brace and the name, so no terminator exists to anchor on.
  // `${#NAME}` is string length and `${!NAME}` is indirect expansion; both read
  // NAME. Claiming the terminator handles "whatever POSIX adds next" was an
  // over-claim: it handles whatever POSIX adds *after* the name.
  /\$\{[#!]([A-Z][A-Z0-9_]{1,})(?![A-Za-z0-9_])/g,
];

export function collectDeclared(sources) {
  const declared = new Map(); // NAME -> origin

  const patterns = [
    // Every quoted SCREAMING_SNAKE literal anywhere in a config file. This is
    // deliberately the broad net rather than a list of call shapes: two rounds
    // of shape-matching missed `env['X']`-only, then helper-call first
    // arguments only — `readS3StaticCredentialPair(env, 'S3_ACCESS_KEY_ID',
    // 'S3_SECRET_ACCESS_KEY')` hid a *secret* from the gate because the
    // pattern captured one group. Over-collecting fails loudly (a name is
    // reported undocumented); under-collecting fails silently, which is the
    // failure this gate exists to prevent.
    /'([A-Z][A-Z0-9_]{1,})'/g,
    /"([A-Z][A-Z0-9_]{1,})"/g,
    // Dot access. `noPropertyAccessFromIndexSignature` is not set, so this compiles.
    // `\??` so optional chaining (`env?.NAME`) is not a hole.
    /\benv\??\.([A-Z][A-Z0-9_]{1,})\b/g,
  ];

  for (const [origin, body] of Object.entries(sources)) {
    const active = /\.sh$/.test(origin) ? [...patterns, ...SHELL_PATTERNS] : patterns;
    for (const pattern of active) {
      for (const m of body.matchAll(pattern)) {
        if (!declared.has(m[1])) declared.set(m[1], origin);
      }
    }
    // Destructuring: `const { A, B } = env`. Names are bare identifiers here,
    // so the quoted-literal net cannot see them — this is the one shape where a
    // miss is total rather than partial.
    //
    // `(?:process\.)?` because `= process.env` is the more idiomatic spelling
    // and matching only `= env` missed it entirely, in both collectors. Nothing
    // in the tree writes it today; it is closed because a bare destructure
    // leaves no quoted literal for the broad net to catch, so the failure would
    // have been silent — the direction this gate exists to prevent.
    for (const m of body.matchAll(/\{([^{}]*)\}\s*=\s*(?:process\.)?env\b/g)) {
      for (const name of m[1].split(',')) {
        // `NAME = 'default'` and `NAME: alias` both carry the real name first.
        const id = name.split(/[:=]/)[0].trim();
        if (/^[A-Z][A-Z0-9_]{1,}$/.test(id) && !declared.has(id)) {
          declared.set(id, origin);
        }
      }
    }
  }
  return declared;
}

/**
 * The subset of {@link collectDeclared} that is read through an environment
 * accessor, rather than merely appearing as a quoted literal.
 *
 * `collectDeclared` deliberately over-collects — every quoted SCREAMING_SNAKE
 * literal in a config file — because under-collecting fails silently. That is
 * the right trade for the undocumented direction, where a false positive is a
 * name in a report and a false negative is an undocumented variable.
 *
 * It is the wrong trade for the register's dead-entry check, which asks the
 * opposite question: is this `not-an-env-var` entry lying? Correlating two weak
 * signals (documented AND declared) got that wrong twice — first passing
 * `MUST_BE_CORE` and eight other exported constants, then flagging `GET` and
 * `POST`, which are HTTP methods in a route table *and* HTTP methods in a
 * fixture's request options. Both signals fire; the name is still not a
 * variable.
 *
 * A read through `process.env` is not evidence that a name is a variable, it is
 * the definition of one, so the dead check asks that directly instead.
 *
 * Names are `[A-Z][A-Z0-9_]{1,}` — two characters and up. The minimum used to be
 * three, which silently excluded `TZ`: a real variable the local runtime
 * forwards into every sandbox, whose README row could be deleted with the gate
 * still green.
 *
 * An earlier version of this comment said lowering it "costs nothing: zero new
 * names on this tree". That was wrong, and wrong in the way this whole gate
 * exists to catch — a sentence about the code that the code contradicts. It adds
 * `TZ` to both declared and documented, and it activates the `LC_*` family,
 * which the three-character pattern could not match at all: the family
 * declaration in the harness README had been silently inert. What is actually
 * zero is new *problems* — no undocumented name, no phantom, no dead entry.
 */
export function collectEnvAccessed(sources) {
  const accessed = new Set();
  const patterns = [
    /\benv\??\.([A-Z][A-Z0-9_]{1,})\b/g,
    /\benv\??\.?\[\s*['"`]([A-Z][A-Z0-9_]{1,})['"`]\s*\]/g,
    // A helper called with `env` and the name: `optionalEnv(env, 'NAME')`,
    // `requiredEnv`, `parseBooleanEnv`, `readS3StaticCredentialPair(env, 'A',
    // 'B')`. Every quoted name in that argument list counts, because capturing
    // only the first is what once hid a secret from the undocumented check.
    //
    // This is the shape this collector was missing, and it is the DOMINANT one
    // in this repo: direct access saw 106 of 189 declared names. The other 83
    // — `TRUST_PROXY_CIDRS`, `S3_SECRET_ACCESS_KEY`, `KAFKA_SASL_PASSWORD` —
    // reach the environment through a helper, and none of them could ever
    // contradict a `not-an-env-var` entry claiming they are not variables.
    // One nesting level, and `env` reachable through a member or `process.`:
    // `[^()]*` died at the first inner paren, so `f(env, 'NAME', Number(D))`
    // contributed nothing while `collectDeclared` still saw the name — the
    // asymmetry a false register entry exploits. A token in an unrelated third
    // argument decided whether the gate could be bypassed.
    /\b[A-Za-z_$][\w$]*\s*\(\s*(?:[\w$]+\.)?env\s*,((?:[^()]|\([^()]*\))*)\)/g,
    // The name-constant indirection: `const REPLAY_ENV_VAR = 'SANDBOX_HARNESS_REPLAY'`,
    // read elsewhere as `env[REPLAY_ENV_VAR]`. Naming a constant `*_ENV` or
    // `*_ENV_VAR` and assigning it a SCREAMING_SNAKE string is a declaration
    // that the string IS an environment variable.
    /\b[A-Z][A-Z0-9_]*_ENV(?:_VAR)?\s*=\s*['"`]([A-Z][A-Z0-9_]{1,})['"`]/g,
  ];
  for (const body of Object.values(sources)) {
    for (const pattern of patterns) {
      for (const m of body.matchAll(pattern)) {
        // The helper pattern captures a whole argument list; the rest capture
        // one name.
        if (/['"`]/.test(m[1])) {
          for (const lit of m[1].matchAll(/['"`]([A-Z][A-Z0-9_]{1,})['"`]/g)) accessed.add(lit[1]);
        } else if (/^[A-Z][A-Z0-9_]{1,}$/.test(m[1])) {
          accessed.add(m[1]);
        }
      }
    }
    for (const m of body.matchAll(/\{([^{}]*)\}\s*=\s*(?:process\.)?env\b/g)) {
      for (const name of m[1].split(',')) {
        const id = name.split(/[:=]/)[0].trim();
        if (/^[A-Z][A-Z0-9_]{1,}$/.test(id)) accessed.add(id);
      }
    }
  }
  return accessed;
}

/**
 * Strip comments so a variable *named* in prose is not mistaken for a consumer.
 *
 * `# MEMORY_WATCHER_POLL_INTERVAL_MS is no longer read` is documentation about
 * a dead knob, not a use of it. Counting it as a use is how a phantom survives
 * — and it is why the earlier version had to special-case this checker's own
 * source file, whose header comment names the variables it hunts. With comments
 * stripped, that hack is unnecessary: the header no longer lies to it.
 *
 * Line comments only strip from an unquoted `//` or `#`, so a URL or a
 * `'#anchor'` string is left intact.
 */
export function commentStyleFor(relPath) {
  return /\.(ts|mjs|js)$/.test(relPath) ? 'c' : 'hash';
}

export function stripComments(body, style = 'c') {
  let out = '';
  let quote = null; // the open quote character, or null in code
  let inBlock = false;
  let inLine = false;

  for (let i = 0; i < body.length; i += 1) {
    const c = body[i];
    const next = body[i + 1];

    if (inLine) {
      if (c === '\n') {
        inLine = false;
        out += c;
      }
      continue;
    }

    if (inBlock) {
      if (c === '*' && next === '/') {
        inBlock = false;
        i += 1;
      } else if (c === '\n') {
        // Keep newlines so line numbers and `^`-anchored patterns still line up.
        out += c;
      }
      continue;
    }

    if (quote) {
      out += c;
      if (c === '\\') {
        // Escaped character: copy it verbatim so a `\"` cannot close the string.
        if (next !== undefined) {
          out += next;
          i += 1;
        }
      } else if (c === quote) {
        quote = null;
      } else if (c === '\n' && quote !== '`') {
        // An unterminated single- or double-quoted string cannot span lines;
        // recover at the newline rather than swallowing the rest of the file.
        quote = null;
      }
      continue;
    }

    if (c === '"' || c === "'" || c === '`') {
      quote = c;
      out += c;
      continue;
    }
    if (style === 'c') {
      // `/*` and `//` are comment syntax only in the C-family files. In a YAML
      // workflow `services/*/Dockerfile` is a path and `https://…` is a URL;
      // reading either as a comment opener swallowed 87% of one workflow.
      if (c === '/' && next === '*') {
        inBlock = true;
        i += 1;
        continue;
      }
      if (c === '/' && next === '/') {
        inLine = true;
        continue;
      }
    } else if (c === '#' && (i === 0 || /[\s;&|(]/.test(body[i - 1]))) {
      // In shell a `#` opens a comment only at the start of a word. Treating
      // every unquoted `#` as one truncated the rest of its line on parameter
      // length (`${#arr[@]}`) and prefix strip (`${VAR##*/}`), so an unrelated
      // shell idiom decided whether a read on that line was visible. That was
      // harmless while `.sh` files were only wiring evidence and became
      // load-bearing when they became scanned sources.
      inLine = true;
      continue;
    }
    out += c;
  }
  return out;
}

/**
 * Every `BACKTICKED_IDENTIFIER` a doc offers, with where it said it.
 *
 * The trailing `(?:_\*)?` is load-bearing. Docs write families as
 * `` `KAFKA_SASL_*` ``, and an earlier form of this pattern ended in `\*?` —
 * which cannot consume the `_` before the star, so it matched none of the three
 * families in the tree and the family logic below was unreachable. Nothing
 * failed, because a feature that never fires looks exactly like a feature
 * nobody needed.
 */
export function collectDocumented(docs) {
  const documented = new Map(); // NAME -> [ "path:line", … ]
  const families = new Set(); // `KAFKA_SASL_*` -> `KAFKA_SASL_`
  for (const [path, body] of Object.entries(docs)) {
    body.split('\n').forEach((line, i) => {
      for (const m of line.matchAll(/`([A-Z][A-Z0-9_]{1,}(?:_\*)?)`/g)) {
        const name = m[1];
        if (name.endsWith('_*')) {
          families.add(name.slice(0, -1));
          continue;
        }
        if (!documented.has(name)) documented.set(name, []);
        documented.get(name).push(`${path}:${i + 1}`);
      }
    });
  }
  return { documented, families };
}

/**
 * Is this repo-relative path a file whose contents count as evidence of wiring?
 *
 * This lived inline in the shell, where it could not be tested, and that is how
 * a guard survived there that could never fire: an explicit `.env.example`
 * exclusion sat *after* the extension test that already rejected it. Deleting
 * dead code is easy; noticing it is not, and untestable logic is where it hides.
 *
 * Two things are excluded, by two different mechanisms — worth stating because
 * this function has now grown a dead guard twice:
 *
 *   - **Operator templates** (`.env.example`). Nothing reads them, so a variable
 *     appearing only there is documentation; adding one line used to flip this
 *     gate green. The extension allowlist is what excludes them — `.example` is
 *     not an allowed extension — and no separate guard can help, because no path
 *     can end in both `.env.example` and one of the allowed extensions. A guard
 *     for this was written three times and was unreachable all three; the third
 *     attempt reordered the checks and claimed that made it reachable, which the
 *     regexes cannot support. There is no guard now, on purpose.
 *   - **Test files**, which hold *depictions* of reads — this module's own suite
 *     passes `"process.env['X']"` as a string, and no scan tells a read from a
 *     picture of one. This exclusion IS load-bearing: `a.spec.ts` matches the
 *     allowlist, so removing the check would let every spec back in.
 */
/**
 * Hidden directories are where tooling keeps its state, and that state must not
 * decide this gate's verdict.
 *
 * Orthogonal to `.gitignore`, not redundant with it: `.gitignore` covers
 * `.claude/` and `.playwright-mcp/` fully and `.pi/` partially, and says nothing
 * about `.vscode/`, `.zed/`, `.cursor/`, or whatever the next tool creates.
 *
 * Per COMPONENT, not per path prefix. The recursive walk this replaced tested
 * every directory name it descended through, so it skipped
 * `packages/e2e-tests/.vscode/` as readily as `.vscode/`. A `startsWith('.')`
 * test on the whole relative path covers only depth 0, which left the rule
 * incoherent — `.vscode/NOTES.md` skipped, `packages/e2e-tests/.vscode/NOTES.md`
 * scanned — and "hidden directories are where tooling keeps their state" does
 * not stop being true one level down.
 *
 * Three rounds on this one filter: deleted from two sites, restored to one,
 * then restored in a form that only covered the top level. Each fix was right
 * about the case in front of it. The test that discriminates is the NESTED one —
 * a top-level case passes under all three spellings, including the two that were
 * wrong.
 */
export function isHiddenPath(rel) {
  return rel.split('/').some((seg, i) => seg.startsWith('.') && !(i === 0 && seg === '.github'));
}

/**
 * One answer to "is this a test file", because the shell and this module each
 * had their own and they disagreed.
 *
 * `envReaders` excluded `.spec.ts` only, then was widened to `[cm]?[jt]s`; this
 * function still excluded `(ts|js|mjs)`. The gap pointed the unsafe way: a
 * `foo.spec.cjs` was no longer a candidate, so it could never be demanded into
 * `CONFIG_SOURCES`, yet was still accepted as proof a documented name is live —
 * able to silence a phantom while never being asked to justify one. That is the
 * recurring shape in this gate, and widening one predicate without the other
 * widened the gap instead of closing it.
 *
 * `.d.[cm]?ts` is here too: the extension filter accepts `.mts` and `.cts`, so a
 * `.d.mts` declaring `process.env` would otherwise have become a candidate.
 */
export function isTestFile(relPath) {
  return /\.(spec|test)\.[cm]?[jt]s$/.test(relPath) || /\.d\.[cm]?ts$/.test(relPath);
}

export function isWiringEvidence(relPath) {
  if (/(^|\/)(test|tests|__tests__)\//.test(relPath) || isTestFile(relPath)) return false;
  return /\.(ts|mjs|js|ya?ml|sh|tpl)$/.test(relPath) || /(^|\/)Makefile$/.test(relPath);
}

/**
 * Does this text name this exact identifier, outside a comment?
 *
 * One function, because every place that has re-asked this question in its own
 * words has re-acquired the same two bugs. The site check shipped with both
 * within an hour of each other: a raw `.includes()` accepted a doc comment as a
 * consumer, and then — comments stripped — accepted
 * `ORCA_E2E_SANDBOX_HARNESS_ENABLED` as evidence for
 * `ORCA_E2E_SANDBOX_HARNESS`, which is the substring bug a reviewer reported
 * against the doc side weeks earlier.
 *
 * `\b` is not enough: `_` is a word character, so `\bHTTP_PORT\b` matches inside
 * `INTERNAL_HTTP_PORT`. The guards below reject an adjacent identifier
 * character on either side.
 */
export function namesIdentifier(text, name, style = 'c') {
  const n = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`(?<![A-Za-z0-9_])${n}(?![A-Za-z0-9_])`).test(stripComments(text, style));
}

/** The reasons a documented name can legitimately not be read by a config. */
export const EXEMPTION_REASONS = new Set([
  'not-an-env-var',
  'wired-elsewhere',
  'intentionally-absent',
  // A real variable, read by real code — on the implementation branch, not here.
  // Design docs land ahead of the decomposed implementation PRs, so a doc can
  // name a knob whose reader has not merged yet. Deliberately carries no `site`:
  // the file that reads it does not exist in this tree, so there is nothing for
  // `validateExemptionSites` to check. What keeps it honest is the dead
  // direction below — the entry fails the moment the reader lands (the name
  // becomes declared) or the docs stop naming it, so it cannot outlive its
  // purpose. The `note` must name the branch and the reading file.
  'ahead-of-implementation',
]);

/**
 * Validate the register's shape. Pure; the shell supplies the parsed YAML.
 *
 * Mirrors `validateDecisions` in `conformance-core.mjs`, deliberately: this repo
 * already had a reviewed answer to "how do we record a deliberate exception,"
 * and the first version of this gate wrote a regex instead.
 */
export function validateExemptions(register) {
  const problems = [];
  if (!register || typeof register !== 'object')
    return ['env-exemptions.yaml did not parse to an object'];
  if (register.version !== 1)
    problems.push(`unsupported register version: ${JSON.stringify(register.version)}`);
  if (!Array.isArray(register.exemptions)) return [...problems, '`exemptions` must be a list'];

  const seen = new Set();
  for (const [index, entry] of register.exemptions.entries()) {
    const where = `exemptions[${index}]${entry?.name ? ` (${entry.name})` : ''}`;
    if (!entry || typeof entry !== 'object') {
      problems.push(`${where}: not a mapping`);
      continue;
    }
    if (typeof entry.name !== 'string' || !/^[A-Z][A-Z0-9_]{1,}$/.test(entry.name)) {
      problems.push(`${where}: \`name\` must be a SCREAMING_SNAKE identifier`);
    } else if (seen.has(entry.name)) {
      problems.push(`${where}: duplicate \`name\``);
    } else {
      seen.add(entry.name);
    }
    if (!EXEMPTION_REASONS.has(entry.reason)) {
      problems.push(`${where}: \`reason\` must be one of ${[...EXEMPTION_REASONS].join(', ')}`);
    }
    // The note is the whole point: an exemption without a stated reason is the
    // silent regex rescue this register replaced, just spelled differently.
    if (typeof entry.note !== 'string' || entry.note.trim().length < 10) {
      problems.push(`${where}: \`note\` must say why, in a sentence`);
    }
    if (entry.reason === 'wired-elsewhere' && (typeof entry.site !== 'string' || !entry.site)) {
      problems.push(`${where}: \`wired-elsewhere\` requires \`site\` — the file that consumes it`);
    }
    if (entry.reason !== 'wired-elsewhere' && entry.site !== undefined) {
      problems.push(`${where}: \`site\` is only meaningful for \`wired-elsewhere\``);
    }
  }
  return problems;
}

/**
 * Every `site` must exist and still contain the name.
 *
 * This is the half that stops the register rotting into false coverage, and it
 * is not hypothetical: retiring `delivery-phases.md` left four conformance rules
 * — 30 operations — pointed at a deleted file, and nothing failed. An exemption
 * whose justification cannot be reached is the same as no exemption.
 *
 * Comments are stripped **here**, not by the caller. The first version left that
 * to the shell and was immediately bypassed: deleting both real uses of
 * `ORCA_GIT_CREDS_URL` from `dispatcher.ts` left a doc comment naming it on line
 * 380, and a raw `.includes()` accepted that as justification. Same category as
 * every other round on this gate — a mention is not a use — reappearing inside
 * the check written to retire it. Putting the rule in the function that depends
 * on it is what stops the next caller re-introducing it.
 *
 * A stripped mention is the right strength, deliberately. Demanding an access
 * shape would mean re-deriving per-language wiring for TypeScript, YAML,
 * workflows and shell — the inference this register exists to replace. The
 * register records a human's claim that a file consumes the variable; this only
 * checks the claim has not rotted.
 *
 * Takes file access as callbacks so this module stays pure and unit-testable;
 * the I/O shell passes the real ones.
 *
 * @param io `{ exists(relPath) -> boolean, read(relPath) -> string }`
 */
export function validateExemptionSites(register, io) {
  const problems = [];
  for (const entry of register?.exemptions ?? []) {
    if (entry?.reason !== 'wired-elsewhere' || typeof entry.site !== 'string' || !entry.site)
      continue;
    if (!io.exists(entry.site)) {
      problems.push(`${entry.name}: site \`${entry.site}\` does not exist`);
      continue;
    }
    if (!namesIdentifier(io.read(entry.site), entry.name, commentStyleFor(entry.site))) {
      problems.push(
        `${entry.name}: site \`${entry.site}\` no longer names it outside a comment ` +
          `(a longer identifier containing it does not count)`,
      );
    }
  }
  return problems;
}

/**
 * The register's header enumerates every single-token `not-an-env-var` entry, so
 * the claim "these exist only because the collectors over-collect" is checkable
 * rather than illustrative. This checks that the enumeration is still true.
 *
 * It was written by hand twice and was wrong both times — first counting three
 * config-side entries as disjoint from twenty-one single-token ones when they
 * were a subset, then omitting `NODE` the same commit that added it. A sentence
 * that has to be re-derived by eye on every register change will drift on some
 * register change; this is the same "make the claim checkable" move the file
 * argues for everywhere else, applied to the file itself.
 *
 * The inventory is delimited so parsing is exact rather than prose-guessing.
 */
export function validateRegisterInventory(rawRegisterText, exemptions) {
  const block = /inventory >>>([\s\S]*?)<<< end inventory/.exec(rawRegisterText);
  if (!block) return ['env-exemptions.yaml: the single-token inventory block is missing'];

  const listed = new Set(block[1].match(/\b[A-Z][A-Z0-9_]*\b/g) ?? []);
  const actual = new Set(
    [...exemptions.values()]
      .filter((e) => e.reason === 'not-an-env-var' && !e.name.includes('_'))
      .map((e) => e.name),
  );

  const problems = [];
  for (const name of actual) {
    if (!listed.has(name))
      problems.push(
        `${name} is a single-token not-an-env-var entry but the header inventory omits it`,
      );
  }
  for (const name of listed) {
    if (!actual.has(name))
      problems.push(
        `the header inventory lists ${name}, which is no longer a single-token not-an-env-var entry`,
      );
  }
  return problems;
}

/** Index the register by name for `compare`. */
export function indexExemptions(register) {
  return new Map((register?.exemptions ?? []).map((entry) => [entry.name, entry]));
}

/**
 * Every file that reads `process.env` must be scanned, or explicitly excused.
 *
 * This is the asymmetry that made the gate green while four variables were read
 * and documented nowhere. `isWiringEvidence` lets *any* source file prove a
 * documented name is still live, but only `CONFIG_SOURCES` can demand that a
 * name be documented. A file on the wrong side of that split is trusted when it
 * would silence a finding and ignored when it would raise one.
 *
 * A hand-listed inclusion list cannot fail loudly, because the failure is a line
 * nobody wrote. So the list is checked against the tree: a file that reads the
 * environment and appears in neither `scanned` nor `excused` fails the run, and
 * whoever adds it must say which it is.
 *
 * `excused` is a map of path -> reason, not a bare list, so an omission carries
 * its justification the way a register entry does.
 */
export function validateScanCoverage({
  candidates,
  scanned,
  excused = new Map(),
  what = 'reads the environment',
}) {
  const known = new Set(scanned);
  const candidateSet = new Set(candidates);
  const problems = [];
  for (const rel of candidates) {
    if (known.has(rel) || excused.has(rel)) continue;
    problems.push(`${rel} ${what} but is neither scanned nor excused`);
  }
  for (const rel of excused.keys()) {
    if (known.has(rel)) {
      problems.push(`${rel} is both scanned and excused — delete the excuse`);
    } else if (!candidateSet.has(rel)) {
      problems.push(`${rel} is excused but no longer ${what} — delete the excuse`);
    }
  }
  return problems;
}

/**
 * The floor below which this gate is not measuring anything, per source file.
 *
 * A global floor was the first attempt and it measured almost nothing. With two
 * config files carrying 153 of 189 names, blanking any of the nine small
 * sources stayed above a global 120 — `bootstrap-admin.ts` could lose all nine
 * of its `ORCA_BOOTSTRAP_*` reads and the run stayed green. Worse, the four
 * smallest could go dark *together* and land on 120 exactly, taking eighteen
 * operator-facing variables out of the check while the total still cleared the
 * bar. A sum cannot see one addend go to zero.
 *
 * So each source holds its own line. A refactor that moves reads out of both
 * recognised shapes — to a config schema, say — now fails naming the file it
 * happened in, instead of being absorbed by the two big files' slack.
 *
 * The three sources pinned at 0 pass `process.env` wholesale to a callee and
 * name nothing; they are listed so that "declares nothing" stays a recorded
 * fact rather than an absence.
 */
export function validateSourceFloors(counts, floors) {
  const problems = [];
  for (const [rel, min] of Object.entries(floors)) {
    const actual = counts[rel];
    if (actual === undefined) {
      problems.push(`${rel} has a floor but was not scanned`);
    } else if (actual < min) {
      problems.push(
        `${rel} declares ${actual} variables, expected at least ${min} — ` +
          `either its reads moved out of the recognised shapes, or the floor is stale`,
      );
    }
  }
  // And the other direction, for the same reason `validateScanCoverage` exists:
  // a floors map that is only ever read forwards is an inclusion list, and an
  // inclusion list cannot fail loudly — the failure is a line nobody wrote. A
  // source added to the scan set but not to the map would carry no floor and
  // could go to zero silently, which is precisely the bug this file was changed
  // to fix, one function over.
  for (const rel of Object.keys(counts)) {
    if (!(rel in floors)) {
      problems.push(`${rel} is scanned but has no floor — pin one, even if it is 0`);
    }
  }
  return problems;
}

export const MIN_DECLARED = 180;

/**
 * The same floor for the other side.
 *
 * `MIN_DECLARED` guards `declared` only, and the doc side had no equivalent: six
 * of the twelve hand-listed doc files could disappear and the run stayed
 * byte-identically green, because the phantom check simply stopped covering
 * whatever they held. The shell now treats a missing DOC_FILE as fatal, which
 * catches the enumerated ones; this catches erosion of the walked tree, which is
 * not enumerated anywhere.
 */
export const MIN_DOCUMENTED = 140;

/**
 * Is this variable wired to anything — read, or set for something else to read?
 *
 * The phantom check used to ask "does the name appear anywhere in the tree,"
 * which is not the same question. Two things defeated it: a comment naming a
 * dead knob, and then — after comments were stripped — this gate's own unit
 * test, which contains the string `'# MEMORY_WATCHER_POLL_INTERVAL_MS is gone'`.
 * The `#` is inside quotes, so `stripComments` correctly preserved it, and the
 * single variable this gate exists to catch looked live again. Excluding the
 * test file would have been the third instance-level patch; the category is
 * that a *mention* is not a *use*.
 *
 * So the evidence must be an access shape: the name reached through the
 * environment object. That is a read in practice — every documented name alive
 * only through this function is kept alive by one — but the regex sees the
 * access, not the direction, so `process.env.X = v` and `delete process.env.X`
 * would satisfy it too. Narrowing to reads alone would mean guessing at intent
 * from punctuation, which is the move that produced every bug in this file.
 *
 * An earlier version also accepted "setting" shapes (`X:`, `${X}`, a bare
 * quoted `'X'`) so that variables this repo exports for something else to read
 * would pass. Those were too loose to keep: `['SOME_VAR', 'OTHER']` is an
 * unrelated array, and `\bX\s*[:=]` fires on any object key in the tree. They
 * were rescuing 19 documented names, 7 of which are not environment variables
 * at all (`MUST_BE_CORE`, `MAX_MEMORY_PATH_LENGTH`, …) but exported constants
 * named in prose, matched via their `export const NAME =`.
 *
 * They could not simply be tightened, either. `subprocess-entry.ts` reads
 *
 *     const DEFAULT_CWD_ENV = 'SANDBOX_HARNESS_DEFAULT_CWD';
 *     process.env[DEFAULT_CWD_ENV]
 *
 * and no name-based pattern resolves that indirection — requiring env-adjacency
 * would have flagged two genuinely-wired variables. The doc side over-collected
 * and the wiring side over-accepted, so the two errors cancelled; fixing either
 * alone turns the gate red on the other's noise.
 *
 * A regex cannot infer intent, so it stopped trying: anything consumed outside
 * a scanned config is now *declared* in `env-exemptions.yaml`, where adding one
 * is a reviewable line in a diff rather than a silent rescue.
 *
 * **Test files are not evidence.** They used to be, on the narrower rule that a
 * genuine `process.env.X` in a spec is a real consumer while a spec merely
 * naming X is prose. That rule cannot hold, because a file whose subject is this
 * gate contains *depictions* of reads: this module's own suite passes
 * `"if (process.env['ORCA_E2E_SANDBOX_HARNESS'])"` as a string argument, and no
 * textual scan can tell a read from a picture of one. Rename the two real
 * consumers of that variable and the fixture alone kept it alive — proven by
 * mutation, and the sixth appearance of "a mention is not a use".
 *
 * Excluding the suite would have been the fourth instance-level patch of that
 * category. Dropping test evidence entirely removes it: a variable that exists
 * only for the e2e suite is declared in the register with the spec as its site,
 * which is more useful to a reader than a silent rescue anyway.
 */
export function isWired(runtimeText, name) {
  const n = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

  // Touching the variable through the environment object, in a non-test file.
  // Two patterns, not four: `\benv\.` already matches inside `process.env.`,
  // because the `.` before `env` is a word boundary. The two `process\.env`
  // alternations this used to carry could never fire on anything the bare pair
  // missed — dead code that read as extra coverage.
  const accesses = [`\\benv\\.${n}\\b`, `\\benv\\[['"\`]${n}['"\`]\\]`];

  return new RegExp(accesses.join('|')).test(runtimeText);
}

/**
 * Compare both directions and return the problems.
 *
 * @param declared   Map NAME -> origin, from `collectDeclared`
 * @param documented Map NAME -> sites, from `collectDocumented`
 * @param families   Set of documented wildcard prefixes
 * @param sourceText concatenated source, **comment-stripped** by the caller
 */
export function compare({
  declared,
  documented,
  families,
  runtimeText,
  exemptions = new Map(),
  envAccessed = new Set(),
}) {
  const problems = { undocumented: [], phantom: [], resurrected: [], dead: [], floor: null };

  if (declared.size < MIN_DECLARED) {
    problems.floor =
      `only ${declared.size} environment variables were collected from the config files ` +
      `(expected at least ${MIN_DECLARED}). Either the read patterns no longer match the code, ` +
      `or this repo genuinely shrank — if it is the latter, lower the floor in the same commit. ` +
      `Until then this check is not measuring anything.`;
  }
  if (documented.size < MIN_DOCUMENTED) {
    // Not `else if`: a path break collapses both sides at once, and reporting
    // only the first sends the operator back for a second run to learn the rest.
    const second =
      `only ${documented.size} documented identifiers were collected (expected at least ` +
      `${MIN_DOCUMENTED}). Documentation has gone missing from the scan rather than from the ` +
      `repo — check that every doc tree still resolves before trusting a green run.`;
    problems.floor = problems.floor ? `${problems.floor}\n\nAlso: ${second}` : second;
  }

  const coveredByFamily = (name) => [...families].some((f) => name.startsWith(f));
  // One predicate for both directions. `undocumented` honoured wildcard families
  // and the dead-exemption check did not, so collapsing per-variable rows into a
  // `KAFKA_SASL_*` row would report a live exemption as dead — and the printed
  // remedy is to delete it, discarding a recorded judgement over a formatting
  // change.
  const isDocumented = (name) => documented.has(name) || coveredByFamily(name);

  // Exact identifier membership, never substring: `INTERNAL_HTTP_PORT` in the
  // docs must not satisfy `HTTP_PORT`, which is a different variable on a
  // different listener.
  for (const name of declared.keys()) {
    if (isDocumented(name)) continue;
    // `not-an-env-var` means exactly that, so it holds in BOTH directions. The
    // reason previously silenced only the phantom check, which left a declared
    // over-collection — `'BEGIN'` in a PEM guard, `'SIGINT'` in a signal
    // handler — demanding documentation for something that is not a variable.
    if (exemptions.get(name)?.reason === 'not-an-env-var') continue;
    problems.undocumented.push({ name, origin: declared.get(name) });
  }

  for (const [name, sites] of documented) {
    const exempt = exemptions.get(name);

    // A variable documented as absent must stay absent. Finding it in a config
    // file means the invariant the doc asserts has been broken.
    if (exempt?.reason === 'intentionally-absent') {
      if (declared.has(name)) {
        problems.resurrected.push({ name, origin: declared.get(name), sites });
      }
      continue;
    }

    if (declared.has(name)) continue;
    if (isWired(runtimeText, name)) continue;
    if (exempt) continue;

    // Single tokens used to be skipped here, to avoid 17 phantoms like `GET`
    // and `INSERT`. That left a hole: once `PORT` became declared, a doc for it
    // could go stale without the phantom check noticing. The skip is gone and
    // the seventeen are declared in the register instead — inference is what
    // every bug in this gate has been made of, so the register absorbs the cost.

    problems.phantom.push({ name, sites });
  }

  // The second direction, without which the register only ever grows and starts
  // reading as coverage it no longer provides: an entry is dead once the docs
  // stop mentioning the name, or once a config genuinely reads it.
  for (const [name, entry] of exemptions) {
    if (entry.reason === 'not-an-env-var' && envAccessed.has(name)) {
      // `not-an-env-var` claims the name is not a variable. Only one thing
      // contradicts that: a config reading it through `process.env`. Being
      // quoted in a config does not — `'SIGINT'`, `'BEGIN'`, and the HTTP verbs
      // in a fixture's request options are all literals — and being named in a
      // doc does not either, since that is why the entry exists.
      problems.dead.push({
        name,
        why: `read from process.env by ${declared.get(name) ?? 'a config source'} — it is an env var after all`,
      });
    } else if (
      entry.reason === 'not-an-env-var'
        ? !isDocumented(name) && !declared.has(name)
        : !isDocumented(name)
    ) {
      // A `not-an-env-var` entry can be earning its keep on either side — a
      // constant named in prose, or a string literal the collector picked up out
      // of a config file. It is dead only when neither is true any more.
      problems.dead.push({ name, why: 'neither the docs nor the config mention it any more' });
      // `not-an-env-var` is allowed to be earning its keep on the declared side:
      // the collector lifts quoted literals out of config files, and suppressing
      // that over-collection is half of what the reason is for.
    } else if (
      entry.reason !== 'intentionally-absent' &&
      entry.reason !== 'not-an-env-var' &&
      declared.has(name)
    ) {
      problems.dead.push({
        name,
        why: `now read by ${declared.get(name)} — the exemption is obsolete`,
      });
    }
  }

  problems.undocumented.sort((a, b) => a.name.localeCompare(b.name));
  problems.phantom.sort((a, b) => a.name.localeCompare(b.name));
  problems.dead.sort((a, b) => a.name.localeCompare(b.name));
  problems.resurrected.sort((a, b) => a.name.localeCompare(b.name));
  return problems;
}

/** True when nothing is wrong. */
export function isClean(problems) {
  return (
    !problems.floor &&
    problems.undocumented.length === 0 &&
    problems.phantom.length === 0 &&
    problems.resurrected.length === 0 &&
    problems.dead.length === 0
  );
}
