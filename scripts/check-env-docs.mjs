#!/usr/bin/env node
// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Fails when the docs and the config code disagree about environment variables.
 *
 * Two directions, both of which have bitten us:
 *
 *   undocumented — a variable the config reads that no doc mentions. An
 *                  operator cannot discover it without reading the source.
 *   phantom      — a variable the docs describe that nothing reads. Worse than
 *                  silence: the memory watcher's poll interval was documented
 *                  in six places, with a default, as a tunable that did nothing.
 *
 * This file is the I/O shell. The matching logic lives in `env-docs-core.mjs`,
 * which is pure and unit-tested — because the first version fused the two and
 * shipped with three false negatives nobody could have caught with a test that
 * did not exist.
 */
import { execFileSync } from 'node:child_process';
import { existsSync, lstatSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { load } from 'js-yaml';
import {
  collectDeclared,
  collectEnvAccessed,
  collectDocumented,
  compare,
  indexExemptions,
  isClean,
  commentStyleFor,
  isHiddenPath,
  isTestFile,
  isWiringEvidence,
  stripComments,
  validateExemptions,
  validateRegisterInventory,
  validateScanCoverage,
  validateSourceFloors,
  validateExemptionSites,
} from './env-docs-core.mjs';

// `fileURLToPath`, not `.pathname`: the latter keeps percent-encoding, so a
// repo path containing a space became `/tmp/rev%20space`. Harmless while ROOT
// was only joined onto, and actively misleading once it became `cwd` for git —
// the failure read as `spawnSync git ENOENT`, i.e. "git is not installed".
const ROOT = fileURLToPath(new URL('..', import.meta.url)).replace(/\/$/, '');

/**
 * Every file this repository actually contains — tracked, plus untracked files
 * that are not ignored.
 *
 * One list, because four separate walks each carried their own skip-list and the
 * skip-lists disagreed. Two reviewers found the same defect through two of them:
 * a walk descended into `.claude/worktrees` (hidden) and another into `logs/`
 * (not hidden) — both gitignored, both making the gate's verdict depend on what
 * happened to be lying around in the checkout. A gate that is red locally and
 * green in CI teaches people to ignore it.
 *
 * `--others --exclude-standard` keeps untracked-but-unignored files, so a
 * brand-new reader nobody has staged yet is still a candidate. That is the case
 * the coverage checks exist for, and dropping it would have traded one blind
 * spot for another.
 */
function listRepoFiles() {
  try {
    return (
      execFileSync(
        'git',
        // `-z` for NUL-separated, UNQUOTED paths. Without it `core.quotePath` (on by
        // default) emits a non-ASCII path as `"src/na\303\257ve.md"` — literal quotes
        // and octal escapes — which fails `readFileSync` and breaks `repoFilesUnder`'s
        // prefix match, so the leading quote read as a top-level directory named
        // `"services`. A filename is not required to be ASCII.
        ['ls-files', '-z', '--cached', '--others', '--exclude-standard'],
        {
          cwd: ROOT,
          encoding: 'utf8',
          maxBuffer: 64 * 1024 * 1024,
          stdio: ['ignore', 'pipe', 'pipe'],
        },
      )
        .split('\0')
        .filter(Boolean)
        // `--cached` enumerates the INDEX, so it still names a tracked file after a
        // plain `rm`, during a sparse checkout, or mid-rebase. The four disk walks
        // this replaced could only ever name files that exist, and three new read
        // sites took an entry straight to `readFileSync`/`lstatSync` with no guard —
        // turning a missing file into a raw ENOENT trace under exit 1, the code that
        // means "your docs are wrong". Filtering here restores the old invariant for
        // all three consumers at once.
        .filter((rel) => existsSync(join(ROOT, rel)))
    );
  } catch (err) {
    // Surface the dependency rather than a stack trace. This gate asks git what
    // the repository contains, so it cannot run against a tarball export or a
    // build context without `.git` — the same standard `readIfPresent` holds
    // itself to a few lines down.
    console.error('check-env-docs: could not list repository files with git.');
    console.error('  This check reads `git ls-files`, so it needs a git checkout — not an');
    console.error('  export or a build context with .git stripped.');
    console.error(
      `  git said: ${
        String(err.stderr ?? err.message)
          .trim()
          .split('\n')[0]
      }`,
    );
    process.exit(2);
  }
}

const REPO_FILES = listRepoFiles();

const repoFilesUnder = (dir) =>
  REPO_FILES.filter((rel) => rel === dir || rel.startsWith(`${dir}/`));

/**
 * The files that define each service's environment surface.
 *
 * Not only `config.ts`: the offline bootstrap and key-rotation entrypoints have
 * their own operator-facing surface, and it was invisible here — sixteen
 * variables, including every `ORCA_BOOTSTRAP_*`, read by checked-in scripts and
 * documented nowhere, while this gate reported "env docs in sync".
 */
const CONFIG_SOURCES = [
  'packages/sdk-harness/src/credentials.ts',
  'packages/pi-harness/src/credentials.ts',
  'services/harness-server/src/config.ts',
  'services/registry-service-ts/src/config.ts',
  'services/session-runner/src/config.ts',
  'services/environment-worker/src/config.ts',
  'services/observability-exporter/src/config.ts',
  'packages/transcript-store/src/kafka/config.ts',
  // `orca worker` hand-copies the seven worker env-var names, because
  // `@orca/environment-worker` exports only `main`/`startWorker` — there is no
  // symbol to import. Scanning it is the next-best guarantee: the CLI's copy of a
  // name must be documented in the worker's own AGENTS.md, so a name that drifts
  // from the worker's contract becomes undocumented and fails here.
  'packages/oeadm/src/commands/worker.ts',
  'services/registry-service-ts/src/bootstrap-admin.ts',
  'services/registry-service-ts/src/create-admin-key.ts',
  'services/registry-service-ts/src/create-platform-key.ts',
  'services/harness-server/src/main.ts',
  'services/harness-server/src/harness/claude/index.ts',
  'services/registry-service-ts/src/migrate.ts',
  'services/sandbox-harness/src/index.ts',
  'services/sandbox-harness/src/subprocess-entry.ts',
  'services/sandbox-harness/src/providers/claude.ts',
  'services/sandbox-harness/src/session-manager.ts',
  'packages/e2e-tests/scripts/kind-helm-e2e.ts',
  'packages/e2e-tests/scripts/run-agent-tests.sh',
  'packages/e2e-tests/src/client.ts',
  'packages/e2e-tests/src/seed.ts',
  'packages/file-store/drizzle.config.ts',
  'packages/memory-store/drizzle.config.ts',
  'services/harness-server/scripts/spike-orca-default.ts',
  'services/harness-server/src/sandbox/local/runtime.ts',
  'packages/sandbox-runtime/src/local/runtime.ts',
  'services/registry-service-ts/drizzle.config.ts',
  'services/registry-service-ts/src/main.ts',
  'services/registry-service-ts/src/secrets/runtime-resolver.ts',
  'services/sandbox-harness/src/providers/types.ts',
  'services/sandbox-harness/src/session.ts',
  'services/sandbox-harness/docker-entrypoint.sh',
  'services/dev/scripts/start-services.sh',
  'services/dev/scripts/test-transcript-avro.sh',
  'services/dev/scripts/start-self-hosted.sh',
  'services/dev/scripts/stop-self-hosted.sh',
  'services/dev/scripts/dev-infra-services.sh',
  'services/dev/scripts/init-secrets.sh',
  'services/dev/scripts/prepare-ai-gateway-image.sh',
  'services/dev/scripts/stop-services.sh',
  'services/dev/scripts/with-dev-env.sh',
  '.github/scripts/pnpm-install-with-retry.sh',
  '.github/scripts/load-kind-image-with-retry.sh',
  '.github/scripts/supervise-kubectl-port-forward.sh',
  '.github/scripts/changed-areas.sh',
];

// This list is not maintained by hand alone: `validateScanCoverage` walks
// `services/` and `packages/` and fails the run for any file that reads
// `process.env` and appears in neither this list nor `EXCUSED_SOURCES`. So the
// rule is the inverse of what stood here until recently — every reader is
// scanned, and a considered "no" has to be written down rather than expressed
// by leaving a line out.
//
// The paragraph this replaces said `local/runtime.ts` was "deliberately NOT
// here", on the grounds that its eleven POSIX locale forwards bought no
// operator-facing coverage. That file is at line 66, was added when the
// coverage check went in, and its POSIX names are absorbed by the register and
// the harness README instead. The comment outlived the decision it recorded and
// pointed the next maintainer at reintroducing the bug.

/**
 * Markdown outside `docs/` that names environment identifiers and is
 * deliberately not scanned, with the reason.
 *
 * These document the sandbox *image's* env contract — what an entrypoint or a
 * mount helper reads inside the container — which is a different surface from
 * the services' operator-facing configuration. Scanning them would demand
 * register entries for a Linux capability, an `open(2)` flag, and the AWS
 * credential names the image receives, without gating anything this repo's
 * configs read.
 *
 * The boundary is defensible; leaving it unwritten was not. `DOC_FILES` is the
 * third hand-maintained inclusion list in this gate, after `CONFIG_SOURCES` and
 * `SOURCE_FLOORS`, and the first two each silently omitted something real.
 */
const EXCUSED_DOCS = new Map([
  [
    'services/harness-server/sandbox-templates/orca-default/README.md',
    'the orca-default image env contract, read by its entrypoint and s3fs mount helper',
  ],
  [
    'services/harness-server/sandbox-templates/orca-opensandbox/README.md',
    'the OpenSandbox image env contract',
  ],
  [
    'services/harness-server/sandbox-templates/orca-opensandbox-server/README.md',
    'the OpenSandbox server image env contract',
  ],
  [
    'services/harness-server/src/sandbox/local/README.md',
    'names the POSIX variables the local runtime sets, already documented in the harness README',
  ],
  ['charts/opensandbox-patches/README.md', 'names Linux capabilities in a securityContext patch'],
  [
    'services/environment-image/README.md',
    'the Environment image env contract (ENVIRONMENT_*, ORCA_ENVIRONMENT_TOKEN, REGISTRY_TUNNEL_BASE_URL, RUNNER_LAUNCH_COMMAND) — read by the worker inside the container',
  ],
]);

/** Where an operator is entitled to look a variable up. */
const DOC_ROOTS = ['docs'];
const DOC_FILES = [
  'charts/orca-managed-agents/README.md',
  'README.md',
  'AGENTS.md',
  'services/harness-server/README.md',
  'services/harness-server/sandbox-templates/orca-agentenv/README.md',
  'services/observability-exporter/README.md',
  'services/registry-service-ts/README.md',
  'services/sandbox-harness/README.md',
  'services/dev/README.md',
  'packages/e2e-tests/README.md',
  'packages/file-store/README.md',
  'packages/memory-store/README.md',
  'packages/skill-store/README.md',
  'packages/transcript-store/README.md',
  'packages/harness-catalog/README.md',
  // The per-directory agent guides are documentation too — this PR mirrors hard
  // rule 1 into each — and they name eight environment variables between them.
  // Scanning only their sibling READMEs left those mentions unchecked.
  'packages/AGENTS.md',
  'services/harness-server/AGENTS.md',
  'services/registry-service-ts/AGENTS.md',
  'services/sandbox-harness/AGENTS.md',
  // The session-runner had no doc surface at all, which is how its own operator
  // knobs — the idle watchdog and the `ANTHROPIC_ALLOWED_MODELS` allow-list its
  // persistent provider enforces — had nowhere to be registered, and how its
  // `config.ts` came to be excused on the false grounds that it read nothing of its
  // own. This is that surface; `services/session-runner/src/config.ts` is scanned
  // against it in `CONFIG_SOURCES`.
  'services/session-runner/AGENTS.md',
  // Same story, one service over, and the second time the same excuse hid the
  // same kind of surface: the environment-worker's `config.ts` was excused as
  // "launcher-injected … not an operator config surface of its own" while nine
  // operator-facing variables — every one of them settable, five of them mapped
  // from an `orca worker` flag — had nowhere to be documented. This is that
  // surface; `services/environment-worker/src/config.ts` is scanned against it.
  'services/environment-worker/AGENTS.md',
  // The CLI's own two client variables (`ORCA_BASE_URL`, `ORCA_API_KEY`) had no
  // doc surface at all — `src/client.ts` is excused from the scan, and its excuse
  // claimed they were "registered wired-elsewhere" when the register has never
  // held either name. This is where they are actually written down.
  'packages/oeadm/AGENTS.md',
];

/**
 * Trees searched for a real consumer of a documented variable.
 *
 * Which files inside them count is `isWiringEvidence` in the pure core, where it
 * is unit-tested. It used to be three separate patterns inline here, and that is
 * how one of them ended up unreachable without anyone noticing.
 */
const SOURCE_DIRS = ['services', 'packages', 'charts', 'scripts', '.github'];

/**
 * Top-level directories deliberately outside the scan, with the reason.
 *
 * `SOURCE_DIRS` is the fifth hand-maintained list in this file, and it was the
 * last one nothing checked. It drives BOTH the candidate walk (which demands a
 * name be documented) and `runtimeBlob` (which proves a name is live), so a
 * directory missing from it is invisible on both sides at once — a new
 * `tools/reader.ts` reading two undocumented variables left the success line
 * byte-identical.
 */
const EXCUSED_DIRS = new Map([
  ['docs', 'documentation; scanned by DOC_ROOTS instead, which walks it wholesale'],
  ['patches', 'third-party dependency patch diffs, not operator-facing environment configuration'],
  [
    'proposals',
    'design records (OIPs): they explain why variables exist and may name ones that were later renamed or removed, so they are not operator documentation',
  ],
]);

const walk = (dir) => repoFilesUnder(dir).filter((rel) => rel.endsWith('.md'));

/**
 * `null` means "not there" and nothing else.
 *
 * A bare catch made EACCES and EISDIR indistinguishable from ENOENT, so
 * `chmod 000 config.ts` printed "missing config source …/config.ts" for a file
 * that was sitting right there. Everything but a genuine absence is a bug or an
 * environment fault and should surface as itself.
 */
function readIfPresent(p) {
  try {
    return readFileSync(p, 'utf8');
  } catch (err) {
    if (err.code === 'ENOENT') return null;
    throw err;
  }
}

// ---- what the code reads -------------------------------------------------

const sources = {};
for (const rel of CONFIG_SOURCES) {
  const body = readIfPresent(join(ROOT, rel));
  if (body === null) {
    console.error(`check-env-docs: missing config source ${rel}`);
    process.exit(2);
  }
  // Comment-stripped, like every other collector. `collectDeclared` matched any
  // quoted SCREAMING_SNAKE token in raw source, so a name mentioned in a comment
  // counted as declared — and an over-collected name is excluded from the
  // phantom check, which is how a documented-but-dead knob could hide behind its
  // own obituary. No name in the tree relies on this today; the rule is applied
  // here because this was the one place it was missing.
  sources[rel] = stripComments(body, commentStyleFor(rel));
}
// ---- nothing may read the environment unscanned --------------------------

/**
 * Source files that read `process.env` and are deliberately not scanned, with
 * the reason. A considered "no" has to be written down rather than expressed by
 * leaving a line out.
 *
 * All three are under `scripts/`, which `SOURCE_DIRS` already trusts as wiring
 * evidence. That asymmetry — trusted to keep a documented name alive, ignored
 * when it would demand a name be documented — is the same one that let four
 * variables stay undocumented on a green run, so the candidate walk now covers
 * every directory `SOURCE_DIRS` trusts and these three are excused by name.
 */
const EXCUSED_SOURCES = new Map([
  [
    'packages/codex-harness/src/worker.ts',
    'forwards only PATH to the SDK subprocess; credentials are explicit worker input, not ambient environment settings',
  ],
  [
    'services/session-runner/src/harness/codex-sdk/index.ts',
    'forwards standard PATH and certificate trust variables to the private worker; credentials arrive through explicit worker input, not operator settings declared here',
  ],
  [
    'services/registry-service-ts/src/domain/runner-git-snapshot.ts',
    'forwards standard PATH and certificate trust variables to the trusted Git child; Git authorization is explicit scoped proxy input, not ambient service configuration',
  ],
  [
    'scripts/check-env-docs.mjs',
    'this gate; it carries env-var names as the subject of its own matching',
  ],
  [
    'scripts/env-docs-core.mjs',
    'the same, and an earlier version had to special-case this checker to avoid it',
  ],
  [
    'services/harness-server/sandbox-templates/orca-default/start.sh',
    'the orca-default image env contract, like its README in EXCUSED_DOCS — read inside the container',
  ],
  [
    'scripts/merge-coverage.mjs',
    "build tooling: COVERAGE_REPORT_URL is documented in AGENTS.md, GITHUB_STEP_SUMMARY is GitHub's own",
  ],
  [
    'packages/harness-tunnel/src/identity.ts',
    'reads ORCA_PEER_ID / ORCA_HOST_ID / ORCA_HOST_NAME — runner and host identity injected by the launcher into the runner env, not operator configuration',
  ],
  [
    'packages/sandbox-runtime/src/in-memory/runtime.ts',
    'forwards the ambient env into the in-memory spawn ({ ...process.env, ...opts.env }); declares no variable of its own',
  ],
  [
    'services/session-runner/src/main.ts',
    'selects the sandbox runtime via SANDBOX_RUNTIME (documented, scanned in harness-server) and forwards the ambient env; declares no operator var of its own',
  ],
  [
    'services/session-runner/src/sandbox/seam.ts',
    'reads SANDBOX_RUNTIME / AI_GATEWAY_URL / S3_ENDPOINT (documented, scanned in their owning services) to build the network allow-list; forwards the ambient env',
  ],
  [
    'services/session-runner/src/harness/claude-code/reattach-sandbox.ts',
    'forwards the ambient env into the reattached sandbox spawn ({ ...process.env }); declares nothing itself',
  ],
  [
    'services/session-runner/src/harness/pi/orca-extension.mjs',
    'forwards the ambient env into the Pi bridge subprocess; declares nothing itself',
  ],
  // The previous reason said ORCA_BASE_URL was "registered wired-elsewhere". It
  // is not, and never was: the name appears zero times in `env-exemptions.yaml`.
  // What actually holds it up is `isWired` — this file's own `env['ORCA_BASE_URL']`
  // is wiring evidence, because a file excused from being SCANNED is still read
  // into `runtimeText`. Both names are now documented in
  // `packages/oeadm/AGENTS.md`, so the phantom direction is covered by a doc
  // rather than by a register entry that does not exist.
  [
    'packages/oeadm/src/client.ts',
    "the oeadm client config resolver; ORCA_BASE_URL and ORCA_API_KEY are the CLI's own client credentials, documented in packages/oeadm/AGENTS.md — not managed-service operator configuration. Scanning it would also demand register entries for the HTTP verbs `collectDeclared` lifts out of its request options",
  ],
  [
    'packages/oeadm/src/colors.ts',
    'reads the standard terminal color vars (NO_COLOR / FORCE_COLOR) to decide ANSI styling; not managed-service configuration',
  ],
  // `services/environment-worker/src/config.ts` was excused here, on the grounds
  // that its contract is "launcher-injected … not an operator config surface of
  // its own". Both halves were false, and the file it excused says so: its own
  // header reads "a self-hosted operator runs it", and `loadConfig`'s docblock
  // describes "a minimal self-hosted deployment [that] sets only the five
  // required vars". `orca worker` maps five operator FLAGS onto five of those
  // names. Nine operator-facing variables were undocumented behind that
  // sentence, and this is the SAME excuse on the SAME false grounds that was
  // deleted for `session-runner/src/config.ts` — the second time, so it is worth
  // stating the rule: a config file is excused because nothing reads it as
  // configuration, never because someone else happens to set it. It is scanned in
  // `CONFIG_SOURCES` now, against `services/environment-worker/AGENTS.md`.
  [
    'services/environment-worker/src/worker.ts',
    'filters and forwards the ambient env into the runner it spawns (buildRunnerEnv); declares nothing itself',
  ],
  [
    'services/registry-service-ts/src/environment/launcher/launcher-factory.ts',
    'the registry sandbox-backend provisioning factory; reads the local/E2B/OpenSandbox operator config (API keys, domains, template ids, command templates, image refs). Documenting these backend vars is tracked as a follow-up; the credentials it shares with harness-server (E2B_API_KEY, OPEN_SANDBOX_DOMAIN) stay documented and scanned there',
  ],
  [
    'services/registry-service-ts/src/environment/launcher/local-environment-launcher.ts',
    'sets the launcher->worker contract (ENVIRONMENT_*, ORCA_ENVIRONMENT_TOKEN, REGISTRY_TUNNEL_BASE_URL, RUNNER_LAUNCH_COMMAND, WORKSPACE_DIR) on the spawned worker env; not an operator config surface of its own',
  ],
  [
    'services/registry-service-ts/src/server.ts',
    'the registry HTTP server, not a config surface (config.ts is); reads only the optional REGISTRY_LOG_LEVEL log-verbosity knob (documentation tracked with the environment-launcher follow-up), and is dense with uppercase string literals (HTTP methods, headers, error codes) the declared-var collector would misread as env reads',
  ],
]);

// Anything that can execute, not just TypeScript. `isWiringEvidence` already
// accepted `.sh`, so a shell file was trusted to keep a documented name alive
// while being invisible to the check that demands names be documented — the
// same asymmetry as the directory list, one filter over. Two entrypoints read
// the environment through an embedded `node - <<'NODE'` heredoc.
//
// Not simply `isWiringEvidence` itself: that also accepts `.yaml`, and a YAML
// file cannot read the environment. Naming a variable in a chart's `env:` block
// is wiring without being a read, so the two predicates answer different
// questions and only overlap on the executable extensions. Using it verbatim
// flagged this gate's own register, whose notes contain the words `process.env`.
/**
 * Does this file read the environment at all?
 *
 * `body.includes('process.env')` alone made the candidate test a *JavaScript*
 * test. A shell script reads the environment without ever writing that string,
 * so a pure-shell reader stayed invisible even once `.sh` became an accepted
 * extension — `.github/scripts/pnpm-install-with-retry.sh` reads two variables
 * and was not even a candidate.
 */
function readsEnvironment(body, filename) {
  if (body.includes('process.env')) return true;
  // `import { env } from 'node:process'` and `const { env } = process` read the
  // environment without ever writing that literal, and `isWired` matches a bare
  // `env.NAME` — so such a file could keep a documented name alive while never
  // being asked to justify one. A spelling test is not a semantic one.
  if (
    /from\s+['"]node:process['"]/.test(body) ||
    /\{\s*env\s*(?:,[^}]*)?\}\s*=\s*process\b/.test(body)
  ) {
    return true;
  }
  // Same terminator rule as SHELL_PATTERNS: anything that cannot continue a name.
  // `{1,}` and the prefix forms, matching SHELL_PATTERNS. This kept the
  // three-character minimum after the rest of the gate dropped it, so a shell
  // file whose only env reads are two-character names was not recognised as a
  // reader at all — the very boundary lowering the minimum was meant to remove.
  return /\.sh$/.test(filename) && /\$\{?[#!]?[A-Z][A-Z0-9_]{1,}(?![A-Za-z0-9_])/.test(body);
}

function envReaders(dir) {
  return repoFilesUnder(dir).filter((rel) => {
    const name = rel.slice(rel.lastIndexOf('/') + 1);
    if (!/\.(ts|mts|cts|mjs|cjs|js|sh)$/.test(name)) return false;
    // One predicate, imported from the core, so the shell and `isWiringEvidence`
    // cannot drift apart again — they already had.
    if (isTestFile(rel)) return false;
    if (/(^|\/)(test|tests|__tests__)\//.test(rel)) return false;
    return readsEnvironment(readFileSync(join(ROOT, rel), 'utf8'), name);
  });
}

// Driven by `SOURCE_DIRS`, not a separate list, so the set of files that may
// PROVE a documented name is live and the set that may DEMAND a name be
// documented are the same set by construction. They were not: `SOURCE_DIRS`
// trusted `charts/`, `scripts/` and `.github/`, while the candidate walk covered
// only `services/` and `packages/`, so a new reader under `scripts/` could go
// undocumented on a green run while still silencing a phantom.
//
// Files under a directory named `test` stay out. That is not a new call —
// `isWiringEvidence` already refuses test files as wiring evidence, on the
// grounds that a variable read only by a test is not wired to anything.
//
// `vitest.shared.mjs` sits at the repo root, which is on neither side: it is
// not walked here and not in `runtimeBlob`. The one variable it reads,
// `COVERAGE`, is documented in `AGENTS.md`.
// Which top-level directories exist, and is each one classified? Same shape as
// every other list here: an omission has to be argued rather than expressed by
// leaving a line out.
// The hidden-directory filter is orthogonal to gitignore, not redundant with
// it, and dropping it when the git list arrived was a regression of the fix one
// commit earlier. `.gitignore` covers `.claude/`, `.playwright-mcp/` and part of
// `.pi/` — it says nothing about `.vscode/`, `.zed/`, `.cursor/` or whatever the
// next tool creates, so an unignored dot-dir turned the gate red locally while
// CI stayed green. That is the failure this check exists to prevent, so the
// filter is applied to the git-derived list rather than instead of it.
const topLevel = [
  ...new Set(REPO_FILES.filter((rel) => rel.includes('/')).map((rel) => rel.split('/')[0])),
].filter((name) => !isHiddenPath(`${name}/`));

const dirCoverage = validateScanCoverage({
  candidates: topLevel,
  scanned: SOURCE_DIRS,
  excused: EXCUSED_DIRS,
  what: 'is a top-level directory',
});
if (dirCoverage.length) {
  console.error(`\n✗ top-level directories outside the scan (${dirCoverage.length}):\n`);
  for (const problem of dirCoverage) console.error(`    ${problem}`);
  console.error(
    '\n  Add it to SOURCE_DIRS, or to EXCUSED_DIRS with a reason. SOURCE_DIRS\n' +
      '  drives both the candidate walk and the wiring evidence, so a directory\n' +
      '  missing from it is invisible in both directions at once.',
  );
  process.exit(1);
}

const candidates = SOURCE_DIRS.flatMap((d) => envReaders(d));

const coverage = validateScanCoverage({
  candidates,
  scanned: CONFIG_SOURCES,
  excused: EXCUSED_SOURCES,
});
if (coverage.length) {
  console.error(`\n✗ env readers outside the scan set (${coverage.length}):\n`);
  for (const problem of coverage) console.error(`    ${problem}`);
  console.error(
    '\n  Add the file to CONFIG_SOURCES, or to EXCUSED_SOURCES with a reason.\n' +
      '  A file that reads the environment but is not scanned can drift its docs\n' +
      '  with the run still green — which is how KIND_HELM_NAMESPACE and three\n' +
      '  others stayed undocumented while this gate reported "env docs in sync".',
  );
  process.exit(1);
}

const declared = collectDeclared(sources);
/**
 * What each source is expected to keep declaring. A ratchet, like
 * `coverage-thresholds.json`: raise a number after adding reads, lower one only
 * deliberately. Pinned to the counts measured when per-source floors replaced a
 * single global one.
 */
const SOURCE_FLOORS = {
  'packages/sdk-harness/src/credentials.ts': 6,
  'packages/pi-harness/src/credentials.ts': 5,
  'services/harness-server/src/config.ts': 76,
  'services/registry-service-ts/src/config.ts': 76,
  'services/session-runner/src/config.ts': 6,
  'services/environment-worker/src/config.ts': 9,
  'services/observability-exporter/src/config.ts': 29,
  'packages/transcript-store/src/kafka/config.ts': 11,
  'packages/oeadm/src/commands/worker.ts': 7,
  'packages/e2e-tests/scripts/kind-helm-e2e.ts': 21,
  'packages/e2e-tests/scripts/run-agent-tests.sh': 2,
  'services/harness-server/src/sandbox/local/runtime.ts': 17,
  'packages/sandbox-runtime/src/local/runtime.ts': 3,
  'services/sandbox-harness/src/session-manager.ts': 10,
  'services/registry-service-ts/src/bootstrap-admin.ts': 9,
  'services/registry-service-ts/src/create-admin-key.ts': 6,
  'services/sandbox-harness/src/providers/claude.ts': 6,
  'services/harness-server/src/main.ts': 5,
  'packages/e2e-tests/src/client.ts': 5,
  'packages/e2e-tests/src/seed.ts': 5,
  'services/registry-service-ts/src/create-platform-key.ts': 4,
  'services/harness-server/src/harness/claude/index.ts': 4,
  'services/sandbox-harness/src/subprocess-entry.ts': 4,
  'services/harness-server/scripts/spike-orca-default.ts': 3,
  'services/registry-service-ts/src/main.ts': 2,
  'services/registry-service-ts/src/migrate.ts': 1,
  'services/sandbox-harness/src/index.ts': 1,
  'packages/file-store/drizzle.config.ts': 1,
  'packages/memory-store/drizzle.config.ts': 1,
  'services/registry-service-ts/drizzle.config.ts': 1,
  // Pass `process.env` wholesale to a callee; they name nothing themselves.
  'services/registry-service-ts/src/secrets/runtime-resolver.ts': 0,
  'services/sandbox-harness/src/providers/types.ts': 0,
  'services/sandbox-harness/src/session.ts': 0,
  'services/sandbox-harness/docker-entrypoint.sh': 1,
  'services/dev/scripts/start-services.sh': 3,
  'services/dev/scripts/test-transcript-avro.sh': 3,
  'services/dev/scripts/start-self-hosted.sh': 0,
  'services/dev/scripts/stop-self-hosted.sh': 0,
  'services/dev/scripts/dev-infra-services.sh': 0,
  'services/dev/scripts/init-secrets.sh': 0,
  'services/dev/scripts/prepare-ai-gateway-image.sh': 0,
  'services/dev/scripts/stop-services.sh': 0,
  'services/dev/scripts/with-dev-env.sh': 0,
  '.github/scripts/pnpm-install-with-retry.sh': 0,
  '.github/scripts/load-kind-image-with-retry.sh': 0,
  '.github/scripts/supervise-kubectl-port-forward.sh': 0,
  '.github/scripts/changed-areas.sh': 0,
};

const perSourceCounts = Object.fromEntries(
  Object.entries(sources).map(([rel, body]) => [rel, collectDeclared({ [rel]: body }).size]),
);
const floorProblems = validateSourceFloors(perSourceCounts, SOURCE_FLOORS);
if (floorProblems.length) {
  console.error(
    `\n✗ a config source stopped declaring what it used to (${floorProblems.length}):\n`,
  );
  for (const problem of floorProblems) console.error(`    ${problem}`);
  console.error('');
  process.exit(1);
}

// ---- what the docs claim -------------------------------------------------

const docs = {};

/**
 * `roadmap.md` is not documentation for this purpose.
 *
 * Hard rule 1 makes it the doc for what does NOT exist, so a variable whose only
 * backticked mention is a roadmap gap entry has no operator-facing documentation
 * — counting it would let the gaps file satisfy the very requirement it exists
 * to record the absence of. `ANTHROPIC_AUTH_TOKEN` is named only there today.
 */
const NOT_DOCUMENTATION = /(^|\/)roadmap\.md$/;

for (const rel of DOC_ROOTS.flatMap((d) => walk(d))) {
  if (NOT_DOCUMENTATION.test(rel)) continue;
  const body = readIfPresent(join(ROOT, rel));
  if (body !== null) docs[rel] = body;
}

// The hand-listed files are different from the walked tree: each is named on
// purpose, so one going missing is a bug in THIS file, not a condition to
// tolerate. Six of the twelve could vanish and leave a byte-identical success
// line, because the phantom check simply stopped covering whatever they held.
for (const f of DOC_FILES) {
  const body = readIfPresent(join(ROOT, f));
  if (body === null) {
    console.error(`check-env-docs: missing documentation source ${f}`);
    console.error('  It is listed in DOC_FILES; re-point that list or restore the file.');
    process.exit(2);
  }
  docs[f] = body;
}
const { documented, families } = collectDocumented(docs);

// The third hand-maintained inclusion list, checked the same way as the first
// two. Any markdown outside `docs/` that names an environment identifier must be
// scanned or excused: a doc nobody scans can go stale forever, and unlike a
// missing config source that failure is silent — the phantom check simply stops
// covering whatever the file held.
//
// Symlinks are skipped: each service's `CLAUDE.md` points at the `AGENTS.md`
// beside it, which is already listed, and following both would double-count.
function docsNamingIdentifiers() {
  return REPO_FILES.filter(
    (rel) =>
      rel.endsWith('.md') &&
      !rel.startsWith('docs/') &&
      // A top-level tree excused from both runtime wiring and current-behavior
      // documentation evidence records that boundary once in EXCUSED_DIRS.
      !EXCUSED_DIRS.has(rel.split('/')[0]) &&
      !isHiddenPath(rel) &&
      // Each service's `CLAUDE.md` is a symlink to the `AGENTS.md` beside it,
      // which is already listed; following both would double-count.
      !lstatSync(join(ROOT, rel)).isSymbolicLink() &&
      collectDocumented({ [rel]: readFileSync(join(ROOT, rel), 'utf8') }).documented.size > 0,
  );
}

const docCoverage = validateScanCoverage({
  candidates: docsNamingIdentifiers(),
  scanned: DOC_FILES,
  excused: EXCUSED_DOCS,
  what: 'names environment identifiers',
});
if (docCoverage.length) {
  console.error(
    `\n✗ documentation naming env identifiers, outside the scan (${docCoverage.length}):\n`,
  );
  for (const problem of docCoverage) console.error(`    ${problem}`);
  console.error(
    '\n  Add the file to DOC_FILES, or to EXCUSED_DOCS with a reason. A doc that\n' +
      '  nobody scans can describe a variable that no longer exists, forever.',
  );
  process.exit(1);
}

// ---- what any source file actually references ----------------------------

const runtimeBlob = [];
for (const rel of SOURCE_DIRS.flatMap((d) => repoFilesUnder(d))) {
  // Stripped per file, never per blob: a `/*` inside a string used to open a
  // block comment that ran past the end of its own file and ate the head of the
  // next one — 29% of the scanned surface, measured.
  if (isWiringEvidence(rel)) {
    runtimeBlob.push(stripComments(readFileSync(join(ROOT, rel), 'utf8'), commentStyleFor(rel)));
  }
}
runtimeBlob.push(stripComments(readIfPresent(join(ROOT, 'Makefile')) ?? '', 'hash'));
runtimeBlob.push(stripComments(readIfPresent(join(ROOT, 'package.json')) ?? '', 'hash'));

// Already stripped, per file, above.
const runtimeText = runtimeBlob.join('\n');

// ---- the exemption register ----------------------------------------------

const REGISTER = 'scripts/env-exemptions.yaml';
const registerBody = readIfPresent(join(ROOT, REGISTER));
if (registerBody === null) {
  console.error(`check-env-docs: missing ${REGISTER}`);
  process.exit(2);
}
let register;
try {
  register = load(registerBody);
} catch (err) {
  // Its two neighbouring failures (absent, malformed shape) report cleanly; a
  // syntax error used to escape as a raw stack trace with js-yaml's line/column
  // buried inside it.
  console.error(`\n✗ ${REGISTER} is not valid YAML:\n\n    ${err.message}\n`);
  process.exit(2);
}

const shapeProblems = validateExemptions(register);
if (shapeProblems.length) {
  console.error(`\n✗ ${REGISTER} is malformed:\n`);
  for (const p of shapeProblems) console.error(`    ${p}`);
  console.error('');
  process.exit(1);
}

const siteProblems = validateExemptionSites(register, {
  exists: (rel) => existsSync(join(ROOT, rel)),
  read: (rel) => readFileSync(join(ROOT, rel), 'utf8'),
});
if (siteProblems.length) {
  console.error(`\n✗ ${REGISTER} points at sites that no longer justify it:\n`);
  for (const p of siteProblems) console.error(`    ${p}`);
  console.error(
    '\n  An exemption whose justification cannot be reached is the same as no\n' +
      '  exemption. Re-point it at the real consumer, or delete the entry.',
  );
  console.error('');
  process.exit(1);
}

// ---- report --------------------------------------------------------------

const exemptions = indexExemptions(register);
const inventoryProblems = validateRegisterInventory(registerBody, exemptions);
if (inventoryProblems.length) {
  console.error(`\n✗ ${REGISTER}'s header no longer describes its entries:\n`);
  for (const problem of inventoryProblems) console.error(`    ${problem}`);
  console.error(
    '\n  The inventory block exists so the header is checkable rather than\n' +
      '  illustrative. Update it, or correct the entry it disagrees with.',
  );
  process.exit(1);
}
const envAccessed = collectEnvAccessed(sources);
const problems = compare({ declared, documented, families, runtimeText, exemptions, envAccessed });

if (problems.floor) {
  console.error(`\n✗ ${problems.floor}\n`);
}

if (problems.undocumented.length) {
  console.error(`\n✗ read by config.ts, documented nowhere (${problems.undocumented.length}):\n`);
  for (const { name, origin } of problems.undocumented) {
    console.error(`    ${name.padEnd(36)} ${origin}`);
  }
  console.error('\n  Add each to the env table in the owning service README.');
}

if (problems.phantom.length) {
  console.error(`\n✗ documented, but no source file reads it (${problems.phantom.length}):\n`);
  for (const { name, sites } of problems.phantom) {
    console.error(`    ${name}`);
    for (const site of sites.slice(0, 6)) console.error(`        ${site}`);
    const extra = sites.length - 6;
    if (extra > 0) console.error(`        … ${extra} more`);
  }
  console.error(
    '\n  Either wire the variable up, or correct the docs to describe what the\n' +
      '  code actually does. A documented knob that does nothing is worse than\n' +
      '  an undocumented one. If it IS consumed outside the scanned configs — a\n' +
      `  chart, a workflow, an e2e spec — declare it in ${REGISTER} with a reason\n` +
      '  and a site.',
  );
}

if (problems.resurrected.length) {
  console.error(
    `\n✗ documented as absent, but the config now reads it (${problems.resurrected.length}):\n`,
  );
  for (const { name, origin, sites } of problems.resurrected) {
    console.error(`    ${name}  now read by ${origin}`);
    for (const site of sites.slice(0, 4)) console.error(`        asserted absent at ${site}`);
  }
  console.error(
    '\n  A doc states this variable does not exist, as an invariant. Either the\n' +
      '  invariant was deliberately dropped — update the doc — or this read is a\n' +
      '  regression.',
  );
}

if (problems.dead.length) {
  console.error(`\n✗ ${REGISTER} has entries that no longer apply (${problems.dead.length}):\n`);
  for (const { name, why } of problems.dead) {
    console.error(`    ${name.padEnd(36)} ${why}`);
  }
  console.error(
    '\n  If the docs genuinely dropped the name, delete the entry — a register\n' +
      '  that only grows stops describing the exceptions and starts reading as\n' +
      '  coverage it does not provide. But check first, twice:\n' +
      '\n' +
      '    - a moved or renamed file produces this same report, and deleting the\n' +
      '      entry would throw away a recorded human judgement to silence a path\n' +
      '      problem;\n' +
      '    - "read from process.env by …" is inferred from an access SHAPE, not\n' +
      "      from execution. A call like `installShutdownHandlers(env, 'SIGINT')`\n" +
      '      matches the helper shape while passing a signal name, not a variable.\n' +
      '      If the entry is right and the call is a coincidence, leave it and say\n' +
      '      so in the note — do not document a POSIX signal as a knob.',
  );
}

if (!isClean(problems)) {
  console.error('');
  process.exit(1);
}

console.log(
  `✓ env docs in sync — ${declared.size} variables read by config.ts, ` +
    `${documented.size} documented identifiers checked, ` +
    `${exemptions.size} exemptions declared`,
);
