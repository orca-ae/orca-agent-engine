#!/usr/bin/env node
// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

//
// Merge per-package Vitest coverage into one workspace-wide report and enforce
// the regression ratchet.
//
// Why merge at all: unit and integration suites run in separate CI jobs and
// cover disjoint code. Reporting either alone is misleading — the integration
// suite is what exercises the S3/Kafka/Postgres glue, and the unit suite is
// what exercises the pure logic.
//
// Why merge workspace-wide rather than per-package: `harness-server`
// integration specs import `../../../registry-service-ts/src/server.ts`
// directly, so one package's run emits coverage for another package's source.
// Merging per-package would silently drop that. Attribution is therefore
// derived from each covered file's own path, not from which run produced it.
//
// Usage:
//   node scripts/merge-coverage.mjs                 merge, report, check thresholds
//   node scripts/merge-coverage.mjs --update        merge, report, re-pin thresholds
//   node scripts/merge-coverage.mjs --report-only   merge, report, skip the check
//   node scripts/merge-coverage.mjs --input-dir DIR read flat *.json instead of the
//                                                   in-repo coverage/ layout (CI artifacts)

import fs from 'node:fs';
import path from 'node:path';
import url from 'node:url';

import libCoverage from 'istanbul-lib-coverage';
import libReport from 'istanbul-lib-report';
import reports from 'istanbul-reports';

import { COVERED_PACKAGES } from '../vitest.shared.mjs';

const REPO_ROOT = path.resolve(path.dirname(url.fileURLToPath(import.meta.url)), '..');
const OUT_DIR = path.join(REPO_ROOT, 'coverage', 'merged');
const THRESHOLDS_FILE = path.join(REPO_ROOT, 'coverage-thresholds.json');
const METRICS = ['lines', 'statements', 'functions', 'branches'];

const args = process.argv.slice(2);
const UPDATE = args.includes('--update');
const REPORT_ONLY = args.includes('--report-only');
const inputDirFlag = args.indexOf('--input-dir');
const INPUT_DIR = inputDirFlag !== -1 ? path.resolve(args[inputDirFlag + 1]) : null;

/** Collect coverage-final.json inputs, either from CI artifacts or the repo layout. */
function findInputs() {
  if (INPUT_DIR) {
    if (!fs.existsSync(INPUT_DIR)) {
      fail(`--input-dir does not exist: ${INPUT_DIR}`);
    }
    return walk(INPUT_DIR).filter((f) => f.endsWith('.json'));
  }
  const found = [];
  for (const pkg of COVERED_PACKAGES) {
    for (const kind of ['unit', 'integration']) {
      const p = path.join(REPO_ROOT, pkg, 'coverage', kind, 'coverage-final.json');
      if (fs.existsSync(p)) found.push(p);
    }
  }
  return found;
}

function walk(dir) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...walk(p));
    else out.push(p);
  }
  return out;
}

/**
 * Map an absolute path from a coverage report to `<package-dir>/<rest>`.
 *
 * Absolute paths differ between a local checkout and a CI runner, so they can
 * never be compared directly. Attribution is done by locating a known package
 * directory inside the path; anything outside the covered packages (a
 * node_modules dependency, a stray temp file) is dropped.
 */
function toRepoRelative(absPath) {
  const norm = absPath.split(path.sep).join('/');
  for (const pkg of COVERED_PACKAGES) {
    const marker = `/${pkg}/`;
    const i = norm.lastIndexOf(marker);
    if (i !== -1) {
      return { pkg, rel: `${pkg}/${norm.slice(i + marker.length)}` };
    }
  }
  return null;
}

function fail(msg) {
  console.error(`\n  merge-coverage: ${msg}\n`);
  process.exit(1);
}

// --------------------------------------------------------------------- merge

const inputs = findInputs();
if (inputs.length === 0) {
  fail(
    'no coverage reports found.\n' +
      '  Run the suites with coverage first:\n' +
      '    COVERAGE=1 pnpm test\n' +
      '    make dev-up && COVERAGE=1 pnpm -r test:integration',
  );
}

console.log(`\nMerging ${inputs.length} coverage report(s):`);
for (const f of inputs) {
  console.log(`  - ${path.relative(REPO_ROOT, f)}`);
}

const merged = libCoverage.createCoverageMap({});
let droppedFiles = 0;

for (const file of inputs) {
  let raw;
  try {
    raw = JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (err) {
    fail(`could not parse ${file}: ${err.message}`);
  }

  // Rewrite every key to a repo-relative path *before* merging, so the same
  // source file produced by two different runs (or two different machines)
  // collapses into one entry instead of two.
  const rekeyed = {};
  for (const [absPath, fileCov] of Object.entries(raw)) {
    const hit = toRepoRelative(absPath);
    if (!hit) {
      droppedFiles += 1;
      continue;
    }
    rekeyed[hit.rel] = { ...fileCov, path: hit.rel };
  }
  merged.merge(libCoverage.createCoverageMap(rekeyed));
}

if (droppedFiles > 0) {
  console.log(`\n  (${droppedFiles} file entries outside the covered packages were dropped)`);
}

const coveredFiles = merged.files();
if (coveredFiles.length === 0) {
  fail('merged report contains no files from any covered package.');
}

// ----------------------------------------------------------------- summarize

function summarize(files) {
  const summary = libCoverage.createCoverageSummary();
  for (const f of files) summary.merge(merged.fileCoverageFor(f).toSummary());
  return summary;
}

function pcts(summary) {
  const out = {};
  for (const m of METRICS) {
    // istanbul reports 100 for a metric with a zero denominator ("nothing to
    // cover"). Keep that — it is the correct neutral value for a threshold.
    out[m] = Number(summary[m].pct.toFixed(2));
  }
  return out;
}

const perPackage = {};
for (const pkg of COVERED_PACKAGES) {
  const files = coveredFiles.filter((f) => f.startsWith(`${pkg}/`));
  if (files.length === 0) continue;
  perPackage[pkg] = { ...pcts(summarize(files)), files: files.length };
}
const totals = pcts(summarize(coveredFiles));

// -------------------------------------------------------------------- report

fs.mkdirSync(OUT_DIR, { recursive: true });
const context = libReport.createContext({
  dir: OUT_DIR,
  coverageMap: merged,
  // Report paths are repo-relative, so the html/lcov reporters resolve source
  // files correctly only when run from the repo root.
  sourceFinder: (f) => fs.readFileSync(path.join(REPO_ROOT, f), 'utf8'),
});
for (const name of ['text-summary', 'lcov', 'html']) {
  reports.create(name, { skipEmpty: false }).execute(context);
}

const rows = [...Object.entries(perPackage)].sort(([a], [b]) => a.localeCompare(b));
const pad = Math.max(...rows.map(([p]) => p.length), 'TOTAL'.length);

console.log('\nMerged coverage by package:\n');
console.log(
  `  ${'package'.padEnd(pad)}  ${'lines'.padStart(7)} ${'stmts'.padStart(7)} ${'funcs'.padStart(7)} ${'branch'.padStart(7)}  files`,
);
for (const [pkg, v] of rows) {
  console.log(
    `  ${pkg.padEnd(pad)}  ${`${v.lines}%`.padStart(7)} ${`${v.statements}%`.padStart(7)} ${`${v.functions}%`.padStart(7)} ${`${v.branches}%`.padStart(7)}  ${v.files}`,
  );
}
console.log(
  `  ${'TOTAL'.padEnd(pad)}  ${`${totals.lines}%`.padStart(7)} ${`${totals.statements}%`.padStart(7)} ${`${totals.functions}%`.padStart(7)} ${`${totals.branches}%`.padStart(7)}  ${coveredFiles.length}`,
);
console.log(`\n  HTML report: ${path.relative(REPO_ROOT, OUT_DIR)}/index.html`);

// ----------------------------------------------------------------- thresholds

// Loaded up front rather than inside the check: the summary renders a delta
// against the baseline in every mode, including `--report-only`.
//
// `let`, not `const`: `--update` rewrites the file and must reassign this
// before building the summary, or the deltas would be measured against the
// baseline that was just replaced.
let thresholds = fs.existsSync(THRESHOLDS_FILE)
  ? JSON.parse(fs.readFileSync(THRESHOLDS_FILE, 'utf8'))
  : null;
// v8 coverage is not perfectly deterministic across runs; without slack a gate
// this tight flakes on unrelated changes.
const tolerance = thresholds?.tolerance ?? 0.5;

/** Compare against the pinned baseline. Returns [] when there is nothing to check. */
function findFailures() {
  if (!thresholds) return [];
  const out = [];
  for (const [pkg, expected] of Object.entries(thresholds.packages ?? {})) {
    const actual = perPackage[pkg];
    if (!actual) {
      out.push(`${pkg}: expected coverage but no report was produced`);
      continue;
    }
    for (const m of METRICS) {
      if (expected[m] === undefined) continue;
      if (actual[m] < expected[m] - tolerance) {
        out.push(
          `${pkg} ${m}: ${actual[m]}% is below the ${expected[m]}% baseline (tolerance ${tolerance}pp)`,
        );
      }
    }
  }
  for (const m of METRICS) {
    const expected = thresholds.total?.[m];
    if (expected === undefined) continue;
    if (totals[m] < expected - tolerance) {
      out.push(`TOTAL ${m}: ${totals[m]}% is below the ${expected}% baseline`);
    }
  }
  return out;
}

/** `+1.2` / `−0.4` / `—`, against the pinned baseline for that package+metric. */
function delta(pkg, metric, actual) {
  const expected =
    pkg === null ? thresholds?.total?.[metric] : thresholds?.packages?.[pkg]?.[metric];
  if (expected === undefined) return '—';
  const d = Number((actual - expected).toFixed(2));
  if (d === 0) return '—';
  // U+2212 minus, so negative deltas line up with the positives in the column.
  return d > 0 ? `+${d}` : `−${Math.abs(d)}`;
}

/**
 * Build the markdown once and write it to `coverage/merged/summary.md` (always,
 * so it is inspectable locally) and to the Actions job summary when present.
 *
 * Called on every exit path *before* `process.exit`, so a ratchet regression
 * still produces a report to post — that is when it matters most.
 */
function writeSummary(failures, { mode }) {
  let verdict;
  if (mode === 'updated') {
    verdict = '📌 **Baseline re-pinned to this run.** Deltas are all `—` by definition.';
  } else if (mode === 'reporting') {
    verdict = [
      '> **Reporting only** — coverage was not enforced for this run.',
      '> Either a test job did not succeed (partial reports cannot be fairly',
      '> compared to the baseline) or no baseline is pinned yet.',
    ].join('\n');
  } else if (failures.length > 0) {
    verdict = [
      '❌ **Coverage regression.**',
      '',
      ...failures.map((f) => `- ${f}`),
      '',
      'Add tests for the code you changed, or run `pnpm coverage:update` to',
      'deliberately re-pin the baseline.',
    ].join('\n');
  } else {
    verdict = '✅ Meets the pinned baseline.';
  }

  const link = process.env.COVERAGE_REPORT_URL
    ? `\n[Full HTML report](${process.env.COVERAGE_REPORT_URL}) (\`coverage-html\` artifact) · baseline in \`coverage-thresholds.json\`\n`
    : '';

  const md = [
    '## Test coverage',
    '',
    `**${totals.lines}% lines** · **${totals.branches}% branches** — merged from ` +
      `${inputs.length} unit + integration report(s) over ${coveredFiles.length} files.`,
    '',
    '| Package | Lines | Δ | Branches | Δ | Funcs | Files |',
    '| --- | ---: | ---: | ---: | ---: | ---: | ---: |',
    ...rows.map(
      ([pkg, v]) =>
        `| \`${pkg}\` | ${v.lines}% | ${delta(pkg, 'lines', v.lines)} | ` +
        `${v.branches}% | ${delta(pkg, 'branches', v.branches)} | ${v.functions}% | ${v.files} |`,
    ),
    `| **TOTAL** | **${totals.lines}%** | ${delta(null, 'lines', totals.lines)} | ` +
      `**${totals.branches}%** | ${delta(null, 'branches', totals.branches)} | ` +
      `**${totals.functions}%** | **${coveredFiles.length}** |`,
    '',
    verdict,
    link,
  ].join('\n');

  fs.writeFileSync(path.join(OUT_DIR, 'summary.md'), `${md}\n`);
  if (process.env.GITHUB_STEP_SUMMARY) {
    fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${md}\n`);
  }
}

const current = { tolerance: 0.5, packages: perPackage, total: totals };

if (UPDATE) {
  const previous = thresholds;

  if (previous) {
    for (const [pkg, v] of Object.entries(perPackage)) {
      const before = previous.packages?.[pkg];
      if (!before) continue;
      for (const m of METRICS) {
        if (v[m] < before[m]) {
          console.log(
            `  ! lowering ${pkg} ${m}: ${before[m]}% -> ${v[m]}%  (accepting a coverage drop)`,
          );
        }
      }
    }
  }

  const out = {
    _comment:
      'Coverage ratchet baseline. Regenerate with `pnpm coverage:update` after ' +
      'adding tests. Values are merged unit+integration percentages; a run is ' +
      'failed when it falls more than `tolerance` points below any of these.',
    tolerance: current.tolerance,
    packages: Object.fromEntries(
      rows.map(([pkg, v]) => [pkg, Object.fromEntries(METRICS.map((m) => [m, v[m]]))]),
    ),
    total: Object.fromEntries(METRICS.map((m) => [m, totals[m]])),
  };
  fs.writeFileSync(THRESHOLDS_FILE, `${JSON.stringify(out, null, 2)}\n`);
  console.log(`\n  Wrote ${path.relative(REPO_ROOT, THRESHOLDS_FILE)}`);
  // Adopt the baseline just written before summarising. Without this the
  // summary would compare this run against the *previous* baseline and report
  // non-zero deltas for a file it had already overwritten.
  thresholds = out;
  writeSummary([], { mode: 'updated' });
  process.exit(0);
}

if (REPORT_ONLY) {
  console.log('\n  --report-only: skipping threshold check.');
  writeSummary([], { mode: 'reporting' });
  process.exit(0);
}

if (!thresholds) {
  console.log(
    `\n  No ${path.basename(THRESHOLDS_FILE)} yet — run \`pnpm coverage:update\` to pin this run as the baseline.`,
  );
  writeSummary([], { mode: 'reporting' });
  process.exit(0);
}

const failures = findFailures();
writeSummary(failures, { mode: 'enforced' });

if (failures.length > 0) {
  console.error('\n  Coverage regression:\n');
  for (const f of failures) console.error(`    - ${f}`);
  console.error(
    '\n  Add tests for the code you changed, or run `pnpm coverage:update`\n' +
      '  to deliberately re-pin the baseline.\n',
  );
  process.exit(1);
}

console.log('\n  Coverage meets the pinned baseline.\n');
