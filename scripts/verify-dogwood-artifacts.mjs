// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';

export const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');
const readJson = (path) => JSON.parse(readFileSync(path, 'utf8'));
const sourceName = 'orca-dogwood-source-0.1.0.tgz';
const npmName = 'runorca-dogwood-0.1.0-df6624b.tgz';
function check(condition, message) {
  if (!condition) throw new Error(message);
}
function checkFile(path, expected, label) {
  check(/^[a-f0-9]{64}$/.test(expected ?? ''), `Invalid SHA-256 for ${label}`);
  check(sha256(readFileSync(path)) === expected, `SHA-256 mismatch: ${label}`);
}
export function verifyDogwoodPackage(root, expected) {
  const actual = readJson(join(root, 'provenance.json'));
  for (const key of ['core', 'upstream', 'vendor_tree_sha256', 'wasm_sha256', 'clock_helpers']) {
    check(isDeepStrictEqual(actual[key], expected[key]), `Dogwood provenance mismatch: ${key}`);
  }
  const pkg = readJson(join(root, 'package.json'));
  check(
    pkg.name === '@runorca/dogwood' && pkg.version === expected.core.version,
    'Dogwood package identity mismatch',
  );
  checkFile(join(root, 'wasm/orca_dogwood_bg.wasm'), expected.wasm_sha256, 'WASM');
  for (const [platform, helper] of Object.entries(expected.clock_helpers)) {
    check(helper.path === `bin/${platform}/orca-dogwood-clock`, 'Unexpected clock helper path');
    checkFile(join(root, helper.path), helper.sha256, `clock helper ${platform}`);
  }
  for (const entry of expected.core.sources.filter(({ path }) => path.startsWith('package/'))) {
    checkFile(join(root, entry.path.slice('package/'.length)), entry.sha256, entry.path);
  }
  return actual;
}
/** Verify immutable inputs; this does not claim a reproducible Linux binary build. */
export function verifyDogwoodArtifacts(directory) {
  const provenance = readJson(join(directory, 'provenance.json'));
  for (const [name, recorded] of [
    [sourceName, sourceName],
    [npmName, 'runorca-dogwood-0.1.0.tgz'],
  ]) {
    const artifact = provenance.artifacts.find(({ path }) => path === `dist/${recorded}`);
    checkFile(join(directory, name), artifact?.sha256, name);
  }
  const temporary = mkdtempSync(join(tmpdir(), 'dogwood-verify-'));
  try {
    for (const [name, target] of [
      [sourceName, 'source'],
      [npmName, 'node'],
    ]) {
      mkdirSync(join(temporary, target));
      execFileSync('tar', ['-xzf', join(directory, name), '-C', join(temporary, target)]);
    }
    const sourceRoot = join(temporary, 'source');
    check(
      sha256(JSON.stringify(provenance.core.sources)) === provenance.core.source_tree_sha256,
      'Source manifest SHA-256 mismatch',
    );
    for (const entry of provenance.core.sources) {
      check(
        !entry.path.startsWith('/') && !entry.path.split('/').includes('..'),
        'Unexpected source path',
      );
      checkFile(join(sourceRoot, entry.path), entry.sha256, entry.path);
    }
    check(
      isDeepStrictEqual(readJson(join(sourceRoot, 'UPSTREAM.json')), provenance.upstream),
      'Upstream pin mismatch',
    );
    verifyDogwoodPackage(join(temporary, 'node/package'), provenance);
    return provenance;
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}
/** Bind the builder's checksum to the verified package and source archive. */
export function verifyDogwoodClockBuild(packageRoot, source, builderChecksum, provenance) {
  verifyDogwoodPackage(packageRoot, provenance);
  const expected = builderChecksum.trim().split(/\s+/)[0];
  checkFile(source, expected, 'Linux clock build');
  const artifact = provenance.artifacts.find(({ path }) => path === `dist/${sourceName}`);
  check(/^[a-f0-9]{64}$/.test(artifact?.sha256 ?? ''), 'Missing source archive checksum');
  return {
    rust: provenance.core.rust,
    source_archive: sourceName,
    source_archive_sha256: artifact.sha256,
    source_commit: provenance.core.source_commit,
    source_tree_sha256: provenance.core.source_tree_sha256,
    helper_sha256: expected,
  };
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const directory = resolve(
    process.argv[2] ?? join(dirname(fileURLToPath(import.meta.url)), '../vendor/dogwood'),
  );
  const provenance = verifyDogwoodArtifacts(directory);
  console.log(
    `Verified Dogwood ${provenance.core.source_commit}: archives, source manifest, WASM, package, and packaged helpers`,
  );
}
