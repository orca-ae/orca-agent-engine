// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import {
  sha256,
  verifyDogwoodArtifacts,
  verifyDogwoodClockBuild,
} from './verify-dogwood-artifacts.mjs';

const vendor = resolve(dirname(fileURLToPath(import.meta.url)), '../vendor/dogwood');
test('verifies the actual pinned archives, source files, WASM and packaged helper', () => {
  const provenance = verifyDogwoodArtifacts(vendor);
  assert.equal(provenance.core.source_commit, 'df6624b5226fde0fd2f0e49af01f5513be1bd067');
});
test('rejects altered archives before extracting them', () => {
  const temporary = mkdtempSync(join(tmpdir(), 'dogwood-tamper-'));
  try {
    cpSync(vendor, temporary, { recursive: true });
    writeFileSync(join(temporary, 'orca-dogwood-source-0.1.0.tgz'), 'changed');
    assert.throws(() => verifyDogwoodArtifacts(temporary), /SHA-256 mismatch: orca-dogwood-source/);
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
});
test('binds the consumer helper to builder checksum and the selected source pin', () => {
  const temporary = mkdtempSync(join(tmpdir(), 'dogwood-build-'));
  try {
    execFileSync('tar', [
      '-xzf',
      join(vendor, 'runorca-dogwood-0.1.0-df6624b.tgz'),
      '-C',
      temporary,
    ]);
    const packageRoot = join(temporary, 'package');
    const source = join(temporary, 'test-helper');
    // Integrity checks only: this fixture does not execute or qualify a Linux clock.
    writeFileSync(source, 'fixture build');
    const checksum = sha256(readFileSync(source));
    const provenance = JSON.parse(readFileSync(join(vendor, 'provenance.json'), 'utf8'));
    const build = verifyDogwoodClockBuild(
      packageRoot,
      source,
      `${checksum}  target/release/orca-dogwood-clock`,
      provenance,
    );
    assert.equal(
      build.source_archive_sha256,
      '855e047801b3be3e812b6bd7a691217f40ab6f3b118e0952992ee6cd09fc65d3',
    );
    assert.equal(build.helper_sha256, checksum);
    assert.throws(
      () => verifyDogwoodClockBuild(packageRoot, source, '0'.repeat(64), provenance),
      /Linux clock build/,
    );
    const changed = structuredClone(provenance);
    changed.core.source_commit = 'other-pin';
    assert.throws(
      () => verifyDogwoodClockBuild(packageRoot, source, checksum, changed),
      /provenance mismatch: core/,
    );
    writeFileSync(join(packageRoot, 'wasm/orca_dogwood_bg.wasm'), 'changed');
    assert.throws(
      () => verifyDogwoodClockBuild(packageRoot, source, checksum, provenance),
      /SHA-256 mismatch: WASM/,
    );
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
});
