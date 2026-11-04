// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createRequire } from 'node:module';
import { readFileSync, writeFileSync, mkdirSync, copyFileSync, chmodSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { verifyDogwoodClockBuild } from './verify-dogwood-artifacts.mjs';

const [deploy, source, checksum, provenancePath] = process.argv.slice(2);
if (!deploy || !source || !checksum || !provenancePath) {
  throw new Error(
    'usage: install-dogwood-clock.mjs DEPLOY HELPER BUILDER_SHA256 VERIFIED_PROVENANCE',
  );
}
const require = createRequire(join(deploy, 'package.json'));
const root = dirname(require.resolve('@runorca/dogwood'));
const platform = `${process.platform}-${process.arch}`;
if (process.platform !== 'linux') throw new Error('Container helper installation requires Linux');
const provenance = JSON.parse(readFileSync(provenancePath, 'utf8'));
const build = verifyDogwoodClockBuild(root, source, readFileSync(checksum, 'utf8'), provenance);
const path = `bin/${platform}/orca-dogwood-clock`;
mkdirSync(dirname(join(root, path)), { recursive: true });
copyFileSync(source, join(root, path));
chmodSync(join(root, path), 0o755);
provenance.clock_helpers[platform] = { path, sha256: build.helper_sha256 };
provenance.consumer_clock_build = build;
writeFileSync(join(root, 'provenance.json'), JSON.stringify(provenance, null, 2) + '\n');
require('@runorca/dogwood').readClock();
