// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

assert.equal(process.argv.length, 3, 'Usage: node package-profile.mjs <native-profile-directory>');
const names = ['cordis.patch.yml', 'package.json', 'pnpm-lock.yaml', 'pnpm-workspace.yaml'];
const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
const pairs = await Promise.all(
  names.map(async (name) => [name, decoder.decode(await readFile(join(process.argv[2], name)))]),
);
const envelope = JSON.stringify({ version: 1, files: Object.fromEntries(pairs) }) + '\n';
assert(Buffer.byteLength(envelope) <= 1024 * 1024, 'Profile envelope exceeds 1 MiB');
assert(
  pairs.reduce((size, [, text]) => size + Buffer.byteLength(text), 0) <= 1024 * 1024,
  'Profile texts exceed 1 MiB',
);
const digest = createHash('sha256')
  .update(JSON.stringify([1, pairs]))
  .digest('hex');
process.stderr.write('config_sha256=' + digest + '\n');
process.stdout.write(envelope);
