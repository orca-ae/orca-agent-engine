// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

const script = fileURLToPath(new URL('./package-profile.mjs', import.meta.url));

test('packages only the four native texts, preserving bytes and reporting the approval digest', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-profile-package-'));
  try {
    const files = {
      'cordis.patch.yml': '\uFEFF# 参数\r\n[]\r\n',
      'package.json': '{ "private": true }\n',
      'pnpm-lock.yaml': 'lockfileVersion: "9.0"\n',
      'pnpm-workspace.yaml': 'autoInstallPeers: false\nnodeLinker: hoisted\n',
    };
    await Promise.all(
      Object.entries({ ...files, 'ignored-code.js': 'throw new Error("must not execute")' }).map(
        ([name, text]) => writeFile(join(directory, name), text),
      ),
    );
    const packed = spawnSync(process.execPath, [script, directory], { encoding: 'utf8' });
    assert.equal(packed.status, 0, packed.stderr);
    assert.deepEqual(JSON.parse(packed.stdout), { version: 1, files });
    const expected = createHash('sha256')
      .update(
        JSON.stringify([
          1,
          Object.keys(files)
            .sort()
            .map((name) => [name, files[name]]),
        ]),
      )
      .digest('hex');
    assert.equal(packed.stderr.trim(), 'config_sha256=' + expected);

    await writeFile(join(directory, 'cordis.patch.yml'), Buffer.from([0xff]));
    const invalid = spawnSync(process.execPath, [script, directory], { encoding: 'utf8' });
    assert.notEqual(invalid.status, 0);
    assert.equal(invalid.stdout, '');

    // JSON escaping can exceed the transport bound before the text-byte bound.
    await writeFile(join(directory, 'cordis.patch.yml'), '\n'.repeat(600_000));
    const oversized = spawnSync(process.execPath, [script, directory], { encoding: 'utf8' });
    assert.notEqual(oversized.status, 0);
    assert.equal(oversized.stdout, '');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
