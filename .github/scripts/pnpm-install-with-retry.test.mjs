// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

const script = new URL('./pnpm-install-with-retry.sh', import.meta.url).pathname;
function fixture(t) {
  const dir = mkdtempSync(join(tmpdir(), 'pnpm-install-retry-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const bin = join(dir, 'bin');
  mkdirSync(bin);
  for (const path of [
    'node_modules',
    'services/fixture/node_modules',
    'packages/fixture/node_modules',
  ]) {
    mkdirSync(join(dir, path), { recursive: true });
  }
  writeFileSync(
    join(bin, 'pnpm'),
    `#!/usr/bin/env bash
set -eu
attempt=0
[[ ! -f "$INSTALL_TEST_DIR/count" ]] || read -r attempt < "$INSTALL_TEST_DIR/count"
attempt=$((attempt + 1))
printf '%s\\n' "$attempt" > "$INSTALL_TEST_DIR/count"
[[ "$*" == 'install --frozen-lockfile' ]] || exit 99
[[ "$attempt" -le "$INSTALL_TEST_FAILURES" ]] || exit 0
exit "$INSTALL_TEST_EXIT"
`,
    { mode: 0o755 },
  );
  writeFileSync(
    join(bin, 'sleep'),
    `#!/usr/bin/env bash
printf '%s\\n' "$*" >> "$INSTALL_TEST_DIR/sleeps"
`,
    { mode: 0o755 },
  );
  return {
    dir,
    run({ failures = 0, attempts = 3, code = 23 } = {}) {
      return spawnSync('bash', [script], {
        cwd: dir,
        env: {
          ...process.env,
          PATH: `${bin}:${process.env.PATH}`,
          INSTALL_TEST_DIR: dir,
          INSTALL_TEST_FAILURES: String(failures),
          INSTALL_TEST_EXIT: String(code),
          PNPM_INSTALL_ATTEMPTS: String(attempts),
          PNPM_INSTALL_RETRY_DELAY_SECONDS: '7',
        },
        encoding: 'utf8',
        timeout: 5_000,
      });
    },
  };
}

test('stops after the first successful install without removing dependencies', (t) => {
  const { dir, run } = fixture(t);
  const result = run();
  assert.equal(result.status, 0, result.stderr);
  assert.equal(readFileSync(join(dir, 'count'), 'utf8').trim(), '1');
  assert.ok(existsSync(join(dir, 'node_modules')));
  assert.ok(!existsSync(join(dir, 'sleeps')));
});

test('cleans partial dependencies between retries and accepts a later success', (t) => {
  const { dir, run } = fixture(t);
  const result = run({ failures: 2 });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(readFileSync(join(dir, 'count'), 'utf8').trim(), '3');
  assert.equal(readFileSync(join(dir, 'sleeps'), 'utf8'), '7\n7\n');
  for (const path of [
    'node_modules',
    'services/fixture/node_modules',
    'packages/fixture/node_modules',
  ]) {
    assert.ok(!existsSync(join(dir, path)));
  }
});

for (const attempts of [1, 3]) {
  test(`returns the installer failure after exhausting ${attempts} attempt(s)`, (t) => {
    const { dir, run } = fixture(t);
    const result = run({ failures: attempts, attempts, code: 71 });
    assert.equal(result.status, 71, result.stderr);
    assert.equal(readFileSync(join(dir, 'count'), 'utf8').trim(), String(attempts));
    assert.equal(
      existsSync(join(dir, 'sleeps')) ? readFileSync(join(dir, 'sleeps'), 'utf8') : '',
      '7\n'.repeat(attempts - 1),
    );
  });
}
