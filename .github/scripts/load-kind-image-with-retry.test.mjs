// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

const script = new URL('./load-kind-image-with-retry.sh', import.meta.url).pathname;
const bytes = Buffer.from([0, 255, 10, 13, 0, 128, 42]);
function fixture(t) {
  const dir = mkdtempSync(join(tmpdir(), 'kind-stream-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  writeFileSync(join(dir, 'archive'), bytes);
  function executable(name, source) {
    const path = join(dir, name);
    writeFileSync(path, '#!/usr/bin/env bash\nset -eu\n' + source, { mode: 0o755 });
    return path;
  }
  const kind = executable(
    'kind',
    `
    printf '%s\\n' "$*" >> "$TEST_DIR/kind.log"
    if [[ "$1" == get ]]; then
      [[ "\${FAIL_DISCOVERY:-0}" != 1 ]] || exit 17
      [[ "\${EMPTY_NODES:-0}" != 1 ]] || exit 0
      printf 'cluster-control-plane\\ncluster-worker\\n'
    fi
  `,
  );
  const docker = executable(
    'docker',
    `
    printf '%s\\n' "$*" >> "$TEST_DIR/docker.log"
    if [[ "$1 $2" == 'image save' ]]; then
      cat "$TEST_DIR/archive"
      [[ "\${FAIL_SAVE:-0}" != 1 ]] || exit 18
    elif [[ "$1 $2" == 'exec --privileged' ]]; then
      cat > "$TEST_DIR/$4.bytes"
      [[ "\${HANG_IMPORT:-0}" != 1 ]] || exec sleep 30
      [[ "\${FAIL_IMPORT:-0}" != 1 ]] || exit 19
    fi
  `,
  );
  const timeout = executable(
    'timeout',
    `
    printf '%s\\n' "$*" >> "$TEST_DIR/timeout.log"
    shift 3
    exec "$@"
  `,
  );
  return {
    dir,
    run(overrides = {}) {
      return spawnSync('bash', [script, 'image:test', 'cluster'], {
        env: {
          ...process.env,
          TEST_DIR: dir,
          KIND_BIN: kind,
          DOCKER_BIN: docker,
          TIMEOUT_BIN: timeout,
          KIND_IMAGE_LOAD_STREAM: '1',
          KIND_IMAGE_LOAD_MAX_ATTEMPTS: '1',
          KIND_IMAGE_LOAD_RETRY_DELAY_SECONDS: '0',
          ...overrides,
        },
        encoding: 'utf8',
        timeout: 10_000,
      });
    },
  };
}

test('streams unchanged binary bytes to every node with a bounded import and no host archive', (t) => {
  const { dir, run } = fixture(t);
  const result = run();
  assert.equal(result.status, 0, result.stderr);
  for (const node of ['cluster-control-plane', 'cluster-worker']) {
    assert.deepEqual(readFileSync(join(dir, `${node}.bytes`)), bytes);
  }
  const calls = readFileSync(join(dir, 'docker.log'), 'utf8');
  assert.match(calls, /images import --all-platforms --digests --snapshotter=overlayfs -/);
  assert.doesNotMatch(calls, /--output| -o | -t /);
  assert.match(readFileSync(join(dir, 'timeout.log'), 'utf8'), /--signal=TERM --kill-after=30s 8m/);
});

for (const [failure, code] of [
  ['FAIL_SAVE', 18],
  ['FAIL_IMPORT', 19],
  ['FAIL_DISCOVERY', 17],
  ['EMPTY_NODES', 1],
]) {
  test(`propagates ${failure}, retries and fails with diagnostics`, (t) => {
    const { dir, run } = fixture(t);
    const result = run({ [failure]: '1', KIND_IMAGE_LOAD_MAX_ATTEMPTS: '2' });
    assert.equal(result.status, code, result.stderr);
    assert.equal(readFileSync(join(dir, 'kind.log'), 'utf8').match(/get nodes/g).length, 2);
    assert.match(result.stdout, /attempt 2\/2/);
    assert.match(readFileSync(join(dir, 'docker.log'), 'utf8'), /system df/);
  });
}

test('retains the ordinary kind loader when streaming is disabled', (t) => {
  const { dir, run } = fixture(t);
  assert.equal(run({ KIND_IMAGE_LOAD_STREAM: '0' }).status, 0);
  assert.equal(
    readFileSync(join(dir, 'kind.log'), 'utf8').trim(),
    'load docker-image image:test --name cluster',
  );
});

test('terminates a stalled streaming pipeline under the real GNU timeout', (t) => {
  const timeout = ['timeout', 'gtimeout'].find((bin) => spawnSync(bin, ['--version']).status === 0);
  if (!timeout) return t.skip('GNU timeout is not installed on this host');
  const { run } = fixture(t);
  const started = Date.now();
  const result = run({ TIMEOUT_BIN: timeout, KIND_IMAGE_LOAD_TIMEOUT: '0.2s', HANG_IMPORT: '1' });
  assert.equal(result.status, 124, result.stderr);
  assert.ok(Date.now() - started < 5000, 'the import and its pipe must not outlive the deadline');
});
