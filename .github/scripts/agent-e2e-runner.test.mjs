// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

test('agent E2E reports each scenario and preserves the first exhausted failure', (t) => {
  const dir = mkdtempSync(join(tmpdir(), 'agent-e2e-runner-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  writeFileSync(join(dir, 'vitest'), `#!/bin/sh\nprintf '%s\n' "$@"\nexit 17\n`, { mode: 0o755 });
  const result = spawnSync(
    'bash',
    [
      fileURLToPath(
        new URL('../../packages/e2e-tests/scripts/run-agent-tests.sh', import.meta.url),
      ),
    ],
    {
      env: {
        ...process.env,
        PATH: `${dir}:${process.env.PATH}`,
        ORCA_E2E_AGENT_HARNESS: 'codex_sdk',
      },
      encoding: 'utf8',
    },
  );
  assert.equal(result.status, 17, result.stderr);
  const args = result.stdout.trim().split('\n');
  assert.ok(args.includes('--reporter=verbose'));
  assert.ok(args.includes('--bail=1'));
  assert.ok(args.includes('test/real-agent-loop.spec.ts'));
  assert.ok(args.includes('test/trigger-agent-loop.spec.ts'));
  assert.ok(args.includes('test/guardrails-agent.spec.ts'));
  assert.ok(args.includes('test/guardrails-budget-agent.spec.ts'));
});
