// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { load } from 'js-yaml';

const workflow = load(
  readFileSync(new URL('../workflows/self-hosted-e2e.yml', import.meta.url), 'utf8'),
);
const summary = workflow.jobs['self-hosted-e2e-summary'].steps[0].run;

test('both runner suites execute tests and retain reports and logs before teardown', () => {
  assert.equal(workflow.on.pull_request.paths, undefined);
  for (const [jobId, command] of [
    ['self-hosted-e2e', 'pnpm e2e:self-hosted'],
    ['self-hosted-e2e-real', 'pnpm e2e:self-hosted:real'],
  ]) {
    const job = workflow.jobs[jobId];
    const run = job.steps.find((step) => step.run?.includes(command));
    assert.ok(run, jobId);
    assert.match(run.run, /--reporter=junit/);
    assert.ok(run.run.includes('--outputFile=../../.artifacts/self-hosted/junit.xml'));
    assert.equal(run['continue-on-error'], undefined);
    const collect = job.steps.findIndex((step) => step.name === 'Collect stack logs');
    const upload = job.steps.findIndex((step) => step.uses === 'actions/upload-artifact@v4');
    const teardown = job.steps.findIndex((step) => step.name === 'Tear down');
    assert.ok(collect >= 0 && collect < upload && upload < teardown, jobId);
    assert.equal(job.steps[collect].if, 'always()');
    assert.equal(job.steps[upload].if, 'always()');
    assert.equal(job.steps[upload].with.path, '.artifacts/self-hosted/');
    assert.equal(job.steps[upload].with['retention-days'], 14);
    assert.match(job.if, /needs.changes.outputs.stack != 'false'/);
  }
  assert.match(
    workflow.jobs['self-hosted-e2e-real'].if,
    /github.event.pull_request.head.repo.full_name == github.repository/,
  );
  assert.ok(
    workflow.jobs['self-hosted-e2e-real'].if.includes(
      "contains(github.event.pull_request.labels.*.name, 'run-e2e')",
    ),
  );
});

const defaults = {
  EVENT: 'push',
  LABELED: 'false',
  SAME_REPO: 'true',
  CHANGES: 'success',
  STACK: 'true',
  MOCK: 'success',
  REAL: 'success',
};

for (const [name, overrides, passes] of [
  ['main runs both suites', {}, true],
  ['unlabelled PR skips real only', { EVENT: 'pull_request', REAL: 'skipped' }, true],
  ['labelled PR runs both', { EVENT: 'pull_request', LABELED: 'true' }, true],
  [
    'fork cannot use the model secret',
    {
      EVENT: 'pull_request',
      LABELED: 'true',
      SAME_REPO: 'false',
      REAL: 'skipped',
    },
    true,
  ],
  ['unaffected stack skips both', { STACK: 'false', MOCK: 'skipped', REAL: 'skipped' }, true],
  ['failed filter requires both suites', { CHANGES: 'failure', STACK: '' }, true],
  ['cancelled plumbing fails', { MOCK: 'cancelled' }, false],
  ['unexpectedly skipped real fails', { REAL: 'skipped' }, false],
  [
    'failed filter cannot skip work',
    {
      CHANGES: 'failure',
      STACK: '',
      MOCK: 'skipped',
      REAL: 'skipped',
    },
    false,
  ],
]) {
  test(name, () => {
    const result = spawnSync('bash', ['-c', summary], {
      env: { ...process.env, ...defaults, ...overrides },
      encoding: 'utf8',
    });
    assert.equal(result.status, passes ? 0 : 1, result.stdout + result.stderr);
  });
}
