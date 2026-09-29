// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import yaml from 'js-yaml';

test('automated review pins the known working model instead of the rolling CLI default', () => {
  const workflow = yaml.load(
    readFileSync(new URL('../workflows/claude-code-review.yml', import.meta.url), 'utf8'),
  );
  const step = workflow.jobs['claude-review'].steps.find(
    (candidate) => candidate.uses === 'anthropics/claude-code-action@v1',
  );
  assert.match(step.with.claude_args, /^--model claude-sonnet-5$/m);
});
