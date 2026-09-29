// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { expect, it } from 'vitest';

const require = createRequire(import.meta.url);
const { load } = require('js-yaml') as { load: (source: string) => unknown };
type Env = Record<string, string>;

it('enables Layer B.1 sandbox tests without retaining resources in CI', () => {
  const workflow = load(
    readFileSync(new URL('../../../../.github/workflows/e2e-stack.yml', import.meta.url), 'utf8'),
  ) as {
    env?: Env;
    jobs: Record<string, { env?: Env; steps: Array<{ name: string; env?: Env }> }>;
  };
  const job = workflow.jobs['e2e-sandbox-harness'];
  if (!job) throw new Error('missing sandbox harness CI job');
  const step = job.steps.find((step) => step.name === 'Layer B.1 — sandbox harness mode');
  if (!step) throw new Error('missing Layer B.1 CI step');

  // Configuration contract only: this does not prove session/Pod deletion.
  // GitHub Actions resolves step env over job env over workflow env.
  const env = { ...workflow.env, ...job.env, ...step.env };
  expect(env['ORCA_E2E_SANDBOX_HARNESS']).toBe('1');
  expect(env['ORCA_E2E_SANDBOX_HARNESS_KEEP_RESOURCES']).toBe('0');
});
