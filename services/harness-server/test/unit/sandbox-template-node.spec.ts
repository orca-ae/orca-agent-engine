// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const E2B_DOCKERFILE = new URL(
  '../../sandbox-templates/orca-default/e2b.Dockerfile',
  import.meta.url,
);
const OPENSANDBOX_DOCKERFILE = new URL(
  '../../sandbox-templates/orca-opensandbox/Dockerfile',
  import.meta.url,
);
const AGENTENV_DOCKERFILE = new URL(
  '../../sandbox-templates/orca-agentenv/Dockerfile',
  import.meta.url,
);

describe('sandbox template Node contract', () => {
  it.each([
    ['E2B', E2B_DOCKERFILE],
    ['OpenSandbox', OPENSANDBOX_DOCKERFILE],
    ['AgentENV', AGENTENV_DOCKERFILE],
  ])('%s exposes Node on the bounded-read helper PATH', (_name, dockerfileUrl) => {
    const dockerfile = readFileSync(dockerfileUrl, 'utf8');
    expect(dockerfile).toContain('node_path="$(command -v node)"');
    expect(dockerfile).toContain('ln -s "${node_path}" /usr/local/bin/node');
    expect(dockerfile).toContain('/usr/bin/env -i PATH=/usr/local/bin:/usr/bin node --version');
  });
});
