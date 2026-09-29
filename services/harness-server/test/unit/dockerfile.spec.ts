// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

describe('harness image workspace contract', () => {
  it('stages the exporter devDependency manifest before install and production deploy', () => {
    const dockerfile = readFileSync(new URL('../../Dockerfile', import.meta.url), 'utf8');
    const manifestCopy =
      'COPY services/observability-exporter/package.json ./services/observability-exporter/';
    const install = 'RUN pnpm install --frozen-lockfile --filter @orca/harness-server...';
    const deploy = 'RUN pnpm deploy --filter @orca/harness-server --prod /deploy';

    // pnpm 9 deploy resolves workspace devDependencies even with --prod.
    expect(dockerfile).toContain(manifestCopy);
    expect(dockerfile).toContain(install);
    expect(dockerfile).toContain(deploy);
    expect(dockerfile.indexOf(manifestCopy)).toBeLessThan(dockerfile.indexOf(install));
    expect(dockerfile.indexOf(install)).toBeLessThan(dockerfile.indexOf(deploy));
  });
});
