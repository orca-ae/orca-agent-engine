// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { join } from 'node:path';
import { build } from 'tsup';
import { expect, it } from 'vitest';

it('loads both built runtime entrypoints with native Node ESM', async () => {
  const packageRoot = fileURLToPath(new URL('../../', import.meta.url));
  // Stay under the package so native Node resolves its real production dependencies.
  const outDir = await mkdtemp(join(packageRoot, '.runtime-import-test-'));
  try {
    // Match the Docker build's ESM entrypoints and target without Vitest's CJS interop.
    await build({
      entry: [join(packageRoot, 'src/index.ts'), join(packageRoot, 'src/main.ts')],
      format: ['esm'],
      target: 'node22',
      outDir,
      silent: true,
    });
    const files = await readdir(outDir);
    for (const [prefix, exportName] of [
      ['broker-main-', 'runBrokerExporter'],
      ['postgres-main-', 'runPostgresExporter'],
    ] as const) {
      const entrypoints = files.filter((file) => file.startsWith(prefix) && file.endsWith('.js'));
      expect(entrypoints).toHaveLength(1);
      const entrypoint = pathToFileURL(join(outDir, entrypoints[0]!)).href;
      // Import only: do not start a runtime or connect to Kafka, Postgres, or Registry.
      execFileSync(
        process.execPath,
        [
          '--input-type=module',
          '--eval',
          `const runtime = await import(${JSON.stringify(entrypoint)});
           if (typeof runtime[${JSON.stringify(exportName)}] !== 'function') process.exit(1);`,
        ],
        { cwd: packageRoot, timeout: 10_000, stdio: 'pipe' },
      );
    }
  } finally {
    await rm(outDir, { recursive: true, force: true });
  }
}, 30_000);
