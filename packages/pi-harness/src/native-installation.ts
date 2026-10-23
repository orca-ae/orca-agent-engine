// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync, realpathSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

/** Resolve the controlled dependency, not a PATH binary's self-reported version. */
export function nativePiInstallation(): { cli: string; readPaths: string[] } {
  const [major, minor] = process.versions.node.split('.').map(Number);
  if (major !== 22 || minor! < 21)
    throw new Error('Native completion requires Node 22.21 or newer Node 22');
  const main = fileURLToPath(import.meta.resolve('@earendil-works/pi-coding-agent'));
  const root = dirname(dirname(realpathSync(main)));
  if (JSON.parse(readFileSync(join(root, 'package.json'), 'utf8')).version !== '0.87.1')
    throw new Error('Native completion requires the controlled Pi 0.87.1 package');
  const cli = realpathSync(join(root, 'dist/cli.js'));
  const api = fileURLToPath(import.meta.resolve('@earendil-works/pi-ai/api/anthropic-messages'));
  let cliApi: string | undefined;
  for (const path of createRequire(cli).resolve.paths('@earendil-works/pi-ai') ?? []) {
    try {
      cliApi = realpathSync(join(path, '@earendil-works/pi-ai/dist/api/anthropic-messages.js'));
      break;
    } catch {
      /* Next Node lookup root. */
    }
  }
  if (cliApi !== realpathSync(api))
    throw new Error('Native CLI and report source use different Pi AI dependencies');
  const apiRoot = dirname(dirname(realpathSync(api)));
  if (JSON.parse(readFileSync(join(dirname(apiRoot), 'package.json'), 'utf8')).version !== '0.87.1')
    throw new Error('Native completion requires Pi AI 0.87.1');
  // pnpm's peers and all workspace library symlinks remain read-only in the runtime.
  const modules = realpathSync(join(root, '../../../..'));
  return {
    cli,
    readPaths: [
      root,
      modules,
      dirname(dirname(dirname(fileURLToPath(import.meta.url)))),
      realpathSync(process.execPath),
    ],
  };
}
