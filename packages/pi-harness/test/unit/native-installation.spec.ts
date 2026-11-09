// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';

it('keeps the controlled native CLI on 0.87.1 while the managed SDK uses 1.5.0', () => {
  // Run the built entry with Node's real import resolution, as the runner does.
  const output = execFileSync(
    process.execPath,
    [
      '--input-type=module',
      '-e',
      `
    import { readFileSync } from 'node:fs';
    import { dirname, join } from 'node:path';
    import { fileURLToPath } from 'node:url';
    import { nativePiInstallation } from './dist/index.js';
    const managed = fileURLToPath(import.meta.resolve('@earendil-works/pi-coding-agent'));
    const version = (entry) => JSON.parse(readFileSync(join(dirname(dirname(entry)), 'package.json'), 'utf8')).version;
    const native = process.versions.node.split('.')[0] === '22' ? nativePiInstallation() : undefined;
    console.log(JSON.stringify({ managed: version(managed), ...(native ? { cli: version(native.cli) } : {}) }));
  `,
    ],
    { encoding: 'utf8' },
  );
  expect(JSON.parse(output)).toEqual({
    managed: '1.5.0',
    ...(process.versions.node.split('.')[0] === '22' ? { cli: '0.87.1' } : {}),
  });
});
