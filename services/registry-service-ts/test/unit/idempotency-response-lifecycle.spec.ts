// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFile } from 'node:child_process';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { promisify } from 'node:util';
import { describe, expect, it } from 'vitest';

const execFileAsync = promisify(execFile);
const tsxLoader = pathToFileURL(createRequire(import.meta.url).resolve('tsx')).href;

describe('idempotency response lifecycle over real HTTP', () => {
  it.each([
    { operation: 'create', mode: 'immediate' },
    { operation: 'create', mode: 'delayed' },
    { operation: 'create', mode: 'failure' },
    { operation: 'create', mode: 'no-key' },
    { operation: 'delete', mode: 'immediate' },
    { operation: 'delete', mode: 'delayed' },
  ])(
    'sends one complete vault $operation response and stays alive with $mode cache behavior',
    async ({ operation, mode }) => {
      const { stdout, stderr } = await execFileAsync(
        process.execPath,
        [
          '--import',
          tsxLoader,
          fileURLToPath(new URL('../helpers/idempotency-response-process.ts', import.meta.url)),
          mode,
          operation,
        ],
        { timeout: 15_000 },
      );
      expect(stderr).not.toContain('ERR_HTTP_HEADERS_SENT');
      expect(stdout).toContain('response lifecycle ok');
    },
    20_000,
  );
});
