// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { describe, expect, it } from 'vitest';

const execFileAsync = promisify(execFile);
const probe = fileURLToPath(
  new URL('../fixtures/sdk-tool-result-provenance-probe.mjs', import.meta.url),
);

describe('pinned real SDK tool-result provenance', () => {
  it('accepts same-tool legacy replay of an observed spill notice in fresh and resumed queries', async () => {
    // The subprocess owns all behavioral/schema assertions and local endpoints.
    // Successful exit plus this summary proves both phases completed their oracle.
    const { stdout } = await execFileAsync(process.execPath, [probe], {
      timeout: 55_000,
      killSignal: 'SIGKILL',
      maxBuffer: 128 * 1024,
      env: { PATH: process.env.PATH },
    });
    expect(JSON.parse(stdout)).toEqual({
      sdkVersion: '0.3.283',
      phases: ['fresh', 'resume'],
      payloadBytes: 156186,
    });
  }, 60_000);
});
