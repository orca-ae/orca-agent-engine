// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { E2BSandboxRuntime } from '../../src/sandbox/e2b/runtime.js';

const apiKey = process.env['E2B_API_KEY'];

describe.skipIf(!apiKey)('E2BSandboxRuntime (live)', () => {
  it('write + read + bash round-trip in a real sandbox', async () => {
    const runtime = new E2BSandboxRuntime({ apiKey: apiKey! });
    const sb = await runtime.acquire({});
    try {
      await sb.files.write('/tmp/orca-e2b.txt', Buffer.from('hello e2b'));
      const back = await sb.files.read('/tmp/orca-e2b.txt');
      expect(back.toString('utf8')).toBe('hello e2b');
      const r = await sb.run({ tool: 'bash', args: { command: 'cat /tmp/orca-e2b.txt' } });
      expect(r.stdout?.trim()).toBe('hello e2b');
    } finally {
      await sb.destroy();
    }
  }, 120_000);
});
