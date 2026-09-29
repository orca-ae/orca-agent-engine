// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { Readable } from 'node:stream';
import { statSync } from 'node:fs';
import { join } from 'node:path';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';

describe('InMemorySandboxRuntime', () => {
  it('write + read round-trips bytes', async () => {
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    try {
      await sb.files.write('/data/hello.txt', Buffer.from('hi'));
      const back = await sb.files.read('/data/hello.txt');
      expect(back.toString('utf8')).toBe('hi');
    } finally {
      await sb.destroy();
    }
  });

  it('write accepts streams', async () => {
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    try {
      await sb.files.write('/data/streamed.bin', Readable.from(Buffer.from('streamed-payload')));
      expect((await sb.files.read('/data/streamed.bin')).toString('utf8')).toBe('streamed-payload');
    } finally {
      await sb.destroy();
    }
  });

  it('applies file modes through chmod', async () => {
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    try {
      await sb.files.write('/data/readonly.txt', Buffer.from('locked'));
      await sb.files.chmod('/data/readonly.txt', 0o444);
      const root = (sb as unknown as { rootDir(): string }).rootDir();
      expect(statSync(join(root, 'data', 'readonly.txt')).mode & 0o777).toBe(0o444);
    } finally {
      await sb.destroy();
    }
  });

  it('list returns names at the requested directory', async () => {
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    try {
      await sb.files.write('/dir/a.txt', Buffer.from('a'));
      await sb.files.write('/dir/b.txt', Buffer.from('b'));
      const entries = await sb.files.list('/dir');
      expect(entries.sort()).toEqual(['a.txt', 'b.txt']);
    } finally {
      await sb.destroy();
    }
  });

  it('bash run captures stdout and exit code', async () => {
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    try {
      const r = await sb.run({ tool: 'bash', args: { command: 'echo hello' } });
      expect(r.stdout?.trim()).toBe('hello');
      expect(r.exit_code).toBe(0);
    } finally {
      await sb.destroy();
    }
  });

  it('bash run reports non-zero exit codes', async () => {
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    try {
      const r = await sb.run({ tool: 'bash', args: { command: 'exit 7' } });
      expect(r.exit_code).toBe(7);
    } finally {
      await sb.destroy();
    }
  });

  it('glob lists matching paths', async () => {
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    try {
      await sb.files.write('/srv/a.txt', Buffer.from('a'));
      await sb.files.write('/srv/b.txt', Buffer.from('b'));
      const r = await sb.run({ tool: 'glob', args: { pattern: '*.txt', root: '/srv' } });
      const matches = r.output as string[];
      expect(matches.sort()).toEqual(['a.txt', 'b.txt']);
    } finally {
      await sb.destroy();
    }
  });

  it('grep searches recursively', async () => {
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    try {
      await sb.files.write('/srv/a.txt', Buffer.from('hello world\n'));
      await sb.files.write('/srv/b.txt', Buffer.from('goodbye\n'));
      const r = await sb.run({ tool: 'grep', args: { pattern: 'hello', root: '/srv' } });
      expect(r.output as string).toContain('a.txt');
      expect(r.output as string).not.toContain('b.txt');
    } finally {
      await sb.destroy();
    }
  });

  it('destroy cleans up the tmp dir', async () => {
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    await sb.files.write('/data/x.txt', Buffer.from('x'));
    await sb.destroy();
    await expect(sb.files.read('/data/x.txt')).rejects.toThrow();
  });
});
