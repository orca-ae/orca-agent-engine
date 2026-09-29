// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { Readable } from 'node:stream';
import { statSync } from 'node:fs';
import { join } from 'node:path';
import { InMemorySandboxRuntime } from '../../src/in-memory/runtime.js';

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

  it('advertises supportsFuse=false', () => {
    const rt = new InMemorySandboxRuntime();
    expect(rt.capabilities.supportsFuse).toBe(false);
    expect(rt.capabilities.supportsLocalMemory).toBe(true);
    expect(rt.capabilities.supportsWritePolicy).toBe(true);
  });

  it('reports a timed-out bash command as exit 124 with a kill marker, never success', async () => {
    const sb = await new InMemorySandboxRuntime().acquire({});
    try {
      const result = await sb.run({
        tool: 'bash',
        args: { command: 'sleep 5; echo done', timeout_ms: 100 },
      });
      // `code ?? 0` on the SIGKILL path used to report exit 0 with partial
      // output — a timed-out command masquerading as success.
      expect(result.exit_code).toBe(124);
      expect(result.stderr).toContain('[orca: killed by SIGKILL after timeout]');
    } finally {
      await sb.destroy();
    }
  });

  it('surfaces a nonexistent glob/grep root as an error result instead of crashing or lying', async () => {
    const sb = await new InMemorySandboxRuntime().acquire({});
    try {
      // Bad spawn cwd used to emit an unhandled 'error' event (process crash)
      // and `|| true` reported bad roots as empty success.
      const glob = await sb.run({ tool: 'glob', args: { pattern: '*', root: '/no/such/dir' } });
      expect(glob.exit_code).toBe(127);
      expect(glob.stderr).toContain('[orca: spawn failed');
      expect(glob.output).toEqual([]);
      const grep = await sb.run({ tool: 'grep', args: { pattern: 'x', root: '/no/such/dir' } });
      expect(grep.exit_code).toBe(127);
    } finally {
      await sb.destroy();
    }
  });

  it('rejects malformed known-tool args with exit 2 instead of shipping undefined', async () => {
    const sb = await new InMemorySandboxRuntime().acquire({});
    try {
      const result = await sb.run({ tool: 'bash', args: { cmd: 'echo oops' } });
      expect(result.exit_code).toBe(2);
      expect(result.stderr).toMatch(/requires a string 'command'/);
    } finally {
      await sb.destroy();
    }
  });

  it('runPrivileged is absent — no privilege boundary, absence is the feature signal', async () => {
    // Optional-member convention (like spawn/endpoint): a runtime that cannot
    // do privileged ops OMITS the method rather than shipping an
    // always-throwing stub the type system presents as callable.
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    try {
      expect(sb.runPrivileged).toBeUndefined();
    } finally {
      await sb.destroy();
    }
  });

  it('accepts filesystem-root preparation inside its private workdir', async () => {
    const sb = await new InMemorySandboxRuntime().acquire({});
    try {
      await expect(
        sb.prepareFilesystemRoots!(['/mnt/inputs', '/workspace/skills']),
      ).resolves.toBeUndefined();
    } finally {
      await sb.destroy();
    }
  });
});
