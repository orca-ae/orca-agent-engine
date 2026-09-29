// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';
import { buildAgentToolset } from '../../src/sandbox/agent-toolset.js';

async function withSandbox<T>(
  fn: (sandbox: import('../../src/sandbox/sandbox-runtime.js').SandboxHandle) => Promise<T>,
): Promise<T> {
  const rt = new InMemorySandboxRuntime();
  const sb = await rt.acquire({});
  const rawReadUtf8Page = sb.files.readUtf8Page.bind(sb.files);
  sb.files.readUtf8Page = async (path, input, constraint) =>
    await rawReadUtf8Page(path, input, constraint ?? { readableRoots: ['/'] });
  try {
    return await fn(sb);
  } finally {
    await sb.destroy();
  }
}

describe('agent_toolset', () => {
  it('lists and deletes through the sandbox files API', async () => {
    await withSandbox(async (sandbox) => {
      await sandbox.files.write('/tmp/toolset-list/file.txt', Buffer.from('test'));
      const tools = buildAgentToolset(sandbox);
      const listing = await tools
        .find((tool) => tool.name === 'list')!
        .execute({ path: '/tmp/toolset-list' });
      expect(listing.content).toContain('file.txt');
      const deleted = await tools
        .find((tool) => tool.name === 'delete')!
        .execute({ path: '/tmp/toolset-list' });
      expect(deleted.error).toBeUndefined();
      await expect(sandbox.files.read('/tmp/toolset-list/file.txt')).rejects.toThrow();
    });
  });
  it('builds 9 tools', async () => {
    await withSandbox(async (sb) => {
      const tools = buildAgentToolset(sb);
      expect(tools.map((t) => t.name).sort()).toEqual(
        ['bash', 'edit', 'glob', 'grep', 'read', 'list', 'delete', 'web_fetch', 'write'].sort(),
      );
    });
  });

  it('bash returns stdout + zero exit', async () => {
    await withSandbox(async (sb) => {
      const tools = buildAgentToolset(sb);
      const bash = tools.find((t) => t.name === 'bash')!;
      const r = await bash.execute({ command: 'echo hi' });
      expect(r.error).toBeUndefined();
      expect(r.content).toContain('hi');
    });
  });

  it('bash surfaces non-zero exit as error', async () => {
    await withSandbox(async (sb) => {
      const tools = buildAgentToolset(sb);
      const bash = tools.find((t) => t.name === 'bash')!;
      const r = await bash.execute({ command: 'exit 3' });
      expect(r.error).toContain('exit code 3');
    });
  });

  it('write + read round-trips text', async () => {
    await withSandbox(async (sb) => {
      const tools = buildAgentToolset(sb);
      const write = tools.find((t) => t.name === 'write')!;
      const read = tools.find((t) => t.name === 'read')!;
      const w = await write.execute({ path: '/data/hi.txt', content: 'hello' });
      expect(w.error).toBeUndefined();
      const r = await read.execute({ path: '/data/hi.txt' });
      expect(r.content).toMatch(/^hello\n\[orca_read /);
    });
  });

  it('read exposes bounded byte pagination while keeping path-only calls valid', async () => {
    await withSandbox(async (sb) => {
      const tools = buildAgentToolset(sb);
      const read = tools.find((t) => t.name === 'read')!;
      const marker = 'LEGACY_TOOLSET_TAIL_MARKER';
      await sb.files.write('/data/large.txt', Buffer.from(`${'x'.repeat(4100)}${marker}`, 'utf8'));
      const wholeRead = vi.spyOn(sb.files, 'read').mockRejectedValue(new Error('whole read used'));

      const first = await read.execute({ path: '/data/large.txt' });
      expect(first.content).not.toContain(marker);
      expect(first.output).toMatchObject({
        truncation: true,
        next_offset: 4096,
        offset_unit: 'utf8_bytes',
      });

      const nextOffset = (first.output as { next_offset: number }).next_offset;
      const second = await read.execute({ path: '/data/large.txt', offset: nextOffset });
      expect(second.content).toContain(marker);
      expect(second.output).toMatchObject({ truncation: false, next_offset: null });
      expect(wholeRead).not.toHaveBeenCalled();
    });
  });

  it('edit replaces all occurrences', async () => {
    await withSandbox(async (sb) => {
      const tools = buildAgentToolset(sb);
      const write = tools.find((t) => t.name === 'write')!;
      const edit = tools.find((t) => t.name === 'edit')!;
      const read = tools.find((t) => t.name === 'read')!;
      await write.execute({ path: '/f.txt', content: 'foo bar foo' });
      const e = await edit.execute({ path: '/f.txt', find: 'foo', replace: 'baz' });
      expect(e.error).toBeUndefined();
      expect(e.content).toContain('replaced 2');
      const r = await read.execute({ path: '/f.txt' });
      expect(r.content).toMatch(/^baz bar baz\n\[orca_read /);
    });
  });

  it('edit no-ops when pattern not found', async () => {
    await withSandbox(async (sb) => {
      const tools = buildAgentToolset(sb);
      const write = tools.find((t) => t.name === 'write')!;
      const edit = tools.find((t) => t.name === 'edit')!;
      await write.execute({ path: '/f.txt', content: 'hello' });
      const e = await edit.execute({ path: '/f.txt', find: 'nope', replace: 'X' });
      expect(e.content).toContain('no occurrences');
    });
  });

  it('edit refuses files above the bounded-read limit without a whole-file read', async () => {
    await withSandbox(async (sb) => {
      await sb.files.write('/large.txt', Buffer.alloc(100_001, 0x61));
      const wholeRead = vi.spyOn(sb.files, 'read').mockRejectedValue(new Error('whole read used'));
      const edit = buildAgentToolset(sb).find((tool) => tool.name === 'edit')!;

      const result = await edit.execute({
        path: '/large.txt',
        find: 'a',
        replace: 'b',
      });

      expect(result.error).toContain('exceeds the 100000-byte edit limit');
      expect(wholeRead).not.toHaveBeenCalled();
    });
  });

  it('glob returns matching paths', async () => {
    await withSandbox(async (sb) => {
      const tools = buildAgentToolset(sb);
      const write = tools.find((t) => t.name === 'write')!;
      const glob = tools.find((t) => t.name === 'glob')!;
      await write.execute({ path: '/srv/a.txt', content: 'a' });
      await write.execute({ path: '/srv/b.txt', content: 'b' });
      const r = await glob.execute({ pattern: '*.txt', root: '/srv' });
      const matches = (r.output as string[]).sort();
      expect(matches).toEqual(['a.txt', 'b.txt']);
    });
  });

  it('grep returns matching lines', async () => {
    await withSandbox(async (sb) => {
      const tools = buildAgentToolset(sb);
      const write = tools.find((t) => t.name === 'write')!;
      const grep = tools.find((t) => t.name === 'grep')!;
      await write.execute({ path: '/srv/a.txt', content: 'hello world\n' });
      await write.execute({ path: '/srv/b.txt', content: 'goodbye\n' });
      const r = await grep.execute({ pattern: 'hello', root: '/srv' });
      expect(r.content).toContain('a.txt');
      expect(r.content).not.toContain('b.txt');
    });
  });

  it('web_fetch reaches an HTTP server', async () => {
    const http = await import('node:http');
    const server = http.createServer((_req, res) => {
      res.statusCode = 200;
      res.setHeader('content-type', 'text/plain');
      res.end('the body');
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()));
    const port = (server.address() as { port: number }).port;
    try {
      await withSandbox(async (sb) => {
        const tools = buildAgentToolset(sb);
        const wf = tools.find((t) => t.name === 'web_fetch')!;
        const r = await wf.execute({ url: `http://127.0.0.1:${port}/` });
        expect(r.error).toBeUndefined();
        expect(r.content).toContain('the body');
      });
    } finally {
      await new Promise<void>((r) => server.close(() => r()));
    }
  });
});
