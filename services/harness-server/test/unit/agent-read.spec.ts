// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { buildOrcaSdkTools } from '../../src/harness/claude/mcp-tools.js';
import { buildAgentToolset } from '../../src/sandbox/agent-toolset.js';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';
import type { SandboxHandle } from '../../src/sandbox/sandbox-runtime.js';
import type { ReadPageMetadata } from '../../src/sandbox/read-page.js';

describe.each(['mcp', 'legacy'] as const)('bounded agent Read: %s', (surface) => {
  let sandbox: SandboxHandle;
  let call: (name: string, input: Record<string, unknown>) => Promise<unknown>;

  beforeEach(async () => {
    sandbox = await new InMemorySandboxRuntime().acquire({});
    const read = sandbox.files.readUtf8Page.bind(sandbox.files);
    sandbox.files.readUtf8Page = (path, input) => read(path, input, { readableRoots: ['/data'] });
    call = async (name, input) => {
      if (surface === 'mcp') {
        // Dynamic lookup erases the SDK's per-schema generic argument type.
        const tool = buildOrcaSdkTools(sandbox).find((tool) => tool.name === name)! as unknown as {
          handler: (input: Record<string, unknown>, extra: object) => Promise<unknown>;
        };
        return await tool.handler(input, {});
      }
      return await buildAgentToolset(sandbox)
        .find((tool) => tool.name === name)!
        .execute(input);
    };
  });

  afterEach(async () => {
    await sandbox.destroy();
  });

  function unpack(result: unknown) {
    expect(Buffer.byteLength(JSON.stringify(result))).toBeLessThanOrEqual(16_384);
    const value = result as {
      content: string | Array<{ text: string }>;
      error?: string;
      isError?: boolean;
    };
    expect(value.error).toBeUndefined();
    expect(value.isError).not.toBe(true);
    const text = typeof value.content === 'string' ? value.content : value.content[0]!.text;
    const prefix = '[orca_read ';
    const start = text.lastIndexOf(prefix);
    expect(start).toBeGreaterThanOrEqual(0);
    const metadata = JSON.parse(text.slice(start + prefix.length, -1)) as ReadPageMetadata;
    const body = Buffer.from(text).subarray(0, metadata.bytes_read).toString();
    const separator =
      body.length === 0 || body.endsWith(String.fromCharCode(10)) ? '' : String.fromCharCode(10);
    expect(text.slice(0, start)).toBe(body + separator);
    if (surface === 'legacy') expect((result as { output: unknown }).output).toEqual(metadata);
    return { text: body, metadata };
  }

  it.each([undefined, 100_000, 8192, 31])(
    'caps requested limit %s and reports effective bytes',
    async (limit) => {
      await sandbox.files.write('/data/file', Buffer.alloc(156_186, 0x61));
      const read = vi.spyOn(sandbox.files, 'readUtf8Page');
      const wholeRead = vi
        .spyOn(sandbox.files, 'read')
        .mockRejectedValue(new Error('whole read used'));
      const { text, metadata } = unpack(
        await call('read', { path: '/data/file', ...(limit === undefined ? {} : { limit }) }),
      );
      const effective = Math.min(limit ?? 4096, 8192);
      expect(metadata).toMatchObject({
        offset: 0,
        limit: effective,
        bytes_read: effective,
        total_bytes: 156_186,
        next_offset: effective,
        truncation: true,
        offset_unit: 'utf8_bytes',
      });
      expect(Buffer.byteLength(text)).toBe(effective);
      expect(read).toHaveBeenCalledWith('/data/file', { limit: effective });
      expect(wholeRead).not.toHaveBeenCalled();
    },
  );

  it('returns a full page in one read when the actual envelope exactly fits the budget', async () => {
    // Calibrated counterexamples from formatter ablation: budgeting a canonical
    // ReadPage instead would add 98 bytes (MCP) or 2 bytes (legacy), halve this
    // already-fitting page, and force another tool call to finish the file.
    const bytes = Buffer.alloc(8192, 0x61);
    const controls = surface === 'mcp' ? 1596 : 1576;
    for (let i = 0; i < controls; i++) bytes[Math.floor((i * 8191) / controls)] = i % 2;
    if (surface === 'legacy') {
      let quotes = 0;
      for (let i = 0; quotes < 4; i++) {
        if (bytes[i] !== 0x61) continue;
        bytes[i] = quotes++ % 2 === 0 ? 0x22 : 0x5c;
      }
    }
    await sandbox.files.write('/data/exact', bytes);
    const read = vi.spyOn(sandbox.files, 'readUtf8Page');
    const result = await call('read', { path: '/data/exact', limit: 8192 });
    expect(Buffer.byteLength(JSON.stringify(result))).toBe(16_384);
    const { text, metadata } = unpack(result);
    expect(Buffer.from(text)).toEqual(bytes);
    expect(metadata).toMatchObject({
      limit: 8192,
      bytes_read: 8192,
      next_offset: null,
      truncation: false,
    });
    expect(read).toHaveBeenCalledTimes(1);
  });

  it.each([String.fromCharCode(0, 1, 9, 10, 13, 34, 92), 'a界😀é', 'x'.repeat(156_186)])(
    'reconstructs escaped and Unicode pages',
    async (unit) => {
      const original = unit.length > 1000 ? unit : unit.repeat(2500);
      await sandbox.files.write('/data/file', Buffer.from(original));
      const read = vi.spyOn(sandbox.files, 'readUtf8Page');
      let offset = 0;
      let reconstructed = '';
      do {
        const { text, metadata } = unpack(
          await call('read', { path: '/data/file', offset, limit: 100_000 }),
        );
        expect(metadata.bytes_read).toBe(Buffer.byteLength(text));
        expect(metadata.limit).toBeLessThanOrEqual(8192);
        expect(text).not.toContain(String.fromCharCode(0xfffd));
        reconstructed += text;
        if (!metadata.truncation) {
          expect(metadata.next_offset).toBeNull();
          break;
        }
        expect(metadata.next_offset).toBe(offset + Buffer.byteLength(text));
        expect(metadata.next_offset).toBeGreaterThan(offset);
        offset = metadata.next_offset!;
      } while (offset < Buffer.byteLength(original));
      expect(reconstructed).toBe(original);
      if (unit.includes(String.fromCharCode(0)))
        expect(read.mock.calls.some(([, input]) => input.limit! < 8192)).toBe(true);
    },
  );

  it('preserves code point progress for tiny limits and handles EOF', async () => {
    await sandbox.files.write('/data/file', Buffer.from('😀'));
    expect(unpack(await call('read', { path: '/data/file', limit: 1 })).metadata).toMatchObject({
      bytes_read: 4,
      next_offset: null,
    });
    expect(unpack(await call('read', { path: '/data/file', offset: 4 })).metadata).toMatchObject({
      bytes_read: 0,
      truncation: false,
      next_offset: null,
    });
    await sandbox.files.write('/data/empty', Buffer.alloc(0));
    expect(unpack(await call('read', { path: '/data/empty' })).text).toBe('');
  });

  it.each([
    { limit: 100_001 },
    { limit: 0 },
    { limit: -1 },
    { limit: 1.5 },
    { offset: -1 },
    { offset: 1 },
    { offset: 10 },
  ])('rejects invalid input %j', async (input) => {
    await sandbox.files.write('/data/file', Buffer.from('😀'));
    const result = await call('read', { path: '/data/file', ...input });
    expect(JSON.stringify(result)).toContain('read failed:');
    expect(
      surface === 'mcp'
        ? (result as { isError: boolean }).isError
        : (result as { error: string }).error,
    ).toBeTruthy();
  });

  it('bounds missing, denied, and oversized runtime errors', async () => {
    for (const path of ['/data/missing', '/outside']) {
      expect(JSON.stringify(await call('read', { path }))).toContain('read failed:');
    }
    vi.spyOn(sandbox.files, 'readUtf8Page').mockRejectedValue(
      new Error(String.fromCharCode(0).repeat(100_000)),
    );
    const result = await call('read', { path: '/data/file' });
    expect(Buffer.byteLength(JSON.stringify(result))).toBeLessThanOrEqual(16_384);
    expect(JSON.stringify(result)).toContain('read failed:');
  });

  it('returns only the final independently authorized reread, not stale metadata', async () => {
    await sandbox.files.write('/data/file', Buffer.alloc(20_000, 0));
    const originalRead = sandbox.files.readUtf8Page.bind(sandbox.files);
    const read = vi
      .spyOn(sandbox.files, 'readUtf8Page')
      .mockImplementationOnce(async (path, input) => {
        const page = await originalRead(path, input);
        await sandbox.files.write(path, Buffer.from('new'));
        return page;
      });
    const result = unpack(await call('read', { path: '/data/file', limit: 100_000 }));
    expect(read).toHaveBeenCalledTimes(2);
    expect(result.text).toBe('new');
    expect(result.metadata).toMatchObject({
      limit: 4096,
      total_bytes: 3,
      bytes_read: 3,
      truncation: false,
      next_offset: null,
    });
  });

  it('does not return an oversized prior attempt when the reread fails', async () => {
    await sandbox.files.write('/data/file', Buffer.alloc(20_000, 0));
    const originalRead = sandbox.files.readUtf8Page.bind(sandbox.files);
    const read = vi
      .spyOn(sandbox.files, 'readUtf8Page')
      .mockImplementationOnce(originalRead)
      .mockRejectedValueOnce(new Error('read denied'));
    const result = await call('read', { path: '/data/file', limit: 100_000 });
    expect(read).toHaveBeenCalledTimes(2);
    expect(JSON.stringify(result)).toContain('read failed: read denied');
    expect(Buffer.byteLength(JSON.stringify(result))).toBeLessThanOrEqual(16_384);
  });

  it('budgets malformed UTF-8 replacements while retaining source-byte offsets', async () => {
    await sandbox.files.write('/data/file', Buffer.alloc(20_000, 0xff));
    const result = await call('read', { path: '/data/file', limit: 100_000 });
    expect(Buffer.byteLength(JSON.stringify(result))).toBeLessThanOrEqual(16_384);
    const value = result as { content: string | Array<{ text: string }> };
    const text = typeof value.content === 'string' ? value.content : value.content[0]!.text;
    const start = text.lastIndexOf('[orca_read ');
    const metadata = JSON.parse(text.slice(start + '[orca_read '.length, -1)) as ReadPageMetadata;
    expect(metadata).toMatchObject({ limit: 4096, bytes_read: 4096, next_offset: 4096 });
    expect(text.slice(0, start)).toBe(
      String.fromCharCode(0xfffd).repeat(4096) + String.fromCharCode(10),
    );
  });

  it('keeps Edit capacity at 100000 bytes', async () => {
    await sandbox.files.write('/data/file', Buffer.from('a'.repeat(99_999) + 'z'));
    expect(
      JSON.stringify(await call('edit', { path: '/data/file', find: 'z', replace: 'b' })),
    ).toContain('replaced 1');
    expect((await sandbox.files.read('/data/file')).toString()).toBe('a'.repeat(99_999) + 'b');
    await sandbox.files.write('/data/file', Buffer.alloc(100_001, 0x61));
    expect(
      JSON.stringify(await call('edit', { path: '/data/file', find: 'a', replace: 'b' })),
    ).toContain('exceeds the 100000-byte edit limit');
    expect((await sandbox.files.read('/data/file'))[0]).toBe(0x61);
  });
});
