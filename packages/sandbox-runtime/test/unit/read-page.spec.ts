// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Mirrors services/harness-server/test/unit/read-page.spec.ts: the service
// suite exercises its own physical copy of this logic, so the canonical
// package copy needs the safety net in-package.
import { constants } from 'node:fs';
import { link, mkdtemp, mkdir, open, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import {
  buildSandboxReadPageCommand,
  buildSandboxReadPrerequisiteProbeCommand,
  parseSandboxReadPageResult,
  readUtf8FilePage,
  resolveOpenedDescriptorPath,
} from '../../src/read-page.js';

describe('atomic bounded sandbox reads', () => {
  it('reads one bounded UTF-8 page and advances across a four-byte code point', async () => {
    const root = await mkdtemp(join(tmpdir(), 'orca-ranged-read-'));
    try {
      const file = join(root, 'SKILL.md');
      await writeFile(file, '😀tail', 'utf8');

      const first = await readUtf8FilePage(file, { readableRoots: [root] }, { limit: 1 });
      expect(first.content).toContain('😀');
      expect(first.content).not.toContain('\uFFFD');
      expect(first.metadata).toMatchObject({
        bytes_read: 4,
        next_offset: 4,
        truncation: true,
      });

      const second = await readUtf8FilePage(
        file,
        { readableRoots: [root] },
        { offset: first.metadata.next_offset!, limit: 4 },
      );
      expect(second.content).toContain('tail');
      expect(second.metadata).toMatchObject({ truncation: false, next_offset: null });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('allows in-root symlinks but denies descriptor targets outside the root', async () => {
    const root = await mkdtemp(join(tmpdir(), 'orca-ranged-root-'));
    const outside = await mkdtemp(join(tmpdir(), 'orca-ranged-outside-'));
    try {
      const inside = join(root, 'inside.txt');
      const outsideSecret = join(outside, 'secret.txt');
      await writeFile(inside, 'inside', 'utf8');
      await writeFile(outsideSecret, 'secret', 'utf8');
      await symlink(inside, join(root, 'inside-link.txt'));
      await symlink(outsideSecret, join(root, 'outside-link.txt'));

      await expect(
        readUtf8FilePage(join(root, 'inside-link.txt'), { readableRoots: [root] }, {}),
      ).resolves.toMatchObject({ content: 'inside' });
      await expect(
        readUtf8FilePage(join(root, 'outside-link.txt'), { readableRoots: [root] }, {}),
      ).rejects.toThrow(/escapes its session resource root/);
    } finally {
      await rm(root, { recursive: true, force: true });
      await rm(outside, { recursive: true, force: true });
    }
  });

  it('rejects hard-linked files and non-regular targets', async () => {
    const root = await mkdtemp(join(tmpdir(), 'orca-ranged-hardlink-'));
    try {
      const source = join(root, 'source.txt');
      const alias = join(root, 'alias.txt');
      await writeFile(source, 'linked', 'utf8');
      await link(source, alias);
      await mkdir(join(root, 'directory'));

      await expect(readUtf8FilePage(alias, { readableRoots: [root] }, {})).rejects.toThrow(
        /hard-linked files/,
      );
      await expect(
        readUtf8FilePage(join(root, 'directory'), { readableRoots: [root] }, {}),
      ).rejects.toThrow(/not a regular file/);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('binds the Darwin fallback to the opened fd identity', async () => {
    const root = await mkdtemp(join(tmpdir(), 'orca-ranged-darwin-'));
    try {
      const openedPath = join(root, 'opened.txt');
      const checkedPath = join(root, 'checked.txt');
      await writeFile(openedPath, 'opened', 'utf8');
      await writeFile(checkedPath, 'checked', 'utf8');
      const handle = await open(openedPath, constants.O_RDONLY | constants.O_NONBLOCK);
      try {
        const openedStat = await handle.stat();
        await expect(
          resolveOpenedDescriptorPath(handle.fd, checkedPath, openedStat, 'darwin'),
        ).rejects.toThrow(/changed during identity check/);
      } finally {
        await handle.close();
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('fails closed when Linux descriptor identity is unavailable', async () => {
    await expect(
      resolveOpenedDescriptorPath(
        Number.MAX_SAFE_INTEGER,
        '/mnt/session/outputs/missing',
        { dev: 1, ino: 1 },
        'linux',
      ),
    ).rejects.toThrow(/Linux descriptor identity is unavailable/);
  });

  it('requires trusted roots before rendering a remote helper command', () => {
    expect(() => buildSandboxReadPageCommand('/allowed/file', undefined, {})).toThrow(
      /requires trusted readable roots/,
    );
    expect(() =>
      buildSandboxReadPageCommand('/outside/file', { readableRoots: ['/allowed'] }, {}),
    ).toThrow(/outside session resource roots/);

    const command = buildSandboxReadPageCommand(
      '/allowed/file',
      { readableRoots: ['/allowed'] },
      { offset: 2, limit: 10 },
    );
    expect(command).toContain('/usr/bin/env -i PATH=/usr/local/bin:/usr/bin node');
    expect(command).toContain('/proc/self/fd/');
    expect(command).toContain('path.relative(root.requestedPath, candidate)');
    expect(command).not.toContain('fs.openSync(candidate');
    expect(command).toContain('fs.readSync');
    expect(command).not.toContain('cat ');
  });

  it('probes the exact cleared-environment Linux fd prerequisites', () => {
    const command = buildSandboxReadPrerequisiteProbeCommand();

    expect(command).toContain('/usr/bin/env -i PATH=/usr/local/bin:/usr/bin node');
    expect(command).toContain('fs.openSync(');
    expect(command).toContain('/proc/self');
    expect(command).toContain('rootFd +');
    expect(command).toContain('/exe');
    expect(command).toContain('fs.statfsSync(descriptor)');
    expect(command).toContain('fs.readSync(targetFd');
  });

  it('rejects malformed or oversized remote helper envelopes', () => {
    expect(() => parseSandboxReadPageResult('not-json', {})).toThrow(/invalid JSON/);
    expect(() =>
      parseSandboxReadPageResult(
        JSON.stringify({ data_base64: Buffer.alloc(5).toString('base64'), total_bytes: 4 }),
        { limit: 1 },
      ),
    ).toThrow(/unexpected byte count/);
    expect(() =>
      parseSandboxReadPageResult(
        JSON.stringify({ data_base64: '*not-base64*', total_bytes: 1 }),
        {},
      ),
    ).toThrow(/invalid base64/);
    expect(() => parseSandboxReadPageResult('x'.repeat(150_001), {})).toThrow(/oversized response/);
  });
});
