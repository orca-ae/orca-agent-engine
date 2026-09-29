// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Mirrors services/sandbox-harness/test/read-page.spec.ts: the service suites
// exercise their own physical copies of this logic, so the canonical package
// copy needs the safety net in-package. Adjusted to this module's
// (path, constraint, input) signature — the constraint carries bare
// readableRoots rather than a full write policy.
import { constants } from 'node:fs';
import { link, mkdtemp, open, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { readUtf8FilePage, resolveOpenedDescriptorPath } from '../../src/read-page.js';
import type { SandboxReadConstraint } from '../../src/sandbox-runtime.js';

describe('bounded policy-enforced file reads', () => {
  it('pages a large file without losing a UTF-8 boundary', async () => {
    const root = await mkdtemp(join(tmpdir(), 'orca-readable-root-'));
    try {
      const file = join(root, 'SKILL.md');
      const marker = '技能尾部';
      await writeFile(file, `${'x'.repeat(100_001)}${marker}`, 'utf8');
      const policy = readonlyPolicy(root);

      const first = await readUtf8FilePage(file, policy, {});
      expect(first.content).not.toContain(marker);
      expect(first.metadata).toMatchObject({
        bytes_read: 100_000,
        truncation: true,
        next_offset: 100_000,
      });

      const second = await readUtf8FilePage(file, policy, {
        offset: first.metadata.next_offset!,
      });
      expect(second.content).toContain(marker);
      expect(second.metadata).toMatchObject({ truncation: false, next_offset: null });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('denies paths outside roots and symlinks that escape a readable root', async () => {
    const root = await mkdtemp(join(tmpdir(), 'orca-readable-root-'));
    const outside = await mkdtemp(join(tmpdir(), 'orca-readable-outside-'));
    try {
      const secret = join(outside, 'secret.txt');
      await writeFile(secret, 'secret', 'utf8');
      const alias = join(root, 'alias.txt');
      await symlink(secret, alias);
      const policy = readonlyPolicy(root);

      await expect(readUtf8FilePage(secret, policy, {})).rejects.toThrow(
        /outside session resource roots/,
      );
      await expect(readUtf8FilePage(alias, policy, {})).rejects.toThrow(
        /escapes its session resource root/,
      );
      await expect(readUtf8FilePage('/proc/self/environ', policy, {})).rejects.toThrow(
        /outside session resource roots/,
      );
    } finally {
      await rm(root, { recursive: true, force: true });
      await rm(outside, { recursive: true, force: true });
    }
  });

  it('rejects hard-linked files', async () => {
    const root = await mkdtemp(join(tmpdir(), 'orca-readable-root-'));
    try {
      const source = join(root, 'source.txt');
      const alias = join(root, 'alias.txt');
      await writeFile(source, 'secret', 'utf8');
      await link(source, alias);

      await expect(readUtf8FilePage(alias, readonlyPolicy(root), {})).rejects.toThrow(
        /hard-linked files/,
      );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('binds Darwin fallback validation to the opened fd identity', async () => {
    const root = await mkdtemp(join(tmpdir(), 'orca-readable-root-'));
    try {
      const openedPath = join(root, 'opened.txt');
      const checkedPath = join(root, 'checked.txt');
      await writeFile(openedPath, 'opened', 'utf8');
      await writeFile(checkedPath, 'checked', 'utf8');
      const handle = await open(openedPath, constants.O_RDONLY | constants.O_NONBLOCK);
      try {
        await expect(
          resolveOpenedDescriptorPath(handle.fd, checkedPath, await handle.stat(), 'darwin'),
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
        '/workspace/skills/missing',
        { dev: 1, ino: 1 },
        'linux',
      ),
    ).rejects.toThrow(/Linux descriptor identity is unavailable/);
  });
});

function readonlyPolicy(root: string): SandboxReadConstraint {
  return { readableRoots: [root] };
}
