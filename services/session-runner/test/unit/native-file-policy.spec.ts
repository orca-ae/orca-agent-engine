// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import { mkdtemp, mkdir, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { nativeFileToolSandbox } from '../../src/harness/pi/file-policy.js';
import type { SandboxHandle } from '../../src/sandbox/seam.js';

const directories: string[] = [];
afterEach(async () => {
  for (const directory of directories.splice(0))
    await rm(directory, { recursive: true, force: true });
});

async function fixture(paged = true) {
  const root = await mkdtemp(join(tmpdir(), 'orca-native-policy-'));
  directories.push(root);
  await mkdir(join(root, 'mnt/session/outputs'), { recursive: true });
  await mkdir(join(root, 'mnt/context'), { recursive: true });
  const raw = {
    id: 'sbx_fixture',
    files: {
      write: vi.fn(async () => {}),
      read: vi.fn(async () => Buffer.from('context')),
      list: vi.fn(async () => ['context.txt']),
      chmod: vi.fn(async () => {}),
      delete: vi.fn(async () => {}),
      ...(paged ? { readUtf8Page: vi.fn(async () => ({ text: 'context' })) } : {}),
    },
    run: vi.fn(async () => ({ exit_code: 0, output: 'match' })),
    pause: vi.fn(async () => {}),
    resume: vi.fn(async () => {}),
    destroy: vi.fn(async () => {}),
  };
  const policy = {
    writablePaths: [{ path: '/mnt/session/outputs', kind: 'session_output' }],
    readonlyPaths: ['/mnt/context'],
  };
  const sandbox = nativeFileToolSandbox(raw as unknown as SandboxHandle, root, policy);
  return { root, raw, policy, sandbox };
}

it('authorizes logical and executable paths and preserves paged-read confinement', async () => {
  const { root, raw, sandbox } = await fixture();
  expect(await sandbox.files.read(join(root, 'mnt/context/input.txt'))).toEqual(
    Buffer.from('context'),
  );
  expect(raw.files.read).toHaveBeenCalledWith('/mnt/context/input.txt');
  expect(await sandbox.files.list('/mnt/context')).toEqual(['context.txt']);
  await sandbox.files.readUtf8Page!('/mnt/context/input.txt', { offset: 0, limit: 20 });
  expect(raw.files.readUtf8Page).toHaveBeenCalledWith(
    '/mnt/context/input.txt',
    { offset: 0, limit: 20 },
    { readableRoots: ['/mnt/context', '/mnt/session/outputs'] },
  );
  await sandbox.files.write('/mnt/session/outputs/a.txt', Buffer.from('a'));
  await sandbox.files.write(
    join(root, 'mnt/session/outputs/b.txt'),
    Readable.from([Buffer.from('b'), Buffer.from('c')]),
  );
  expect(raw.files.write).toHaveBeenLastCalledWith('/mnt/session/outputs/b.txt', Buffer.from('bc'));
  await sandbox.files.chmod('/mnt/session/outputs/b.txt', 0o600);
  await sandbox.files.delete('/mnt/session/outputs/b.txt');
  expect(raw.files.chmod).toHaveBeenCalledWith('/mnt/session/outputs/b.txt', 0o600);
  expect(raw.files.delete).toHaveBeenCalledWith('/mnt/session/outputs/b.txt');
  await sandbox.pause();
  await sandbox.resume();
  await sandbox.destroy();
  expect(raw.pause).toHaveBeenCalledOnce();
  expect(raw.resume).toHaveBeenCalledOnce();
  expect(raw.destroy).toHaveBeenCalledOnce();
});

it.each([
  '/etc/passwd',
  '/mnt/context-other/input',
  '/mnt/context/../secret',
  '/mnt/context\\input',
])('rejects unauthorized or ambiguous read paths: %s', async (path) => {
  const { raw, sandbox } = await fixture();
  await expect(sandbox.files.read(path)).rejects.toThrow();
  expect(raw.files.read).not.toHaveBeenCalled();
});

it('refuses mutation of readonly files and aliases into protected roots', async () => {
  const { root, raw, sandbox } = await fixture();
  await symlink(join(root, 'mnt/context'), join(root, 'mnt/session/outputs/alias'));
  await expect(sandbox.files.write('/mnt/context/input', Buffer.from('bad'))).rejects.toThrow();
  await expect(sandbox.files.chmod('/mnt/context/input', 0o777)).rejects.toThrow();
  await expect(sandbox.files.delete('/mnt/context/input')).rejects.toThrow();
  await expect(
    sandbox.files.write('/mnt/session/outputs/alias/input', Buffer.from('bad')),
  ).rejects.toThrow('protected root');
  expect(raw.files.write).not.toHaveBeenCalled();
  expect(raw.files.chmod).not.toHaveBeenCalled();
  expect(raw.files.delete).not.toHaveBeenCalled();
});

it('rejects malformed policy and fails closed when paged reads are unavailable', async () => {
  const { root, raw, policy, sandbox } = await fixture(false);
  expect(() =>
    nativeFileToolSandbox(raw as unknown as SandboxHandle, root, {
      ...policy,
      readonlyPaths: ['/mnt/session/outputs/private'],
    }),
  ).toThrow('overlapping');
  await expect(
    sandbox.files.readUtf8Page!('/mnt/context/input', { offset: 0, limit: 1 }),
  ).rejects.toThrow('unavailable');
});

it.each([
  ['bash', 'echo escaped'],
  ['glob', '../secret'],
  ['grep', '-e'],
  ['grep', '/etc/passwd'],
  ['glob', '.private'],
  ['glob', '**/.private'],
  ['glob', '$(id)'],
  ['glob', undefined],
])('rejects unsafe native searches: %s %s', async (tool, pattern) => {
  const { raw, sandbox } = await fixture();
  await expect(sandbox.run({ tool, args: { pattern } })).rejects.toThrow();
  expect(raw.run).not.toHaveBeenCalled();
});

it('anchors allowed searches to authorized roots while preserving grep expressions', async () => {
  const { root, raw, sandbox } = await fixture();
  await sandbox.run({ tool: 'glob', args: { pattern: '**/*.txt' } });
  expect(raw.run).toHaveBeenLastCalledWith({
    tool: 'glob',
    args: { pattern: '**/*.txt', root: '/mnt/session/outputs' },
  });
  await sandbox.run({ tool: 'grep', args: { root: join(root, 'mnt/context'), pattern: 'a|b' } });
  expect(raw.run).toHaveBeenLastCalledWith({
    tool: 'grep',
    args: { root: '/mnt/context', pattern: 'a|b' },
  });
});
