// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { beforeEach, expect, it, vi } from 'vitest';
import { walkSdkConfig } from '../fixtures/walk-sdk-config.mjs';

vi.mock('node:fs/promises', () => ({ readdir: vi.fn() }));
const root = '/fixture-config';
const directory = (name) => ({ name, isDirectory: () => true, isFile: () => false });
const file = (name) => ({ name, isDirectory: () => false, isFile: () => true });
beforeEach(() => vi.mocked(readdir).mockReset());

it('continues to discover spills when a listed SDK lock directory disappears', async () => {
  vi.mocked(readdir)
    .mockResolvedValueOnce([directory('.claude.json.lock'), directory('tool-results')])
    .mockRejectedValueOnce(Object.assign(new Error('removed lock'), { code: 'ENOENT' }))
    .mockResolvedValueOnce([file('result.txt')]);
  expect(await walkSdkConfig(root)).toEqual([join(root, 'tool-results', 'result.txt')]);
});

it('does not hide a missing config root', async () => {
  const error = Object.assign(new Error('missing root'), { code: 'ENOENT' });
  vi.mocked(readdir).mockRejectedValueOnce(error);
  await expect(walkSdkConfig(root)).rejects.toBe(error);
});

it('does not hide permission failures in a child directory', async () => {
  const error = Object.assign(new Error('denied'), { code: 'EACCES' });
  vi.mocked(readdir)
    .mockResolvedValueOnce([directory('tool-results')])
    .mockRejectedValueOnce(error);
  await expect(walkSdkConfig(root)).rejects.toBe(error);
});

it('does not traverse symlinks', async () => {
  vi.mocked(readdir).mockResolvedValueOnce([
    { name: 'link', isDirectory: () => false, isFile: () => false },
    file('config.json'),
  ]);
  expect(await walkSdkConfig(root)).toEqual([join(root, 'config.json')]);
  expect(readdir).toHaveBeenCalledTimes(1);
});
