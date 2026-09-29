// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import {
  InMemorySkillStore,
  SkillBundleNotFoundError,
  SkillBundleValidationError,
} from '../../src/index.js';

describe('InMemorySkillStore', () => {
  it('encodes deterministically regardless of input order and normalizes safe paths', async () => {
    const store = new InMemorySkillStore();
    const files = [
      {
        path: './demo/assets/example.txt',
        content: Buffer.from('example'),
        mode: 0o600,
        mimeType: 'text/plain',
      },
      {
        path: 'demo/SKILL.md',
        content: Buffer.from('# Demo'),
        mimeType: 'text/markdown',
      },
    ];

    const first = await store.put('ws_alpha', 'sklv_one', files);
    const second = await store.put('ws_alpha', 'sklv_one', [...files].reverse());

    expect(second).toEqual(first);
    expect(first.files.map((file) => file.path)).toEqual([
      'demo/SKILL.md',
      'demo/assets/example.txt',
    ]);
    expect(first.files[0]).toMatchObject({
      sizeBytes: 6,
      sha256: createHash('sha256').update('# Demo').digest('hex'),
      mode: 0o644,
      mimeType: 'text/markdown',
    });
    expect(first.files[1]!.mode).toBe(0o644);
    expect(store.size()).toBe(1);
  });

  it('accepts a root-relative SKILL.md and preserves only executable intent', async () => {
    const store = new InMemorySkillStore();
    const record = await store.put('ws_alpha', 'sklv_root', [
      { path: 'SKILL.md', content: Buffer.from('# Root skill'), mode: 0o600 },
      { path: 'scripts/run.sh', content: Buffer.from('#!/bin/sh'), mode: 0o777 },
    ]);

    expect(record.files).toEqual([
      expect.objectContaining({ path: 'SKILL.md', mode: 0o644 }),
      expect.objectContaining({ path: 'scripts/run.sh', mode: 0o755 }),
    ]);
    const opened = await store.open('ws_alpha', 'sklv_root', record.sha256);
    expect(opened.files.map((file) => [file.path, file.mode])).toEqual([
      ['SKILL.md', 0o644],
      ['scripts/run.sh', 0o755],
    ]);
  });

  it('keeps stored and returned buffers immutable across callers', async () => {
    const store = new InMemorySkillStore();
    const original = Buffer.from('original');
    const record = await store.put('ws_alpha', 'sklv_one', [
      { path: 'demo/SKILL.md', content: original },
    ]);
    original.fill(0);

    const first = await store.open('ws_alpha', 'sklv_one', record.sha256);
    first.files[0]!.content.fill(0);
    first.record.files[0]!.path = 'mutated';
    const second = await store.open('ws_alpha', 'sklv_one', record.sha256);

    expect(second.files[0]!.content.toString()).toBe('original');
    expect(second.record.files[0]!.path).toBe('demo/SKILL.md');
  });

  it('isolates bundles by workspace, version, and digest and deletes idempotently', async () => {
    const store = new InMemorySkillStore();
    const record = await store.put('ws_alpha', 'sklv_one', [
      { path: 'demo/SKILL.md', content: Buffer.from('alpha') },
    ]);

    await expect(store.open('ws_beta', 'sklv_one', record.sha256)).rejects.toBeInstanceOf(
      SkillBundleNotFoundError,
    );
    await expect(store.open('ws_alpha', 'sklv_two', record.sha256)).rejects.toBeInstanceOf(
      SkillBundleNotFoundError,
    );
    await store.delete('ws_alpha', 'sklv_one', record.sha256);
    await store.delete('ws_alpha', 'sklv_one', record.sha256);
    await expect(store.open('ws_alpha', 'sklv_one', record.sha256)).rejects.toBeInstanceOf(
      SkillBundleNotFoundError,
    );
  });

  it.each([
    '/absolute/SKILL.md',
    '../escape/SKILL.md',
    'demo/../../escape',
    'C:/absolute/SKILL.md',
    'demo\\SKILL.md',
    `${'技'.repeat(86)}/SKILL.md`,
  ])('rejects unsafe path %j', async (unsafePath) => {
    const store = new InMemorySkillStore();
    await expect(
      store.put('ws_alpha', 'sklv_one', [{ path: unsafePath, content: Buffer.from('unsafe') }]),
    ).rejects.toBeInstanceOf(SkillBundleValidationError);
    expect(store.size()).toBe(0);
  });

  it('rejects duplicate normalized paths and invalid modes', async () => {
    const store = new InMemorySkillStore();
    await expect(
      store.put('ws_alpha', 'sklv_one', [
        { path: 'demo//SKILL.md', content: Buffer.from('one') },
        { path: 'demo/SKILL.md', content: Buffer.from('two') },
      ]),
    ).rejects.toThrow(/duplicate skill file path/);
    await expect(
      store.put('ws_alpha', 'sklv_one', [
        { path: 'demo/SKILL.md', content: Buffer.from('one'), mode: 0o1000 },
      ]),
    ).rejects.toThrow(/invalid mode/);
  });

  it('rejects file and directory path collisions', async () => {
    const store = new InMemorySkillStore();
    await expect(
      store.put('ws_alpha', 'sklv_one', [
        { path: 'references', content: Buffer.from('file') },
        { path: 'references/example.txt', content: Buffer.from('nested') },
      ]),
    ).rejects.toThrow(/conflicts with file ancestor/);
    expect(store.size()).toBe(0);
  });

  it.each([
    ['case', 'SKILL.md', 'skill.md'],
    ['Unicode composition', 'references/café.md', 'references/cafe\u0301.md'],
    ['full Unicode case fold', 'references/ß.md', 'references/ẞ.md'],
    ['full Unicode expansion', 'references/ß.md', 'references/SS.md'],
  ])('rejects %s-folded file path collisions', async (_case, firstPath, secondPath) => {
    const store = new InMemorySkillStore();
    await expect(
      store.put('ws_alpha', 'sklv_one', [
        { path: firstPath, content: Buffer.from('one') },
        { path: secondPath, content: Buffer.from('two') },
      ]),
    ).rejects.toThrow(/portable skill file path collision/);
    expect(store.size()).toBe(0);
  });

  it.each([
    ['case', 'References', 'references/example.txt'],
    ['Unicode composition', 'café', 'cafe\u0301/example.txt'],
    ['full Unicode case fold', 'references/ẞ', 'references/ss/example.txt'],
  ])('rejects %s-folded file and directory collisions', async (_case, filePath, nestedPath) => {
    const store = new InMemorySkillStore();
    await expect(
      store.put('ws_alpha', 'sklv_one', [
        { path: filePath, content: Buffer.from('file') },
        { path: nestedPath, content: Buffer.from('nested') },
      ]),
    ).rejects.toThrow(/conflicts with file ancestor/);
    expect(store.size()).toBe(0);
  });
});
