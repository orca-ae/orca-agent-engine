// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Readable } from 'node:stream';
import { describe, expect, it } from 'vitest';
import { InMemoryBlobStore } from '../../src/blob/in-memory.js';
import { LocalFileStore } from '../../src/file-store.js';
import { InMemoryFileMetadataStore } from '../../src/metadata/in-memory.js';
import type { CreateFileInput } from '../../src/types.js';

const id = `file_${'a'.repeat(64)}`;
function input(overrides: Partial<CreateFileInput> = {}): CreateFileInput {
  return {
    id,
    workspaceId: 'ws_output_replay',
    scopeId: 'ses_output_replay',
    purpose: 'agent_output',
    filename: 'answer.txt',
    mimeType: 'text/plain',
    metadata: { output_path: '/mnt/session/outputs/answer.txt', checkpoint_id: 'checkpoint1' },
    content: Readable.from('answer'),
    ...overrides,
  };
}
function backing() {
  const metadataStore = new InMemoryFileMetadataStore();
  const blobStore = new InMemoryBlobStore();
  return { metadataStore, blobStore };
}

describe('durable output identity', () => {
  it('replays the same output after store reconstruction without creating another file', async () => {
    const data = backing();
    const first = await new LocalFileStore(data).create(input());
    const rebuilt = new LocalFileStore(data);
    expect(await rebuilt.create(input())).toEqual(first);
    expect(data.metadataStore.size()).toBe(1);
    const opened = await rebuilt.open(first.workspaceId, first.id);
    const chunks: Buffer[] = [];
    for await (const chunk of opened!.stream) chunks.push(Buffer.from(chunk));
    expect(Buffer.concat(chunks).toString()).toBe('answer');
  });

  it('joins concurrent identical retries and keeps distinct output identities separate', async () => {
    const data = backing();
    const stores = [new LocalFileStore(data), new LocalFileStore(data)];
    const result = await Promise.all(stores.map((store) => store.create(input())));
    expect(result[0]).toEqual(result[1]);
    const other = await stores[0]!.create(input({ id: `file_${'b'.repeat(64)}` }));
    expect(other.id).not.toBe(result[0]!.id);
    expect(data.metadataStore.size()).toBe(2);
  });

  it.each([
    { workspaceId: 'ws_other' },
    { scopeId: 'ses_other' },
    { filename: 'other.txt' },
    { mimeType: 'application/octet-stream' },
    { downloadable: false },
    { metadata: { checkpoint_id: 'other' } },
  ])('rejects identity reuse with changed attributes: %j', async (overrides) => {
    const data = backing();
    const store = new LocalFileStore(data);
    const original = await store.create(input());
    await expect(store.create(input(overrides))).rejects.toThrow();
    expect(await store.get(original.workspaceId, id)).toEqual(original);
  });

  it('rejects changed bytes, including concurrent writers, without overwriting metadata', async () => {
    const data = backing();
    const store = new LocalFileStore(data);
    const results = await Promise.allSettled([
      store.create(input()),
      store.create(input({ content: Readable.from('different') })),
    ]);
    expect(results.filter((item) => item.status === 'fulfilled')).toHaveLength(1);
    expect(results.filter((item) => item.status === 'rejected')).toHaveLength(1);
    expect(data.metadataStore.size()).toBe(1);
  });

  it.each(['archive', 'delete'] as const)(
    'never resurrects a removed output after %s',
    async (method) => {
      const store = new LocalFileStore(backing());
      await store.create(input());
      await store[method]('ws_output_replay', id);
      await expect(store.create(input())).rejects.toThrow();
      expect((await store.list('ws_output_replay')).items).toEqual([]);
    },
  );

  it.each([{ id: '../file_invalid' }, { purpose: 'agent' as const }, { scopeId: null }])(
    'limits explicit IDs to session outputs: %j',
    async (overrides) => {
      await expect(new LocalFileStore(backing()).create(input(overrides))).rejects.toThrow(
        'valid session output identity',
      );
    },
  );
});
