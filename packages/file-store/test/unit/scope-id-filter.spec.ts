// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { Readable } from 'node:stream';
import { LocalFileStore } from '../../src/file-store.js';
import { InMemoryBlobStore } from '../../src/blob/in-memory.js';
import { InMemoryFileMetadataStore } from '../../src/metadata/in-memory.js';

function newStore(): LocalFileStore {
  return new LocalFileStore({
    blobStore: new InMemoryBlobStore(),
    metadataStore: new InMemoryFileMetadataStore(),
  });
}

describe('LocalFileStore.list — scopeId filter', () => {
  it('filters to the matching scope only and returns all rows when omitted', async () => {
    const store = newStore();
    const ws = 'ws_scope';

    const a = await store.create({
      workspaceId: ws,
      filename: 'a.txt',
      mimeType: 'text/plain',
      content: Readable.from(Buffer.from('aaa')),
      purpose: 'agent_output',
      scopeId: 'ses_x',
    });
    const b = await store.create({
      workspaceId: ws,
      filename: 'b.txt',
      mimeType: 'text/plain',
      content: Readable.from(Buffer.from('bbb')),
      purpose: 'agent_output',
      scopeId: 'ses_x',
    });
    const c = await store.create({
      workspaceId: ws,
      filename: 'c.txt',
      mimeType: 'text/plain',
      content: Readable.from(Buffer.from('ccc')),
      // No scopeId — defaults to null.
    });

    const scoped = await store.list(ws, { scopeId: 'ses_x' });
    const scopedIds = scoped.items.map((f) => f.id).sort();
    expect(scopedIds).toEqual([a.id, b.id].sort());

    const all = await store.list(ws);
    const allIds = all.items.map((f) => f.id).sort();
    expect(allIds).toEqual([a.id, b.id, c.id].sort());

    const noMatch = await store.list(ws, { scopeId: 'ses_other' });
    expect(noMatch.items).toEqual([]);
  });

  it('combines scope and purpose filters before pagination', async () => {
    const store = newStore();
    const ws = 'ws_scope_purpose';
    const output = await store.create({
      workspaceId: ws,
      filename: 'output.txt',
      mimeType: 'text/plain',
      content: Readable.from(Buffer.from('output')),
      purpose: 'agent_output',
      scopeId: 'ses_x',
    });
    await store.create({
      workspaceId: ws,
      filename: 'input.txt',
      mimeType: 'text/plain',
      content: Readable.from(Buffer.from('input')),
      purpose: 'agent',
      scopeId: 'ses_x',
    });

    const page = await store.list(ws, {
      limit: 1,
      scopeId: 'ses_x',
      purpose: 'agent_output',
    });

    expect(page.items.map((file) => file.id)).toEqual([output.id]);
    expect(page.nextCursor).toBeNull();
  });

  it('returns the adjacent previous page with beforeCursor', async () => {
    const store = newStore();
    const ws = 'ws_before_cursor';
    const created = await Promise.all(
      ['a.txt', 'b.txt', 'c.txt', 'd.txt'].map((name) =>
        store.create({
          workspaceId: ws,
          filename: name,
          mimeType: 'text/plain',
          content: Readable.from(Buffer.from(name)),
        }),
      ),
    );

    const orderedIds = created.map((file) => file.id).sort((a, b) => (a < b ? 1 : a > b ? -1 : 0));

    const pageBeforeOldest = await store.list(ws, {
      limit: 2,
      beforeCursor: orderedIds[3],
    });

    expect(pageBeforeOldest.items.map((file) => file.id)).toEqual(orderedIds.slice(1, 3));
    expect(pageBeforeOldest.nextCursor).toBe(orderedIds[1]);
  });
});
