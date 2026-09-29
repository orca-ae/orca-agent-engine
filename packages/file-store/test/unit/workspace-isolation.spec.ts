// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Readable } from 'node:stream';
import { describe, expect, it } from 'vitest';
import { InMemoryBlobStore } from '../../src/blob/in-memory.js';
import { LocalFileStore } from '../../src/file-store.js';
import { InMemoryFileMetadataStore } from '../../src/metadata/in-memory.js';

describe('LocalFileStore workspace isolation', () => {
  it('does not dedup identical content across workspaces', async () => {
    const blobs = new InMemoryBlobStore();
    const store = new LocalFileStore({
      blobStore: blobs,
      metadataStore: new InMemoryFileMetadataStore(),
    });
    const content = Buffer.from('identical bytes');

    const alpha = await store.create({
      workspaceId: 'ws_alpha',
      filename: 'same.txt',
      mimeType: 'text/plain',
      content: Readable.from(content),
    });
    const beta = await store.create({
      workspaceId: 'ws_beta',
      filename: 'same.txt',
      mimeType: 'text/plain',
      content: Readable.from(content),
    });

    expect(alpha.sha256).toBe(beta.sha256);
    expect(alpha.id).not.toBe(beta.id);
    expect(blobs.size()).toBe(2);
  });

  it('does not resolve another workspace file id or blob', async () => {
    const blobs = new InMemoryBlobStore();
    const store = new LocalFileStore({
      blobStore: blobs,
      metadataStore: new InMemoryFileMetadataStore(),
    });
    const alpha = await store.create({
      workspaceId: 'ws_alpha',
      filename: 'secret.txt',
      mimeType: 'text/plain',
      content: Readable.from(Buffer.from('secret')),
    });

    await expect(store.open('ws_beta', alpha.id)).resolves.toBeNull();
    await expect(blobs.open('ws_beta', alpha.sha256)).rejects.toThrow(/not found/);
  });
});
