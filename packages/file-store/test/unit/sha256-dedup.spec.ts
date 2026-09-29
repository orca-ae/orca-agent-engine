// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Readable } from 'node:stream';
import { describe, expect, it, vi } from 'vitest';
import { InMemoryBlobStore } from '../../src/blob/in-memory.js';
import { LocalFileStore } from '../../src/file-store.js';
import { InMemoryFileMetadataStore } from '../../src/metadata/in-memory.js';

describe('LocalFileStore (unit)', () => {
  it('does not treat a non-unique insert failure as a dedup race', async () => {
    const metadata = new InMemoryFileMetadataStore();
    const findBySha = vi.spyOn(metadata, 'findBySha').mockResolvedValue(null);
    vi.spyOn(metadata, 'insert').mockRejectedValue(
      Object.assign(new Error('database unavailable'), { code: '08006' }),
    );
    const store = new LocalFileStore({
      blobStore: new InMemoryBlobStore(),
      metadataStore: metadata,
    });

    await expect(
      store.create({
        workspaceId: 'ws_1',
        filename: 'upload.txt',
        mimeType: 'text/plain',
        content: Readable.from('bytes'),
      }),
    ).rejects.toThrow('database unavailable');
    expect(findBySha).toHaveBeenCalledTimes(1);
  });
});
