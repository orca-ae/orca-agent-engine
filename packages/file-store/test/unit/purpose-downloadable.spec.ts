// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { Readable } from 'node:stream';
import { LocalFileStore } from '../../src/file-store.js';
import { InMemoryBlobStore } from '../../src/blob/in-memory.js';
import { InMemoryFileMetadataStore } from '../../src/metadata/in-memory.js';
import { defaultDownloadable } from '../../src/types.js';

function newStore(): LocalFileStore {
  return new LocalFileStore({
    blobStore: new InMemoryBlobStore(),
    metadataStore: new InMemoryFileMetadataStore(),
  });
}

describe('LocalFileStore.create — purpose / downloadable defaults', () => {
  it('defaults purpose to "agent" and downloadable to false', async () => {
    const store = newStore();
    const created = await store.create({
      workspaceId: 'ws_1',
      filename: 'upload.txt',
      mimeType: 'text/plain',
      content: Readable.from(Buffer.from('user upload')),
    });
    expect(created.purpose).toBe('agent');
    expect(created.downloadable).toBe(false);
    expect(created.scopeId).toBeNull();
  });

  it('purpose=agent_output flips downloadable to true via defaultDownloadable', async () => {
    const store = newStore();
    const created = await store.create({
      workspaceId: 'ws_1',
      filename: 'output.json',
      mimeType: 'application/json',
      content: Readable.from(Buffer.from('{"agent":"output"}')),
      purpose: 'agent_output',
      scopeId: 'ses_x',
    });
    expect(created.purpose).toBe('agent_output');
    expect(created.downloadable).toBe(true);
    expect(created.scopeId).toBe('ses_x');
  });

  it('explicit downloadable=false on agent_output wins over the default', async () => {
    const store = newStore();
    const created = await store.create({
      workspaceId: 'ws_1',
      filename: 'gated-output.bin',
      mimeType: 'application/octet-stream',
      content: Readable.from(Buffer.from('gated bytes')),
      purpose: 'agent_output',
      downloadable: false,
      scopeId: 'ses_y',
    });
    expect(created.purpose).toBe('agent_output');
    expect(created.downloadable).toBe(false);
    expect(created.scopeId).toBe('ses_y');
  });

  it('defaultDownloadable mirrors Anthropic contract', () => {
    expect(defaultDownloadable('agent')).toBe(false);
    expect(defaultDownloadable('agent_output')).toBe(true);
  });

  it('two agent_output creates with the same content do NOT dedup', async () => {
    const store = newStore();
    const bytes = Buffer.from('identical session output bytes');
    const a = await store.create({
      workspaceId: 'ws_1',
      filename: 'out.json',
      mimeType: 'application/json',
      content: Readable.from(bytes),
      purpose: 'agent_output',
      scopeId: 'ses_a',
    });
    const b = await store.create({
      workspaceId: 'ws_1',
      filename: 'out.json',
      mimeType: 'application/json',
      content: Readable.from(bytes),
      purpose: 'agent_output',
      scopeId: 'ses_b',
    });
    // Distinct file rows even though sha256 matches…
    expect(a.id).not.toBe(b.id);
    expect(a.sha256).toBe(b.sha256);
    // …both `agent_output`, both downloadable=true, scoped to their own session.
    expect(a.purpose).toBe('agent_output');
    expect(b.purpose).toBe('agent_output');
    expect(a.scopeId).toBe('ses_a');
    expect(b.scopeId).toBe('ses_b');
    expect(a.downloadable).toBe(true);
    expect(b.downloadable).toBe(true);
  });

  it('agent then agent_output with same content yields two records (no cross-purpose dedup)', async () => {
    const store = newStore();
    const bytes = Buffer.from('shared bytes');
    const upload = await store.create({
      workspaceId: 'ws_1',
      filename: 'shared.txt',
      mimeType: 'text/plain',
      content: Readable.from(bytes),
      // purpose defaults to 'agent'
    });
    const output = await store.create({
      workspaceId: 'ws_1',
      filename: 'shared.txt',
      mimeType: 'text/plain',
      content: Readable.from(bytes),
      purpose: 'agent_output',
      scopeId: 'ses_z',
    });
    // The agent_output create did NOT collapse onto the prior agent record.
    expect(output.id).not.toBe(upload.id);
    expect(output.sha256).toBe(upload.sha256);
    expect(upload.purpose).toBe('agent');
    expect(output.purpose).toBe('agent_output');
    expect(upload.scopeId).toBeNull();
    expect(output.scopeId).toBe('ses_z');
    expect(upload.downloadable).toBe(false);
    expect(output.downloadable).toBe(true);
  });

  it('two agent uploads with same content DO dedup (within-purpose)', async () => {
    const store = newStore();
    const bytes = Buffer.from('user upload bytes');
    const first = await store.create({
      workspaceId: 'ws_1',
      filename: 'first.txt',
      mimeType: 'text/plain',
      content: Readable.from(bytes),
    });
    const second = await store.create({
      workspaceId: 'ws_1',
      filename: 'second-name.txt',
      mimeType: 'text/plain',
      content: Readable.from(bytes),
    });
    // Within purpose='agent', dedup still kicks in.
    expect(second.id).toBe(first.id);
    expect(second.purpose).toBe('agent');
  });
});
