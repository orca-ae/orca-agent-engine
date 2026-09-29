// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Readable } from 'node:stream';
import { createHash } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import { InMemoryBlobStore, InMemoryFileMetadataStore, LocalFileStore } from '@orca/file-store';
import {
  InMemoryMemoryBlobStore,
  InMemoryMemoryMetadataStore,
  LocalMemoryStore,
} from '@orca/memory-store';
import type { PreparedExecutionV2 } from '../../src/contracts/internal.contract.js';
import {
  prepareRunnerResources,
  runnerResourceRevision,
  runnerToolNetworkDomains,
  runnerToolNetworkUnrestricted,
} from '../../src/domain/runner-resources.js';

const workspaceId = 'ws_runner_resources';
const hash = (value: string) => createHash('sha256').update(value).digest('hex');
function prepared(resources: PreparedExecutionV2['resources']): PreparedExecutionV2 {
  return {
    workspace_id: workspaceId,
    session: { id: 'ses_resources' },
    resources,
    environment: { networking: { type: 'limited', allowed_hosts: ['example.com'] } },
  } as unknown as PreparedExecutionV2;
}
const common = {
  id: 'sesrsc_memory',
  file_id: null,
  memory_store_id: 'mems_test',
  repo_ref: null,
  mount_path: '/mnt/memory/',
  access: 'read_write' as const,
  mount_strategy: null,
  instructions: null,
  attached_at: new Date(0).toISOString(),
  detached_at: null,
};

describe('Registry resource manifest preparation', () => {
  it('maps File and Memory bytes to opaque binding ids without storage credentials', async () => {
    const fileStore = new LocalFileStore({
      blobStore: new InMemoryBlobStore(),
      metadataStore: new InMemoryFileMetadataStore(),
    });
    const memoryStore = new LocalMemoryStore({
      blobStore: new InMemoryMemoryBlobStore(),
      metadataStore: new InMemoryMemoryMetadataStore(),
    });
    const file = await fileStore.create({
      workspaceId,
      filename: 'input.txt',
      mimeType: 'text/plain',
      content: Readable.from('file'),
    });
    const memory = await memoryStore.createStore({ workspaceId, name: 'notes' });
    await memoryStore.writeMemory({
      workspaceId,
      storeId: memory.id,
      path: 'notes.txt',
      content: Readable.from('memory'),
      sizeBytes: 6,
      sha256: hash('memory'),
    });
    const snapshot = prepared([
      {
        ...common,
        type: 'file',
        id: 'sesrsc_file',
        memory_store_id: null,
        file_id: file.id,
        mount_path: '/mnt/input.txt',
        access: 'read_only',
        file: {
          filename: file.filename,
          mime_type: file.mimeType,
          size_bytes: file.sizeBytes,
          sha256: file.sha256,
          purpose: file.purpose,
        },
      },
      {
        ...common,
        type: 'memory_store',
        memory_store_id: memory.id,
        memory_store: { id: memory.id, workspace_id: workspaceId, name: memory.name },
      },
    ]);
    const resources = await prepareRunnerResources(snapshot, {
      fileStore,
      memoryStore,
      gitSnapshot: vi.fn(),
    });
    expect(resources.manifest.resources.map((item) => item.mount_path)).toEqual([
      '/mnt/input.txt',
      '/mnt/memory',
    ]);
    expect(resources.memoryStores.get('sesrsc_memory')).toBe(memory.id);
    const wire = JSON.stringify(resources.manifest);
    expect(wire).not.toContain(memory.id);
    expect(wire).not.toContain(file.id);
    let content = '';
    for await (const bytes of await resources.open('sesrsc_file', '')) content += String(bytes);
    expect(content).toBe('file');
    await expect(resources.open('sesrsc_foreign', 'notes.txt')).rejects.toThrow('undeclared');
    await resources.close();
  });

  it('pins binding/network authority independently of mutable Memory content', () => {
    const resource = {
      ...common,
      type: 'memory_store' as const,
      memory_store: { id: 'mems_test', workspace_id: workspaceId, name: 'notes' },
    };
    const base = prepared([resource]);
    const revision = runnerResourceRevision(base);
    expect(
      runnerResourceRevision(
        prepared([{ ...resource, memory_store: { ...resource.memory_store, name: 'renamed' } }]),
      ),
    ).toBe(revision);
    expect(runnerResourceRevision(prepared([{ ...resource, access: 'read_only' }]))).not.toBe(
      revision,
    );
    expect(
      runnerResourceRevision(prepared([{ ...resource, memory_store_id: 'mems_other' }])),
    ).not.toBe(revision);
    const changed = structuredClone(base);
    changed.environment!.networking.allowed_hosts = ['different.example'];
    expect(runnerResourceRevision(changed)).not.toBe(revision);
  });

  it('grants Git proxy access without implicitly granting the upstream repository host', () => {
    const snapshot = prepared([
      {
        ...common,
        type: 'github_repository',
        memory_store_id: null,
        mount_path: '/mnt/repo',
        repo_ref: { url: 'https://upstream.example/repo.git', git_credential_id: 'gitcred_test' },
      },
    ]);
    expect(runnerToolNetworkDomains(snapshot, ['registry.internal'])).toEqual([
      'example.com',
      'registry.internal',
    ]);
    const originalRevision = runnerResourceRevision(snapshot, ['registry.internal']);
    snapshot.environment!.networking.allowed_hosts = ['upstream.example'];
    expect(runnerToolNetworkDomains(snapshot, ['registry.internal'])).toEqual([
      'registry.internal',
      'upstream.example',
    ]);
    expect(runnerResourceRevision(snapshot, ['registry.internal'])).not.toBe(originalRevision);
    expect(runnerToolNetworkDomains(prepared([]), ['registry.internal'])).toEqual(['example.com']);
  });

  it('closes trusted Git snapshots on validation failure and rejects wildcard network grants', async () => {
    const close = vi.fn(async () => {});
    const gitSnapshot = vi.fn(async () => ({ files: [], open: () => Readable.from(''), close }));
    const resource = {
      ...common,
      type: 'github_repository' as const,
      memory_store_id: null,
      mount_path: '/workspace/skills',
      repo_ref: {
        url: 'https://github.com/example/private.git',
        git_credential_id: 'gitcred_test',
      },
    };
    const fileStore = {} as LocalFileStore;
    await expect(
      prepareRunnerResources(prepared([resource]), { fileStore, gitSnapshot }),
    ).rejects.toThrow('mount path');
    expect(close).toHaveBeenCalledOnce();
    const invalid = prepared([]);
    invalid.environment!.networking.allowed_hosts = ['*'];
    expect(() => runnerResourceRevision(invalid)).toThrow('domain');
  });
});

it.each(['file', 'memory_store'] as const)(
  'closes an opened %s stream when metadata changes before delivery',
  async (kind) => {
    const stream = Readable.from('new content');
    const opened = { stream, sha256: hash('new content'), sizeBytes: 11 };
    const fileStore = { open: async () => opened } as unknown as LocalFileStore;
    const memoryStore = {
      listMemories: async () => [
        { id: 'mem_original', path: 'note.txt', currentSha256: hash('old'), sizeBytes: 3 },
      ],
      openMemory: async () => opened,
    } as unknown as LocalMemoryStore;
    const binding =
      kind === 'file'
        ? {
            ...common,
            type: 'file' as const,
            file_id: 'file_test',
            memory_store_id: null,
            access: 'read_only' as const,
            mount_path: '/mnt/file.txt',
            file: {
              filename: 'file.txt',
              mime_type: 'text/plain',
              size_bytes: 3,
              sha256: hash('old'),
              purpose: 'agent' as const,
            },
          }
        : {
            ...common,
            type: 'memory_store' as const,
            memory_store: { id: common.memory_store_id, workspace_id: workspaceId, name: 'notes' },
          };
    const resources = await prepareRunnerResources(prepared([binding]), {
      fileStore,
      memoryStore,
      gitSnapshot: vi.fn(),
    });
    try {
      await expect(resources.open(common.id, kind === 'file' ? '' : 'note.txt')).rejects.toThrow(
        'changed during resource preparation',
      );
      expect(stream.destroyed).toBe(true);
    } finally {
      await resources.close();
    }
  },
);

it('refreshes every runner Git grant after all slow snapshots and cleans up if refresh fails', async () => {
  const resources = [1, 2].map((id) => ({
    ...common,
    id: `sesrsc_git${id}`,
    type: 'github_repository' as const,
    memory_store_id: null,
    mount_path: `/mnt/repo${id}`,
    repo_ref: { url: `https://upstream.example/repo${id}.git`, git_credential_id: 'gitcred_test' },
  }));
  let now = 0;
  const calls: string[] = [];
  const close = vi.fn(async () => {});
  const gitCapability = vi.fn(async (binding: (typeof resources)[number]) => {
    calls.push(`grant:${binding.id}`);
    return {
      resource_id: binding.id,
      remote_url: `https://registry.example/v1/git-proxy/${binding.id}`,
      authorization_header: 'Authorization: Bearer aaa.bbb.ccc',
      expires_at: (now + 900_000) / 1000,
    };
  });
  const gitSnapshot = vi.fn(async (binding: (typeof resources)[number]) => {
    calls.push(`clone:${binding.id}`);
    now += 600_000; // The first grant expires before the second snapshot finishes.
    return { files: [], open: () => Readable.from(''), close };
  });
  const result = await prepareRunnerResources(prepared(resources), {
    fileStore: {} as LocalFileStore,
    gitCapability,
    gitSnapshot,
  });
  // Resource transfer can take longer than a token lifetime. Mint only at send time.
  now += 1_200_000;
  const capabilities = await result.refreshGitCapabilities!();
  expect(capabilities).toHaveLength(2);
  for (const grant of capabilities) expect(grant.expires_at * 1000 - now).toBe(900_000);
  expect(calls.slice(-2)).toEqual(['grant:sesrsc_git1', 'grant:sesrsc_git2']);
  await result.close();
  expect(close).toHaveBeenCalledTimes(2);
  close.mockClear();
  // A final refresh failure closes both already-prepared temporary checkouts.
  gitCapability.mockReset();
  gitCapability.mockImplementation(async (binding) => {
    if (gitSnapshot.mock.calls.length >= 4) throw new Error('refresh unavailable');
    return {
      resource_id: binding.id,
      remote_url: 'https://registry.example/proxy',
      authorization_header: 'Authorization: Bearer aaa.bbb.ccc',
      expires_at: (now + 900_000) / 1000,
    };
  });
  const retry = await prepareRunnerResources(prepared(resources), {
    fileStore: {} as LocalFileStore,
    gitCapability,
    gitSnapshot,
  });
  await expect(retry.refreshGitCapabilities!()).rejects.toThrow('refresh unavailable');
  await retry.close();
  expect(close).toHaveBeenCalledTimes(2);
});

it('reuses committed Git descriptors without upstream access but refreshes grants and binding changes', async () => {
  const resource = {
    ...common,
    id: 'sesrsc_git',
    type: 'github_repository' as const,
    memory_store_id: null,
    mount_path: '/mnt/repo',
    repo_ref: { url: 'https://upstream.example/repo.git', git_credential_id: 'gitcred_test' },
  };
  const snapshot = prepared([resource]);
  const files = [{ path: 'README.md', sha256: hash('git'), size_bytes: 3, mode: 0o644 }];
  const gitSnapshot = vi.fn(async () => ({
    files,
    open: () => Readable.from('git'),
    close: vi.fn(async () => {}),
  }));
  const gitCapability = vi.fn(async () => ({
    resource_id: resource.id,
    remote_url: 'https://registry.example/v1/git-proxy/sesrsc_git',
    authorization_header: 'Authorization: Bearer a.b.c',
    expires_at: Math.floor(Date.now() / 1000) + 900,
  }));
  const stores = { fileStore: {} as LocalFileStore, gitSnapshot, gitCapability };
  const initial = await prepareRunnerResources(snapshot, stores);
  await initial.close();
  gitSnapshot.mockRejectedValue(new Error('upstream unavailable'));
  gitCapability.mockClear();
  const next = await prepareRunnerResources(snapshot, {
    ...stores,
    retainedManifest: initial.manifest,
  });
  expect(gitSnapshot).toHaveBeenCalledTimes(1);
  expect(gitCapability).not.toHaveBeenCalled();
  await next.refreshGitCapabilities!();
  expect(gitCapability).toHaveBeenCalledTimes(1);
  expect(next.retainedGitResourceIds).toEqual([resource.id]);
  expect(next.manifest).toEqual(initial.manifest);
  await next.close();
  await expect(prepareRunnerResources(snapshot, stores)).rejects.toThrow('upstream unavailable');
  await expect(
    prepareRunnerResources(
      prepared([
        { ...resource, repo_ref: { ...resource.repo_ref, checkout: { branch: 'other' } } },
      ]),
      { ...stores, retainedManifest: initial.manifest },
    ),
  ).rejects.toThrow('upstream unavailable');
});

it('preserves omitted and explicit unrestricted networking and pins the mode in the revision', () => {
  const snapshot = prepared([]);
  const limitedRevision = runnerResourceRevision(snapshot);
  expect(runnerToolNetworkUnrestricted(snapshot)).toBe(false);
  snapshot.environment!.networking = { type: 'unrestricted' };
  expect(runnerToolNetworkUnrestricted(snapshot)).toBe(true);
  expect(runnerResourceRevision(snapshot)).not.toBe(limitedRevision);
  snapshot.environment!.networking = {};
  expect(runnerToolNetworkUnrestricted(snapshot)).toBe(true);
});
