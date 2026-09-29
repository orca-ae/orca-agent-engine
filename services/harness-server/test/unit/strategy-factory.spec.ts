// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import type { FileStore, FileRecord } from '@orca/file-store';
import type { SessionS3Creds } from '../../src/auth/sts-creds.js';
import type {
  EnvironmentSpec,
  SandboxCapabilities,
  SandboxHandle,
  SandboxRuntime,
} from '../../src/sandbox/sandbox-runtime.js';
import type { MountResource } from '../../src/sandbox/mounts/mount-strategy.js';
import { LocalMemoryStrategy } from '../../src/sandbox/mounts/local-memory.js';
import { MemoryFuseStrategy } from '../../src/sandbox/mounts/memory-fuse.js';
import { TarballPrefetchStrategy } from '../../src/sandbox/mounts/tarball-prefetch.js';
import { pickStrategy, type PickStrategyInput } from '../../src/sandbox/mounts/strategy-factory.js';

class FakeRuntime implements SandboxRuntime {
  constructor(public readonly capabilities: SandboxCapabilities) {}
  async acquire(env: EnvironmentSpec): Promise<SandboxHandle> {
    void env;
    throw new Error('acquire not used in factory tests');
  }
}

function fakeFileStore(): FileStore {
  return {
    async create() {
      throw new Error('not used');
    },
    async get(ws, id) {
      void ws;
      void id;
      return null as unknown as FileRecord;
    },
    async list() {
      return { items: [], nextCursor: null };
    },
    async open() {
      return null;
    },
    async archive() {
      /* no-op */
    },
    async delete() {
      /* no-op */
    },
    async close() {
      /* no-op */
    },
  };
}

function fakeResource(): MountResource {
  return {
    id: 'sesrsc_factory',
    type: 'file',
    fileId: 'file_factory',
    mountPath: '/mnt/x.txt',
    access: 'read_only',
  };
}

function fakeMemoryResource(): MountResource {
  return {
    id: 'sesrsc_factory_mem',
    type: 'memory_store',
    memoryStoreId: 'mems_factory',
    storeName: 'prefs',
    mountPath: '/mnt/memory/prefs/',
    access: 'read_write',
  };
}

function fakeCreds(): SessionS3Creds {
  return {
    accessKeyId: 'AKIA',
    secretAccessKey: 'secret',
    sessionToken: 'token',
    expiresAt: 9_999_999_999,
  };
}

function baseInput(overrides?: Partial<PickStrategyInput>): PickStrategyInput {
  return {
    resource: fakeResource(),
    overrideStrategy: null,
    runtime: new FakeRuntime({ supportsFuse: false, supportsLocalMemory: true }),
    workspaceId: 'ws_factory',
    sessionId: 'ses_factory',
    fileStore: fakeFileStore(),
    ...overrides,
  };
}

describe('pickStrategy', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('throws when the removed override=s3_fuse is used against a file resource', () => {
    // 's3_fuse' was removed from MountStrategyName (multi-workspace
    // isolation): sandbox creds never cover the file-blob namespace, so a
    // stale registry value must throw instead of silently mounting.
    expect(() =>
      pickStrategy(
        baseInput({
          overrideStrategy: 's3_fuse' as never,
          runtime: new FakeRuntime({ supportsFuse: true }),
          creds: fakeCreds(),
          bucket: 'orca-files',
          endpoint: 'http://minio:9000',
        }),
      ),
    ).toThrow(/mount_strategy=s3_fuse is invalid for resource\.type=file/);
  });

  it('returns TarballPrefetchStrategy when override=tarball_prefetch even on FUSE runtime', () => {
    const strategy = pickStrategy(
      baseInput({
        overrideStrategy: 'tarball_prefetch',
        runtime: new FakeRuntime({ supportsFuse: true }),
      }),
    );
    expect(strategy).toBeInstanceOf(TarballPrefetchStrategy);
    expect(strategy.name).toBe('tarball_prefetch');
  });

  it('auto-picks TarballPrefetchStrategy even when supportsFuse=true and FUSE wiring is present', () => {
    // No FUSE-based file path exists — auto-pick must never prefer anything
    // over tarball_prefetch, regardless of runtime capabilities or wiring.
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const strategy = pickStrategy(
      baseInput({
        overrideStrategy: null,
        runtime: new FakeRuntime({ supportsFuse: true }),
        creds: fakeCreds(),
        bucket: 'orca-files',
        endpoint: 'http://minio:9000',
      }),
    );
    expect(strategy).toBeInstanceOf(TarballPrefetchStrategy);
    expect(strategy.name).toBe('tarball_prefetch');
    expect(warn).not.toHaveBeenCalled();
  });

  it('returns TarballPrefetchStrategy without warning when supportsFuse=false and no override', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const strategy = pickStrategy(
      baseInput({
        overrideStrategy: null,
        runtime: new FakeRuntime({ supportsFuse: false }),
      }),
    );
    expect(strategy).toBeInstanceOf(TarballPrefetchStrategy);
    expect(warn).not.toHaveBeenCalled();
  });

  it('throws when override=memory_fuse is used against a file resource', () => {
    expect(() =>
      pickStrategy(
        baseInput({
          overrideStrategy: 'memory_fuse',
          runtime: new FakeRuntime({ supportsFuse: true }),
        }),
      ),
    ).toThrow(/invalid for resource\.type=file/);
  });
});

describe('pickStrategy (memory_store branch)', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('returns MemoryFuseStrategy when override=memory_fuse and runtime supportsFuse=true', () => {
    const strategy = pickStrategy(
      baseInput({
        resource: fakeMemoryResource(),
        overrideStrategy: 'memory_fuse',
        runtime: new FakeRuntime({ supportsFuse: true }),
        creds: fakeCreds(),
        bucket: 'orca-files',
        endpoint: 'http://minio:9000',
        memoryKeyPrefix: 'memory/',
      }),
    );
    expect(strategy).toBeInstanceOf(MemoryFuseStrategy);
    expect(strategy.name).toBe('memory_fuse');
  });

  it('throws when override=memory_fuse but runtime supportsFuse=false', () => {
    expect(() =>
      pickStrategy(
        baseInput({
          resource: fakeMemoryResource(),
          overrideStrategy: 'memory_fuse',
          runtime: new FakeRuntime({ supportsFuse: false }),
          creds: fakeCreds(),
          bucket: 'orca-files',
          endpoint: 'http://minio:9000',
          memoryKeyPrefix: 'memory/',
        }),
      ),
    ).toThrow(/supportsFuse=false/);
  });

  it('returns LocalMemoryStrategy when override=local_memory regardless of FUSE support', () => {
    const strategy = pickStrategy({
      resource: fakeMemoryResource(),
      overrideStrategy: 'local_memory',
      runtime: new FakeRuntime({ supportsFuse: true, supportsLocalMemory: true }),
      workspaceId: 'ws_factory',
      sessionId: 'ses_factory',
    });
    expect(strategy).toBeInstanceOf(LocalMemoryStrategy);
    expect(strategy.name).toBe('local_memory');
  });

  it('auto-picks MemoryFuseStrategy when supportsFuse=true and full memory_fuse config is present', () => {
    const strategy = pickStrategy(
      baseInput({
        resource: fakeMemoryResource(),
        overrideStrategy: null,
        runtime: new FakeRuntime({ supportsFuse: true }),
        creds: fakeCreds(),
        bucket: 'orca-files',
        endpoint: 'http://minio:9000',
        memoryKeyPrefix: 'memory/',
        forcePathStyle: false,
      }),
    );
    expect(strategy).toBeInstanceOf(MemoryFuseStrategy);
    expect(
      (strategy as unknown as { config: { forcePathStyle?: boolean } }).config.forcePathStyle,
    ).toBe(false);
  });

  it('falls back to LocalMemoryStrategy with a warn when supportsFuse=true but memory_fuse config is missing', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const strategy = pickStrategy(
      baseInput({
        resource: fakeMemoryResource(),
        overrideStrategy: null,
        runtime: new FakeRuntime({ supportsFuse: true, supportsLocalMemory: true }),
        // creds + bucket + endpoint + memoryKeyPrefix intentionally omitted
      }),
    );
    expect(strategy).toBeInstanceOf(LocalMemoryStrategy);
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0]![0]).toMatch(/memory_fuse not configured/);
  });

  it('returns LocalMemoryStrategy without warning when supportsFuse=false and no override', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const strategy = pickStrategy(
      baseInput({
        resource: fakeMemoryResource(),
        overrideStrategy: null,
        runtime: new FakeRuntime({ supportsFuse: false, supportsLocalMemory: true }),
      }),
    );
    expect(strategy).toBeInstanceOf(LocalMemoryStrategy);
    expect(warn).not.toHaveBeenCalled();
  });

  it('throws when override=memory_fuse but memory_fuse wiring (e.g. memoryKeyPrefix) is missing', () => {
    expect(() =>
      pickStrategy(
        baseInput({
          resource: fakeMemoryResource(),
          overrideStrategy: 'memory_fuse',
          runtime: new FakeRuntime({ supportsFuse: true }),
          // bucket/endpoint/memoryKeyPrefix/creds intentionally omitted
        }),
      ),
    ).toThrow(/wiring is incomplete/);
  });

  it('throws when override=tarball_prefetch is used against a memory_store resource', () => {
    expect(() =>
      pickStrategy(
        baseInput({
          resource: fakeMemoryResource(),
          overrideStrategy: 'tarball_prefetch',
          runtime: new FakeRuntime({ supportsFuse: false }),
        }),
      ),
    ).toThrow(/invalid for resource\.type=memory_store/);
  });

  it('refuses local_memory when runtime does not explicitly support it', () => {
    expect(() =>
      pickStrategy(
        baseInput({
          resource: fakeMemoryResource(),
          overrideStrategy: 'local_memory',
          runtime: new FakeRuntime({ supportsFuse: false }),
        }),
      ),
    ).toThrow(/supportsLocalMemory=false/);
  });

  it('refuses automatic local_memory fallback when runtime does not support it', () => {
    expect(() =>
      pickStrategy(
        baseInput({
          resource: fakeMemoryResource(),
          overrideStrategy: null,
          runtime: new FakeRuntime({ supportsFuse: false }),
        }),
      ),
    ).toThrow(/supportsLocalMemory=false/);
  });
});
