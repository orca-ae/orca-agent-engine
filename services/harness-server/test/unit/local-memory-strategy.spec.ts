// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import type {
  SandboxFiles,
  SandboxHandle,
  ToolCall,
  ToolResult,
} from '../../src/sandbox/sandbox-runtime.js';
import { LocalMemoryStrategy } from '../../src/sandbox/mounts/local-memory.js';
import type { MountResource } from '../../src/sandbox/mounts/mount-strategy.js';

class FakeSandboxHandle implements SandboxHandle {
  readonly id = 'sbx_fake';
  readonly fileWrites: string[] = [];
  readonly fileDeletes: string[] = [];
  readonly files: SandboxFiles = {
    write: async (path) => {
      this.fileWrites.push(path);
    },
    async read() {
      throw new Error('files.read not used');
    },
    async readUtf8Page() {
      throw new Error('files.readUtf8Page not used');
    },
    async list() {
      return [];
    },
    async chmod() {},
    delete: async (path) => {
      this.fileDeletes.push(path);
    },
  };

  async run(_call: ToolCall): Promise<ToolResult> {
    throw new Error('run not used');
  }

  async runPrivileged(): Promise<ToolResult> {
    throw new Error('runPrivileged not used');
  }

  async pause(): Promise<void> {}
  async resume(): Promise<void> {}
  async destroy(): Promise<void> {}
}

function memoryResource(mountPath: string): MountResource {
  return {
    id: 'sr_memory',
    type: 'memory_store',
    memoryStoreId: 'mem_test',
    storeName: 'notes',
    mountPath,
    access: 'read_write',
  };
}

describe('LocalMemoryStrategy', () => {
  it('creates an empty memory root through the Files API before returning', async () => {
    const sandbox = new FakeSandboxHandle();
    const strategy = new LocalMemoryStrategy();

    const handle = await strategy.activate(sandbox, memoryResource('/mnt/memory/notes/'));

    expect(sandbox.fileWrites).toEqual(['/mnt/memory/notes/.orca-memory-mount']);
    expect(sandbox.fileDeletes).toEqual(['/mnt/memory/notes/.orca-memory-mount']);
    expect(handle.mountPath).toBe('/mnt/memory/notes/');
  });
});
