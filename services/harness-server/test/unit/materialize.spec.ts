// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { isAgentEventId } from '@orca/agent-event-contract';
import { materializeResources } from '../../src/sandbox/materialize.js';
import type {
  MountHandle,
  MountResource,
  MountStrategy,
  TornDownState,
} from '../../src/sandbox/mounts/mount-strategy.js';
import type {
  SandboxFiles,
  SandboxHandle,
  ToolCall,
  ToolResult,
} from '../../src/sandbox/sandbox-runtime.js';

describe('materializeResources', () => {
  it('assigns an explicit canonical envelope ID to resource-mounted events', async () => {
    const result = await materializeResources({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      sandbox: new FakeSandboxHandle(),
      resources: [
        {
          id: 'sesrsc_1',
          type: 'file',
          file_id: 'file_1',
          memory_store_id: null,
          repo_ref: null,
          mount_path: '/mnt/input.txt',
          access: 'read_only',
          instructions: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
          mount_strategy: null,
        },
      ],
      chooseStrategy: () => new RecordingMountStrategy(),
    });

    expect(result.events).toHaveLength(1);
    expect(isAgentEventId(result.events[0]?.id)).toBe(true);
    expect(result.events[0]).toMatchObject({ kind: 'session.resource_mounted', subpath: '' });
  });

  it('rejects when a requested file resource cannot be mounted', async () => {
    const sandbox = new FakeSandboxHandle();
    const strategy = new FailingMountStrategy();

    await expect(
      materializeResources({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        sandbox,
        resources: [
          {
            id: 'sesrsc_1',
            type: 'file',
            file_id: 'file_missing',
            memory_store_id: null,
            repo_ref: null,
            mount_path: '/mnt/missing.txt',
            access: 'read_only',
            instructions: null,
            attached_at: new Date().toISOString(),
            detached_at: null,
            mount_strategy: null,
          },
        ],
        chooseStrategy: () => strategy,
      }),
    ).rejects.toThrow(/forced mount failure/);
  });

  it.each([
    '/workspace/skills',
    '/workspace/skills/',
    '/workspace/skills/untrusted.txt',
    '/workspace/project/../skills',
    '/workspace',
    '/',
  ])('rejects a file mount that intersects the reserved Skill root: %s', async (mountPath) => {
    const strategy = new RecordingMountStrategy();

    await expect(
      materializeResources({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        sandbox: new FakeSandboxHandle(),
        resources: [
          {
            id: 'sesrsc_reserved',
            type: 'file',
            file_id: 'file_untrusted',
            memory_store_id: null,
            repo_ref: null,
            mount_path: mountPath,
            access: 'read_only',
            instructions: null,
            attached_at: new Date().toISOString(),
            detached_at: null,
            mount_strategy: null,
          },
        ],
        chooseStrategy: () => strategy,
      }),
    ).rejects.toThrow(/overlaps reserved Skill root/);
    expect(strategy.activations).toBe(0);
  });

  it.each([
    new Map([['/mnt/aliased-input', '/workspace/skills/untrusted.txt']]),
    new Map([['/workspace/skills', '/mnt/shared']]),
  ])('rejects a canonical path alias before activating the resource', async (canonicalPaths) => {
    const strategy = new RecordingMountStrategy();
    const mountPath =
      canonicalPaths.get('/workspace/skills') === undefined
        ? '/mnt/aliased-input'
        : '/mnt/shared/input.txt';

    await expect(
      materializeResources({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        sandbox: new FakeSandboxHandle(canonicalPaths),
        resources: [
          {
            id: 'sesrsc_alias',
            type: 'file',
            file_id: 'file_untrusted',
            memory_store_id: null,
            repo_ref: null,
            mount_path: mountPath,
            access: 'read_only',
            instructions: null,
            attached_at: new Date().toISOString(),
            detached_at: null,
            mount_strategy: null,
          },
        ],
        chooseStrategy: () => strategy,
      }),
    ).rejects.toThrow(/resolves across reserved Skill root/);
    expect(strategy.activations).toBe(0);
  });
});

class FailingMountStrategy implements MountStrategy {
  readonly name = 'tarball_prefetch';
  readonly supports = ['file'] as const;

  async activate(_sandbox: SandboxHandle, _resource: MountResource): Promise<MountHandle> {
    throw new Error('forced mount failure');
  }

  async deactivate(_sandbox: SandboxHandle, _handle: MountHandle): Promise<void> {}

  async teardownForSnapshot(_sandbox: SandboxHandle, handle: MountHandle): Promise<TornDownState> {
    return {
      resourceId: handle.resourceId,
      resourceType: handle.resourceType,
      mountPath: handle.mountPath,
    };
  }

  async restoreAfterSnapshot(_sandbox: SandboxHandle, torn: TornDownState): Promise<MountHandle> {
    return {
      id: 'mount_restored',
      resourceId: torn.resourceId,
      resourceType: torn.resourceType,
      mountPath: torn.mountPath,
    };
  }
}

class RecordingMountStrategy extends FailingMountStrategy {
  activations = 0;

  override async activate(_sandbox: SandboxHandle, _resource: MountResource): Promise<MountHandle> {
    this.activations += 1;
    return {
      id: 'mount_recording',
      resourceId: _resource.id,
      resourceType: _resource.type,
      mountPath: _resource.mountPath,
    };
  }
}

class FakeSandboxHandle implements SandboxHandle {
  readonly id = 'sbx_fake';
  readonly files: SandboxFiles = {
    async write(): Promise<void> {},
    async read(): Promise<Buffer> {
      return Buffer.from('');
    },
    async readUtf8Page() {
      throw new Error('files.readUtf8Page not used');
    },
    async list(): Promise<string[]> {
      return [];
    },
    async chmod(): Promise<void> {},
    async delete(): Promise<void> {},
  };

  constructor(private readonly canonicalPaths = new Map<string, string>()) {}

  async run(_call: ToolCall): Promise<ToolResult> {
    return { stdout: '', stderr: '', exit_code: 0 };
  }

  async canonicalizePathForPolicy(path: string): Promise<string> {
    return this.canonicalPaths.get(path) ?? path;
  }

  async runPrivileged(_cmd: string): Promise<ToolResult> {
    return { stdout: '', stderr: '', exit_code: 0 };
  }

  async pause(): Promise<void> {}

  async resume(): Promise<void> {}

  async destroy(): Promise<void> {}
}
