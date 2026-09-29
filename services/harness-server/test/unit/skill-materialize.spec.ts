// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import { InMemorySkillStore, type SkillBundle, type SkillStore } from '@orca/skill-store';
import type { SkillDescriptor } from '../../src/clients/registry.js';
import type {
  SandboxFiles,
  SandboxHandle,
  ToolCall,
  ToolResult,
} from '../../src/sandbox/sandbox-runtime.js';
import { materializeSkills, SKILLS_ROOT } from '../../src/sandbox/skills/materialize.js';

describe('Skill bundle materialization', () => {
  it('opens exact pins, writes verified files, and seals safe read-only modes', async () => {
    const store = new InMemorySkillStore();
    const files = [
      { path: 'SKILL.md', content: Buffer.from('Read references/guide.md') },
      { path: 'references/guide.md', content: Buffer.from('Guide') },
      { path: 'scripts/run.sh', content: Buffer.from('#!/bin/sh\ntrue\n'), mode: 0o755 },
    ];
    const first = await store.put('ws_one', 'sklv_one', files);
    const second = await store.put('ws_one', 'sklv_two', files);
    const open = vi.spyOn(store, 'open');
    const sandbox = new RecordingSandbox();

    await materializeSkills({
      workspaceId: 'ws_one',
      sandbox,
      skillStore: store,
      skills: [
        descriptor('sklv_one', 'first-skill', first),
        descriptor('sklv_two', 'second-skill', second),
        descriptor('sklv_one', 'first-skill', first),
      ],
    });

    expect(open).toHaveBeenCalledTimes(2);
    expect(open).toHaveBeenCalledWith('ws_one', 'sklv_one', first.sha256);
    expect(open).toHaveBeenCalledWith('ws_one', 'sklv_two', second.sha256);
    expect(sandbox.writes.get(`${SKILLS_ROOT}/first-skill/SKILL.md`)?.toString()).toBe(
      'Read references/guide.md',
    );
    expect(sandbox.modes.get(`${SKILLS_ROOT}/first-skill/SKILL.md`)).toBe(0o444);
    expect(sandbox.modes.get(`${SKILLS_ROOT}/first-skill/scripts/run.sh`)).toBe(0o555);
    expect(sandbox.modes.get(`${SKILLS_ROOT}/first-skill/scripts`)).toBe(0o555);
    expect(sandbox.modes.get(`${SKILLS_ROOT}/first-skill`)).toBe(0o555);
    expect(sandbox.modes.get(SKILLS_ROOT)).toBe(0o555);
    expect(sandbox.chmodManyCalls).toBe(3);
    expect(sandbox.chmodCalls).toBe(0);
  });

  it('removes image-preinstalled files before writing exact Skill pins', async () => {
    const store = new InMemorySkillStore();
    const record = await store.put('ws_one', 'sklv_clean', [
      { path: 'SKILL.md', content: Buffer.from('Trusted instructions') },
    ]);
    const sandbox = new RecordingSandbox();
    sandbox.writes.set(`${SKILLS_ROOT}/preinstalled/EXTRA.md`, Buffer.from('untrusted'));

    await materializeSkills({
      workspaceId: 'ws_one',
      sandbox,
      skillStore: store,
      skills: [descriptor('sklv_clean', 'clean-skill', record)],
    });

    expect(sandbox.deletedPaths).toEqual([SKILLS_ROOT]);
    expect(sandbox.writes.has(`${SKILLS_ROOT}/preinstalled/EXTRA.md`)).toBe(false);
    expect(sandbox.writes.get(`${SKILLS_ROOT}/clean-skill/SKILL.md`)?.toString()).toBe(
      'Trusted instructions',
    );
  });

  it('removes an image-preinstalled Skill tree even when the Session has no Skills', async () => {
    const sandbox = new RecordingSandbox();
    sandbox.writes.set(`${SKILLS_ROOT}/preinstalled/EXTRA.md`, Buffer.from('untrusted'));

    await materializeSkills({
      workspaceId: 'ws_one',
      sandbox,
      skillStore: undefined,
      skills: [],
    });

    expect(sandbox.deletedPaths).toEqual([SKILLS_ROOT]);
    expect(sandbox.writes.size).toBe(0);
  });

  it('rejects one effective name resolving to different immutable bundles', async () => {
    const store = new InMemorySkillStore();
    const first = await store.put('ws_one', 'sklv_one', [
      { path: 'SKILL.md', content: Buffer.from('one') },
    ]);
    const second = await store.put('ws_one', 'sklv_two', [
      { path: 'SKILL.md', content: Buffer.from('two') },
    ]);
    const open = vi.spyOn(store, 'open');

    await expect(
      materializeSkills({
        workspaceId: 'ws_one',
        sandbox: new RecordingSandbox(),
        skillStore: store,
        skills: [
          descriptor('sklv_one', 'same-name', first),
          descriptor('sklv_two', 'same-name', second),
        ],
      }),
    ).rejects.toThrow(/name collision/);
    expect(open).not.toHaveBeenCalled();
  });

  it('rejects unsafe paths and per-file digest drift before writing', async () => {
    const content = Buffer.from('body');
    const digest = sha256(content);
    const bundle: SkillBundle = {
      record: {
        sha256: 'a'.repeat(64),
        sizeBytes: 10,
        files: [
          {
            path: '../SKILL.md',
            sizeBytes: content.length,
            sha256: digest,
            mode: 0o644,
            mimeType: null,
          },
        ],
      },
      files: [
        {
          path: '../SKILL.md',
          sizeBytes: content.length,
          sha256: digest,
          mode: 0o644,
          mimeType: null,
          content,
        },
      ],
    };
    const store: SkillStore = {
      put: async () => bundle.record,
      open: async () => bundle,
      delete: async () => {},
      close: async () => {},
    };
    const sandbox = new RecordingSandbox();

    await expect(
      materializeSkills({
        workspaceId: 'ws_one',
        sandbox,
        skillStore: store,
        skills: [
          {
            ...descriptor('sklv_bad', 'bad-skill', bundle.record),
            package_sha256: bundle.record.sha256,
            package_size_bytes: bundle.record.sizeBytes,
          },
        ],
      }),
    ).rejects.toThrow(/path is unsafe/);
    expect(sandbox.writes.size).toBe(0);
  });

  it('opens, validates, and releases exact bundles sequentially', async () => {
    const store = new InMemorySkillStore();
    const records = await Promise.all(
      Array.from({ length: 12 }, async (_, index) => {
        return await store.put('ws_one', `sklv_${index}`, [
          { path: 'SKILL.md', content: Buffer.from(`skill ${index}`) },
        ]);
      }),
    );
    const sandbox = new RecordingSandbox();
    let openIndex = 0;
    const open = vi.spyOn(store, 'open').mockImplementation(async (...args) => {
      // Every previous bundle must already have been written before the next
      // one is opened; opening all pins up front would retain all bundle bytes.
      expect(sandbox.writeCalls).toHaveLength(openIndex);
      openIndex += 1;
      return await InMemorySkillStore.prototype.open.call(store, ...args);
    });

    await materializeSkills({
      workspaceId: 'ws_one',
      sandbox,
      skillStore: store,
      skills: records.map((record, index) => descriptor(`sklv_${index}`, `skill-${index}`, record)),
    });

    expect(open).toHaveBeenCalledTimes(records.length);
    expect(sandbox.writes.size).toBe(records.length);
  });

  it('validates duplicate-name exact pins but materializes an identical digest once', async () => {
    const store = new InMemorySkillStore();
    const files = [{ path: 'SKILL.md', content: Buffer.from('shared') }];
    const first = await store.put('ws_one', 'sklv_one', files);
    const second = await store.put('ws_one', 'sklv_two', files);
    const open = vi.spyOn(store, 'open');
    const sandbox = new RecordingSandbox();

    await materializeSkills({
      workspaceId: 'ws_one',
      sandbox,
      skillStore: store,
      skills: [
        descriptor('sklv_one', 'same-name', first),
        descriptor('sklv_two', 'same-name', second),
      ],
    });

    expect(open).toHaveBeenCalledTimes(2);
    expect(sandbox.writeCalls).toEqual([`${SKILLS_ROOT}/same-name/SKILL.md`]);
  });

  it('removes already-written Skill files if a later exact bundle fails validation', async () => {
    const store = new InMemorySkillStore();
    const first = await store.put('ws_one', 'sklv_one', [
      { path: 'SKILL.md', content: Buffer.from('valid') },
    ]);
    const second = await store.put('ws_one', 'sklv_two', [
      { path: 'SKILL.md', content: Buffer.from('later') },
    ]);
    const firstBundle = await store.open('ws_one', 'sklv_one', first.sha256);
    const secondBundle = await store.open('ws_one', 'sklv_two', second.sha256);
    const open = vi.spyOn(store, 'open');
    open.mockResolvedValueOnce(firstBundle);
    open.mockResolvedValueOnce({
      ...secondBundle,
      record: { ...second, sha256: 'f'.repeat(64) },
    });
    const sandbox = new RecordingSandbox();

    await expect(
      materializeSkills({
        workspaceId: 'ws_one',
        sandbox,
        skillStore: store,
        skills: [
          descriptor('sklv_one', 'first-skill', first),
          descriptor('sklv_two', 'second-skill', second),
        ],
      }),
    ).rejects.toThrow(/digest mismatch/);

    expect(open).toHaveBeenCalledTimes(2);
    expect(sandbox.deletedPaths).toEqual([SKILLS_ROOT, SKILLS_ROOT]);
    expect(sandbox.writes.size).toBe(0);
  });
});

function descriptor(
  id: string,
  name: string,
  record: { sha256: string; sizeBytes: number },
): SkillDescriptor {
  return {
    id,
    skill_id: `skl_${name}`,
    source: 'custom',
    version_identifier: '1',
    name,
    description: `${name} description`,
    entrypoint: 'SKILL.md',
    package_sha256: record.sha256,
    package_size_bytes: record.sizeBytes,
  };
}

class RecordingSandbox implements SandboxHandle {
  readonly id = 'sbx_recording';
  readonly writes = new Map<string, Buffer>();
  readonly writeCalls: string[] = [];
  readonly modes = new Map<string, number>();
  readonly deletedPaths: string[] = [];
  chmodCalls = 0;
  chmodManyCalls = 0;
  readonly files: SandboxFiles = {
    write: async (path, content) => {
      if (!Buffer.isBuffer(content)) throw new Error('test expects buffered Skill files');
      this.writeCalls.push(path);
      this.writes.set(path, Buffer.from(content));
    },
    read: async (path) => this.writes.get(path) ?? Buffer.alloc(0),
    readUtf8Page: async () => {
      throw new Error('files.readUtf8Page not used');
    },
    list: async () => [],
    chmod: async (path, mode) => {
      this.chmodCalls += 1;
      this.modes.set(path, mode);
    },
    chmodMany: async (_root, entries) => {
      this.chmodManyCalls += 1;
      for (const entry of entries) {
        this.modes.set(entry.path, entry.mode);
      }
    },
    delete: async (path) => {
      this.deletedPaths.push(path);
      for (const writtenPath of this.writes.keys()) {
        if (writtenPath === path || writtenPath.startsWith(`${path}/`)) {
          this.writes.delete(writtenPath);
        }
      }
    },
  };

  async run(_call: ToolCall): Promise<ToolResult> {
    return { exit_code: 0 };
  }

  async runPrivileged(_cmd: string): Promise<ToolResult> {
    return { exit_code: 0 };
  }

  async pause(): Promise<void> {}

  async resume(): Promise<void> {}

  async destroy(): Promise<void> {}
}

function sha256(content: Buffer): string {
  return createHash('sha256').update(content).digest('hex');
}
