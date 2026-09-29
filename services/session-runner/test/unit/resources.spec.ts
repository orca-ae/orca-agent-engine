// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { mkdtemp, mkdir, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  createRunnerSandboxRuntime,
  resourceManifestDigest,
  type ResourceManifest,
  type SandboxHandle,
} from '../../src/sandbox/seam.js';
import { RunnerResources, type ResourceCheckpoint } from '../../src/resources.js';

const cleanupGate = vi.hoisted(() => ({
  beforeRemove: undefined as undefined | ((path: unknown) => Promise<void>),
  beforeOpen: undefined as undefined | ((path: unknown) => Promise<void>),
  beforeRename: undefined as undefined | ((from: string, to: string) => Promise<void>),
}));
vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return {
    ...actual,
    rename: async (...args: Parameters<typeof actual.rename>) => {
      await cleanupGate.beforeRename?.(String(args[0]), String(args[1]));
      return actual.rename(...args);
    },
    open: async (...args: Parameters<typeof actual.open>) => {
      await cleanupGate.beforeOpen?.(args[0]);
      return actual.open(...args);
    },
    rm: async (...args: Parameters<typeof actual.rm>) => {
      await cleanupGate.beforeRemove?.(args[0]);
      return actual.rm(...args);
    },
  };
});

const sessionId = 'ses_resources';
const hash = (content: string): string => createHash('sha256').update(content).digest('hex');
const fixtures: Array<() => Promise<void>> = [];
afterEach(async () => {
  cleanupGate.beforeRemove = undefined;
  cleanupGate.beforeOpen = undefined;
  cleanupGate.beforeRename = undefined;
  for (const cleanup of fixtures.splice(0)) await cleanup();
});

async function setup() {
  const workspaceDir = await mkdtemp(join(tmpdir(), 'orca-resource-controller-'));
  const runtime = createRunnerSandboxRuntime({ kind: 'in-memory' });
  let raw: SandboxHandle | undefined;
  const controller = new RunnerResources({
    workspaceDir,
    acquire: async () => {
      raw = await runtime.acquire({});
      return raw;
    },
    // Byte staging/checkpoint tests inject a tool handle. Actual SRT enforcement
    // is tested separately; an InMemory pass does not establish kernel isolation.
    enforce: async (handle) => handle,
  });
  fixtures.push(async () => {
    await controller.close();
    await rm(workspaceDir, { recursive: true, force: true });
  });
  return {
    controller,
    workspaceDir,
    root: () => (raw as SandboxHandle & { rootDir(): string }).rootDir(),
  };
}

function manifest(): ResourceManifest {
  return {
    version: 1,
    revision: hash('binding revision'),
    resources: [
      {
        resource_id: 'sesrsc_file',
        kind: 'file',
        mount_path: '/mnt/data/input.txt',
        access: 'read_only',
        files: [{ path: '', sha256: hash('attached'), size_bytes: 8, mode: 0o444 }],
      },
      {
        resource_id: 'sesrsc_memory',
        kind: 'memory_store',
        mount_path: '/mnt/memory',
        access: 'read_write',
        files: [{ path: 'note.txt', sha256: hash('memory'), size_bytes: 6, mode: 0o644 }],
      },
      {
        resource_id: 'sesrsc_ro',
        kind: 'memory_store',
        mount_path: '/mnt/readonly',
        access: 'read_only',
        files: [{ path: 'note.txt', sha256: hash('read only'), size_bytes: 9, mode: 0o444 }],
      },
    ],
  };
}
async function stage(controller: RunnerResources, value = manifest(), domains: string[] = []) {
  await controller.push(sessionId, {
    type: 'manifest',
    manifest: value,
    network_allowed_domains: domains,
  });
  for (const [resource_id, path, content] of [
    ['sesrsc_file', '', 'attached'],
    ['sesrsc_memory', 'note.txt', 'memory'],
    ['sesrsc_ro', 'note.txt', 'read only'],
  ])
    await controller.push(sessionId, {
      type: 'file_chunk',
      revision: value.revision,
      resource_id,
      path,
      offset: 0,
      content_base64: Buffer.from(content!).toString('base64'),
    });
  return controller.push(sessionId, {
    type: 'commit',
    revision: value.revision,
    manifest_sha256: resourceManifestDigest(value),
  });
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}

describe('credential-free runner resources', () => {
  it('rejects a file replaced by a FIFO between scanning and opening without blocking shutdown', async () => {
    const h = await setup();
    await stage(h.controller);
    const target = join(await realpath(h.root()), 'mnt/session/outputs/swapped');
    cleanupGate.beforeOpen = async (path) => {
      if (path !== target) return;
      cleanupGate.beforeOpen = undefined;
      await rm(target);
      execFileSync('mkfifo', [target]);
    };
    await expect(
      h.controller.runTool(
        async () => {
          await writeFile(target, 'regular');
        },
        () => {
          throw new Error('must not publish FIFO');
        },
        new AbortController().signal,
      ),
    ).rejects.toThrow('bounded regular file');
    await h.controller.close();
  });

  it('serializes duplicate ACKs so a delayed retry cannot acknowledge the next checkpoint', async () => {
    const h = await setup();
    await stage(h.controller);
    const first = deferred<{ checkpoint: ResourceCheckpoint; digest: string }>();
    const tool1 = h.controller.runTool(
      async () => {
        await writeFile(join(h.root(), 'mnt/session/outputs/result'), 'first');
      },
      (checkpoint, digest) => first.resolve({ checkpoint, digest }),
      new AbortController().signal,
    );
    const cp1 = await first.promise;
    const second = deferred<{ checkpoint: ResourceCheckpoint; digest: string }>();
    const tool2 = h.controller.runTool(
      async () => {
        await writeFile(join(h.root(), 'mnt/session/outputs/result'), 'second');
      },
      (checkpoint, digest) => second.resolve({ checkpoint, digest }),
      new AbortController().signal,
    );
    let secondFinished = false;
    void tool2.then(() => {
      secondFinished = true;
    });
    const releaseStale = deferred<void>();
    let oldDirectory: string | undefined;
    let removals = 0;
    cleanupGate.beforeRemove = async (path) => {
      if (typeof path !== 'string' || !/checkpoint-[^/]+$/.test(path)) return;
      oldDirectory ??= path;
      if (path === oldDirectory && ++removals === 2) await releaseStale.promise;
    };
    const ack = { checkpoint_id: cp1.checkpoint.checkpoint_id, manifest_sha256: cp1.digest };
    const ack1 = h.controller.acknowledge(sessionId, ack);
    const duplicate = h.controller.acknowledge(sessionId, ack);
    await tool1;
    const cp2 = await second.promise;
    releaseStale.resolve();
    await Promise.all([ack1, duplicate]);
    expect(h.controller.pendingCheckpoint?.checkpoint_id).toBe(cp2.checkpoint.checkpoint_id);
    expect(secondFinished).toBe(false);
    await h.controller.acknowledge(sessionId, {
      checkpoint_id: cp2.checkpoint.checkpoint_id,
      manifest_sha256: cp2.digest,
    });
    await tool2;
    expect(
      await h.controller.runTool(
        async () => 'unchanged',
        () => {
          throw new Error('stale baseline');
        },
        new AbortController().signal,
      ),
    ).toBe('unchanged');
  });

  it('closes during a tool operation without waiting on a future checkpoint ACK', async () => {
    const h = await setup();
    await stage(h.controller);
    const entered = deferred<void>();
    const release = deferred<void>();
    const tool = h.controller.runTool(
      async () => {
        entered.resolve();
        await release.promise;
        await writeFile(join(h.root(), 'mnt/session/outputs/result'), 'interrupted');
      },
      () => {
        throw new Error('closed controller must not publish');
      },
      new AbortController().signal,
    );
    const rejection = expect(tool).rejects.toThrow('stopped');
    await entered.promise;
    const closing = h.controller.close();
    release.resolve();
    await closing;
    await rejection;
    expect(h.controller.toolSandbox).toBeUndefined();
    await expect(
      h.controller.push(sessionId, { type: 'manifest', manifest: manifest() }),
    ).rejects.toThrow('stopped');
  });

  it('stages exact File and Memory bytes before exposing the tool handle', async () => {
    const h = await setup();
    expect(h.controller.toolSandbox).toBeUndefined();
    expect(await stage(h.controller)).toEqual({ committed: true });
    expect(await readFile(join(h.root(), 'mnt/data/input.txt'), 'utf8')).toBe('attached');
    expect(await readFile(join(h.root(), 'mnt/memory/note.txt'), 'utf8')).toBe('memory');
    expect(h.controller.revision).toBe(manifest().revision);
    expect(h.controller.toolSandbox).toBeDefined();
  });

  it('rejects wrong digest, out-of-order and changed replay chunks before commit', async () => {
    const h = await setup();
    const value = manifest();
    await h.controller.push(sessionId, { type: 'manifest', manifest: value });
    const chunk = {
      type: 'file_chunk',
      revision: value.revision,
      resource_id: 'sesrsc_file',
      path: '',
      offset: 0,
      content_base64: Buffer.from('attached').toString('base64'),
    };
    await expect(h.controller.push(sessionId, { ...chunk, offset: 2 })).rejects.toThrow('offset');
    await h.controller.push(sessionId, chunk);
    await h.controller.push(sessionId, chunk);
    await expect(
      h.controller.push(sessionId, {
        ...chunk,
        content_base64: Buffer.from('changed!').toString('base64'),
      }),
    ).rejects.toThrow('replay differs');
    await expect(
      h.controller.push(sessionId, {
        type: 'commit',
        revision: value.revision,
        manifest_sha256: 'b'.repeat(64),
      }),
    ).rejects.toThrow('digest mismatch');
    await expect(
      h.controller.push(sessionId, {
        type: 'commit',
        revision: value.revision,
        manifest_sha256: resourceManifestDigest(value),
      }),
    ).rejects.toThrow('integrity');
    expect(h.controller.toolSandbox).toBeUndefined();
  });

  it('keeps live RW changes on reconnect and rejects cross-session or rebound revisions', async () => {
    const h = await setup();
    await stage(h.controller);
    await writeFile(join(h.root(), 'mnt/memory/note.txt'), 'live');
    expect(await stage(h.controller)).toEqual({ committed: true });
    expect(await readFile(join(h.root(), 'mnt/memory/note.txt'), 'utf8')).toBe('live');
    await expect(
      h.controller.push('ses_other', { type: 'manifest', manifest: manifest() }),
    ).rejects.toThrow('session mismatch');
    const changed = manifest();
    changed.resources[1]!.mount_path = '/mnt/other';
    await expect(
      h.controller.push(sessionId, { type: 'manifest', manifest: changed }),
    ).rejects.toThrow('binding revision differs');
  });

  it('refreshes shared Memory updates and deletions while preserving acknowledged output and Skills', async () => {
    const h = await setup();
    await stage(h.controller);
    const originalRoot = h.root();
    await writeFile(join(originalRoot, 'workspace/skills/keep.txt'), 'skill');
    const run = (work: () => Promise<unknown>) =>
      h.controller.runTool(
        work,
        (cp, digest) => {
          void h.controller.acknowledge(sessionId, {
            checkpoint_id: cp.checkpoint_id,
            manifest_sha256: digest,
          });
        },
        new AbortController().signal,
      );
    await run(async () => {
      await writeFile(join(originalRoot, 'mnt/memory/note.txt'), 'acknowledged');
      await writeFile(join(originalRoot, 'mnt/session/outputs/keep.txt'), 'output');
    });
    const next = manifest();
    next.resources[1]!.files = [
      { path: 'new.txt', sha256: hash('remote'), size_bytes: 6, mode: 0o644 },
    ];
    next.resources[2]!.files = [];
    expect(await h.controller.push(sessionId, { type: 'manifest', manifest: next })).toEqual({
      committed: false,
    });
    for (const [resource_id, path, content] of [
      ['sesrsc_file', '', 'attached'],
      ['sesrsc_memory', 'new.txt', 'remote'],
    ])
      await h.controller.push(sessionId, {
        type: 'file_chunk',
        revision: next.revision,
        resource_id,
        path,
        offset: 0,
        content_base64: Buffer.from(content!).toString('base64'),
      });
    await h.controller.push(sessionId, {
      type: 'commit',
      revision: next.revision,
      manifest_sha256: resourceManifestDigest(next),
    });
    expect(h.root()).toBe(originalRoot);
    expect(await readFile(join(h.root(), 'mnt/memory/new.txt'), 'utf8')).toBe('remote');
    await expect(readFile(join(h.root(), 'mnt/memory/note.txt'))).rejects.toThrow();
    await expect(readFile(join(h.root(), 'mnt/readonly/note.txt'))).rejects.toThrow();
    expect(await readFile(join(h.root(), 'mnt/session/outputs/keep.txt'), 'utf8')).toBe('output');
    expect(await readFile(join(h.root(), 'workspace/skills/keep.txt'), 'utf8')).toBe('skill');
    await h.controller.runTool(
      async () => {},
      () => {
        throw new Error('refresh must not echo writes');
      },
      new AbortController().signal,
    );
    expect(await h.controller.push(sessionId, { type: 'manifest', manifest: next })).toEqual({
      committed: true,
    });
  });

  it('rolls back a partial Memory refresh and retries the same binding revision', async () => {
    const h = await setup();
    await stage(h.controller);
    const next = manifest();
    next.resources[1]!.files = [];
    next.resources[2]!.files = [];
    const deliver = async () => {
      await h.controller.push(sessionId, { type: 'manifest', manifest: next });
      await h.controller.push(sessionId, {
        type: 'file_chunk',
        revision: next.revision,
        resource_id: 'sesrsc_file',
        path: '',
        offset: 0,
        content_base64: Buffer.from('attached').toString('base64'),
      });
      return h.controller.push(sessionId, {
        type: 'commit',
        revision: next.revision,
        manifest_sha256: resourceManifestDigest(next),
      });
    };
    cleanupGate.beforeRename = async (from, to) => {
      if (from.includes('/next-') && to.endsWith('/mnt/readonly')) {
        cleanupGate.beforeRename = undefined;
        throw new Error('refresh interrupted');
      }
    };
    await expect(deliver()).rejects.toThrow('refresh interrupted');
    expect(await readFile(join(h.root(), 'mnt/memory/note.txt'), 'utf8')).toBe('memory');
    expect(await readFile(join(h.root(), 'mnt/readonly/note.txt'), 'utf8')).toBe('read only');
    expect(h.controller.ready).toBe(false);
    await deliver();
    expect(h.controller.ready).toBe(true);
    await expect(readFile(join(h.root(), 'mnt/memory/note.txt'))).rejects.toThrow();
    await expect(readFile(join(h.root(), 'mnt/readonly/note.txt'))).rejects.toThrow();
  });

  it('does not refresh over unacknowledged local Memory changes', async () => {
    const h = await setup();
    await stage(h.controller);
    await writeFile(join(h.root(), 'mnt/memory/note.txt'), 'local');
    const next = manifest();
    next.resources[1]!.files = [];
    next.resources[2]!.files = [];
    await h.controller.push(sessionId, { type: 'manifest', manifest: next });
    await h.controller.push(sessionId, {
      type: 'file_chunk',
      revision: next.revision,
      resource_id: 'sesrsc_file',
      path: '',
      offset: 0,
      content_base64: Buffer.from('attached').toString('base64'),
    });
    await expect(
      h.controller.push(sessionId, {
        type: 'commit',
        revision: next.revision,
        manifest_sha256: resourceManifestDigest(next),
      }),
    ).rejects.toThrow('unacknowledged Memory');
    expect(await readFile(join(h.root(), 'mnt/memory/note.txt'), 'utf8')).toBe('local');
  });

  it('recovers a failed atomic Skill replacement without clearing resource failures', async () => {
    const h = await setup();
    await stage(h.controller);
    const skills = (text: string) => ({
      dir: 'skills',
      skills: ['guide'],
      files: [
        {
          skill: 'guide',
          path: 'SKILL.md',
          content: Buffer.from(text),
          mimeType: 'text/markdown',
          mode: 0o444,
        },
      ],
    });
    await h.controller.replaceSkills(sessionId, skills('old'));
    cleanupGate.beforeRename = async (from, to) => {
      if (from.includes('/.skills-') && to.endsWith('/skills')) {
        cleanupGate.beforeRename = undefined;
        throw new Error('transient rename');
      }
    };
    await expect(h.controller.replaceSkills(sessionId, skills('new'))).rejects.toThrow(
      'transient rename',
    );
    expect(await readFile(join(h.root(), 'workspace/skills/guide/SKILL.md'), 'utf8')).toBe('old');
    expect(h.controller.ready).toBe(false);
    expect(h.controller.readyForSnapshot).toBe(true);
    await h.controller.replaceSkills(sessionId, skills('new'));
    expect(h.controller.ready).toBe(true);
    expect(await readFile(join(h.root(), 'workspace/skills/guide/SKILL.md'), 'utf8')).toBe('new');
    // A corrupt output makes freezing fail; successful Skills cannot heal it.
    await symlink('/etc/passwd', join(h.root(), 'mnt/session/outputs/unsafe'));
    await expect(
      h.controller.runTool(
        async () => {},
        () => {},
        new AbortController().signal,
      ),
    ).rejects.toThrow();
    await h.controller.replaceSkills(sessionId, skills('new'));
    expect(h.controller.readyForSnapshot).toBe(false);
    expect(h.controller.ready).toBe(false);
  });

  it('freezes outputs and RW memory, waits for exact ACK, then allows another tool', async () => {
    const h = await setup();
    await stage(h.controller);
    const published = deferred<{ checkpoint: ResourceCheckpoint; digest: string }>();
    let finished = false;
    const tool = h.controller
      .runTool(
        async () => {
          await writeFile(join(h.root(), 'mnt/session/outputs/result.txt'), 'output');
          await writeFile(join(h.root(), 'mnt/memory/note.txt'), 'updated');
          return 'done';
        },
        (checkpoint, digest) => published.resolve({ checkpoint, digest }),
        new AbortController().signal,
      )
      .then((value) => {
        finished = true;
        return value;
      });
    const { checkpoint, digest } = await published.promise;
    expect(finished).toBe(false);
    expect(checkpoint.files.map((file) => file.resource_id).sort()).toEqual([
      'sesrsc_memory',
      'session_outputs',
    ]);
    expect(
      checkpoint.files.find((file) => file.resource_id === 'sesrsc_memory')?.previous_sha256,
    ).toBe(hash('memory'));
    // A later external write cannot alter the bytes Registry is committing.
    await writeFile(join(h.root(), 'mnt/session/outputs/result.txt'), 'later');
    expect(
      await h.controller.changes(sessionId, {
        type: 'file_chunk',
        checkpoint_id: checkpoint.checkpoint_id,
        resource_id: 'session_outputs',
        path: 'result.txt',
        offset: 0,
      }),
    ).toEqual({ offset: 0, content_base64: Buffer.from('output').toString('base64') });
    await expect(
      h.controller.acknowledge(sessionId, {
        checkpoint_id: checkpoint.checkpoint_id,
        manifest_sha256: hash('wrong'),
      }),
    ).rejects.toThrow('differs');
    await h.controller.acknowledge(sessionId, {
      checkpoint_id: checkpoint.checkpoint_id,
      manifest_sha256: digest,
    });
    expect(await tool).toBe('done');
    // A duplicate ACK is harmless even after the private bytes were removed.
    await h.controller.acknowledge(sessionId, {
      checkpoint_id: checkpoint.checkpoint_id,
      manifest_sha256: digest,
    });
    const second = deferred<{ checkpoint: ResourceCheckpoint; digest: string }>();
    const next = h.controller.runTool(
      async () => 'next',
      (checkpoint, digest) => second.resolve({ checkpoint, digest }),
      new AbortController().signal,
    );
    const changed = await second.promise;
    expect(changed.checkpoint.files).toHaveLength(1);
    expect(changed.checkpoint.files[0]!.sha256).toBe(hash('later'));
    await h.controller.acknowledge(sessionId, {
      checkpoint_id: changed.checkpoint.checkpoint_id,
      manifest_sha256: changed.digest,
    });
    expect(await next).toBe('next');
  });

  it('retains frozen bytes on interrupted ACK and blocks the next tool until persistence resolves', async () => {
    const h = await setup();
    await stage(h.controller);
    const abort = new AbortController();
    const published = deferred<{ checkpoint: ResourceCheckpoint; digest: string }>();
    const tool = h.controller.runTool(
      async () => {
        await writeFile(join(h.root(), 'mnt/session/outputs/o'), 'pending');
      },
      (checkpoint, digest) => published.resolve({ checkpoint, digest }),
      abort.signal,
    );
    const rejection = expect(tool).rejects.toThrow('acknowledgement interrupted');
    const frozen = await published.promise;
    abort.abort();
    await rejection;
    expect(await h.controller.changes(sessionId, { type: 'pending' })).toMatchObject({
      checkpoint: { checkpoint_id: frozen.checkpoint.checkpoint_id },
    });
    await expect(
      h.controller.runTool(
        async () => 'unsafe',
        () => {},
        new AbortController().signal,
      ),
    ).rejects.toThrow('not ready');
    await h.controller.acknowledge(sessionId, {
      checkpoint_id: frozen.checkpoint.checkpoint_id,
      manifest_sha256: frozen.digest,
    });
    expect(
      await h.controller.runTool(
        async () => 'safe',
        () => {
          throw new Error('unchanged files should not publish');
        },
        new AbortController().signal,
      ),
    ).toBe('safe');
  });

  it('exports memory deletion and saves writes made by a tool that throws', async () => {
    const h = await setup();
    await stage(h.controller);
    const published = deferred<{ checkpoint: ResourceCheckpoint; digest: string }>();
    const tool = h.controller.runTool(
      async () => {
        await rm(join(h.root(), 'mnt/memory/note.txt'));
        await writeFile(join(h.root(), 'mnt/session/outputs/error.log'), 'written before error');
        throw new Error('tool failed');
      },
      (checkpoint, digest) => published.resolve({ checkpoint, digest }),
      new AbortController().signal,
    );
    const rejection = expect(tool).rejects.toThrow('tool failed');
    const frozen = await published.promise;
    expect(frozen.checkpoint.deleted).toEqual([
      { resource_id: 'sesrsc_memory', path: 'note.txt', previous_sha256: hash('memory') },
    ]);
    await h.controller.acknowledge(sessionId, {
      checkpoint_id: frozen.checkpoint.checkpoint_id,
      manifest_sha256: frozen.digest,
    });
    await rejection;
    expect(
      await h.controller.runTool(
        async () => 'next',
        () => {},
        new AbortController().signal,
      ),
    ).toBe('next');
  });

  it('rejects links and keeps the resource generation blocked after a freeze failure', async () => {
    const h = await setup();
    await stage(h.controller);
    await mkdir(join(h.workspaceDir, 'private'), { recursive: true });
    await writeFile(join(h.workspaceDir, 'private', 'secret'), 'private');
    await expect(
      h.controller.runTool(
        async () => {
          await symlink(
            join(h.workspaceDir, 'private', 'secret'),
            join(h.root(), 'mnt/session/outputs/leak'),
          );
        },
        () => {
          throw new Error('must not publish');
        },
        new AbortController().signal,
      ),
    ).rejects.toThrow('link or device');
    await expect(
      h.controller.runTool(
        async () => 'unsafe',
        () => {},
        new AbortController().signal,
      ),
    ).rejects.toThrow('not ready');
  });
});

const gitCapability = (token = 'eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjIwMDAwMDAwMDB9.signature') => ({
  resource_id: 'sesrsc_git',
  remote_url: 'https://registry.example/v1/git-proxy/sesrsc_git',
  authorization_header: `Authorization: Bearer ${token}`,
  expires_at: Math.floor(Date.now() / 1000) + 900,
});
async function stageGit(controller: RunnerResources) {
  const value = manifest();
  value.resources.push({
    resource_id: 'sesrsc_git',
    kind: 'github_repository',
    mount_path: '/workspace/repository',
    access: 'read_write',
    files: [],
  });
  await stage(controller, value, ['registry.example']);
  return value;
}
it('refreshes a read-only Git capability without replacing the live checkout', async () => {
  const { controller, root } = await setup();
  const value = await stageGit(controller);
  expect(controller.ready).toBe(false);
  const send = (capabilities: unknown[]) =>
    controller.push(sessionId, {
      type: 'git_capabilities',
      revision: value.revision,
      capabilities,
    });
  await send([gitCapability()]);
  expect(controller.ready).toBe(true);
  await writeFile(join(root(), 'workspace/repository/local.txt'), 'local work');
  await send([gitCapability('new.token.signature')]);
  expect(await readFile(join(root(), '.orca/git/sesrsc_git.config'), 'utf8')).toContain(
    'Authorization: Bearer new.token.signature',
  );
  expect(await readFile(join(root(), 'workspace/repository/local.txt'), 'utf8')).toBe('local work');
});
it('rejects missing, extra, duplicate or unsafe Git capabilities before changing auth files', async () => {
  const { controller, root } = await setup();
  const value = await stageGit(controller);
  const send = (capabilities: unknown[], revision = value.revision) =>
    controller.push(sessionId, {
      type: 'git_capabilities',
      revision,
      capabilities,
    });
  await send([gitCapability()]);
  const original = await readFile(join(root(), '.orca/git/sesrsc_git.config'), 'utf8');
  for (const caps of [
    [],
    [gitCapability(), gitCapability()],
    [{ ...gitCapability(), resource_id: 'sesrsc_other' }],
    [
      {
        ...gitCapability(),
        remote_url: 'https://user:password@registry.example/v1/git-proxy/sesrsc_git',
      },
    ],
    [{ ...gitCapability(), remote_url: 'https://registry.example/other' }],
    [{ ...gitCapability(), authorization_header: 'Authorization: Bearer x\n[alias]' }],
    [{ ...gitCapability(), expires_at: 1 }],
    [{ ...gitCapability(), expires_at: Math.floor(Date.now() / 1000) + 599 }],
  ]) {
    await expect(send(caps)).rejects.toThrow();
    expect(controller.ready).toBe(false);
    expect(await readFile(join(root(), '.orca/git/sesrsc_git.config'), 'utf8')).toBe(original);
    await send([gitCapability()]);
    expect(controller.ready).toBe(true);
  }
  await expect(send([gitCapability()], hash('stale'))).rejects.toThrow();
});

it('refreshes Memory without retransmitting a retained Git checkout and rejects unverified retention', async () => {
  const { controller, root } = await setup();
  expect(await controller.push(sessionId, { type: 'status' })).toEqual({ committed: false });
  const value = manifest();
  const git = {
    resource_id: 'sesrsc_git',
    kind: 'github_repository' as const,
    mount_path: '/workspace/repository',
    access: 'read_write' as const,
    files: [{ path: 'README', sha256: hash('git'), size_bytes: 3, mode: 0o644 }],
  };
  value.resources.push(git);
  const begin = (manifest: ResourceManifest, retained: unknown = ['sesrsc_git']) =>
    controller.push(sessionId, {
      type: 'manifest',
      manifest,
      retained_git_resource_ids: retained,
    });
  await expect(begin(value)).rejects.toThrow('not active');
  await expect(begin(value, [42])).rejects.toThrow('invalid retained');
  await begin(value, []);
  for (const [resource_id, path, content] of [
    ['sesrsc_file', '', 'attached'],
    ['sesrsc_memory', 'note.txt', 'memory'],
    ['sesrsc_ro', 'note.txt', 'read only'],
    ['sesrsc_git', 'README', 'git'],
  ])
    await controller.push(sessionId, {
      type: 'file_chunk',
      revision: value.revision,
      resource_id,
      path,
      offset: 0,
      content_base64: Buffer.from(content!).toString('base64'),
    });
  await controller.push(sessionId, {
    type: 'commit',
    revision: value.revision,
    manifest_sha256: resourceManifestDigest(value),
  });
  expect(await controller.push(sessionId, { type: 'status' })).toEqual({
    committed: true,
    revision: value.revision,
  });
  await writeFile(join(root(), 'workspace/repository/README'), 'local edits');
  const next = structuredClone(value);
  next.resources[1]!.files = [
    { path: 'note.txt', sha256: hash('new memory'), size_bytes: 10, mode: 0o644 },
  ];
  await expect(begin({ ...next, revision: hash('different') })).rejects.toThrow('not active');
  await expect(begin(next, ['sesrsc_memory'])).rejects.toThrow('not active');
  const forged = structuredClone(next);
  forged.resources[3]!.mount_path = '/workspace/other';
  await expect(begin(forged)).rejects.toThrow('binding revision differs');
  // A reconnect can prepare a newer upstream snapshot before discovering the
  // live checkout. Retention must still preserve local edits on the next turn.
  next.resources[3]!.files[0]!.sha256 = hash('new upstream');
  await begin(next);
  for (const [resource_id, path, content] of [
    ['sesrsc_file', '', 'attached'],
    ['sesrsc_memory', 'note.txt', 'new memory'],
    ['sesrsc_ro', 'note.txt', 'read only'],
  ])
    await controller.push(sessionId, {
      type: 'file_chunk',
      revision: next.revision,
      resource_id,
      path,
      offset: 0,
      content_base64: Buffer.from(content!).toString('base64'),
    });
  await controller.push(sessionId, {
    type: 'commit',
    revision: next.revision,
    manifest_sha256: resourceManifestDigest(next),
  });
  expect(await readFile(join(root(), 'mnt/memory/note.txt'), 'utf8')).toBe('new memory');
  expect(await readFile(join(root(), 'workspace/repository/README'), 'utf8')).toBe('local edits');
});

it('pins unrestricted egress to the resource revision and rejects silent policy changes', async () => {
  const { controller } = await setup();
  await stage(controller);
  await expect(
    controller.push(sessionId, {
      type: 'manifest',
      manifest: manifest(),
      network_unrestricted: true,
    }),
  ).rejects.toThrow('binding revision differs');
  await expect(
    controller.push(sessionId, {
      type: 'manifest',
      manifest: manifest(),
      network_unrestricted: 'yes',
    }),
  ).rejects.toThrow('invalid resource network policy');
});

it('requires a full grant window for a new turn without interrupting a still-authorized turn', async () => {
  const { controller } = await setup();
  const value = await stageGit(controller);
  const now = Date.now();
  await controller.push(sessionId, {
    type: 'git_capabilities',
    revision: value.revision,
    capabilities: [gitCapability()],
  });
  const clock = vi.spyOn(Date, 'now').mockReturnValue(now + 400_000);
  try {
    expect(controller.ready).toBe(true);
    expect(controller.readyForTurn).toBe(false);
  } finally {
    clock.mockRestore();
  }
});
