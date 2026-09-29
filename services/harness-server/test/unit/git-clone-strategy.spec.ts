// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';
import { WorkDirManager } from '../../src/git/work-dir.js';
import type { GitWorker } from '../../src/git/git-worker.js';
import { GitCloneStrategy } from '../../src/sandbox/mounts/git-clone.js';
import type { MountResource } from '../../src/sandbox/mounts/mount-strategy.js';
import { registry } from '../../src/metrics.js';

describe('GitCloneStrategy', () => {
  let workBase: string;
  let workDir: WorkDirManager;
  let runtime: InMemorySandboxRuntime;

  beforeEach(() => {
    workBase = mkdtempSync(join(tmpdir(), 'git-clone-strategy-'));
    workDir = new WorkDirManager({ baseDir: workBase });
    runtime = new InMemorySandboxRuntime();
  });

  afterEach(() => {
    try {
      rmSync(workBase, { recursive: true, force: true });
    } catch {
      // ignore
    }
  });

  function makeFakeWorker(seedFiles: Record<string, string>): GitWorker {
    return {
      async cloneInto(input) {
        // The dest is created by WorkDirManager.acquire; populate it with the
        // seed files INCLUDING a .git/ subdir so the strategy walks both.
        for (const [rel, content] of Object.entries(seedFiles)) {
          const full = join(input.dest, rel);
          mkdirSync(join(full, '..'), { recursive: true });
          writeFileSync(full, content);
        }
        // Also seed a fake .git/HEAD so the strategy ships it.
        if (!seedFiles['.git/HEAD']) {
          mkdirSync(join(input.dest, '.git'), { recursive: true });
          writeFileSync(join(input.dest, '.git', 'HEAD'), 'ref: refs/heads/main\n');
        }
        return { commit: 'a'.repeat(40) };
      },
      async listWorkingTree() {
        // Not used by GitCloneStrategy.activate — strategy walks the dir
        // directly via fs.readdir.
        return [];
      },
    };
  }

  it('clones into the sandbox: working tree + .git/ files appear at mount_path', async () => {
    const sandbox = await runtime.acquire({});
    const fake = makeFakeWorker({
      'README.md': '# orca-test\n',
      'src/index.ts': 'export const ok = true;\n',
    });
    const strategy = new GitCloneStrategy({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      worker: fake,
      workDir,
      resolvePat: async () => 'fake-pat',
    });
    const resource: MountResource = {
      id: 'sesrsc_1',
      type: 'github_repository',
      url: 'https://github.com/org/repo',
      mountPath: '/workspace/repo/',
      access: 'read_write',
      gitCredentialId: 'gitcred_1',
      repoIdx: 0,
    };

    const handle = await strategy.activate(sandbox, resource);
    expect(handle.resourceType).toBe('github_repository');
    expect(handle.mountPath).toBe('/workspace/repo/');

    // Verify the sandbox FS has the streamed files.
    const readme = await sandbox.files.read('/workspace/repo/README.md');
    expect(readme.toString()).toBe('# orca-test\n');

    const src = await sandbox.files.read('/workspace/repo/src/index.ts');
    expect(src.toString()).toBe('export const ok = true;\n');

    const gitHead = await sandbox.files.read('/workspace/repo/.git/HEAD');
    expect(gitHead.toString()).toContain('refs/heads/main');
  });

  it('throws on non-github_repository resource', async () => {
    const sandbox = await runtime.acquire({});
    const fake = makeFakeWorker({});
    const strategy = new GitCloneStrategy({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      worker: fake,
      workDir,
      resolvePat: async () => 'pat',
    });
    await expect(
      strategy.activate(sandbox, {
        id: 'x',
        type: 'file',
        fileId: 'file_x',
        mountPath: '/x',
        access: 'read_only',
      } as unknown as MountResource),
    ).rejects.toThrow(/unsupported resource type/);
  });

  it('resolvePat is called exactly once with the resource gitCredentialId', async () => {
    const sandbox = await runtime.acquire({});
    const fake = makeFakeWorker({});
    const calls: string[] = [];
    const strategy = new GitCloneStrategy({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      worker: fake,
      workDir,
      resolvePat: async (id) => {
        calls.push(id);
        return 'pat-' + id;
      },
    });
    await strategy.activate(sandbox, {
      id: 'sesrsc_1',
      type: 'github_repository',
      url: 'https://github.com/org/repo',
      mountPath: '/workspace/repo/',
      access: 'read_write',
      gitCredentialId: 'gitcred_pat_1',
      repoIdx: 0,
    });
    expect(calls).toEqual(['gitcred_pat_1']);
  });

  it('forwards branch checkout via the --branch arg', async () => {
    const sandbox = await runtime.acquire({});
    let observedRef: string | undefined;
    const fake: GitWorker = {
      async cloneInto(input) {
        observedRef = input.ref;
        // Seed a minimal .git/ so the post-clone walk has at least one file.
        mkdirSync(join(input.dest, '.git'), { recursive: true });
        writeFileSync(join(input.dest, '.git', 'HEAD'), 'ref: refs/heads/feature-x\n');
        return { commit: 'b'.repeat(40) };
      },
      async listWorkingTree() {
        return [];
      },
    };
    const strategy = new GitCloneStrategy({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      worker: fake,
      workDir,
      resolvePat: async () => 'pat',
    });
    await strategy.activate(sandbox, {
      id: 'sesrsc_1',
      type: 'github_repository',
      url: 'https://github.com/org/repo',
      mountPath: '/workspace/repo/',
      access: 'read_write',
      gitCredentialId: 'gitcred_1',
      repoIdx: 0,
      checkout: { type: 'branch', value: 'feature-x' },
    });
    expect(observedRef).toBe('feature-x');
  });

  it('forwards commit pin via shallow=false (full history needed)', async () => {
    const sandbox = await runtime.acquire({});
    let observedShallow: boolean | undefined;
    const fake: GitWorker = {
      async cloneInto(input) {
        observedShallow = input.shallow;
        mkdirSync(join(input.dest, '.git'), { recursive: true });
        writeFileSync(join(input.dest, '.git', 'HEAD'), 'ref: refs/heads/main\n');
        return { commit: 'c'.repeat(40) };
      },
      async listWorkingTree() {
        return [];
      },
    };
    const strategy = new GitCloneStrategy({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      worker: fake,
      workDir,
      resolvePat: async () => 'pat',
    });
    await strategy.activate(sandbox, {
      id: 'sesrsc_1',
      type: 'github_repository',
      url: 'https://github.com/org/repo',
      mountPath: '/workspace/repo/',
      access: 'read_write',
      gitCredentialId: 'gitcred_1',
      repoIdx: 0,
      checkout: { type: 'commit', value: 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeef' },
    });
    expect(observedShallow).toBe(false);
  });

  it('increments harness_git_clone_total{result=ok} on successful activate', async () => {
    const sandbox = await runtime.acquire({});
    const fake = makeFakeWorker({ 'README.md': 'x' });
    // Use a unique workspace_id so the counter cell is isolated from other
    // tests in this file (which all use 'ws_x'). The histogram's count is
    // global, but the counter is label-partitioned, so a fresh label gives
    // a deterministic before/after delta of exactly 1.
    const workspaceId = 'ws_metrics_clone_ok';
    const strategy = new GitCloneStrategy({
      workspaceId,
      sessionId: 'ses_y',
      worker: fake,
      workDir,
      resolvePat: async () => 'fake-pat',
    });
    const resource: MountResource = {
      id: 'sesrsc_metrics_1',
      type: 'github_repository',
      url: 'https://github.com/org/repo',
      mountPath: '/workspace/repo/',
      access: 'read_write',
      gitCredentialId: 'gitcred_1',
      repoIdx: 0,
    };

    const before = await readCounterCell({
      name: 'harness_git_clone_total',
      labels: { workspace_id: workspaceId, result: 'ok' },
    });
    await strategy.activate(sandbox, resource);
    const after = await readCounterCell({
      name: 'harness_git_clone_total',
      labels: { workspace_id: workspaceId, result: 'ok' },
    });
    expect(after - before).toBe(1);
  });

  it('snapshot hooks are no-ops', async () => {
    const sandbox = await runtime.acquire({});
    const fake = makeFakeWorker({ 'README.md': 'x' });
    const strategy = new GitCloneStrategy({
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      worker: fake,
      workDir,
      resolvePat: async () => 'pat',
    });
    const handle = await strategy.activate(sandbox, {
      id: 'sesrsc_1',
      type: 'github_repository',
      url: 'https://github.com/org/repo',
      mountPath: '/workspace/repo/',
      access: 'read_write',
      gitCredentialId: 'gitcred_1',
      repoIdx: 0,
    });
    const torn = await strategy.teardownForSnapshot(sandbox, handle);
    expect(torn.mountPath).toBe('/workspace/repo/');
    const restored = await strategy.restoreAfterSnapshot(sandbox, torn);
    expect(restored.mountPath).toBe('/workspace/repo/');
  });
});

/**
 * Read a single counter label-cell value out of the harness registry by name +
 * labels. Returns 0 when no matching cell exists yet (prom-client doesn't
 * materialize counter cells until the first `.inc()` for that label
 * combination). Mirrors the helper in `metrics-phase5.1.spec.ts`.
 */
async function readCounterCell(opts: {
  name: string;
  labels: Record<string, string>;
}): Promise<number> {
  const json = await registry.getMetricsAsJSON();
  const metric = json.find((m) => m.name === opts.name);
  if (!metric) return 0;
  const values = (metric as { values: Array<{ labels: Record<string, string>; value: number }> })
    .values;
  const cell = values.find((v) =>
    Object.entries(opts.labels).every(([k, val]) => v.labels[k] === val),
  );
  return cell?.value ?? 0;
}
