// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { chmodSync, mkdtempSync, mkdirSync, writeFileSync, rmSync, statSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';
import { WorkDirManager } from '../../src/git/work-dir.js';
import type { GitWorker } from '../../src/git/git-worker.js';
import { GitCloneStrategy } from '../../src/sandbox/mounts/git-clone.js';
import type { MountResource } from '../../src/sandbox/mounts/mount-strategy.js';
import { registry } from '../../src/metrics.js';
import { serializeSandboxFileModes } from '../../src/sandbox/chmod-many.js';
import type { SandboxFileMode } from '../../src/sandbox/sandbox-runtime.js';

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

  it.each([true, false])('preserves real Git file modes (batch=%s)', async (batch) => {
    const sandbox = await runtime.acquire({});
    const mountPath = join(workBase, 'sandbox-repo');
    const git = (cwd: string, ...args: string[]) =>
      execFileSync('git', ['-C', cwd, ...args], { encoding: 'utf8' }).trim();
    const worker: GitWorker = {
      async cloneInto({ dest }) {
        git(dest, 'init', '-q');
        git(dest, 'config', 'core.fileMode', 'true');
        writeFileSync(join(dest, 'plain.txt'), 'plain\n');
        writeFileSync(join(dest, 'run.sh'), '#!/bin/sh\nprintf executable\n');
        chmodSync(join(dest, 'plain.txt'), 0o644);
        chmodSync(join(dest, 'run.sh'), 0o755);
        git(dest, 'add', '.');
        git(
          dest,
          '-c',
          'user.name=Fixture',
          '-c',
          'user.email=fixture@example.com',
          '-c',
          'commit.gpgsign=false',
          'commit',
          '-qm',
          'fixture',
        );
        expect(git(dest, 'status', '--porcelain')).toBe('');
        // Special bits are not part of the sandbox copy contract.
        chmodSync(join(dest, 'run.sh'), 0o6755);
        chmodSync(join(dest, '.git/config'), 0o640);
        return { commit: git(dest, 'rev-parse', 'HEAD') };
      },
      async listWorkingTree() {
        return [];
      },
    };
    sandbox.files.write = async (path, content) => {
      if (!Buffer.isBuffer(content)) throw new Error('expected buffered copy');
      mkdirSync(join(path, '..'), { recursive: true });
      writeFileSync(path, content);
      // Invert both tracked files' execution bits in every copy.
      chmodSync(path, path.endsWith('/run.sh') ? 0o644 : 0o755);
    };
    const chmod = vi.fn(async (path: string, mode: number) => {
      chmodSync(path, mode);
    });
    const chmodMany = vi.fn(async (root: string, entries: readonly SandboxFileMode[]) => {
      serializeSandboxFileModes(root, entries);
      for (const entry of entries) chmodSync(entry.path, entry.mode);
    });
    sandbox.files.chmod = chmod;
    if (batch) sandbox.files.chmodMany = chmodMany;
    const strategy = new GitCloneStrategy({
      workspaceId: 'ws_modes',
      sessionId: 'ses_modes',
      worker,
      workDir,
      resolvePat: async () => 'fake-pat',
    });
    try {
      await strategy.activate(sandbox, {
        id: 'repo_modes',
        type: 'github_repository',
        url: 'https://github.com/org/repo',
        mountPath: `${mountPath}/`,
        access: 'read_write',
        gitCredentialId: 'cred',
        repoIdx: 0,
      });
      expect(statSync(join(mountPath, 'plain.txt')).mode & 0o7777).toBe(0o644);
      expect(statSync(join(mountPath, 'run.sh')).mode & 0o7777).toBe(0o755);
      expect(statSync(join(mountPath, '.git/config')).mode & 0o7777).toBe(0o640);
      expect(git(mountPath, 'config', '--get', 'core.fileMode')).toBe('true');
      expect(git(mountPath, 'status', '--porcelain')).toBe('');
      expect(execFileSync(join(mountPath, 'run.sh'), { encoding: 'utf8' })).toBe('executable');
      // Prove the fixture detects execution-bit drift instead of hiding it.
      chmodSync(join(mountPath, 'plain.txt'), 0o755);
      expect(git(mountPath, 'status', '--porcelain')).toContain('M plain.txt');
      if (batch) {
        expect(chmodMany).toHaveBeenCalledTimes(1);
        expect(chmod).not.toHaveBeenCalled();
      } else {
        expect(chmod).toHaveBeenCalled();
      }
    } finally {
      await sandbox.destroy();
    }
  });

  it.each([true, false])('rejects permission restoration failure (batch=%s)', async (batch) => {
    const sandbox = await runtime.acquire({});
    const failure = vi.fn().mockRejectedValue(new Error('chmod denied'));
    if (batch) sandbox.files.chmodMany = failure;
    else sandbox.files.chmod = failure;
    const workspaceId = `ws_chmod_failure_${batch}`;
    const strategy = new GitCloneStrategy({
      workspaceId,
      sessionId: 'ses_y',
      worker: makeFakeWorker({ 'README.md': 'x' }),
      workDir,
      resolvePat: async () => 'pat',
    });
    try {
      await expect(
        strategy.activate(sandbox, {
          id: 'repo',
          type: 'github_repository',
          url: 'https://github.com/org/repo',
          mountPath: '/workspace/repo',
          access: 'read_write',
          gitCredentialId: 'cred',
          repoIdx: 0,
        }),
      ).rejects.toThrow('chmod denied');
      expect(
        await readCounterCell({
          name: 'harness_git_clone_total',
          labels: { workspace_id: workspaceId, result: 'ok' },
        }),
      ).toBe(0);
      expect(
        await readCounterCell({
          name: 'harness_git_clone_total',
          labels: { workspace_id: workspaceId, result: 'error' },
        }),
      ).toBe(1);
    } finally {
      await sandbox.destroy();
    }
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
