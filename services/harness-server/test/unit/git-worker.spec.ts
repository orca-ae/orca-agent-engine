// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import simpleGit from 'simple-git';
import { makeGitWorker } from '../../src/git/git-worker.js';

describe('GitWorker', () => {
  let remoteDir: string;
  let workspaceDir: string;
  let cloneDest: string;
  let seedBranch: string;

  beforeAll(async () => {
    remoteDir = mkdtempSync(join(tmpdir(), 'git-remote-'));
    workspaceDir = mkdtempSync(join(tmpdir(), 'git-seed-'));
    cloneDest = join(tmpdir(), `git-clone-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`);

    // Bare remote.
    await simpleGit(remoteDir).init(true);

    // Seed a commit in the workspace and push.
    const seed = simpleGit(workspaceDir);
    await seed.init();
    await seed.addConfig('user.email', 'orca@test.local');
    await seed.addConfig('user.name', 'Orca Test');
    writeFileSync(join(workspaceDir, 'README.md'), '# orca-test\n');
    await seed.add('README.md');
    await seed.commit('initial');
    // Detect default branch (master vs main); newer git defaults to main.
    seedBranch = (await seed.revparse(['--abbrev-ref', 'HEAD'])).trim();
    await seed.addRemote('origin', remoteDir);
    await seed.push('origin', seedBranch);
  });

  afterAll(() => {
    for (const d of [remoteDir, workspaceDir, cloneDest]) {
      try {
        rmSync(d, { recursive: true, force: true });
      } catch {
        // ignore
      }
    }
  });

  it('clones a local file:// remote with --depth=1', async () => {
    const worker = makeGitWorker();
    const { commit } = await worker.cloneInto({
      url: `file://${remoteDir}`,
      dest: cloneDest,
      shallow: true,
      // Some git builds reject `--filter=blob:none` against local file:// remotes
      // (partial-clone protocol isn't supported on local transport in all builds).
      filterBlobs: false,
    });
    expect(commit).toMatch(/^[0-9a-f]{40}$/);
  });

  it('resets the remote URL to the bare form after clone (PAT not in .git/config)', async () => {
    const remoteUrl = await simpleGit(cloneDest).raw(['config', '--get', 'remote.origin.url']);
    expect(remoteUrl.trim()).toBe(`file://${remoteDir}`);
  });

  it('lists tracked files in the working tree', async () => {
    const worker = makeGitWorker();
    const files = await worker.listWorkingTree(cloneDest);
    expect(files).toContain('README.md');
    expect(files.every((f) => !f.startsWith('.git/'))).toBe(true);
  });

  it('clones a specific branch via the ref arg', async () => {
    const branchClone = join(tmpdir(), `git-clone-branch-${Date.now()}`);
    try {
      const worker = makeGitWorker();
      await worker.cloneInto({
        url: `file://${remoteDir}`,
        dest: branchClone,
        ref: seedBranch,
        shallow: true,
        filterBlobs: false,
      });
      const log = await simpleGit(branchClone).log({ maxCount: 1 });
      expect(log.latest?.hash).toMatch(/^[0-9a-f]{40}$/);
    } finally {
      rmSync(branchClone, { recursive: true, force: true });
    }
  });

  it('refuses to embed a PAT in a non-HTTPS clone URL', async () => {
    const worker = makeGitWorker();
    await expect(
      worker.cloneInto({
        url: 'http://github.com/orca/test',
        dest: join(tmpdir(), `git-clone-http-${Date.now()}`),
        pat: 'fake-pat-for-smoke-test',
      }),
    ).rejects.toThrow('refusing to send Git credentials over a non-HTTPS repository URL');
  });
});
