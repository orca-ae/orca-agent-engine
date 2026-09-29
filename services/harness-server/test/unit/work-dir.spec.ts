// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, rmSync, statSync, writeFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { WorkDirManager } from '../../src/git/work-dir.js';

describe('WorkDirManager', () => {
  let baseDir: string;
  let manager: WorkDirManager;

  beforeEach(() => {
    baseDir = mkdtempSync(join(tmpdir(), 'work-dir-test-'));
    manager = new WorkDirManager({ baseDir });
  });

  afterEach(() => {
    try {
      rmSync(baseDir, { recursive: true, force: true });
    } catch {
      // ignore
    }
  });

  it('acquire creates the per-repo dir and returns its absolute path', async () => {
    const dir = await manager.acquire('ws_test', 'ses_test', 0);
    expect(dir).toBe(join(baseDir, 'sessions', 'ws_test', 'ses_test', 'repo-0'));
    expect(statSync(dir).isDirectory()).toBe(true);
  });

  it('acquire is idempotent (same triplet returns same path)', async () => {
    const a = await manager.acquire('ws_x', 'ses_y', 1);
    const b = await manager.acquire('ws_x', 'ses_y', 1);
    expect(a).toBe(b);
    expect(statSync(a).isDirectory()).toBe(true);
  });

  it('different repoIdx yields different dirs under the same session', async () => {
    const a = await manager.acquire('ws_x', 'ses_y', 0);
    const b = await manager.acquire('ws_x', 'ses_y', 1);
    expect(a).not.toBe(b);
    expect(a.endsWith('repo-0')).toBe(true);
    expect(b.endsWith('repo-1')).toBe(true);
  });

  it("releaseSession rm-rf's the entire session subtree", async () => {
    const dir = await manager.acquire('ws_x', 'ses_y', 0);
    writeFileSync(join(dir, 'sentinel.txt'), 'hello');
    await manager.releaseSession('ws_x', 'ses_y');
    expect(existsSync(dir)).toBe(false);
    expect(existsSync(join(baseDir, 'sessions', 'ws_x', 'ses_y'))).toBe(false);
  });

  it('releaseSession is idempotent (double-release does not throw)', async () => {
    await manager.releaseSession('ws_x', 'ses_y');
    await manager.releaseSession('ws_x', 'ses_y');
    // No throw = pass.
  });

  it('releaseSession on a fresh workspace (no acquire) is a no-op', async () => {
    await manager.releaseSession('ws_never_existed', 'ses_either');
    // No throw = pass.
  });

  it("releaseWorkspace rm-rf's every session under the workspace", async () => {
    await manager.acquire('ws_z', 'ses_a', 0);
    await manager.acquire('ws_z', 'ses_b', 0);
    await manager.releaseWorkspace('ws_z');
    expect(existsSync(join(baseDir, 'sessions', 'ws_z'))).toBe(false);
  });

  it('releaseWorkspace does not affect other workspaces', async () => {
    await manager.acquire('ws_a', 'ses_x', 0);
    const dirB = await manager.acquire('ws_b', 'ses_x', 0);
    await manager.releaseWorkspace('ws_a');
    expect(existsSync(dirB)).toBe(true);
  });

  it('rejects traversal before acquire or recursive cleanup can escape the root', async () => {
    const sentinel = join(baseDir, 'sentinel.txt');
    writeFileSync(sentinel, 'keep');

    await expect(manager.acquire('../other', 'ses_x', 0)).rejects.toThrow(/path segment/);
    await expect(manager.releaseSession('ws_x', '../../other')).rejects.toThrow(/path segment/);
    await expect(manager.releaseWorkspace('..')).rejects.toThrow(/path segment/);

    expect(existsSync(sentinel)).toBe(true);
  });

  it('rejects invalid repository indexes', async () => {
    await expect(manager.acquire('ws_x', 'ses_x', -1)).rejects.toThrow(/repository index/);
    await expect(manager.acquire('ws_x', 'ses_x', Number.NaN)).rejects.toThrow(/repository index/);
  });
});
