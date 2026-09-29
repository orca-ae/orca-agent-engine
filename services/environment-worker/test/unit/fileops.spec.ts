// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Worker-side filesystem handlers, exercised against a real temp filesystem.
//
// `handleStat` / `handleListDir` / `handleCreateDir` back the registry's
// workspace-selection round-trips (stat a picked path, list a directory for the
// picker, make a new folder). They run against the real OS filesystem here so a
// regression in symlink following, ~ expansion, error collapsing, or pagination
// fails loud. The worker dials out to the registry and answers the matching worker
// frames by calling these; the dispatch wiring is covered in
// worker-fileops.spec.ts.

import { afterEach, describe, expect, it } from 'vitest';
import {
  chmodSync,
  mkdirSync,
  mkdtempSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  WorkerFrameKind,
  type WorkerCreateDirResultFrame,
  type WorkerListDirResultFrame,
  type WorkerStatResultFrame,
} from '@orca/harness-tunnel';
import { handleCreateDir, handleListDir, handleStat } from '../../src/fileops.js';

const tmpDirs: string[] = [];
const origHome = process.env.HOME;

afterEach(() => {
  for (const dir of tmpDirs.splice(0)) {
    // Restore any permissions the test stripped so cleanup doesn't fail.
    try {
      chmodSync(dir, 0o700);
    } catch {
      // ignore
    }
    rmSync(dir, { recursive: true, force: true });
  }
  if (origHome === undefined) {
    delete process.env.HOME;
  } else {
    process.env.HOME = origHome;
  }
});

/** A fresh resolved temp dir (realpath so it matches the handler's canonical paths). */
function makeTmp(): string {
  const dir = realpathSync(mkdtempSync(join(tmpdir(), 'orca-fs-')));
  tmpDirs.push(dir);
  return dir;
}

// ── worker.stat handler ───────────────────────────────────

describe('handleStat', () => {
  it('reports exists:true, type:directory and the realpath for an existing directory', () => {
    // These three fields drive the registry-side validation contract (path
    // exists, is a directory, canonicalPath is what gets stored). If any one is
    // wrong, every worker-launched session would be rejected or land in the wrong
    // directory.
    const tmp = makeTmp();
    const target = join(tmp, 'project');
    mkdirSync(target);

    const result: WorkerStatResultFrame = handleStat({
      kind: WorkerFrameKind.Stat,
      requestId: 'r_dir',
      path: target,
    });

    expect(result.kind).toBe(WorkerFrameKind.StatResult);
    expect(result.status).toBe('ok');
    expect(result.exists).toBe(true);
    expect(result.type).toBe('directory');
    expect(result.canonicalPath).toBe(realpathSync(target));
    expect(result.error ?? null).toBeNull();
  });

  it('reports type:file for a regular file', () => {
    // The validator rejects non-directories at session create — without `type` it
    // would happily store a file path as the workspace and the runner would fail
    // on chdir.
    const tmp = makeTmp();
    const target = join(tmp, 'README.md');
    writeFileSync(target, 'hi');

    const result = handleStat({ kind: WorkerFrameKind.Stat, requestId: 'r_file', path: target });

    expect(result.exists).toBe(true);
    expect(result.type).toBe('file');
  });

  it('follows a symlink to a directory and canonicalizes to the target realpath', () => {
    // The load-bearing case for "symlinks cannot smuggle a workspace out of the
    // agent's boundary". If canonicalPath were the symlink path (not the target),
    // a `cwd: ~/foo` boundary check would pass for ~/foo/link -> /etc and the
    // runner would end up in /etc.
    const tmp = makeTmp();
    const realDir = join(tmp, 'real');
    mkdirSync(realDir);
    const link = join(tmp, 'link');
    symlinkSync(realDir, link);

    const result = handleStat({ kind: WorkerFrameKind.Stat, requestId: 'r_sym', path: link });

    expect(result.exists).toBe(true);
    expect(result.type).toBe('directory');
    expect(result.canonicalPath).toBe(realpathSync(realDir));
  });

  it('returns exists:false for a dangling symlink', () => {
    // Without this collapse, the runner would later fail on chdir with a confusing
    // error. The design defines this as part of the exists/not-exists contract.
    const tmp = makeTmp();
    const dangling = join(tmp, 'dangling');
    symlinkSync(join(tmp, 'does_not_exist'), dangling);

    const result = handleStat({
      kind: WorkerFrameKind.Stat,
      requestId: 'r_dangling',
      path: dangling,
    });

    expect(result.status).toBe('ok');
    expect(result.exists).toBe(false);
    expect(result.canonicalPath ?? null).toBeNull();
  });

  it('returns exists:false (not status failed) for a missing path', () => {
    // The design treats non-existence as a normal answer, not an error — so the
    // route can return a 400 with a clean "doesn't exist" message instead of a 500.
    const tmp = makeTmp();
    const missing = join(tmp, 'does_not_exist');

    const result = handleStat({
      kind: WorkerFrameKind.Stat,
      requestId: 'r_missing',
      path: missing,
    });

    expect(result.status).toBe('ok');
    expect(result.exists).toBe(false);
    expect(result.canonicalPath ?? null).toBeNull();
    expect(result.error ?? null).toBeNull();
  });

  it('collapses a permission-denied (EACCES) child path to exists:false', () => {
    // v1 collapses ENOENT and EACCES into a single "not reachable" answer. If we
    // ever distinguish them the wire shape changes (a `readable` field gets
    // added); this pins the v1 contract and fails loud on a silent regression.
    const tmp = makeTmp();
    const locked = join(tmp, 'locked');
    mkdirSync(locked);
    // chmod 0 so even the owner cannot stat children; ask for a child to force
    // EACCES rather than ENOENT (the directory itself is still stat-able).
    chmodSync(locked, 0);
    const child = join(locked, 'child');
    const result = handleStat({ kind: WorkerFrameKind.Stat, requestId: 'r_eacces', path: child });
    // Many filesystems/kernels return EACCES from stat on the child of a
    // 0-permission dir; some ultra-permissive setups (root, certain CI sandboxes)
    // short-circuit to ENOENT. Both collapse to exists:false per the design.
    expect(result.status).toBe('ok');
    expect(result.exists).toBe(false);
    chmodSync(locked, 0o700);
  });

  it('expands ~ against the worker process owner home', () => {
    // The worker (not the registry) is the source of truth for ~. If the handler
    // skipped expansion, agent specs with `cwd: ~/foo` would never resolve and
    // validation would fail on every worker.
    const tmp = makeTmp();
    process.env.HOME = tmp;
    const target = join(tmp, 'subdir');
    mkdirSync(target);

    const result = handleStat({
      kind: WorkerFrameKind.Stat,
      requestId: 'r_tilde',
      path: '~/subdir',
    });

    expect(result.exists).toBe(true);
    expect(result.type).toBe('directory');
    expect(result.canonicalPath).toBe(realpathSync(target));
  });

  it("uses the clean POSIX strerror (not Node's errno-prefixed message) on status:failed", () => {
    // An unexpected stat error (ENAMETOOLONG — not ENOENT/EACCES/EPERM, so it does
    // NOT collapse to exists:false) is the only path that surfaces status:failed.
    // The error string must be the clean strerror ("name too long"), not Node's
    // verbose "ENAMETOOLONG: name too long, stat '/…'" form (errno prefix + path
    // leak).
    const tooLong = `/${'x'.repeat(300)}`;

    const result = handleStat({ kind: WorkerFrameKind.Stat, requestId: 'r_long', path: tooLong });

    expect(result.status).toBe('failed');
    const error = result.error ?? '';
    expect(error).toBe('stat failed: name too long');
    // No errno-code prefix and no path leak (the Node-message artifacts).
    expect(error).not.toContain('ENAMETOOLONG');
    expect(error).not.toContain(tooLong);
  });
});

// ── worker.list_dir handler ───────────────────────────────

describe('handleListDir', () => {
  it('returns entries sorted by name with type + bytes + modifiedAt', () => {
    const tmp = makeTmp();
    mkdirSync(join(tmp, 'b-dir'));
    writeFileSync(join(tmp, 'a-file.txt'), 'hello');

    const result: WorkerListDirResultFrame = handleListDir({
      kind: WorkerFrameKind.ListDir,
      requestId: 'r1',
      path: tmp,
      limit: 20,
      after: null,
      before: null,
    });

    expect(result.kind).toBe(WorkerFrameKind.ListDirResult);
    expect(result.status).toBe('ok');
    const entries = result.entries ?? [];
    expect(entries.map((e) => e.name)).toEqual(['a-file.txt', 'b-dir']);
    const file = entries[0]!;
    expect(file.type).toBe('file');
    expect(file.bytes).toBe(5);
    expect(typeof file.modifiedAt).toBe('number');
    const dir = entries[1]!;
    expect(dir.type).toBe('directory');
    expect(dir.bytes).toBeNull();
    expect(dir.path).toBe(join(tmp, 'b-dir'));
  });

  it('returns status:ok with an error message (not failed) for a missing path', () => {
    // The route maps a non-empty error on a missing path to a 404. Surfacing
    // status:failed instead would 502 on every missing-directory navigation.
    const tmp = makeTmp();
    const missing = join(tmp, 'does_not_exist');

    const result = handleListDir({
      kind: WorkerFrameKind.ListDir,
      requestId: 'r2',
      path: missing,
      limit: 20,
      after: null,
      before: null,
    });

    expect(result.status).toBe('ok');
    expect(result.error ?? '').toContain('does not exist');
  });

  it('returns "not a directory" for a regular file', () => {
    const tmp = makeTmp();
    const target = join(tmp, 'file.txt');
    writeFileSync(target, 'hi');

    const result = handleListDir({
      kind: WorkerFrameKind.ListDir,
      requestId: 'r3',
      path: target,
      limit: 20,
      after: null,
      before: null,
    });

    expect(result.status).toBe('ok');
    expect(result.error ?? '').toContain('not a directory');
  });

  it('follows a symlink to a directory (same posture as stat)', () => {
    const tmp = makeTmp();
    const realDir = join(tmp, 'real');
    mkdirSync(realDir);
    writeFileSync(join(realDir, 'inside.txt'), 'x');
    const link = join(tmp, 'link');
    symlinkSync(realDir, link);

    const result = handleListDir({
      kind: WorkerFrameKind.ListDir,
      requestId: 'r4',
      path: link,
      limit: 20,
      after: null,
      before: null,
    });

    expect(result.status).toBe('ok');
    expect((result.entries ?? []).map((e) => e.name)).toEqual(['inside.txt']);
  });

  it('skips a single dangling symlink per-entry rather than failing the listing', () => {
    // Matches the runner's list_dir posture: a single broken entry doesn't fail
    // the whole listing.
    const tmp = makeTmp();
    writeFileSync(join(tmp, 'real.txt'), 'x');
    symlinkSync(join(tmp, 'missing-target'), join(tmp, 'broken-link'));

    const result = handleListDir({
      kind: WorkerFrameKind.ListDir,
      requestId: 'r5',
      path: tmp,
      limit: 20,
      after: null,
      before: null,
    });

    expect(result.status).toBe('ok');
    // The broken symlink is skipped; the real entry still surfaces.
    expect((result.entries ?? []).map((e) => e.name)).toEqual(['real.txt']);
  });

  it('expands ~ against the worker process owner home', () => {
    const tmp = makeTmp();
    process.env.HOME = tmp;
    const sub = join(tmp, 'subdir');
    mkdirSync(sub);
    writeFileSync(join(sub, 'f.txt'), 'x');

    const result = handleListDir({
      kind: WorkerFrameKind.ListDir,
      requestId: 'r6',
      path: '~/subdir',
      limit: 20,
      after: null,
      before: null,
    });

    expect(result.status).toBe('ok');
    expect((result.entries ?? []).map((e) => e.name)).toEqual(['f.txt']);
  });

  it('paginates forward past an `after` cursor', () => {
    const tmp = makeTmp();
    for (const name of ['a', 'b', 'c', 'd', 'e']) {
      writeFileSync(join(tmp, name), 'x');
    }
    const result = handleListDir({
      kind: WorkerFrameKind.ListDir,
      requestId: 'r7',
      path: tmp,
      limit: 2,
      after: join(tmp, 'b'),
      before: null,
    });
    expect(result.status).toBe('ok');
    // Entries strictly after "b": c, d (limit 2) with more remaining.
    expect((result.entries ?? []).map((e) => e.name)).toEqual(['c', 'd']);
    expect(result.hasMore).toBe(true);
  });

  it('paginates backward before a `before` cursor returning the previous page', () => {
    const tmp = makeTmp();
    for (const name of ['a', 'b', 'c', 'd', 'e']) {
      writeFileSync(join(tmp, name), 'x');
    }
    // before "d" with limit 2 returns the page ending just before d: b, c.
    const middle = handleListDir({
      kind: WorkerFrameKind.ListDir,
      requestId: 'r7b',
      path: tmp,
      limit: 2,
      after: null,
      before: join(tmp, 'd'),
    });
    expect(middle.status).toBe('ok');
    expect((middle.entries ?? []).map((e) => e.name)).toEqual(['b', 'c']);
    // More entries remain before this page (a), so hasMore is set.
    expect(middle.hasMore).toBe(true);

    // before "c" with limit large returns everything before c: a, b — nothing more
    // before, so hasMore is false.
    const first = handleListDir({
      kind: WorkerFrameKind.ListDir,
      requestId: 'r7c',
      path: tmp,
      limit: 10,
      after: null,
      before: join(tmp, 'c'),
    });
    expect(first.status).toBe('ok');
    expect((first.entries ?? []).map((e) => e.name)).toEqual(['a', 'b']);
    expect(first.hasMore).toBe(false);
  });

  it('reports hasMore:false when the last page fits the limit', () => {
    const tmp = makeTmp();
    for (const name of ['a', 'b', 'c']) {
      writeFileSync(join(tmp, name), 'x');
    }
    const result = handleListDir({
      kind: WorkerFrameKind.ListDir,
      requestId: 'r8',
      path: tmp,
      limit: 10,
      after: null,
      before: null,
    });
    expect(result.status).toBe('ok');
    expect((result.entries ?? []).map((e) => e.name)).toEqual(['a', 'b', 'c']);
    expect(result.hasMore).toBe(false);
  });
});

// ── worker.create_dir handler ─────────────────────────────

describe('handleCreateDir', () => {
  it('creates the directory and returns its absolute path', () => {
    // The picker's "New folder" happy path — the returned path is what the picker
    // navigates into afterward.
    const tmp = makeTmp();
    const target = join(tmp, 'new-app');

    const result: WorkerCreateDirResultFrame = handleCreateDir({
      kind: WorkerFrameKind.CreateDir,
      requestId: 'm1',
      path: target,
    });

    expect(result.kind).toBe(WorkerFrameKind.CreateDirResult);
    expect(result.status).toBe('ok');
    expect(result.error ?? null).toBeNull();
    expect(result.path).toBe(target);
    expect(realpathSync(target)).toBe(target);
  });

  it('creates missing parent directories', () => {
    // Lets the picker accept a nested name like a/b/c in one go.
    const tmp = makeTmp();
    const target = join(tmp, 'a', 'b', 'c');

    const result = handleCreateDir({
      kind: WorkerFrameKind.CreateDir,
      requestId: 'm2',
      path: target,
    });

    expect(result.status).toBe('ok');
    expect(realpathSync(target)).toBe(target);
  });

  it('returns status:ok with "directory already exists" (not failed) for an existing dir', () => {
    // The route maps a non-empty error to a 409 so the picker shows the message
    // inline; surfacing failed would 502.
    const tmp = makeTmp();
    const existing = join(tmp, 'dup');
    mkdirSync(existing);

    const result = handleCreateDir({
      kind: WorkerFrameKind.CreateDir,
      requestId: 'm3',
      path: existing,
    });

    expect(result.status).toBe('ok');
    expect(result.error).toBe('directory already exists');
    expect(result.path ?? null).toBeNull();
  });

  it('reports a file (not a directory) when the leaf path is a regular file', () => {
    // A plain mkdir raises EEXIST for both an existing directory and an existing
    // file; the handler must distinguish them so the picker doesn't mislabel "a
    // file is in the way" as "directory already exists".
    const tmp = makeTmp();
    const aFile = join(tmp, 'taken');
    writeFileSync(aFile, 'hi');

    const result = handleCreateDir({
      kind: WorkerFrameKind.CreateDir,
      requestId: 'm3b',
      path: aFile,
    });

    expect(result.status).toBe('ok');
    expect(result.error).toBe('a file already exists at that path');
    expect(result.path ?? null).toBeNull();
  });

  it('returns a clean error when a parent path component is a file', () => {
    const tmp = makeTmp();
    const aFile = join(tmp, 'file.txt');
    writeFileSync(aFile, 'hi');
    const target = join(aFile, 'child');

    const result = handleCreateDir({
      kind: WorkerFrameKind.CreateDir,
      requestId: 'm4',
      path: target,
    });

    expect(result.status).toBe('ok');
    expect(result.error ?? '').toContain('not a directory');
    expect(result.path ?? null).toBeNull();
  });

  it('reports "directory already exists" for a symlink whose target is a directory', () => {
    // A recursive mkdir would EEXIST on the symlink name; the classification must
    // FOLLOW the link (a symlink-following isdir check) so a symlink→directory
    // reads as an existing directory, not "a file is in the way". A no-follow lstat
    // would misclassify it as a non-directory and mislabel the message.
    const tmp = makeTmp();
    const realDir = join(tmp, 'real');
    mkdirSync(realDir);
    const link = join(tmp, 'link-to-dir');
    symlinkSync(realDir, link);

    const result = handleCreateDir({
      kind: WorkerFrameKind.CreateDir,
      requestId: 'm3c',
      path: link,
    });

    expect(result.status).toBe('ok');
    expect(result.error).toBe('directory already exists');
    expect(result.path ?? null).toBeNull();
  });

  it('reports "a file already exists at that path" for a broken symlink', () => {
    // A symlink whose target is missing still occupies the name (no-follow
    // existence), so it is "taken"; following it (a symlink-following isdir check)
    // yields false, so it reads as a file in the way.
    const tmp = makeTmp();
    const broken = join(tmp, 'broken-link');
    symlinkSync(join(tmp, 'missing-target'), broken);

    const result = handleCreateDir({
      kind: WorkerFrameKind.CreateDir,
      requestId: 'm3d',
      path: broken,
    });

    expect(result.status).toBe('ok');
    expect(result.error).toBe('a file already exists at that path');
    expect(result.path ?? null).toBeNull();
  });

  it('returns a normalized absolute path (collapses .. and // during absolute-path resolution)', () => {
    // Absolute-path resolution always normalizes — even an already-absolute input.
    // The returned path is what the picker navigates into, so an unresolved
    // ..//. segment would send it to a confusing display path. Pin the
    // normalization on an absolute input carrying redundant segments.
    const tmp = makeTmp();
    const messy = join(tmp, 'a', 'b', '..', 'c') + '//';
    const normalized = join(tmp, 'a', 'c');

    const result = handleCreateDir({
      kind: WorkerFrameKind.CreateDir,
      requestId: 'm6',
      path: messy,
    });

    expect(result.status).toBe('ok');
    expect(result.path).toBe(normalized);
    expect(realpathSync(normalized)).toBe(normalized);
  });

  it('expands ~ against the worker process owner home', () => {
    // The worker owns ~ resolution; without expansion ~/scratch would become a
    // literal ~ subdir of the process cwd.
    const tmp = makeTmp();
    process.env.HOME = tmp;

    const result = handleCreateDir({
      kind: WorkerFrameKind.CreateDir,
      requestId: 'm5',
      path: '~/scratch',
    });

    expect(result.status).toBe('ok');
    expect(result.path).toBe(join(tmp, 'scratch'));
    expect(realpathSync(join(tmp, 'scratch'))).toBe(join(tmp, 'scratch'));
  });
});
