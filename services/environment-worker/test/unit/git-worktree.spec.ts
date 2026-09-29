// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Worker-side git worktree operations, exercised against real `git`.
//
// `createWorktree` / `removeWorktree` / `validateBranchName` run actual
// `git worktree add` / `remove` / `branch -D` in a temp repository, so a
// regression in argv construction, repo-root resolution, or removal ordering
// fails loud here. The worker dials out to the registry and answers
// `worker.create_worktree` / `worker.remove_worktree` by calling these functions;
// the dispatch wiring is covered in worker-fileops.spec.ts.

import { afterEach, describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import {
  CreatedWorktree,
  WorktreeError,
  createWorktree,
  removeWorktree,
  validateBranchName,
} from '../../src/git-worktree.js';

// Deterministic identity + config so the tests don't depend on the developer's
// global git config (user.name / init.defaultBranch).
const GIT_ENV: Record<string, string> = {
  GIT_AUTHOR_NAME: 't',
  GIT_AUTHOR_EMAIL: 't@t',
  GIT_COMMITTER_NAME: 't',
  GIT_COMMITTER_EMAIL: 't@t',
};

const tmpDirs: string[] = [];

afterEach(() => {
  for (const dir of tmpDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

/** Run a git command in `repo`, raising on failure. */
function git(repo: string, ...args: string[]): void {
  execFileSync('git', args, { cwd: repo, env: { ...process.env, ...GIT_ENV } });
}

/** Return the checked-out branch name at `path` (main or linked worktree). */
function currentBranch(path: string): string {
  return execFileSync('git', ['rev-parse', '--abbrev-ref', 'HEAD'], {
    cwd: path,
    env: { ...process.env, ...GIT_ENV },
    encoding: 'utf-8',
  }).trim();
}

/** Return the commit sha that `ref` resolves to at `path`. */
function revParse(path: string, ref = 'HEAD'): string {
  return execFileSync('git', ['rev-parse', ref], {
    cwd: path,
    env: { ...process.env, ...GIT_ENV },
    encoding: 'utf-8',
  }).trim();
}

/** Return whether `branch` exists in `repo`. */
function branchExists(repo: string, branch: string): boolean {
  const out = execFileSync('git', ['branch', '--list', branch], {
    cwd: repo,
    env: { ...process.env, ...GIT_ENV },
    encoding: 'utf-8',
  }).trim();
  return out !== '';
}

/** Return how many worktrees are registered for `repo` (1 == main only). */
function worktreeCount(repo: string): number {
  const out = execFileSync('git', ['worktree', 'list', '--porcelain'], {
    cwd: repo,
    env: { ...process.env, ...GIT_ENV },
    encoding: 'utf-8',
  });
  // --porcelain emits one "worktree <path>" line per worktree.
  return (out.match(/^worktree /gm) ?? []).length;
}

/**
 * Create a one-commit git repo and return its resolved root. Resolve so
 * comparisons match git's realpath output (macOS /tmp -> /private/tmp).
 */
function makeGitRepo(): string {
  // Realpath the PARENT (it exists) so the child path is canonical (macOS /tmp ->
  // /private/tmp); realpath'ing the not-yet-created child would ENOENT.
  const parent = realpathSync(mkdtempSync(join(tmpdir(), 'orca-wt-')));
  tmpDirs.push(parent);
  const repo = join(parent, 'myrepo');
  mkdirSync(repo);
  git(repo, 'init', '-q', '-b', 'main');
  writeFileSync(join(repo, 'README.md'), 'hi');
  git(repo, 'add', '.');
  git(repo, 'commit', '-q', '-m', 'init');
  return repo;
}

describe('createWorktree', () => {
  it('places a new worktree at <repo>-worktrees/<branch> with the branch checked out', () => {
    const repo = makeGitRepo();
    const created = createWorktree({ repoPath: repo, branchName: 'feature/login' });
    const expected = join(dirname(repo), 'myrepo-worktrees', 'feature-login');
    // Path proves the sibling layout + slash->dash dir sanitization; a regression
    // in resolveWorktreePath would change this.
    expect(created.worktreePath).toBe(expected);
    expect(existsSync(created.worktreePath)).toBe(true);
    // The branch is actually checked out in the worktree (not just the dir made).
    expect(currentBranch(created.worktreePath)).toBe('feature/login');
    expect(created).toBeInstanceOf(CreatedWorktree);
  });

  it('resolves the repo root from a subdirectory (sibling of the root, not the subdir)', () => {
    const repo = makeGitRepo();
    const sub = join(repo, 'src');
    mkdirSync(sub);
    const created = createWorktree({ repoPath: sub, branchName: 'wip' });
    // Sibling of the repo ROOT, not of the picked subdir — proves the main
    // work-tree resolution is used rather than the raw repoPath.
    expect(created.worktreePath).toBe(join(dirname(repo), 'myrepo-worktrees', 'wip'));
  });

  it('anchors at the MAIN repo when created from inside a LINKED worktree', () => {
    // Resolving the repo root naively from a linked worktree would nest the new
    // worktree under it (…/feature-a-worktrees/feature-b). mainWorkTree resolves
    // to the main checkout so worktrees stay siblings (…/myrepo-worktrees/…) —
    // the fork-resume picker prefills a worktree as the source workspace, so this
    // is the common path.
    const repo = makeGitRepo();
    const first = createWorktree({ repoPath: repo, branchName: 'feature/a' });
    expect(first.worktreePath).toBe(join(dirname(repo), 'myrepo-worktrees', 'feature-a'));

    // Second worktree, requested from INSIDE the first (linked) worktree.
    const second = createWorktree({ repoPath: first.worktreePath, branchName: 'feature/b' });

    // Sibling of the MAIN repo, NOT nested under the first worktree. A regression
    // to a naive --show-toplevel would put it under feature-a-worktrees/ and this
    // fails.
    expect(second.worktreePath).toBe(join(dirname(repo), 'myrepo-worktrees', 'feature-b'));
    expect(second.worktreePath).not.toContain('feature-a-worktrees');
    expect(existsSync(second.worktreePath)).toBe(true);
    expect(currentBranch(second.worktreePath)).toBe('feature/b');
  });

  it('branches from the explicit base ref tip, not HEAD', () => {
    const repo = makeGitRepo();
    // Advance develop with its own commit so it differs from main — otherwise the
    // test would pass even if baseBranch were ignored (both resolve to the same
    // single commit).
    git(repo, 'checkout', '-q', '-b', 'develop');
    writeFileSync(join(repo, 'dev.txt'), 'dev-only');
    git(repo, 'add', '.');
    git(repo, 'commit', '-q', '-m', 'dev commit');
    git(repo, 'checkout', '-q', 'main');

    const created = createWorktree({
      repoPath: repo,
      branchName: 'from-develop',
      baseBranch: 'develop',
    });
    expect(currentBranch(created.worktreePath)).toBe('from-develop');
    // Points at develop's tip, not main's — proves baseBranch routed the new
    // branch to develop rather than falling back to HEAD.
    expect(revParse(created.worktreePath)).toBe(revParse(repo, 'develop'));
    expect(revParse(created.worktreePath)).not.toBe(revParse(repo, 'main'));
  });

  it('fails loud on an unresolvable base ref (after the best-effort fetch)', () => {
    const repo = makeGitRepo();
    // Proves ensureBaseResolvable rejects rather than silently branching from HEAD
    // when the requested base is missing.
    expect(() =>
      createWorktree({ repoPath: repo, branchName: 'x', baseBranch: 'nope-not-a-branch' }),
    ).toThrow(/base branch does not exist/);
  });

  it.each(['-f', '--exec-path'])(
    'rejects an option-like base ref (%s) instead of executing it',
    (optionLike) => {
      // baseBranch is registry-supplied and reaches `git rev-parse` /
      // `git worktree add` argv. An option-like value (e.g. "-f", which is
      // `git worktree add`'s --force) must be treated as an unresolvable rev, not
      // a flag. The ref-resolution pre-check + the --end-of-options terminators
      // together keep such a value from creating a worktree. A regression that let
      // "-f" through as a flag would build a worktree from the wrong base (and
      // force-create it) instead of failing — the count below would become 2.
      const repo = makeGitRepo();
      expect(() =>
        createWorktree({ repoPath: repo, branchName: 'from-flag', baseBranch: optionLike }),
      ).toThrow(WorktreeError);
      // Still only the main work tree — no linked worktree was added, proving git
      // treated the value as a (rejected) rev rather than a flag.
      expect(worktreeCount(repo)).toBe(1);
    },
  );

  it('fails loud with the friendly error when the branch name is already taken', () => {
    const repo = makeGitRepo();
    createWorktree({ repoPath: repo, branchName: 'dup' });
    // The pre-check catches the existing branch before git's raw error; we must
    // NOT silently reuse the existing worktree.
    expect(() => createWorktree({ repoPath: repo, branchName: 'dup' })).toThrow(/already exists/);
  });

  it('rejects a branch that exists WITHOUT a worktree (pre-check keys off branch existence)', () => {
    // Proves the pre-check keys off branch existence, not directory occupancy —
    // creating a worktree for a plain pre-existing branch would otherwise hit
    // git's raw error.
    const repo = makeGitRepo();
    git(repo, 'branch', 'preexisting');
    expect(() => createWorktree({ repoPath: repo, branchName: 'preexisting' })).toThrow(
      /preexisting.*already exists|already exists.*preexisting/,
    );
  });

  it('rejects a directory that is not a git repo', () => {
    const parent = mkdtempSync(join(tmpdir(), 'orca-wt-'));
    tmpDirs.push(parent);
    const plain = join(parent, 'plain');
    mkdirSync(plain);
    expect(() => createWorktree({ repoPath: plain, branchName: 'x' })).toThrow(
      /not a git repository/,
    );
  });
});

describe('removeWorktree', () => {
  it('removes the directory AND the branch when deleteBranch is true', () => {
    const repo = makeGitRepo();
    const created = createWorktree({ repoPath: repo, branchName: 'feature/login' });
    removeWorktree({
      worktreePath: created.worktreePath,
      branch: 'feature/login',
      deleteBranch: true,
    });
    // Directory gone (git worktree remove --force ran)…
    expect(existsSync(created.worktreePath)).toBe(false);
    // …and the branch deleted (git branch -D ran, after the worktree was removed —
    // git would refuse otherwise).
    expect(branchExists(repo, 'feature/login')).toBe(false);
  });

  it('removes the directory but keeps the branch when deleteBranch is false', () => {
    const repo = makeGitRepo();
    const created = createWorktree({ repoPath: repo, branchName: 'feature/keep' });
    removeWorktree({
      worktreePath: created.worktreePath,
      branch: 'feature/keep',
      deleteBranch: false,
    });
    expect(existsSync(created.worktreePath)).toBe(false);
    // Branch survives — only the checkout directory was removed.
    expect(branchExists(repo, 'feature/keep')).toBe(true);
  });

  it('fails loud when the worktree path does not exist', () => {
    const repo = makeGitRepo();
    expect(() =>
      removeWorktree({
        worktreePath: join(dirname(repo), 'myrepo-worktrees', 'ghost'),
        branch: null,
        deleteBranch: false,
      }),
    ).toThrow(/does not exist/);
  });

  it('validates the branch before ANY destructive step, not just before git branch -D', () => {
    // createWorktree validates its branch name and documents why; the delete
    // path takes a registry-supplied name into the same argv position and must
    // apply the same rule, so a malformed name is refused with a named
    // violation rather than handed to git.
    //
    // The ORDER is the point. Validating at the point of use rejected the name
    // only after `git worktree remove` had already succeeded, so the caller was
    // answered `failed` on a request that had half happened — and a retry then
    // failed differently, on a path that no longer existed.
    const repo = makeGitRepo();
    const created = createWorktree({ repoPath: repo, branchName: 'feature/validated' });
    expect(() =>
      removeWorktree({
        worktreePath: created.worktreePath,
        branch: '--exec-path=/tmp/evil',
        deleteBranch: true,
      }),
    ).toThrow(WorktreeError);
    // Nothing was destroyed: a rejected request leaves the worktree intact, so
    // `failed` and "the worktree is still there" agree with each other.
    expect(existsSync(created.worktreePath)).toBe(true);
    expect(branchExists(repo, 'feature/validated')).toBe(true);
  });

  it('does not validate the branch when only the directory is being removed', () => {
    // The up-front check must not fire when deleteBranch is false: the name
    // never reaches an argv, so an odd one is irrelevant to this request and
    // refusing it would break removals that used to work.
    const repo = makeGitRepo();
    const created = createWorktree({ repoPath: repo, branchName: 'feature/untouched' });
    removeWorktree({
      worktreePath: created.worktreePath,
      branch: '--exec-path=/tmp/evil',
      deleteBranch: false,
    });
    expect(existsSync(created.worktreePath)).toBe(false);
    expect(branchExists(repo, 'feature/untouched')).toBe(true);
  });
});

describe('createWorktree — filesystem failures are classified, not crashes', () => {
  it('raises a WorktreeError when the worktree parent directory cannot be created', () => {
    // The worktree parent (`<repo>-worktrees`) is created before `git worktree
    // add` runs. An unguarded mkdir throws a plain Error for a permanent,
    // actionable condition, which the dispatch would then have to treat as an
    // unexpected crash rather than a failed request. Here a regular FILE already
    // occupies the parent's path.
    const repo = makeGitRepo();
    writeFileSync(join(dirname(repo), 'myrepo-worktrees'), 'not a directory');

    let thrown: unknown;
    try {
      createWorktree({ repoPath: repo, branchName: 'feature/blocked' });
    } catch (exc) {
      thrown = exc;
    }

    expect(thrown).toBeInstanceOf(WorktreeError);
    expect((thrown as Error).message).toContain('myrepo-worktrees');
  });
});

describe('createWorktree — a failed fetch is named in the base-branch error', () => {
  it('includes the git fetch stderr when the base ref still cannot be resolved', () => {
    // An unresolvable base ref triggers one best-effort `git fetch`. With a
    // remote that does not exist, that fetch fails — and reporting only "base
    // branch does not exist" sends the user hunting for a typo when the real
    // cause (offline, no such remote, auth) is sitting in git's stderr.
    const repo = makeGitRepo();
    git(repo, 'remote', 'add', 'origin', join(dirname(repo), 'no-such-remote.git'));

    let thrown: unknown;
    try {
      createWorktree({ repoPath: repo, branchName: 'feature/base', baseBranch: 'origin/nope' });
    } catch (exc) {
      thrown = exc;
    }

    expect(thrown).toBeInstanceOf(WorktreeError);
    const message = (thrown as Error).message;
    expect(message).toContain('base branch does not exist: origin/nope');
    expect(message).toContain('git fetch also failed');
  });
});

describe('validateBranchName', () => {
  it.each([
    '',
    '-leading',
    'a..b',
    'a/.hidden',
    'x.lock',
    'x.lock/y',
    'a b',
    'a~b',
    'a:b',
    '/lead',
    'trail/',
  ])('rejects a branch name violating git ref-format: %j', (bad) => {
    expect(() => validateBranchName(bad)).toThrow(WorktreeError);
  });

  it.each(['feature/login', 'fix-123', 'a/b/c', 'release_2', 'v1.2'])(
    'accepts a well-formed branch name: %s',
    (good) => {
      expect(() => validateBranchName(good)).not.toThrow();
    },
  );
});
