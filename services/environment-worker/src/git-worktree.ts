// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Worker-side git worktree operations for session-start worktrees.
//
// Runs `git` (via argv arrays, never a shell) on the worker in response to
// `worker.create_worktree` / `worker.remove_worktree` frames. Branch names are
// validated against git ref-format rules before reaching argv. The worker dials
// out to the registry and calls these in response to the matching worker frames
// (see worker.ts dispatch); the result is mapped onto the
// `worker.*_worktree_result` frames.

import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, statSync } from 'node:fs';
import { dirname, isAbsolute, basename, join, resolve } from 'node:path';

// fetch/add can be slow on large repos; bound it so git can't hang the worker's
// tunnel loop.
const GIT_TIMEOUT_MS = 120_000;

// Max directory-collision suffixes (`-2` .. `-N`) before giving up.
const MAX_DIR_COLLISION_SUFFIX = 50;

// Chars git refuses in a ref: space, control chars, ~^:?*[\, DEL. (`..`,
// leading `-`/`.`, `/` edges, `.lock`, `@{` are checked separately.)
// eslint-disable-next-line no-control-regex
const INVALID_BRANCH_CHARS = /[\x00-\x20~^:?*[\\\x7f]/;

/**
 * Raised when a git worktree operation fails. The message is user-facing and is
 * surfaced verbatim in the `worker.*_worktree_result` frame's `error` field, e.g.
 * `"not a git repository: /tmp/x"`.
 */
export class WorktreeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'WorktreeError';
  }
}

/**
 * Validate a git branch name against `git check-ref-format` rules.
 *
 * @param name Proposed branch name, e.g. `"feature/login"`.
 * @throws WorktreeError if the name is empty or violates any ref-format rule;
 *   the message names the specific violation.
 */
export function validateBranchName(name: string): void {
  if (name === '') {
    throw new WorktreeError('branch name must not be empty');
  }
  if (name.startsWith('-')) {
    throw new WorktreeError(`branch name must not start with '-': ${quote(name)}`);
  }
  if (name.startsWith('/') || name.endsWith('/')) {
    throw new WorktreeError(`branch name must not start or end with '/': ${quote(name)}`);
  }
  if (name.endsWith('.')) {
    throw new WorktreeError(`branch name must not end with '.': ${quote(name)}`);
  }
  if (name.split('/').some((part) => part.endsWith('.lock'))) {
    throw new WorktreeError(
      `branch name path components must not end with '.lock': ${quote(name)}`,
    );
  }
  if (name.includes('..')) {
    throw new WorktreeError(`branch name must not contain '..': ${quote(name)}`);
  }
  if (name.includes('//')) {
    throw new WorktreeError(`branch name must not contain '//': ${quote(name)}`);
  }
  if (name.includes('@{')) {
    throw new WorktreeError(`branch name must not contain '@{': ${quote(name)}`);
  }
  if (name === '@') {
    throw new WorktreeError("branch name must not be '@'");
  }
  if (INVALID_BRANCH_CHARS.test(name)) {
    throw new WorktreeError(
      `branch name ${quote(name)} contains an invalid character; spaces, ` +
        'control characters, and any of ~ ^ : ? * [ \\ are not allowed',
    );
  }
  // No path component may start with '.' (e.g. ".hidden" or "a/.b").
  if (name.split('/').some((part) => part.startsWith('.'))) {
    throw new WorktreeError(`branch name path components must not start with '.': ${quote(name)}`);
  }
}

/**
 * Derive a single-segment directory name from a branch name. Slashes collapse to
 * `-` so the worktree lives in one directory, e.g. `"feature/login"` ->
 * `"feature-login"`.
 */
function sanitizeDirname(branchName: string): string {
  return stripSlashes(branchName).replace(/\//g, '-');
}

/** Strip leading/trailing `/` from a path-like string (a `strip("/")` of both ends). */
function stripSlashes(value: string): string {
  return value.replace(/^\/+/, '').replace(/\/+$/, '');
}

/** The captured outcome of a git invocation. */
interface GitResult {
  returncode: number;
  stdout: string;
  stderr: string;
}

/**
 * Run a git command, returning the completed result.
 *
 * @param args Git argv *after* `git`, e.g. `["rev-parse", "--show-toplevel"]`.
 *   Passed as an array so no shell parsing occurs.
 * @param cwd Working directory to run git in.
 * @throws WorktreeError if git is not installed, or the command exceeds the
 *   timeout.
 */
function runGit(args: string[], cwd: string): GitResult {
  const result = spawnSync('git', args, {
    cwd,
    encoding: 'utf-8',
    timeout: GIT_TIMEOUT_MS,
  });
  if (result.error !== undefined) {
    const code = (result.error as NodeJS.ErrnoException).code;
    if (code === 'ENOENT') {
      throw new WorktreeError('git is not installed on the worker');
    }
    if (code === 'ETIMEDOUT' || result.signal === 'SIGTERM') {
      throw new WorktreeError(`git command timed out after ${Math.round(GIT_TIMEOUT_MS / 1000)}s`);
    }
    throw new WorktreeError(`git command failed to run: ${result.error.message}`);
  }
  // A process killed by the timeout has no `error` on some platforms but a
  // non-null signal — surface it as a timeout rather than a bogus exit code.
  if (result.signal === 'SIGTERM') {
    throw new WorktreeError(`git command timed out after ${Math.round(GIT_TIMEOUT_MS / 1000)}s`);
  }
  return {
    returncode: result.status ?? 1,
    stdout: result.stdout ?? '',
    stderr: result.stderr ?? '',
  };
}

/**
 * Build a WorktreeError from a failed git command. Includes the exit code
 * (always present) and stderr when non-empty, so no invented "unknown error"
 * fallback is needed.
 *
 * @param label What failed, e.g. `"git worktree add failed"`.
 * @param result The completed result with a non-zero return code.
 */
function gitError(label: string, result: GitResult): WorktreeError {
  const detail = result.stderr.trim();
  const suffix = detail !== '' ? `: ${detail}` : '';
  return new WorktreeError(`${label} (exit ${result.returncode})${suffix}`);
}

/**
 * Resolve the MAIN work tree for any path inside a git repo.
 *
 * `git worktree list --porcelain` enumerates every work tree of the repository;
 * its first entry is always the main one (the checkout all linked worktrees
 * share). Run from `repoPath`, this resolves the same main work tree whether the
 * user picked the main checkout, a subdirectory, or a *linked worktree* — so a
 * new worktree is always created as a sibling of the MAIN repo (e.g.
 * `…/myrepo-worktrees/<branch>`) rather than nested inside a worktree the session
 * happened to start in.
 *
 * @param repoPath Absolute path inside a git repository — the directory the user
 *   picked.
 * @throws WorktreeError if `repoPath` is not a directory or not inside a git
 *   work tree.
 */
function mainWorkTree(repoPath: string): string {
  if (!isDirectory(repoPath)) {
    throw new WorktreeError(`path is not a directory: ${repoPath}`);
  }
  const result = runGit(['worktree', 'list', '--porcelain'], repoPath);
  if (result.returncode !== 0) {
    throw new WorktreeError(`not a git repository: ${repoPath}`);
  }
  for (const line of result.stdout.split('\n')) {
    // Porcelain format: the first record's `worktree <path>` line is the main
    // work tree; linked worktrees follow.
    if (line.startsWith('worktree ')) {
      return line.slice('worktree '.length).trim();
    }
  }
  throw new WorktreeError(`could not resolve main work tree for ${repoPath}`);
}

/**
 * Return whether a local branch already exists in the repo.
 *
 * @param repoRoot Absolute repo work-tree root.
 * @param branchName Branch name to check.
 */
function localBranchExists(repoRoot: string, branchName: string): boolean {
  return (
    runGit(['rev-parse', '--verify', '--quiet', `refs/heads/${branchName}`], repoRoot)
      .returncode === 0
  );
}

/**
 * Compute a collision-free sibling worktree directory path.
 *
 * Places the worktree at
 * `<parent-of-repo-root>/<repo-name>-worktrees/<sanitized-branch>`, appending a
 * numeric suffix if that path already exists on disk.
 *
 * @param repoRoot Absolute repo work-tree root.
 * @param branchName Validated branch name.
 * @throws WorktreeError if no free path is found within the suffix cap.
 */
function resolveWorktreePath(repoRoot: string, branchName: string): string {
  const baseDir = join(dirname(repoRoot), `${basename(repoRoot)}-worktrees`);
  const dirname_ = sanitizeDirname(branchName);
  let candidate = join(baseDir, dirname_);
  if (!existsSync(candidate)) {
    return candidate;
  }
  for (let suffix = 2; suffix <= MAX_DIR_COLLISION_SUFFIX; suffix += 1) {
    candidate = join(baseDir, `${dirname_}-${suffix}`);
    if (!existsSync(candidate)) {
      return candidate;
    }
  }
  throw new WorktreeError(
    `could not find a free worktree directory under ${baseDir} ` +
      `after ${MAX_DIR_COLLISION_SUFFIX} attempts`,
  );
}

/**
 * Make `baseBranch` resolvable, fetching once if needed.
 *
 * If the base ref doesn't resolve locally (e.g. a remote-tracking branch not yet
 * fetched), attempt a single `git fetch` and re-check. A fetch failure (offline)
 * is not fatal on its own — the subsequent re-check produces the user-facing
 * error.
 *
 * @param repoRoot Absolute repo work-tree root.
 * @param baseBranch Base ref the user requested, e.g. `"main"` or `"origin/main"`.
 * @throws WorktreeError if the base ref cannot be resolved even after a fetch.
 */
function ensureBaseResolvable(repoRoot: string, baseBranch: string): void {
  // --end-of-options forces git to treat the user-supplied baseBranch as a rev,
  // never an option, so a value like "--exec-path" can't inject a git flag
  // (argv-only, no shell). Note: a bare "--" would not work here — git rev-parse
  // treats args after "--" as pathspecs, not revs.
  if (
    runGit(['rev-parse', '--verify', '--quiet', '--end-of-options', baseBranch], repoRoot)
      .returncode === 0
  ) {
    return;
  }
  // Best-effort fetch from the default remote, then re-verify.
  const fetched = runGit(['fetch'], repoRoot);
  if (
    runGit(['rev-parse', '--verify', '--quiet', '--end-of-options', baseBranch], repoRoot)
      .returncode !== 0
  ) {
    // A failed fetch is the usual reason the re-check still can't see the ref
    // (offline, no remote, auth). Reporting only "base branch does not exist"
    // sends the user looking for a typo when the real cause is in git's stderr.
    let message = `base branch does not exist: ${baseBranch}`;
    if (fetched.returncode !== 0) {
      const detail = fetched.stderr.trim();
      message += `; git fetch also failed (exit ${fetched.returncode})${detail !== '' ? `: ${detail}` : ''}`;
    }
    throw new WorktreeError(message);
  }
}

/**
 * Result of a successful worktree creation. A class (not a plain object) so
 * callers can assert the type.
 */
export class CreatedWorktree {
  constructor(
    /** Absolute path of the created worktree directory. */
    readonly worktreePath: string,
    /** The branch checked out in the worktree. */
    readonly branch: string,
  ) {}
}

/** Arguments for {@link createWorktree}. */
export interface CreateWorktreeArgs {
  /** Absolute path inside the source repo — the directory the user picked. */
  repoPath: string;
  /** New branch to create and check out, e.g. `"feature/login"`. */
  branchName: string;
  /** Optional base ref, e.g. `"main"`. `null`/absent branches from `HEAD`. */
  baseBranch?: string | null;
}

/**
 * Create a git worktree with a new branch checked out.
 *
 * Resolves the repo root, picks a collision-free sibling directory, and runs
 * `git worktree add -b` (fetching once if `baseBranch` isn't locally resolvable).
 *
 * @throws WorktreeError if the branch name is invalid, the path is not a git
 *   repo, the base ref can't be resolved, or `git worktree add` fails (e.g. the
 *   branch already exists).
 */
export function createWorktree(args: CreateWorktreeArgs): CreatedWorktree {
  const { repoPath, branchName } = args;
  const baseBranch = args.baseBranch ?? null;
  validateBranchName(branchName);
  // Always create the worktree off the MAIN work tree, even when `repoPath` is
  // itself a linked worktree (e.g. the fork-resume picker prefilled a worktree as
  // the source). Otherwise the new worktree would nest under the picked worktree
  // (…/feature-worktrees/<branch>); resolving to the main repo keeps all
  // worktrees as siblings (…/myrepo-worktrees/<branch>).
  const repoRoot = mainWorkTree(repoPath);
  // Friendly pre-check before git's raw "branch already exists" error. We don't
  // reuse the existing worktree: two sessions sharing one working tree would
  // clobber each other.
  if (localBranchExists(repoRoot, branchName)) {
    throw new WorktreeError(
      `a branch named ${quote(branchName)} already exists; choose a different branch name`,
    );
  }
  if (baseBranch !== null) {
    ensureBaseResolvable(repoRoot, baseBranch);
  }
  const worktreePath = resolveWorktreePath(repoRoot, branchName);
  const worktreeParent = dirname(worktreePath);
  try {
    mkdirSync(worktreeParent, { recursive: true });
  } catch (exc) {
    // Classify the filesystem failure HERE, as a WorktreeError. Left unguarded
    // this throws a plain Error, which is a permanent, actionable condition
    // (EACCES on the repo's parent, a file already occupying the path, a
    // read-only mount) that the caller would otherwise have to report as an
    // unexpected crash rather than a failed request.
    throw new WorktreeError(
      `could not create the worktree parent directory ${worktreeParent}: ${errMessage(exc)}`,
    );
  }

  const addArgs = ['worktree', 'add', '-b', branchName, worktreePath];
  if (baseBranch !== null) {
    // --end-of-options: treat baseBranch as a rev, never a git flag, so a
    // user-supplied value starting with '-' can't inject an option.
    addArgs.push('--end-of-options', baseBranch);
  }
  const result = runGit(addArgs, repoRoot);
  if (result.returncode !== 0) {
    throw gitError('git worktree add failed', result);
  }
  return new CreatedWorktree(worktreePath, branchName);
}

/**
 * Find the main repository work tree for a linked worktree.
 *
 * Uses `git rev-parse --git-common-dir` (which points at the shared `.git` of the
 * main work tree) and returns that directory's parent. Run from inside the
 * worktree so the relative result resolves correctly.
 *
 * @param worktreePath Absolute path of a linked worktree.
 * @throws WorktreeError if `worktreePath` is missing or not part of a git repo.
 */
function mainRepoForWorktree(worktreePath: string): string {
  if (!existsSync(worktreePath)) {
    throw new WorktreeError(`worktree path does not exist: ${worktreePath}`);
  }
  const result = runGit(['rev-parse', '--git-common-dir'], worktreePath);
  if (result.returncode !== 0) {
    throw new WorktreeError(`not a git worktree: ${worktreePath}`);
  }
  let commonDir = result.stdout.trim();
  if (!isAbsolute(commonDir)) {
    commonDir = resolve(worktreePath, commonDir);
  }
  return dirname(commonDir);
}

/** Arguments for {@link removeWorktree}. */
export interface RemoveWorktreeArgs {
  /** Absolute path of the worktree to remove (the stored session workspace). */
  worktreePath: string;
  /** Branch to delete when `deleteBranch` is `true`. `null`/absent skips it. */
  branch?: string | null;
  /** When `true`, run `git branch -D` on `branch` after removing the directory. */
  deleteBranch?: boolean;
}

/**
 * Remove a git worktree and optionally delete its branch.
 *
 * Removes the directory with `--force`, then (if requested) deletes the branch —
 * in that order, since git refuses to delete a branch still checked out in a
 * linked worktree. `git worktree remove` refuses to remove the main work tree.
 *
 * Every input is validated BEFORE the first destructive command. Validating the
 * branch name only at its point of use meant a malformed one was rejected after
 * `git worktree remove` had already succeeded: the caller was told the whole
 * request `failed` while the worktree was in fact gone, so a retry then failed
 * again on a path that no longer exists.
 *
 * @throws WorktreeError if the worktree path is missing/invalid, the branch name
 *   is malformed, or a git command fails.
 */
export function removeWorktree(args: RemoveWorktreeArgs): void {
  const { worktreePath } = args;
  const branch = args.branch ?? null;
  const deleteBranch = args.deleteBranch ?? false;
  const mainRepo = mainRepoForWorktree(worktreePath);
  if (deleteBranch && branch !== null) {
    // The same ref-format rules createWorktree applies to its own argv, checked
    // up front so a named violation costs nothing.
    validateBranchName(branch);
  }
  // --end-of-options for the same reason createWorktree uses it: both values are
  // registry-supplied, and without it one starting with '-' is read by git as a
  // flag rather than as the operand (argv-only, so there is no shell to worry
  // about — the hazard is git's own option parsing).
  const removeResult = runGit(
    ['worktree', 'remove', '--force', '--end-of-options', worktreePath],
    mainRepo,
  );
  if (removeResult.returncode !== 0) {
    throw gitError('git worktree remove failed', removeResult);
  }
  if (deleteBranch && branch !== null) {
    const branchResult = runGit(['branch', '-D', '--end-of-options', branch], mainRepo);
    if (branchResult.returncode !== 0) {
      throw gitError('git branch -D failed', branchResult);
    }
  }
}

/** Whether `path` exists and is a directory (no symlink resolution beyond statSync). */
function isDirectory(path: string): boolean {
  try {
    return statSync(path).isDirectory();
  } catch {
    return false;
  }
}

/** Single-quote a value for error messages, e.g. `feature/x` -> `'feature/x'`. */
function quote(value: string): string {
  return `'${value}'`;
}

/** Best-effort message extraction from an unknown thrown value. */
function errMessage(exc: unknown): string {
  return exc instanceof Error ? exc.message : String(exc);
}
