// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Worker-side filesystem handlers backing workspace selection.
//
// The registry sends a connected worker `worker.stat` / `worker.list_dir` /
// `worker.create_dir` frames to verify a picked path, render the directory picker,
// and make a new folder — all before any runner exists. These handlers answer
// them against the worker's own filesystem. The worker owns `~` expansion (only the
// worker knows its own HOME); the registry passes whatever the user supplied.
//
// Error posture mirrors the registry's REST mapping: a "failed" status is
// reserved for UNEXPECTED I/O errors (mapped to a 500/502). Expected conditions —
// a missing path, EACCES, a name already taken — collapse to `status: "ok"` with
// a populated `error` (mapped to a 400/404/409), so the picker shows an inline
// message instead of a server error. The worker's dispatch (worker.ts) calls
// these and ships the returned frame back over the tunnel.

import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { lstatSync, mkdirSync, readdirSync, realpathSync, statSync, type Stats } from 'node:fs';
import {
  WorkerFrameKind,
  type WorkerCreateDirFrame,
  type WorkerCreateDirResultFrame,
  type WorkerListDirEntry,
  type WorkerListDirFrame,
  type WorkerListDirResultFrame,
  type WorkerStatFrame,
  type WorkerStatResultFrame,
} from '@orca/harness-tunnel';

/**
 * Handle a `worker.stat` request.
 *
 * Expands `~` against the worker process owner's home (the worker is the source of
 * truth for `~` — the registry never does this), follows symlinks via `stat`,
 * computes the canonical realpath, and collapses ENOENT + EACCES into
 * `exists: false`. Unexpected I/O errors return `status: "failed"`.
 *
 * @param frame The stat request. `frame.path` may be absolute or tilde-prefixed.
 * @returns A stat result with `exists`, `type`, and `canonicalPath` populated
 *   when the path is reachable.
 */
export function handleStat(frame: WorkerStatFrame): WorkerStatResultFrame {
  let expanded: string;
  try {
    expanded = expandUser(frame.path);
  } catch (exc) {
    // Defensive: an expansion failure maps to a "path expansion failed"
    // status:failed result. expandUser is pure string work over a string-typed
    // `frame.path`, so this is not reachable for valid inputs — but the guard is
    // kept so any future expansion change fails loud here rather than bubbling a
    // generic error to the registry.
    return {
      kind: WorkerFrameKind.StatResult,
      requestId: frame.requestId,
      status: 'failed',
      exists: false,
      type: null,
      canonicalPath: null,
      error: `path expansion failed: ${describeError(exc)}`,
    };
  }

  let st: Stats;
  try {
    // statSync follows symlinks by default — exactly what the design wants ("type
    // reflects the target").
    st = statSync(expanded);
  } catch (exc) {
    if (isNotReachable(exc)) {
      // ENOENT plus the permission errnos (EACCES/EPERM, treated as one
      // condition) collapse to "exists: false" so the registry validation has a
      // single contract for "not reachable".
      return {
        kind: WorkerFrameKind.StatResult,
        requestId: frame.requestId,
        status: 'ok',
        exists: false,
        type: null,
        canonicalPath: null,
        error: null,
      };
    }
    return {
      kind: WorkerFrameKind.StatResult,
      requestId: frame.requestId,
      status: 'failed',
      exists: false,
      type: null,
      canonicalPath: null,
      error: `stat failed: ${posixStrerror(exc)}`,
    };
  }

  let canonical: string;
  try {
    canonical = realpathSync(expanded);
  } catch (exc) {
    return {
      kind: WorkerFrameKind.StatResult,
      requestId: frame.requestId,
      status: 'failed',
      exists: false,
      type: null,
      canonicalPath: null,
      error: `realpath failed: ${posixStrerror(exc)}`,
    };
  }

  return {
    kind: WorkerFrameKind.StatResult,
    requestId: frame.requestId,
    status: 'ok',
    exists: true,
    type: entryType(st),
    canonicalPath: canonical,
    error: null,
  };
}

/**
 * Handle a `worker.list_dir` request.
 *
 * Walks the requested directory, follows symlinks for type detection (matching
 * `worker.stat`), and returns a paginated result. `~` expands against the worker
 * process owner's home, same rules as `worker.stat`. Per-entry I/O errors (broken
 * symlinks, ephemeral files) are silently skipped so a single bad entry doesn't
 * fail the whole listing.
 *
 * @param frame The list_dir request. `frame.path` may be absolute or
 *   tilde-prefixed; `limit` / `after` / `before` drive pagination.
 * @returns A list_dir result with entries sorted by name plus a `hasMore` flag.
 */
export function handleListDir(frame: WorkerListDirFrame): WorkerListDirResultFrame {
  let expanded: string;
  try {
    expanded = expandUser(frame.path);
  } catch (exc) {
    // See handleStat: an expansion failure maps to a status:failed "path
    // expansion failed" result, guarded here too even though expandUser cannot
    // throw on a string input.
    return {
      kind: WorkerFrameKind.ListDirResult,
      requestId: frame.requestId,
      status: 'failed',
      entries: [],
      hasMore: false,
      error: `path expansion failed: ${describeError(exc)}`,
    };
  }

  let dirents: ReadonlyArray<{ name: string }>;
  try {
    dirents = readdirSync(expanded, { withFileTypes: true });
  } catch (exc) {
    const code = errnoCode(exc);
    if (code === 'ENOENT') {
      return listDirError(frame.requestId, 'path does not exist');
    }
    if (code === 'ENOTDIR') {
      return listDirError(frame.requestId, 'path is not a directory');
    }
    if (isPermission(code)) {
      // EACCES and EPERM mean the same thing to a caller — collapse both to the
      // same "permission denied" answer rather than letting EPERM fall through to
      // status: "failed".
      return listDirError(frame.requestId, 'permission denied');
    }
    return {
      kind: WorkerFrameKind.ListDirResult,
      requestId: frame.requestId,
      status: 'failed',
      entries: [],
      hasMore: false,
      error: `scandir failed: ${posixStrerror(exc)}`,
    };
  }

  // Walk every entry, classifying by target type. A per-entry error → skip (e.g.
  // a dangling symlink) so the listing surfaces real entries instead of failing
  // wholesale.
  const entries: WorkerListDirEntry[] = [];
  for (const dirent of dirents) {
    const entryPath = join(expanded, dirent.name);
    let st: Stats;
    try {
      // statSync follows symlinks so the type reflects the target.
      st = statSync(entryPath);
    } catch {
      continue;
    }
    let type: string;
    let bytes: number | null;
    if (st.isDirectory()) {
      type = 'directory';
      bytes = null;
    } else if (st.isFile()) {
      type = 'file';
      bytes = st.size;
    } else {
      type = 'other';
      bytes = null;
    }
    entries.push({
      name: dirent.name,
      path: entryPath,
      type,
      bytes,
      modifiedAt: Math.trunc(st.mtimeMs / 1000),
    });
  }

  // Sort by name for stable pagination cursors. Cursors are entry paths so they
  // survive concurrent directory writes better than an in-memory index.
  entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));

  return paginateListDir({
    entries,
    requestId: frame.requestId,
    // `limit` is optional on the wire type; the frame decoder already defaults a
    // missing value to 20, so this `?? 20` only guards the type (and matches the
    // decoder's default) — it never changes a decoded frame's behavior.
    limit: frame.limit ?? 20,
    after: frame.after ?? null,
    before: frame.before ?? null,
  });
}

/**
 * Slice a sorted directory listing into a page.
 *
 * Cursors (`after` / `before`) reference an entry's `path`. Forward pagination
 * (`after`) returns up to `limit` entries strictly after the cursor; backward
 * pagination (`before`) returns up to `limit` entries strictly before. Empty
 * cursors return the first page. `hasMore` is set when more entries remain in the
 * pagination direction.
 */
function paginateListDir(args: {
  entries: WorkerListDirEntry[];
  requestId: string;
  limit: number;
  after: string | null;
  before: string | null;
}): WorkerListDirResultFrame {
  const { entries, requestId, limit, after, before } = args;
  // Identify the cut points by entry path so cursors survive concurrent directory
  // mutations between calls.
  let start = 0;
  let end = entries.length;
  if (after !== null) {
    for (let idx = 0; idx < entries.length; idx += 1) {
      if (entries[idx]!.path === after) {
        start = idx + 1;
        break;
      }
    }
  }
  if (before !== null) {
    for (let idx = 0; idx < entries.length; idx += 1) {
      if (entries[idx]!.path === before) {
        end = idx;
        break;
      }
    }
  }
  let page: WorkerListDirEntry[];
  let hasMore: boolean;
  if (before !== null) {
    const pageStart = Math.max(start, end - limit);
    page = entries.slice(pageStart, end);
    hasMore = pageStart > start;
  } else {
    page = entries.slice(start, end).slice(0, limit);
    hasMore = end - start > limit;
  }
  return {
    kind: WorkerFrameKind.ListDirResult,
    requestId,
    status: 'ok',
    entries: page,
    hasMore,
    error: null,
  };
}

/**
 * Handle a `worker.create_dir` request.
 *
 * Creates the directory (and any missing parents). `~` expands against the worker
 * process owner's home, same rules as `worker.list_dir`. Expected filesystem errors
 * (the directory already exists, permission denied, a parent component is a file)
 * return `status: "ok"` with a descriptive `error` so the route layer can map
 * them to a 409 rather than a 500 — mirroring how {@link handleListDir} reports a
 * missing path. Only unexpected I/O errors surface as `status: "failed"`.
 *
 * @param frame The create-dir request. `frame.path` may be absolute or
 *   tilde-prefixed.
 * @returns A result carrying the created absolute path on success, or an `error`
 *   describing why it was not created.
 */
export function handleCreateDir(frame: WorkerCreateDirFrame): WorkerCreateDirResultFrame {
  let expanded: string;
  try {
    expanded = expandUser(frame.path);
  } catch (exc) {
    // See handleStat: an expansion failure maps to a status:failed "path
    // expansion failed" result, guarded here too even though expandUser cannot
    // throw on a string input.
    return {
      kind: WorkerFrameKind.CreateDirResult,
      requestId: frame.requestId,
      status: 'failed',
      path: null,
      error: `path expansion failed: ${describeError(exc)}`,
    };
  }

  // Non-clobbering create posture: an occupied leaf — an existing directory, a
  // regular file, a symlink to a directory, AND a broken symlink — is a clear "name
  // is taken", never a silent success. Node's recursive mkdir does NOT provide that
  // posture: an existing directory (or a symlink to one) succeeds silently, and a
  // broken symlink raises ENOENT rather than EEXIST. So a pre-check detects an
  // occupied leaf up front via `lstat` (any entry, symlink included, resolving or
  // dangling — the no-follow existence check) and reports it, classified by
  // FOLLOWING the symlink (an isdir check) so a symlink→directory reports
  // "directory already exists" and a broken symlink reports "a file already exists
  // at that path". A parent component that is a file is NOT decided here: it falls
  // through to the recursive mkdir below, which reports it as ENOTDIR.
  if (leafExists(expanded)) {
    return createDirExistsResult(frame.requestId, expanded);
  }

  try {
    // Recursive so missing parents are created in one go. On a path whose parent
    // component is a regular file this raises ENOTDIR (handled below); a leaf that
    // appeared in a race raises EEXIST.
    mkdirSync(expanded, { recursive: true });
  } catch (exc) {
    const code = errnoCode(exc);
    if (code === 'EEXIST') {
      // The leaf was created between the pre-check and the mkdir. Re-classify it
      // by following the symlink (an isdir check) so a racing create gets the same
      // directory/file answer as the pre-check.
      return createDirExistsResult(frame.requestId, expanded);
    }
    if (code === 'ENOTDIR') {
      return {
        kind: WorkerFrameKind.CreateDirResult,
        requestId: frame.requestId,
        status: 'ok',
        path: null,
        error: 'a parent path component is not a directory',
      };
    }
    if (isPermission(code)) {
      // EACCES and EPERM mean the same thing to a caller — collapse both rather
      // than letting EPERM fall through to status: "failed".
      return {
        kind: WorkerFrameKind.CreateDirResult,
        requestId: frame.requestId,
        status: 'ok',
        path: null,
        error: 'permission denied',
      };
    }
    return {
      kind: WorkerFrameKind.CreateDirResult,
      requestId: frame.requestId,
      status: 'failed',
      path: null,
      error: `mkdir failed: ${posixStrerror(exc)}`,
    };
  }

  const created = absolutePath(expanded);
  return {
    kind: WorkerFrameKind.CreateDirResult,
    requestId: frame.requestId,
    status: 'ok',
    path: created,
    error: null,
  };
}

// ── helpers ──────────────────────────────────────────────

/** Build a list_dir result that carries an expected error under `status: "ok"`. */
function listDirError(requestId: string, error: string): WorkerListDirResultFrame {
  return {
    kind: WorkerFrameKind.ListDirResult,
    requestId,
    status: 'ok',
    entries: [],
    hasMore: false,
    error,
  };
}

/** Classify a (symlink-followed) Stats into the wire `type` string. */
function entryType(st: Stats): string {
  if (st.isDirectory()) {
    return 'directory';
  }
  if (st.isFile()) {
    return 'file';
  }
  return 'other';
}

/**
 * Whether a directory entry already occupies `path` — the "name is taken" trigger
 * for the non-clobbering create. `lstat` (no symlink follow) so a symlink occupying
 * the name counts as occupied without being followed (no-follow existence
 * semantics): the name is taken whether the symlink resolves or dangles. Any stat
 * error (ENOENT included) → `false` (free to create); the create attempt then
 * makes the real decision and surfaces its own error.
 */
function leafExists(path: string): boolean {
  try {
    lstatSync(path);
    return true;
  } catch {
    return false;
  }
}

/**
 * Build the create-dir "name is taken" result for an occupied leaf. Classifies by
 * FOLLOWING the symlink ({@link isDirectory}, a symlink-following isdir check): a
 * symlink→directory reports "directory already exists"; a plain file, or a symlink
 * whose target is missing/non-directory, reports "a file already exists at that
 * path".
 */
function createDirExistsResult(requestId: string, path: string): WorkerCreateDirResultFrame {
  return {
    kind: WorkerFrameKind.CreateDirResult,
    requestId,
    status: 'ok',
    path: null,
    error: isDirectory(path) ? 'directory already exists' : 'a file already exists at that path',
  };
}

/**
 * Expand a leading `~` (or `~/...`) against the worker process owner's home. A
 * bare `~` becomes HOME; `~/x` becomes `<HOME>/x`. A `~user` form (no own-home
 * knowledge) is left untouched, as is any non-leading `~`.
 *
 * Pure string work over a string-typed `frame.path` — `homedir()`/`join` do not
 * throw on string input — so this does not raise for valid inputs. Each caller
 * still guards the call, mapping a failure to a "path expansion failed"
 * status:failed result, so a future change that can throw fails loud rather than
 * escaping the handler.
 */
function expandUser(path: string): string {
  if (path === '~') {
    return homedir();
  }
  if (path.startsWith('~/')) {
    return join(homedir(), path.slice(2));
  }
  return path;
}

/**
 * Absolutize AND normalize a path: join it onto the working directory and
 * normalize the result, so `..`, `.`, repeated and trailing slashes always
 * collapse — even for an already-absolute input. `path.resolve` does exactly that
 * (the join is a no-op when `path` is already absolute), so an input like
 * `/tmp/a/b/../c` returns `/tmp/a/c` rather than verbatim — the picker navigates
 * the clean path.
 */
function absolutePath(path: string): string {
  return resolve(path);
}

/** Whether `path` is a directory (symlink-followed), a symlink-following isdir check. */
function isDirectory(path: string): boolean {
  try {
    return statSync(path).isDirectory();
  } catch {
    return false;
  }
}

/** The POSIX errno string of a thrown filesystem error, or `undefined`. */
function errnoCode(exc: unknown): string | undefined {
  return (exc as NodeJS.ErrnoException | undefined)?.code;
}

/**
 * Render a thrown value for the `path expansion failed: …` message: an `Error`'s
 * full `message` (not the trimmed {@link posixStrerror} form), else the value
 * stringified. Only reachable on the (defensive, unreachable-for-string-input)
 * expansion-guard path.
 */
function describeError(exc: unknown): string {
  if (exc instanceof Error) {
    return exc.message;
  }
  return String(exc);
}

/**
 * Whether `code` is one of the permission errnos treated as a single permission
 * error: EACCES (the usual case) and EPERM. Collapsing both keeps the handlers'
 * "expected condition → status: ok" contract: neither ever reaches
 * `status: "failed"`.
 */
function isPermission(code: string | undefined): boolean {
  return code === 'EACCES' || code === 'EPERM';
}

/**
 * Whether a thrown stat error means "not reachable" for {@link handleStat}: a
 * missing path (ENOENT) or a permission error (EACCES/EPERM, see
 * {@link isPermission}). These collapse to `exists: false`.
 */
function isNotReachable(exc: unknown): boolean {
  const code = errnoCode(exc);
  return code === 'ENOENT' || isPermission(code);
}

/**
 * Clean POSIX strerror of a thrown filesystem error (e.g. "name too long"). Node
 * has no `strerror` field; its message is the well-defined
 * `"<CODE>: <strerror>, <syscall> '<path>'"`, so the human-readable middle segment
 * is extracted (dropping the errno-code prefix and the syscall + path noise).
 * Falls back to the full message when the shape is unexpected. Only surfaces on
 * the unexpected `status: "failed"` path.
 */
function posixStrerror(exc: unknown): string {
  if (exc instanceof Error) {
    const match = /^[A-Z][A-Z0-9]*: (.+?)(?:, [a-z]+ .*)?$/.exec(exc.message);
    if (match !== null) {
      return match[1]!;
    }
    return exc.message;
  }
  return String(exc);
}
