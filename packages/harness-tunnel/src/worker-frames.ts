// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Worker tunnel frame schema.
//
// Worker-specific frame kinds, all JSON (see `WorkerFrameKind`). Worker frames carry
// the control + filesystem-op messages the registry sends a connected worker
// over the worker tunnel: launch/stop a runner and their results, a one-way
// runner-exited report, and the stat / list-dir / worktree / create-dir
// operations that back workspace selection — plus the worker hello handshake.
// They do NOT carry HTTP request/response traffic; runners connect directly to
// the registry with their own tunnels (see `frames.ts`).
//
// This module is intentionally separate from the runner tunnel's `frames.ts` to
// keep the two protocols partitioned. The runner module has a closed
// `FrameKind` enum and `decodeFrame` switch that handles every runner frame
// kind; adding worker kinds there would force runner-side decoders to handle
// frames they never see.
//
// This module exports a discriminated union of frame objects keyed on `kind`,
// plus `encodeWorkerFrame` / `decodeWorkerFrame` helpers. The contract is small
// enough that hand-rolled validation in `decodeWorkerFrame` is the right level of
// machinery — no schema library. TS field names are camelCase; the JSON wire
// keys stay snake_case (`request_id`, `binding_token`, `frame_protocol_version`,
// `configured_harnesses`, `modified_at`) because the wire schema is the
// cross-component contract.

/**
 * Structured error code carried in `WorkerLaunchRunnerResultFrame.errorCode` when
 * the worker refuses a launch because the session's harness is not configured on
 * that machine (CLI missing or no default credential). Shared by the worker
 * (producer), registry (maps it to a "harness not configured" error), and
 * tests.
 */
export const HARNESS_NOT_CONFIGURED_ERROR_CODE = 'harness_not_configured';

// ── Status + filesystem-type literal unions ──────────────
//
// Result discriminators are literal unions, not bare strings, and
// `decodeWorkerFrame` CHECKS membership (an unknown status is rejected exactly
// like an unknown `kind` — under strict-major versioning a new status value is
// a protocol rev). A bare `string` here would let a producer or consumer typo
// ('lauched', or 'ok' where a frame uses 'launched') compile clean and turn
// every launch into a silent failure path.

/** `worker.launch_runner_result` outcome discriminator. */
export type WorkerLaunchStatus = 'launched' | 'failed';
/** `worker.stop_runner_result` outcome discriminator. */
export type WorkerStopStatus = 'stopped' | 'failed';
/** Outcome discriminator shared by the filesystem-op results (`"ok"` on success). */
export type WorkerOpStatus = 'ok' | 'failed';
/** Filesystem entry type after symlink resolution (symlinks are never surfaced). */
export type WorkerFsEntryType = 'directory' | 'file' | 'other';

/** All worker frame kinds; the value is the JSON wire string. */
export enum WorkerFrameKind {
  Hello = 'worker.hello',
  LaunchRunner = 'worker.launch_runner',
  LaunchRunnerResult = 'worker.launch_runner_result',
  StopRunner = 'worker.stop_runner',
  StopRunnerResult = 'worker.stop_runner_result',
  RunnerExited = 'worker.runner_exited',
  Stat = 'worker.stat',
  StatResult = 'worker.stat_result',
  ListDir = 'worker.list_dir',
  ListDirResult = 'worker.list_dir_result',
  CreateWorktree = 'worker.create_worktree',
  CreateWorktreeResult = 'worker.create_worktree_result',
  RemoveWorktree = 'worker.remove_worktree',
  RemoveWorktreeResult = 'worker.remove_worktree_result',
  CreateDir = 'worker.create_dir',
  CreateDirResult = 'worker.create_dir_result',
}

// ── Frame shapes ─────────────────────────────────────────
//
// Fields with a wire default are optional on the TS object — `encodeWorkerFrame`
// substitutes the field's wire default, and `decodeWorkerFrame` always returns
// them fully populated. Construction mirrors that: supply the required fields,
// and the defaults fill the rest.

/**
 * Worker's first frame on a fresh tunnel.
 *
 * `runners` lists the runner IDs currently alive on this worker so the registry
 * can reconcile state on reconnect (it diffs this against sessions in the DB).
 * `configuredHarnesses` reports per-harness readiness on this machine, e.g.
 * `{"claude-sdk": true, "codex": false}`; keys cover every accepted harness
 * spelling. `null` means unknown (an older worker that doesn't report it) — never
 * treat `null` as "nothing is configured". The launch-time check is
 * authoritative.
 */
export interface WorkerHelloFrame {
  kind: WorkerFrameKind.Hello;
  /** Worker software version, e.g. `"0.1.0"`. */
  version: string;
  /** Wire-protocol major. Registry refuses on major mismatch. */
  frameProtocolVersion: number;
  /** Human-readable worker name from config, e.g. `"workstation-01"`. */
  name: string;
  /** Runner IDs currently alive on this worker. Defaults to `[]`. */
  runners?: string[];
  /** Per-harness readiness, or `null` ("unknown"). Defaults to `null`. */
  configuredHarnesses?: Record<string, boolean> | null;
}

/**
 * Registry → worker: spawn a new runner process.
 *
 * The registry derives `runnerId` from `bindingToken` via the token-binding
 * helper. `harness` is the canonical harness the session will run; the worker
 * checks it is configured before spawning and refuses with
 * `HARNESS_NOT_CONFIGURED_ERROR_CODE` when not. `null` (older registry, or no
 * resolvable harness) skips the check — fail open.
 */
export interface WorkerLaunchRunnerFrame {
  kind: WorkerFrameKind.LaunchRunner;
  /** Unique ID for correlating the result, e.g. `"req_abc123"`. */
  requestId: string;
  /** Secret token the runner must present when connecting. */
  bindingToken: string;
  /** Absolute path on the worker to use as the runner's working directory. */
  workspace: string;
  /** Canonical harness, or `null` to skip the configured check. Defaults to `null`. */
  harness?: string | null;
}

/**
 * Worker → registry: outcome of a launch request.
 *
 * A discriminated union on `status` so the null-correlations are structural: a
 * `"launched"` result always carries the spawned `runnerId` (decode rejects a
 * launched result without one), and error details exist only on `"failed"`.
 * `errorCode` is a machine-readable failure category, e.g.
 * `HARNESS_NOT_CONFIGURED_ERROR_CODE`; `null` for uncategorized failures and
 * always from older hosts. Decode normalizes the cross-arm fields (`error` /
 * `errorCode` on a success, `runnerId` on a failure) to `null`.
 */
export type WorkerLaunchRunnerResultFrame =
  | {
      kind: WorkerFrameKind.LaunchRunnerResult;
      /** Correlates to the launch frame, e.g. `"req_abc123"`. */
      requestId: string;
      status: 'launched';
      /** Runner ID derived from the binding token. */
      runnerId: string;
      /** Meaningless on success; always `null`. */
      error?: null;
      /** Meaningless on success; always `null`. */
      errorCode?: null;
    }
  | {
      kind: WorkerFrameKind.LaunchRunnerResult;
      /** Correlates to the launch frame, e.g. `"req_abc123"`. */
      requestId: string;
      status: 'failed';
      /** A failed launch has no usable runner; always `null`. */
      runnerId?: null;
      /** Error message, or `null` when the worker gave none. Defaults to `null`. */
      error?: string | null;
      /** Machine-readable failure category, else `null`. Defaults to `null`. */
      errorCode?: string | null;
    };

/** Registry → worker: terminate a runner process. */
export interface WorkerStopRunnerFrame {
  kind: WorkerFrameKind.StopRunner;
  /** Unique ID for correlating the result, e.g. `"req_def456"`. */
  requestId: string;
  /** Runner to stop, e.g. `"runner_token_abc123..."`. */
  runnerId: string;
}

/** Worker → registry: outcome of a stop request. */
export interface WorkerStopRunnerResultFrame {
  kind: WorkerFrameKind.StopRunnerResult;
  /** Correlates to the stop frame, e.g. `"req_def456"`. */
  requestId: string;
  status: WorkerStopStatus;
  /** Error message when failed, else `null`. Defaults to `null`. */
  error?: string | null;
}

/**
 * Worker → registry: a spawned runner process died unexpectedly.
 *
 * One-way report (no result frame). The worker watches every runner it spawns;
 * when one exits without a `worker.stop_runner` request, it composes a
 * human-readable error — exit code plus the tail of the runner's captured log —
 * and reports it here. The registry stashes it so the runner status endpoint can
 * answer "offline, and here is why": a client waiting for the runner to connect
 * fails fast with the actual cause instead of polling to a timeout.
 */
export interface WorkerRunnerExitedFrame {
  kind: WorkerFrameKind.RunnerExited;
  /** The runner that died, e.g. `"runner_abc123..."`. */
  runnerId: string;
  /** Human-readable cause, including the trailing lines of the runner's log. */
  error: string;
}

/**
 * Registry → worker: stat a path on the worker's filesystem.
 *
 * Used by session-create validation to verify a workspace path (or an agent's
 * cwd boundary) exists and is a directory before storing the session row.
 * Single round-trip; no directory walking. `path` is absolute or
 * tilde-prefixed — the worker expands `~` against its own process owner's home
 * before stating; only the worker knows its own `HOME`.
 */
export interface WorkerStatFrame {
  kind: WorkerFrameKind.Stat;
  /** Unique ID for correlating the result, e.g. `"req_stat_1"`. */
  requestId: string;
  /** Absolute or tilde-prefixed path on the worker. */
  path: string;
}

/**
 * Worker → registry: outcome of a stat request.
 *
 * `status` is `"ok"` or `"failed"`; `"failed"` is reserved for unexpected I/O
 * errors (e.g. EIO). EACCES and ENOENT both produce `status: "ok", exists:
 * false`. `type` reflects the target's type after symlink resolution — a
 * symlink to a directory returns `"directory"`, never `"symlink"`; `null` when
 * not existing. `canonicalPath` is the absolute, normalized realpath, stored on
 * the session row instead of the user's input so symlinks cannot smuggle a
 * workspace out of an agent's cwd boundary; `null` when not existing.
 */
export interface WorkerStatResultFrame {
  kind: WorkerFrameKind.StatResult;
  /** Correlates to the stat frame. */
  requestId: string;
  status: WorkerOpStatus;
  /**
   * `true` when the path exists and is accessible. REQUIRED on the wire — the
   * strict decoder rejects a stat result without it (an absent liveness bit is
   * a protocol violation, not a default), and encode writes it verbatim.
   */
  exists: boolean;
  /** Entry type after symlink resolution, or `null` when not existing. Defaults to `null`. */
  type?: WorkerFsEntryType | null;
  /** Absolute normalized realpath, or `null` when not existing. Defaults to `null`. */
  canonicalPath?: string | null;
  /** Filesystem error when `status` is `"failed"`, else `null`. Defaults to `null`. */
  error?: string | null;
}

/**
 * A single entry in a `worker.list_dir_result`.
 *
 * Mirrors the runner's filesystem-entry shape so the Web UI's existing tree
 * component can consume worker browse results without a different mapping. `type`
 * reflects the target type after symlink resolution; symlinks themselves are not
 * surfaced (consistent with `worker.stat_result`). `bytes` is the file size for
 * regular files and `null` for directories and other types.
 */
export interface WorkerListDirEntry {
  /** Basename of the entry, e.g. `"src"`. */
  name: string;
  /** Absolute path on the worker, e.g. `"/home/dev/workspace/src"`. */
  path: string;
  type: WorkerFsEntryType;
  /** File size for regular files; `null` for directories and other types. */
  bytes: number | null;
  /** Unix epoch seconds of last modification, e.g. `1779980000`. */
  modifiedAt: number;
}

/**
 * Registry → worker: list contents of a directory on the worker.
 *
 * Used to render the directory picker before any runner exists. The worker owns
 * `~` resolution (same rules as `worker.stat`); the registry passes whatever the
 * user supplied (or `~` when the path is empty). Pagination is in-memory at the
 * worker since most directories fit easily in one page.
 */
export interface WorkerListDirFrame {
  kind: WorkerFrameKind.ListDir;
  /** Unique ID for correlating the result, e.g. `"req_list_1"`. */
  requestId: string;
  /** Absolute or tilde-prefixed directory path. */
  path: string;
  /** Maximum entries to return per page. Defaults to `20`. */
  limit?: number;
  /** Forward-pagination cursor (entry `path`), or `null` for the first page. Defaults to `null`. */
  after?: string | null;
  /** Backward-pagination cursor, or `null` to paginate forward only. Defaults to `null`. */
  before?: string | null;
}

/**
 * Worker → registry: outcome of a list_dir request.
 *
 * `status` is `"ok"` or `"failed"`; `"failed"` is reserved for unexpected I/O
 * errors. A missing path collapses to `"ok"` with an empty entries list and a
 * descriptive `error` (the route layer maps these into 404). `error` is
 * populated even when `status` is `"ok"` so a missing path still carries a
 * useful message into the REST response.
 */
export interface WorkerListDirResultFrame {
  kind: WorkerFrameKind.ListDirResult;
  /** Correlates to the list_dir frame, e.g. `"req_list_1"`. */
  requestId: string;
  status: WorkerOpStatus;
  /** Directory contents, possibly paginated. Defaults to `[]`. */
  entries?: WorkerListDirEntry[];
  /** `true` when more pages exist. Defaults to `false`. */
  hasMore?: boolean;
  /** Filesystem error, or `null` on success. Defaults to `null`. */
  error?: string | null;
}

/**
 * Registry → worker: create a git worktree for a new branch.
 *
 * `repoPath` is an absolute path inside the source repo (the picked dir or a
 * subdir). `baseBranch` is an optional base ref; `null` branches from `HEAD`.
 */
export interface WorkerCreateWorktreeFrame {
  kind: WorkerFrameKind.CreateWorktree;
  /** Correlates the result, e.g. `"req_wt_1"`. */
  requestId: string;
  /** Absolute path inside the source repo, e.g. `"/home/dev/projects/myrepo"`. */
  repoPath: string;
  /** New branch to create, e.g. `"feature/login"`. */
  branchName: string;
  /** Optional base ref, or `null` to branch from `HEAD`. Defaults to `null`. */
  baseBranch?: string | null;
}

/**
 * Worker → registry: outcome of a create-worktree request.
 *
 * `worktreePath` is the created worktree directory (stored as the session
 * workspace); `branch` is the branch checked out. Both `null` on failure.
 */
export interface WorkerCreateWorktreeResultFrame {
  kind: WorkerFrameKind.CreateWorktreeResult;
  /** Correlates to the create-worktree frame, e.g. `"req_wt_1"`. */
  requestId: string;
  status: WorkerOpStatus;
  /** Created worktree directory, or `null` on failure. Defaults to `null`. */
  worktreePath?: string | null;
  /** Branch checked out, or `null` on failure. Defaults to `null`. */
  branch?: string | null;
  /** Error message when failed, else `null`. Defaults to `null`. */
  error?: string | null;
}

/**
 * Registry → worker: remove a git worktree (opt-in session cleanup).
 *
 * The worker derives the main repo from `worktreePath` itself, so no repo path is
 * carried. When `deleteBranch` is `true`, the worker runs `git branch -D` after
 * removing the directory; when `false`, it removes only the directory. `branch`
 * is the branch to delete when `deleteBranch` is `true`; `null` skips deletion.
 */
export interface WorkerRemoveWorktreeFrame {
  kind: WorkerFrameKind.RemoveWorktree;
  /** Correlates the result, e.g. `"req_wt_rm_1"`. */
  requestId: string;
  /** Worktree directory to remove (the stored session workspace). */
  worktreePath: string;
  /** Branch to delete when `deleteBranch` is `true`, or `null`. Defaults to `null`. */
  branch?: string | null;
  /** When `true`, delete the branch after removing the directory. Defaults to `false`. */
  deleteBranch?: boolean;
}

/** Worker → registry: outcome of a remove-worktree request. */
export interface WorkerRemoveWorktreeResultFrame {
  kind: WorkerFrameKind.RemoveWorktreeResult;
  /** Correlates to the remove-worktree frame, e.g. `"req_wt_rm_1"`. */
  requestId: string;
  status: WorkerOpStatus;
  /** Error message when failed, else `null`. Defaults to `null`. */
  error?: string | null;
}

/**
 * Registry → worker: create a new directory on the worker.
 *
 * Backs the workspace picker's "New folder" action so a user can make a fresh
 * folder to start a session in without dropping to a terminal. The worker owns `~`
 * resolution, same rules as `worker.list_dir` / `worker.stat`. Missing parent
 * directories are created.
 */
export interface WorkerCreateDirFrame {
  kind: WorkerFrameKind.CreateDir;
  /** Correlates the result, e.g. `"req_mkdir_1"`. */
  requestId: string;
  /** Absolute or tilde-prefixed directory path to create. */
  path: string;
}

/**
 * Worker → registry: outcome of a create-dir request.
 *
 * `status` is `"ok"` or `"failed"`; `"failed"` is reserved for unexpected I/O
 * errors. An expected filesystem error (the directory already exists, permission
 * denied, a parent path component is a file) collapses to `"ok"` with a
 * descriptive `error` so the route layer can map it to a 409 rather than a 500 —
 * same posture as `worker.list_dir` for a missing path. `path` is the absolute
 * path of the created directory, `null` when it was not created.
 */
export interface WorkerCreateDirResultFrame {
  kind: WorkerFrameKind.CreateDirResult;
  /** Correlates to the create-dir frame, e.g. `"req_mkdir_1"`. */
  requestId: string;
  status: WorkerOpStatus;
  /** Absolute path of the created directory, or `null` when not created. Defaults to `null`. */
  path?: string | null;
  /** Filesystem error, or `null` on success. Defaults to `null`. */
  error?: string | null;
}

export type WorkerFrame =
  | WorkerHelloFrame
  | WorkerLaunchRunnerFrame
  | WorkerLaunchRunnerResultFrame
  | WorkerStopRunnerFrame
  | WorkerStopRunnerResultFrame
  | WorkerRunnerExitedFrame
  | WorkerStatFrame
  | WorkerStatResultFrame
  | WorkerListDirFrame
  | WorkerListDirResultFrame
  | WorkerCreateWorktreeFrame
  | WorkerCreateWorktreeResultFrame
  | WorkerRemoveWorktreeFrame
  | WorkerRemoveWorktreeResultFrame
  | WorkerCreateDirFrame
  | WorkerCreateDirResultFrame;

// ── Encode ───────────────────────────────────────────────

/**
 * Serialize a worker frame to its JSON wire form. The output is what goes onto the
 * WebSocket as a text message.
 *
 * @throws TypeError if the frame has an unrecognized `kind`.
 */
export function encodeWorkerFrame(frame: WorkerFrame): string {
  switch (frame.kind) {
    case WorkerFrameKind.Hello:
      return JSON.stringify({
        kind: WorkerFrameKind.Hello,
        version: frame.version,
        frame_protocol_version: frame.frameProtocolVersion,
        name: frame.name,
        runners: [...(frame.runners ?? [])],
        configured_harnesses: frame.configuredHarnesses ?? null,
      });
    case WorkerFrameKind.LaunchRunner:
      return JSON.stringify({
        kind: WorkerFrameKind.LaunchRunner,
        request_id: frame.requestId,
        binding_token: frame.bindingToken,
        workspace: frame.workspace,
        harness: frame.harness ?? null,
      });
    case WorkerFrameKind.LaunchRunnerResult:
      return JSON.stringify({
        kind: WorkerFrameKind.LaunchRunnerResult,
        request_id: frame.requestId,
        status: frame.status,
        runner_id: frame.runnerId ?? null,
        error: frame.error ?? null,
        error_code: frame.errorCode ?? null,
      });
    case WorkerFrameKind.StopRunner:
      return JSON.stringify({
        kind: WorkerFrameKind.StopRunner,
        request_id: frame.requestId,
        runner_id: frame.runnerId,
      });
    case WorkerFrameKind.StopRunnerResult:
      return JSON.stringify({
        kind: WorkerFrameKind.StopRunnerResult,
        request_id: frame.requestId,
        status: frame.status,
        error: frame.error ?? null,
      });
    case WorkerFrameKind.RunnerExited:
      return JSON.stringify({
        kind: WorkerFrameKind.RunnerExited,
        runner_id: frame.runnerId,
        error: frame.error,
      });
    case WorkerFrameKind.Stat:
      return JSON.stringify({
        kind: WorkerFrameKind.Stat,
        request_id: frame.requestId,
        path: frame.path,
      });
    case WorkerFrameKind.StatResult:
      return JSON.stringify({
        kind: WorkerFrameKind.StatResult,
        request_id: frame.requestId,
        status: frame.status,
        exists: frame.exists,
        type: frame.type ?? null,
        canonical_path: frame.canonicalPath ?? null,
        error: frame.error ?? null,
      });
    case WorkerFrameKind.ListDir:
      return JSON.stringify({
        kind: WorkerFrameKind.ListDir,
        request_id: frame.requestId,
        path: frame.path,
        limit: frame.limit ?? 20,
        after: frame.after ?? null,
        before: frame.before ?? null,
      });
    case WorkerFrameKind.ListDirResult:
      return JSON.stringify({
        kind: WorkerFrameKind.ListDirResult,
        request_id: frame.requestId,
        status: frame.status,
        entries: (frame.entries ?? []).map((entry) => ({
          name: entry.name,
          path: entry.path,
          type: entry.type,
          bytes: entry.bytes,
          modified_at: entry.modifiedAt,
        })),
        has_more: frame.hasMore ?? false,
        error: frame.error ?? null,
      });
    case WorkerFrameKind.CreateWorktree:
      return JSON.stringify({
        kind: WorkerFrameKind.CreateWorktree,
        request_id: frame.requestId,
        repo_path: frame.repoPath,
        branch_name: frame.branchName,
        base_branch: frame.baseBranch ?? null,
      });
    case WorkerFrameKind.CreateWorktreeResult:
      return JSON.stringify({
        kind: WorkerFrameKind.CreateWorktreeResult,
        request_id: frame.requestId,
        status: frame.status,
        worktree_path: frame.worktreePath ?? null,
        branch: frame.branch ?? null,
        error: frame.error ?? null,
      });
    case WorkerFrameKind.RemoveWorktree:
      return JSON.stringify({
        kind: WorkerFrameKind.RemoveWorktree,
        request_id: frame.requestId,
        worktree_path: frame.worktreePath,
        branch: frame.branch ?? null,
        delete_branch: frame.deleteBranch ?? false,
      });
    case WorkerFrameKind.RemoveWorktreeResult:
      return JSON.stringify({
        kind: WorkerFrameKind.RemoveWorktreeResult,
        request_id: frame.requestId,
        status: frame.status,
        error: frame.error ?? null,
      });
    case WorkerFrameKind.CreateDir:
      return JSON.stringify({
        kind: WorkerFrameKind.CreateDir,
        request_id: frame.requestId,
        path: frame.path,
      });
    case WorkerFrameKind.CreateDirResult:
      return JSON.stringify({
        kind: WorkerFrameKind.CreateDirResult,
        request_id: frame.requestId,
        status: frame.status,
        path: frame.path ?? null,
        error: frame.error ?? null,
      });
    default: {
      const kind = (frame as { kind?: unknown }).kind;
      throw new TypeError(`unknown worker frame type: ${String(kind)}`);
    }
  }
}

// ── Decode ───────────────────────────────────────────────

type FrameObject = Record<string, unknown>;

/**
 * Parse a JSON wire frame back into its typed worker frame object.
 *
 * @throws Error on malformed JSON, missing `kind`, unknown kind, or missing /
 *   ill-typed required fields for the kind.
 */
export function decodeWorkerFrame(text: string): WorkerFrame {
  const msg = parseFrameObject(text);
  const kind = parseWorkerFrameKind(msg);
  return decodeKnownWorkerFrame(kind, msg);
}

/** Parse a JSON frame object, rejecting non-object roots. */
function parseFrameObject(text: string): FrameObject {
  let msg: unknown;
  try {
    msg = JSON.parse(text);
  } catch (exc) {
    const detail = exc instanceof Error ? exc.message : String(exc);
    throw new Error(`frame is not valid JSON: ${detail}`);
  }
  if (typeof msg !== 'object' || msg === null || Array.isArray(msg)) {
    throw new Error(`frame must be a JSON object, got ${describeType(msg)}`);
  }
  return msg as FrameObject;
}

/** Parse the worker frame `kind` discriminator. */
function parseWorkerFrameKind(msg: FrameObject): WorkerFrameKind {
  const kind = msg['kind'];
  if (typeof kind !== 'string') {
    throw new Error("frame missing 'kind' field");
  }
  if (!isKnownWorkerKind(kind)) {
    throw new Error(`unknown worker frame kind: ${JSON.stringify(kind)}`);
  }
  return kind;
}

function isKnownWorkerKind(kind: string): kind is WorkerFrameKind {
  return (Object.values(WorkerFrameKind) as string[]).includes(kind);
}

/** Decode a worker frame with a validated kind. */
function decodeKnownWorkerFrame(kind: WorkerFrameKind, msg: FrameObject): WorkerFrame {
  switch (kind) {
    case WorkerFrameKind.Hello:
      return decodeWorkerHello(msg);
    case WorkerFrameKind.LaunchRunner:
      return decodeLaunchRunner(msg);
    case WorkerFrameKind.LaunchRunnerResult:
      return decodeLaunchRunnerResult(msg);
    case WorkerFrameKind.StopRunner:
      return decodeStopRunner(msg);
    case WorkerFrameKind.StopRunnerResult:
      return decodeStopRunnerResult(msg);
    case WorkerFrameKind.RunnerExited:
      return decodeRunnerExited(msg);
    case WorkerFrameKind.Stat:
      return decodeStat(msg);
    case WorkerFrameKind.StatResult:
      return decodeStatResult(msg);
    case WorkerFrameKind.ListDir:
      return decodeListDir(msg);
    case WorkerFrameKind.ListDirResult:
      return decodeListDirResult(msg);
    case WorkerFrameKind.CreateWorktree:
      return decodeCreateWorktree(msg);
    case WorkerFrameKind.CreateWorktreeResult:
      return decodeCreateWorktreeResult(msg);
    case WorkerFrameKind.RemoveWorktree:
      return decodeRemoveWorktree(msg);
    case WorkerFrameKind.RemoveWorktreeResult:
      return decodeRemoveWorktreeResult(msg);
    case WorkerFrameKind.CreateDir:
      return decodeCreateDir(msg);
    case WorkerFrameKind.CreateDirResult:
      return decodeCreateDirResult(msg);
  }
}

function decodeWorkerHello(msg: FrameObject): WorkerHelloFrame {
  return {
    kind: WorkerFrameKind.Hello,
    version: requiredStr(msg, 'version'),
    frameProtocolVersion: requiredInt(msg, 'frame_protocol_version'),
    name: requiredStr(msg, 'name'),
    runners: optionalStrList(msg, 'runners'),
    configuredHarnesses: optionalStrBoolMap(msg, 'configured_harnesses'),
  };
}

function decodeLaunchRunner(msg: FrameObject): WorkerLaunchRunnerFrame {
  return {
    kind: WorkerFrameKind.LaunchRunner,
    requestId: requiredStr(msg, 'request_id'),
    bindingToken: requiredStr(msg, 'binding_token'),
    workspace: requiredStr(msg, 'workspace'),
    harness: optionalNullableStr(msg, 'harness'),
  };
}

function decodeLaunchRunnerResult(msg: FrameObject): WorkerLaunchRunnerResultFrame {
  const requestId = requiredStr(msg, 'request_id');
  const status = parseStatusUnion(msg, 'status', ['launched', 'failed']);
  const runnerId = optionalNullableStr(msg, 'runner_id');
  if (status === 'launched') {
    // The DU makes the null-correlation structural: a launched result MUST name
    // the runner it spawned (the registry stores it), so an absent runner_id is
    // a protocol violation, and any error text on a success is normalized away.
    if (runnerId === null) {
      throw new Error('launch result with status "launched" must carry \'runner_id\'');
    }
    return {
      kind: WorkerFrameKind.LaunchRunnerResult,
      requestId,
      status,
      runnerId,
      error: null,
      errorCode: null,
    };
  }
  return {
    kind: WorkerFrameKind.LaunchRunnerResult,
    requestId,
    status,
    runnerId: null,
    error: optionalNullableStr(msg, 'error'),
    errorCode: optionalNullableStr(msg, 'error_code'),
  };
}

function decodeStopRunner(msg: FrameObject): WorkerStopRunnerFrame {
  return {
    kind: WorkerFrameKind.StopRunner,
    requestId: requiredStr(msg, 'request_id'),
    runnerId: requiredStr(msg, 'runner_id'),
  };
}

function decodeStopRunnerResult(msg: FrameObject): WorkerStopRunnerResultFrame {
  return {
    kind: WorkerFrameKind.StopRunnerResult,
    requestId: requiredStr(msg, 'request_id'),
    status: parseStatusUnion(msg, 'status', ['stopped', 'failed']),
    error: optionalNullableStr(msg, 'error'),
  };
}

function decodeRunnerExited(msg: FrameObject): WorkerRunnerExitedFrame {
  return {
    kind: WorkerFrameKind.RunnerExited,
    runnerId: requiredStr(msg, 'runner_id'),
    error: requiredStr(msg, 'error'),
  };
}

function decodeStat(msg: FrameObject): WorkerStatFrame {
  return {
    kind: WorkerFrameKind.Stat,
    requestId: requiredStr(msg, 'request_id'),
    path: requiredStr(msg, 'path'),
  };
}

function decodeStatResult(msg: FrameObject): WorkerStatResultFrame {
  return {
    kind: WorkerFrameKind.StatResult,
    requestId: requiredStr(msg, 'request_id'),
    status: parseStatusUnion(msg, 'status', ['ok', 'failed']),
    exists: requiredBool(msg, 'exists'),
    type: parseNullableFsType(msg, 'type'),
    canonicalPath: optionalNullableStr(msg, 'canonical_path'),
    error: optionalNullableStr(msg, 'error'),
  };
}

function decodeListDir(msg: FrameObject): WorkerListDirFrame {
  const limitValue = 'limit' in msg && msg['limit'] !== undefined ? msg['limit'] : 20;
  if (!isInteger(limitValue)) {
    throw new Error("frame field must be an int: 'limit'");
  }
  return {
    kind: WorkerFrameKind.ListDir,
    requestId: requiredStr(msg, 'request_id'),
    path: requiredStr(msg, 'path'),
    limit: limitValue,
    after: optionalNullableStr(msg, 'after'),
    before: optionalNullableStr(msg, 'before'),
  };
}

function decodeListDirResult(msg: FrameObject): WorkerListDirResultFrame {
  const rawEntries = 'entries' in msg && msg['entries'] !== undefined ? msg['entries'] : [];
  if (!Array.isArray(rawEntries)) {
    throw new Error("frame field must be a list: 'entries'");
  }
  const entries: WorkerListDirEntry[] = [];
  for (const raw of rawEntries) {
    if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
      throw new Error("each entry in 'entries' must be a JSON object");
    }
    entries.push(decodeListDirEntry(raw as FrameObject));
  }
  const hasMore = 'has_more' in msg && msg['has_more'] !== undefined ? msg['has_more'] : false;
  if (typeof hasMore !== 'boolean') {
    throw new Error("frame field must be a bool: 'has_more'");
  }
  return {
    kind: WorkerFrameKind.ListDirResult,
    requestId: requiredStr(msg, 'request_id'),
    status: parseStatusUnion(msg, 'status', ['ok', 'failed']),
    entries,
    hasMore,
    error: optionalNullableStr(msg, 'error'),
  };
}

function decodeListDirEntry(msg: FrameObject): WorkerListDirEntry {
  const bytesVal = msg['bytes'];
  if (bytesVal !== undefined && bytesVal !== null && !isInteger(bytesVal)) {
    throw new Error("entry field must be int or null: 'bytes'");
  }
  const modifiedAt = msg['modified_at'];
  if (!isInteger(modifiedAt)) {
    throw new Error("entry field must be an int: 'modified_at'");
  }
  return {
    name: requiredStr(msg, 'name'),
    path: requiredStr(msg, 'path'),
    type: parseEntryType(msg, 'type'),
    bytes: bytesVal === undefined ? null : (bytesVal as number | null),
    modifiedAt,
  };
}

function decodeCreateWorktree(msg: FrameObject): WorkerCreateWorktreeFrame {
  return {
    kind: WorkerFrameKind.CreateWorktree,
    requestId: requiredStr(msg, 'request_id'),
    repoPath: requiredStr(msg, 'repo_path'),
    branchName: requiredStr(msg, 'branch_name'),
    baseBranch: optionalNullableStr(msg, 'base_branch'),
  };
}

function decodeCreateWorktreeResult(msg: FrameObject): WorkerCreateWorktreeResultFrame {
  return {
    kind: WorkerFrameKind.CreateWorktreeResult,
    requestId: requiredStr(msg, 'request_id'),
    status: parseStatusUnion(msg, 'status', ['ok', 'failed']),
    worktreePath: optionalNullableStr(msg, 'worktree_path'),
    branch: optionalNullableStr(msg, 'branch'),
    error: optionalNullableStr(msg, 'error'),
  };
}

function decodeRemoveWorktree(msg: FrameObject): WorkerRemoveWorktreeFrame {
  const deleteBranch =
    'delete_branch' in msg && msg['delete_branch'] !== undefined ? msg['delete_branch'] : false;
  if (typeof deleteBranch !== 'boolean') {
    throw new Error("frame field must be a bool: 'delete_branch'");
  }
  return {
    kind: WorkerFrameKind.RemoveWorktree,
    requestId: requiredStr(msg, 'request_id'),
    worktreePath: requiredStr(msg, 'worktree_path'),
    branch: optionalNullableStr(msg, 'branch'),
    deleteBranch,
  };
}

function decodeRemoveWorktreeResult(msg: FrameObject): WorkerRemoveWorktreeResultFrame {
  return {
    kind: WorkerFrameKind.RemoveWorktreeResult,
    requestId: requiredStr(msg, 'request_id'),
    status: parseStatusUnion(msg, 'status', ['ok', 'failed']),
    error: optionalNullableStr(msg, 'error'),
  };
}

function decodeCreateDir(msg: FrameObject): WorkerCreateDirFrame {
  return {
    kind: WorkerFrameKind.CreateDir,
    requestId: requiredStr(msg, 'request_id'),
    path: requiredStr(msg, 'path'),
  };
}

function decodeCreateDirResult(msg: FrameObject): WorkerCreateDirResultFrame {
  return {
    kind: WorkerFrameKind.CreateDirResult,
    requestId: requiredStr(msg, 'request_id'),
    status: parseStatusUnion(msg, 'status', ['ok', 'failed']),
    path: optionalNullableStr(msg, 'path'),
    error: optionalNullableStr(msg, 'error'),
  };
}

// ── Field validators ─────────────────────────────────────
//
// Integer fields reject booleans: on the JSON wire a boolean is never a valid
// integer, so the validators exclude `boolean` explicitly. They also require a
// *safe* integer (`Number.isSafeInteger`), i.e. magnitude ≤ 2^53 - 1, matching
// the runner tunnel's `frames.ts`: every integer that round-trips through
// `JSON.parse` without losing precision is accepted, and one beyond 2^53 —
// which `JSON.parse` has already silently rounded — is rejected loudly rather
// than passed through corrupted. Every real worker frame int field
// (`frame_protocol_version`, `limit`, `bytes`, `modified_at`) is small and well
// inside this range, so this only changes behavior for the corrupted-value case.

function isInteger(val: unknown): val is number {
  return typeof val === 'number' && Number.isSafeInteger(val);
}

function requiredStr(msg: FrameObject, key: string): string {
  const val = msg[key];
  if (typeof val !== 'string') {
    throw new Error(`frame missing required string field: ${JSON.stringify(key)}`);
  }
  return val;
}

function requiredInt(msg: FrameObject, key: string): number {
  const val = msg[key];
  if (!isInteger(val)) {
    throw new Error(`frame missing required int field: ${JSON.stringify(key)}`);
  }
  return val;
}

function requiredBool(msg: FrameObject, key: string): boolean {
  const val = msg[key];
  if (typeof val !== 'boolean') {
    throw new Error(`frame missing required bool field: ${JSON.stringify(key)}`);
  }
  return val;
}

/**
 * Validate a required status field into its literal union, rejecting any value
 * outside `allowed` — membership is CHECKED, not cast, so a peer's unknown or
 * typo'd status fails loudly at the frame boundary like an unknown `kind`.
 */
function parseStatusUnion<T extends string>(
  msg: FrameObject,
  key: string,
  allowed: readonly T[],
): T {
  const val = requiredStr(msg, key);
  if (!(allowed as readonly string[]).includes(val)) {
    const expected = allowed.map((item) => JSON.stringify(item)).join(' | ');
    throw new Error(`frame field ${JSON.stringify(key)} must be ${expected}`);
  }
  return val as T;
}

const FS_ENTRY_TYPES: readonly WorkerFsEntryType[] = ['directory', 'file', 'other'];

/** Validate a required filesystem entry `type` into {@link WorkerFsEntryType}. */
function parseEntryType(msg: FrameObject, key: string): WorkerFsEntryType {
  return parseStatusUnion(msg, key, FS_ENTRY_TYPES);
}

/** Validate an optional nullable filesystem `type` (absent / JSON-null → `null`). */
function parseNullableFsType(msg: FrameObject, key: string): WorkerFsEntryType | null {
  const val = optionalNullableStr(msg, key);
  if (val === null) {
    return null;
  }
  if (!(FS_ENTRY_TYPES as readonly string[]).includes(val)) {
    throw new Error(
      `frame field ${JSON.stringify(key)} must be "directory" | "file" | "other" | null`,
    );
  }
  return val as WorkerFsEntryType;
}

/**
 * Return an optional list of strings, defaulting to `[]` when absent.
 *
 * @throws Error if the field is present and not a string list.
 */
function optionalStrList(msg: FrameObject, key: string): string[] {
  if (!(key in msg) || msg[key] === undefined) {
    return [];
  }
  const val = msg[key];
  if (!Array.isArray(val) || !val.every((item) => typeof item === 'string')) {
    throw new Error(`frame field must be a list of strings: ${JSON.stringify(key)}`);
  }
  return [...(val as string[])];
}

/**
 * Return an optional string→bool mapping field.
 *
 * Tolerant by design: absent, null, or non-object values all decode to `null`
 * ("unknown") rather than throwing, so an older or newer peer's hello never
 * breaks the tunnel handshake. Entries with a non-string key or non-bool value
 * are dropped for the same reason.
 */
function optionalStrBoolMap(msg: FrameObject, key: string): Record<string, boolean> | null {
  const val = msg[key];
  if (typeof val !== 'object' || val === null || Array.isArray(val)) {
    return null;
  }
  const out: Record<string, boolean> = {};
  for (const [k, v] of Object.entries(val as Record<string, unknown>)) {
    if (typeof v === 'boolean') {
      out[k] = v;
    }
  }
  return out;
}

/**
 * Return an optional nullable string field, decoding absent or JSON-null to
 * `null`.
 *
 * @throws Error if the field is present and neither a string nor null.
 */
function optionalNullableStr(msg: FrameObject, key: string): string | null {
  const val = msg[key];
  if (val === undefined || val === null) {
    return null;
  }
  if (typeof val !== 'string') {
    throw new Error(`frame field must be a string or null: ${JSON.stringify(key)}`);
  }
  return val;
}

function describeType(val: unknown): string {
  if (val === null) return 'null';
  if (Array.isArray(val)) return 'array';
  return typeof val;
}
