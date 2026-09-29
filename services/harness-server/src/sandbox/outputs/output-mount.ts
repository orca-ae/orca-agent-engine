// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Output-capture mount.
 *
 * Mounts one runner generation's output prefix at
 * `/mnt/session/outputs/` inside the sandbox so the agent's `write` / `bash`
 * tools land bytes directly in S3. The output indexer lists the
 * prefix after tool results and once more on session-end, registering each
 * new or changed blob as a `File` row.
 *
 * Two paths, picked by `runtime.capabilities.supportsFuse`:
 *   - **FUSE (E2B/OpenSandbox):** mount via `s3fs-fuse` with the session's STS creds; no
 *     local cache (`use_cache=` empty) so writes flush straight to S3 — same
 *     opts the template's `start.sh` uses, kept consistent so an operator
 *     debugging a manual spike sees identical behavior. The indexer scans the
 *     S3 prefix.
 *   - **InMemory (no FUSE):** just `mkdir -p /mnt/session/outputs` so the
 *     `write` tool has somewhere to land bytes. The indexer scans the sandbox
 *     filesystem instead of S3 (the indexer picks the path off `kind`).
 *
 * The dispatcher calls {@link mountSessionOutputs} before materializing
 * resources.
 */
import { buildExecutionOutputPrefix, type SessionS3Creds } from '../../auth/sts-creds.js';
import type { SandboxHandle, ToolResult } from '../sandbox-runtime.js';
import { buildS3fsCredentialEnvironment, buildS3fsMountCommand } from '../s3fs-mount.js';
import { assertCanonicalPathOutsideSkillsRoot } from '../skills/materialize.js';
import { OUTPUT_CAPTURE_DIRECTORY } from './output-instructions.js';

/**
 * Discriminator the indexer reads to decide whether to list S3 or scan the
 * sandbox FS. Keeps the indexer free of any sandbox-runtime knowledge.
 */
export type OutputMountKind = 's3' | 'inmemory_local';

export interface OutputMountHandle {
  kind: OutputMountKind;
  /**
   * `/mnt/session/outputs` — same canonical path in both runtimes so agent
   * prompts and tooling can hard-code it.
   */
  sandboxPath: string;
  /**
   * Populated when `kind === 's3'`. The indexer reads `bucket` + `prefix` to
   * paginate `ListObjectsV2`. The prefix MUST end with a `/` — s3fs treats
   * the bucket-relative path as a directory, and a missing trailing slash
   * would mount at the wrong key namespace.
   */
  s3?: {
    bucket: string;
    endpoint: string;
    /** Canonical execution output prefix; always ends with `/`. */
    prefix: string;
    region?: string;
  };
}

export interface MountSessionOutputsInput {
  sandbox: SandboxHandle;
  workspaceId: string;
  sessionId: string;
  /**
   * Unique to one live SessionRunner assignment. The S3 staging prefix is
   * generation-scoped so a later runner can publish the same filename as a
   * new File without rescanning artifacts left by an earlier runner.
   */
  generationId: string;
  /** From `runtime.capabilities.supportsFuse`. */
  supportsFuse: boolean;
  /**
   * Use the sandbox-local directory even when the runtime supports FUSE.
   * `colocated` harnesses use this only when S3 output capture is not
   * configured; configured output capture uses the same FUSE path as
   * `separate` mode.
   */
  forceLocal?: boolean;

  // Required only when supportsFuse=true. Validated at the top of the FUSE
  // branch so the failure path emits a single clear error.
  bucket?: string;
  endpoint?: string;
  /** Root before `workspaces/{workspaceId}/sessions/...`; may be empty. */
  outputsRoot?: string;
  creds?: SessionS3Creds;
  region?: string;
  /** Defaults to true for MinIO compatibility. Set false for virtual-hosted addressing. */
  forcePathStyle?: boolean;
}

/** Canonical sandbox-side path. Same in FUSE + InMemory paths by design. */
const OUTPUTS_SANDBOX_PATH = OUTPUT_CAPTURE_DIRECTORY;

/**
 * Mount the runner-generation output prefix, picking the strategy off
 * `supportsFuse`. The local path creates the root through the portable Files
 * API; the FUSE path issues one `mkdir` followed by an `s3fs` mount via
 * `runPrivileged`.
 */
export async function mountSessionOutputs(
  input: MountSessionOutputsInput,
): Promise<OutputMountHandle> {
  if (!input.supportsFuse || input.forceLocal) {
    return mountInMemoryLocal(input.sandbox);
  }
  return mountFuse(input);
}

/**
 * Ensure the write-policy's fixed output root exists, independently of
 * whether output capture/indexing is configured. Separate-mode policy probes
 * bind and chdir to this path, so treating it as an output-mount side effect
 * makes no-S3 sessions fail before their agent starts.
 *
 * The Files API is runtime-neutral; issuing shell `mkdir` would target the
 * harness host in the InMemory runtime.
 */
export async function ensureSessionOutputRoot(sandbox: SandboxHandle): Promise<void> {
  await assertCanonicalPathOutsideSkillsRoot(sandbox, OUTPUTS_SANDBOX_PATH);
  // `SandboxFiles.write` is the one portable way to create a directory for
  // every runtime. A temporary marker avoids relying on `bash mkdir -p`,
  // which targets the harness host rather than the sandbox in the InMemory
  // runtime. Remove it immediately so it can never become a user artifact.
  const marker = `${OUTPUTS_SANDBOX_PATH}/.orca-output-mount`;
  await sandbox.files.write(marker, Buffer.alloc(0));
  await sandbox.files.delete(marker);
}

/**
 * Non-FUSE runtimes: no privilege and no libfuse. The indexer reads bytes via
 * `sandbox.files.read` instead.
 */
async function mountInMemoryLocal(sandbox: SandboxHandle): Promise<OutputMountHandle> {
  await ensureSessionOutputRoot(sandbox);
  return {
    kind: 'inmemory_local',
    sandboxPath: OUTPUTS_SANDBOX_PATH,
  };
}

/**
 * FUSE-capable runtime: real s3fs mount. Per-session STS creds (minted by
 * `SessionCredsMinter`) use short-lived `runPrivileged` transport envs to
 * create a root-only credentials file. The daemon starts under a scrubbed
 * environment and retains no secret in argv or `/proc/<pid>/environ`.
 */
async function mountFuse(input: MountSessionOutputsInput): Promise<OutputMountHandle> {
  const { bucket, endpoint, outputsRoot, creds, region } = input;
  if (!bucket || !endpoint || outputsRoot === undefined || !creds) {
    throw new Error(
      'mountSessionOutputs: supportsFuse=true requires bucket, endpoint, outputsRoot, creds',
    );
  }
  await assertCanonicalPathOutsideSkillsRoot(input.sandbox, OUTPUTS_SANDBOX_PATH);

  // Per-runner-generation prefix — the trailing slash is REQUIRED. File ids,
  // not filenames, define output identity: a later runner gets a fresh
  // staging prefix and can therefore publish `report.pdf` again as a new File
  // without rescanning the prior runner's copy.
  const prefix = buildExecutionOutputPrefix(outputsRoot, input);

  // mkdir is idempotent; the template pre-creates the dir but we don't depend
  // on it. Non-privileged so the privilege surface stays minimal.
  const mkdirResult = await input.sandbox.run({
    tool: 'bash',
    args: { command: `mkdir -p ${OUTPUTS_SANDBOX_PATH}` },
  });
  assertSuccess(mkdirResult, `mountSessionOutputs: mkdir -p ${OUTPUTS_SANDBOX_PATH} failed`);

  // Mount opts mirror the template's `start.sh::mount_outputs` exactly:
  //   - use_cache=          (empty value) disables on-disk caching so writes
  //                         flush to S3 promptly and the indexer's session-
  //                         end ListObjectsV2 sees them.
  //   - ensure_diskfree=0   keeps s3fs from refusing to upload on tight
  //                         scratch space.
  //   - allow_other         every process inside the sandbox needs to read
  //                         the mount, not just the s3fs uid.
  //   - use_path_request_style works against MinIO (no DNS-style buckets) and
  //     is omitted when virtual-hosted addressing is requested for AWS S3.
  // s3fs's `endpoint=<region>` is required when the bucket lives outside the
  // SDK's default `us-east-1`. Without it, SigV4 signing claims us-east-1 and
  // AWS rejects with `AuthorizationHeaderMalformed`.
  //
  // `compat_dir` makes s3fs accept the prefix even without a directory-marker
  // object (the kind S3 console "folders" produce). `S3BlobStore.put` only
  // writes data objects under the prefix; without `compat_dir` s3fs's
  // `CheckBucket` request returns `NoSuchKey` and the daemon refuses to mount.
  //
  // `uid=1000,gid=1000,umask=0022`: the mount runs as root (sudo), but the
  // sandbox agent runs as `user` (uid 1000). Without these flags s3fs serves
  // every file owned by root with implicit-deny perms, so even with
  // `allow_other` the agent gets `Permission denied`. Pinning uid/gid +
  // umask makes the mount serve `user:user` 0755/0644.
  const regionArg = region ? `,endpoint=${region}` : '';
  const pathStyleArg = input.forcePathStyle === false ? '' : 'use_path_request_style,';
  const mountCmd = buildS3fsMountCommand({
    bucketAndPrefix: `${bucket}:/${prefix}`,
    mountPath: OUTPUTS_SANDBOX_PATH,
    options: `allow_other,${pathStyleArg}use_cache=,ensure_diskfree=0,compat_dir,uid=1000,gid=1000,umask=0022,url=${endpoint}${regionArg}`,
  });
  const envs = buildS3fsCredentialEnvironment(creds);
  const mountResult = await input.sandbox.runPrivileged(mountCmd, { envs });
  assertSuccess(
    mountResult,
    'mountSessionOutputs: s3fs outputs mount failed (CAP_SYS_ADMIN missing? sudo policy?)',
  );

  return {
    kind: 's3',
    sandboxPath: OUTPUTS_SANDBOX_PATH,
    s3: region !== undefined ? { bucket, endpoint, prefix, region } : { bucket, endpoint, prefix },
  };
}

/**
 * Convert a non-zero exit_code into a thrown error carrying the stderr — the
 * operator-side diagnostic for the FUSE path is the difference between "easy
 * fix" (creds typo) and "phase blocker" (missing CAP_SYS_ADMIN).
 */
function assertSuccess(result: ToolResult, prefix: string): void {
  if (result.exit_code !== undefined && result.exit_code !== 0) {
    const stderr = result.stderr ?? '';
    throw new Error(`${prefix} (exit_code=${result.exit_code}): ${stderr}`);
  }
}
