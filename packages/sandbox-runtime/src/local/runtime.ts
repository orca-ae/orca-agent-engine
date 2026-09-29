// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import {
  accessSync,
  constants,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  readlinkSync,
  realpathSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join, relative, resolve as resolvePath, sep as pathSep } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseToolCall } from '../sandbox-runtime.js';
import { killSandboxProcessTree } from '../kill-process-tree.js';
import type {
  EnvironmentSpec,
  SandboxCapabilities,
  SandboxFiles,
  SandboxHandle,
  SandboxReadConstraint,
  SandboxRuntime,
  SpawnHandle,
  ToolCall,
  ToolResult,
  SandboxWritePolicy,
  ValidatedSandboxWritePolicy,
} from '../sandbox-runtime.js';
import type { ReadPage, ReadPageInput } from '../read-page.js';
import { readUtf8FilePage, resolveSandboxPathUnderRoot } from '../read-page.js';
import {
  assertMappedToolPolicyRoots,
  buildMappedToolBubblewrapCommand,
  canonicalAbsolutePath,
  canonicalizeVirtualPathUnderRoot,
  OUTPUT_WRITABLE_PATH,
  validateSandboxWritePolicy,
  type MappedToolRuntimeBind,
} from '../write-policy.js';
import {
  acquireSessionWorkDir,
  releaseSessionWorkDir,
  type SessionWorkDirLayout,
} from './materialize.js';

/**
 * Adapter shape for `@anthropic-ai/sandbox-runtime`'s `SandboxManager` so the
 * runtime can be unit-tested without booting the real sandbox-exec/bwrap
 * chain. Tests inject a fake that records the calls; production passes the
 * real `SandboxManager` from the package.
 *
 * Mirrors only the surface the runtime calls — full type tree lives in the
 * upstream package.
 */
export interface SandboxManagerLike {
  /**
   * Initialize the global sandbox manager. The real implementation starts the
   * HTTP/SOCKS proxy servers and parses the runtime config; subsequent calls
   * are idempotent (the package guards re-init internally).
   */
  initialize(config: SandboxManagerInitConfig): Promise<void>;

  /**
   * Wrap a shell command with the sandbox profile. Returns the wrapped
   * command string suitable for `child_process.spawn(..., { shell: true })`.
   * The fourth `abortSignal` argument is not used here because the runtime
   * owns each spawn end-to-end (including timeout enforcement via
   * `setTimeout` + `proc.kill('SIGKILL')`).
   */
  wrapWithSandbox(
    command: string,
    binShell?: string,
    customConfig?: Partial<SandboxManagerInitConfig>,
  ): Promise<string>;

  /** Release command-scoped trusted state after its subprocess has exited. */
  releaseSandboxCommand?(wrappedCommand: string): Promise<void> | void;
}

/**
 * Subset of `SandboxRuntimeConfig` used by the local runtime. Mirrors the
 * upstream `@anthropic-ai/sandbox-runtime` shape so the adapter can be
 * unit-tested without importing the real zod-validated schema. Fields here
 * are typed permissively (no zod validation in tests) — the runtime forwards
 * the object verbatim and the upstream `SandboxManager.initialize` enforces
 * the strict schema at boot.
 */
export interface SandboxManagerInitConfig {
  network: {
    allowedDomains: string[];
    unrestricted?: boolean;
    deniedDomains: string[];
    allowUnixSockets?: string[];
    allowLocalBinding?: boolean;
  };
  filesystem: {
    denyRead: string[];
    allowRead?: string[];
    allowWrite: string[];
    denyWrite: string[];
  };
  // Forward-compat: any other keys (`enableWeakerNestedSandbox` etc.) pass
  // through unchanged — the upstream type union allows them.
  [extra: string]: unknown;
}

export interface LocalSandboxRuntimeOptions {
  /**
   * Top-level base directory for per-session work-dirs (mirrors
   * {@link ServiceConfig.harnessWorkDir}). Each session gets a fresh directory
   * under `{harnessWorkDir}/sessions/` and that path is exposed
   * read+write to the in-sandbox process.
   */
  harnessWorkDir: string;

  /**
   * Hosts the agent process is allowed to reach, passed through to
   * `SandboxManager.initialize` as `network.allowedDomains`. Empty list = no
   * network access unless `networkUnrestricted` is set. Localhost binding is
   * controlled separately by `network.allowLocalBinding`.
   *
   * The session-runner's local runtime passes the provider API hosts plus the
   * hosts of `AI_GATEWAY_URL` (ai-gateway) and `S3_ENDPOINT` (object storage);
   * its managed-tool runtime passes the domains delivered with its resources.
   */
  allowedNetworkHosts: string[];
  networkUnrestricted?: boolean;

  /**
   * Extra paths (besides the per-session work-dir and minimal system runtime
   * paths) the agent can read. These are operator-controlled trust grants,
   * not tenant input.
   */
  extraReadPaths?: string[];

  /**
   * Extra deny-read paths layered on top of the upstream defaults. The locked
   * design decision (no auto-allow `~`) is enforced by NEVER adding a home-dir
   * path to the allow-list; the deny-list adds belt-and-braces protection
   * against accidental writes to credentials dirs.
   *
   * Defaults to `['~/.ssh', '~/.aws', '~/.config/gcloud']` when undefined.
   */
  extraDenyReadPaths?: string[];

  /**
   * Concrete `SandboxManager` instance. Production passes the singleton from
   * `@anthropic-ai/sandbox-runtime`; tests inject a fake.
   */
  manager: SandboxManagerLike;

  /**
   * Optional shell override. Production defaults to `/bin/bash` so the
   * executable always lives inside the minimal system read roots.
   */
  binShell?: string;

  /** Linux-only model tools: map the private root to / inside nested bubblewrap. */
  managedToolFilesystem?: boolean;
}

let nextId = 0;

const DEFAULT_SANDBOX_RUNTIME_READ_PATHS = [
  '/bin',
  '/usr',
  '/lib',
  '/lib64',
  '/System',
  '/Library',
  '/dev',
  '/etc/ssl',
  '/etc/pki',
  '/etc/ca-certificates',
  '/etc/resolv.conf',
  '/etc/hosts',
  '/etc/nsswitch.conf',
  '/etc/passwd',
  '/etc/group',
  '/etc/ld.so.cache',
  '/etc/localtime',
] as const;

// @anthropic-ai/sandbox-runtime always adds these shared host paths to its
// write allow-list. Deny precedence is stronger than allow precedence, so
// pin them here and use the canonical per-session tmp directory instead.
const SRT_SHARED_WRITE_PATHS = [
  '/tmp/claude',
  '/private/tmp/claude',
  '~/.npm/_logs',
  '~/.claude/debug',
] as const;

// Linux SRT applies its Unix-socket seccomp filter after bubblewrap has
// hidden every path not in filesystem.allowRead. Resolve and pin the exact
// helper shipped by our direct dependency so it remains executable inside
// that mount namespace. Setting seccomp.applyPath also keeps SRT's helper
// lookup and our read grant on the same path (instead of relying on whichever
// local/global installation its fallback search finds first).
const sandboxRuntimeEntry = createRequire(import.meta.url).resolve('@anthropic-ai/sandbox-runtime');
const SRT_SECCOMP_BINARY_PATH = join(
  dirname(dirname(sandboxRuntimeEntry)),
  'vendor',
  'seccomp',
  process.arch,
  'apply-seccomp',
);

// Only system executables, shared libraries and TLS/DNS runtime data enter the
// model filesystem. Never grant /opt, host homes, the worker tree or all of /usr.
const MAPPED_TOOL_RUNTIME_PATHS = [
  '/bin',
  '/usr/bin',
  '/lib',
  '/lib64',
  '/usr/lib',
  '/usr/lib64',
  '/usr/local/bin/node',
  '/usr/share/ca-certificates',
  '/etc/ssl/certs',
  '/etc/ld.so.cache',
  '/etc/nsswitch.conf',
  '/etc/hosts',
  '/etc/resolv.conf',
] as const;

/**
 * SRT's proxy filters use its process-global initialization config, even when
 * wrapWithSandbox receives a different per-command network config. A fresh
 * manager process per command avoids inheriting another handle's host grants.
 * The child carries only the already-scrubbed command environment, and exits
 * after cleaning up its own proxies. No settings files enter the tool root.
 */
export function createManagedToolSandboxManager(): SandboxManagerLike {
  const commandTempDirs = new Map<string, string>();
  return {
    async initialize(): Promise<void> {},
    async wrapWithSandbox(command, binShell, config): Promise<string> {
      if (!config) throw new Error('managed tool sandbox requires an explicit SRT config');
      // AF_UNIX paths have a small fixed limit (108 bytes on Linux). Worker
      // and tool paths may be much longer, so proxy sockets need their own
      // short, private trusted directory, never a path below the tool root.
      const managerTmp = mkdtempSync('/tmp/orca-srt-');
      const managerConfig = {
        ...config,
        filesystem: {
          ...config.filesystem,
          // SRT expands '/' to literal children, including /bin -> /usr/bin
          // on merged-/usr images. Bubblewrap refuses symlink destinations.
          // Canonicalize those masks and remove descendants already covered by
          // an ancestor, retaining the same deny-all/read-exceptions policy.
          denyRead: canonicalManagedReadDenials(config.filesystem?.denyRead ?? []),
          // SRT mounts its bridge sockets before applying denyRead. Re-open
          // their private directory for its trusted outer socat processes;
          // the mapped model namespace hides it beneath its fresh /tmp.
          allowRead: [...(config.filesystem?.allowRead ?? []), managerTmp],
        },
      };
      const script = `
import { spawn } from 'node:child_process';
Object.assign(process.env, ${JSON.stringify({ TMPDIR: managerTmp, TMP: managerTmp, TEMP: managerTmp })});
const { SandboxManager } = await import(${JSON.stringify(pathToFileURL(sandboxRuntimeEntry).href)});
const { command, binShell, config } = JSON.parse(process.argv[1]);
// SRT distinguishes an absent allowedDomains (no network restriction) from []
// (deny all). Only the explicit trusted policy flag can select the former.
if (config.network.unrestricted === true) delete config.network.allowedDomains;
delete config.network.unrestricted;
try {
  await SandboxManager.initialize(config);
  const wrapped = await SandboxManager.wrapWithSandbox(command, binShell, config);
  const code = await new Promise((resolve, reject) => {
    const child = spawn(wrapped, { shell: true, stdio: 'inherit' });
    child.once('error', reject);
    child.once('exit', (code, signal) => resolve(signal ? 124 : (code ?? 1)));
  });
  process.exitCode = code;
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
} finally {
  SandboxManager.cleanupAfterCommand();
  await SandboxManager.reset();
}
// SRT leaves fallback cleanup timers scheduled after the bridge processes exit.
// Their work is complete after reset; do not delay every model tool by 5 seconds.
process.exit(process.exitCode ?? 1);`;
      const wrapped = [
        process.execPath,
        '--input-type=module',
        '-e',
        script,
        JSON.stringify({ command, binShell, config: managerConfig }),
      ]
        .map((value) => `'${escapeSingleQuotes(value)}'`)
        .join(' ');
      commandTempDirs.set(wrapped, managerTmp);
      return wrapped;
    },
    releaseSandboxCommand(wrapped): void {
      const directory = commandTempDirs.get(wrapped);
      if (directory === undefined) return;
      rmSync(directory, { recursive: true, force: true });
      commandTempDirs.delete(wrapped);
    },
  };
}

function canonicalManagedReadDenials(paths: readonly string[]): string[] {
  const skip = new Set(['proc', 'dev', 'sys']);
  const expanded = paths.flatMap((path) =>
    path === '/'
      ? readdirSync('/')
          .filter((name) => !skip.has(name))
          .map((name) => `/${name}`)
      : [path],
  );
  const canonical = [...new Set(expanded.map(canonicalPathIfPresent))];
  return canonical.filter(
    (path) => !canonical.some((parent) => parent !== path && path.startsWith(`${parent}/`)),
  );
}

function canonicalPathIfPresent(path: string): string {
  try {
    return realpathSync(path);
  } catch {
    return path;
  }
}

function filesystemIdentity(path: string): string {
  const info = statSync(path);
  return `${info.dev}:${info.ino}`;
}

function prepareManagedRuntimeTargets(root: string): MappedToolRuntimeBind[] {
  const bindings: MappedToolRuntimeBind[] = [];
  for (const target of MAPPED_TOOL_RUNTIME_PATHS) {
    if (!existsSync(target)) continue;
    const source = realpathSync(target);
    if (
      source === '/' ||
      source === root ||
      source.startsWith(`${root}/`) ||
      root.startsWith(`${source}/`)
    ) {
      throw new Error(`mapped tool runtime source aliases the private root: ${source}`);
    }
    const info = statSync(source);
    if (!info.isDirectory() && !info.isFile()) {
      throw new Error(`mapped tool runtime source is not a file or directory: ${source}`);
    }
    const destination = join(root, target);
    assertNoSymlinkComponents(root, destination, true);
    if (info.isDirectory()) {
      mkdirSync(destination, { recursive: true, mode: 0o755 });
    } else {
      mkdirSync(dirname(destination), { recursive: true, mode: 0o755 });
      writeFileSync(destination, '', { flag: 'wx', mode: 0o644 });
    }
    bindings.push({ source, target });
  }
  for (const target of ['/tmp', '/dev', '/proc']) {
    mkdirSync(join(root, target), { recursive: true, mode: 0o755 });
  }
  return bindings;
}

/**
 * Resolve a sandbox-visible path under {@link root} on the host FS, blocking
 * any traversal that would escape the work-dir.
 *
 * `path.join` collapses `..` segments but happily lets the result climb above
 * `root` (e.g. `join('/work/sessions/x', 'a/../../etc/passwd')` →
 * `/work/etc/passwd`). The harness's `agent-toolset` calls
 * {@link LocalSandboxFiles}'s read/write/list/delete OUTSIDE the `srt` kernel
 * sandbox, so an LLM-driven `path` parameter has to be checked here — there
 * is no second line of defense.
 *
 * Used by both {@link LocalSandboxFiles.resolve} and
 * {@link LocalSandboxHandle.toHostPath} so the FS allow-list is consistent
 * across host-side file ops and the in-sandbox `bash`/`glob`/`grep` cwd.
 */
function resolveUnderRoot(root: string, p: string): string {
  if (p.includes('\0')) {
    throw new Error('sandbox file path must not contain a NUL byte');
  }
  const trimmed = p.startsWith('/') ? p.slice(1) : p;
  // `path.resolve` normalizes `..` against `root` — but it doesn't bound the
  // result to `root`. We then explicitly assert containment.
  const resolved = resolvePath(root, trimmed);
  if (resolved !== root && !resolved.startsWith(root + pathSep)) {
    throw new Error(`path '${p}' escapes the sandbox work-dir`);
  }
  return resolved;
}

type LocalFileOperation = 'write' | 'read' | 'list' | 'chmod' | 'delete';

interface CapturedCommandResult {
  stdout: Buffer;
  stderr: Buffer;
  exitCode: number;
}

/**
 * Runs one filesystem operation inside the same OS sandbox used for agent
 * commands. Host-side preflight checks alone cannot close the lstat/open race:
 * a sandbox process could replace an intermediate directory with a symlink
 * after the check. The helper still uses O_NOFOLLOW and checks every existing
 * component, while the enclosing Seatbelt/bwrap profile is the authoritative
 * boundary if a component changes concurrently.
 */
const LOCAL_FILE_HELPER_SOURCE = String.raw`
const fs = require('node:fs');
const path = require('node:path');

function fail(message) {
  throw new Error(message);
}

function resolveUnderRoot(root, requested) {
  const full = path.resolve(requested);
  const rel = path.relative(root, full);
  if (rel === '..' || rel.startsWith('..' + path.sep) || path.isAbsolute(rel)) {
    fail('file operation escaped the sandbox root');
  }
  return full;
}

function assertNoSymlinkComponents(root, target, allowMissing) {
  const rel = path.relative(root, target);
  let current = root;
  for (const segment of rel === '' ? [] : rel.split(path.sep)) {
    current = path.join(current, segment);
    try {
      if (fs.lstatSync(current).isSymbolicLink()) {
        fail('file operation path contains a symbolic link');
      }
    } catch (error) {
      if (allowMissing && error && error.code === 'ENOENT') return;
      throw error;
    }
  }
}

async function main() {
  const operation = process.env.ORCA_LOCAL_FILE_OPERATION;
  const root = path.resolve(process.env.ORCA_LOCAL_FILE_ROOT || '');
  const target = resolveUnderRoot(root, process.env.ORCA_LOCAL_FILE_PATH || '');

  if (operation === 'write') {
    assertNoSymlinkComponents(root, target, true);
    fs.mkdirSync(path.dirname(target), { recursive: true, mode: 0o700 });
    assertNoSymlinkComponents(root, target, true);
    const chunks = [];
    for await (const chunk of process.stdin) chunks.push(chunk);
    const fd = fs.openSync(
      target,
      fs.constants.O_WRONLY |
        fs.constants.O_CREAT |
        fs.constants.O_TRUNC |
        fs.constants.O_NOFOLLOW,
      0o600,
    );
    try {
      fs.writeFileSync(fd, Buffer.concat(chunks));
    } finally {
      fs.closeSync(fd);
    }
    return;
  }

  if (operation === 'read') {
    assertNoSymlinkComponents(root, target, false);
    const fd = fs.openSync(target, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    try {
      process.stdout.write(fs.readFileSync(fd));
    } finally {
      fs.closeSync(fd);
    }
    return;
  }

  if (operation === 'list') {
    assertNoSymlinkComponents(root, target, false);
    process.stdout.write(JSON.stringify(fs.readdirSync(target)));
    return;
  }

  if (operation === 'chmod') {
    assertNoSymlinkComponents(root, target, false);
    const chunks = [];
    for await (const chunk of process.stdin) chunks.push(chunk);
    const modeText = Buffer.concat(chunks).toString('utf8');
    // SRT's shell quoting makes the JavaScript negation token invalid inside node -e source.
    if (/^[0-7]{3}$/.test(modeText) === false) fail('invalid chmod mode');
    fs.chmodSync(target, Number.parseInt(modeText, 8));
    return;
  }

  if (operation === 'delete') {
    if (target === root) fail('refusing to delete the sandbox root');
    assertNoSymlinkComponents(root, target, true);
    fs.rmSync(target, { recursive: true, force: true });
    return;
  }

  fail('unknown local file operation');
}

main().catch((error) => {
  process.stderr.write(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
`;

function assertNoSymlinkComponents(root: string, target: string, allowMissing: boolean): void {
  const relativePath = relative(root, target);
  let current = root;
  const segments = relativePath === '' ? [] : relativePath.split(pathSep);
  for (const segment of segments) {
    current = join(current, segment);
    try {
      if (lstatSync(current).isSymbolicLink()) {
        throw new Error(`path '${target}' contains a symbolic link`);
      }
    } catch (error) {
      if (
        allowMissing &&
        typeof error === 'object' &&
        error !== null &&
        (error as NodeJS.ErrnoException).code === 'ENOENT'
      ) {
        return;
      }
      throw error;
    }
  }
}

/**
 * Build the environment visible to an agent-owned subprocess.
 *
 * The harness process itself carries infrastructure credentials and service
 * configuration. Passing its full environment into a sandboxed command would
 * bypass the filesystem/network boundary by exposing those values directly to
 * the agent. Keep only the small set of host settings needed for command
 * lookup, locale handling, terminal behavior, and time-zone formatting; temp
 * and home directories are always replaced with session-scoped paths.
 */
function buildSandboxSubprocessEnv(
  cwd: string,
  tmpDir: string,
  hostEnv: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {
    HOME: cwd,
    TMPDIR: tmpDir,
    TMP: tmpDir,
    TEMP: tmpDir,
  };
  const allowedHostKeys = [
    'PATH',
    'LANG',
    'LANGUAGE',
    'LC_ALL',
    'LC_COLLATE',
    'LC_CTYPE',
    'LC_MESSAGES',
    'LC_MONETARY',
    'LC_NUMERIC',
    'LC_TIME',
    'TERM',
    'COLORTERM',
    'TZ',
  ] as const;
  for (const key of allowedHostKeys) {
    const value = hostEnv[key];
    if (value !== undefined) env[key] = value;
  }
  return env;
}

/**
 * Local-host sandbox: spawns agent processes inside an `srt`-managed sandbox
 * (the `@anthropic-ai/sandbox-runtime` package). Selected explicitly via
 * `SANDBOX_RUNTIME=local` for the local-stack release.
 *
 * The runtime is the production-equivalent of `InMemorySandboxRuntime` —
 * filesystem ops go through a host directory and `bash` runs locally — but
 * unlike InMemory it wraps every command via
 * `SandboxManager.wrapWithSandbox`, so the OS-level filesystem + network
 * allow-lists are enforced. `supportsFuse=false`: the strategy factory will
 * pick `tarball_prefetch` for files and `local_memory` for memory stores.
 *
 * Per-session lifecycle:
 *   1. `acquire`: ensure the SandboxManager is initialized, then mkdir the
 *      per-session work-dir.
 *   2. The dispatcher runs `materializeResources` which calls into the
 *      existing strategies (`TarballPrefetchStrategy`, `LocalMemoryStrategy`,
 *      `GitCloneStrategy`) that all write through `SandboxHandle.files` —
 *      with the work-dir as the file root, this materializes resources onto
 *      the host FS at the agreed `mount_path`.
 *   3. Each `sandbox.run({tool:'bash'})` is spawned via
 *      `wrapWithSandbox(...) → child_process.spawn(wrapped, { shell: true })`.
 *      The wrapped string contains the OS-level sandbox invocation
 *      (`sandbox-exec` on macOS / `bwrap` on Linux) so violations are caught
 *      by the kernel, not the runtime.
 *   4. `destroy`: rm -rf the per-session work-dir.
 */
export class LocalSandboxRuntime implements SandboxRuntime {
  /**
   * No FUSE: the local runtime has no privilege boundary inside the srt
   * sandbox (`bubblewrap` blocks `mount` calls, `sandbox-exec` doesn't
   * surface `CAP_SYS_ADMIN`). The strategy factory falls back to
   * `tarball_prefetch` for files and `local_memory` for memory_store
   * resources whenever `supportsFuse` is false.
   */
  readonly capabilities: SandboxCapabilities = {
    supportsFuse: false,
    supportsLocalMemory: true,
    supportsWritePolicy: true,
  };

  private initialized = false;
  private initPromise: Promise<void> | undefined;

  constructor(private readonly opts: LocalSandboxRuntimeOptions) {
    if (opts.managedToolFilesystem) {
      if (process.platform !== 'linux') {
        throw new Error('managed tool filesystem requires Linux SRT and bubblewrap');
      }
      accessSync(SRT_SECCOMP_BINARY_PATH, constants.X_OK);
      // Validate before SRT sees the list; never collect ambient gateway or
      // object-store origins for model tools.
      const policy = validateSandboxWritePolicy({
        writablePaths: [],
        readonlyPaths: [],
        networkAllowedDomains: opts.allowedNetworkHosts,
      });
      this.opts = { ...opts, allowedNetworkHosts: [...(policy.networkAllowedDomains ?? [])] };
    }
  }

  /**
   * Build the {@link SandboxManagerInitConfig} for a given session work-dir.
   * Exported as a method (rather than a free function) so tests can assert
   * the exact shape that gets passed to `SandboxManager.initialize`.
   *
   * Layout decisions:
   *   - `network.allowedDomains` = `allowedNetworkHosts`, each origin listed
   *     explicitly; `network.unrestricted` is set only by `networkUnrestricted`.
   *   - `network.allowLocalBinding` is true (false with `managedToolFilesystem`)
   *     so harness child processes can bind to localhost ports (the srt manager
   *     itself runs the proxy on 127.0.0.1).
   *   - `filesystem.allowWrite` = the per-session work-dir + its `tmp/`
   *     subdir. The system `/tmp` is NOT auto-allowed; the runtime exposes
   *     the session's own `tmp/` instead so cross-session leakage is
   *     impossible.
   *   - SRT's read model is deny-then-re-allow (reads otherwise default to
   *     the whole host), so `filesystem.denyRead` always contains `/` and
   *     `allowRead` re-opens only this session plus minimal system paths.
   */
  buildSandboxConfig(layout: SessionWorkDirLayout): SandboxManagerInitConfig {
    const extraReadPaths = this.opts.extraReadPaths ?? [];
    const extraDenyReadPaths = this.opts.extraDenyReadPaths ?? [
      '~/.ssh',
      '~/.aws',
      '~/.config/gcloud',
    ];
    // session-scoped tmp under work-dir; NOT host /tmp (spec: "/tmp/{sessionId}-...")
    const writePaths = [layout.root, layout.tmp].map(canonicalPathIfPresent);
    const readPaths = [
      ...writePaths,
      ...DEFAULT_SANDBOX_RUNTIME_READ_PATHS,
      SRT_SECCOMP_BINARY_PATH,
      ...extraReadPaths,
    ].map(canonicalPathIfPresent);
    return {
      network: {
        allowedDomains: [...this.opts.allowedNetworkHosts],
        ...(this.opts.networkUnrestricted === true ? { unrestricted: true } : {}),
        deniedDomains: [],
        allowLocalBinding: this.opts.managedToolFilesystem !== true,
      },
      filesystem: {
        denyRead: ['/', ...extraDenyReadPaths],
        allowRead: [...new Set(readPaths)],
        allowWrite: writePaths,
        denyWrite: [...SRT_SHARED_WRITE_PATHS],
      },
      seccomp: {
        applyPath: SRT_SECCOMP_BINARY_PATH,
      },
    };
  }

  async acquire(env: EnvironmentSpec): Promise<SandboxHandle> {
    void env;
    const id = `sbx_local_${++nextId}_${Date.now().toString(36)}`;
    const layout = await acquireSessionWorkDir({
      baseDir: this.opts.harnessWorkDir,
      sessionId: id,
    });
    try {
      const config = this.buildSandboxConfig(layout);
      await this.ensureInitialized(config);
      // Mirror the constructor pattern: only set `binShell` when defined, so
      // `exactOptionalPropertyTypes` doesn't flag a `string | undefined` slot
      // as a strict-mode error.
      const handleOpts: LocalSandboxHandleOptions = {
        id,
        layout,
        config,
        manager: this.opts.manager,
        managedToolFilesystem: this.opts.managedToolFilesystem === true,
      };
      if (this.opts.binShell !== undefined) handleOpts.binShell = this.opts.binShell;
      return new LocalSandboxHandle(handleOpts);
    } catch (error) {
      try {
        await releaseSessionWorkDir(layout);
      } catch (cleanupError) {
        throw new AggregateError(
          [error, cleanupError],
          `failed to initialize and clean up local sandbox ${id}`,
        );
      }
      throw error;
    }
  }

  /**
   * Initialize the global SandboxManager exactly once. The upstream package
   * already guards re-init, but threading the promise here ensures concurrent
   * `acquire` calls share a single in-flight initialization rather than racing
   * — important because the manager spins up TCP/Unix proxies that take a
   * moment to bind.
   */
  private async ensureInitialized(config: SandboxManagerInitConfig): Promise<void> {
    if (this.initialized) return;
    if (!this.initPromise) {
      const attempt = this.opts.manager.initialize(config).then(() => {
        this.initialized = true;
      });
      // A rejected attempt must not be cached forever: one transient failure
      // (e.g. the srt proxy losing a port-bind race at startup) would otherwise
      // poison every later acquire() with the same stale rejection until the
      // process restarts. Clear the slot so the next acquire retries; current
      // awaiters still observe this attempt's rejection.
      attempt.catch(() => {
        if (this.initPromise === attempt) {
          this.initPromise = undefined;
        }
      });
      this.initPromise = attempt;
    }
    await this.initPromise;
  }
}

interface LocalSandboxHandleOptions {
  id: string;
  layout: SessionWorkDirLayout;
  config: SandboxManagerInitConfig;
  manager: SandboxManagerLike;
  binShell?: string;
  managedToolFilesystem?: boolean;
}

class LocalSandboxFiles implements SandboxFiles {
  constructor(
    private readonly root: string,
    private readonly runOperation: (
      operation: LocalFileOperation,
      fullPath: string,
      input?: Buffer,
    ) => Promise<Buffer>,
  ) {}

  /**
   * Resolve a sandbox-visible path to its host-FS counterpart. Absolute paths
   * are pinned under `{root}/{abs}`; relative paths land under `root/`.
   * Delegates to {@link resolveUnderRoot} so any `..` traversal that would
   * escape the work-dir is rejected — these calls run in the harness Node
   * process, OUTSIDE the `srt` kernel sandbox, so this is the only barrier.
   */
  private resolve(path: string): string {
    return resolveUnderRoot(this.root, path);
  }

  async write(path: string, content: Buffer | NodeJS.ReadableStream): Promise<void> {
    const full = this.resolve(path);
    assertNoSymlinkComponents(this.root, full, true);
    let bytes: Buffer;
    if (Buffer.isBuffer(content)) {
      bytes = content;
    } else {
      const chunks: Buffer[] = [];
      for await (const chunk of content) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as unknown as Uint8Array));
      }
      bytes = Buffer.concat(chunks);
    }
    await this.runOperation('write', full, bytes);
  }

  async read(path: string): Promise<Buffer> {
    const full = this.resolve(path);
    assertNoSymlinkComponents(this.root, full, false);
    return await this.runOperation('read', full);
  }

  async readUtf8Page(
    path: string,
    input: ReadPageInput,
    constraint?: SandboxReadConstraint,
  ): Promise<ReadPage> {
    const mappedConstraint = constraint
      ? {
          readableRoots: constraint.readableRoots.map((root) =>
            resolveSandboxPathUnderRoot(this.root, root),
          ),
        }
      : undefined;
    return await readUtf8FilePage(
      resolveSandboxPathUnderRoot(this.root, path),
      mappedConstraint,
      input,
    );
  }

  async list(path: string): Promise<string[]> {
    const full = this.resolve(path);
    assertNoSymlinkComponents(this.root, full, false);
    const output = await this.runOperation('list', full);
    const parsed: unknown = JSON.parse(output.toString('utf8'));
    if (!Array.isArray(parsed) || !parsed.every((entry) => typeof entry === 'string')) {
      throw new Error('local sandbox file list returned an invalid response');
    }
    return parsed;
  }

  async chmod(path: string, mode: number): Promise<void> {
    const full = this.resolve(path);
    assertNoSymlinkComponents(this.root, full, false);
    const normalizedMode = normalizeFileMode(mode);
    await this.runOperation(
      'chmod',
      full,
      Buffer.from(normalizedMode.toString(8).padStart(3, '0')),
    );
  }

  async delete(path: string): Promise<void> {
    const full = this.resolve(path);
    assertNoSymlinkComponents(this.root, full, true);
    await this.runOperation('delete', full);
  }
}

class LocalSandboxHandle implements SandboxHandle {
  readonly id: string;
  readonly files: SandboxFiles;
  private destroyed = false;
  private destroyComplete = false;
  private destroyPromise: Promise<void> | null = null;
  private readonly layout: SessionWorkDirLayout;
  private readonly canonicalRoot: string;
  private readonly config: SandboxManagerInitConfig;
  private readonly manager: SandboxManagerLike;
  private readonly binShell?: string;
  private readonly managedToolFilesystem: boolean;
  private readonly managedRuntimeBinds: MappedToolRuntimeBind[];
  private readonly rootIdentity: string;
  private managedWritePolicy: ValidatedSandboxWritePolicy | undefined;

  constructor(opts: LocalSandboxHandleOptions) {
    this.id = opts.id;
    this.layout = opts.layout;
    this.canonicalRoot = canonicalPathIfPresent(opts.layout.root);
    this.config = opts.config;
    this.manager = opts.manager;
    this.managedToolFilesystem = opts.managedToolFilesystem === true;
    this.rootIdentity = filesystemIdentity(this.canonicalRoot);
    this.managedRuntimeBinds = this.managedToolFilesystem
      ? prepareManagedRuntimeTargets(this.canonicalRoot)
      : [];
    if (opts.binShell !== undefined) this.binShell = opts.binShell;
    this.files = new LocalSandboxFiles(opts.layout.root, async (operation, fullPath, input) => {
      return await this.runFileOperation(operation, fullPath, input);
    });
  }

  async run(call: ToolCall): Promise<ToolResult> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);
    const parsed = parseToolCall(call);
    if (parsed.tool === 'invalid') {
      return { exit_code: 2, stderr: parsed.error };
    }

    if (parsed.tool === 'bash') {
      const args = parsed.args;
      return this.runWrapped(args.command, args.timeout_ms);
    }

    if (parsed.tool === 'glob') {
      const args = parsed.args;
      const cwd = args.root ?? '/';
      // Same shell shape as InMemorySandboxRuntime so behavior stays identical
      // between the two runtimes. No `|| true`: a failed cd exits 2 and
      // compgen's no-match exit 1 is the only non-zero success — a bad root or
      // compgen error must reach the agent, not read as an empty match list.
      const cmd = `cd '${escapeSingleQuotes(this.toHostPath(cwd))}' || exit 2; compgen -G '${escapeSingleQuotes(args.pattern)}'`;
      const result = await this.runWrapped(cmd);
      const matches = (result.stdout ?? '').split('\n').filter((s) => s.length > 0);
      if ((result.exit_code ?? 0) > 1) {
        // Keep any partial matches alongside the fault (see grep below).
        return { exit_code: result.exit_code ?? 1, stderr: result.stderr ?? '', output: matches };
      }
      return { output: matches };
    }

    if (parsed.tool === 'grep') {
      const args = parsed.args;
      const cwd = args.root ?? '/';
      // grep exits 0 match / 1 no match / >=2 error; cd failure exits 2.
      const cmd = `cd '${escapeSingleQuotes(this.toHostPath(cwd))}' || exit 2; grep -rn '${escapeSingleQuotes(args.pattern)}' .`;
      const result = await this.runWrapped(cmd);
      if ((result.exit_code ?? 0) > 1) {
        // grep exits 2 on ANY error even when it also matched — surface the
        // fault WITH the partial matches, not instead of them.
        return {
          exit_code: result.exit_code ?? 1,
          stderr: result.stderr ?? '',
          output: result.stdout ?? '',
        };
      }
      return { output: result.stdout ?? '' };
    }

    return { exit_code: 127, stderr: `unknown tool: ${parsed.name}` };
  }

  /**
   * Spawn a long-lived command inside the `srt` sandbox, returning its live
   * stdio streams. Wraps the command with {@link SandboxManagerLike.wrapWithSandbox}
   * exactly like {@link runWrapped} — so the OS-level filesystem + network
   * allow-lists apply to the spawned process — then hands back the child's
   * stdout/stdin plus a `kill` for explicit termination.
   *
   * `opts.cwd` is a sandbox-visible path re-anchored under the work-dir root
   * (matching {@link toHostPath}); `opts.env` layers over the same scrubbed
   * subprocess environment `run()` uses, so a long-lived spawned process gets
   * the identical host-secret boundary as a one-shot `bash` call.
   */
  async spawn(
    cmd: string,
    opts?: { env?: Record<string, string>; cwd?: string },
  ): Promise<SpawnHandle> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);
    const wrapped = await this.manager.wrapWithSandbox(cmd, this.binShell, this.config);
    const cwd = opts?.cwd ? this.toHostPath(opts.cwd) : this.canonicalRoot;
    const proc = spawn(wrapped, {
      shell: true,
      // Detached: the wrapper is `sh -> sandbox-exec -> bash -c 'cmd'`, and a
      // signal aimed at the outer shell alone orphans the actual command. As a
      // group leader, kill() below can signal the whole tree.
      detached: true,
      cwd,
      env: {
        ...buildSandboxSubprocessEnv(cwd, canonicalPathIfPresent(this.layout.tmp)),
        ...opts?.env,
      },
    });
    // The 'error' listener is load-bearing: a spawn failure (nonexistent cwd,
    // EMFILE under fork pressure) with no listener is an unhandled 'error'
    // event that crashes the whole long-lived harness process. `exited` gives
    // the caller the death signal SpawnHandle otherwise lacks; the no-op catch
    // keeps it from surfacing as an unhandled rejection when unobserved.
    const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
      (resolve, reject) => {
        proc.on('error', (err) => reject(err));
        proc.on('close', (code, signal) => resolve({ code, signal }));
      },
    ).finally(async () => await this.manager.releaseSandboxCommand?.(wrapped));
    exited.catch(() => {});
    return {
      stdout: proc.stdout!,
      stdin: proc.stdin!,
      kill: (signal?: NodeJS.Signals) => {
        killSandboxProcessTree(proc, signal ?? 'SIGTERM');
      },
      exited,
    };
  }

  async prepareFilesystemRoots(_paths: readonly string[]): Promise<void> {
    // LocalSandboxRuntime owns a fresh private workdir for this handle; planned
    // virtual roots cannot be supplied by an external sandbox image.
  }

  async prepareWritePolicy(policy: SandboxWritePolicy): Promise<void> {
    if (this.managedToolFilesystem) {
      const validated = validateSandboxWritePolicy(policy);
      const hostPidNamespace = readlinkSync('/proc/self/ns/pid');
      const hostNetworkNamespace = readlinkSync('/proc/self/ns/net');
      // Some filesystems enforce read-only data writes but still allow chmod.
      // Test an owned inode, not just creation on the read-only root mount.
      const sentinel = `/.orca-readonly-probe-${randomUUID()}`;
      const sentinelPath = join(this.canonicalRoot, sentinel);
      writeFileSync(sentinelPath, 'readonly', { mode: 0o444, flag: 'wx' });
      try {
        // Exercise the exact SRT -> seccomp -> mapped bwrap chain. A version
        // check or successful outer sandbox alone cannot establish this boundary.
        const probeCommand = `set -eu
[ "$(stat -Lc '%d:%i' /)" = '${escapeSingleQuotes(this.rootIdentity)}' ]
[ "$(readlink /proc/self/ns/pid)" != '${escapeSingleQuotes(hostPidNamespace)}' ]
[ "$(readlink /proc/self/ns/net)" ${validated.networkUnrestricted ? '=' : '!='} '${escapeSingleQuotes(hostNetworkNamespace)}' ]
[ "$HOME" = /tmp/home ]
[ "$TMPDIR" = /tmp ]
if ( : > /.orca-readonly-probe ) 2>/dev/null; then
  echo 'mapped tool root is writable' >&2
  exit 73
fi
chmod 0644 '${sentinel}' 2>/dev/null || true
if [ "$(stat -c %a '${sentinel}')" != 444 ]; then
  echo 'mapped tool filesystem permits read-only metadata changes' >&2
  exit 74
fi
probe_file="$(mktemp /mnt/session/outputs/.orca-probe.XXXXXX)"
rm -- "$probe_file"`;
        const probe = await this.runPolicyCommand(probeCommand, validated, 10_000);
        if (probe.exit_code !== 0) {
          throw new Error(
            `managed tool filesystem isolation probe failed: ${probe.stderr ?? 'unknown error'}`,
          );
        }
        if ((statSync(sentinelPath).mode & 0o777) !== 0o444) {
          throw new Error('managed tool filesystem permits read-only metadata changes');
        }
        this.managedWritePolicy = validated;
      } finally {
        rmSync(sentinelPath, { force: true });
      }
      return;
    }
    // Unlike the remote Linux runtimes, Local owns a fresh private workdir and
    // cannot inherit image mounts or hard links. Its SRT probe therefore checks
    // the generated policy without the Linux-only /proc mount-identity probe.
    const probe = await this.runPolicyCommand('true', policy);
    if (probe.exit_code !== 0) {
      throw new Error(
        `local write-policy sandbox probe failed: ${probe.stderr ?? 'unknown error'}`,
      );
    }
  }

  async runWithWritePolicy(call: ToolCall, policy: SandboxWritePolicy): Promise<ToolResult> {
    const parsed = parseToolCall(call);
    if (parsed.tool === 'invalid') {
      return { exit_code: 2, stderr: parsed.error };
    }
    if (parsed.tool === 'bash') {
      const args = parsed.args;
      return await this.runPolicyCommand(args.command, policy, args.timeout_ms);
    }
    if (parsed.tool === 'glob') {
      const args = parsed.args;
      const cwd = args.root ?? '/';
      const cmd = `cd '${escapeSingleQuotes(this.policyCommandCwd(cwd))}' || exit 2; compgen -G '${escapeSingleQuotes(args.pattern)}'`;
      const result = await this.runPolicyCommand(cmd, policy);
      const matches = (result.stdout ?? '').split('\n').filter((s) => s.length > 0);
      if ((result.exit_code ?? 0) > 1) {
        return { exit_code: result.exit_code ?? 1, stderr: result.stderr ?? '', output: matches };
      }
      return { output: matches };
    }
    if (parsed.tool === 'grep') {
      const args = parsed.args;
      const cwd = args.root ?? '/';
      const cmd = `cd '${escapeSingleQuotes(this.policyCommandCwd(cwd))}' || exit 2; grep -rn '${escapeSingleQuotes(args.pattern)}' .`;
      const result = await this.runPolicyCommand(cmd, policy);
      if ((result.exit_code ?? 0) > 1) {
        return {
          exit_code: result.exit_code ?? 1,
          stderr: result.stderr ?? '',
          output: result.stdout ?? '',
        };
      }
      return { output: result.stdout ?? '' };
    }
    return { exit_code: 127, stderr: `unknown tool: ${parsed.name}` };
  }

  async canonicalizePathForPolicy(path: string): Promise<string> {
    return canonicalizeVirtualPathUnderRoot(this.layout.root, path);
  }

  // `runPrivileged` is deliberately ABSENT: `srt` blocks `sudo` and the FUSE
  // strategies are never selected against this runtime (the strategy factory
  // enforces that). The missing method is the feature-detection signal, same
  // convention as `spawn` / `endpoint`.

  async pause(): Promise<void> {
    /* no-op: host FS persists across calls */
  }

  async resume(): Promise<void> {
    /* no-op */
  }

  async destroy(): Promise<void> {
    if (this.destroyComplete) return;
    this.destroyed = true;
    this.destroyPromise ??= releaseSessionWorkDir(this.layout);
    try {
      await this.destroyPromise;
      this.destroyComplete = true;
    } catch (error) {
      this.destroyPromise = null;
      throw error;
    }
  }

  /**
   * Test helper — exposes the host work-dir so unit specs can assert that
   * resource bytes landed in the right place. Mirrors
   * `InMemorySandboxRuntime.rootDir`.
   */
  rootDir(): string {
    return this.layout.root;
  }

  /**
   * Wrap a shell command with the sandbox profile and execute it. Splits
   * stdout / stderr / exit code into `ToolResult`. Honors an optional
   * `timeout_ms` by SIGKILL'ing the child when it expires.
   *
   * The per-session `config` is forwarded as `customConfig` (third arg) so
   * the wrap reflects THIS session's allow-list, not the first session's.
   * `SandboxManager.initialize` is global+idempotent, so without this
   * override every session would inherit session-1's writable work-dir —
   * that's a cross-session write leak. See upstream `wrapWithSandbox`
   * signature: `(command, binShell?, customConfig?, abortSignal?)`.
   */
  private async runWrapped(command: string, timeoutMs?: number): Promise<ToolResult> {
    return await this.runWrappedWithConfig(command, this.config, this.canonicalRoot, timeoutMs);
  }

  private async runPolicyCommand(
    command: string,
    policy: SandboxWritePolicy,
    timeoutMs?: number,
  ): Promise<ToolResult> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);
    const validated = validateSandboxWritePolicy(policy);
    if (this.managedToolFilesystem) {
      this.validateManagedRoots(validated);
      const allowedDomains = [...(validated.networkAllowedDomains ?? [])];
      if (
        (validated.networkUnrestricted === true && this.config.network.unrestricted !== true) ||
        (this.config.network.unrestricted !== true &&
          allowedDomains.some((domain) => !this.config.network.allowedDomains.includes(domain)))
      ) {
        throw new Error('managed tool policy exceeds the runtime network allow-list');
      }
      const config: SandboxManagerInitConfig = {
        ...this.config,
        network: {
          allowedDomains,
          deniedDomains: [],
          allowLocalBinding: false,
          unrestricted: validated.networkUnrestricted === true,
        },
        filesystem: {
          ...this.config.filesystem,
          // SRT restores narrow write binds BEFORE re-binding read ancestors;
          // allowRead:[toolRoot] otherwise masks those writes again. The inner
          // mapped bwrap owns the authoritative RO/RW mounts for this command.
          // Only this private tool tree is writable in its trusted outer stage.
          allowWrite: [this.canonicalRoot],
        },
      };
      return await this.runWrappedWithConfig(
        buildMappedToolBubblewrapCommand(
          command,
          validated,
          this.canonicalRoot,
          this.managedRuntimeBinds,
        ),
        config,
        this.canonicalRoot,
        timeoutMs,
      );
    }
    const allowWrite = [
      canonicalPathIfPresent(this.layout.tmp),
      ...policy.writablePaths.map((root) => this.toHostPath(root.path)),
    ];
    const config: SandboxManagerInitConfig = {
      ...this.config,
      filesystem: {
        ...this.config.filesystem,
        allowRead: [...(this.config.filesystem.allowRead ?? [])],
        allowWrite,
      },
    };
    return await this.runWrappedWithConfig(
      command,
      config,
      this.toHostPath(OUTPUT_WRITABLE_PATH),
      timeoutMs,
    );
  }

  private policyCommandCwd(path: string): string {
    return this.managedToolFilesystem ? canonicalAbsolutePath(path) : this.toHostPath(path);
  }

  private validateManagedRoots(policy: ReturnType<typeof validateSandboxWritePolicy>): void {
    assertMappedToolPolicyRoots(policy);
    if (
      realpathSync(this.canonicalRoot) !== this.canonicalRoot ||
      filesystemIdentity(this.canonicalRoot) !== this.rootIdentity
    ) {
      throw new Error('mapped tool filesystem root was replaced or aliased');
    }
    const roots = [...policy.readonlyPaths, ...policy.writablePaths.map((entry) => entry.path)];
    for (const virtualRoot of [
      ...roots,
      ...this.managedRuntimeBinds.map((bind) => bind.target),
      '/tmp',
      '/dev',
      '/proc',
    ]) {
      const source = this.toHostPath(virtualRoot);
      assertNoSymlinkComponents(this.canonicalRoot, source, false);
      const info = lstatSync(source);
      if (info.isFile() && info.nlink !== 1) {
        throw new Error(`mapped tool filesystem root is hard-linked: ${virtualRoot}`);
      }
      if (!info.isFile() && !info.isDirectory()) {
        throw new Error(`mapped tool filesystem root is not a file or directory: ${virtualRoot}`);
      }
    }
    // Trusted provisioning cannot smuggle bind mounts under resource roots.
    // Model commands cannot create later mounts: each invocation drops all caps.
    for (const line of readFileSync('/proc/self/mountinfo', 'utf8').split('\n')) {
      const encoded = line.split(' ')[4];
      if (!encoded) continue;
      const mountpoint = encoded.replace(/\\([0-7]{3})/g, (_match, digits: string) =>
        String.fromCharCode(parseInt(digits, 8)),
      );
      if (mountpoint === this.canonicalRoot || mountpoint.startsWith(`${this.canonicalRoot}/`)) {
        throw new Error(`mapped tool filesystem contains a pre-existing mount: ${mountpoint}`);
      }
    }
  }

  private async runWrappedWithConfig(
    command: string,
    config: SandboxManagerInitConfig,
    cwd: string,
    timeoutMs?: number,
  ): Promise<ToolResult> {
    const result = await this.captureWrappedCommand(command, config, cwd, timeoutMs);
    return {
      stdout: result.stdout.toString('utf8'),
      stderr: result.stderr.toString('utf8'),
      exit_code: result.exitCode,
    };
  }

  private async runFileOperation(
    operation: LocalFileOperation,
    fullPath: string,
    input?: Buffer,
  ): Promise<Buffer> {
    if (this.destroyed) throw new Error(`sandbox ${this.id} destroyed`);

    let allowWrite = [this.canonicalRoot];
    let fileReadRoots = [this.canonicalRoot];
    if (this.managedToolFilesystem) {
      const policy = this.managedWritePolicy;
      if (!policy) throw new Error('managed file operations require a prepared write policy');
      this.validateManagedRoots(policy);
      // Do not add the common tool-root ancestor: SRT emits its read-only
      // mount after write binds and would mask every writable resource below.
      fileReadRoots = [
        ...policy.readonlyPaths,
        ...policy.writablePaths.map((root) => root.path),
      ].map((path) => this.toHostPath(path));
      // The helper's component checks cannot close a concurrent rename/symlink
      // race. Its kernel sandbox must enforce the same immutable read-only
      // roots as Bash, including while stdin is being consumed. Read-only
      // operations receive no writable resource mounts at all.
      allowWrite =
        operation === 'read' || operation === 'list'
          ? []
          : policy.writablePaths.map((root) => this.toHostPath(root.path));
    }

    const canonicalPath = resolvePath(this.canonicalRoot, relative(this.layout.root, fullPath));
    let resolvedNode = process.execPath;
    try {
      resolvedNode = realpathSync(process.execPath);
    } catch {
      // The executable was already loaded; keep the original path so the
      // sandboxed launch reports the useful failure if it becomes unavailable.
    }
    const runtimeRoots = [
      dirname(dirname(process.execPath)),
      dirname(dirname(resolvedNode)),
      '/bin',
      '/usr',
      '/lib',
      '/lib64',
      '/System',
      '/Library',
      '/etc/ld.so.cache',
      SRT_SECCOMP_BINARY_PATH,
    ];
    const fileConfig: SandboxManagerInitConfig = {
      ...this.config,
      network: {
        ...this.config.network,
        unrestricted: false,
        allowedDomains: [],
        deniedDomains: [],
      },
      filesystem: {
        denyRead: ['/'],
        allowRead: [...new Set([...fileReadRoots, ...runtimeRoots.map(canonicalPathIfPresent)])],
        allowWrite,
        denyWrite: [...SRT_SHARED_WRITE_PATHS],
      },
    };
    const command = `'${escapeSingleQuotes(process.execPath)}' -e '${escapeSingleQuotes(LOCAL_FILE_HELPER_SOURCE)}'`;
    const result = await this.captureWrappedCommand(
      command,
      fileConfig,
      this.layout.root,
      undefined,
      input ?? Buffer.alloc(0),
      {
        ORCA_LOCAL_FILE_OPERATION: operation,
        ORCA_LOCAL_FILE_ROOT: this.canonicalRoot,
        ORCA_LOCAL_FILE_PATH: canonicalPath,
      },
      '/bin/bash',
    );
    if (result.exitCode !== 0) {
      const detail = result.stderr.toString('utf8').trim() || `exit code ${result.exitCode}`;
      throw new Error(`local sandbox ${operation} failed: ${detail}`);
    }
    return result.stdout;
  }

  private async captureWrappedCommand(
    command: string,
    config: SandboxManagerInitConfig,
    cwd: string,
    timeoutMs?: number,
    stdin?: Buffer,
    envOverrides: NodeJS.ProcessEnv = {},
    sandboxBinShell: string | undefined = this.binShell ?? '/bin/bash',
  ): Promise<CapturedCommandResult> {
    const sessionTmp = canonicalPathIfPresent(this.layout.tmp);
    const sessionEnvPrefix = [
      `export HOME='${escapeSingleQuotes(this.canonicalRoot)}'`,
      `TMPDIR='${escapeSingleQuotes(sessionTmp)}'`,
      `TMP='${escapeSingleQuotes(sessionTmp)}'`,
      `TEMP='${escapeSingleQuotes(sessionTmp)}'`,
    ].join(' ');
    // SRT injects its own shared TMPDIR while building the wrapper. Reset it
    // inside the sandbox shell before any caller-controlled command runs.
    const sandboxCommand = `${sessionEnvPrefix}; ${command}`;
    const wrapped = await this.manager.wrapWithSandbox(sandboxCommand, sandboxBinShell, config);
    try {
      return await new Promise<CapturedCommandResult>((resolve) => {
        const proc = spawn(wrapped, {
          shell: true,
          // Detached => group leader, so the timeout's group SIGKILL reaches the
          // real command inside the `sh -> sandbox-exec -> bash` wrapper chain
          // instead of reaping only the outer shell (macOS would otherwise leave
          // the command running with work-dir write access after we report 124).
          detached: true,
          cwd,
          env: {
            ...buildSandboxSubprocessEnv(cwd, canonicalPathIfPresent(this.layout.tmp)),
            ...envOverrides,
          },
        });
        const out: Buffer[] = [];
        const err: Buffer[] = [];
        let timer: NodeJS.Timeout | undefined;
        let settled = false;
        const finish = (result: CapturedCommandResult): void => {
          if (settled) return;
          settled = true;
          if (timer) clearTimeout(timer);
          resolve(result);
        };
        if (timeoutMs && timeoutMs > 0) {
          timer = setTimeout(() => killSandboxProcessTree(proc, 'SIGKILL'), timeoutMs);
        }
        proc.stdout?.on('data', (b: Buffer) => out.push(b));
        proc.stderr?.on('data', (b: Buffer) => err.push(b));
        proc.on('close', (code, signal) => {
          let stderrText = Buffer.concat(err).toString('utf8');
          let exitCode: number;
          if (signal !== null) {
            // SIGKILL/SIGTERM came from our timeout (or an external kill).
            // Without this branch, `code` would be `null` and we'd resolve
            // `exit_code: 0`, hiding the failure from the LLM. `124` matches
            // GNU `timeout`'s exit convention for "timed out".
            exitCode = 124;
            const reason =
              timeoutMs && timeoutMs > 0
                ? `[orca: process killed by ${signal} after ${timeoutMs}ms timeout]`
                : `[orca: process killed by ${signal}]`;
            stderrText = `${stderrText}\n${reason}\n`;
          } else {
            exitCode = code ?? 0;
          }
          finish({ stdout: Buffer.concat(out), stderr: Buffer.from(stderrText), exitCode });
        });
        proc.on('error', (e) => {
          finish({
            stdout: Buffer.concat(out),
            stderr: Buffer.from(`${Buffer.concat(err).toString('utf8')}${e.message}`),
            exitCode: 1,
          });
        });
        if (stdin !== undefined) {
          proc.stdin?.on('error', () => {
            // The exit/error handler reports the authoritative subprocess error.
          });
          proc.stdin?.end(stdin);
        }
      });
    } finally {
      await this.manager.releaseSandboxCommand?.(wrapped);
    }
  }

  /**
   * Translate a sandbox-visible path (e.g. `/data/`) to the host-side path
   * the spawned bash actually sees. The host process runs with cwd at the
   * work-dir root, so absolute sandbox paths re-anchor under the work-dir,
   * matching the resolution done by {@link LocalSandboxFiles.resolve}.
   * Delegates to {@link resolveUnderRoot} to reject traversal outside the
   * work-dir.
   */
  private toHostPath(path: string): string {
    return resolveUnderRoot(this.canonicalRoot, path);
  }

  /** Visible to tests for snapshot assertions. */
  getConfig(): SandboxManagerInitConfig {
    return this.config;
  }
}

function escapeSingleQuotes(s: string): string {
  return s.replace(/'/g, "'\\''");
}

function normalizeFileMode(mode: number): number {
  if (!Number.isInteger(mode) || mode < 0 || mode > 0o777) {
    throw new Error(`invalid file mode: ${mode}`);
  }
  return mode;
}

/** Test helper exported separately so tests can downcast safely. */
export function asLocalSandboxHandle(h: SandboxHandle): LocalSandboxHandle {
  if (!(h instanceof LocalSandboxHandle)) {
    throw new Error('not a LocalSandboxHandle');
  }
  return h;
}
