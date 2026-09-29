// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { existsSync, realpathSync } from 'node:fs';
import { basename, dirname, posix as path, relative, resolve, sep } from 'node:path';
import type {
  SandboxFiles,
  SandboxHandle,
  SandboxReadConstraint,
  SandboxWritePolicy,
  ToolCall,
  ToolResult,
  ValidatedSandboxWritePolicy,
  WritablePath,
  WritablePathKind,
  WritePolicyCapableHandle,
} from './sandbox-runtime.js';
import type { ReadPage, ReadPageInput } from './read-page.js';

/**
 * Reserved sandbox path for exact Registry-pinned Skill bundles.
 * `services/harness-server/src/sandbox/skills/materialize.ts` defines its own
 * equal `SKILLS_ROOT`, and the two definitions must be changed in lockstep
 * (they guard the same alias-probe boundary); the registry's
 * `RESERVED_SKILLS_ROOT` holds the same path. Defined here (not imported from
 * harness-server) because the bubblewrap / alias-probe commands below run
 * inside the runtime adapters that ship in this package and
 * `@orca/cloud-sandbox`, neither of which may depend on a service.
 */
export const SKILLS_ROOT = '/workspace/skills';
export const GIT_PROXY_AUTH_ROOT = '/.orca/git';
export const OUTPUT_WRITABLE_PATH = '/mnt/session/outputs';
export const SANDBOX_WRITE_POLICY_ENV = 'ORCA_SANDBOX_WRITE_POLICY';
export const SANDBOX_FILESYSTEM_ROOT_PREFLIGHT_TIMEOUT_MS = 10_000;
const AGENT_FILE_API_DENIED_ROOTS = ['/dev', '/proc', '/sys'] as const;

/**
 * One resource mount contributing to the write policy.
 *
 * A union so the illegal state is unrepresentable: `file` mounts are read-only
 * by construction (there is no writable-file enforcement path), while store and
 * repository mounts may request either access. `buildSandboxWritePolicy` also
 * rejects an untyped rw file mount at runtime rather than silently demoting the
 * writes it asked for.
 */
export type WritePolicyMount =
  | { path: string; kind: 'file'; access: 'read_only' }
  | {
      path: string;
      kind: 'memory_store' | 'github_repository';
      access: 'read_only' | 'read_write';
    };

/**
 * A path failed symlink-safe canonicalization because it escapes its sandbox
 * root. A dedicated class so the policy wrapper can classify a canonicalizer
 * throw correctly: THIS is a genuine containment violation (deny + audit),
 * while any other canonicalizer failure is infrastructure (a dead sandbox, a
 * transport timeout) and must propagate as such rather than masquerade as a
 * policy denial.
 */
export class SandboxPathEscapeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SandboxPathEscapeError';
  }
}

export interface WritePolicyDenial {
  path: string;
  operation: 'write' | 'delete';
}

export interface BuildSandboxWritePolicyOptions {
  networkAllowedDomains?: readonly string[];
  networkUnrestricted?: boolean;
  /** Expose the reserved Skill tree only for executions with a non-empty catalog. */
  includeSkillsRoot?: boolean;
  /** Read-only, scoped Git proxy capabilities refreshed by the trusted runner. */
  includeGitProxyRoot?: boolean;
}

export function normalizeWritePolicyAccess(access: string | undefined): WritePolicyMount['access'] {
  return access === 'read_write' ? 'read_write' : 'read_only';
}

export function buildSandboxWritePolicy(
  mounts: readonly WritePolicyMount[],
  options: BuildSandboxWritePolicyOptions = {},
): ValidatedSandboxWritePolicy {
  const writablePaths: WritablePath[] = [{ path: OUTPUT_WRITABLE_PATH, kind: 'session_output' }];
  const readonlyPaths: string[] = options.includeSkillsRoot === true ? [SKILLS_ROOT] : [];
  if (options.includeGitProxyRoot === true) readonlyPaths.push(GIT_PROXY_AUTH_ROOT);

  for (const mount of mounts) {
    const canonical = canonicalAbsolutePath(mount.path);
    if (mount.access === 'read_write') {
      // Positive ALLOWLIST, not a denylist: only the two kinds with a writable
      // enforcement path may become writable roots. The mount union already
      // forbids the rest in TS; this runtime guard covers untyped callers (a
      // wire-decoded mount list), where an unknown or typo'd kind must fail
      // loudly — falling through to writable would hand the agent a writable
      // root (and a bubblewrap `--bind`) the policy checks believe cannot
      // exist, and demoting to read-only would silently deny the requested
      // writes.
      if (mount.kind !== 'memory_store' && mount.kind !== 'github_repository') {
        throw new Error(
          `sandbox write policy cannot grant read_write to a ${String(mount.kind)} mount: ${mount.path}`,
        );
      }
      writablePaths.push({ path: canonical, kind: mount.kind });
    } else {
      readonlyPaths.push(canonical);
    }
  }

  const networkAllowedDomains = dedupeNetworkDomains(options.networkAllowedDomains ?? []);
  return validateSandboxWritePolicy({
    ...(options.networkUnrestricted === true ? { networkUnrestricted: true } : {}),
    writablePaths: dedupeWritablePaths(writablePaths),
    readonlyPaths: [...new Set(readonlyPaths)].sort(),
    ...(networkAllowedDomains.length > 0 ? { networkAllowedDomains } : {}),
  });
}

/**
 * Check a structurally-shaped policy against the builder invariants and brand
 * it {@link ValidatedSandboxWritePolicy}.
 *
 * This is the ONLY place (besides `buildSandboxWritePolicy`, which ends here)
 * that mints the brand. Use it to re-validate a policy that crossed a trust
 * boundary as plain data — parsed back out of {@link SANDBOX_WRITE_POLICY_ENV},
 * or hand-assembled in a test — before handing it to an enforcement surface.
 * Checks: every root is canonical-absolute, writable roots are kind-unique and
 * non-overlapping (with each other and with read-only roots). Freezes and
 * returns the branded policy.
 */
export function validateSandboxWritePolicy(
  policy: SandboxWritePolicy,
): ValidatedSandboxWritePolicy {
  if (policy.networkUnrestricted !== undefined && typeof policy.networkUnrestricted !== 'boolean')
    throw new Error('invalid unrestricted network policy');
  const normalizedWritable = dedupeWritablePaths(policy.writablePaths);
  const normalizedReadonly = [...new Set(policy.readonlyPaths)].sort();
  const knownWritableKinds: readonly WritablePathKind[] = [
    'session_output',
    'memory_store',
    'github_repository',
  ];
  for (const writable of policy.writablePaths) {
    if (canonicalAbsolutePath(writable.path) !== writable.path) {
      throw new Error(`sandbox write policy root is not canonical: ${writable.path}`);
    }
    // Membership, not just consistency: a policy arriving as plain data (the
    // env round trip) must not smuggle an unknown writable kind past the gate.
    if (!(knownWritableKinds as readonly string[]).includes(writable.kind)) {
      throw new Error(
        `sandbox write policy has unknown writable kind: ${String(writable.kind)} (${writable.path})`,
      );
    }
  }
  for (const readonly of policy.readonlyPaths) {
    if (canonicalAbsolutePath(readonly) !== readonly) {
      throw new Error(`sandbox write policy root is not canonical: ${readonly}`);
    }
  }
  for (const [index, writable] of normalizedWritable.entries()) {
    for (const other of normalizedWritable.slice(index + 1)) {
      if (pathsOverlap(writable.path, other.path)) {
        throw new Error(
          `sandbox write policy has overlapping writable roots: ${writable.path} and ${other.path}`,
        );
      }
    }
    for (const readonly of normalizedReadonly) {
      if (pathsOverlap(writable.path, readonly)) {
        throw new Error(
          `sandbox write policy has overlapping writable/read-only roots: ${writable.path} and ${readonly}`,
        );
      }
    }
  }
  const networkAllowedDomains = dedupeNetworkDomains(policy.networkAllowedDomains ?? []);
  return Object.freeze({
    ...(policy.networkUnrestricted === true ? { networkUnrestricted: true } : {}),
    writablePaths: Object.freeze(normalizedWritable),
    readonlyPaths: Object.freeze(normalizedReadonly),
    ...(networkAllowedDomains.length > 0
      ? { networkAllowedDomains: Object.freeze(networkAllowedDomains) }
      : {}),
  }) as ValidatedSandboxWritePolicy;
}

export function encodeSandboxWritePolicy(policy: SandboxWritePolicy): string {
  return JSON.stringify(policy);
}

/** Convert trusted HTTP(S) resource endpoints into exact SDK sandbox host allow-list entries. */
export function networkAllowedDomainsFromUrls(urls: readonly string[]): string[] {
  return dedupeNetworkDomains(
    urls.map((value) => {
      let parsed: URL;
      try {
        parsed = new URL(value);
      } catch {
        throw new Error(`sandbox network URL is invalid: ${value}`);
      }
      if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
        throw new Error(`sandbox network URL must use http or https: ${value}`);
      }
      return parsed.hostname;
    }),
  );
}

export function canonicalAbsolutePath(value: string): string {
  if (!value.startsWith('/')) throw new Error(`sandbox path must be absolute: ${value}`);
  if (value.includes('\0')) throw new Error('sandbox path contains NUL');
  const normalized = path.normalize(value);
  return normalized.length > 1 ? normalized.replace(/\/$/, '') : normalized;
}

export function writablePathFor(
  policy: ValidatedSandboxWritePolicy,
  candidate: string,
): WritablePath | undefined {
  const canonical = canonicalAbsolutePath(candidate);
  return policy.writablePaths.find((root) => isPathWithin(canonical, root.path));
}

export function readablePathFor(
  policy: ValidatedSandboxWritePolicy,
  candidate: string,
): string | undefined {
  const canonical = canonicalAbsolutePath(candidate);
  const writable = policy.writablePaths.find((root) => isPathWithin(canonical, root.path));
  if (writable) return writable.path;
  return policy.readonlyPaths.find((root) => isPathWithin(canonical, root));
}

export async function createPolicyEnforcedSandbox(
  sandbox: WritePolicyCapableHandle,
  policy: ValidatedSandboxWritePolicy,
  onDenied?: (denial: WritePolicyDenial) => void,
): Promise<SandboxHandle> {
  await sandbox.prepareWritePolicy(policy);

  const files = new PolicyEnforcedFiles(sandbox, policy, onDenied);
  // `runPrivileged` is deliberately OMITTED (not an always-throwing stub):
  // absence is the feature-detection signal that privileged operations are
  // unavailable to the agent behind this wrapper.
  return {
    id: sandbox.id,
    files,
    run: async (call: ToolCall): Promise<ToolResult> =>
      await sandbox.runWithWritePolicy(call, policy),
    pause: async () => await sandbox.pause(),
    resume: async () => await sandbox.resume(),
    destroy: async () => await sandbox.destroy(),
    ...(sandbox.endpoint
      ? { endpoint: async (port: number) => await sandbox.endpoint!(port) }
      : {}),
  };
}

export function buildBubblewrapCommand(
  command: string,
  policy: ValidatedSandboxWritePolicy,
): string {
  const args = [
    'bwrap',
    '--new-session',
    '--die-with-parent',
    '--unshare-user',
    '--unshare-pid',
    '--uid',
    '1000',
    '--gid',
    '1000',
    '--cap-drop',
    'ALL',
    '--ro-bind',
    '/',
    '/',
    '--perms',
    '1777',
    '--tmpfs',
    '/tmp',
    '--perms',
    '0700',
    '--dir',
    '/tmp/home',
    '--dev',
    '/dev',
    '--proc',
    '/proc',
  ];
  for (const root of policy.writablePaths) {
    args.push('--bind', root.path, root.path);
  }
  args.push(
    '--chdir',
    OUTPUT_WRITABLE_PATH,
    '--setenv',
    'HOME',
    '/tmp/home',
    '--setenv',
    'TMPDIR',
    '/tmp',
    '--',
    'bash',
    '-lc',
    command,
  );
  return args.map(shellQuote).join(' ');
}

/** Trusted, pre-created runtime targets for a mapped Linux tool filesystem. */
export interface MappedToolRuntimeBind {
  source: string;
  target: string;
}

export function assertMappedToolPolicyRoots(policy: ValidatedSandboxWritePolicy): void {
  const reserved = [
    '/bin',
    '/sbin',
    '/usr',
    '/lib',
    '/lib64',
    '/etc',
    '/dev',
    '/proc',
    '/sys',
    '/tmp',
  ];
  for (const root of [
    ...policy.readonlyPaths,
    ...policy.writablePaths.map((entry) => entry.path),
  ]) {
    if (root === '/' || reserved.some((runtimeRoot) => pathsOverlap(root, runtimeRoot))) {
      throw new Error(`mapped tool policy overlaps a reserved runtime path: ${root}`);
    }
  }
  for (const root of policy.writablePaths) {
    if (pathsOverlap(root.path, SKILLS_ROOT)) {
      throw new Error(`mapped tool policy cannot make Skills writable: ${root.path}`);
    }
  }
}

/**
 * The outer SRT sandbox owns network filtering and starts its loopback proxies
 * before this command. Keep that network namespace and its proxy environment;
 * only replace the filesystem and PID/user namespaces here. In particular, do
 * not bind the host root, host /proc, or SRT's Unix proxy sockets into this view.
 * The caller validates source/target aliases and pre-creates all mount targets.
 */
export function buildMappedToolBubblewrapCommand(
  command: string,
  policy: ValidatedSandboxWritePolicy,
  root: string,
  runtimeBinds: readonly MappedToolRuntimeBind[],
): string {
  assertMappedToolPolicyRoots(policy);
  if (root === '/' || canonicalAbsolutePath(root) !== root) {
    throw new Error('mapped tool filesystem needs a canonical private root');
  }
  const args = [
    'bwrap',
    '--new-session',
    '--die-with-parent',
    '--unshare-user',
    '--unshare-pid',
    '--uid',
    '1000',
    '--gid',
    '1000',
    '--cap-drop',
    'ALL',
    '--ro-bind',
    root,
    '/',
  ];
  for (const bind of runtimeBinds) {
    if (
      bind.source === '/' ||
      bind.target === '/' ||
      canonicalAbsolutePath(bind.source) !== bind.source ||
      canonicalAbsolutePath(bind.target) !== bind.target
    ) {
      throw new Error('mapped tool runtime binding must use canonical non-root paths');
    }
    args.push('--ro-bind', bind.source, bind.target);
  }
  for (const writable of policy.writablePaths) {
    args.push('--bind', `${root}${writable.path}`, writable.path);
  }
  args.push(
    '--perms',
    '1777',
    '--tmpfs',
    '/tmp',
    '--perms',
    '0700',
    '--dir',
    '/tmp/home',
    '--dev',
    '/dev',
    '--proc',
    '/proc',
    '--chdir',
    OUTPUT_WRITABLE_PATH,
    '--setenv',
    'HOME',
    '/tmp/home',
    '--setenv',
    'TMPDIR',
    '/tmp',
    '--setenv',
    'TMP',
    '/tmp',
    '--setenv',
    'TEMP',
    '/tmp',
    '--setenv',
    'PATH',
    '/usr/local/bin:/usr/bin:/bin',
    '--',
    '/bin/bash',
    '--noprofile',
    '--norc',
    '-c',
    command,
  );
  return args.map(shellQuote).join(' ');
}

/**
 * Inspect an image-backed sandbox before any trusted resource write/mount.
 *
 * A final write-policy probe cannot distinguish a mount created by Orca from
 * one baked into the image at the exact same path. This preflight runs first:
 * every planned root must resolve to its lexical path, and neither that root
 * nor anything below it may already be a mount point. Exact mount points
 * created later by trusted resource strategies are then allowed by the final
 * probe, while nested mounts remain forbidden.
 */
export function buildSandboxFilesystemRootPreflightCommand(roots: readonly string[]): string {
  const canonicalRoots = [...new Set(roots.map(canonicalAbsolutePath))].sort();
  if (canonicalRoots.length === 0) return 'true';

  const script = `set -eu
for policy_root in "$@"; do
  canonical="$(realpath -m -- "$policy_root")"
  if [ "$canonical" != "$policy_root" ]; then
    echo "sandbox filesystem root is aliased: $policy_root -> $canonical" >&2
    exit 73
  fi
done
while IFS=' ' read -r _mount_id _parent_id _device _mount_root encoded_mountpoint _rest; do
  mountpoint="$(printf '%b' "$encoded_mountpoint")"
  for policy_root in "$@"; do
    case "$mountpoint" in
      "$policy_root"|"$policy_root"/*)
        echo "sandbox filesystem root contains a pre-existing mount: $policy_root -> $mountpoint" >&2
        exit 73
        ;;
    esac
  done
done < /proc/self/mountinfo
exit 0`;
  return ['bash', '-c', shellQuote(script), '--', ...canonicalRoots.map(shellQuote)].join(' ');
}

/**
 * Linux runtime probe for mount and hard-link aliases that realpath cannot
 * distinguish. It compares each writable root's device/inode with the
 * reserved Skill root's full ancestor chain, and rejects nested mounts below
 * every readable policy root. A nested bind can otherwise smuggle the image
 * root (including this Skill tree) back through an allowed resource path.
 */
export function buildSkillsAliasProbeCommand(policy: ValidatedSandboxWritePolicy): string {
  const script = `set -eu
reserved="$1"
shift
writable_count="$1"
shift
identity() {
  stat -Lc '%d:%i' -- "$1"
}
contains_identity() {
  needle="$1"
  cursor="$2"
  while :; do
    [ "$(identity "$cursor")" != "$needle" ] || return 0
    [ "$cursor" != "/" ] || break
    cursor="\${cursor%/*}"
    [ -n "$cursor" ] || cursor="/"
  done
  return 1
}
if [ -e "$reserved" ]; then
  reserved_identity="$(identity "$reserved")"
else
  reserved_identity=""
fi
writable_index=0
for policy_root in "$@"; do
  [ -e "$policy_root" ] || {
    echo "sandbox policy root does not exist: $policy_root" >&2
    exit 73
  }
  if [ "$writable_index" -lt "$writable_count" ] && [ -n "$reserved_identity" ]; then
    writable_identity="$(identity "$policy_root")"
    if contains_identity "$writable_identity" "$reserved" ||
       contains_identity "$reserved_identity" "$policy_root"; then
      echo "sandbox writable root aliases reserved Skill root: $policy_root and $reserved" >&2
      exit 73
    fi
  fi
  writable_index=$((writable_index + 1))
done
while IFS=' ' read -r _mount_id _parent_id _device _mount_root encoded_mountpoint _rest; do
  mountpoint="$(printf '%b' "$encoded_mountpoint")"
  for policy_root in "$@"; do
    case "$mountpoint" in
      "$policy_root"/*)
        echo "sandbox policy root contains nested mount: $policy_root -> $mountpoint" >&2
        exit 73
        ;;
    esac
  done
done < /proc/self/mountinfo
exit 0`;
  const writableRoots = policy.writablePaths.map((root) => root.path);
  const readonlyRoots = policy.readonlyPaths;
  return [
    'bash',
    '-c',
    shellQuote(script),
    '--',
    shellQuote(SKILLS_ROOT),
    shellQuote(String(writableRoots.length)),
    ...writableRoots.map(shellQuote),
    ...readonlyRoots.map(shellQuote),
  ].join(' ');
}

/** Resolve a virtual path through existing symlinks without escaping a host-backed sandbox root. */
export function canonicalizeVirtualPathUnderRoot(root: string, virtualPath: string): string {
  const canonical = canonicalAbsolutePath(virtualPath);
  const realRoot = realpathSync(root);
  let cursor = resolve(realRoot, canonical.slice(1));
  const missing: string[] = [];
  while (!existsSync(cursor) && cursor !== realRoot) {
    missing.unshift(basename(cursor));
    cursor = dirname(cursor);
  }
  const resolved = resolve(realpathSync(cursor), ...missing);
  if (resolved !== realRoot && !resolved.startsWith(`${realRoot}${sep}`)) {
    throw new SandboxPathEscapeError(`sandbox path escapes through a symlink: ${virtualPath}`);
  }
  const inside = relative(realRoot, resolved).split(sep).join('/');
  return inside.length > 0 ? `/${inside}` : '/';
}

class PolicyEnforcedFiles implements SandboxFiles {
  constructor(
    private readonly sandbox: WritePolicyCapableHandle,
    private readonly policy: ValidatedSandboxWritePolicy,
    private readonly onDenied?: (denial: WritePolicyDenial) => void,
  ) {}

  async write(pathValue: string, content: Buffer | NodeJS.ReadableStream): Promise<void> {
    await this.assertWritable(pathValue, 'write');
    await this.sandbox.files.write(pathValue, content);
  }

  async read(pathValue: string): Promise<Buffer> {
    await this.assertReadable(pathValue);
    return await this.sandbox.files.read(pathValue);
  }

  async readUtf8Page(
    pathValue: string,
    input: ReadPageInput,
    _constraint?: SandboxReadConstraint,
  ): Promise<ReadPage> {
    const lexical = canonicalAbsolutePath(pathValue);
    if (!readablePathFor(this.policy, lexical)) {
      throw new Error(`read denied for ${pathValue}; path is outside session resource roots`);
    }
    return await this.sandbox.files.readUtf8Page(lexical, input, {
      readableRoots: [
        ...this.policy.writablePaths.map((root) => root.path),
        ...this.policy.readonlyPaths,
      ],
    });
  }

  async list(pathValue: string): Promise<string[]> {
    await this.assertReadable(pathValue);
    return await this.sandbox.files.list(pathValue);
  }

  async chmod(pathValue: string, mode: number): Promise<void> {
    await this.assertWritable(pathValue, 'write');
    await this.sandbox.files.chmod(pathValue, mode);
  }

  async delete(pathValue: string): Promise<void> {
    await this.assertWritable(pathValue, 'delete');
    await this.sandbox.files.delete(pathValue);
  }

  /**
   * Run the sandbox's canonicalizer, keeping infra failures distinguishable.
   *
   * On the cloud runtimes this is a remote realpath over the sandbox command
   * API — a fetch timeout, an execd 500, or a destroyed sandbox surfaces here.
   * Reclassifying such a failure as a policy denial would discard the real
   * error, send the agent chasing "path outside resource roots" for a sandbox
   * outage, and (on the write path) pollute the onDenied audit with violations
   * that never happened. So infra failures propagate wrapped with `cause`,
   * fail-closed but truthful; only genuine containment checks produce denials.
   */
  private async canonicalizeOrInfraError(
    requestedPath: string,
    lexical: string,
    deny: () => never,
  ): Promise<string> {
    try {
      return canonicalAbsolutePath(await this.sandbox.canonicalizePathForPolicy(lexical));
    } catch (exc) {
      if (exc instanceof SandboxPathEscapeError) {
        // A containment violation surfaced by the canonicalizer itself (the
        // local runtimes throw instead of resolving an escaping path): a real
        // denial, audited like any other.
        deny();
      }
      throw new Error(`sandbox path canonicalization failed for ${requestedPath}`, { cause: exc });
    }
  }

  private async assertReadable(requestedPath: string): Promise<void> {
    const deny: () => never = () => {
      throw new Error(`read denied for ${requestedPath}; path is outside session resource roots`);
    };
    let lexical: string;
    try {
      lexical = canonicalAbsolutePath(requestedPath);
    } catch {
      deny();
    }
    if (!readablePathFor(this.policy, lexical)) deny();
    if (AGENT_FILE_API_DENIED_ROOTS.some((root) => isPathWithin(lexical, root))) deny();
    const canonical = await this.canonicalizeOrInfraError(requestedPath, lexical, deny);
    if (AGENT_FILE_API_DENIED_ROOTS.some((root) => isPathWithin(canonical, root))) deny();
    if (!readablePathFor(this.policy, canonical)) deny();
  }

  private async assertWritable(
    requestedPath: string,
    operation: WritePolicyDenial['operation'],
  ): Promise<void> {
    const deny: () => never = () => {
      this.onDenied?.({ path: requestedPath, operation });
      throw new Error(
        `${operation} denied for ${requestedPath}; user-downloadable files must be written under ${OUTPUT_WRITABLE_PATH}/`,
      );
    };
    let lexical: string;
    try {
      lexical = canonicalAbsolutePath(requestedPath);
    } catch {
      deny();
    }
    if (!writablePathFor(this.policy, lexical)) deny();
    const canonical = await this.canonicalizeOrInfraError(requestedPath, lexical, deny);
    if (!writablePathFor(this.policy, canonical)) deny();
  }
}

function dedupeWritablePaths(paths: readonly WritablePath[]): WritablePath[] {
  const byPath = new Map<string, WritablePathKind>();
  for (const item of paths) {
    const canonical = canonicalAbsolutePath(item.path);
    const existing = byPath.get(canonical);
    if (existing && existing !== item.kind) {
      throw new Error(`sandbox write policy assigns multiple kinds to ${canonical}`);
    }
    byPath.set(canonical, item.kind);
  }
  return [...byPath.entries()]
    .map(([pathValue, kind]) => ({ path: pathValue, kind }))
    .sort((a, b) => a.path.localeCompare(b.path));
}

function dedupeNetworkDomains(domains: readonly string[]): string[] {
  return [...new Set(domains.map(canonicalNetworkDomain))].sort();
}

function canonicalNetworkDomain(value: string): string {
  if (value.length === 0 || value.trim() !== value || /[\s/\\@?#\0]/.test(value)) {
    throw new Error(`sandbox network domain is invalid: ${value}`);
  }
  let parsed: URL;
  try {
    parsed = new URL(`https://${value}`);
  } catch {
    throw new Error(`sandbox network domain is invalid: ${value}`);
  }
  const canonical = parsed.hostname.toLowerCase();
  if (
    parsed.username ||
    parsed.password ||
    parsed.port ||
    parsed.pathname !== '/' ||
    canonical !== value.toLowerCase()
  ) {
    throw new Error(`sandbox network domain is invalid: ${value}`);
  }
  return canonical;
}

function isPathWithin(candidate: string, root: string): boolean {
  return candidate === root || candidate.startsWith(`${root}/`);
}

function pathsOverlap(a: string, b: string): boolean {
  return isPathWithin(a, b) || isPathWithin(b, a);
}

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'"'"'`)}'`;
}
