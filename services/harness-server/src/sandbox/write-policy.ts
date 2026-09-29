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
  WritablePath,
  WritablePathKind,
} from './sandbox-runtime.js';
import type { ReadPage, ReadPageInput } from './read-page.js';
import { SKILLS_ROOT } from './skills/materialize.js';

export const OUTPUT_WRITABLE_PATH = '/mnt/session/outputs';
export const SANDBOX_WRITE_POLICY_ENV = 'ORCA_SANDBOX_WRITE_POLICY';
export const SANDBOX_FILESYSTEM_ROOT_PREFLIGHT_TIMEOUT_MS = 10_000;
const AGENT_FILE_API_DENIED_ROOTS = ['/dev', '/proc', '/sys'] as const;

export interface WritePolicyMount {
  path: string;
  kind: 'file' | 'memory_store' | 'github_repository';
  access: 'read_only' | 'read_write';
}

export interface WritePolicyDenial {
  path: string;
  operation: 'write' | 'delete';
}

export interface BuildSandboxWritePolicyOptions {
  networkAllowedDomains?: readonly string[];
  /** Expose the reserved Skill tree only for executions with a non-empty catalog. */
  includeSkillsRoot?: boolean;
}

export function normalizeWritePolicyAccess(access: string | undefined): WritePolicyMount['access'] {
  return access === 'read_write' ? 'read_write' : 'read_only';
}

export function buildSandboxWritePolicy(
  mounts: readonly WritePolicyMount[],
  options: BuildSandboxWritePolicyOptions = {},
): SandboxWritePolicy {
  const writablePaths: WritablePath[] = [{ path: OUTPUT_WRITABLE_PATH, kind: 'session_output' }];
  const readonlyPaths: string[] = options.includeSkillsRoot === true ? [SKILLS_ROOT] : [];

  for (const mount of mounts) {
    const canonical = canonicalAbsolutePath(mount.path);
    if (
      mount.access === 'read_write' &&
      (mount.kind === 'memory_store' || mount.kind === 'github_repository')
    ) {
      writablePaths.push({ path: canonical, kind: mount.kind });
    } else {
      readonlyPaths.push(canonical);
    }
  }

  const normalizedWritable = dedupeWritablePaths(writablePaths);
  const normalizedReadonly = [...new Set(readonlyPaths)].sort();
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
  const networkAllowedDomains = dedupeNetworkDomains(options.networkAllowedDomains ?? []);
  return Object.freeze({
    writablePaths: Object.freeze(normalizedWritable),
    readonlyPaths: Object.freeze(normalizedReadonly),
    ...(networkAllowedDomains.length > 0
      ? { networkAllowedDomains: Object.freeze(networkAllowedDomains) }
      : {}),
  });
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
  policy: SandboxWritePolicy,
  candidate: string,
): WritablePath | undefined {
  const canonical = canonicalAbsolutePath(candidate);
  return policy.writablePaths.find((root) => isPathWithin(canonical, root.path));
}

export function readablePathFor(policy: SandboxWritePolicy, candidate: string): string | undefined {
  const canonical = canonicalAbsolutePath(candidate);
  const writable = policy.writablePaths.find((root) => isPathWithin(canonical, root.path));
  if (writable) return writable.path;
  return policy.readonlyPaths.find((root) => isPathWithin(canonical, root));
}

export async function createPolicyEnforcedSandbox(
  sandbox: SandboxHandle,
  policy: SandboxWritePolicy,
  onDenied?: (denial: WritePolicyDenial) => void,
): Promise<SandboxHandle> {
  if (
    !sandbox.prepareWritePolicy ||
    !sandbox.runWithWritePolicy ||
    !sandbox.canonicalizePathForPolicy
  ) {
    throw new Error('sandbox handle does not implement write-policy enforcement');
  }
  await sandbox.prepareWritePolicy(policy);

  const files = new PolicyEnforcedFiles(sandbox, policy, onDenied);
  return {
    id: sandbox.id,
    files,
    run: async (call: ToolCall): Promise<ToolResult> =>
      await sandbox.runWithWritePolicy!(call, policy),
    runPrivileged: async () => {
      throw new Error('privileged operations are unavailable to the agent');
    },
    pause: async () => await sandbox.pause(),
    resume: async () => await sandbox.resume(),
    destroy: async () => await sandbox.destroy(),
    ...(sandbox.endpoint
      ? { endpoint: async (port: number) => await sandbox.endpoint!(port) }
      : {}),
  };
}

export function buildBubblewrapCommand(command: string, policy: SandboxWritePolicy): string {
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
export function buildSkillsAliasProbeCommand(policy: SandboxWritePolicy): string {
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
    throw new Error(`sandbox path escapes through a symlink: ${virtualPath}`);
  }
  const inside = relative(realRoot, resolved).split(sep).join('/');
  return inside.length > 0 ? `/${inside}` : '/';
}

class PolicyEnforcedFiles implements SandboxFiles {
  constructor(
    private readonly sandbox: SandboxHandle,
    private readonly policy: SandboxWritePolicy,
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

  private async assertReadable(requestedPath: string): Promise<void> {
    try {
      const lexical = canonicalAbsolutePath(requestedPath);
      if (!readablePathFor(this.policy, lexical)) throw new Error('outside readable roots');
      if (AGENT_FILE_API_DENIED_ROOTS.some((root) => isPathWithin(lexical, root))) {
        throw new Error('control path');
      }
      const canonical = canonicalAbsolutePath(
        await this.sandbox.canonicalizePathForPolicy!(lexical),
      );
      if (AGENT_FILE_API_DENIED_ROOTS.some((root) => isPathWithin(canonical, root))) {
        throw new Error('control path');
      }
      if (!readablePathFor(this.policy, canonical)) {
        throw new Error('symlink escapes readable root');
      }
    } catch {
      throw new Error(`read denied for ${requestedPath}; path is outside session resource roots`);
    }
  }

  private async assertWritable(
    requestedPath: string,
    operation: WritePolicyDenial['operation'],
  ): Promise<void> {
    let canonical: string;
    try {
      canonical = canonicalAbsolutePath(requestedPath);
      if (!writablePathFor(this.policy, canonical)) throw new Error('outside writable roots');
      canonical = canonicalAbsolutePath(await this.sandbox.canonicalizePathForPolicy!(canonical));
      if (!writablePathFor(this.policy, canonical))
        throw new Error('symlink escapes writable root');
    } catch {
      this.onDenied?.({ path: requestedPath, operation });
      throw new Error(
        `${operation} denied for ${requestedPath}; user-downloadable files must be written under ${OUTPUT_WRITABLE_PATH}/`,
      );
    }
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
