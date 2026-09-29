// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { CanUseTool, Options } from '@anthropic-ai/claude-agent-sdk';
import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, realpathSync, statSync } from 'node:fs';
import { basename, dirname, posix as path, resolve as resolveHostPath, sep } from 'node:path';

export const SANDBOX_WRITE_POLICY_ENV = 'ORCA_SANDBOX_WRITE_POLICY';
export const OUTPUT_WRITABLE_PATH = '/mnt/session/outputs';
export const SKILLS_READONLY_PATH = '/workspace/skills';

type WritablePathKind = 'session_output' | 'memory_store' | 'github_repository';

export interface SandboxWritePolicy {
  writablePaths: ReadonlyArray<{ path: string; kind: WritablePathKind }>;
  readonlyPaths: readonly string[];
  networkAllowedDomains?: readonly string[];
}

/**
 * Decode the policy installed by harness-server. A present but malformed policy
 * is fatal: silently falling back to an unsandboxed provider would turn a
 * control-plane configuration bug into arbitrary filesystem write access.
 */
export function parseSandboxWritePolicy(
  env: Record<string, string | undefined>,
): SandboxWritePolicy | undefined {
  const raw = env[SANDBOX_WRITE_POLICY_ENV];
  if (raw === undefined || raw.trim() === '') return undefined;

  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch (error) {
    throw new Error(`invalid ${SANDBOX_WRITE_POLICY_ENV}: ${errorMessage(error)}`);
  }
  if (
    !isRecord(value) ||
    !Array.isArray(value.writablePaths) ||
    !Array.isArray(value.readonlyPaths)
  ) {
    throw new Error(
      `invalid ${SANDBOX_WRITE_POLICY_ENV}: expected writablePaths and readonlyPaths arrays`,
    );
  }

  const writablePaths = value.writablePaths.map((entry, index) => {
    if (!isRecord(entry) || typeof entry.path !== 'string' || !isWritablePathKind(entry.kind)) {
      throw new Error(`invalid ${SANDBOX_WRITE_POLICY_ENV}: writablePaths[${index}] is malformed`);
    }
    return { path: canonicalPolicyPath(entry.path), kind: entry.kind };
  });
  const readonlyPaths = value.readonlyPaths.map((entry, index) => {
    if (typeof entry !== 'string') {
      throw new Error(
        `invalid ${SANDBOX_WRITE_POLICY_ENV}: readonlyPaths[${index}] is not a string`,
      );
    }
    return canonicalPolicyPath(entry);
  });
  const networkAllowedDomains = parseNetworkAllowedDomains(value.networkAllowedDomains);

  if (!writablePaths.some((entry) => entry.path === OUTPUT_WRITABLE_PATH)) {
    throw new Error(`invalid ${SANDBOX_WRITE_POLICY_ENV}: ${OUTPUT_WRITABLE_PATH} is not writable`);
  }
  for (const [index, writable] of writablePaths.entries()) {
    for (const other of writablePaths.slice(index + 1)) {
      if (pathsOverlap(writable.path, other.path)) {
        throw new Error(
          `invalid ${SANDBOX_WRITE_POLICY_ENV}: writable roots overlap (${writable.path}, ${other.path})`,
        );
      }
    }
    for (const readonly of readonlyPaths) {
      if (pathsOverlap(writable.path, readonly)) {
        throw new Error(
          `invalid ${SANDBOX_WRITE_POLICY_ENV}: writable/read-only roots overlap (${writable.path}, ${readonly})`,
        );
      }
    }
  }

  return Object.freeze({
    writablePaths: Object.freeze(writablePaths),
    readonlyPaths: Object.freeze(readonlyPaths),
    ...(networkAllowedDomains.length > 0
      ? { networkAllowedDomains: Object.freeze(networkAllowedDomains) }
      : {}),
  });
}

/** Claude Agent SDK settings that make filesystem sandbox availability a hard gate. */
export function claudeSandboxSettings(policy: SandboxWritePolicy): NonNullable<Options['sandbox']> {
  return {
    enabled: true,
    failIfUnavailable: true,
    autoAllowBashIfSandboxed: true,
    allowUnsandboxedCommands: false,
    // The harness itself already runs inside an E2B/OpenSandbox container.
    enableWeakerNestedSandbox: true,
    filesystem: {
      allowWrite: policy.writablePaths.map((entry) => entry.path),
    },
    ...(policy.networkAllowedDomains?.length
      ? {
          network: {
            // `allowManagedDomainsOnly` is a policy-tier setting that the SDK
            // honors only via Options.managedSettings. This is a per-session
            // Options.sandbox value; allowedDomains itself remains allow-only.
            allowedDomains: [...policy.networkAllowedDomains],
          },
        }
      : {}),
  };
}

/**
 * Probe bubblewrap as the same OS user and with the same nested-userns shape
 * that the Claude SDK uses when enableWeakerNestedSandbox is set. Binding the
 * existing /proc avoids requiring CAP_SYS_ADMIN after the image entrypoint has
 * dropped privileges.
 */
export function probeSandboxWritePolicy(policy: SandboxWritePolicy): void {
  const result = spawnSync('bwrap', bubblewrapProbeArgs(policy), {
    encoding: 'utf8',
    timeout: 10_000,
  });
  if (result.error || result.status !== 0) {
    const detail = result.error?.message || result.stderr.trim() || `exit ${String(result.status)}`;
    throw new Error(`sandbox write-policy probe failed: ${detail}`);
  }
}

export function bubblewrapProbeArgs(policy: SandboxWritePolicy): string[] {
  const args = [
    '--die-with-parent',
    '--ro-bind',
    '/',
    '/',
    '--dev-bind',
    '/dev',
    '/dev',
    '--unshare-user',
    '--bind',
    '/proc',
    '/proc',
  ];
  for (const root of policy.writablePaths) {
    args.push('--bind', root.path, root.path);
  }
  args.push('--chdir', OUTPUT_WRITABLE_PATH, '--', 'true');
  return args;
}

/**
 * Give native Write/Edit calls a normal tool denial before touching the FS.
 * Bash remains protected by the SDK's bubblewrap boundary; blocked-path
 * callbacks are denied as well so the model cannot request an unsandboxed retry.
 */
export function writePolicyPermissionHandler(
  policy: SandboxWritePolicy,
  cwd = OUTPUT_WRITABLE_PATH,
): CanUseTool {
  return async (toolName, input, context) => {
    const requestedPath = nativeWritePath(toolName, input) ?? context.blockedPath;
    if (requestedPath !== undefined) {
      try {
        const absolute = path.isAbsolute(requestedPath)
          ? canonicalPolicyPath(requestedPath)
          : canonicalPolicyPath(path.resolve(cwd, requestedPath));
        const canonical = canonicalizeExistingPath(absolute);
        if (!isWritable(policy, canonical)) throw new Error('outside writable roots');
      } catch {
        return {
          behavior: 'deny',
          message: `write denied for ${requestedPath}; user-downloadable files must be written under ${OUTPUT_WRITABLE_PATH}/`,
        };
      }
    }
    return { behavior: 'allow', updatedInput: input };
  };
}

export function isWritable(policy: SandboxWritePolicy, candidate: string): boolean {
  const canonical = canonicalPolicyPath(candidate);
  return policy.writablePaths.some((root) => isPathWithin(canonical, root.path));
}

export function isReadable(policy: SandboxWritePolicy, candidate: string): boolean {
  const canonical = canonicalPolicyPath(candidate);
  return (
    policy.writablePaths.some((root) => isPathWithin(canonical, root.path)) ||
    policy.readonlyPaths.some((root) => isPathWithin(canonical, root))
  );
}

/**
 * realpath alone cannot reveal bind-mount aliases. Compare the device/inode of
 * each writable root with the complete ancestor chain of the reserved Skill
 * root (and vice versa) after all mounts are live.
 */
export function assertNoWritableAliasToSkills(
  policy: SandboxWritePolicy,
  skillsPath = SKILLS_READONLY_PATH,
): void {
  const canonicalSkillsPath = canonicalPolicyPath(skillsPath);
  const skillIdentity = existsSync(canonicalSkillsPath)
    ? filesystemIdentity(canonicalSkillsPath)
    : undefined;
  const skillAncestors =
    skillIdentity === undefined ? undefined : ancestorIdentities(canonicalSkillsPath);

  for (const writable of policy.writablePaths) {
    if (!existsSync(writable.path)) {
      throw new Error(`sandbox writable root does not exist: ${writable.path}`);
    }
    if (
      skillIdentity !== undefined &&
      (skillAncestors!.has(filesystemIdentity(writable.path)) ||
        ancestorIdentities(writable.path).has(skillIdentity))
    ) {
      throw new Error(
        `sandbox writable root aliases reserved Skill root: ${writable.path} and ${canonicalSkillsPath}`,
      );
    }
  }
  for (const readonly of policy.readonlyPaths) {
    if (!existsSync(readonly)) {
      throw new Error(`sandbox read-only root does not exist: ${readonly}`);
    }
  }

  if (process.platform === 'linux') {
    assertNoNestedMountsUnderPolicyRoots(policy, readFileSync('/proc/self/mountinfo', 'utf8'));
  }
}

/**
 * Reject mount points strictly below any readable session root. Bubblewrap
 * preserves nested bind mounts when it re-exposes a resource root; without
 * this check an image can pre-bind `/` below a repository and regain access
 * to the reserved Skill tree or image credentials through an allowed path.
 */
export function assertNoNestedMountsUnderPolicyRoots(
  policy: SandboxWritePolicy,
  mountInfo: string,
): void {
  const roots = [...policy.writablePaths.map((entry) => entry.path), ...policy.readonlyPaths];
  for (const line of mountInfo.split('\n')) {
    if (line.length === 0) continue;
    const fields = line.split(' ');
    if (fields.length < 6) {
      throw new Error('invalid /proc/self/mountinfo entry');
    }
    const mountpoint = decodeMountInfoPath(fields[4]!);
    for (const root of roots) {
      if (mountpoint !== root && isPathWithin(mountpoint, root)) {
        throw new Error(`sandbox policy root contains nested mount: ${root} -> ${mountpoint}`);
      }
    }
  }
}

function nativeWritePath(toolName: string, input: Record<string, unknown>): string | undefined {
  if (toolName === 'Write' || toolName === 'Edit' || toolName === 'MultiEdit') {
    return typeof input.file_path === 'string' ? input.file_path : undefined;
  }
  if (toolName === 'NotebookEdit') {
    return typeof input.notebook_path === 'string' ? input.notebook_path : undefined;
  }
  return undefined;
}

function canonicalPolicyPath(value: string): string {
  if (!value.startsWith('/')) throw new Error(`sandbox path must be absolute: ${value}`);
  if (value.includes('\0')) throw new Error('sandbox path contains NUL');
  const normalized = path.normalize(value);
  const canonical = normalized.length > 1 ? normalized.replace(/\/$/, '') : normalized;
  if (canonical !== value) throw new Error(`sandbox path is not canonical: ${value}`);
  return canonical;
}

/** Resolve existing symlinks while retaining non-existent leaf components. */
function canonicalizeExistingPath(absolutePath: string): string {
  let cursor = absolutePath;
  const missing: string[] = [];
  while (!existsSync(cursor) && cursor !== '/') {
    missing.unshift(basename(cursor));
    cursor = dirname(cursor);
  }
  const resolved = resolveHostPath(realpathSync(cursor), ...missing);
  return resolved.split(sep).join('/');
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isWritablePathKind(value: unknown): value is WritablePathKind {
  return value === 'session_output' || value === 'memory_store' || value === 'github_repository';
}

function parseNetworkAllowedDomains(value: unknown): string[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) {
    throw new Error(`invalid ${SANDBOX_WRITE_POLICY_ENV}: networkAllowedDomains is not an array`);
  }
  return [...new Set(value.map((entry, index) => canonicalNetworkDomain(entry, index)))].sort();
}

function canonicalNetworkDomain(value: unknown, index: number): string {
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    value.trim() !== value ||
    /[\s/\\@?#\0]/.test(value)
  ) {
    throw new Error(
      `invalid ${SANDBOX_WRITE_POLICY_ENV}: networkAllowedDomains[${index}] is malformed`,
    );
  }
  let parsed: URL;
  try {
    parsed = new URL(`https://${value}`);
  } catch {
    throw new Error(
      `invalid ${SANDBOX_WRITE_POLICY_ENV}: networkAllowedDomains[${index}] is malformed`,
    );
  }
  const canonical = parsed.hostname.toLowerCase();
  if (
    parsed.username ||
    parsed.password ||
    parsed.port ||
    parsed.pathname !== '/' ||
    canonical !== value.toLowerCase()
  ) {
    throw new Error(
      `invalid ${SANDBOX_WRITE_POLICY_ENV}: networkAllowedDomains[${index}] is malformed`,
    );
  }
  return canonical;
}

function isPathWithin(candidate: string, root: string): boolean {
  return candidate === root || candidate.startsWith(`${root}/`);
}

function ancestorIdentities(value: string): Set<string> {
  const identities = new Set<string>();
  let cursor = canonicalPolicyPath(value);
  while (true) {
    identities.add(filesystemIdentity(cursor));
    if (cursor === '/') break;
    cursor = path.dirname(cursor);
  }
  return identities;
}

function filesystemIdentity(value: string): string {
  const stat = statSync(value, { bigint: true });
  return `${stat.dev}:${stat.ino}`;
}

function decodeMountInfoPath(value: string): string {
  return value.replace(/\\([0-7]{3})/g, (_match, octal: string) =>
    String.fromCharCode(Number.parseInt(octal, 8)),
  );
}

function pathsOverlap(a: string, b: string): boolean {
  return isPathWithin(a, b) || isPathWithin(b, a);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
