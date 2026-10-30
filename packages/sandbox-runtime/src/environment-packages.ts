// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { posix } from 'node:path';
import type { Packages, SandboxWritePolicy } from './sandbox-runtime.js';

/** Versioned provider contract. The directory contains untrusted workload files only. */
export const ENVIRONMENT_PACKAGES_EXTENSION = 'orca.environment.packages.v1';
export const ENVIRONMENT_PACKAGES_ROOT = '/opt/orca-environment-packages';
export const ENVIRONMENT_PACKAGES_ROOTFS = `${ENVIRONMENT_PACKAGES_ROOT}/rootfs`;
export const ENVIRONMENT_PACKAGES_EXEC_ROOT = '/opt/orca-environment-rootfs';
export const ENVIRONMENT_PACKAGES_PROBE = '/usr/local/libexec/orca-environment-probe';
export const ENVIRONMENT_PACKAGES_ENV = 'ORCA_ENVIRONMENT_PACKAGES';
export const ENVIRONMENT_PACKAGE_TIMEOUT_MS = 900_000;

/** Test the local management listener directly, without gateway authentication. */
export function buildOpenSandboxExecdAuthenticationProbeCommand(): string {
  const script = `// orca_execd_authentication_probe
fetch('http://127.0.0.1:44772/ping', { signal: AbortSignal.timeout(5000) }).then(response => {
  if (response.status !== 401) throw new Error('execd permits unauthenticated sandbox access');
}).catch(() => process.exit(1));`;
  return `env -i PATH=/usr/local/bin:/usr/bin node -e ${shellQuote(script)}`;
}

const MANAGERS = ['apt', 'cargo', 'gem', 'go', 'npm', 'pip'] as const;

/** Canonical, bounded input; package specifications are arguments, never shell programs. */
export function normalizeEnvironmentPackages(value: unknown): Packages {
  if (value === undefined || value === null) return {};
  if (typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('environment packages must be a package-manager object');
  }
  const input = value as Record<string, unknown>;
  if (Object.keys(input).some((key) => !MANAGERS.includes(key as (typeof MANAGERS)[number]))) {
    throw new Error('environment packages contains an unsupported package manager');
  }
  const out: Packages = {};
  for (const manager of MANAGERS) {
    const entries = input[manager];
    if (entries === undefined || entries === null) continue;
    if (!Array.isArray(entries) || entries.length > 128) {
      throw new Error(`environment packages.${manager} must contain at most 128 specifications`);
    }
    const specs = entries.map((entry: unknown) => {
      if (
        typeof entry !== 'string' ||
        entry.length === 0 ||
        entry.length > 1024 ||
        Buffer.from(entry, 'utf8').toString('utf8') !== entry ||
        entry.trim() !== entry ||
        entry.startsWith('\u0085') ||
        entry.endsWith('\u0085') ||
        entry.startsWith('-') ||
        [...entry].some(
          (character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127,
        )
      ) {
        throw new Error(`invalid environment packages.${manager} specification`);
      }
      return entry;
    });
    if (specs.length > 0) out[manager] = specs;
  }
  if (Buffer.byteLength(JSON.stringify(out)) > 128_000)
    throw new Error('environment package plan exceeds size limit');
  return out;
}

export function environmentPackagesRevision(packages: Packages): string {
  return createHash('sha256')
    .update(JSON.stringify(normalizeEnvironmentPackages(packages)))
    .digest('hex');
}

export function currentEnvironmentPackagesRevision(
  env: NodeJS.ProcessEnv = process.env,
): string | undefined {
  const raw = env[ENVIRONMENT_PACKAGES_ENV];
  if (raw === undefined) return undefined;
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    throw new Error('invalid prepared environment package configuration');
  }
  const packages = normalizeEnvironmentPackages(value);
  return Object.keys(packages).length > 0 ? environmentPackagesRevision(packages) : undefined;
}

/** Only the provider init container receives this plan, with no Session credentials. */
export function prepareEnvironmentPackagePlan(
  packages: Packages | undefined,
  networking?: Record<string, unknown>,
): { packages: Packages; extension: string } | undefined {
  const normalized = normalizeEnvironmentPackages(packages);
  if (Object.keys(normalized).length === 0) return undefined;
  if (networking?.type !== undefined && networking.type !== 'unrestricted') {
    throw new Error('OpenSandbox package preparation requires unrestricted environment networking');
  }
  return { packages: normalized, extension: JSON.stringify({ version: 1, packages: normalized }) };
}

export function encodeEnvironmentPackages(
  packages: Packages | undefined,
  networking?: Record<string, unknown>,
): string | undefined {
  return prepareEnvironmentPackagePlan(packages, networking)?.extension;
}

/** Lifecycle messages may include installer-controlled data; expose only fixed reasons. */
export function environmentPackageFailureMessage(value: unknown): string | undefined {
  if (typeof value !== 'object' || value === null || !('status' in value)) return undefined;
  const status = value.status;
  if (typeof status !== 'object' || status === null || !('state' in status)) return undefined;
  if (!['Failed', 'Terminated', 'Stopping'].includes(String(status.state))) return undefined;
  const reasons: Record<string, string> = {
    ENVIRONMENT_PACKAGES_INSTALL_FAILED: 'package installer exited unsuccessfully',
    ENVIRONMENT_PACKAGES_START_FAILED: 'package installer could not start',
    ENVIRONMENT_PACKAGES_STORAGE_LIMIT: 'package filesystem exceeds available ephemeral storage',
    ENVIRONMENT_PACKAGES_POD_EVICTED: 'package Pod was evicted; check storage and resource limits',
  };
  const reason = 'reason' in status && typeof status.reason === 'string' ? status.reason : '';
  const detail = Object.hasOwn(reasons, reason)
    ? `${reason}: ${reasons[reason]}`
    : 'sandbox terminated before package preparation completed';
  return `OpenSandbox isolated environment package preparation failed: ${detail}`;
}

/** Prove the server honored the extension; an older provider must fail closed. */
export function buildEnvironmentPackagesProbe(packages: Packages): string {
  const script = `
const fs = require('node:fs');
const root = ${JSON.stringify(ENVIRONMENT_PACKAGES_ROOT)};
const mount = fs.readFileSync('/proc/self/mountinfo', 'utf8').split('\\n').find(line => {
  const fields = line.split(' ');
  return fields[4] === root && fields[5].split(',').includes('ro');
});
if (!mount) throw new Error('provider did not mount a read-only environment package filesystem');
const fd = fs.openSync(root + '/manifest.json', fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
if (!fs.fstatSync(fd).isFile() || fs.fstatSync(fd).size > 4096) throw new Error('invalid package manifest');
let manifest;
try { manifest = JSON.parse(fs.readFileSync(fd, 'utf8')); }
catch { throw new Error('invalid package manifest'); }
finally { fs.closeSync(fd); }
if (manifest.version !== 1 || manifest.revision !== ${JSON.stringify(environmentPackagesRevision(packages))})
  throw new Error('environment package revision mismatch');
const stat = fs.lstatSync(root + '/rootfs');
if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error('invalid environment package rootfs');
`;
  return `env -i PATH=/usr/local/bin:/usr/bin /usr/bin/env node -e ${shellQuote(script)}`;
}

/** Runs with the platform's Node, never an executable from the package filesystem. */
export const ENVIRONMENT_PACKAGE_COPY_SCRIPT = String.raw`
const fs = require('node:fs');
const path = require('node:path');
const [source, target, privateRoot, markerUid] = process.argv.slice(1);
if (source === '/' || target === '/') throw new Error('invalid package staging root');
if (!fs.lstatSync(source).isDirectory() || fs.lstatSync(source).isSymbolicLink()) throw new Error('invalid package source');
if (privateRoot === 'private-root') {
  if (fs.readdirSync(target).join(',') !== 'tmp' || !fs.lstatSync(target + '/tmp').isDirectory() ||
      fs.lstatSync(target + '/tmp').isSymbolicLink() || fs.readdirSync(target + '/tmp').length !== 0)
    throw new Error('package target is not a fresh private tool root');
  fs.rmdirSync(target + '/tmp');
  fs.rmdirSync(target);
}
if (fs.existsSync(target)) throw new Error('invalid package staging root');
let bytes = 0;
let count = 0;
function copy(from, to) {
  const stat = fs.lstatSync(from);
  if (++count > 500000) throw new Error('package filesystem exceeds file limit');
  if (stat.isSymbolicLink()) { fs.symlinkSync(fs.readlinkSync(from), to); return; }
  if (stat.isDirectory()) {
    fs.mkdirSync(to, { mode: 0o755 });
    for (const entry of fs.readdirSync(from)) copy(path.join(from, entry), path.join(to, entry));
    return;
  }
  if (!stat.isFile()) throw new Error('package filesystem contains a special file');
  bytes += stat.size;
  if (bytes > 8 * 1024 * 1024 * 1024) throw new Error('package filesystem exceeds size limit');
  fs.copyFileSync(from, to, fs.constants.COPYFILE_EXCL | fs.constants.COPYFILE_FICLONE);
  fs.chmodSync(to, (stat.mode & 0o777) | 0o444);
}
try {
  copy(source, target);
  for (const name of ['tmp', 'run', 'proc', 'sys', 'dev', 'workspace', 'mnt', '.orca']) {
    fs.rmSync(path.join(target, name), { recursive: true, force: true });
    fs.mkdirSync(path.join(target, name), { mode: 0o755 });
  }
  fs.mkdirSync(path.join(target, 'workspace/skills'), { recursive: true });
  fs.mkdirSync(path.join(target, 'mnt/session/outputs'), { recursive: true });
  fs.writeFileSync(path.join(target, '.orca/package-probe'), '', { mode: 0o444 });
  fs.writeFileSync(path.join(target, '.orca/package-readonly'), 'readonly', { mode: 0o444 });
  const uid = markerUid === undefined ? process.getuid() : Number(markerUid);
  fs.chownSync(path.join(target, '.orca/package-readonly'), uid, markerUid === undefined ? process.getgid() : uid);
}
catch (error) { fs.rmSync(target, { recursive: true, force: true }); throw error; }
`;

export function buildStageEnvironmentPackagesCommand(): string {
  return `env -i PATH=/usr/local/bin:/usr/bin node -e ${shellQuote(ENVIRONMENT_PACKAGE_COPY_SCRIPT)} ${shellQuote(ENVIRONMENT_PACKAGES_ROOTFS)} ${shellQuote(ENVIRONMENT_PACKAGES_EXEC_ROOT)} remote 1000`;
}

/** Invoke an immutable static executable directly, without a package shell or loader. */
export function buildEnvironmentPackageBoundaryProbe(
  policy: SandboxWritePolicy,
  root = ENVIRONMENT_PACKAGES_EXEC_ROOT,
  mapped = false,
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
    root,
    '/',
    '--ro-bind',
    ENVIRONMENT_PACKAGES_PROBE,
    '/.orca/package-probe',
    '--tmpfs',
    '/tmp',
    '--dev',
    '/dev',
    '--proc',
    '/proc',
  ];
  for (const path of policy.readonlyPaths)
    args.push('--ro-bind', mapped ? root + path : path, path);
  for (const path of policy.writablePaths)
    args.push('--bind', mapped ? root + path.path : path.path, path.path);
  // Probe only the disposable output root; never create sentinels in tenant Memory/Git mounts.
  args.push('--', '/.orca/package-probe', '/mnt/session/outputs');
  return args.map(shellQuote).join(' ');
}

export function assertEnvironmentPackageMountPaths(paths: readonly string[]): void {
  const reservedRoots = [
    ENVIRONMENT_PACKAGES_ROOT,
    ENVIRONMENT_PACKAGES_EXEC_ROOT,
    '/usr',
    '/bin',
    '/sbin',
    '/lib',
    '/lib64',
    '/etc',
    '/opt',
    '/app',
    '/tmp',
    '/run',
    '/var',
    '/home',
    '/root',
    '/proc',
    '/sys',
    '/dev',
    '/.orca/package-probe',
    '/.orca/package-readonly',
  ];
  for (const requested of paths) {
    const root = posix.normalize(requested).replace(/\/$/, '') || '/';
    if (
      reservedRoots.some(
        (reserved) =>
          root === '/' ||
          root === reserved ||
          root.startsWith(reserved + '/') ||
          reserved.startsWith(root + '/'),
      )
    )
      throw new Error('session resource overlaps the environment package runtime');
  }
}

/** Pre-create exact bind targets without following any package-provided path alias. */
export function buildEnvironmentPackageMountTargets(paths: readonly string[]): string {
  assertEnvironmentPackageMountPaths(paths);
  const script = String.raw`
const fs = require('node:fs');
const path = require('node:path');
const root = process.argv[1];
for (const source of JSON.parse(process.argv[2])) {
  const parts = source.split('/').filter(Boolean);
  let cursor = root;
  for (let index = 0; index < parts.length; index++) {
    cursor = path.join(cursor, parts[index]);
    const leaf = index === parts.length - 1;
    if (fs.existsSync(cursor) || (() => { try { fs.lstatSync(cursor); return true; } catch { return false; } })()) {
      const stat = fs.lstatSync(cursor);
      if (stat.isSymbolicLink() || (!leaf && !stat.isDirectory())) throw new Error('package filesystem aliases a session mount');
      if (leaf && stat.isDirectory() !== fs.statSync(source).isDirectory()) throw new Error('package mount target type mismatch');
    } else if (!leaf || fs.statSync(source).isDirectory()) fs.mkdirSync(cursor, { mode: 0o755 });
    else fs.writeFileSync(cursor, '', { flag: 'wx', mode: 0o644 });
  }
}
`;
  return `env -i PATH=/usr/local/bin:/usr/bin node -e ${shellQuote(script)} ${shellQuote(ENVIRONMENT_PACKAGES_EXEC_ROOT)} ${shellQuote(JSON.stringify(paths))}`;
}

/** Trusted launcher arguments. Installed binaries execute only inside this nested namespace. */
export function environmentPackageShellEnvironment(): Record<string, string> {
  return {
    PATH: '/opt/orca-packages/python/bin:/opt/orca-packages/npm/bin:/opt/orca-packages/cargo/bin:/opt/orca-packages/gems/bin:/opt/orca-packages/go/bin:/opt/orca-packages/toolchain-bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
    NODE_PATH: '/opt/orca-packages/npm/lib/node_modules',
    GEM_HOME: '/opt/orca-packages/gems',
    CARGO_HOME: '/tmp/orca-cargo',
    GOPATH: '/tmp/orca-go',
  };
}

/** This profile executes only after entering the workload's kernel boundary. */
export function environmentPackageShellCommand(command: string): string {
  // SRT's shell-quote layer escapes literal ! even inside quoted programs.
  // Decode only after entering the workload namespace so Bash receives the
  // original bytes without invoking a package executable in the trusted stage.
  return (
    'if [ -f /opt/orca-packages/environment.sh ]; then . /opt/orca-packages/environment.sh; fi\n' +
    `eval -- "$(printf '%s' '${Buffer.from(command).toString('base64')}' | base64 -d)"`
  );
}

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'"'"'`)}'`;
}
