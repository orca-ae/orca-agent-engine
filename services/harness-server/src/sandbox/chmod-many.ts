// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { SandboxFileMode } from './sandbox-runtime.js';

export const SANDBOX_CHMOD_MANY_TIMEOUT_MS = 10_000;

export function serializeSandboxFileModes(
  root: string,
  entries: readonly SandboxFileMode[],
): Buffer {
  const normalizedRoot = normalizeAbsolutePath(root, 'chmod root');
  return Buffer.from(
    JSON.stringify(entries.map((entry) => normalizeEntry(normalizedRoot, entry))),
    'utf8',
  );
}

export function buildSandboxChmodManyCommand(manifestPath: string, root: string): string {
  const normalizedManifestPath = normalizeAbsolutePath(manifestPath, 'chmod manifest path');
  const normalizedRoot = normalizeAbsolutePath(root, 'chmod root');
  return [
    '/usr/bin/env',
    '-i',
    'PATH=/usr/local/bin:/usr/bin',
    'node',
    '-e',
    shellQuote(SANDBOX_CHMOD_MANY_SOURCE),
    shellQuote(normalizedManifestPath),
    shellQuote(normalizedRoot),
  ].join(' ');
}

function normalizeEntry(root: string, entry: SandboxFileMode): SandboxFileMode {
  const normalizedPath = normalizeAbsolutePath(entry.path, 'chmod path');
  if (normalizedPath !== root && !normalizedPath.startsWith(`${root}/`)) {
    throw new Error(`chmod path is outside root ${root}: ${entry.path}`);
  }
  if (entry.mode !== 0o444 && entry.mode !== 0o555) {
    throw new Error(`invalid file mode: ${entry.mode}`);
  }
  return { path: normalizedPath, mode: entry.mode };
}

function normalizeAbsolutePath(value: string, label: string): string {
  if (!value.startsWith('/') || value.includes('\0')) {
    throw new Error(`${label} must be absolute and NUL-free`);
  }
  const normalized = value.replace(/\/+/g, '/').replace(/\/$/, '') || '/';
  if (normalized.split('/').includes('..')) {
    throw new Error(`${label} must not contain parent traversal`);
  }
  return normalized;
}

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, "'\\''")}'`;
}

const SANDBOX_CHMOD_MANY_SOURCE = String.raw`
const fs = require('node:fs');
const path = require('node:path').posix;
const [manifestPath, expectedRoot] = process.argv.slice(1);
const canonical = value => {
  if (typeof value !== 'string' || !path.isAbsolute(value) || value.includes('\0')) {
    throw new Error('chmod path must be absolute and NUL-free');
  }
  const normalized = path.normalize(value);
  if (normalized.split('/').includes('..')) throw new Error('chmod path contains parent traversal');
  return normalized.length > 1 ? normalized.replace(/\/$/, '') : normalized;
};
const within = (candidate, root) =>
  candidate === root || candidate.startsWith(root === '/' ? '/' : root + '/');
let rootFd;
try {
  const entries = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  if (!Array.isArray(entries)) throw new Error('chmod manifest must be an array');
  const root = canonical(expectedRoot);
  rootFd = fs.openSync(
    root,
    fs.constants.O_RDONLY | fs.constants.O_NONBLOCK | fs.constants.O_NOFOLLOW,
  );
  if (!fs.fstatSync(rootFd).isDirectory()) throw new Error('chmod root must be a directory');
  const openedRoot = fs.realpathSync('/proc/self/fd/' + rootFd);
  if (openedRoot !== root) throw new Error('chmod root resolves outside its declared path');
  for (const entry of entries) {
    if (
      !entry ||
      typeof entry !== 'object' ||
      typeof entry.path !== 'string' ||
      (entry.mode !== ${0o444} && entry.mode !== ${0o555})
    ) {
      throw new Error('chmod manifest contains an invalid entry');
    }
    const target = canonical(entry.path);
    if (!within(target, root)) throw new Error('chmod target is outside its declared root');
    const relative = path.relative(root, target);
    if (relative.length === 0) {
      fs.fchmodSync(rootFd, entry.mode);
      continue;
    }
    const descriptorPath = '/proc/self/fd/' + rootFd + '/' + relative;
    const targetFd = fs.openSync(
      descriptorPath,
      fs.constants.O_RDONLY | fs.constants.O_NONBLOCK | fs.constants.O_NOFOLLOW,
    );
    try {
      const stat = fs.fstatSync(targetFd);
      if (!stat.isFile() && !stat.isDirectory()) {
        throw new Error('chmod target must be a regular file or directory: ' + target);
      }
      const openedTarget = fs.realpathSync('/proc/self/fd/' + targetFd);
      if (!within(openedTarget, openedRoot)) {
        throw new Error('chmod target escapes its declared root');
      }
      fs.fchmodSync(targetFd, entry.mode);
    } finally {
      fs.closeSync(targetFd);
    }
  }
} finally {
  if (rootFd !== undefined) fs.closeSync(rootFd);
  fs.rmSync(manifestPath, { force: true });
}`;
