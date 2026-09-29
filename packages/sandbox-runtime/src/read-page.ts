// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { constants, type Stats } from 'node:fs';
import { open, realpath, stat, statfs, type FileHandle } from 'node:fs/promises';
import { posix as path, resolve as resolveHostPath, sep } from 'node:path';
import type { SandboxReadConstraint } from './sandbox-runtime.js';

export const DEFAULT_READ_LIMIT_BYTES = 100_000;
export const MAX_READ_LIMIT_BYTES = 100_000;
export const SANDBOX_READ_TIMEOUT_MS = 10_000;
export const SANDBOX_READ_COMMAND_ENVS = Object.freeze({
  BASH_ENV: '',
  ENV: '',
  NODE_OPTIONS: '',
  PATH: '/usr/local/bin:/usr/bin',
});

const MAX_UTF8_CODE_POINT_BYTES = 4;
const MAX_SANDBOX_READ_STDOUT_BYTES = 150_000;
const FORBIDDEN_FILESYSTEM_TYPES = new Set([
  0x9fa0, // procfs
  0x62656572, // sysfs
  0x1cd1, // devpts
]);

export interface ReadPageInput {
  offset?: number;
  limit?: number;
}

export interface ReadPageMetadata {
  offset: number;
  limit: number;
  bytes_read: number;
  total_bytes: number;
  offset_unit: 'utf8_bytes';
  truncation: boolean;
  next_offset: number | null;
}

export interface ReadPage {
  content: string;
  metadata: ReadPageMetadata;
}

interface OpenedRoot {
  handle: FileHandle;
  path: string;
  requestedPath: string;
  stat: Stats;
}

interface SandboxReadWindow {
  data_base64: string;
  total_bytes: number;
}

/**
 * Return one bounded UTF-8 page from an in-memory buffer. Runtime adapters use
 * the window variant below so agent reads never need to load the whole file.
 */
export function readUtf8Page(bytes: Buffer, input: ReadPageInput): ReadPage {
  const { offset, limit } = normalizedReadInput(input);
  assertOffset(offset, bytes.length);
  const windowEnd = Math.min(bytes.length, offset + limit + MAX_UTF8_CODE_POINT_BYTES - 1);
  return readUtf8Window(bytes.subarray(offset, windowEnd), bytes.length, input);
}

/**
 * Atomically authorize and pread one page from a host-visible file. The
 * descriptor used for identity checks is the descriptor used for all reads.
 */
export async function readUtf8FilePage(
  filePath: string,
  constraint: SandboxReadConstraint | undefined,
  input: ReadPageInput,
  platform = process.platform,
): Promise<ReadPage> {
  const { offset, limit } = normalizedReadInput(input);
  const { candidate, roots } = matchingHostRoots(filePath, constraint);
  const openedRoots: OpenedRoot[] = [];
  let target: FileHandle | undefined;

  try {
    for (const root of roots) {
      const handle = await open(root, constants.O_RDONLY | constants.O_NONBLOCK);
      try {
        const rootStat = await handle.stat();
        if (!rootStat.isDirectory() && !rootStat.isFile()) {
          throw new Error(`read denied for ${filePath}; readable root is not a file or directory`);
        }
        openedRoots.push({
          handle,
          path: await resolveOpenedDescriptorPath(handle.fd, root, rootStat, platform),
          requestedPath: root,
          stat: rootStat,
        });
      } catch (cause) {
        await handle.close().catch(() => undefined);
        throw cause;
      }
    }

    target = await openTargetFromRoots(candidate, openedRoots, platform);
    const targetStat = await target.stat();
    if (!targetStat.isFile() || !Number.isSafeInteger(targetStat.size) || targetStat.size < 0) {
      throw new Error(`read denied for ${filePath}; path is not a regular file`);
    }
    // A hard link inside an allowed root can alias an image secret without
    // changing its canonical path. Session resources are materialized copies,
    // so agent-readable regular files are required to have one link.
    if (targetStat.nlink !== 1) {
      throw new Error(`read denied for ${filePath}; hard-linked files are not readable`);
    }

    const openedPath = await resolveOpenedDescriptorPath(
      target.fd,
      candidate,
      targetStat,
      platform,
    );
    const currentRoots = await Promise.all(
      openedRoots.map(async (root) => ({
        ...root,
        path: await resolveOpenedDescriptorPath(
          root.handle.fd,
          root.requestedPath,
          root.stat,
          platform,
        ),
      })),
    );
    if (!currentRoots.some((root) => openedTargetIsWithinRoot(openedPath, targetStat, root))) {
      throw new Error(`read denied for ${filePath}; path escapes its session resource root`);
    }

    const filesystemPath = platform === 'linux' ? `/proc/self/fd/${target.fd}` : openedPath;
    const filesystem = await statfs(filesystemPath);
    if (FORBIDDEN_FILESYSTEM_TYPES.has(Number(filesystem.type))) {
      throw new Error(`read denied for ${filePath}; pseudo-filesystems are not readable`);
    }

    assertOffset(offset, targetStat.size);
    const window = await preadWindow(target, targetStat.size, offset, limit, filePath);
    return readUtf8Window(window, targetStat.size, input);
  } finally {
    await target?.close().catch(() => undefined);
    await Promise.all(
      openedRoots.map(async (root) => await root.handle.close().catch(() => undefined)),
    );
  }
}

/**
 * Resolve the object behind an already-open descriptor without replacing the
 * checked object with a second path lookup.
 */
export async function resolveOpenedDescriptorPath(
  fd: number,
  requestedPath: string,
  openedStat: Pick<Stats, 'dev' | 'ino'>,
  platform = process.platform,
): Promise<string> {
  if (platform === 'linux') {
    try {
      return await realpath(`/proc/self/fd/${fd}`);
    } catch (cause) {
      throw new Error(
        `read denied for ${requestedPath}; Linux descriptor identity is unavailable`,
        { cause },
      );
    }
  }
  if (platform === 'darwin') {
    const canonical = await realpath(requestedPath);
    const current = await stat(canonical);
    if (!sameIdentity(openedStat, current)) {
      throw new Error(`read denied for ${requestedPath}; file changed during identity check`);
    }
    return canonical;
  }
  throw new Error(
    `read denied for ${requestedPath}; descriptor verification is unsupported on ${platform}`,
  );
}

/** Map an absolute sandbox path into a private Local/InMemory work directory. */
export function resolveSandboxPathUnderRoot(sandboxRoot: string, sandboxPath: string): string {
  const canonicalRoot = resolveHostPath(sandboxRoot);
  const canonicalPath = canonicalSandboxPath(sandboxPath);
  const resolved = resolveHostPath(canonicalRoot, `.${canonicalPath}`);
  if (!isHostPathWithin(resolved, canonicalRoot)) {
    throw new Error(`sandbox path escapes its runtime root: ${sandboxPath}`);
  }
  return resolved;
}

/**
 * Build the single remote command used by E2B/OpenSandbox ranged reads. The
 * helper opens stable root and target descriptors, validates them, and preads
 * only limit+3 bytes before emitting bounded JSON.
 */
export function buildSandboxReadPageCommand(
  filePath: string,
  constraint: SandboxReadConstraint | undefined,
  input: ReadPageInput,
): string {
  const { offset, limit } = normalizedReadInput(input);
  const candidate = canonicalSandboxPath(filePath);
  const roots = matchingSandboxRoots(candidate, constraint);
  return [
    '/usr/bin/env',
    '-i',
    'PATH=/usr/local/bin:/usr/bin',
    'node',
    '-e',
    shellQuote(SANDBOX_READ_HELPER_SOURCE),
    '--',
    shellQuote(candidate),
    shellQuote(String(offset)),
    shellQuote(String(limit)),
    shellQuote(JSON.stringify(roots)),
  ].join(' ');
}

/** Verify and decode the bounded response from the remote fd helper. */
export function parseSandboxReadPageResult(stdout: string, input: ReadPageInput): ReadPage {
  if (Buffer.byteLength(stdout, 'utf8') > MAX_SANDBOX_READ_STDOUT_BYTES) {
    throw new Error('sandbox ranged read returned an oversized response');
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(stdout);
  } catch {
    throw new Error('sandbox ranged read returned invalid JSON');
  }
  if (!isSandboxReadWindow(parsed)) {
    throw new Error('sandbox ranged read returned an invalid response');
  }

  const { offset, limit } = normalizedReadInput(input);
  assertOffset(offset, parsed.total_bytes);
  if (!isCanonicalBase64(parsed.data_base64)) {
    throw new Error('sandbox ranged read returned invalid base64');
  }
  const window = Buffer.from(parsed.data_base64, 'base64');
  const expected = Math.min(parsed.total_bytes - offset, limit + MAX_UTF8_CODE_POINT_BYTES - 1);
  if (window.length !== expected) {
    throw new Error('sandbox ranged read returned an unexpected byte count');
  }
  return readUtf8Window(window, parsed.total_bytes, input);
}

/** Probe the exact Node + Linux descriptor APIs required by remote adapters. */
export function buildSandboxReadPrerequisiteProbeCommand(): string {
  return [
    '/usr/bin/env',
    '-i',
    'PATH=/usr/local/bin:/usr/bin',
    'node',
    '-e',
    shellQuote(SANDBOX_READ_PREREQUISITE_SOURCE),
  ].join(' ');
}

function readUtf8Window(window: Buffer, totalBytes: number, input: ReadPageInput): ReadPage {
  const { offset, limit } = normalizedReadInput(input);
  assertOffset(offset, totalBytes);
  const expected = Math.min(totalBytes - offset, limit + MAX_UTF8_CODE_POINT_BYTES - 1);
  if (window.length !== expected) {
    throw new Error('ranged read window has an unexpected byte count');
  }
  if (offset < totalBytes && isUtf8ContinuationByte(window[0]!)) {
    throw new Error(`offset ${offset} is not a UTF-8 code point boundary`);
  }

  let pageLength = Math.min(limit, window.length);
  if (offset + pageLength < totalBytes) {
    while (pageLength > 0 && isUtf8ContinuationByte(window[pageLength]!)) pageLength -= 1;
    // A limit smaller than the next code point must still make progress. The
    // page may exceed the requested limit by at most three bytes.
    if (pageLength === 0) {
      pageLength = 1;
      while (pageLength < window.length && isUtf8ContinuationByte(window[pageLength]!)) {
        pageLength += 1;
      }
    }
  }

  const truncation = offset + pageLength < totalBytes;
  const metadata: ReadPageMetadata = {
    offset,
    limit,
    bytes_read: pageLength,
    total_bytes: totalBytes,
    offset_unit: 'utf8_bytes',
    truncation,
    next_offset: truncation ? offset + pageLength : null,
  };
  const text = window.toString('utf8', 0, pageLength);
  const includeMetadata = truncation || input.offset !== undefined || input.limit !== undefined;
  return {
    content: includeMetadata ? appendMetadata(text, metadata) : text,
    metadata,
  };
}

async function preadWindow(
  handle: FileHandle,
  totalBytes: number,
  offset: number,
  limit: number,
  filePath: string,
): Promise<Buffer> {
  const requestedBytes = Math.min(totalBytes - offset, limit + MAX_UTF8_CODE_POINT_BYTES - 1);
  const bytes = Buffer.alloc(requestedBytes);
  let bytesRead = 0;
  while (bytesRead < requestedBytes) {
    const result = await handle.read(
      bytes,
      bytesRead,
      requestedBytes - bytesRead,
      offset + bytesRead,
    );
    if (result.bytesRead === 0) break;
    bytesRead += result.bytesRead;
  }
  if (bytesRead !== requestedBytes) {
    throw new Error(`file changed while reading ${filePath}; retry the read`);
  }
  return bytes;
}

function matchingHostRoots(
  filePath: string,
  constraint: SandboxReadConstraint | undefined,
): { candidate: string; roots: string[] } {
  if (!constraint) throw new Error('sandbox ranged read requires trusted readable roots');
  const candidate = canonicalHostPath(filePath);
  const roots = [...new Set(constraint.readableRoots.map(canonicalHostPath))].filter((root) =>
    isHostPathWithin(candidate, root),
  );
  if (roots.length === 0) {
    throw new Error(`read denied for ${filePath}; path is outside session resource roots`);
  }
  return { candidate, roots };
}

function matchingSandboxRoots(
  candidate: string,
  constraint: SandboxReadConstraint | undefined,
): string[] {
  if (!constraint) throw new Error('sandbox ranged read requires trusted readable roots');
  const roots = [...new Set(constraint.readableRoots.map(canonicalSandboxPath))].filter((root) =>
    isSandboxPathWithin(candidate, root),
  );
  if (roots.length === 0) {
    throw new Error(`read denied for ${candidate}; path is outside session resource roots`);
  }
  return roots;
}

async function openTargetFromRoots(
  candidate: string,
  roots: OpenedRoot[],
  platform: NodeJS.Platform,
): Promise<FileHandle> {
  if (platform !== 'linux') {
    return await open(candidate, constants.O_RDONLY | constants.O_NONBLOCK);
  }

  let lastError: unknown;
  for (const root of roots) {
    const descriptorPath = root.stat.isFile()
      ? `/proc/self/fd/${root.handle.fd}`
      : `/proc/self/fd/${root.handle.fd}/${path.relative(root.requestedPath, candidate)}`;
    try {
      return await open(descriptorPath, constants.O_RDONLY | constants.O_NONBLOCK);
    } catch (cause) {
      lastError = cause;
    }
  }
  throw new Error(`read denied for ${candidate}; target could not be opened from a readable root`, {
    cause: lastError,
  });
}

function canonicalHostPath(value: string): string {
  if (!value.startsWith('/')) throw new Error(`sandbox path must be absolute: ${value}`);
  if (value.includes('\0')) throw new Error('sandbox path contains NUL');
  return resolveHostPath(value);
}

function canonicalSandboxPath(value: string): string {
  if (!path.isAbsolute(value)) throw new Error(`sandbox path must be absolute: ${value}`);
  if (value.includes('\0')) throw new Error('sandbox path contains NUL');
  const normalized = path.normalize(value);
  return normalized.length > 1 ? normalized.replace(/\/$/, '') : normalized;
}

function normalizedReadInput(input: ReadPageInput): { offset: number; limit: number } {
  const offset = input.offset ?? 0;
  const limit = input.limit ?? DEFAULT_READ_LIMIT_BYTES;
  if (!Number.isSafeInteger(offset) || offset < 0) {
    throw new Error('offset must be a non-negative integer UTF-8 byte position');
  }
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_READ_LIMIT_BYTES) {
    throw new Error(`limit must be an integer between 1 and ${MAX_READ_LIMIT_BYTES} bytes`);
  }
  return { offset, limit };
}

function assertOffset(offset: number, totalBytes: number): void {
  if (!Number.isSafeInteger(totalBytes) || totalBytes < 0) {
    throw new Error('sandbox ranged read returned an invalid file size');
  }
  if (offset > totalBytes) {
    throw new Error(`offset ${offset} exceeds file size ${totalBytes} bytes`);
  }
}

function openedTargetIsWithinRoot(
  openedPath: string,
  targetStat: Stats,
  root: OpenedRoot,
): boolean {
  if (root.stat.isFile()) return sameIdentity(targetStat, root.stat);
  return targetStat.dev === root.stat.dev && isHostPathWithin(openedPath, root.path);
}

function sameIdentity(
  first: Pick<Stats, 'dev' | 'ino'>,
  second: Pick<Stats, 'dev' | 'ino'>,
): boolean {
  return first.dev === second.dev && first.ino === second.ino;
}

function isHostPathWithin(candidate: string, root: string): boolean {
  return candidate === root || candidate.startsWith(root === sep ? sep : `${root}${sep}`);
}

function isSandboxPathWithin(candidate: string, root: string): boolean {
  return candidate === root || candidate.startsWith(root === '/' ? '/' : `${root}/`);
}

function isUtf8ContinuationByte(byte: number): boolean {
  return (byte & 0xc0) === 0x80;
}

function appendMetadata(content: string, metadata: ReadPageMetadata): string {
  const separator = content.length === 0 || content.endsWith('\n') ? '' : '\n';
  return `${content}${separator}[orca_read ${JSON.stringify(metadata)}]`;
}

function isSandboxReadWindow(value: unknown): value is SandboxReadWindow {
  if (!value || typeof value !== 'object') return false;
  const candidate = value as Partial<SandboxReadWindow>;
  return (
    typeof candidate.data_base64 === 'string' &&
    Number.isSafeInteger(candidate.total_bytes) &&
    candidate.total_bytes! >= 0
  );
}

function isCanonicalBase64(value: string): boolean {
  if (value.length === 0) return true;
  if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) {
    return false;
  }
  return Buffer.from(value, 'base64').toString('base64') === value;
}

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, "'\\''")}'`;
}

const SANDBOX_READ_PREREQUISITE_SOURCE = String.raw`
const fs = require('node:fs');
let rootFd;
let targetFd;
try {
  rootFd = fs.openSync('/proc/self', fs.constants.O_RDONLY | fs.constants.O_NONBLOCK);
  if (!fs.fstatSync(rootFd).isDirectory()) throw new Error('descriptor root is not a directory');
  targetFd = fs.openSync(
    '/proc/self/fd/' + rootFd + '/exe',
    fs.constants.O_RDONLY | fs.constants.O_NONBLOCK,
  );
  const stat = fs.fstatSync(targetFd);
  if (!stat.isFile()) throw new Error('descriptor is not regular');
  const descriptor = '/proc/self/fd/' + targetFd;
  fs.realpathSync(descriptor);
  fs.statfsSync(descriptor);
  fs.readSync(targetFd, Buffer.alloc(1), 0, 1, 0);
} finally {
  if (targetFd !== undefined) fs.closeSync(targetFd);
  if (rootFd !== undefined) fs.closeSync(rootFd);
}`;

const SANDBOX_READ_HELPER_SOURCE = String.raw`
const fs = require('node:fs');
const path = require('node:path').posix;
const [requestedPath, offsetRaw, limitRaw, rootsRaw] = process.argv.slice(1);
const offset = Number(offsetRaw);
const limit = Number(limitRaw);
const canonical = value => {
  if (typeof value !== 'string' || !path.isAbsolute(value) || value.includes('\0')) {
    throw new Error('sandbox read path must be absolute');
  }
  const normalized = path.normalize(value);
  return normalized.length > 1 ? normalized.replace(/\/$/, '') : normalized;
};
const within = (candidate, root) =>
  candidate === root || candidate.startsWith(root === '/' ? '/' : root + '/');
const identity = (first, second) => first.dev === second.dev && first.ino === second.ino;
const forbiddenFilesystems = new Set([0x9fa0, 0x62656572, 0x1cd1]);
if (!Number.isSafeInteger(offset) || offset < 0) throw new Error('invalid read offset');
if (!Number.isSafeInteger(limit) || limit < 1 || limit > ${MAX_READ_LIMIT_BYTES}) {
  throw new Error('invalid read limit');
}
const candidate = canonical(requestedPath);
const roots = JSON.parse(rootsRaw).map(canonical).filter(root => within(candidate, root));
if (roots.length === 0) throw new Error('read path is outside session resource roots');
const rootDescriptors = [];
let targetFd;
try {
  for (const root of roots) {
    const fd = fs.openSync(root, fs.constants.O_RDONLY | fs.constants.O_NONBLOCK);
    const stat = fs.fstatSync(fd);
    if (!stat.isDirectory() && !stat.isFile()) throw new Error('invalid readable root type');
    rootDescriptors.push({
      fd,
      requestedPath: root,
      path: fs.realpathSync('/proc/self/fd/' + fd),
      stat,
    });
  }
  let targetOpenError;
  for (const root of rootDescriptors) {
    const descriptorPath = root.stat.isFile()
      ? '/proc/self/fd/' + root.fd
      : '/proc/self/fd/' + root.fd + '/' + path.relative(root.requestedPath, candidate);
    try {
      targetFd = fs.openSync(
        descriptorPath,
        fs.constants.O_RDONLY | fs.constants.O_NONBLOCK,
      );
      break;
    } catch (error) {
      targetOpenError = error;
    }
  }
  if (targetFd === undefined) {
    throw new Error('read target could not be opened from a readable root', {
      cause: targetOpenError,
    });
  }
  const targetStat = fs.fstatSync(targetFd);
  if (!targetStat.isFile() || !Number.isSafeInteger(targetStat.size) || targetStat.size < 0) {
    throw new Error('read target is not a regular file');
  }
  if (targetStat.nlink !== 1) throw new Error('hard-linked files are not readable');
  const targetDescriptor = '/proc/self/fd/' + targetFd;
  const targetPath = fs.realpathSync(targetDescriptor);
  const authorized = rootDescriptors.some(root => {
    const currentRootPath = fs.realpathSync('/proc/self/fd/' + root.fd);
    return root.stat.isFile()
      ? identity(targetStat, root.stat)
      : targetStat.dev === root.stat.dev && within(targetPath, currentRootPath);
  });
  if (!authorized) throw new Error('read target escapes its session resource root');
  const filesystem = fs.statfsSync(targetDescriptor);
  if (forbiddenFilesystems.has(Number(filesystem.type))) {
    throw new Error('pseudo-filesystems are not readable');
  }
  if (offset > targetStat.size) throw new Error('read offset exceeds file size');
  const requested = Math.min(targetStat.size - offset, limit + ${MAX_UTF8_CODE_POINT_BYTES - 1});
  const bytes = Buffer.alloc(requested);
  let bytesRead = 0;
  while (bytesRead < requested) {
    const count = fs.readSync(
      targetFd,
      bytes,
      bytesRead,
      requested - bytesRead,
      offset + bytesRead,
    );
    if (count === 0) break;
    bytesRead += count;
  }
  if (bytesRead !== requested) throw new Error('file changed while reading');
  process.stdout.write(JSON.stringify({
    data_base64: bytes.toString('base64'),
    total_bytes: targetStat.size,
  }));
} finally {
  if (targetFd !== undefined) fs.closeSync(targetFd);
  for (const root of rootDescriptors) fs.closeSync(root.fd);
}`;
