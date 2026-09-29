// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import path from 'node:path';
import {
  SkillBundleIntegrityError,
  type SkillBundle,
  type SkillBundleFile,
  type SkillBundleInputFile,
  type SkillBundleManifestEntry,
  type SkillBundleRecord,
  SkillBundleValidationError,
} from './types.js';
import { unicodeFullCaseFold } from './unicode-case-fold.js';

const ENVELOPE_FORMAT = 'orca.skill-bundle.v1';
const DEFAULT_FILE_MODE = 0o644;
const WINDOWS_ABSOLUTE_PATH = /^[A-Za-z]:\//;
const MAX_BUNDLE_BYTES = 64 * 1024 * 1024;
const MAX_PATH_BYTES = 4_000;
const MAX_PATH_COMPONENT_BYTES = 255;

interface CanonicalEnvelopeFile {
  path: string;
  mode: number;
  mimeType: string | null;
  sha256: string;
  contentBase64: string;
}

interface CanonicalEnvelope {
  format: typeof ENVELOPE_FORMAT;
  files: CanonicalEnvelopeFile[];
}

interface EncodedSkillBundle {
  bytes: Buffer;
  bundle: SkillBundle;
}

export function encodeSkillBundle(files: SkillBundleInputFile[]): EncodedSkillBundle {
  if (!Array.isArray(files) || files.length === 0) {
    throw new SkillBundleValidationError('skill bundle must contain at least one file');
  }

  const normalized = files.map(normalizeInputFile).sort(compareByPath);
  assertNonCollidingPaths(normalized);
  const envelope = toEnvelope(normalized);
  const bytes = serializeEnvelope(envelope);
  if (bytes.length > MAX_BUNDLE_BYTES) {
    throw new SkillBundleValidationError(
      `skill bundle exceeds ${MAX_BUNDLE_BYTES} encoded byte limit`,
    );
  }
  const digest = sha256(bytes);
  return {
    bytes,
    bundle: buildBundle(digest, bytes.length, normalized),
  };
}

export function decodeSkillBundle(bytes: Buffer, expectedSha256: string): SkillBundle {
  if (bytes.length > MAX_BUNDLE_BYTES) {
    throw new SkillBundleIntegrityError(
      `skill bundle exceeds ${MAX_BUNDLE_BYTES} encoded byte limit`,
    );
  }
  const actualDigest = sha256(bytes);
  if (actualDigest !== expectedSha256) {
    throw new SkillBundleIntegrityError(
      `skill bundle digest mismatch: expected ${expectedSha256}, got ${actualDigest}`,
    );
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(bytes.toString('utf8'));
  } catch {
    throw new SkillBundleIntegrityError('skill bundle is not valid JSON');
  }
  const envelope = parseEnvelope(parsed);
  const canonicalBytes = serializeEnvelope(envelope);
  if (!canonicalBytes.equals(bytes)) {
    throw new SkillBundleIntegrityError('skill bundle envelope is not canonical JSON');
  }

  const files = envelope.files.map((file): SkillBundleFile => {
    const content = decodeCanonicalBase64(file.contentBase64, file.path);
    const contentDigest = sha256(content);
    if (contentDigest !== file.sha256) {
      throw new SkillBundleIntegrityError(`content digest mismatch for ${file.path}`);
    }
    return {
      path: file.path,
      sizeBytes: content.length,
      sha256: contentDigest,
      mode: file.mode,
      mimeType: file.mimeType,
      content,
    };
  });
  return buildBundle(actualDigest, bytes.length, files);
}

export function cloneBundleRecord(record: SkillBundleRecord): SkillBundleRecord {
  return {
    sha256: record.sha256,
    sizeBytes: record.sizeBytes,
    files: record.files.map(cloneManifestEntry),
  };
}

export function cloneSkillBundle(bundle: SkillBundle): SkillBundle {
  return {
    record: cloneBundleRecord(bundle.record),
    files: bundle.files.map((file) => ({
      ...cloneManifestEntry(file),
      content: Buffer.from(file.content),
    })),
  };
}

function normalizeInputFile(input: SkillBundleInputFile): SkillBundleFile {
  if (!Buffer.isBuffer(input.content)) {
    throw new SkillBundleValidationError(`skill file content must be a Buffer: ${input.path}`);
  }
  const normalizedPath = normalizeSkillPath(input.path);
  const mode = normalizeMode(input.mode, normalizedPath);
  const mimeType = normalizeMimeType(input.mimeType, normalizedPath);
  const content = Buffer.from(input.content);
  return {
    path: normalizedPath,
    sizeBytes: content.length,
    sha256: sha256(content),
    mode,
    mimeType,
    content,
  };
}

export function normalizeSkillPath(input: string): string {
  if (typeof input !== 'string' || input.length === 0 || input.includes('\0')) {
    throw new SkillBundleValidationError('skill file path must be a non-empty, NUL-free string');
  }
  if (input.includes('\\')) {
    throw new SkillBundleValidationError(`skill file path must use forward slashes: "${input}"`);
  }
  if (path.posix.isAbsolute(input) || WINDOWS_ABSOLUTE_PATH.test(input)) {
    throw new SkillBundleValidationError(`absolute skill file path is not allowed: "${input}"`);
  }
  if (input.split('/').includes('..')) {
    throw new SkillBundleValidationError(
      `parent traversal is not allowed in skill file path: "${input}"`,
    );
  }

  const normalized = path.posix.normalize(input);
  if (normalized === '.' || normalized === '..' || normalized.startsWith('../')) {
    throw new SkillBundleValidationError(`invalid skill file path: "${input}"`);
  }
  const relative = normalized.replace(/^\.\//, '');
  if (Buffer.byteLength(relative, 'utf8') > MAX_PATH_BYTES) {
    throw new SkillBundleValidationError(`skill file path is too long: "${input}"`);
  }
  if (
    relative
      .split('/')
      .some((segment) => Buffer.byteLength(segment, 'utf8') > MAX_PATH_COMPONENT_BYTES)
  ) {
    throw new SkillBundleValidationError(`skill file path component is too long: "${input}"`);
  }
  return relative;
}

function normalizeMode(value: number | undefined, filePath: string): number {
  const mode = value ?? DEFAULT_FILE_MODE;
  if (!Number.isInteger(mode) || mode < 0 || mode > 0o777) {
    throw new SkillBundleValidationError(`invalid mode for ${filePath}: ${String(value)}`);
  }
  // Preserve only executable intent. Customer-provided ownership/write bits
  // must not influence how a materializer creates the immutable skill tree.
  return (mode & 0o111) !== 0 ? 0o755 : DEFAULT_FILE_MODE;
}

function normalizeMimeType(value: string | null | undefined, filePath: string): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== 'string' || value.length === 0) {
    throw new SkillBundleValidationError(`invalid mimeType for ${filePath}`);
  }
  return value;
}

function parseEnvelope(value: unknown): CanonicalEnvelope {
  if (!isRecord(value) || value['format'] !== ENVELOPE_FORMAT || !Array.isArray(value['files'])) {
    throw new SkillBundleIntegrityError('invalid skill bundle envelope');
  }
  if (Object.keys(value).length !== 2) {
    throw new SkillBundleIntegrityError('skill bundle envelope contains unknown fields');
  }
  if (value['files'].length === 0) {
    throw new SkillBundleIntegrityError('skill bundle envelope contains no files');
  }

  const files = value['files'].map((raw): CanonicalEnvelopeFile => {
    if (!isRecord(raw) || Object.keys(raw).length !== 5) {
      throw new SkillBundleIntegrityError('invalid skill bundle file entry');
    }
    const rawPath = raw['path'];
    const rawMode = raw['mode'];
    const rawMimeType = raw['mimeType'];
    const rawSha256 = raw['sha256'];
    const rawContentBase64 = raw['contentBase64'];
    if (
      typeof rawPath !== 'string' ||
      typeof rawMode !== 'number' ||
      (rawMimeType !== null && typeof rawMimeType !== 'string') ||
      typeof rawSha256 !== 'string' ||
      typeof rawContentBase64 !== 'string'
    ) {
      throw new SkillBundleIntegrityError('invalid skill bundle file entry fields');
    }

    let normalizedPath: string;
    let normalizedMode: number;
    try {
      normalizedPath = normalizeSkillPath(rawPath);
      normalizedMode = normalizeMode(rawMode, normalizedPath);
      normalizeMimeType(rawMimeType, normalizedPath);
    } catch (error) {
      throw new SkillBundleIntegrityError(
        error instanceof Error ? error.message : 'invalid skill bundle file entry',
      );
    }
    if (
      normalizedPath !== rawPath ||
      normalizedMode !== rawMode ||
      !/^[0-9a-f]{64}$/.test(rawSha256)
    ) {
      throw new SkillBundleIntegrityError(`invalid canonical metadata for ${rawPath}`);
    }
    return {
      path: normalizedPath,
      mode: rawMode,
      mimeType: rawMimeType,
      sha256: rawSha256,
      contentBase64: rawContentBase64,
    };
  });

  const sorted = [...files].sort(compareByPath);
  assertCanonicalFileOrder(files, sorted);
  return { format: ENVELOPE_FORMAT, files };
}

function toEnvelope(files: SkillBundleFile[]): CanonicalEnvelope {
  return {
    format: ENVELOPE_FORMAT,
    files: files.map((file) => ({
      path: file.path,
      mode: file.mode,
      mimeType: file.mimeType,
      sha256: file.sha256,
      contentBase64: file.content.toString('base64'),
    })),
  };
}

function serializeEnvelope(envelope: CanonicalEnvelope): Buffer {
  return Buffer.from(JSON.stringify(envelope), 'utf8');
}

function buildBundle(digest: string, sizeBytes: number, files: SkillBundleFile[]): SkillBundle {
  const clonedFiles = files.map((file) => ({
    ...cloneManifestEntry(file),
    content: Buffer.from(file.content),
  }));
  return {
    record: {
      sha256: digest,
      sizeBytes,
      files: clonedFiles.map(cloneManifestEntry),
    },
    files: clonedFiles,
  };
}

function cloneManifestEntry(file: SkillBundleManifestEntry): SkillBundleManifestEntry {
  return {
    path: file.path,
    sizeBytes: file.sizeBytes,
    sha256: file.sha256,
    mode: file.mode,
    mimeType: file.mimeType,
  };
}

function decodeCanonicalBase64(value: string, filePath: string): Buffer {
  if (
    value.length % 4 !== 0 ||
    (value.length > 0 &&
      !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value))
  ) {
    throw new SkillBundleIntegrityError(`invalid base64 content for ${filePath}`);
  }
  const decoded = Buffer.from(value, 'base64');
  if (decoded.toString('base64') !== value) {
    throw new SkillBundleIntegrityError(`non-canonical base64 content for ${filePath}`);
  }
  return decoded;
}

function assertNonCollidingPaths(files: Array<{ path: string }>): void {
  const exactFilePaths = new Set<string>();
  const filePathByPortableKey = new Map<string, string>();

  for (const file of files) {
    if (exactFilePaths.has(file.path)) {
      throw new SkillBundleValidationError(`duplicate skill file path: ${file.path}`);
    }
    exactFilePaths.add(file.path);

    const portableKey = portablePathKey(file.path);
    const collidingPath = filePathByPortableKey.get(portableKey);
    if (collidingPath !== undefined) {
      throw new SkillBundleValidationError(
        `portable skill file path collision: ${collidingPath} and ${file.path}`,
      );
    }
    filePathByPortableKey.set(portableKey, file.path);
  }

  for (const file of files) {
    const segments = portablePathKey(file.path).split('/');
    let ancestor = '';
    for (let index = 0; index < segments.length - 1; index += 1) {
      ancestor = ancestor === '' ? segments[index]! : `${ancestor}/${segments[index]!}`;
      const ancestorPath = filePathByPortableKey.get(ancestor);
      if (ancestorPath !== undefined) {
        throw new SkillBundleValidationError(
          `skill file path conflicts with file ancestor: ${ancestorPath} and ${file.path}`,
        );
      }
    }
  }
}

function portablePathKey(filePath: string): string {
  return unicodeFullCaseFold(filePath.normalize('NFC')).normalize('NFC');
}

function assertCanonicalFileOrder(
  original: CanonicalEnvelopeFile[],
  sorted: CanonicalEnvelopeFile[],
): void {
  for (let index = 0; index < original.length; index += 1) {
    if (original[index]!.path !== sorted[index]!.path) {
      throw new SkillBundleIntegrityError('skill bundle files are not in canonical path order');
    }
  }
  try {
    assertNonCollidingPaths(original);
  } catch (error) {
    throw new SkillBundleIntegrityError(
      error instanceof Error ? error.message : 'colliding skill file path',
    );
  }
}

function compareByPath(left: { path: string }, right: { path: string }): number {
  return left.path < right.path ? -1 : left.path > right.path ? 1 : 0;
}

function sha256(content: Buffer): string {
  return createHash('sha256').update(content).digest('hex');
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
