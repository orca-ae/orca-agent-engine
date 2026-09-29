// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

const SAFE_SEGMENT = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const SAFE_WORKSPACE_SEGMENT = /^[A-Za-z0-9_-]{1,128}$/;
const SHA256_HEX = /^[0-9a-f]{64}$/;

export const MAX_MEMORY_PATH_LENGTH = 1024;

export function assertMemoryWorkspaceId(value: string): void {
  if (!SAFE_WORKSPACE_SEGMENT.test(value)) {
    throw new Error(`MemoryBlobStore: invalid workspaceId "${value}"`);
  }
}

export function assertMemoryStoreId(value: string): void {
  if (!SAFE_SEGMENT.test(value) || value === '.' || value === '..') {
    throw new Error(`MemoryBlobStore: invalid storeId "${value}"`);
  }
}

export function assertMemoryVersionSha256(value: string): void {
  if (!SHA256_HEX.test(value)) {
    throw new Error(`MemoryBlobStore: invalid sha256 "${value}"`);
  }
}

export function normalizeMemoryRelativePath(path: string): string {
  if (path.length > MAX_MEMORY_PATH_LENGTH) {
    throw new Error(`MemoryBlobStore: path exceeds ${MAX_MEMORY_PATH_LENGTH} characters`);
  }
  const clean = path.replace(/^\/+/, '');
  if (clean.length === 0) {
    throw new Error('MemoryBlobStore: path must not be empty');
  }
  const segments = clean.split('/');
  if (segments.some((segment) => segment.length === 0 || segment === '.' || segment === '..')) {
    throw new Error(`MemoryBlobStore: invalid relative path "${path}"`);
  }
  return segments.join('/');
}
