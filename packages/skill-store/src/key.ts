// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

const SAFE_ID = /^[A-Za-z0-9_-]+$/;
const SHA256_HEX = /^[0-9a-f]{64}$/;
const SAFE_PREFIX_SEGMENT = /^[A-Za-z0-9._-]+$/;

export function assertSafeStoreId(kind: 'workspaceId' | 'versionId', value: string): void {
  if (!SAFE_ID.test(value)) {
    throw new Error(`invalid ${kind} for skill bundle key: "${value}"`);
  }
}

export function assertSha256(value: string): void {
  if (!SHA256_HEX.test(value)) {
    throw new Error(`invalid sha256 for skill bundle key: "${value}"`);
  }
}

export function normalizeRootPrefix(root: string | undefined): string {
  if (root === undefined || root === '') return '';
  if (root.startsWith('/') || root.includes('\\') || root.includes('\0')) {
    throw new Error(`invalid skill bundle root prefix: "${root}"`);
  }

  const withoutTrailingSlash = root.endsWith('/') ? root.slice(0, -1) : root;
  const segments = withoutTrailingSlash.split('/');
  if (
    segments.length === 0 ||
    segments.some(
      (segment) =>
        segment.length === 0 ||
        segment === '.' ||
        segment === '..' ||
        !SAFE_PREFIX_SEGMENT.test(segment),
    )
  ) {
    throw new Error(`invalid skill bundle root prefix: "${root}"`);
  }
  return `${withoutTrailingSlash}/`;
}

export function skillBundleKey(
  rootPrefix: string,
  workspaceId: string,
  versionId: string,
  sha256: string,
): string {
  assertSafeStoreId('workspaceId', workspaceId);
  assertSafeStoreId('versionId', versionId);
  assertSha256(sha256);
  return (
    `${rootPrefix}workspaces/${workspaceId}/skill-versions/${versionId}/bundles/` +
    `${sha256}/bundle.json`
  );
}
