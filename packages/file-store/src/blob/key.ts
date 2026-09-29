// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

const SAFE_WORKSPACE_ID = /^[A-Za-z0-9_-]+$/;
const SHA256_HEX = /^[0-9a-f]{64}$/;
const SAFE_PREFIX_SEGMENT = /^[A-Za-z0-9._-]+$/;

/**
 * Normalize the operator-configured object-store root. The root is trusted
 * configuration, but validating it here prevents ambiguous keys and IAM glob
 * characters from entering the storage namespace.
 */
export function normalizeRootPrefix(root: string | undefined): string {
  if (root === undefined || root === '') return '';
  if (root.startsWith('/') || root.includes('\\') || root.includes('\0')) {
    throw new Error(`invalid file blob root prefix: "${root}"`);
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
    throw new Error(`invalid file blob root prefix: "${root}"`);
  }
  return `${withoutTrailingSlash}/`;
}

/**
 * Build the canonical workspace-owned object key for one content digest.
 */
export function fileBlobKey(rootPrefix: string, workspaceId: string, sha256: string): string {
  if (!SAFE_WORKSPACE_ID.test(workspaceId)) {
    throw new Error(`invalid workspaceId for file blob key: "${workspaceId}"`);
  }
  if (!SHA256_HEX.test(sha256)) {
    throw new Error(`invalid sha256 for file blob key: "${sha256}"`);
  }
  return (
    `${rootPrefix}workspaces/${workspaceId}/files/blobs/` +
    `${sha256.slice(0, 2)}/${sha256.slice(2, 4)}/${sha256}/content`
  );
}
