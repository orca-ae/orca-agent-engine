// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Identify only our name indexes, including errors wrapped by Drizzle. */
export function nameConflictMessage(error: unknown): string | undefined {
  let current: unknown = error;
  for (let depth = 0; depth < 3; depth += 1) {
    if (!current || typeof current !== 'object') return undefined;
    const record = current as { code?: unknown; constraint?: unknown; cause?: unknown };
    if (record.code === '23505') {
      if (record.constraint === 'organizations_name_idx') return 'organization name already exists';
      if (record.constraint === 'workspaces_organization_name_idx') {
        return 'workspace name already exists in this organization';
      }
      return undefined;
    }
    current = record.cause;
  }
  return undefined;
}
