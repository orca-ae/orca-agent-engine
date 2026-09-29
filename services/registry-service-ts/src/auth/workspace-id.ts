// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export const WORKSPACE_ID_MAX_LENGTH = 128;
export const WORKSPACE_ID_RE = new RegExp(`^[A-Za-z0-9_-]{1,${WORKSPACE_ID_MAX_LENGTH}}$`);

export function parseWorkspaceId(value: unknown): string | null {
  return typeof value === 'string' && WORKSPACE_ID_RE.test(value) ? value : null;
}

export function isWorkspaceId(value: unknown): value is string {
  return parseWorkspaceId(value) !== null;
}
