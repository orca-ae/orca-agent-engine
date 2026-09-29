// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

type McpServerRecord = Record<string, unknown>;

export function stripMcpServerPermissionPolicyFields(input: unknown[] | undefined): unknown[] {
  return (input ?? []).map((server) => {
    if (!server || typeof server !== 'object' || Array.isArray(server)) return server;
    const out: McpServerRecord = { ...(server as McpServerRecord) };
    delete out['permission_policy'];
    return out;
  });
}

export function stripSessionMcpServerPermissionPolicyFields(
  input: unknown[] | null | undefined,
): unknown[] | null {
  return input == null ? null : stripMcpServerPermissionPolicyFields(input);
}
