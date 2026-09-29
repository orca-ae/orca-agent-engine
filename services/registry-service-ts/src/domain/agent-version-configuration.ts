// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { McpServer, ToolDef } from '../contracts/agents.contract.js';
import { PreparedAgentSnapshotSchema } from '../contracts/internal.contract.js';
import { normalizeModelForStorage, type StoredModel } from '../contracts/model-wire.js';
import { toCanonicalToolName } from '../contracts/toolset-aliasing.js';
import { resolveHarnessAnnotation, type ResolvedHarness } from '@orca/harness-catalog';

export interface AgentTool {
  type: string;
  [key: string]: unknown;
}

export interface CanonicalMultiagentAgentRef {
  type: 'agent';
  id: string;
  version: number;
}

export interface CanonicalMultiagent {
  type: 'coordinator';
  agents: CanonicalMultiagentAgentRef[];
}

export interface StrictAgentVersionConfiguration {
  harness: ResolvedHarness;
  model: StoredModel;
  tools: AgentTool[];
  mcpServers: unknown[];
  multiagent: CanonicalMultiagent | null;
}

/** Persisted AgentVersion state is unavailable or corrupted for Session creation. */
export class SessionAgentSnapshotUnavailableError extends Error {
  override readonly name = 'SessionAgentSnapshotUnavailableError';

  constructor() {
    super('session agent snapshot unavailable');
  }
}

export function canonicalizeAgentTools(values: unknown[]): AgentTool[] {
  return values.map((value) => {
    const tool = value as AgentTool;
    return {
      ...tool,
      type: toCanonicalToolName(tool.type),
    };
  });
}

/**
 * Validate the frozen AgentVersion snapshot used by a new Session. Every
 * exception or invalid configuration maps to one non-leaking availability
 * error so HTTP and Trigger callers share fail-closed behavior.
 */
export function strictAgentVersionConfiguration(
  snapshot: unknown,
  agentId: string,
  version: number,
): StrictAgentVersionConfiguration {
  try {
    const parsedSnapshot = PreparedAgentSnapshotSchema.safeParse(snapshot);
    if (
      !parsedSnapshot.success ||
      parsedSnapshot.data.id !== agentId ||
      parsedSnapshot.data.version !== version ||
      !parsedSnapshot.data.tools.every((tool) => ToolDef.safeParse(tool).success) ||
      !parsedSnapshot.data.mcp_servers.every(isValidPinnedMcpServer)
    ) {
      throw new SessionAgentSnapshotUnavailableError();
    }
    const model = normalizeModelForStorage(parsedSnapshot.data.model);
    if ('error' in model) throw new SessionAgentSnapshotUnavailableError();
    const harness = resolveHarnessAnnotation(parsedSnapshot.data.metadata);
    if ('error' in harness) throw new SessionAgentSnapshotUnavailableError();
    const tools = canonicalizeAgentTools(parsedSnapshot.data.tools);
    const mcpServers = parsedSnapshot.data.mcp_servers;
    if (validateAgentConfiguration(tools, mcpServers)) {
      throw new SessionAgentSnapshotUnavailableError();
    }
    return {
      harness,
      model,
      tools,
      mcpServers,
      multiagent: isCanonicalMultiagent(parsedSnapshot.data.multiagent)
        ? parsedSnapshot.data.multiagent
        : null,
    };
  } catch {
    throw new SessionAgentSnapshotUnavailableError();
  }
}

export function validateAgentConfiguration(tools: AgentTool[], servers: unknown[]): string | null {
  const serverNames = new Set<string>();
  for (const [index, value] of servers.entries()) {
    if (!isRecord(value)) return `mcp_servers.${index} must be an object`;
    if (value.type !== undefined && value.type !== 'url') {
      return `mcp_servers.${index}.type must be 'url'`;
    }
    if (typeof value.name !== 'string' || value.name.length < 1 || value.name.length > 255) {
      return `mcp_servers.${index}.name must contain 1-255 characters`;
    }
    if (serverNames.has(value.name)) return 'mcp_servers names must be unique';
    serverNames.add(value.name);
  }

  const builtins = new Set([
    'bash',
    'edit',
    'read',
    'write',
    'glob',
    'grep',
    'web_fetch',
    'web_search',
  ]);
  const customToolNames = new Set<string>();
  const referencedServerNames = new Set<string>();
  for (const [index, tool] of tools.entries()) {
    if (tool.type === 'custom') {
      if (typeof tool.name !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(tool.name)) {
        return `tools.${index}.name must contain 1-128 letters, digits, underscores, or hyphens`;
      }
      if (builtins.has(tool.name)) {
        return `tools.${index}.name is reserved by agent_toolset`;
      }
      if (customToolNames.has(tool.name)) return 'custom tool names must be unique';
      customToolNames.add(tool.name);
      if (
        typeof tool.description !== 'string' ||
        tool.description.length < 1 ||
        tool.description.length > 4096
      ) {
        return `tools.${index}.description must contain 1-4096 characters`;
      }
      if (!isRecord(tool.input_schema) || tool.input_schema.type !== 'object') {
        return `tools.${index}.input_schema.type must be 'object'`;
      }
      if (
        tool.input_schema.properties !== undefined &&
        tool.input_schema.properties !== null &&
        !isRecord(tool.input_schema.properties)
      ) {
        return `tools.${index}.input_schema.properties must be an object or null`;
      }
      if (
        tool.input_schema.required !== undefined &&
        tool.input_schema.required !== null &&
        (!Array.isArray(tool.input_schema.required) ||
          !tool.input_schema.required.every((value) => typeof value === 'string'))
      ) {
        return `tools.${index}.input_schema.required must be an array of strings or null`;
      }
      continue;
    }
    const configs = Array.isArray(tool.configs) ? tool.configs : [];
    const configNames = new Set<string>();
    for (const [configIndex, configValue] of configs.entries()) {
      if (!isRecord(configValue) || typeof configValue.name !== 'string') {
        return `tools.${index}.configs.${configIndex}.name is required`;
      }
      if (configNames.has(configValue.name)) return `tools.${index}.configs names must be unique`;
      configNames.add(configValue.name);
      if (tool.type === 'agent_toolset' && !builtins.has(configValue.name)) {
        return `tools.${index}.configs.${configIndex}.name is not a built-in tool`;
      }
      const policyError = validatePermissionPolicy(
        configValue.permission_policy,
        `tools.${index}.configs.${configIndex}.permission_policy`,
      );
      if (policyError) return policyError;
    }
    const defaultPolicyError = validatePermissionPolicy(
      isRecord(tool.default_config) ? tool.default_config.permission_policy : undefined,
      `tools.${index}.default_config.permission_policy`,
    );
    if (defaultPolicyError) return defaultPolicyError;
    if (tool.type === 'mcp_toolset') {
      if (typeof tool.mcp_server_name !== 'string' || !serverNames.has(tool.mcp_server_name)) {
        return `tools.${index}.mcp_server_name must reference mcp_servers`;
      }
      referencedServerNames.add(tool.mcp_server_name);
    }
  }
  for (const serverName of serverNames) {
    if (!referencedServerNames.has(serverName)) {
      return `mcp_servers.${serverName} must be referenced by an mcp_toolset`;
    }
  }
  return null;
}

export function isCanonicalMultiagent(value: unknown): value is CanonicalMultiagent {
  return (
    isRecord(value) &&
    value.type === 'coordinator' &&
    Array.isArray(value.agents) &&
    value.agents.every(
      (entry) =>
        isRecord(entry) &&
        entry.type === 'agent' &&
        typeof entry.id === 'string' &&
        Number.isInteger(entry.version) &&
        (entry.version as number) > 0,
    )
  );
}

function isValidPinnedMcpServer(value: unknown): boolean {
  if (!isRecord(value)) return false;
  return McpServer.safeParse(value.type === undefined ? { ...value, type: 'url' } : value).success;
}

function validatePermissionPolicy(value: unknown, path: string): string | null {
  if (value === undefined || value === null) return null;
  if (!isRecord(value) || (value.type !== 'always_allow' && value.type !== 'always_ask')) {
    return `${path} must be always_allow or always_ask`;
  }
  return null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}
