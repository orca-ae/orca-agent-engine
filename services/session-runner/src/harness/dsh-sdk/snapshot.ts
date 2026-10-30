// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { RunnerSnapshot } from '../../snapshot.js';

const tools = new Set(['read', 'write', 'edit', 'list', 'glob', 'grep', 'delete', 'bash']);

/** Bootstrap validation only; this does not grant execution or resource placement. */
export function validateDshBootstrapSnapshot(snapshot: RunnerSnapshot): void {
  if (
    snapshot.provider !== 'dsh-sdk' ||
    snapshot.model.provider !== 'deepseek' ||
    snapshot.model.id !== 'deepseek-v4-flash' ||
    snapshot.model.effort !== undefined
  )
    throw new Error('DSH bootstrap requires the pinned DeepSeek Messages profile');
  if (
    snapshot.allowed_tool_names.some(
      (name) =>
        !tools.has(name) &&
        !(
          name === 'web_fetch' &&
          (snapshot.tool_permissions[name] ?? snapshot.default_tool_permission) === 'always_deny'
        ),
    ) ||
    snapshot.allowed_mcp_server_names.length ||
    snapshot.custom_tools?.length ||
    snapshot.skills?.length ||
    snapshot.skills_plugin_dir !== undefined ||
    snapshot.multiagent !== undefined ||
    snapshot.custom_spec !== undefined ||
    snapshot.guardrails?.length ||
    Object.keys(snapshot.guardrail_state ?? {}).length ||
    snapshot.request_guardrails_owner !== undefined ||
    snapshot.managed_resources !== undefined ||
    snapshot.sandbox_resources !== undefined ||
    snapshot.harness_state !== undefined
  )
    throw new Error('Unsupported DSH bootstrap snapshot surface');
  const egress = snapshot.egress as {
    mode?: unknown;
    gateway?: { llm_base_url?: unknown; llm_jwt?: unknown; mcp_servers?: unknown };
  } | null;
  const gateway = egress?.gateway;
  if (
    egress?.mode !== 'gateway' ||
    typeof gateway?.llm_base_url !== 'string' ||
    typeof gateway.llm_jwt !== 'string' ||
    !gateway.llm_jwt ||
    (gateway.mcp_servers !== undefined &&
      (gateway.mcp_servers === null ||
        typeof gateway.mcp_servers !== 'object' ||
        Object.keys(gateway.mcp_servers).length !== 0))
  )
    throw new Error('DSH bootstrap requires scoped Gateway egress without MCP');
  let origin: URL;
  try {
    origin = new URL(gateway.llm_base_url);
  } catch {
    throw new Error('DSH bootstrap requires a Gateway origin');
  }
  if (
    !['http:', 'https:'].includes(origin.protocol) ||
    origin.username ||
    origin.password ||
    origin.search ||
    origin.hash ||
    origin.pathname !== '/'
  )
    throw new Error('DSH bootstrap requires a Gateway origin');
}
