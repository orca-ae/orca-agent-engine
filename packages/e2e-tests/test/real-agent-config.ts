// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Provider selection for the shared real-model E2E scenarios. */
const harness = process.env['ORCA_E2E_AGENT_HARNESS'] ?? 'claude_agent_sdk';
if (harness !== 'claude_agent_sdk' && harness !== 'codex_sdk' && harness !== 'pi_sdk') {
  throw new Error(`Unsupported ORCA_E2E_AGENT_HARNESS: ${harness}`);
}

export const REAL_AGENT = {
  harness,
  isNativeSdk: harness === 'codex_sdk' || harness === 'pi_sdk',
  model:
    harness === 'codex_sdk'
      ? { provider: 'openai', id: 'gpt-5.4', effort: 'low' }
      : {
          provider: 'anthropic',
          id: harness === 'pi_sdk' ? 'claude-sonnet-4-6' : 'claude-sonnet-4-5-20250929',
        },
  // Omit mode deliberately: managed SDKs must default to separate execution.
  metadata: { harness },
  keyVariable: harness === 'codex_sdk' ? 'OPENAI_API_KEY' : 'ANTHROPIC_API_KEY',
};

export function requireRealAgentKey(): void {
  if (!process.env[REAL_AGENT.keyVariable]) {
    throw new Error(
      `Layer B ${REAL_AGENT.harness} requires ${REAL_AGENT.keyVariable} in the test and harness environments. ` +
        'Set it in services/dev/.env, restart the stack, and run pnpm e2e:agent.',
    );
  }
}

/** SDK adapters expose sandbox tool names with or without the Orca MCP prefix. */
export function canonicalSandboxToolName(name: string): string {
  return REAL_AGENT.isNativeSdk && !name.startsWith('mcp__') ? `mcp__orca__${name}` : name;
}

/** The B.1 SDKs run inside the shared sandbox-harness image and HTTP bridge. */
export const COLOCATED_AGENT = {
  harness: REAL_AGENT.isNativeSdk ? REAL_AGENT.harness : 'claude_code',
  model: {
    ...REAL_AGENT.model,
    id:
      process.env['ORCA_E2E_SANDBOX_HARNESS_MODEL'] ??
      (REAL_AGENT.isNativeSdk ? REAL_AGENT.model.id : 'claude-sonnet-4-6'),
  },
  metadata: {
    harness: REAL_AGENT.isNativeSdk ? REAL_AGENT.harness : 'claude_code',
    mode: 'colocated',
  },
  tools: REAL_AGENT.isNativeSdk
    ? { read: 'read', write: 'write', bash: 'bash' }
    : { read: 'mcp__orca__read', write: 'Write', bash: 'Bash' },
};
