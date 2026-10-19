// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Provider selection for the shared real-model E2E scenarios. */
const harness = process.env['ORCA_E2E_AGENT_HARNESS'] ?? 'claude_agent_sdk';
if (!['claude_agent_sdk', 'codex_sdk', 'pi_sdk', 'dsh_sdk'].includes(harness)) {
  throw new Error(`Unsupported ORCA_E2E_AGENT_HARNESS: ${harness}`);
}

export const REAL_AGENT = {
  harness,
  isNativeSdk: harness === 'codex_sdk' || harness === 'pi_sdk' || harness === 'dsh_sdk',
  model:
    harness === 'codex_sdk'
      ? { provider: 'openai', id: 'gpt-5.4', effort: 'low' }
      : harness === 'dsh_sdk'
        ? { provider: 'deepseek', id: 'deepseek-v4-flash' }
        : {
            provider: 'anthropic',
            id: harness === 'pi_sdk' ? 'claude-sonnet-4-6' : 'claude-sonnet-4-5-20250929',
          },
  // DSH is cloud/colocated only; the other managed SDKs default to separate.
  metadata: harness === 'dsh_sdk' ? { harness, mode: 'colocated' } : { harness },
  keyVariable:
    harness === 'dsh_sdk'
      ? 'DEEPSEEK_API_KEY'
      : harness === 'codex_sdk'
        ? 'OPENAI_API_KEY'
        : 'ANTHROPIC_API_KEY',
};

export function requireRealAgentKey(): void {
  // A pre-provisioned colocated stack keeps the provider key in its Gateway, not the client.
  if (harness === 'dsh_sdk' && process.env['ORCA_BASE_URL'] && process.env['ORCA_API_KEY']) return;
  if (!process.env[REAL_AGENT.keyVariable]) {
    if (harness === 'dsh_sdk')
      throw new Error(
        'DSH live smoke requires a local stack with DEEPSEEK_API_KEY configured in its Gateway, or ORCA_BASE_URL and ORCA_API_KEY for a pre-provisioned DSH stack. Missing prerequisites fail rather than skip.',
      );
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
