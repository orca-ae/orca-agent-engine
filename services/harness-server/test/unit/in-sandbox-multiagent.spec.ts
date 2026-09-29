// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import { InSandboxHarness } from '../../src/harness/in-sandbox/index.js';
import type {
  HarnessChannel,
  HarnessTransport,
  OpenSessionOptions,
  RawSandboxEvent,
} from '../../src/harness/in-sandbox/transport.js';
import type { SessionStartInput } from '../../src/harness/agent-harness.js';

class RecordingTransport implements HarnessTransport {
  opened: OpenSessionOptions | null = null;
  submits = 0;

  async open(opts: OpenSessionOptions): Promise<HarnessChannel> {
    this.opened = opts;
    return {
      async *events(): AsyncIterable<RawSandboxEvent> {
        yield* [];
      },
      submit: async (): Promise<void> => {
        this.submits += 1;
      },
      async stop(): Promise<void> {},
    };
  }
}

describe('InSandboxHarness multi-agent bridge', () => {
  it('passes only always-allow managed built-ins to the sandbox harness', async () => {
    const transport = new RecordingTransport();
    const harness = new InSandboxHarness({
      providerId: 'claude',
      port: 9000,
      transport,
    });
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

    try {
      await harness.start({
        workspaceId: 'wrk_1',
        sessionId: 'ses_1',
        agentSnapshot: {
          system: 'Read only the mounted session resources.',
          allowed_tool_names: ['read', 'bash', 'write', 'delete'],
          tool_permission_policies: {
            'mcp__orca__*': 'always_ask',
            mcp__orca__read: 'always_allow',
            mcp__orca__bash: 'always_deny',
          },
        },
      } satisfies SessionStartInput);

      expect(transport.opened).toMatchObject({
        agent: 'claude',
        systemPrompt: 'Read only the mounted session resources.',
        tools: [],
        allowedTools: ['mcp__orca__read'],
      });
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('"write" requires approval'));
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('"delete" has no'));
    } finally {
      warn.mockRestore();
    }
  });

  it('defaults tools without a managed policy to unavailable', async () => {
    const transport = new RecordingTransport();
    const harness = new InSandboxHarness({
      providerId: 'claude',
      port: 9000,
      transport,
    });
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

    try {
      await harness.start({
        workspaceId: 'wrk_1',
        sessionId: 'ses_1',
        agentSnapshot: { allowed_tool_names: ['write'] },
      } satisfies SessionStartInput);

      expect(transport.opened).toMatchObject({
        agent: 'claude',
        tools: [],
        allowedTools: [],
      });
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('"write" requires approval'));
    } finally {
      warn.mockRestore();
    }
  });

  it('registers child-only read without exposing it to the primary agent', async () => {
    const transport = new RecordingTransport();
    const harness = new InSandboxHarness({
      providerId: 'claude',
      port: 9000,
      transport,
    });

    await harness.start({
      workspaceId: 'wrk_1',
      sessionId: 'ses_1',
      agentSnapshot: {
        model_provider: 'anthropic',
        model_id: 'claude-opus-5',
        model_speed: 'fast',
        model_effort: 'high',
        multiagent: {
          type: 'coordinator',
          agents: [
            {
              id: 'agt_worker',
              name: 'Research Worker',
              version: 3,
              model_provider: 'anthropic',
              model_id: 'claude-opus-4-8',
              model_speed: 'fast',
              model_effort: 'low',
              system:
                'Available Skill: /workspace/skills/research/SKILL.md. Read it before working.',
              allowed_tool_names: ['read'],
              tool_permission_policies: {
                mcp__orca__read: 'always_allow',
              },
            },
          ],
        },
      },
    } satisfies SessionStartInput);

    expect(transport.opened).toMatchObject({
      agent: 'claude',
      model: 'claude-opus-5',
      runtimeTools: ['read'],
      modelSpeed: 'fast',
      modelEffort: 'high',
      forwardSubagentText: true,
      agents: {
        'research-worker': {
          description: 'Managed agent Research Worker (agt_worker v3)',
          prompt: 'Available Skill: /workspace/skills/research/SKILL.md. Read it before working.',
          model: 'claude-opus-4-8',
          modelSpeed: 'fast',
          effort: 'low',
          tools: ['mcp__orca__read'],
        },
      },
    });
    expect(transport.opened?.tools).toBeUndefined();
    expect(transport.opened?.allowedTools).toBeUndefined();
  });

  it('maps child filesystem tools to the same SDK names as the primary agent', async () => {
    const transport = new RecordingTransport();
    const harness = new InSandboxHarness({
      providerId: 'claude',
      port: 9000,
      transport,
    });

    await harness.start({
      workspaceId: 'wrk_1',
      sessionId: 'ses_1',
      agentSnapshot: {
        multiagent: {
          type: 'coordinator',
          agents: [
            {
              id: 'agt_worker',
              name: 'Filesystem Worker',
              version: 1,
              allowed_tool_names: ['read', 'bash', 'write', 'edit', 'glob', 'grep'],
              tool_permission_policies: {
                'mcp__orca__*': 'always_allow',
              },
            },
            {
              id: 'agt_observer',
              name: 'No Tools',
              version: 1,
              allowed_tool_names: [],
            },
          ],
        },
      },
    } satisfies SessionStartInput);

    expect(transport.opened?.agents).toMatchObject({
      'filesystem-worker': {
        tools: ['mcp__orca__read', 'Bash', 'Write', 'Edit', 'Glob', 'Grep'],
      },
      'no-tools': {
        tools: [],
      },
    });
  });

  it('filters each child tool surface with that child own permission policies', async () => {
    const transport = new RecordingTransport();
    const harness = new InSandboxHarness({
      providerId: 'claude',
      port: 9000,
      transport,
    });

    await harness.start({
      workspaceId: 'wrk_1',
      sessionId: 'ses_1',
      agentSnapshot: {
        tool_permission_policies: { 'mcp__orca__*': 'always_deny' },
        multiagent: {
          type: 'coordinator',
          agents: [
            {
              id: 'agt_reader',
              name: 'Reader',
              version: 1,
              allowed_tool_names: ['read', 'write'],
              tool_permission_policies: {
                mcp__orca__read: 'always_allow',
                mcp__orca__write: 'always_deny',
              },
            },
            {
              id: 'agt_writer',
              name: 'Writer',
              version: 1,
              allowed_tool_names: ['read', 'write'],
              tool_permission_policies: {
                mcp__orca__read: 'always_deny',
                mcp__orca__write: 'always_allow',
              },
            },
          ],
        },
      },
    } satisfies SessionStartInput);

    expect(transport.opened?.agents).toMatchObject({
      reader: { tools: ['mcp__orca__read'] },
      writer: { tools: ['Write'] },
    });
  });

  it('applies persistent child guardrail bindings to each child tool surface', async () => {
    const transport = new RecordingTransport();
    const harness = new InSandboxHarness({ providerId: 'claude', port: 9000, transport });

    await harness.start({
      workspaceId: 'wrk_1',
      sessionId: 'ses_1',
      agentSnapshot: {
        multiagent: {
          type: 'coordinator',
          agents: [
            {
              id: 'agt_blocked',
              name: 'Blocked',
              version: 1,
              allowed_tool_names: ['bash'],
              tool_permission_policies: { mcp__orca__bash: 'always_allow' },
            },
            {
              id: 'agt_allowed',
              name: 'Allowed',
              version: 1,
              allowed_tool_names: ['bash'],
              tool_permission_policies: { mcp__orca__bash: 'always_allow' },
            },
          ],
        },
      },
      guardrails: [
        {
          id: 'grd_block_child_shell',
          name: 'Block one child shell',
          tier: 'agent',
          phases: ['tool_call'],
          rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
          stateful: false,
          subagentId: 'agt_blocked',
        },
      ],
    } satisfies SessionStartInput);

    expect(transport.opened?.agents).toMatchObject({
      blocked: { tools: [] },
      allowed: { tools: ['Bash'] },
    });
  });

  it('omits a tool when a matching builtin needs invocation arguments', async () => {
    const transport = new RecordingTransport();
    const harness = new InSandboxHarness({ providerId: 'claude', port: 9000, transport });
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      await harness.start({
        workspaceId: 'wrk_1',
        sessionId: 'ses_1',
        agentSnapshot: {
          allowed_tool_names: ['bash', 'write'],
          tool_permission_policies: { 'mcp__orca__*': 'always_allow' },
        },
        guardrails: [
          {
            id: 'grd_blast_radius',
            name: 'Shell blast radius',
            tier: 'organization',
            phases: ['tool_call'],
            rule: { kind: 'builtin', builtin: 'blast_radius', params: {} },
            stateful: false,
          },
        ],
      } satisfies SessionStartInput);

      expect(transport.opened).toMatchObject({
        tools: ['Write'],
        allowedTools: ['Write'],
      });
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('"bash" requires approval'));
    } finally {
      warn.mockRestore();
    }
  });

  it('short-circuits name checks before marking expression input unavailable', async () => {
    const transport = new RecordingTransport();
    const harness = new InSandboxHarness({ providerId: 'claude', port: 9000, transport });
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      await harness.start({
        workspaceId: 'wrk_1',
        sessionId: 'ses_1',
        agentSnapshot: {
          allowed_tool_names: ['bash', 'write'],
          tool_permission_policies: { 'mcp__orca__*': 'always_allow' },
        },
        guardrails: [
          {
            id: 'grd_command_expression',
            name: 'Command expression',
            tier: 'workspace',
            phases: ['tool_call'],
            rule: {
              kind: 'expression',
              expression: `event.tool.name != 'Bash' || event.tool.input.command == 'pwd'`,
              onFalse: 'deny',
            },
            stateful: false,
          },
        ],
      } satisfies SessionStartInput);

      expect(transport.opened).toMatchObject({
        tools: ['Write'],
        allowedTools: ['Write'],
      });
    } finally {
      warn.mockRestore();
    }
  });

  it('fails closed on request guardrails before submitting to the sandbox harness', async () => {
    const transport = new RecordingTransport();
    const harness = new InSandboxHarness({ providerId: 'claude', port: 9000, transport });
    await harness.start({
      workspaceId: 'wrk_1',
      sessionId: 'ses_1',
      agentSnapshot: {},
      guardrails: [
        {
          id: 'grd_request',
          name: 'Request gate',
          tier: 'workspace',
          phases: ['request'],
          rule: {
            kind: 'expression',
            expression: `event.session.id == 'ses_allowed'`,
            onFalse: 'deny',
            reason: 'This session cannot start a model turn.',
          },
          stateful: false,
        },
      ],
    } satisfies SessionStartInput);

    await expect(
      harness.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'hello' }] },
      }),
    ).rejects.toThrow('This session cannot start a model turn.');
    expect(transport.submits).toBe(0);
  });

  it('rejects mixed coordinator speeds before opening the sandbox session', async () => {
    const transport = new RecordingTransport();
    const harness = new InSandboxHarness({
      providerId: 'claude',
      port: 9000,
      transport,
    });

    await expect(
      harness.start({
        workspaceId: 'wrk_1',
        sessionId: 'ses_1',
        agentSnapshot: {
          model_provider: 'anthropic',
          model_id: 'claude-opus-5',
          model_speed: 'fast',
          multiagent: {
            type: 'coordinator',
            agents: [
              {
                id: 'agt_worker',
                name: 'Worker',
                version: 1,
                model_speed: 'standard',
              },
            ],
          },
        },
      } satisfies SessionStartInput),
    ).rejects.toThrow(/mixed model\.speed values are unsupported/);
    expect(transport.opened).toBeNull();
  });

  it('passes custom tool definitions to the sandbox harness open request', async () => {
    const transport = new RecordingTransport();
    const harness = new InSandboxHarness({
      providerId: 'claude',
      port: 9000,
      transport,
    });

    await harness.start({
      workspaceId: 'wrk_1',
      sessionId: 'ses_1',
      agentSnapshot: {
        custom_tools: [
          {
            name: 'lookup_ticket',
            description: 'Look up a support ticket.',
            input_schema: {
              type: 'object',
              properties: { ticket_id: { type: 'string' } },
              required: ['ticket_id'],
            },
          },
        ],
      },
    } satisfies SessionStartInput);

    expect(transport.opened).toMatchObject({
      agent: 'claude',
      customTools: [
        {
          name: 'lookup_ticket',
          description: 'Look up a support ticket.',
          input_schema: {
            type: 'object',
            properties: { ticket_id: { type: 'string' } },
            required: ['ticket_id'],
          },
        },
      ],
    });
  });
});
