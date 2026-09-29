// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer } from 'node:http';
import { pngBase64 } from '../../../../packages/codex-harness/test/support/custom-tool-results.js';
import { PendingCustomToolResults } from '../../src/pending-custom-tool-results.js';
import type { CustomToolResult } from '../../src/custom-tools.js';
import { describe, it, expect, vi } from 'vitest';
import { createHash } from 'node:crypto';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import { CodexSdkHarness } from '../../src/harness/codex-sdk/index.js';
import type {
  AgentEvent,
  ToolConfirmer,
  ToolPermissionResolver,
} from '../../src/harness/agent-harness.js';
import { scriptedNativeCliWithDeath } from './support/failed-native-cli.js';

async function gatedTurn(
  gate: ToolConfirmer,
  allowed: string[] = ['write'],
  permissions?: ToolPermissionResolver,
) {
  const sandbox = await new InMemorySandboxRuntime().acquire({});
  const native = scriptedNativeCliWithDeath({
    respond: (line) => {
      const command = JSON.parse(line);
      if (command.type === 'start') return [JSON.stringify({ type: 'ready' })];
      if (command.type === 'submit')
        return [
          JSON.stringify({
            type: 'tool_call',
            id: 'call1',
            name: 'write',
            arguments: { path: 'proof.txt', content: 'proof' },
          }),
        ];
      if (command.type === 'tool_result') return [JSON.stringify({ type: 'done' })];
      return [];
    },
  });
  const harness = new CodexSdkHarness({
    apiKey: 'test',
    launch: () => native.cli,
    timeoutMs: 1000,
  });
  const events: AgentEvent[] = [];
  const drain = (async () => {
    for await (const event of harness.events()) events.push(event);
  })();
  try {
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      sandbox,
      agentSnapshot: { model_provider: 'openai', model_id: 'gpt-5.4', allowed_tool_names: allowed },
      confirmTool: gate,
      ...(permissions ? { toolPermissions: permissions } : {}),
    });
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'write' }] },
    });
    let content: string | undefined;
    try {
      content = (await sandbox.files.read('proof.txt')).toString();
    } catch {
      /* denied write */
    }
    return { events, frames: native.frames, content };
  } finally {
    await harness.stop('client.archived');
    await drain;
    await sandbox.destroy();
  }
}

describe('Codex SDK tool relay', () => {
  it.each([
    { serverName: 'remote', outcome: 'allow' },
    { serverName: 'remote', outcome: 'deny' },
    { serverName: 'remote', outcome: 'error' },
    { serverName: 'orca', outcome: 'allow' },
  ])(
    'preserves MCP event identity for $serverName tools ($outcome)',
    async ({ serverName, outcome }) => {
      let calls = 0;
      const server = createServer(async (req, res) => {
        if (req.method !== 'POST') {
          res.writeHead(405).end();
          return;
        }
        const chunks: Buffer[] = [];
        for await (const chunk of req) chunks.push(Buffer.from(chunk));
        const rpc = JSON.parse(Buffer.concat(chunks).toString());
        if (rpc.id === undefined) {
          res.writeHead(202).end();
          return;
        }
        let result: unknown;
        if (rpc.method === 'initialize')
          result = {
            protocolVersion: rpc.params.protocolVersion,
            capabilities: { tools: {} },
            serverInfo: { name: 'fixture', version: '1' },
          };
        else if (rpc.method === 'tools/list')
          result = { tools: [{ name: 'echo', inputSchema: { type: 'object' } }] };
        else if (rpc.method === 'tools/call') {
          calls++;
          result = {
            content: [{ type: 'text', text: outcome === 'error' ? 'upstream failed' : 'hello' }],
            isError: outcome === 'error',
          };
        } else {
          res.writeHead(400).end();
          return;
        }
        res
          .writeHead(200, { 'content-type': 'application/json' })
          .end(JSON.stringify({ jsonrpc: '2.0', id: rpc.id, result }));
      });
      await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
      const address = server.address();
      if (!address || typeof address === 'string') throw new Error('missing MCP address');
      const name = `mcp__${serverName}__echo`;
      const sandbox = await new InMemorySandboxRuntime().acquire({});
      const native = scriptedNativeCliWithDeath({
        respond: (line) => {
          const command = JSON.parse(line);
          if (command.type === 'start') return [JSON.stringify({ type: 'ready' })];
          if (command.type === 'submit')
            return [
              JSON.stringify({
                type: 'tool_call',
                id: 'private-worker-id',
                name,
                arguments: { text: 'hello' },
              }),
            ];
          if (command.type === 'tool_result') return [JSON.stringify({ type: 'done' })];
          return [];
        },
      });
      const harness = new CodexSdkHarness({
        apiKey: 'test',
        launch: () => native.cli,
        timeoutMs: 1000,
      });
      const events: AgentEvent[] = [];
      const drain = (async () => {
        for await (const event of harness.events()) events.push(event);
      })();
      try {
        await harness.start({
          workspaceId: 'ws',
          sessionId: 'ses_sdk',
          sandbox,
          agentSnapshot: { model_provider: 'openai', model_id: 'gpt-5.4', allowed_tool_names: [] },
          remoteMcpToolsets: [{ serverName }],
          mcpServers: {
            [serverName]: {
              type: 'http',
              url: `http://127.0.0.1:${address.port}/mcp`,
              headers: {},
            },
          },
          toolPermissions: {
            policyFor: () => (outcome === 'deny' ? 'always_deny' : 'always_allow'),
          },
        });
        await harness.submit({ kind: 'user.message', payload: { content: 'invoke MCP' } });
        await harness.stop('client.archived');
        await drain;
        const use = events.find((event) => event.kind === 'agent.mcp_tool_use')!;
        expect(use).toBeDefined();
        expect(use.payload).toMatchObject({
          name: 'echo',
          mcp_server_name: serverName,
          tool_use_id: use.id,
          input: { text: 'hello' },
        });
        expect(
          events.find((event) => event.kind === 'agent.mcp_tool_result')?.payload,
        ).toMatchObject({
          mcp_tool_use_id: use.id,
          mcp_server_name: serverName,
          is_error: outcome !== 'allow',
        });
        expect(
          events.some(
            (event) => event.kind === 'agent.tool_use' || event.kind === 'agent.tool_result',
          ),
        ).toBe(false);
        expect(calls).toBe(outcome === 'deny' ? 0 : 1);
        expect(
          native.frames
            .map((line) => JSON.parse(line))
            .find((frame) => frame.type === 'tool_result'),
        ).toMatchObject({ id: 'private-worker-id' });
      } finally {
        await harness.stop('client.archived');
        await drain;
        await sandbox.destroy();
        server.closeAllConnections();
        await new Promise<void>((resolve) => server.close(() => resolve()));
      }
    },
  );

  it('refuses malformed raw usage before normalized usage or a following checkpoint escapes', async () => {
    const sandbox = await new InMemorySandboxRuntime().acquire({});
    const native = scriptedNativeCliWithDeath({
      respond: (line) => {
        const command = JSON.parse(line);
        if (command.type === 'start') return [JSON.stringify({ type: 'ready' })];
        if (command.type === 'submit')
          return [
            JSON.stringify({
              type: 'event',
              event: {
                type: 'turn.completed',
                usage: { input_tokens: 1, cached_input_tokens: 2, output_tokens: 0 },
              },
            }),
            JSON.stringify({
              type: 'checkpoint',
              checkpoint: { version: 1, threadId: 'thread', files: {} },
            }),
            JSON.stringify({ type: 'done' }),
          ];
        return [];
      },
    });
    const harness = new CodexSdkHarness({
      apiKey: 'test',
      launch: () => native.cli,
      timeoutMs: 1000,
    });
    const events: AgentEvent[] = [];
    const drain = (async () => {
      for await (const event of harness.events()) events.push(event);
    })();
    try {
      await harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        sandbox,
        agentSnapshot: { model_provider: 'openai', model_id: 'gpt-5.4', allowed_tool_names: [] },
      });
      await harness.submit({
        kind: 'user.message',
        payload: { content: 'invalid terminal usage' },
      });
      await harness.stop('client.archived');
      await drain;
      expect(events.some((event) => event.kind === 'agent.error')).toBe(true);
      expect(
        events.some(
          (event) => event.kind === 'agent.usage' || event.kind === 'orca.harness_checkpoint',
        ),
      ).toBe(false);
    } finally {
      await harness.stop('client.archived');
      await drain;
      await sandbox.destroy();
    }
  });

  it('rejects changed checkpoint instructions before launching the worker', async () => {
    const sandbox = await new InMemorySandboxRuntime().acquire({});
    const launch = vi.fn();
    const harness = new CodexSdkHarness({
      apiKey: 'test',
      launch,
      checkpoint: {
        version: 1,
        threadId: 'thread',
        files: { 'sessions/2026/09/20/rollout-thread.jsonl': 'e30=' },
        instructionsSha256: createHash('sha256').update('original').digest('hex'),
      },
    });
    try {
      await expect(
        harness.start({
          workspaceId: 'ws_test',
          sessionId: 'ses_test',
          sandbox,
          agentSnapshot: {
            model_provider: 'openai',
            model_id: 'gpt-5.4',
            system: 'changed',
            allowed_tool_names: [],
          },
        }),
      ).rejects.toThrow('checkpoint developer instructions differ');
      expect(launch).not.toHaveBeenCalled();
    } finally {
      await harness.stop('client.archived');
      await sandbox.destroy();
    }
  });

  it.each([true, false])('preserves fatal=%s across turn boundaries', async (fatal) => {
    const sandbox = await new InMemorySandboxRuntime().acquire({});
    let turns = 0;
    const native = scriptedNativeCliWithDeath({
      respond: (line) => {
        const command = JSON.parse(line);
        if (command.type === 'start') return [JSON.stringify({ type: 'ready' })];
        if (command.type === 'submit') {
          turns++;
          return [
            JSON.stringify({
              type: 'failure',
              message: fatal ? 'native checkpoint failed' : 'model request failed',
              ...(fatal ? { fatal: true } : {}),
            }),
            JSON.stringify({ type: 'done' }),
          ];
        }
        return [];
      },
    });
    const harness = new CodexSdkHarness({
      apiKey: 'test',
      launch: () => native.cli,
      timeoutMs: 1000,
    });
    const events: AgentEvent[] = [];
    const drain = (async () => {
      for await (const event of harness.events()) events.push(event);
    })();
    try {
      await harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        sandbox,
        agentSnapshot: { model_provider: 'openai', model_id: 'gpt-5.4', allowed_tool_names: [] },
      });
      const message = { kind: 'user.message', payload: { content: 'try a turn' } };
      await harness.submit(message);
      if (fatal) {
        await expect(harness.submit(message)).rejects.toThrow('unavailable');
        expect(turns).toBe(1);
      } else {
        await harness.submit(message);
        expect(turns).toBe(2);
      }
      await harness.stop('client.archived');
      await drain;
      expect(events.filter((event) => event.kind === 'agent.error')).toHaveLength(fatal ? 1 : 2);
      expect(events.filter((event) => event.kind === 'agent.turn_completed')).toHaveLength(
        fatal ? 1 : 2,
      );
    } finally {
      await harness.stop('client.archived');
      await drain;
      await sandbox.destroy();
    }
  });

  it('executes only after approval and preserves confirmation identities', async () => {
    const result = await gatedTurn(async (_, input) => ({
      behavior: 'allow',
      updatedInput: input,
    }));
    expect(result.content).toBe('proof');
    const action = result.events.find((event) => event.kind === 'agent.requires_action')!
      .payload as { tool_use_id: string };
    expect(
      result.events.find((event) => event.kind === 'agent.tool_result')?.payload,
    ).toMatchObject({ tool_use_id: action.tool_use_id, is_error: false });
    expect(result.events.filter((event) => event.kind === 'agent.turn_completed')).toHaveLength(1);
  });
  it('fails closed for an unexpected policy instead of converting it to approval', async () => {
    const gate = vi.fn();
    const result = await gatedTurn(gate, ['write'], {
      policyFor: () => 'always_deny' as 'always_ask',
    });
    expect(gate).not.toHaveBeenCalled();
    expect(result.content).toBeUndefined();
    expect(result.events.some((e) => e.kind === 'agent.requires_action')).toBe(false);
    expect(result.events.find((e) => e.kind === 'agent.tool_result')?.payload).toMatchObject({
      is_error: true,
    });
  });

  it('denies a failed approval gate without executing the tool', async () => {
    const result = await gatedTurn(async () => {
      throw new Error('approval unavailable');
    });
    expect(result.content).toBeUndefined();
    expect(
      result.events.find((event) => event.kind === 'agent.tool_result')?.payload,
    ).toMatchObject({ is_error: true });
    expect(result.events.filter((event) => event.kind === 'agent.turn_completed')).toHaveLength(1);
  });
  it('honors an empty allowlist even if a child requests a tool', async () => {
    const result = await gatedTurn(
      async (_, input) => ({ behavior: 'allow', updatedInput: input }),
      [],
    );
    expect(result.content).toBeUndefined();
    expect(result.events.some((event) => event.kind === 'agent.requires_action')).toBe(false);
  });
});

describe('Codex SDK client callback', () => {
  it.each([false, true])(
    'uses one public identity and preserves blocks/is_error=%s without approval',
    async (isError) => {
      const sandbox = await new InMemorySandboxRuntime().acquire({});
      const table = new PendingCustomToolResults();
      const content: CustomToolResult['content'] = [
        { type: 'text', text: 'ticket result' },
        { type: 'image', source: { type: 'base64', media_type: 'image/png', data: pngBase64 } },
        { type: 'document', title: 'Ticket', source: { type: 'text', data: 'document body' } },
        { type: 'search_result', title: 'Ticket result', content: [{ type: 'text', text: 'hit' }] },
      ];
      const native = scriptedNativeCliWithDeath({
        respond: (line) => {
          const command = JSON.parse(line);
          if (command.type === 'start') return [JSON.stringify({ type: 'ready' })];
          if (command.type === 'submit')
            return [
              JSON.stringify({
                type: 'tool_call',
                id: 'worker-private-id',
                name: 'lookup_ticket',
                arguments: { id: 'T1' },
              }),
            ];
          if (command.type === 'tool_result') return [JSON.stringify({ type: 'done' })];
          return [];
        },
      });
      const harness = new CodexSdkHarness({
        apiKey: 'test',
        launch: () => native.cli,
        timeoutMs: 1000,
      });
      const events: AgentEvent[] = [];
      const confirm = vi.fn();
      const drain = (async () => {
        for await (const event of harness.events()) {
          events.push(event);
          if (event.kind === 'agent.custom_tool_use') {
            expect(table.hasPending()).toBe(true);
            expect(event.id).toMatch(/^evt_/);
            expect(event.payload).toEqual({
              id: event.id,
              name: 'lookup_ticket',
              input: { id: 'T1' },
            });
            expect(
              table.resolve({
                type: 'user.custom_tool_result',
                custom_tool_use_id: event.id!,
                content,
                is_error: isError,
              }),
            ).toBe(true);
          }
        }
      })();
      try {
        await harness.start({
          workspaceId: 'ws',
          sessionId: 'ses',
          sandbox,
          agentSnapshot: {
            model_provider: 'openai',
            model_id: 'gpt-5.4',
            allowed_tool_names: ['lookup_ticket'],
            custom_tools: [
              {
                name: 'lookup_ticket',
                description: 'Ticket lookup',
                input_schema: {
                  type: 'object',
                  properties: { id: { type: 'string' } },
                  required: ['id'],
                },
              },
            ],
          },
          toolPermissions: { policyFor: () => 'always_ask' },
          confirmTool: confirm,
          awaitCustomToolResult: (id, signal) => table.park(id, signal),
        });
        await harness.submit({ kind: 'user.message', payload: { content: 'Find ticket' } });
        await harness.stop('client.archived');
        await drain;
        expect(confirm).not.toHaveBeenCalled();
        expect(events.filter((e) => e.kind === 'agent.custom_tool_use')).toHaveLength(1);
        expect(
          events.filter((e) =>
            ['agent.tool_use', 'agent.tool_result', 'agent.requires_action'].includes(e.kind),
          ),
        ).toEqual([]);
        const frames = native.frames.map((frame) => JSON.parse(frame));
        expect(frames.find((f) => f.type === 'start').tools).toEqual([
          {
            name: 'lookup_ticket',
            description: 'Ticket lookup',
            inputSchema: {
              type: 'object',
              properties: { id: { type: 'string' } },
              required: ['id'],
            },
          },
        ]);
        expect(frames.find((f) => f.type === 'tool_result')).toEqual({
          type: 'tool_result',
          id: 'worker-private-id',
          result: {
            content: [
              content[0],
              { type: 'image', data: pngBase64, mimeType: 'image/png' },
              { type: 'text', text: JSON.stringify(content[2]) },
              { type: 'text', text: JSON.stringify(content[3]) },
            ],
            isError,
          },
        });
        const custom = events.find((e) => e.kind === 'agent.custom_tool_use')!;
        expect(events).toContainEqual({
          kind: 'session.status_idle',
          payload: { stop_reason: { type: 'requires_action', event_ids: [custom.id] } },
        });
        expect(events.filter((e) => e.kind === 'session.status_running')).toHaveLength(2);
        expect(events.at(-2)).toEqual({
          kind: 'session.status_idle',
          payload: { stop_reason: { type: 'end_turn' } },
        });
        expect(table.hasPending()).toBe(false);
      } finally {
        await harness.stop('client.archived');
        await drain;
        await sandbox.destroy();
      }
    },
  );

  it.each(['interrupt', 'worker exit', 'unsupported result'] as const)(
    'releases a parked callback on %s',
    async (termination) => {
      const sandbox = await new InMemorySandboxRuntime().acquire({});
      const pending = new PendingCustomToolResults();
      const native = scriptedNativeCliWithDeath({
        respond: (line) => {
          const command = JSON.parse(line);
          if (command.type === 'start') return [JSON.stringify({ type: 'ready' })];
          if (command.type === 'submit')
            return [
              JSON.stringify({
                type: 'tool_call',
                id: 'worker',
                name: 'lookup_ticket',
                arguments: {},
              }),
            ];
          if (command.type === 'interrupt') return [JSON.stringify({ type: 'done' })];
          return [];
        },
      });
      const harness = new CodexSdkHarness({
        apiKey: 'test',
        launch: () => native.cli,
        timeoutMs: 1000,
      });
      const events: AgentEvent[] = [];
      const drain = (async () => {
        for await (const event of harness.events()) {
          events.push(event);
          if (event.kind === 'agent.custom_tool_use') {
            if (termination === 'interrupt') harness.interrupt();
            else if (termination === 'worker exit') native.cli.kill();
            else
              pending.resolve({
                type: 'user.custom_tool_result',
                custom_tool_use_id: event.id!,
                content: [
                  { type: 'image', source: { type: 'url', url: 'https://example.test/image.png' } },
                ],
              });
          }
        }
      })();
      try {
        await harness.start({
          workspaceId: 'ws',
          sessionId: 'ses',
          sandbox,
          agentSnapshot: {
            model_provider: 'openai',
            model_id: 'gpt-5.4',
            allowed_tool_names: ['lookup_ticket'],
            custom_tools: [
              {
                name: 'lookup_ticket',
                description: 'Ticket lookup',
                input_schema: { type: 'object' },
              },
            ],
          },
          awaitCustomToolResult: (id, signal) => pending.park(id, signal),
        });
        await harness.submit({ kind: 'user.message', payload: { content: 'Find ticket' } });
        await harness.stop('client.archived');
        await drain;
        if (termination === 'unsupported result') {
          expect(events.find((event) => event.kind === 'agent.error')?.payload).toMatchObject({
            message: expect.stringContaining("image source 'url' is not supported"),
          });
          expect(events.filter((event) => event.kind === 'agent.turn_completed')).toHaveLength(1);
        }
        expect(pending.hasPending()).toBe(false);
        expect(
          native.frames.map((f) => JSON.parse(f)).filter((f) => f.type === 'tool_result'),
        ).toEqual([]);
      } finally {
        await harness.stop('client.archived');
        await drain;
        await sandbox.destroy();
      }
    },
  );
});
