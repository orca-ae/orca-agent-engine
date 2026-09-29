// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { createSdkMcpServer, query, tool } from '@anthropic-ai/claude-agent-sdk';
import { z } from 'zod';

const requests = [];
const agentHookTargets = [];
const runtimeSubagentTypeById = new Map();
const subagentLifecycle = [];
const permissionDecisions = [];
let readHandlerCalls = 0;
const requestedScenario = process.argv[2];
const scenario =
  requestedScenario === 'self' ||
  requestedScenario === 'no-tools' ||
  requestedScenario === 'child-policies'
    ? requestedScenario
    : 'worker';
const configDir = await mkdtemp(join(tmpdir(), 'orca-sdk-tool-boundary-'));
const server = createServer(async (request, response) => {
  let body = '';
  for await (const chunk of request) body += chunk;
  if (!request.url?.includes('/v1/messages')) {
    response.writeHead(404).end();
    return;
  }

  const parsed = JSON.parse(body);
  requests.push(parsed);
  if (scenario === 'child-policies') {
    respondToChildPolicyRequest(requests.length, response);
    return;
  }
  if (requests.length === 1) {
    // SDK 0.3 defaults Agent calls to background execution. Keep this protocol
    // probe foreground so each following request belongs to the delegated child.
    const delegation =
      scenario === 'self'
        ? {
            description: 'delegate to primary',
            prompt: 'Delegate recursively.',
            subagent_type: '__orca_primary',
            run_in_background: false,
          }
        : scenario === 'no-tools'
          ? {
              description: 'observe without tools',
              prompt: 'Respond without using tools.',
              subagent_type: 'observer',
              run_in_background: false,
            }
          : {
              description: 'read child skill',
              prompt: 'Read the Skill entrypoint.',
              subagent_type: 'worker',
              run_in_background: false,
            };
    sendToolUse(response, 'toolu_delegate', 'Agent', {
      ...delegation,
    });
    return;
  }
  sendText(
    response,
    scenario === 'worker' && requests.length === 2 ? 'child complete' : 'primary complete',
  );
});

try {
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('fake API did not bind');

  const orca = createSdkMcpServer({
    name: 'orca',
    version: '0.0.0',
    tools: [
      tool('read', 'read probe', { path: z.string() }, async () => ({
        content: [
          {
            type: 'text',
            text: `read-${String((readHandlerCalls += 1))}`,
          },
        ],
      })),
    ],
  });
  const abortController = new AbortController();
  const stream = query({
    prompt: 'Delegate to the worker.',
    options: {
      model: 'claude-sonnet-4-5-20250929',
      tools: ['Agent'],
      agent: '__orca_primary',
      agents: {
        worker: {
          description: 'Worker with the child Skill.',
          prompt: 'Use the child Skill.',
          tools: ['mcp__orca__read'],
        },
        observer: {
          description: 'Worker with no tools.',
          prompt: 'Respond without using tools.',
          tools: [],
        },
        'allowed-reader': {
          description: 'Worker whose read policy allows execution.',
          prompt: 'Read the file.',
          tools: ['mcp__orca__read'],
        },
        'denied-reader': {
          description: 'Worker whose read policy denies execution.',
          prompt: 'Try to read the file.',
          tools: ['mcp__orca__read'],
        },
        __orca_primary: {
          description: 'Internal primary managed-agent boundary. Never delegate to this agent.',
          prompt: 'PRIMARY_CATALOG_MARKER\n\nTURN_SYSTEM_MARKER',
          tools: ['Agent'],
        },
      },
      mcpServers: { orca },
      strictMcpConfig: true,
      settingSources: [],
      maxTurns: scenario === 'child-policies' ? 10 : 3,
      abortController,
      canUseTool: async (toolName, input, options) => {
        if (toolName === 'Agent') {
          return {
            behavior: 'allow',
            updatedInput: input,
            toolUseID: options.toolUseID,
          };
        }
        const agentType = options.agentID
          ? runtimeSubagentTypeById.get(options.agentID)
          : '__primary';
        const decision = agentType === 'allowed-reader' ? 'allow' : 'deny';
        permissionDecisions.push({
          toolName,
          agentID: options.agentID ?? null,
          agentType: agentType ?? null,
          decision,
        });
        return decision === 'allow'
          ? {
              behavior: 'allow',
              updatedInput: input,
              toolUseID: options.toolUseID,
            }
          : {
              behavior: 'deny',
              message: `denied for ${String(agentType)}`,
              toolUseID: options.toolUseID,
            };
      },
      hooks: {
        PreToolUse: [
          {
            matcher: 'Agent',
            hooks: [
              async (input) => {
                if (input.hook_event_name !== 'PreToolUse' || input.tool_name !== 'Agent') {
                  return {};
                }
                const target = input.tool_input?.subagent_type;
                if (typeof target === 'string') agentHookTargets.push(target);
                if (target !== '__orca_primary') return {};
                return {
                  hookSpecificOutput: {
                    hookEventName: 'PreToolUse',
                    permissionDecision: 'deny',
                    permissionDecisionReason:
                      'The internal primary managed-agent boundary cannot be delegated to.',
                  },
                };
              },
            ],
          },
        ],
        SubagentStart: [
          {
            hooks: [
              async (input) => {
                if (input.hook_event_name === 'SubagentStart') {
                  runtimeSubagentTypeById.set(input.agent_id, input.agent_type);
                  subagentLifecycle.push({
                    event: 'start',
                    agentID: input.agent_id,
                    agentType: input.agent_type,
                  });
                }
                return {};
              },
            ],
          },
        ],
        SubagentStop: [
          {
            hooks: [
              async (input) => {
                if (input.hook_event_name === 'SubagentStop') {
                  subagentLifecycle.push({
                    event: 'stop',
                    agentID: input.agent_id,
                    agentType: input.agent_type,
                  });
                  runtimeSubagentTypeById.delete(input.agent_id);
                }
                return {};
              },
            ],
          },
        ],
      },
      env: {
        ...process.env,
        ANTHROPIC_BASE_URL: `http://127.0.0.1:${address.port}`,
        ANTHROPIC_API_KEY: 'probe-key',
        CLAUDE_CONFIG_DIR: configDir,
        CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1',
      },
    },
  });
  for await (const _message of stream) {
    // Drain the real SDK/CLI stream through primary -> child -> primary.
  }

  if (requests.length < 2) throw new Error(`expected child request, received ${requests.length}`);
  if (scenario === 'child-policies') {
    process.stdout.write(
      JSON.stringify({
        requestTools: requests.map(toolNames),
        subagentLifecycle,
        permissionDecisions,
        readHandlerCalls,
        hasDeniedToolResult: requests.some(hasDeniedToolResult),
      }),
    );
  } else if (scenario === 'self') {
    process.stdout.write(
      JSON.stringify({
        requestTools: requests.map(toolNames),
        agentHookTargets,
        deniedToolResult: hasDeniedToolResult(requests[1]),
      }),
    );
  } else {
    const primarySystem = requestSystemText(requests[0]);
    const childSystem = requestSystemText(requests[1]);
    process.stdout.write(
      JSON.stringify({
        primaryTools: toolNames(requests[0]),
        childTools: toolNames(requests[1]),
        // SDK 0.3 publishes the agent catalog in a user system-reminder rather
        // than embedding it in the Agent tool description.
        primaryRoster: requestMessageText(requests[0]),
        agentHookTargets,
        primaryCatalogCount: occurrenceCount(primarySystem, 'PRIMARY_CATALOG_MARKER'),
        primaryTurnSystemCount: occurrenceCount(primarySystem, 'TURN_SYSTEM_MARKER'),
        childCatalogCount: occurrenceCount(childSystem, 'PRIMARY_CATALOG_MARKER'),
        childTurnSystemCount: occurrenceCount(childSystem, 'TURN_SYSTEM_MARKER'),
      }),
    );
  }
} finally {
  await new Promise((resolve) => server.close(() => resolve()));
  await rm(configDir, { recursive: true, force: true });
}

function toolNames(request) {
  return requestTools(request).map((definition) => String(definition.name));
}

function requestTools(request) {
  return Array.isArray(request.tools)
    ? request.tools.filter((entry) => typeof entry === 'object' && entry !== null)
    : [];
}

function requestSystemText(request) {
  if (typeof request.system === 'string') return request.system;
  return Array.isArray(request.system)
    ? request.system
        .map((block) =>
          typeof block === 'object' && block !== null && typeof block.text === 'string'
            ? block.text
            : '',
        )
        .join('\n')
    : '';
}

function requestMessageText(request) {
  return Array.isArray(request.messages)
    ? request.messages
        .flatMap((message) => (Array.isArray(message?.content) ? message.content : []))
        .map((block) =>
          typeof block === 'object' && block !== null && typeof block.text === 'string'
            ? block.text
            : '',
        )
        .join('\n')
    : '';
}

function hasDeniedToolResult(request) {
  return Array.isArray(request.messages)
    ? request.messages.some(
        (message) =>
          Array.isArray(message.content) &&
          message.content.some((block) => block?.type === 'tool_result' && block.is_error === true),
      )
    : false;
}

function occurrenceCount(value, marker) {
  return value.split(marker).length - 1;
}

function respondToChildPolicyRequest(requestNumber, response) {
  // Foreground delegation keeps the fake provider's request sequence stable.
  if (requestNumber === 1) {
    sendToolUse(response, 'toolu_delegate_allowed', 'Agent', {
      description: 'delegate to allowed reader',
      prompt: 'Read the file.',
      subagent_type: 'allowed-reader',
      run_in_background: false,
    });
    return;
  }
  if (requestNumber === 2) {
    sendToolUse(response, 'toolu_allowed_read', 'mcp__orca__read', {
      path: '/workspace/allowed.txt',
    });
    return;
  }
  if (requestNumber === 3) {
    sendText(response, 'allowed reader complete');
    return;
  }
  if (requestNumber === 4) {
    sendToolUse(response, 'toolu_delegate_denied', 'Agent', {
      description: 'delegate to denied reader',
      prompt: 'Try to read the file.',
      subagent_type: 'denied-reader',
      run_in_background: false,
    });
    return;
  }
  if (requestNumber === 5) {
    sendToolUse(response, 'toolu_denied_read', 'mcp__orca__read', {
      path: '/workspace/denied.txt',
    });
    return;
  }
  sendText(response, requestNumber === 6 ? 'denied reader handled denial' : 'primary complete');
}

function sendToolUse(response, id, name, input) {
  sendSse(response, [
    messageStart(),
    [
      'content_block_start',
      {
        type: 'content_block_start',
        index: 0,
        content_block: { type: 'tool_use', id, name, input: {} },
      },
    ],
    [
      'content_block_delta',
      {
        type: 'content_block_delta',
        index: 0,
        delta: { type: 'input_json_delta', partial_json: JSON.stringify(input) },
      },
    ],
    ['content_block_stop', { type: 'content_block_stop', index: 0 }],
    [
      'message_delta',
      {
        type: 'message_delta',
        delta: { stop_reason: 'tool_use', stop_sequence: null },
        usage: { output_tokens: 10 },
      },
    ],
    ['message_stop', { type: 'message_stop' }],
  ]);
}

function sendText(response, text) {
  sendSse(response, [
    messageStart(),
    [
      'content_block_start',
      {
        type: 'content_block_start',
        index: 0,
        content_block: { type: 'text', text: '' },
      },
    ],
    [
      'content_block_delta',
      {
        type: 'content_block_delta',
        index: 0,
        delta: { type: 'text_delta', text },
      },
    ],
    ['content_block_stop', { type: 'content_block_stop', index: 0 }],
    [
      'message_delta',
      {
        type: 'message_delta',
        delta: { stop_reason: 'end_turn', stop_sequence: null },
        usage: { output_tokens: 5 },
      },
    ],
    ['message_stop', { type: 'message_stop' }],
  ]);
}

function messageStart() {
  return [
    'message_start',
    {
      type: 'message_start',
      message: {
        id: `msg_${randomUUID()}`,
        type: 'message',
        role: 'assistant',
        model: 'claude-sonnet-4-5-20250929',
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: 1, output_tokens: 0 },
      },
    },
  ];
}

function sendSse(response, events) {
  response.writeHead(200, {
    'content-type': 'text/event-stream',
    'cache-control': 'no-cache',
  });
  for (const [event, data] of events) {
    response.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  }
  response.end();
}
