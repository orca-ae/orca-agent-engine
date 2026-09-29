// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { mkdtemp, mkdir, readFile, readdir, realpath, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, relative, sep } from 'node:path';
import { randomUUID } from 'node:crypto';
import { walkSdkConfig } from './walk-sdk-config.mjs';

await main();

async function main() {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'orca-sdk-provenance-')));
  const config = join(root, 'config');
  await mkdir(config);
  // Sanitize the SDK parent as well as the child; SessionStore resume allocates
  // its final config using the parent's TMPDIR, not options.env.TMPDIR.
  const path = process.env.PATH;
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, {
    PATH: path,
    HOME: root,
    TMPDIR: root,
    CLAUDE_CONFIG_DIR: config,
    ANTHROPIC_API_KEY: 'probe-key',
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1',
  });
  const { query, InMemorySessionStore } = await import('@anthropic-ai/claude-agent-sdk');
  const sdkVersion = JSON.parse(
    await readFile(
      new URL('package.json', import.meta.resolve('@anthropic-ai/claude-agent-sdk')),
      'utf8',
    ),
  ).version;
  assert.equal(sdkVersion, '0.3.283');
  const payload = 'x'.repeat(156186);
  const store = new InMemorySessionStore();
  const sessionId = randomUUID();
  const phases = [];
  let active;
  let stream;
  const children = [];
  const abortController = new globalThis.AbortController();
  const watchdog = setTimeout(() => abortController.abort(), 45000);
  const server = createServer(async (req, res) => {
    try {
      let raw = '';
      for await (const chunk of req) {
        raw += chunk;
        assert.ok(raw.length < 2000000);
      }
      const body = raw ? JSON.parse(raw) : {};
      if (req.url?.startsWith('/v1/messages/count_tokens')) {
        const large = JSON.stringify(body.messages).includes(payload);
        active.counts.push(large ? 30000 : 20);
        res
          .writeHead(200, { 'content-type': 'application/json' })
          .end(JSON.stringify({ input_tokens: large ? 30000 : 20 }));
        return;
      }
      if (req.url?.startsWith('/v1/messages')) {
        assert.ok(active.wire.length < 4, 'bounded model request count');
        active.wire.push(body);
        const step = active.wire.length;
        send(
          res,
          step < 4
            ? {
                type: 'tool_use',
                id: 'toolu_' + active.phase + '_' + step,
                name: 'mcp__remote__result',
                input: { call: ['A', 'B', 'C'][step - 1] },
              }
            : { type: 'text', text: 'complete' },
        );
        return;
      }
      if (req.url !== '/mcp') {
        active.unexpectedRequest = true;
        res.writeHead(403).end();
        return;
      }
      if (req.method === 'GET') {
        res.writeHead(405).end();
        return;
      }
      if (req.method === 'DELETE' || body.id === undefined) {
        res.writeHead(202).end();
        return;
      }
      let result = {};
      if (body.method === 'initialize')
        result = {
          protocolVersion: body.params.protocolVersion,
          capabilities: { tools: {} },
          serverInfo: { name: 'provenance-probe', version: '1' },
        };
      if (body.method === 'tools/list')
        result = {
          tools: [
            {
              name: 'result',
              description: 'Return test text',
              inputSchema: {
                type: 'object',
                properties: { call: { type: 'string' } },
                required: ['call'],
              },
            },
          ],
        };
      if (body.method === 'tools/call') {
        active.calls.push(body.params.arguments.call);
        // Same MCP tool/server for all calls: only caller arguments differ.
        // B exercises the SDK's accepted legacy result union; C retains the
        // ordinary text-block control. Both replay A's observed notice; neither
        // discovers an unknown path or causes a new SDK spill.
        assert.ok(body.params.arguments.call === 'A' || active.notice);
        result =
          body.params.arguments.call === 'B'
            ? { toolResult: active.notice }
            : {
                content: [
                  {
                    type: 'text',
                    text: body.params.arguments.call === 'A' ? payload : active.notice,
                  },
                ],
              };
      }
      res
        .writeHead(200, { 'content-type': 'application/json', 'mcp-session-id': 'probe-session' })
        .end(JSON.stringify({ jsonrpc: '2.0', id: body.id, result }));
    } catch {
      active.serverError = true;
      res.writeHead(500).end();
    }
  });
  try {
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const base = 'http://127.0.0.1:' + server.address().port;
    // The native CLI does not execute NODE_OPTIONS preloads. Send all HTTP
    // proxy traffic to this deny-only local listener instead of claiming a JS
    // socket monkey-patch can isolate the native executable. On macOS also
    // enforce the loopback boundary with the OS sandbox at spawn.
    Object.assign(process.env, {
      ANTHROPIC_BASE_URL: base,
      HTTP_PROXY: base,
      HTTPS_PROXY: base,
      ALL_PROXY: base,
      NO_PROXY: '127.0.0.1,localhost,::1',
    });
    for (const phase of ['fresh', 'resume']) {
      active = {
        phase,
        wire: [],
        counts: [],
        calls: [],
        hooks: [],
        lifecycle: [],
        native: [],
        control: [],
        results: [],
      };
      const current = active;
      stream = query({
        prompt: 'Run A then B then C.',
        options: {
          ...(phase === 'fresh' ? { sessionId } : { resume: sessionId }),
          sessionStore: store,
          model: 'claude-sonnet-4-5-20250929',
          cwd: root,
          tools: [],
          settingSources: [],
          strictMcpConfig: true,
          mcpServers: { remote: { type: 'http', url: base + '/mcp' } },
          maxTurns: 4,
          abortController,
          canUseTool: async (_name, input) => ({ behavior: 'allow', updatedInput: input }),
          env: {
            ...process.env,
            CLAUDE_CODE_USE_BEDROCK: '0',
            CLAUDE_CODE_USE_VERTEX: '0',
            CLAUDE_CODE_USE_FOUNDRY: '0',
          },
          spawnClaudeCodeProcess(options) {
            current.config = options.env.CLAUDE_CONFIG_DIR;
            assert.ok(within(root, current.config));
            const sandboxed = process.platform === 'darwin';
            const command = sandboxed ? '/usr/bin/sandbox-exec' : options.command;
            const args = sandboxed
              ? [
                  '-p',
                  '(version 1)(allow default)(deny network*)(allow network-outbound (remote ip "localhost:*"))(allow network-inbound (local ip "localhost:*"))',
                  options.command,
                  ...options.args,
                ]
              : options.args;
            const child = spawn(command, args, {
              cwd: options.cwd,
              env: options.env,
              stdio: ['pipe', 'pipe', 'pipe'],
              signal: options.signal,
            });
            children.push(child);
            // Observe raw control/native schemas before the SDK consumes them;
            // never emit complete frames, environment, notice paths, or payload.
            let pending = '';
            child.stdout.on('data', (chunk) => {
              pending += chunk;
              let end;
              while ((end = pending.indexOf('\n')) >= 0) {
                const line = pending.slice(0, end);
                pending = pending.slice(end + 1);
                try {
                  const frame = JSON.parse(line);
                  if (
                    frame.type === 'control_request' &&
                    frame.request?.subtype === 'hook_callback'
                  )
                    current.control.push({
                      envelopeKeys: Object.keys(frame).sort(),
                      keys: Object.keys(frame.request).sort(),
                      inputKeys: Object.keys(frame.request.input ?? {}).sort(),
                      callbackId: frame.request.callback_id,
                      toolUseId: frame.request.tool_use_id,
                      input: frame.request.input,
                    });
                } catch {
                  /* Ignore non-JSON CLI diagnostics. */
                }
              }
            });
            child.stderr.resume();
            return child;
          },
          hooks: {
            SessionStart: [
              {
                hooks: [
                  async (input) => {
                    current.lifecycle.push(input);
                    return {};
                  },
                ],
              },
            ],
            PostToolUse: [
              {
                matcher: 'mcp__remote__result',
                timeout: 10,
                hooks: [
                  async (input, toolUseId, context) => {
                    current.hooks.push({
                      input,
                      toolUseId,
                      contextKeys: Object.keys(context).sort(),
                    });
                    if (input.tool_input.call === 'A') {
                      // Deliberately share A's observed notice with the fake MCP
                      // server to model replay, not synthesis without observation.
                      current.notice = input.tool_response;
                      // Discover ONLY fixture-owned files, never open a path supplied by
                      // a tool notice. This is evidence collection, not a bridge reader.
                      const files = await walkSdkConfig(current.config).catch((error) => {
                        // The SDK catches hook exceptions. Preserve discovery failures
                        // for the parent oracle instead of a later undefined-path error.
                        current.discoveryError = error;
                        throw error;
                      });
                      const spills = files.filter((file) =>
                        file.includes(sep + 'tool-results' + sep),
                      );
                      assert.equal(spills.length, 1);
                      current.spill = spills[0];
                      current.bytes = await readFile(current.spill);
                    }
                    return {
                      hookSpecificOutput: {
                        hookEventName: 'PostToolUse',
                        updatedToolOutput: [
                          { type: 'text', text: 'PROBE_REPLACED_' + input.tool_input.call },
                        ],
                      },
                    };
                  },
                ],
              },
            ],
          },
        },
      });
      for await (const message of stream) {
        if (message.type === 'user') current.native.push(message);
        if (message.type === 'result') current.results.push(message.subtype);
      }
      assert.equal(current.serverError, undefined);
      assert.equal(current.unexpectedRequest, undefined);
      if (current.discoveryError) throw current.discoveryError;
      assert.equal(current.hooks.length, 3);
      const [a, b, c] = current.hooks;
      assert.equal(a.input.session_id, sessionId);
      assert.equal(b.input.session_id, sessionId);
      assert.equal(a.input.transcript_path, b.input.transcript_path);
      assert.ok(within(current.config, a.input.transcript_path));
      assert.ok(within(join(a.input.transcript_path.slice(0, -6), 'tool-results'), current.spill));
      assert.ok(JSON.stringify(a.input.tool_response).includes(current.spill));
      assert.equal(typeof a.input.tool_response, 'string');
      assert.ok(
        b.input.tool_response === a.input.tool_response,
        'legacy forgery is the exact same primitive string',
      );
      assert.ok(Array.isArray(c.input.tool_response));
      assert.ok(c.input.tool_response[0].text === a.input.tool_response);
      const differingHookFields = Object.keys(a.input)
        .filter((key) => JSON.stringify(a.input[key]) !== JSON.stringify(b.input[key]))
        .sort();
      assert.ok(
        differingHookFields.every((key) =>
          ['duration_ms', 'tool_input', 'tool_use_id'].includes(key),
        ),
      );
      assert.ok(differingHookFields.includes('tool_input'));
      assert.ok(differingHookFields.includes('tool_use_id'));
      assert.equal(a.input.tool_name, b.input.tool_name);
      assert.deepEqual(
        current.hooks.map(({ input }) => input.mcp_server),
        Array.from({ length: 3 }, () => ({ name: 'remote', source: 'dynamic' })),
      );
      assert.equal(current.control.length, 3);
      assert.equal(current.control[0].callbackId, current.control[1].callbackId);
      for (let index = 0; index < current.hooks.length; index++) {
        assert.equal(current.control[index].toolUseId, current.hooks[index].input.tool_use_id);
        assert.ok(
          JSON.stringify(current.control[index].input) ===
            JSON.stringify(current.hooks[index].input),
        );
        assert.deepEqual(current.hooks[index].contextKeys, ['signal']);
      }
      assert.notEqual(a.input.tool_use_id, b.input.tool_use_id);
      assert.equal(a.toolUseId, a.input.tool_use_id);
      assert.equal(b.toolUseId, b.input.tool_use_id);
      assert.ok(current.counts.includes(30000));
      assert.deepEqual(current.calls, ['A', 'B', 'C']);
      assert.ok(
        current.bytes.equals(Buffer.from(payload)),
        'spill preserves all 156186 bytes exactly',
      );
      assert.ok((await readFile(current.spill)).equals(current.bytes), 'B must not change A file');
      assert.equal(
        (await readdir(dirname(current.spill))).length,
        1,
        'forged results must not create another SDK spill',
      );
      for (const [index, call] of [
        [1, 'A'],
        [2, 'B'],
        [3, 'C'],
      ]) {
        const blocks = current.wire[index].messages.flatMap((message) =>
          Array.isArray(message.content) ? message.content : [],
        );
        const result = blocks.find(
          (block) =>
            block.type === 'tool_result' &&
            block.tool_use_id === current.hooks[index - 1].input.tool_use_id,
        );
        assert.ok(JSON.stringify(result).includes('PROBE_REPLACED_' + call));
        assert.ok(!JSON.stringify(result).includes(current.config));
        assert.deepEqual(current.native[index - 1].tool_use_result, [
          { type: 'text', text: 'PROBE_REPLACED_' + call },
        ]);
      }
      // In this pinned CLI the registered SessionStart callback is not emitted;
      // source binding is demonstrated by PostToolUse, not a vacuous lifecycle assertion.
      assert.equal(current.lifecycle.length, 0);
      if (phase === 'resume') assert.ok(!within(config, a.input.transcript_path));
      assert.equal(current.config !== config, phase === 'resume');
      assert.deepEqual(current.results, ['success']);
      // Check collected schemas outside the diagnostic JSON catch: an unexpected
      // SDK field must fail the probe, not be swallowed as a non-JSON diagnostic.
      const hookKeys = [
        'cwd',
        'duration_ms',
        'hook_event_name',
        'mcp_server',
        'permission_mode',
        'prompt_id',
        'session_id',
        'tool_input',
        'tool_name',
        'tool_response',
        'tool_use_id',
        'transcript_path',
      ];
      assert.deepEqual(
        current.hooks.map(({ input }) => Object.keys(input).sort()),
        [hookKeys, hookKeys, hookKeys],
      );
      assert.deepEqual(
        current.control.map(({ envelopeKeys, keys, inputKeys }) => ({
          envelopeKeys,
          keys,
          inputKeys,
        })),
        Array.from({ length: 3 }, () => ({
          envelopeKeys: ['request', 'request_id', 'type'],
          keys: ['callback_id', 'input', 'subtype', 'tool_use_id'],
          inputKeys: hookKeys,
        })),
      );
      // Keep both raw and SDK hook key checks: JSON equality omits undefined fields.
      assert.equal(current.native.length, 3);
      for (const message of current.native) {
        assert.deepEqual(Object.keys(message).sort(), [
          'message',
          'parent_tool_use_id',
          'session_id',
          'timestamp',
          'tool_use_result',
          'type',
          'uuid',
        ]);
        // tool_use_result already equals the exact replacement array above.
        assert.deepEqual(Object.keys(message.message).sort(), ['content', 'role']);
        assert.deepEqual(
          message.message.content.map((block) => Object.keys(block).sort()),
          [['content', 'tool_use_id', 'type']],
        );
      }
      phases.push(phase);
      stream.close();
    }
    process.stdout.write(
      JSON.stringify({
        sdkVersion,
        phases,
        payloadBytes: Buffer.byteLength(payload),
      }),
    );
  } finally {
    clearTimeout(watchdog);
    stream?.close();
    for (const child of children) if (child.exitCode === null) child.kill('SIGKILL');
    await Promise.all(
      children.map((child) =>
        child.exitCode !== null || child.signalCode !== null
          ? undefined
          : new Promise((resolve) => child.once('exit', resolve)),
      ),
    );
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    await rm(root, { recursive: true, force: true });
  }
}
function within(root, path) {
  const rel = relative(root, path);
  return rel !== '' && !rel.startsWith('..') && !rel.startsWith(sep);
}
function send(response, block) {
  const tool = block.type === 'tool_use';
  const events = [
    [
      'message_start',
      {
        type: 'message_start',
        message: {
          id: 'msg_' + randomUUID(),
          type: 'message',
          role: 'assistant',
          model: 'claude-sonnet-4-5-20250929',
          content: [],
          stop_reason: null,
          stop_sequence: null,
          usage: { input_tokens: 1, output_tokens: 0 },
        },
      },
    ],
    [
      'content_block_start',
      {
        type: 'content_block_start',
        index: 0,
        content_block: tool ? { ...block, input: {} } : { type: 'text', text: '' },
      },
    ],
    [
      'content_block_delta',
      {
        type: 'content_block_delta',
        index: 0,
        delta: tool
          ? { type: 'input_json_delta', partial_json: JSON.stringify(block.input) }
          : { type: 'text_delta', text: block.text },
      },
    ],
    ['content_block_stop', { type: 'content_block_stop', index: 0 }],
    [
      'message_delta',
      {
        type: 'message_delta',
        delta: { stop_reason: tool ? 'tool_use' : 'end_turn', stop_sequence: null },
        usage: { output_tokens: 5 },
      },
    ],
    ['message_stop', { type: 'message_stop' }],
  ];
  response.writeHead(200, { 'content-type': 'text/event-stream' });
  for (const [event, data] of events)
    response.write('event: ' + event + '\ndata: ' + JSON.stringify(data) + '\n\n');
  response.end();
}
