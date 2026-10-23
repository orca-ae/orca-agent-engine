// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { mkdtemp, realpath, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { expect, it } from 'vitest';
import type { StartCommand, WorkerEvent } from '@orca/sdk-harness';
import type { DshWorkerEvent } from '../../src/orchestration.js';
import { DshSdkWorker } from '../../dist/index.js';

type Canonical = {
  type: 'canonical_event';
  event: { subpath: string; kind: string; payload: Record<string, unknown> };
};

it('repairs cold root and lazy child cwd before claiming the accepted input', async () => {
  const events: Array<DshWorkerEvent | Canonical> = [];
  const requests: Array<{ messages: Array<{ role: string; content: unknown }>; cwd?: string }> = [];
  let sequence = 0;
  let spawned = false;
  const executedDirectories: string[] = [];
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      const body = JSON.parse(Buffer.concat(chunks).toString());
      const last = JSON.stringify(
        body.messages.findLast((m: { role: string }) => m.role === 'user')?.content,
      );
      const history = JSON.stringify(body.messages);
      const context = body.messages
        .flatMap((m: { content: string | Array<{ text?: string }> }) =>
          typeof m.content === 'string' ? [m.content] : m.content.map((b) => b.text ?? ''),
        )
        .join('\n');
      const cwd = [...context.matchAll(/Current working directory: ("[^"]+")\./g)].at(-1)?.[1];
      requests.push({ ...body, cwd: cwd ? JSON.parse(cwd) : undefined });
      let blocks: Array<Record<string, unknown>>;
      let stop = 'end_turn';
      if (last.includes('CREATE_CHILD') && !spawned) {
        spawned = true;
        blocks = [
          {
            type: 'tool_use',
            id: 'spawn-' + ++sequence,
            name: 'spawn',
            input: { description: 'resume child', prompt: 'Remember CHILD_MEMORY' },
          },
        ];
        stop = 'tool_use';
      } else if (last.includes('FAIL_UNCOMMITTED')) {
        blocks = [{ type: 'text', text: 'failed tree' }];
        stop = 'max_tokens';
      } else if (last.includes('EXECUTE_PWD')) {
        blocks = [{ type: 'tool_use', id: 'pwd-' + ++sequence, name: 'pwd', input: {} }];
        stop = 'tool_use';
      } else if (last.includes('STRICT_REPLY')) {
        blocks = [
          {
            type: 'text',
            text:
              history.includes('ROOT_MEMORY') || history.includes('CHILD_MEMORY')
                ? 'REMEMBERED'
                : 'LOST',
          },
        ];
      } else if (last.includes('RETURN_CWD')) {
        blocks = [{ type: 'text', text: cwd ? JSON.parse(cwd) : 'MISSING_CWD' }];
      } else if (last.includes('Its closing message:')) {
        // Native child settlement still runs the parent; no additional caller reply is needed.
        blocks = [{ type: 'thinking', thinking: 'Child settled.', signature: 'fixture' }];
      } else if (last.includes('tool_result') && executedDirectories.length) {
        blocks = [{ type: 'text', text: executedDirectories.at(-1)! }];
      } else {
        blocks = [
          { type: 'text', text: last.includes('The working directory') ? 'CWD_ACK' : 'SEEDED' },
        ];
      }
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      const send = (frame: Record<string, unknown>) =>
        res.write('event: ' + frame.type + '\ndata: ' + JSON.stringify(frame) + '\n\n');
      send({
        type: 'message_start',
        message: {
          id: 'fixture',
          type: 'message',
          role: 'assistant',
          model: body.model,
          content: [],
          usage: { input_tokens: 4, output_tokens: 0 },
        },
      });
      blocks.forEach((block, index) => {
        send({
          type: 'content_block_start',
          index,
          content_block: block.type === 'tool_use' ? { ...block, input: {} } : block,
        });
        if (block.type === 'tool_use')
          send({
            type: 'content_block_delta',
            index,
            delta: { type: 'input_json_delta', partial_json: JSON.stringify(block.input) },
          });
        send({ type: 'content_block_stop', index });
      });
      send({
        type: 'message_delta',
        delta: { stop_reason: stop, stop_sequence: null },
        usage: { output_tokens: 2 },
      });
      send({ type: 'message_stop' });
      res.end();
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const roots: string[] = [];
  const newRoot = async () => {
    const path = await realpath(await mkdtemp(join(tmpdir(), 'dsh-cold-')));
    roots.push(path);
    return path;
  };
  let root = await newRoot();
  let worker: DshSdkWorker;
  const emit = (event: DshWorkerEvent | Canonical) => {
    events.push(event);
    if (event.type === 'orchestration_request')
      void worker.handle({ type: 'orchestration_result', id: event.id, allowed: true });
    if (event.type === 'tool_call') {
      const actual = execFileSync('pwd', { cwd: root, encoding: 'utf8' }).trim();
      executedDirectories.push(actual);
      void worker.handle({
        type: 'tool_result',
        id: event.id,
        result: { content: [{ type: 'text', text: actual }] },
      });
    }
  };
  worker = new DshSdkWorker(emit);
  const input: StartCommand = {
    type: 'start',
    root,
    sessionId: 'cold-resume',
    model: 'deepseek-v4-flash',
    system: 'Follow the latest user request.',
    apiKey: 'fixture-token',
    tools: [{ name: 'pwd', inputSchema: { type: 'object', properties: {} } }],
    dshGatewayUrl: 'http://127.0.0.1:' + (server.address() as { port: number }).port,
  };
  const canonical = () => events.filter((e): e is Canonical => e.type === 'canonical_event');
  const checkpoints = () =>
    events.filter(
      (e): e is Extract<WorkerEvent, { type: 'checkpoint' }> => e.type === 'checkpoint',
    );
  const replies = (source: string) =>
    canonical()
      .filter(
        ({ event }) => event.kind === 'agent.message' && event.payload.source_event_id === source,
      )
      .flatMap(({ event }) =>
        (event.payload.content as Array<{ text: string }>).map((b) => b.text),
      );
  const submit = (text: string, sourceEventId: string, threadId?: string) =>
    worker.handle({ type: 'submit', text, sourceEventId, ...(threadId ? { threadId } : {}) });
  const persisted = (id: string) =>
    Object.entries(checkpoints().at(-1)!.checkpoint.files)
      .filter(([name]) => name.includes('/' + id + '/'))
      .flatMap(([, data]) =>
        Buffer.from(data, 'base64')
          .toString()
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line)),
      );
  const restore = async () => {
    const checkpoint = checkpoints().at(-1)!.checkpoint;
    await worker.close();
    await rm(root, { recursive: true, force: true });
    root = await newRoot();
    worker = new DshSdkWorker(emit);
    await worker.handle({ ...input, root, checkpoint });
  };
  const strict = async (source: string, threadId?: string, cwd = root) => {
    const before = requests.length;
    const eventBefore = events.length;
    await submit('STRICT_REPLY: return only the remembered code.', source, threadId);
    expect(events.slice(eventBefore).filter((e) => e.type === 'failure')).toEqual([]);
    expect(
      events
        .slice(eventBefore)
        .filter(
          (e): e is Canonical => e.type === 'canonical_event' && e.event.kind === 'agent.message',
        )
        .every(({ event }) => event.payload.source_event_id === source),
    ).toBe(true);
    expect({ replies: replies(source), requests: requests.length - before }).toEqual({
      replies: ['REMEMBERED'],
      // A child reply also triggers one parent settlement request, without another text reply.
      requests: threadId ? 2 : 1,
    });
    expect(requests[before].cwd).toBe(cwd);
    expect(JSON.stringify(requests[before].messages)).toContain(
      threadId ? 'CHILD_MEMORY' : 'ROOT_MEMORY',
    );
    if (threadId) {
      expect(JSON.stringify(requests[before].messages)).not.toContain('ROOT_MEMORY');
      expect(JSON.stringify(requests[before + 1].messages)).toContain('ROOT_MEMORY');
      // The parent retains its spawn arguments, but never receives the child's user history.
      expect(
        JSON.stringify(requests[before + 1].messages.filter((message) => message.role === 'user')),
      ).not.toContain('CHILD_MEMORY');
      expect(JSON.stringify(requests[before + 1].messages.at(-1))).toContain(
        'Its closing message:',
      );
    }
    expect(
      persisted(threadId ?? checkpoints().at(-1)!.checkpoint.threadId)
        .filter((row) => row.type === 'working-directory/change')
        .at(-1)?.data.cwd,
    ).toBe(cwd);
  };
  try {
    await worker.handle(input);
    await submit('Remember ROOT_MEMORY', 'evt_seed');
    await submit('CREATE_CHILD', 'evt_child_seed');
    const child = String(
      canonical().find(({ event }) => event.kind === 'session.thread_created')!.event.payload
        .native_session_id,
    );
    await restore();
    await strict('evt_cold');
    await strict('evt_warm');
    expect(
      persisted(checkpoints().at(-1)!.checkpoint.threadId).filter(
        (row) =>
          row.type === 'user/message' &&
          JSON.stringify(row.data).includes('is unavailable. The working directory is now'),
      ),
    ).toHaveLength(1);
    await submit('RETURN_CWD: return the current working directory.', 'evt_cwd');
    expect(replies('evt_cwd')).toEqual([root]);
    await submit('EXECUTE_PWD', 'evt_exec');
    expect(executedDirectories).toEqual([root]);
    expect(replies('evt_exec')).toEqual([root]);
    await strict('evt_lazy_child', child);
    const count = checkpoints().length;
    await submit('FAIL_UNCOMMITTED', 'evt_failed');
    expect(checkpoints()).toHaveLength(count);
    expect(events.some((e) => e.type === 'failure' && e.fatal)).toBe(true);
    await restore();
    const before = requests.length;
    const recoveryEvents = events.length;
    await strict('evt_recovered');
    await strict('evt_recovered_child', child);
    expect(JSON.stringify(requests.slice(before))).not.toContain('FAIL_UNCOMMITTED');
    expect(
      events
        .slice(recoveryEvents)
        .filter(
          (e): e is Canonical => e.type === 'canonical_event' && e.event.kind === 'agent.message',
        )
        .map(({ event }) => event.payload.source_event_id),
    ).toEqual(['evt_recovered', 'evt_recovered_child']);
    const checkpoint = checkpoints().at(-1)!.checkpoint;
    // Keep the previous root on disk deliberately: this restore must preserve a valid old cwd.
    const validCwd = root;
    await worker.close();
    root = await newRoot();
    worker = new DshSdkWorker(emit);
    await worker.handle({ ...input, root, checkpoint });
    await strict('evt_valid_cwd', undefined, validCwd);

    // Native ensure cannot recover when both the recorded and original directories are absent.
    const committed = checkpoints().at(-1)!.checkpoint;
    const snapshot = JSON.stringify(committed);
    const unavailable = structuredClone(committed);
    const name = Object.keys(unavailable.files).find((path) =>
      path.includes('/' + unavailable.threadId + '/'),
    )!;
    const rows = Buffer.from(unavailable.files[name], 'base64').toString().trimEnd().split('\n');
    const missingOriginal = 'orca-dsh-unavailable-' + randomUUID();
    rows[0] = JSON.stringify({ ...JSON.parse(rows[0]), cwd: '/' + missingOriginal });
    // The native project directory must match the header's original cwd.
    unavailable.files[name.replace('_no-cwd', '--' + missingOriginal + '--')] = Buffer.from(
      rows.join('\n') + '\n',
    ).toString('base64');
    delete unavailable.files[name];
    await worker.close();
    await rm(validCwd, { recursive: true, force: true });
    const requestCount = requests.length;
    const toolCount = executedDirectories.length;
    const checkpointCount = checkpoints().length;
    const startupEvents = events.length;
    worker = new DshSdkWorker(emit);
    await expect(worker.handle({ ...input, root, checkpoint: unavailable })).rejects.toThrow(
      'working-directory: directory does not exist:',
    );
    await expect(submit('EXECUTE_PWD', 'evt_rejected')).rejects.toThrow('DSH not ready');
    expect(events.slice(startupEvents)).toEqual([]);
    expect(requests).toHaveLength(requestCount);
    expect(executedDirectories).toHaveLength(toolCount);
    expect(checkpoints()).toHaveLength(checkpointCount);
    expect(JSON.stringify(committed)).toBe(snapshot);
    // A fresh worker can still recover the unchanged committed checkpoint using its fallback.
    worker = new DshSdkWorker(emit);
    await worker.handle({ ...input, root, checkpoint: committed });
    await strict('evt_after_setup_failure');
    expect(
      canonical().every(
        ({ event }) => event.payload.source_event_id === event.payload.turn_event_id,
      ),
    ).toBe(true);
  } finally {
    await worker.close();
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await Promise.all(roots.map((path) => rm(path, { recursive: true, force: true })));
  }
}, 30000);
