// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'vitest';
import { createServer } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { WorkerEvent, StartCommand } from '@orca/sdk-harness';
import { DshSdkWorker } from '../../dist/index.js';
import type { DshWorkerEvent } from '../../src/orchestration.js';

type Canonical = {
  type: 'canonical_event';
  event: { id: string; subpath: string; kind: string; payload: Record<string, unknown> };
};
type ThreadTree = {
  type: 'thread_tree';
  threads: Array<{ subpath: string; parentSubpath: string }>;
};
type FixtureEvent = DshWorkerEvent | Canonical | ThreadTree;

export async function exerciseNativeTree(
  observe: (event: DshWorkerEvent) => void = () => {},
  model = { provider: 'deepseek', id: 'deepseek-v4-flash' },
): Promise<void> {
  const requests: Array<{
    id: string;
    auth: string | undefined;
    session: string;
    url: string | undefined;
    body: { model: string; messages: unknown[]; tools: Array<{ name: string }> };
  }> = [];
  const events: FixtureEvent[] = [];
  let rootId = '';
  let a = '';
  let b = '';
  let grandchild = '';
  let nested = false;
  let interrupting = false;
  let callSequence = 0;
  let launched = false;
  let controlled = false;
  let childFailed = false;
  let archiveControl = false;
  let denied = false;
  let cancelGate = false;
  let fenceRace = false;
  let heldAborted = false;
  let gateMode: 'allow' | 'deny' | 'ask' = 'allow';
  const approvals: Array<Extract<DshWorkerEvent, { type: 'orchestration_request' }>> = [];
  let gateReady: () => void = () => {};
  const childReplies: Array<() => void> = [];
  let held: () => void = () => {};
  const waiting = new Promise<void>((resolve) => {
    held = resolve;
  });
  let siblingReady: () => void = () => {};
  const siblingWaiting = new Promise<void>((resolve) => {
    siblingReady = resolve;
  });
  let siblingReply: (() => void) | undefined;
  let siblingAborted = false;
  let siblingReleased = false;
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', async () => {
      const body = JSON.parse(Buffer.concat(chunks).toString());
      expect(req.headers['x-deepseek-harness-session-id']).toBeUndefined();
      // Use private runtime facts and the fixture's user prompts, not an outbound native ID.
      const userText = JSON.stringify(
        body.messages
          .filter((message: { role: string }) => message.role === 'user')
          .flatMap((message: { content: string | Array<{ type: string; text?: string }> }) =>
            typeof message.content === 'string'
              ? [message.content]
              : message.content.filter((block) => block.type === 'text').map((block) => block.text),
          ),
      );
      const label = userText.includes('GRANDCHILD')
        ? 'bounded nested child'
        : userText.includes('CHILD_A')
          ? 'fresh parallel child'
          : userText.includes('CHILD_B')
            ? 'fork parallel child'
            : undefined;
      const identity = () =>
        label
          ? events.find(
              (event) =>
                event.type === 'canonical_event' &&
                event.event.kind === 'session.thread_created' &&
                event.event.payload.agent_name === label,
            )
          : events.find((event) => event.type === 'event' && event.event.type === 'thread.started');
      await expect.poll(identity, { timeout: 2000 }).toBeDefined();
      const source = identity()!;
      const id =
        source.type === 'canonical_event'
          ? String(source.event.payload.native_session_id)
          : source.type === 'event' && source.event.type === 'thread.started'
            ? source.event.thread_id
            : '';
      requests.push({
        id,
        auth: req.headers.authorization,
        session: String(req.headers['x-orca-session-id']),
        url: req.url,
        body,
      });
      rootId ||= id;
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      const text = JSON.stringify(body.messages);
      const reply = (
        blocks: Array<Record<string, unknown>> = [{ type: 'text', text: 'OK' }],
        stop = 'end_turn',
      ) => {
        const tool = blocks.some((block) => block.type === 'tool_use');
        const frames: Array<Record<string, unknown>> = [
          {
            type: 'message_start',
            message: {
              id: 'fixture',
              type: 'message',
              role: 'assistant',
              content: [],
              model: body.model,
              usage: { input_tokens: 4, output_tokens: 0, cache_read_input_tokens: 3 },
            },
          },
        ];
        blocks.forEach((block, index) => {
          frames.push({
            type: 'content_block_start',
            index,
            content_block: block.type === 'tool_use' ? { ...block, input: {} } : block,
          });
          if (block.type === 'tool_use')
            frames.push({
              type: 'content_block_delta',
              index,
              delta: { type: 'input_json_delta', partial_json: JSON.stringify(block.input) },
            });
          frames.push({ type: 'content_block_stop', index });
        });
        frames.push(
          {
            type: 'message_delta',
            delta: { stop_reason: tool ? 'tool_use' : stop, stop_sequence: null },
            usage: { output_tokens: 2 },
          },
          { type: 'message_stop' },
        );
        res.end(
          frames
            .map((frame) => 'event: ' + frame.type + '\ndata: ' + JSON.stringify(frame) + '\n\n')
            .join(''),
        );
      };
      const call = (name: string, input: object) => ({
        type: 'tool_use',
        id: 'native-' + name + '-' + ++callSequence,
        name,
        input,
      });
      if (id === rootId && text.includes('FENCE_LATE_ALLOW') && !fenceRace) {
        fenceRace = true;
        reply([call('send_message', { agent_id: b, message: 'LATE_ALLOW_MUST_NOT_ACTIVATE' })]);
      } else if (id === rootId && text.includes('DENIED_SPAWN') && !denied) {
        denied = true;
        reply([call('spawn', { description: 'denied child', prompt: 'NEVER_EXECUTE' })]);
      } else if (id === rootId && text.includes('CANCEL_GATE') && !cancelGate) {
        cancelGate = true;
        reply([call('spawn', { description: 'cancelled child', prompt: 'NEVER_EXECUTE' })]);
      } else if (id === rootId && text.includes('DELEGATE') && !launched) {
        launched = true;
        reply([
          call('spawn', { description: 'fresh parallel child', prompt: 'CHILD_A' }),
          call('fork', { description: 'fork parallel child', prompt: 'CHILD_B' }),
        ]);
      } else if (id !== rootId && !a) {
        a = id;
        childReplies.push(() => reply());
      } else if (id !== rootId && id !== a && !b) {
        b = id;
        childReplies.push(() => reply());
        // Neither child's model request completes until both are concurrently admitted.
        childReplies.forEach((complete) => complete());
      } else if (id === a && text.includes('NEST_NOW') && !nested) {
        nested = true;
        reply([call('spawn', { description: 'bounded nested child', prompt: 'GRANDCHILD' })]);
      } else if (id !== rootId && id !== a && id !== b && !grandchild) {
        grandchild = id;
        reply();
      } else if (id === rootId && text.includes('INTERRUPT_SIBLINGS') && !interrupting) {
        interrupting = true;
        reply([
          call('send_message', { agent_id: b, message: 'HOLD' }),
          call('send_message', { agent_id: b, message: 'QUEUED_ARCHIVE_WAKE' }),
          call('send_message', { agent_id: a, message: 'SIBLING_HOLD' }),
        ]);
      } else if (id === a && text.includes('SIBLING_HOLD') && !siblingReleased) {
        siblingReply = () => {
          siblingReleased = true;
          reply([{ type: 'text', text: 'SIBLING_COMPLETED' }]);
        };
        res.on('close', () => {
          if (!res.writableEnded) siblingAborted = true;
        });
        siblingReady();
      } else if (id === rootId && text.includes('ARCHIVE_CONTROL') && !archiveControl) {
        archiveControl = true;
        reply([call('send_message', { agent_id: a, message: 'ARCHIVED_NO_ACTIVATION' })]);
      } else if (id === rootId && text.includes('CONTROL') && !controlled) {
        controlled = true;
        reply([
          call('send_message', { agent_id: b, message: 'NATIVE_FOLLOWUP' }),
          call('interrupt_agent', { agent_id: a }),
        ]);
      } else if (
        id !== rootId &&
        text.includes('HOST_FOLLOWUP') &&
        !text.includes('managed denied')
      ) {
        reply([call('read', { path: 'managed' })]);
      } else if (id === b && text.includes('NATIVE_FOLLOWUP') && !childFailed) {
        childFailed = true;
        reply([{ type: 'text', text: 'parent handles bounded child failure' }], 'max_tokens');
      } else if (id === b && text.includes('HOLD')) {
        res.on('close', () => {
          if (!res.writableEnded) heldAborted = true;
        });
        res.write('event: ping\ndata: {"type":"ping"}\n\n');
        held();
      } else
        reply([
          { type: 'thinking', thinking: 'own reasoning', signature: 'native-signature' },
          { type: 'text', text: 'OK' },
        ]);
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const root = await mkdtemp(join(tmpdir(), 'dsh-tree-fixture-'));
  let worker: DshSdkWorker;
  const emit = (event: FixtureEvent) => {
    observe(event);
    events.push(event);
    if (event.type === 'orchestration_request') {
      const projected = events.find(
        (candidate) =>
          candidate.type === 'canonical_event' && candidate.event.id === event.origin.eventId,
      );
      expect(projected).toMatchObject({
        type: 'canonical_event',
        event: {
          kind: 'agent.tool_use',
          subpath: event.origin.subpath,
          payload: { name: event.name, input: event.arguments },
        },
      });
      if (gateMode === 'ask') {
        approvals.push(event);
        gateReady();
      } else
        void worker.handle({
          type: 'orchestration_result',
          id: event.id,
          allowed: gateMode === 'allow',
          reason: 'policy denied',
        });
    }
    if (event.type === 'tool_call')
      void worker.handle({
        type: 'tool_result',
        id: event.id,
        result: { isError: true, content: [{ type: 'text', text: 'managed denied' }] },
      });
  };
  worker = new DshSdkWorker(emit);
  const input: StartCommand = {
    type: 'start',
    root,
    sessionId: 'orca-session',
    model: model.id,
    modelProvider: model.provider,
    system: 'Fixed grant',
    apiKey: 'scoped-jwt',
    dshGatewayUrl: 'http://127.0.0.1:' + (server.address() as { port: number }).port,
    tools: [
      {
        name: 'read',
        inputSchema: {
          type: 'object',
          properties: { path: { type: 'string' } },
          required: ['path'],
        },
      },
    ],
  };
  const submit = (text: string, sourceEventId: string, threadId?: string) =>
    worker.handle({ type: 'submit', text, sourceEventId, ...(threadId ? { threadId } : {}) });
  const canonical = () =>
    events.filter((event): event is Canonical => event.type === 'canonical_event');
  const checkpoints = () =>
    events.filter(
      (event): event is Extract<WorkerEvent, { type: 'checkpoint' }> => event.type === 'checkpoint',
    );
  try {
    await worker.handle({
      ...input,
      tools: [...input.tools, { name: 'web_fetch', inputSchema: { type: 'object' } }],
    });
    await expect(worker.handle({ type: 'submit', text: 'missing source' })).rejects.toThrow(
      'sourceEventId',
    );
    await submit('Remember SAFFRON', 'evt_seed');
    gateMode = 'deny';
    await submit('DENIED_SPAWN', 'evt_denied');
    expect(requests.every((request) => request.id === rootId)).toBe(true);
    expect(canonical().filter((event) => event.event.kind === 'session.thread_created')).toEqual(
      [],
    );
    expect(canonical().filter((event) => event.event.kind === 'agent.tool_result')).toMatchObject([
      { event: { payload: { is_error: true } } },
    ]);
    gateMode = 'ask';
    const asked = new Promise<void>((resolve) => {
      gateReady = resolve;
    });
    const delegation = submit('DELEGATE', 'evt_delegate');
    await asked;
    expect(requests.every((request) => request.id === rootId)).toBe(true);
    expect(canonical().filter((event) => event.event.kind === 'session.thread_created')).toEqual(
      [],
    );
    gateMode = 'allow';
    for (const approval of approvals.splice(0))
      await worker.handle({ type: 'orchestration_result', id: approval.id, allowed: true });
    await delegation;
    expect(events.filter((event) => event.type === 'failure')).toEqual([]);
    expect(a).not.toBe(b);
    expect(childReplies).toHaveLength(2);
    const children = requests.filter((request) => request.id !== rootId);
    expect(children).toHaveLength(2);
    expect(
      children.filter((request) => JSON.stringify(request.body.messages).includes('SAFFRON')),
    ).toHaveLength(1);
    expect(
      children.every((request) => request.body.tools.some((tool) => tool.name === 'read')),
    ).toBe(true);
    expect(
      requests.every((request) => !request.body.tools.some((tool) => tool.name === 'web_fetch')),
    ).toBe(true);
    const delegateEvents = canonical().filter(
      (event) => event.event.payload.source_event_id === 'evt_delegate',
    );
    expect(
      delegateEvents.filter((event) => event.event.kind === 'session.thread_created'),
    ).toHaveLength(2);
    expect(
      delegateEvents
        .filter((event) => event.event.kind === 'session.thread_created')
        .map((event) => event.event.payload.agent_name),
    ).toEqual(expect.arrayContaining(['fresh parallel child', 'fork parallel child']));
    const settlements = delegateEvents.filter(
      (event) => event.event.payload.message_origin === 'runtime_settlement',
    );
    expect(settlements).toHaveLength(2);
    for (const { event } of settlements) {
      expect(event.kind).toBe('agent.thread_message_received');
      expect(event.subpath).toBe('');
      expect(event.payload.recipient_subpath).toBe('');
      expect(event.payload.sender_subpath).toMatch(/^subagents\//);
      expect(event.payload.native_message_id).toEqual(expect.any(String));
      expect(event.payload.content).toEqual([
        {
          type: 'text',
          text:
            'Child ' +
            String(event.payload.sender_subpath).slice('subagents/'.length) +
            ' settled.',
        },
      ]);
    }
    expect(
      delegateEvents
        .filter((event) => event.event.kind === 'session.thread_status_idle')
        .every((event) => event.event.payload.stop_reason === 'completed'),
    ).toBe(true);
    expect(
      delegateEvents
        .filter((event) => event.event.kind === 'session.thread_created')
        .every((event) =>
          delegateEvents.some(
            (tool) =>
              tool.event.kind === 'agent.tool_use' &&
              tool.event.id === event.event.payload.parent_tool_use_id,
          ),
        ),
    ).toBe(true);
    expect(
      delegateEvents.filter(
        (event) => event.event.kind === 'agent.message' && event.event.subpath !== '',
      ),
    ).toHaveLength(2);
    expect(
      delegateEvents.filter(
        (event) => event.event.kind === 'agent.thinking' && event.event.subpath !== '',
      ),
    ).toHaveLength(0);
    expect(
      checkpoints().at(-1)?.checkpoint.files && Object.keys(checkpoints().at(-1)!.checkpoint.files),
    ).toHaveLength(4);
    await submit('HOST_FOLLOWUP', 'evt_host_followup', a);
    expect(events.filter((event) => event.type === 'tool_call')).toMatchObject([
      { name: 'read', arguments: { path: 'managed' }, origin: { subpath: 'subagents/' + a } },
    ]);
    await submit('CONTROL', 'evt_control');
    expect(events.filter((event) => event.type === 'failure')).toEqual([]);
    expect(childFailed).toBe(true);
    expect(
      canonical().filter(
        (event) =>
          event.event.payload.source_event_id === 'evt_control' &&
          event.event.kind === 'session.thread_status_idle' &&
          event.event.subpath === 'subagents/' + b,
      ),
    ).toMatchObject([{ event: { payload: { stop_reason: 'max-tokens' } } }]);
    expect(
      canonical().filter(
        (event) =>
          event.event.payload.source_event_id === 'evt_control' &&
          event.event.kind === 'span.model_request_end' &&
          event.event.subpath === 'subagents/' + b,
      ),
    ).toMatchObject([{ event: { payload: { is_error: true } } }]);
    expect(
      canonical().some(
        (event) =>
          event.event.kind === 'agent.thread_message_received' &&
          event.event.subpath === 'subagents/' + b,
      ),
    ).toBe(true);
    await submit('NEST_NOW', 'evt_nested', a);
    expect(grandchild).not.toBe('');
    expect(events.filter((event) => event.type === 'failure')).toEqual([]);
    expect(Object.keys(checkpoints().at(-1)!.checkpoint.files)).toHaveLength(5);
    expect(
      canonical().find(
        (event) =>
          event.event.kind === 'session.thread_created' &&
          event.event.subpath === 'subagents/' + grandchild,
      )?.event.payload.parent_subpath,
    ).toBe('subagents/' + a);
    const checkpoint = checkpoints().at(-1)!.checkpoint;
    const summaries = canonical().filter((event) => event.event.kind === 'span.model_request_end');
    const totals = events.filter(
      (event): event is Extract<WorkerEvent, { type: 'event' }> =>
        event.type === 'event' && event.event.type === 'turn.completed',
    );
    const observed = requests.length;
    expect(
      totals.reduce(
        (sum, event) =>
          sum + (event.event.type === 'turn.completed' ? event.event.usage.input_tokens : 0),
        0,
      ),
    ).toBe(observed * 7);
    expect(
      summaries.reduce(
        (sum, event) =>
          sum + Number((event.event.payload.model_usage as { input_tokens: number }).input_tokens),
        0,
      ),
    ).toBe(observed * 4);
    expect(summaries.some((event) => event.event.subpath === '')).toBe(true);
    expect(summaries.some((event) => event.event.subpath === 'subagents/' + a)).toBe(true);
    await worker.close();
    worker = new DshSdkWorker(emit);
    await worker.handle({ ...input, checkpoint });
    const lineage = events
      .filter((event): event is ThreadTree => event.type === 'thread_tree')
      .at(-1)!.threads;
    expect(lineage).toHaveLength(3);
    expect(lineage).toContainEqual({
      subpath: 'subagents/' + grandchild,
      parentSubpath: 'subagents/' + a,
    });
    const announced = new Set(['']);
    for (const thread of lineage) {
      expect(announced.has(thread.parentSubpath)).toBe(true);
      announced.add(thread.subpath);
    }
    await worker.refreshOptions({ apiKey: 'refreshed-jwt' });
    const before = events.length;
    await submit('RESTORED_CHILD', 'evt_restore', b);
    expect(events.filter((event) => event.type === 'failure')).toEqual([]);
    const restored = requests.at(-1)!;
    expect(restored.auth).toBe('Bearer refreshed-jwt');
    expect(
      requests
        .slice(observed)
        .some(
          (request) =>
            request.id === b && JSON.stringify(request.body.messages).includes('NATIVE_FOLLOWUP'),
        ),
    ).toBe(true);
    expect(
      events
        .slice(before)
        .filter(
          (event) =>
            event.type === 'canonical_event' && event.event.kind === 'session.thread_created',
        ),
    ).toEqual([]);
    expect(
      canonical()
        .filter((event) => event.event.payload.source_event_id === 'evt_restore')
        .every(
          (event) =>
            !canonical().some(
              (old) =>
                old.event.payload.source_event_id === 'evt_control' &&
                old.event.id === event.event.id,
            ),
        ),
    ).toBe(true);
    const holding = submit('INTERRUPT_SIBLINGS', 'evt_interrupt');
    await Promise.all([waiting, siblingWaiting]);
    const committed = checkpoints().length;
    const childRequestsBeforeArchive = requests.filter((request) => request.id === b).length;
    await worker.handle({ type: 'archive_fence', threadId: b });
    expect(heldAborted).toBe(false);
    expect(siblingAborted).toBe(false);
    expect(siblingReleased).toBe(false);
    expect(checkpoints()).toHaveLength(committed);
    await worker.handle({ type: 'interrupt', threadId: b, archived: true });
    expect(siblingAborted).toBe(false);
    expect(siblingReleased).toBe(false);
    siblingReply!();
    await holding;
    expect(requests.filter((request) => request.id === b)).toHaveLength(childRequestsBeforeArchive);
    expect(siblingAborted).toBe(false);
    expect(
      canonical().some(
        (event) =>
          event.event.payload.turn_event_id === 'evt_interrupt' &&
          String(JSON.stringify(event.event.payload.content)).includes('SIBLING_COMPLETED'),
      ),
    ).toBe(true);
    expect(checkpoints()).toHaveLength(committed);
    expect(events.filter((event) => event.type === 'failure')).toMatchObject([{ type: 'failure' }]);
    expect(events.find((event) => event.type === 'failure' && event.fatal)).toBeDefined();
    expect(
      canonical().filter(
        (event) =>
          event.event.payload.turn_event_id === 'evt_interrupt' &&
          event.event.subpath === 'subagents/' + b &&
          event.event.kind === 'session.thread_status_idle',
      ),
    ).toMatchObject([
      { event: { payload: { stop_reason: 'interrupted', native_reason: 'aborted' } } },
    ]);
    expect(
      canonical().filter(
        (event) =>
          event.event.payload.source_event_id === 'evt_interrupt' &&
          event.event.kind === 'span.model_request_end',
      ),
    ).toEqual([]);
    await expect(submit('Continue root after interruption', 'evt_after_interrupt')).rejects.toThrow(
      'not ready',
    );
    const committedCheckpoint = checkpoints().at(-1)!.checkpoint;
    await worker.close();
    worker = new DshSdkWorker(emit);
    await worker.handle({
      ...input,
      checkpoint: committedCheckpoint,
      archivedThreadSubpaths: ['subagents/' + b],
    });
    await expect(submit('archive survives checkpoint rollback', 'evt_archived', b)).rejects.toThrow(
      'archived',
    );
    await submit('Continue root after interruption', 'evt_after_interrupt');
    expect(checkpoints()).toHaveLength(committed + 1);
    const recoveredInput = JSON.stringify(requests.at(-1)!.body.messages);
    expect(recoveredInput).not.toContain('INTERRUPT_SIBLINGS');
    expect(recoveredInput).not.toContain('SIBLING_COMPLETED');
    expect(recoveredInput).toContain(
      'uncommitted prior turn may have produced external side effects',
    );
    gateMode = 'ask';
    const pendingGate = new Promise<void>((resolve) => {
      gateReady = resolve;
    });
    const pendingTurn = submit('CANCEL_GATE', 'evt_cancel_gate');
    await pendingGate;
    const requestCount = requests.length;
    const childCount = canonical().filter(
      (event) => event.event.kind === 'session.thread_created',
    ).length;
    const checkpointCount = checkpoints().length;
    await worker.handle({ type: 'interrupt' });
    await pendingTurn;
    for (const approval of approvals.splice(0))
      await worker.handle({ type: 'orchestration_result', id: approval.id, allowed: true });
    expect(requests).toHaveLength(requestCount);
    expect(
      canonical().filter((event) => event.event.kind === 'session.thread_created'),
    ).toHaveLength(childCount);
    expect(checkpoints()).toHaveLength(checkpointCount);
    await expect(submit('late approval cannot revive', 'evt_late')).rejects.toThrow('not ready');
    gateMode = 'allow';
    expect(
      requests.every(
        (request) =>
          request.session === 'orca-session' &&
          request.url?.split('?')[0] === '/v1/proxy/' + model.provider + '/v1/messages' &&
          request.body.model === model.id,
      ),
    ).toBe(true);
    expect(canonical().some((event) => event.event.kind === 'agent.thinking')).toBe(true);
    expect(
      canonical().every(
        (event) => event.event.payload.turn_event_id === event.event.payload.source_event_id,
      ),
    ).toBe(true);
    expect(
      canonical()
        .filter((event) => event.event.kind === 'agent.thinking')
        .every((event) =>
          (event.event.payload.content as Array<{ type: string; thinking?: string }>).every(
            (block) => block.type === 'thinking' && typeof block.thinking === 'string',
          ),
        ),
    ).toBe(true);
    // Published DSH cannot materialize a dormant ancestor Activation without delivering a new prompt.
    const coldCheckpoint = checkpoints().at(-1)!.checkpoint;
    await worker.close();
    worker = new DshSdkWorker(emit);
    await worker.handle({
      ...input,
      checkpoint: coldCheckpoint,
      archivedThreadSubpaths: ['subagents/' + a],
    });
    const beforeArchivedFollowup = requests.length;
    await expect(submit('archived descendant', 'evt_archived_nested', grandchild)).rejects.toThrow(
      'archived',
    );
    expect(requests).toHaveLength(beforeArchivedFollowup);
    await submit('ARCHIVE_CONTROL', 'evt_archive_control');
    expect(requests.slice(beforeArchivedFollowup).every((request) => request.id === rootId)).toBe(
      true,
    );
    expect(
      canonical().filter(
        (event) =>
          event.event.payload.source_event_id === 'evt_archive_control' &&
          event.event.kind === 'agent.tool_result',
      ),
    ).toMatchObject([{ event: { payload: { is_error: true } } }]);
    expect(
      events.filter(
        (event) =>
          event.type === 'orchestration_request' &&
          event.origin.eventId ===
            canonical().find(
              (candidate) =>
                candidate.event.payload.source_event_id === 'evt_archive_control' &&
                candidate.event.kind === 'agent.tool_use',
            )?.event.id,
      ),
    ).toEqual([]);
    // An idle native child also needs a durable runtime fence, not just a one-off stop.
    await expect(worker.handle({ type: 'interrupt', archived: true })).rejects.toThrow(
      'thread target',
    );
    await worker.handle({ type: 'interrupt', threadId: b, archived: true });
    await expect(submit('idle archive cannot reactivate', 'evt_idle_archive', b)).rejects.toThrow(
      'archived',
    );
    await submit('ROOT_AFTER_IDLE_ARCHIVE', 'evt_root_after_archive');
    await worker.close();
    worker = new DshSdkWorker(emit);
    await worker.handle({ ...input, checkpoint: coldCheckpoint });
    await expect(worker.handle({ type: 'archive_fence', threadId: 'not-native' })).rejects.toThrow(
      'outside tree',
    );
    await expect(
      worker.handle({ type: 'archive_fence', threadId: '99999999-9999-9999-9999-999999999999' }),
    ).rejects.toThrow('outside tree');
    gateMode = 'ask';
    const raceWaiting = new Promise<void>((resolve) => {
      gateReady = resolve;
    });
    const raceTurn = submit('FENCE_LATE_ALLOW', 'evt_fence_race');
    await raceWaiting;
    const requestsBeforeFence = requests.length;
    const checkpointsBeforeFence = checkpoints().length;
    // Cold known lineage is enough: b is dormant and the root is parked on authorization.
    await worker.handle({ type: 'archive_fence', threadId: b });
    await worker.handle({ type: 'archive_fence', threadId: b });
    expect(checkpoints()).toHaveLength(checkpointsBeforeFence);
    expect(requests).toHaveLength(requestsBeforeFence);
    gateMode = 'allow';
    for (const approval of approvals.splice(0))
      await worker.handle({ type: 'orchestration_result', id: approval.id, allowed: true });
    await raceTurn;
    expect(requests.slice(requestsBeforeFence).every((request) => request.id === rootId)).toBe(
      true,
    );
    expect(
      canonical().filter(
        (event) =>
          event.event.payload.source_event_id === 'evt_fence_race' &&
          event.event.kind === 'agent.tool_result',
      ),
    ).toMatchObject([{ event: { payload: { is_error: true } } }]);
    await expect(submit('fence remains installed', 'evt_fence_still_archived', b)).rejects.toThrow(
      'archived',
    );
    await submit('NESTED_COLD_FOLLOWUP', 'evt_nested_cold', grandchild);
    expect(events.filter((event) => event.type === 'failure').at(-1)).toMatchObject({
      fatal: true,
      message: 'DSH follow-up parent unavailable',
    });
  } finally {
    await worker.close();
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(root, { recursive: true, force: true });
  }
}
