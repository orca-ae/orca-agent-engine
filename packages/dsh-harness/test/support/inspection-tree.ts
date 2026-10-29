// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'vitest';
import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { StartCommand, WorkerEvent } from '@orca/sdk-harness';
import { DshSdkWorker } from '../../dist/index.js';
import { replyMessages } from './native-tree.js';

type Canonical = Extract<WorkerEvent, { type: 'canonical_event' }>;
type Query = { name: string; args: Record<string, unknown> };

/** Real pinned DSH processes, with deterministic local model responses rather than a live model. */
export async function exerciseInspectionTree(observe: (event: WorkerEvent) => void = () => {}) {
  const events: WorkerEvent[] = [];
  const requests: string[] = [];
  const directories: string[] = [];
  let worker: DshSdkWorker;
  let rootId = '';
  let a = '';
  let b = '';
  let grandchild = '';
  let building = true;
  let spawned = false;
  let nested = false;
  let seeded = false;
  let childStep = 0;
  let aInspected = false;
  let leafInspected = false;
  let reactivating = false;
  let reactivationStep = 0;
  let firstContinuation = '';
  let requestedBuildReads = 0;
  const buildReads: Array<{ payload: Record<string, unknown>; text: string }> = [];
  let query: Query | undefined;
  let dispatched = false;
  let sequence = 0;
  let holdApproval = false;
  let pendingApproval: Extract<WorkerEvent, { type: 'orchestration_request' }> | undefined;
  let approvalReady!: () => void;
  const approvalWaiting = new Promise<void>((resolve) => {
    approvalReady = resolve;
  });
  let holdReply: (() => void) | undefined;
  let holdReady!: () => void;
  const holding = new Promise<void>((resolve) => {
    holdReady = resolve;
  });
  let progressReady!: () => void;
  const progressing = new Promise<void>((resolve) => {
    progressReady = resolve;
  });
  let reattachedReady!: () => void;
  let reattached = new Promise<void>((resolve) => {
    reattachedReady = resolve;
  });
  const releaseReply = () => {
    const release = holdReply;
    holdReply = undefined;
    release?.();
  };
  const call = (name: string, input: object) => ({
    type: 'tool_use',
    id: 'inspection-' + ++sequence,
    name,
    input,
  });
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', async () => {
      const body = JSON.parse(Buffer.concat(chunks).toString());
      const text = body.messages
        .filter((m: { role: string }) => m.role === 'user')
        .flatMap((m: { content: string | Array<{ type: string; text?: string }> }) =>
          typeof m.content === 'string'
            ? [m.content]
            : m.content.filter((c) => c.type === 'text').map((c) => c.text ?? ''),
        )
        .join('\n');
      const role = text.includes('INSPECT_GRAND')
        ? 'grandchild'
        : text.includes('INSPECT_CHILD_A')
          ? 'a'
          : text.includes('INSPECT_CHILD_B')
            ? 'b'
            : 'root';
      requests.push(role);
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      const reply = (blocks?: Array<Record<string, unknown>>) => {
        if (building && role === 'root')
          requestedBuildReads +=
            blocks?.filter(
              (block) => block.type === 'tool_use' && block.name === 'read_child_session',
            ).length ?? 0;
        replyMessages(res, body.model, blocks);
      };
      if (building && role === 'root' && !spawned) {
        spawned = true;
        reply([
          call('spawn', { description: 'inspection A', prompt: 'INSPECT_CHILD_A' }),
          call('fork', { description: 'inspection B', prompt: 'INSPECT_CHILD_B' }),
        ]);
      } else if (building && role === 'a' && !nested) {
        nested = true;
        reply([call('spawn', { description: 'inspection grandchild', prompt: 'INSPECT_GRAND' })]);
      } else if (building && role === 'b' && !seeded) {
        seeded = true;
        reply(
          Array.from({ length: 55 }, (_, index) => call('echo', { text: 'child-record-' + index })),
        );
      } else if (building && role === 'a' && !aInspected) {
        aInspected = true;
        reply([
          call('list_child_sessions', {}),
          call('read_child_session', { agent_id: b }),
          call('read_child_session', { agent_id: randomUUID() }),
          call('read_child_session', { agent_id: rootId }),
        ]);
      } else if (building && role === 'grandchild' && !leafInspected) {
        leafInspected = true;
        reply([call('list_child_sessions', {})]);
      } else if (building && role === 'b') {
        if (childStep++ === 0) {
          holdReply = () =>
            reply([
              { type: 'text', text: 'B_PROGRESS' },
              call('echo', { text: 'appended-after-first-read' }),
            ]);
          holdReady();
        } else {
          holdReply = () => reply([{ type: 'text', text: 'B_FINISHED' }]);
          progressReady();
        }
      } else if (building && role === 'root') {
        await holding;
        if (buildReads.length === 0) {
          reply([call('read_child_session', { agent_id: b })]);
        } else {
          await progressing;
          const page = JSON.parse(buildReads.at(-1)!.text);
          if (page.continuation !== null && buildReads.length < 20)
            reply([call('read_child_session', { agent_id: b, continuation: page.continuation })]);
          else {
            releaseReply();
            reply([{ type: 'text', text: 'DONE' }]);
          }
        }
      } else if (reactivating && role === 'b') {
        holdReply = () => reply([{ type: 'text', text: 'B_REACTIVATED' }]);
        reattachedReady();
      } else if (reactivating && role === 'root') {
        switch (reactivationStep++) {
          case 0:
            reply([call('send_message', { agent_id: b, message: 'normal child execution' })]);
            break;
          case 1:
            await reattached;
            reply([call('read_child_session', { agent_id: b, continuation: firstContinuation })]);
            break;
          case 2:
            reply([call('read_child_session', { agent_id: b })]);
            break;
          default:
            releaseReply();
            reply([{ type: 'text', text: 'DONE' }]);
        }
      } else if (query && !dispatched && role === 'root') {
        dispatched = true;
        reply([call(query.name, query.args)]);
      } else {
        reply([
          { type: 'thinking', thinking: 'PRIVATE_REASONING', signature: 'fixture' },
          { type: 'text', text: role === 'a' ? 'OWN_CHILD_TEXT' : 'DONE' },
        ]);
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const newRoot = async () => {
    const path = await mkdtemp(join(tmpdir(), 'dsh-inspection-'));
    directories.push(path);
    return path;
  };
  const emit = (event: WorkerEvent) => {
    observe(event);
    events.push(event);
    if (event.type === 'event' && event.event.type === 'thread.started')
      rootId = event.event.thread_id;
    if (event.type === 'canonical_event' && event.event.kind === 'session.thread_created') {
      const id = String(event.event.payload.native_session_id);
      if (event.event.payload.agent_name === 'inspection A') a = id;
      if (event.event.payload.agent_name === 'inspection B') b = id;
      if (event.event.payload.agent_name === 'inspection grandchild') grandchild = id;
    }
    if (event.type === 'orchestration_request') {
      if (holdApproval) {
        pendingApproval = event;
        approvalReady();
      } else void worker.handle({ type: 'orchestration_result', id: event.id, allowed: true });
    }
    if (event.type === 'tool_call')
      void worker.handle({
        type: 'tool_result',
        id: event.id,
        result: {
          content: [{ type: 'text', text: String(event.arguments.text) }],
        },
      });
    if (building && event.type === 'canonical_event' && event.event.kind === 'agent.tool_result') {
      const use = events.find(
        (candidate): candidate is Canonical =>
          candidate.type === 'canonical_event' &&
          candidate.event.id === event.event.payload.tool_use_id,
      );
      if (use?.event.payload.name === 'read_child_session' && use.event.subpath === '') {
        const payload = event.event.payload;
        const text = (payload.content as Array<{ text?: string }>)
          .map((block) => block.text ?? '')
          .join('');
        buildReads.push({ payload, text });
        if (buildReads.length === 1) releaseReply();
      }
    }
  };
  worker = new DshSdkWorker(emit);
  const input: StartCommand = {
    type: 'start',
    root: await newRoot(),
    sessionId: 'inspection-session',
    model: 'deepseek-v4-flash',
    system: 'Follow the current task. PRIVATE_SYSTEM_INSTRUCTIONS',
    apiKey: 'fixture-token',
    dshGatewayUrl: 'http://127.0.0.1:' + (server.address() as { port: number }).port,
    tools: [
      {
        name: 'echo',
        inputSchema: {
          type: 'object',
          properties: { text: { type: 'string' } },
          required: ['text'],
        },
      },
    ],
  };
  const canonical = (source: string) =>
    events.filter(
      (event): event is Canonical =>
        event.type === 'canonical_event' && event.event.payload.source_event_id === source,
    );
  const result = (source: string, name: string, caller = '', index = 0) => {
    const uses = canonical(source).filter(
      ({ event }) =>
        event.kind === 'agent.tool_use' &&
        event.payload.name === name &&
        event.subpath === (caller ? 'subagents/' + caller : ''),
    );
    expect(uses.length).toBeGreaterThan(index);
    const results = canonical(source).filter(
      ({ event }) =>
        event.kind === 'agent.tool_result' && event.payload.tool_use_id === uses[index]!.event.id,
    );
    expect(results).toHaveLength(1);
    const payload = results[0]!.event.payload;
    const text = (payload.content as Array<{ type: string; text?: string }>)
      .map((block) => block.text ?? '')
      .join('');
    return { payload, text };
  };
  const inspect = async (source: string, next: Query) => {
    query = next;
    dispatched = false;
    await worker.handle({
      type: 'submit',
      text: source,
      sourceEventId: source,
    });
    query = undefined;
    expect(dispatched).toBe(true);
    return result(source, next.name);
  };
  const reactivate = async (source: string) => {
    reactivating = true;
    reactivationStep = 0;
    reattached = new Promise<void>((resolve) => {
      reattachedReady = resolve;
    });
    await worker.handle({
      type: 'submit',
      text: 'reactivate and inspect child',
      sourceEventId: source,
    });
    reactivating = false;
    const obsolete = result(source, 'read_child_session', '', 0);
    expect(obsolete.payload.is_error).toBe(true);
    expect(obsolete.text).toContain('Invalid inspection continuation; retry without continuation');
    const fresh = result(source, 'read_child_session', '', 1);
    expect(fresh.payload.is_error).toBe(false);
    expect(JSON.parse(fresh.text)).toMatchObject({
      history_source: 'live',
      child: { state: 'running', history_availability: 'available' },
    });
  };
  try {
    await worker.handle(input);
    await worker.handle({
      type: 'submit',
      text: 'ROOT_SECRET build a child tree',
      sourceEventId: 'evt_build',
    });
    building = false;
    expect(events.filter((event) => event.type === 'failure')).toEqual([]);
    expect([a, b, grandchild].every(Boolean)).toBe(true);
    expect(buildReads.length).toBeGreaterThan(1);
    expect(
      canonical('evt_build').filter(
        ({ event }) =>
          event.subpath === '' &&
          event.kind === 'agent.tool_use' &&
          event.payload.name === 'read_child_session',
      ),
    ).toHaveLength(requestedBuildReads);
    expect(buildReads).toHaveLength(requestedBuildReads);
    for (let index = 0; index < requestedBuildReads; index++)
      result('evt_build', 'read_child_session', '', index);
    const running = buildReads[0]!;
    expect(running.payload.is_error).toBe(false);
    const first = JSON.parse(running.text);
    expect(first.child.state).toBe('running');
    expect(first.history_source).toBe('live');
    expect(first.records.length).toBeGreaterThan(0);
    expect(first.continuation).toEqual(expect.any(String));
    firstContinuation = first.continuation;
    expect(running.text).not.toContain('B_PROGRESS');
    expect(running.text).not.toMatch(/ROOT_SECRET|PRIVATE_REASONING|PRIVATE_SYSTEM_INSTRUCTIONS/);
    const list = JSON.parse(
      (await inspect('evt_list', { name: 'list_child_sessions', args: {} })).text,
    );
    expect(list.children.map((child: { agent_id: string }) => child.agent_id).sort()).toEqual(
      [a, b, grandchild].sort(),
    );
    expect(list.catalog_complete).toBe(true);
    const own = await inspect('evt_own', { name: 'read_child_session', args: { agent_id: a } });
    expect(JSON.parse(own.text)).toMatchObject({
      child: { state: 'dormant', history_availability: 'unavailable' },
      history_source: null,
      records: [],
      continuation: null,
    });
    const records: Array<{ source_id: string; kind: string; content: unknown }> = [];
    const identities = new Set<string>();
    for (const read of buildReads) {
      expect(read.payload.is_error).toBe(false);
      const page = JSON.parse(read.text);
      expect(page.child.agent_id).toBe(b);
      expect(page.child.state).toBe('running');
      expect(page.history_source).toBe('live');
      expect(read.text).not.toMatch(/ROOT_SECRET|PRIVATE_REASONING|PRIVATE_SYSTEM_INSTRUCTIONS/);
      for (const record of page.records) {
        expect(identities.has(record.source_id)).toBe(false);
        identities.add(record.source_id);
        records.push(record);
      }
    }
    expect(JSON.parse(buildReads.at(-1)!.text).continuation).toBeNull();
    expect(
      records.some(
        (record) =>
          record.kind === 'assistant/message' &&
          Array.isArray(record.content) &&
          record.content.some((block) => block.type === 'text' && block.text === 'B_PROGRESS'),
      ),
    ).toBe(true);
    expect(records.filter((record) => record.kind === 'tool/call')).toHaveLength(56);
    const nestedList = JSON.parse(result('evt_build', 'list_child_sessions', a).text);
    expect(nestedList.children.map((child: { agent_id: string }) => child.agent_id)).toEqual([
      grandchild,
    ]);
    const leaf = JSON.parse(result('evt_build', 'list_child_sessions', grandchild).text);
    expect(leaf.children).toEqual([]);
    const denied = result('evt_build', 'read_child_session', a, 0);
    const unknown = result('evt_build', 'read_child_session', a, 1);
    const ancestor = result('evt_build', 'read_child_session', a, 2);
    expect(denied.payload.is_error).toBe(true);
    expect(unknown.text).toBe(denied.text);
    expect(ancestor.text).toBe(denied.text);
    const completed = await inspect('evt_completed_b', {
      name: 'read_child_session',
      args: { agent_id: b, continuation: firstContinuation },
    });
    expect(completed.payload.is_error).toBe(false);
    expect(JSON.parse(completed.text)).toMatchObject({
      child: { agent_id: b, state: 'dormant', history_reason: 'source_not_attached' },
      history_source: null,
      records: [],
      continuation: null,
    });
    // Replacing B's activation alone must invalidate the cursor, without a worker restart.
    await reactivate('evt_same_worker_reattach');
    await worker.handle({ type: 'archive_fence', threadId: a });
    const archived = JSON.parse(
      (
        await inspect('evt_archived_read', {
          name: 'read_child_session',
          args: { agent_id: grandchild },
        })
      ).text,
    );
    expect(archived.child.archived).toBe(true);
    expect(archived).toMatchObject({
      history_source: null,
      records: [],
      child: { state: 'dormant', history_availability: 'unavailable' },
    });
    await expect(
      worker.handle({
        type: 'submit',
        text: 'invalid caller',
        sourceEventId: 'evt_archived_caller',
        threadId: a,
      }),
    ).rejects.toThrow('archived');
    const checkpoint = events
      .filter(
        (event): event is Extract<WorkerEvent, { type: 'checkpoint' }> =>
          event.type === 'checkpoint',
      )
      .at(-1)!.checkpoint;
    await worker.close();
    worker = new DshSdkWorker(emit);
    await worker.handle({ ...input, root: await newRoot(), checkpoint });
    const before = requests.length;
    const beforeEvents = events.length;
    const restored = JSON.parse(
      (await inspect('evt_restored_list', { name: 'list_child_sessions', args: {} })).text,
    );
    expect(restored.children).toHaveLength(3);
    for (const child of restored.children) {
      expect(child.state).toBe('dormant');
      expect(child.history_availability).toBe('unavailable');
      expect(child.last_stop_reason).toBeUndefined();
    }
    const unavailable = JSON.parse(
      (
        await inspect('evt_unavailable', {
          name: 'read_child_session',
          args: { agent_id: b, continuation: first.continuation },
        })
      ).text,
    );
    expect(unavailable).toMatchObject({
      history_source: null,
      records: [],
      continuation: null,
      child: { history_availability: 'unavailable' },
    });
    // Each submit has a tool-request response and a final response; no hidden child request.
    expect(requests.slice(before)).toEqual(['root', 'root', 'root', 'root']);
    expect(
      events
        .slice(beforeEvents)
        .filter(
          (event) =>
            event.type === 'canonical_event' &&
            (event.event.subpath !== '' || event.event.kind === 'session.thread_created'),
        ),
    ).toEqual([]);
    await reactivate('evt_reattach');
    expect(events.filter((event) => event.type === 'failure')).toEqual([]);
    holdApproval = true;
    query = { name: 'read_child_session', args: { agent_id: b } };
    dispatched = false;
    const retirementEvents = events.length;
    const pending = worker.handle({
      type: 'submit',
      text: 'pending read',
      sourceEventId: 'evt_host_retired',
    });
    // Observe rejection immediately: retirement can close the transport before close() returns.
    const settlement = pending.then(
      () => 'resolved',
      () => 'rejected',
    );
    await approvalWaiting;
    expect(pendingApproval).toMatchObject({
      name: 'read_child_session',
      arguments: { agent_id: b },
    });
    const requestCount = requests.length;
    await worker.close();
    // The worker reports transport failure as events and resolves its submit.
    expect(await settlement).toBe('resolved');
    expect(events.slice(retirementEvents)).toContainEqual({
      type: 'failure',
      fatal: true,
      message: 'DSH worker transport failed',
    });
    expect(events.slice(retirementEvents)).toContainEqual({ type: 'done' });
    const closedEventCount = events.length;
    await worker.handle({ type: 'orchestration_result', id: pendingApproval!.id, allowed: true });
    expect(events).toHaveLength(closedEventCount);
    expect(requests).toHaveLength(requestCount);
    expect(
      canonical('evt_host_retired').filter(
        ({ event }) => event.kind === 'agent.tool_result' && event.payload.is_error !== true,
      ),
    ).toEqual([]);
    return events;
  } finally {
    holdReply?.();
    await worker.close();
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await Promise.all(
      directories.map((directory) => rm(directory, { recursive: true, force: true })),
    );
  }
}
