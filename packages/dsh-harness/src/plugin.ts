// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Context } from '@deepseek-ai/cordis';
import type { Agent, AgentHandle } from '@deepseek-ai/dsh-agent';
import type {} from '@deepseek-ai/dsh-session-projection';
import type {} from '@deepseek-ai/dsh-session-query';
import type { SubagentPromptRequestId } from '@deepseek-ai/dsh-subagent';
import { apply as delegation } from '@deepseek-ai/dsh-tool-subagent';
import { apply as control } from '@deepseek-ai/dsh-tool-subagent-control';
import { SessionId, type Session } from '@deepseek-ai/dsh-session';
import { createUserMessage, ReasoningEffortId } from '@deepseek-ai/dsh-llm';
import { registerDeepSeekProvider, resolveAdapterOptions } from '@deepseek-ai/dsh-llm-deepseek';
import { DSH_MODEL } from '@orca/harness-catalog';
import { registerManagedPi } from './pi-adapter.js';
import { JsonRpcLineTransport } from '@deepseek-ai/dsh-sdk-protocol';
import { createHash, randomUUID } from 'node:crypto';
import { AsyncLocalStorage } from 'node:async_hooks';
import { readFile, readdir, lstat } from 'node:fs/promises';
import { join } from 'node:path';
import type { SdkTerminalUsage } from '@orca/sdk-harness';
import { assertSdkTerminalUsage } from '@orca/sdk-harness';
import { captureCheckpoint, MAX_STATE_BYTES, MAX_STATE_FILES, NATIVE_ID } from './checkpoint.js';
import { boundedLines } from './bounds.js';
import { gatewayBase, installManagedFetch } from './transport.js';
import { orchestration, orchestrationArguments, type DshStartCommand } from './orchestration.js';
import { archivedSubpaths, assertNotArchived } from './archive.js';
import {
  catalogName,
  turnEndPayload,
  isSurfaceReplacement,
  CompactionFacts,
} from './event-facts.js';

export const name = 'orca-managed';
export const inject = [
  'agents',
  'llm',
  'tools',
  'sessions',
  'sessionPersistence',
  'subagents',
  'sessionQuery',
];
const zero = (): SdkTerminalUsage => ({
  input_tokens: 0,
  output_tokens: 0,
  cached_input_tokens: 0,
  cache_write_input_tokens: 0,
});

/** Single stdio owner; native DSH owns scheduling, lineage, context and cancellation. */
export function apply(ctx: Context): void {
  const inputStream = process.stdin.pipe(boundedLines());
  const rpc = new JsonRpcLineTransport(inputStream, process.stdout);
  let input: DshStartCommand | undefined;
  let nativeProvider = 'orca-deepseek';
  let managedPi: { api: string; baseUrl: string } | undefined;
  let archived = new Set<string>();
  let handle: AgentHandle | undefined;
  let active = false;
  let interrupted = false;
  let targetInterrupted = false;
  let resumed = false;
  let failed: Error | undefined;
  let retired = false;
  let sourceEventId = '';
  const usage = new Map<string, { counts: SdkTerminalUsage; started: number; completed: number }>();
  const terminals = new Map<string, string>();
  const calls = new Map<string, { seq: number; name: string }>();
  const compactions = new CompactionFacts();
  const declaredChildren = new Map<string, string>();
  const pendingChildEvents = new Map<string, object[]>();
  const toolOrigin = new AsyncLocalStorage<string>();
  const emit = (event: object): void => rpc.notify('orca.event', event);
  const subpath = (id: string): string => (id === handle?.agent.id ? '' : 'subagents/' + id);
  const assertAvailable = (id: string): void =>
    assertNotArchived(subpath(id), archived, (path) => {
      const parent = declaredChildren.get(path.slice('subagents/'.length));
      return parent === undefined ? undefined : subpath(parent);
    });
  const fenceThread = (id: unknown): void => {
    if (typeof id !== 'string' || !NATIVE_ID.test(id) || !declaredChildren.has(id))
      throw new Error('DSH archive fence target outside tree');
    archived.add(subpath(id));
  };
  const identity = (id: string, fact: string): string =>
    'evt_' +
    createHash('sha256')
      .update(JSON.stringify([input!.sessionId, sourceEventId, id, fact]))
      .digest('hex');
  const canonical = (
    id: string,
    fact: string,
    kind: string,
    payload: Record<string, unknown>,
    parent?: string,
  ): string => {
    const eventId = identity(id, fact);
    parent ??= ctx.agents.get(SessionId(id))?.session.header.parentSession;
    if (
      kind === 'session.thread_created' &&
      (!parent || (!declaredChildren.has(id) && declaredChildren.size >= 127))
    )
      throw new Error('Invalid or oversized DSH child tree');
    const notification = {
      type: 'canonical_event',
      event: {
        id: eventId,
        subpath: subpath(id),
        kind,
        payload: {
          ...payload,
          native_session_id: id,
          source_event_id: sourceEventId,
          turn_event_id: sourceEventId,
          subpath: subpath(id),
          ...(parent ? { parent_subpath: subpath(parent) } : {}),
        },
      },
    };
    if (parent && !declaredChildren.has(id) && kind !== 'session.thread_created') {
      const pending = pendingChildEvents.get(id) ?? [];
      if (pending.length >= 32 || pendingChildEvents.size >= 127)
        throw new Error('DSH child declaration did not precede execution');
      pending.push(notification);
      pendingChildEvents.set(id, pending);
    } else {
      emit(notification);
      if (kind === 'session.thread_created') {
        declaredChildren.set(id, parent!);
        for (const pending of pendingChildEvents.get(id) ?? []) emit(pending);
        pendingChildEvents.delete(id);
      }
    }
    return eventId;
  };
  const record = (id: string) => {
    if (!active || !NATIVE_ID.test(id) || (id !== handle?.agent.id && !declaredChildren.has(id)))
      throw new Error('DSH request outside owned turn');
    let value = usage.get(id);
    if (!value) {
      value = { counts: zero(), started: 0, completed: 0 };
      usage.set(id, value);
      canonical(id, 'model:start', 'span.model_request_start', {
        model_observation_kind: 'turn_model_summary',
        provider: input!.modelProvider ?? 'deepseek',
        model: input!.model,
      });
    }
    return value;
  };
  const releaseFetch = installManagedFetch(
    () => ({
      origin: input!.dshGatewayUrl!,
      token: input!.apiKey,
      sessionId: input!.sessionId,
      ...(managedPi ? { ...managedPi, provider: input!.modelProvider!, model: input!.model } : {}),
    }),
    (next, id) => {
      const value = record(id);
      value.completed++;
      value.counts.input_tokens += next.input_tokens;
      value.counts.output_tokens += next.output_tokens;
      value.counts.cached_input_tokens += next.cached_input_tokens;
      value.counts.cache_write_input_tokens! += next.cache_write_input_tokens ?? 0;
      if (next.cache_write_input_tokens_1h !== undefined)
        value.counts.cache_write_input_tokens_1h =
          (value.counts.cache_write_input_tokens_1h ?? 0) + next.cache_write_input_tokens_1h;
      assertSdkTerminalUsage(value.counts);
    },
    (id) => {
      assertAvailable(id);
      record(id).started++;
    },
  );

  const setup = async (agentCtx: Context, agent: Agent): Promise<void> => {
    if (!input) throw new Error('DSH composition before initialization');
    if (agent.options.provider !== nativeProvider || agent.options.model !== input.model)
      throw new Error('DSH child model expansion rejected');
    for (const tool of input.tools) {
      if (orchestration.has(tool.name) || tool.name === 'web_fetch')
        throw new Error('Reserved DSH tool name');
      agentCtx.tools.register({
        name: tool.name,
        description: tool.description ?? tool.name,
        parameters: tool.inputSchema,
        output: {
          schema: {},
          render: (_args, value) =>
            (value as { content: Array<{ type: string; text?: string }> }).content.map((block) => {
              if (block.type !== 'text' || typeof block.text !== 'string')
                throw new Error('DSH managed tools require text results');
              return { type: 'text' as const, text: block.text };
            }),
        },
        execute: async (args, exec) => {
          assertAvailable(String(agent.id));
          const call = calls.get(agent.id + ':' + exec.callId);
          if (
            !active ||
            !call ||
            (agent.session.header.parentSession && !declaredChildren.has(String(agent.id)))
          )
            throw new Error('DSH tool source unavailable');
          const result = await rpc.request(
            'orca.tool',
            {
              id: identity(agent.id, 'bridge:' + call.seq),
              name: tool.name,
              arguments: args,
              origin: {
                eventId: identity(agent.id, 'tool:' + call.seq),
                subpath: subpath(agent.id),
                ...(agent.session.header.parentSession
                  ? { parentSubpath: subpath(agent.session.header.parentSession) }
                  : {}),
              },
            },
            exec.signal,
          );
          if ((result as { isError?: boolean }).isError)
            throw new Error(JSON.stringify((result as { content: unknown }).content));
          return result;
        },
      });
    }
    await agentCtx.plugin({
      name: 'orca-orchestration',
      inject: ['tools', 'subagents', 'systemPrompt', 'sessionProjections', 'llm'],
      apply(runtimeCtx: Context) {
        delegation(
          runtimeCtx,
          { provider: 'spawn', toolName: 'spawn', agentOptions: agent.options, maxDepth: 2 },
          agent.session,
        );
        delegation(
          runtimeCtx,
          { provider: 'fork', toolName: 'fork', agentOptions: agent.options, maxDepth: 2 },
          agent.session,
        );
        control(runtimeCtx);
      },
    });
    agentCtx.on('tools/pre-execute', async (exec, next) => {
      assertAvailable(String(agent.id));
      if (!orchestration.has(exec.name) && !input!.tools.some((tool) => tool.name === exec.name))
        throw new Error('DSH tool outside fixed grant');
      if (orchestration.has(exec.name)) {
        const call = calls.get(agent.id + ':' + exec.callId);
        const source = sourceEventId;
        const args = orchestrationArguments(exec.name, exec.arguments);
        const validate = async () => {
          assertAvailable(String(agent.id));
          if (
            !active ||
            retired ||
            interrupted ||
            exec.signal.aborted ||
            source !== sourceEventId ||
            !call ||
            call.name !== exec.name ||
            calls.get(agent.id + ':' + exec.callId) !== call ||
            exec.agent !== agent ||
            ctx.agents.get(agent.id) !== agent ||
            (agent.id !== handle?.agent.id && !declaredChildren.has(String(agent.id))) ||
            JSON.stringify(orchestrationArguments(exec.name, exec.arguments)) !==
              JSON.stringify(args)
          )
            throw new Error('DSH orchestration source changed or cancelled');
          if (args.agent_id !== undefined) {
            if (!NATIVE_ID.test(args.agent_id)) throw new Error('Invalid DSH control target');
            assertAvailable(args.agent_id);
            const descendants = await ctx.subagents.listDescendants(agent.id);
            const direct = await ctx.subagents.listChildren(agent.id);
            const allowed =
              exec.name === 'interrupt_agent'
                ? descendants.some((row) => row.id === args.agent_id)
                : direct.some((row) => row.id === args.agent_id && row.mode === 'continuable') ||
                  (agent.session.header.parentSession === args.agent_id &&
                    declaredChildren.has(String(agent.id)));
            if (
              !allowed ||
              (args.agent_id !== handle?.agent.id && !declaredChildren.has(args.agent_id))
            )
              throw new Error('DSH control target outside owned authority');
          }
          assertAvailable(String(agent.id));
          if (args.agent_id !== undefined) assertAvailable(args.agent_id);
          exec.signal.throwIfAborted();
        };
        await validate();
        const result = (await rpc.request(
          'orca.authorize',
          {
            id: identity(agent.id, 'authorize:' + call!.seq),
            name: exec.name,
            arguments: args,
            origin: {
              eventId: identity(agent.id, 'tool:' + call!.seq),
              subpath: subpath(agent.id),
              ...(agent.session.header.parentSession
                ? { parentSubpath: subpath(agent.session.header.parentSession) }
                : {}),
            },
          },
          exec.signal,
        )) as { allowed?: unknown; reason?: unknown };
        if (result?.allowed !== true)
          throw new Error(
            typeof result?.reason === 'string' ? result.reason : 'DSH orchestration denied',
          );
        await validate();
      }
      return next();
    });
  };
  // Native child scopes are fresh. The loop holds its inbox until this awaited listener finishes.
  const releaseCreated = ctx.on('agent/created', async ({ agent }) => {
    const parent = agent.session.header.parentSession;
    if (
      agent.session.header.origin === 'subagent' &&
      parent &&
      (parent === handle?.agent.id || declaredChildren.has(parent))
    )
      await setup(agent.ctx, agent);
    return undefined;
  });
  const releaseToolOrigin = ctx.on('tools/execute', async (exec, next) => {
    if (
      exec.agent &&
      (exec.agent.id === handle?.agent.id || declaredChildren.has(String(exec.agent.id)))
    ) {
      assertAvailable(String(exec.agent.id));
      if (orchestration.has(exec.name)) {
        const args = orchestrationArguments(exec.name, exec.arguments);
        if (args.agent_id !== undefined) assertAvailable(args.agent_id);
      }
    }
    const call = calls.get(exec.agent?.id + ':' + exec.callId);
    return call && orchestration.has(exec.name)
      ? toolOrigin.run(identity(String(exec.agent!.id), 'tool:' + call.seq), next)
      : next();
  });
  const releaseEvents = ctx.on('session/event', (session, event) => {
    if (!active || !session.isOwnSeq(event.seq)) return;
    if (
      session.id !== handle?.agent.id &&
      !declaredChildren.has(String(session.id)) &&
      (!session.header.parentSession ||
        (session.header.parentSession !== handle?.agent.id &&
          !declaredChildren.has(session.header.parentSession)))
    )
      return;
    const parent = session.header.parentSession;
    const id = String(session.id);
    const compacted = compactions.accept(id, event);
    if (compacted)
      canonical(id, 'compacted:' + event.seq, 'agent.thread_context_compacted', compacted, parent);
    // Surface replacements and their sourceEventSeqs refer to old history, not new executions.
    // In particular, a rewritten result must not consume a live call with the same native ID.
    if (isSurfaceReplacement(event)) return;
    if (event.type === 'turn/end') {
      terminals.set(id, event.data.reason.kind);
      for (const key of calls.keys()) if (key.startsWith(id + ':')) calls.delete(key);
      if (parent)
        canonical(
          id,
          'idle:' + event.seq,
          'session.thread_status_idle',
          turnEndPayload(event.data.reason),
          parent,
        );
    }
    if (event.type === 'turn/start' && parent)
      canonical(id, 'running:' + event.seq, 'session.thread_status_running', {}, parent);
    if (event.type === 'subagent/catalog')
      canonical(
        event.data.childId,
        'created:' + id + ':' + event.seq,
        'session.thread_created',
        {
          native_source: { session_id: id, seq: event.seq },
          parent_tool_use_id: toolOrigin.getStore(),
          ...catalogName(event.data.label),
        },
        id,
      );
    if (event.type === 'assistant/message')
      for (const [index, block] of event.data.message.content.entries()) {
        if (block.type === 'text' || block.type === 'reasoning')
          canonical(
            id,
            event.seq + ':' + index,
            block.type === 'text' ? 'agent.message' : 'agent.thinking',
            {
              content: [
                block.type === 'text'
                  ? { type: 'text', text: block.text }
                  : { type: 'thinking', thinking: block.text },
              ],
            },
            parent,
          );
      }
    if (event.type === 'tool/call') {
      calls.set(id + ':' + event.data.callId, { seq: event.seq, name: event.data.name });
      if (orchestration.has(event.data.name))
        canonical(
          id,
          'tool:' + event.seq,
          'agent.tool_use',
          { name: event.data.name, input: JSON.parse(event.data.arguments) },
          parent,
        );
    }
    if (event.type === 'tool/result') {
      const call = calls.get(id + ':' + event.data.message.toolCallId);
      if (call && orchestration.has(call.name))
        canonical(
          id,
          'result:' + event.seq,
          'agent.tool_result',
          {
            tool_use_id: identity(id, 'tool:' + call.seq),
            content: event.data.message.content,
            is_error: event.data.message.isError ?? false,
          },
          parent,
        );
      calls.delete(id + ':' + event.data.message.toolCallId);
    }
    if (event.type === 'user/message') {
      const source = event.data.source as { kind: string; senderSessionId?: string };
      if (source.kind === 'subagent-settled' && source.senderSessionId) {
        const sender = source.senderSessionId;
        if (
          !declaredChildren.has(sender) ||
          declaredChildren.get(sender) !== id ||
          typeof event.data.id !== 'string' ||
          !event.data.id ||
          event.data.id.length > 128
        )
          throw new Error('Invalid DSH settlement source');
        canonical(
          id,
          'settled:' + event.data.id,
          'agent.thread_message_received',
          {
            native_message_id: event.data.id,
            message_origin: 'runtime_settlement',
            sender_subpath: subpath(sender),
            recipient_subpath: subpath(id),
            content: [{ type: 'text', text: 'Child ' + sender + ' settled.' }],
          },
          parent,
        );
      } else if (source.kind === 'agent-message' && source.senderSessionId) {
        const payload = {
          native_message_id: event.data.id,
          sender_subpath: subpath(source.senderSessionId),
          recipient_subpath: subpath(id),
          content: event.data.content,
        };
        canonical(
          source.senderSessionId,
          'sent:' + id + ':' + event.seq,
          'agent.thread_message_sent',
          payload,
        );
        canonical(id, 'received:' + event.seq, 'agent.thread_message_received', payload, parent);
      }
    }
  });
  const releaseErrors = ctx.on('agent/error', ({ agent, error }) => {
    // Child errors are delivered to their parent; only root failure decides the whole turn.
    if (active && agent === handle?.agent)
      failed = error instanceof Error ? error : new Error('DSH agent failed');
  });
  const waitTree = async (): Promise<void> => {
    do {
      await handle!.agent.whenIdle();
    } while (await ctx.subagents.waitForChildren(handle!.agent));
  };
  const tree = async () => {
    const descendants = await ctx.subagents.listDescendants(handle!.agent.id);
    if (
      descendants.length + 2 > MAX_STATE_FILES ||
      descendants.some((row) => row.kind !== 'child' || row.mode !== 'continuable')
    )
      throw new Error('Unsupported DSH native tree');
    return descendants;
  };
  const checkpoint = async () => {
    const ids = new Set([String(handle!.agent.id), ...(await tree()).map((row) => String(row.id))]);
    for (const id of ids) {
      const session: Session | undefined = ctx.agents.get(SessionId(id))?.session;
      if (session && !(await ctx.sessions.flush(session)))
        throw new Error('DSH persistence flush unavailable');
    }
    const root = join(process.env.DSH_HOME!, 'sessions');
    const files: Record<string, Buffer> = {};
    let size = 0;
    for (const project of await readdir(root)) {
      if (!(await lstat(join(root, project))).isDirectory())
        throw new Error('Invalid DSH project directory');
      for (const id of await readdir(join(root, project))) {
        if (!ids.has(id)) continue;
        if (!(await lstat(join(root, project, id))).isDirectory())
          throw new Error('Invalid DSH session directory');
        const path = join(root, project, id, 'session.v4.jsonl');
        const stat = await lstat(path);
        size += stat.size;
        if (!stat.isFile() || size > MAX_STATE_BYTES)
          throw new Error('Invalid or oversized DSH tree state');
        const data = await readFile(path);
        if (data.length !== stat.size) throw new Error('DSH state changed during export');
        files[project + '/' + id + '/session.v4.jsonl'] = data;
      }
    }
    if (Object.keys(files).length !== ids.size) throw new Error('Incomplete DSH tree state');
    return captureCheckpoint(
      String(handle!.agent.id),
      input!.system,
      files,
      input!.model,
      input!.modelProvider ?? 'deepseek',
    );
  };
  inputStream.on('error', () => {
    void ctx.root.fiber.dispose();
  });
  rpc.onRequest(async (method, params) => {
    if (method === 'initialize') {
      await ctx.get('loader')?.await();
      if (input) throw new Error('DSH already initialized');
      input = params as unknown as DshStartCommand;
      archived = archivedSubpaths(input.archivedThreadSubpaths);
      gatewayBase(input.dshGatewayUrl);
      const provider = input.modelProvider ?? 'deepseek';
      const legacy = provider === 'deepseek' && input.model === DSH_MODEL;
      if (legacy) {
        registerDeepSeekProvider(ctx, 'orca-deepseek', {
          options: () =>
            resolveAdapterOptions({
              baseURL: gatewayBase(input!.dshGatewayUrl),
              retryPolicy: { mode: 'normal', maxRetries: 0 },
              streamIdleTimeoutMs: 600000,
            }),
          resolveAuth: async () => ({
            headers: {
              Authorization: 'Bearer ' + input!.apiKey,
              'X-Orca-Session-Id': input!.sessionId,
            },
          }),
        });
      } else {
        nativeProvider = provider;
        managedPi = registerManagedPi(ctx, provider, input.model);
      }
      const options = {
        provider: nativeProvider,
        model: input.model,
        ...(input.effort ? { reasoningEffort: ReasoningEffortId(input.effort) } : {}),
      };
      const id = SessionId(input.checkpoint?.threadId ?? randomUUID());
      resumed = !!input.checkpoint;
      handle = input.checkpoint
        ? await ctx.agents.resume({ resumeSessionId: id, agentOptions: options, setup })
        : await ctx.agents.create({ sessionId: id, agentOptions: options, setup });
      const threads = (await tree()).map((row) => ({
        subpath: subpath(row.id),
        parentSubpath: subpath(row.parentId),
      }));
      for (const thread of threads)
        declaredChildren.set(
          thread.subpath.slice('subagents/'.length),
          thread.parentSubpath ? thread.parentSubpath.slice('subagents/'.length) : String(id),
        );
      return { threadId: String(id), threads };
    }
    if (!input || !handle) throw new Error('DSH not initialized');
    if (method === 'archive_fence') {
      fenceThread(params.threadId);
      return {}; // ACK only the monotonic fence, never native or physical tool termination.
    }
    if (method === 'refresh') {
      if (active) throw new Error('DSH refresh requires idle');
      input = { ...input, ...params };
      gatewayBase(input.dshGatewayUrl);
      return {};
    }
    if (method === 'interrupt') {
      if (params.archived !== undefined && typeof params.archived !== 'boolean')
        throw new Error('Invalid DSH archive command');
      if (params.archived === true) {
        // Install before any awaited native lookup/drain can let queued work resume.
        fenceThread(params.threadId);
      }
      if (params.threadId) {
        const row = (await tree()).find((child) => child.id === params.threadId);
        if (!row) throw new Error('DSH interrupt target outside tree');
        targetInterrupted = true;
        const parent = ctx.agents.get(row.parentId);
        if (parent) await ctx.subagents.drainChildren(parent, [row.id]);
      } else {
        interrupted = true;
        handle.agent.cancel({ kind: 'user' });
        await ctx.subagents.drainChildren(
          handle.agent,
          (await ctx.subagents.listChildren(handle.agent.id)).map((child) => child.id),
        );
        await waitTree();
      }
      return {};
    }
    if (method !== 'submit') throw new Error('Unsupported DSH bridge method');
    if (
      active ||
      retired ||
      typeof params.sourceEventId !== 'string' ||
      !params.sourceEventId.startsWith('evt_') ||
      params.sourceEventId.length <= 4
    )
      throw new Error('DSH requires accepted root sourceEventId');
    sourceEventId = params.sourceEventId;
    active = true;
    interrupted = false;
    targetInterrupted = false;
    failed = undefined;
    usage.clear();
    terminals.clear();
    calls.clear();
    compactions.clear();
    let quiesced = false;
    try {
      assertAvailable(
        typeof params.threadId === 'string' ? params.threadId : String(handle.agent.id),
      );
      emit({ type: 'event', event: { type: 'turn.started' } });
      const content = [{ type: 'text' as const, text: String(params.text) }];
      if (resumed)
        content.unshift({
          type: 'text',
          text: 'Orca runtime notice: native conversation context was restored from a committed checkpoint. An uncommitted prior turn may have produced external side effects; check them before proceeding. History restoration does not roll back files or external services. Temporary workspace files and processes may be absent; verify them before continuing.',
        });
      if (params.threadId) {
        const row = (await tree()).find((child) => child.id === params.threadId);
        if (!row || !ctx.agents.get(row.parentId))
          throw new Error('DSH follow-up parent unavailable');
        assertAvailable(String(row.id));
        await ctx.subagents.prompt(
          {
            requestId: sourceEventId as SubagentPromptRequestId,
            parentSessionId: row.parentId,
            childSessionId: row.id,
            mode: 'continuable',
            delivery: 'queue',
            content,
          },
          AbortSignal.timeout(600000),
        );
      } else
        handle.agent.followup(
          createUserMessage({ content, source: { kind: 'user', rpcId: sourceEventId } }),
        );
      resumed = false;
      await waitTree();
      quiesced = true;
      if (failed) throw failed;
      if (
        interrupted ||
        !usage.size ||
        (usage.has(String(handle.agent.id)) &&
          terminals.get(String(handle.agent.id)) !== 'completed') ||
        [...usage].some(([id, value]) => value.started !== value.completed || !terminals.has(id))
      )
        throw new Error('DSH tree incomplete or terminal usage unavailable');
      const total = zero();
      for (const value of usage.values())
        for (const key of Object.keys(value.counts) as Array<keyof SdkTerminalUsage>)
          total[key] = (total[key] ?? 0) + (value.counts[key] ?? 0);
      assertSdkTerminalUsage(total);
      emit({ type: 'checkpoint', checkpoint: await checkpoint() });
      for (const [id, value] of usage)
        canonical(id, 'model:end', 'span.model_request_end', {
          model_request_start_id: identity(id, 'model:start'),
          model_observation_kind: 'turn_model_summary',
          provider: input.modelProvider ?? 'deepseek',
          model: input.model,
          is_error: terminals.get(id) !== 'completed',
          model_usage: {
            input_tokens: value.counts.input_tokens - value.counts.cached_input_tokens,
            output_tokens: value.counts.output_tokens,
            cache_read_input_tokens: value.counts.cached_input_tokens,
            cache_creation_input_tokens: value.counts.cache_write_input_tokens ?? 0,
          },
        });
      emit({ type: 'event', event: { type: 'turn.completed', usage: total } });
    } catch (error) {
      retired = true;
      // No terminal counters are invented on an aborted/failed request; host accounting remains unknown.
      if (targetInterrupted) await waitTree();
      else if (!quiesced) {
        handle.agent.cancel({ kind: 'user' });
        await ctx.subagents.drainChildren(
          handle.agent,
          (await ctx.subagents.listChildren(handle.agent.id)).map((child) => child.id),
        );
        await handle.agent.whenIdle();
      }
      emit({
        type: 'failure',
        fatal: true,
        message: error instanceof Error ? error.message : 'DSH tree failed',
      });
    } finally {
      calls.clear();
      compactions.clear();
      active = false;
      emit({ type: 'done' });
    }
    return {};
  });
  ctx.effect(() => {
    rpc.start();
    return async () => {
      handle?.agent.cancel({ kind: 'user' });
      if (handle) await ctx.subagents.drainDescendants([handle.agent]);
      await handle?.dispose();
      rpc.close();
      inputStream.destroy();
      releaseCreated();
      releaseToolOrigin();
      releaseEvents();
      releaseErrors();
      releaseFetch();
    };
  }, 'orca-managed.serve');
}
