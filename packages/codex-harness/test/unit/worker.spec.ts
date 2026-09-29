// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import { mkdtemp, mkdir, readFile, readdir, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import { dirname, join } from 'node:path';
import type { Codex, ThreadEvent } from '@openai/codex-sdk';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import {
  CodexSdkWorker,
  captureCheckpoint,
  restoreCheckpoint,
  transitionCodexCheckpointInstructions,
  type CodexCheckpoint,
  type CodexFactory,
  type StartCommand,
  type WorkerEvent,
} from '../../src/index.js';

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});
const rollout = 'sessions/2026/09/20/rollout-native-thread.jsonl';
const digest = (system: string) => createHash('sha256').update(system).digest('hex');
const history = Buffer.from('{"native":"history"}\n');

async function directory(): Promise<string> {
  const path = await mkdtemp(join(tmpdir(), 'orca-codex-worker-test-'));
  cleanup.push(() => rm(path, { recursive: true, force: true }));
  return path;
}
async function writeHistory(home: string): Promise<void> {
  await mkdir(dirname(join(home, rollout)), { recursive: true });
  await writeFile(join(home, rollout), history);
}
function fakeSdk(run?: (signal: AbortSignal) => AsyncIterable<ThreadEvent>) {
  const created: Array<{
    options: Parameters<CodexFactory>[0];
    threadOptions: unknown;
    resumedId?: string;
  }> = [];
  const makeCodex: CodexFactory = (options) => {
    const record: (typeof created)[number] = { options, threadOptions: undefined };
    created.push(record);
    const makeThread = (threadOptions: unknown, resumedId?: string) => {
      record.threadOptions = threadOptions;
      if (resumedId) record.resumedId = resumedId;
      let threadId = resumedId ?? null;
      return {
        get id() {
          return threadId;
        },
        async runStreamed(_text: string, input: { signal: AbortSignal }) {
          threadId = 'native-thread';
          await writeHistory(options!.env!.CODEX_HOME!);
          return {
            events: run
              ? run(input.signal)
              : (async function* () {
                  yield { type: 'thread.started', thread_id: threadId } as ThreadEvent;
                  yield {
                    type: 'turn.completed',
                    usage: { input_tokens: 11, cached_input_tokens: 3, output_tokens: 5 },
                  } as ThreadEvent;
                })(),
          };
        },
      };
    };
    return {
      startThread: (options: unknown) => makeThread(options),
      resumeThread: (id: string, options: unknown) => makeThread(options, id),
    } as unknown as Codex;
  };
  return { makeCodex, created };
}
async function setup(makeCodex: CodexFactory, receive?: (event: WorkerEvent) => void) {
  const events: WorkerEvent[] = [];
  const worker = new CodexSdkWorker((event) => {
    events.push(event);
    receive?.(event);
  }, makeCodex);
  cleanup.push(() => worker.close());
  const input: StartCommand = {
    type: 'start',
    root: await directory(),
    sessionId: 'session-private',
    model: 'gpt-5.4',
    effort: 'high',
    system: 'Original developer instructions',
    apiKey: 'old-scoped-token',
    baseUrl: 'http://gateway/v1',
    tools: [{ name: 'read', inputSchema: { type: 'object' } }],
  };
  await worker.handle(input);
  return { worker, events, input };
}

function relay(options: Parameters<CodexFactory>[0]) {
  const value = options!.configOverrides![0]!;
  return {
    url: value.match(/url="([^"]+)"/)![1]!,
    token: value.match(/Authorization="Bearer ([^"]+)"/)![1]!,
  };
}

describe('shared Codex worker lifecycle', () => {
  const validUsage = { input_tokens: 11, cached_input_tokens: 3, output_tokens: 5 };
  it.each([
    ['missing input', { cached_input_tokens: 0, output_tokens: 0 }],
    ['negative input', { ...validUsage, input_tokens: -1 }],
    ['negative output', { ...validUsage, output_tokens: -1 }],
    ['negative cache', { ...validUsage, cached_input_tokens: -1 }],
    ['NaN', { ...validUsage, input_tokens: Number.NaN }],
    ['fractional', { ...validUsage, cached_input_tokens: 0.5 }],
    ['unsafe', { ...validUsage, input_tokens: Number.MAX_SAFE_INTEGER + 1 }],
    ['cache exceeds input', { ...validUsage, cached_input_tokens: 12 }],
    ['invalid cache write', { ...validUsage, cache_write_input_tokens: -1 }],
    ['unsafe aggregate', { ...validUsage, input_tokens: Number.MAX_SAFE_INTEGER }],
  ])('rejects %s usage before forwarding or checkpointing native history', async (_, usage) => {
    const sdk = fakeSdk(async function* () {
      yield { type: 'turn.completed', usage } as ThreadEvent;
    });
    const { worker, events } = await setup(sdk.makeCodex);
    await worker.handle({ type: 'submit', text: 'invalid accounting' });
    expect(events).toEqual([
      { type: 'ready' },
      { type: 'failure', message: 'Invalid Codex SDK terminal usage', fatal: true },
      { type: 'done' },
    ]);
    await expect(worker.handle({ type: 'submit', text: 'do not continue' })).rejects.toThrow(
      'not ready',
    );
  });

  it.each([
    { input_tokens: 0, cached_input_tokens: 0, output_tokens: 0 },
    { input_tokens: 11, cached_input_tokens: 11, output_tokens: 0, cache_write_input_tokens: 2 },
  ])('accepts zero and fully cached valid terminal usage: %j', async (usage) => {
    const sdk = fakeSdk(async function* () {
      yield { type: 'turn.completed', usage } as ThreadEvent;
    });
    const { worker, events } = await setup(sdk.makeCodex);
    await worker.handle({ type: 'submit', text: 'valid accounting' });
    expect(events[1]).toEqual({ type: 'event', event: { type: 'turn.completed', usage } });
    expect(events.at(-2)?.type).toBe('checkpoint');
    expect(events.at(-1)?.type).toBe('done');
    expect(events.some((event) => event.type === 'failure')).toBe(false);
  });

  it('makes checkpoint export failures fatal and refuses every later turn', async () => {
    let runs = 0;
    const sdk = fakeSdk(async function* () {
      runs++;
      await rm(join(sdk.created[0]!.options!.env!.CODEX_HOME!, 'sessions'), { recursive: true });
      yield {
        type: 'turn.completed',
        usage: {
          input_tokens: 1,
          cached_input_tokens: 0,
          output_tokens: 1,
          cache_write_input_tokens: 0,
          reasoning_output_tokens: 0,
        },
      };
    });
    const { worker, events } = await setup(sdk.makeCodex);
    await worker.handle({ type: 'submit', text: 'turn with unavailable native history' });
    expect(events.at(-2)).toEqual({
      type: 'failure',
      message: 'Codex SDK native history could not be checkpointed',
      fatal: true,
    });
    expect(events.at(-1)).toEqual({ type: 'done' });
    await expect(
      worker.handle({ type: 'submit', text: 'must not run on undurable history' }),
    ).rejects.toThrow('not ready');
    expect(() => worker.refreshOptions({ apiKey: 'replacement' })).toThrow('idle');
    expect(runs).toBe(1);
  });

  it('joins concurrent startup before closing its private home and relay', async () => {
    const sdk = fakeSdk();
    const events: WorkerEvent[] = [];
    const worker = new CodexSdkWorker((event) => events.push(event), sdk.makeCodex);
    cleanup.push(() => worker.close());
    const input: StartCommand = {
      type: 'start',
      root: await directory(),
      sessionId: 'startup-race',
      model: 'gpt-5.4',
      system: '',
      apiKey: 'test',
      tools: [],
    };
    const starting = worker.handle(input);
    const rejected = expect(starting).rejects.toThrow('closed during startup');
    await expect(worker.handle(input)).rejects.toThrow('already started or closed');
    await worker.close();
    await rejected;
    const options = sdk.created[0]!.options!;
    await expect(stat(options.env!.CODEX_HOME!)).rejects.toMatchObject({ code: 'ENOENT' });
    await expect(fetch(relay(options).url)).rejects.toThrow();
    expect(events).toEqual([]);
  });

  it('refreshes scoped credentials while retaining native history and pinned instructions', async () => {
    const sdk = fakeSdk();
    const { worker, events, input } = await setup(sdk.makeCodex);
    await worker.handle({ type: 'submit', text: 'first turn' });
    expect(() => worker.refreshOptions({ apiKey: 'test', system: 'Changed instructions' })).toThrow(
      'cannot change developer instructions',
    );
    worker.refreshOptions({
      apiKey: 'replacement-scoped-token',
      baseUrl: 'http://replacement-gateway/v1',
      system: input.system,
    });
    await worker.handle({ type: 'submit', text: 'second turn' });
    expect(sdk.created).toHaveLength(2);
    const [first, second] = sdk.created;
    expect(second!.resumedId).toBe('native-thread');
    expect(second!.options).toMatchObject({
      apiKey: 'replacement-scoped-token',
      baseUrl: 'http://replacement-gateway/v1',
      config: { developer_instructions: input.system },
      env: first!.options!.env,
    });
    expect(second!.options!.configOverrides).toContainEqual(
      expect.stringContaining('base_url="http://replacement-gateway/v1"'),
    );
    expect(second!.options!.configOverrides![0]).toBe(first!.options!.configOverrides![0]);
    expect(second!.threadOptions).toMatchObject({
      workingDirectory: input.root,
      approvalPolicy: 'never',
      sandboxMode: 'read-only',
      webSearchMode: 'disabled',
      modelReasoningEffort: 'high',
    });
    expect(Object.keys(second!.options!.env!).sort()).toEqual(['CODEX_HOME', 'HOME', 'PATH']);
    expect(events.filter((event) => event.type === 'failure')).toEqual([]);
    const checkpoints = events.filter((event) => event.type === 'checkpoint');
    expect(checkpoints).toHaveLength(2);
    expect(events.at(-2)?.type).toBe('checkpoint');
    expect(events.at(-1)?.type).toBe('done');
    expect(await readFile(join(first!.options!.env!.CODEX_HOME!, rollout))).toEqual(history);
    const home = first!.options!.env!.CODEX_HOME!;
    expect((await stat(home)).mode & 0o777).toBe(0o700);
    expect((await stat(join(home, 'model-catalog.json'))).mode & 0o777).toBe(0o600);
    await worker.handle({ type: 'stop' });
    await worker.close();
    await expect(stat(home)).rejects.toMatchObject({ code: 'ENOENT' });
    expect(() => worker.refreshOptions({ apiKey: 'closed' })).toThrow('idle');
    await expect(worker.handle({ type: 'submit', text: 'closed' })).rejects.toThrow('not ready');
    await expect(worker.handle(input)).rejects.toThrow('already started or closed');
  });

  it('can rotate a key before the first turn without manufacturing a resume ID', async () => {
    const sdk = fakeSdk();
    const { worker } = await setup(sdk.makeCodex);
    expect(() => worker.refreshOptions({ apiKey: '' })).toThrow('API key');
    worker.refreshOptions({ apiKey: 'new-before-first-turn', system: 'Updated before first turn' });
    expect(sdk.created[1]!.resumedId).toBeUndefined();
    expect(sdk.created[1]!.options).toMatchObject({
      apiKey: 'new-before-first-turn',
      baseUrl: 'http://gateway/v1',
      config: { developer_instructions: 'Updated before first turn' },
    });
  });

  it('rejects refresh before start and concurrent turns, and aborts an active turn on close', async () => {
    const sdk = fakeSdk(async function* (signal) {
      yield* [];
      await new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve()));
      throw new Error('aborted');
    });
    const unopened = new CodexSdkWorker(() => {}, sdk.makeCodex);
    expect(() => unopened.refreshOptions({ apiKey: 'early' })).toThrow('idle');
    await unopened.close();
    const { worker, events } = await setup(sdk.makeCodex);
    const turn = worker.handle({ type: 'submit', text: 'wait for interrupt' });
    await vi.waitFor(() => expect(events.some((event) => event.type === 'ready')).toBe(true));
    // Let runStreamed install the abort observer after writing its rollout.
    await vi.waitFor(async () => {
      expect(await readFile(join(sdk.created[0]!.options!.env!.CODEX_HOME!, rollout))).toEqual(
        history,
      );
    });
    expect(() => worker.refreshOptions({ apiKey: 'racing' })).toThrow('idle');
    await expect(worker.handle({ type: 'submit', text: 'concurrent' })).rejects.toThrow(
      'not ready',
    );
    await worker.close();
    await turn;
    expect(events.filter((event) => event.type === 'failure')).toEqual([]);
    expect(events.at(-1)?.type).toBe('done');
  });

  it.each([new Error('model connection failed'), 'non-Error rejection'])(
    'restores a checkpoint and reports run failure %s before done',
    async (failure) => {
      const sdk = fakeSdk(async function* () {
        yield* [];
        throw failure;
      });
      const root = await directory();
      const events: WorkerEvent[] = [];
      const worker = new CodexSdkWorker((event) => events.push(event), sdk.makeCodex);
      cleanup.push(() => worker.close());
      await worker.handle({
        type: 'start',
        root,
        sessionId: 'resume',
        model: 'gpt-5.4',
        system: '',
        apiKey: 'test',
        tools: [],
        checkpoint: {
          version: 1,
          threadId: 'native-thread',
          instructionsSha256: digest(''),
          files: { [rollout]: history.toString('base64') },
        },
      });
      expect(sdk.created[0]!.resumedId).toBe('native-thread');
      expect(await readFile(join(sdk.created[0]!.options!.env!.CODEX_HOME!, rollout))).toEqual(
        history,
      );
      await worker.handle({ type: 'submit', text: 'model failure' });
      expect(events).toContainEqual({
        type: 'failure',
        message: failure instanceof Error ? failure.message : 'Codex SDK failed',
      });
      expect(events.at(-2)?.type).toBe('checkpoint');
      expect(events.at(-1)?.type).toBe('done');
    },
  );

  it('requires authentication and active turns for the private MCP tool relay', async () => {
    let finish!: () => void;
    const blocked = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const sdk = fakeSdk(async function* () {
      await blocked;
      yield {
        type: 'turn.completed',
        usage: {
          input_tokens: 1,
          cached_input_tokens: 0,
          output_tokens: 1,
          cache_write_input_tokens: 0,
          reasoning_output_tokens: 0,
        },
      };
    });
    const { worker, events } = await setup(sdk.makeCodex);
    const transport = relay(sdk.created[0]!.options);
    expect((await fetch(transport.url, { method: 'POST' })).status).toBe(403);
    expect(
      (await fetch(transport.url, { headers: { Authorization: `Bearer ${transport.token}` } }))
        .status,
    ).toBe(403);
    const client = new Client({ name: 'worker-test', version: '1' });
    await client.connect(
      new StreamableHTTPClientTransport(new URL(transport.url), {
        requestInit: { headers: { Authorization: `Bearer ${transport.token}` } },
      }) as Transport,
    );
    cleanup.push(() => client.close());
    expect((await client.listTools()).tools.map((tool) => tool.name)).toEqual(['read']);
    await expect(client.callTool({ name: 'other' })).rejects.toThrow('not enabled');
    await expect(client.callTool({ name: 'read' })).rejects.toThrow('interrupted');
    const turn = worker.handle({ type: 'submit', text: 'hold for tool' });
    const completedCall = client.callTool({ name: 'read' });
    await vi.waitFor(() => expect(events.some((event) => event.type === 'tool_call')).toBe(true));
    const request = events.find((event) => event.type === 'tool_call')!;
    if (request.type !== 'tool_call') throw new Error('missing tool call');
    expect(request.arguments).toEqual({});
    await worker.handle({
      type: 'tool_result',
      id: request.id,
      result: { content: [{ type: 'text', text: 'successful result' }] },
    });
    expect(await completedCall).toMatchObject({
      content: [{ type: 'text', text: 'successful result' }],
    });
    const call = client.callTool({ name: 'read', arguments: { path: 'remote-file' } });
    await vi.waitFor(() =>
      expect(events.filter((event) => event.type === 'tool_call')).toHaveLength(2),
    );
    await worker.handle({ type: 'interrupt' });
    expect(await call).toMatchObject({
      isError: true,
      content: [{ type: 'text', text: 'turn interrupted' }],
    });
    finish();
    await turn;
    await worker.handle({ type: 'tool_result', id: 'expired-call', result: { content: [] } });
  });
});

describe('native history checkpoints', () => {
  it('refuses mismatched and unverifiable legacy instructions before replacing history', async () => {
    const root = await directory();
    await writeHistory(root);
    const checkpoint = {
      version: 1 as const,
      threadId: 'native-thread',
      files: { [rollout]: history.toString('base64') },
    };
    for (const state of [
      checkpoint,
      { ...checkpoint, instructionsSha256: digest('old instructions') },
      { ...checkpoint, instructionsSha256: 'invalid' },
    ]) {
      await expect(restoreCheckpoint(root, state, 'new instructions')).rejects.toThrow(
        /instructions/,
      );
      expect(await readFile(join(root, rollout))).toEqual(history);
    }
    // An arbitrary later developer message is not proof of initial instructions.
    const records = [
      { type: 'session_meta', payload: { id: 'native-thread', cli_version: '0.154.0' } },
      { type: 'response_item', payload: { type: 'message', role: 'user', content: [] } },
      {
        type: 'response_item',
        payload: {
          type: 'message',
          role: 'developer',
          content: [{ type: 'input_text', text: 'new instructions' }],
        },
      },
    ];
    await expect(
      restoreCheckpoint(
        root,
        {
          ...checkpoint,
          files: {
            [rollout]: Buffer.from(
              records.map((record) => JSON.stringify(record)).join('\n'),
            ).toString('base64'),
          },
        },
        'new instructions',
      ),
    ).rejects.toThrow('cannot be verified');
    expect(await readFile(join(root, rollout))).toEqual(history);
  });

  it.each(['valid', 'wrong-role', 'wrong-content', 'wrong-text', 'malformed-json'])(
    'verifies the initial developer instructions in a legacy rollout: %s',
    async (kind) => {
      const root = await directory();
      const system = 'original instructions';
      const records = [
        { type: 'session_meta', payload: { id: 'native-thread', cli_version: '0.154.0' } },
        {
          type: 'response_item',
          payload: {
            type: 'message',
            role: kind === 'wrong-role' ? 'user' : 'developer',
            content: [
              {
                type: kind === 'wrong-content' ? 'text' : 'input_text',
                text: kind === 'wrong-text' ? 'different instructions' : system,
              },
            ],
          },
        },
      ];
      const bytes = Buffer.from(
        kind === 'malformed-json' ? '{' : records.map((r) => JSON.stringify(r)).join('\n'),
      );
      const state = {
        version: 1 as const,
        threadId: 'native-thread',
        files: { [rollout]: bytes.toString('base64') },
      };
      await writeHistory(root);
      if (kind === 'valid') {
        await restoreCheckpoint(root, state, system);
        expect(await readFile(join(root, rollout))).toEqual(bytes);
      } else {
        await expect(restoreCheckpoint(root, state, system)).rejects.toThrow('cannot be verified');
        expect(await readFile(join(root, rollout))).toEqual(history);
      }
    },
  );

  it('rejects histories beyond byte and file limits without replacing the previous checkpoint', async () => {
    const root = await directory();
    await writeHistory(root);
    const oversized = Buffer.alloc(16 * 1024 * 1024 + 1, 120);
    await expect(
      restoreCheckpoint(
        root,
        {
          version: 1,
          threadId: 'native-thread',
          files: { [rollout]: oversized.toString('base64') },
        },
        '',
      ),
    ).rejects.toThrow('exceeds checkpoint limit');
    expect(await readFile(join(root, rollout))).toEqual(history);
    await writeFile(join(root, rollout), oversized);
    await expect(captureCheckpoint(root, 'native-thread', '')).rejects.toThrow(
      'exceeds checkpoint limit',
    );
    await writeHistory(root);
    await Promise.all(
      Array.from({ length: 32 }, (_, index) =>
        writeFile(join(root, rollout.replace('.jsonl', `-${index}.jsonl`)), history),
      ),
    );
    await expect(captureCheckpoint(root, 'native-thread', '')).rejects.toThrow(
      'checkpoint file limit',
    );
  });

  it('captures only matching regular rollout files and ignores symlinks and other threads', async () => {
    const root = await directory();
    await writeHistory(root);
    await writeFile(join(dirname(join(root, rollout)), 'rollout-other-thread.jsonl'), 'other');
    await symlink(
      join(root, rollout),
      join(dirname(join(root, rollout)), 'rollout-link-native-thread.jsonl'),
    );
    await writeFile(join(root, 'auth.json'), 'host secret');
    expect(await captureCheckpoint(root, 'native-thread', '')).toEqual({
      version: 1,
      threadId: 'native-thread',
      instructionsSha256: digest(''),
      files: { [rollout]: history.toString('base64') },
    });
  });

  it('validates every file before replacing existing native history', async () => {
    const root = await directory();
    await writeHistory(root);
    const checkpoint = {
      version: 1 as const,
      threadId: 'native-thread',
      files: { [rollout]: history.toString('base64') },
    };
    for (const files of [
      { '../auth.json': 'e30=' },
      {},
      { [rollout]: 'invalid base64*' },
      { [rollout.replace('native-thread', 'other-thread')]: 'e30=' },
    ]) {
      await expect(restoreCheckpoint(root, { ...checkpoint, files }, '')).rejects.toThrow(
        'invalid Codex',
      );
      expect(await readFile(join(root, rollout))).toEqual(history);
    }
    await expect(restoreCheckpoint(root, { ...checkpoint, version: 2 as 1 }, '')).rejects.toThrow(
      'invalid Codex checkpoint',
    );
    await expect(
      restoreCheckpoint(root, { ...checkpoint, threadId: '../escape' }, ''),
    ).rejects.toThrow('invalid Codex checkpoint');
    await expect(captureCheckpoint(root, '../escape', '')).rejects.toThrow(
      'invalid Codex thread id',
    );
    await expect(captureCheckpoint(root, 'missing-thread', '')).rejects.toThrow('missing');
    const tooMany = Object.fromEntries(
      Array.from({ length: 33 }, (_, i) => [rollout.replace('.jsonl', `-${i}.jsonl`), 'e30=']),
    );
    await expect(restoreCheckpoint(root, { ...checkpoint, files: tooMany }, '')).rejects.toThrow(
      'invalid Codex rollout path',
    );
  });

  it('removes stale history and restores only the authoritative files with private permissions', async () => {
    const root = await directory();
    await writeHistory(root);
    await writeFile(join(root, 'sessions/stale.jsonl'), 'old partial turn');
    await restoreCheckpoint(
      root,
      {
        version: 1,
        threadId: 'native-thread',
        instructionsSha256: digest(''),
        files: { [rollout]: history.toString('base64') },
      },
      '',
    );
    expect(await readdir(join(root, 'sessions'))).toEqual(['2026']);
    expect(await readFile(join(root, rollout))).toEqual(history);
    expect((await stat(join(root, rollout))).mode & 0o777).toBe(0o600);
    expect((await stat(dirname(join(root, rollout)))).mode & 0o777).toBe(0o700);
  });
});

describe('managed native instruction transitions', () => {
  const previous = 'Pinned base\nold catalog';
  const next = 'Pinned base\nnew catalog';
  const developer = () => ({
    type: 'message',
    role: 'developer',
    content: [
      { type: 'input_text', text: previous },
      { type: 'input_text', text: 'native SDK context' },
    ],
  });
  const records = () => [
    { type: 'session_meta', payload: { id: 'native-thread', cli_version: '0.154.0' } },
    { type: 'response_item', payload: developer() },
    {
      type: 'response_item',
      payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text: previous }] },
    },
    {
      type: 'response_item',
      payload: { type: 'function_call_output', call_id: 'call', output: 'keep tool history' },
    },
  ];
  const state = (items = records()) => ({
    version: 1 as const,
    threadId: 'native-thread',
    instructionsSha256: digest(previous),
    files: {
      [rollout]: Buffer.from(items.map((item) => JSON.stringify(item)).join('\n') + '\n').toString(
        'base64',
      ),
    },
  });

  it('changes only the managed developer block and preserves the source checkpoint', () => {
    const checkpoint = state();
    const original = JSON.stringify(checkpoint);
    const transitioned = transitionCodexCheckpointInstructions(
      checkpoint,
      next,
      (text) => text === previous,
    );
    const changed = Buffer.from(transitioned.files[rollout]!, 'base64')
      .toString()
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line));
    expect(changed[1].payload.content).toEqual([
      { type: 'input_text', text: next },
      { type: 'input_text', text: 'native SDK context' },
    ]);
    expect(changed[2]).toEqual(records()[2]);
    expect(changed[3]).toEqual(records()[3]);
    expect(transitioned.instructionsSha256).toBe(digest(next));
    expect(transitioned.threadId).toBe(checkpoint.threadId);
    expect(JSON.stringify(checkpoint)).toBe(original);
    expect(transitionCodexCheckpointInstructions(transitioned, next, () => false)).toBe(
      transitioned,
    );
  });

  it('updates the initial developer block retained by native compaction', () => {
    const checkpoint = state();
    const compacted = {
      type: 'compacted',
      payload: { replacement_history: [developer(), records()[2]!.payload] },
    };
    const text =
      Buffer.from(checkpoint.files[rollout]!, 'base64').toString() +
      JSON.stringify(compacted) +
      '\n';
    checkpoint.files[rollout] = Buffer.from(text).toString('base64');
    const transitioned = transitionCodexCheckpointInstructions(checkpoint, next, () => true);
    const changed = JSON.parse(
      Buffer.from(transitioned.files[rollout]!, 'base64').toString().trim().split('\n').at(-1)!,
    );
    expect(changed.payload.replacement_history[0].content[0].text).toBe(next);
    expect(changed.payload.replacement_history[1]).toEqual(records()[2]!.payload);
  });

  it('removes and restores an empty-base catalog without altering SDK context or compacted history', () => {
    const checkpoint = state();
    const original = Buffer.from(checkpoint.files[rollout]!, 'base64').toString();
    checkpoint.files[rollout] = Buffer.from(
      original +
        JSON.stringify({
          type: 'compacted',
          payload: { replacement_history: [developer(), records()[2]!.payload] },
        }) +
        '\n',
    ).toString('base64');
    const source = JSON.stringify(checkpoint);
    const blocked = transitionCodexCheckpointInstructions(
      checkpoint,
      '',
      (text) => text === previous,
    );
    const decode = (value: CodexCheckpoint) =>
      Buffer.from(value.files[rollout]!, 'base64')
        .toString()
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line));
    const blockedRecords = decode(blocked);
    const nativeContext = [{ type: 'input_text', text: 'native SDK context' }];
    expect(blockedRecords[1].payload.content).toEqual(nativeContext);
    expect(blockedRecords.at(-1).payload.replacement_history[0].content).toEqual(nativeContext);
    expect(blocked.instructionsSha256).toBe(digest(''));

    const acceptsEmpty = vi.fn((text: string) => text === '');
    const restored = transitionCodexCheckpointInstructions(blocked, previous, acceptsEmpty);
    expect(acceptsEmpty).toHaveBeenCalledWith('');
    expect(decode(restored)).toEqual(decode(checkpoint));
    expect(restored.threadId).toBe(checkpoint.threadId);
    expect(restored.instructionsSha256).toBe(checkpoint.instructionsSha256);
    expect(JSON.stringify(checkpoint)).toBe(source);
  });

  it('rejects ambiguous multiple rollouts before authorizing an instruction change', () => {
    const checkpoint = state();
    const ambiguous = {
      ...checkpoint,
      files: {
        ...checkpoint.files,
        'sessions/2026/09/21/rollout-native-thread.jsonl': checkpoint.files[rollout],
      },
    };
    const authorize = vi.fn(() => true);
    const source = JSON.stringify(ambiguous);
    expect(() => transitionCodexCheckpointInstructions(ambiguous, next, authorize)).toThrow(
      'unsupported Codex instruction transition layout',
    );
    expect(authorize).not.toHaveBeenCalled();
    expect(JSON.stringify(ambiguous)).toBe(source);
  });

  it.each([
    { name: 'missing session metadata', metadata: {}, initial: developer() },
    {
      name: 'different thread identity',
      metadata: { type: 'session_meta', payload: { id: 'other-thread', cli_version: '0.154.0' } },
      initial: developer(),
    },
    { name: 'missing developer message', initial: undefined },
    {
      name: 'user message instead of developer instructions',
      initial: { ...developer(), role: 'user' },
    },
    { name: 'missing developer content', initial: { type: 'message', role: 'developer' } },
    { name: 'empty developer content', initial: { ...developer(), content: [] } },
    {
      name: 'non-text developer content',
      initial: { ...developer(), content: [{ type: 'input_text', text: 42 }] },
    },
  ])('rejects $name without changing the checkpoint', ({ metadata, initial }) => {
    const checkpoint = state();
    const items: unknown[] = [metadata ?? records()[0]];
    if (initial) items.push({ type: 'response_item', payload: initial });
    checkpoint.files[rollout] = Buffer.from(
      items.map((item) => JSON.stringify(item)).join('\n') + '\n',
    ).toString('base64');
    const source = JSON.stringify(checkpoint);
    const authorize = vi.fn(() => true);
    expect(() => transitionCodexCheckpointInstructions(checkpoint, next, authorize)).toThrow(
      'unsupported Codex instruction transition layout',
    );
    expect(authorize).not.toHaveBeenCalled();
    expect(JSON.stringify(checkpoint)).toBe(source);
  });

  it.each([
    { name: 'missing replacement history', payload: {}, error: 'unsupported Codex compacted' },
    {
      name: 'replacement history without developer instructions',
      payload: { replacement_history: [records()[2]!.payload] },
      error: 'Codex compacted developer instructions are missing',
    },
    {
      name: 'compacted instructions that differ from the verified original',
      payload: {
        replacement_history: [
          { ...developer(), content: [{ type: 'input_text', text: 'unverified instructions' }] },
        ],
      },
      error: 'Codex compacted instructions cannot be verified',
    },
  ])('rejects $name without partially rewriting history', ({ payload, error }) => {
    const checkpoint = state();
    const text =
      Buffer.from(checkpoint.files[rollout]!, 'base64').toString() +
      JSON.stringify({ type: 'compacted', payload }) +
      '\n';
    checkpoint.files[rollout] = Buffer.from(text).toString('base64');
    const source = JSON.stringify(checkpoint);
    expect(() => transitionCodexCheckpointInstructions(checkpoint, next, () => true)).toThrow(
      error,
    );
    expect(JSON.stringify(checkpoint)).toBe(source);
  });

  it('fails closed on unapproved bases, corrupt digests and unsupported native layouts', () => {
    expect(() => transitionCodexCheckpointInstructions(state(), next, () => false)).toThrow(
      'not a pinned Skill policy change',
    );
    expect(() =>
      transitionCodexCheckpointInstructions(
        { ...state(), instructionsSha256: digest('other') },
        next,
        () => true,
      ),
    ).toThrow('instructions differ');
    const items = records();
    items[0]!.payload = { id: 'native-thread', cli_version: 'unknown' };
    expect(() => transitionCodexCheckpointInstructions(state(items), next, () => true)).toThrow(
      'unsupported',
    );
  });
});
