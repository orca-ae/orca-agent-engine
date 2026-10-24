// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import { createServer } from 'node:http';
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DshSdkWorker } from '@orca/dsh-harness';
import type { WorkerEvent } from '@orca/sdk-harness';
import { replyMessages } from '../../../../packages/dsh-harness/test/support/native-tree.js';
import { CodexSdkHarness } from '../../src/harness/codex-sdk/index.js';
import type { AgentEvent, SessionStartInput } from '../../src/harness/agent-harness.js';
import { turnStoreFixture } from '../support/codex-turn-store.js';

it.each([
  { known: true, authorizationDelay: 0 },
  { known: false, authorizationDelay: 0 },
  { known: true, authorizationDelay: 100 },
])(
  'settles real native interrupt_agent with %j',
  async ({ known, authorizationDelay }) => {
    const directory = await mkdtemp(join(tmpdir(), 'dsh-native-settlement-'));
    const events: AgentEvent[] = [];
    const raw: WorkerEvent[] = [];
    const requests: Array<{ role: string; body: string; terminal: boolean }> = [];
    const processes = new Map<string, ChildProcessWithoutNullStreams>();
    const exits = new Map<string, { code: number | null; signal: string | null }>();
    let a = '';
    let b = '';
    let launched = false;
    let interrupted = false;
    let aAborted = false;
    let readyA!: () => void;
    let readyB!: () => void;
    const waitingA = new Promise<void>((resolve) => {
      readyA = resolve;
    });
    const waitingB = new Promise<void>((resolve) => {
      readyB = resolve;
    });
    let sequence = 0;
    const call = (name: string, input: object) => ({
      type: 'tool_use',
      id: 'fixture-' + ++sequence,
      name,
      input,
    });
    const server = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on('data', (chunk) => chunks.push(chunk));
      req.on('end', async () => {
        const body = JSON.parse(Buffer.concat(chunks).toString());
        const userText = JSON.stringify(
          body.messages.filter((m: { role: string }) => m.role === 'user'),
        );
        const role = userText.includes('CHILD_A')
          ? 'A'
          : userText.includes('CHILD_B')
            ? 'B'
            : 'root';
        const request = { role, body: JSON.stringify(body), terminal: false };
        requests.push(request);
        res.writeHead(200, { 'content-type': 'text/event-stream' });
        const reply = (blocks?: Array<Record<string, unknown>>) => {
          request.terminal = true;
          replyMessages(res, body.model, blocks);
        };
        if (role === 'root' && userText.includes('DELEGATE_NATIVE') && !launched) {
          launched = true;
          reply([
            call('spawn', { description: 'child A', prompt: 'CHILD_A' }),
            call('fork', { description: 'child B', prompt: 'CHILD_B' }),
          ]);
        } else if (role === 'root' && launched && !interrupted) {
          await Promise.all([waitingA, waitingB]);
          interrupted = true;
          reply([call('interrupt_agent', { agent_id: a })]);
        } else if (role === 'A' && requests.filter((r) => r.role === 'A').length === 2) {
          // Native steps await their tools: abort the follow-up request after A's tool exits.
          res.on('close', () => {
            aAborted = !res.writableEnded;
          });
          res.write('event: ping\ndata: {"type":"ping"}\n\n');
          readyA();
        } else if (
          role === 'A' ||
          (role === 'B' && requests.filter((r) => r.role === 'B').length === 1)
        ) {
          reply([call('bash', { command: role })]);
        } else reply([{ type: 'text', text: role === 'B' ? 'SIBLING_COMPLETED' : 'OK' }]);
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const turns = turnStoreFixture();
    const consumed: Promise<void>[] = [];
    const createHarness = () => {
      const harness = new CodexSdkHarness({
        harness: 'dsh_sdk',
        apiKey: 'jwt',
        turns: turns.store,
        dshGatewayUrl: 'http://127.0.0.1:' + (server.address() as { port: number }).port,
        llmGatewayJwtProvider: {
          getValidToken: async () => ({ token: 'jwt' }) as never,
          close: () => {},
        },
        createWorker: (emit) => {
          const worker = new DshSdkWorker((event) => {
            raw.push(event);
            const canonical = event as unknown as {
              type: string;
              event: { kind: string; payload: Record<string, unknown> };
            };
            if (canonical.type === 'canonical_event') {
              const { kind, payload } = canonical.event;
              if (kind === 'session.thread_created') {
                if (payload.agent_name === 'child A') a = String(payload.native_session_id);
                if (payload.agent_name === 'child B') b = String(payload.native_session_id);
              }
              if (kind === 'session.thread_status_idle' && payload.native_session_id === a) {
                processes.get('B')!.stdin.end('finish');
              }
            }
            emit(event);
          });
          const handle = worker.handle.bind(worker);
          worker.handle = async (command) => {
            // Remote authorization can arrive after a canceled host tool result.
            if (authorizationDelay && interrupted && command.type === 'orchestration_result')
              await new Promise((resolve) => setTimeout(resolve, authorizationDelay));
            await handle(command);
          };
          return worker;
        },
      });
      consumed.push(
        (async () => {
          for await (const event of harness.events()) {
            events.push(event);
            event.persistence?.resolve();
          }
        })(),
      );
      return harness;
    };
    let harness = createHarness();
    const input: SessionStartInput = {
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      archivedThreadSubpaths: [],
      supportsPerCallCancellation: true,
      agentSnapshot: {
        model_provider: 'anthropic',
        model_id: 'claude-sonnet-4-6',
        allowed_tool_names: ['bash'],
      },
      tools: [
        {
          name: 'bash',
          description: '',
          input_schema: { type: 'object' },
          execute: async (args, options) => {
            const role = String((args as { command: string }).command);
            const script =
              role === 'A' && known
                ? "const fs = require('node:fs'); process.stdout.write('START'); setTimeout(() => fs.writeFileSync('A-finish', 'FINISH'), 60000)"
                : role === 'A'
                  ? "const fs = require('node:fs'); process.stdout.write('START'); fs.writeFileSync('A-finish', 'FINISH')"
                  : "const fs = require('node:fs'); process.stdout.write('START'); process.stdin.once('data', () => { fs.writeFileSync('B-finish', 'FINISH'); process.exit(0); })";
            const child = spawn(process.execPath, ['-e', script], { cwd: directory });
            processes.set(role, child);
            child.stdout.once('data', () => {
              if (role === 'B') readyB();
              else if (known) readyA();
            });
            const cancel = () => {
              child.kill('SIGKILL');
            };
            options!.signal!.addEventListener('abort', cancel, { once: true });
            if (options!.signal!.aborted) cancel();
            await new Promise<void>((resolve) =>
              child.once('exit', (code, signal) => {
                exits.set(role, { code, signal });
                resolve();
              }),
            );
            options!.signal!.removeEventListener('abort', cancel);
            return { content: 'finished' };
          },
        },
      ],
    };
    const submit = (id: string, text: string) =>
      harness.submit({ id, kind: 'user.message', payload: { content: text } });
    try {
      await harness.start(input);
      await submit('evt_seed', 'Remember SAFFRON');
      const checkpoint = structuredClone(turns.snapshot.state);
      const before = raw.length;
      const beforeRequests = requests.length;
      if (known) await submit('evt_native', 'DELEGATE_NATIVE');
      else
        await expect(submit('evt_native', 'DELEGATE_NATIVE')).rejects.toThrow(
          'DSH tree incomplete or terminal usage unavailable',
        );
      expect(interrupted).toBe(true);
      expect(a).not.toBe('');
      expect(b).not.toBe('');
      expect(
        events.filter(
          (e) => e.kind === 'session.thread_status_idle' && e.subpath === 'subagents/' + a,
        ),
      ).toMatchObject([{ payload: { native_reason: 'aborted', stop_reason: 'interrupted' } }]);
      expect(exits.get('B')).toEqual({ code: 0, signal: null });
      expect(await readFile(join(directory, 'B-finish'), 'utf8')).toBe('FINISH');
      const observed = requests.slice(beforeRequests);
      const childRequests = observed.filter((r) => r.role === 'A');
      expect(childRequests.map((r) => r.terminal)).toEqual(known ? [true] : [true, false]);
      const emitted = raw.slice(before);
      const totals = emitted.filter((e) => e.type === 'event' && e.event.type === 'turn.completed');
      const summaries = events
        .filter(
          (e) =>
            e.kind === 'span.model_request_end' &&
            (e.payload as { turn_event_id?: string }).turn_event_id === 'evt_native',
        )
        .map((e) => ({ subpath: e.subpath, payload: e.payload as Record<string, unknown> }));
      if (known) {
        expect(exits.get('A')).toEqual({ code: null, signal: 'SIGKILL' });
        await expect(readFile(join(directory, 'A-finish'))).rejects.toMatchObject({
          code: 'ENOENT',
        });
        expect(emitted.filter((e) => e.type === 'failure')).toEqual([]);
        expect(observed.every((r) => r.terminal)).toBe(true);
        expect(totals).toMatchObject([
          {
            event: {
              usage: {
                input_tokens: observed.length * 7,
                output_tokens: observed.length * 2,
                cached_input_tokens: observed.length * 3,
                cache_write_input_tokens: 0,
              },
            },
          },
        ]);
        expect(totals).toHaveLength(1);
        const checkpoints = emitted.filter((e) => e.type === 'checkpoint');
        expect(checkpoints).toHaveLength(1);
        expect(turns.snapshot.state).toEqual(checkpoints[0]!.checkpoint);
        expect(summaries).toHaveLength(3);
        for (const [id, role] of [
          ['', 'root'],
          ['subagents/' + a, 'A'],
          ['subagents/' + b, 'B'],
        ]) {
          const count = observed.filter((r) => r.role === role).length;
          expect(summaries.find((e) => e.subpath === id)?.payload.model_usage).toEqual({
            input_tokens: count * 4,
            output_tokens: count * 2,
            cache_read_input_tokens: count * 3,
            cache_creation_input_tokens: 0,
          });
        }
        expect(summaries.find((e) => e.subpath === 'subagents/' + b)?.payload).toMatchObject({
          is_error: false,
          model_usage: {
            input_tokens: 8,
            output_tokens: 4,
            cache_read_input_tokens: 6,
            cache_creation_input_tokens: 0,
          },
        });
        expect(turns.snapshot.state).not.toEqual(checkpoint);
        expect(turns.snapshot.receipt).toMatchObject({ phase: 'settled', error: null });
      } else {
        expect(exits.get('A')).toEqual({ code: 0, signal: null });
        expect(await readFile(join(directory, 'A-finish'), 'utf8')).toBe('FINISH');
        expect(childRequests[1]!.body).toContain('tool_result');
        expect(aAborted).toBe(true);
        expect(observed.filter((r) => !r.terminal).map((r) => r.role)).toEqual(['A']);
        expect(totals).toEqual([]);
        expect(summaries).toEqual([]);
        expect(emitted.filter((e) => e.type === 'checkpoint')).toEqual([]);
        expect(emitted.filter((e) => e.type === 'failure')).toMatchObject([
          {
            fatal: true,
            message:
              'DSH tree incomplete or terminal usage unavailable: provider_requests_unmatched',
          },
        ]);
        for (const event of emitted) {
          if (event.type === 'failure') {
            expect(event.message).not.toContain(a);
            expect(event.message).not.toContain(b);
          }
        }
        expect(turns.snapshot.state).toEqual(checkpoint);
        expect(turns.snapshot.receipt?.phase).toBe('settled');
        const recoveredFrom = requests.length;
        await harness.stop('error');
        harness = createHarness();
        await harness.start(input);
        expect(requests).toHaveLength(recoveredFrom);
        await submit('evt_recover', 'Continue explicitly with SAFFRON');
        expect(
          requests
            .slice(recoveredFrom)
            .every(
              (r) =>
                r.role === 'root' &&
                !r.body.includes('DELEGATE_NATIVE') &&
                !r.body.includes('SIBLING_COMPLETED'),
            ),
        ).toBe(true);
        expect(requests.length).toBeGreaterThan(recoveredFrom);
        expect(turns.snapshot.state).not.toEqual(checkpoint);
        expect(turns.snapshot.receipt).toMatchObject({ phase: 'settled', error: null });
      }
    } finally {
      await harness.stop('error');
      await Promise.all(consumed);
      for (const child of processes.values())
        if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await rm(directory, { recursive: true, force: true });
    }
  },
  45000,
);
