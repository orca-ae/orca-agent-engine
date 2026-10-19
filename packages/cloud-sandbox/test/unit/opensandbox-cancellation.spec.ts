// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, expect, it, vi } from 'vitest';
import { OpenSandboxRuntime as CloudRuntime } from '../../src/opensandbox/runtime.js';

afterEach(() => vi.unstubAllGlobals());

it.each(['cloud', 'main'])(
  'cancels a real command process group, preserving its sibling (%#)',
  async (backend) => {
    // Load the service in Vitest without applying the library's stricter tsconfig to it.
    const { OpenSandboxRuntime: Runtime } =
      backend === 'cloud'
        ? { OpenSandboxRuntime: CloudRuntime }
        : await vi.importActual<{ OpenSandboxRuntime: typeof CloudRuntime }>(
            '../../../../services/harness-server/src/sandbox/opensandbox/runtime.js',
          );
    const dir = await mkdtemp(join(tmpdir(), 'orca-cancel-'));
    const processes = new Map<
      string,
      {
        child: ChildProcess;
        exited: Promise<void>;
        code: number | null;
        finished: string | undefined;
      }
    >();
    let sequence = 0;
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string, init?: RequestInit) => {
        const url = new URL(input);
        if (url.pathname === '/v1/sandboxes') return Response.json({ id: 'sandbox' });
        if (url.pathname.includes('/endpoints/'))
          return Response.json({ endpoint: 'http://execd.test' });
        if (url.pathname === '/ping') return new Response();
        if (url.pathname === '/command' && init?.method === 'POST') {
          const body = JSON.parse(String(init.body)) as { command: string };
          // Main runtime probes FUSE at acquire; fixture owns only command cancellation.
          if (!body.command.startsWith('sleep ')) {
            return new Response('data: {"type":"execution_complete"}\n\n');
          }
          const id = `command-${++sequence}`;
          const child = spawn('/bin/sh', ['-c', body.command], { detached: true, stdio: 'ignore' });
          let finish!: () => void;
          const state = {
            child,
            code: null as number | null,
            finished: undefined as string | undefined,
            exited: new Promise<void>((resolve) => {
              finish = resolve;
            }),
          };
          processes.set(id, state);
          const stream = new ReadableStream<Uint8Array>({
            start(controller) {
              const send = (event: unknown) =>
                controller.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(event)}\n\n`));
              send({ type: 'init', text: id });
              child.once('exit', (code) => {
                state.code = code ?? -1;
                state.finished = new Date().toISOString();
                send(
                  code === 0
                    ? { type: 'execution_complete' }
                    : { type: 'error', error: { evalue: '-1' } },
                );
                controller.close();
                finish();
              });
            },
          });
          return new Response(stream);
        }
        if (url.pathname === '/command' && init?.method === 'DELETE') {
          const state = processes.get(url.searchParams.get('id')!)!;
          process.kill(-state.child.pid!, 'SIGKILL');
          await state.exited;
          return new Response();
        }
        if (url.pathname.startsWith('/command/status/')) {
          const id = url.pathname.split('/').at(-1)!;
          const state = processes.get(id)!;
          return Response.json({
            id,
            running: state.finished === undefined,
            exit_code: state.code,
            finished_at: state.finished,
          });
        }
        throw new Error(`Unexpected fixture request ${url.pathname}`);
      }),
    );
    try {
      const runtime = new Runtime({
        domain: 'http://lifecycle.test',
        protocol: 'http',
        image: 'fixture',
        timeoutSeconds: 60,
        requestTimeoutSeconds: 5,
        useServerProxy: false,
      });
      expect(runtime.capabilities.supportsPerCallCancellation).toBe(true);
      const handle = await runtime.acquire({});
      const controller = new AbortController();
      const target = handle.run(
        { tool: 'bash', args: { command: `sleep 0.4; echo bad > '${dir}/target'` } },
        { signal: controller.signal },
      );
      const stopped = expect(target).rejects.toMatchObject({ name: 'AbortError' });
      const sibling = handle.run(
        { tool: 'bash', args: { command: `sleep 0.2; echo good > '${dir}/sibling'` } },
        { signal: new AbortController().signal },
      );
      await vi.waitFor(() => expect(processes.size).toBe(2));
      controller.abort();
      await stopped;
      expect(await sibling).toMatchObject({ exit_code: 0 });
      await new Promise((resolve) => setTimeout(resolve, 450));
      await expect(readFile(join(dir, 'target'))).rejects.toMatchObject({ code: 'ENOENT' });
      expect(await readFile(join(dir, 'sibling'), 'utf8')).toBe('good\n');
    } finally {
      for (const state of processes.values())
        if (!state.finished) {
          try {
            process.kill(-state.child.pid!, 'SIGKILL');
          } catch {
            /* already exited */
          }
          await state.exited;
        }
      await rm(dir, { recursive: true, force: true });
    }
  },
  10_000,
);
