// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { runCommand } from '../../src/commands/run.js';
import { attachCommand } from '../../src/commands/attach.js';
import { envCommand, envCreateCommand } from '../../src/commands/env.js';
import { resolveWorkerEnv } from '../../src/commands/worker.js';
import { noColor } from '../../src/colors.js';
import { OrcaClient } from '../../src/client.js';
import type { MinimalWebSocket, WebSocketCtor } from '../../src/terminal-attach.js';
import { fakeRegistry, scriptSse } from '../fakes/registry.js';

const BASE = 'http://localhost:8080';

/** Minimal scripted IO for the command orchestration tests. */
function scriptedIo(lines: string[]): {
  io: {
    readLine(prompt: string): Promise<string | null>;
    confirm(question: string): Promise<boolean>;
    write(text: string): void;
  };
  output: string[];
} {
  const pending = [...lines];
  const output: string[] = [];
  return {
    output,
    io: {
      async readLine() {
        return pending.length === 0 ? null : pending.shift()!;
      },
      async confirm() {
        return false;
      },
      write(text: string) {
        output.push(text);
      },
    },
  };
}

describe('run command', () => {
  it('creates a session from --agent/--environment then drives the chat loop', async () => {
    const reg = fakeRegistry({
      'POST /v1/sessions': { status: 201, json: { id: 'ses_new', status: 'idle' } },
      'GET /v1/sessions/ses_new/events/stream': {
        stream: scriptSse([
          { type: 'agent.message', content: [{ type: 'text', text: 'hello from agent' }] },
          { type: 'agent.turn_completed' },
        ]),
      },
      'POST /v1/sessions/ses_new/events': { json: { events: [] } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    const { io, output } = scriptedIo(['hi agent']);

    await runCommand({
      args: ['--agent', 'agt_1', '--environment', 'env_1'],
      client,
      io,
      colors: noColor,
    });

    const created = reg.requestsFor('POST /v1/sessions')[0]!;
    expect(created.body).toEqual({ agent_id: 'agt_1', environment_id: 'env_1' });
    const posted = reg.requestsFor('POST /v1/sessions/ses_new/events')[0]!;
    expect(posted.body).toEqual({
      events: [{ type: 'user.message', content: [{ type: 'text', text: 'hi agent' }] }],
    });
    expect(output.join('\n')).toContain('hello from agent');
    // The session id is surfaced to the operator.
    expect(output.join('\n')).toContain('ses_new');
  });

  it('rejects when --agent is missing', async () => {
    const reg = fakeRegistry({});
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    const { io } = scriptedIo([]);
    await expect(
      runCommand({ args: ['--environment', 'env_1'], client, io, colors: noColor }),
    ).rejects.toThrow(/--agent/);
  });
});

describe('attach command', () => {
  it('attaches to an existing --session and drives the chat loop (no create call)', async () => {
    const reg = fakeRegistry({
      'GET /v1/sessions/ses_existing': { json: { id: 'ses_existing', status: 'running' } },
      'GET /v1/sessions/ses_existing/events/stream': {
        stream: scriptSse([
          { type: 'agent.message', content: [{ type: 'text', text: 'resumed reply' }] },
          { type: 'agent.turn_completed' },
        ]),
      },
      'POST /v1/sessions/ses_existing/events': { json: { events: [] } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    const { io, output } = scriptedIo(['continue please']);

    await attachCommand({ args: ['--session', 'ses_existing'], client, io, colors: noColor });

    // attach never creates a session.
    expect(reg.requestsFor('POST /v1/sessions')).toHaveLength(0);
    const posted = reg.requestsFor('POST /v1/sessions/ses_existing/events')[0]!;
    expect(posted.body).toEqual({
      events: [{ type: 'user.message', content: [{ type: 'text', text: 'continue please' }] }],
    });
    expect(output.join('\n')).toContain('resumed reply');
  });

  it('rejects when --session is missing', async () => {
    const reg = fakeRegistry({});
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    const { io } = scriptedIo([]);
    await expect(attachCommand({ args: [], client, io, colors: noColor })).rejects.toThrow(
      /--session/,
    );
  });

  // The banner is printed BEFORE the dial, so a refused or dropped attach used to
  // leave the operator reading "attaching to terminal …" with exit status 0.
  // Anything but a clean 1000 must throw, so `main` prints it and exits 1.
  it('throws when the registry refuses the terminal attach, naming the reason', async () => {
    const attempt = attachTerminalCommand({
      webSocket: () => scriptedSocket({ close: { code: 1008, reason: 'terminal not found' } }),
      terminalId: 'term_missing',
    });

    await expect(attempt).rejects.toThrow(/terminal not found/);
    await expect(attempt).rejects.toThrow(/1008/);
  });

  it('throws when the terminal socket faults mid-stream', async () => {
    await expect(
      attachTerminalCommand({
        webSocket: () => scriptedSocket({ errorAfterOpen: new Error('read ECONNRESET') }),
      }),
    ).rejects.toThrow(/ECONNRESET/);
  });

  it('returns cleanly when the operator detaches normally', async () => {
    await expect(
      attachTerminalCommand({ webSocket: () => scriptedSocket({ close: { code: 1000 } }) }),
    ).resolves.toBeUndefined();
  });
});

/** Run `attach --terminal` against a scripted socket, with the boilerplate hidden. */
function attachTerminalCommand(opts: {
  webSocket: WebSocketCtor;
  terminalId?: string;
}): Promise<void> {
  const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, fakeRegistry({}).fetch);
  return attachCommand({
    args: ['--session', 'ses_1', '--terminal', opts.terminalId ?? 'term_1'],
    client,
    io: scriptedIo([]).io,
    colors: noColor,
    config: { baseURL: BASE, apiKey: 'sk-1' },
    webSocket: opts.webSocket,
  });
}

/**
 * A WebSocket that opens and then does exactly one scripted thing.
 *
 * The events fire on a microtask so the proxy's listeners are registered first —
 * `attachTerminal` subscribes synchronously after construction, and firing inside
 * the constructor would land before anyone is listening.
 */
function scriptedSocket(script: {
  close?: { code: number; reason?: string };
  errorAfterOpen?: Error;
}): MinimalWebSocket {
  const listeners = new Map<string, Array<(ev: unknown) => void>>();
  const emit = (type: string, ev?: unknown): void => {
    for (const fn of listeners.get(type) ?? []) fn(ev);
  };
  queueMicrotask(() => {
    emit('open');
    if (script.errorAfterOpen !== undefined) emit('error', script.errorAfterOpen);
    if (script.close !== undefined) emit('close', script.close);
  });
  return {
    binaryType: 'blob',
    send: () => {},
    close: () => {},
    addEventListener: (type: string, listener: (ev: never) => void) => {
      const existing = listeners.get(type) ?? [];
      existing.push(listener as (ev: unknown) => void);
      listeners.set(type, existing);
    },
  } as unknown as MinimalWebSocket;
}

describe('env create command', () => {
  it('POSTs the environment and prints the env_key exactly once', async () => {
    const reg = fakeRegistry({
      'POST /v1/environments': {
        status: 201,
        json: { id: 'env_1', name: 'laptop', target: 'self_hosted', env_key: 'envk_raw_ABC' },
      },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    const output: string[] = [];

    await envCreateCommand({
      args: ['--name', 'laptop'],
      client,
      write: (t) => output.push(t),
    });

    const req = reg.requestsFor('POST /v1/environments')[0]!;
    // Default target is self_hosted per the CLI flag default.
    expect(req.body).toEqual({ name: 'laptop', target: 'self_hosted' });
    const joined = output.join('\n');
    expect(joined).toContain('env_1');
    const keyOccurrences = joined.split('envk_raw_ABC').length - 1;
    expect(keyOccurrences).toBe(1);
  });

  it('honors an explicit --target', async () => {
    const reg = fakeRegistry({
      'POST /v1/environments': {
        status: 201,
        json: { id: 'env_2', name: 'cloudy', target: 'cloud', env_key: 'k' },
      },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    await envCreateCommand({
      args: ['--name', 'cloudy', '--target', 'cloud'],
      client,
      write: () => {},
    });
    expect(reg.requestsFor('POST /v1/environments')[0]!.body).toEqual({
      name: 'cloudy',
      target: 'cloud',
    });
  });

  it('rejects when --name is missing', async () => {
    const reg = fakeRegistry({});
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    await expect(envCreateCommand({ args: [], client, write: () => {} })).rejects.toThrow(/--name/);
  });

  // Handing back the one-time key is the whole point of `env create`: the raw key
  // is echoed on create and never again, so a create without one has produced an
  // environment no worker can ever be wired to, and nothing can recover it. That
  // is a failure, not a warning printed to stdout above exit status 0.
  it.each([
    ['omitted entirely', { id: 'env_nokey', name: 'laptop', target: 'self_hosted' }],
    ['present but empty', { id: 'env_nokey', name: 'laptop', target: 'self_hosted', env_key: '' }],
  ])('throws when the registry returns no env_key (%s)', async (_label, json) => {
    const reg = fakeRegistry({ 'POST /v1/environments': { status: 201, json } });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    const output: string[] = [];

    await expect(
      envCreateCommand({ args: ['--name', 'laptop'], client, write: (t) => output.push(t) }),
    ).rejects.toThrow(/no env_key/);
  });

  it('routes `env create ...` through the command group dispatcher', async () => {
    const reg = fakeRegistry({
      'POST /v1/environments': {
        status: 201,
        json: { id: 'env_grp', name: 'grp', target: 'self_hosted', env_key: 'k' },
      },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    const output: string[] = [];
    await envCommand({ args: ['create', '--name', 'grp'], client, write: (t) => output.push(t) });
    expect(reg.requestsFor('POST /v1/environments')).toHaveLength(1);
    expect(output.join('\n')).toContain('env_grp');
  });

  it('rejects an unknown env subcommand', async () => {
    const reg = fakeRegistry({});
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    await expect(envCommand({ args: ['destroy'], client, write: () => {} })).rejects.toThrow(
      /unknown env subcommand/,
    );
  });
});

describe('worker command env resolution', () => {
  it('maps CLI flags to the environment-worker env-var contract', () => {
    const env = resolveWorkerEnv(
      [
        '--environment',
        'env_1',
        '--env-key',
        'envk_raw',
        '--registry',
        'wss://reg.example.com',
        '--workspace-dir',
        '/tmp/ws',
        '--runner-command',
        'node dist/runner.js',
      ],
      {},
    );
    expect(env.ENVIRONMENT_ID).toBe('env_1');
    expect(env.ENVIRONMENT_KEY).toBe('envk_raw');
    expect(env.REGISTRY_TUNNEL_BASE_URL).toBe('wss://reg.example.com');
    expect(env.WORKSPACE_DIR).toBe('/tmp/ws');
    expect(env.RUNNER_LAUNCH_COMMAND).toBe('node dist/runner.js');
  });

  it('falls back to ORCA_* / existing env vars when a flag is omitted', () => {
    const env = resolveWorkerEnv(['--environment', 'env_1', '--env-key', 'k'], {
      REGISTRY_TUNNEL_BASE_URL: 'wss://from-env',
      WORKSPACE_DIR: '/from/env',
      RUNNER_LAUNCH_COMMAND: 'run me',
    });
    expect(env.REGISTRY_TUNNEL_BASE_URL).toBe('wss://from-env');
    expect(env.WORKSPACE_DIR).toBe('/from/env');
    expect(env.RUNNER_LAUNCH_COMMAND).toBe('run me');
  });

  it('throws when a required worker setting is missing entirely', () => {
    expect(() => resolveWorkerEnv(['--environment', 'env_1'], {})).toThrow(
      /env-key|ENVIRONMENT_KEY/,
    );
  });
});
