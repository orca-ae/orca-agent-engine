// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { stat } from 'node:fs/promises';
import { CodexSdkRuntime } from '../src/providers/codex-sdk.js';
import { resolveProvider } from '../src/providers/registry.js';

const worker = vi.hoisted(() => ({ handle: vi.fn(), refreshOptions: vi.fn() }));
vi.mock('@orca/codex-harness', () => ({ CodexSdkWorker: vi.fn(() => worker) }));
vi.mock('@orca/pi-harness', () => ({ PiSdkWorker: vi.fn(() => worker) }));
const runtimes: CodexSdkRuntime[] = [];
beforeEach(() => {
  vi.clearAllMocks();
});
afterEach(async () => {
  for (const runtime of runtimes.splice(0))
    await runtime.handleSdkCommand({ type: 'stop' }, () => {}).catch(() => {});
});
const start = {
  type: 'start',
  root: '/host/private-checkout',
  sessionId: 'ses',
  model: 'gpt-5.4',
  system: 'managed policy',
  apiKey: 'scoped-jwt',
  baseUrl: 'http://gateway/v1',
  tools: [],
};
describe.each(['codex-sdk', 'pi-sdk'] as const)('sandbox %s provider', (provider) => {
  const commandStart =
    provider === 'pi-sdk' ? { ...start, piGatewayUrl: 'http://gateway/v1' } : start;
  function runtime() {
    const value = new CodexSdkRuntime(provider);
    runtimes.push(value);
    return value;
  }

  it('registers separately from the CLI and uses a private sandbox cwd', async () => {
    expect(resolveProvider(provider).harnessId).toBe(provider.replace('-', '_'));
    expect(() => resolveProvider('codex')).toThrow('unsupported agent');
    const r = runtime();
    await r.handleSdkCommand(commandStart, () => {});
    const command = worker.handle.mock.calls[0]![0];
    expect(command.piGatewayUrl).toBe(provider === 'pi-sdk' ? 'http://gateway/v1' : undefined);
    expect(command.root).not.toBe(start.root);
    expect((await stat(command.root)).isDirectory()).toBe(true);
    expect(command).toMatchObject({
      apiKey: 'scoped-jwt',
      baseUrl: 'http://gateway/v1',
      system: 'managed policy',
    });
    await expect(r.handleSdkCommand(commandStart, () => {})).rejects.toThrow('already started');
    await r.handleSdkCommand(
      { type: 'refresh', apiKey: 'next-jwt', baseUrl: 'http://gateway/v1' },
      () => {},
    );
    expect(worker.refreshOptions).toHaveBeenCalledWith(
      expect.objectContaining({ apiKey: 'next-jwt' }),
    );
    await r.handleSdkCommand({ type: 'stop' }, () => {});
    await expect(stat(command.root)).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('routes submit, interruption and callback results to the same worker', async () => {
    const r = runtime();
    await r.handleSdkCommand(commandStart, () => {});
    const commands = [
      { type: 'submit', text: 'hello' },
      {
        type: 'tool_result',
        id: 'call',
        result: { content: [{ type: 'text', text: 'ok' }], isError: true },
      },
      { type: 'interrupt' },
    ];
    for (const command of commands) await r.handleSdkCommand(command, () => {});
    expect(worker.handle.mock.calls.slice(1).map(([command]) => command)).toEqual(commands);
    expect(() => r.runTurn()).toThrow('managed SDK command channel');
  });

  it('refuses malformed or uninitialized commands before SDK submission', async () => {
    const r = runtime();
    await expect(r.handleSdkCommand({ type: 'submit', text: 'hello' }, () => {})).rejects.toThrow(
      'not started',
    );
    await expect(r.handleSdkCommand({ ...start, apiKey: '' }, () => {})).rejects.toThrow();
    await expect(r.handleSdkCommand({ type: 'unknown' }, () => {})).rejects.toThrow();
    expect(worker.handle).not.toHaveBeenCalled();
  });
});

it('refuses a colocated Pi command missing its Gateway destination before using a token', async () => {
  const runtime = new CodexSdkRuntime('pi-sdk');
  await expect(runtime.handleSdkCommand(start, () => {})).rejects.toThrow(
    'explicit native Gateway URL',
  );
  expect(worker.handle).not.toHaveBeenCalled();
});
