// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { stat } from 'node:fs/promises';
import { CodexSdkRuntime } from '../src/providers/codex-sdk.js';
import { resolveProvider } from '../src/providers/registry.js';
import { dshProfileConfigSha256, type DshProfileFiles } from '@orca/harness-catalog';

const worker = vi.hoisted(() => ({ handle: vi.fn(), refreshOptions: vi.fn() }));
vi.mock('@orca/codex-harness', () => ({ CodexSdkWorker: vi.fn(() => worker) }));
vi.mock('@orca/pi-harness', () => ({ PiSdkWorker: vi.fn(() => worker) }));
vi.mock('@orca/dsh-harness', () => ({ DshSdkWorker: vi.fn(() => worker) }));
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

it('retains the bounded native profile descriptor across the private command', async () => {
  const files: DshProfileFiles = {
    'package.json': '{}',
    'pnpm-lock.yaml': '',
    'pnpm-workspace.yaml': '',
    'cordis.patch.yml': '[]',
  };
  const profile = {
    version: 1,
    source_file_id: 'file_profile',
    source_sha256: 'a'.repeat(64),
    config_sha256: dshProfileConfigSha256(files),
    files,
  };
  const runtime = new CodexSdkRuntime('dsh-sdk');
  runtimes.push(runtime);
  const { baseUrl: _, ...command } = start;
  const input = {
    ...command,
    modelProvider: 'deepseek',
    model: 'deepseek-v4-flash',
    dshGatewayUrl: 'http://gateway',
    archivedThreadSubpaths: [],
    dshProfile: profile,
  };
  await runtime.handleSdkCommand(input, () => {});
  expect(worker.handle).toHaveBeenCalledWith(expect.objectContaining({ dshProfile: profile }));
  const invalid = new CodexSdkRuntime('dsh-sdk');
  await expect(
    invalid.handleSdkCommand(
      { ...input, dshProfile: { ...profile, config_sha256: '0'.repeat(64) } },
      () => {},
    ),
  ).rejects.toThrow('digest mismatch');
  await expect(
    new CodexSdkRuntime().handleSdkCommand({ ...start, dshProfile: profile }, () => {}),
  ).rejects.toThrow('DSH-only');
});
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
      { type: 'submit', text: 'long', executionBudgetMs: 1_200_000 },
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

it('DSH transports authorization replies and monotonic archive fence before targeted cleanup', async () => {
  const runtime = new CodexSdkRuntime('dsh-sdk');
  runtimes.push(runtime);
  const { baseUrl: _, ...command } = start;
  await runtime.handleSdkCommand(
    {
      ...command,
      modelProvider: 'deepseek',
      model: 'deepseek-v4-flash',
      dshGatewayUrl: 'http://gateway',
      archivedThreadSubpaths: [],
    },
    () => {},
  );
  const threadId = '11111111-1111-4111-8111-111111111111';
  const controls = [
    { type: 'submit', text: 'long', sourceEventId: 'evt_long', executionBudgetMs: 1_200_000 },
    { type: 'archive_fence', threadId },
    { type: 'orchestration_result', id: 'call', allowed: false, reason: 'archived' },
    { type: 'interrupt', threadId, archived: true },
  ];
  for (const control of controls) await runtime.handleSdkCommand(control, () => {});
  expect(worker.handle.mock.calls.slice(1).map(([command]) => command)).toEqual(controls);
  for (const executionBudgetMs of [0, -1, 1.5, '1200000', null, 2_147_483_648])
    await expect(
      runtime.handleSdkCommand({ type: 'submit', text: 'invalid', executionBudgetMs }, () => {}),
    ).rejects.toThrow();
  const nonDsh = new CodexSdkRuntime();
  await expect(nonDsh.handleSdkCommand(controls[1], () => {})).rejects.toThrow('DSH-only');
  const missing = new CodexSdkRuntime('dsh-sdk');
  await expect(
    missing.handleSdkCommand(
      {
        ...command,
        modelProvider: 'deepseek',
        model: 'deepseek-v4-flash',
        dshGatewayUrl: 'http://gateway',
      },
      () => {},
    ),
  ).rejects.toThrow('archive state');
});
