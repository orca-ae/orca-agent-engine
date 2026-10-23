// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, readFile, readdir, rm, symlink, link, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, expect, it, vi } from 'vitest';
import { RunnerResources } from '../../src/resources.js';
import { SessionLoop } from '../../src/session-loop.js';
import { ProviderRegistry } from '../../src/harness/provider.js';
import type { AgentEvent, AgentHarness } from '../../src/harness/agent-harness.js';
import {
  createRunnerSandboxRuntime,
  resourceManifestDigest,
  NATIVE_USAGE_SOURCE,
  type NativeCompletion,
  type ResourceManifest,
  type SandboxHandle,
} from '../../src/sandbox/seam.js';

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});
const encode = (value: unknown) => Buffer.from(JSON.stringify(value));

// InMemory verifies Files/process mapping for controller tests; it proves no OS confinement.
async function fixture() {
  const workspaceDir = await mkdtemp(join(tmpdir(), 'orca-native-flow-'));
  const runtime = createRunnerSandboxRuntime({ kind: 'in-memory' });
  let raw!: SandboxHandle;
  const acquire = vi.fn(async () => (raw = await runtime.acquire({})));
  const controller = new RunnerResources({
    workspaceDir,
    sessionSandbox: { acquire, capabilities: runtime.capabilities },
  });
  cleanup.push(async () => {
    await controller.close();
    if (raw) await raw.destroy();
    await rm(workspaceDir, { recursive: true, force: true });
  });
  const manifest: ResourceManifest = {
    version: 1,
    revision: 'a'.repeat(64),
    resources: [
      {
        resource_id: 'sesrsc_input',
        kind: 'file',
        mount_path: '/mnt/context.txt',
        access: 'read_only',
        files: [
          {
            path: '',
            size_bytes: 7,
            mode: 0o444,
            sha256: createHash('sha256').update('context').digest('hex'),
          },
        ],
      },
    ],
  };
  const push = (body: unknown) => controller.push('ses_fixture', body);
  const stage = async () => {
    await push({ type: 'manifest', placement: 'session_sandbox', manifest });
    await push({
      type: 'file_chunk',
      revision: manifest.revision,
      resource_id: 'sesrsc_input',
      path: '',
      offset: 0,
      content_base64: Buffer.from('context').toString('base64'),
    });
    await push({
      type: 'commit',
      revision: manifest.revision,
      manifest_sha256: resourceManifestDigest(manifest),
    });
  };
  await stage();
  const completion = (): NativeCompletion => ({
    version: 1,
    source: NATIVE_USAGE_SOURCE,
    turn_id: 'evt_turn',
    execution_id: controller.executionId,
    placement_id: controller.placement!.id,
    outcome: 'success',
    report_set_sealed: true,
    expected_reports: ['report_1'],
    reports: [
      {
        id: 'report_1',
        provider: 'anthropic',
        semantics: 'delta',
        complete: true,
        model: 'actual-model',
        usage: {
          input_tokens: 3,
          output_tokens: 2,
          cache_read_input_tokens: 0,
          cache_creation: { ephemeral_5m_input_tokens: 0, ephemeral_1h_input_tokens: 0 },
        },
      },
    ],
  });
  return { controller, workspaceDir, manifest, push, stage, completion, raw, acquire };
}

it('verifies placement, freezes nested output bytes and only captures subsequent changes after both ACKs', async () => {
  const f = await fixture();
  const placement = f.controller.placement!;
  expect(await readFile(placement.resolvedPaths.sesrsc_input!, 'utf8')).toBe('context');
  expect(await f.push({ type: 'status' })).toMatchObject({
    committed: true,
    placement_id: placement.id,
    resolved_paths: placement.resolvedPaths,
    turn: null,
  });
  await f.raw.files.write('/mnt/session/outputs/nested/report.txt', Buffer.from('first'));
  await f.controller.beginNativeTurn('evt_turn');
  const frozen = await f.controller.freezeNativeTurn(f.completion());
  expect(frozen.checkpoint.files).toMatchObject([{ path: 'nested/report.txt', size_bytes: 5 }]);
  await f.raw.files.write('/mnt/session/outputs/nested/report.txt', Buffer.from('later'));
  expect(
    await f.controller.changes('ses_fixture', {
      type: 'file_chunk',
      checkpoint_id: frozen.checkpoint.checkpoint_id,
      resource_id: frozen.checkpoint.files[0]!.resource_id,
      path: 'nested/report.txt',
      offset: 0,
    }),
  ).toMatchObject({ content_base64: Buffer.from('first').toString('base64') });
  const ack = { checkpoint_id: frozen.checkpoint.checkpoint_id, manifest_sha256: frozen.digest };
  await expect(
    f.controller.acknowledge('ses_fixture', { ...ack, completion_phase: 'completed' }),
  ).rejects.toThrow('not prepared');
  await expect(f.controller.acknowledge('ses_fixture', ack)).rejects.toThrow(
    'receipt acknowledgement missing',
  );
  await f.controller.acknowledge('ses_fixture', { ...ack, completion_phase: 'prepared' });
  expect(f.controller.readyForTurn).toBe(false);
  await expect(f.stage()).rejects.toThrow('blocks refresh');
  await f.controller.acknowledge('ses_fixture', { ...ack, completion_phase: 'completed' });
  expect(f.controller.readyForTurn).toBe(true);
  await f.controller.beginNativeTurn('evt_second');
  const second = await f.controller.freezeNativeTurn({ ...f.completion(), turn_id: 'evt_second' });
  expect(second.checkpoint.files).toMatchObject([
    { path: 'nested/report.txt', sha256: createHash('sha256').update('later').digest('hex') },
  ]);
});

it.each(['symlink', 'hardlink', 'invalid-list', 'changed-read'])(
  'holds incomplete evidence and removes partial capture on unsafe output: %s',
  async (kind) => {
    const f = await fixture();
    await f.raw.files.write('/mnt/session/outputs/report.txt', Buffer.from('result'));
    const output = f.controller.placement!.outputs;
    if (kind === 'symlink')
      await symlink(f.controller.placement!.resolvedPaths.sesrsc_input!, join(output, 'alias'));
    if (kind === 'hardlink') await link(join(output, 'report.txt'), join(output, 'alias'));
    if (kind === 'invalid-list') vi.spyOn(f.raw.files, 'list').mockResolvedValue(['../escape']);
    if (kind === 'changed-read')
      vi.spyOn(f.raw.files, 'read').mockResolvedValue(Buffer.from('short'));
    await f.controller.beginNativeTurn('evt_turn');
    await expect(f.controller.freezeNativeTurn(f.completion())).rejects.toThrow(
      kind === 'invalid-list'
        ? 'Invalid native output entry'
        : kind === 'changed-read'
          ? 'changed during capture'
          : 'aliased',
    );
    expect(await f.controller.changes('ses_fixture', { type: 'pending' })).toMatchObject({
      checkpoint: null,
      incomplete_completion: { reports: f.completion().reports, report_set_sealed: false },
    });
    expect(await readdir(join(f.workspaceDir, 'private-resources'))).toEqual([]);
    expect(f.controller.readyForTurn).toBe(false);
    await expect(f.controller.beginNativeTurn('evt_retry')).rejects.toThrow('blocks next turn');
  },
);

it.each(['/.pi', '/.pi/config', '/.pi-agent-home', '/.pi-agent-home/config'])(
  'rejects provider configuration mounts before sandbox acquisition: %s',
  async (mount_path) => {
    const f = await fixture();
    f.acquire.mockClear();
    const manifest = { ...f.manifest, resources: [{ ...f.manifest.resources[0]!, mount_path }] };
    await expect(
      f.push({ type: 'manifest', placement: 'session_sandbox', manifest }),
    ).rejects.toThrow('overlaps provider configuration');
    expect(f.acquire).not.toHaveBeenCalled();
  },
);

it('recovers a frozen checkpoint after restart and refuses changed saved evidence', async () => {
  const f = await fixture();
  await f.controller.beginNativeTurn('evt_turn');
  const frozen = await f.controller.freezeNativeTurn(f.completion());
  const replacement = new RunnerResources({ workspaceDir: f.workspaceDir });
  expect(await replacement.changes('ses_fixture', { type: 'pending' })).toMatchObject({
    checkpoint: frozen.checkpoint,
  });
  const header = JSON.parse(
    await readFile(join(f.workspaceDir, 'private-native-turn/evt_turn.json'), 'utf8'),
  );
  const saved = {
    ...frozen.checkpoint,
    completion: { ...frozen.checkpoint.completion, outcome: 'failed' },
  };
  await writeFile(join(header.directory, 'checkpoint.json'), JSON.stringify(saved));
  const corrupted = new RunnerResources({ workspaceDir: f.workspaceDir });
  await expect(corrupted.changes('ses_fixture', { type: 'pending' })).rejects.toThrow(
    'checkpoint changed',
  );
});

async function loopFixture(events: AgentEvent[], submitError = false) {
  const f = await fixture();
  const harness: AgentHarness = {
    start: vi.fn(async () => {}),
    submit: vi.fn(async () => {
      if (submitError) throw new Error('model unavailable');
    }),
    refreshCredentials: vi.fn(async () => {}),
    stop: vi.fn(async () => {}),
    interrupt: vi.fn(),
    async *events() {
      yield* events;
    },
  };
  const factory = vi.fn(() => harness);
  const providers = new ProviderRegistry().register('pi', factory, { sessionSandboxReadPaths: [] });
  const loop = new SessionLoop({ workspaceId: 'ws_fixture', providers, resources: f.controller });
  cleanup.push(() => loop.stop());
  const snapshot = {
    provider: 'pi',
    model: { provider: 'anthropic', id: 'actual-model' },
    system: 'system',
    allowed_tool_names: ['read', 'write'],
    allowed_mcp_server_names: [],
    tool_permissions: {},
    egress: {},
    sandbox_resources: {
      version: 1,
      revision: f.manifest.revision,
      placement_id: f.controller.placement!.id,
    },
  };
  await loop.applySnapshot('ses_fixture', encode(snapshot));
  return { ...f, loop, snapshot, harness, factory };
}

it.each([
  ['missing terminal', [], 'ended without completion evidence'],
  [
    'invalid start',
    [{ kind: 'orca.native_usage_started', payload: { turn_id: 'other', id: 'report_1' } }],
    'Invalid native report start',
  ],
  [
    'unexpected report',
    [
      {
        kind: 'orca.native_usage',
        payload: {
          turn_id: 'evt_turn',
          report: { id: 'report_1', provider: 'anthropic', semantics: 'delta', complete: false },
        },
      },
    ],
    'Native report identity differs',
  ],
  [
    'invalid seal',
    [
      {
        kind: 'orca.native_usage_set',
        payload: { turn_id: 'evt_turn', outcome: 'completed', expected_reports: ['missing'] },
      },
    ],
    'expected report set differs',
  ],
] as const)('retains failed unsealed completion for %s', async (_name, events, message) => {
  const f = await loopFixture([...events] as AgentEvent[]);
  const lines = [];
  for await (const line of f.loop.runTurn(
    'ses_fixture',
    encode({ type: 'user.message', native_turn_id: 'evt_turn' }),
    new AbortController().signal,
  ))
    lines.push(JSON.parse(Buffer.from(line).toString()));
  expect(lines).toMatchObject([{ type: 'orca.native_completion' }]);
  expect(f.controller.pendingCheckpoint!.completion).toMatchObject({
    outcome: 'failed',
    report_set_sealed: false,
    error: expect.stringContaining(message),
  });
  expect(f.controller.readyForTurn).toBe(false);
});

it('keeps model submission failures explicit even when provider reports normal settlement', async () => {
  const f = await loopFixture(
    [
      {
        kind: 'orca.native_usage_set',
        payload: { turn_id: 'evt_turn', outcome: 'completed', expected_reports: [] },
      },
      { kind: 'agent.turn_completed', payload: {} },
    ],
    true,
  );
  for await (const _line of f.loop.runTurn(
    'ses_fixture',
    encode({ type: 'user.message', native_turn_id: 'evt_turn' }),
    new AbortController().signal,
  )) {
    /* drain */
  }
  expect(f.controller.pendingCheckpoint!.completion).toMatchObject({
    outcome: 'failed',
    error: 'model unavailable',
  });
  expect(f.harness.stop).toHaveBeenCalled();
});

it('refreshes native credentials without restarting history and rejects placement drift', async () => {
  const f = await loopFixture([]);
  await f.loop.applySnapshot('ses_fixture', encode(f.snapshot));
  expect(f.factory).toHaveBeenCalledOnce();
  expect(f.harness.start).toHaveBeenCalledOnce();
  expect(f.harness.refreshCredentials).toHaveBeenCalledOnce();
  vi.spyOn(f.controller, 'placement', 'get').mockReturnValue(undefined);
  await expect(f.loop.applySnapshot('ses_fixture', encode(f.snapshot))).rejects.toThrow(
    'cannot reuse this execution',
  );
});

it.each(['revision', 'placement', 'bash', 'provider'])(
  'refuses unsupported native snapshot before provider construction: %s',
  async (kind) => {
    const f = await loopFixture([]);
    const snapshot = structuredClone(f.snapshot);
    if (kind === 'revision') snapshot.sandbox_resources.revision = 'b'.repeat(64);
    if (kind === 'placement') snapshot.sandbox_resources.placement_id = 'other';
    if (kind === 'bash') snapshot.allowed_tool_names = ['bash'];
    if (kind === 'provider') snapshot.provider = 'custom';
    await expect(f.loop.applySnapshot('ses_fixture', encode(snapshot))).rejects.toThrow(
      'not committed or supported',
    );
    expect(f.factory).toHaveBeenCalledOnce();
  },
);
