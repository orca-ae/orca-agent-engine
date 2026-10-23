// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, rm, stat, readFile, readdir, mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RunnerResources } from '../../src/resources.js';
import { SessionLoop } from '../../src/session-loop.js';
import { ProviderRegistry } from '../../src/harness/provider.js';
import type { AgentHarness, AgentEvent } from '../../src/harness/agent-harness.js';
import { nativeFileToolSandbox } from '../../src/harness/pi/file-policy.js';
import {
  createRunnerSandboxRuntime,
  SandboxManager,
  resourceManifestDigest,
  resourceCheckpointDigest,
  NATIVE_USAGE_SOURCE,
  type ResourceManifest,
  type NativeCompletion,
} from '../../src/sandbox/seam.js';

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0)) await close();
});
const local = describe.skipIf(
  !['darwin', 'linux'].includes(process.platform) || process.versions.node.split('.')[0] !== '22',
);

async function fixture() {
  const workspaceDir = await mkdtemp(join(tmpdir(), 'orca-native-resource-test-'));
  const controller = new RunnerResources({
    workspaceDir,
    sessionSandbox: createRunnerSandboxRuntime({
      kind: 'local',
      local: {
        harnessWorkDir: join(workspaceDir, 'sandbox'),
        allowedNetworkHosts: [],
        manager: SandboxManager,
        extraReadPaths: [process.execPath],
      },
    }),
  });
  cleanup.push(async () => {
    await controller.close();
    await SandboxManager.reset();
    await rm(workspaceDir, { recursive: true, force: true });
  });
  const bytes = Buffer.from('attached-context');
  const manifest: ResourceManifest = {
    version: 1,
    revision: 'a'.repeat(64),
    resources: [
      {
        resource_id: 'sesrsc_fixture',
        kind: 'file',
        mount_path: '/mnt/context.txt',
        access: 'read_only',
        files: [
          {
            path: '',
            sha256: createHash('sha256').update(bytes).digest('hex'),
            size_bytes: bytes.length,
            mode: 0o444,
          },
        ],
      },
    ],
  };
  const push = (body: unknown) => controller.push('ses_fixture', body);
  await push({ type: 'manifest', placement: 'session_sandbox', manifest });
  await push({
    type: 'file_chunk',
    revision: manifest.revision,
    resource_id: 'sesrsc_fixture',
    path: '',
    offset: 0,
    content_base64: bytes.toString('base64'),
  });
  await push({
    type: 'commit',
    revision: manifest.revision,
    manifest_sha256: resourceManifestDigest(manifest),
  });
  const completion = (turn = 'evt_turn'): NativeCompletion => ({
    version: 1,
    source: NATIVE_USAGE_SOURCE,
    turn_id: turn,
    execution_id: controller.executionId,
    placement_id: controller.placement!.id,
    outcome: 'success',
    report_set_sealed: true,
    expected_reports: ['report_1'],
    reports: [
      {
        id: 'report_1',
        provider: 'anthropic',
        model: 'actual-model',
        semantics: 'delta',
        complete: true,
        usage: {
          input_tokens: 13,
          output_tokens: 7,
          cache_read_input_tokens: 0,
          cache_creation: { ephemeral_5m_input_tokens: 0, ephemeral_1h_input_tokens: 0 },
        },
      },
    ],
  });
  return { controller, manifest, push, completion, workspaceDir };
}

local('verified native placement and checkpoint barriers', () => {
  it('projects public paths without changing verified guidance or nested tool arguments', async () => {
    const { controller } = await fixture();
    const placement = controller.placement!;
    const value = {
      input: { path: placement.resolvedPaths.sesrsc_fixture },
      content: ['Wrote ' + placement.outputs + '/report.txt'],
    };
    expect(controller.projectNativePublicValue(value)).toEqual({
      input: { path: '/mnt/context.txt' },
      content: ['Wrote /mnt/session/outputs/report.txt'],
    });
    expect(value.input.path).toBe(placement.resolvedPaths.sesrsc_fixture);
    expect(placement.guidance).toContain(placement.resolvedPaths.sesrsc_fixture);
  });

  it.each(['success', 'failed', 'interrupted'] as const)(
    'retains %s evidence over restart, then cleans reports and retries completed ACK idempotently',
    async (outcome) => {
      const { controller, completion, workspaceDir } = await fixture();
      const sandboxRoot = controller.placement!.outputs.slice(0, -'/mnt/session/outputs'.length);
      await controller.beginNativeTurn('evt_turn');
      await controller.retainNativeReports({ reports: completion().reports });
      const temporary = join(
        workspaceDir,
        'private-native-turn/evt_turn.reports.json.00000000-0000-0000-0000-000000000000',
      );
      await writeFile(temporary, 'retained temporary report');
      const frozen = await controller.freezeNativeTurn({ ...completion(), outcome });
      const ack = {
        checkpoint_id: frozen.checkpoint.checkpoint_id,
        manifest_sha256: frozen.digest,
      };
      await controller.acknowledge('ses_fixture', { ...ack, completion_phase: 'prepared' });
      await controller.close();
      expect(await readFile(temporary, 'utf8')).toBe('retained temporary report');
      const replacement = new RunnerResources({ workspaceDir });
      cleanup.push(() => replacement.close());
      expect(await replacement.changes('ses_fixture', { type: 'pending' })).toEqual({
        checkpoint: null,
      });
      expect(await readdir(join(workspaceDir, 'private-native-turn'))).toContain(
        'evt_turn.reports.ndjson',
      );
      await replacement.acknowledge('ses_fixture', { ...ack, completion_phase: 'prepared' });
      await replacement.acknowledge('ses_fixture', { ...ack, completion_phase: 'completed' });
      expect(await readdir(join(workspaceDir, 'private-native-turn'))).toEqual(['evt_turn.json']);
      expect(await readdir(join(workspaceDir, 'private-resources'))).toEqual([]);
      await expect(stat(sandboxRoot)).rejects.toMatchObject({ code: 'ENOENT' });
      const tombstone = await readFile(
        join(workspaceDir, 'private-native-turn/evt_turn.json'),
        'utf8',
      );
      expect(tombstone).not.toContain('reports');
      expect(tombstone.length).toBeLessThan(512);
      await replacement.close();
      const restarted = new RunnerResources({ workspaceDir });
      cleanup.push(() => restarted.close());
      await restarted.acknowledge('ses_fixture', { ...ack, completion_phase: 'completed' });
      await restarted.acknowledge('ses_fixture', { ...ack, completion_phase: 'prepared' });
    },
  );

  it('recovers cleanup failure only after recording durable completed evidence', async () => {
    const { controller, completion, workspaceDir } = await fixture();
    await controller.beginNativeTurn('evt_turn');
    const frozen = await controller.freezeNativeTurn(completion());
    const ack = { checkpoint_id: frozen.checkpoint.checkpoint_id, manifest_sha256: frozen.digest };
    await controller.acknowledge('ses_fixture', { ...ack, completion_phase: 'prepared' });
    const obstruction = join(workspaceDir, 'private-native-turn/evt_turn.reports.json');
    await mkdir(obstruction);
    await expect(
      controller.acknowledge('ses_fixture', { ...ack, completion_phase: 'completed' }),
    ).rejects.toThrow();
    expect(
      JSON.parse(await readFile(join(workspaceDir, 'private-native-turn/evt_turn.json'), 'utf8'))
        .phase,
    ).toBe('completed');
    await controller.close();
    await rm(obstruction, { recursive: true });
    const replacement = new RunnerResources({ workspaceDir });
    cleanup.push(() => replacement.close());
    await replacement.acknowledge('ses_fixture', { ...ack, completion_phase: 'completed' });
    expect(await readdir(join(workspaceDir, 'private-native-turn'))).toEqual(['evt_turn.json']);
  });
  it('marks invalid event evidence incomplete while keeping validated reports available for accounting', async () => {
    const { controller, completion, push } = await fixture();
    await controller.beginNativeTurn('evt_turn');
    const evidence = completion();
    evidence.events = [{ id: 'evt_invalid', type: 'unknown.event' }];
    await expect(controller.freezeNativeTurn(evidence)).rejects.toThrow('public event');
    expect(await push({ type: 'status' })).toMatchObject({ turn: { phase: 'incomplete' } });
    expect(await controller.changes('ses_fixture', { type: 'pending' })).toMatchObject({
      checkpoint: null,
      incomplete_completion: { report_set_sealed: false, reports: evidence.reports },
    });
  });
  it('streams approval before settlement and reattaches the same job with stable replay IDs', async () => {
    const { controller, manifest, completion, workspaceDir } = await fixture();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let submissions = 0;
    const refreshCredentials = vi.fn(async () => {});
    const harness: AgentHarness = {
      start: async () => {},
      refreshCredentials,
      submit: async () => {
        submissions++;
        await gate;
      },
      stop: async () => {
        release();
      },
      interrupt: () => {
        release();
      },
      async *events(): AsyncIterable<AgentEvent> {
        yield {
          kind: 'orca.native_usage_started',
          payload: { turn_id: 'evt_turn', id: 'report_1' },
        };
        yield { kind: 'agent.requires_action', payload: { tool_use_id: 'call_1' } };
        await gate;
        yield {
          kind: 'orca.native_usage',
          payload: { turn_id: 'evt_turn', report: completion().reports[0] },
        };
        yield {
          kind: 'orca.native_usage_set',
          payload: { turn_id: 'evt_turn', expected_reports: ['report_1'], outcome: 'completed' },
        };
        yield { kind: 'agent.turn_completed', payload: {} };
      },
    };
    const providers = new ProviderRegistry().register('pi', () => harness, {
      sessionSandboxReadPaths: [],
    });
    const loop = new SessionLoop({ workspaceId: 'ws_fixture', providers, resources: controller });
    cleanup.push(() => loop.stop());
    const snapshot = {
      provider: 'pi',
      model: { provider: 'anthropic', id: 'actual-model' },
      system: '',
      allowed_tool_names: ['read', 'write'],
      allowed_mcp_server_names: [],
      tool_permissions: {},
      egress: {
        mode: 'gateway',
        gateway: { llm_base_url: 'http://fixture.invalid', llm_jwt: 'first' },
      },
      sandbox_resources: {
        version: 1,
        revision: manifest.revision,
        placement_id: controller.placement!.id,
      },
    };
    const encode = (value: unknown) => Buffer.from(JSON.stringify(value));
    await loop.applySnapshot('ses_fixture', encode(snapshot));
    const body = encode({
      type: 'user.message',
      native_turn_id: 'evt_turn',
      content: [{ type: 'text', text: 'fixture' }],
    });
    const disconnected = new AbortController();
    const first = loop.runTurn('ses_fixture', body, disconnected.signal)[Symbol.asyncIterator]();
    const approval = JSON.parse(Buffer.from((await first.next()).value!).toString());
    expect(approval.type).toBe('agent.requires_action');
    expect(approval.id).toMatch(/^evt_/);
    expect(controller.pendingCheckpoint).toBeUndefined();
    disconnected.abort();
    await expect(first.next()).rejects.toThrow('detached before completion');
    expect(() =>
      loop.runTurn(
        'ses_fixture',
        encode({ type: 'user.message', native_turn_id: 'evt_next' }),
        new AbortController().signal,
      ),
    ).toThrow('not ready');
    expect(await loop.applyResources('ses_fixture', { type: 'status' })).toMatchObject({
      turn: { phase: 'running' },
    });
    await loop.applySkills(
      'ses_fixture',
      Buffer.from(JSON.stringify({ type: 'skills_manifest', dir: 'skills', skills: [] })),
    );
    await loop.applySnapshot(
      'ses_fixture',
      encode({
        ...snapshot,
        egress: {
          ...snapshot.egress,
          gateway: { ...snapshot.egress.gateway, llm_jwt: 'refreshed' },
        },
      }),
    );
    expect(refreshCredentials).toHaveBeenCalledWith('refreshed');
    await expect(
      loop.applySnapshot(
        'ses_fixture',
        encode({ ...snapshot, model: { provider: 'anthropic', id: 'changed' } }),
      ),
    ).rejects.toThrow('busy');
    const reattached = loop
      .runTurn('ses_fixture', body, new AbortController().signal)
      [Symbol.asyncIterator]();
    expect(JSON.parse(Buffer.from((await reattached.next()).value!).toString())).toEqual(approval);
    release();
    expect(JSON.parse(Buffer.from((await reattached.next()).value!).toString()).type).toBe(
      'orca.native_completion',
    );
    expect((await reattached.next()).done).toBe(true);
    expect(submissions).toBe(1);
    expect(controller.pendingCheckpoint!.completion!.events).toEqual([approval]);
    const retained = (
      await readFile(join(workspaceDir, 'private-native-turn/evt_turn.reports.ndjson'), 'utf8')
    )
      .trimEnd()
      .split('\n')
      .map((line) => JSON.parse(line));
    expect(retained.flatMap((entry) => entry.events ?? [])).toEqual([approval]);
  });

  it('restores retained accounting after process loss without inventing a frozen output checkpoint', async () => {
    const { controller, completion, workspaceDir } = await fixture();
    await controller.beginNativeTurn('evt_turn');
    const reports = completion().reports;
    await controller.retainNativeReports({
      turnId: 'evt_turn',
      expected: ['report_1', 'missing'],
      reports,
      closed: false,
      outcome: 'failed',
      events: [],
    });
    await controller.close();
    const replacement = new RunnerResources({ workspaceDir });
    cleanup.push(() => replacement.close());
    expect(await replacement.changes('ses_fixture', { type: 'pending' })).toMatchObject({
      checkpoint: null,
      incomplete_completion: {
        turn_id: 'evt_turn',
        report_set_sealed: false,
        expected_reports: ['missing', 'report_1'],
        reports,
      },
    });
  });

  it('freezes an explicit incomplete receipt for malformed report evidence', async () => {
    const { controller, manifest } = await fixture();
    const harness: AgentHarness = {
      start: async () => {},
      submit: async () => {},
      stop: async () => {},
      interrupt: () => {},
      async *events() {
        yield { kind: 'orca.native_usage', payload: { turn_id: 'another_turn' } };
      },
    };
    const providers = new ProviderRegistry().register('pi', () => harness, {
      sessionSandboxReadPaths: [],
    });
    const loop = new SessionLoop({ workspaceId: 'ws_fixture', providers, resources: controller });
    cleanup.push(() => loop.stop());
    await loop.applySnapshot(
      'ses_fixture',
      Buffer.from(
        JSON.stringify({
          provider: 'pi',
          model: { provider: 'anthropic', id: 'actual-model' },
          system: '',
          allowed_tool_names: [],
          allowed_mcp_server_names: [],
          tool_permissions: {},
          egress: {},
          sandbox_resources: {
            version: 1,
            revision: manifest.revision,
            placement_id: controller.placement!.id,
          },
        }),
      ),
    );
    for await (const _line of loop.runTurn(
      'ses_fixture',
      Buffer.from(
        JSON.stringify({
          type: 'user.message',
          native_turn_id: 'evt_turn',
          content: [{ type: 'text', text: 'fixture' }],
        }),
      ),
      new AbortController().signal,
    )) {
      /* drain */
    }
    expect(controller.pendingCheckpoint!.completion).toMatchObject({
      outcome: 'failed',
      report_set_sealed: false,
      reports: [],
      error: 'Native report belongs to another turn',
    });
  });
  it('retains an interrupt arriving while the accepted turn header is written', async () => {
    const { controller, manifest } = await fixture();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const begin = controller.beginNativeTurn.bind(controller);
    const started = vi.spyOn(controller, 'beginNativeTurn').mockImplementation(async (turnId) => {
      await begin(turnId);
      await gate;
    });
    let submitted!: () => void;
    const secondSubmission = new Promise<void>((resolve) => {
      submitted = resolve;
    });
    const submit = vi.fn(async () => {
      submitted();
    });
    const harness: AgentHarness = {
      start: async () => {},
      submit,
      stop: async () => {},
      interrupt: () => {},
      async *events() {
        await secondSubmission;
        yield {
          kind: 'orca.native_usage_set',
          payload: { turn_id: 'evt_second', expected_reports: [], outcome: 'completed' },
        };
        yield { kind: 'agent.turn_completed', payload: {} };
      },
    };
    const providers = new ProviderRegistry().register('pi', () => harness, {
      sessionSandboxReadPaths: [],
    });
    const loop = new SessionLoop({ workspaceId: 'ws_fixture', providers, resources: controller });
    cleanup.push(() => loop.stop());
    await loop.applySnapshot(
      'ses_fixture',
      Buffer.from(
        JSON.stringify({
          provider: 'pi',
          model: { provider: 'anthropic', id: 'actual-model' },
          system: '',
          allowed_tool_names: [],
          allowed_mcp_server_names: [],
          tool_permissions: {},
          egress: {},
          sandbox_resources: {
            version: 1,
            revision: manifest.revision,
            placement_id: controller.placement!.id,
          },
        }),
      ),
    );
    const stream = loop.runTurn(
      'ses_fixture',
      Buffer.from(
        JSON.stringify({
          type: 'user.message',
          native_turn_id: 'evt_turn',
          content: [{ type: 'text', text: 'fixture' }],
        }),
      ),
      new AbortController().signal,
    );
    expect(started).toHaveBeenCalledTimes(1);
    expect(loop.interruptTurn()).toBe(true);
    release();
    for await (const _line of stream) {
      /* drain */
    }
    expect(submit).not.toHaveBeenCalled();
    expect(controller.pendingCheckpoint!.completion).toMatchObject({
      outcome: 'interrupted',
      report_set_sealed: true,
      reports: [],
    });
    const frozen = controller.pendingCheckpoint!;
    const ack = {
      checkpoint_id: frozen.checkpoint_id,
      manifest_sha256: resourceCheckpointDigest(frozen),
    };
    await controller.acknowledge('ses_fixture', { ...ack, completion_phase: 'prepared' });
    await controller.acknowledge('ses_fixture', { ...ack, completion_phase: 'completed' });
    started.mockRestore();
    for await (const _line of loop.runTurn(
      'ses_fixture',
      Buffer.from(
        JSON.stringify({
          type: 'user.message',
          native_turn_id: 'evt_second',
          content: [{ type: 'text', text: 'second' }],
        }),
      ),
      new AbortController().signal,
    )) {
      /* drain */
    }
    expect(submit).toHaveBeenCalledTimes(1);
    expect(controller.pendingCheckpoint!.completion).toMatchObject({
      outcome: 'success',
      report_set_sealed: true,
    });
  });
  it('places a readonly File and publishes the verified process path and writable output root', async () => {
    const { controller, push } = await fixture();
    const placement = controller.placement!;
    const root = placement.outputs.slice(0, -'/mnt/session/outputs'.length);
    const native = nativeFileToolSandbox(placement.sandbox, root, placement.policy);
    expect((await native.files.read(placement.resolvedPaths.sesrsc_fixture!)).toString()).toBe(
      'attached-context',
    );
    await expect(
      native.files.write(placement.resolvedPaths.sesrsc_fixture!, Buffer.from('bad')),
    ).rejects.toThrow();
    await expect(native.files.read(root + '/.orca/native-placement')).rejects.toThrow();
    await expect(
      native.files.read(root + '/mnt/session/outputs/../../context.txt'),
    ).rejects.toThrow();
    expect(await readFile(placement.resolvedPaths.sesrsc_fixture!, 'utf8')).toBe(
      'attached-context',
    );
    expect((await stat(placement.resolvedPaths.sesrsc_fixture!)).mode & 0o777).toBe(0o444);
    expect(placement.guidance).toContain(
      '/mnt/context.txt => ' + placement.resolvedPaths.sesrsc_fixture,
    );
    expect(await push({ type: 'status' })).toMatchObject({
      usage_source: NATIVE_USAGE_SOURCE,
      completion_version: 1,
      placement_id: placement.id,
      outputs_directory: placement.outputs,
    });
    await expect(
      controller.toolSandbox!.files.write('/mnt/context.txt', Buffer.from('bad')),
    ).rejects.toThrow();
    await expect(controller.toolSandbox!.files.read('/.orca/native-placement')).rejects.toThrow();
    await controller.toolSandbox!.files.write(
      '/mnt/session/outputs/report.txt',
      Buffer.from('output'),
    );
    expect(await readFile(join(placement.outputs, 'report.txt'), 'utf8')).toBe('output');
  });

  it('retains a no-output receipt and blocks the next turn through both ACK phases', async () => {
    const { controller, completion } = await fixture();
    await controller.beginNativeTurn('evt_turn');
    const frozen = await controller.freezeNativeTurn(completion());
    expect(frozen.checkpoint.files).toEqual([]);
    await expect(controller.beginNativeTurn('evt_next')).rejects.toThrow();
    const ack = { checkpoint_id: frozen.checkpoint.checkpoint_id, manifest_sha256: frozen.digest };
    await expect(controller.acknowledge('ses_fixture', ack)).rejects.toThrow(
      'receipt acknowledgement',
    );
    await controller.acknowledge('ses_fixture', { ...ack, completion_phase: 'prepared' });
    expect(controller.pendingCheckpoint).toBeUndefined();
    await expect(controller.beginNativeTurn('evt_next')).rejects.toThrow();
    await controller.acknowledge('ses_fixture', { ...ack, completion_phase: 'prepared' });
    await controller.acknowledge('ses_fixture', { ...ack, completion_phase: 'completed' });
    await controller.acknowledge('ses_fixture', { ...ack, completion_phase: 'completed' });
    await expect(controller.beginNativeTurn('evt_next')).resolves.toBeUndefined();
  });

  it('freezes immutable output bytes, rejects incomplete report ACK, and retains evidence on close', async () => {
    const { controller, completion, workspaceDir } = await fixture();
    await controller.toolSandbox!.files.write(
      '/mnt/session/outputs/report.txt',
      Buffer.from('original'),
    );
    await controller.beginNativeTurn('evt_turn');
    const input = completion();
    input.expected_reports.push('missing_report');
    const frozen = await controller.freezeNativeTurn(input);
    await controller.toolSandbox!.files.write(
      '/mnt/session/outputs/report.txt',
      Buffer.from('changed'),
    );
    const chunk = (await controller.changes('ses_fixture', {
      type: 'file_chunk',
      checkpoint_id: frozen.checkpoint.checkpoint_id,
      resource_id: 'session_outputs',
      path: 'report.txt',
      offset: 0,
      length: 8,
    })) as { content_base64: string };
    expect(Buffer.from(chunk.content_base64, 'base64').toString()).toBe('original');
    await expect(
      controller.acknowledge('ses_fixture', {
        checkpoint_id: frozen.checkpoint.checkpoint_id,
        manifest_sha256: frozen.digest,
        completion_phase: 'prepared',
      }),
    ).rejects.toThrow('Incomplete');
    const root = controller.placement!.outputs;
    await controller.close();
    expect(await readFile(join(root, 'report.txt'), 'utf8')).toBe('changed');
    expect(workspaceDir).toContain('orca-native-resource-test-');
  });

  it('rejects provider-configuration mounts before staging', async () => {
    const { controller, manifest, push } = await fixture();
    const next = structuredClone(manifest);
    next.resources[0]!.mount_path = '/.pi-agent-home/settings.json';
    await expect(
      push({ type: 'manifest', placement: 'session_sandbox', manifest: next }),
    ).rejects.toThrow('configuration');
    expect(controller.placement).toBeDefined();
  });
});
