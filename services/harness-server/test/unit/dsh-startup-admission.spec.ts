// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import type { TranscriptStore } from '@orca/transcript-store';
import type { AgentHarness, UserEvent } from '../../src/harness/agent-harness.js';
import type { PreparedExecutionV2, RegistryClient } from '../../src/clients/registry.js';
import { Dispatcher } from '../../src/runner/dispatcher.js';
import { SessionRunner } from '../../src/runner/session-runner.js';
import { dshProfileConfigSha256 } from '@orca/harness-catalog';

const child = 'subagents/11111111-1111-4111-8111-111111111111';
const key = 'ws_test/ses_test';
const runners: SessionRunner[] = [];
afterEach(async () => {
  await Promise.all(runners.splice(0).map((runner) => runner.stop('error')));
});
function prepared(paths: string[] = []): PreparedExecutionV2 {
  return {
    schema_version: 2,
    workspace_id: 'ws_test',
    archived_thread_subpaths: paths,
    session: { id: 'ses_test', workspace_id: 'ws_test', agent_id: 'agt_test', agent_version: 1 },
    primary_agent: {
      id: 'agt_test',
      workspace_id: 'ws_test',
      version: 1,
      skills: [],
      metadata: { harness: 'dsh_sdk' },
    },
    subagents: [],
    environment: null,
    resources: [],
    vault_credentials: [],
  } as unknown as PreparedExecutionV2;
}
async function runner() {
  const archived = new Set<string>();
  const calls: string[] = [];
  const harness: AgentHarness = {
    supportsNativeThreadInterrupt: true,
    start: async () => {},
    submit: vi.fn(async (event: UserEvent) => {
      calls.push(event.kind);
      if (event.kind === 'user.message' && archived.has(event.subpath ?? ''))
        throw new Error('archived');
      if (event.kind === 'user.interrupt') archived.add(event.subpath ?? '');
    }),
    applyArchivedThreadSubpaths: vi.fn(async (paths) => {
      for (const path of paths) archived.add(path);
    }),
    stop: vi.fn(async () => {}),
    async *events() {},
  };
  const result = new SessionRunner({
    workspaceId: 'ws_test',
    sessionId: 'ses_test',
    harness,
    store: {} as TranscriptStore,
  });
  runners.push(result);
  await result.start({ agentSnapshot: {} });
  return { runner: result, harness, calls };
}
function dispatcher(prepareExecution: RegistryClient['prepareExecution']) {
  const result = new Dispatcher({
    groupId: 'admission',
    anthropicApiKey: 'unused',
    modelDefault: 'unused',
    registry: { prepareExecution } as RegistryClient,
    store: {} as TranscriptStore,
  });
  return result as unknown as {
    runners: Map<string, SessionRunner>;
    admitDshRunner(
      ws: string,
      session: string,
      runner: SessionRunner,
      generation: number,
    ): Promise<SessionRunner>;
    respawnRunnerIfSessionConfigChanged(
      ws: string,
      session: string,
      key: string,
      kind: string,
      runner: SessionRunner,
      generation: number,
    ): Promise<SessionRunner | null>;
    refreshDeferredGuardrailUsageState(
      ws: string,
      session: string,
      runner: SessionRunner,
      generation: number,
    ): Promise<boolean>;
  };
}

it.each([
  'missing',
  'digest',
  'source',
  'version',
  'session_override',
  'resource_home',
  'resource_parent',
] as const)(
  'rejects unusable custom execution snapshots before accepting input: %s',
  async (failure) => {
    const value = prepared();
    const files = {
      'cordis.patch.yml': '[]',
      'package.json': '{}',
      'pnpm-lock.yaml': '',
      'pnpm-workspace.yaml': '',
    };
    value.primary_agent.metadata = {
      harness: 'dsh_sdk',
      mode: 'colocated',
      dsh_profile: 'file_profile',
    };
    if (failure !== 'missing')
      value.primary_agent.dsh_profile = {
        version: 1,
        source_file_id: failure === 'source' ? 'file_other' : 'file_profile',
        source_sha256: 'a'.repeat(64),
        config_sha256: failure === 'digest' ? '0'.repeat(64) : dshProfileConfigSha256(files),
        files,
      };
    if (failure === 'version') value.primary_agent.version++;
    if (failure === 'session_override') value.session.metadata = { dsh_profile: 'file_override' };
    if (failure === 'resource_home' || failure === 'resource_parent')
      value.resources = [
        {
          id: 'sesrsc_private',
          type: 'file',
          file_id: 'file_data',
          memory_store_id: null,
          repo_ref: null,
          mount_path:
            failure === 'resource_home' ? '/app/packages/dsh-harness/.orca-dsh/sessions' : '/app',
          access: 'read_write',
          instructions: null,
          mount_strategy: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
        },
      ];
    const f = await runner();
    const d = dispatcher(async () => value);
    await expect(d.admitDshRunner('ws_test', 'ses_test', f.runner, 0)).rejects.toThrow(
      'archive admission failed',
    );
    expect(f.harness.submit).not.toHaveBeenCalled();
  },
);

it('registers before the authoritative read, holds follow-up, and lets live archive bypass admission', async () => {
  let resolve!: (value: PreparedExecutionV2) => void;
  const read = new Promise<PreparedExecutionV2>((ready) => {
    resolve = ready;
  });
  const f = await runner();
  const prepare = vi.fn(() => read);
  const d = dispatcher(prepare);
  const admission = d.admitDshRunner('ws_test', 'ses_test', f.runner, 0);
  expect(d.runners.get(key)).toBe(f.runner);
  const followup = f.runner.submit({ kind: 'user.message', subpath: child, payload: {} });
  const denied = expect(followup).rejects.toThrow('archived');
  await Promise.resolve();
  expect(f.harness.submit).not.toHaveBeenCalled();
  await f.runner.submit({
    kind: 'user.interrupt',
    subpath: child,
    payload: { stop_reason: 'archived' },
  });
  expect(f.calls).toEqual(['user.interrupt']);
  resolve(prepared([child]));
  await admission;
  await denied;
  expect(f.harness.applyArchivedThreadSubpaths).toHaveBeenCalledWith([child]);
});

it('failed startup read rejects waiting input and removes only the current generation without model execution', async () => {
  let fail!: (error: Error) => void;
  const read = new Promise<PreparedExecutionV2>((_, reject) => {
    fail = reject;
  });
  const f = await runner();
  const d = dispatcher(() => read);
  const admission = d.admitDshRunner('ws_test', 'ses_test', f.runner, 0);
  const rejected = expect(admission).rejects.toThrow('archive admission failed');
  const input = f.runner.submit({ kind: 'user.message', payload: {} });
  const rejectedInput = expect(input).rejects.toThrow();
  fail(new Error('query failed'));
  await rejected;
  await rejectedInput;
  expect(d.runners.has(key)).toBe(false);
  expect(f.harness.submit).not.toHaveBeenCalled();
  expect(f.harness.stop).toHaveBeenCalled();
});

it('a concurrent ready candidate never overwrites the existing generation', async () => {
  const winner = await runner();
  const loser = await runner();
  const prepare = vi.fn(async () => prepared());
  const d = dispatcher(prepare);
  d.runners.set(key, winner.runner);
  expect(await d.admitDshRunner('ws_test', 'ses_test', loser.runner, 0)).toBe(winner.runner);
  expect(d.runners.get(key)).toBe(winner.runner);
  expect(loser.harness.stop).toHaveBeenCalledOnce();
  expect(prepare).not.toHaveBeenCalled();
  expect(loser.harness.submit).not.toHaveBeenCalled();
});

it('a late startup read cannot reopen admission or remove a replacement generation', async () => {
  let resolve!: (value: PreparedExecutionV2) => void;
  const read = new Promise<PreparedExecutionV2>((ready) => {
    resolve = ready;
  });
  const old = await runner();
  const replacement = await runner();
  const d = dispatcher(() => read);
  const admission = d.admitDshRunner('ws_test', 'ses_test', old.runner, 0);
  const rejected = expect(admission).rejects.toThrow('archive admission failed');
  const input = old.runner.submit({ kind: 'user.message', payload: {} });
  const rejectedInput = expect(input).rejects.toThrow();
  d.runners.set(key, replacement.runner);
  resolve(prepared());
  await rejected;
  await rejectedInput;
  expect(d.runners.get(key)).toBe(replacement.runner);
  expect(old.harness.submit).not.toHaveBeenCalled();
  expect(replacement.harness.stop).not.toHaveBeenCalled();
});

it('both warm prepared refresh paths install durable archive state before next input', async () => {
  const f = await runner();
  const d = dispatcher(vi.fn(async () => prepared([child])));
  d.runners.set(key, f.runner);
  expect(
    await d.respawnRunnerIfSessionConfigChanged(
      'ws_test',
      'ses_test',
      key,
      'user.message',
      f.runner,
      0,
    ),
  ).toBe(f.runner);
  expect(await d.refreshDeferredGuardrailUsageState('ws_test', 'ses_test', f.runner, 0)).toBe(true);
  expect(f.harness.applyArchivedThreadSubpaths).toHaveBeenCalledTimes(2);
  await expect(
    f.runner.submit({ kind: 'user.message', subpath: child, payload: {} }),
  ).rejects.toThrow('archived');
});

it('stop rejects an admission waiter even when Registry never returns', async () => {
  const f = await runner();
  f.runner.deferAdmissionUntil(new Promise(() => {}));
  const input = f.runner.submit({ kind: 'user.message', payload: {} });
  const rejected = expect(input).rejects.toThrow('stopping');
  await f.runner.stop('client.archived');
  await rejected;
  expect(f.harness.submit).not.toHaveBeenCalled();
});
