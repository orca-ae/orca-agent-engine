// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import type { TranscriptStore } from '@orca/transcript-store';
import {
  RegistryClient,
  type AgentRecord,
  type PreparedExecutionV2,
} from '../../src/clients/registry.js';
import type { AgentHarness } from '../../src/harness/agent-harness.js';
import { Dispatcher } from '../../src/runner/dispatcher.js';
import { SessionRunner } from '../../src/runner/session-runner.js';

const workspaceId = 'ws_test',
  sessionId = 'ses_test',
  key = `${workspaceId}/${sessionId}`;
const running: SessionRunner[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(running.splice(0).map((runner) => runner.stop('error')));
});
function agent(id: string): AgentRecord {
  return {
    id,
    name: id,
    workspace_id: workspaceId,
    version: 1,
    system: '',
    tools: [],
    mcp_servers: [],
    skills: [],
    multiagent: null,
  };
}
function prepared(): PreparedExecutionV2 {
  const ids = ['a', 'Z', 'ä'];
  return {
    schema_version: 2,
    workspace_id: workspaceId,
    archived_thread_subpaths: [],
    session: {
      id: sessionId,
      workspace_id: workspaceId,
      agent_id: 'agt_primary',
      agent_version: 1,
    },
    primary_agent: agent('agt_primary'),
    subagents: ids.map((id) => agent(`agt_${id}`)),
    environment: null,
    vault_credentials: ids.map((id) => ({
      credential_id: `vcrd_${id}`,
      vault_id: 'vlt_test',
      auth_type: 'environment_variable',
      mcp_server_url: null,
    })),
    resources: ids.map((id) => ({
      id: `sesrsc_${id}`,
      type: 'file',
      file_id: `file_${id}`,
      memory_store_id: null,
      repo_ref: null,
      mount_path: `/mnt/session/uploads/${id}`,
      access: 'read_only',
      instructions: null,
      attached_at: '2026-11-02T00:00:00Z',
      detached_at: null,
    })),
    guardrails: ids.map((id) => ({
      id: `grd_${id}`,
      name: id,
      tier: 'workspace',
      mode: 'enforce',
      phases: ['tool_call'],
      stateful: false,
      rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
    })),
    guardrail_state: {},
  };
}
async function makeRunner() {
  const harness: AgentHarness = {
    supportsNativeThreadInterrupt: true,
    start: vi.fn(async () => {}),
    submit: vi.fn(async () => {}),
    stop: vi.fn(async () => {}),
    applyArchivedThreadSubpaths: vi.fn(async () => {}),
    async *events() {},
  };
  const runner = new SessionRunner({
    workspaceId,
    sessionId,
    harness,
    store: {} as TranscriptStore,
  });
  running.push(runner);
  await runner.start({ agentSnapshot: {} });
  return runner;
}
type RuntimeDispatcher = {
  runners: Map<string, SessionRunner>;
  spawnRunnerWithMetrics(
    ws: string,
    session: string,
    prepared: PreparedExecutionV2,
  ): Promise<SessionRunner>;
  respawnRunnerIfSessionConfigChanged(
    ws: string,
    session: string,
    key: string,
    kind: string,
    runner: SessionRunner,
    generation: number,
  ): Promise<SessionRunner | null>;
};
async function fixture() {
  const snapshot = prepared(),
    current = await makeRunner(),
    replacement = await makeRunner();
  const registry = new RegistryClient('http://registry.test', async () => 'internal-test-token');
  vi.spyOn(registry, 'prepareExecution').mockImplementation(async () => structuredClone(snapshot));
  const dispatcher = new Dispatcher({
    groupId: 'runtime-config',
    registry,
    anthropicApiKey: 'unused',
    modelDefault: 'unused',
    store: {} as TranscriptStore,
  });
  const internal = dispatcher as unknown as RuntimeDispatcher;
  internal.runners.set(key, current);
  const spawn = vi.spyOn(internal, 'spawnRunnerWithMetrics').mockImplementation(async () => {
    internal.runners.set(key, replacement);
    return replacement;
  });
  const stop = vi.spyOn(current, 'stop');
  const mirror = vi.spyOn(current, 'applyGuardrailUsageState');
  const refresh = () =>
    internal.respawnRunnerIfSessionConfigChanged(
      workspaceId,
      sessionId,
      key,
      'user.message',
      current,
      0,
    );
  return { snapshot, current, replacement, spawn, stop, mirror, refresh };
}
function refuseLocaleSorting() {
  vi.spyOn(String.prototype, 'localeCompare').mockImplementation(() => {
    throw new Error('Runtime configuration must not depend on locale');
  });
}
describe('warm runner configuration identity', () => {
  it('reuses a runner for reordered prepared collections without locale comparisons', async () => {
    const f = await fixture();
    refuseLocaleSorting();
    expect(await f.refresh()).toBe(f.current);
    f.snapshot.subagents.reverse();
    f.snapshot.guardrails!.reverse();
    f.snapshot.vault_credentials.reverse();
    f.snapshot.resources.reverse();
    f.snapshot.guardrail_state = { session_cost_usd: 2 };
    expect(await f.refresh()).toBe(f.current);
    expect(f.spawn).not.toHaveBeenCalled();
    expect(f.stop).not.toHaveBeenCalled();
    expect(f.mirror).toHaveBeenLastCalledWith({ session_cost_usd: 2 });
  });
  it('replaces a warm runner when a guardrail changes under locale-independent ordering', async () => {
    const f = await fixture();
    refuseLocaleSorting();
    expect(await f.refresh()).toBe(f.current);
    f.snapshot.guardrails![0]!.mode = 'observe';
    expect(await f.refresh()).toBe(f.replacement);
    expect(f.stop).toHaveBeenCalledWith('session.updated');
    expect(f.spawn).toHaveBeenCalledOnce();
    expect(f.spawn).toHaveBeenCalledWith(
      workspaceId,
      sessionId,
      expect.objectContaining({
        guardrails: expect.arrayContaining([expect.objectContaining({ mode: 'observe' })]),
      }),
    );
  });
});
