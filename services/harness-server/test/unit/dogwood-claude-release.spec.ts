// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0
import { describe, it, expect, vi } from 'vitest';
import type { TranscriptStore, Event } from '@orca/transcript-store';
import type { PermissionResult } from '@anthropic-ai/claude-agent-sdk';
import { ClaudeAgentSdkHarness } from '../../src/harness/claude/index.js';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';
import type { DogwoodDecision } from '../../src/harness/dogwood.js';
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
const empty = (): AsyncIterable<Event> => ({
  [Symbol.asyncIterator]: () => ({ next: async () => ({ done: true, value: undefined }) }),
});
async function fixture() {
  const store: TranscriptStore = {
    append: async () => [],
    read: empty,
    tail: empty,
    archive: async () => {},
    close: async () => {},
  };
  const sandbox = await new InMemorySandboxRuntime().acquire({});
  const harness = new ClaudeAgentSdkHarness({
    apiKey: 'unused',
    modelDefault: 'fake',
    adapter: new ClaudeAgentSdkAdapter(store, 'ws_test'),
    workspaceId: 'ws_test',
    sessionId: 'ses_test',
  });
  await harness.start({
    workspaceId: 'ws_test',
    sessionId: 'ses_test',
    sandbox,
    agentSnapshot: {
      allowed_tool_names: ['bash'],
      tool_permission_policies: { mcp__orca__bash: 'always_allow' },
    },
  });
  const internals = harness as unknown as {
    canUseManagedAgentTool: (
      name: string,
      input: Record<string, unknown>,
      options: { signal: AbortSignal; toolUseID: string },
    ) => Promise<PermissionResult>;
    pendingToolConfirmations: Map<string, unknown>;
  };
  return {
    harness,
    internals,
    close: async () => {
      await harness.stop('replica.shutting_down');
      await sandbox.destroy();
    },
  };
}
describe('Claude SDK callback final Dogwood release', () => {
  it('checks the live receipt after the direct authorization await', async () => {
    const f = await fixture(),
      controller = new AbortController(),
      ack = deferred<DogwoodDecision>();
    const release = vi.fn(() => !controller.signal.aborted),
      interrupt = vi.fn(async () => {});
    Object.assign(f.harness, { dogwood: { decide: () => ack.promise, stop: async () => {} } });
    try {
      const pending = f.internals.canUseManagedAgentTool(
        'mcp__orca__bash',
        { command: 'ls' },
        { signal: controller.signal, toolUseID: 'tool-direct' },
      );
      controller.abort();
      ack.resolve({ verdict: 'allow', release, interrupt });
      expect((await pending).behavior).toBe('deny');
      expect(release).toHaveBeenCalledOnce();
      expect(interrupt).toHaveBeenCalledOnce();
    } finally {
      await f.close();
    }
  });
  it('checks the receipt again immediately before resolving a confirmed allow', async () => {
    const f = await fixture(),
      ack = deferred<DogwoodDecision>(),
      controller = new AbortController();
    const release = vi.fn(() => false),
      interrupt = vi.fn(async () => {});
    const decide = vi
      .fn()
      .mockResolvedValueOnce({ verdict: 'ask', release: () => false, interrupt: async () => {} })
      .mockImplementationOnce(() => ack.promise);
    Object.assign(f.harness, { dogwood: { decide, stop: async () => {} } });
    try {
      const pending = f.internals.canUseManagedAgentTool(
        'mcp__orca__bash',
        { command: 'ls' },
        { signal: controller.signal, toolUseID: 'tool-confirmed' },
      );
      await vi.waitFor(() => expect(f.internals.pendingToolConfirmations.size).toBe(1));
      const publicId = [...f.internals.pendingToolConfirmations.keys()][0]!;
      const confirmation = f.harness.submit({
        id: 'evt_019aaa00-0000-7000-8000-000000000001',
        kind: 'user.tool_confirmation',
        payload: { tool_use_id: publicId, result: 'allow' },
      });
      await vi.waitFor(() => expect(decide).toHaveBeenCalledTimes(2));
      ack.resolve({ verdict: 'allow', release, interrupt });
      await confirmation;
      expect((await pending).behavior).toBe('deny');
      expect(release).toHaveBeenCalledOnce();
      expect(interrupt).toHaveBeenCalledOnce();
    } finally {
      await f.close();
    }
  });
});
