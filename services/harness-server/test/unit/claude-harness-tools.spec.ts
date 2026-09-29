// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  buildMcpToolExposureOptions,
  ClaudeAgentSdkHarness,
} from '../../src/harness/claude/index.js';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';
import type { ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';

function buildStubStore(): TranscriptStore {
  return {
    append: async () => [],
    read: (_workspaceId: string, _sessionId: string, _opts: ReadOptions) => {
      void _workspaceId;
      void _sessionId;
      void _opts;
      return emptyAsyncIterable();
    },
    tail: (_workspaceId: string, _sessionId: string, _opts: TailOptions) => {
      void _workspaceId;
      void _sessionId;
      void _opts;
      return emptyAsyncIterable();
    },
    archive: async () => {},
    close: async () => {},
  } satisfies TranscriptStore;
}

function emptyAsyncIterable<T>(): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<T> {
      return {
        async next(): Promise<IteratorResult<T>> {
          return { done: true, value: undefined as T };
        },
      };
    },
  };
}

function getOrcaMcpServer(harness: ClaudeAgentSdkHarness): unknown {
  return (harness as unknown as { orcaMcpServer?: unknown }).orcaMcpServer;
}

describe('ClaudeAgentSdkHarness tool allowlist', () => {
  it('exposes remote-only MCP toolsets without SDK auto-approval rules', () => {
    expect(
      buildMcpToolExposureOptions({
        hasOrcaMcpServer: false,
        remoteMcpServerNames: ['gateway-e2e'],
      }),
    ).toEqual({
      tools: [],
      disallowedTools: expect.arrayContaining(['Bash', 'Read', 'Write']),
    });
  });

  it('exposes sandbox and remote MCP tools through the policy gate', () => {
    expect(
      buildMcpToolExposureOptions({
        hasOrcaMcpServer: true,
        remoteMcpServerNames: ['gateway-e2e'],
        allowedOrcaToolNames: ['read'],
      }),
    ).toEqual({
      tools: [],
      disallowedTools: expect.arrayContaining(['Bash', 'Read', 'Write']),
    });
  });

  it('exposes the Agent tool only when multi-agent subagents are configured', () => {
    expect(
      buildMcpToolExposureOptions({
        hasOrcaMcpServer: true,
        remoteMcpServerNames: [],
        enableAgentTool: true,
      }),
    ).toEqual({
      tools: ['Agent'],
      disallowedTools: expect.arrayContaining(['Bash', 'Read', 'Write']),
    });
  });

  it('keeps the sandbox MCP server wired when the agent allows zero tools', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    try {
      const harness = new ClaudeAgentSdkHarness({
        apiKey: 'unused',
        modelDefault: 'fake',
        adapter: new ClaudeAgentSdkAdapter(buildStubStore(), 'ws_test'),
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
      });

      await harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: { allowed_tool_names: [] },
        sandbox,
      });

      const server = getOrcaMcpServer(harness) as { instance?: unknown };
      expect(server).toBeTruthy();
      expect(server.instance).toBeTruthy();
    } finally {
      await sandbox.destroy();
    }
  });
});
