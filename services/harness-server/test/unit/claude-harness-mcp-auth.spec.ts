// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { TranscriptStore } from '@orca/transcript-store';

const { queryMock } = vi.hoisted(() => ({ queryMock: vi.fn() }));
vi.mock('@anthropic-ai/claude-agent-sdk', () => ({
  query: queryMock,
  createSdkMcpServer: vi.fn((config: unknown) => ({ type: 'sdk', instance: config })),
  tool: vi.fn((name: string, description: string, inputSchema: unknown, handler: unknown) => ({
    name,
    description,
    inputSchema,
    handler,
  })),
}));

import { ClaudeAgentSdkHarness } from '../../src/harness/claude/index.js';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import { SessionJwtProvider, type SessionJwt } from '../../src/mcp/session-jwt-provider.js';
import type { AgentEvent } from '../../src/harness/agent-harness.js';

function emptyStream<T>(): AsyncIterable<T> {
  return { async *[Symbol.asyncIterator]() {} };
}

function buildHarness() {
  const store: TranscriptStore = {
    append: async () => [],
    read: () => emptyStream(),
    tail: () => emptyStream(),
    archive: async () => {},
    close: async () => {},
  };
  return new ClaudeAgentSdkHarness({
    apiKey: 'unused',
    modelDefault: 'fake',
    adapter: new ClaudeAgentSdkAdapter(store, 'ws_test'),
    workspaceId: 'ws_test',
    sessionId: 'ses_test',
  });
}

const initialServers = {
  remote: {
    type: 'http' as const,
    url: 'https://gateway.example/v1/mcp',
    headers: {
      Authorization: 'Bearer initial-token',
      'X-Orca-Backend': 'remote',
      'X-Orca-Session-Id': 'ses_test',
      'X-Orca-Credential-Id': 'vcrd_test',
    },
    alwaysLoad: true as const,
  },
};
const message = { kind: 'user.message', payload: { content: [{ type: 'text', text: 'hello' }] } };

describe('Claude harness MCP JWT freshness before a new query', () => {
  beforeEach(() => {
    queryMock.mockReset();
    queryMock.mockImplementation(() => emptyStream());
  });

  it('uses the Session JWT for model calls only when gateway egress is selected', async () => {
    const getValidToken = vi
      .fn()
      .mockResolvedValueOnce({ token: 'llm-first', expiresAt: 1_900_000_000 })
      .mockResolvedValueOnce({ token: 'llm-second', expiresAt: 1_900_000_300 });
    const close = vi.fn();
    const direct = buildHarness();
    const gateway = new ClaudeAgentSdkHarness({
      apiKey: '',
      baseURL: 'http://gateway.test/v1/llm',
      llmGatewayJwtProvider: { getValidToken, close },
      modelDefault: 'fake',
      adapter: new ClaudeAgentSdkAdapter(
        {
          append: async () => [],
          read: () => emptyStream(),
          tail: () => emptyStream(),
          archive: async () => {},
          close: async () => {},
        },
        'ws_test',
      ),
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
    });
    try {
      await direct.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });
      await direct.submit(message);
      const directEnv = queryMock.mock.calls[0]![0].options.env;
      expect(directEnv['ANTHROPIC_API_KEY']).toBe('unused');
      expect(directEnv['ANTHROPIC_AUTH_TOKEN']).toBeUndefined();

      await gateway.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });
      await gateway.submit(message);
      await gateway.submit(message);
      expect(getValidToken).toHaveBeenCalledTimes(2);
      const first = queryMock.mock.calls[1]![0].options.env;
      const second = queryMock.mock.calls[2]![0].options.env;
      expect(first).toMatchObject({
        ANTHROPIC_BASE_URL: 'http://gateway.test/v1/llm',
        ANTHROPIC_AUTH_TOKEN: 'llm-first',
        ANTHROPIC_CUSTOM_HEADERS: 'X-Orca-Session-Id: ses_test',
      });
      expect(first['ANTHROPIC_API_KEY']).toBeUndefined();
      expect(second['ANTHROPIC_AUTH_TOKEN']).toBe('llm-second');
    } finally {
      await direct.stop('error');
      await gateway.stop('error');
    }
    expect(close).toHaveBeenCalledOnce();
  });

  it('gets a valid token for each new query without mutating routing or the orca instance', async () => {
    const getValidToken = vi
      .fn()
      .mockResolvedValueOnce({ token: 'first-token', expiresAt: 1_900_000_000 })
      .mockResolvedValueOnce({ token: 'second-token', expiresAt: 1_900_000_300 });
    const close = vi.fn();
    const harness = buildHarness();
    try {
      await harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: {},
        clientToolExecution: true,
        mcpServers: initialServers,
        mcpJwtProvider: { getValidToken, close },
      });
      await harness.submit(message);
      await harness.submit(message);
      expect(getValidToken).toHaveBeenCalledTimes(2);
      const first = queryMock.mock.calls[0]![0].options.mcpServers;
      const second = queryMock.mock.calls[1]![0].options.mcpServers;
      expect(first.remote).toEqual({
        ...initialServers.remote,
        headers: { ...initialServers.remote.headers, Authorization: 'Bearer first-token' },
      });
      expect(second.remote.headers.Authorization).toBe('Bearer second-token');
      expect(first.orca).toBeDefined();
      expect(second.orca).toBe(first.orca);
      expect(initialServers.remote.headers.Authorization).toBe('Bearer initial-token');
    } finally {
      await harness.stop('error');
    }
    expect(close).toHaveBeenCalledTimes(1);
  });

  it('does not launch a query with stale headers when refresh fails', async () => {
    const harness = buildHarness();
    try {
      await harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: {},
        mcpServers: initialServers,
        mcpJwtProvider: {
          getValidToken: vi.fn().mockRejectedValue(new Error('JWT refresh unavailable')),
          close: vi.fn(),
        },
      });
      await expect(harness.submit(message)).rejects.toThrow('JWT refresh unavailable');
      expect(queryMock).not.toHaveBeenCalled();
    } finally {
      await harness.stop('error');
    }
  });

  it('stops a query waiting for credentials without launching the SDK', async () => {
    let rejectRefresh!: (error: Error) => void;
    const getValidToken = vi.fn(
      () =>
        new Promise<never>((_, reject) => {
          rejectRefresh = reject;
        }),
    );
    const close = vi.fn(() => rejectRefresh(new Error('JWT provider closed')));
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {},
      mcpServers: initialServers,
      mcpJwtProvider: { getValidToken, close },
    });
    const outcome = harness.submit(message).then(
      () => undefined,
      (error: unknown) => error,
    );
    try {
      await vi.waitFor(() => expect(getValidToken).toHaveBeenCalledTimes(1));
      await harness.stop('error');
      expect(await outcome).toEqual(new Error('JWT provider closed'));
    } finally {
      await harness.stop('error');
    }
    expect(queryMock).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalledTimes(1);
  });

  it('interrupts pending JWT startup cleanly and reuses the shared mint for the next query', async () => {
    let resolveMint!: (jwt: SessionJwt) => void;
    const pendingMint = new Promise<SessionJwt>((resolve) => {
      resolveMint = resolve;
    });
    const mint = vi.fn((_signal: AbortSignal) => pendingMint);
    const provider = new SessionJwtProvider(mint);
    const harness = buildHarness();
    const events: AgentEvent[] = [];
    const drained = (async () => {
      for await (const event of harness.events()) events.push(event);
    })();
    try {
      await harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: {},
        mcpServers: initialServers,
        mcpJwtProvider: provider,
      });
      const first = harness.submit(message).then(
        (value) => ({ value }),
        (error: unknown) => ({ error }),
      );
      await vi.waitFor(() => expect(mint).toHaveBeenCalledTimes(1));
      await expect(harness.submit({ kind: 'user.interrupt', payload: {} })).resolves.toBe(
        'submitted',
      );
      expect(await first).toEqual({ value: 'submitted' });
      expect(queryMock).not.toHaveBeenCalled();
      await vi.waitFor(() => expect(events).toHaveLength(1));
      expect(events[0]).toMatchObject({
        kind: 'session.status_idle',
        payload: { stop_reason: { type: 'end_turn' } },
      });
      expect(events.some((event) => event.kind === 'session.error')).toBe(false);
      expect(mint.mock.calls[0]![0].aborted).toBe(false);

      const second = harness.submit(message);
      resolveMint({
        token: 'fresh-after-interrupt',
        expiresAt: Math.floor(Date.now() / 1000) + 300,
      });
      await expect(second).resolves.toBe('submitted');
      expect(mint).toHaveBeenCalledTimes(1);
      expect(queryMock).toHaveBeenCalledTimes(1);
      expect(queryMock.mock.calls[0]![0].options.mcpServers.remote.headers.Authorization).toBe(
        'Bearer fresh-after-interrupt',
      );
    } finally {
      await harness.stop('error');
      await drained;
    }
  });

  it('does not swallow a refresh failure when interrupt wins the continuation race', async () => {
    const failure = new Error('JWT refresh unavailable');
    let rejectRefresh!: (reason: unknown) => void;
    const pending = new Promise<SessionJwt>((_, reject) => {
      rejectRefresh = reject;
    });
    const getValidToken = vi.fn((_signal?: AbortSignal) => pending);
    const harness = buildHarness();
    const events: AgentEvent[] = [];
    const drained = (async () => {
      for await (const event of harness.events()) events.push(event);
    })();
    try {
      await harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: {},
        mcpServers: initialServers,
        mcpJwtProvider: { getValidToken, close: vi.fn() },
      });
      const outcome = harness.submit(message).then(
        () => undefined,
        (error: unknown) => error,
      );
      await vi.waitFor(() => expect(getValidToken).toHaveBeenCalledTimes(1));
      await harness.submit(
        { kind: 'user.interrupt', payload: {} },
        {
          // Reject first, then complete interrupt acceptance before runQuery's
          // rejected await resumes. Cancellation must not mask that failure.
          onAccepted: async () => {
            queueMicrotask(() => rejectRefresh(failure));
          },
        },
      );
      expect(getValidToken.mock.calls[0]![0]!.aborted).toBe(true);
      expect(await outcome).toBe(failure);
      expect(queryMock).not.toHaveBeenCalled();
      expect(events).toEqual([]);
    } finally {
      await harness.stop('error');
      await drained;
    }
  });
});
