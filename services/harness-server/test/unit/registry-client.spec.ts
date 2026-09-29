// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import { RegistryClient, RegistryInvalidRuntimeBindingError } from '../../src/clients/registry.js';
import { SessionJwtProvider } from '../../src/mcp/session-jwt-provider.js';

const TEST_TOKEN_PROVIDER = async () => 'test-internal-service-token';

interface FetchCall {
  input: RequestInfo | URL;
  init: RequestInit | undefined;
}

function preparedExecution(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  const resolvedSkill = {
    id: 'sklv_primary',
    skill_id: 'skl_primary',
    source: 'custom',
    version_identifier: '2',
    name: 'primary-skill',
    description: 'Primary skill description',
    entrypoint: 'SKILL.md',
    package_sha256: 'a'.repeat(64),
    package_size_bytes: 123,
  };
  return {
    schema_version: 2,
    workspace_id: 'ws_one',
    session: {
      id: 'ses_one',
      workspace_id: 'ws_one',
      runtime_revision: 4,
      status: 'idle',
      agent_id: 'agt_primary',
      agent_version: 3,
      environment_id: null,
      vault_ids: [],
      metadata: {},
    },
    primary_agent: {
      id: 'agt_primary',
      name: 'Primary',
      workspace_id: 'ws_one',
      version: 3,
      model: {
        provider: 'anthropic',
        id: 'claude-opus-5',
        speed: 'fast',
        effort: 'high',
      },
      system: 'Primary prompt',
      tools: [],
      mcp_servers: [],
      skills: [resolvedSkill],
      metadata: {},
      multiagent: null,
    },
    subagents: [],
    environment: null,
    vault_credentials: [],
    resources: [],
    ...overrides,
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe('RegistryClient mintSessionJwt', () => {
  const client = () => new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);
  const expiry = () => Math.floor(Date.now() / 1000) + 3600;
  const signal = new AbortController().signal;

  it('preserves scope, allowlists and workload authorization with the caller signal', async () => {
    const expiresAt = expiry();
    const fetchSpy = vi
      .fn()
      .mockResolvedValue(Response.json({ token: 'jwt', expires_at: expiresAt }));
    vi.stubGlobal('fetch', fetchSpy);
    await expect(
      client().mintSessionJwt('ws_one', 'ses_one', ['github'], ['vlt_one'], signal),
    ).resolves.toEqual({ token: 'jwt', expiresAt });
    const [url, init] = fetchSpy.mock.calls[0]!;
    expect(url).toBe(
      'https://registry.internal/internal/v1/workspaces/ws_one/sessions/ses_one/mint-jwt',
    );
    expect(init.method).toBe('POST');
    expect(JSON.parse(init.body)).toEqual({ mcp_server_names: ['github'], vault_ids: ['vlt_one'] });
    expect(new Headers(init.headers).get('authorization')).toBe(
      'Bearer test-internal-service-token',
    );
    expect(new Headers(init.headers).has('x-api-key')).toBe(false);
    expect(init.signal).toBe(signal);
  });

  it.each([400, 401, 403, 404, 408, 429, 500, 503])('sanitizes HTTP %i', async (status) => {
    const response = new Response('secret-token-body', { status });
    const read = vi.spyOn(response, 'text');
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response));
    const error = await client()
      .mintSessionJwt('ws_one', 'ses_one', [], [], signal)
      .catch((e: unknown) => e);
    expect(error).toEqual(new Error(`mintSessionJwt failed: HTTP ${status}`));
    expect(String(error)).not.toContain('secret');
    expect(JSON.stringify(error)).not.toContain('secret');
    expect(read).not.toHaveBeenCalled();
  });

  it.each([
    null,
    [],
    {},
    { token: '', expires_at: 9999999999 },
    { token: 'bad token', expires_at: 9999999999 },
    { token: 'secret', expires_at: '9999999999' },
    { token: 'secret', expires_at: null },
    { token: 'secret', expires_at: 0 },
    { token: 'secret', expires_at: 9999999999.5 },
  ])('rejects invalid body %j without leaking content', async (body) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(Response.json(body)));
    await expect(client().mintSessionJwt('ws_one', 'ses_one', [], [], signal)).rejects.toThrow(
      /^mintSessionJwt returned an invalid response$/,
    );
  });

  it('sanitizes malformed JSON and transport errors', async () => {
    const fetchSpy = vi
      .fn()
      .mockResolvedValueOnce(new Response('secret malformed JSON'))
      .mockRejectedValueOnce(new Error('secret transport'));
    vi.stubGlobal('fetch', fetchSpy);
    await expect(client().mintSessionJwt('ws_one', 'ses_one', [], [], signal)).rejects.toThrow(
      /^mintSessionJwt returned an invalid response$/,
    );
    await expect(client().mintSessionJwt('ws_one', 'ses_one', [], [], signal)).rejects.toThrow(
      /^mintSessionJwt request failed$/,
    );
  });

  it('rejects invalid path scope without fetching', async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal('fetch', fetchSpy);
    await expect(client().mintSessionJwt('..', 'ses_one', [], [], signal)).rejects.toThrow();
    await expect(client().mintSessionJwt('ws_one', '../ses_one', [], [], signal)).rejects.toThrow();
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it('aborts a pending fetch even when fetch ignores the signal', async () => {
    let fetchSignal!: AbortSignal;
    let started!: () => void;
    const start = new Promise<void>((resolve) => {
      started = resolve;
    });
    vi.stubGlobal(
      'fetch',
      vi.fn((_url, init) => {
        fetchSignal = init.signal;
        started();
        return new Promise(() => {});
      }),
    );
    const controller = new AbortController();
    const result = client().mintSessionJwt('ws_one', 'ses_one', [], [], controller.signal);
    const rejected = expect(result).rejects.toMatchObject({
      name: 'AbortError',
      message: 'mintSessionJwt request cancelled',
    });
    await start;
    controller.abort(new Error('secret abort reason'));
    await rejected;
    expect(fetchSignal.aborted).toBe(true);
  });

  it('uses only the JWT provider deadline while acquiring workload identity, preventing late fetch', async () => {
    vi.useFakeTimers();
    const extraDeadline = vi.spyOn(AbortSignal, 'timeout');
    let resolveToken!: (token: string) => void;
    const token = new Promise<string>((resolve) => {
      resolveToken = resolve;
    });
    const fetchSpy = vi.fn();
    vi.stubGlobal('fetch', fetchSpy);
    const registry = new RegistryClient('https://registry.internal', () => token);
    const provider = new SessionJwtProvider((signal) =>
      registry.mintSessionJwt('ws_one', 'ses_one', [], [], signal),
    );
    const rejected = expect(provider.getValidToken()).rejects.toThrow('refresh timed out');
    expect(extraDeadline).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(10_000);
    await rejected;
    expect(vi.getTimerCount()).toBe(0);
    resolveToken('late-token');
    await vi.advanceTimersByTimeAsync(0);
    expect(fetchSpy).not.toHaveBeenCalled();
    provider.close();
  });

  it('uses the JWT provider deadline for body reading and consumes a late parse rejection', async () => {
    vi.useFakeTimers();
    let rejectBody!: (reason: unknown) => void;
    let started!: () => void;
    const start = new Promise<void>((resolve) => {
      started = resolve;
    });
    const response = new Response();
    vi.spyOn(response, 'json').mockImplementation(() => {
      started();
      return new Promise((_resolve, reject) => {
        rejectBody = reject;
      });
    });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response));
    const registry = client();
    const provider = new SessionJwtProvider((signal) =>
      registry.mintSessionJwt('ws_one', 'ses_one', [], [], signal),
    );
    const rejected = expect(provider.getValidToken()).rejects.toThrow('refresh timed out');
    await start;
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(10_000);
    await rejected;
    rejectBody(new Error('late secret body'));
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(0);
    provider.close();
  });

  it('uses the JWT provider deadline for fetch and never installs a late response', async () => {
    vi.useFakeTimers();
    let resolveFetch!: (response: Response) => void;
    const pending = new Promise<Response>((resolve) => {
      resolveFetch = resolve;
    });
    let started!: () => void;
    const start = new Promise<void>((resolve) => {
      started = resolve;
    });
    const fetchSpy = vi.fn().mockImplementationOnce(() => {
      started();
      return pending;
    });
    vi.stubGlobal('fetch', fetchSpy);
    const registry = client();
    const provider = new SessionJwtProvider((signal) =>
      registry.mintSessionJwt('ws_one', 'ses_one', [], [], signal),
    );
    const rejected = expect(provider.getValidToken()).rejects.toThrow('refresh timed out');
    await start;
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(10_000);
    await rejected;
    expect(vi.getTimerCount()).toBe(0);
    fetchSpy.mockResolvedValue(Response.json({ token: 'fresh', expires_at: expiry() }));
    expect((await provider.getValidToken()).token).toBe('fresh');
    resolveFetch(Response.json({ token: 'late', expires_at: expiry() }));
    await vi.advanceTimersByTimeAsync(0);
    expect((await provider.getValidToken()).token).toBe('fresh');
    provider.close();
  });

  it('does not call auth or fetch for an already cancelled caller', async () => {
    const auth = vi.fn();
    const fetchSpy = vi.fn();
    vi.stubGlobal('fetch', fetchSpy);
    await expect(
      new RegistryClient('https://registry.internal', auth).mintSessionJwt(
        'ws_one',
        'ses_one',
        [],
        [],
        AbortSignal.abort('secret'),
      ),
    ).rejects.toMatchObject({ name: 'AbortError' });
    expect(auth).not.toHaveBeenCalled();
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});

describe('RegistryClient execution ownership', () => {
  it('reads ownership without preparing and fails closed on an invalid response', async () => {
    const fetchSpy = vi
      .fn()
      .mockResolvedValueOnce(Response.json({ owner: 'registry' }))
      .mockResolvedValueOnce(Response.json({ owner: 'unknown' }));
    vi.stubGlobal('fetch', fetchSpy);
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);
    const input = { workspaceId: 'ws_one', sessionId: 'ses_one' };
    await expect(client.getExecutionOwner(input)).resolves.toBe('registry');
    expect(fetchSpy.mock.calls[0]![0]).toBe(
      'https://registry.internal/internal/v1/workspaces/ws_one/sessions/ses_one/execution-owner',
    );
    expect(fetchSpy.mock.calls[0]![1].method).toBe('GET');
    await expect(client.getExecutionOwner(input)).rejects.toThrow(/invalid owner/);
  });
});

describe('RegistryClient prepared execution', () => {
  it('rejects path segments before URL normalization can change the scoped route', async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal('fetch', fetchSpy);
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);

    await expect(
      client.prepareExecution({ workspaceId: '..', sessionId: 'ses_one' }),
    ).rejects.toThrow(/invalid workspace id/);
    await expect(
      client.prepareExecution({ workspaceId: 'ws_one', sessionId: '..' }),
    ).rejects.toThrow(/invalid session id/);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it('posts to the workspace/session-scoped internal endpoint without a public API key', async () => {
    const calls: FetchCall[] = [];
    vi.stubGlobal('fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ input, init });
      return new Response(JSON.stringify(preparedExecution()), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    });
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);

    const prepared = await client.prepareExecution({
      workspaceId: 'ws_one',
      sessionId: 'ses_one',
    });

    expect(calls).toHaveLength(1);
    expect(String(calls[0]!.input)).toBe(
      'https://registry.internal/internal/v1/workspaces/ws_one/sessions/ses_one/executions:prepare',
    );
    expect(calls[0]!.init).toMatchObject({
      method: 'POST',
      body: '{}',
    });
    const headers = new Headers(calls[0]!.init?.headers);
    expect(headers.get('content-type')).toBe('application/json');
    expect(headers.get('authorization')).toBe('Bearer test-internal-service-token');
    expect(headers.has('x-api-key')).toBe(false);
    expect(prepared).toMatchObject({
      schema_version: 2,
      workspace_id: 'ws_one',
      session: {
        id: 'ses_one',
        runtime_revision: 4,
        usage_writer: 'harness',
        resources: [],
      },
      primary_agent: {
        model: {
          provider: 'anthropic',
          id: 'claude-opus-5',
          speed: 'fast',
          effort: 'high',
        },
      },
    });
    expect(prepared?.primary_agent.skills).toEqual([
      expect.objectContaining({
        id: 'sklv_primary',
        skill_id: 'skl_primary',
        version_identifier: '2',
      }),
    ]);
  });

  it('adds the current internal bearer token to every request', async () => {
    const calls: FetchCall[] = [];
    vi.stubGlobal('fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ input, init });
      return new Response(JSON.stringify(preparedExecution()), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    });
    let token = 'first-token';
    const client = new RegistryClient('https://registry.internal', async () => token);

    await client.prepareExecution({ workspaceId: 'ws_one', sessionId: 'ses_one' });
    token = 'rotated-token';
    await client.prepareExecution({ workspaceId: 'ws_one', sessionId: 'ses_one' });

    expect(calls).toHaveLength(2);
    expect(new Headers(calls[0]!.init?.headers).get('authorization')).toBe('Bearer first-token');
    expect(new Headers(calls[1]!.init?.headers).get('authorization')).toBe('Bearer rotated-token');
    expect(new Headers(calls[0]!.init?.headers).has('x-api-key')).toBe(false);
  });

  it('keeps pinned skills scoped to each agent instead of flattening them', async () => {
    const wire = preparedExecution();
    vi.stubGlobal(
      'fetch',
      vi.fn(
        async () =>
          new Response(JSON.stringify(wire), {
            status: 200,
            headers: { 'content-type': 'application/json' },
          }),
      ),
    );
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);

    const prepared = await client.prepareExecution({ workspaceId: 'ws_one', sessionId: 'ses_one' });

    expect(prepared?.schema_version).toBe(2);
    expect(prepared?.primary_agent.skills.map((skill) => skill.id)).toEqual(['sklv_primary']);
  });

  it('forwards prepared guardrails and restored state to the harness runtime', async () => {
    const guardrails = [
      {
        id: 'grd_one',
        name: 'Block shell',
        tier: 'workspace',
        phases: ['tool_call'],
        rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
        stateful: false,
      },
    ];
    vi.stubGlobal(
      'fetch',
      vi.fn(
        async () =>
          new Response(
            JSON.stringify(
              preparedExecution({ guardrails, guardrail_state: { 'g:grd_calls:tool_calls': 4 } }),
            ),
            { status: 200, headers: { 'content-type': 'application/json' } },
          ),
      ),
    );
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);

    await expect(
      client.prepareExecution({ workspaceId: 'ws_one', sessionId: 'ses_one' }),
    ).resolves.toMatchObject({
      guardrails,
      guardrail_state: { 'g:grd_calls:tool_calls': 4 },
    });
  });

  it('accepts an explicit Gateway writer and rejects unknown usage authority', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify(
            preparedExecution({
              session: {
                ...(preparedExecution()['session'] as Record<string, unknown>),
                usage_writer: 'ai-gateway',
              },
            }),
          ),
          { status: 200, headers: { 'content-type': 'application/json' } },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify(
            preparedExecution({
              session: {
                ...(preparedExecution()['session'] as Record<string, unknown>),
                usage_writer: 'unknown-writer',
              },
            }),
          ),
          { status: 200, headers: { 'content-type': 'application/json' } },
        ),
      );
    vi.stubGlobal('fetch', fetchMock);
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);

    await expect(
      client.prepareExecution({ workspaceId: 'ws_one', sessionId: 'ses_one' }),
    ).resolves.toMatchObject({ session: { usage_writer: 'ai-gateway' } });
    await expect(
      client.prepareExecution({ workspaceId: 'ws_one', sessionId: 'ses_one' }),
    ).rejects.toThrow(/unsupported usage writer/);
  });

  it('returns null for a missing scoped session and rejects unsupported schemas', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response('', { status: 404 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify(preparedExecution({ schema_version: 1 })), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
      );
    vi.stubGlobal('fetch', fetchMock);
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);

    await expect(
      client.prepareExecution({ workspaceId: 'ws_one', sessionId: 'ses_missing' }),
    ).resolves.toBeNull();
    await expect(
      client.prepareExecution({ workspaceId: 'ws_one', sessionId: 'ses_one' }),
    ).rejects.toThrow(/unsupported schema version/);
  });

  it('preserves structured invalid runtime binding failures from prepare', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              error: 'invalid_runtime_binding',
              resource_type: 'model_config',
              resource_id: 'mixed model.speed values are unsupported',
            }),
            { status: 409, headers: { 'content-type': 'application/json' } },
          ),
      ),
    );
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);

    await expect(
      client.prepareExecution({ workspaceId: 'ws_one', sessionId: 'ses_one' }),
    ).rejects.toEqual(
      expect.objectContaining<Partial<RegistryInvalidRuntimeBindingError>>({
        name: 'RegistryInvalidRuntimeBindingError',
        resourceType: 'model_config',
        resourceId: 'mixed model.speed values are unsupported',
      }),
    );
  });
});

describe('RegistryClient private harness receipts', () => {
  it('preserves permanent invalid stored-state failures and keeps transport errors distinct', async () => {
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValueOnce(
          new Response(
            JSON.stringify({
              error: 'invalid_runtime_binding',
              resource_type: 'harness_state',
              resource_id: 'ses_one',
            }),
            { status: 409 },
          ),
        )
        .mockResolvedValueOnce(new Response('unavailable', { status: 503 })),
    );
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);
    const input = {
      workspaceId: 'ws_one',
      sessionId: 'ses_one',
      request: { action: { type: 'inspect' as const } },
    };
    await expect(client.harnessTurn(input)).rejects.toBeInstanceOf(
      RegistryInvalidRuntimeBindingError,
    );
    await expect(client.harnessTurn(input)).rejects.toThrow('harnessTurn ses_one failed: 503');
  });
});

describe('RegistryClient private harness checkpoints', () => {
  const state = {
    version: 1,
    threadId: 'thread',
    files: {
      'sessions/2026/09/20/rollout-thread.jsonl': 'YQ==',
    },
  };
  const revision = 'a'.repeat(64);
  const input = {
    workspaceId: 'ws_one',
    sessionId: 'ses_one',
    runtimeRevision: 7,
    expectedCheckpointRevision: null,
    state,
  };

  it('sends the prepared revision and previous checkpoint with internal workload auth', async () => {
    const calls: FetchCall[] = [];
    vi.stubGlobal('fetch', async (request: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ input: request, init });
      return new Response(JSON.stringify({ checkpoint_revision: revision }), { status: 200 });
    });
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);
    await expect(client.saveHarnessState(input)).resolves.toBe(revision);
    expect(String(calls[0]!.input)).toBe(
      'https://registry.internal/internal/v1/workspaces/ws_one/sessions/ses_one/harness-state',
    );
    expect(JSON.parse(calls[0]!.init!.body as string)).toEqual({
      runtime_revision: 7,
      expected_checkpoint_revision: null,
      state,
    });
    expect(new Headers(calls[0]!.init!.headers).get('authorization')).toBe(
      'Bearer test-internal-service-token',
    );
  });

  it('preserves private state from preparation for restart recovery', async () => {
    const wire = preparedExecution();
    Object.assign(wire.session as Record<string, unknown>, {
      harness_state: state,
      harness_state_revision: revision,
    });
    vi.stubGlobal('fetch', async () => new Response(JSON.stringify(wire), { status: 200 }));
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);
    expect((await client.prepareExecution(input))?.session).toMatchObject({
      harness_state: state,
      harness_state_revision: revision,
    });
  });

  it('fails closed on stale ownership without including private response data in errors', async () => {
    vi.stubGlobal('fetch', async () => new Response('private native history', { status: 409 }));
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);
    await expect(client.saveHarnessState(input)).rejects.toThrow(
      'saveHarnessState ses_one failed: 409',
    );
  });

  it('rejects malformed acknowledgments instead of advancing its checkpoint revision', async () => {
    vi.stubGlobal(
      'fetch',
      async () => new Response(JSON.stringify({ checkpoint_revision: null }), { status: 200 }),
    );
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);
    await expect(client.saveHarnessState(input)).rejects.toThrow('invalid checkpoint revision');
  });
});

describe('RegistryClient scoped memory operations', () => {
  it('keeps workspace, session, and store in every memory request and sends no API key', async () => {
    const calls: FetchCall[] = [];
    vi.stubGlobal('fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ input, init });
      const url = String(input);
      if (url.endsWith('/memories')) {
        return new Response(JSON.stringify({ data: [] }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      if (url.endsWith('/memories/mem_one/content')) {
        return new Response('content', { status: 200 });
      }
      if (url.includes('/memory-versions?')) {
        return new Response(JSON.stringify({ data: [], next_page: null }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      return new Response(
        JSON.stringify({
          memory: {
            id: 'mem_one',
            store_id: 'mems_one',
            path: 'notes.txt',
            current_sha256: 'a'.repeat(64),
            size_bytes: 7,
            updated_at: '2026-07-17T00:00:00.000Z',
            updated_by_session_id: 'ses_one',
            updated_by_event_id: null,
          },
          version: {
            id: 'memver_one',
            store_id: 'mems_one',
            memory_id: 'mem_one',
            path: 'notes.txt',
            sha256: 'a'.repeat(64),
            size_bytes: 7,
            written_by_session_id: 'ses_one',
            written_by_event_id: null,
            written_at: '2026-07-17T00:00:00.000Z',
            redacted_at: null,
          },
          conflict: false,
        }),
        { status: 200, headers: { 'content-type': 'application/json' } },
      );
    });
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);
    const scope = { workspaceId: 'ws_one', sessionId: 'ses_one', storeId: 'mems_one' };

    await client.listSessionMemories(scope);
    await client.getSessionMemoryContent({ ...scope, memoryId: 'mem_one' });
    await client.listSessionMemoryVersions({ ...scope, memoryId: 'mem_one' });
    await client.recordSessionMemoryVersion({
      ...scope,
      path: 'notes.txt',
      contentBase64: Buffer.from('content').toString('base64'),
      contentSha256: 'a'.repeat(64),
      previousSha256: null,
    });

    const base =
      'https://registry.internal/internal/v1/workspaces/ws_one/sessions/ses_one/memory-stores/mems_one';
    expect(calls.map((call) => String(call.input))).toEqual([
      `${base}/memories`,
      `${base}/memories/mem_one/content`,
      `${base}/memory-versions?memory_id=mem_one`,
      `${base}/memory-versions`,
    ]);
    for (const call of calls) {
      expect(new Headers(call.init?.headers).has('x-api-key')).toBe(false);
    }
  });

  it('leaves LLM JWT authorization claims for Registry to derive', async () => {
    const calls: FetchCall[] = [];
    vi.stubGlobal('fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ input, init });
      return new Response(JSON.stringify({ token: 'jwt_test', expires_at: 123 }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    });
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);

    await client.mintLlmGatewayJwt('ws_one', 'ses_one');

    expect(calls).toHaveLength(1);
    expect(JSON.parse(String(calls[0]!.init?.body))).toEqual({
      audience: 'ai-gateway',
      mcp_server_names: [],
      vault_ids: [],
    });
  });
});

describe('RegistryClient scoped session operations', () => {
  it('preserves OpenAI provider identity when reporting Codex usage for pricing', async () => {
    const calls: FetchCall[] = [];
    vi.stubGlobal('fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ input, init });
      return new Response(JSON.stringify({ id: 'ses_one', workspace_id: 'ws_one' }), {
        status: 200,
      });
    });
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);
    await client.recordSessionUsageInternal({
      workspaceId: 'ws_one',
      sessionId: 'ses_one',
      provider: 'openai',
      model: 'gpt-5.4-mini',
      usage: { input_tokens: 11, cache_read_input_tokens: 4, output_tokens: 2 },
    });
    expect(JSON.parse(String(calls[0]!.init?.body))).toEqual({
      provider: 'openai',
      model: 'gpt-5.4-mini',
      usage: { input_tokens: 11, cache_read_input_tokens: 4, output_tokens: 2 },
    });
  });

  it('uses the workspace/session path for state, usage, JWT, git, and file operations', async () => {
    const calls: FetchCall[] = [];
    vi.stubGlobal('fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ input, init });
      const url = String(input);
      if (url.endsWith('/git-credentials/gitcred_one/resolve')) {
        return new Response(JSON.stringify({ secret_value: 'pat-secret' }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      }
      if (url.endsWith('/files')) {
        return new Response(
          JSON.stringify({
            id: 'file_one',
            filename: 'report.txt',
            mime_type: 'text/plain',
            size_bytes: 6,
            sha256: 'a'.repeat(64),
            metadata: {},
            purpose: 'agent_output',
            scope_id: 'ses_one',
            downloadable: true,
            archived_at: null,
            created_at: '2026-07-17T00:00:00.000Z',
            updated_at: '2026-07-17T00:00:00.000Z',
          }),
          { status: 201, headers: { 'content-type': 'application/json' } },
        );
      }
      if (url.endsWith('/mint-jwt')) {
        return new Response(
          JSON.stringify({ token: 'jwt', expires_at: Math.floor(Date.now() / 1000) + 3600 }),
          {
            status: 200,
            headers: { 'content-type': 'application/json' },
          },
        );
      }
      return new Response(
        JSON.stringify({
          id: 'ses_one',
          workspace_id: 'ws_one',
          agent_id: 'agt_primary',
          agent_version: 3,
        }),
        { status: 200, headers: { 'content-type': 'application/json' } },
      );
    });
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);

    await client.updateSessionStateInternal({
      workspaceId: 'ws_one',
      sessionId: 'ses_one',
      status: 'running',
      sandboxHandleId: 'sbx_one',
    });
    await client.recordSessionUsageInternal({
      workspaceId: 'ws_one',
      sessionId: 'ses_one',
      usage: { input_tokens: 1 },
      model: 'claude-opus-5',
      subagentId: 'agt_child',
      turnEventId: 'evt_turn_one',
      usageEventId: 'evt_usage_one',
    });
    await client.refreshGuardrailSubjectWindowInternal({
      workspaceId: 'ws_one',
      sessionId: 'ses_one',
      turnEventId: 'evt_turn_one',
    });
    await client.mintSessionJwt(
      'ws_one',
      'ses_one',
      ['github'],
      ['vlt_one'],
      new AbortController().signal,
    );
    await client.mintGitCredsJwt({
      workspaceId: 'ws_one',
      sessionId: 'ses_one',
      repoUrls: ['https://github.com/orca/repo.git'],
    });
    await client.mintLlmGatewayJwt('ws_one', 'ses_one');
    await client.resolveGitCredentialSecret({
      workspaceId: 'ws_one',
      sessionId: 'ses_one',
      gitCredentialId: 'gitcred_one',
    });
    await client.createFile({
      workspaceId: 'ws_one',
      sessionId: 'ses_one',
      filename: 'report.txt',
      mimeType: 'text/plain',
      content: Buffer.from('report'),
    });

    const base = 'https://registry.internal/internal/v1/workspaces/ws_one/sessions/ses_one';
    expect(calls.map((call) => String(call.input))).toEqual([
      `${base}/state`,
      `${base}/usage`,
      `${base}/guardrail-subject-window`,
      `${base}/mint-jwt`,
      `${base}/mint-jwt`,
      `${base}/mint-jwt`,
      `${base}/git-credentials/gitcred_one/resolve`,
      `${base}/files`,
    ]);
    expect(JSON.parse(String(calls[3]!.init?.body))).toEqual({
      mcp_server_names: ['github'],
      vault_ids: ['vlt_one'],
    });
    expect(JSON.parse(String(calls[1]!.init?.body))).toEqual({
      usage: { input_tokens: 1 },
      model: 'claude-opus-5',
      subagent_id: 'agt_child',
      turn_event_id: 'evt_turn_one',
      usage_event_id: 'evt_usage_one',
    });
    expect(JSON.parse(String(calls[2]!.init?.body))).toEqual({
      turn_event_id: 'evt_turn_one',
    });
    expect(JSON.parse(String(calls[4]!.init?.body))).toEqual({
      audience: 'git-creds',
      repo_urls: ['https://github.com/orca/repo.git'],
    });
    expect(JSON.parse(String(calls[5]!.init?.body))).toEqual({
      audience: 'ai-gateway',
      mcp_server_names: [],
      vault_ids: [],
    });
    const fileForm = calls[7]!.init?.body as FormData;
    expect([...fileForm.keys()]).toEqual(['file']);
    for (const call of calls) {
      expect(new Headers(call.init?.headers).has('x-api-key')).toBe(false);
    }
  });

  it('writes persistent guardrail state through the scoped internal endpoint', async () => {
    const calls: FetchCall[] = [];
    vi.stubGlobal('fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ input, init });
      return new Response(JSON.stringify({ applied: 1 }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    });
    const client = new RegistryClient('https://registry.internal', TEST_TOKEN_PROVIDER);

    await expect(
      client.applyGuardrailStateInternal({
        workspaceId: 'ws_one',
        sessionId: 'ses_one',
        updates: [
          { scope: 'session', key: 'g:grd_calls:tool_calls', action: 'increment', value: 1 },
        ],
      }),
    ).resolves.toBe(1);

    expect(String(calls[0]!.input)).toBe(
      'https://registry.internal/internal/v1/workspaces/ws_one/sessions/ses_one/guardrail-state',
    );
    expect(JSON.parse(String(calls[0]!.init?.body))).toEqual({
      updates: [{ scope: 'session', key: 'g:grd_calls:tool_calls', action: 'increment', value: 1 }],
    });
  });
});
