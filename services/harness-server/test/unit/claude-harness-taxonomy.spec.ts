// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, it, expect, vi } from 'vitest';
import type { ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';

const { queryMock } = vi.hoisted(() => ({ queryMock: vi.fn() }));

vi.mock('@anthropic-ai/claude-agent-sdk', () => ({
  query: queryMock,
  createSdkMcpServer: vi.fn((cfg: unknown) => ({ type: 'sdk', instance: cfg })),
  tool: vi.fn((name: string, description: string, inputSchema: unknown, handler: unknown) => ({
    name,
    description,
    inputSchema,
    handler,
  })),
}));

import {
  ClaudeAgentSdkHarness,
  type ClaudeHarnessOptions,
} from '../../src/harness/claude/index.js';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import type { AgentEvent } from '../../src/harness/agent-harness.js';
import type { OutcomeEvaluator } from '../../src/harness/outcome/evaluator.js';
import {
  projectCanonicalTurns,
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  reduceCanonicalEventBatch,
} from '../../../observability-exporter/src/projector.js';
import { encodeLangfuseOtlpJson } from '../../../observability-exporter/src/otlp-json.js';
import { event as transcriptEvent } from '../../../observability-exporter/test/support/events.js';
import {
  expectCanonicalRootTurn,
  expectTerminalError,
  expectTurnModelSummary,
  expectToolUseResultCorrelation,
  expectUniqueAgentEventEnvelopes,
} from '../support/model-summary.js';

function emptyAsyncIterable<T>(): AsyncIterable<T> {
  return {
    async *[Symbol.asyncIterator](): AsyncGenerator<T> {},
  };
}

function stubStore(): TranscriptStore {
  return {
    append: async () => [],
    read: (_w: string, _s: string, _o: ReadOptions) => emptyAsyncIterable(),
    tail: (_w: string, _s: string, _o: TailOptions) => emptyAsyncIterable(),
    archive: async () => {},
    close: async () => {},
  } satisfies TranscriptStore;
}

function arrayAsyncIterable<T>(items: T[]): AsyncIterable<T> {
  return {
    async *[Symbol.asyncIterator](): AsyncGenerator<T> {
      for (const item of items) yield item;
    },
  };
}

function buildHarness(overrides: Partial<ClaudeHarnessOptions> = {}): ClaudeAgentSdkHarness {
  return new ClaudeAgentSdkHarness({
    apiKey: 'unused',
    modelDefault: 'fake-model',
    adapter: new ClaudeAgentSdkAdapter(stubStore(), 'ws_test'),
    workspaceId: 'ws_test',
    sessionId: 'ses_test',
    ...overrides,
  });
}

/** Drive one full turn and return every emitted event (framing included). */
async function runTurn(
  harness: ClaudeAgentSdkHarness,
  frames: unknown[],
  opts: { defineOutcome?: Record<string, unknown> } = {},
): Promise<AgentEvent[]> {
  const events: AgentEvent[] = [];
  const collector = (async () => {
    for await (const e of harness.events()) events.push(e);
  })();
  await harness.start({
    workspaceId: 'ws_test',
    sessionId: 'ses_test',
    agentSnapshot: { model_provider: 'anthropic', model_id: 'fake-model' },
  });
  queryMock.mockReturnValueOnce(arrayAsyncIterable(frames));
  if (opts.defineOutcome) {
    await harness.submit({ kind: 'user.define_outcome', payload: opts.defineOutcome });
  } else {
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'do the thing' }] },
    });
  }
  await new Promise((r) => setTimeout(r, 0));
  await harness.stop('client.archived');
  await collector;
  expectUniqueAgentEventEnvelopes(events);
  return events;
}

function kinds(events: AgentEvent[]): string[] {
  return events.map((e) => e.kind);
}

const assistantFrame = (content: unknown[], parentToolUseId: string | null = null) => ({
  type: 'assistant',
  message: { role: 'assistant', content, stop_reason: 'tool_use', usage: {} },
  parent_tool_use_id: parentToolUseId,
  uuid: 'a1',
  session_id: 's1',
});

const partialAssistantFrame = (event: unknown) => ({
  type: 'stream_event',
  event,
  parent_tool_use_id: null,
  // Stream frame UUIDs are transport ids and need not match the later
  // buffered assistant UUID; session_id + parent_tool_use_id is the scope.
  uuid: 'partial-frame',
  session_id: 's1',
});

const successResult = {
  type: 'result',
  subtype: 'success',
  stop_reason: 'end_turn',
  usage: {
    input_tokens: 10,
    output_tokens: 5,
    cache_creation_input_tokens: 30,
    cache_creation: {
      ephemeral_1h_input_tokens: 13,
      ephemeral_5m_input_tokens: 17,
    },
    cache_read_input_tokens: 3,
  },
  modelUsage: {},
  total_cost_usd: 0.02,
  is_error: false,
  num_turns: 1,
  duration_ms: 1,
  duration_api_ms: 1,
  result: 'done',
  permission_denials: [],
  uuid: 'r1',
  session_id: 's1',
};

beforeEach(() => {
  queryMock.mockReset();
});

describe('ClaudeAgentSdkHarness RECEIVED taxonomy', () => {
  it('decomposes an assistant frame into thinking + text-only message + tool_use, plus framing', async () => {
    const events = await runTurn(buildHarness(), [
      assistantFrame(
        [
          { type: 'thinking', thinking: 'let me think' },
          { type: 'text', text: 'here you go' },
          { type: 'tool_use', id: 'toolu_1', name: 'mcp__orca__bash', input: { cmd: 'ls' } },
        ],
        'toolu_parent',
      ),
      {
        type: 'user',
        message: {
          role: 'user',
          content: [
            { type: 'tool_result', tool_use_id: 'toolu_1', content: 'files', is_error: false },
          ],
        },
        parent_tool_use_id: null,
        uuid: 'u1',
        session_id: 's1',
      },
      successResult,
    ]);

    expect(kinds(events)).toEqual([
      'session.status_running',
      'span.model_request_start',
      'agent.thinking',
      'agent.message',
      'agent.tool_use',
      'agent.tool_result',
      'agent.usage',
      'span.model_request_end',
      'session.status_idle',
    ]);

    // agent.message content is text-only: thinking + tool_use are stripped out.
    const message = events.find((e) => e.kind === 'agent.message')!;
    expect(message.payload).toEqual({
      content: [{ type: 'text', text: 'here you go' }],
      parent_tool_use_id: 'toolu_parent',
    });
    expect(message.payload).not.toHaveProperty('message');
    expect(message.payload).not.toHaveProperty('uuid');
    expect(message.payload).not.toHaveProperty('session_id');
    const content = (message.payload as { content: Array<{ type: string }> }).content;
    expect(content.map((b) => b.type)).toEqual(['text']);

    // The public tool-use event id is the exact reference carried by its result.
    const { toolUse } = expectToolUseResultCorrelation(events);
    const publicToolUseId = (toolUse.payload as { id: string }).id;
    expect(publicToolUseId).toMatch(/^evt_/);
    expect(toolUse.payload).not.toHaveProperty('tool_use_id');

    // agent.usage preserves the SDK's TTL split for cumulative session accounting.
    const usagePayload = events.find((e) => e.kind === 'agent.usage')!.payload as {
      usage: {
        cache_creation: {
          ephemeral_1h_input_tokens: number;
          ephemeral_5m_input_tokens: number;
        };
      };
    };
    expect(usagePayload.usage.cache_creation).toEqual({
      ephemeral_1h_input_tokens: 13,
      ephemeral_5m_input_tokens: 17,
    });

    // span.model_request_end carries the public token usage; agent.usage is internal.
    const {
      start: spanStart,
      end: spanEnd,
      terminalIdle,
    } = expectCanonicalRootTurn(events, {
      provider: 'anthropic',
      model: 'fake-model',
    });
    const spanPayload = spanEnd.payload as {
      model_usage: { input_tokens: number; cache_creation_input_tokens: number };
      is_error: boolean;
      total_cost_usd: number;
      model_request_start_id: string;
      model_observation_kind: string;
      provider: string;
      model: string;
    };
    expect(spanPayload.model_usage.input_tokens).toBe(10);
    expect(spanPayload.model_usage.cache_creation_input_tokens).toBe(30);
    expect(spanPayload.is_error).toBe(false);
    // Fractional dollar cost is preserved (regression for the Math.floor bug).
    expect(spanPayload.total_cost_usd).toBe(0.02);
    expect(spanPayload.model_observation_kind).toBe('turn_model_summary');
    expect(spanPayload.provider).toBe('anthropic');
    expect(spanPayload.model).toBe('fake-model');
    expect(spanEnd.id).toMatch(/^evt_/);
    // The end span correlates to the start envelope. The start payload carries
    // no duplicate event identity.
    expect(spanStart.id).toMatch(/^evt_/);
    expect(spanStart.payload).toEqual({
      model_observation_kind: 'turn_model_summary',
      provider: 'anthropic',
      model: 'fake-model',
    });
    expect(spanPayload.model_request_start_id).toBe(spanStart.id);

    // stop_reason on the terminal idle is end_turn.
    const idle = terminalIdle;
    expect((idle.payload as { stop_reason: { type: string } }).stop_reason.type).toBe('end_turn');
  });

  it('records each assistant model call with its actual model and does not double-count result usage', async () => {
    const onUsage = vi.fn(async () => ({ total_tokens: 15, session_cost_usd: 0.02 }));
    const assistant = {
      ...assistantFrame([{ type: 'text', text: 'priced response' }]),
      message: {
        role: 'assistant',
        model: 'claude-sonnet-5',
        content: [{ type: 'text', text: 'priced response' }],
        stop_reason: 'end_turn',
        usage: {
          input_tokens: 10,
          output_tokens: 5,
          cache_read_input_tokens: 0,
          cache_creation_input_tokens: 0,
        },
      },
    };

    const events = await runTurn(buildHarness({ onUsage }), [assistant, successResult]);

    expect(onUsage).toHaveBeenCalledTimes(1);
    expect(onUsage).toHaveBeenCalledWith(
      {
        cache_creation: {
          ephemeral_1h_input_tokens: 0,
          ephemeral_5m_input_tokens: 0,
        },
        cache_read_input_tokens: 0,
        input_tokens: 10,
        output_tokens: 5,
      },
      'claude-sonnet-5',
      undefined,
      undefined,
      expect.stringMatching(/^evt_/),
    );
    const usageEvents = events.filter((event) => event.kind === 'agent.usage');
    expect(usageEvents).toHaveLength(1);
    expect(usageEvents[0]?.payload).toMatchObject({
      model: 'claude-sonnet-5',
      guardrail_usage_recorded: true,
    });
  });

  it('emits reconcilable message deltas and start-only thinking previews from SDK partial frames', async () => {
    const events = await runTurn(buildHarness(), [
      partialAssistantFrame({
        type: 'message_start',
        message: { id: 'msg_1', type: 'message', role: 'assistant', content: [] },
      }),
      partialAssistantFrame({
        type: 'content_block_start',
        index: 0,
        content_block: { type: 'thinking', thinking: '', signature: '' },
      }),
      partialAssistantFrame({
        type: 'content_block_delta',
        index: 0,
        delta: { type: 'thinking_delta', thinking: 'private reasoning' },
      }),
      partialAssistantFrame({
        type: 'content_block_start',
        index: 1,
        content_block: { type: 'text', text: '', citations: null },
      }),
      partialAssistantFrame({
        type: 'content_block_delta',
        index: 1,
        delta: { type: 'text_delta', text: 'hel' },
      }),
      partialAssistantFrame({
        type: 'content_block_delta',
        index: 1,
        delta: { type: 'text_delta', text: 'lo' },
      }),
      assistantFrame([
        { type: 'thinking', thinking: 'private reasoning', signature: 'sig' },
        { type: 'text', text: 'hello', citations: null },
      ]),
      successResult,
    ]);

    expect(queryMock).toHaveBeenCalledWith(
      expect.objectContaining({
        options: expect.objectContaining({ includePartialMessages: true }),
      }),
    );
    expect(kinds(events)).toEqual([
      'session.status_running',
      'span.model_request_start',
      'event_start',
      'event_start',
      'event_delta',
      'event_delta',
      'agent.thinking',
      'agent.message',
      'agent.usage',
      'span.model_request_end',
      'session.status_idle',
    ]);

    const starts = events.filter((event) => event.kind === 'event_start');
    const thinkingStart = starts.find(
      (event) => (event.payload as { event: { type: string } }).event.type === 'agent.thinking',
    )!;
    const messageStart = starts.find(
      (event) => (event.payload as { event: { type: string } }).event.type === 'agent.message',
    )!;
    const thinkingPreviewId = (thinkingStart.payload as { event: { id: string } }).event.id;
    const messagePreviewId = (messageStart.payload as { event: { id: string } }).event.id;

    const deltas = events.filter((event) => event.kind === 'event_delta');
    expect(deltas.map((event) => event.payload)).toEqual([
      {
        event_id: messagePreviewId,
        delta: {
          type: 'content_delta',
          content: { type: 'text', text: 'hel' },
          index: 0,
        },
      },
      {
        event_id: messagePreviewId,
        delta: {
          type: 'content_delta',
          content: { type: 'text', text: 'lo' },
          index: 0,
        },
      },
    ]);

    const thinking = events.find((event) => event.kind === 'agent.thinking')!;
    const message = events.find((event) => event.kind === 'agent.message')!;
    expect((thinking.payload as { id: string }).id).toBe(thinkingPreviewId);
    expect((message.payload as { id: string }).id).toBe(messagePreviewId);
    expect(thinking.id).toBe(thinkingPreviewId);
    expect(message.id).toBe(messagePreviewId);
  });

  it('surfaces an api_retry frame as session.error{will_retry} + session.status_rescheduled', async () => {
    const events = await runTurn(buildHarness(), [
      {
        type: 'system',
        subtype: 'api_retry',
        attempt: 1,
        max_retries: 3,
        retry_delay_ms: 500,
        error_status: 429,
        error: 'rate_limit',
        uuid: 'ar1',
        session_id: 's1',
      },
      assistantFrame([{ type: 'text', text: 'ok' }]),
      successResult,
    ]);

    const retryError = events.find(
      (e) =>
        e.kind === 'session.error' &&
        (e.payload as { retry_status: { will_retry: boolean } }).retry_status.will_retry,
    );
    expect(retryError).toBeDefined();
    expect(
      (retryError!.payload as { retry_status: { next_attempt: number } }).retry_status.next_attempt,
    ).toBe(2);
    expect(kinds(events)).toContain('session.status_rescheduled');
    // Turn still completes normally afterwards.
    const idle = events.find((e) => e.kind === 'session.status_idle')!;
    expect((idle.payload as { stop_reason: { type: string } }).stop_reason.type).toBe('end_turn');
  });

  it('maps an error result to session.error{!will_retry} + session.status_idle{retries_exhausted}', async () => {
    const events = await runTurn(buildHarness(), [
      assistantFrame([{ type: 'text', text: 'trying' }]),
      {
        type: 'result',
        subtype: 'error_max_turns',
        errors: ['too many turns'],
        usage: {
          input_tokens: 1,
          output_tokens: 1,
          cache_creation_input_tokens: 0,
          cache_read_input_tokens: 0,
        },
        modelUsage: {},
        total_cost_usd: 0,
        is_error: true,
        num_turns: 5,
        duration_ms: 1,
        duration_api_ms: 1,
        permission_denials: [],
        uuid: 'er1',
        session_id: 's1',
      },
    ]);

    const err = events.find((e) => e.kind === 'session.error')!;
    expectTerminalError(events);
    expect((err.payload as { error: { type: string } }).error.type).toBe('error_max_turns');
    expect((err.payload as { retry_status: { will_retry: boolean } }).retry_status.will_retry).toBe(
      false,
    );
    const { end: modelEnd } = expectTurnModelSummary(events, {
      provider: 'anthropic',
      model: 'fake-model',
    });
    expect((modelEnd.payload as { is_error: boolean }).is_error).toBe(true);
    const idle = events.find((e) => e.kind === 'session.status_idle')!;
    expect((idle.payload as { stop_reason: { type: string } }).stop_reason.type).toBe(
      'retries_exhausted',
    );
  });

  it('maps a compact_boundary system frame to agent.thread_context_compacted', async () => {
    const events = await runTurn(buildHarness(), [
      {
        type: 'system',
        subtype: 'compact_boundary',
        compact_metadata: { trigger: 'auto', pre_tokens: 1000 },
        uuid: 'cb1',
        session_id: 's1',
      },
      assistantFrame([{ type: 'text', text: 'continuing' }]),
      successResult,
    ]);
    const compacted = events.find((e) => e.kind === 'agent.thread_context_compacted')!;
    expect(compacted).toBeDefined();
    expect(
      (compacted.payload as { compact_metadata: { trigger: string } }).compact_metadata.trigger,
    ).toBe('auto');
  });

  it('classifies remote MCP tool_use/tool_result as agent.mcp_tool_use/result', async () => {
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {},
      remoteMcpToolsets: [{ serverName: 'github', permissionPolicy: 'always_allow' }],
    });
    const events: AgentEvent[] = [];
    const collector = (async () => {
      for await (const e of harness.events()) events.push(e);
    })();
    queryMock.mockReturnValueOnce(
      arrayAsyncIterable([
        assistantFrame([
          {
            type: 'tool_use',
            id: 'toolu_9',
            name: 'mcp__github__create_issue',
            input: { title: 'x' },
          },
        ]),
        {
          type: 'user',
          message: {
            role: 'user',
            content: [
              { type: 'tool_result', tool_use_id: 'toolu_9', content: 'created', is_error: false },
            ],
          },
          uuid: 'u9',
          session_id: 's1',
        },
        successResult,
      ]),
    );
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'file a bug' }] },
    });
    await new Promise((r) => setTimeout(r, 0));
    await harness.stop('client.archived');
    await collector;

    expect(kinds(events)).toContain('agent.mcp_tool_use');
    expect(kinds(events)).toContain('agent.mcp_tool_result');
    const mcpResult = events.find((e) => e.kind === 'agent.mcp_tool_result')!;
    expect((mcpResult.payload as { mcp_server_name: string }).mcp_server_name).toBe('github');
  });

  it.each([
    { result: 'satisfied', achieved: true, maxIterations: 2, throws: false },
    { result: 'needs_revision', achieved: false, maxIterations: 2, throws: false },
    { result: 'max_iterations_reached', achieved: false, maxIterations: 1, throws: false },
    { result: 'failed', achieved: false, maxIterations: 2, throws: true },
  ])('emits the canonical outcome evaluator trio for $result', async (verdict) => {
    const sensitiveDescription = 'Patient Alice HIV positive';
    const sensitiveSlug = 'outcome_patient_alice_hiv_positive';
    const evaluateOutcome: OutcomeEvaluator = vi.fn(async () => {
      if (verdict.throws) throw new Error('judge failed');
      return { achieved: verdict.achieved, reasoning: 'the agent greeted the user' };
    });
    const events = await runTurn(
      buildHarness({ evaluateOutcome }),
      [assistantFrame([{ type: 'text', text: 'hello there' }]), successResult],
      {
        defineOutcome: {
          type: 'user.define_outcome',
          description: sensitiveDescription,
          rubric: 'satisfied when the agent greets the user',
          max_iterations: verdict.maxIterations,
        },
      },
    );

    const eventKinds = kinds(events);
    expect(eventKinds).toContain('span.outcome_evaluation_start');
    expect(eventKinds).toContain('span.outcome_evaluation_ongoing');
    expect(eventKinds.indexOf('span.outcome_evaluation_end')).toBeLessThan(
      eventKinds.lastIndexOf('session.status_idle'),
    );
    const start = events.find((e) => e.kind === 'span.outcome_evaluation_start')!;
    expect((start.payload as { outcome_id: string }).outcome_id).toBe(sensitiveSlug);
    const ongoing = events.find((e) => e.kind === 'span.outcome_evaluation_ongoing')!;
    const end = events.find((e) => e.kind === 'span.outcome_evaluation_end')!;
    expect(end).toBeDefined();
    const payload = end.payload as {
      result: string;
      explanation: string;
      iteration: number;
      outcome_evaluation_start_id: string;
    };
    expect(payload.result).toBe(verdict.result);
    expect(payload.explanation).toBe(
      verdict.throws ? 'outcome evaluation failed: judge failed' : 'the agent greeted the user',
    );
    expect(payload.iteration).toBe(0);
    expect((start.payload as { id: string }).id).toBe(start.id);
    expect(payload.outcome_evaluation_start_id).toBe(start.id);
    expect(ongoing.payload).toMatchObject({
      outcome_evaluation_start_id: start.id,
      outcome_id: (start.payload as { outcome_id: string }).outcome_id,
      iteration: 0,
    });
    expect(end.payload).toMatchObject({
      outcome_id: (start.payload as { outcome_id: string }).outcome_id,
      usage: {
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
      },
    });
    expect([start.subpath, ongoing.subpath, end.subpath]).toEqual(['', '', '']);
    // Feed real producer envelopes/payloads into the reducer under an explicit accepted anchor.
    const transcript = [
      transcriptEvent(1, 'user.message', {}, { id: 'evt_conformance_input', producedBy: 'client' }),
      transcriptEvent(2, 'session.user_event_processed', {
        user_event_id: 'evt_conformance_input',
      }),
      ...[start, ongoing, end].map((source, index) =>
        transcriptEvent(index + 3, source.kind, source.payload as Record<string, unknown>, {
          id: source.id,
          subpath: source.subpath,
        }),
      ),
      transcriptEvent(6, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ];
    const [trace] = projectCanonicalTurns(transcript);
    const open = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      transcript.slice(0, 3),
    );
    const resumed = reduceCanonicalEventBatch(
      parseCanonicalProjectionState(JSON.parse(JSON.stringify(open.state))),
      transcript.slice(3),
      new Set(open.acceptedSourceIds),
    );
    expect(resumed.completedTraces).toEqual([trace]);
    for (const snapshot of [open.state, resumed.state, trace, encodeLangfuseOtlpJson(trace!)]) {
      const serialized = JSON.stringify(snapshot);
      expect(serialized).not.toContain(sensitiveDescription);
      expect(serialized).not.toContain(sensitiveSlug);
      expect(serialized).not.toContain('patient_alice_hiv_positive');
    }
    expect(trace!.spans[0]!.metadata).not.toHaveProperty('orca.outcome.id');
    expect(trace!.spans).toHaveLength(1);
    expect(trace!.spans[0]).toMatchObject({
      sourceEventId: start.id,
      observationType: 'outcome_evaluation',
      metadata: { 'orca.source.end_event_id': end.id, 'orca.outcome.result': verdict.result },
    });
    const wire = JSON.stringify(encodeLangfuseOtlpJson(trace!));
    expect(wire).not.toContain(payload.explanation);
    expect(wire).not.toMatch(/usage_details|gen_ai|cost_details|explanation/);
    // The judge received the conversation transcript.
    expect(evaluateOutcome).toHaveBeenCalledWith(
      expect.objectContaining({
        criterion: expect.objectContaining({ description: sensitiveDescription }),
        transcript: expect.arrayContaining([
          expect.objectContaining({ role: 'user' }),
          expect.objectContaining({ role: 'agent', text: 'hello there' }),
        ]),
      }),
    );
  });
});
