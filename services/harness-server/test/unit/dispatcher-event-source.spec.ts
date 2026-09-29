// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { turnStoreFixture } from '../support/codex-turn-store.js';
import { Readable } from 'node:stream';
import { describe, expect, it, vi } from 'vitest';
import {
  PulsarSessionEventSource,
  RetryableSessionEventError,
  SessionEventBarrierError,
  type Event,
  type ReadOptions,
  type SessionEventSourceStatus,
  type TailOptions,
  type TranscriptStore,
} from '@orca/transcript-store';
import type { FileStore, OpenStream } from '@orca/file-store';
import type { MemoryRecord } from '@orca/memory-store';
import { InMemorySkillStore } from '@orca/skill-store';
import { S3Client } from '@aws-sdk/client-s3';
import { projectCanonicalTurns } from '@orca/observability-exporter';
import { kafkaTranscriptHash } from '../../../observability-exporter/src/kafka-state.js';
import { harnessAcceptedEventToStatusRunningSeconds } from '../../src/metrics.js';
import { Dispatcher, type SessionEventSource } from '../../src/runner/dispatcher.js';
import type {
  AgentEvent,
  AgentEventInput,
  AgentHarness,
  SessionStartInput,
  SubmitHooks,
  TerminationReason,
  UserEvent,
} from '../../src/harness/agent-harness.js';
import {
  DurableHarnessStateError,
  GuardrailPolicyDeniedError,
  UnappliedUserEventError,
  withCanonicalAgentEventEnvelope,
} from '../../src/harness/agent-harness.js';
import type {
  EnvironmentSpec,
  SandboxHandle,
  SandboxRuntime,
  ToolCall,
  ToolResult,
} from '../../src/sandbox/sandbox-runtime.js';
import { RegistryInvalidRuntimeBindingError } from '../../src/clients/registry.js';
import type {
  AgentRecord,
  PreparedExecutionV2,
  RegistryClient,
  SessionRecord,
  SkillDescriptor,
} from '../../src/clients/registry.js';

describe('Dispatcher with custom SessionEventSource', () => {
  it('wires Codex durable usage to OpenAI and the wrapper does not record it again', async () => {
    const turns = turnStoreFixture();
    const registry = {
      recordSessionUsageInternal: vi.fn(async () => ({
        guardrail_usage_state: { total_tokens: 17, session_cost_usd: 0.1 },
        guardrail_subject_window_state: { daily_cost_usd: 0.1, daily_cost_unpriced: false },
      })),
      refreshGuardrailSubjectWindowInternal: vi.fn(async () => ({})),
      applyGuardrailStateInternal: vi.fn(async () => {}),
      harnessTurn: vi.fn(
        async ({ request }: { request: import('@orca/harness-catalog').HarnessTurnRequest }) =>
          request.action.type === 'claim'
            ? turns.store.claim()
            : request.action.type === 'inspect'
              ? null
              : turns.store.update(request.action),
      ),
    };
    const dispatcher = new Dispatcher({
      store: new RecordingStore(),
      groupId: 'codex-usage',
      anthropicApiKey: '',
      openaiApiKey: 'fixture',
      modelDefault: 'unused',
      registry: registry as unknown as RegistryClient,
    });
    const harness = (
      dispatcher as unknown as {
        buildCodexHarness(
          ws: string,
          session: string,
          record: Partial<SessionRecord>,
        ): AgentHarness;
      }
    ).buildCodexHarness('ws_codex', 'ses_codex', { runtime_revision: 'runtime-1' });
    let emit!: (event: import('@orca/codex-harness').WorkerEvent) => void;
    // Keep the real dispatcher callbacks and adapter/wrapper boundary while
    // replacing only the provider worker with deterministic terminal events.
    (
      harness as unknown as {
        opts: import('../../src/harness/codex-sdk/index.js').CodexSdkHarnessOptions;
      }
    ).opts.createWorker = (send) => {
      emit = send;
      return {
        refreshOptions: async () => {},
        close: async () => {},
        handle: async (command) => {
          if (command.type !== 'submit') return;
          emit({
            type: 'event',
            event: {
              type: 'turn.completed',
              usage: {
                input_tokens: 15,
                cached_input_tokens: 4,
                output_tokens: 2,
                cache_write_input_tokens: 0,
                reasoning_output_tokens: 0,
              },
            },
          });
          emit({ type: 'checkpoint', checkpoint: { version: 1, threadId: 'thread', files: {} } });
        },
      };
    };
    const wrapperRecord = vi.fn(async () => ({}));
    const { SessionRunner } = await import('../../src/runner/session-runner.js');
    const runner = new SessionRunner({
      workspaceId: 'ws_codex',
      sessionId: 'ses_codex',
      harness,
      store: new RecordingStore(),
      recordUsage: wrapperRecord,
    });
    try {
      await runner.start({
        agentSnapshot: { model_provider: 'openai', model_id: 'gpt-5.4-mini' },
        guardrails: [
          {
            id: 'gr_budget',
            name: 'budget',
            tier: 'session',
            phases: ['request'],
            stateful: true,
            rule: { kind: 'builtin', builtin: 'token_budget', params: { max_total_tokens: 10 } },
          },
        ],
      });
      await runner.submit({
        id: 'evt_codex_usage_turn',
        kind: 'user.message',
        payload: { content: 'hello' },
      });
      await vi.waitFor(() => expect(turns.snapshot.receipt?.phase).toBe('settled'));
      expect(registry.recordSessionUsageInternal).toHaveBeenCalledOnce();
      expect(registry.recordSessionUsageInternal).toHaveBeenCalledWith(
        expect.objectContaining({
          provider: 'openai',
          model: 'gpt-5.4-mini',
          turnEventId: 'evt_codex_usage_turn',
          usageEventId: expect.stringMatching(/^evt_/),
        }),
      );
      expect(registry.refreshGuardrailSubjectWindowInternal).toHaveBeenCalledWith({
        workspaceId: 'ws_codex',
        sessionId: 'ses_codex',
        turnEventId: 'evt_codex_usage_turn',
      });
      expect(registry.applyGuardrailStateInternal).toHaveBeenCalledTimes(1);
    } finally {
      await runner.stop('client.archived');
    }
    expect(wrapperRecord).not.toHaveBeenCalled();
  });

  it.each(
    ['user.message', 'user.interrupt', 'user.tool_confirmation', 'user.custom_tool_result'].flatMap(
      (kind) => ['registry', null].map((owner) => ({ kind, owner })),
    ),
  )(
    'never prepares or completes $kind when owner is $owner',
    async ({ kind, owner: resolvedOwner }) => {
      const source = new FakeEventSource();
      const store = new RecordingStore();
      const registry = new RecordingRegistry();
      const owner = vi
        .spyOn(registry, 'getExecutionOwner')
        .mockResolvedValue(resolvedOwner as 'registry' | null);
      const factory = vi.fn(() => new EchoHarness());
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'owner-skip',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        registry: registry as unknown as RegistryClient,
        harnessFactory: factory,
      });
      await dispatcher.start();
      try {
        for (let i = 0; i < 2; i++) {
          const event = userMessage(`evt_owner_${i}`, 'ws_pg_dispatcher', 'ses_pg_dispatcher');
          await source.emit({ ...event, kind });
        }
        expect(owner).toHaveBeenCalledTimes(2);
        expect(factory).not.toHaveBeenCalled();
        expect(registry.prepareCalls).toEqual([]);
        expect(registry.states).toEqual([]);
        expect(store.appended).toEqual([]);
      } finally {
        await dispatcher.stop();
      }
    },
  );

  it('fails setup for persistent Claude on harness-server instead of constructing ordinary Claude', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      agentMetadata: { harness: 'claude_agent_sdk_persistent' },
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'owner-unregistered',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      registry: registry as unknown as RegistryClient,
    });
    await dispatcher.start();
    try {
      await source.emit(userMessage('evt_unregistered', 'ws_pg_dispatcher', 'ses_pg_dispatcher'));
      expect(setupFailedPayload(store)).toMatchObject({ phase: 'harness_selection' });
      expect(setupFailedPayload(store)?.error).toMatch(
        /no harness-server provider registered.*persistent/,
      );
      expect(activeRunnerCount(dispatcher)).toBe(0);
    } finally {
      await dispatcher.stop();
    }
  });

  it('is ready after a non-Kafka event source starts', async () => {
    const source = new FakeEventSource();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'event-source-readiness-unit',
      store: new RecordingStore(),
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
    });

    expect(dispatcher.readiness()).toEqual({ ready: false, reasons: ['event_source_not_started'] });
    await dispatcher.start();
    expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });
    await dispatcher.stop();
    expect(dispatcher.readiness()).toEqual({ ready: false, reasons: ['dispatcher_stopping'] });
  });

  it('uses an optional source status after startup without changing legacy source behavior', async () => {
    const source = new StatusEventSource();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'event-source-status-unit',
      store: new RecordingStore(),
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
    });

    await dispatcher.start();
    expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });

    source.setStatus({ ready: false, state: 'failed' });
    expect(dispatcher.readiness()).toEqual({ ready: false, reasons: ['event_source_failed'] });

    source.setStatus({ ready: false, state: 'stopped' });
    expect(dispatcher.readiness()).toEqual({ ready: false, reasons: ['event_source_stopped'] });

    await dispatcher.stop();
  });

  it('keeps Kafka consumer membership alive during long message handling', async () => {
    vi.useFakeTimers();
    try {
      const dispatcher = new Dispatcher({
        eventSource: new FakeEventSource(),
        groupId: 'kafka-heartbeat-unit',
        store: new RecordingStore(),
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
      });
      const heartbeat = vi.fn().mockResolvedValue(undefined);
      const promise = (
        dispatcher as unknown as {
          withKafkaHeartbeat(
            payload: { heartbeat: () => Promise<void> },
            work: () => Promise<void>,
          ): Promise<void>;
        }
      ).withKafkaHeartbeat({ heartbeat }, async () => {
        await new Promise((resolve) => setTimeout(resolve, 12_000));
      });

      await vi.advanceTimersByTimeAsync(12_100);
      await promise;

      expect(heartbeat.mock.calls.length).toBeGreaterThanOrEqual(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it('submits client user events without a Kafka client', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
    });

    await dispatcher.start();
    await source.emit({
      id: 'evt_user_1',
      workspaceId: 'ws_pg_dispatcher',
      sessionId: 'ses_pg_dispatcher',
      subpath: '',
      seq: 1,
      producedAt: new Date().toISOString(),
      producedBy: 'client',
      kind: 'user.message',
      payload: new TextEncoder().encode(
        JSON.stringify({ content: [{ type: 'text', text: 'from postgres' }] }),
      ),
      idempotencyKey: '',
    });

    await waitFor(() => store.appended.some((event) => event.kind === 'agent.message'));
    expect(store.appended).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: 'session.user_event_processed',
          producedBy: 'harness',
          payload: expect.any(Uint8Array),
        }),
      ]),
    );
    const acceptedIndex = store.appended.findIndex(
      (event) => event.kind === 'session.user_event_processed',
    );
    const agentMessageIndex = store.appended.findIndex((event) => event.kind === 'agent.message');
    expect(acceptedIndex).toBeGreaterThanOrEqual(0);
    expect(acceptedIndex).toBeLessThan(agentMessageIndex);
    expect(store.appended[0]?.workspaceId).toBe('ws_pg_dispatcher');
    expect(store.appended[0]?.sessionId).toBe('ses_pg_dispatcher');
    await dispatcher.stop();
  });

  it('rejects a noncanonical source id before recording acceptance or starting work', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-invalid-acceptance-id-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
    });

    await dispatcher.start();
    await expect(
      source.emit(userMessage('evt_', 'ws_pg_dispatcher', 'ses_pg_dispatcher')),
    ).rejects.toThrow('failed to persist acceptance for user.message');

    expect(store.appended.some((event) => event.kind === 'session.user_event_processed')).toBe(
      false,
    );
    expect(store.appended.some((event) => event.kind === 'agent.message')).toBe(false);
    await dispatcher.stop();
  });

  it('uses hard submit failure lifecycle after a poisoned event pump and recreates the runner', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const poisonedHarness = new PumpPoisonHarness();
    const replacementHarness = new EchoHarness();
    let harnessStarts = 0;
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-pump-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => {
        harnessStarts += 1;
        return harnessStarts === 1 ? poisonedHarness : replacementHarness;
      },
      sessionIdleTimeoutMs: 60_000,
    });
    const workspaceId = 'ws_pump_failure';
    const sessionId = 'ses_pump_failure';
    const runnerKey = `${workspaceId}/${sessionId}`;
    const runners = (dispatcher as unknown as { runners: Map<string, { pumpFailure: unknown }> })
      .runners;

    await dispatcher.start();
    try {
      await source.emit(userMessage('evt_pump_failure_1', workspaceId, sessionId, 'first', 1));
      await waitFor(() => {
        const runner = runners.get(runnerKey);
        return runner !== undefined && runner.pumpFailure !== null;
      });
      expect(poisonedHarness.submitCalls).toBe(1);

      await source.emit(userMessage('evt_pump_failure_2', workspaceId, sessionId, 'second', 2));

      expect(poisonedHarness.submitCalls).toBe(1);
      expect(poisonedHarness.stopReasons).toEqual(['replica.shutting_down']);
      expect(runners.has(runnerKey)).toBe(false);
      const runnerError = store.appended.find(
        (event) =>
          event.kind === 'session.error' &&
          (eventPayload(event).error as { type?: unknown } | undefined)?.type === 'runner_error',
      );
      expect(eventPayload(runnerError!)).toMatchObject({
        error: { type: 'runner_error', message: 'runner submit failed' },
        retry_status: { will_retry: false },
      });
      const terminal = store.appended.find(
        (event) =>
          event.kind === 'session.status_idle' &&
          (eventPayload(event).stop_reason as { type?: unknown } | undefined)?.type ===
            'retries_exhausted',
      );
      expect(eventPayload(terminal!)).toEqual({ stop_reason: { type: 'retries_exhausted' } });

      await source.emit(userMessage('evt_pump_failure_3', workspaceId, sessionId, 'third', 3));
      await waitFor(() => replacementHarness.stopReasons.length === 0 && harnessStarts === 2);
      await waitFor(
        () =>
          store.appended.filter(
            (event) => event.kind === 'agent.message' && event.workspaceId === workspaceId,
          ).length === 1,
      );
    } finally {
      await dispatcher.stop();
    }
  });

  it('measures producer-to-running latency from source producedAt and deduplicates a retry', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new DelayedStatusRunningHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'event-produced-at-latency-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
    });
    const event = userMessage('evt_produced_at_latency', 'ws_latency', 'ses_latency');
    event.producedAt = new Date(Date.now() - 2_000).toISOString();
    const before = await statusRunningHistogramTotals();

    await dispatcher.start();
    try {
      await source.emit(event);
      harness.emitStatusRunning();
      await waitFor(() =>
        store.appended.some((candidate) => candidate.kind === 'session.status_running'),
      );

      // This source retry happens after durable status_running but before a
      // terminal status, so event-id dedupe must still suppress another sample.
      await source.emit(event);
      const after = await statusRunningHistogramTotals();

      expect(after.count - before.count).toBe(1);
      // No timer is advanced here: only source `producedAt` can supply this
      // delay, proving the metric includes time before dispatcher consumption.
      expect(after.sum - before.sum).toBeGreaterThanOrEqual(1.5);

      harness.emitStatusIdle();
      await waitFor(() =>
        store.appended.some((candidate) => candidate.kind === 'session.status_idle'),
      );
      expect(statusRunningLatencyEntryCount(dispatcher)).toBe(0);
    } finally {
      await dispatcher.stop();
    }
  });

  it('clears pending producer latency when a turn terminates before status_running', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new DelayedStatusRunningHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'event-produced-at-terminal-cleanup-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
    });

    await dispatcher.start();
    try {
      await source.emit(userMessage('evt_terminal_before_running', 'ws_latency', 'ses_latency'));
      expect(pendingStatusRunningLatencyCount(dispatcher)).toBe(1);

      harness.emitStatusIdle();
      await waitFor(() =>
        store.appended.some((candidate) => candidate.kind === 'session.status_idle'),
      );

      expect(pendingStatusRunningLatencyCount(dispatcher)).toBe(0);
    } finally {
      await dispatcher.stop();
    }
  });

  it('uses Kafka produced_at header for producer-to-running latency', async () => {
    const store = new RecordingStore();
    const harness = new DelayedStatusRunningHarness();
    const dispatcher = new Dispatcher({
      groupId: 'kafka-produced-at-latency-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
    });
    const before = await statusRunningHistogramTotals();
    const producedAt = new Date(Date.now() - 2_000).toISOString();

    try {
      await (
        dispatcher as unknown as {
          onMessage(payload: {
            topic: string;
            message: {
              headers: Record<string, Buffer>;
              value: Buffer;
              offset: string;
            };
          }): Promise<void>;
        }
      ).onMessage({
        topic: 'orca.ws_kafka_latency.sessions.ses_kafka_latency.events',
        message: {
          headers: {
            id: Buffer.from('evt_kafka_produced_at_latency'),
            workspace_id: Buffer.from('ws_kafka_latency'),
            session_id: Buffer.from('ses_kafka_latency'),
            produced_by: Buffer.from('client'),
            kind: Buffer.from('user.message'),
            produced_at: Buffer.from(producedAt),
          },
          value: Buffer.from(JSON.stringify({ content: [{ type: 'text', text: 'delayed' }] })),
          offset: '1',
        },
      });
      harness.emitStatusRunning();
      await waitFor(() =>
        store.appended.some((candidate) => candidate.kind === 'session.status_running'),
      );

      const after = await statusRunningHistogramTotals();
      expect(after.count - before.count).toBe(1);
      expect(after.sum - before.sum).toBeGreaterThanOrEqual(1.5);
    } finally {
      await dispatcher.stop();
    }
  });

  it('attaches an explicitly correlated system.message across interleaved events', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new ReplayDeferredMessageHarness({ firstUserMessage: false });
    const userEvent: Event = {
      id: 'evt_user_system_context',
      workspaceId: 'ws_pg_dispatcher',
      sessionId: 'ses_pg_system_context',
      subpath: '',
      seq: 1,
      producedAt: new Date().toISOString(),
      producedBy: 'client',
      kind: 'user.message',
      payload: new TextEncoder().encode(
        JSON.stringify({
          content: [{ type: 'text', text: 'diagnose this' }],
          _orca_companion_system_event_id: 'evt_system_context',
        }),
      ),
      idempotencyKey: '',
    };
    const systemEvent: Event = {
      ...userEvent,
      id: 'evt_system_context',
      seq: 3,
      kind: 'system.message',
      payload: new TextEncoder().encode(
        JSON.stringify({ content: [{ type: 'text', text: 'Use production safeguards.' }] }),
      ),
    };
    store.seed([
      userEvent,
      {
        ...userEvent,
        id: 'evt_interleaved',
        seq: 2,
        producedBy: 'harness',
        kind: 'session.status_running',
        payload: new TextEncoder().encode('{}'),
      },
      systemEvent,
    ]);
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-system-message-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
    });

    await dispatcher.start();
    await source.emit(userEvent);

    await waitFor(() => harness.submitted.length === 1);
    expect(harness.submitted[0]).toEqual({
      id: 'evt_user_system_context',
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'diagnose this' }] },
      systemMessage: {
        content: [{ type: 'text', text: 'Use production safeguards.' }],
      },
    });
    expect(
      store.appended
        .filter((event) => event.kind === 'session.user_event_processed')
        .map((event) => eventPayload(event).user_event_id),
    ).toEqual(['evt_user_system_context', 'evt_system_context']);
    await dispatcher.stop();
  });

  it('uses an explicit event id with the Kafka offset domain to find a companion', async () => {
    const store = new RecordingStore();
    const harness = new ReplayDeferredMessageHarness({ firstUserMessage: false });
    const userEvent = userMessage(
      'evt_kafka_user_system_context',
      'ws_kafka_dispatcher',
      'ses_kafka_system_context',
      'diagnose this',
      41,
    );
    const systemEvent: Event = {
      ...userEvent,
      id: 'evt_kafka_system_context',
      seq: 42,
      kind: 'system.message',
      payload: new TextEncoder().encode(
        JSON.stringify({ content: [{ type: 'text', text: 'Use production safeguards.' }] }),
      ),
    };
    userEvent.payload = new TextEncoder().encode(
      JSON.stringify({
        content: [{ type: 'text', text: 'diagnose this' }],
        _orca_companion_system_event_id: systemEvent.id,
      }),
    );
    store.seed([
      userEvent,
      {
        ...userEvent,
        id: 'evt_kafka_interleaved',
        seq: 42,
        producedBy: 'harness',
        kind: 'session.status_running',
        payload: new TextEncoder().encode('{}'),
      },
      { ...systemEvent, seq: 43 },
    ]);
    const dispatcher = new Dispatcher({
      groupId: 'kafka-system-message-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
    });

    await (
      dispatcher as unknown as {
        onMessage(payload: {
          topic: string;
          message: {
            headers: Record<string, Buffer>;
            value: Buffer;
            offset: string;
          };
        }): Promise<void>;
      }
    ).onMessage({
      topic: 'orca.ws_kafka_dispatcher.sessions.ses_kafka_system_context.events',
      message: {
        headers: {
          workspace_id: Buffer.from('ws_kafka_dispatcher'),
          session_id: Buffer.from('ses_kafka_system_context'),
          produced_by: Buffer.from('client'),
          kind: Buffer.from('user.message'),
          id: Buffer.from(userEvent.id),
        },
        value: Buffer.from(userEvent.payload),
        offset: '41',
      },
    });

    await waitFor(() => harness.submitted.length === 1);
    expect(harness.submitted[0]).toEqual({
      id: 'evt_kafka_user_system_context',
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'diagnose this' }] },
      systemMessage: {
        content: [{ type: 'text', text: 'Use production safeguards.' }],
      },
    });
    expect(store.readOptions[0]).toMatchObject({ fromCursor: '42', maxEvents: 0 });
    await dispatcher.stop();
  });

  it.each(['session.archived', 'session.deleted'] as const)(
    'stops a warm runner when %s emits a lifecycle sentinel',
    async (kind) => {
      const source = new FakeEventSource();
      const store = new RecordingStore();
      const harness = new EchoHarness();
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'archive-sentinel-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory: () => harness,
        sessionIdleTimeoutMs: 60_000,
      });

      await dispatcher.start();
      await source.emit(userMessage('evt_before_archive', 'ws_archive', 'ses_archive'));
      await source.emit({
        id: 'evt_archive',
        workspaceId: 'ws_archive',
        sessionId: 'ses_archive',
        subpath: '',
        seq: 2,
        producedAt: new Date().toISOString(),
        producedBy: 'transcript-store',
        kind,
        payload: new Uint8Array(),
        idempotencyKey: '',
      });

      expect(harness.stopReasons).toContain('client.archived');
      await dispatcher.stop();
    },
  );

  it('does not start harness work when the acceptance marker cannot be persisted', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore({ failOnKind: 'session.user_event_processed' });
    const harness = new EchoHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-acceptance-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      sessionIdleTimeoutMs: 20,
    });

    await dispatcher.start();
    try {
      await expect(
        source.emit(
          userMessage('evt_user_acceptance_fail', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
        ),
      ).rejects.toThrow('failed to persist acceptance for user.message');
      expect(store.appended.some((event) => event.kind === 'agent.message')).toBe(false);
      expect(store.appended.some((event) => event.kind === 'session.status_idle')).toBe(false);
      await waitFor(() => harness.stopReasons.includes('idle.timeout'));
    } finally {
      await dispatcher.stop();
    }
  });

  it('does not use an existing acceptance marker to suppress redelivery', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-redelivery-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
    });
    const event = userMessage(
      'evt_user_acceptance_redelivery',
      'ws_pg_dispatcher',
      'ses_pg_dispatcher',
    );

    await dispatcher.start();
    await source.emit(event);
    await source.emit(event);
    await waitFor(
      () => store.appended.filter((candidate) => candidate.kind === 'agent.message').length === 2,
    );

    expect(
      store.appended.filter((candidate) => candidate.kind === 'session.user_event_processed'),
    ).toHaveLength(2);
    await dispatcher.stop();
  });

  it('skips a redelivered source event only after terminal status and completion marker append', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new TerminalEchoHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-completed-redelivery-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
    });
    const event = userMessage(
      'evt_user_completed_redelivery',
      'ws_pg_dispatcher',
      'ses_completed_redelivery',
    );

    await dispatcher.start();
    try {
      await source.emit(event);
      await waitFor(() =>
        store.appended.some((candidate) => candidate.kind === 'session.user_event_completed'),
      );

      const terminalBatch = store.appendBatches.find((batch) =>
        batch.some((candidate) => candidate.kind === 'session.status_idle'),
      );
      expect(terminalBatch?.map((candidate) => candidate.kind)).toEqual([
        'session.status_idle',
        'session.user_event_completed',
      ]);
      if (!terminalBatch) throw new Error('terminal completion batch missing');
      const completionMarker = terminalBatch.find(
        (candidate) => candidate.kind === 'session.user_event_completed',
      );
      if (!completionMarker) throw new Error('completion marker missing');
      expect(eventPayload(completionMarker)).toEqual({ user_event_id: event.id });

      await source.emit(event);
      expect(harness.submitCount).toBe(1);
    } finally {
      await dispatcher.stop();
    }
  });

  it('rebuilds completed-turn dedupe from transcript after dispatcher restart', async () => {
    const store = new RecordingStore();
    const event = userMessage(
      'evt_user_completed_restart',
      'ws_pg_dispatcher',
      'ses_completed_restart',
    );
    const firstSource = new FakeEventSource();
    const firstHarness = new TerminalEchoHarness();
    const firstDispatcher = new Dispatcher({
      eventSource: firstSource,
      groupId: 'pg-dispatcher-completed-restart-first-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => firstHarness,
    });

    await firstDispatcher.start();
    await firstSource.emit(event);
    await waitFor(() =>
      store.appended.some((candidate) => candidate.kind === 'session.user_event_completed'),
    );
    await firstDispatcher.stop();

    const restartSource = new FakeEventSource();
    const restartedHarness = new TerminalEchoHarness();
    const restartedDispatcher = new Dispatcher({
      eventSource: restartSource,
      groupId: 'pg-dispatcher-completed-restart-second-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => restartedHarness,
    });

    await restartedDispatcher.start();
    try {
      await restartSource.emit(event);
      expect(restartedHarness.submitCount).toBe(0);
    } finally {
      await restartedDispatcher.stop();
    }
  });

  it('evicts the completed cache with an idle runner and rebuilds it before redelivery', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harnessFactory = vi.fn(() => new TerminalEchoHarness());
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-completed-idle-rebuild-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory,
      sessionIdleTimeoutMs: 1,
    });
    const event = userMessage(
      'evt_user_completed_idle_rebuild',
      'ws_pg_dispatcher',
      'ses_completed_idle_rebuild',
    );

    await dispatcher.start();
    try {
      await source.emit(event);
      await waitFor(() =>
        store.appended.some((candidate) => candidate.kind === 'session.user_event_completed'),
      );
      await waitFor(
        () => completedUserEventCacheSize(dispatcher) === 0 && activeRunnerCount(dispatcher) === 0,
      );

      await source.emit(event);
      expect(harnessFactory).toHaveBeenCalledTimes(1);
    } finally {
      await dispatcher.stop();
    }
  });

  it('does not complete-cache a source event when terminal append fails', async () => {
    const store = new FailFirstCompletedMarkerStore();
    const event = userMessage(
      'evt_user_completed_append_failure',
      'ws_pg_dispatcher',
      'ses_completed_append_failure',
    );
    const firstSource = new FakeEventSource();
    const firstHarness = new TerminalEchoHarness();
    const firstDispatcher = new Dispatcher({
      eventSource: firstSource,
      groupId: 'pg-dispatcher-completed-append-failure-first-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => firstHarness,
    });
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await firstDispatcher.start();
      await firstSource.emit(event);
      await waitFor(() => expect(store.failedCompletionBatches).toHaveLength(1));
      expect(
        store.appended.some((candidate) => candidate.kind === 'session.user_event_completed'),
      ).toBe(false);
      await firstSource.emit(event);
      expect(firstHarness.submitCount).toBe(2);
      await firstDispatcher.stop();

      store.recover();
      const retrySource = new FakeEventSource();
      const retryHarness = new TerminalEchoHarness();
      const retryDispatcher = new Dispatcher({
        eventSource: retrySource,
        groupId: 'pg-dispatcher-completed-append-failure-retry-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory: () => retryHarness,
      });

      await retryDispatcher.start();
      try {
        await retrySource.emit(event);
        await waitFor(() =>
          store.appended.some((candidate) => candidate.kind === 'session.user_event_completed'),
        );
        expect(retryHarness.submitCount).toBe(1);
        const failedMarker = store.failedCompletionBatches[0]?.find(
          (candidate) => candidate.kind === 'session.user_event_completed',
        );
        const persistedMarker = store.appended.find(
          (candidate) => candidate.kind === 'session.user_event_completed',
        );
        expect(persistedMarker).toMatchObject({
          id: failedMarker?.id,
          idempotencyKey: failedMarker?.idempotencyKey,
        });
      } finally {
        await retryDispatcher.stop();
      }
    } finally {
      error.mockRestore();
    }
  });

  it('keeps source retryable when completed-marker cache rebuild fails', async () => {
    const source = new FakeEventSource();
    const store = new FailingReadStore();
    const harnessFactory = vi.fn(() => new TerminalEchoHarness());
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-completed-cache-read-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory,
    });

    await dispatcher.start();
    try {
      await expect(
        source.emit(
          userMessage(
            'evt_user_completed_cache_read_failure',
            'ws_pg_dispatcher',
            'ses_completed_cache_read_failure',
          ),
        ),
      ).rejects.toBeInstanceOf(RetryableSessionEventError);
      expect(harnessFactory).not.toHaveBeenCalled();
      expect(store.appended).toEqual([]);
    } finally {
      await dispatcher.stop();
    }
  });

  it('rethrows unexpected runner start failures to the event source', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-start-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new FailingStartHarness(),
    });

    await dispatcher.start();
    try {
      await expect(
        source.emit(userMessage('evt_user_start_fail_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher')),
      ).rejects.toThrow('harness unavailable');
      expect(store.appended.some((event) => event.kind === 'session.status_terminated')).toBe(
        false,
      );
      expect(store.appended.some((event) => event.kind === 'agent.message')).toBe(false);
    } finally {
      await dispatcher.stop();
    }
  });

  it('acks permanent Claude model-control errors as setup failures', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      agent: {
        id: 'agt_fake',
        name: 'invalid controls',
        workspace_id: 'ws_pg_dispatcher',
        version: 1,
        model: { provider: 'openai', id: 'gpt-5', speed: 'fast' },
        system: '',
        tools: [],
        mcp_servers: [],
        skills: [],
      },
    });
    const harnessFactory = vi.fn(() => new EchoHarness());
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-model-controls-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory,
      registry: registry as unknown as RegistryClient,
    });

    await dispatcher.start();
    try {
      await expect(
        source.emit(
          userMessage('evt_invalid_controls', 'ws_pg_dispatcher', 'ses_invalid_controls'),
        ),
      ).resolves.toBeUndefined();
      expect(harnessFactory).not.toHaveBeenCalled();
      expect(store.appended.map((event) => event.kind)).toEqual([
        'session.error',
        'session.status_idle',
      ]);
      expect(registry.states.at(-1)).toMatchObject({
        sessionId: 'ses_invalid_controls',
        status: 'idle',
      });
    } finally {
      await dispatcher.stop();
    }
  });

  it.each(['model_config', 'agent_version', 'mcp_server', 'environment_version', 'skill'])(
    'completes permanent Registry %s prepare failures without retrying the source event',
    async (resourceType) => {
      const source = new FakeEventSource();
      const store = new RecordingStore();
      const registry = new RecordingRegistry();
      registry.prepareError = new RegistryInvalidRuntimeBindingError(
        resourceType,
        'invalid binding',
      );
      const harnessFactory = vi.fn(() => new EchoHarness());
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'pg-dispatcher-registry-model-controls-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory,
        registry: registry as unknown as RegistryClient,
      });

      await dispatcher.start();
      try {
        await expect(
          source.emit(
            userMessage(
              'evt_registry_invalid_controls',
              'ws_pg_dispatcher',
              'ses_invalid_controls',
            ),
          ),
        ).resolves.toBeUndefined();
        expect(harnessFactory).not.toHaveBeenCalled();
        expect(store.appended.map((event) => event.kind)).toEqual([
          'session.user_event_processed',
          'session.error',
          'session.status_idle',
          'session.user_event_completed',
        ]);
        expect(JSON.parse(Buffer.from(store.appended[1]!.payload).toString())).toMatchObject({
          error: { type: 'setup_failed' },
          retry_status: { will_retry: false },
          phase: resourceType === 'model_config' ? 'model_config' : 'execution_preparation',
        });
        expect(JSON.parse(Buffer.from(store.appended[2]!.payload).toString())).toMatchObject({
          stop_reason: { type: 'retries_exhausted' },
        });
        await source.emit(
          userMessage('evt_registry_invalid_controls', 'ws_pg_dispatcher', 'ses_invalid_controls'),
        );
        expect(registry.prepareCalls).toHaveLength(1);
        expect(store.appended).toHaveLength(4);
        expect(registry.states.at(-1)).toMatchObject({
          sessionId: 'ses_invalid_controls',
          status: 'idle',
        });
      } finally {
        await dispatcher.stop();
      }
    },
  );

  it.each([
    ['an HTTP 503', new Error('prepareExecution failed: 503 registry unavailable')],
    ['a network failure', new Error('fetch failed')],
  ])('keeps cold Registry failure retryable for %s', async (_label, prepareError) => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    registry.prepareError = prepareError;
    const harnessFactory = vi.fn(() => new EchoHarness());
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-retryable-registry-prepare-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory,
      registry: registry as unknown as RegistryClient,
    });

    await dispatcher.start();
    try {
      await expect(
        source.emit(
          userMessage('evt_registry_retryable_prepare', 'ws_pg_dispatcher', 'ses_registry_retry'),
        ),
      ).rejects.toBeInstanceOf(RetryableSessionEventError);
      expect(harnessFactory).not.toHaveBeenCalled();
      expect(store.appended).toEqual([]);
      expect(registry.states).toEqual([]);
    } finally {
      await dispatcher.stop();
    }
  });

  it.each(['model_config', 'agent_version'])(
    'completes permanent Registry %s failures while refreshing a warm runner',
    async (resourceType) => {
      const source = new FakeEventSource();
      const store = new RecordingStore();
      const registry = new RecordingRegistry();
      const harness = new EchoHarness();
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'pg-dispatcher-refresh-model-controls-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory: () => harness,
        registry: registry as unknown as RegistryClient,
      });

      await dispatcher.start();
      try {
        await source.emit(
          userMessage('evt_valid_turn', 'ws_pg_dispatcher', 'ses_refresh_controls'),
        );
        registry.prepareError = new RegistryInvalidRuntimeBindingError(
          resourceType,
          'invalid binding',
        );

        await expect(
          source.emit(
            userMessage('evt_invalid_refresh', 'ws_pg_dispatcher', 'ses_refresh_controls'),
          ),
        ).resolves.toBeUndefined();
        expect(store.appended.slice(-4).map((event) => event.kind)).toEqual([
          'session.user_event_processed',
          'session.error',
          'session.status_idle',
          'session.user_event_completed',
        ]);
        expect(runnerSessionConfigKeyCount(dispatcher)).toBe(0);
        expect(harness.stopReasons).toEqual(['session.updated']);
        expect(registry.states.at(-1)).toMatchObject({
          sessionId: 'ses_refresh_controls',
          status: 'idle',
        });
        await source.emit(
          userMessage('evt_invalid_refresh', 'ws_pg_dispatcher', 'ses_refresh_controls'),
        );
        expect(registry.prepareCalls).toHaveLength(2);
      } finally {
        await dispatcher.stop();
      }
    },
  );

  it.each(['cold', 'warm'])(
    'exports a %s preparation failure before accepting the next normal turn',
    async (start) => {
      const source = new FakeEventSource();
      const store = new RecordingStore();
      const registry = new RecordingRegistry();
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'preparation-projection-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory: () => new TerminalEchoHarness(),
        registry: registry as unknown as RegistryClient,
      });
      const send = async (id: string): Promise<Event> => {
        const event = userMessage(id, 'ws_pg_dispatcher', 'ses_prepare_projection');
        await store.append(event.workspaceId, event.sessionId, [event]);
        await source.emit(event);
        await waitFor(() =>
          store.appended.some(
            (candidate) =>
              candidate.kind === 'session.user_event_completed' &&
              eventPayload(candidate).user_event_id === id,
          ),
        );
        return event;
      };
      await dispatcher.start();
      try {
        if (start === 'warm') await send('evt_projection_warmup');
        registry.prepareError = new RegistryInvalidRuntimeBindingError(
          'agent_version',
          'agt_archived@1',
        );
        const failed = await send('evt_projection_failed');
        const traces = projectCanonicalTurns(store.appended);
        expect(traces.at(-1)).toMatchObject({
          anchorEventId: failed.id,
          root: { status: 'error', metadata: { 'orca.turn.terminal_reason': 'retries_exhausted' } },
        });
        registry.prepareError = null;
        const next = await send('evt_projection_next');
        const recovered = projectCanonicalTurns(store.appended);
        expect(recovered.at(-1)).toMatchObject({ anchorEventId: next.id, root: { status: 'ok' } });
        expect(recovered).toHaveLength(start === 'warm' ? 3 : 2);
      } finally {
        await dispatcher.stop();
      }
    },
  );

  it('preserves committed outcome envelopes after a dispatcher and store restart', async () => {
    const registry = new RecordingRegistry();
    const sourceEvent = userMessage(
      'evt_outcome_envelope_restart',
      'ws_pg_dispatcher',
      'ses_envelope_restart',
    );
    const persisted: Event[] = [];
    vi.useFakeTimers({ toFake: ['Date'] });
    try {
      for (let run = 0; run < 2; run++) {
        vi.setSystemTime(new Date(`2026-09-12T10:0${run}:00.000Z`));
        const source = new FakeEventSource();
        // Each client starts with no event-ID cache, as native Kafka/Pulsar do.
        const store = new RecordingStore();
        store.seed([{ ...sourceEvent, seq: 1 }, ...persisted]);
        registry.prepareError = new RegistryInvalidRuntimeBindingError(
          run === 0 ? 'agent_version' : 'model_config',
          `binding-attempt-${run}`,
        );
        if (run === 0) {
          const append = store.append.bind(store);
          vi.spyOn(store, 'append').mockImplementation(async (...args) => {
            if (args[2].some(({ kind }) => kind === 'session.user_event_completed')) {
              throw new Error('crashed before completion marker');
            }
            return await append(...args);
          });
        } else {
          const read = store.read.bind(store);
          const preparationsBeforeRun = registry.prepareCalls.length;
          let truncated = false;
          vi.spyOn(store, 'read').mockImplementation(async function* (...args) {
            const truncate = !truncated && registry.prepareCalls.length > preparationsBeforeRun;
            if (truncate) truncated = true;
            for await (const event of read(...args)) {
              yield event;
              if (truncate) throw new Error('Pulsar recovery read timed out after a prefix');
            }
          });
        }
        const dispatcher = new Dispatcher({
          eventSource: source,
          groupId: 'outcome-envelope-restart-unit',
          store,
          anthropicApiKey: 'unused',
          modelDefault: 'fake',
          registry: registry as unknown as RegistryClient,
        });
        await dispatcher.start();
        try {
          if (run === 0)
            await expect(source.emit(sourceEvent)).rejects.toBeInstanceOf(
              RetryableSessionEventError,
            );
          else {
            await expect(source.emit(sourceEvent)).rejects.toBeInstanceOf(SessionEventBarrierError);
            expect(store.appended).toEqual([]);
            await expect(source.emit(sourceEvent)).resolves.toBeUndefined();
          }
          for (const event of store.appended) {
            const original = persisted.find(({ id }) => id === event.id);
            if (original) expect(kafkaTranscriptHash(event)).toBe(kafkaTranscriptHash(original));
          }
          if (run === 1)
            expect(store.appended.map(({ kind }) => kind)).toEqual([
              'session.user_event_completed',
            ]);
          persisted.push(...store.appended);
        } finally {
          await dispatcher.stop();
        }
      }
      expect(new Set(persisted.map(({ id }) => id)).size).toBe(4);
      expect(persisted).toHaveLength(4);
    } finally {
      vi.useRealTimers();
    }
  });

  it('retains earlier completed-turn dedupe after a permanent warm preparation failure', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const harness = new TerminalEchoHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'warm-preparation-completed-cache-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
    });
    const first = userMessage('evt_earlier_completed', 'ws_pg_dispatcher', 'ses_warm_dedupe');
    await dispatcher.start();
    try {
      await source.emit(first);
      await waitFor(() =>
        store.appended.some(({ kind }) => kind === 'session.user_event_completed'),
      );
      registry.prepareError = new RegistryInvalidRuntimeBindingError(
        'agent_version',
        'agt_archived@1',
      );
      await source.emit(userMessage('evt_later_failed', 'ws_pg_dispatcher', 'ses_warm_dedupe'));
      const appendedBeforeReplay = store.appended.length;

      await source.emit(first);

      expect(registry.prepareCalls).toHaveLength(2);
      expect(store.appended).toHaveLength(appendedBeforeReplay);
      expect(harness.submitCount).toBe(1);
    } finally {
      await dispatcher.stop();
    }
  });

  it('deduplicates permanent preparation failures after dispatcher restart', async () => {
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    registry.prepareError = new RegistryInvalidRuntimeBindingError(
      'agent_version',
      'agt_archived@1',
    );
    const harnessFactory = vi.fn(() => new EchoHarness());
    const event = userMessage('evt_failed_restart', 'ws_pg_dispatcher', 'ses_failed_restart');
    for (let run = 0; run < 2; run++) {
      const source = new FakeEventSource();
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'preparation-restart-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory,
        registry: registry as unknown as RegistryClient,
      });
      await dispatcher.start();
      try {
        await source.emit(event);
      } finally {
        await dispatcher.stop();
      }
    }
    expect(registry.prepareCalls).toHaveLength(1);
    expect(store.appended).toHaveLength(4);
    expect(harnessFactory).not.toHaveBeenCalled();
  });

  it.each([
    ['outcome', 2],
    ['completion marker', 1],
  ] as const)(
    'recovers a lost %s append response after permanent preparation failure',
    async (batch, prepareCalls) => {
      const source = new FakeEventSource();
      const store = new RecordingStore();
      const append = store.append.bind(store);
      let fail = true;
      vi.spyOn(store, 'append').mockImplementation(async (...args) => {
        const result = await append(...args);
        const isCompletion = args[2].some(({ kind }) => kind === 'session.user_event_completed');
        if (fail && isCompletion === (batch === 'completion marker')) {
          fail = false;
          throw new Error('lost append response');
        }
        return result;
      });
      const registry = new RecordingRegistry();
      registry.prepareError = new RegistryInvalidRuntimeBindingError(
        'agent_version',
        'agt_archived@1',
      );
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'preparation-append-response-failure-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        registry: registry as unknown as RegistryClient,
      });
      const event = userMessage(
        'evt_lost_append_response',
        'ws_pg_dispatcher',
        'ses_lost_append_response',
      );
      await dispatcher.start();
      try {
        await expect(source.emit(event)).rejects.toBeInstanceOf(RetryableSessionEventError);
        await expect(source.emit(event)).resolves.toBeUndefined();
        expect(registry.prepareCalls).toHaveLength(prepareCalls);
        expect(registry.states).toHaveLength(prepareCalls);
        expect(store.appended).toHaveLength(4);
      } finally {
        await dispatcher.stop();
      }
    },
  );

  it.each([
    ['user.message', 'session.user_event_processed'],
    ['user.message', 'session.error'],
    ['user.message', 'session.status_idle'],
    ['user.interrupt', 'session.user_event_processed'],
    ['user.interrupt', 'session.status_idle'],
  ])(
    'repairs a partial %s outcome append missing %s before completing after restart',
    async (kind, failedKind) => {
      const store = new RecordingStore();
      const append = store.append.bind(store);
      let fail = true;
      vi.spyOn(store, 'append').mockImplementation(async (workspaceId, sessionId, events) => {
        if (fail && events.some((event) => event.kind === failedKind)) {
          fail = false;
          // A backend can persist every other send in a multi-event batch.
          await append(
            workspaceId,
            sessionId,
            events.filter((event) => event.kind !== failedKind),
          );
          throw new Error(`partial append failed for ${failedKind}`);
        }
        return await append(workspaceId, sessionId, events);
      });
      const registry = new RecordingRegistry();
      registry.prepareError = new RegistryInvalidRuntimeBindingError(
        'agent_version',
        'agt_archived@1',
      );
      const harnessFactory = vi.fn(() => new EchoHarness());
      const event = {
        ...userMessage('evt_partial_outcome', 'ws_pg_dispatcher', 'ses_partial_outcome'),
        kind,
      };
      const orderedKinds = [
        'session.user_event_processed',
        ...(kind === 'user.message' ? ['session.error'] : []),
        'session.status_idle',
        'session.user_event_completed',
      ];
      for (let run = 0; run < 3; run++) {
        const source = new FakeEventSource();
        const dispatcher = new Dispatcher({
          eventSource: source,
          groupId: 'partial-outcome-restart-unit',
          store,
          anthropicApiKey: 'unused',
          modelDefault: 'fake',
          harnessFactory,
          registry: registry as unknown as RegistryClient,
        });
        await dispatcher.start();
        try {
          if (run === 0) {
            await expect(source.emit(event)).rejects.toBeInstanceOf(RetryableSessionEventError);
            expect(store.appended.map(({ kind }) => kind)).toEqual(
              orderedKinds.slice(0, orderedKinds.indexOf(failedKind)),
            );
            expect(store.appended.some((event) => event.kind === failedKind)).toBe(false);
            expect(
              store.appended.some((event) => event.kind === 'session.user_event_completed'),
            ).toBe(false);
          } else {
            await expect(source.emit(event)).resolves.toBeUndefined();
            expect(store.appended.map(({ kind }) => kind)).toEqual(orderedKinds);
            expect(store.appended.filter((event) => event.kind === failedKind)).toHaveLength(1);
            expect(
              store.appended.filter((event) => event.kind === 'session.status_idle'),
            ).toHaveLength(1);
            expect(
              store.appended.filter((event) => event.kind === 'session.user_event_processed'),
            ).toHaveLength(1);
            expect(
              store.appended.filter((event) => event.kind === 'session.user_event_completed'),
            ).toHaveLength(1);
            expect(store.appended.at(-1)?.kind).toBe('session.user_event_completed');
          }
        } finally {
          await dispatcher.stop();
        }
      }
      expect(registry.prepareCalls).toHaveLength(kind === 'user.message' ? 2 : 0);
      expect(harnessFactory).not.toHaveBeenCalled();
    },
  );

  it.each([
    'session.user_event_processed',
    'session.error',
    'session.status_idle',
    'session.user_event_completed',
  ])(
    'repairs a failed %s append before Pulsar delivers the queued interrupt',
    async (failedKind) => {
      const message = userMessage('evt_barrier_message', 'ws_pg_dispatcher', 'ses_barrier');
      const interrupt = { ...message, id: 'evt_barrier_interrupt', seq: 2, kind: 'user.interrupt' };
      const { source, acknowledged, nacked } = queuedPulsarSource([message, interrupt]);
      const store = new RecordingStore();
      store.seed([message, interrupt]);
      const append = store.append.bind(store);
      let available = false;
      let failures = 0;
      vi.spyOn(store, 'append').mockImplementation(async (workspaceId, sessionId, events) => {
        if (!available && events.some(({ kind }) => kind === failedKind)) {
          failures += 1;
          throw new Error('outcome storage unavailable');
        }
        return await append(workspaceId, sessionId, events);
      });
      const registry = new RecordingRegistry();
      registry.prepareError = new RegistryInvalidRuntimeBindingError(
        'agent_version',
        'agt_archived@1',
      );
      const harnessFactory = vi.fn(() => new EchoHarness());
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'pulsar-outcome-barrier-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory,
        registry: registry as unknown as RegistryClient,
      });
      try {
        await dispatcher.start();
        await waitFor(() => failures >= 7 || acknowledged.length > 0);
        expect(acknowledged).toEqual([]);
        expect(nacked).toEqual([]);
        expect(failures).toBeGreaterThanOrEqual(7);
        expect(registry.prepareCalls).toHaveLength(1);
        // A changed preparation result cannot replace the captured failure during repair.
        registry.prepareError = null;
        available = true;
        await waitFor(() => acknowledged.length === 2);
        expect(acknowledged).toEqual([message.id, interrupt.id]);
        expect(registry.prepareCalls).toHaveLength(1);
        expect(harnessFactory).not.toHaveBeenCalled();
        expect(store.appended.map(({ kind }) => kind)).toEqual([
          'session.user_event_processed',
          'session.error',
          'session.status_idle',
          'session.user_event_completed',
          'session.user_event_processed',
          'session.status_idle',
          'session.user_event_completed',
        ]);
        const traces = projectCanonicalTurns([message, interrupt, ...store.appended]);
        expect(traces).toHaveLength(1);
        expect(traces[0]).toMatchObject({
          anchorEventId: message.id,
          root: { status: 'error', metadata: { 'orca.turn.terminal_reason': 'retries_exhausted' } },
        });
      } finally {
        await dispatcher.stop();
      }
    },
  );

  it('cancels the Pulsar repair backoff on shutdown without acknowledging unfinished work', async () => {
    const message = userMessage('evt_barrier_stop', 'ws_pg_dispatcher', 'ses_barrier_stop');
    const { source, acknowledged, nacked } = queuedPulsarSource([message], 10_000);
    const store = new RecordingStore({ failOnKind: 'session.error' });
    const append = vi.spyOn(store, 'append');
    const registry = new RecordingRegistry();
    registry.prepareError = new RegistryInvalidRuntimeBindingError(
      'agent_version',
      'agt_archived@1',
    );
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pulsar-barrier-stop-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      registry: registry as unknown as RegistryClient,
    });
    try {
      await dispatcher.start();
      await waitFor(() =>
        append.mock.calls.some(([, , events]) => events[0]?.kind === 'session.error'),
      );
      await new Promise((resolve) => setTimeout(resolve, 10));
      let stopped = false;
      const stopping = dispatcher.stop().then(() => {
        stopped = true;
      });
      await vi.waitFor(() => expect(stopped).toBe(true), { timeout: 500 });
      await stopping;
      expect(acknowledged).toEqual([]);
      expect(nacked).toEqual([]);
      const attempts = append.mock.calls.length;
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(append).toHaveBeenCalledTimes(attempts);
    } finally {
      await dispatcher.stop();
    }
  });

  it('keeps unstarted work retryable when reading existing outcomes fails', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const read = store.read.bind(store);
    let reads = 0;
    vi.spyOn(store, 'read').mockImplementation(async function* (...args) {
      if (++reads === 2) throw new Error('outcome recovery read unavailable');
      yield* read(...args);
    });
    const registry = new RecordingRegistry();
    registry.prepareError = new RegistryInvalidRuntimeBindingError(
      'agent_version',
      'agt_archived@1',
    );
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'unstarted-outcome-read-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      registry: registry as unknown as RegistryClient,
    });
    const event = userMessage(
      'evt_recovery_read_failure',
      'ws_pg_dispatcher',
      'ses_recovery_read_failure',
    );
    await dispatcher.start();
    try {
      await expect(source.emit(event)).rejects.toBeInstanceOf(RetryableSessionEventError);
      expect(store.appended).toEqual([]);
      await expect(source.emit(event)).resolves.toBeUndefined();
      expect(store.appended).toHaveLength(4);
      expect(store.appended.at(-1)?.kind).toBe('session.user_event_completed');
    } finally {
      await dispatcher.stop();
    }
  });

  it.each(['state', 'transcript'])(
    'keeps permanent preparation failures retryable until %s persistence succeeds',
    async (failure) => {
      const source = new FakeEventSource();
      const store = new FailFirstCompletedMarkerStore();
      if (failure === 'state') store.recover();
      const registry = new RecordingRegistry();
      registry.prepareError = new RegistryInvalidRuntimeBindingError(
        'agent_version',
        'agt_archived@1',
      );
      const stateUpdate = vi.spyOn(registry, 'updateSessionStateInternal');
      if (failure === 'state') stateUpdate.mockRejectedValueOnce(new Error('Registry unavailable'));
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'preparation-persistence-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory: () => new EchoHarness(),
        registry: registry as unknown as RegistryClient,
      });
      const event = userMessage('evt_failed_persist', 'ws_pg_dispatcher', 'ses_failed_persist');
      await dispatcher.start();
      try {
        await expect(source.emit(event)).rejects.toBeInstanceOf(RetryableSessionEventError);
        expect(store.appended.map(({ kind }) => kind)).toEqual(
          failure === 'state'
            ? []
            : ['session.user_event_processed', 'session.error', 'session.status_idle'],
        );
        store.recover();
        await expect(source.emit(event)).resolves.toBeUndefined();
        expect(store.appended).toHaveLength(4);
        if (failure === 'transcript') {
          expect(
            store.failedCompletionBatches[0]!.map(({ id, idempotencyKey }) => ({
              id,
              idempotencyKey,
            })),
          ).toEqual(
            store.appended
              .filter(({ kind }) => kind === 'session.user_event_completed')
              .map(({ id, idempotencyKey }) => ({ id, idempotencyKey })),
          );
        }
      } finally {
        await dispatcher.stop();
      }
    },
  );

  it('completes a cold interrupt without preparing or spawning a runner', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    registry.prepareError = new Error('Registry execution preparation unavailable');
    const harnessFactory = vi.fn(() => new EchoHarness());
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'cold-interrupt-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory,
      registry: registry as unknown as RegistryClient,
    });
    const event = {
      ...userMessage('evt_cold_interrupt', 'ws_pg_dispatcher', 'ses_cold_interrupt'),
      kind: 'user.interrupt',
      payload: new TextEncoder().encode('{}'),
    };
    await dispatcher.start();
    try {
      await source.emit(event);
      await source.emit(event);
      expect(registry.prepareCalls).toEqual([]);
      expect(harnessFactory).not.toHaveBeenCalled();
      expect(registry.states).toEqual([
        { sessionId: 'ses_cold_interrupt', status: 'idle', sandboxHandleId: null },
      ]);
      expect(store.appended.map(({ kind }) => kind)).toEqual([
        'session.user_event_processed',
        'session.status_idle',
        'session.user_event_completed',
      ]);
      expect(JSON.parse(Buffer.from(store.appended[1]!.payload).toString())).toMatchObject({
        stop_reason: { type: 'end_turn' },
      });
    } finally {
      await dispatcher.stop();
    }
  });

  it.each(['state', 'transcript'])(
    'keeps a cold interrupt retryable until %s persistence succeeds',
    async (failure) => {
      const source = new FakeEventSource();
      const store = new FailFirstCompletedMarkerStore();
      if (failure === 'state') store.recover();
      const registry = new RecordingRegistry();
      const stateUpdate = vi.spyOn(registry, 'updateSessionStateInternal');
      if (failure === 'state') stateUpdate.mockRejectedValueOnce(new Error('Registry unavailable'));
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'cold-interrupt-persistence-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        registry: registry as unknown as RegistryClient,
      });
      const event = {
        ...userMessage('evt_interrupt_persist', 'ws_pg_dispatcher', 'ses_interrupt_persist'),
        kind: 'user.interrupt',
      };
      await dispatcher.start();
      try {
        await expect(source.emit(event)).rejects.toBeInstanceOf(RetryableSessionEventError);
        expect(store.appended.map(({ kind }) => kind)).toEqual(
          failure === 'state' ? [] : ['session.user_event_processed', 'session.status_idle'],
        );
        store.recover();
        await expect(source.emit(event)).resolves.toBeUndefined();
        expect(store.appended).toHaveLength(3);
        expect(registry.prepareCalls).toEqual([]);
      } finally {
        await dispatcher.stop();
      }
    },
  );

  it.each(['user.message', 'user.interrupt'])(
    'does not publish an idle outcome for %s when Registry rejects a closed session state update',
    async (kind) => {
      const source = new FakeEventSource();
      const store = new RecordingStore();
      const registry = new RecordingRegistry();
      registry.prepareError = new RegistryInvalidRuntimeBindingError(
        'agent_version',
        'agt_archived@1',
      );
      vi.spyOn(registry, 'updateSessionStateInternal').mockResolvedValue(null);
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'closed-session-preparation-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        registry: registry as unknown as RegistryClient,
      });
      await dispatcher.start();
      try {
        await expect(
          source.emit({
            ...userMessage('evt_closed_session', 'ws_pg_dispatcher', 'ses_closed_session'),
            kind,
          }),
        ).resolves.toBeUndefined();
        expect(store.appended).toEqual([]);
        expect(registry.prepareCalls).toHaveLength(kind === 'user.message' ? 1 : 0);
      } finally {
        await dispatcher.stop();
      }
    },
  );

  it('submits a warm interrupt to the existing runner without refreshing broken preparation', async () => {
    const source = new FakeEventSource();
    const registry = new RecordingRegistry();
    const harness = new EchoHarness();
    const submit = vi.spyOn(harness, 'submit');
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'warm-interrupt-unit',
      store: new RecordingStore(),
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
    });
    await dispatcher.start();
    try {
      await source.emit(userMessage('evt_warm_first', 'ws_pg_dispatcher', 'ses_warm_interrupt'));
      registry.prepareError = new RegistryInvalidRuntimeBindingError(
        'agent_version',
        'agt_archived@1',
      );
      await source.emit({
        ...userMessage('evt_warm_interrupt', 'ws_pg_dispatcher', 'ses_warm_interrupt'),
        kind: 'user.interrupt',
      });
      expect(submit.mock.calls.at(-1)?.[0].kind).toBe('user.interrupt');
      expect(registry.prepareCalls).toHaveLength(1);
      expect(harness.stopReasons).toEqual([]);
    } finally {
      await dispatcher.stop();
    }
  });

  it('drops a mismatched warm snapshot without publishing failure or updating Registry state', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const harness = new EchoHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'warm-snapshot-mismatch-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
    });
    await dispatcher.start();
    try {
      await source.emit(userMessage('evt_valid_tenant', 'ws_pg_dispatcher', 'ses_warm_tenant'));
      await waitFor(() => store.appended.some(({ kind }) => kind === 'agent.message'));
      const appendedBefore = store.appended.length;
      const statesBefore = registry.states.length;
      registry.sessionOverrides.workspace_id = 'ws_other_tenant';
      await source.emit(userMessage('evt_invalid_tenant', 'ws_pg_dispatcher', 'ses_warm_tenant'));
      expect(store.appended).toHaveLength(appendedBefore);
      expect(registry.states).toHaveLength(statesBefore);
      expect(harness.stopReasons).toEqual(['session.updated']);
    } finally {
      await dispatcher.stop();
    }
  });

  it('completes malformed cold receipt sources without preparation or model work', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = Object.assign(new RecordingRegistry(), {
      harnessTurn: vi.fn(async () => {
        throw new RegistryInvalidRuntimeBindingError('harness_state', 'ses_bad_receipt');
      }),
    });
    const harnessFactory = vi.fn(() => new EchoHarness());
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'invalid-cold-receipt',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory,
      registry: registry as unknown as RegistryClient,
    });
    await dispatcher.start();
    try {
      const event = userMessage('evt_bad_receipt', 'ws_pg_dispatcher', 'ses_bad_receipt');
      await expect(source.emit(event)).resolves.toBeUndefined();
      await expect(source.emit(event)).resolves.toBeUndefined();
      expect(registry.prepareCalls).toEqual([]);
      expect(harnessFactory).not.toHaveBeenCalled();
      expect(store.appended.map((event) => event.kind)).toEqual([
        'session.user_event_processed',
        'session.error',
        'session.status_idle',
        'session.user_event_completed',
      ]);
      expect(eventPayload(store.appended[1]!)).toMatchObject({
        error: { type: 'setup_failed' },
        retry_status: { will_retry: false },
      });
    } finally {
      await dispatcher.stop();
    }
  });

  it('keeps a durable recovery failure during runner startup retryable without a fabricated setup terminal', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const harness = new EchoHarness();
    vi.spyOn(harness, 'start').mockRejectedValue(new DurableHarnessStateError('settle ACK lost'));
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'startup-receipt-retry',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
    });
    await dispatcher.start();
    try {
      await expect(
        source.emit(userMessage('evt_startup_retry', 'ws_pg_dispatcher', 'ses_startup_retry')),
      ).rejects.toBeInstanceOf(RetryableSessionEventError);
      expect(store.appended).toEqual([]);
      expect(registry.states).toEqual([]);
    } finally {
      await dispatcher.stop();
    }
  });

  it('settles delayed cold sources when receipt inspection has no active owner and preparation drops the Session', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const inspect = vi.fn(async () => null);
    const registry = Object.assign(new RecordingRegistry({ sessionMissing: true }), {
      harnessTurn: inspect,
    });
    const harnessFactory = vi.fn(() => new EchoHarness());
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'inactive-receipt-source',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory,
      registry: registry as unknown as RegistryClient,
    });
    await dispatcher.start();
    try {
      const event = userMessage('evt_retired_source', 'ws_pg_dispatcher', 'ses_retired');
      await expect(source.emit(event)).resolves.toBeUndefined();
      await expect(source.emit(event)).resolves.toBeUndefined();
      expect(inspect).toHaveBeenCalledTimes(2);
      expect(inspect).toHaveBeenCalledWith({
        workspaceId: 'ws_pg_dispatcher',
        sessionId: 'ses_retired',
        request: { action: { type: 'inspect' } },
      });
      expect(harnessFactory).not.toHaveBeenCalled();
      expect(store.appended).toEqual([]);
      expect(registry.states).toEqual([]);
    } finally {
      await dispatcher.stop();
    }
  });

  it('skips user events for registry sessions that no longer exist', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({ sessionMissing: true });
    const harnessFactory = vi.fn(() => new EchoHarness());
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-missing-session-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory,
      registry: registry as unknown as RegistryClient,
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_missing_session_1', 'ws_pg_dispatcher', 'ses_missing_session'),
      );

      expect(harnessFactory).not.toHaveBeenCalled();
      expect(store.appended).toHaveLength(0);
      expect(registry.states).toHaveLength(0);
    } finally {
      await dispatcher.stop();
    }
  });

  it('skips user events when the prepared workspace does not match the event workspace', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({ preparedWorkspaceId: 'ws_other' });
    const harnessFactory = vi.fn(() => new EchoHarness());
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-agent-not-visible-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory,
      registry: registry as unknown as RegistryClient,
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_agent_not_visible_1', 'ws_pg_dispatcher', 'ses_not_visible'),
      );

      expect(harnessFactory).not.toHaveBeenCalled();
      expect(store.appended).toHaveLength(0);
      expect(registry.states).toHaveLength(0);
    } finally {
      await dispatcher.stop();
    }
  });

  it('skips user events when the prepared session belongs to another workspace', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({ sessionWorkspaceId: 'ws_other' });
    const harnessFactory = vi.fn(() => new EchoHarness());
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-session-workspace-mismatch-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory,
      registry: registry as unknown as RegistryClient,
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_session_workspace_mismatch_1', 'ws_pg_dispatcher', 'ses_mismatch'),
      );

      expect(registry.prepareCalls).toEqual([
        { workspaceId: 'ws_pg_dispatcher', sessionId: 'ses_mismatch' },
      ]);
      expect(harnessFactory).not.toHaveBeenCalled();
      expect(store.appended).toHaveLength(0);
      expect(registry.states).toHaveLength(0);
    } finally {
      await dispatcher.stop();
    }
  });

  it('skips user events when required workspace fields are missing from the snapshot', async () => {
    for (const registry of [
      new RecordingRegistry({ omitSessionWorkspaceId: true }),
      new RecordingRegistry({ omitAgentWorkspaceId: true }),
    ]) {
      const source = new FakeEventSource();
      const harnessFactory = vi.fn(() => new EchoHarness());
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'pg-dispatcher-missing-workspace-unit',
        store: new RecordingStore(),
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory,
        registry: registry as unknown as RegistryClient,
      });

      await dispatcher.start();
      try {
        await source.emit(
          userMessage('evt_user_missing_workspace_1', 'ws_pg_dispatcher', 'ses_missing_workspace'),
        );
        expect(harnessFactory).not.toHaveBeenCalled();
      } finally {
        await dispatcher.stop();
      }
    }
  });

  it('rejects a forged prepared resource that intersects the reserved Skill root before acquire', async () => {
    const source = new FakeEventSource();
    const runtime = new RecordingSandboxRuntime();
    const harnessFactory = vi.fn(() => new EchoHarness());
    const registry = new RecordingRegistry({
      resources: [
        {
          id: 'sesrsc_reserved_parent',
          type: 'file',
          file_id: 'file_untrusted',
          memory_store_id: null,
          repo_ref: null,
          mount_path: '/workspace',
          access: 'read_only',
          instructions: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
          mount_strategy: null,
        },
      ],
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-reserved-skill-root-unit',
      store: new RecordingStore(),
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory,
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: runtime,
      fileStore: new StubFileStore(new Map([['file_untrusted', Buffer.from('untrusted')]])),
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_reserved_skill_root_1', 'ws_pg_dispatcher', 'ses_reserved_root'),
      );

      expect(runtime.acquireCount).toBe(0);
      expect(harnessFactory).not.toHaveBeenCalled();
    } finally {
      await dispatcher.stop();
    }
  });

  it('stops a warm runner and skips the turn after its registry session is deleted', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const harnesses: StopRecordingHarness[] = [];
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-deleted-warm-session-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => {
        const harness = new StopRecordingHarness();
        harnesses.push(harness);
        return harness;
      },
      registry: registry as unknown as RegistryClient,
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_deleted_session_1', 'ws_pg_dispatcher', 'ses_deleted_session'),
      );
      await waitFor(() => store.appended.some((event) => event.kind === 'agent.message'));
      expect(harnesses).toHaveLength(1);

      const appendedBeforeDelete = store.appended.length;
      registry.sessionMissing = true;
      await source.emit(
        userMessage(
          'evt_user_deleted_session_2',
          'ws_pg_dispatcher',
          'ses_deleted_session',
          'two',
          2,
        ),
      );

      expect(harnesses).toHaveLength(1);
      expect(harnesses[0]?.stopReasons).toEqual(['client.archived']);
      expect(store.appended).toHaveLength(appendedBeforeDelete);
    } finally {
      await dispatcher.stop();
    }
  });

  it('stops a warm runner and keeps the turn retryable when config refresh fails', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const harnesses: StopRecordingHarness[] = [];
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-warm-refresh-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => {
        const harness = new StopRecordingHarness();
        harnesses.push(harness);
        return harness;
      },
      registry: registry as unknown as RegistryClient,
      sessionIdleTimeoutMs: 60_000,
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_refresh_failure_1', 'ws_pg_dispatcher', 'ses_refresh_failure'),
      );
      await waitFor(() => store.appended.some((event) => event.kind === 'agent.message'));
      expect(harnesses).toHaveLength(1);

      const messagesBeforeFailure = store.appended.filter(
        (event) => event.kind === 'agent.message',
      ).length;
      registry.prepareError = new Error('registry unavailable');
      await expect(
        source.emit(
          userMessage(
            'evt_user_refresh_failure_2',
            'ws_pg_dispatcher',
            'ses_refresh_failure',
            'two',
            2,
          ),
        ),
      ).rejects.toBeInstanceOf(RetryableSessionEventError);

      expect(harnesses[0]?.stopReasons).toEqual(['session.updated']);
      expect(store.appended.filter((event) => event.kind === 'agent.message')).toHaveLength(
        messagesBeforeFailure,
      );

      registry.prepareError = null;
      await source.emit(
        userMessage(
          'evt_user_refresh_failure_2',
          'ws_pg_dispatcher',
          'ses_refresh_failure',
          'two',
          2,
        ),
      );
      await waitFor(() => harnesses.length === 2);
    } finally {
      await dispatcher.stop();
    }
  });

  it('emits setup_failed and marks idle when the harness annotation cannot be resolved', async () => {
    // Previously this warned to stderr and silently ran the agent on
    // claude_agent_sdk/separate — a different harness, topology, and tool
    // surface than the operator pinned, with no evidence the API caller sees.
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      agentMetadata: { harness: 'claude_code', mode: 'hybrid' },
    });
    const harnesses: EchoHarness[] = [];
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-bad-annotation-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => {
        const h = new EchoHarness();
        harnesses.push(h);
        return h;
      },
      registry: registry as unknown as RegistryClient,
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_bad_annotation_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
      );

      expect(setupFailedPayload(store)?.phase).toBe('harness_selection');
      expect(setupFailedPayload(store)?.error).toMatch(/metadata\.mode must be one of/);
      // No harness was built: the session did NOT silently run on a fallback.
      expect(harnesses).toHaveLength(0);
      expect(registry.states.at(-1)).toEqual({
        sessionId: 'ses_pg_dispatcher',
        status: 'idle',
        sandboxHandleId: null,
      });
    } finally {
      await dispatcher.stop();
    }
  });

  it('emits setup_failed for a colocated harness harness-server cannot boot', async () => {
    // `mock` is accepted by `resolveHarnessAnnotation` (so `POST /v1/agents`
    // returns 200) but has no in-sandbox image or port. The throw used to escape
    // `spawnRunner` entirely — outside every try — leaving the client with no
    // setup_failed, no idle transition, and a wedged session.
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({ agentMetadata: { harness: 'mock' } });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-unbootable-harness-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: new RecordingSandboxRuntime(),
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_unbootable_harness_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
      );

      expect(setupFailedPayload(store)?.phase).toBe('harness_selection');
      expect(setupFailedPayload(store)?.error).toMatch(/declares no in-sandbox image\/port/);
      expect(registry.states.at(-1)).toEqual({
        sessionId: 'ses_pg_dispatcher',
        status: 'idle',
        sandboxHandleId: null,
      });
    } finally {
      await dispatcher.stop();
    }
  });

  it('accepts the deprecated in_sandbox mode on a persisted agent and routes it colocated', async () => {
    // Rows written before the `in_sandbox` -> `colocated` rename are still in
    // `metadata` JSONB and in `agent_versions`; no migration rewrote them.
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      agentMetadata: { harness: 'claude_code', mode: 'in_sandbox' },
    });
    const runtime = new RecordingSandboxRuntime();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-in-sandbox-alias-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_in_sandbox_alias_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
      );

      expect(setupFailedPayload(store)).toBeNull();
      // `colocated` was selected: the colocated path acquires a sandbox and
      // stamps the colocated upload ownership on its spec.
      expect(runtime.acquireCount).toBe(1);
      expect(runtime.acquiredSpecs[0]?.fileUploadOwnership).toEqual({
        owner: 'node',
        group: 'node',
      });
    } finally {
      await dispatcher.stop();
    }
  });

  it('emits setup_failed and marks idle when colocated harness start fails', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      agentMetadata: { harness: 'claude_code', mode: 'colocated' },
    });
    const runtime = new RecordingSandboxRuntime();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-in-sandbox-start-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new FailingStartHarness(),
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_in_sandbox_start_fail_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
      );

      expect(runtime.acquireCount).toBe(1);
      expect(runtime.destroyCount).toBe(1);
      expect(store.appended.filter((event) => event.kind === 'session.error')).toHaveLength(1);
      expect(setupFailedPayload(store)?.phase).toBe('sandbox_setup');
      expect(setupFailedPayload(store)?.error).toMatch(/harness unavailable/);
      expect(registry.states.at(-1)).toEqual({
        sessionId: 'ses_pg_dispatcher',
        status: 'idle',
        sandboxHandleId: null,
      });
    } finally {
      await dispatcher.stop();
    }
  });

  it.each([
    {
      label: 'an Environment custom image',
      agentMetadata: { harness: 'claude_code', mode: 'colocated' },
      environment: {
        id: 'env_custom_image',
        workspace_id: 'ws_pg_dispatcher',
        name: 'custom image',
        image: 'customer.example/agent:latest',
      },
      error: /custom in-sandbox images/,
    },
    {
      label: 'Environment package installers',
      agentMetadata: undefined,
      environment: {
        id: 'env_packages',
        workspace_id: 'ws_pg_dispatcher',
        name: 'packages',
        packages: { npm: ['untrusted-package'] },
      },
      error: /package installers/,
    },
  ])('rejects $label before sandbox acquisition', async ({ agentMetadata, environment, error }) => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({ agentMetadata, environment });
    const runtime = new RecordingSandboxRuntime();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-environment-trust-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: runtime,
      fileStore: new StubFileStore(),
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_environment_trust_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
      );
      expect(runtime.acquireCount).toBe(0);
      expect(setupFailedPayload(store)?.phase).toBe('write_policy');
      expect(setupFailedPayload(store)?.error).toMatch(error);
      expect(registry.states.at(-1)).toEqual({
        sessionId: 'ses_pg_dispatcher',
        status: 'idle',
        sandboxHandleId: null,
      });
    } finally {
      await dispatcher.stop();
    }
  });

  it('fails closed before sandbox writes or mounts when filesystem-root preflight fails', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new RecordingStartInputHarness();
    const fileStore = new StubFileStore(
      new Map([['file_preflight', Buffer.from('must not be materialized')]]),
    );
    const registry = new RecordingRegistry({
      resources: [
        {
          id: 'sesrsc_preflight_file',
          type: 'file',
          file_id: 'file_preflight',
          memory_store_id: null,
          repo_ref: null,
          mount_path: '/mnt/preflight.txt',
          access: 'read_only',
          instructions: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
          mount_strategy: null,
        },
      ],
    });
    const runtime = new RecordingSandboxRuntime({
      filesystemRootPreparationError: new Error('filesystem root contains an alias'),
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-filesystem-preflight-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      fileStore,
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_preflight_failure_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
      );

      expect(runtime.filesystemRootPreparations).toEqual([
        ['/mnt/session/outputs', '/mnt/preflight.txt'],
      ]);
      expect(runtime.sandboxFileWrites).toEqual([]);
      expect(runtime.sandboxFileDeletes).toEqual([]);
      expect(runtime.privilegedCommands).toEqual([]);
      expect(fileStore.openedFileIds).toEqual([]);
      expect(harness.startedInputs).toEqual([]);
      expect(runtime.destroyCount).toBe(1);
      expect(setupFailedPayload(store)).toEqual({
        phase: 'write_policy',
        error: 'filesystem root contains an alias',
      });
      expect(registry.states.at(-1)).toEqual({
        sessionId: 'ses_pg_dispatcher',
        status: 'idle',
        sandboxHandleId: null,
      });
    } finally {
      await dispatcher.stop();
    }
  });

  it('preflights output, file, memory, repository, and Skill roots in one planned set', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const skillStore = new InMemorySkillStore();
    const skill = await putTestSkill(skillStore, 'sklv_planned_roots', 'planned-roots');
    const registry = new RecordingRegistry({
      agent: {
        id: 'agt_fake',
        workspace_id: 'ws_pg_dispatcher',
        name: 'fake agent',
        version: 1,
        model: { provider: 'anthropic', id: 'fake' },
        system: '',
        tools: [{ type: 'agent_toolset' }],
        mcp_servers: [],
        skills: [skill],
      },
      resources: [
        {
          id: 'sesrsc_planned_file',
          type: 'file',
          file_id: 'file_planned',
          memory_store_id: null,
          repo_ref: null,
          mount_path: '/mnt/files/reference.txt',
          access: 'read_only',
          instructions: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
          mount_strategy: null,
        },
        {
          id: 'sesrsc_planned_memory',
          type: 'memory_store',
          file_id: null,
          memory_store_id: 'mems_planned',
          repo_ref: null,
          mount_path: '/mnt/memory/project/',
          access: 'read_write',
          instructions: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
          mount_strategy: null,
        },
        {
          id: 'sesrsc_planned_repo',
          type: 'github_repository',
          file_id: null,
          memory_store_id: null,
          repo_ref: {
            git_credential_id: 'gitcred_planned',
            url: 'https://github.com/acme/planned.git',
          },
          mount_path: '/workspace/repository',
          access: 'read_write',
          instructions: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
          mount_strategy: null,
        },
      ],
      memoryStores: new Map([['mems_planned', { id: 'mems_planned', name: 'planned-memory' }]]),
    });
    const runtime = new RecordingSandboxRuntime({
      filesystemRootPreparationError: new Error('stop after recording planned roots'),
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-filesystem-preflight-roots-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new RecordingStartInputHarness(),
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      skillStore,
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_preflight_roots_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
      );

      expect(runtime.filesystemRootPreparations).toEqual([
        [
          '/mnt/session/outputs',
          '/mnt/files/reference.txt',
          '/mnt/memory/project/',
          '/workspace/repository',
          '/workspace/skills',
        ],
      ]);
    } finally {
      await dispatcher.stop();
    }
  });

  it.each(['claude_agent_sdk', 'codex_sdk'] as const)(
    '%s: leaves a guardrail-blocked Skill out of materialization entirely',
    async (harnessType) => {
      // `block_skills` is written against the tool that loads a Skill, and this
      // runtime exposes no such tool — Skills are staged as files. The rule
      // compiled, validated and evaluated, and could never fire. The list built
      // here is the enforcement point that does exist: a blocked Skill is never
      // staged, so `/workspace/skills` is not even prepared when it was the only
      // one.
      const source = new FakeEventSource();
      const store = new RecordingStore();
      const skillStore = new InMemorySkillStore();
      const skill = await putTestSkill(skillStore, 'sklv_blocked', 'deploy-prod');
      const open = vi.spyOn(skillStore, 'open');
      const harness = new RecordingStartInputHarness();
      const registry = new RecordingRegistry({
        agent: {
          id: 'agt_fake',
          workspace_id: 'ws_pg_dispatcher',
          name: 'fake agent',
          version: 1,
          model:
            harnessType === 'codex_sdk'
              ? { provider: 'openai', id: 'gpt-5.4' }
              : { provider: 'anthropic', id: 'fake' },
          metadata: { harness: harnessType },
          system: 'Base prompt',
          tools: [{ type: 'agent_toolset' }],
          mcp_servers: [],
          skills: [skill],
        },
        guardrails: [
          {
            id: 'grd_block_skill',
            name: 'No deploy skills',
            tier: 'workspace',
            phases: ['tool_call'],
            rule: { kind: 'builtin', builtin: 'block_skills', params: { blocked: ['deploy-*'] } },
            stateful: false,
          },
        ],
      });
      const runtime = new RecordingSandboxRuntime();
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'pg-dispatcher-blocked-skill-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory: () => harness,
        registry: registry as unknown as RegistryClient,
        fileStore: new StubFileStore(),
        skillStore,
        sandboxRuntime: runtime,
      });

      await dispatcher.start();
      try {
        await source.emit(
          userMessage('evt_user_blocked_skill_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
        );
        expect(setupFailedPayload(store)).toBeNull();
        expect(harness.startedInputs).toHaveLength(1);
        expect(harness.startedInputs[0]!.agentSnapshot.system).toBe('Base prompt');
        expect(runtime.filesystemRootPreparations.flat()).not.toContain('/workspace/skills');
        expect(
          runtime.sandboxFileWrites.some((path) => path.startsWith('/workspace/skills/')),
        ).toBe(false);
        expect(open).not.toHaveBeenCalled();
      } finally {
        await dispatcher.stop();
      }
    },
  );

  it('fails colocated setup when the harness image cannot start bubblewrap', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      agentMetadata: { harness: 'claude_code', mode: 'colocated' },
    });
    const runtime = new RecordingSandboxRuntime({
      runResult: { exit_code: 127, stderr: 'bwrap: not found' },
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-in-sandbox-write-policy-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_in_sandbox_policy_fail_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
      );

      expect(runtime.destroyCount).toBe(1);
      expect(store.appended.some((event) => event.kind === 'agent.message')).toBe(false);
      expect(setupFailedPayload(store)?.phase).toBe('write_policy');
      expect(setupFailedPayload(store)?.error).toContain('bwrap: not found');
    } finally {
      await dispatcher.stop();
    }
  });

  it('preserves the separate-mode sandbox skip when fileStore is not configured', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      agentMetadata: { harness: 'claude_agent_sdk', mode: 'separate' },
    });
    const runtime = new RecordingSandboxRuntime();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-separate-no-file-store-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_separate_no_file_store_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    await waitFor(() => store.appended.some((event) => event.kind === 'agent.message'));
    expect(runtime.acquireCount).toBe(0);

    await dispatcher.stop();
  });

  it('handles emitted sandbox setup failures without retrying the user event', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime({ acquireError: new Error('sandbox offline') });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-sandbox-setup-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_sandbox_setup_fail_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    expect(store.appended.filter((event) => event.kind === 'session.error')).toHaveLength(1);
    expect(setupFailedPayload(store)?.phase).toBe('sandbox_setup');
    expect(setupFailedPayload(store)?.error).toMatch(/sandbox offline/);
    expect(registry.states.at(-1)).toEqual({
      sessionId: 'ses_pg_dispatcher',
      status: 'idle',
      sandboxHandleId: null,
    });

    await dispatcher.stop();
  });

  it('sanitizes setup failure errors before publishing them', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime({
      acquireError: new Error(
        'git clone https://x-access-token:ghp_secretToken123@github.com/acme/private.git failed with token=abc123',
      ),
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-sanitized-setup-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_sanitized_setup_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    const payload = setupFailedPayload(store);
    expect(payload?.error).toContain('https://x-access-token:***@github.com/acme/private.git');
    expect(payload?.error).toContain('token=***');
    expect(payload?.error).not.toContain('ghp_secretToken123');
    expect(payload?.error).not.toContain('abc123');

    await dispatcher.stop();
  });

  it('marks turns idle while keeping warm sandboxes until idle timeout', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-idle-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
      sessionIdleTimeoutMs: 250,
    });

    await dispatcher.start();
    expect(runtime.acquireCount).toBe(0);

    await source.emit(userMessage('evt_user_idle_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'));

    await waitFor(() => store.appended.some((event) => event.kind === 'agent.message'));
    expect(runtime.acquireCount).toBe(1);
    expect(registry.states[0]).toEqual({
      sessionId: 'ses_pg_dispatcher',
      status: 'running',
      sandboxHandleId: 'sbx_local_1',
    });
    await waitFor(() =>
      registry.states.some(
        (state) => state.status === 'idle' && state.sandboxHandleId === 'sbx_local_1',
      ),
    );
    expect(runtime.destroyCount).toBe(0);

    await source.emit(userMessage('evt_user_idle_2', 'ws_pg_dispatcher', 'ses_pg_dispatcher'));
    await waitFor(
      () =>
        registry.states.filter(
          (state) => state.status === 'running' && state.sandboxHandleId === 'sbx_local_1',
        ).length === 2,
    );
    expect(runtime.acquireCount).toBe(1);

    await waitFor(() => runtime.destroyCount === 1);
    expect(registry.states.at(-1)).toEqual({
      sessionId: 'ses_pg_dispatcher',
      status: 'idle',
      sandboxHandleId: null,
    });

    await source.emit(userMessage('evt_user_idle_3', 'ws_pg_dispatcher', 'ses_pg_dispatcher'));
    await waitFor(() => runtime.acquireCount === 2);
    expect(
      registry.states.some(
        (state) => state.status === 'running' && state.sandboxHandleId === 'sbx_local_2',
      ),
    ).toBe(true);

    await dispatcher.stop();
  });

  it('clears registry sandbox state when shutting down warm runners', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-shutdown-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
      sessionIdleTimeoutMs: 10_000,
    });

    await dispatcher.start();
    await source.emit(userMessage('evt_user_shutdown_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'));

    await waitFor(() =>
      registry.states.some(
        (state) => state.status === 'idle' && state.sandboxHandleId === 'sbx_local_1',
      ),
    );
    expect(runtime.destroyCount).toBe(0);

    await dispatcher.stop();
    expect(runtime.destroyCount).toBe(1);
    expect(registry.states.at(-1)).toEqual({
      sessionId: 'ses_pg_dispatcher',
      status: 'idle',
      sandboxHandleId: null,
    });
  });

  it('rejects an in-flight user event on shutdown without completing its turn', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new BlockingSubmitHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-interrupted-turn-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      shutdownGraceMs: 20,
    });
    const event = userMessage('evt_interrupted_turn', 'ws_pg_dispatcher', 'ses_interrupted_turn');

    await dispatcher.start();
    const delivery = source.emit(event);
    await waitFor(() => harness.submitStarted);

    await dispatcher.stop();
    harness.releaseSubmit();

    await expect(delivery).rejects.toBeInstanceOf(RetryableSessionEventError);
    expect(
      store.appended.some((candidate) => candidate.kind === 'session.user_event_completed'),
    ).toBe(false);
    expect(completedUserEventCacheSize(dispatcher)).toBe(0);
  });

  it('retires a runner whose spawn crosses shutdown without restoring runner state', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new StopRecordingHarness();
    const registry = new BlockingRunningStateRegistry();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-spawn-shutdown-fence-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      shutdownGraceMs: 20,
    });

    await dispatcher.start();
    const delivery = source.emit(
      userMessage('evt_spawn_shutdown', 'ws_pg_dispatcher', 'ses_spawn_shutdown'),
    );
    await waitFor(() => registry.runningUpdateStarted);

    await dispatcher.stop();
    registry.releaseRunningUpdate();

    await expect(delivery).rejects.toBeInstanceOf(RetryableSessionEventError);
    await waitFor(() => harness.stopReasons.includes('replica.shutting_down'));
    expect(activeRunnerCount(dispatcher)).toBe(0);
    expect(runnerSessionConfigKeyCount(dispatcher)).toBe(0);
    expect(completedUserEventCacheSize(dispatcher)).toBe(0);
  });

  it('uses one deadline for blocked source stop and parallel runner cleanup', async () => {
    const source = new BlockingStopEventSource();
    const store = new RecordingStore();
    const harness = new BlockingStopHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-shutdown-deadline-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      shutdownGraceMs: 20,
    });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);

    try {
      await dispatcher.start();
      await source.emit(
        userMessage('evt_shutdown_deadline', 'ws_pg_dispatcher', 'ses_shutdown_deadline'),
      );

      const startedAt = Date.now();
      await dispatcher.stop();

      expect(Date.now() - startedAt).toBeLessThan(500);
      expect(source.stopStarted).toBe(true);
      expect(harness.stopStarted).toBe(true);
      expect(errorSpy).toHaveBeenCalledWith(
        'dispatcher: event source shutdown grace exceeded; continuing cleanup',
      );
      expect(errorSpy).toHaveBeenCalledWith(
        'dispatcher: runner shutdown grace exceeded; continuing cleanup',
      );
    } finally {
      source.releaseStop();
      harness.releaseStop();
      errorSpy.mockRestore();
    }
  });

  it('does not restore terminal completion or latency state after a late append settles', async () => {
    const source = new FakeEventSource();
    const store = new BlockingTerminalAppendStore();
    const harness = new TerminalEchoHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-late-terminal-append-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      shutdownGraceMs: 20,
    });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);

    try {
      await dispatcher.start();
      await source.emit(userMessage('evt_late_terminal', 'ws_pg_dispatcher', 'ses_late_terminal'));
      await waitFor(() => store.terminalAppendStarted);

      await dispatcher.stop();
      store.releaseTerminalAppend();
      await waitFor(() =>
        store.appended.some((candidate) => candidate.kind === 'session.user_event_completed'),
      );
      await Promise.resolve();

      expect(completedUserEventCacheSize(dispatcher)).toBe(0);
      expect(activeTurnUserEventCount(dispatcher)).toBe(0);
      expect(statusRunningLatencyEntryCount(dispatcher)).toBe(0);
    } finally {
      store.releaseTerminalAppend();
      errorSpy.mockRestore();
    }
  });

  it('does not idle-timeout warm runners while required actions are pending', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime();
    const harness = new RequiredActionHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-required-action-idle-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
      sessionIdleTimeoutMs: 25,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_required_action_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    await waitFor(() => store.appended.some((event) => event.kind === 'session.status_idle'));
    await new Promise((resolve) => setTimeout(resolve, 75));
    expect(runtime.destroyCount).toBe(0);

    await source.emit({
      id: 'evt_tool_confirmation_1',
      workspaceId: 'ws_pg_dispatcher',
      sessionId: 'ses_pg_dispatcher',
      subpath: '',
      seq: 2,
      producedAt: new Date().toISOString(),
      producedBy: 'client',
      kind: 'user.tool_confirmation',
      payload: new TextEncoder().encode(
        JSON.stringify({ tool_use_id: 'evt_required_action', result: 'allow' }),
      ),
      idempotencyKey: '',
    });

    await waitFor(() => store.appended.some((event) => event.kind === 'agent.message'));
    await waitFor(() => runtime.destroyCount === 1);

    await dispatcher.stop();
  });

  it('does not idle-timeout a runner with unacknowledged guardrail usage', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime();
    const harness = new PendingUsageHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-guardrail-usage-idle-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
      sessionIdleTimeoutMs: 25,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_usage_lock_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );
    await new Promise((resolve) => setTimeout(resolve, 75));
    expect(runtime.destroyCount).toBe(0);

    harness.releaseUsageLock();
    await source.emit(
      userMessage('evt_user_usage_lock_2', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );
    await waitFor(() => runtime.destroyCount === 1);

    await dispatcher.stop();
  });

  it('replays deferred user messages after runner recreation', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime();
    const firstHarness = new ReplayDeferredMessageHarness();
    const firstUserMessage = userMessage(
      'evt_deferred_replay_1',
      'ws_pg_dispatcher',
      'ses_pg_dispatcher',
      'first',
      1,
    );
    const deferredUserMessage = {
      ...userMessage('evt_deferred_replay_2', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'second', 2),
      subpath: 'subagents/worker/0',
    };
    const confirmation: Event = {
      id: 'evt_deferred_replay_confirmation',
      workspaceId: 'ws_pg_dispatcher',
      sessionId: 'ses_pg_dispatcher',
      subpath: '',
      seq: 3,
      producedAt: new Date().toISOString(),
      producedBy: 'client',
      kind: 'user.tool_confirmation',
      payload: new TextEncoder().encode(
        JSON.stringify({ tool_use_id: 'evt_required_action', result: 'allow' }),
      ),
      idempotencyKey: '',
    };
    store.seed([firstUserMessage, deferredUserMessage, confirmation]);
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-deferred-replay-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => firstHarness,
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    await source.emit(firstUserMessage);
    await waitFor(() => store.appended.some((event) => event.kind === 'session.status_idle'));
    await source.emit(deferredUserMessage);

    expect(store.appended.some((event) => event.kind === 'session.deferred_user_message')).toBe(
      true,
    );
    // The dispatcher asks the harness to validate/select the message before it
    // persists a deferred queue item. The harness, not the dispatcher, owns
    // the required-action boundary.
    expect(firstHarness.submitted.filter((event) => event.kind === 'user.message')).toHaveLength(2);
    await dispatcher.stop();

    const restartSource = new FakeEventSource();
    const secondHarness = new ReplayDeferredMessageHarness({ firstUserMessage: false });
    const restarted = new Dispatcher({
      eventSource: restartSource,
      groupId: 'pg-dispatcher-deferred-replay-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => secondHarness,
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
    });

    await restarted.start();
    await restartSource.emit(confirmation);

    expect(secondHarness.submitted.map((event) => event.kind)).toEqual([
      'user.tool_confirmation',
      'user.message',
    ]);
    expect(secondHarness.submitted[1]?.payload).toEqual({
      content: [{ type: 'text', text: 'second' }],
    });
    expect(
      store.appended.some((event) => event.kind === 'session.deferred_user_message_submitted'),
    ).toBe(true);
    const acceptedIndex = store.appended.findIndex(
      (event) =>
        event.kind === 'session.user_event_processed' &&
        eventPayload(event).user_event_id === 'evt_deferred_replay_2',
    );
    const submittedIndex = store.appended.findIndex(
      (event) =>
        event.kind === 'session.deferred_user_message_submitted' &&
        eventPayload(event).user_event_id === 'evt_deferred_replay_2',
    );
    expect(acceptedIndex).toBeGreaterThanOrEqual(0);
    expect(acceptedIndex).toBeLessThan(submittedIndex);

    await restarted.stop();
  });

  it('persists user messages when the harness defers at a required-action boundary', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime();
    const harness = new DeferredSubmitHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-deferred-submit-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_deferred_submit_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'queued'),
    );

    expect(harness.submitted).toHaveLength(1);
    expect(store.appended).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: 'session.deferred_user_message',
          producedBy: 'harness',
        }),
      ]),
    );
    expect(registry.states.at(-1)).toMatchObject({
      sessionId: 'ses_pg_dispatcher',
      status: 'idle',
    });

    await dispatcher.stop();
  });

  it('does not expose a deferred message in memory before its durable record succeeds', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore({ failOnKind: 'session.deferred_user_message' });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-deferred-persistence-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new DeferredSubmitHarness(),
    });

    await dispatcher.start();
    await expect(
      source.emit(
        userMessage(
          'evt_deferred_persistence_failure',
          'ws_pg_dispatcher',
          'ses_pg_dispatcher',
          'queued',
        ),
      ),
    ).rejects.toThrow('failed to persist deferred user.message');

    const payloads = (
      dispatcher as unknown as { deferredUserMessagePayloads: Map<string, Map<string, unknown>> }
    ).deferredUserMessagePayloads;
    expect(payloads.size).toBe(0);
    await dispatcher.stop();
  });

  it('retries deferred acceptance persistence without another client event', async () => {
    const source = new FakeEventSource();
    const deferredEventId = 'evt_deferred_acceptance_retry_2';
    const store = new FailingAcceptanceStore(deferredEventId, 1);
    const harness = new ReplayDeferredMessageHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-deferred-acceptance-retry-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      deferredDrainRetryDelayMs: 1,
    });

    await dispatcher.start();
    await source.emit(
      userMessage(
        'evt_deferred_acceptance_retry_1',
        'ws_pg_dispatcher',
        'ses_pg_dispatcher',
        'first',
        1,
      ),
    );
    await source.emit(
      userMessage(deferredEventId, 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'second', 2),
    );

    await expect(
      source.emit({
        id: 'evt_deferred_acceptance_retry_confirmation',
        workspaceId: 'ws_pg_dispatcher',
        sessionId: 'ses_pg_dispatcher',
        subpath: '',
        seq: 3,
        producedAt: new Date().toISOString(),
        producedBy: 'client',
        kind: 'user.tool_confirmation',
        payload: new TextEncoder().encode(
          JSON.stringify({ tool_use_id: 'evt_required_action', result: 'allow' }),
        ),
        idempotencyKey: '',
      }),
    ).resolves.toBeUndefined();

    expect(store.acceptanceAttempts).toBe(2);
    expect(
      store.appended.some(
        (event) =>
          event.kind === 'session.user_event_processed' &&
          eventPayload(event).user_event_id === deferredEventId,
      ),
    ).toBe(true);
    expect(
      store.appended.some(
        (event) =>
          event.kind === 'session.deferred_user_message_submitted' &&
          eventPayload(event).user_event_id === deferredEventId,
      ),
    ).toBe(true);
    expect(harness.submitted.filter((event) => event.kind === 'user.message')).toHaveLength(4);

    await dispatcher.stop();
  });

  it('bounds deferred acceptance retries and drains the old message before a later one', async () => {
    const source = new FakeEventSource();
    const deferredEventId = 'evt_deferred_acceptance_exhausted_2';
    const store = new FailingAcceptanceStore(deferredEventId, 10);
    const harness = new ReplayDeferredMessageHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-deferred-acceptance-exhausted-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      deferredDrainRetryDelayMs: 1,
      deferredDrainMaxAcceptanceRetries: 2,
    });

    await dispatcher.start();
    await source.emit(
      userMessage(
        'evt_deferred_acceptance_exhausted_1',
        'ws_pg_dispatcher',
        'ses_pg_dispatcher',
        'first',
        1,
      ),
    );
    await source.emit(
      userMessage(deferredEventId, 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'second', 2),
    );

    await expect(
      source.emit({
        id: 'evt_deferred_acceptance_exhausted_confirmation',
        workspaceId: 'ws_pg_dispatcher',
        sessionId: 'ses_pg_dispatcher',
        subpath: '',
        seq: 3,
        producedAt: new Date().toISOString(),
        producedBy: 'client',
        kind: 'user.tool_confirmation',
        payload: new TextEncoder().encode(
          JSON.stringify({ tool_use_id: 'evt_required_action', result: 'allow' }),
        ),
        idempotencyKey: '',
      }),
    ).resolves.toBeUndefined();

    expect(store.acceptanceAttempts).toBe(3);
    expect(
      store.appended.some(
        (event) =>
          event.kind === 'session.deferred_user_message_submitted' &&
          eventPayload(event).user_event_id === deferredEventId,
      ),
    ).toBe(false);
    expect(
      store.appended.some(
        (event) =>
          event.kind === 'session.deferred_user_message' &&
          eventPayload(event).user_event_id === deferredEventId,
      ),
    ).toBe(true);
    expect(harness.stopReasons).toHaveLength(0);

    store.recover();
    const laterEventId = 'evt_deferred_acceptance_exhausted_later';
    await source.emit(
      userMessage(laterEventId, 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'later', 4),
    );

    expect(store.acceptanceAttempts).toBe(4);
    const oldSubmittedIndex = store.appended.findIndex(
      (event) =>
        event.kind === 'session.deferred_user_message_submitted' &&
        eventPayload(event).user_event_id === deferredEventId,
    );
    const laterAcceptedIndex = store.appended.findIndex(
      (event) =>
        event.kind === 'session.user_event_processed' &&
        eventPayload(event).user_event_id === laterEventId,
    );
    expect(oldSubmittedIndex).toBeGreaterThanOrEqual(0);
    expect(oldSubmittedIndex).toBeLessThan(laterAcceptedIndex);

    await dispatcher.stop();
  });

  it('keeps a later user message retryable when the deferred queue read fails', async () => {
    const source = new FakeEventSource();
    const store = new FailingReadStore();
    const laterEventId = 'evt_deferred_read_failure_later';
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-deferred-read-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
    });

    await dispatcher.start();
    await expect(
      source.emit(userMessage(laterEventId, 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'later', 3)),
    ).rejects.toBeInstanceOf(RetryableSessionEventError);

    expect(
      store.appended.some(
        (event) =>
          event.kind === 'session.user_event_processed' &&
          eventPayload(event).user_event_id === laterEventId,
      ),
    ).toBe(false);
    await dispatcher.stop();
  });

  it('keeps a later user message retryable when settling an older deferred message fails', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore({ failOnKind: 'session.deferred_user_message_submitted' });
    const deferredEventId = 'evt_deferred_settlement_failure_old';
    const laterEventId = 'evt_deferred_settlement_failure_later';
    store.seed([
      userMessage(deferredEventId, 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'old', 1),
      {
        id: 'evt_deferred_settlement_failure_marker',
        workspaceId: 'ws_pg_dispatcher',
        sessionId: 'ses_pg_dispatcher',
        subpath: '',
        seq: 2,
        producedAt: new Date().toISOString(),
        producedBy: 'harness',
        kind: 'session.deferred_user_message',
        payload: new TextEncoder().encode(JSON.stringify({ user_event_id: deferredEventId })),
        idempotencyKey: '',
      },
    ]);
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-deferred-settlement-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
    });

    await dispatcher.start();
    await expect(
      source.emit(userMessage(laterEventId, 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'later', 3)),
    ).rejects.toBeInstanceOf(RetryableSessionEventError);

    expect(
      store.appended.some(
        (event) =>
          event.kind === 'session.user_event_processed' &&
          eventPayload(event).user_event_id === laterEventId,
      ),
    ).toBe(false);
    await dispatcher.stop();
  });

  it('interrupts a deferred acceptance retry delay during shutdown', async () => {
    const source = new FakeEventSource();
    const deferredEventId = 'evt_deferred_acceptance_shutdown_2';
    const store = new FailingAcceptanceStore(deferredEventId, 10);
    const harness = new ReplayDeferredMessageHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-deferred-acceptance-shutdown-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      deferredDrainRetryDelayMs: 60_000,
      deferredDrainMaxAcceptanceRetries: 10,
    });

    await dispatcher.start();
    await source.emit(
      userMessage(
        'evt_deferred_acceptance_shutdown_1',
        'ws_pg_dispatcher',
        'ses_pg_dispatcher',
        'first',
        1,
      ),
    );
    await source.emit(
      userMessage(deferredEventId, 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'second', 2),
    );

    const confirmation = source.emit({
      id: 'evt_deferred_acceptance_shutdown_confirmation',
      workspaceId: 'ws_pg_dispatcher',
      sessionId: 'ses_pg_dispatcher',
      subpath: '',
      seq: 3,
      producedAt: new Date().toISOString(),
      producedBy: 'client',
      kind: 'user.tool_confirmation',
      payload: new TextEncoder().encode(
        JSON.stringify({ tool_use_id: 'evt_required_action', result: 'allow' }),
      ),
      idempotencyKey: '',
    });
    await vi.waitFor(() => expect(store.acceptanceAttempts).toBe(1));

    await expect(dispatcher.stop()).resolves.toBeUndefined();
    await expect(confirmation).rejects.toBeInstanceOf(RetryableSessionEventError);
  });

  it('handles deferred message drain failures after applying control events', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime();
    const harness = new FailingDeferredDrainHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-drain-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_deferred_drain_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'first', 1),
    );
    await waitFor(() => store.appended.some((event) => event.kind === 'session.status_idle'));
    await source.emit(
      userMessage('evt_deferred_drain_2', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'second', 2),
    );

    await expect(
      source.emit({
        id: 'evt_deferred_drain_confirmation',
        workspaceId: 'ws_pg_dispatcher',
        sessionId: 'ses_pg_dispatcher',
        subpath: '',
        seq: 3,
        producedAt: new Date().toISOString(),
        producedBy: 'client',
        kind: 'user.tool_confirmation',
        payload: new TextEncoder().encode(
          JSON.stringify({ tool_use_id: 'evt_required_action', result: 'allow' }),
        ),
        idempotencyKey: '',
      }),
    ).resolves.toBeUndefined();

    expect(
      store.appended.some((event) => event.kind === 'session.deferred_user_message_submitted'),
    ).toBe(false);
    expect(registry.states.at(-1)).toMatchObject({
      sessionId: 'ses_pg_dispatcher',
      status: 'idle',
      sandboxHandleId: null,
    });

    await dispatcher.stop();
  });

  it('does not rescan the full transcript for every deferred message drain', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime();
    const harness = new ReplayDeferredMessageHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-deferred-drain-cache-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_deferred_cache_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'first', 1),
    );
    await waitFor(() => store.appended.some((event) => event.kind === 'session.status_idle'));
    await source.emit(
      userMessage('evt_deferred_cache_2', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'second', 2),
    );
    await source.emit(
      userMessage('evt_deferred_cache_3', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'third', 3),
    );
    store.readCalls = 0;

    await source.emit({
      id: 'evt_deferred_cache_confirmation',
      workspaceId: 'ws_pg_dispatcher',
      sessionId: 'ses_pg_dispatcher',
      subpath: '',
      seq: 4,
      producedAt: new Date().toISOString(),
      producedBy: 'client',
      kind: 'user.tool_confirmation',
      payload: new TextEncoder().encode(
        JSON.stringify({ tool_use_id: 'evt_required_action', result: 'allow' }),
      ),
      idempotencyKey: '',
    });

    await waitFor(() => harness.submitted.length >= 4);
    // The two deferred messages are each seen once for validation/selection
    // and once again when their queued turn is drained.
    expect(harness.submitted.filter((event) => event.kind === 'user.message')).toHaveLength(5);
    expect(store.readCalls).toBeLessThanOrEqual(1);

    await dispatcher.stop();
  });

  it('clears deferred payload caches when runners are removed', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime();
    const harness = new ReplayDeferredMessageHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-deferred-cache-cleanup-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
      sessionIdleTimeoutMs: 5,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_deferred_cleanup_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'first', 1),
    );
    await waitFor(() => store.appended.some((event) => event.kind === 'session.status_idle'));
    await source.emit(
      userMessage('evt_deferred_cleanup_2', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'second', 2),
    );
    expect(
      (
        dispatcher as unknown as {
          deferredUserMessagePayloads: Map<string, Map<string, unknown>>;
        }
      ).deferredUserMessagePayloads.size,
    ).toBe(1);

    await source.emit({
      id: 'evt_deferred_cleanup_confirmation',
      workspaceId: 'ws_pg_dispatcher',
      sessionId: 'ses_pg_dispatcher',
      subpath: '',
      seq: 3,
      producedAt: new Date().toISOString(),
      producedBy: 'client',
      kind: 'user.tool_confirmation',
      payload: new TextEncoder().encode(
        JSON.stringify({ tool_use_id: 'evt_required_action', result: 'allow' }),
      ),
      idempotencyKey: '',
    });

    await waitFor(() => runtime.destroyCount === 1);
    expect(
      (
        dispatcher as unknown as {
          deferredUserMessagePayloads: Map<string, Map<string, unknown>>;
        }
      ).deferredUserMessagePayloads.size,
    ).toBe(0);

    await dispatcher.stop();
  });

  it('acknowledges Pulsar control events the harness cannot apply', async () => {
    // Regression test: the mismatch an UnappliedUserEventError signals is permanent
    // (redelivering the identical event can never make it apply), so the pulsar path
    // must not rethrow. Rethrowing used to trigger PulsarSessionEventSource's
    // negativeAcknowledge/redelivery retry loop, which grew the transcript unboundedly
    // and ultimately crashed the native pulsar client. This must behave the same as the
    // Kafka path below: the handler resolves, and a session.error is emitted instead.
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime();
    const harness = new UnappliedControlEventHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-unapplied-control-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
      sessionIdleTimeoutMs: 25,
    });

    await dispatcher.start();
    await expect(
      source.emit({
        id: 'evt_unknown_confirmation_1',
        workspaceId: 'ws_pg_dispatcher',
        sessionId: 'ses_pg_dispatcher',
        subpath: '',
        seq: 1,
        producedAt: new Date().toISOString(),
        producedBy: 'client',
        kind: 'user.tool_confirmation',
        payload: new TextEncoder().encode(
          JSON.stringify({ tool_use_id: 'evt_missing', result: 'allow' }),
        ),
        idempotencyKey: '',
      }),
    ).resolves.toBeUndefined();

    expect(harness.submitted).toHaveLength(1);
    expect(store.appended).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: 'session.error',
          producedBy: 'harness',
        }),
      ]),
    );
    expect(registry.states.at(-1)).toMatchObject({
      sessionId: 'ses_pg_dispatcher',
      status: 'idle',
    });
    await waitFor(() => runtime.destroyCount === 1);

    await dispatcher.stop();
  });

  it('surfaces a request guardrail refusal as policy_denied without tearing down the runner', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new PolicyDenyHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-policy-denied-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      sessionIdleTimeoutMs: 60_000,
    });

    await dispatcher.start();
    await expect(
      source.emit(userMessage('evt_policy_denied', 'ws_pg_dispatcher', 'ses_pg_dispatcher')),
    ).resolves.toBeUndefined();

    const errorEvent = store.appended.find((event) => event.kind === 'session.error');
    expect(errorEvent).toBeDefined();
    expect(JSON.parse(Buffer.from(errorEvent!.payload).toString('utf8'))).toMatchObject({
      error: { type: 'policy_denied', message: 'Sensitive request blocked.' },
      retry_status: { will_retry: false },
      reasons: ['Sensitive request blocked.'],
    });
    expect(harness.stopReasons).toEqual([]);
    expect(
      (dispatcher as unknown as { runners: Map<string, unknown> }).runners.has(
        'ws_pg_dispatcher/ses_pg_dispatcher',
      ),
    ).toBe(true);

    await dispatcher.stop();
  });

  it('acknowledges Kafka control events the harness cannot apply', async () => {
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime();
    const harness = new UnappliedControlEventHarness();
    const dispatcher = new Dispatcher({
      groupId: 'kafka-dispatcher-unapplied-control-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
    });

    await expect(
      (
        dispatcher as unknown as {
          onMessage(payload: {
            topic: string;
            message: {
              headers: Record<string, Buffer>;
              value: Buffer;
              offset: string;
            };
          }): Promise<void>;
        }
      ).onMessage({
        topic: 'orca.ws_pg_dispatcher.sessions.ses_pg_dispatcher.events',
        message: {
          headers: {
            workspace_id: Buffer.from('ws_pg_dispatcher'),
            session_id: Buffer.from('ses_pg_dispatcher'),
            produced_by: Buffer.from('client'),
            kind: Buffer.from('user.tool_confirmation'),
            id: Buffer.from('evt_unknown_confirmation_kafka'),
          },
          value: Buffer.from(JSON.stringify({ tool_use_id: 'evt_missing', result: 'allow' })),
          offset: '42',
        },
      }),
    ).resolves.toBeUndefined();

    expect(harness.submitted).toHaveLength(1);
    expect(store.appended).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: 'session.error',
          producedBy: 'harness',
        }),
      ]),
    );
    expect(registry.states.at(-1)).toMatchObject({
      sessionId: 'ses_pg_dispatcher',
      status: 'idle',
    });

    await dispatcher.stop();
  });

  it('drops direct Kafka events with missing or forged route headers', async () => {
    const store = new RecordingStore();
    const harness = new EchoHarness();
    const dispatcher = new Dispatcher({
      groupId: 'kafka-dispatcher-route-authority-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
    });
    const onMessage = (
      dispatcher as unknown as {
        onMessage(payload: {
          topic: string;
          message: {
            headers: Record<string, Buffer>;
            value: Buffer;
            offset: string;
          };
        }): Promise<void>;
      }
    ).onMessage;
    const topic = 'orca.ws_pg_dispatcher.sessions.ses_pg_dispatcher.events';
    const baseHeaders = {
      produced_by: Buffer.from('client'),
      kind: Buffer.from('user.message'),
      id: Buffer.from('evt_route_header'),
    };
    const value = Buffer.from(JSON.stringify({ content: [{ type: 'text', text: 'ignored' }] }));

    await onMessage({ topic, message: { headers: baseHeaders, value, offset: '1' } });
    await onMessage({
      topic,
      message: {
        headers: {
          ...baseHeaders,
          workspace_id: Buffer.from('ws_forged'),
          session_id: Buffer.from('ses_pg_dispatcher'),
        },
        value,
        offset: '2',
      },
    });
    await onMessage({
      topic,
      message: {
        headers: {
          ...baseHeaders,
          workspace_id: Buffer.from('ws_pg_dispatcher'),
          session_id: Buffer.from('ses_forged'),
        },
        value,
        offset: '3',
      },
    });

    expect(store.appended).toEqual([]);
    await dispatcher.stop();
  });

  it('spawns a fresh runner when events arrive during idle teardown', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime();
    const firstHarness = new BlockingStopHarness();
    let starts = 0;
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-idle-race-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => {
        starts += 1;
        return starts === 1 ? firstHarness : new EchoHarness();
      },
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
      sessionIdleTimeoutMs: 20,
    });

    await dispatcher.start();
    await source.emit(userMessage('evt_user_race_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'));
    await waitFor(() =>
      registry.states.some(
        (state) => state.status === 'idle' && state.sandboxHandleId === 'sbx_local_1',
      ),
    );

    await waitFor(() => firstHarness.stopStarted);
    await source.emit(userMessage('evt_user_race_2', 'ws_pg_dispatcher', 'ses_pg_dispatcher'));
    expect(runtime.acquireCount).toBe(2);
    expect(
      registry.states.some(
        (state) => state.status === 'running' && state.sandboxHandleId === 'sbx_local_2',
      ),
    ).toBe(true);

    firstHarness.releaseStop();
    await waitFor(() => runtime.destroyCount >= 1);

    await dispatcher.stop();
  });

  it('does not emit resource_mounted events when a later file mount fails setup', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      resources: [
        {
          id: 'sesrsc_file_ok',
          type: 'file',
          file_id: 'file_ok',
          memory_store_id: null,
          repo_ref: null,
          mount_path: '/mnt/ok.txt',
          access: 'read_only',
          instructions: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
          mount_strategy: null,
        },
        {
          id: 'sesrsc_file_missing',
          type: 'file',
          file_id: 'file_missing',
          memory_store_id: null,
          repo_ref: null,
          mount_path: '/mnt/missing.txt',
          access: 'read_only',
          instructions: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
          mount_strategy: null,
        },
      ],
    });
    const runtime = new RecordingSandboxRuntime();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-delayed-resource-mounted-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(new Map([['file_ok', Buffer.from('ok')]])),
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_delayed_resource_mounted_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    expect(store.appended.some((event) => event.kind === 'session.resource_mounted')).toBe(false);
    expect(setupFailedPayload(store)?.phase).toBe('resource_mount');
    expect(runtime.destroyCount).toBe(1);

    await dispatcher.stop();
  });

  it('always tarball-prefetches files even when the prepared resource requests the removed s3_fuse strategy', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      resources: [
        {
          id: 'sesrsc_file_forced_prefetch',
          type: 'file',
          file_id: 'file_prefetch',
          memory_store_id: null,
          repo_ref: null,
          mount_path: '/mnt/input.txt',
          access: 'read_only',
          instructions: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
          // Deliberately simulate contract drift from a compromised/older
          // internal Registry response. The dispatcher must still avoid a
          // sandbox-visible file S3 grant.
          mount_strategy: 's3_fuse' as never,
        },
      ],
    });
    const runtime = new RecordingSandboxRuntime({ supportsFuse: true });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-file-prefetch-security-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(new Map([['file_prefetch', Buffer.from('trusted copy')]])),
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_file_prefetch_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    const mounted = store.appended.find((event) => event.kind === 'session.resource_mounted');
    expect(mounted).toBeDefined();
    expect(JSON.parse(Buffer.from(mounted!.payload).toString('utf8'))).toMatchObject({
      resource_id: 'sesrsc_file_forced_prefetch',
      mount_strategy: 'tarball_prefetch',
    });
    expect(runtime.privilegedCommands).toEqual([]);

    await dispatcher.stop();
  });

  it('records internal usage events without appending them to the transcript', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const harness = new UsageHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-usage-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
    });

    await dispatcher.start();
    await source.emit(userMessage('evt_user_usage_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'));

    await waitFor(() => registry.usages.length === 1);
    expect(registry.usages[0]).toEqual({
      workspaceId: 'ws_pg_dispatcher',
      sessionId: 'ses_pg_dispatcher',
      model: 'claude-opus-5',
      provider: 'anthropic',
      usage: {
        cache_creation: {
          ephemeral_1h_input_tokens: 2,
          ephemeral_5m_input_tokens: 3,
        },
        cache_read_input_tokens: 5,
        input_tokens: 7,
        output_tokens: 11,
      },
      // Registry dedups a replayed usage delta by the event that carried it,
      // so the id rides along with the report.
      usageEventId: expect.stringMatching(/^evt_/),
    });
    expect(store.appended.some((event) => event.kind === 'agent.usage')).toBe(false);
    expect(store.appended.some((event) => event.kind === 'agent.message')).toBe(true);
    expect(harness.guardrailUsageStates).toEqual([{ total_tokens: 28, session_cost_usd: 0.25 }]);

    await dispatcher.stop();
  });

  it('serializes persistent guardrail writes and bounds each Registry request', async () => {
    let releaseFirst!: () => void;
    const firstBlocked = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const writes: Array<{
      updates: unknown;
      signal?: AbortSignal;
    }> = [];
    const registry = {
      applyGuardrailStateInternal: vi.fn(
        async (input: { updates: unknown; signal?: AbortSignal }) => {
          writes.push(input);
          if (writes.length === 1) await firstBlocked;
          return 1;
        },
      ),
    };
    const dispatcher = new Dispatcher({
      eventSource: new FakeEventSource(),
      groupId: 'pg-dispatcher-guardrail-state-unit',
      store: new RecordingStore(),
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      registry: registry as unknown as RegistryClient,
    });
    const writer = (
      dispatcher as unknown as {
        guardrailStateWriter(
          workspaceId: string,
          sessionId: string,
        ): (updates: Array<Record<string, unknown>>) => Promise<void>;
      }
    ).guardrailStateWriter('ws_pg_dispatcher', 'ses_pg_dispatcher');

    const first = writer([
      { scope: 'session', key: 'g:grd_calls:count', action: 'increment', value: 1 },
    ]);
    const second = writer([
      { scope: 'session', key: 'g:grd_calls:approved', action: 'set', value: true },
      { scope: 'turn', key: 'ephemeral', action: 'increment', value: 1 },
    ]);
    await waitFor(() => writes.length === 1);
    expect(writes[0]?.signal).toBeInstanceOf(AbortSignal);
    expect(writes[0]?.signal?.aborted).toBe(false);
    releaseFirst();
    await Promise.all([first, second]);

    expect(writes.map((write) => write.updates)).toEqual([
      [{ scope: 'session', key: 'g:grd_calls:count', action: 'increment', value: 1 }],
      [{ scope: 'session', key: 'g:grd_calls:approved', action: 'set', value: true }],
    ]);
  });

  it('records legacy flat cache-creation usage instead of dropping it', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-flat-usage-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new FlatUsageHarness(),
      registry: registry as unknown as RegistryClient,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_flat_usage_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    await waitFor(() => registry.usages.length === 1);
    expect(registry.usages[0]!.usage).toEqual({
      cache_creation: {
        ephemeral_1h_input_tokens: 0,
        ephemeral_5m_input_tokens: 19,
      },
      cache_read_input_tokens: 5,
      input_tokens: 7,
      output_tokens: 11,
    });

    await dispatcher.stop();
  });

  it('emits setup_failed and does not start the runner when output mounting fails', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime({
      supportsFuse: true,
      privilegedResult: { exit_code: 32, stdout: '', stderr: 's3fs failed' },
    });
    const mintInputs: Array<{
      workspaceId: string;
      sessionId: string;
      generationId: string;
      memoryStores: Array<{ storeId: string; access: string }>;
    }> = [];
    const harness = new EchoHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-output-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
      credsMinter: {
        async mint(input) {
          mintInputs.push(input);
          return {
            accessKeyId: 'AKIA_TEST',
            secretAccessKey: 'secret',
            sessionToken: 'token',
            expiresAt: Math.floor(Date.now() / 1000) + 3600,
          };
        },
      },
      s3Bucket: 'orca-files',
      s3Endpoint: 'http://minio:9000',
      s3KeyPrefix: 'outputs/',
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_output_fail_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    expect(store.appended.some((event) => event.kind === 'agent.message')).toBe(false);
    expect(setupFailedPayload(store)?.phase).toBe('output_mount');
    expect(setupFailedPayload(store)?.error).toMatch(/mountSessionOutputs/);
    expect(runtime.destroyCount).toBe(1);
    expect(mintInputs).toHaveLength(1);
    expect(mintInputs[0]).toMatchObject({
      workspaceId: 'ws_pg_dispatcher',
      sessionId: 'ses_pg_dispatcher',
      memoryStores: [],
    });
    expect(mintInputs[0]!.generationId).toMatch(/^run_/);
    expect(runtime.privilegedCommands[0]).toContain(
      `outputs/workspaces/ws_pg_dispatcher/sessions/ses_pg_dispatcher/executions/${mintInputs[0]!.generationId}/outputs/`,
    );
    expect(registry.states.at(-1)).toEqual({
      sessionId: 'ses_pg_dispatcher',
      status: 'idle',
      sandboxHandleId: null,
    });

    await dispatcher.stop();
  });

  it('emits setup_failed when FUSE output capture is configured but creds minting fails', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime({ supportsFuse: true });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-output-creds-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
      credsMinter: {
        async mint() {
          throw new Error('sts unavailable');
        },
      },
      s3Bucket: 'orca-files',
      s3Endpoint: 'http://minio:9000',
      s3KeyPrefix: 'outputs/',
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_output_creds_fail_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    expect(store.appended.some((event) => event.kind === 'agent.message')).toBe(false);
    expect(setupFailedPayload(store)?.phase).toBe('output_mount');
    expect(setupFailedPayload(store)?.error).toMatch(/sts unavailable/);
    expect(runtime.destroyCount).toBe(1);

    await dispatcher.stop();
  });

  it('fails output-enabled setup when the separate runtime cannot enforce write policy', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const runtime = new RecordingSandboxRuntime({ supportsWritePolicy: false });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-write-policy-capability-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
      s3Bucket: 'orca-files',
      s3Endpoint: 'http://minio:9000',
      s3KeyPrefix: 'outputs/',
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_write_policy_cap_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    expect(store.appended.some((event) => event.kind === 'agent.message')).toBe(false);
    expect(setupFailedPayload(store)?.phase).toBe('write_policy');
    expect(setupFailedPayload(store)?.error).toMatch(/does not support sandbox write-policy/);
    expect(runtime.destroyCount).toBe(1);

    await dispatcher.stop();
  });

  it('denies writes to read-only mounts without S3 output capture', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      resources: [
        {
          id: 'sesrsc_readonly_no_output',
          type: 'file',
          file_id: 'file_readonly_no_output',
          memory_store_id: null,
          repo_ref: null,
          mount_path: '/mnt/read-only.txt',
          access: 'read_only',
          instructions: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
          mount_strategy: null,
        },
      ],
    });
    const runtime = new RecordingSandboxRuntime({ supportsWritePolicy: true });
    const harness = new RecordingStartInputHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-no-output-write-policy-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(
        new Map([['file_readonly_no_output', Buffer.from('trusted input')]]),
      ),
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    try {
      await source.emit(
        userMessage('evt_user_no_output_policy_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
      );

      const write = harness.startedInputs[0]?.tools?.find((tool) => tool.name === 'write');
      expect(write).toBeDefined();
      await expect(
        write!.execute({ path: '/mnt/read-only.txt', content: 'tampered' }),
      ).resolves.toMatchObject({ error: expect.stringContaining('write denied') });
    } finally {
      await dispatcher.stop();
    }
  });

  it('emits setup_failed and does not start the runner when memory setup fails', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      agentMetadata: { harness: 'claude_code', mode: 'colocated' },
      resources: [
        {
          id: 'sesrsc_mem_1',
          type: 'memory_store',
          file_id: null,
          memory_store_id: 'mems_missing',
          repo_ref: null,
          mount_path: '/mnt/memory/prefs/',
          access: 'read_write',
          instructions: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
          mount_strategy: null,
        },
      ],
      memoryStores: new Map(),
    });
    const runtime = new RecordingSandboxRuntime();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-memory-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_memory_fail_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    expect(store.appended.some((event) => event.kind === 'agent.message')).toBe(false);
    expect(setupFailedPayload(store)?.phase).toBe('memory_setup');
    expect(setupFailedPayload(store)?.error).toMatch(
      /memory_store mems_missing missing from prepared execution/,
    );
    expect(JSON.parse(runtime.acquiredSpecs[0]!.harnessEnv!['ORCA_SANDBOX_WRITE_POLICY']!)).toEqual(
      {
        writablePaths: [
          { path: '/mnt/memory/prefs', kind: 'memory_store' },
          { path: '/mnt/session/outputs', kind: 'session_output' },
        ],
        readonlyPaths: [],
      },
    );
    expect(runtime.filesystemRootPreparations).toEqual([
      ['/mnt/session/outputs', '/mnt/memory/prefs/'],
    ]);
    expect(runtime.destroyCount).toBe(1);

    await dispatcher.stop();
  });

  it('skips raced-away memory content during InMemory seeding', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      resources: [
        {
          id: 'res_memory_race',
          type: 'memory_store',
          memory_store_id: 'mems_live',
          mount_path: '/mnt/memory/live/',
          access: 'read_write',
        },
      ],
      memoryStores: new Map([['mems_live', { id: 'mems_live', name: 'live' }]]),
      memories: new Map([['mems_live', [memoryRecord('mem_deleted', 'gone.txt', 'mems_live')]]]),
      memoryContents: new Map([['mem_deleted', null]]),
    });
    const runtime = new RecordingSandboxRuntime();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-memory-content-race-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
      memoryWatcherIntervalMs: 0,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_memory_race_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    await waitFor(() => store.appended.some((event) => event.kind === 'agent.message'));
    expect(store.appended.some((event) => event.kind === 'session.error')).toBe(false);
    expect(runtime.destroyCount).toBe(0);

    await dispatcher.stop();
  });

  it('emits setup_failed when watcher seed fails on the S3 memory path', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      resources: [
        {
          id: 'res_memory_fuse',
          type: 'memory_store',
          memory_store_id: 'mems_fuse',
          mount_path: '/mnt/memory/fuse/',
          access: 'read_write',
        },
      ],
      memoryStores: new Map([['mems_fuse', { id: 'mems_fuse', name: 'fuse' }]]),
      failListMemoriesFor: new Set(['mems_fuse']),
    });
    const runtime = new RecordingSandboxRuntime({ supportsFuse: true });
    const memoryGrantInputs: Array<{
      generationId: string;
      memoryStores: Array<{ storeId: string; access: string }>;
    }> = [];
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-memory-watcher-seed-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
      credsMinter: {
        mint: async (input) => {
          memoryGrantInputs.push(input);
          return sessionCreds();
        },
      },
      s3Client: new S3Client({ region: 'us-east-1' }),
      s3Bucket: 'orca-files',
      s3Endpoint: 'http://minio:9000',
      s3KeyPrefix: 'memory/',
      memoryWatcherIntervalMs: 0,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_memory_seed_fail_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    expect(store.appended.some((event) => event.kind === 'agent.message')).toBe(false);
    expect(setupFailedPayload(store)?.phase).toBe('memory_setup');
    expect(setupFailedPayload(store)?.error).toMatch(/listSessionMemories mems_fuse failed/);
    expect(runtime.destroyCount).toBe(1);
    expect(memoryGrantInputs).toEqual([
      expect.objectContaining({
        generationId: expect.stringMatching(/^run_/),
        memoryStores: [{ storeId: 'mems_fuse', access: 'read_write' }],
      }),
    ]);

    await dispatcher.stop();
  });

  it('rethrows setup failures when setup_failed cannot be recorded', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore({ failOnKind: 'session.error' });
    const registry = new RecordingRegistry({
      resources: [
        {
          id: 'res_missing_memory',
          type: 'memory_store',
          memory_store_id: 'mems_missing',
          mount_path: '/mnt/memory/missing/',
          access: 'read_write',
        },
      ],
    });
    const runtime = new RecordingSandboxRuntime();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-setup-failed-append-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
    });

    await dispatcher.start();
    try {
      await expect(
        source.emit(
          userMessage('evt_user_setup_failed_append_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
        ),
      ).rejects.toThrow('append failed for session.error');
      expect(store.appended.some((event) => event.kind === 'agent.message')).toBe(false);
      expect(runtime.destroyCount).toBe(1);
      expect(registry.states.at(-1)).toEqual({
        sessionId: 'ses_pg_dispatcher',
        status: 'idle',
        sandboxHandleId: null,
      });
    } finally {
      await dispatcher.stop();
    }
  });

  it('emits setup_failed and does not start the runner when repo setup fails', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      resources: [
        {
          id: 'sesrsc_repo_1',
          type: 'github_repository',
          file_id: null,
          memory_store_id: null,
          repo_ref: {
            git_credential_id: 'gitcred_repo',
            url: 'https://github.com/acme/repo.git',
          },
          mount_path: '/workspace/repo',
          access: 'read_write',
          instructions: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
          mount_strategy: null,
        },
      ],
    });
    const runtime = new RecordingSandboxRuntime();
    const workDir = new RecordingWorkDir();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-repo-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      fileStore: new StubFileStore(),
      sandboxRuntime: runtime,
      gitWorker: {} as never,
      workDir: workDir as never,
    });

    await dispatcher.start();
    await source.emit(userMessage('evt_user_repo_fail_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'));

    expect(store.appended.some((event) => event.kind === 'agent.message')).toBe(false);
    expect(setupFailedPayload(store)?.phase).toBe('repo_setup');
    expect(setupFailedPayload(store)?.error).toMatch(/GIT_CREDS_PUBLIC_URL/);
    expect(runtime.destroyCount).toBe(1);
    expect(workDir.released).toEqual([
      { workspaceId: 'ws_pg_dispatcher', sessionId: 'ses_pg_dispatcher' },
    ]);

    await dispatcher.stop();
  });

  it('injects in-sandbox git helper credentials before acquiring the sandbox', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      agentMetadata: { harness: 'claude_code', mode: 'colocated' },
      resources: [
        {
          id: 'sesrsc_repo_in_sandbox',
          type: 'github_repository',
          file_id: null,
          memory_store_id: null,
          repo_ref: {
            git_credential_id: 'gitcred_repo',
            url: 'https://github.com/acme/repo.git',
          },
          mount_path: '/workspace/repo',
          access: 'read_write',
          instructions: null,
          attached_at: new Date().toISOString(),
          detached_at: null,
          mount_strategy: null,
        },
      ],
    });
    const runtime = new RecordingSandboxRuntime();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-in-sandbox-git-env-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new EchoHarness(),
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: runtime,
      gitCredsPublicUrl: 'https://registry.example.com/v1/git-creds',
    });

    await dispatcher.start();
    await source.emit(userMessage('evt_user_repo_env_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'));

    expect(runtime.acquiredSpecs).toHaveLength(1);
    expect(runtime.acquiredSpecs[0]?.harnessEnv).toEqual(
      expect.objectContaining({
        ORCA_GIT_CREDS_URL: 'https://registry.example.com/v1/git-creds',
        ORCA_GIT_CREDS_TOKEN: 'git-jwt',
      }),
    );
    expect(JSON.parse(runtime.acquiredSpecs[0]!.harnessEnv!['ORCA_SANDBOX_WRITE_POLICY']!)).toEqual(
      {
        writablePaths: [
          { path: '/mnt/session/outputs', kind: 'session_output' },
          { path: '/workspace/repo', kind: 'github_repository' },
        ],
        readonlyPaths: [],
        networkAllowedDomains: ['github.com', 'registry.example.com'],
      },
    );
    expect(JSON.stringify(runtime.acquiredSpecs[0])).not.toContain('gitcred_repo');
    expect(setupFailedPayload(store)?.phase).toBe('repo_setup');

    await dispatcher.stop();
  });

  it('defaults enabled remote MCP runtime policies to always_ask when snapshot policy is missing', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new RecordingStartInputHarness();
    const registry = new RecordingRegistry({
      agent: {
        id: 'agt_fake',
        workspace_id: 'ws_pg_dispatcher',
        name: 'fake agent',
        version: 1,
        model: { provider: 'anthropic', id: 'fake' },
        system: '',
        tools: [{ type: 'mcp_toolset', mcp_server_name: 'github' }],
        mcp_servers: [{ name: 'slack', url: 'https://mcp.slack.example.com' }],
        skills: [],
      },
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-missing-mcp-policy-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      gatewayMcpUrl: 'https://gateway.example.com',
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_mcp_policy_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    expect(harness.startedInputs[0]?.remoteMcpToolsets).toEqual([
      { serverName: 'github', permissionPolicy: 'always_ask' },
    ]);

    await dispatcher.stop();
  });

  it('passes custom tool definitions from the agent record into the harness snapshot', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new RecordingStartInputHarness();
    const inputSchema = {
      type: 'object',
      properties: { ticket_id: { type: 'string' } },
      required: ['ticket_id'],
    };
    const registry = new RecordingRegistry({
      agent: {
        id: 'agt_fake',
        workspace_id: 'ws_pg_dispatcher',
        name: 'fake agent',
        version: 1,
        model: { provider: 'anthropic', id: 'fake' },
        system: '',
        tools: [
          {
            type: 'custom',
            name: 'lookup_ticket',
            description: 'Look up a support ticket.',
            input_schema: inputSchema,
          },
        ],
        mcp_servers: [],
        skills: [],
      },
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-custom-tool-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_custom_tool_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    expect(harness.startedInputs[0]?.agentSnapshot.custom_tools).toEqual([
      {
        name: 'lookup_ticket',
        description: 'Look up a support ticket.',
        input_schema: inputSchema,
      },
    ]);

    await dispatcher.stop();
  });

  it('enables client tool execution for self-hosted separated Claude SDK sessions', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new RecordingStartInputHarness();
    const registry = new RecordingRegistry({
      environment: {
        id: 'env_self_hosted',
        workspace_id: 'ws_pg_dispatcher',
        name: 'self hosted',
        target: 'self_hosted',
      },
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-client-tool-execution-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_client_tool_execution_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    expect(harness.startedInputs[0]?.clientToolExecution).toBe(true);
    await dispatcher.stop();
  });

  it.each(['claude_agent_sdk', 'codex_sdk'] as const)(
    '%s: discloses only the ordered Skill catalog and leaves agent tools unchanged',
    async (harnessType) => {
      const source = new FakeEventSource();
      const store = new RecordingStore();
      const harness = new RecordingStartInputHarness();
      const skillStore = new InMemorySkillStore();
      const skillBody = Buffer.from(
        '---\nname: runtime-skill\ndescription: Runtime skill\n---\nEAGER_BODY_MARKER',
      );
      const bundle = await skillStore.put('ws_pg_dispatcher', 'sklv_runtime', [
        { path: 'SKILL.md', content: skillBody },
      ]);
      const open = vi.spyOn(skillStore, 'open');
      const runtime = new RecordingSandboxRuntime();
      const chmod = vi.fn(async () => {});
      const acquire = runtime.acquire.bind(runtime);
      vi.spyOn(runtime, 'acquire').mockImplementation(async (env) => {
        const sandbox = await acquire(env);
        sandbox.files.chmod = chmod;
        return sandbox;
      });
      const registry = new RecordingRegistry({
        agent: {
          id: 'agt_fake',
          workspace_id: 'ws_pg_dispatcher',
          name: 'fake agent',
          version: 1,
          model:
            harnessType === 'codex_sdk'
              ? { provider: 'openai', id: 'gpt-5.4' }
              : { provider: 'anthropic', id: 'fake' },
          metadata: { harness: harnessType },
          system: 'Base prompt',
          tools: [{ type: 'agent_toolset' }],
          mcp_servers: [],
          skills: [
            {
              id: 'sklv_runtime',
              skill_id: 'skl_runtime',
              source: 'custom',
              version_identifier: '1784801377522001',
              name: 'runtime-skill',
              description: 'Runtime skill',
              entrypoint: 'SKILL.md',
              package_sha256: bundle.sha256,
              package_size_bytes: bundle.sizeBytes,
            },
          ],
        },
      });
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'pg-dispatcher-public-skill-version-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory: () => harness,
        registry: registry as unknown as RegistryClient,
        sandboxRuntime: runtime,
        skillStore,
      });

      await dispatcher.start();
      await source.emit(
        userMessage('evt_user_skill_version_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
      );

      expect(setupFailedPayload(store)).toBeNull();
      expect(open).toHaveBeenCalledTimes(1);
      expect(open).toHaveBeenCalledWith('ws_pg_dispatcher', 'sklv_runtime', bundle.sha256);
      expect(runtime.sandboxFileWrites).toContain('/workspace/skills/runtime-skill/SKILL.md');
      expect(chmod).toHaveBeenCalledWith('/workspace/skills/runtime-skill/SKILL.md', 0o444);
      expect(chmod).toHaveBeenCalledWith('/workspace/skills/runtime-skill', 0o555);
      expect(chmod).toHaveBeenCalledWith('/workspace/skills', 0o555);
      const snapshot = harness.startedInputs[0]?.agentSnapshot;
      expect(snapshot?.system).toContain('Base prompt');
      expect(snapshot?.system).toContain('"name":"runtime-skill"');
      expect(snapshot?.system).toContain('/workspace/skills/runtime-skill/SKILL.md');
      expect(snapshot?.system).not.toContain('EAGER_BODY_MARKER');
      expect(snapshot?.allowed_tool_names).toEqual([
        'bash',
        'read',
        'write',
        'edit',
        'glob',
        'grep',
        'list',
        'delete',
      ]);

      await dispatcher.stop();
    },
  );

  it('builds enabled tools and permission policies independently for every coordinator child', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new RecordingStartInputHarness();
    const child: AgentRecord = {
      id: 'agt_child',
      workspace_id: 'ws_pg_dispatcher',
      name: 'child',
      version: 2,
      model: { provider: 'anthropic', id: 'fake-child' },
      system: '',
      tools: [
        {
          type: 'agent_toolset',
          default_config: {
            enabled: false,
            permission_policy: { type: 'always_ask' },
          },
          configs: [
            {
              name: 'read',
              enabled: true,
              permission_policy: { type: 'always_allow' },
            },
          ],
        },
      ],
      mcp_servers: [],
      skills: [],
      multiagent: null,
    };
    const registry = new RecordingRegistry({
      agent: {
        id: 'agt_fake',
        workspace_id: 'ws_pg_dispatcher',
        name: 'coordinator',
        version: 1,
        model: { provider: 'anthropic', id: 'fake' },
        system: '',
        tools: [
          {
            type: 'agent_toolset',
            default_config: {
              enabled: false,
              permission_policy: { type: 'always_ask' },
            },
            configs: [
              {
                name: 'bash',
                enabled: true,
                permission_policy: { type: 'always_allow' },
              },
            ],
          },
        ],
        mcp_servers: [],
        skills: [],
        multiagent: {
          type: 'coordinator',
          agents: [{ type: 'agent', id: child.id, version: child.version }],
        },
      },
      subagents: [child],
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-child-tool-policy-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_child_tool_policy_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    expect(harness.startedInputs[0]?.agentSnapshot).toMatchObject({
      allowed_tool_names: ['bash'],
      tool_permission_policies: {
        'mcp__orca__*': 'always_deny',
        mcp__orca__bash: 'always_allow',
      },
      multiagent: {
        agents: [
          {
            id: 'agt_child',
            allowed_tool_names: ['read'],
            tool_permission_policies: {
              'mcp__orca__*': 'always_deny',
              mcp__orca__read: 'always_allow',
            },
          },
        ],
      },
    });

    await dispatcher.stop();
  });

  it('fails closed before runner start when a skilled session has no sandbox runtime', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new RecordingStartInputHarness();
    const skillStore = new InMemorySkillStore();
    const skill = await putTestSkill(skillStore, 'sklv_no_sandbox', 'no-sandbox');
    const registry = new RecordingRegistry({
      agent: {
        id: 'agt_fake',
        workspace_id: 'ws_pg_dispatcher',
        name: 'fake agent',
        version: 1,
        model: { provider: 'anthropic', id: 'fake' },
        system: '',
        tools: [{ type: 'agent_toolset' }],
        mcp_servers: [],
        skills: [skill],
      },
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-skill-sandbox-required-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      skillStore,
    });

    await dispatcher.start();
    await source.emit(userMessage('evt_skill_no_sandbox', 'ws_pg_dispatcher', 'ses_pg_dispatcher'));

    expect(harness.startedInputs).toHaveLength(0);
    expect(setupFailedPayload(store)).toEqual({
      phase: 'skill_setup',
      error: 'sessions with skills require a sandbox runtime',
    });
    await dispatcher.stop();
  });

  it.each(['claude_agent_sdk', 'codex_sdk'] as const)(
    '%s: fails closed when a skilled agent explicitly disables read',
    async (harnessType) => {
      const source = new FakeEventSource();
      const store = new RecordingStore();
      const harness = new RecordingStartInputHarness();
      const skillStore = new InMemorySkillStore();
      const runtime = new RecordingSandboxRuntime();
      const skill = await putTestSkill(skillStore, 'sklv_no_read', 'no-read');
      const registry = new RecordingRegistry({
        agent: {
          id: 'agt_fake',
          workspace_id: 'ws_pg_dispatcher',
          name: 'fake agent',
          version: 1,
          model:
            harnessType === 'codex_sdk'
              ? { provider: 'openai', id: 'gpt-5.4' }
              : { provider: 'anthropic', id: 'fake' },
          metadata: { harness: harnessType },
          system: '',
          tools: [
            {
              type: 'agent_toolset',
              configs: [{ name: 'read', enabled: false }],
            },
          ],
          mcp_servers: [],
          skills: [skill],
        },
      });
      const dispatcher = new Dispatcher({
        eventSource: source,
        groupId: 'pg-dispatcher-skill-read-required-unit',
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory: () => harness,
        registry: registry as unknown as RegistryClient,
        sandboxRuntime: runtime,
        skillStore,
      });

      await dispatcher.start();
      await source.emit(userMessage('evt_skill_no_read', 'ws_pg_dispatcher', 'ses_pg_dispatcher'));

      expect(runtime.acquireCount).toBe(0);
      expect(harness.startedInputs).toHaveLength(0);
      expect(setupFailedPayload(store)).toEqual({
        phase: 'skill_setup',
        error: 'agent agt_fake has skills but no enabled read tool',
      });
      await dispatcher.stop();
    },
  );

  it('requires in-sandbox read to be always_allow because approvals are unavailable', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new RecordingStartInputHarness();
    const skillStore = new InMemorySkillStore();
    const runtime = new RecordingSandboxRuntime();
    const skill = await putTestSkill(skillStore, 'sklv_ask_read', 'ask-read');
    const registry = new RecordingRegistry({
      agent: {
        id: 'agt_fake',
        workspace_id: 'ws_pg_dispatcher',
        name: 'fake agent',
        version: 1,
        model: { provider: 'anthropic', id: 'fake' },
        system: '',
        tools: [
          {
            type: 'agent_toolset',
            configs: [{ name: 'read', permission_policy: { type: 'always_ask' } }],
          },
        ],
        mcp_servers: [],
        skills: [skill],
        metadata: { harness: 'claude_code', mode: 'colocated' },
      },
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-skill-read-policy-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: runtime,
      skillStore,
    });

    await dispatcher.start();
    await source.emit(userMessage('evt_skill_ask_read', 'ws_pg_dispatcher', 'ses_pg_dispatcher'));

    expect(runtime.acquireCount).toBe(0);
    expect(harness.startedInputs).toHaveLength(0);
    expect(setupFailedPayload(store)).toEqual({
      phase: 'skill_setup',
      error: 'in-sandbox agent agt_fake requires read=always_allow to load skills',
    });
    await dispatcher.stop();
  });

  it('does not silently skip a pinned anthropic descriptor whose bundle is missing', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new RecordingStartInputHarness();
    const skillStore = new InMemorySkillStore();
    const skill = {
      ...(await putTestSkill(skillStore, 'sklv_anthropic', 'anthropic-skill')),
      source: 'anthropic' as const,
    };
    await skillStore.delete('ws_pg_dispatcher', skill.id, skill.package_sha256);
    const registry = new RecordingRegistry({
      agent: {
        id: 'agt_fake',
        workspace_id: 'ws_pg_dispatcher',
        name: 'fake agent',
        version: 1,
        model: { provider: 'anthropic', id: 'fake' },
        system: '',
        tools: [{ type: 'agent_toolset' }],
        mcp_servers: [],
        skills: [skill],
      },
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-anthropic-skill-bundle-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: new RecordingSandboxRuntime(),
      skillStore,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_skill_anthropic_missing', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    expect(harness.startedInputs).toHaveLength(0);
    expect(setupFailedPayload(store)).toMatchObject({
      phase: 'skill_setup',
      error: expect.stringContaining('failed to open skill bundle sklv_anthropic'),
    });
    await dispatcher.stop();
  });

  it('omits reserved-name custom tools from the harness snapshot', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const harness = new RecordingStartInputHarness();
    const registry = new RecordingRegistry({
      agent: {
        id: 'agt_fake',
        workspace_id: 'ws_pg_dispatcher',
        name: 'fake agent',
        version: 1,
        model: { provider: 'anthropic', id: 'fake' },
        system: '',
        tools: [
          { type: 'agent_toolset' },
          { type: 'custom', name: 'bash', description: 'Reserved collision.' },
        ],
        mcp_servers: [],
        skills: [],
      },
    });
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-reserved-custom-tool-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_reserved_custom_tool_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher'),
    );

    expect(harness.startedInputs[0]?.agentSnapshot.custom_tools).toEqual([]);
    expect(harness.startedInputs[0]?.agentSnapshot.allowed_tool_names).toContain('bash');

    await dispatcher.stop();
  });

  it('respawns warm runner when session-local overrides change before next turn', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const harnesses: RecordingStartInputHarness[] = [];
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-session-override-refresh-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => {
        const harness = new RecordingStartInputHarness();
        harnesses.push(harness);
        return harness;
      },
      registry: registry as unknown as RegistryClient,
      gatewayMcpUrl: 'https://gateway.example.com',
      sessionIdleTimeoutMs: 60_000,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_override_refresh_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'one'),
    );
    await waitFor(() => store.appended.some((event) => event.kind === 'agent.message'));
    expect(harnesses).toHaveLength(1);
    expect(harnesses[0]?.startedInputs[0]?.agentSnapshot?.allowed_tool_names).toContain('bash');

    registry.sessionOverrides.tools = [];
    await source.emit(
      userMessage('evt_user_override_refresh_2', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'two', 2),
    );

    await waitFor(() => harnesses.length === 2);
    expect(harnesses[1]?.startedInputs[0]?.agentSnapshot?.allowed_tool_names).toEqual([]);

    await dispatcher.stop();
  });

  it('refreshes authoritative session usage state before a warm runner evaluates the next turn', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({ guardrailState: { session_cost_usd: 0.25 } });
    const harness = new GuardrailStateRefreshHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-guardrail-state-refresh-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      sessionIdleTimeoutMs: 60_000,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_guardrail_refresh_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'one'),
    );
    await waitFor(() => harness.submitCount === 1);

    registry.guardrailState = { session_cost_usd: 2 };
    await source.emit(
      userMessage(
        'evt_user_guardrail_refresh_2',
        'ws_pg_dispatcher',
        'ses_pg_dispatcher',
        'two',
        2,
      ),
    );
    await waitFor(() => harness.submitCount === 2);

    expect(harness.stopReasons).toEqual([]);
    expect(harness.stateAtSubmit[1]).toEqual({ session_cost_usd: 2 });
    expect(registry.prepareCalls).toHaveLength(2);

    await dispatcher.stop();
  });

  it('refreshes authoritative usage state before draining a deferred user message', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({ guardrailState: { session_cost_usd: 0.25 } });
    const harness = new ReplayDeferredMessageHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-deferred-guardrail-refresh-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      sessionIdleTimeoutMs: 60_000,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_deferred_guardrail_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'first'),
    );
    await waitFor(() => store.appended.some((event) => event.kind === 'session.status_idle'));
    await source.emit(
      userMessage('evt_deferred_guardrail_2', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'queued', 2),
    );

    registry.guardrailState = { session_cost_usd: 2 };
    await source.emit(
      customToolResult('evt_deferred_guardrail_result', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 3),
    );

    expect(harness.stateAtUserMessageSubmit).toEqual([
      undefined,
      undefined,
      { session_cost_usd: 2 },
    ]);
    expect(registry.prepareCalls).toHaveLength(2);
    expect(
      store.appended.some(
        (event) =>
          event.kind === 'session.deferred_user_message_submitted' &&
          eventPayload(event).user_event_id === 'evt_deferred_guardrail_2',
      ),
    ).toBe(true);

    await dispatcher.stop();
  });

  it('leaves a deferred user message queued when authoritative usage refresh fails', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry();
    const harness = new ReplayDeferredMessageHarness();
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-deferred-guardrail-refresh-failure-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => harness,
      registry: registry as unknown as RegistryClient,
      sessionIdleTimeoutMs: 60_000,
    });

    await dispatcher.start();
    await source.emit(
      userMessage(
        'evt_deferred_guardrail_failure_1',
        'ws_pg_dispatcher',
        'ses_pg_dispatcher',
        'first',
      ),
    );
    await waitFor(() => store.appended.some((event) => event.kind === 'session.status_idle'));
    await source.emit(
      userMessage(
        'evt_deferred_guardrail_failure_2',
        'ws_pg_dispatcher',
        'ses_pg_dispatcher',
        'queued',
        2,
      ),
    );

    registry.prepareError = new Error('registry unavailable');
    await source.emit(
      customToolResult(
        'evt_deferred_guardrail_failure_result',
        'ws_pg_dispatcher',
        'ses_pg_dispatcher',
        3,
      ),
    );

    expect(harness.submitted.filter((event) => event.kind === 'user.message')).toHaveLength(2);
    expect(registry.prepareCalls).toHaveLength(2);
    expect(
      store.appended.some(
        (event) =>
          event.kind === 'session.deferred_user_message_submitted' &&
          eventPayload(event).user_event_id === 'evt_deferred_guardrail_failure_2',
      ),
    ).toBe(false);

    await dispatcher.stop();
  });

  it('keeps a warm runner when an advisory MCP credential is replaced', async () => {
    const source = new FakeEventSource();
    const store = new RecordingStore();
    const registry = new RecordingRegistry({
      agent: {
        id: 'agt_fake',
        workspace_id: 'ws_pg_dispatcher',
        name: 'fake agent',
        version: 1,
        model: { provider: 'anthropic', id: 'fake' },
        system: '',
        tools: [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'gateway-e2e',
            default_config: { permission_policy: { type: 'always_allow' } },
          },
        ],
        mcp_servers: [{ name: 'gateway-e2e', url: 'https://mcp.example.com' }],
        skills: [],
      },
      session: { vault_ids: ['vlt_gateway'] },
      vaultCredentials: [
        {
          credential_id: 'vcrd_old',
          vault_id: 'vlt_gateway',
          auth_type: 'static_bearer',
          mcp_server_url: 'https://mcp.example.com',
        },
      ],
    });
    const harnesses: TerminalEchoHarness[] = [];
    const dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'pg-dispatcher-advisory-mcp-credential-refresh-unit',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => {
        const harness = new TerminalEchoHarness();
        harnesses.push(harness);
        return harness;
      },
      registry: registry as unknown as RegistryClient,
      gatewayMcpUrl: 'https://gateway.example.com/v1/mcp',
      sessionIdleTimeoutMs: 60_000,
    });

    await dispatcher.start();
    await source.emit(
      userMessage('evt_user_mcp_credential_1', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'one'),
    );
    await waitFor(() => harnesses[0]?.submitCount === 1);

    registry.vaultCredentials = [
      {
        credential_id: 'vcrd_replacement',
        vault_id: 'vlt_gateway',
        auth_type: 'static_bearer',
        mcp_server_url: 'https://mcp.example.com',
      },
    ];
    await source.emit(
      userMessage('evt_user_mcp_credential_2', 'ws_pg_dispatcher', 'ses_pg_dispatcher', 'two', 2),
    );
    await waitFor(() => harnesses[0]?.submitCount === 2);

    expect(harnesses).toHaveLength(1);
    expect(harnesses[0]?.stopReasons).toEqual([]);

    await dispatcher.stop();
  });
});

function userMessage(
  id: string,
  workspaceId: string,
  sessionId: string,
  text = 'hi',
  seq = 1,
): Event {
  return {
    id,
    workspaceId,
    sessionId,
    subpath: '',
    seq,
    producedAt: new Date().toISOString(),
    producedBy: 'client',
    kind: 'user.message',
    payload: new TextEncoder().encode(JSON.stringify({ content: [{ type: 'text', text }] })),
    idempotencyKey: '',
  };
}

function customToolResult(id: string, workspaceId: string, sessionId: string, seq: number): Event {
  return {
    id,
    workspaceId,
    sessionId,
    subpath: '',
    seq,
    producedAt: new Date().toISOString(),
    producedBy: 'client',
    kind: 'user.custom_tool_result',
    payload: new TextEncoder().encode(
      JSON.stringify({ tool_use_id: 'evt_required_action', content: 'ok' }),
    ),
    idempotencyKey: '',
  };
}

function eventPayload(event: Event): Record<string, unknown> {
  return JSON.parse(Buffer.from(event.payload).toString('utf8')) as Record<string, unknown>;
}

async function statusRunningHistogramTotals(): Promise<{ count: number; sum: number }> {
  const values = (await harnessAcceptedEventToStatusRunningSeconds.get()).values;
  const count = values.find((value) => value.metricName.endsWith('_count'))?.value;
  const sum = values.find((value) => value.metricName.endsWith('_sum'))?.value;
  if (typeof count !== 'number' || typeof sum !== 'number') {
    throw new Error('status_running histogram values are missing');
  }
  return { count, sum };
}

function pendingStatusRunningLatencyCount(dispatcher: Dispatcher): number {
  const pending = (
    dispatcher as unknown as {
      pendingStatusRunningLatency: Map<string, unknown[]>;
    }
  ).pendingStatusRunningLatency;
  return [...pending.values()].reduce((count, queue) => count + queue.length, 0);
}

function statusRunningLatencyEntryCount(dispatcher: Dispatcher): number {
  const state = dispatcher as unknown as {
    pendingStatusRunningLatency: Map<string, unknown[]>;
    activeStatusRunningLatency: Map<string, unknown[]>;
  };
  return (
    [...state.pendingStatusRunningLatency.values()].reduce(
      (count, queue) => count + queue.length,
      0,
    ) +
    [...state.activeStatusRunningLatency.values()].reduce((count, queue) => count + queue.length, 0)
  );
}

function completedUserEventCacheSize(dispatcher: Dispatcher): number {
  return (
    dispatcher as unknown as {
      completedUserEventIds: Map<string, Set<string>>;
    }
  ).completedUserEventIds.size;
}

function activeTurnUserEventCount(dispatcher: Dispatcher): number {
  const active = (
    dispatcher as unknown as {
      activeTurnUserEventIds: Map<string, string[]>;
    }
  ).activeTurnUserEventIds;
  return [...active.values()].reduce((count, ids) => count + ids.length, 0);
}

function activeRunnerCount(dispatcher: Dispatcher): number {
  return (dispatcher as unknown as { runners: Map<string, unknown> }).runners.size;
}

function runnerSessionConfigKeyCount(dispatcher: Dispatcher): number {
  return (
    dispatcher as unknown as {
      runnerSessionConfigKeys: Map<string, string>;
    }
  ).runnerSessionConfigKeys.size;
}

/** Native-client seam: nacks stay parked while later queued deliveries remain available. */
function queuedPulsarSource(events: Event[], retryDelayMs = 1) {
  const acknowledged: string[] = [];
  const nacked: string[] = [];
  const messages = events.map((event) => ({
    getData: () => Buffer.from(event.payload),
    getProperties: () => ({
      id: event.id,
      workspace_id: event.workspaceId,
      session_id: event.sessionId,
      subpath: event.subpath,
      produced_at: event.producedAt,
      produced_by: event.producedBy,
      kind: event.kind,
      idempotency_key: event.idempotencyKey,
    }),
    getMessageId: () => ({
      toString: () => String(event.seq),
      serialize: () => Buffer.from(event.id),
    }),
    getPublishTimestamp: () => Date.parse(event.producedAt),
    getRedeliveryCount: () => 0,
    getTopicName: () =>
      `persistent://public/default/orca.${event.workspaceId}.sessions.${event.sessionId}.events`,
  }));
  const consumer = {
    receive: async () => {
      const message = messages.shift();
      if (message) return message;
      await new Promise((resolve) => setTimeout(resolve, 1));
      throw new Error('timeout');
    },
    acknowledge: async (message: (typeof messages)[number]) => {
      acknowledged.push(message.getProperties().id);
      return null;
    },
    negativeAcknowledge: (message: (typeof messages)[number]) => {
      nacked.push(message.getProperties().id);
    },
    close: async () => null,
    unsubscribe: async () => null,
  };
  const source = new PulsarSessionEventSource({
    client: {
      createProducer: async () => {
        throw new Error('source does not create producers');
      },
      createReader: async () => {
        throw new Error('source does not create readers');
      },
      subscribe: async () => consumer,
      close: async () => null,
    },
    subscription: 'queued-outcome-test',
    receiveTimeoutMs: 1,
    nAckRedeliverTimeoutMs: retryDelayMs,
    topicRediscoverIntervalMs: 0,
  });
  return { source, acknowledged, nacked };
}

class FakeEventSource implements SessionEventSource {
  private handler: ((event: Event) => Promise<void>) | null = null;

  async start(handler: (event: Event) => Promise<void>): Promise<void> {
    this.handler = handler;
  }

  async stop(): Promise<void> {
    this.handler = null;
  }

  async emit(event: Event): Promise<void> {
    if (!this.handler) throw new Error('source not started');
    await this.handler(event);
  }
}

class StatusEventSource extends FakeEventSource {
  private currentStatus: SessionEventSourceStatus = { ready: true, state: 'running' };

  setStatus(status: SessionEventSourceStatus): void {
    this.currentStatus = status;
  }

  status(): SessionEventSourceStatus {
    return this.currentStatus;
  }
}

class BlockingStopEventSource extends FakeEventSource {
  stopStarted = false;
  private release: (() => void) | null = null;
  private readonly stopped = new Promise<void>((resolve) => {
    this.release = resolve;
  });

  override async stop(): Promise<void> {
    this.stopStarted = true;
    await this.stopped;
    await super.stop();
  }

  releaseStop(): void {
    this.release?.();
  }
}

class RecordingStore implements TranscriptStore {
  readonly appended: Event[] = [];
  readonly appendBatches: Event[][] = [];
  private readonly persisted: Event[] = [];
  readCalls = 0;
  readonly readOptions: ReadOptions[] = [];

  constructor(private readonly opts: { failOnKind?: string } = {}) {}

  seed(events: Event[]): void {
    this.persisted.push(...events);
  }

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    const failingEvent = events.find((event) => event.kind === this.opts.failOnKind);
    if (failingEvent) {
      throw new Error(`append failed for ${failingEvent.kind}`);
    }
    const normalized = events.map((event, index) => ({
      ...event,
      workspaceId,
      sessionId,
      seq: this.persisted.length + index + 1,
    }));
    this.appendBatches.push(normalized);
    this.appended.push(...normalized);
    this.persisted.push(...normalized);
    return events.map((event) => event.id);
  }

  async *read(workspaceId: string, sessionId: string, opts: ReadOptions): AsyncIterable<Event> {
    this.readCalls += 1;
    this.readOptions.push(opts);
    yield* this.persisted.filter(
      (event) => event.workspaceId === workspaceId && event.sessionId === sessionId,
    );
  }

  async *tail(_workspaceId: string, _sessionId: string, _opts: TailOptions): AsyncIterable<Event> {
    yield* [];
  }

  async archive(_workspaceId: string, _sessionId: string): Promise<void> {
    void _workspaceId;
    void _sessionId;
  }

  async close(): Promise<void> {}
}

class BlockingTerminalAppendStore extends RecordingStore {
  terminalAppendStarted = false;
  private release: (() => void) | null = null;
  private readonly terminalAppend = new Promise<void>((resolve) => {
    this.release = resolve;
  });

  override async append(
    workspaceId: string,
    sessionId: string,
    events: Event[],
  ): Promise<string[]> {
    if (events.some((event) => event.kind === 'session.user_event_completed')) {
      this.terminalAppendStarted = true;
      await this.terminalAppend;
    }
    return await super.append(workspaceId, sessionId, events);
  }

  releaseTerminalAppend(): void {
    this.release?.();
  }
}

class FailFirstCompletedMarkerStore extends RecordingStore {
  readonly failedCompletionBatches: Event[][] = [];
  private fail = true;

  recover(): void {
    this.fail = false;
  }

  override async append(
    workspaceId: string,
    sessionId: string,
    events: Event[],
  ): Promise<string[]> {
    if (this.fail && events.some((event) => event.kind === 'session.user_event_completed')) {
      this.failedCompletionBatches.push(events.map((event) => ({ ...event })));
      throw new Error('terminal completion append failed');
    }
    return await super.append(workspaceId, sessionId, events);
  }
}

class FailingAcceptanceStore extends RecordingStore {
  acceptanceAttempts = 0;

  constructor(
    private readonly userEventId: string,
    private failuresRemaining: number,
  ) {
    super();
  }

  recover(): void {
    this.failuresRemaining = 0;
  }

  override async append(
    workspaceId: string,
    sessionId: string,
    events: Event[],
  ): Promise<string[]> {
    const matchesAcceptance = events.some(
      (event) =>
        event.kind === 'session.user_event_processed' &&
        eventPayload(event).user_event_id === this.userEventId,
    );
    if (matchesAcceptance) {
      this.acceptanceAttempts += 1;
      if (this.failuresRemaining > 0) {
        this.failuresRemaining -= 1;
        throw new Error('transient acceptance failure');
      }
    }
    return await super.append(workspaceId, sessionId, events);
  }
}

class FailingReadStore extends RecordingStore {
  override read(
    _workspaceId: string,
    _sessionId: string,
    _opts: ReadOptions,
  ): AsyncIterable<Event> {
    void _workspaceId;
    void _sessionId;
    void _opts;
    this.readCalls += 1;
    return {
      [Symbol.asyncIterator]: () => ({
        next: async () => {
          throw new Error('transient transcript read failure');
        },
      }),
    };
  }
}

class EchoHarness implements AgentHarness {
  private queue: AgentEvent[] = [];
  private resolvers: Array<(result: IteratorResult<AgentEvent>) => void> = [];
  private done = false;
  readonly stopReasons: TerminationReason[] = [];

  async start(_input: SessionStartInput): Promise<void> {
    void _input;
  }

  async submit(event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    if (event.kind !== 'user.message') return;
    await hooks?.onAccepted();
    this.emit({
      kind: 'agent.message',
      payload: { content: [{ type: 'text', text: 'ok' }] },
    });
    this.afterAgentMessage();
  }

  async stop(reason: TerminationReason): Promise<void> {
    this.stopReasons.push(reason);
    this.done = true;
    for (const resolve of this.resolvers.splice(0)) {
      resolve({ value: undefined, done: true });
    }
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.done || this.queue.length > 0) {
      const event = this.queue.shift();
      if (event !== undefined) {
        yield event;
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        this.resolvers.push(resolve);
      });
      if (next.done) return;
      yield next.value;
    }
  }

  protected afterAgentMessage(): void {}

  protected emit(event: AgentEventInput): void {
    const enveloped = withCanonicalAgentEventEnvelope(event);
    const resolve = this.resolvers.shift();
    if (resolve) resolve({ value: enveloped, done: false });
    else this.queue.push(enveloped);
  }

  protected emitUnsafe(event: AgentEvent): void {
    const resolve = this.resolvers.shift();
    if (resolve) resolve({ value: event, done: false });
    else this.queue.push(event);
  }
}

class PumpPoisonHarness extends EchoHarness {
  submitCalls = 0;

  override async submit(event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    if (event.kind !== 'user.message') return;
    this.submitCalls += 1;
    await hooks?.onAccepted();
    setTimeout(() => {
      this.emitUnsafe({
        kind: 'agent.message',
        id: 'evt_invalid_subpath',
        payload: { content: [{ type: 'text', text: 'invalid subpath' }] },
        subpath: '*' as never,
      });
    }, 0);
  }
}

class DelayedStatusRunningHarness extends EchoHarness {
  override async submit(event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    if (event.kind === 'user.message') await hooks?.onAccepted();
  }

  emitStatusRunning(): void {
    this.emit({ kind: 'session.status_running', payload: {} });
  }

  emitStatusIdle(): void {
    this.emit({ kind: 'session.status_idle', payload: { stop_reason: { type: 'end_turn' } } });
  }
}

class TerminalEchoHarness extends EchoHarness {
  submitCount = 0;

  override async submit(event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    if (event.kind !== 'user.message') return;
    this.submitCount += 1;
    await hooks?.onAccepted();
    this.emit({
      kind: 'agent.message',
      payload: { content: [{ type: 'text', text: 'ok' }] },
    });
    this.emit({
      kind: 'session.status_idle',
      payload: { stop_reason: { type: 'end_turn' } },
    });
  }
}

class RecordingStartInputHarness extends EchoHarness {
  readonly startedInputs: SessionStartInput[] = [];

  override async start(input: SessionStartInput): Promise<void> {
    this.startedInputs.push(input);
  }
}

class GuardrailStateRefreshHarness extends TerminalEchoHarness {
  readonly appliedStates: Array<Readonly<Record<string, unknown>>> = [];
  readonly stateAtSubmit: Array<Readonly<Record<string, unknown>> | undefined> = [];

  applyGuardrailUsageState(state: Readonly<Record<string, unknown>>): void {
    this.appliedStates.push(state);
  }

  override async submit(event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    this.stateAtSubmit.push(this.appliedStates.at(-1));
    await super.submit(event, hooks);
  }
}

class FailingStartHarness extends EchoHarness {
  override async start(_input: SessionStartInput): Promise<void> {
    void _input;
    throw new Error('harness unavailable');
  }
}

class StopRecordingHarness extends EchoHarness {}

class UsageHarness extends EchoHarness {
  readonly guardrailUsageStates: Array<Readonly<Record<string, unknown>>> = [];

  applyGuardrailUsageState(state: Readonly<Record<string, unknown>>): void {
    this.guardrailUsageStates.push(state);
  }

  protected override afterAgentMessage(): void {
    this.emit({
      kind: 'agent.usage',
      payload: {
        model: 'claude-opus-5',
        usage: {
          cache_creation: {
            ephemeral_1h_input_tokens: 2,
            ephemeral_5m_input_tokens: 3,
          },
          cache_read_input_tokens: 5,
          input_tokens: 7,
          output_tokens: 11,
        },
      },
    });
  }
}

class PendingUsageHarness extends EchoHarness {
  private pendingUsage = true;

  hasPendingGuardrailUsage(): boolean {
    return this.pendingUsage;
  }

  releaseUsageLock(): void {
    this.pendingUsage = false;
  }
}

class FlatUsageHarness extends EchoHarness {
  protected override afterAgentMessage(): void {
    this.emit({
      kind: 'agent.usage',
      payload: {
        usage: {
          cache_creation_input_tokens: 19,
          cache_read_input_tokens: 5,
          input_tokens: 7,
          output_tokens: 11,
        },
      },
    });
  }
}

class BlockingStopHarness extends EchoHarness {
  stopStarted = false;
  private release: (() => void) | null = null;
  private readonly released = new Promise<void>((resolve) => {
    this.release = resolve;
  });

  override async stop(reason: TerminationReason): Promise<void> {
    this.stopStarted = true;
    await this.released;
    await super.stop(reason);
  }

  releaseStop(): void {
    this.release?.();
  }
}

class BlockingSubmitHarness extends EchoHarness {
  submitStarted = false;
  private release: (() => void) | null = null;
  private readonly submitted = new Promise<void>((resolve) => {
    this.release = resolve;
  });

  override async submit(event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    if (event.kind !== 'user.message') return;
    await hooks?.onAccepted();
    this.submitStarted = true;
    await this.submitted;
  }

  releaseSubmit(): void {
    this.release?.();
  }
}

class RequiredActionHarness extends EchoHarness {
  private pending = false;

  hasPendingRequiredAction(): boolean {
    return this.pending;
  }

  override async submit(event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    if (event.kind === 'user.message') {
      await hooks?.onAccepted();
      this.pending = true;
      this.emit({
        kind: 'agent.tool_use',
        id: 'evt_required_action',
        payload: {
          id: 'evt_required_action',
          name: 'bash',
          input: { command: 'echo ok' },
        },
      });
      this.emit({
        kind: 'session.status_idle',
        payload: {
          stop_reason: {
            type: 'requires_action',
            event_ids: ['evt_required_action'],
          },
        },
      });
      return;
    }
    if (event.kind === 'user.tool_confirmation') {
      await hooks?.onAccepted();
      this.pending = false;
      this.emit({
        kind: 'agent.message',
        payload: { content: [{ type: 'text', text: 'confirmed' }] },
      });
    }
  }
}

class ReplayDeferredMessageHarness extends EchoHarness {
  readonly submitted: UserEvent[] = [];
  readonly appliedStates: Array<Readonly<Record<string, unknown>>> = [];
  readonly stateAtUserMessageSubmit: Array<Readonly<Record<string, unknown>> | undefined> = [];
  private pending: boolean;
  private firstUserMessage: boolean;

  constructor(opts: { startWithPending?: boolean; firstUserMessage?: boolean } = {}) {
    super();
    this.pending = opts.startWithPending ?? false;
    this.firstUserMessage = opts.firstUserMessage ?? true;
  }

  hasPendingRequiredAction(): boolean {
    return this.pending;
  }

  applyGuardrailUsageState(state: Readonly<Record<string, unknown>>): void {
    this.appliedStates.push(state);
  }

  override async submit(event: UserEvent, hooks?: SubmitHooks): Promise<'deferred' | void> {
    this.submitted.push(event);
    if (event.kind === 'user.message') {
      this.stateAtUserMessageSubmit.push(this.appliedStates.at(-1));
    }
    if (event.kind === 'user.message' && this.pending) return 'deferred';
    if (event.kind === 'user.message' && this.firstUserMessage) {
      await hooks?.onAccepted();
      this.firstUserMessage = false;
      this.pending = true;
      this.emit({
        kind: 'agent.tool_use',
        id: 'evt_required_action',
        payload: {
          id: 'evt_required_action',
          name: 'bash',
          input: { command: 'echo ok' },
        },
      });
      this.emit({
        kind: 'session.status_idle',
        payload: {
          stop_reason: {
            type: 'requires_action',
            event_ids: ['evt_required_action'],
          },
        },
      });
      return;
    }
    if (event.kind === 'user.message') {
      await hooks?.onAccepted();
      return;
    }
    if (event.kind === 'user.tool_confirmation' || event.kind === 'user.custom_tool_result') {
      await hooks?.onAccepted();
      this.pending = false;
      this.emit({
        kind: 'agent.message',
        payload: { content: [{ type: 'text', text: 'confirmed' }] },
      });
      return;
    }
    if (event.kind === 'user.message') {
      await hooks?.onAccepted();
      this.emit({
        kind: 'agent.message',
        payload: { content: [{ type: 'text', text: 'replayed' }] },
      });
    }
  }
}

class DeferredSubmitHarness extends EchoHarness {
  readonly submitted: UserEvent[] = [];

  override async submit(event: UserEvent, _hooks?: SubmitHooks): Promise<'deferred' | void> {
    this.submitted.push(event);
    if (event.kind === 'user.message') return 'deferred';
  }
}

class UnappliedControlEventHarness extends EchoHarness {
  readonly submitted: UserEvent[] = [];

  override async submit(event: UserEvent, _hooks?: SubmitHooks): Promise<void> {
    this.submitted.push(event);
    throw new UnappliedUserEventError(event.kind, 'missing required action');
  }
}

class PolicyDenyHarness extends EchoHarness {
  override async submit(_event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    await hooks?.onAccepted();
    throw new GuardrailPolicyDeniedError(
      ['Sensitive request blocked.'],
      'Sensitive request blocked.',
    );
  }
}

class FailingDeferredDrainHarness extends EchoHarness {
  private pending = false;
  private firstUserMessage = true;

  hasPendingRequiredAction(): boolean {
    return this.pending;
  }

  override async submit(event: UserEvent, hooks?: SubmitHooks): Promise<'deferred' | void> {
    if (event.kind === 'user.message' && this.pending) return 'deferred';
    if (event.kind === 'user.message' && this.firstUserMessage) {
      await hooks?.onAccepted();
      this.firstUserMessage = false;
      this.pending = true;
      this.emit({
        kind: 'agent.tool_use',
        id: 'evt_required_action',
        payload: {
          id: 'evt_required_action',
          name: 'bash',
          input: { command: 'echo ok' },
        },
      });
      this.emit({
        kind: 'session.status_idle',
        payload: {
          stop_reason: {
            type: 'requires_action',
            event_ids: ['evt_required_action'],
          },
        },
      });
      return;
    }
    if (event.kind === 'user.tool_confirmation') {
      await hooks?.onAccepted();
      this.pending = false;
      this.emit({
        kind: 'agent.message',
        payload: { content: [{ type: 'text', text: 'confirmed' }] },
      });
      return;
    }
    if (event.kind === 'user.message') {
      await hooks?.onAccepted();
      throw new Error('deferred turn failed');
    }
  }
}

class RecordingRegistry {
  private readonly resources: SessionRecord['resources'];
  private readonly agentMetadata: Record<string, unknown> | undefined;
  private readonly environment: PreparedExecutionV2['environment'];
  private readonly memoryStores: Map<string, { id: string; name: string }>;
  private readonly memories: Map<string, MemoryRecord[]>;
  private readonly memoryContents: Map<string, Buffer | null>;
  private readonly failListMemoriesFor: Set<string>;
  private readonly subagents: AgentRecord[];
  private readonly guardrails: PreparedExecutionV2['guardrails'];
  private readonly preparedWorkspaceId: string;
  private readonly sessionWorkspaceId: string;
  private readonly omitSessionWorkspaceId: boolean;
  private readonly omitAgentWorkspaceId: boolean;
  readonly sessionOverrides: Partial<SessionRecord>;
  guardrailState: Record<string, unknown> | undefined;
  vaultCredentials: PreparedExecutionV2['vault_credentials'];
  sessionMissing: boolean;
  prepareError: Error | null = null;
  readonly prepareCalls: Array<{ workspaceId: string; sessionId: string }> = [];
  readonly states: Array<{
    sessionId: string;
    status: 'idle' | 'running' | 'rescheduling' | 'terminated';
    sandboxHandleId: string | null | undefined;
  }> = [];
  readonly usages: Array<{
    workspaceId: string;
    sessionId: string;
    model?: string;
    usage: {
      cache_creation?: {
        ephemeral_1h_input_tokens?: number;
        ephemeral_5m_input_tokens?: number;
      };
      cache_read_input_tokens?: number;
      input_tokens?: number;
      output_tokens?: number;
    };
  }> = [];
  private readonly agent: AgentRecord;

  constructor(opts?: {
    agent?: AgentRecord;
    resources?: SessionRecord['resources'];
    agentMetadata?: Record<string, unknown>;
    environment?: PreparedExecutionV2['environment'];
    memoryStores?: Map<string, { id: string; name: string }>;
    memories?: Map<string, MemoryRecord[]>;
    memoryContents?: Map<string, Buffer | null>;
    failListMemoriesFor?: Set<string>;
    subagents?: AgentRecord[];
    guardrails?: PreparedExecutionV2['guardrails'];
    preparedWorkspaceId?: string;
    sessionWorkspaceId?: string;
    omitSessionWorkspaceId?: boolean;
    omitAgentWorkspaceId?: boolean;
    session?: Partial<SessionRecord>;
    guardrailState?: Record<string, unknown>;
    vaultCredentials?: PreparedExecutionV2['vault_credentials'];
    sessionMissing?: boolean;
  }) {
    this.agent = opts?.agent ?? {
      id: 'agt_fake',
      name: 'fake agent',
      workspace_id: 'ws_pg_dispatcher',
      version: 1,
      model: { provider: 'anthropic', id: 'fake' },
      system: '',
      tools: [{ type: 'agent_toolset' }],
      mcp_servers: [],
      skills: [],
    };
    this.resources = opts?.resources ?? [];
    this.agentMetadata = opts?.agentMetadata;
    this.environment = opts?.environment ?? null;
    this.memoryStores = opts?.memoryStores ?? new Map();
    this.memories = opts?.memories ?? new Map();
    this.memoryContents = opts?.memoryContents ?? new Map();
    this.failListMemoriesFor = opts?.failListMemoriesFor ?? new Set();
    this.subagents = opts?.subagents ?? [];
    this.guardrails = opts?.guardrails;
    this.preparedWorkspaceId = opts?.preparedWorkspaceId ?? 'ws_pg_dispatcher';
    this.sessionWorkspaceId = opts?.sessionWorkspaceId ?? 'ws_pg_dispatcher';
    this.omitSessionWorkspaceId = opts?.omitSessionWorkspaceId ?? false;
    this.omitAgentWorkspaceId = opts?.omitAgentWorkspaceId ?? false;
    this.sessionOverrides = opts?.session ?? {};
    this.guardrailState = opts?.guardrailState;
    this.vaultCredentials = opts?.vaultCredentials ?? [];
    this.sessionMissing = opts?.sessionMissing ?? false;
  }

  executionOwner: 'registry' | 'harness-server' = 'harness-server';
  async getExecutionOwner(): Promise<'registry' | 'harness-server' | null> {
    return this.executionOwner;
  }

  async prepareExecution(input: {
    workspaceId: string;
    sessionId: string;
  }): Promise<PreparedExecutionV2 | null> {
    this.prepareCalls.push(input);
    if (this.prepareError) throw this.prepareError;
    if (this.sessionMissing) return null;
    const resources = (this.resources ?? []).map((resource) => {
      if (resource.type !== 'memory_store' || !resource.memory_store_id) return resource;
      const store = this.memoryStores.get(resource.memory_store_id);
      return store
        ? {
            ...resource,
            memory_store: {
              id: store.id,
              name: store.name,
              workspace_id: 'ws_pg_dispatcher',
            },
          }
        : resource;
    });
    const session: SessionRecord = {
      id: input.sessionId,
      agent_id: 'agt_fake',
      agent_version: 1,
      workspace_id: this.sessionWorkspaceId,
      vault_ids: [],
      resources,
      ...this.sessionOverrides,
    };
    if (this.omitSessionWorkspaceId) {
      delete (session as Partial<SessionRecord>).workspace_id;
    }
    const primaryAgent: AgentRecord = {
      ...this.agent,
      id: session.agent_id,
      version: session.agent_version,
      ...(this.agentMetadata ? { metadata: this.agentMetadata } : {}),
    };
    if (this.omitAgentWorkspaceId) {
      delete (primaryAgent as Partial<AgentRecord>).workspace_id;
    }
    return {
      schema_version: 2,
      workspace_id: this.preparedWorkspaceId,
      session,
      primary_agent: primaryAgent,
      subagents: this.subagents.map((agent) => ({ ...agent })),
      environment: this.environment,
      vault_credentials: this.vaultCredentials.map((credential) => ({ ...credential })),
      resources,
      ...(this.guardrails ? { guardrails: this.guardrails } : {}),
      ...(this.guardrailState ? { guardrail_state: this.guardrailState } : {}),
    };
  }

  async listSessionMemories(input: { storeId: string }): Promise<MemoryRecord[]> {
    if (this.failListMemoriesFor.has(input.storeId)) {
      throw new Error(`listSessionMemories ${input.storeId} failed`);
    }
    return this.memories.get(input.storeId) ?? [];
  }

  async getSessionMemoryContent(input: { memoryId: string }): Promise<Buffer | null> {
    if (!this.memoryContents.has(input.memoryId)) return Buffer.from('seed');
    return this.memoryContents.get(input.memoryId) ?? null;
  }

  async mintSessionJwt() {
    return { token: 'mcp-jwt', expiresAt: Math.floor(Date.now() / 1000) + 3600 };
  }

  async mintGitCredsJwt() {
    return { token: 'git-jwt' };
  }

  async updateSessionStateInternal(input: {
    sessionId: string;
    status: 'idle' | 'running' | 'rescheduling' | 'terminated';
    sandboxHandleId?: string | null;
  }): Promise<SessionRecord | null> {
    this.states.push({
      sessionId: input.sessionId,
      status: input.status,
      sandboxHandleId: input.sandboxHandleId,
    });
    return {
      id: input.sessionId,
      agent_id: 'agt_fake',
      agent_version: 1,
      workspace_id: 'ws_pg_dispatcher',
      status: input.status,
      sandbox_handle_id: input.sandboxHandleId ?? null,
      vault_ids: [],
      resources: [],
    };
  }

  async recordSessionUsageInternal(input: {
    workspaceId: string;
    sessionId: string;
    model?: string;
    usage: {
      cache_creation?: {
        ephemeral_1h_input_tokens?: number;
        ephemeral_5m_input_tokens?: number;
      };
      cache_read_input_tokens?: number;
      input_tokens?: number;
      output_tokens?: number;
    };
  }): Promise<SessionRecord | null> {
    this.usages.push(input);
    return {
      id: input.sessionId,
      agent_id: 'agt_fake',
      agent_version: 1,
      workspace_id: 'ws_pg_dispatcher',
      usage: input.usage,
      guardrail_usage_state: { total_tokens: 28, session_cost_usd: 0.25 },
      vault_ids: [],
      resources: [],
    };
  }
}

class BlockingRunningStateRegistry extends RecordingRegistry {
  runningUpdateStarted = false;
  private releaseRunning: (() => void) | null = null;
  private readonly runningReleased = new Promise<void>((resolve) => {
    this.releaseRunning = resolve;
  });

  override async updateSessionStateInternal(input: {
    sessionId: string;
    status: 'idle' | 'running' | 'rescheduling' | 'terminated';
    sandboxHandleId?: string | null;
  }): Promise<SessionRecord | null> {
    if (input.status === 'running') {
      this.runningUpdateStarted = true;
      await this.runningReleased;
    }
    return await super.updateSessionStateInternal(input);
  }

  releaseRunningUpdate(): void {
    this.releaseRunning?.();
  }
}

class RecordingSandboxRuntime implements SandboxRuntime {
  readonly capabilities: SandboxRuntime['capabilities'];
  readonly acquiredSpecs: EnvironmentSpec[] = [];
  readonly filesystemRootPreparations: string[][] = [];
  readonly sandboxFileWrites: string[] = [];
  readonly sandboxFileDeletes: string[] = [];
  readonly privilegedCommands: string[] = [];
  acquireCount = 0;
  destroyCount = 0;

  constructor(
    private readonly opts: {
      supportsFuse?: boolean;
      supportsLocalMemory?: boolean;
      supportsWritePolicy?: boolean;
      privilegedResult?: ToolResult;
      runResult?: ToolResult;
      acquireError?: Error;
      filesystemRootPreparationError?: Error;
    } = {},
  ) {
    this.capabilities = {
      supportsFuse: opts.supportsFuse ?? false,
      supportsLocalMemory: opts.supportsLocalMemory ?? true,
      supportsWritePolicy: opts.supportsWritePolicy ?? true,
    };
  }

  async acquire(_env: EnvironmentSpec): Promise<SandboxHandle> {
    this.acquiredSpecs.push(_env);
    if (this.opts.acquireError) throw this.opts.acquireError;
    this.acquireCount += 1;
    const id = `sbx_local_${this.acquireCount}`;
    return new RecordingSandboxHandle(
      id,
      () => {
        this.destroyCount += 1;
      },
      (paths) => {
        this.filesystemRootPreparations.push([...paths]);
        if (this.opts.filesystemRootPreparationError) {
          throw this.opts.filesystemRootPreparationError;
        }
      },
      (path) => this.sandboxFileWrites.push(path),
      (path) => this.sandboxFileDeletes.push(path),
      (command) => this.privilegedCommands.push(command),
      this.opts.privilegedResult,
      this.opts.runResult,
    );
  }
}

class RecordingSandboxHandle implements SandboxHandle {
  readonly files: SandboxHandle['files'];
  private destroyed = false;

  constructor(
    readonly id: string,
    private readonly onDestroy: () => void,
    private readonly onPrepareFilesystemRoots: (paths: readonly string[]) => void,
    onFileWrite: (path: string) => void,
    onFileDelete: (path: string) => void,
    private readonly onRunPrivileged: (command: string) => void,
    private readonly privilegedResult: ToolResult = { stdout: '', stderr: '', exit_code: 0 },
    private readonly runResult: ToolResult = { stdout: '', stderr: '', exit_code: 0 },
  ) {
    this.files = {
      async write(path: string, content: Buffer | NodeJS.ReadableStream): Promise<void> {
        void content;
        onFileWrite(path);
      },
      async read(path: string): Promise<Buffer> {
        void path;
        return Buffer.from('');
      },
      async readUtf8Page() {
        throw new Error('files.readUtf8Page not used');
      },
      async list(path: string): Promise<string[]> {
        void path;
        return [];
      },
      async chmod(path: string, mode: number): Promise<void> {
        void path;
        void mode;
      },
      async delete(path: string): Promise<void> {
        onFileDelete(path);
      },
    };
  }

  async run(_call: ToolCall): Promise<ToolResult> {
    void _call;
    return this.runResult;
  }

  async prepareFilesystemRoots(paths: readonly string[]): Promise<void> {
    this.onPrepareFilesystemRoots(paths);
  }

  async prepareWritePolicy(): Promise<void> {}

  async runWithWritePolicy(call: ToolCall): Promise<ToolResult> {
    return await this.run(call);
  }

  async canonicalizePathForPolicy(path: string): Promise<string> {
    return path;
  }

  async runPrivileged(_cmd: string): Promise<ToolResult> {
    this.onRunPrivileged(_cmd);
    return this.privilegedResult;
  }

  async pause(): Promise<void> {}

  async resume(): Promise<void> {}

  async destroy(): Promise<void> {
    if (this.destroyed) return;
    this.destroyed = true;
    this.onDestroy();
  }
}

class RecordingWorkDir {
  readonly released: Array<{ workspaceId: string; sessionId: string }> = [];

  async releaseSession(workspaceId: string, sessionId: string): Promise<void> {
    this.released.push({ workspaceId, sessionId });
  }
}

class StubFileStore implements FileStore {
  readonly openedFileIds: string[] = [];

  constructor(private readonly files = new Map<string, Buffer>()) {}

  async create(): Promise<never> {
    throw new Error('stub fileStore');
  }

  async get(): Promise<null> {
    return null;
  }

  async list(): Promise<{ items: []; nextCursor: null }> {
    return { items: [], nextCursor: null };
  }

  async open(_workspaceId: string, fileId: string): Promise<OpenStream | null> {
    void _workspaceId;
    this.openedFileIds.push(fileId);
    const content = this.files.get(fileId);
    if (!content) return null;
    return {
      stream: Readable.from(content),
      sizeBytes: content.length,
      sha256: 'a'.repeat(64),
    };
  }

  async archive(): Promise<void> {}

  async delete(): Promise<void> {}

  async close(): Promise<void> {}
}

async function putTestSkill(
  store: InMemorySkillStore,
  versionId: string,
  name: string,
): Promise<SkillDescriptor> {
  const record = await store.put('ws_pg_dispatcher', versionId, [
    { path: 'SKILL.md', content: Buffer.from(`---\nname: ${name}\n---\nInstructions`) },
  ]);
  return {
    id: versionId,
    skill_id: `skl_${name}`,
    source: 'custom',
    version_identifier: '1',
    name,
    description: `${name} description`,
    entrypoint: 'SKILL.md',
    package_sha256: record.sha256,
    package_size_bytes: record.sizeBytes,
  };
}

async function waitFor(predicate: () => boolean): Promise<void> {
  const started = Date.now();
  while (Date.now() - started < 1000) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error('condition not met');
}

function setupFailedPayload(store: RecordingStore): { phase?: string; error?: string } | null {
  // Setup failure is now the Claude pair session.error{type:'setup_failed'} +
  // session.status_idle{retries_exhausted}. The phase + message ride
  // on session.error.
  const event = store.appended.find((e) => {
    if (e.kind !== 'session.error') return false;
    const parsed = JSON.parse(Buffer.from(e.payload).toString('utf8')) as {
      error?: { type?: string };
    };
    return parsed.error?.type === 'setup_failed';
  });
  if (!event) return null;
  const parsed = JSON.parse(Buffer.from(event.payload).toString('utf8')) as {
    phase?: string;
    error?: { message?: string };
  };
  return { phase: parsed.phase, error: parsed.error?.message };
}

function sessionCreds() {
  return {
    accessKeyId: 'AKIA_TEST',
    secretAccessKey: 'secret',
    sessionToken: 'token',
    expiresAt: Math.floor(Date.now() / 1000) + 3600,
  };
}

function memoryRecord(id: string, path: string, storeId: string): MemoryRecord {
  return {
    id,
    storeId,
    path,
    currentSha256: 'sha256',
    sizeBytes: 4,
    updatedAt: new Date(),
    updatedBySessionId: null,
    updatedByEventId: null,
  };
}
