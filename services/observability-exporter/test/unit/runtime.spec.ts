// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type {
  KafkaReadOptions,
  KafkaSessionEventSource,
  KafkaTranscriptStore,
} from '@orca/transcript-store';
import type { Event } from '@orca/transcript-store-types';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type {
  ClaimedOutboxItem,
  ClaimedProjectionSession,
  ObservabilityExporterRepository,
} from '../../src/persistence.js';
import { ExporterLeaseLostError } from '../../src/persistence.js';
import { initialCanonicalProjectionState, reduceCanonicalEventBatch } from '../../src/projector.js';
import type {
  RegistryEnabledObservabilityContext,
  RegistryObservabilityClient,
} from '../../src/registry-client.js';
import {
  RegistryResolverHttpError,
  RegistryResolverResponseError,
  RegistryResolverScopeError,
} from '../../src/registry-client.js';
import {
  KafkaObservabilityExporterRuntime,
  selectCanonicalReplayEvents,
} from '../../src/runtime.js';
import { eventIdentityKey } from '../../src/event-identity.js';
import { hashTranscriptEnvelope } from '../../src/persistence.js';
import type { PinnedDeliveryContext } from '../../src/types.js';
import * as otlpMapper from '../../src/otlp-json.js';
import { parsePinnedDeliveryContext } from '../../src/canonical-validation.js';
import {
  TRANSCRIPT_SECRET,
  completedPrimaryTurnEvents,
  completedProjectedTrace,
  event,
} from '../support/events.js';

const deliveryContext: PinnedDeliveryContext = {
  organizationId: 'org_runtime',
  bindingId: 'aob_runtime',
  bindingVersion: 1,
  adapterType: 'otlp_http',
  endpointKind: 'traces_endpoint',
  endpointClass: 'public',
  endpointUrl: 'https://collector.example/api/public/otel/v1/traces',
  semanticProfile: 'langfuse',
  protocol: 'http/json',
  compression: 'none',
  timeoutMs: 1_000,
  captureMode: 'metadata_only',
  sampleRate: 1,
  configSchemaVersion: 1,
};

afterEach(() => {
  vi.useRealTimers();
});

describe('KafkaObservabilityExporterRuntime', () => {
  it.each([false, true])(
    'SQL runtime restarts retain pinned attribution without current enrichment (labels=%s)',
    async (labels) => {
      const pinned = {
        ...deliveryContext,
        ...(labels
          ? {
              agentId: 'agt_pinned',
              agentVersion: 7,
              harness: 'claude_code',
              harnessMode: 'colocated',
              environment: 'prod',
              release: 'v1.2.3',
            }
          : {}),
      };
      const persisted = JSON.stringify(outboxItem(pinned));
      const mapper = vi.spyOn(otlpMapper, 'encodeLangfuseOtlpJson');
      const fetchImpl = vi.fn<typeof fetch>(async () => Response.json({}));
      const resolveContext = vi.fn(async () => ({
        status: 'enabled' as const,
        deliveryContext: { ...deliveryContext, agentVersion: 99, release: 'new-release' },
      }));
      try {
        for (let restart = 0; restart < 2; restart++) {
          const item = JSON.parse(persisted) as ClaimedOutboxItem;
          item.deliveryContext = parsePinnedDeliveryContext(item.deliveryContext);
          const repository = {
            claimOutbox: vi.fn(async () => item),
            renewOutboxClaim: vi.fn(async () => undefined),
            completeOutboxDelivery: vi.fn(async () => undefined),
          } as unknown as ObservabilityExporterRepository;
          const runtime = runtimeFor({
            repository,
            otlpFetchImpl: fetchImpl,
            registryClient: { ...basicSecretClient(item), resolveContext },
          });
          await expect(runtime.deliverOnce()).resolves.toBe(true);
          expect(mapper).toHaveBeenLastCalledWith(item.trace, pinned);
          expect(repository.completeOutboxDelivery).toHaveBeenCalledOnce();
        }
        expect(resolveContext).not.toHaveBeenCalled();
        expect(fetchImpl.mock.calls[0]![1]!.body).toBe(fetchImpl.mock.calls[1]![1]!.body);
        expect(String(fetchImpl.mock.calls[0]![1]!.body)).not.toContain('new-release');
      } finally {
        mapper.mockRestore();
      }
    },
  );

  it('projects exact evaluator pairs through runtime without persisting explanation or usage', async () => {
    const events = [
      event(1, 'user.message', {}, { id: 'evt_turn', producedBy: 'client' }),
      event(2, 'session.user_event_processed', { user_event_id: 'evt_turn' }),
      event(3, 'session.status_running'),
      event(
        4,
        'span.outcome_evaluation_start',
        { outcome_id: 'outcome_1', iteration: 0 },
        { id: 'evt_eval_first' },
      ),
      event(
        5,
        'span.outcome_evaluation_start',
        { outcome_id: 'outcome_2', iteration: 0 },
        { id: 'evt_eval_second' },
      ),
      // First-in-first-out, not the latest open evaluator. Ongoing must not create a child.
      event(6, 'span.outcome_evaluation_ongoing', {
        outcome_evaluation_start_id: 'evt_eval_first',
        explanation: TRANSCRIPT_SECRET,
      }),
      event(7, 'span.outcome_evaluation_end', {
        outcome_evaluation_start_id: 'evt_eval_first',
        outcome_id: 'outcome_1',
        iteration: 0,
        result: 'satisfied',
        explanation: TRANSCRIPT_SECRET,
        usage: { input_tokens: 123, output_tokens: 456 },
      }),
      event(8, 'span.outcome_evaluation_end', {
        outcome_evaluation_start_id: 'evt_eval_second',
        outcome_id: 'outcome_2',
        iteration: 0,
        result: 'needs_revision',
        explanation: TRANSCRIPT_SECRET,
        usage: { input_tokens: 123, output_tokens: 456 },
      }),
      event(9, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ];
    const repository = projectionRepository(projectionClaim(events[0]!));
    repository.completeProjection = vi.fn(async () => undefined);
    const runtime = runtimeFor({ repository, transcriptStore: transcriptReader(events) });
    await expect(runtime.projectOnce()).resolves.toBe(true);
    const complete = vi.mocked(repository.completeProjection).mock.calls[0]!;
    expect(complete[1]).toBe('10');
    expect(complete[3]).toHaveLength(1);
    const trace = complete[3][0]!.trace;
    expect(trace.spans).toHaveLength(2);
    expect(trace.spans).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          observationType: 'outcome_evaluation',
          sourceEventId: 'evt_eval_first',
          startedAt: events[3]!.producedAt,
          endedAt: events[6]!.producedAt,
          parentSpanId: trace.root.spanId,
        }),
        expect.objectContaining({
          observationType: 'outcome_evaluation',
          sourceEventId: 'evt_eval_second',
          startedAt: events[4]!.producedAt,
          endedAt: events[7]!.producedAt,
          parentSpanId: trace.root.spanId,
        }),
      ]),
    );
    expect(JSON.stringify(complete.slice(2, 4))).not.toContain(TRANSCRIPT_SECRET);
    expect(JSON.stringify(trace)).not.toMatch(
      /explanation|input_tokens|output_tokens|modelSummary/,
    );
  });

  it('replays authoritative Kafka events with subpath=* and writes only completed canonical traces', async () => {
    const events = completedPrimaryTurnEvents();
    const claim: ClaimedProjectionSession = {
      workspaceId: events[0]!.workspaceId,
      sessionId: events[0]!.sessionId,
      nextSeq: '0',
      firstPendingSeq: '0',
      state: initialCanonicalProjectionState(),
      leaseOwner: 'worker-projector',
      leaseGeneration: '1',
    };
    const read = vi.fn(async function* (): AsyncIterable<Event> {
      for (const event of events) yield event;
    });
    const repository = {
      claimSession: vi.fn(async () => claim),
      loadInboxEventIdentities: vi.fn(async () => []),
      loadAcceptedSourceIds: vi.fn(async () => new Set<string>()),
      renewProjectionClaim: vi.fn(async () => undefined),
      completeProjection: vi.fn(async () => undefined),
      releaseProjectionClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const registryClient = {
      resolveContext: vi.fn(async () => enabledContext()),
    } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
    const runtime = runtimeFor({
      repository,
      transcriptStore: { read } as unknown as Pick<KafkaTranscriptStore, 'read'>,
      registryClient,
    });

    await expect(runtime.projectOnce()).resolves.toBe(true);

    expect(read).toHaveBeenCalledWith(claim.workspaceId, claim.sessionId, {
      fromCursor: '0',
      maxEvents: 1_000,
      maxBytes: 8 * 1024 * 1024,
      onScannedCursor: expect.any(Function),
      subpath: '*',
      signal: expect.any(AbortSignal),
    });
    expect(vi.mocked(repository.loadAcceptedSourceIds)).toHaveBeenCalledWith(
      claim,
      expect.arrayContaining(['evt_user_turn', 'evt_queued']),
    );
    const complete = vi.mocked(repository.completeProjection).mock.calls[0]!;
    expect(complete[1]).toBe('9');
    expect(complete[3]).toHaveLength(1);
    expect(complete[4]).toEqual(['evt_user_turn']);
    expect(JSON.stringify(complete[2])).not.toContain(TRANSCRIPT_SECRET);
    expect(JSON.stringify(complete[3])).not.toContain(TRANSCRIPT_SECRET);
  });

  it('advances across a scanned poison-offset gap to the first pending canonical event', async () => {
    const claim: ClaimedProjectionSession = {
      workspaceId: 'ws_gap',
      sessionId: 'ses_gap',
      nextSeq: '0',
      firstPendingSeq: '1',
      state: initialCanonicalProjectionState(),
      leaseOwner: 'worker-projector',
      leaseGeneration: '1',
    };
    const repository = {
      claimSession: vi.fn(async () => claim),
      loadInboxEventIdentities: vi.fn(async () => []),
      loadAcceptedSourceIds: vi.fn(async () => new Set<string>()),
      renewProjectionClaim: vi.fn(async () => undefined),
      completeProjection: vi.fn(async () => undefined),
      releaseProjectionClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({ repository });

    await expect(runtime.projectOnce()).resolves.toBe(true);
    expect(vi.mocked(repository.completeProjection)).toHaveBeenCalledWith(
      claim,
      '1',
      initialCanonicalProjectionState(),
      [],
      [],
    );
  });

  it('advances only through raw records admitted by the Kafka byte budget', async () => {
    const claim: ClaimedProjectionSession = {
      workspaceId: 'ws_budget_gap',
      sessionId: 'ses_budget_gap',
      nextSeq: '0',
      firstPendingSeq: '2',
      state: initialCanonicalProjectionState(),
      leaseOwner: 'worker-projector',
      leaseGeneration: '1',
    };
    const repository = {
      claimSession: vi.fn(async () => claim),
      loadInboxEventIdentities: vi.fn(async () => []),
      loadAcceptedSourceIds: vi.fn(async () => new Set<string>()),
      renewProjectionClaim: vi.fn(async () => undefined),
      completeProjection: vi.fn(async () => undefined),
      releaseProjectionClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({
      repository,
      transcriptStore: {
        read: async function* (
          _workspaceId: string,
          _sessionId: string,
          opts: KafkaReadOptions,
        ): AsyncIterable<Event> {
          opts.onScannedCursor?.('1');
          yield* [];
        },
      } as unknown as Pick<KafkaTranscriptStore, 'read'>,
    });

    await expect(runtime.projectOnce()).resolves.toBe(true);
    expect(vi.mocked(repository.completeProjection)).toHaveBeenCalledWith(
      claim,
      '1',
      initialCanonicalProjectionState(),
      [],
      [],
    );
  });

  it('quarantines a permanently unreadable claimed cursor instead of reclaiming it', async () => {
    const source = completedPrimaryTurnEvents()[0]!;
    const claim = { ...projectionClaim(source), nextSeq: '1', firstPendingSeq: '1' };
    const repository = projectionRepository(claim);
    const runtime = runtimeFor({ repository });

    await expect(runtime.projectOnce()).rejects.toThrow(
      'claimed Transcript cursor was not readable',
    );
    expect(vi.mocked(repository.quarantineProjectionClaim)).toHaveBeenCalledWith(
      claim,
      'transcript_cursor_unreadable',
    );
    expect(vi.mocked(repository.releaseProjectionClaim)).not.toHaveBeenCalled();
  });

  it('renews the projection lease while Kafka replay remains in flight', async () => {
    vi.useFakeTimers();
    const events = completedPrimaryTurnEvents();
    const claim: ClaimedProjectionSession = {
      workspaceId: events[0]!.workspaceId,
      sessionId: events[0]!.sessionId,
      nextSeq: '0',
      firstPendingSeq: '0',
      state: initialCanonicalProjectionState(),
      leaseOwner: 'worker-projector',
      leaseGeneration: '1',
    };
    let releaseReplay!: () => void;
    const replayGate = new Promise<void>((resolve) => {
      releaseReplay = resolve;
    });
    const repository = {
      claimSession: vi.fn(async () => claim),
      loadInboxEventIdentities: vi.fn(async () => []),
      loadAcceptedSourceIds: vi.fn(async () => new Set<string>()),
      renewProjectionClaim: vi.fn(async () => undefined),
      completeProjection: vi.fn(async () => undefined),
      releaseProjectionClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({
      repository,
      transcriptStore: {
        read: async function* (): AsyncIterable<Event> {
          await replayGate;
          yield* events;
        },
      } as unknown as Pick<KafkaTranscriptStore, 'read'>,
      registryClient: {
        resolveContext: vi.fn(async () => enabledContext()),
      } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>,
    });

    const projecting = runtime.projectOnce();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(vi.mocked(repository.renewProjectionClaim)).toHaveBeenCalledWith(claim, 30_000);
    releaseReplay();
    await projecting;
  });

  it('stops stale projection work after a heartbeat loses the lease', async () => {
    vi.useFakeTimers();
    const source = completedPrimaryTurnEvents()[0]!;
    const claim: ClaimedProjectionSession = {
      workspaceId: source.workspaceId,
      sessionId: source.sessionId,
      nextSeq: '0',
      firstPendingSeq: '0',
      state: initialCanonicalProjectionState(),
      leaseOwner: 'worker-projector',
      leaseGeneration: '1',
    };
    const repository = {
      claimSession: vi.fn(async () => claim),
      loadInboxEventIdentities: vi.fn(async () => []),
      loadAcceptedSourceIds: vi.fn(async () => new Set<string>()),
      renewProjectionClaim: vi.fn(async () => {
        throw new ExporterLeaseLostError('session');
      }),
      releaseProjectionClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({
      repository,
      transcriptStore: {
        read: async function* (
          _workspaceId: string,
          _sessionId: string,
          opts: KafkaReadOptions,
        ): AsyncIterable<Event> {
          await waitForAbort(opts.signal);
          if (!opts.signal?.aborted) yield source;
        },
      } as unknown as Pick<KafkaTranscriptStore, 'read'>,
    });

    const projecting = runtime.projectOnce();
    const rejected = expect(projecting).rejects.toBeInstanceOf(ExporterLeaseLostError);
    await vi.advanceTimersByTimeAsync(10_000);
    await rejected;
    expect(vi.mocked(repository.releaseProjectionClaim)).not.toHaveBeenCalled();
  });

  it('preserves lease loss when heartbeat aborts Registry context resolution', async () => {
    vi.useFakeTimers();
    const events = completedPrimaryTurnEvents();
    const claim = projectionClaim(events[0]!);
    const repository = {
      claimSession: vi.fn(async () => claim),
      loadInboxEventIdentities: vi.fn(async () => []),
      loadAcceptedSourceIds: vi.fn(async () => new Set<string>()),
      renewProjectionClaim: vi.fn(async () => {
        throw new ExporterLeaseLostError('session');
      }),
      releaseProjectionClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const registryClient = {
      resolveContext: vi.fn(async (input: { signal?: AbortSignal }) => {
        await waitForAbort(input.signal);
        throw new RegistryResolverHttpError('context', 503);
      }),
    } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
    const runtime = runtimeFor({
      repository,
      transcriptStore: transcriptReader(events),
      registryClient,
    });

    const projecting = runtime.projectOnce();
    const rejected = expect(projecting).rejects.toBeInstanceOf(ExporterLeaseLostError);
    await vi.advanceTimersByTimeAsync(10_000);
    await rejected;

    expect(vi.mocked(repository.releaseProjectionClaim)).not.toHaveBeenCalled();
  });

  it('aborts in-flight Kafka replay, releases its lease, and stops the heartbeat', async () => {
    vi.useFakeTimers();
    const source = completedPrimaryTurnEvents()[0]!;
    const claim = projectionClaim(source);
    const controller = new AbortController();
    let markReplayStarted!: () => void;
    const replayStarted = new Promise<void>((resolve) => {
      markReplayStarted = resolve;
    });
    const read = vi.fn(async function* (
      _workspaceId: string,
      _sessionId: string,
      opts: KafkaReadOptions,
    ): AsyncIterable<Event> {
      markReplayStarted();
      await waitForAbort(opts.signal);
      if (opts.signal?.aborted) return;
      yield source;
    });
    const repository = {
      claimSession: vi.fn(async () => claim),
      claimOutbox: vi.fn(async () => null),
      renewProjectionClaim: vi.fn(async () => undefined),
      releaseProjectionClaim: vi.fn(async () => undefined),
      completeProjection: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({
      repository,
      transcriptStore: { read } as unknown as Pick<KafkaTranscriptStore, 'read'>,
    });

    const running = runtime.run(controller.signal);
    await replayStarted;
    controller.abort();

    await expect(running).resolves.toBeUndefined();
    expect(vi.mocked(repository.releaseProjectionClaim)).toHaveBeenCalledWith(claim);
    expect(vi.mocked(repository.completeProjection)).not.toHaveBeenCalled();
    expect(read.mock.calls[0]?.[2]).toMatchObject({
      maxEvents: 1_000,
      maxBytes: 8 * 1024 * 1024,
      signal: expect.objectContaining({ aborted: true }),
    });
    await vi.advanceTimersByTimeAsync(30_000);
    expect(vi.mocked(repository.renewProjectionClaim)).not.toHaveBeenCalled();
  });

  it('aborts in-flight Registry context resolution and releases the projection lease', async () => {
    const events = completedPrimaryTurnEvents();
    const claim = projectionClaim(events[0]!);
    const controller = new AbortController();
    let markResolverStarted!: () => void;
    const resolverStarted = new Promise<void>((resolve) => {
      markResolverStarted = resolve;
    });
    const repository = projectionRepository(claim);
    const registryClient = {
      resolveContext: vi.fn(async (input: { signal?: AbortSignal }) => {
        markResolverStarted();
        await waitForAbort(input.signal);
        throw input.signal?.reason;
      }),
    } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
    const runtime = runtimeFor({
      repository,
      transcriptStore: transcriptReader(events),
      registryClient,
    });

    const projecting = runtime.projectOnce(controller.signal);
    await resolverStarted;
    controller.abort();

    await expect(projecting).rejects.toBeDefined();
    expect(vi.mocked(repository.releaseProjectionClaim)).toHaveBeenCalledWith(claim);
    expect(vi.mocked(repository.quarantineProjectionClaim)).not.toHaveBeenCalled();
  });

  it('passes shutdown to selection and releases a claim committed during cancellation', async () => {
    const item = outboxItem();
    const controller = new AbortController();
    const repository = {
      claimOutbox: vi.fn(async () => {
        controller.abort(new Error('test shutdown'));
        return item;
      }),
      releaseOutboxClaim: vi.fn(async () => undefined),
      scheduleOutboxRetry: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const registryClient = basicSecretClient(item);
    const runtime = runtimeFor({ repository, registryClient });

    await expect(runtime.deliverOnce(controller.signal)).rejects.toThrow('test shutdown');
    expect(repository.claimOutbox).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(Number),
      controller.signal,
    );
    expect(repository.releaseOutboxClaim).toHaveBeenCalledWith(item);
    expect(repository.scheduleOutboxRetry).not.toHaveBeenCalled();
    expect(registryClient.resolveSecret).not.toHaveBeenCalled();
  });

  it('aborts in-flight OTLP delivery, releases its lease, and lets run return', async () => {
    vi.useFakeTimers();
    const item = outboxItem();
    const controller = new AbortController();
    let markSendStarted!: () => void;
    const sendStarted = new Promise<void>((resolve) => {
      markSendStarted = resolve;
    });
    let requestSignal: AbortSignal | undefined;
    const repository = {
      claimSession: vi.fn(async () => null),
      claimOutbox: vi.fn(async () => item),
      renewOutboxClaim: vi.fn(async () => undefined),
      releaseOutboxClaim: vi.fn(async () => undefined),
      scheduleOutboxRetry: vi.fn(async () => undefined),
      completeOutboxDelivery: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const fetchImpl = vi.fn(
      async (_input: string | URL | Request, init?: RequestInit): Promise<Response> => {
        requestSignal = init?.signal ?? undefined;
        markSendStarted();
        await waitForAbort(requestSignal);
        throw requestSignal?.reason;
      },
    );
    const runtime = runtimeFor({
      repository,
      registryClient: basicSecretClient(item),
      otlpFetchImpl: fetchImpl,
    });

    const running = runtime.run(controller.signal);
    await sendStarted;
    controller.abort();

    await expect(running).resolves.toBeUndefined();
    expect(requestSignal?.aborted).toBe(true);
    expect(vi.mocked(repository.releaseOutboxClaim)).toHaveBeenCalledWith(item);
    expect(vi.mocked(repository.scheduleOutboxRetry)).not.toHaveBeenCalled();
    expect(vi.mocked(repository.completeOutboxDelivery)).not.toHaveBeenCalled();
  });

  it('uses one freshly authorized Registry credential for one Langfuse request', async () => {
    const item = outboxItem();
    const repository = {
      claimOutbox: vi.fn(async () => item),
      renewOutboxClaim: vi.fn(async () => undefined),
      completeOutboxDelivery: vi.fn(async () => undefined),
      markOutboxSuppressed: vi.fn(async () => undefined),
      releaseOutboxClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const registryClient = {
      resolveSecret: vi.fn(async () => ({
        bindingId: item.deliveryContext.bindingId,
        bindingVersion: item.deliveryContext.bindingVersion,
        effectiveCaptureMode: 'metadata_only' as const,
        auth: { type: 'basic' as const, username: 'pk-runtime', password: 'sk-runtime' },
      })),
    } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
    const fetchImpl = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      const headers = new Headers(init?.headers);
      expect(headers.get('authorization')).toBe(
        `Basic ${Buffer.from('pk-runtime:sk-runtime').toString('base64')}`,
      );
      expect(headers.get('x-langfuse-ingestion-version')).toBe('4');
      expect(String(init?.body)).not.toContain(TRANSCRIPT_SECRET);
      return new Response('{}', {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    });
    const runtime = runtimeFor({ repository, registryClient, otlpFetchImpl: fetchImpl });

    await expect(runtime.deliverOnce()).resolves.toBe(true);
    expect(fetchImpl).toHaveBeenCalledOnce();
    expect(vi.mocked(repository.renewOutboxClaim)).toHaveBeenCalledWith(item, 6_000);
    expect(vi.mocked(repository.completeOutboxDelivery)).toHaveBeenCalledWith(item, {
      kind: 'accepted',
    });
    expect(vi.mocked(repository.releaseOutboxClaim)).not.toHaveBeenCalled();
  });

  it.each([401, 403])(
    'fresh-resolves Registry credentials once after OTLP HTTP %s and can recover',
    async (status) => {
      const item = outboxItem();
      const repository = {
        claimOutbox: vi.fn(async () => item),
        renewOutboxClaim: vi.fn(async () => undefined),
        completeOutboxDelivery: vi.fn(async () => undefined),
        markOutboxSuppressed: vi.fn(async () => undefined),
        releaseOutboxClaim: vi.fn(async () => undefined),
        scheduleOutboxRetry: vi.fn(async () => undefined),
      } as unknown as ObservabilityExporterRepository;
      const credentials = [
        { username: 'pk-stale', password: 'sk-stale' },
        { username: 'pk-fresh', password: 'sk-fresh' },
      ];
      const registryClient = {
        resolveSecret: vi.fn(async () => ({
          bindingId: item.deliveryContext.bindingId,
          bindingVersion: item.deliveryContext.bindingVersion,
          effectiveCaptureMode: 'metadata_only' as const,
          auth: { type: 'basic' as const, ...credentials.shift()! },
        })),
      } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
      const authorizations: string[] = [];
      const fetchImpl = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
        authorizations.push(new Headers(init?.headers).get('authorization') ?? '');
        if (authorizations.length === 1) return new Response('', { status });
        return new Response('{}', {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      });
      const runtime = runtimeFor({ repository, registryClient, otlpFetchImpl: fetchImpl });

      await expect(runtime.deliverOnce()).resolves.toBe(true);
      expect(vi.mocked(registryClient.resolveSecret)).toHaveBeenCalledTimes(2);
      expect(authorizations).toEqual([
        `Basic ${Buffer.from('pk-stale:sk-stale').toString('base64')}`,
        `Basic ${Buffer.from('pk-fresh:sk-fresh').toString('base64')}`,
      ]);
      expect(vi.mocked(repository.renewOutboxClaim).mock.calls).toEqual([
        [item, 6_000],
        [item, 7_000],
        [item, 6_000],
      ]);
      expect(vi.mocked(repository.completeOutboxDelivery)).toHaveBeenCalledWith(item, {
        kind: 'accepted',
      });
      expect(vi.mocked(repository.markOutboxSuppressed)).not.toHaveBeenCalled();
      expect(vi.mocked(repository.scheduleOutboxRetry)).not.toHaveBeenCalled();
    },
  );

  it.each([401, 403])(
    'suppresses repeated OTLP HTTP %s after exactly one fresh credential resolve',
    async (status) => {
      const item = outboxItem();
      const repository = {
        claimOutbox: vi.fn(async () => item),
        renewOutboxClaim: vi.fn(async () => undefined),
        completeOutboxDelivery: vi.fn(async () => undefined),
        markOutboxSuppressed: vi.fn(async () => undefined),
        releaseOutboxClaim: vi.fn(async () => undefined),
        scheduleOutboxRetry: vi.fn(async () => undefined),
      } as unknown as ObservabilityExporterRepository;
      const registryClient = basicSecretClient(item);
      const fetchImpl = vi.fn(async () => new Response('', { status }));
      const runtime = runtimeFor({ repository, registryClient, otlpFetchImpl: fetchImpl });

      await expect(runtime.deliverOnce()).resolves.toBe(true);
      expect(vi.mocked(registryClient.resolveSecret)).toHaveBeenCalledTimes(2);
      expect(fetchImpl).toHaveBeenCalledTimes(2);
      expect(vi.mocked(repository.renewOutboxClaim).mock.calls).toEqual([
        [item, 6_000],
        [item, 7_000],
        [item, 6_000],
      ]);
      expect(vi.mocked(repository.markOutboxSuppressed)).toHaveBeenCalledWith(
        item,
        'credential_rejected',
      );
      expect(vi.mocked(repository.completeOutboxDelivery)).not.toHaveBeenCalled();
      expect(vi.mocked(repository.scheduleOutboxRetry)).not.toHaveBeenCalled();
    },
  );

  it.each([
    ['binding id', { bindingId: 'aob_other' }, 'binding_mismatch'],
    ['binding version', { bindingVersion: 2 }, 'binding_mismatch'],
    ['effective capture mode', { effectiveCaptureMode: 'raw_io' }, 'capture_mode_mismatch'],
    ['credential type', { auth: { type: 'unsupported' } }, 'unsupported_credential'],
  ] as const)(
    'suppresses mismatched Registry %s before sending',
    async (_kind, override, reason) => {
      const item = outboxItem();
      const repository = {
        claimOutbox: vi.fn(async () => item),
        renewOutboxClaim: vi.fn(async () => undefined),
        markOutboxSuppressed: vi.fn(async () => undefined),
      } as unknown as ObservabilityExporterRepository;
      const registryClient = {
        resolveSecret: vi.fn(async () => ({
          bindingId: item.deliveryContext.bindingId,
          bindingVersion: item.deliveryContext.bindingVersion,
          effectiveCaptureMode: 'metadata_only' as const,
          auth: { type: 'basic' as const, username: 'pk-other', password: 'sk-other' },
          ...override,
        })),
      } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
      const runtime = runtimeFor({ repository, registryClient });

      await expect(runtime.deliverOnce()).resolves.toBe(true);
      expect(vi.mocked(repository.markOutboxSuppressed)).toHaveBeenCalledWith(item, reason);
      expect(vi.mocked(repository.renewOutboxClaim)).not.toHaveBeenCalled();
    },
  );

  it('suppresses an outbox row with a locally invalid Registry scope', async () => {
    const item = outboxItem();
    const repository = {
      claimOutbox: vi.fn(async () => item),
      markOutboxSuppressed: vi.fn(async () => undefined),
      releaseOutboxClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const registryClient = {
      resolveSecret: vi.fn(async () => {
        throw new RegistryResolverScopeError();
      }),
    } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
    const runtime = runtimeFor({ repository, registryClient });

    await expect(runtime.deliverOnce()).resolves.toBe(true);
    expect(vi.mocked(repository.markOutboxSuppressed)).toHaveBeenCalledWith(
      item,
      'registry_scope_invalid',
    );
    expect(vi.mocked(repository.releaseOutboxClaim)).not.toHaveBeenCalled();
  });

  it.each([
    { sampleRate: 0, count: 0 },
    { sampleRate: 0.5, count: 1 },
    { sampleRate: 1, count: 1 },
  ])(
    'applies rate $sampleRate before projection and writes only selected traces',
    async ({ sampleRate, count }) => {
      const events = completedPrimaryTurnEvents();
      const claim: ClaimedProjectionSession = {
        workspaceId: events[0]!.workspaceId,
        sessionId: events[0]!.sessionId,
        nextSeq: '0',
        firstPendingSeq: '0',
        state: initialCanonicalProjectionState(),
        leaseOwner: 'worker-projector',
        leaseGeneration: '1',
      };
      const repository = {
        claimSession: vi.fn(async () => claim),
        loadInboxEventIdentities: vi.fn(async () => []),
        loadAcceptedSourceIds: vi.fn(async () => new Set<string>()),
        renewProjectionClaim: vi.fn(async () => undefined),
        completeProjection: vi.fn(async () => undefined),
        releaseProjectionClaim: vi.fn(async () => undefined),
        claimOutbox: vi.fn(async () => null),
      } as unknown as ObservabilityExporterRepository;
      const registryClient = {
        resolveContext: vi.fn(async () => ({
          ...enabledContext(),
          deliveryContext: { ...deliveryContext, sampleRate },
        })),
        resolveSecret: vi.fn(),
      } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
      const otlpFetchImpl = vi.fn<typeof fetch>();
      const runtime = runtimeFor({
        repository,
        transcriptStore: {
          read: async function* (): AsyncIterable<Event> {
            yield* events;
          },
        } as unknown as Pick<KafkaTranscriptStore, 'read'>,
        registryClient,
        otlpFetchImpl,
      });

      await expect(runtime.projectOnce()).resolves.toBe(true);
      const complete = vi.mocked(repository.completeProjection).mock.calls[0]!;
      expect(complete[1]).toBe('9');
      expect(complete[2].sampling?.policy.sampleRate).toBe(sampleRate);
      expect(complete[3]).toHaveLength(count);
      if (count === 0) {
        expect(complete[2].sampling?.suppressed).toMatchObject({
          reason: 'sampled_out',
          turnCount: '1',
        });
        await expect(runtime.deliverOnce()).resolves.toBe(false);
      }
      expect(registryClient.resolveSecret).not.toHaveBeenCalled();
      expect(otlpFetchImpl).not.toHaveBeenCalled();
    },
  );

  it('quarantines a typed canonical projection failure instead of hot-looping it', async () => {
    const marker = completedPrimaryTurnEvents()[2]!;
    const claim: ClaimedProjectionSession = {
      workspaceId: marker.workspaceId,
      sessionId: marker.sessionId,
      nextSeq: '0',
      firstPendingSeq: '0',
      state: initialCanonicalProjectionState(),
      leaseOwner: 'worker-projector',
      leaseGeneration: '1',
    };
    const repository = {
      claimSession: vi.fn(async () => claim),
      loadInboxEventIdentities: vi.fn(async () => []),
      loadAcceptedSourceIds: vi.fn(async () => new Set<string>()),
      renewProjectionClaim: vi.fn(async () => undefined),
      quarantineProjectionClaim: vi.fn(async () => undefined),
      releaseProjectionClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({
      repository,
      transcriptStore: {
        read: async function* (): AsyncIterable<Event> {
          yield marker;
        },
      } as unknown as Pick<KafkaTranscriptStore, 'read'>,
    });

    await expect(runtime.projectOnce()).rejects.toThrow('acceptance issue');
    expect(vi.mocked(repository.quarantineProjectionClaim)).toHaveBeenCalledWith(
      claim,
      'canonical_projection_error',
    );
    expect(vi.mocked(repository.releaseProjectionClaim)).not.toHaveBeenCalled();
  });

  it.each([400, 404])(
    'quarantines terminal Registry context HTTP %s instead of reclaiming the Session',
    async (status) => {
      const events = completedPrimaryTurnEvents();
      const claim = projectionClaim(events[0]!);
      const repository = projectionRepository(claim);
      const registryClient = {
        resolveContext: vi.fn(async () => {
          throw new RegistryResolverHttpError('context', status);
        }),
      } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
      const runtime = runtimeFor({
        repository,
        transcriptStore: transcriptReader(events),
        registryClient,
      });

      await expect(runtime.projectOnce()).rejects.toMatchObject({ resolver: 'context', status });
      expect(vi.mocked(repository.quarantineProjectionClaim)).toHaveBeenCalledWith(
        claim,
        'registry_context_rejected',
      );
      expect(vi.mocked(repository.releaseProjectionClaim)).not.toHaveBeenCalled();
    },
  );

  it.each([401, 403, 503])('keeps Registry context HTTP %s retryable', async (status) => {
    const events = completedPrimaryTurnEvents();
    const claim = projectionClaim(events[0]!);
    const repository = projectionRepository(claim);
    const registryClient = {
      resolveContext: vi.fn(async () => {
        throw new RegistryResolverHttpError('context', status);
      }),
    } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
    const runtime = runtimeFor({
      repository,
      transcriptStore: transcriptReader(events),
      registryClient,
    });

    await expect(runtime.projectOnce()).rejects.toMatchObject({ resolver: 'context', status });
    expect(vi.mocked(repository.releaseProjectionClaim)).toHaveBeenCalledWith(claim);
    expect(vi.mocked(repository.quarantineProjectionClaim)).not.toHaveBeenCalled();
  });

  it('quarantines a locally invalid Registry scope instead of reclaiming the Session', async () => {
    const events = completedPrimaryTurnEvents();
    const claim = projectionClaim(events[0]!);
    const repository = projectionRepository(claim);
    const registryClient = {
      resolveContext: vi.fn(async () => {
        throw new RegistryResolverScopeError();
      }),
    } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
    const runtime = runtimeFor({
      repository,
      transcriptStore: transcriptReader(events),
      registryClient,
    });

    await expect(runtime.projectOnce()).rejects.toBeInstanceOf(RegistryResolverScopeError);
    expect(vi.mocked(repository.quarantineProjectionClaim)).toHaveBeenCalledWith(
      claim,
      'registry_scope_invalid',
    );
    expect(vi.mocked(repository.releaseProjectionClaim)).not.toHaveBeenCalled();
  });

  it('suppresses partial rejection without retrying the whole OTLP request', async () => {
    const item = outboxItem();
    const repository = {
      claimOutbox: vi.fn(async () => item),
      renewOutboxClaim: vi.fn(async () => undefined),
      completeOutboxDelivery: vi.fn(async () => undefined),
      markOutboxSuppressed: vi.fn(async () => undefined),
      releaseOutboxClaim: vi.fn(async () => undefined),
      scheduleOutboxRetry: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const registryClient = {
      resolveSecret: vi.fn(async () => ({
        bindingId: item.deliveryContext.bindingId,
        bindingVersion: item.deliveryContext.bindingVersion,
        effectiveCaptureMode: 'metadata_only' as const,
        auth: { type: 'basic' as const, username: 'pk-runtime', password: 'sk-runtime' },
      })),
    } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
    const runtime = runtimeFor({
      repository,
      registryClient,
      otlpFetchImpl: async () =>
        new Response(JSON.stringify({ partialSuccess: { rejectedSpans: 1 } }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
    });

    await expect(runtime.deliverOnce()).resolves.toBe(true);
    expect(vi.mocked(repository.completeOutboxDelivery)).toHaveBeenCalledWith(item, {
      kind: 'partial_rejection',
      rejectedSpans: '1',
      messageBytes: 0,
      messageSha256: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    });
    expect(vi.mocked(repository.markOutboxSuppressed)).not.toHaveBeenCalled();
    expect(vi.mocked(repository.releaseOutboxClaim)).not.toHaveBeenCalled();
    expect(vi.mocked(repository.scheduleOutboxRetry)).not.toHaveBeenCalled();
  });

  it.each(['0', '9223372036854775807'])(
    'completes safe response metadata for rejectedSpans=%s without logs or retry',
    async (rejectedSpans) => {
      const item = outboxItem();
      const controller = new AbortController();
      const errorLog = vi.spyOn(console, 'error').mockImplementation(() => undefined);
      const repository = {
        claimSession: vi.fn(async () => null),
        claimOutbox: vi
          .fn()
          .mockResolvedValueOnce(item)
          .mockImplementation(async () => {
            controller.abort();
            return null;
          }),
        renewOutboxClaim: vi.fn(async () => undefined),
        completeOutboxDelivery: vi.fn(async () => undefined),
        markOutboxSuppressed: vi.fn(async () => undefined),
        releaseOutboxClaim: vi.fn(async () => undefined),
        scheduleOutboxRetry: vi.fn(async () => undefined),
      } as unknown as ObservabilityExporterRepository;
      const fetchImpl = vi.fn(
        async () =>
          new Response(
            // Unquoted int64 also has to survive runtime completion without Number conversion.
            `{"partialSuccess":{"rejectedSpans":${rejectedSpans},"errorMessage":"collector warning that must not escape"}}`,
            { headers: { 'content-type': 'application/json' } },
          ),
      );
      const registryClient = basicSecretClient(item);
      const runtime = runtimeFor({ repository, registryClient, otlpFetchImpl: fetchImpl });

      try {
        await runtime.run(controller.signal);
        expect(repository.completeOutboxDelivery).toHaveBeenCalledOnce();
        expect(repository.completeOutboxDelivery).toHaveBeenCalledWith(item, {
          kind: rejectedSpans === '0' ? 'accepted_with_warning' : 'partial_rejection',
          rejectedSpans,
          messageBytes: 38,
          messageSha256: 'b68a6b79b62c91ae5da675a96c3f38d74f4a41ec27870536f2ff7174b5cbe707',
        });
        expect(fetchImpl).toHaveBeenCalledOnce();
        expect(registryClient.resolveSecret).toHaveBeenCalledOnce();
        expect(repository.markOutboxSuppressed).not.toHaveBeenCalled();
        expect(repository.releaseOutboxClaim).not.toHaveBeenCalled();
        expect(repository.scheduleOutboxRetry).not.toHaveBeenCalled();
        expect(errorLog).not.toHaveBeenCalled();
        expect(
          JSON.stringify(vi.mocked(repository.completeOutboxDelivery).mock.calls),
        ).not.toContain('collector warning that must not escape');
      } finally {
        controller.abort();
        errorLog.mockRestore();
      }
    },
  );

  it.each([
    ['{}', { kind: 'accepted' }],
    [
      '{"partialSuccess":{"errorMessage":"😀"}}',
      {
        kind: 'accepted_with_warning',
        rejectedSpans: '0',
        messageBytes: 4,
        messageSha256: 'f0443a342c5ef54783a111b51ba56c938e474c32324d90c3a60c9c8e3a37e2d9',
      },
    ],
    [
      '{"partialSuccess":{"rejectedSpans":1}}',
      {
        kind: 'partial_rejection',
        rejectedSpans: '1',
        messageBytes: 0,
        messageSha256: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
      },
    ],
  ])(
    'attempts terminal completion when shutdown arrives at complete response EOF: %s',
    async (body, outcome) => {
      const item = outboxItem();
      const controller = new AbortController();
      const repository = {
        claimOutbox: vi.fn(async () => item),
        renewOutboxClaim: vi.fn(async () => undefined),
        completeOutboxDelivery: vi.fn(async () => undefined),
        markOutboxSuppressed: vi.fn(async () => undefined),
        releaseOutboxClaim: vi.fn(async () => undefined),
        scheduleOutboxRetry: vi.fn(async () => undefined),
      } as unknown as ObservabilityExporterRepository;
      const fetchImpl = vi.fn(
        async () =>
          new Response(
            new ReadableStream({
              start(stream) {
                stream.enqueue(Buffer.from(body));
              },
              pull(stream) {
                stream.close();
                controller.abort();
              },
            }),
            { headers: { 'content-type': 'application/json' } },
          ),
      );
      const runtime = runtimeFor({
        repository,
        registryClient: basicSecretClient(item),
        otlpFetchImpl: fetchImpl,
      });

      await expect(runtime.deliverOnce(controller.signal)).resolves.toBe(true);
      expect(controller.signal.aborted).toBe(true);
      expect(repository.completeOutboxDelivery).toHaveBeenCalledOnce();
      expect(repository.completeOutboxDelivery).toHaveBeenCalledWith(item, outcome);
      expect(repository.markOutboxSuppressed).not.toHaveBeenCalled();
      expect(repository.releaseOutboxClaim).not.toHaveBeenCalled();
      expect(repository.scheduleOutboxRetry).not.toHaveBeenCalled();
      expect(fetchImpl).toHaveBeenCalledOnce();
    },
  );

  it('releases an incomplete partial response cancelled before EOF without terminal metadata', async () => {
    const item = outboxItem();
    const controller = new AbortController();
    const repository = {
      claimOutbox: vi.fn(async () => item),
      renewOutboxClaim: vi.fn(async () => undefined),
      completeOutboxDelivery: vi.fn(async () => undefined),
      markOutboxSuppressed: vi.fn(async () => undefined),
      releaseOutboxClaim: vi.fn(async () => undefined),
      scheduleOutboxRetry: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({
      repository,
      registryClient: basicSecretClient(item),
      otlpFetchImpl: async () =>
        new Response(
          new ReadableStream({
            start(stream) {
              stream.enqueue(Buffer.from('{"partialSuccess":{"rejectedSpans":1}}'));
            },
            pull(stream) {
              controller.abort();
              stream.error(new Error('cancelled incomplete response'));
            },
          }),
          { headers: { 'content-type': 'application/json' } },
        ),
    });

    await expect(runtime.deliverOnce(controller.signal)).rejects.toMatchObject({
      kind: 'cancelled',
    });
    expect(repository.releaseOutboxClaim).toHaveBeenCalledOnce();
    expect(repository.releaseOutboxClaim).toHaveBeenCalledWith(item);
    expect(repository.completeOutboxDelivery).not.toHaveBeenCalled();
    expect(repository.markOutboxSuppressed).not.toHaveBeenCalled();
    expect(repository.scheduleOutboxRetry).not.toHaveBeenCalled();
  });

  it.each([429, 502, 503, 504])('durably schedules retryable OTLP HTTP %s', async (status) => {
    const item = outboxItem();
    const repository = {
      claimOutbox: vi.fn(async () => item),
      renewOutboxClaim: vi.fn(async () => undefined),
      markOutboxSuppressed: vi.fn(async () => undefined),
      releaseOutboxClaim: vi.fn(async () => undefined),
      scheduleOutboxRetry: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({
      repository,
      registryClient: basicSecretClient(item),
      otlpFetchImpl: async () => new Response('', { status }),
      retryRandom: () => 0,
    });

    await expect(runtime.deliverOnce()).resolves.toBe(true);
    expect(vi.mocked(repository.scheduleOutboxRetry)).toHaveBeenCalledWith(
      item,
      1_000,
      `otlp_http_${status}`,
    );
    expect(vi.mocked(repository.releaseOutboxClaim)).not.toHaveBeenCalled();
    expect(vi.mocked(repository.markOutboxSuppressed)).not.toHaveBeenCalled();
  });

  it.each([
    ['OTLP 503', 'otlp_http_503', 'otlp_http_503'],
    ['OTLP timeout', 'otlp_timeout', 'otlp_timeout'],
    ['Registry 503', 'registry_http_503', 'registry_unavailable'],
    ['Registry transport', 'unexpected_error', 'registry_unavailable'],
  ] as const)(
    'logs safe, deduplicated %s retries through run and logs again after recovery',
    async (failure, logCode, retryCode) => {
      const item = outboxItem({ ...deliveryContext, timeoutMs: 1 });
      const controller = new AbortController();
      const errorLog = vi.spyOn(console, 'error').mockImplementation(() => undefined);
      // Fail twice across an idle poll, recover, then fail twice again. Idle
      // polls and durable retry scheduling must not reset log deduplication.
      const claims = [item, null, item, item, item, item, null];
      const logCounts: number[] = [];
      let attempts = 0;
      const repository = {
        claimSession: vi.fn(async () => null),
        claimOutbox: vi.fn(async () => {
          logCounts.push(errorLog.mock.calls.length);
          if (claims.length === 0) controller.abort();
          return claims.shift() ?? null;
        }),
        renewOutboxClaim: vi.fn(async () => undefined),
        scheduleOutboxRetry: vi.fn(async () => undefined),
        completeOutboxDelivery: vi.fn(async () => undefined),
        markOutboxSuppressed: vi.fn(async () => undefined),
        releaseOutboxClaim: vi.fn(async () => undefined),
      } as unknown as ObservabilityExporterRepository;
      const registryClient = basicSecretClient(item);
      const secret = await registryClient.resolveSecret({
        workspaceId: item.trace.workspaceId,
        sessionId: item.trace.sessionId,
      });
      vi.mocked(registryClient.resolveSecret).mockImplementation(async () => {
        attempts += 1;
        if (attempts !== 3) {
          if (failure === 'Registry 503') throw new RegistryResolverHttpError('secret', 503);
          if (failure === 'Registry transport') {
            throw new TypeError(`private Registry detail: ${TRANSCRIPT_SECRET}, sk-runtime`);
          }
        }
        return secret;
      });
      const runtime = runtimeFor({
        repository,
        registryClient,
        retryRandom: () => 0,
        otlpFetchImpl: async (_input, init) => {
          if (attempts === 3) {
            return new Response('{}', { headers: { 'content-type': 'application/json' } });
          }
          if (failure === 'OTLP timeout') {
            await waitForAbort(init?.signal ?? undefined);
            throw init?.signal?.reason;
          }
          return new Response(TRANSCRIPT_SECRET, {
            status: 503,
            headers: { 'retry-after': 'private-header-sk-runtime' },
          });
        },
      });

      try {
        await runtime.run(controller.signal);
        expect(logCounts).toEqual([0, 1, 1, 1, 1, 2, 2, 2]);
        expect(errorLog.mock.calls).toEqual(
          Array.from({ length: 2 }, () => [
            JSON.stringify({ component: 'observability-exporter-delivery', code: logCode }),
          ]),
        );
        expect(vi.mocked(repository.scheduleOutboxRetry).mock.calls).toEqual(
          Array.from({ length: 4 }, () => [item, 1_000, retryCode]),
        );
        expect(vi.mocked(repository.completeOutboxDelivery).mock.calls).toEqual([
          [item, { kind: 'accepted' }],
        ]);
        expect(repository.markOutboxSuppressed).not.toHaveBeenCalled();
        expect(repository.releaseOutboxClaim).not.toHaveBeenCalled();
      } finally {
        controller.abort();
        errorLog.mockRestore();
      }
    },
  );

  it('uses bounded Retry-After as the minimum durable OTLP retry delay', async () => {
    const item = outboxItem();
    const repository = {
      claimOutbox: vi.fn(async () => item),
      renewOutboxClaim: vi.fn(async () => undefined),
      scheduleOutboxRetry: vi.fn(async () => undefined),
      releaseOutboxClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({
      repository,
      registryClient: basicSecretClient(item),
      otlpFetchImpl: async () =>
        new Response('', { status: 503, headers: { 'retry-after': '30' } }),
      retryRandom: () => 0,
    });

    await expect(runtime.deliverOnce()).resolves.toBe(true);
    expect(vi.mocked(repository.scheduleOutboxRetry)).toHaveBeenCalledWith(
      item,
      30_000,
      'otlp_http_503',
    );
  });

  it('durably schedules OTLP transport failures', async () => {
    const item = outboxItem();
    const repository = {
      claimOutbox: vi.fn(async () => item),
      renewOutboxClaim: vi.fn(async () => undefined),
      scheduleOutboxRetry: vi.fn(async () => undefined),
      releaseOutboxClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({
      repository,
      registryClient: basicSecretClient(item),
      otlpFetchImpl: async () => {
        throw new TypeError('transport detail');
      },
      retryRandom: () => 0,
    });

    await expect(runtime.deliverOnce()).resolves.toBe(true);
    expect(vi.mocked(repository.scheduleOutboxRetry)).toHaveBeenCalledWith(
      item,
      1_000,
      'otlp_transport',
    );
  });

  it('durably schedules OTLP timeouts', async () => {
    const item = outboxItem({ ...deliveryContext, timeoutMs: 1 });
    const repository = {
      claimOutbox: vi.fn(async () => item),
      renewOutboxClaim: vi.fn(async () => undefined),
      scheduleOutboxRetry: vi.fn(async () => undefined),
      releaseOutboxClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({
      repository,
      registryClient: basicSecretClient(item),
      otlpFetchImpl: async (_input, init) => {
        await waitForAbort(init?.signal ?? undefined);
        throw init?.signal?.reason;
      },
      retryRandom: () => 0,
    });

    await expect(runtime.deliverOnce()).resolves.toBe(true);
    expect(vi.mocked(repository.scheduleOutboxRetry)).toHaveBeenCalledWith(
      item,
      1_000,
      'otlp_timeout',
    );
  });

  it.each([
    [
      'authentication unavailable',
      new RegistryResolverHttpError('secret', 401),
      'registry_unavailable',
    ],
    [
      'authorization unavailable',
      new RegistryResolverHttpError('secret', 403),
      'registry_unavailable',
    ],
    ['unavailable', new RegistryResolverHttpError('secret', 503), 'registry_unavailable'],
    ['invalid response', new RegistryResolverResponseError('secret'), 'registry_invalid_response'],
  ] as const)('durably schedules Registry %s', async (_kind, resolverError, errorCode) => {
    const item = outboxItem();
    const repository = {
      claimOutbox: vi.fn(async () => item),
      scheduleOutboxRetry: vi.fn(async () => undefined),
      releaseOutboxClaim: vi.fn(async () => undefined),
      markOutboxSuppressed: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const registryClient = {
      resolveSecret: vi.fn(async () => {
        throw resolverError;
      }),
    } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
    const runtime = runtimeFor({ repository, registryClient, retryRandom: () => 0 });

    await expect(runtime.deliverOnce()).resolves.toBe(true);
    expect(vi.mocked(repository.scheduleOutboxRetry)).toHaveBeenCalledWith(item, 1_000, errorCode);
    expect(vi.mocked(repository.releaseOutboxClaim)).not.toHaveBeenCalled();
    expect(vi.mocked(repository.markOutboxSuppressed)).not.toHaveBeenCalled();
  });

  it.each([400, 404, 409])('suppresses terminal Registry HTTP %s', async (status) => {
    const item = outboxItem();
    const repository = {
      claimOutbox: vi.fn(async () => item),
      scheduleOutboxRetry: vi.fn(async () => undefined),
      releaseOutboxClaim: vi.fn(async () => undefined),
      markOutboxSuppressed: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const registryClient = {
      resolveSecret: vi.fn(async () => {
        throw new RegistryResolverHttpError('secret', status);
      }),
    } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
    const runtime = runtimeFor({ repository, registryClient });

    await expect(runtime.deliverOnce()).resolves.toBe(true);
    expect(vi.mocked(repository.markOutboxSuppressed)).toHaveBeenCalledWith(
      item,
      'registry_denied',
    );
    expect(vi.mocked(repository.scheduleOutboxRetry)).not.toHaveBeenCalled();
    expect(vi.mocked(repository.releaseOutboxClaim)).not.toHaveBeenCalled();
  });

  it.each([400, 408, 500, 501, 505])('suppresses terminal OTLP HTTP %s', async (status) => {
    const item = outboxItem();
    const repository = {
      claimOutbox: vi.fn(async () => item),
      renewOutboxClaim: vi.fn(async () => undefined),
      markOutboxSuppressed: vi.fn(async () => undefined),
      releaseOutboxClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({
      repository,
      registryClient: basicSecretClient(item),
      otlpFetchImpl: async () => new Response('', { status }),
    });

    await expect(runtime.deliverOnce()).resolves.toBe(true);
    expect(vi.mocked(repository.markOutboxSuppressed)).toHaveBeenCalledWith(
      item,
      'permanent_http_status',
    );
    expect(vi.mocked(repository.releaseOutboxClaim)).not.toHaveBeenCalled();
  });

  it('suppresses a public-class binding that resolves to a private address', async () => {
    const item = outboxItem({
      ...deliveryContext,
      endpointUrl: 'https://localhost/api/public/otel/v1/traces',
    });
    const repository = {
      claimOutbox: vi.fn(async () => item),
      renewOutboxClaim: vi.fn(async () => undefined),
      markOutboxSuppressed: vi.fn(async () => undefined),
      releaseOutboxClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({ repository, registryClient: basicSecretClient(item) });

    await expect(runtime.deliverOnce()).resolves.toBe(true);
    expect(vi.mocked(repository.markOutboxSuppressed)).toHaveBeenCalledWith(item, 'egress_denied');
    expect(vi.mocked(repository.releaseOutboxClaim)).not.toHaveBeenCalled();
  });

  it('drops a later Kafka offset for an already canonical event identity', () => {
    const canonical = event(
      5,
      'session.status_idle',
      { stop_reason: { type: 'end_turn' } },
      { id: 'evt_terminal_once' },
    );
    const duplicate = { ...canonical, seq: 9 };
    const later = event(10, 'session.status_running', {}, { id: 'evt_later_turn' });
    const result = selectCanonicalReplayEvents(
      [duplicate, later],
      [
        {
          eventKey: eventIdentityKey(canonical.id),
          sourceSeq: '5',
          sourceHash: hashTranscriptEnvelope(canonical),
        },
      ],
    );

    expect(result.conflict).toBeUndefined();
    expect(result.events.map((candidate) => candidate.id)).toEqual(['evt_later_turn']);
  });

  it('prevents an old duplicate terminal identity from closing a later turn', () => {
    const firstEvents = completedPrimaryTurnEvents();
    const firstTerminal = firstEvents.at(-1)!;
    firstTerminal.id = 'evt_terminal_once';
    const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), firstEvents);
    const secondSource = event(
      20,
      'user.message',
      {},
      { id: 'evt_second_turn', producedBy: 'client' },
    );
    const duplicateTerminal = {
      ...firstTerminal,
      seq: 22,
    };
    const secondTerminal = event(
      23,
      'session.status_idle',
      { stop_reason: { type: 'end_turn' } },
      { id: 'evt_second_terminal' },
    );
    secondTerminal.producedAt = '2026-01-01T00:02:00.000Z';
    const replay = [
      secondSource,
      event(
        21,
        'session.user_event_processed',
        { user_event_id: secondSource.id },
        { id: 'evt_accept_second_turn' },
      ),
      duplicateTerminal,
      secondTerminal,
    ];
    const canonical = selectCanonicalReplayEvents(replay, [
      {
        eventKey: eventIdentityKey(firstTerminal.id),
        sourceSeq: String(firstTerminal.seq),
        sourceHash: hashTranscriptEnvelope(firstTerminal),
      },
    ]);
    const second = reduceCanonicalEventBatch(first.state, canonical.events);

    expect(second.completedTraces).toHaveLength(1);
    expect(second.completedTraces[0]?.root.endedAt).toBe('2026-01-01T00:02:00.000Z');
  });

  it('reports a Kafka logical identity whose content changes', () => {
    const canonical = event(5, 'session.status_idle', {}, { id: 'evt_conflicting_terminal' });
    const changed = {
      ...canonical,
      seq: 9,
      payload: Buffer.from(JSON.stringify({ stop_reason: { type: 'retries_exhausted' } })),
    };
    const result = selectCanonicalReplayEvents(
      [changed],
      [
        {
          eventKey: eventIdentityKey(canonical.id),
          sourceSeq: '5',
          sourceHash: hashTranscriptEnvelope(canonical),
        },
      ],
    );

    expect(result.conflict).toMatchObject({ eventKey: eventIdentityKey(canonical.id) });
  });

  it('quarantines a replay hash conflict before reducer state can advance', async () => {
    const source = event(1, 'session.status_running', {}, { id: 'evt_replay_conflict' });
    const claim: ClaimedProjectionSession = {
      workspaceId: source.workspaceId,
      sessionId: source.sessionId,
      nextSeq: '0',
      firstPendingSeq: '1',
      state: initialCanonicalProjectionState(),
      leaseOwner: 'worker-projector',
      leaseGeneration: '1',
    };
    const repository = {
      claimSession: vi.fn(async () => claim),
      renewProjectionClaim: vi.fn(async () => undefined),
      loadInboxEventIdentities: vi.fn(async () => [
        {
          eventKey: eventIdentityKey(source.id),
          sourceSeq: '1',
          sourceHash: hashTranscriptEnvelope({ ...source, payload: Buffer.from('different') }),
        },
      ]),
      quarantineReplayConflict: vi.fn(async () => undefined),
      releaseProjectionClaim: vi.fn(async () => undefined),
    } as unknown as ObservabilityExporterRepository;
    const runtime = runtimeFor({
      repository,
      transcriptStore: {
        read: async function* (): AsyncIterable<Event> {
          yield source;
        },
      } as unknown as Pick<KafkaTranscriptStore, 'read'>,
    });

    await expect(runtime.projectOnce()).rejects.toThrow('replay identity conflict');
    expect(vi.mocked(repository.quarantineReplayConflict)).toHaveBeenCalledOnce();
    expect(vi.mocked(repository.releaseProjectionClaim)).not.toHaveBeenCalled();
  });
});

function runtimeFor(
  options: Partial<ConstructorParameters<typeof KafkaObservabilityExporterRuntime>[0]>,
): KafkaObservabilityExporterRuntime {
  return new KafkaObservabilityExporterRuntime({
    repository: options.repository ?? ({} as ObservabilityExporterRepository),
    transcriptStore:
      options.transcriptStore ??
      ({ read: async function* (): AsyncIterable<Event> {} } as unknown as Pick<
        KafkaTranscriptStore,
        'read'
      >),
    eventSource:
      options.eventSource ??
      ({
        start: async () => undefined,
        stop: async () => undefined,
        status: () => ({ ready: false, state: 'stopped' }),
      } as Pick<KafkaSessionEventSource, 'start' | 'stop' | 'status'>),
    registryClient:
      options.registryClient ??
      ({ resolveContext: vi.fn(async () => enabledContext()), resolveSecret: vi.fn() } as Pick<
        RegistryObservabilityClient,
        'resolveContext' | 'resolveSecret'
      >),
    workerId: 'worker',
    projectorLeaseMs: 30_000,
    projectorBatchSize: 1_000,
    projectorBatchBytes: 8 * 1024 * 1024,
    deliveryLeaseMs: 180_000,
    registryRequestTimeoutMs: 1_000,
    projectorPollMs: 1,
    deliveryPollMs: 1,
    ...(options.otlpFetchImpl === undefined ? {} : { otlpFetchImpl: options.otlpFetchImpl }),
    ...(options.retryRandom === undefined ? {} : { retryRandom: options.retryRandom }),
  });
}

function enabledContext(): RegistryEnabledObservabilityContext {
  return {
    status: 'enabled',
    deliveryContext,
  };
}

function projectionClaim(source: Event): ClaimedProjectionSession {
  return {
    workspaceId: source.workspaceId,
    sessionId: source.sessionId,
    nextSeq: '0',
    firstPendingSeq: '0',
    state: initialCanonicalProjectionState(),
    leaseOwner: 'worker-projector',
    leaseGeneration: '1',
  };
}

function projectionRepository(claim: ClaimedProjectionSession): ObservabilityExporterRepository {
  return {
    claimSession: vi.fn(async () => claim),
    loadInboxEventIdentities: vi.fn(async () => []),
    loadAcceptedSourceIds: vi.fn(async () => new Set<string>()),
    renewProjectionClaim: vi.fn(async () => undefined),
    quarantineProjectionClaim: vi.fn(async () => undefined),
    releaseProjectionClaim: vi.fn(async () => undefined),
  } as unknown as ObservabilityExporterRepository;
}

function transcriptReader(events: readonly Event[]): Pick<KafkaTranscriptStore, 'read'> {
  return {
    read: async function* (): AsyncIterable<Event> {
      yield* events;
    },
  } as unknown as Pick<KafkaTranscriptStore, 'read'>;
}

async function waitForAbort(signal: AbortSignal | undefined): Promise<void> {
  if (signal === undefined) throw new Error('expected AbortSignal');
  if (signal.aborted) return;
  await new Promise<void>((resolve) =>
    signal.addEventListener('abort', () => resolve(), { once: true }),
  );
}

function outboxItem(context: PinnedDeliveryContext = deliveryContext): ClaimedOutboxItem {
  const trace = completedProjectedTrace();
  return {
    id: '1',
    trace,
    deliveryContext: context,
    attemptCount: 0,
    leaseOwner: 'worker-delivery',
    leaseGeneration: '1',
  };
}

function basicSecretClient(item: ClaimedOutboxItem) {
  return {
    resolveSecret: vi.fn(async () => ({
      bindingId: item.deliveryContext.bindingId,
      bindingVersion: item.deliveryContext.bindingVersion,
      effectiveCaptureMode: 'metadata_only' as const,
      auth: { type: 'basic' as const, username: 'pk-runtime', password: 'sk-runtime' },
    })),
  } as unknown as Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
}
