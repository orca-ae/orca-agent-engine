// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { getEventListeners } from 'node:events';
import kafkaJs from 'kafkajs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { runBrokerExporter } from '../../src/broker-main.js';
import { loadConfig } from '../../src/config.js';
import { metadataAdmin } from '../support/kafka-metadata-admin.js';

const {
  KafkaJSError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
  KafkaJSConnectionError,
  KafkaJSRequestTimeoutError,
} = kafkaJs;
// The real class is exported by KafkaJS 2.2.4, but missing from its declarations.
const { KafkaJSConnectionClosedError } = kafkaJs as typeof kafkaJs & {
  KafkaJSConnectionClosedError: new (
    message: string,
    options: { host: string; port: number },
  ) => InstanceType<typeof KafkaJSConnectionError>;
};

const mocks = vi.hoisted(() => ({
  kafka: vi.fn(),
  admin: { connect: vi.fn(), listTopics: vi.fn(), createTopics: vi.fn(), disconnect: vi.fn() },
  runtimeConstructor: vi.fn(),
  runtime: { start: vi.fn(), addSessions: vi.fn(), status: vi.fn(), stop: vi.fn() },
  createHealth: vi.fn(),
  health: { once: vi.fn(), listen: vi.fn(), close: vi.fn(), closeAllConnections: vi.fn() },
  delay: vi.fn(),
  codecFactory: vi.fn(),
  codec: { prepareWriter: vi.fn(), decode: vi.fn(), encode: vi.fn(), close: vi.fn() },
}));

vi.mock('@orca/transcript-store', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@orca/transcript-store')>()),
  createKafkaTranscriptCodec: mocks.codecFactory,
}));

vi.mock('kafkajs', async (importOriginal) => ({
  ...(await importOriginal<typeof import('kafkajs')>()),
  Kafka: mocks.kafka.mockImplementation(() => ({ admin: () => mocks.admin })),
}));
vi.mock('../../src/kafka-runtime.js', () => ({
  KafkaOnlyObservabilityExporterRuntime: mocks.runtimeConstructor.mockImplementation(
    () => mocks.runtime,
  ),
}));
vi.mock('../../src/health.js', () => ({
  createExporterHealthServer: mocks.createHealth.mockImplementation(() => mocks.health),
}));
vi.mock('node:timers/promises', () => ({ setTimeout: mocks.delay }));
// Fail immediately if bootstrap ever introduces a SQL dependency.
vi.mock('pg', () => {
  throw new Error('broker bootstrap must not import pg');
});

const baseEnv = {
  REGISTRY_INTERNAL_BASE_URL: 'http://registry.internal:8081',
  INTERNAL_SERVICE_TOKEN: 'x'.repeat(32),
};
const checkpoint = 'orca.observability.v1.checkpoints';
const delivery = 'orca.observability.v1.delivery';
const first = 'orca.ws_a.sessions.ses_first.events';
const second = 'orca.ws_b.sessions.ses_second.events';

describe('broker exporter bootstrap', () => {
  let controller: AbortController;
  let listeners: Record<'SIGTERM' | 'SIGINT', Array<(signal: NodeJS.Signals) => void>>;

  beforeEach(() => {
    vi.clearAllMocks();
    mocks.codecFactory.mockReturnValue(mocks.codec);
    mocks.codec.close.mockResolvedValue(undefined);
    mocks.kafka.mockImplementation(() => ({ admin: () => mocks.admin }));
    mocks.runtimeConstructor.mockImplementation(() => mocks.runtime);
    mocks.createHealth.mockImplementation(() => mocks.health);
    controller = new AbortController();
    listeners = { SIGTERM: process.listeners('SIGTERM'), SIGINT: process.listeners('SIGINT') };
    mocks.admin.connect.mockReset().mockResolvedValue(undefined);
    mocks.admin.listTopics.mockReset().mockResolvedValue([]);
    mocks.admin.createTopics.mockReset().mockResolvedValue(true);
    mocks.admin.disconnect.mockReset().mockResolvedValue(undefined);
    mocks.runtime.start.mockReset().mockResolvedValue(undefined);
    mocks.runtime.addSessions.mockReset().mockResolvedValue(undefined);
    mocks.runtime.stop.mockReset().mockResolvedValue(undefined);
    mocks.runtime.status.mockReset().mockReturnValue({ state: 'running', ready: true });
    mocks.health.once.mockReset().mockReturnValue(mocks.health);
    mocks.health.listen.mockReset().mockImplementation((_port, _host, ready: () => void) => {
      ready();
      return mocks.health;
    });
    // End after one discovery by default; tests can supply more iterations.
    mocks.delay.mockReset().mockImplementation(async () => controller.abort());
  });

  afterEach(() => {
    controller.abort();
    expect(process.listeners('SIGTERM')).toEqual(listeners.SIGTERM);
    expect(process.listeners('SIGINT')).toEqual(listeners.SIGINT);
    expect(getEventListeners(controller.signal, 'abort')).toEqual([]);
    vi.restoreAllMocks();
  });

  function run(prefix = '', mode = 'plaintext', listingMode = 'canonical'): Promise<void> {
    return runBrokerExporter(
      loadConfig({
        ...baseEnv,
        KAFKA_TOPIC_PREFIX: prefix,
        KAFKA_CONNECTION_MODE: mode,
        KAFKA_TOPIC_LISTING_MODE: listingMode,
        KAFKA_AUTH_TOKEN: 'test-token',
        KAFKA_SASL_USERNAME: 'public',
        KAFKA_SSL: 'true',
        KAFKA_SASL_MECHANISM: 'plain',
        KAFKA_SASL_PASSWORD: 'test-password',
      }),
      controller.signal,
    );
  }

  function expectCleanedUp(): void {
    expect(mocks.health.close).toHaveBeenCalledTimes(1);
    expect(mocks.health.closeAllConnections).toHaveBeenCalledTimes(1);
    expect(mocks.runtime.stop).toHaveBeenCalledTimes(1);
    expect(mocks.admin.disconnect).toHaveBeenCalledTimes(1);
    expect(mocks.codec.close).toHaveBeenCalledTimes(1);
  }

  function metadataError(type = 'LEADER_NOT_AVAILABLE'): InstanceType<typeof KafkaJSProtocolError> {
    return new KafkaJSProtocolError(
      Object.assign(new Error('secret broker detail'), { type, code: 5, retriable: true }),
    );
  }

  it.each([false, true])(
    'retries initial metadata (wrapped=%s) while Live/NotReady, then becomes ready',
    async (wrapped) => {
      const cause = metadataError();
      const error = wrapped
        ? new KafkaJSNumberOfRetriesExceeded(cause, { retryCount: 5, retryTime: 300 })
        : cause;
      const warning = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
      mocks.admin.listTopics
        .mockRejectedValueOnce(error)
        .mockResolvedValue([checkpoint, delivery, first]);
      mocks.delay
        .mockImplementationOnce(async (ms, _value, options) => {
          expect(ms).toBe(1000);
          expect(options.signal).toBeInstanceOf(AbortSignal);
          const health = mocks.createHealth.mock.calls[0]![0];
          expect(health.isLive()).toBe(true);
          expect(await health.checkReady()).toBe(false);
          expect(mocks.runtime.start).not.toHaveBeenCalled();
        })
        .mockImplementationOnce(async () => {
          expect(await mocks.createHealth.mock.calls[0]![0].checkReady()).toBe(true);
          controller.abort();
        });
      await run();
      expect(mocks.runtime.start).toHaveBeenCalledOnce();
      expect(warning).toHaveBeenCalledOnce();
      expect(JSON.parse(warning.mock.calls[0]![0])).toEqual({
        component: 'observability-exporter',
        code: 'metadata_retry',
        phase: 'startup',
        type: 'LEADER_NOT_AVAILABLE',
        delayMs: 1000,
      });
      expect(JSON.stringify(warning.mock.calls)).not.toContain('secret broker detail');
      expectCleanedUp();
    },
  );

  it('keeps discovered routes during metadata failure and recovers without restarting', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    mocks.admin.listTopics
      .mockResolvedValueOnce([checkpoint, delivery])
      .mockResolvedValueOnce([first])
      .mockRejectedValueOnce(metadataError())
      .mockResolvedValueOnce([first, second]);
    mocks.delay.mockResolvedValueOnce(undefined).mockImplementationOnce(async () => {
      expect(mocks.runtime.addSessions).toHaveBeenCalledTimes(1);
      expect(mocks.runtime.stop).not.toHaveBeenCalled();
    });
    await run();
    expect(mocks.runtimeConstructor).toHaveBeenCalledOnce();
    expect(mocks.runtime.start).toHaveBeenCalledOnce();
    expect(
      mocks.runtime.addSessions.mock.calls.map(([routes]) =>
        routes.map((route: { topic: string }) => route.topic),
      ),
    ).toEqual([[first], [first, second]]);
    expectCleanedUp();
  });

  it('recovers discovery through the real KafkaJS warm-cache null-metadata path', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const warm = metadataAdmin([first], true);
    const cold = metadataAdmin([first, second]);
    const error = metadataError();
    const factory = vi.fn().mockReturnValueOnce(mocks.admin).mockReturnValue(cold.admin);
    mocks.kafka.mockImplementation(() => ({ admin: factory }));
    mocks.admin.listTopics
      .mockResolvedValueOnce([checkpoint, delivery])
      .mockImplementation(() => warm.admin.listTopics());
    mocks.delay
      .mockImplementationOnce(async () => {
        warm.broker.metadata.mockRejectedValue(error);
        cold.broker.metadata.mockRejectedValue(error);
        // Prove the client erases the protocol error, rather than stubbing a TypeError.
        await expect(warm.admin.listTopics()).rejects.toThrow(TypeError);
      })
      .mockImplementationOnce(async () => {
        expect(mocks.runtime.addSessions).toHaveBeenCalledTimes(1);
        expect(mocks.runtime.stop).not.toHaveBeenCalled();
        expect(cold.broker.disconnect).toHaveBeenCalled();
        warm.broker.metadata.mockResolvedValue(cold.response);
      });
    await run();
    expect(factory).toHaveBeenCalledTimes(2);
    expect(mocks.runtime.start).toHaveBeenCalledOnce();
    expect(
      mocks.runtime.addSessions.mock.calls.map(([routes]) =>
        routes.map((route: { topic: string }) => route.topic),
      ),
    ).toEqual([[first], [first, second]]);
    expectCleanedUp();
  });

  it.each(['TOPIC_AUTHORIZATION_FAILED', 'INVALID_REQUEST', 'TLS', 'TypeError'])(
    'does not retry warm-cache %s hidden by KafkaJS',
    async (type) => {
      const warm = metadataAdmin([first], true);
      const cold = metadataAdmin([first]);
      const error =
        type === 'TLS'
          ? new KafkaJSConnectionError('TLS', { code: 'CERT_HAS_EXPIRED' })
          : type === 'TypeError'
            ? new TypeError('broker invariant')
            : metadataError(type);
      warm.broker.metadata.mockRejectedValue(error);
      cold.broker.metadata.mockRejectedValue(error);
      mocks.kafka.mockImplementation(() => ({
        admin: vi.fn().mockReturnValueOnce(mocks.admin).mockReturnValue(cold.admin),
      }));
      mocks.admin.listTopics.mockImplementation(() => warm.admin.listTopics());
      await expect(run()).rejects.toBe(error);
      expect(mocks.delay).not.toHaveBeenCalled();
      expect(cold.broker.disconnect).toHaveBeenCalled();
      expectCleanedUp();
    },
  );

  it('accepts a successful cold probe without restarting the runtime', async () => {
    const warm = metadataAdmin([first], true);
    const cold = metadataAdmin([checkpoint, delivery, first]);
    warm.broker.metadata.mockRejectedValue(metadataError());
    const factory = vi.fn().mockReturnValueOnce(mocks.admin).mockReturnValue(cold.admin);
    mocks.kafka.mockImplementation(() => ({ admin: factory }));
    mocks.admin.listTopics
      .mockImplementationOnce(() => warm.admin.listTopics())
      .mockResolvedValue([first]);
    await run();
    expect(factory).toHaveBeenCalledTimes(2);
    expect(cold.broker.metadata).toHaveBeenCalledTimes(2);
    expect(cold.broker.disconnect).toHaveBeenCalled();
    expect(mocks.runtime.start).toHaveBeenCalledOnce();
    expect(mocks.runtime.addSessions).toHaveBeenCalledOnce();
    expectCleanedUp();
  });

  it('does not recursively probe when the cold client also loses an error', async () => {
    const warm = metadataAdmin([first], true);
    const cold = metadataAdmin([first]);
    warm.broker.metadata.mockRejectedValue(metadataError());
    // Refresh succeeds, but the second Metadata request inside listTopics fails.
    cold.broker.metadata.mockResolvedValueOnce(cold.response).mockRejectedValue(metadataError());
    const factory = vi.fn().mockReturnValueOnce(mocks.admin).mockReturnValue(cold.admin);
    mocks.kafka.mockImplementation(() => ({ admin: factory }));
    mocks.admin.listTopics.mockImplementation(() => warm.admin.listTopics());
    await expect(run()).rejects.toThrow(TypeError);
    expect(factory).toHaveBeenCalledTimes(2);
    expect(mocks.delay).not.toHaveBeenCalled();
    expect(cold.broker.disconnect).toHaveBeenCalled();
    expectCleanedUp();
  });

  it.each(['abort', 'deadline', 'runtime failure'] as const)(
    'stops the cold probe after connect on %s',
    async (stop) => {
      let now = 0;
      vi.spyOn(performance, 'now').mockImplementation(() => now);
      const warm = metadataAdmin([first], true);
      const cold = metadataAdmin([first]);
      warm.broker.metadata.mockRejectedValue(metadataError());
      vi.spyOn(cold.admin, 'connect').mockImplementation(async () => {
        if (stop === 'abort') controller.abort();
        else if (stop === 'deadline') now += 900001;
        else mocks.runtime.status.mockReturnValue({ state: 'failed', ready: false });
      });
      mocks.kafka.mockImplementation(() => ({
        admin: vi.fn().mockReturnValueOnce(mocks.admin).mockReturnValue(cold.admin),
      }));
      mocks.admin.listTopics.mockImplementation(() => warm.admin.listTopics());
      if (stop === 'abort') await run();
      else if (stop === 'deadline') await expect(run()).rejects.toThrow(TypeError);
      else await expect(run()).rejects.toThrow('Kafka exporter runtime failed');
      expect(cold.broker.metadata).not.toHaveBeenCalled();
      expect(cold.broker.disconnect).toHaveBeenCalled();
      expect(mocks.delay).not.toHaveBeenCalled();
      expectCleanedUp();
    },
  );

  it('preserves the cold probe error when probe cleanup also fails', async () => {
    const warm = metadataAdmin([first], true);
    const cold = metadataAdmin([first]);
    const error = metadataError('TOPIC_AUTHORIZATION_FAILED');
    warm.broker.metadata.mockRejectedValue(error);
    cold.broker.metadata.mockRejectedValue(error);
    vi.spyOn(cold.admin, 'disconnect').mockRejectedValue(new Error('cleanup failed'));
    mocks.kafka.mockImplementation(() => ({
      admin: vi.fn().mockReturnValueOnce(mocks.admin).mockReturnValue(cold.admin),
    }));
    mocks.admin.listTopics.mockImplementation(() => warm.admin.listTopics());
    await expect(run()).rejects.toBe(error);
    expect(cold.admin.disconnect).toHaveBeenCalledOnce();
    expect(mocks.delay).not.toHaveBeenCalled();
    expectCleanedUp();
  });

  it.each([false, true])(
    'retries a clean connection close with no code (wrapped=%s)',
    async (wrapped) => {
      vi.spyOn(console, 'warn').mockImplementation(() => undefined);
      const cause = new KafkaJSConnectionClosedError('closed', { host: 'broker', port: 9092 });
      expect(cause).toHaveProperty('code', undefined);
      const error = wrapped
        ? new KafkaJSNumberOfRetriesExceeded(cause, { retryCount: 5, retryTime: 300 })
        : cause;
      mocks.admin.listTopics.mockRejectedValueOnce(error);
      mocks.delay.mockResolvedValueOnce(undefined);
      await run();
      expect(mocks.runtime.start).toHaveBeenCalledOnce();
      expectCleanedUp();
    },
  );

  it.each([
    new TypeError('unrelated metadata bug'),
    new TypeError(
      "Cannot destructure property 'topicMetadata' of '(intermediate value)' as it is null.",
    ),
  ])(
    'does not retry unrelated TypeErrors or probe based only on a matching message',
    async (error) => {
      mocks.admin.listTopics.mockRejectedValue(error);
      await expect(run()).rejects.toBe(error);
      expect(mocks.delay).not.toHaveBeenCalled();
      expectCleanedUp();
    },
  );

  it.each([
    'TOPIC_AUTHORIZATION_FAILED',
    'INVALID_TOPIC_EXCEPTION',
    'INVALID_REQUEST',
    'UNKNOWN_SERVER_ERROR',
  ])('does not retry protocol %s even with retriable=true', async (type) => {
    const error = new KafkaJSNumberOfRetriesExceeded(metadataError(type), {
      retryCount: 5,
      retryTime: 300,
    });
    mocks.admin.listTopics.mockRejectedValue(error);
    await expect(run()).rejects.toBe(error);
    expect(mocks.delay).not.toHaveBeenCalled();
    expectCleanedUp();
  });

  it.each([
    new KafkaJSError('invariant'),
    new KafkaJSConnectionError('TLS', { code: 'CERT_HAS_EXPIRED' }),
  ])('does not retry other retriable errors', async (error) => {
    mocks.admin.listTopics.mockRejectedValue(error);
    await expect(run()).rejects.toBe(error);
    expect(mocks.delay).not.toHaveBeenCalled();
    expectCleanedUp();
  });

  it.each([
    new KafkaJSConnectionError('reset', { code: 'ECONNRESET' }),
    new KafkaJSRequestTimeoutError('timeout'),
  ])('retries transient metadata transport errors', async (error) => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    mocks.admin.listTopics.mockRejectedValueOnce(error);
    mocks.delay.mockResolvedValueOnce(undefined);
    await run();
    expect(mocks.runtime.start).toHaveBeenCalledOnce();
    expectCleanedUp();
  });

  it('bounds exponential backoff at fifteen minutes including time in KafkaJS and preserves the original error', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    let now = 0;
    vi.spyOn(performance, 'now').mockImplementation(() => now);
    const error = new KafkaJSNumberOfRetriesExceeded(metadataError(), {
      retryCount: 5,
      retryTime: 300,
    });
    mocks.admin.listTopics.mockImplementation(async () => {
      now += 10000;
      throw error;
    });
    mocks.delay.mockImplementation(async (ms) => {
      now += ms;
    });
    await expect(run()).rejects.toBe(error);
    expect(now).toBe(900000);
    expect(mocks.delay.mock.calls.slice(0, 6).map(([ms]) => ms)).toEqual([
      1000, 2000, 4000, 8000, 16000, 30000,
    ]);
    expect(mocks.admin.listTopics).toHaveBeenCalledTimes(mocks.delay.mock.calls.length);
    expectCleanedUp();
  });

  it('resets the retry budget after each successful listing', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    let now = 0;
    vi.spyOn(performance, 'now').mockImplementation(() => now);
    mocks.admin.listTopics
      .mockRejectedValueOnce(metadataError())
      .mockResolvedValueOnce([checkpoint, delivery])
      .mockRejectedValueOnce(metadataError())
      .mockResolvedValueOnce([first]);
    mocks.delay
      .mockImplementationOnce(async () => {
        now += 899000;
      })
      .mockImplementationOnce(async () => {
        now += 899000;
      });
    await run();
    expect(mocks.runtime.addSessions).toHaveBeenCalledOnce();
    expectCleanedUp();
  });

  it.each([false, true])(
    'lets an in-flight listing settle past the budget (success=%s)',
    async (success) => {
      let now = 0;
      vi.spyOn(performance, 'now').mockImplementation(() => now);
      const error = metadataError();
      mocks.admin.listTopics.mockImplementationOnce(async () => {
        now += 900001;
        if (!success) throw error;
        return [checkpoint, delivery];
      });
      if (success) {
        await run();
        expect(mocks.runtime.start).toHaveBeenCalledOnce();
      } else {
        await expect(run()).rejects.toBe(error);
        expect(mocks.delay).not.toHaveBeenCalled();
        expect(mocks.runtime.start).not.toHaveBeenCalled();
      }
      expectCleanedUp();
    },
  );

  it('never retries a partially started runtime even for a metadata availability error', async () => {
    const error = metadataError();
    mocks.runtime.start.mockRejectedValueOnce(error);
    await expect(run()).rejects.toBe(error);
    expect(mocks.runtime.start).toHaveBeenCalledOnce();
    expect(mocks.delay).not.toHaveBeenCalled();
    expectCleanedUp();
  });

  it('aborts metadata backoff and cleans up without another listing', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    mocks.admin.listTopics.mockRejectedValue(metadataError());
    mocks.delay.mockImplementationOnce(async (_ms, _value, options) => {
      controller.abort();
      expect(options.signal.aborted).toBe(true);
      throw new Error('aborted sleep');
    });
    await run();
    expect(mocks.admin.listTopics).toHaveBeenCalledOnce();
    expect(mocks.runtime.start).not.toHaveBeenCalled();
    expectCleanedUp();
  });

  it('detects runtime failure on the next discovery retry rather than waiting fifteen minutes', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    mocks.admin.listTopics
      .mockResolvedValueOnce([checkpoint, delivery])
      .mockRejectedValue(metadataError());
    mocks.delay.mockImplementationOnce(async () => {
      mocks.runtime.status.mockReturnValue({ state: 'failed', ready: false });
    });
    await expect(run()).rejects.toThrow('Kafka exporter runtime failed');
    expect(mocks.delay).toHaveBeenCalledOnce();
    expect(mocks.admin.listTopics).toHaveBeenCalledTimes(2);
    expectCleanedUp();
  });

  it('defaults to Kafka without a database and provisions internal topics with broker replication', async () => {
    const config = loadConfig(baseEnv);
    expect(config.stateBackend).toBe('kafka');
    expect(config.databaseUrl).toBeUndefined();
    await run();

    expect(mocks.kafka).toHaveBeenCalledWith(config.kafka);
    expect(mocks.admin.createTopics).toHaveBeenCalledTimes(1);
    expect(mocks.admin.createTopics).toHaveBeenCalledWith({
      waitForLeaders: true,
      topics: [
        {
          topic: checkpoint,
          numPartitions: 1,
          configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
        },
        {
          topic: delivery,
          numPartitions: 8,
          configEntries: [
            { name: 'cleanup.policy', value: 'delete' },
            { name: 'retention.ms', value: '-1' },
            { name: 'retention.bytes', value: '-1' },
          ],
        },
      ],
    });
    expect(mocks.runtimeConstructor).toHaveBeenCalledWith(
      expect.objectContaining({
        sessions: [],
        checkpointTopic: checkpoint,
        deliveryTopic: delivery,
        batchSize: config.projectorBatchSize,
        startupConcurrency: 8,
        restoreConcurrency: 2,
        registryRequestTimeoutMs: config.registryRequestTimeoutMs,
      }),
    );
    expect(mocks.runtime.start).toHaveBeenCalledTimes(1);
    expectCleanedUp();
  });

  it('shares a read-only codec and closes it even when runtime cleanup fails', async () => {
    mocks.runtime.stop.mockRejectedValueOnce(new Error('stop failed'));
    await expect(run()).rejects.toThrow('Kafka exporter resource cleanup failed');
    expect(mocks.runtimeConstructor).toHaveBeenCalledWith(
      expect.objectContaining({ codec: mocks.codec }),
    );
    expect(mocks.codec.prepareWriter).not.toHaveBeenCalled();
    expect(mocks.codec.close).toHaveBeenCalledTimes(1);
  });

  it('SIGTERM stops the runtime immediately while discovery is still joining', async () => {
    let release!: () => void;
    mocks.runtime.addSessions.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          release = resolve;
        }),
    );
    const running = run();
    await vi.waitFor(() => expect(mocks.runtime.addSessions).toHaveBeenCalledOnce());
    process.emit('SIGTERM', 'SIGTERM');
    expect(mocks.runtime.stop).toHaveBeenCalledOnce();
    expect(mocks.admin.disconnect).not.toHaveBeenCalled();
    release();
    await running;
    expectCleanedUp();
  });

  it.each(['discovery', 'already aborted', 'connect', 'listTopics', 'createTopics'] as const)(
    'reports a late fatal drain after SIGTERM at %s',
    async (stage) => {
      let release!: () => void;
      const draining = new Promise<void>((resolve) => {
        release = resolve;
      });
      mocks.runtime.stop.mockImplementation(async () => {
        await draining;
        mocks.runtime.status.mockReturnValue({
          state: 'failed',
          ready: false,
          error: new Error('sensitive runtime failure'),
        });
      });
      // Release only once finally has started cleaning up the discovery admin.
      mocks.admin.disconnect.mockImplementation(async () => {
        expect(mocks.runtime.status().state).toBe('running');
        release();
      });
      const terminate = (): void => {
        const handler = process
          .listeners('SIGTERM')
          .find((listener) => !listeners.SIGTERM.includes(listener));
        expect(handler).toBeDefined();
        handler!('SIGTERM');
      };
      if (stage === 'already aborted') controller.abort();
      if (stage === 'connect') mocks.admin.connect.mockImplementationOnce(async () => terminate());
      if (stage === 'listTopics')
        mocks.admin.listTopics.mockImplementationOnce(async () => {
          terminate();
          return [];
        });
      if (stage === 'createTopics')
        mocks.admin.createTopics.mockImplementationOnce(async () => {
          terminate();
          return true;
        });
      if (stage === 'discovery') mocks.delay.mockImplementationOnce(async () => terminate());

      await expect(run()).rejects.toEqual(new Error('Kafka exporter runtime failed'));
      if (stage !== 'discovery') expect(mocks.runtime.start).not.toHaveBeenCalled();
      expectCleanedUp();
    },
  );

  it.each([new Error('initiating failure'), undefined])(
    'preserves the initiating try failure %s over a late fatal drain and cleanup failures',
    async (original) => {
      mocks.runtime.start.mockRejectedValueOnce(original);
      mocks.runtime.stop.mockImplementationOnce(async () => {
        mocks.runtime.status.mockReturnValue({ state: 'failed', ready: false });
        throw new Error('sensitive stop failure');
      });
      mocks.admin.disconnect.mockRejectedValueOnce(new Error('sensitive disconnect failure'));
      await expect(run()).rejects.toBe(original);
      expectCleanedUp();
    },
  );

  it('reports cleanup failure even on an early return', async () => {
    controller.abort();
    mocks.runtime.stop.mockRejectedValueOnce(new Error('sensitive stop failure'));
    await expect(run()).rejects.toEqual(new Error('Kafka exporter resource cleanup failed'));
    expectCleanedUp();
  });

  it.each([undefined, 'raw', 'avro'] as const)(
    'isolates discovery and internal state for codec encoding %s',
    async (encoding) => {
      const prefix = 'public.default.';
      const suffix = encoding === 'avro' ? '-avro' : '';
      mocks.codecFactory.mockReturnValue({ ...mocks.codec, encoding });
      mocks.admin.listTopics.mockResolvedValue(
        [
          first,
          first + '-avro',
          checkpoint,
          checkpoint + '-avro',
          delivery,
          delivery + '-avro',
        ].map((topic) => prefix + topic),
      );
      await run(prefix);
      expect(mocks.admin.createTopics).not.toHaveBeenCalled();
      expect(mocks.runtime.addSessions).toHaveBeenCalledWith([
        { topic: prefix + first + suffix, workspaceId: 'ws_a', sessionId: 'ses_first' },
      ]);
      expect(mocks.runtimeConstructor).toHaveBeenCalledWith(
        expect.objectContaining({
          checkpointTopic: prefix + checkpoint + suffix,
          deliveryTopic: prefix + delivery + suffix,
          groupId: `observability-exporter-kafka-v1-${createHash('sha256').update(prefix).digest('hex').slice(0, 16)}${suffix}`,
        }),
      );
      expect(mocks.codec.prepareWriter).not.toHaveBeenCalled();
      expect(mocks.codec.close).toHaveBeenCalledTimes(1);
      expectCleanedUp();
    },
  );

  it('provisions Avro internal topics rather than consuming existing raw state through KoP aliases', async () => {
    const prefix = 'public.default.';
    mocks.codecFactory.mockReturnValue({ ...mocks.codec, encoding: 'avro' });
    mocks.admin.listTopics.mockResolvedValue([checkpoint, delivery, first, first + '-avro']);
    await run(prefix, 'sasl-plain-token-tls', 'bare-alias');
    expect(
      mocks.admin.createTopics.mock.calls[0]![0].topics.map(
        (entry: { topic: string }) => entry.topic,
      ),
    ).toEqual([prefix + checkpoint + '-avro', prefix + delivery + '-avro']);
    expect(mocks.runtime.addSessions).toHaveBeenCalledWith([
      { topic: prefix + first + '-avro', workspaceId: 'ws_a', sessionId: 'ses_first' },
    ]);
    expectCleanedUp();
  });

  it('uses the configured codec without preparing a writer in a reader-only process', async () => {
    const kafkaTranscript = {
      encoding: 'avro' as const,
      schemaRegistry: { url: 'http://schemas.local' },
    };
    await runBrokerExporter({ ...loadConfig(baseEnv), kafkaTranscript }, controller.signal);
    expect(mocks.codecFactory).toHaveBeenCalledWith(kafkaTranscript);
    expect(mocks.codec.prepareWriter).not.toHaveBeenCalled();
    expect(mocks.codec.close).toHaveBeenCalledTimes(1);
  });

  it.each([
    { prefix: '', mode: 'plaintext', listingMode: 'canonical', listed: [checkpoint, delivery] },
    { prefix: '', mode: 'plaintext', listingMode: 'bare-alias', listed: [checkpoint, delivery] },
    {
      prefix: 'public.default.',
      mode: 'sasl-plain-token-tls',
      listingMode: 'bare-alias',
      listed: [checkpoint, delivery],
    },
    {
      prefix: 'public.default.',
      mode: 'plaintext',
      listingMode: 'canonical',
      listed: ['public.default.' + checkpoint, 'public.default.' + delivery],
    },
  ])(
    'reuses internal topics listed as $listed with prefix $prefix',
    async ({ prefix, mode, listingMode, listed }) => {
      mocks.admin.listTopics.mockResolvedValue(listed);
      await run(prefix, mode, listingMode);
      expect(mocks.admin.createTopics).not.toHaveBeenCalled();
      expect(mocks.runtimeConstructor).toHaveBeenCalledWith(
        expect.objectContaining({
          checkpointTopic: prefix + checkpoint,
          deliveryTopic: prefix + delivery,
        }),
      );
    },
  );

  it('creates only the missing prefixed topic when KoP lists its sibling bare', async () => {
    mocks.admin.listTopics.mockResolvedValue([checkpoint]);
    await run('public.default.', 'sasl-plain-token-tls', 'bare-alias');
    const topics = mocks.admin.createTopics.mock.calls[0]![0].topics;
    expect(topics).toHaveLength(1);
    expect(topics[0].topic).toBe('public.default.' + delivery);
    expect(topics[0]).not.toHaveProperty('replicationFactor');
  });

  it('discovers new Sessions on later iterations and excludes non-Session topics', async () => {
    const prefix = 'public.default.';
    const invalid = [
      checkpoint,
      delivery,
      '__consumer_offsets',
      'orca.ws.sessions..events',
      'orca..sessions.ses_bad.events',
      'orca.ws.sessions.ses_bad.events.extra',
      'orca.ws.sessions.ses/bad.events',
      'other.namespace.' + first,
    ];
    mocks.admin.listTopics
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([...invalid, first])
      .mockResolvedValueOnce([...invalid, first, prefix + second]);
    mocks.delay.mockResolvedValueOnce(undefined);
    await run(prefix, 'sasl-plain-token-tls', 'bare-alias');

    const firstRoute = { topic: prefix + first, workspaceId: 'ws_a', sessionId: 'ses_first' };
    expect(mocks.runtime.addSessions.mock.calls).toEqual([
      [[firstRoute]],
      [[firstRoute, { topic: prefix + second, workspaceId: 'ws_b', sessionId: 'ses_second' }]],
    ]);
    expect(mocks.delay).toHaveBeenCalledTimes(2);
    expect(mocks.delay).toHaveBeenCalledWith(1000, undefined, { signal: expect.any(AbortSignal) });
  });

  it.each(['plaintext', 'custom', 'sasl-plain-tls', 'sasl-plain-token-tls'])(
    'creates canonical internal topics rather than reusing bare names in %s mode',
    async (mode) => {
      const prefix = 'public.default.';
      mocks.admin.listTopics.mockResolvedValue([checkpoint, delivery]);
      await run(prefix, mode);
      expect(mocks.admin.createTopics).toHaveBeenCalledTimes(1);
      expect(
        mocks.admin.createTopics.mock.calls[0]![0].topics.map(
          (topic: { topic: string }) => topic.topic,
        ),
      ).toEqual([prefix + checkpoint, prefix + delivery]);
    },
  );

  it.each(['plaintext', 'custom', 'sasl-plain-tls', 'sasl-plain-token-tls'])(
    'ignores bare Sessions with a prefix in %s mode while accepting canonical discoveries',
    async (mode) => {
      const prefix = 'public.default.';
      mocks.admin.listTopics.mockResolvedValue([first, prefix + second, prefix + second]);
      await run(prefix, mode);
      const route = { topic: prefix + second, workspaceId: 'ws_b', sessionId: 'ses_second' };
      expect(mocks.runtime.addSessions).toHaveBeenCalledWith([route, route]);
    },
  );

  it.each(['plaintext', 'custom', 'sasl-plain-tls', 'sasl-plain-token-tls'])(
    'reuses bare internal topics and discovers aliases only when explicitly enabled with %s',
    async (mode) => {
      const prefix = 'public.default.';
      mocks.admin.listTopics.mockResolvedValue([checkpoint, delivery, first, prefix + second]);
      await run(prefix, mode, 'bare-alias');
      expect(mocks.admin.createTopics).not.toHaveBeenCalled();
      expect(mocks.runtime.addSessions).toHaveBeenCalledWith([
        { topic: prefix + first, workspaceId: 'ws_a', sessionId: 'ses_first' },
        { topic: prefix + second, workspaceId: 'ws_b', sessionId: 'ses_second' },
      ]);
    },
  );

  it.each(['canonical', 'bare-alias'])(
    'discovers bare Sessions without a prefix in %s mode',
    async (listingMode) => {
      mocks.admin.listTopics.mockResolvedValue([first]);
      await run('', 'plaintext', listingMode);
      expect(mocks.runtime.addSessions).toHaveBeenCalledWith([
        { topic: first, workspaceId: 'ws_a', sessionId: 'ses_first' },
      ]);
    },
  );

  it('passes both KoP alias and canonical discoveries to runtime deduplication', async () => {
    const prefix = 'public.default.';
    mocks.admin.listTopics.mockResolvedValue([first, prefix + first]);
    await run(prefix, 'sasl-plain-token-tls', 'bare-alias');
    const route = { topic: prefix + first, workspaceId: 'ws_a', sessionId: 'ses_first' };
    expect(mocks.runtime.addSessions).toHaveBeenCalledWith([route, route]);
  });

  it.each(['SIGTERM', 'SIGINT', 'abort'] as const)(
    'cleans up on %s during discovery sleep',
    async (signal) => {
      mocks.delay.mockImplementation(async (_ms, _value, options: { signal: AbortSignal }) => {
        expect(options.signal.aborted).toBe(false);
        if (signal === 'abort') controller.abort();
        else {
          // Deliver only this bootstrap's handler, not unrelated process listeners.
          const handler = process
            .listeners(signal)
            .find((listener) => !listeners[signal].includes(listener));
          expect(handler).toBeDefined();
          handler!(signal);
        }
        expect(options.signal.aborted).toBe(true);
        throw new Error('aborted sleep');
      });
      await run();
      expectCleanedUp();
    },
  );

  it('skips startup when the caller is already aborted', async () => {
    controller.abort();
    await run();
    expect(mocks.health.listen).not.toHaveBeenCalled();
    expect(mocks.admin.connect).not.toHaveBeenCalled();
    expect(mocks.runtime.start).not.toHaveBeenCalled();
    expectCleanedUp();
  });

  it.each(['connect', 'createTopics', 'start', 'discovery', 'addSessions', 'delay'])(
    'propagates %s failure while cleaning up acquired resources',
    async (stage) => {
      const error = new Error(stage + ' failed');
      if (stage === 'connect') mocks.admin.connect.mockRejectedValueOnce(error);
      if (stage === 'createTopics') mocks.admin.createTopics.mockRejectedValueOnce(error);
      if (stage === 'start') mocks.runtime.start.mockRejectedValueOnce(error);
      if (stage === 'discovery')
        mocks.admin.listTopics.mockResolvedValueOnce([]).mockRejectedValueOnce(error);
      if (stage === 'addSessions') mocks.runtime.addSessions.mockRejectedValueOnce(error);
      if (stage === 'delay') mocks.delay.mockRejectedValueOnce(error);
      await expect(run()).rejects.toBe(error);
      expectCleanedUp();
    },
  );

  it('cleans up after a health bind failure without starting Kafka', async () => {
    const error = new Error('address in use');
    mocks.health.listen.mockImplementation(() => {
      mocks.health.once.mock.calls[0]![1](error);
      return mocks.health;
    });
    await expect(run()).rejects.toBe(error);
    expect(mocks.admin.connect).not.toHaveBeenCalled();
    expect(mocks.runtime.start).not.toHaveBeenCalled();
    expectCleanedUp();
  });

  it('disconnects discovery admin even when runtime cleanup fails', async () => {
    mocks.runtime.stop.mockRejectedValueOnce(new Error('stop failed'));
    await expect(run()).rejects.toThrow('Kafka exporter resource cleanup failed');
    expectCleanedUp();
  });

  it('preserves the original failure when cleanup also fails', async () => {
    const original = new Error('startup failed');
    mocks.runtime.start.mockRejectedValueOnce(original);
    mocks.runtime.stop.mockRejectedValueOnce(new Error('stop failed'));
    mocks.admin.disconnect.mockRejectedValueOnce(new Error('disconnect failed'));
    await expect(run()).rejects.toBe(original);
    expectCleanedUp();
  });

  it('fails and cleans up when the runtime enters failed state', async () => {
    mocks.runtime.status.mockReturnValue({ state: 'failed', ready: false });
    await expect(run()).rejects.toThrow('Kafka exporter runtime failed');
    expect(mocks.runtime.addSessions).not.toHaveBeenCalled();
    expectCleanedUp();
  });

  it('gates readiness on discovery, runtime readiness, broker access, and shutdown', async () => {
    function probes(): { isLive: () => boolean; checkReady: () => Promise<boolean> } {
      return mocks.createHealth.mock.calls[0]![0];
    }
    mocks.runtime.start.mockImplementation(async () => {
      expect(probes().isLive()).toBe(true);
      expect(await probes().checkReady()).toBe(false);
      expect(mocks.admin.listTopics).toHaveBeenCalledTimes(1);
    });
    mocks.delay.mockImplementation(async () => {
      expect(await probes().checkReady()).toBe(true);
      expect(mocks.admin.listTopics).toHaveBeenCalledTimes(3);
      mocks.runtime.status.mockReturnValue({ state: 'running', ready: false });
      expect(await probes().checkReady()).toBe(false);
      expect(mocks.admin.listTopics).toHaveBeenCalledTimes(3);
      mocks.runtime.status.mockReturnValue({ state: 'running', ready: true });
      const error = new Error('broker unavailable');
      mocks.admin.listTopics.mockRejectedValueOnce(error);
      await expect(probes().checkReady()).rejects.toBe(error);
      expect(probes().isLive()).toBe(true);
      controller.abort();
      expect(probes().isLive()).toBe(false);
      expect(await probes().checkReady()).toBe(false);
    });
    await run();
    expectCleanedUp();
  });
});
