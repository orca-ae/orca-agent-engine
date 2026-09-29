// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  codec: { prepareWriter: vi.fn(), close: vi.fn() },
  factory: vi.fn(),
  storeConstructor: vi.fn(),
  sourceConstructor: vi.fn(),
  store: { close: vi.fn() },
  source: { status: vi.fn() },
  pool: { end: vi.fn() },
  runtime: { startIngestion: vi.fn(), stopIngestion: vi.fn(), run: vi.fn() },
  health: { once: vi.fn(), listen: vi.fn(), close: vi.fn(), closeAllConnections: vi.fn() },
}));
vi.mock('@orca/transcript-store', () => ({
  createKafkaTranscriptCodec: mocks.factory,
  KafkaTranscriptStore: mocks.storeConstructor,
  KafkaSessionEventSource: mocks.sourceConstructor,
}));
vi.mock('kafkajs', () => ({ Kafka: vi.fn() }));
vi.mock('pg', () => ({ Pool: vi.fn(() => mocks.pool) }));
vi.mock('../../src/config.js', () => ({ buildExporterPoolConfig: () => ({}) }));
vi.mock('../../src/persistence.js', () => ({
  applyObservabilityExporterMigrations: vi.fn(),
  ObservabilityExporterRepository: vi.fn(),
}));
vi.mock('../../src/runtime.js', () => ({
  KafkaObservabilityExporterRuntime: vi.fn(() => mocks.runtime),
}));
vi.mock('../../src/health.js', () => ({ createExporterHealthServer: () => mocks.health }));

import { runPostgresExporter } from '../../src/postgres-main.js';
import type { ExporterConfig } from '../../src/config.js';

describe('legacy exporter codec ownership', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.factory.mockReturnValue(mocks.codec);
    mocks.storeConstructor.mockReturnValue(mocks.store);
    mocks.sourceConstructor.mockReturnValue(mocks.source);
    mocks.codec.close.mockResolvedValue(undefined);
    mocks.store.close.mockResolvedValue(undefined);
    mocks.runtime.stopIngestion.mockResolvedValue(undefined);
    mocks.health.listen.mockImplementation((_port, _host, ready: () => void) => ready());
  });

  it.each([undefined, 'raw', 'avro'] as const)(
    'shares a read-only %s codec and isolates the inbox group',
    async (encoding) => {
      const codec = { ...mocks.codec, encoding };
      mocks.factory.mockReturnValue(codec);
      await runPostgresExporter({
        kafka: { brokers: ['localhost:9092'] },
        kafkaTopicPrefix: '',
        registryInternalBaseUrl: 'http://registry.internal:8081',
        internalServiceToken: 'x'.repeat(32),
      } as ExporterConfig);
      expect(mocks.factory).toHaveBeenCalledWith({});
      expect(mocks.storeConstructor).toHaveBeenCalledWith(expect.objectContaining({ codec }));
      expect(mocks.sourceConstructor).toHaveBeenCalledWith(
        expect.objectContaining({
          codec,
          groupId: 'observability-exporter-inbox' + (encoding === 'avro' ? '-avro' : ''),
        }),
      );
      expect(mocks.codec.prepareWriter).not.toHaveBeenCalled();
      expect(mocks.codec.close).toHaveBeenCalledTimes(1);
      expect(mocks.runtime.stopIngestion.mock.invocationCallOrder[0]).toBeLessThan(
        mocks.codec.close.mock.invocationCallOrder[0]!,
      );
      expect(mocks.pool.end).toHaveBeenCalledTimes(1);
    },
  );

  it('closes the configured reader after ingestion startup fails', async () => {
    const error = new Error('ingestion failed');
    mocks.runtime.startIngestion.mockRejectedValueOnce(error);
    const kafkaTranscript = {
      encoding: 'avro' as const,
      schemaRegistry: { url: 'http://schemas.local' },
    };
    await expect(
      runPostgresExporter({
        kafka: { brokers: ['localhost:9092'] },
        kafkaTopicPrefix: '',
        kafkaTranscript,
        registryInternalBaseUrl: 'http://registry.internal:8081',
        internalServiceToken: 'x'.repeat(32),
      } as ExporterConfig),
    ).rejects.toBe(error);
    expect(mocks.factory).toHaveBeenCalledWith(kafkaTranscript);
    expect(mocks.codec.prepareWriter).not.toHaveBeenCalled();
    expect(mocks.codec.close).toHaveBeenCalledTimes(1);
    expect(mocks.pool.end).toHaveBeenCalledTimes(1);
  });
});
