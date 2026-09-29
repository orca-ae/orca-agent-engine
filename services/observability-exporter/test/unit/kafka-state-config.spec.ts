// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { loadConfig } from '../../src/config.js';

const env = {
  TRANSCRIPT_STORE_BACKEND: 'kafka',
  INTERNAL_SERVICE_TOKEN: 'test-token',
  REGISTRY_INTERNAL_BASE_URL: 'http://registry:8081',
};
describe('Kafka state resource configuration', () => {
  it('uses bounded defaults without changing the shared readiness contract', () => {
    expect(loadConfig(env)).toMatchObject({
      kafkaProjectorConcurrency: 2,
      kafkaStateMaxBytes: 1073741824,
      kafkaStateCatchupTimeoutMs: 120000,
      kafkaMaxAssemblyBytes: 33554432,
      kafkaMaxTransactionBytes: 67108864,
    });
    expect(loadConfig(env).kafkaStateDirectory).toBeUndefined();
  });
  it('accepts an explicit scratch directory and larger operator budgets', () => {
    expect(
      loadConfig({
        ...env,
        OBSERVABILITY_KAFKA_STATE_DIRECTORY: '/var/run/orca/exporter-state',
        OBSERVABILITY_KAFKA_STATE_MAX_BYTES: '2147483648',
        OBSERVABILITY_KAFKA_STATE_CATCHUP_TIMEOUT_MS: '180000',
        OBSERVABILITY_KAFKA_PROJECTOR_CONCURRENCY: '1',
        OBSERVABILITY_KAFKA_MAX_ASSEMBLY_BYTES: '67108864',
        OBSERVABILITY_KAFKA_MAX_TRANSACTION_BYTES: '134217728',
      }),
    ).toMatchObject({
      kafkaStateDirectory: '/var/run/orca/exporter-state',
      kafkaStateMaxBytes: 2147483648,
      kafkaStateCatchupTimeoutMs: 180000,
      kafkaProjectorConcurrency: 1,
      kafkaMaxAssemblyBytes: 67108864,
      kafkaMaxTransactionBytes: 134217728,
    });
  });
  it.each([
    ['OBSERVABILITY_KAFKA_STATE_DIRECTORY', 'relative/path'],
    ['OBSERVABILITY_KAFKA_STATE_DIRECTORY', '/bad\0path'],
    ['OBSERVABILITY_KAFKA_STATE_MAX_BYTES', '100'],
    ['OBSERVABILITY_KAFKA_STATE_CATCHUP_TIMEOUT_MS', '600001'],
    ['OBSERVABILITY_KAFKA_PROJECTOR_CONCURRENCY', '33'],
    ['OBSERVABILITY_KAFKA_MAX_ASSEMBLY_BYTES', '67108865'],
    ['OBSERVABILITY_KAFKA_MAX_TRANSACTION_BYTES', '134217729'],
  ])('rejects invalid Kafka %s without reading it in Postgres mode', (key, value) => {
    expect(() => loadConfig({ ...env, [key]: value })).toThrow();
    expect(() =>
      loadConfig({
        ...env,
        OBSERVABILITY_EXPORTER_STATE_BACKEND: 'postgres',
        OBSERVABILITY_EXPORTER_DATABASE_URL: 'postgres://localhost/exporter',
        [key]: value,
      }),
    ).not.toThrow();
  });
});
