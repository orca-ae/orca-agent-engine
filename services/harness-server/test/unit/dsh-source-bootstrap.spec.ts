// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it, vi } from 'vitest';

const dependencies = vi.hoisted(() => ({
  config: {
    transcriptStoreBackend: 'postgres',
    internalServiceToken: 'test-only-token-never-a-real-secret',
    consumerGroupId: 'dsh-control',
    postgresEventPollIntervalMs: 10,
    postgresEventLeaseMs: 100,
    pulsarServiceUrl: 'pulsar://test',
    pulsarTenant: 'public',
    pulsarNamespace: 'default',
    pulsarTopicPrefix: 'orca.',
    pulsarReceiveTimeoutMs: 100,
  },
  postgres: vi.fn(),
  pulsar: vi.fn(),
}));
vi.mock('../../src/config.js', () => ({ loadConfig: () => dependencies.config }));
vi.mock('../../src/healthz.js', () => ({ buildHealthzServer: () => ({}) }));
vi.mock('pg', () => ({ Pool: vi.fn(() => ({ end: vi.fn() })) }));
vi.mock('@orca/transcript-store', async (original) => ({
  ...(await original<Record<string, unknown>>()),
  PostgresSessionEventSource: dependencies.postgres,
  PulsarSessionEventSource: dependencies.pulsar,
  PostgresTranscriptStore: vi.fn(),
  PulsarTranscriptStore: vi.fn(),
  applyPostgresTranscriptMigrations: vi.fn(),
}));
vi.mock('../../src/clients/registry.js', () => ({
  RegistryClient: class {
    constructor() {
      throw new Error('bootstrap backend captured');
    }
  },
  RegistryInvalidRuntimeBindingError: class extends Error {},
}));

it.each(['postgres', 'pulsar'])(
  'actual %s bootstrap admits Registry controls to the dispatcher rather than filtering them out',
  async (backend) => {
    vi.resetModules();
    dependencies.config.transcriptStoreBackend = backend;
    await expect(import('../../src/main.js')).rejects.toThrow('bootstrap backend captured');
    const source = backend === 'postgres' ? dependencies.postgres : dependencies.pulsar;
    expect(source).toHaveBeenLastCalledWith(expect.objectContaining({ includeAllEvents: true }));
  },
);
