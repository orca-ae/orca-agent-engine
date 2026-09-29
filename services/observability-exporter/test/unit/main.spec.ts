// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  loadConfig: vi.fn(),
  broker: vi.fn(async () => {}),
  postgres: vi.fn(async () => {}),
  postgresModule: vi.fn(),
}));
vi.mock('../../src/config.js', () => ({ loadConfig: mocks.loadConfig }));
vi.mock('../../src/broker-main.js', () => ({ runBrokerExporter: mocks.broker }));
vi.mock('../../src/postgres-main.js', () => {
  mocks.postgresModule();
  return { runPostgresExporter: mocks.postgres };
});

describe('exporter entrypoint backend isolation', () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
  });

  it('does not import or start the SQL bootstrap for Kafka mode', async () => {
    const config = { stateBackend: 'kafka' };
    mocks.loadConfig.mockReturnValue(config);
    await import('../../src/main.js');
    await vi.waitFor(() => expect(mocks.broker).toHaveBeenCalledWith(config));
    expect(mocks.postgresModule).not.toHaveBeenCalled();
    expect(mocks.postgres).not.toHaveBeenCalled();
  });

  it('retains SQL bootstrap only for an explicit legacy selection', async () => {
    const config = { stateBackend: 'postgres', databaseUrl: 'postgres://legacy' };
    mocks.loadConfig.mockReturnValue(config);
    await import('../../src/main.js');
    await vi.waitFor(() => expect(mocks.postgres).toHaveBeenCalledWith(config));
    expect(mocks.broker).not.toHaveBeenCalled();
  });
});
