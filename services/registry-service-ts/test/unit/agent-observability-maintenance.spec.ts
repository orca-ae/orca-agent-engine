// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { startAgentObservabilityMaintenance } from '../../src/domain/agent-observability-maintenance.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import type { SecretStore } from '../../src/secrets/secret-provider.js';

describe('agent observability maintenance', () => {
  it('always retains and runs secret cleanup only when a SecretStore exists', async () => {
    const calls: string[] = [];
    const maintenance = startAgentObservabilityMaintenance({
      db: {} as DbClient,
      intervalMs: 60_000,
      operations: {
        prune: async () => {
          calls.push('prune');
        },
        reconcileStaging: async () => {
          calls.push('staging');
        },
        reconcileSupersededSecrets: async () => {
          calls.push('superseded');
        },
      },
    });
    await waitFor(() => calls.length === 1);
    expect(calls).toEqual(['prune']);
    await maintenance.run();
    expect(calls).toEqual(['prune', 'prune']);
    await maintenance.stop();
  });

  it('does not overlap passes and uses fixed sanitized warnings', async () => {
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    const warnings: string[] = [];
    const store = {} as SecretStore;
    const maintenance = startAgentObservabilityMaintenance({
      db: {} as DbClient,
      secretStore: store,
      intervalMs: 60_000,
      logger: { warn: (message) => warnings.push(message) },
      operations: {
        prune: async () => blocked,
        reconcileStaging: async () => {
          throw new Error('provider echoed secret');
        },
        reconcileSupersededSecrets: async () => {
          throw new Error('provider echoed secret');
        },
      },
    });
    expect(await maintenance.run()).toBe(false);
    release();
    await waitFor(() => warnings.length === 2);
    expect(warnings).toEqual([
      'registry-service-ts failed agent observability staging cleanup',
      'registry-service-ts failed agent observability secret cleanup',
    ]);
    await maintenance.stop();
  });

  it('stops new stages and waits for an active prune pass', async () => {
    const pruneStarted = deferred<void>();
    const releasePrune = deferred<void>();
    const calls: string[] = [];
    const maintenance = startAgentObservabilityMaintenance({
      db: {} as DbClient,
      secretStore: {} as SecretStore,
      intervalMs: 60_000,
      operations: {
        prune: async () => {
          calls.push('prune');
          pruneStarted.resolve();
          await releasePrune.promise;
        },
        reconcileStaging: async () => calls.push('staging'),
        reconcileSupersededSecrets: async () => calls.push('superseded'),
      },
    });
    await pruneStarted.promise;

    let stopped = false;
    const stopping = maintenance.stop().then(() => {
      stopped = true;
    });
    await tick();
    expect(stopped).toBe(false);
    expect(calls).toEqual(['prune']);

    releasePrune.resolve();
    await stopping;
    expect(calls).toEqual(['prune']);
    expect(await maintenance.run()).toBe(false);
  });

  it('stops after staging cleanup without starting superseded cleanup', async () => {
    const stagingStarted = deferred<void>();
    const releaseStaging = deferred<void>();
    const calls: string[] = [];
    const maintenance = startAgentObservabilityMaintenance({
      db: {} as DbClient,
      secretStore: {} as SecretStore,
      intervalMs: 60_000,
      operations: {
        prune: async () => calls.push('prune'),
        reconcileStaging: async () => {
          calls.push('staging');
          stagingStarted.resolve();
          await releaseStaging.promise;
        },
        reconcileSupersededSecrets: async () => calls.push('superseded'),
      },
    });
    await stagingStarted.promise;

    let stopped = false;
    const stopping = maintenance.stop().then(() => {
      stopped = true;
    });
    await tick();
    expect(stopped).toBe(false);
    releaseStaging.resolve();
    await stopping;
    expect(calls).toEqual(['prune', 'staging']);
  });

  it('aborts a never-resolving operation at its pass deadline without starting later stages', async () => {
    const secondPruneStarted = deferred<void>();
    const calls: string[] = [];
    let pruneCalls = 0;
    let deadlineSignal: AbortSignal | undefined;
    const maintenance = startAgentObservabilityMaintenance({
      db: {} as DbClient,
      secretStore: {} as SecretStore,
      intervalMs: 60_000,
      passTimeoutMs: 20,
      shutdownGraceMs: 20,
      logger: { warn: () => {} },
      operations: {
        prune: async (_db, signal) => {
          calls.push('prune');
          pruneCalls += 1;
          if (pruneCalls === 1) return;
          deadlineSignal = signal;
          secondPruneStarted.resolve();
          await new Promise<void>(() => {});
        },
        reconcileStaging: async () => calls.push('staging'),
        reconcileSupersededSecrets: async () => calls.push('superseded'),
      },
    });
    await waitFor(() => calls.length === 3);
    await tick();
    calls.length = 0;

    const startedAt = Date.now();
    const pass = maintenance.run();
    await secondPruneStarted.promise;
    await expect(pass).resolves.toBe(true);

    expect(Date.now() - startedAt).toBeLessThan(500);
    expect(deadlineSignal?.aborted).toBe(true);
    expect(calls).toEqual(['prune']);
    await maintenance.stop();
  });

  it('gives later cleanup a first chance after staging consumes a pass deadline', async () => {
    const stagingStarted = deferred<void>();
    const stagingDeadlineObserved = deferred<void>();
    const supersededStarted = deferred<void>();
    const calls: string[] = [];
    let stagingCalls = 0;
    const maintenance = startAgentObservabilityMaintenance({
      db: {} as DbClient,
      secretStore: {} as SecretStore,
      intervalMs: 60_000,
      passTimeoutMs: 20,
      logger: { warn: () => {} },
      operations: {
        prune: async () => calls.push('prune'),
        reconcileStaging: async (_db, _store, signal) => {
          calls.push('staging');
          stagingCalls += 1;
          if (stagingCalls !== 1) return;
          stagingStarted.resolve();
          await new Promise<void>((resolve) => {
            signal.addEventListener(
              'abort',
              () => {
                stagingDeadlineObserved.resolve();
                resolve();
              },
              { once: true },
            );
          });
        },
        reconcileSupersededSecrets: async () => {
          calls.push('superseded');
          supersededStarted.resolve();
        },
      },
    });
    await stagingStarted.promise;
    await stagingDeadlineObserved.promise;
    await nextTurn();

    await expect(maintenance.run()).resolves.toBe(true);
    await supersededStarted.promise;
    expect(calls).toEqual(['prune', 'staging', 'superseded', 'prune', 'staging']);
    await maintenance.stop();
  });

  it('bounds shutdown when an active operation ignores cancellation', async () => {
    const pruneStarted = deferred<void>();
    const latePrune = deferred<void>();
    const calls: string[] = [];
    const warnings: string[] = [];
    let signal: AbortSignal | undefined;
    const maintenance = startAgentObservabilityMaintenance({
      db: {} as DbClient,
      secretStore: {} as SecretStore,
      intervalMs: 60_000,
      passTimeoutMs: 60_000,
      shutdownGraceMs: 20,
      logger: { warn: (message) => warnings.push(message) },
      operations: {
        prune: async (_db, nextSignal) => {
          calls.push('prune');
          signal = nextSignal;
          pruneStarted.resolve();
          await latePrune.promise;
        },
        reconcileStaging: async () => calls.push('staging'),
        reconcileSupersededSecrets: async () => calls.push('superseded'),
      },
    });
    await pruneStarted.promise;

    const startedAt = Date.now();
    await maintenance.stop();

    expect(Date.now() - startedAt).toBeLessThan(500);
    expect(signal?.aborted).toBe(true);
    expect(calls).toEqual(['prune']);
    expect(warnings).toEqual([
      'registry-service-ts agent observability maintenance shutdown grace exceeded',
    ]);

    latePrune.reject(new Error('late maintenance operation rejection'));
    await tick();
  });

  it('does not start an initial pass after immediate shutdown', async () => {
    const calls: string[] = [];
    const maintenance = startAgentObservabilityMaintenance({
      db: {} as DbClient,
      intervalMs: 60_000,
      operations: {
        prune: async () => calls.push('prune'),
        reconcileStaging: async () => calls.push('staging'),
        reconcileSupersededSecrets: async () => calls.push('superseded'),
      },
    });

    await maintenance.stop();
    expect(calls).toEqual([]);
  });
});

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error('condition was not reached');
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((next, fail) => {
    resolve = next;
    reject = fail;
  });
  return { promise, resolve, reject };
}

async function tick(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 1));
}

function nextTurn(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}
