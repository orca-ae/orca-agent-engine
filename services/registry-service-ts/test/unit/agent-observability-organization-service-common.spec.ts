// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  ORGANIZATION_AGENT_OBSERVABILITY_DB_TRANSACTION_MAX_ATTEMPTS,
  runOrganizationAgentObservabilityFinalizationWithRetry,
  runOrganizationAgentObservabilityRepeatableReadTransactionWithRetry,
} from '../../src/domain/agent-observability-organization-service-common.js';
import type { DbClient, DbTransaction } from '../../src/persistence/postgres/client.js';

describe('organization observability finalization retry', () => {
  it('reruns a generic DB-only transaction from a fresh callback after serialization failure', async () => {
    const { db, transaction } = transactionDb();
    const attempts: number[] = [];

    await expect(
      runOrganizationAgentObservabilityRepeatableReadTransactionWithRetry(
        { db },
        async (_tx, attempt) => {
          attempts.push(attempt);
          if (attempt === 1) throw postgresError('40001');
          return 'committed';
        },
      ),
    ).resolves.toBe('committed');

    expect(attempts).toEqual([1, 2]);
    expect(transaction).toHaveBeenCalledTimes(2);
    expect(transaction).toHaveBeenNthCalledWith(1, expect.any(Function), {
      isolationLevel: 'repeatable read',
    });
    expect(transaction).toHaveBeenNthCalledWith(2, expect.any(Function), {
      isolationLevel: 'repeatable read',
    });
  });

  it('rethrows only after bounded PostgreSQL serialization retries', async () => {
    const serializationError = postgresError('40001');
    const { db, transaction } = transactionDb();
    let attempts = 0;

    await expect(
      runOrganizationAgentObservabilityFinalizationWithRetry({ db }, async () => {
        attempts += 1;
        throw serializationError;
      }),
    ).rejects.toBe(serializationError);

    expect(attempts).toBe(ORGANIZATION_AGENT_OBSERVABILITY_DB_TRANSACTION_MAX_ATTEMPTS);
    expect(transaction).toHaveBeenCalledTimes(
      ORGANIZATION_AGENT_OBSERVABILITY_DB_TRANSACTION_MAX_ATTEMPTS,
    );
  });

  it('does not retry a non-serialization error', async () => {
    const databaseError = postgresError('23505');
    const { db, transaction } = transactionDb();
    let attempts = 0;

    await expect(
      runOrganizationAgentObservabilityFinalizationWithRetry({ db }, async () => {
        attempts += 1;
        throw databaseError;
      }),
    ).rejects.toBe(databaseError);

    expect(attempts).toBe(1);
    expect(transaction).toHaveBeenCalledOnce();
  });
});

function transactionDb(): { db: DbClient; transaction: ReturnType<typeof vi.fn> } {
  const transaction = vi.fn(async (callback: (tx: DbTransaction) => Promise<unknown>) =>
    callback({} as DbTransaction),
  );
  return { db: { transaction } as unknown as DbClient, transaction };
}

function postgresError(code: string): Error & { code: string } {
  return Object.assign(new Error(`PostgreSQL ${code}`), { code });
}
