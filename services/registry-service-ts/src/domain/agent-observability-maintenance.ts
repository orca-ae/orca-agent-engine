// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { DbClient } from '../persistence/postgres/client.js';
import type { SecretStore } from '../secrets/secret-provider.js';
import {
  pruneAgentObservabilityMutationRetention,
  reconcileAgentObservabilitySecretCleanup,
  reconcileAgentObservabilityStagingIntents,
} from './agent-observability-mutations.js';

export const AGENT_OBSERVABILITY_MAINTENANCE_INTERVAL_MS = 60_000;
// Leaves room for a 45-second cleanup delete under its 60-second lease while
// still ending before the next normal maintenance interval.
export const AGENT_OBSERVABILITY_MAINTENANCE_PASS_TIMEOUT_MS = 55_000;
export const AGENT_OBSERVABILITY_MAINTENANCE_SHUTDOWN_GRACE_MS = 5_000;

export interface AgentObservabilityMaintenanceLogger {
  warn(message: string): void;
}

export interface AgentObservabilityMaintenanceOperations {
  prune(db: DbClient, signal: AbortSignal): Promise<unknown>;
  reconcileStaging(db: DbClient, secretStore: SecretStore, signal: AbortSignal): Promise<unknown>;
  reconcileSupersededSecrets(
    db: DbClient,
    secretStore: SecretStore,
    signal: AbortSignal,
  ): Promise<unknown>;
}

const defaultOperations: AgentObservabilityMaintenanceOperations = {
  prune: (db, _signal) => pruneAgentObservabilityMutationRetention(db),
  reconcileStaging: (db, secretStore, signal) =>
    reconcileAgentObservabilityStagingIntents(db, secretStore, { signal }),
  reconcileSupersededSecrets: (db, secretStore, signal) =>
    reconcileAgentObservabilitySecretCleanup(db, secretStore, { signal }),
};

export interface AgentObservabilityMaintenance {
  /** Starts a pass; false means stopped or another pass was live. */
  run(): Promise<boolean>;
  /** Stop future passes and wait only the configured grace for in-flight work. */
  stop(): Promise<void>;
}

/**
 * Retention never needs SecretStore. Staging and superseded-reference cleanup
 * run only when the deployment can issue durable deletes. All error messages
 * are fixed so a SecretStore implementation cannot reflect a ref or bundle.
 */
export function startAgentObservabilityMaintenance(input: {
  db: DbClient;
  secretStore?: SecretStore | undefined;
  intervalMs?: number;
  /** Bounds one maintenance pass. Tests may shorten this. */
  passTimeoutMs?: number;
  /** Bounds shutdown and deadline-abort draining for operations that ignore cancellation. */
  shutdownGraceMs?: number;
  logger?: AgentObservabilityMaintenanceLogger;
  operations?: AgentObservabilityMaintenanceOperations;
}): AgentObservabilityMaintenance {
  const intervalMs = input.intervalMs ?? AGENT_OBSERVABILITY_MAINTENANCE_INTERVAL_MS;
  if (!Number.isSafeInteger(intervalMs) || intervalMs <= 0) {
    throw new Error('agent observability maintenance interval must be positive');
  }
  const passTimeoutMs = input.passTimeoutMs ?? AGENT_OBSERVABILITY_MAINTENANCE_PASS_TIMEOUT_MS;
  if (!Number.isSafeInteger(passTimeoutMs) || passTimeoutMs <= 0) {
    throw new Error('agent observability maintenance pass timeout must be positive');
  }
  const shutdownGraceMs =
    input.shutdownGraceMs ?? AGENT_OBSERVABILITY_MAINTENANCE_SHUTDOWN_GRACE_MS;
  if (!Number.isSafeInteger(shutdownGraceMs) || shutdownGraceMs <= 0) {
    throw new Error('agent observability maintenance shutdown grace must be positive');
  }
  const operations = input.operations ?? defaultOperations;
  const logger = input.logger ?? { warn: (message: string) => console.warn(message) };
  let stopped = false;
  let activePass: ActivePass | null = null;
  let stopPromise: Promise<void> | null = null;
  // A deadline-bound stage starts the following pass after itself. Otherwise a
  // repeatedly slow stage at the front of this serial pass could starve every
  // later cleanup forever.
  let nextStageStart = 0;

  const warn = (message: string): void => {
    try {
      logger.warn(message);
    } catch {
      // A logger must not prevent shutdown from releasing the active-pass guard.
    }
  };

  const run = (): Promise<boolean> => {
    if (stopped || activePass !== null) return Promise.resolve(false);
    const token = Symbol('agent-observability-maintenance-pass');
    const controller = new AbortController();
    let passDeadlineExpired = false;
    const deadlineTimer = setTimeout(() => {
      passDeadlineExpired = true;
      controller.abort();
      warn('registry-service-ts timed out agent observability maintenance pass');
    }, passTimeoutMs);
    deadlineTimer.unref();
    const pass = Promise.resolve().then(async (): Promise<boolean> => {
      const shouldStop = (): boolean => stopped || controller.signal.aborted;
      const runStage = async (
        operation: () => Promise<unknown>,
        failureWarning: string,
      ): Promise<boolean> => {
        if (shouldStop()) return false;
        const outcome = await awaitOperationOrPassDeadline(
          operation,
          controller.signal,
          () => passDeadlineExpired,
          shutdownGraceMs,
        );
        if (outcome === 'failed') warn(failureWarning);
        return outcome !== 'aborted' && !shouldStop();
      };
      try {
        if (shouldStop()) return false;
        const secretStore = input.secretStore;
        const stages: AgentObservabilityMaintenanceStage[] = [
          {
            run: () => operations.prune(input.db, controller.signal),
            failureWarning: 'registry-service-ts failed agent observability retention maintenance',
          },
        ];
        if (secretStore !== undefined) {
          stages.push(
            {
              run: () => operations.reconcileStaging(input.db, secretStore, controller.signal),
              failureWarning: 'registry-service-ts failed agent observability staging cleanup',
            },
            {
              run: () =>
                operations.reconcileSupersededSecrets(input.db, secretStore, controller.signal),
              failureWarning: 'registry-service-ts failed agent observability secret cleanup',
            },
          );
        }
        const start = nextStageStart % stages.length;
        for (let offset = 0; offset < stages.length; offset += 1) {
          const stageIndex = (start + offset) % stages.length;
          const stage = stages[stageIndex]!;
          if (!(await runStage(stage.run, stage.failureWarning))) {
            if (passDeadlineExpired) nextStageStart = (stageIndex + 1) % stages.length;
            return true;
          }
        }
        nextStageStart = 0;
        return true;
      } finally {
        clearTimeout(deadlineTimer);
        if (activePass?.token === token) {
          activePass = null;
        }
      }
    });
    activePass = { token, controller, deadlineTimer, promise: pass };
    return pass;
  };

  detachHandledPromise(run());
  const timer = setInterval(() => detachHandledPromise(run()), intervalMs);
  timer.unref();
  return {
    run,
    stop(): Promise<void> {
      if (stopPromise !== null) return stopPromise;
      stopped = true;
      clearInterval(timer);
      const pass = activePass;
      if (pass !== null) {
        clearTimeout(pass.deadlineTimer);
        pass.controller.abort();
      }
      stopPromise = (async (): Promise<void> => {
        if (pass === null) return;
        const shutdownDeadline = Date.now() + shutdownGraceMs;
        const outcome = await waitForBounded(pass.promise, shutdownDeadline - Date.now());
        if (outcome === 'timed_out') {
          detachHandledPromise(pass.promise);
          warn('registry-service-ts agent observability maintenance shutdown grace exceeded');
        }
      })();
      return stopPromise;
    },
  };
}

interface ActivePass {
  token: symbol;
  controller: AbortController;
  deadlineTimer: ReturnType<typeof setTimeout>;
  promise: Promise<boolean>;
}

interface AgentObservabilityMaintenanceStage {
  run(): Promise<unknown>;
  failureWarning: string;
}

type MaintenanceOperationOutcome = 'completed' | 'failed' | 'aborted';
type BoundedWaitOutcome = 'fulfilled' | 'rejected' | 'timed_out';

/**
 * A pass deadline must release the pass even when an operation ignores its
 * signal. Shutdown abort is intentionally different: stop() owns its explicit
 * grace wait, so a cooperative operation can finish before dependencies close.
 */
function awaitOperationOrPassDeadline(
  operation: () => Promise<unknown>,
  signal: AbortSignal,
  passDeadlineExpired: () => boolean,
  abortDrainMs: number,
): Promise<MaintenanceOperationOutcome> {
  if (signal.aborted) return Promise.resolve('aborted');

  return new Promise((resolve) => {
    let settled = false;
    let abortDrainTimer: ReturnType<typeof setTimeout> | null = null;
    const finish = (outcome: MaintenanceOperationOutcome): void => {
      if (settled) return;
      settled = true;
      if (abortDrainTimer !== null) clearTimeout(abortDrainTimer);
      signal.removeEventListener('abort', onAbort);
      resolve(outcome);
    };
    const operationPromise = Promise.resolve().then(() => {
      if (signal.aborted) return;
      return operation();
    });
    const onAbort = (): void => {
      if (!passDeadlineExpired()) return;
      // Reconciliation uses an aborted signal to durably complete already
      // claimed rows. Give it bounded time to do that before detaching an
      // operation which ignores cancellation altogether.
      abortDrainTimer = setTimeout(() => {
        detachHandledPromise(operationPromise);
        finish('aborted');
      }, abortDrainMs);
      abortDrainTimer.unref?.();
    };

    signal.addEventListener('abort', onAbort, { once: true });
    void operationPromise.then(
      () => finish(signal.aborted ? 'aborted' : 'completed'),
      () => finish(signal.aborted ? 'aborted' : 'failed'),
    );
  });
}

/** Keep a late detached operation from becoming an unhandled rejection. */
function detachHandledPromise(promise: Promise<unknown>): void {
  void promise.catch(() => {});
}

/** Observe a pass without allowing shutdown to wait forever. */
function waitForBounded(
  operation: Promise<unknown>,
  timeoutMs: number,
): Promise<BoundedWaitOutcome> {
  return new Promise((resolve) => {
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const finish = (outcome: BoundedWaitOutcome): void => {
      if (settled) return;
      settled = true;
      if (timer !== null) clearTimeout(timer);
      resolve(outcome);
    };

    void operation.then(
      () => finish('fulfilled'),
      () => finish('rejected'),
    );
    if (timeoutMs <= 0) {
      finish('timed_out');
      return;
    }
    timer = setTimeout(() => finish('timed_out'), timeoutMs);
    timer.unref?.();
  });
}
