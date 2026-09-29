// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export interface ControlledShutdownOptions {
  cleanup(): Promise<void>;
  exit(code: number): void;
  onCleanupFailure?(): void;
}

export interface ControlledShutdown {
  shutdown(exitCode?: number): Promise<void>;
}

export interface TerminalFailureSource {
  whenFailed(): Promise<void>;
}

/**
 * Serialize all shutdown triggers. A source failure may arrive while SIGTERM
 * cleanup is already running; failure exit status wins without starting a
 * second teardown sequence.
 */
export function createControlledShutdown(options: ControlledShutdownOptions): ControlledShutdown {
  let requestedExitCode = 0;
  let shutdownPromise: Promise<void> | null = null;

  return {
    shutdown(exitCode = 0): Promise<void> {
      if (exitCode !== 0) requestedExitCode = exitCode;
      if (shutdownPromise) return shutdownPromise;
      shutdownPromise = (async () => {
        try {
          await options.cleanup();
        } catch {
          requestedExitCode = 1;
          options.onCleanupFailure?.();
        } finally {
          options.exit(requestedExitCode);
        }
      })();
      return shutdownPromise;
    },
  };
}

/** Bridge a terminal source failure into the same re-entrant shutdown path. */
export function monitorEventSourceFailure(
  source: TerminalFailureSource,
  shutdown: (exitCode: number) => Promise<void>,
  onFailure: () => void,
): void {
  void source.whenFailed().then(() => {
    onFailure();
    void shutdown(1);
  });
}
