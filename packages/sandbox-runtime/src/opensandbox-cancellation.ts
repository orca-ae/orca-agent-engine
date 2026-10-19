// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export interface SandboxCallOptions {
  signal?: AbortSignal;
}

export function rejectUnsupportedCancellation(options?: SandboxCallOptions): void {
  if (options?.signal) {
    throw new Error('Sandbox backend does not support per-call cancellation');
  }
}

type CommandEvent = { type?: string; text?: string };
type ExecdRequest = (path: string, init: RequestInit) => Promise<Response>;

/** A deadline or failed status check means unknown termination, never AbortError. */
export async function runCancellableOpenSandboxCommand<T>(
  request: ExecdRequest,
  body: string,
  consume: (response: Response, onEvent: (event: CommandEvent) => void) => Promise<T>,
  signal: AbortSignal,
): Promise<T> {
  signal.throwIfAborted();
  const transport = new AbortController();
  const deadline = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let resolveId!: (id: string) => void;
  const executionId = new Promise<string>((resolve) => {
    resolveId = resolve;
  });
  let terminal = false;
  let cleanup: Promise<void> | undefined;
  let rejectDeadline!: (error: Error) => void;
  const expired = new Promise<never>((_resolve, reject) => {
    rejectDeadline = reject;
  });
  const stop = () => {
    if (cleanup) return;
    // Keep reading init even if cancellation precedes response headers/start.
    timer = setTimeout(() => {
      const error = new Error('OpenSandbox cancellation timed out; execution termination unknown');
      deadline.abort(error);
      transport.abort(error);
      rejectDeadline(error);
    }, 10_000);
    cleanup = (async () => {
      const id = await Promise.race([executionId, expired]);
      const response = await request(`/command?id=${encodeURIComponent(id)}`, {
        method: 'DELETE',
        signal: deadline.signal,
      });
      // Interrupt can race normal completion and return 500: status is authoritative.
      while (true) {
        const statusResponse = await request(`/command/status/${encodeURIComponent(id)}`, {
          method: 'GET',
          signal: deadline.signal,
        });
        if (!statusResponse.ok) {
          throw new Error(
            `OpenSandbox cancellation status failed (${statusResponse.status}); termination unknown`,
          );
        }
        const status = (await statusResponse.json()) as Record<string, unknown>;
        if (status['id'] !== id || typeof status['running'] !== 'boolean') {
          throw new Error('OpenSandbox cancellation status invalid; termination unknown');
        }
        if (
          status['running'] === false &&
          Number.isInteger(status['exit_code']) &&
          typeof status['finished_at'] === 'string' &&
          Number.isFinite(Date.parse(status['finished_at']))
        )
          return;
        if (!response.ok) {
          throw new Error(`OpenSandbox interrupt failed (${response.status}); termination unknown`);
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    })();
    // Cleanup may reject while the command stream is still pending.
    void cleanup.catch(() => undefined);
  };
  let removeAbort: () => void = () => undefined;
  const cancelled = new Promise<never>((_resolve, reject) => {
    const onAbort = () => {
      stop();
      void Promise.race([cleanup!, expired]).then(
        () => {
          transport.abort();
          reject(new DOMException('OpenSandbox execution stopped', 'AbortError'));
        },
        (error: unknown) => {
          transport.abort();
          reject(
            new Error('OpenSandbox cancellation failed; execution termination unknown', {
              cause: error,
            }),
          );
        },
      );
    };
    signal.addEventListener('abort', onAbort, { once: true });
    removeAbort = () => signal.removeEventListener('abort', onAbort);
  });
  try {
    const execution = (async () => {
      const response = await request('/command', {
        method: 'POST',
        headers: { accept: 'text/event-stream', 'content-type': 'application/json' },
        body,
        signal: transport.signal,
      });
      if (!response.ok) throw new Error(`OpenSandbox command failed (${response.status})`);
      const result = await consume(response, (event) => {
        if (event.type === 'init' && event.text) resolveId(event.text);
        if (event.type === 'execution_complete' || event.type === 'error') terminal = true;
      });
      if (!terminal) throw new Error('OpenSandbox command stream ended before completion');
      return result;
    })();
    try {
      const result = await Promise.race([execution, cancelled]);
      if (signal.aborted) return await cancelled;
      return result;
    } catch (error) {
      if (signal.aborted) return await cancelled;
      stop();
      try {
        await Promise.race([cleanup!, expired]);
      } catch (cause) {
        throw new Error('OpenSandbox command failed; execution termination unknown', { cause });
      }
      throw error;
    }
  } finally {
    removeAbort();
    if (timer !== undefined) clearTimeout(timer);
    deadline.abort();
    transport.abort();
  }
}
