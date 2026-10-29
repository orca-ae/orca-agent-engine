// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

const MAX_TIMER_MS = 2_147_483_647;

/** Remaining host execution budget, never a new business deadline. */
export function sdkExecutionBudgetMs(value: unknown): number {
  if (value === undefined) return 600_000;
  if (!Number.isSafeInteger(value) || (value as number) < 1 || (value as number) > MAX_TIMER_MS)
    throw new Error('Invalid SDK execution budget');
  return value as number;
}

export function sdkCommandTimeoutMs(command: unknown): number {
  const input = command as { type?: string; executionBudgetMs?: unknown } | null;
  return input?.type === 'submit' ? sdkExecutionBudgetMs(input.executionBudgetMs) + 60_000 : 60_000;
}

/** Transport cleanup headroom can exceed Node's single-timer limit. */
export function sdkTimeoutSignal(timeoutMs: number): { signal: AbortSignal; dispose(): void } {
  const controller = new AbortController();
  const expires = performance.now() + timeoutMs;
  let timer: ReturnType<typeof setTimeout>;
  const schedule = () => {
    const remaining = expires - performance.now();
    if (remaining <= 0)
      controller.abort(new DOMException('SDK transport timed out', 'TimeoutError'));
    else {
      timer = setTimeout(schedule, Math.min(Math.ceil(remaining), MAX_TIMER_MS));
      timer.unref?.();
    }
  };
  schedule();
  return { signal: controller.signal, dispose: () => clearTimeout(timer) };
}
