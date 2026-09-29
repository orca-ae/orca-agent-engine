// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { CustomToolResult } from './custom-tools.js';

/** One session/generation's bounded routing table. Unknown results are never buffered. */
export class PendingCustomToolResults {
  private readonly entries = new Map<
    string,
    { resolve(result: CustomToolResult): void; cancel(reason: string): void }
  >();
  constructor(private readonly timeoutMs = 120_000) {}

  park(id: string, signal: AbortSignal): Promise<CustomToolResult> {
    if (this.entries.size >= 256) throw new Error('too many pending custom tool callbacks');
    if (this.entries.has(id)) throw new Error('custom tool callback is already pending');
    return new Promise((resolve, reject) => {
      if (signal.aborted) {
        reject(new Error('custom tool callback aborted'));
        return;
      }
      const cleanup = () => {
        clearTimeout(timer);
        signal.removeEventListener('abort', abort);
        this.entries.delete(id);
      };
      const cancel = (reason: string) => {
        cleanup();
        reject(new Error(reason));
      };
      const abort = () => cancel('custom tool callback aborted');
      this.entries.set(id, {
        resolve: (result) => {
          cleanup();
          resolve(result);
        },
        cancel,
      });
      signal.addEventListener('abort', abort, { once: true });
      const timer = setTimeout(() => cancel('custom tool callback timed out'), this.timeoutMs);
      timer.unref?.();
    });
  }

  resolve(result: CustomToolResult): boolean {
    const entry = this.entries.get(result.custom_tool_use_id);
    if (!entry) return false;
    entry.resolve(result);
    return true;
  }

  hasPending(): boolean {
    return this.entries.size > 0;
  }

  reset(): void {
    for (const entry of [...this.entries.values()]) entry.cancel('custom tool callback abandoned');
  }
}
