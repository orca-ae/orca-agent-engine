// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import { DialInTransport } from '../../src/harness/in-sandbox/dial-in.js';

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it.each(['codex-sdk', 'pi-sdk', 'dsh-sdk'])(
  'HTTP %s long submit survives the old 660s limit while control remains bounded',
  async (agent) => {
    vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
    vi.spyOn(AbortSignal, 'timeout').mockImplementation((ms) => {
      const controller = new AbortController();
      setTimeout(() => controller.abort(), ms);
      return controller.signal;
    });
    const pending: Array<{ signal: AbortSignal; finish(): void }> = [];
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string, init?: RequestInit) => {
        if (url.endsWith('/sdk-command'))
          return new Promise<Response>((resolve, reject) => {
            const signal = init!.signal!;
            pending.push({
              signal,
              finish: () => resolve(Response.json({ ok: true, sequence: 0 })),
            });
            signal.addEventListener('abort', () => reject(signal.reason), { once: true });
          });
        return Response.json({ id: 'ses_test' });
      }),
    );
    const channel = await new DialInTransport({ baseUrl: 'http://sandbox.test' }).open({ agent });
    const turn = channel.sdkCommand!({
      type: 'submit',
      text: 'long',
      executionBudgetMs: 1_200_000,
    });
    void turn.catch(() => {});
    await vi.advanceTimersByTimeAsync(660_001);
    expect(pending[0]!.signal.aborted).toBe(false);
    const interrupt = channel.sdkCommand!({ type: 'interrupt' });
    const interrupted = expect(interrupt).rejects.toThrow('SDK transport timed out');
    await vi.advanceTimersByTimeAsync(60_000);
    await interrupted;
    expect(pending[0]!.signal.aborted).toBe(false);
    pending[0]!.finish();
    await turn;
    expect(vi.getTimerCount()).toBe(0);
    await channel.stop();
  },
);
