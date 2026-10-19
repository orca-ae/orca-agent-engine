// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  runCancellableOpenSandboxCommand,
  rejectUnsupportedCancellation,
} from '../../src/opensandbox-cancellation.js';

afterEach(() => vi.useRealTimers());

function fixture() {
  const controller = new AbortController();
  let emit!: (event: { type?: string; text?: string }) => void;
  let finish!: () => void;
  const request = vi.fn(async (path: string, _init: RequestInit): Promise<Response> => {
    if (path.startsWith('/command/status/'))
      return Response.json({
        id: 'exec/id',
        running: false,
        exit_code: -1,
        finished_at: new Date().toISOString(),
      });
    return new Response();
  });
  const consume = vi.fn(async (_response: Response, onEvent: typeof emit) => {
    emit = onEvent;
    return await new Promise<string>((resolve) => {
      finish = () => resolve('done');
    });
  });
  const run = () => runCancellableOpenSandboxCommand(request, '{}', consume, controller.signal);
  return {
    controller,
    request,
    run,
    emit: (event: Parameters<typeof emit>[0]) => emit(event),
    finish: () => finish(),
  };
}

describe('execd per-call cancellation', () => {
  it('rejects unsupported opt-in, but keeps no-signal calls compatible', () => {
    expect(() => rejectUnsupportedCancellation()).not.toThrow();
    expect(() => rejectUnsupportedCancellation({ signal: new AbortController().signal })).toThrow(
      'does not support',
    );
  });

  it('does not dispatch a pre-aborted call', async () => {
    const f = fixture();
    f.controller.abort();
    await expect(f.run()).rejects.toMatchObject({ name: 'AbortError' });
    expect(f.request).not.toHaveBeenCalled();
  });

  it('waits for pending headers and init, then cancels the encoded execution ID', async () => {
    const f = fixture();
    let headers!: () => void;
    f.request.mockImplementationOnce(
      async () =>
        await new Promise<Response>((resolve) => {
          headers = () => resolve(new Response());
        }),
    );
    const pending = f.run();
    const rejected = expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    f.controller.abort();
    expect(f.request).toHaveBeenCalledTimes(1);
    headers();
    await vi.waitFor(() => expect(() => f.emit({ type: 'init', text: 'exec/id' })).not.toThrow());
    await rejected;
    expect(f.request.mock.calls.map(([path]) => path)).toEqual([
      '/command',
      '/command?id=exec%2Fid',
      '/command/status/exec%2Fid',
    ]);
  });

  it('accepts a raced interrupt failure only when status confirms exit', async () => {
    const f = fixture();
    const pending = f.run();
    const rejected = expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    await vi.waitFor(() => expect(() => f.emit({ type: 'init', text: 'exec/id' })).not.toThrow());
    f.request.mockImplementationOnce(async () => new Response(null, { status: 500 }));
    f.controller.abort();
    f.emit({ type: 'execution_complete' });
    f.finish();
    await rejected;
  });

  it.each([404, 503])('does not claim stop on status HTTP %s', async (status) => {
    const f = fixture();
    const pending = f.run();
    const rejected = expect(pending).rejects.toThrow('termination unknown');
    await vi.waitFor(() => expect(() => f.emit({ type: 'init', text: 'exec/id' })).not.toThrow());
    f.request.mockImplementationOnce(async () => new Response());
    f.request.mockImplementationOnce(async () => new Response(null, { status }));
    f.controller.abort();
    await rejected;
  });

  it('kills and checks status on premature EOF rather than accepting stream completion', async () => {
    const f = fixture();
    const pending = f.run();
    const rejected = expect(pending).rejects.toThrow('stream ended before completion');
    await vi.waitFor(() => expect(() => f.emit({ type: 'init', text: 'exec/id' })).not.toThrow());
    f.finish();
    await rejected;
    expect(f.request.mock.calls.map(([path]) => path)).toContain('/command/status/exec%2Fid');
  });

  it('waits beyond the interrupt acknowledgement until status confirms exit', async () => {
    const f = fixture();
    const pending = f.run();
    const rejected = expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    await vi.waitFor(() => expect(() => f.emit({ type: 'init', text: 'exec/id' })).not.toThrow());
    f.request.mockImplementationOnce(async () => new Response());
    f.request.mockImplementationOnce(async () => Response.json({ id: 'exec/id', running: true }));
    f.controller.abort();
    await rejected;
    expect(f.request.mock.calls.filter(([path]) => path.includes('/status/'))).toHaveLength(2);
  });

  it('reports uncertain termination on interrupt transport failure', async () => {
    const f = fixture();
    const pending = f.run();
    const rejected = expect(pending).rejects.toThrow('termination unknown');
    await vi.waitFor(() => expect(() => f.emit({ type: 'init', text: 'exec/id' })).not.toThrow());
    f.request.mockRejectedValueOnce(new Error('connection reset'));
    f.controller.abort();
    await rejected;
  });

  it('rejects a status response belonging to a sibling execution', async () => {
    const f = fixture();
    const pending = f.run();
    const rejected = expect(pending).rejects.toThrow('termination unknown');
    await vi.waitFor(() => expect(() => f.emit({ type: 'init', text: 'exec/id' })).not.toThrow());
    f.request.mockImplementationOnce(async () => new Response());
    f.request.mockImplementationOnce(async () =>
      Response.json({
        id: 'sibling',
        running: false,
        exit_code: 0,
        finished_at: new Date().toISOString(),
      }),
    );
    f.controller.abort();
    await rejected;
  });

  it('bounds cancellation when init never arrives', async () => {
    vi.useFakeTimers();
    const f = fixture();
    const pending = f.run();
    const rejected = expect(pending).rejects.toThrow('termination unknown');
    f.controller.abort();
    await vi.advanceTimersByTimeAsync(10_000);
    await rejected;
    expect(f.request).toHaveBeenCalledTimes(1);
  });
});
