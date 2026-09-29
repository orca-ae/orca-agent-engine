// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { HARNESS_CATALOG } from '@orca/harness-catalog';
import type { CodexTurnOptions, WorkerCommand, WorkerEvent } from '@orca/codex-harness';
import type { SessionStartInput } from '../agent-harness.js';
import { DialInTransport } from '../in-sandbox/dial-in.js';
import type { HarnessChannel, HarnessTransport } from '../in-sandbox/transport.js';

/** Move only the SDK worker into the shared sandbox-harness process lifecycle. */
export class RemoteCodexSdkWorker {
  private channel: HarnessChannel | undefined;
  private pump: Promise<void> | undefined;
  private submitting: Promise<void> | undefined;
  private stopped = false;
  private sequence = 0;
  private readonly abort = new AbortController();
  private readonly barriers = new Set<() => void>();

  constructor(
    private readonly emit: (event: WorkerEvent) => void,
    private readonly input: SessionStartInput,
    private readonly transport?: HarnessTransport,
    private readonly provider: 'codex-sdk' | 'pi-sdk' = 'codex-sdk',
  ) {}

  async handle(command: WorkerCommand): Promise<void> {
    if (this.stopped) throw new Error('SDK worker is closed');
    if (command.type === 'start') {
      if (this.channel) throw new Error('SDK worker already started');
      let transport = this.transport;
      if (!transport) {
        if (!this.input.sandbox?.endpoint)
          throw new Error('colocated SDK requires a sandbox endpoint');
        const endpoint = await this.input.sandbox.endpoint(HARNESS_CATALOG.codex_sdk.port!);
        transport = new DialInTransport({ baseUrl: endpoint.url, headers: endpoint.headers });
      }
      this.channel = await transport.open({ agent: this.provider });
      if (this.stopped) {
        await this.channel.stop();
        throw new Error('SDK worker closed during startup');
      }
      this.pump = this.readEvents().catch(async () => {
        if (!this.stopped)
          this.emit({ type: 'failure', message: 'sandbox SDK event stream failed', fatal: true });
        this.abort.abort();
        await this.channel?.stop();
      });
    }
    if (!this.channel?.sdkCommand) throw new Error('sandbox SDK command channel unavailable');
    const pending = this.command(command);
    if (command.type === 'submit') this.submitting = pending;
    await pending;
  }

  async refreshOptions(options: CodexTurnOptions): Promise<void> {
    // The done event precedes the child control acknowledgement. Join cleanup
    // before starting the next turn or refreshing its credentials.
    await this.submitting;
    if (!this.channel?.sdkCommand) throw new Error('sandbox SDK command channel unavailable');
    await this.command({ type: 'refresh', ...options });
  }

  private async command(command: unknown): Promise<void> {
    const sequence = await this.channel!.sdkCommand!(command, this.sequence);
    // HTTP and SSE are independent sockets. The child acknowledgement proves
    // cleanup; this barrier proves all native usage/history events reached the
    // adapter before its response-persistence fence can commit native history.
    if (this.abort.signal.aborted) throw new Error('SDK event delivery aborted');
    if (this.sequence >= sequence) return;
    await new Promise<void>((resolve, reject) => {
      const signal = AbortSignal.any([this.abort.signal, AbortSignal.timeout(660_000)]);
      const check = () => {
        if (signal.aborted || this.sequence >= sequence) {
          this.barriers.delete(check);
          signal.removeEventListener('abort', check);
          if (signal.aborted) reject(new Error('SDK event delivery aborted'));
          else resolve();
        }
      };
      this.barriers.add(check);
      signal.addEventListener('abort', check, { once: true });
      check();
    });
  }

  private async readEvents(): Promise<void> {
    for await (const event of this.channel!.events()) {
      if (event.type === 'harness.sdk_event') {
        // Independent of the transport's bounded replay cache: old raw SDK
        // events must never re-enter the adapter after a long-stream reconnect.
        if (typeof event.sequence !== 'number' || event.sequence <= this.sequence) continue;
        if (event.sequence !== this.sequence + 1) throw new Error('sandbox SDK event gap');
        this.sequence = event.sequence;
        this.emit(event.event as WorkerEvent);
        for (const check of this.barriers) check();
      } else if (event.type === 'session.status_error') {
        this.emit({
          type: 'failure',
          message: String(event.error ?? 'sandbox SDK exited'),
          fatal: true,
        });
      }
    }
  }

  async close(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    this.abort.abort();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        this.channel?.sdkCommand?.({ type: 'stop' }).catch(() => undefined),
        new Promise<void>((resolve) => {
          timer = setTimeout(resolve, 5_000);
        }),
      ]);
    } finally {
      clearTimeout(timer);
      await this.channel?.stop();
      await this.pump;
    }
  }
}
