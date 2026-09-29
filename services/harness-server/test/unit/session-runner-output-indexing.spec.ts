// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import type { Event, ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';
import type {
  AgentEvent,
  AgentEventInput,
  AgentHarness,
  SessionStartInput,
  UserEvent,
} from '../../src/harness/agent-harness.js';
import { withCanonicalAgentEventEnvelope } from '../../src/harness/agent-harness.js';
import type { RegistryClient } from '../../src/clients/registry.js';
import type { MountHandle, MountStrategy } from '../../src/sandbox/mounts/mount-strategy.js';
import type { SandboxHandle } from '../../src/sandbox/sandbox-runtime.js';
import { AgentEventKind } from '../../src/harness/event-kinds.js';
import { OutputIndexer } from '../../src/sandbox/outputs/output-indexer.js';
import { SessionRunner, SessionRunnerStoppingError } from '../../src/runner/session-runner.js';
import { harnessOutputIndexLagSeconds } from '../../src/metrics.js';

describe('SessionRunner immediate output indexing', () => {
  it('rejects submit after stop and when stop races an in-flight submit', async () => {
    const submitted = deferred<void>();
    let submitStarted = false;
    const harness: AgentHarness = {
      async start() {},
      async submit(_event: UserEvent): Promise<void> {
        submitStarted = true;
        await submitted.promise;
      },
      async stop() {},
      async *events(): AsyncIterable<AgentEvent> {},
    };
    const runner = new SessionRunner({
      workspaceId: 'ws_submit_stop',
      sessionId: 'ses_submit_stop',
      harness,
      store: new RecordingStore(),
    });

    await runner.start({ agentSnapshot: {} });
    const inFlight = runner.submit({ kind: 'user.message', payload: {} });
    await vi.waitFor(() => expect(submitStarted).toBe(true));

    await runner.stop('replica.shutting_down');
    submitted.resolve();

    await expect(inFlight).rejects.toBeInstanceOf(SessionRunnerStoppingError);
    await expect(runner.submit({ kind: 'user.message', payload: {} })).rejects.toBeInstanceOf(
      SessionRunnerStoppingError,
    );
  });

  it('forwards the restricted agent sandbox while retaining lifecycle ownership', async () => {
    const lifecycleOrder: string[] = [];
    const agentOrder: string[] = [];
    const harness = new FakeHarness([]);
    const lifecycleSandbox = fakeSandbox(lifecycleOrder);
    const agentSandbox = fakeSandbox(agentOrder);
    const runner = new SessionRunner({
      workspaceId: 'ws_output_runner',
      sessionId: 'ses_output_runner',
      harness,
      store: new RecordingStore(),
      sandbox: lifecycleSandbox,
      agentSandbox,
    });

    await runner.start({ agentSnapshot: {} });
    expect(harness.startInput?.sandbox).toBe(agentSandbox);

    await runner.stop('idle.timeout');
    expect(lifecycleOrder).toContain('sandbox:destroy');
    expect(agentOrder).not.toContain('sandbox:destroy');
  });

  it('persists the tool result before scanning and does not block later transcript events', async () => {
    const scanGate = deferred<void>();
    const order: string[] = [];
    let scanCalls = 0;
    const harness = new FakeHarness([
      { kind: 'agent.tool_result', payload: { tool_use_id: 'tool_1' } },
      { kind: 'agent.message', payload: { content: 'done' } },
      { kind: 'session.status_idle', payload: { stop_reason: { type: 'end_turn' } } },
    ]);
    const store = new RecordingStore((kind) => order.push(`append:${kind}`));
    const runner = new SessionRunner({
      workspaceId: 'ws_output_runner',
      sessionId: 'ses_output_runner',
      harness,
      store,
      indexOutputs: async () => {
        scanCalls += 1;
        order.push(`scan:${scanCalls}:start`);
        if (scanCalls === 1) await scanGate.promise;
        order.push(`scan:${scanCalls}:end`);
        return { count: 0, skipped: 0, errors: [] };
      },
    });

    await runner.start({ agentSnapshot: {} });
    await vi.waitFor(() => {
      expect(store.appended.map((event) => event.kind)).toEqual([
        'agent.tool_result',
        'agent.message',
        'session.status_idle',
      ]);
    });

    expect(order.indexOf('append:agent.tool_result')).toBeLessThan(order.indexOf('scan:1:start'));
    expect(order).not.toContain('scan:1:end');
    scanGate.resolve();
    await runner.stop('idle.timeout');
    expect(scanCalls).toBe(2); // immediate pass + shutdown fallback
  });

  it('also scans immediately after a separated-mode MCP tool result', async () => {
    let scanCalls = 0;
    const harness = new FakeHarness([
      { kind: AgentEventKind.mcpToolResult, payload: { tool_use_id: 'tool_1' } },
    ]);
    const store = new RecordingStore();
    const runner = new SessionRunner({
      workspaceId: 'ws_output_runner',
      sessionId: 'ses_output_runner',
      harness,
      store,
      indexOutputs: async () => {
        scanCalls += 1;
        return { count: 0, skipped: 0, errors: [] };
      },
    });

    await runner.start({ agentSnapshot: {} });
    await vi.waitFor(() => expect(scanCalls).toBe(1));
    expect(store.appended.map((event) => event.kind)).toEqual([AgentEventKind.mcpToolResult]);

    await runner.stop('idle.timeout');
    expect(scanCalls).toBe(2); // immediate pass + shutdown fallback
  });

  it('records live tool-result scan latency separately from shutdown latency', async () => {
    const observe = vi.spyOn(harnessOutputIndexLagSeconds, 'observe');
    const runner = new SessionRunner({
      workspaceId: 'ws_output_runner',
      sessionId: 'ses_output_runner',
      harness: new FakeHarness([
        { kind: 'agent.tool_result', payload: { tool_use_id: 'tool_metric' } },
      ]),
      store: new RecordingStore(),
      indexOutputs: async () => ({ count: 0, skipped: 0, errors: [] }),
    });

    await runner.start({ agentSnapshot: {} });
    await vi.waitFor(() => {
      expect(observe).toHaveBeenCalledWith({ trigger: 'tool_result' }, expect.any(Number));
    });
    await runner.stop('idle.timeout');

    expect(observe).toHaveBeenCalledWith({ trigger: 'shutdown' }, expect.any(Number));
  });

  it('serializes scans, recovers after failure, and finishes the final scan before teardown', async () => {
    const firstScanGate = deferred<void>();
    const secondScanGate = deferred<void>();
    const order: string[] = [];
    let scanCalls = 0;
    let activeScans = 0;
    let maxActiveScans = 0;
    const harness = new FakeHarness(
      [
        { kind: 'agent.tool_result', payload: { tool_use_id: 'tool_1' } },
        { kind: 'agent.tool_result', payload: { tool_use_id: 'tool_2' } },
      ],
      true,
      order,
    );
    const store = new RecordingStore();
    const sandbox = fakeSandbox(order);
    const mount: MountHandle = {
      id: 'mount_output_runner',
      resourceId: 'res_output_runner',
      resourceType: 'file',
      mountPath: '/mnt/output-runner',
    };
    const strategy = fakeMountStrategy(order);
    const runner = new SessionRunner({
      workspaceId: 'ws_output_runner',
      sessionId: 'ses_output_runner',
      harness,
      store,
      sandbox,
      mounts: [{ handle: mount, strategy }],
      indexOutputs: async () => {
        scanCalls += 1;
        const call = scanCalls;
        activeScans += 1;
        maxActiveScans = Math.max(maxActiveScans, activeScans);
        order.push(`scan:${call}:start`);
        try {
          if (call === 1) {
            await firstScanGate.promise;
            throw new Error('transient registry failure');
          }
          if (call === 2) await secondScanGate.promise;
          return { count: 0, skipped: 0, errors: [] };
        } finally {
          activeScans -= 1;
          order.push(`scan:${call}:end`);
        }
      },
    });

    await runner.start({ agentSnapshot: {} });
    await vi.waitFor(() => expect(scanCalls).toBe(1));
    const stopping = runner.stop('replica.shutting_down');
    await vi.waitFor(() => expect(order).toContain('harness:stop'));
    expect(order).not.toContain('mount:deactivate');

    firstScanGate.resolve();
    await vi.waitFor(() => expect(scanCalls).toBe(2));
    expect(maxActiveScans).toBe(1);
    expect(order).not.toContain('mount:deactivate');

    secondScanGate.resolve();
    await stopping;
    expect(scanCalls).toBe(3); // two tool triggers + one final pass
    expect(maxActiveScans).toBe(1);
    expect(order.indexOf('scan:3:end')).toBeLessThan(order.indexOf('mount:deactivate'));
    expect(order.indexOf('mount:deactivate')).toBeLessThan(order.indexOf('sandbox:destroy'));
  });

  it('does not strand final capture when harness.stop rejects without closing events', async () => {
    const order: string[] = [];
    const harness = new FakeHarness([], true, order, new Error('stop transport failed'));
    const runner = new SessionRunner({
      workspaceId: 'ws_output_runner',
      sessionId: 'ses_output_runner',
      harness,
      store: new RecordingStore(),
      sandbox: fakeSandbox(order),
      shutdownStepGraceMs: 5,
      indexOutputs: async () => {
        order.push('scan:final');
        return { count: 0, skipped: 0, errors: [] };
      },
    });
    vi.spyOn(console, 'error').mockImplementation(() => {});

    await runner.start({ agentSnapshot: {} });
    await runner.stop('error');

    expect(order).toContain('harness:stop');
    expect(order.indexOf('harness:stop')).toBeLessThan(order.indexOf('scan:final'));
    expect(order.indexOf('scan:final')).toBeLessThan(order.indexOf('sandbox:destroy'));
  });

  it('continues final capture when harness.stop never settles', async () => {
    const order: string[] = [];
    const runner = new SessionRunner({
      workspaceId: 'ws_output_runner',
      sessionId: 'ses_output_runner',
      harness: new FakeHarness([], true, order, 'never'),
      store: new RecordingStore(),
      sandbox: fakeSandbox(order),
      shutdownStepGraceMs: 5,
      indexOutputs: async () => {
        order.push('scan:final');
        return { count: 0, skipped: 0, errors: [] };
      },
    });
    vi.spyOn(console, 'error').mockImplementation(() => {});

    await runner.start({ agentSnapshot: {} });
    await runner.stop('error');

    expect(order).toContain('scan:final');
    expect(order.indexOf('scan:final')).toBeLessThan(order.indexOf('sandbox:destroy'));
  });

  it('bounds a stuck shutdown scan and still destroys the sandbox', async () => {
    const order: string[] = [];
    const runner = new SessionRunner({
      workspaceId: 'ws_output_runner',
      sessionId: 'ses_output_runner',
      harness: new FakeHarness([]),
      store: new RecordingStore(),
      sandbox: fakeSandbox(order),
      shutdownStepGraceMs: 5,
      outputIndexShutdownGraceMs: 5,
      indexOutputs: async (signal?: AbortSignal) => {
        order.push('scan:stuck');
        await new Promise<void>((_resolve, reject) => {
          signal?.addEventListener(
            'abort',
            () => {
              order.push('scan:aborted');
              reject(signal.reason);
            },
            { once: true },
          );
        });
        return { count: 0, skipped: 0, errors: [] };
      },
    });
    vi.spyOn(console, 'error').mockImplementation(() => {});

    await runner.start({ agentSnapshot: {} });
    await runner.stop('error');

    expect(order).toContain('scan:stuck');
    expect(order).toContain('scan:aborted');
    expect(order.indexOf('scan:aborted')).toBeLessThan(order.indexOf('sandbox:destroy'));
    expect(order).toContain('sandbox:destroy');
  });

  it('does not start a queued final scan after an incremental scan times out', async () => {
    const scanGate = deferred<void>();
    let scans = 0;
    const runner = new SessionRunner({
      workspaceId: 'ws_output_runner',
      sessionId: 'ses_output_runner',
      harness: new FakeHarness([
        { kind: 'agent.tool_result', payload: { tool_use_id: 'tool_stuck' } },
      ]),
      store: new RecordingStore(),
      shutdownStepGraceMs: 5,
      outputIndexShutdownGraceMs: 5,
      indexOutputs: async () => {
        scans += 1;
        await scanGate.promise;
        return { count: 0, skipped: 0, errors: [] };
      },
    });
    vi.spyOn(console, 'error').mockImplementation(() => {});

    await runner.start({ agentSnapshot: {} });
    await vi.waitFor(() => expect(scans).toBe(1));
    await runner.stop('error');
    scanGate.resolve();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(scans).toBe(1);
  });

  it('does not couple the output-index deadline to the per-step shutdown grace', async () => {
    let scanCompleted = false;
    const runner = new SessionRunner({
      workspaceId: 'ws_output_runner',
      sessionId: 'ses_output_runner',
      harness: new FakeHarness([]),
      store: new RecordingStore(),
      shutdownStepGraceMs: 5,
      indexOutputs: async () => {
        await new Promise((resolve) => setTimeout(resolve, 15));
        scanCompleted = true;
        return { count: 0, skipped: 0, errors: [] };
      },
    });

    await runner.start({ agentSnapshot: {} });
    await runner.stop('idle.timeout');

    expect(scanCompleted).toBe(true);
  });

  it('shares one deadline across the final scan and its retry', async () => {
    let scans = 0;
    const startedAt = Date.now();
    const runner = new SessionRunner({
      workspaceId: 'ws_output_runner',
      sessionId: 'ses_output_runner',
      harness: new FakeHarness([]),
      store: new RecordingStore(),
      shutdownStepGraceMs: 100,
      outputIndexShutdownGraceMs: 200,
      indexOutputs: async (signal?: AbortSignal) => {
        scans += 1;
        if (scans === 1) {
          await new Promise((resolve) => setTimeout(resolve, 70));
          return { count: 0, skipped: 0, errors: [{ key: 'poem.txt', error: 'retry' }] };
        }
        await new Promise<void>((_resolve, reject) => {
          signal?.addEventListener('abort', () => reject(signal.reason), { once: true });
        });
        return { count: 0, skipped: 0, errors: [] };
      },
    });
    vi.spyOn(console, 'error').mockImplementation(() => {});

    await runner.start({ agentSnapshot: {} });
    await runner.stop('error');

    expect(scans).toBe(2);
    expect(Date.now() - startedAt).toBeLessThan(250);
  });

  it('retries a final readiness event without creating another File', async () => {
    const store = new FailFirstOutputEventStore();
    const sandbox = fakeSandbox([], {
      path: '/mnt/session/outputs/poem.txt',
      contents: Buffer.from('final poem'),
    });
    const createdFiles: string[] = [];
    const registry = {
      async createFile(input: { filename: string; content: Buffer }) {
        createdFiles.push(input.filename);
        return {
          id: 'file_final_poem',
          filename: input.filename,
          mime_type: 'application/octet-stream',
          size_bytes: input.content.length,
          sha256: 'a'.repeat(64),
          metadata: {},
          purpose: 'agent_output' as const,
          scope_id: 'ses_output_runner',
          downloadable: true,
          archived_at: null,
          created_at: '2026-07-13T00:00:00.000Z',
          updated_at: '2026-07-13T00:00:00.000Z',
        };
      },
    } as unknown as RegistryClient;
    const indexer = new OutputIndexer();
    const runner = new SessionRunner({
      workspaceId: 'ws_output_runner',
      sessionId: 'ses_output_runner',
      harness: new FakeHarness([]),
      store,
      sandbox,
      shutdownStepGraceMs: 20,
      indexOutputs: async () =>
        await indexer.indexSession({
          workspaceId: 'ws_output_runner',
          sessionId: 'ses_output_runner',
          mount: { kind: 'inmemory_local', sandboxPath: '/mnt/session/outputs' },
          sandbox,
          registry,
          store,
        }),
    });
    vi.spyOn(console, 'error').mockImplementation(() => {});

    await runner.start({ agentSnapshot: {} });
    await runner.stop('error');

    expect(createdFiles).toEqual(['poem.txt']);
    expect(store.appended.filter((event) => event.kind === 'session.output_indexed')).toHaveLength(
      1,
    );
  });
});

class FakeHarness implements AgentHarness {
  private readonly closeGate = deferred<void>();
  private readonly emitted: AgentEvent[];
  startInput: SessionStartInput | undefined;

  constructor(
    emitted: AgentEventInput[],
    private readonly holdOpen = false,
    private readonly order: string[] = [],
    private readonly stopOutcome?: Error | 'never',
  ) {
    this.emitted = emitted.map(withCanonicalAgentEventEnvelope);
  }

  async start(input: SessionStartInput): Promise<void> {
    this.startInput = input;
  }

  async submit(): Promise<void> {}

  async stop(): Promise<void> {
    this.order.push('harness:stop');
    if (this.stopOutcome === 'never') await new Promise<void>(() => {});
    if (this.stopOutcome instanceof Error) throw this.stopOutcome;
    this.closeGate.resolve();
  }

  async *events(): AsyncIterable<AgentEvent> {
    for (const event of this.emitted) yield event;
    if (this.holdOpen) await this.closeGate.promise;
  }
}

class RecordingStore implements TranscriptStore {
  readonly appended: Event[] = [];

  constructor(private readonly onAppend?: (kind: string) => void) {}

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    for (const event of events) {
      this.onAppend?.(event.kind);
      this.appended.push({ ...event, workspaceId, sessionId, seq: this.appended.length + 1 });
    }
    return events.map((event) => event.id);
  }

  async *read(_workspaceId: string, _sessionId: string, _opts: ReadOptions): AsyncIterable<Event> {}

  async *tail(_workspaceId: string, _sessionId: string, _opts: TailOptions): AsyncIterable<Event> {}

  async archive(): Promise<void> {}

  async close(): Promise<void> {}
}

class FailFirstOutputEventStore extends RecordingStore {
  private failed = false;

  override async append(
    workspaceId: string,
    sessionId: string,
    events: Event[],
  ): Promise<string[]> {
    if (!this.failed && events.some((event) => event.kind === 'session.output_indexed')) {
      this.failed = true;
      throw new Error('transcript unavailable');
    }
    return await super.append(workspaceId, sessionId, events);
  }
}

function fakeSandbox(order: string[], output?: { path: string; contents: Buffer }): SandboxHandle {
  return {
    id: 'sbx_output_runner',
    files: {
      async write() {},
      async read(path: string) {
        if (output?.path === path) return output.contents;
        throw new Error(`not found: ${path}`);
      },
      async readUtf8Page() {
        throw new Error('files.readUtf8Page not used');
      },
      async list(path: string) {
        if (output && path === '/mnt/session/outputs') {
          return [output.path.slice('/mnt/session/outputs/'.length)];
        }
        return [];
      },
      async chmod() {},
      async delete() {},
    },
    async run() {
      return { exit_code: 0 };
    },
    async runPrivileged() {
      return { exit_code: 0 };
    },
    async pause() {},
    async resume() {},
    async destroy() {
      order.push('sandbox:destroy');
    },
  };
}

function fakeMountStrategy(order: string[]): MountStrategy {
  return {
    name: 'tarball_prefetch',
    supports: ['file'],
    async activate() {
      throw new Error('not used');
    },
    async deactivate() {
      order.push('mount:deactivate');
    },
    async teardownForSnapshot() {
      throw new Error('not used');
    },
    async restoreAfterSnapshot() {
      throw new Error('not used');
    },
  };
}

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
  reject: (reason?: unknown) => void;
} {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}
