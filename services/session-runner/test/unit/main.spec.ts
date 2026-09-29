// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the runner entry composition (`composeRunner`).
//
// main.ts only resolves the real collaborators (config, Kafka transcript store,
// claude provider) and drives the composition; the WIRING — the idle-watchdog ↔
// loop closures, the onActivity → touchActivity hook, the SIGINT/SIGTERM/idle
// shutdown fan-out with its `shuttingDown` guard, and the run()→finally teardown
// ordering (abort the shutdown controller → await the idle monitor → stop the loop)
// — lives in composeRunner. This spec drives composeRunner with injected fakes so
// that wiring is asserted without a tunnel, Kafka, or the SDK.

import { describe, expect, it, vi } from 'vitest';
import {
  composeRunner,
  type CreateRunnerOptions,
  type RunnerLogger,
  type SessionLoopLike,
  type SessionRunnerLike,
} from '../../src/main.js';
import type { RunnerConfig } from '../../src/config.js';
import type { ProviderRegistry } from '../../src/harness/provider.js';

function baseConfig(overrides: Partial<RunnerConfig> = {}): RunnerConfig {
  return {
    bindingToken: 'binding-token',
    registryRunnerUrl: 'ws://registry:8081/runner',
    workspace: '/var/run/orca/ws',
    workspaceId: 'ws_1',
    idleTimeoutS: 0,
    provider: {
      modelDefault: 'claude-sonnet-4-5',
    },
    ...overrides,
  };
}

/** A fake session loop recording the stop reasons + activity touches. */
class FakeLoop implements SessionLoopLike {
  activityTouches = 0;
  readonly stopReasons: Array<string | undefined> = [];
  activeWork = false;
  lastActivityAt = 0;

  providerNames(): string[] {
    return ['claude'];
  }
  resumeCursors(): Record<string, string> {
    return { ses_1: 'evt_9' };
  }
  lastActivity(): number {
    return this.lastActivityAt;
  }
  hasActiveWork(): boolean {
    return this.activeWork;
  }
  touchActivity(): void {
    this.activityTouches += 1;
  }
  async stop(reason?: string): Promise<void> {
    this.stopReasons.push(reason);
  }
}

/** A fake tunnel runner whose `run()` blocks until released, recording stop(). */
class FakeRunner implements SessionRunnerLike {
  readonly runnerId = 'runner-id';
  readonly registryRunnerUrl = 'ws://registry:8081/runner';
  readonly dispatcher = { register: () => undefined };
  stopCalls = 0;
  runStarted = false;
  capturedOnActivity: (() => void) | undefined;
  capturedResumeCursors: (() => Record<string, string>) | undefined;
  private release: (() => void) | undefined;
  private readonly running = new Promise<void>((resolve) => {
    this.release = resolve;
  });

  async run(): Promise<void> {
    this.runStarted = true;
    await this.running;
  }
  async stop(): Promise<void> {
    this.stopCalls += 1;
    this.release?.();
  }
}

function silentLogger(): RunnerLogger {
  return {
    log: () => undefined,
    error: () => undefined,
    info: () => undefined,
    warn: () => undefined,
  };
}

/**
 * Build composeRunner wired to fakes. Returns the fakes + a handle on the idle
 * monitor's options so a test can assert / drive the watchdog interplay.
 */
function harness(configOverrides: Partial<RunnerConfig> = {}) {
  const loop = new FakeLoop();
  const runner = new FakeRunner();
  let registeredProviders = false;
  let registerOpts: CreateRunnerOptions | undefined;
  let idleOpts: Parameters<typeof import('../../src/idle.js').runInactivityMonitor>[0] | undefined;
  let idleResolve: (() => void) | undefined;

  const proc = composeRunner({
    config: baseConfig(configOverrides),
    logger: silentLogger(),
    registerProviders: (_registry: ProviderRegistry) => {
      registeredProviders = true;
    },
    createLoop: () => loop,
    createRunner: (opts) => {
      registerOpts = opts;
      runner.capturedOnActivity = opts.onActivity;
      runner.capturedResumeCursors = opts.resumeCursors;
      return runner;
    },
    registerHandlers: () => undefined,
    runInactivityMonitor: (opts) => {
      idleOpts = opts;
      return new Promise<void>((resolve) => {
        idleResolve = resolve;
        // End the monitor when the shutdown controller aborts (mirrors the real one).
        opts.signal?.addEventListener('abort', () => resolve(), { once: true });
      });
    },
  });

  return {
    proc,
    loop,
    runner,
    get registeredProviders() {
      return registeredProviders;
    },
    get registerOpts() {
      return registerOpts;
    },
    get idleOpts() {
      return idleOpts;
    },
    releaseIdle: () => idleResolve?.(),
  };
}

describe('composeRunner wiring', () => {
  it('registers providers and advertises the loop provider names to the runner', () => {
    const h = harness();
    expect(h.registeredProviders).toBe(true);
    expect(h.registerOpts?.providers).toEqual(['claude']);
  });

  it('wires the runner resumeCursors hook to the loop', () => {
    const h = harness();
    expect(h.runner.capturedResumeCursors?.()).toEqual({ ses_1: 'evt_9' });
  });

  it('wires the runner onActivity hook to the loop activity touch', () => {
    const h = harness();
    expect(h.loop.activityTouches).toBe(0);
    h.runner.capturedOnActivity?.();
    h.runner.capturedOnActivity?.();
    expect(h.loop.activityTouches).toBe(2);
  });

  it('wires the idle watchdog inputs to the loop (lastActivity + hasActiveWork)', () => {
    const h = harness({ idleTimeoutS: 30 });
    h.loop.lastActivityAt = 1234;
    h.loop.activeWork = true;
    // The composition starts the watchdog only inside run(); kick it off.
    void h.proc.run();
    expect(h.idleOpts?.idleTimeoutS).toBe(30);
    expect(h.idleOpts?.getLastActivity()).toBe(1234);
    expect(h.idleOpts?.hasActiveWork()).toBe(true);
  });
});

describe('composeRunner shutdown fan-out', () => {
  it('stops the runner + loop once per shutdown, idempotent across repeats', async () => {
    const h = harness();
    const runPromise = h.proc.run();
    h.proc.requestShutdown('SIGINT');
    // A second + third request (SIGTERM after SIGINT, idle racing a signal) are no-ops.
    h.proc.requestShutdown('SIGTERM');
    h.proc.requestShutdown('idle.timeout');
    await runPromise;
    expect(h.runner.stopCalls).toBe(1);
    // loop.stop is called once in the shutdown fan-out (replica.shutting_down) and
    // once more in the run() finally teardown — both with no harness-mismatch.
    expect(
      h.loop.stopReasons.filter((r) => r === 'replica.shutting_down').length,
    ).toBeGreaterThanOrEqual(1);
  });

  it('maps an idle-timeout shutdown to the loop stop reason idle.timeout', async () => {
    const h = harness();
    const runPromise = h.proc.run();
    h.proc.requestShutdown('idle.timeout');
    await runPromise;
    expect(h.loop.stopReasons).toContain('idle.timeout');
  });

  it('an idle.timeout from the watchdog drives the same shutdown fan-out', async () => {
    const h = harness({ idleTimeoutS: 10 });
    const runPromise = h.proc.run();
    // Simulate the watchdog firing: it calls requestShutdown('idle.timeout').
    h.idleOpts?.requestShutdown();
    await runPromise;
    expect(h.runner.stopCalls).toBe(1);
    expect(h.loop.stopReasons).toContain('idle.timeout');
  });
});

describe('composeRunner teardown ordering', () => {
  it('aborts the shutdown signal, awaits the idle monitor, then stops the loop (in run finally)', async () => {
    // Build a composition whose idle monitor records WHEN it resolves, so we can
    // assert the run()-finally teardown awaits it before the terminal loop.stop().
    const loop = new FakeLoop();
    const runner = new FakeRunner();
    const order: string[] = [];
    let idleResolvedMarkerPushed = false;

    const proc = composeRunner({
      config: baseConfig({ idleTimeoutS: 10 }),
      logger: silentLogger(),
      registerProviders: () => undefined,
      createLoop: () => loop,
      createRunner: (opts) => {
        runner.capturedOnActivity = opts.onActivity;
        return runner;
      },
      registerHandlers: () => undefined,
      runInactivityMonitor: (opts) =>
        new Promise<void>((resolve) => {
          opts.signal?.addEventListener(
            'abort',
            () => {
              // The monitor ends on the shutdown controller abort; record it.
              order.push('idle-monitor-resolved');
              idleResolvedMarkerPushed = true;
              resolve();
            },
            { once: true },
          );
        }),
    });

    const realStop = loop.stop.bind(loop);
    loop.stop = async (reason?: string) => {
      order.push(`loop.stop:${reason ?? 'default'}`);
      await realStop(reason);
    };

    const runPromise = proc.run();
    proc.requestShutdown('SIGTERM');
    await runPromise;

    expect(idleResolvedMarkerPushed).toBe(true);
    // Teardown invariants (the order the run()-finally guarantees):
    //   - the idle monitor resolved (it is awaited in the finally before the loop stop);
    //   - the fan-out issued the replica.shutting_down stop;
    //   - the run()-finally's terminal loop.stop() (default reason) is the LAST stop,
    //     proving it ran after the monitor was awaited.
    const idleIdx = order.indexOf('idle-monitor-resolved');
    const fanoutStopIdx = order.indexOf('loop.stop:replica.shutting_down');
    const terminalStopIdx = order.lastIndexOf('loop.stop:default');
    expect(idleIdx).toBeGreaterThanOrEqual(0);
    expect(fanoutStopIdx).toBeGreaterThanOrEqual(0);
    expect(terminalStopIdx).toBe(order.length - 1); // terminal stop is dead last
    expect(terminalStopIdx).toBeGreaterThan(idleIdx); // after the idle monitor resolved
  });

  it('run() resolves after the tunnel run() completes (clean stop)', async () => {
    const h = harness();
    const runPromise = h.proc.run();
    expect(h.runner.runStarted).toBe(true);
    h.proc.requestShutdown('SIGINT');
    await expect(runPromise).resolves.toBeUndefined();
  });

  it('does not install process signal handlers unless asked', async () => {
    const onceSpy = vi.spyOn(process, 'once');
    const h = harness();
    const runPromise = h.proc.run(); // installSignalHandlers omitted
    h.proc.requestShutdown('SIGINT');
    await runPromise;
    const installedSignal = onceSpy.mock.calls.some(
      ([evt]) => evt === 'SIGINT' || evt === 'SIGTERM',
    );
    expect(installedSignal).toBe(false);
    onceSpy.mockRestore();
  });
});

describe('composeRunner default sandbox-runtime selection', () => {
  // These exercise the REAL default sandbox-runtime path (no injected `sandboxRuntime`
  // and no injected `createLoop`), so the runner actually resolves the runtime from
  // `SANDBOX_RUNTIME`. Only the tunnel runner is faked (to avoid a real dial). The
  // real loop/providers stand up nothing external at construction, and the InMemory /
  // srt-fallback Local runtime touches nothing until `acquire`, so this is host-safe.
  function composeWithRealSandbox() {
    const runner = new FakeRunner();
    return composeRunner({
      config: baseConfig(),
      logger: silentLogger(),
      // Default providers + default loop + default sandbox runtime (the path under test).
      createRunner: (opts) => {
        runner.capturedOnActivity = opts.onActivity;
        return runner;
      },
      registerHandlers: () => undefined,
      runInactivityMonitor: (opts) =>
        new Promise<void>((resolve) => {
          opts.signal?.addEventListener('abort', () => resolve(), { once: true });
        }),
    });
  }

  it('composes with the default in-memory runtime when SANDBOX_RUNTIME is unset', () => {
    const prev = process.env['SANDBOX_RUNTIME'];
    delete process.env['SANDBOX_RUNTIME'];
    try {
      // The whole composition wires without throwing and the loop advertises its providers.
      const proc = composeWithRealSandbox();
      expect(proc.loop.providerNames()).toContain('claude');
    } finally {
      if (prev !== undefined) process.env['SANDBOX_RUNTIME'] = prev;
      else delete process.env['SANDBOX_RUNTIME'];
    }
  });

  it('composes without boot-failing when SANDBOX_RUNTIME=local (srt-gated Local, else InMemory fallback)', () => {
    const prev = process.env['SANDBOX_RUNTIME'];
    process.env['SANDBOX_RUNTIME'] = 'local';
    try {
      // The `local` selection resolves real srt wiring where `srt` exists and otherwise
      // degrades to InMemory — either way the runner composes rather than crashing. We do
      // NOT assert the concrete runtime class here (it depends on whether the host has
      // `srt`); the seam's own spec pins both branches deterministically via an injected probe.
      const proc = composeWithRealSandbox();
      expect(proc.loop.providerNames()).toContain('claude');
    } finally {
      if (prev !== undefined) process.env['SANDBOX_RUNTIME'] = prev;
      else delete process.env['SANDBOX_RUNTIME'];
    }
  });
});
