// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Runner-bookkeeping cases, driven through the worker's injectable
// process-spawner seam so process lifecycle is deterministic — a fake spawner
// plays the runner-process role without real processes.
//
// Covers: aliveRunnerIds() prunes dead handles; cleanup terminates every live
// runner on shutdown; and an exit report that raced a disconnect parks in the
// unreported queue and flushes right after the next hello (so a death while the
// tunnel was down is still reported instead of the client polling to a timeout).

import { describe, expect, it } from 'vitest';
import {
  FrameKind,
  WorkerFrameKind,
  decodeFrame,
  decodeWorkerFrame,
  encodeFrame,
  encodeWorkerFrame,
  tokenBoundRunnerId,
  type WorkerHelloFrame,
  type WorkerRunnerExitedFrame,
  type WorkerStopRunnerResultFrame,
} from '@orca/harness-tunnel';
import { EnvironmentWorker } from '../../src/worker.js';
import type { RunnerProcess, ProcessSpawner, SpawnRequest } from '../../src/process-spawner.js';
import type { WorkerSocket, WorkerSocketMessage } from '../../src/ws-client.js';

/** A fake runner process whose exit is driven by the test. */
class FakeRunnerProcess implements RunnerProcess {
  readonly pid = Math.floor(Math.random() * 1_000_000) + 2;
  exitCode: number | null = null;
  terminated = false;
  killed = false;
  private readonly exitWaiters: Array<() => void> = [];

  poll(): number | null {
    return this.exitCode;
  }

  onExit(listener: () => void): void {
    if (this.exitCode !== null) {
      listener();
      return;
    }
    this.exitWaiters.push(listener);
  }

  terminate(): boolean {
    this.terminated = true;
    // A real SIGTERM ends the child; reflect that so the worker's terminate→wait
    // path completes without hitting the kill grace window.
    this.markExited(143);
    return true;
  }

  kill(): boolean {
    this.killed = true;
    this.markExited(137);
    return true;
  }

  wait(): Promise<void> {
    if (this.exitCode !== null) {
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => this.exitWaiters.push(resolve));
  }

  outputTail(): string {
    return '';
  }

  /** Drive the process to an exit with `code`, waking watchers. */
  markExited(code: number): void {
    if (this.exitCode !== null) {
      return;
    }
    this.exitCode = code;
    for (const waiter of this.exitWaiters.splice(0)) {
      waiter();
    }
  }
}

/** A spawner that hands back pre-seeded fake processes in order. */
class FakeSpawner implements ProcessSpawner {
  readonly spawned: Array<{ request: SpawnRequest; proc: FakeRunnerProcess }> = [];
  constructor(private readonly queue: FakeRunnerProcess[] = []) {}

  spawn(request: SpawnRequest): RunnerProcess {
    const proc = this.queue.shift() ?? new FakeRunnerProcess();
    this.spawned.push({ request, proc });
    return proc;
  }
}

/** A socket that records sent frames and never closes on its own. */
class RecordingSocket implements WorkerSocket {
  readonly sent: string[] = [];
  private readonly receiveWaiters: Array<(m: { type: 'close' }) => void> = [];

  receive(): Promise<{ type: 'close' }> {
    // Block forever unless explicitly closed via `drop()`.
    return new Promise((resolve) => this.receiveWaiters.push(resolve));
  }

  sendText(data: string): Promise<void> {
    this.sent.push(data);
    return Promise.resolve();
  }

  closeSocket(): void {
    // no-op
  }

  /** Unblock the current receive with a close (simulate disconnect). */
  drop(): void {
    for (const waiter of this.receiveWaiters.splice(0)) {
      waiter({ type: 'close' });
    }
  }

  helloFrames(): WorkerHelloFrame[] {
    const out: WorkerHelloFrame[] = [];
    for (const text of this.sent) {
      const frame = decodeWorkerFrame(text);
      if (frame.kind === WorkerFrameKind.Hello) {
        out.push(frame);
      }
    }
    return out;
  }

  runnerExitedFrames(): WorkerRunnerExitedFrame[] {
    const out: WorkerRunnerExitedFrame[] = [];
    for (const text of this.sent) {
      const frame = decodeWorkerFrame(text);
      if (frame.kind === WorkerFrameKind.RunnerExited) {
        out.push(frame);
      }
    }
    return out;
  }
}

function buildWorker(
  spawner: ProcessSpawner,
  overrides: { sleep?: (ms: number) => Promise<void> } = {},
): EnvironmentWorker {
  return new EnvironmentWorker({
    environmentId: 'env_x',
    environmentKey: 'sk-env-key',
    registryTunnelBaseUrl: 'wss://registry.example.com',
    registryRunnerUrl: 'wss://registry.example.com',
    workspaceDir: '/tmp',
    runnerLaunchCommand: ['/bin/true'],
    name: 'worker',
    spawner,
    ...(overrides.sleep !== undefined ? { sleep: overrides.sleep } : {}),
  });
}

/** Poll `predicate` until true, or throw naming `what` once `timeoutMs` elapses. */
async function waitFor(what: string, predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for: ${what}`);
    }
    await new Promise((r) => setTimeout(r, 2));
  }
}

describe('aliveRunnerIds', () => {
  it('prunes dead runners and returns only the live ones', async () => {
    const alive = new FakeRunnerProcess();
    const dead = new FakeRunnerProcess();
    const spawner = new FakeSpawner([alive, dead]);
    const worker = buildWorker(spawner);

    await worker.handleLaunchForTest({
      requestId: 'r1',
      bindingToken: 'tok_alive',
      workspace: '/tmp',
    });
    await worker.handleLaunchForTest({
      requestId: 'r2',
      bindingToken: 'tok_dead',
      workspace: '/tmp',
    });

    const aliveId = tokenBoundRunnerId('tok_alive');
    const deadId = tokenBoundRunnerId('tok_dead');
    expect(worker.aliveRunnerIds().sort()).toEqual([aliveId, deadId].sort());

    // The dead runner exits; aliveRunnerIds prunes it as a side effect.
    dead.markExited(0);
    const live = worker.aliveRunnerIds();
    expect(live).toContain(aliveId);
    expect(live).not.toContain(deadId);
  });
});

describe('immediate exit at launch', () => {
  it('fails the launch with the exit code + output tail when the runner is already dead', async () => {
    // A handle that reports a non-null poll() right after spawn (already exited)
    // exercises the synchronous immediate-death branch of the launch path: the
    // result is "failed" carrying the exit code + the captured output tail, and
    // no runner is tracked. (A real fast-dying subprocess instead surfaces via
    // the watcher — covered in worker-launch.spec.ts.)
    class DeadOnArrival extends FakeRunnerProcess {
      constructor() {
        super();
        this.exitCode = 7;
      }
      override outputTail(): string {
        return 'RuntimeError: boom-traceback';
      }
    }
    const spawner = new FakeSpawner([new DeadOnArrival()]);
    const worker = buildWorker(spawner);

    const result = await worker.handleLaunchForTest({
      requestId: 'r_dead',
      bindingToken: 'tok_dead',
      workspace: '/tmp',
    });

    expect(result.status).toBe('failed');
    expect(result.error ?? '').toContain('code 7');
    expect(result.error ?? '').toContain('RuntimeError: boom-traceback');
    expect(result.runnerId).toBeNull();
    expect(worker.aliveRunnerIds()).toEqual([]);
  });
});

describe('retried launch (idempotency)', () => {
  it('does not spawn a second runner when a launch is retried with the same binding token', async () => {
    // The registry documents launch_runner as retryable against a reconnected
    // worker, and the runner id is a pure function of the binding token — so a
    // retry maps to the SAME id. Spawning again would overwrite the tracked
    // handle with the second process and orphan the first: still running, no
    // longer watched, absent from the hello, and sharing the first's binding
    // token, runner-tunnel identity and workspace.
    const first = new FakeRunnerProcess();
    const second = new FakeRunnerProcess();
    const spawner = new FakeSpawner([first, second]);
    const worker = buildWorker(spawner);

    const launched = await worker.handleLaunchForTest({
      requestId: 'req_launch',
      bindingToken: 'tok_retry',
      workspace: '/tmp',
    });
    const retried = await worker.handleLaunchForTest({
      requestId: 'req_launch_retry',
      bindingToken: 'tok_retry',
      workspace: '/tmp',
    });

    const runnerId = tokenBoundRunnerId('tok_retry');
    // The retry is still a success — the runner it asked for IS running.
    expect(launched.status).toBe('launched');
    expect(retried.status).toBe('launched');
    expect(launched.runnerId).toBe(runnerId);
    expect(retried.runnerId).toBe(runnerId);
    // …but exactly ONE process was spawned.
    expect(spawner.spawned.length).toBe(1);
    expect(worker.aliveRunnerIds()).toEqual([runnerId]);

    // The one tracked handle is the FIRST process, so shutdown reaches it; the
    // second was never spawned at all.
    await worker.stop();
    expect(first.terminated).toBe(true);
    expect(second.terminated).toBe(false);
    expect(second.exitCode).toBeNull();
  });

  it('spawns again once the previous runner for that token has exited', async () => {
    // Idempotency is keyed on a LIVE handle, not on the id ever having been
    // used: a fresh session that reuses the token after the runner died must
    // still launch.
    const first = new FakeRunnerProcess();
    const second = new FakeRunnerProcess();
    const spawner = new FakeSpawner([first, second]);
    const worker = buildWorker(spawner);

    await worker.handleLaunchForTest({
      requestId: 'req_a',
      bindingToken: 'tok_reuse',
      workspace: '/tmp',
    });
    first.markExited(0);
    await worker.drainWatchersForTest();

    const again = await worker.handleLaunchForTest({
      requestId: 'req_b',
      bindingToken: 'tok_reuse',
      workspace: '/tmp',
    });

    expect(again.status).toBe('launched');
    expect(spawner.spawned.length).toBe(2);
  });
});

describe('cleanup on shutdown', () => {
  it('terminates every live runner', async () => {
    const a = new FakeRunnerProcess();
    const b = new FakeRunnerProcess();
    const c = new FakeRunnerProcess();
    const spawner = new FakeSpawner([a, b, c]);
    const worker = buildWorker(spawner);

    for (const tok of ['tok_a', 'tok_b', 'tok_c']) {
      await worker.handleLaunchForTest({ requestId: tok, bindingToken: tok, workspace: '/tmp' });
    }

    await worker.stop();

    expect(a.terminated).toBe(true);
    expect(b.terminated).toBe(true);
    expect(c.terminated).toBe(true);
    expect(worker.aliveRunnerIds()).toEqual([]);
  });
});

describe('unreported exit flush', () => {
  it('flushes a parked exit report right after the next hello', async () => {
    const spawner = new FakeSpawner();
    const worker = buildWorker(spawner);
    // Seed an exit that could not be sent while the tunnel was down.
    worker.parkUnreportedExitForTest('runner_parked', 'runner process exited with code 1');

    const socket = new RecordingSocket();
    // serveFrames sends hello, flushes parked reports, then blocks on receive;
    // dropping the socket ends the serve loop, which surfaces the close as a
    // throw (the reconnect loop's recycle classifier reads it).
    const served = worker.serveFramesForTest(socket);
    // Allow the microtasks (hello + flush) to run, then drop.
    await new Promise((r) => setTimeout(r, 20));
    socket.drop();
    await expect(served).rejects.toThrow(/worker tunnel closed/);

    const hellos = socket.helloFrames();
    expect(hellos.length).toBe(1);
    const reports = socket.runnerExitedFrames();
    expect(reports.length).toBe(1);
    expect(reports[0]!.runnerId).toBe('runner_parked');
    expect(reports[0]!.error).toBe('runner process exited with code 1');
    // The queue drained — a retained entry would re-send on every reconnect.
    expect(worker.unreportedExitsForTest()).toEqual({});
  });

  it('parks a runner_exited report when no tunnel is connected', async () => {
    const proc = new FakeRunnerProcess();
    const spawner = new FakeSpawner([proc]);
    const worker = buildWorker(spawner);

    await worker.handleLaunchForTest({
      requestId: 'r1',
      bindingToken: 'tok_park',
      workspace: '/tmp',
    });
    const runnerId = tokenBoundRunnerId('tok_park');

    // No socket is serving, so the watcher's report has nowhere to go and parks.
    proc.markExited(2);
    await worker.drainWatchersForTest();

    const parked = worker.unreportedExitsForTest();
    expect(Object.keys(parked)).toEqual([runnerId]);
    expect(parked[runnerId]).toContain('code 2');
  });
});

/**
 * A socket whose receive() returns ONE persistent promise the test resolves by
 * hand, and counts how many times receive() was invoked. The serve loop reuses
 * the in-flight receive across idle wakeups, so across N idle timeouts before a
 * frame finally arrives, receive() must be called exactly once.
 */
class ManualReceiveSocket implements WorkerSocket {
  readonly sent: string[] = [];
  receiveCalls = 0;
  private resolveNext: ((m: WorkerSocketMessage) => void) | undefined;

  receive(): Promise<WorkerSocketMessage> {
    this.receiveCalls += 1;
    return new Promise<WorkerSocketMessage>((resolve) => {
      this.resolveNext = resolve;
    });
  }

  sendText(data: string): Promise<void> {
    this.sent.push(data);
    return Promise.resolve();
  }

  closeSocket(): void {
    // no-op
  }

  /** Deliver one message to the currently-pending receive(). */
  deliver(message: WorkerSocketMessage): void {
    const resolve = this.resolveNext;
    if (resolve === undefined) {
      throw new Error('no pending receive to deliver to');
    }
    this.resolveNext = undefined;
    resolve(message);
  }
}

/**
 * A runner that IGNORES both signals: `terminate()` and `kill()` report the
 * signal as delivered (the syscall succeeded) but the process never exits, so
 * `wait()` never resolves. This is the wedged-child case — uninterruptible I/O,
 * a stuck FUSE mount on the gVisor runtime — not an exotic one.
 */
class UnkillableRunnerProcess extends FakeRunnerProcess {
  killCalls = 0;

  override terminate(): boolean {
    this.terminated = true;
    return true;
  }

  override kill(): boolean {
    this.killed = true;
    this.killCalls += 1;
    return true;
  }
}

/**
 * A runner whose SIGTERM is REFUSED — `child.kill` returned false (EPERM, or the
 * pid was reaped out from under us). Distinct from
 * {@link UnkillableRunnerProcess}: there the signal landed and was ignored; here
 * the process was never asked to stop at all, so waiting out the terminate grace
 * can only ever expire.
 */
class RefusesSigtermRunnerProcess extends UnkillableRunnerProcess {
  override terminate(): boolean {
    this.terminated = true;
    return false;
  }
}

/** Decode the stop results a socket has been sent. */
function stopResults(socket: { sent: string[] }): WorkerStopRunnerResultFrame[] {
  const out: WorkerStopRunnerResultFrame[] = [];
  for (const text of socket.sent) {
    let frame;
    try {
      frame = decodeWorkerFrame(text);
    } catch {
      continue;
    }
    if (frame.kind === WorkerFrameKind.StopRunnerResult) {
      out.push(frame);
    }
  }
  return out;
}

/** Count the runner-tunnel pongs a socket has been sent. */
function pongCount(socket: { sent: string[] }): number {
  return socket.sent.filter((text) => {
    try {
      return decodeFrame(text).kind === FrameKind.Pong;
    } catch {
      return false;
    }
  }).length;
}

describe('stop of a runner that survives SIGKILL', () => {
  it('fails the stop naming the pid instead of wedging the serve loop', async () => {
    // handleStop awaits the terminate→SIGKILL→reap sequence INLINE inside the
    // serve loop. An unbounded final wait therefore does not just hang the stop:
    // the worker answers nothing again — not the stop, not the keepalive pings —
    // and a socket close cannot break it either.
    const proc = new UnkillableRunnerProcess();
    const spawner = new FakeSpawner([proc]);
    // A tiny real sleep: the terminate/kill graces collapse to ~1ms while the
    // watcher's poll loop still yields to the event loop between turns.
    const worker = buildWorker(spawner, {
      sleep: () => new Promise<void>((r) => setTimeout(r, 1)),
    });
    await worker.handleLaunchForTest({
      requestId: 'req_wedge',
      bindingToken: 'tok_wedge',
      workspace: '/tmp',
    });
    const runnerId = tokenBoundRunnerId('tok_wedge');

    const socket = new ManualReceiveSocket();
    const served = worker.serveFramesForTest(socket);
    await waitFor('the serve loop to arm its first receive', () => socket.receiveCalls === 1);

    socket.deliver({
      type: 'text',
      data: encodeWorkerFrame({
        kind: WorkerFrameKind.StopRunner,
        requestId: 'req_stop_wedged',
        runnerId,
      }),
    });

    // The stop is ANSWERED, and answered honestly.
    await waitFor('a stop result frame', () => stopResults(socket).length === 1);
    const stop = stopResults(socket)[0]!;
    expect(stop.status).toBe('failed');
    expect(stop.error ?? '').toContain(String(proc.pid));
    expect(stop.error ?? '').toContain('SIGKILL');
    expect(proc.killCalls).toBe(1);
    // The runner really is still running, so the worker still tracks it — a
    // "stopped" reply for a live runner it had also forgotten is exactly what
    // this must not do.
    expect(worker.aliveRunnerIds()).toContain(runnerId);

    // The serve loop was never parked: a later ping is still ponged.
    await waitFor('the serve loop to read again', () => socket.receiveCalls === 2);
    socket.deliver({ type: 'text', data: encodeFrame({ kind: FrameKind.Ping, ts: 77 }) });
    await waitFor('a pong for the ping sent after the stop', () => pongCount(socket) === 1);

    // Release the wedged child so nothing outlives the test, then close.
    proc.markExited(137);
    await waitFor('the serve loop to read again', () => socket.receiveCalls === 3);
    socket.deliver({ type: 'close', code: 1000 });
    await expect(served).rejects.toThrow(/worker tunnel closed/);
    await worker.stop();
  });

  it('escalates immediately, and says so, when the SIGTERM was refused', async () => {
    // `terminate()` reports whether the signal was DELIVERED. Discarding that
    // boolean cost a refused SIGTERM the full 5s terminate grace — spent waiting
    // for an exit that was never requested — and then blamed the timeout on a
    // SIGTERM the process never received.
    const proc = new RefusesSigtermRunnerProcess();
    const spawner = new FakeSpawner([proc]);
    const slept: number[] = [];
    const worker = buildWorker(spawner, {
      sleep: (ms) => {
        slept.push(ms);
        return new Promise<void>((r) => setTimeout(r, 1));
      },
    });
    await worker.handleLaunchForTest({
      requestId: 'req_eperm',
      bindingToken: 'tok_eperm',
      workspace: '/tmp',
    });
    const runnerId = tokenBoundRunnerId('tok_eperm');

    const socket = new ManualReceiveSocket();
    const served = worker.serveFramesForTest(socket);
    await waitFor('the serve loop to arm its first receive', () => socket.receiveCalls === 1);

    socket.deliver({
      type: 'text',
      data: encodeWorkerFrame({
        kind: WorkerFrameKind.StopRunner,
        requestId: 'req_stop_eperm',
        runnerId,
      }),
    });

    await waitFor('a stop result frame', () => stopResults(socket).length === 1);
    const stop = stopResults(socket)[0]!;
    expect(stop.status).toBe('failed');
    // The cause is NAMED, not flattened into an unexplained timeout.
    expect(stop.error ?? '').toMatch(/SIGTERM was refused/i);
    // And the 5s grace was never waited out — of the two grace windows, only the
    // kill grace was entered. (The sub-second sleeps are the watcher's poll
    // loop, which is unrelated to the stop sequence.)
    const graces = slept.filter((ms) => ms >= 1_000);
    expect(graces).toEqual([2_000]);
    expect(proc.killCalls).toBe(1);

    proc.markExited(137);
    await waitFor('the serve loop to read again', () => socket.receiveCalls === 2);
    socket.deliver({ type: 'close', code: 1000 });
    await expect(served).rejects.toThrow(/worker tunnel closed/);
    await worker.stop();
  });
});

/**
 * A socket whose hello send succeeds but whose PONG send always rejects — a
 * tunnel that has gone one-way. Everything else behaves like
 * {@link ManualReceiveSocket}.
 */
class PongFailingSocket implements WorkerSocket {
  readonly sent: string[] = [];
  receiveCalls = 0;
  private resolveNext: ((m: WorkerSocketMessage) => void) | undefined;

  receive(): Promise<WorkerSocketMessage> {
    this.receiveCalls += 1;
    return new Promise<WorkerSocketMessage>((resolve) => {
      this.resolveNext = resolve;
    });
  }

  sendText(data: string): Promise<void> {
    let isPong = false;
    try {
      isPong = decodeFrame(data).kind === FrameKind.Pong;
    } catch {
      isPong = false;
    }
    if (isPong) {
      return Promise.reject(new Error('socket write failed: tunnel is one-way'));
    }
    this.sent.push(data);
    return Promise.resolve();
  }

  closeSocket(): void {
    // no-op
  }

  deliver(message: WorkerSocketMessage): void {
    const resolve = this.resolveNext;
    if (resolve === undefined) {
      throw new Error('no pending receive to deliver to');
    }
    this.resolveNext = undefined;
    resolve(message);
  }
}

describe('pong send failure', () => {
  it('ends the serve loop so the reconnect loop takes over', async () => {
    // A pong that cannot be written means the tunnel is broken. Swallowing the
    // failure leaves the worker looping in the belief that it is answering
    // keepalives while the registry counts missed pongs and fails every
    // in-flight launch/stop/stat against this worker as retryable.
    const worker = buildWorker(new FakeSpawner());
    const socket = new PongFailingSocket();

    const served = worker.serveFramesForTest(socket);
    await waitFor('the serve loop to arm its receive', () => socket.receiveCalls === 1);
    socket.deliver({ type: 'text', data: encodeFrame({ kind: FrameKind.Ping, ts: 11 }) });

    await expect(served).rejects.toThrow(/socket write failed/);
  });
});

describe('undecodable inbound frame', () => {
  it('is logged and dropped without ending the serve loop', async () => {
    const warnings: Array<{ obj: unknown; msg?: string }> = [];
    const worker = new EnvironmentWorker({
      environmentId: 'env_x',
      environmentKey: 'sk-env-key',
      registryTunnelBaseUrl: 'wss://registry.example.com',
      registryRunnerUrl: 'wss://registry.example.com',
      workspaceDir: '/tmp',
      runnerLaunchCommand: ['/bin/true'],
      name: 'worker',
      spawner: new FakeSpawner(),
      logger: {
        warn: (obj, msg) => {
          warnings.push({ obj, ...(msg !== undefined ? { msg } : {}) });
        },
      },
    });

    const socket = new ManualReceiveSocket();
    const served = worker.serveFramesForTest(socket);
    await waitFor('the serve loop to arm its receive', () => socket.receiveCalls === 1);
    socket.deliver({ type: 'text', data: '{"kind":"not.a.frame"}' });

    await waitFor('the dropped-frame warning', () => warnings.length === 1);
    expect(warnings[0]!.msg ?? '').toContain('decodes as neither');

    // The loop survived the drop and is reading again.
    await waitFor('the serve loop to read again', () => socket.receiveCalls === 2);
    socket.deliver({ type: 'close', code: 1000 });
    await expect(served).rejects.toThrow(/worker tunnel closed/);
  });
});

describe('serve loop idle wakeup', () => {
  it('wakes on the idle timer without consuming the pending receive, then serves the next frame', async () => {
    let idleWakeups = 0;
    const worker = new EnvironmentWorker({
      environmentId: 'env_x',
      environmentKey: 'sk-env-key',
      registryTunnelBaseUrl: 'wss://registry.example.com',
      registryRunnerUrl: 'wss://registry.example.com',
      workspaceDir: '/tmp',
      runnerLaunchCommand: ['/bin/true'],
      name: 'worker',
      spawner: new FakeSpawner(),
      // Tiny idle timeout so several wakeups elapse in milliseconds.
      serveIdleTimeoutMs: 5,
      onServeIdleForTest: () => {
        idleWakeups += 1;
      },
    });

    const socket = new ManualReceiveSocket();
    const served = worker.serveFramesForTest(socket);

    // Let multiple idle wakeups fire while the socket stays silent.
    await new Promise((r) => setTimeout(r, 40));
    // The idle timer actually fired (the loop woke on a silent socket)…
    expect(idleWakeups).toBeGreaterThan(0);
    // …yet the single in-flight receive() was preserved across every wakeup: a
    // wakeup that wrongly consumed the receive would re-issue it each time.
    expect(socket.receiveCalls).toBe(1);

    // A frame finally arrives after the idle wakeups — it must still be serviced:
    // a registry ping is answered with a pong on the same socket.
    socket.deliver({ type: 'text', data: encodeFrame({ kind: FrameKind.Ping, ts: 99 }) });
    // The pong send + the loop re-arming receive() happen on later microtasks.
    await new Promise((r) => setTimeout(r, 20));

    // Decode only the runner-family frames the worker sent (the hello is a worker
    // frame the runner-frame decoder rejects — skip those, as the worker itself
    // distinguishes the two frame families over the one socket).
    const pongs = socket.sent
      .map((t) => {
        try {
          return decodeFrame(t);
        } catch {
          return undefined;
        }
      })
      .filter((f) => f !== undefined && f.kind === FrameKind.Pong);
    expect(pongs.length).toBe(1);
    const pong = pongs[0]!;
    expect(pong.kind === FrameKind.Pong && pong.ts).toBe(99);
    // The loop re-armed a fresh receive() after handling the ping.
    expect(socket.receiveCalls).toBe(2);

    // Close to end the serve loop (it surfaces the close as a throw).
    socket.deliver({ type: 'close', code: 1000 });
    await expect(served).rejects.toThrow(/worker tunnel closed/);
  });
});
