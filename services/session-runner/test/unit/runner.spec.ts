// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for SessionRunner — the construction seam + the dial/serve lifecycle.
//
// SessionRunner is the runner's top-level wiring: it derives the token-bound
// runner id, owns the request-dispatch seam the harness unit registers handlers
// on, and drives the serve loop (dial → hello → serve → reconnect). The serve loop
// itself is covered exhaustively in serve.spec; here we assert SessionRunner wires
// it correctly against the SAME in-process fake registry runner-tunnel peer.

import { afterEach, describe, expect, it } from 'vitest';
import { tokenBoundRunnerId, INTERNAL_WS_ORIGIN } from '@orca/harness-tunnel';
import type { RunnerConfig } from '../../src/config.js';
import { SessionRunner } from '../../src/runner.js';
import { FakeRegistryRunnerTunnel, ndjsonResponse } from './support/fake-registry-runner-tunnel.js';

const BINDING_TOKEN = 'binding-token-fixture';

const baseConfig: RunnerConfig = {
  bindingToken: BINDING_TOKEN,
  registryRunnerUrl: 'ws://registry.internal:8081/runner',
  workspace: '/var/run/orca/ws-abc',
  workspaceId: '',
  idleTimeoutS: 0,
  provider: {
    modelDefault: 'claude-sonnet-4-5',
  },
};

describe('SessionRunner construction seam', () => {
  it('derives the token-bound runner id on construction', () => {
    const runner = new SessionRunner({ config: baseConfig });
    expect(runner.runnerId).toBe(tokenBoundRunnerId(BINDING_TOKEN));
  });

  it('exposes the registry runner-tunnel url', () => {
    const runner = new SessionRunner({ config: baseConfig });
    expect(runner.registryRunnerUrl).toBe(baseConfig.registryRunnerUrl);
  });

  it('exposes a dispatcher for the next unit to register turn/lifecycle handlers on', () => {
    const runner = new SessionRunner({ config: baseConfig });
    // The dispatcher is the registration seam the harness unit wires
    // POST /v1/runner/turn (+ snapshot/replay) onto. It is a live RouteDispatcher.
    expect(runner.dispatcher).toBeDefined();
    expect(typeof runner.dispatcher.register).toBe('function');
    expect(typeof runner.dispatcher.dispatch).toBe('function');
  });

  it('stop() before run() is a no-op', async () => {
    const runner = new SessionRunner({ config: baseConfig });
    await expect(runner.stop()).resolves.toBeUndefined();
  });
});

describe('SessionRunner dial + serve', () => {
  let registry: FakeRegistryRunnerTunnel;

  afterEach(async () => {
    if (registry !== undefined) {
      await registry.close();
    }
  });

  /** Build a runner pointed at a live fake registry on `registry`. */
  function runnerForRegistry(
    reg: FakeRegistryRunnerTunnel,
    overrides: Partial<ConstructorParameters<typeof SessionRunner>[0]> = {},
  ): SessionRunner {
    return new SessionRunner({
      config: { ...baseConfig, registryRunnerUrl: reg.baseUrl() },
      ...overrides,
    });
  }

  it('run() dials the registry runner tunnel with the binding token + internal origin', async () => {
    const runnerId = tokenBoundRunnerId(BINDING_TOKEN);
    registry = new FakeRegistryRunnerTunnel({ runnerId, bindingToken: BINDING_TOKEN });
    await registry.listen();
    const runner = runnerForRegistry(registry);
    void runner.run();

    const live = await registry.nextRunner();
    expect(live.path).toBe(`/v1/tunnels/runners/${runnerId}`);
    expect(live.tokenHeader).toBe(BINDING_TOKEN);
    expect(live.originHeader).toBe(INTERNAL_WS_ORIGIN);
    await runner.stop();
  });

  it('serves a pushed request through a handler the next unit registered on the dispatcher', async () => {
    const runnerId = tokenBoundRunnerId(BINDING_TOKEN);
    registry = new FakeRegistryRunnerTunnel({ runnerId, bindingToken: BINDING_TOKEN });
    await registry.listen();
    const runner = runnerForRegistry(registry);
    // Register a turn handler exactly as the harness unit will, then run.
    let seenSession: string | undefined;
    runner.dispatcher.register('POST', '/v1/runner/turn', async (req) => {
      seenSession = req.header('X-Orca-Session-Id');
      return ndjsonResponse([{ type: 'agent.turn_completed' }]);
    });
    void runner.run();

    const live = await registry.nextRunner();
    const res = await live.request({
      method: 'POST',
      path: '/v1/runner/turn',
      headers: [['x-orca-session-id', 'ses_run_1']],
      body: '{"id":"evt_1"}',
    });
    expect(res.status).toBe(200);
    expect(seenSession).toBe('ses_run_1');
    expect(res.body.trim()).toBe('{"type":"agent.turn_completed"}');
    await runner.stop();
  });

  it('advertises providers + resume cursors in the hello', async () => {
    const runnerId = tokenBoundRunnerId(BINDING_TOKEN);
    registry = new FakeRegistryRunnerTunnel({ runnerId, bindingToken: BINDING_TOKEN });
    await registry.listen();
    const runner = runnerForRegistry(registry, {
      providers: ['claude'],
      resumeCursors: () => ({ ses_1: 'evt_42' }),
    });
    void runner.run();

    const live = await registry.nextRunner();
    expect(live.hello.harnesses).toEqual(['claude']);
    expect(live.hello.resumeCursors).toEqual({ ses_1: 'evt_42' });
    await runner.stop();
  });

  it('forwards onActivity to the serve loop (fired for each real work frame)', async () => {
    const runnerId = tokenBoundRunnerId(BINDING_TOKEN);
    registry = new FakeRegistryRunnerTunnel({ runnerId, bindingToken: BINDING_TOKEN });
    await registry.listen();
    let activityTouches = 0;
    const runner = runnerForRegistry(registry, { onActivity: () => (activityTouches += 1) });
    runner.dispatcher.register('POST', '/v1/runner/turn', async () =>
      ndjsonResponse([{ type: 'agent.turn_completed' }]),
    );
    void runner.run();

    const live = await registry.nextRunner();
    await live.request({
      method: 'POST',
      path: '/v1/runner/turn',
      headers: [['x-orca-session-id', 'ses_act']],
      body: '{"id":"evt_1"}',
    });
    // The `request` work frame fired the activity touch (keepalive pings never do).
    expect(activityTouches).toBeGreaterThanOrEqual(1);
    await runner.stop();
  });

  it('run() resolves cleanly after stop() (no fatal rejection on a normal shutdown)', async () => {
    const runnerId = tokenBoundRunnerId(BINDING_TOKEN);
    registry = new FakeRegistryRunnerTunnel({ runnerId, bindingToken: BINDING_TOKEN });
    await registry.listen();
    const runner = runnerForRegistry(registry);
    const runPromise = runner.run();
    await registry.nextRunner();
    await runner.stop();
    await expect(runPromise).resolves.toBeUndefined();
  });
});
