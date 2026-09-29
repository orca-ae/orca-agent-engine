// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Fail-loud / reconnect / error-classification cases for the worker run loop.
//
// The worker's reconnect loop must: fail LOUD (raise, no backoff) on a permanent
// upgrade rejection (auth / authorization / wrong-or-old registry — a 4xx that
// reconnecting can never fix); RECONNECT on a transient one (retryable 4xx
// 408/429, any 5xx, a server bounce); and pick a PROMPT reconnect on an explicit
// recycle CLOSE CODE (1012/1001) or an abrupt no-close-frame drop of an
// ESTABLISHED tunnel on a non-loopback registry, while backing off normally on a
// loopback drop (no ingress there — a tight loop would fuel a re-registration
// flap).
//
// The recycle cadence is scoped and bounded, and both parts are covered here: it
// never applies to an upgrade REJECTION (502 is the rolling-deploy status, and
// granting it the prompt cadence pins the whole fleet at the base interval for
// the redeploy), it reads the close code rather than substring-matching a
// peer-supplied reason, and consecutive prompt reconnects are capped so a
// persistent condition falls back to the growing backoff.
//
// There is deliberately no login-page-redirect case: the registry endpoint has
// no OAuth proxy in front (the worker authenticates with its Env Key on a
// dedicated header), so there is no login-redirect failure mode to classify. The
// HTTP-status classification + recycle heuristic are the parts that apply here.
//
// These drive the worker through an injected connect seam that scripts each
// handshake, and end each scripted run with `worker.stop()` (the worker's
// cooperative cancellation) so a `'stop'` outcome makes the loop return cleanly.

import { describe, expect, it } from 'vitest';
import { EnvironmentWorker, MAX_RECYCLE_RECONNECTS, RECONNECT_BASE_MS } from '../../src/worker.js';
import { EnvironmentConnectError } from '../../src/errors.js';
import {
  UpgradeRejectedError,
  type RegistryConnector,
  type WorkerSocket,
} from '../../src/ws-client.js';

/** A worker socket whose first receive resolves to a close (drops immediately). */
class DroppedSocket implements WorkerSocket {
  constructor(private readonly close: { code?: number; reason?: string } = {}) {}
  receive(): Promise<{ type: 'close'; code?: number; reason?: string }> {
    return Promise.resolve({ type: 'close', ...this.close });
  }
  sendText(): Promise<void> {
    return Promise.resolve();
  }
  closeSocket(): void {
    // no-op
  }
}

/** One scripted handshake outcome. */
type Outcome =
  | { kind: 'accept'; close?: { code?: number; reason?: string } }
  | { kind: 'reject'; error: unknown }
  | { kind: 'stop' };

/**
 * Connect seam that scripts each dial with the next queued outcome (the final
 * entry repeats). An 'accept' hands back a socket that drops on first receive so
 * the reconnect loop regains control; a 'reject' fails that handshake; a 'stop'
 * calls {@link onStop} (the worker's cooperative cancellation) and then hands
 * back a dropping socket so the loop observes the stop and returns cleanly.
 */
class ScriptedConnector implements RegistryConnector {
  callCount = 0;
  readonly dialedUrls: string[] = [];
  readonly dialedHeaders: Array<Record<string, string>> = [];
  onStop: (() => void) | undefined;
  constructor(private readonly outcomes: Outcome[]) {}

  connect(url: string, headers: Record<string, string>): Promise<WorkerSocket> {
    this.dialedUrls.push(url);
    this.dialedHeaders.push(headers);
    const outcome = this.outcomes[Math.min(this.callCount, this.outcomes.length - 1)]!;
    this.callCount += 1;
    if (outcome.kind === 'reject') {
      return Promise.reject(outcome.error);
    }
    if (outcome.kind === 'stop') {
      this.onStop?.();
      return Promise.resolve(new DroppedSocket());
    }
    return Promise.resolve(new DroppedSocket(outcome.close ?? {}));
  }
}

interface WorkerBuild {
  worker: EnvironmentWorker;
  sleeps: number[];
}

function buildWorker(
  connector: ScriptedConnector,
  overrides: {
    registryTunnelBaseUrl?: string;
    base?: number;
    cap?: number;
    /** Managed-auth override: when set, the worker is built WITHOUT an Env Key. */
    environmentToken?: string;
  } = {},
): WorkerBuild {
  const sleeps: number[] = [];
  const worker = new EnvironmentWorker({
    environmentId: 'env_x',
    ...(overrides.environmentToken !== undefined
      ? { environmentToken: overrides.environmentToken }
      : { environmentKey: 'sk-env-key' }),
    registryTunnelBaseUrl: overrides.registryTunnelBaseUrl ?? 'wss://registry.example.com',
    registryRunnerUrl: 'wss://registry.example.com',
    workspaceDir: '/tmp',
    runnerLaunchCommand: ['/bin/true'],
    name: 'worker',
    connector,
    reconnectBaseMs: overrides.base ?? 0,
    reconnectCapMs: overrides.cap ?? 0,
    sleep: (ms: number) => {
      sleeps.push(ms);
      return Promise.resolve();
    },
  });
  // Wire the 'stop' outcome to the worker's cooperative cancellation.
  connector.onStop = () => void worker.stop();
  return { worker, sleeps };
}

describe('EnvironmentWorker.run — connect headers + URL', () => {
  it('dials the canonical tunnel path with the Env Key + internal origin headers', async () => {
    const connector = new ScriptedConnector([{ kind: 'accept' }, { kind: 'stop' }]);
    const { worker } = buildWorker(connector, {
      registryTunnelBaseUrl: 'wss://registry.example.com',
    });
    await worker.run();

    expect(connector.dialedUrls[0]).toBe(
      'wss://registry.example.com/v1/tunnels/environments/env_x',
    );
    expect(connector.dialedHeaders[0]!['X-Orca-Environment-Key']).toBe('sk-env-key');
    expect(connector.dialedHeaders[0]!.Origin).toBe('orca://internal');
  });

  it('derives a wss URL from an https base and ws from http', async () => {
    const httpsConnector = new ScriptedConnector([{ kind: 'stop' }]);
    const { worker: httpsWorker } = buildWorker(httpsConnector, {
      registryTunnelBaseUrl: 'https://registry.example.com',
    });
    await httpsWorker.run();
    expect(httpsConnector.dialedUrls[0]).toBe(
      'wss://registry.example.com/v1/tunnels/environments/env_x',
    );

    const httpConnector = new ScriptedConnector([{ kind: 'stop' }]);
    const { worker: httpWorker } = buildWorker(httpConnector, {
      registryTunnelBaseUrl: 'http://127.0.0.1:6789',
    });
    await httpWorker.run();
    expect(httpConnector.dialedUrls[0]).toBe('ws://127.0.0.1:6789/v1/tunnels/environments/env_x');
  });
});

// ── Managed-auth fork: Environment Token vs Env Key ─────────
//
// A registry-launched worker (a server-managed sandbox with no operator to
// provision an Env Key) is instead configured with a per-launch Environment
// Token. The worker must send it on its OWN dedicated header and skip the
// Env-Key header entirely — never both — while a worker with no managed
// token keeps sending exactly the pre-existing Env-Key + internal-origin
// headers.

describe('EnvironmentWorker.run — managed auth (Environment Token)', () => {
  it('sends the Environment Token header (no Env Key) when constructed with a managed token', async () => {
    const connector = new ScriptedConnector([{ kind: 'accept' }, { kind: 'stop' }]);
    const { worker } = buildWorker(connector, { environmentToken: 'et-managed-token' });
    await worker.run();

    expect(connector.dialedHeaders[0]!['X-Orca-Environment-Token']).toBe('et-managed-token');
    expect(connector.dialedHeaders[0]!['X-Orca-Environment-Key']).toBeUndefined();
    expect(connector.dialedHeaders[0]!.Origin).toBe('orca://internal');
  });

  it('sends only the Environment Token even when an Env Key is ALSO configured (no fallthrough)', async () => {
    const connector = new ScriptedConnector([{ kind: 'accept' }, { kind: 'stop' }]);
    const worker = new EnvironmentWorker({
      environmentId: 'env_x',
      environmentKey: 'sk-should-never-be-sent',
      environmentToken: 'et-managed-token',
      registryTunnelBaseUrl: 'wss://registry.example.com',
      registryRunnerUrl: 'wss://registry.example.com',
      workspaceDir: '/tmp',
      runnerLaunchCommand: ['/bin/true'],
      name: 'worker',
      connector,
      reconnectBaseMs: 0,
      reconnectCapMs: 0,
      sleep: () => Promise.resolve(),
    });
    connector.onStop = () => void worker.stop();
    await worker.run();

    expect(connector.dialedHeaders[0]!['X-Orca-Environment-Token']).toBe('et-managed-token');
    expect(connector.dialedHeaders[0]!['X-Orca-Environment-Key']).toBeUndefined();
  });

  it('without a managed token, sends the Env Key and no Environment Token header (unchanged)', async () => {
    const connector = new ScriptedConnector([{ kind: 'accept' }, { kind: 'stop' }]);
    const { worker } = buildWorker(connector);
    await worker.run();

    expect(connector.dialedHeaders[0]!['X-Orca-Environment-Key']).toBe('sk-env-key');
    expect(connector.dialedHeaders[0]!['X-Orca-Environment-Token']).toBeUndefined();
  });
});

describe('EnvironmentWorker.run — fail loud on permanent 4xx', () => {
  for (const status of [401, 403, 404]) {
    it(`raises EnvironmentConnectError immediately on HTTP ${status} (no retry)`, async () => {
      const connector = new ScriptedConnector([
        { kind: 'reject', error: new UpgradeRejectedError(status) },
      ]);
      const { worker } = buildWorker(connector);
      await expect(worker.run()).rejects.toBeInstanceOf(EnvironmentConnectError);
      // Exactly one attempt → no silent reconnect/backoff.
      expect(connector.callCount).toBe(1);
    });
  }

  it('names the HTTP status in the fatal message', async () => {
    const connector = new ScriptedConnector([
      { kind: 'reject', error: new UpgradeRejectedError(403) },
    ]);
    const { worker } = buildWorker(connector);
    await expect(worker.run()).rejects.toThrow(/HTTP 403/);
  });
});

describe('EnvironmentWorker.run — reconnect on transient upgrade failures', () => {
  for (const status of [408, 429, 500, 503]) {
    it(`reconnects (does not fail loud) on HTTP ${status}`, async () => {
      // Transient rejection, then a 'stop' to end the loop after a retry.
      const connector = new ScriptedConnector([
        { kind: 'reject', error: new UpgradeRejectedError(status) },
        { kind: 'stop' },
      ]);
      const { worker } = buildWorker(connector);
      await worker.run();
      // 2 = transient attempt + the stop attempt → it genuinely reconnected.
      expect(connector.callCount).toBe(2);
    });
  }

  it('reconnects after a successful connect that then drops', async () => {
    // Accept (drops on first receive) → reconnect → 'stop' ends the loop.
    const connector = new ScriptedConnector([{ kind: 'accept' }, { kind: 'stop' }]);
    const { worker } = buildWorker(connector);
    await worker.run();
    expect(connector.callCount).toBe(2);
  });
});

describe('EnvironmentWorker.run — recycle heuristic (reconnect cadence)', () => {
  // The worker classifies the disconnect reason to choose a prompt reconnect vs
  // a normal backoff. The FIRST wait after any drop is the initial backoff
  // (= base) either way; the heuristic shows in the SECOND wait: a recycle keeps
  // the backoff pinned at base, while a non-recycle has grown it. So each case
  // scripts TWO drops and asserts on `sleeps[1]`. We drive sleep through the
  // injected timer (no real seconds elapse).

  it('keeps the reconnect at the base interval across an explicit 1012 service-restart close', async () => {
    const connector = new ScriptedConnector([
      { kind: 'accept', close: { code: 1012, reason: 'service restart' } },
      { kind: 'accept', close: { code: 1012, reason: 'service restart' } },
      { kind: 'stop' },
    ]);
    const { worker, sleeps } = buildWorker(connector, {
      base: 500,
      cap: 10_000,
      registryTunnelBaseUrl: 'wss://registry.example.com',
    });
    await worker.run();
    // Both recycle reconnects waited the base interval — the backoff never grew.
    expect(sleeps[0]).toBe(500);
    expect(sleeps[1]).toBe(500);
  });

  it('keeps the reconnect prompt on an ingress recycle (no close frame) for a REMOTE registry', async () => {
    const connector = new ScriptedConnector([
      { kind: 'accept', close: { reason: 'no close frame received' } },
      { kind: 'accept', close: { reason: 'no close frame received' } },
      { kind: 'stop' },
    ]);
    const { worker, sleeps } = buildWorker(connector, {
      base: 500,
      cap: 10_000,
      registryTunnelBaseUrl: 'wss://registry.example.com',
    });
    await worker.run();
    expect(sleeps[1]).toBe(500);
  });

  it('backs off normally on a no-close-frame drop for a LOOPBACK registry', async () => {
    // No ingress in front of a loopback registry: an abrupt drop is a real
    // condition, so the worker backs off (grows past base) instead of tight-
    // looping, which would fuel a re-registration flap.
    const connector = new ScriptedConnector([
      { kind: 'accept', close: { reason: 'no close frame received' } },
      { kind: 'accept', close: { reason: 'no close frame received' } },
      { kind: 'stop' },
    ]);
    const { worker, sleeps } = buildWorker(connector, {
      base: 500,
      cap: 10_000,
      registryTunnelBaseUrl: 'ws://127.0.0.1:6789',
    });
    await worker.run();
    // The first wait is the initial backoff (base); the second has grown past it.
    expect(sleeps[0]).toBe(500);
    expect(sleeps[1]).toBeGreaterThan(500);
  });
});

describe('EnvironmentWorker.run — the recycle cadence is scoped to an established tunnel', () => {
  it('backs off on a persistent HTTP 502 upgrade rejection instead of pinning at base', async () => {
    // 502 is the canonical rolling-deploy status, and the whole fleet sees it at
    // once. The recycle heuristic is about a HEALTHY socket being moved out from
    // under the worker; an upgrade the registry is REJECTING is not that, and
    // treating it as one has every worker dialing at 2/s for the redeploy window.
    const connector = new ScriptedConnector([
      { kind: 'reject', error: new UpgradeRejectedError(502) },
      { kind: 'reject', error: new UpgradeRejectedError(502) },
      { kind: 'reject', error: new UpgradeRejectedError(502) },
      { kind: 'stop' },
    ]);
    const { worker, sleeps } = buildWorker(connector, {
      base: 500,
      cap: 10_000,
      registryTunnelBaseUrl: 'wss://registry.example.com',
    });
    await worker.run();

    expect(sleeps[0]).toBe(500);
    expect(sleeps[1]).toBeGreaterThan(500);
    expect(sleeps[2]).toBeGreaterThan(sleeps[1]!);
  });

  it('reads the close CODE, not the composed message, so a forged reason is not a recycle', async () => {
    // The peer controls the close `reason`. A reason mentioning an ingress
    // status or a recycle code must not buy the prompt cadence when the actual
    // close code is an ordinary 1000.
    const forged = {
      kind: 'accept',
      close: { code: 1000, reason: 'upstream returned 502 while draining; see 1012' },
    } as const;
    const connector = new ScriptedConnector([forged, forged, { kind: 'stop' }]);
    const { worker, sleeps } = buildWorker(connector, {
      base: 500,
      cap: 10_000,
      registryTunnelBaseUrl: 'wss://registry.example.com',
    });
    await worker.run();

    expect(sleeps[0]).toBe(500);
    expect(sleeps[1]).toBeGreaterThan(500);
  });

  it('caps consecutive recycle-cadence reconnects so a persistent condition cannot pin base', async () => {
    const recycle = { kind: 'accept', close: { code: 1012, reason: 'service restart' } } as const;
    const connector = new ScriptedConnector([
      recycle,
      recycle,
      recycle,
      recycle,
      recycle,
      recycle,
      recycle,
      { kind: 'stop' },
    ]);
    const { worker, sleeps } = buildWorker(connector, {
      base: 500,
      cap: 10_000,
      registryTunnelBaseUrl: 'wss://registry.example.com',
    });
    await worker.run();

    // The allowance is honored…
    expect(sleeps.slice(0, MAX_RECYCLE_RECONNECTS)).toEqual(
      new Array<number>(MAX_RECYCLE_RECONNECTS).fill(500),
    );
    // …and then the ordinary backoff takes over: a condition that keeps looking
    // like a recycle is not one, and must not hold the worker at the base rate.
    expect(sleeps[MAX_RECYCLE_RECONNECTS + 1]).toBeGreaterThan(500);
  });
});

describe('reconnect constants', () => {
  it('exposes a 0.5s base reconnect interval', () => {
    expect(RECONNECT_BASE_MS).toBe(500);
  });
});
