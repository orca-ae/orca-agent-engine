// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// SessionRunner — the long-running client that dials the registry runner tunnel
// and serves the request/response stream the registry pushes for one session.
//
// The runner is the CLIENT half of the runner tunnel: it dials the registry's
// PUBLIC endpoint `/v1/tunnels/runners/:runnerId` with its binding token, says
// hello, and then SERVES the framed HTTP requests the registry pushes — driving
// each through the {@link RouteDispatcher} and streaming a response back (the
// runner behaves like a local app reached over the tunnel). The serve loop +
// reconnect + recovery handling live in `./tunnel/serve.ts`; this class is the
// construction seam that wires the runner's identity + dispatcher into it.
//
// The turn / snapshot / replay HANDLERS the runner serves are registered on
// {@link SessionRunner.dispatcher} by the NEXT unit (which constructs the claude
// AgentHarness and wires `POST /v1/runner/turn`, `/v1/runner/snapshot`,
// `/v1/runner/replay`). This class deliberately does not register them: it owns
// dialing + serving + the dispatch seam, and exposes the dispatcher + the
// per-session resume-cursor hook the harness drives.

import { tokenBoundRunnerId } from '@orca/harness-tunnel';
import type { RunnerConfig } from './config.js';
import { RouteDispatcher } from './tunnel/request-dispatch.js';
import { serveTunnel, type ServeTunnelHandle, type ServeTunnelLogger } from './tunnel/serve.js';

/** This runner's reported version, carried in the tunnel hello. */
const RUNNER_VERSION = '0.1.0';

/** Options for {@link SessionRunner}. */
export interface SessionRunnerOptions {
  /** Resolved runner-wiring config (binding token, registry URL, workspace). */
  readonly config: RunnerConfig;
  /**
   * Providers this runner can serve, advertised in the tunnel hello
   * (capability-advertise: the registry fails a session whose provider the connected
   * runner does not advertise). Defaults to `[]` — a runner that advertises nothing,
   * which the registry's capability-match treats as fail-open.
   */
  readonly providers?: readonly string[];
  /**
   * Resolve the runner's current per-session resume cursors
   * (`sessionId → lastConsumedEventId`) before each (re)connect, so a reconnect
   * advertises the runner's latest consumed ids and the owner pod serves an
   * incremental `after={cursor}` replay. Defaults to none (a fresh runner).
   */
  readonly resumeCursors?: () => Readonly<Record<string, string>>;
  /**
   * Last-activity touch fired for each real server→runner WORK frame (`request`,
   * `request.cancel`, `ws.*`) but NOT `ping`, so tunnel keepalives never keep an
   * otherwise-idle runner alive. The idle-shutdown lifecycle passes a hook that
   * refreshes the runner's activity stamp — so EVERY work frame (including a
   * standalone `request.cancel` that arrives between turns) defers the idle window.
   * Forwarded verbatim to {@link serveTunnel}.
   */
  readonly onActivity?: () => void;
  /** Optional structured logger threaded into the serve loop. */
  readonly logger?: ServeTunnelLogger;
}

/**
 * Owns one session's tunnel connection + the request-dispatch seam.
 *
 * Construction derives the stable, token-bound runner id from the binding token
 * (the same derivation the registry uses to authorize the tunnel) and builds the
 * empty {@link RouteDispatcher} the harness unit registers handlers on.
 * {@link run} dials the registry runner tunnel and serves it until {@link stop}
 * (or a fatal binding refusal).
 */
export class SessionRunner {
  private readonly config: RunnerConfig;
  private readonly providers: readonly string[];
  private readonly resumeCursors: (() => Readonly<Record<string, string>>) | undefined;
  private readonly onActivity: (() => void) | undefined;
  private readonly logger: ServeTunnelLogger | undefined;
  /** Token-bound runner id derived from the binding token (tunnel auth identity). */
  readonly runnerId: string;
  /**
   * The request-dispatch seam the runner serves. The NEXT unit registers the
   * turn / snapshot / replay handlers (and any tunneled-WS attach handlers) on
   * this before (or after) {@link run} — a handler registered while the tunnel is
   * live is picked up on the next pushed request.
   */
  readonly dispatcher: RouteDispatcher;
  /** The live serve handle while running; `undefined` before {@link run}. */
  private serveHandle: ServeTunnelHandle | undefined;

  constructor(options: SessionRunnerOptions) {
    this.config = options.config;
    this.providers = options.providers ?? [];
    this.resumeCursors = options.resumeCursors;
    this.onActivity = options.onActivity;
    this.logger = options.logger;
    this.runnerId = tokenBoundRunnerId(this.config.bindingToken);
    this.dispatcher = new RouteDispatcher();
  }

  /** Registry runner-tunnel base URL this runner will dial. */
  get registryRunnerUrl(): string {
    return this.config.registryRunnerUrl;
  }

  /**
   * Dial the registry runner tunnel and serve it until termination.
   *
   * Starts the serve loop (which reconnects forever with capped backoff) and
   * blocks on it. Resolves when {@link stop} ends the loop; REJECTS when the
   * registry issues a fatal, non-retryable binding refusal (a 403 upgrade or a
   * 4001/4002/4004/4500 close). Idempotent guard: a second concurrent call awaits
   * the same live tunnel rather than dialing twice.
   */
  async run(): Promise<void> {
    if (this.serveHandle !== undefined) {
      await this.serveHandle.done;
      return;
    }
    const handle = serveTunnel({
      dispatcher: this.dispatcher,
      registryRunnerUrl: this.config.registryRunnerUrl,
      runnerId: this.runnerId,
      bindingToken: this.config.bindingToken,
      runnerVersion: RUNNER_VERSION,
      providers: this.providers,
      ...(this.resumeCursors !== undefined ? { resumeCursorsFactory: this.resumeCursors } : {}),
      ...(this.onActivity !== undefined ? { onActivity: this.onActivity } : {}),
      ...(this.logger !== undefined ? { logger: this.logger } : {}),
    });
    this.serveHandle = handle;
    await handle.done;
  }

  /**
   * Stop the tunnel: end the serve loop + reconnects and await its clean exit.
   * A no-op when {@link run} was never called. Idempotent.
   */
  async stop(): Promise<void> {
    const handle = this.serveHandle;
    if (handle === undefined) {
      return;
    }
    await handle.stop();
  }
}
