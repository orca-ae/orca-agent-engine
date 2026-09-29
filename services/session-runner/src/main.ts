// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { registerPiSdkProvider } from './harness/pi-sdk/provider.js';
import { RunnerResources } from './resources.js';
import { registerCodexSdkProvider } from './harness/codex-sdk/provider.js';
// Entry point for @orca/session-runner — the runner-role glue, wired together.
//
// The environment-worker spawns this process once per session (the launch command
// points at `node dist/main.js`). It reads its runner-wiring config from the
// worker-seeded environment (`buildRunnerEnv`), constructs the runner core loop +
// the tunnel client + the provider registry, and runs them:
//
//   1. SessionLoop — the core: it CONSUMES the snapshot the registry pushes,
//      CONSTRUCTS the provider's AgentHarness from it, DRIVES it per user turn
//      (streaming agent events up the tunnel), and APPLIES the pushed recovery
//      replay (tracking the per-session resume cursor). The provider-dispatch
//      registry is the extension seam: all ten providers are registered here (see
//      {@link defaultRegisterProviders}) — the two in-process Claude Agent SDK
//      harnesses, which read their LLM egress from the snapshot and their
//      conversation history from the IN-MEMORY, TUNNEL-FED transcript store; the
//      two native-SDK harnesses (`codex-sdk`, `pi-sdk`); the five native-CLI
//      harnesses; and the first-class `mock` provider (a deterministic, LLM-free
//      harness for CI/e2e). A snapshot for an unregistered provider acks a
//      capability mismatch (422).
//   2. registerSessionHandlers — wires the loop's handlers onto the runner's
//      request-dispatch seam (the ten routes the registry's owner pod drives).
//   3. SessionRunner — dials the registry runner tunnel, says hello (advertising
//      the loop's provider names + the per-session resume cursors), and serves the
//      pushed request/response stream until termination. Its `onActivity` hook
//      refreshes the loop's idle stamp for EVERY work frame (so a standalone
//      `request.cancel` between turns defers the idle window too).
//   4. the inactivity watchdog — requests a graceful shutdown after the idle window
//      with no work and no turn in flight.
//
// SELF-HOSTED-READY: a self-hosted runner is OUTBOUND-WSS-ONLY behind a NAT — it has
// NO direct Kafka/Postgres/transcript backend. Its conversation-history substrate is
// the in-memory {@link InMemoryTranscriptStore} this module constructs: the registry
// PUSHES the recovery replay DOWN the tunnel (the loop's `applyReplay` feeds it into
// the store) and the runner streams NEW agent events UP the tunnel (the registry is
// the single writer of the durable transcript). So the runner builds + boots with NO
// transcript env — there is no `KAFKA_*` wiring at all.
//
// The composition lives in {@link composeRunner} / {@link RunnerProcess.run} (pure
// of process concerns + injectable) so the wiring, the SIGINT/SIGTERM/idle shutdown
// fan-out, and the teardown ordering are unit-testable; `main` only resolves the
// real collaborators (config, the in-memory transcript store, the providers) and
// drives it.

import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import type { TranscriptStore } from '@orca/transcript-store-types';
import { loadConfig, type RunnerConfig } from './config.js';
import { SessionRunner } from './runner.js';
import { SessionLoop, type TranscriptSink } from './session-loop.js';
import { ProviderRegistry } from './harness/provider.js';
import type { NativeCliLogger } from './sandbox/native-cli-launcher.js';
import { registerClaudeProvider } from './harness/claude/provider.js';
import { registerClaudePersistentProvider } from './harness/claude/persistent-provider.js';
import { registerClaudeCodeProvider } from './harness/claude-code/provider.js';
import { registerCodexProvider } from './harness/codex/provider.js';
import { registerCursorProvider } from './harness/cursor/provider.js';
import { registerPiProvider } from './harness/pi/provider.js';
import { registerCustomProvider } from './harness/custom/provider.js';
import { registerMockProvider } from './harness/mock/provider.js';
import { registerSessionHandlers } from './register-handlers.js';
import { registerTerminalAttach } from './tunnel/terminal-attach.js';
import { InMemoryTranscriptStore } from './transcript/in-memory-transcript-store.js';
import {
  createRunnerSandboxRuntime,
  resolveRunnerLocalSandbox,
  SANDBOX_RUNTIME_ENV_VAR,
  type SandboxRuntime,
} from './sandbox/seam.js';
import { runInactivityMonitor } from './idle.js';

/** The runner's local transcript log — both a {@link TranscriptStore} (the SDK history
 * substrate the providers read + write) and a {@link TranscriptSink} (the recovery
 * replay feed). {@link InMemoryTranscriptStore} satisfies both. */
export type RunnerTranscriptStore = TranscriptStore & TranscriptSink;

/** The structured logger the runner process uses. Defaults to `console`. */
export interface RunnerLogger {
  log(...args: unknown[]): void;
  error(...args: unknown[]): void;
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
}

/** Injectable collaborators for {@link composeRunner} (defaults wire the real ones). */
export interface ComposeRunnerDeps {
  /** Resolved runner-wiring config. */
  config: RunnerConfig;
  /**
   * The runner's in-memory, tunnel-fed transcript store — the SDK history substrate
   * the claude providers read + write AND the sink the loop's recovery replay feeds.
   * One instance shared by both so the runner has a single local log. Defaults to a
   * fresh {@link InMemoryTranscriptStore} (no broker). Injectable so a test can pass a
   * recording double; the composition tests that inject `registerProviders` /
   * `createLoop` ignore it entirely.
   */
  transcriptStore?: RunnerTranscriptStore;
  /**
   * Register the providers the runner can serve onto the supplied empty registry.
   * Defaults to {@link defaultRegisterProviders} — all ten real providers, with the
   * two in-process claude ones over the in-memory transcript store. A test injects a
   * seam that registers a fake provider (or none) without standing up the real ones.
   */
  registerProviders?: (registry: ProviderRegistry, config: RunnerConfig) => void;
  /**
   * The runner's per-process sandbox runtime (from `@orca/sandbox-runtime`), handed
   * to the loop so every provider it builds receives it via the provider context and
   * a native-CLI provider can drive `SandboxHandle.spawn`. Defaults to
   * {@link defaultCreateSandboxRuntime} — an {@link InMemorySandboxRuntime} unless
   * `SANDBOX_RUNTIME=local`, in which case it resolves the real srt-wrapped Local
   * runtime (or an explained InMemory fallback when `srt` is absent). The in-process
   * claude/mock providers ignore it, so it stands up nothing external at construction;
   * a test that injects `createLoop` ignores it entirely.
   */
  sandboxRuntime?: SandboxRuntime;
  /** Construct the {@link SessionLoop}. Injectable for the composition tests. */
  createLoop?: (registry: ProviderRegistry, config: RunnerConfig) => SessionLoopLike;
  /** Construct the {@link SessionRunner}. Injectable for the composition tests. */
  createRunner?: (opts: CreateRunnerOptions) => SessionRunnerLike;
  /** Wire the loop's handlers onto the runner's dispatcher. Defaults to the real wiring. */
  registerHandlers?: (runner: SessionRunnerLike, loop: SessionLoopLike) => void;
  /** Run the inactivity monitor. Defaults to {@link runInactivityMonitor}. */
  runInactivityMonitor?: typeof runInactivityMonitor;
  /** Logger. Defaults to `console`. */
  logger?: RunnerLogger;
}

/** The {@link SessionLoop} surface the composition depends on (for test fakes). */
export interface SessionLoopLike {
  providerNames(): string[];
  resumeCursors(): Record<string, string>;
  lastActivity(): number;
  hasActiveWork(): boolean;
  touchActivity(): void;
  stop(
    reason?: 'replica.shutting_down' | 'idle.timeout' | 'client.archived' | 'error',
  ): Promise<void>;
}

/** The {@link SessionRunner} surface the composition depends on (for test fakes). */
export interface SessionRunnerLike {
  readonly runnerId: string;
  readonly registryRunnerUrl: string;
  readonly dispatcher: unknown;
  run(): Promise<void>;
  stop(): Promise<void>;
}

/** Options the composition passes to {@link ComposeRunnerDeps.createRunner}. */
export interface CreateRunnerOptions {
  config: RunnerConfig;
  logger: RunnerLogger;
  providers: string[];
  resumeCursors: () => Record<string, string>;
  /** Last-activity touch fired for every real work frame (defers the idle window). */
  onActivity: () => void;
}

/** The assembled, runnable runner process. */
export interface RunnerProcess {
  /** The wired session loop. */
  readonly loop: SessionLoopLike;
  /** The wired tunnel client. */
  readonly runner: SessionRunnerLike;
  /**
   * Serve until a clean stop or a fatal binding refusal, owning the idle watchdog +
   * the signal/idle shutdown fan-out + the teardown ordering. Installs the
   * SIGINT/SIGTERM handlers when `installSignalHandlers` is set (the real entry
   * does; a test omits it and drives shutdown directly via {@link requestShutdown}).
   */
  run(opts?: { installSignalHandlers?: boolean }): Promise<void>;
  /** Request a graceful shutdown with the given reason (idempotent across calls). */
  requestShutdown(reason: ShutdownReason): void;
}

/** Why a graceful shutdown was requested. */
export type ShutdownReason = NodeJS.Signals | 'idle.timeout';

/**
 * Compose the runner process from its collaborators WITHOUT touching `process`
 * (signals are installed only when {@link RunnerProcess.run} is asked to). Returns
 * the assembled loop + runner + a `run()` that owns the lifecycle, so the whole
 * wiring is unit-testable against injected fakes.
 */
export function composeRunner(deps: ComposeRunnerDeps): RunnerProcess {
  const logger = deps.logger ?? console;
  const config = deps.config;

  // The runner's in-memory, tunnel-fed transcript log — the SDK history substrate the
  // claude providers read + write AND the sink the loop's recovery replay feeds. ONE
  // instance shared by both, so the runner has a single local log and needs no broker.
  const transcriptStore = deps.transcriptStore ?? new InMemoryTranscriptStore();

  // The runner's sandbox runtime (from `@orca/sandbox-runtime`) — the runner's LIVE
  // link to the shared package. Handed to the loop so every provider it builds gets it
  // via the provider context, letting a native-CLI provider `acquire` a sandbox and
  // drive `SandboxHandle.spawn`. Selected by `SANDBOX_RUNTIME` (mirroring the
  // harness-server): unset / `in-memory` → an InMemory runtime (no external dependency;
  // it only `mkdtemp`s on `acquire`); `local` → a real srt-wrapped Local runtime when
  // this host has `srt` (else an explained InMemory fallback — see
  // {@link defaultCreateSandboxRuntime}). The in-process claude/mock providers ignore it.
  const sandboxRuntime = deps.sandboxRuntime ?? defaultCreateSandboxRuntime(config, logger);

  // The provider-dispatch registry — the seam every provider is registered on (see
  // {@link defaultRegisterProviders}). A snapshot for an unregistered provider acks a
  // capability mismatch. The default registration wires the providers over the
  // shared in-memory store; an injected seam ignores the store.
  const registry = new ProviderRegistry();
  if (deps.registerProviders !== undefined) {
    deps.registerProviders(registry, config);
  } else {
    defaultRegisterProviders(registry, config, transcriptStore, logger);
  }

  // The core loop: owns the session's harness lifecycle, the turn loop, event
  // emission, snapshot consume, and recovery apply. The default loop feeds its
  // recovery replay into the shared store (the tunnel feed); an injected seam decides
  // its own wiring.
  const loop =
    deps.createLoop !== undefined
      ? deps.createLoop(registry, config)
      : defaultCreateLoop(registry, config, transcriptStore, sandboxRuntime);

  // The tunnel client. It advertises the loop's provider names + per-session resume
  // cursors (sourced fresh before each connect), and its onActivity hook refreshes
  // the loop's idle stamp for every real work frame.
  const runner = (deps.createRunner ?? defaultCreateRunner)({
    config,
    logger,
    providers: loop.providerNames(),
    resumeCursors: () => loop.resumeCursors(),
    onActivity: () => loop.touchActivity(),
  });

  // Wire the loop's snapshot / turn / replay handlers onto the runner's dispatch
  // seam — the routes the registry's owner pod pushes over the tunnel.
  (deps.registerHandlers ?? defaultRegisterHandlers)(runner, loop);

  // Structured boot line. No secrets: the binding token / egress JWTs are never
  // logged; only the derived runner id, the tunnel URL, the (pinned) session id, and
  // the advertised providers + idle timeout.
  logger.log(
    JSON.stringify({
      msg: 'session-runner starting',
      runnerId: runner.runnerId,
      registryRunnerUrl: runner.registryRunnerUrl,
      sessionId: config.sessionId ?? null,
      providers: loop.providerNames(),
      idleTimeoutS: config.idleTimeoutS,
    }),
  );

  const inactivityMonitor = deps.runInactivityMonitor ?? runInactivityMonitor;

  // Graceful shutdown: stop the serve loop + reconnects AND tear down the loop's
  // harness on SIGINT / SIGTERM / idle so the process exits cleanly instead of
  // leaving a dangling tunnel or a live harness. The `shuttingDown` guard makes the
  // fan-out idempotent (a SIGTERM after a SIGINT, or an idle timeout racing a
  // signal, runs the teardown once).
  const shutdownController = new AbortController();
  let shuttingDown = false;
  const requestShutdown = (reason: ShutdownReason): void => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    logger.log(JSON.stringify({ msg: 'session-runner stopping', signal: reason }));
    shutdownController.abort();
    void runner.stop().catch((err: unknown) => {
      logger.error(err);
    });
    void loop
      .stop(reason === 'idle.timeout' ? 'idle.timeout' : 'replica.shutting_down')
      .catch((err: unknown) => {
        logger.error(err);
      });
  };

  const run = async (opts: { installSignalHandlers?: boolean } = {}): Promise<void> => {
    const onSigint = (): void => requestShutdown('SIGINT');
    const onSigterm = (): void => requestShutdown('SIGTERM');
    if (opts.installSignalHandlers) {
      process.once('SIGINT', onSigint);
      process.once('SIGTERM', onSigterm);
    }

    // The inactivity watchdog: request a graceful shutdown after the idle window
    // with no work and no turn in flight. Disabled when the timeout is 0. Runs
    // alongside the serve loop; it ends when it requests shutdown or the shutdown
    // signal fires.
    const idleMonitor = inactivityMonitor({
      idleTimeoutS: config.idleTimeoutS,
      getLastActivity: () => loop.lastActivity(),
      hasActiveWork: () => loop.hasActiveWork(),
      requestShutdown: () => requestShutdown('idle.timeout'),
      signal: shutdownController.signal,
      logger,
    });

    // Serve until stop() (clean exit) or a fatal, non-retryable binding refusal
    // (a 403 upgrade or a 4001/4002/4004/4500 close), which rejects out of run().
    try {
      await runner.run();
    } finally {
      shutdownController.abort();
      await idleMonitor;
      await loop.stop();
      if (opts.installSignalHandlers) {
        process.removeListener('SIGINT', onSigint);
        process.removeListener('SIGTERM', onSigterm);
      }
    }
  };

  return { loop, runner, run, requestShutdown };
}

/**
 * Register the runner's TEN providers over the shared in-memory transcript store —
 * the exact set {@link ProviderRegistry.providerNames} advertises in the tunnel hello:
 *   - the two IN-PROCESS Claude Agent SDK providers, which share the one transcript
 *     store (`store`) as their conversation-history substrate: the lean `claude`
 *     (A — stateless one-shot `query()` per turn) and `claude-sdk-persistent`
 *     (B — the persistent live session kept alive across turns);
 *   - the two NATIVE-SDK providers, `codex-sdk` and `pi-sdk`, each running its SDK in a
 *     worker process spawned in the session sandbox and resuming from the native
 *     checkpoint the snapshot carries as `harness_state`;
 *   - the five NATIVE-CLI providers, each driving a long-lived child process over its
 *     own stdio dialect through the shared tool-bridge, and each needing no transcript
 *     store because its history lives in that process: `claude-code`, `codex`,
 *     `cursor`, `pi`, and the operator-declared `custom` (whose whole CLI contract
 *     arrives on the snapshot as `custom_spec`, so registering it adds no per-CLI code);
 *   - the first-class `mock` provider (a deterministic, LLM-free harness — selected
 *     by a `provider: "mock"` snapshot; it needs no transcript store or LLM egress,
 *     so the e2e can drive the full plumbing in CI with no API key).
 * The runner advertises all ten names so an agent selects any per its provider
 * config, and the registry's capability-match validates the snapshot's provider against
 * that advertisement. A snapshot for an unregistered provider acks a capability
 * mismatch. The `multiagent` coordinator is NOT an entry here: it WRAPS a
 * registered provider rather than being one.
 *
 * The persistent provider additionally receives the optional per-turn model allow-list
 * (`config.provider.allowedModels`, from `ANTHROPIC_ALLOWED_MODELS`) — the defense-in-
 * depth model knob. When the operator did not set it, it is omitted and the harness
 * admits a well-formed per-turn override on format alone (the gateway remains the
 * model-policy enforcement point); when set, the harness additionally constrains
 * per-turn overrides to the allow-list. The lean provider does not take per-turn model
 * overrides, so the allow-list applies only to the persistent provider.
 *
 * Exported for the composition test (it asserts the real allow-list plumbing reaches
 * the persistent harness). The in-memory store it is given holds no network, so
 * registering the providers stands up nothing external.
 *
 * @param store The shared in-memory transcript store (the SDK history substrate). The
 *   loop's recovery replay feeds the SAME instance, so the local log is single-sourced.
 * @param logger Handed to the three native-CLI providers whose approval protocols carry a
 *   bare deny VALUE and no message — `codex`, `pi`, `custom`. A faulted approval gate (a
 *   permission-store outage denying every tool call in the session) has no other channel
 *   there, so without this the reason would reach nothing at all. Optional so an existing
 *   caller keeps compiling; a runner that omits it simply logs nothing.
 */
export function defaultRegisterProviders(
  registry: ProviderRegistry,
  config: RunnerConfig,
  store: TranscriptStore,
  logger?: NativeCliLogger,
): void {
  // Spread the logger in only when one was supplied — under `exactOptionalPropertyTypes`
  // an explicit `undefined` is not assignable to the providers' `logger?`.
  const gateLogger = logger !== undefined ? { logger } : {};
  const fallbackApiKey =
    config.provider.fallbackApiKey !== undefined
      ? { fallbackApiKey: config.provider.fallbackApiKey }
      : {};
  // Spread the allow-list in only when the operator configured one — under
  // `exactOptionalPropertyTypes` an explicit `undefined` is not assignable to the
  // provider's `allowedModels?: readonly string[]`.
  const allowedModels =
    config.provider.allowedModels !== undefined
      ? { allowedModels: config.provider.allowedModels }
      : {};
  registerClaudeProvider(registry, {
    store,
    modelDefault: config.provider.modelDefault,
    ...fallbackApiKey,
  });
  registerClaudePersistentProvider(registry, {
    store,
    modelDefault: config.provider.modelDefault,
    ...fallbackApiKey,
    ...allowedModels,
  });
  // The `claude-code` native-CLI provider (D): boots the headless `claude` binary as a
  // long-lived child and drives it over stream-json, wiring the native-CLI tool-bridge as
  // its MCP server. It needs no transcript store (its history lives in the CLI process)
  // and reads its LLM egress from the snapshot; it takes the same default model + optional
  // fallback credential. Registering it advertises `claude-code` in the runner hello so an
  // agent can select the native-CLI harness by its provider config.
  registerClaudeCodeProvider(registry, {
    modelDefault: config.provider.modelDefault,
    ...fallbackApiKey,
  });
  // The two native-SDK providers, `pi-sdk` and `codex-sdk`: each runs its SDK in a worker
  // process spawned in the session sandbox and resumes from the snapshot's native checkpoint.
  // They read LLM egress from the snapshot and fall back to the configured provider
  // credentials when it carries none.
  registerPiSdkProvider(registry, {
    ...(config.provider.piProviderCredentials
      ? { credentials: config.provider.piProviderCredentials }
      : {}),
  });
  registerCodexSdkProvider(registry, {
    ...(config.provider.openaiApiKey ? { apiKey: config.provider.openaiApiKey } : {}),
  });
  // The `codex` native-CLI provider (D): boots `codex app-server` as a long-lived child and
  // drives it over JSON-RPC-over-stdio, wiring the native-CLI tool-bridge as its `orca` MCP
  // server and routing codex's `on-request` approvals to the uniform transcript gate. Like
  // claude-code it needs no transcript store (its history lives in the app-server) and reads
  // its LLM egress from the snapshot; it takes the same default model + optional fallback
  // credential. Registering it advertises `codex` in the runner hello so an agent can select
  // the codex native-CLI harness by its provider config.
  registerCodexProvider(registry, {
    modelDefault: config.provider.modelDefault,
    ...fallbackApiKey,
    ...gateLogger,
  });
  // The `cursor` native-CLI provider (D): boots the headless `cursor-agent` binary as a
  // long-lived child and drives it over its stream-json stdio, wiring the native-CLI tool-bridge
  // as its `orca` MCP server and routing cursor's tool-permission requests to the uniform
  // transcript gate. Like the other native-CLI providers it needs no transcript store (its
  // history lives in the cursor-agent process) and reads its scoped egress from the snapshot; it
  // takes the same default model + optional fallback credential. Registering it advertises
  // `cursor` in the runner hello so an agent can select the cursor native-CLI harness by its
  // provider config.
  registerCursorProvider(registry, {
    modelDefault: config.provider.modelDefault,
    ...fallbackApiKey,
  });
  // The `pi` native-CLI provider (D): boots `pi --mode rpc` as a long-lived child and drives it
  // over pi's newline-delimited JSON command/event protocol, wiring the native-CLI tool-bridge as
  // a pi extension and routing pi's tool executions to the uniform transcript gate. Like the other
  // native-CLI providers it needs no transcript store (its history lives in the pi process) and
  // reads its LLM egress from the snapshot; it takes the same default model + optional fallback
  // credential. Registering it advertises `pi` in the runner hello so an agent can select the pi
  // native-CLI harness by its provider config.
  registerPiProvider(registry, {
    modelDefault: config.provider.modelDefault,
    ...fallbackApiKey,
    ...gateLogger,
  });
  // The generic `custom` native-CLI provider (D): boots an OPERATOR-DECLARED CLI as a long-lived
  // child (per the declarative `custom_spec` block on the snapshot) and drives it over its
  // stdin/stdout per the spec's mapping, wiring the native-CLI tool-bridge as its `orca` MCP server
  // and routing a spec-declared approval request to the uniform transcript gate. Like the other
  // native-CLI providers it needs no transcript store (its history lives in the CLI process) and
  // reads its LLM egress from the snapshot; it takes the same default model + optional fallback
  // credential. Registering it advertises `custom` in the runner hello so an operator can register
  // ANY CLI agent by dropping the spec on the snapshot — no runner change.
  registerCustomProvider(registry, {
    modelDefault: config.provider.modelDefault,
    ...fallbackApiKey,
    ...gateLogger,
  });
  // The mock provider needs no boot collaborators (no store, no LLM egress).
  registerMockProvider(registry);
}

/**
 * Construct the real {@link SessionLoop}, feeding its recovery replay into the shared
 * in-memory transcript store (the tunnel feed) so the runner's local log mirrors the
 * registry's recovery replay, and handing it the runner's sandbox runtime so every
 * provider it builds receives it via the provider context (a native-CLI provider then
 * drives `SandboxHandle.spawn`).
 */
function defaultCreateLoop(
  registry: ProviderRegistry,
  config: RunnerConfig,
  transcriptSink: TranscriptSink,
  sandboxRuntime: SandboxRuntime,
): SessionLoopLike {
  return new SessionLoop({
    workspaceId: config.workspaceId,
    providers: registry,
    transcriptSink,
    sandboxRuntime,
    // The runner materializes the owner-pod-pushed Skill bundles as a `--plugin-dir`
    // plugin under its host workspace root (`config.workspace`).
    skillsWorkspaceDir: config.workspace,
    resources: new RunnerResources({ workspaceDir: config.workspace }),
    logger: console,
  });
}

/**
 * Construct the runner's sandbox runtime from the resolved config + `SANDBOX_RUNTIME`.
 *
 * Mirrors the harness-server's selection: unset / `in-memory` → InMemory; `local` →
 * a real srt-wrapped {@link LocalSandboxRuntime}. For the `local` case this resolves the
 * concrete wiring the Local runtime needs — the `srt` probe + `SandboxManager` + the
 * runner work-dir (`config.workspace`) + the network allow-list — via
 * {@link resolveRunnerLocalSandbox}. When that host has no `srt`, resolution returns
 * `undefined` (having logged the reason) and {@link createRunnerSandboxRuntime} degrades
 * the `local` request to InMemory, so the runner never boot-fails over a `local` knob it
 * cannot honor on this host. This is what closes the previous gap where a
 * `SANDBOX_RUNTIME=local` runner ALWAYS ran InMemory because no `local` wiring was ever
 * supplied here.
 */
function defaultCreateSandboxRuntime(config: RunnerConfig, logger: RunnerLogger): SandboxRuntime {
  const kind = (process.env[SANDBOX_RUNTIME_ENV_VAR] ?? '').trim().toLowerCase();
  if (kind !== 'local') {
    return createRunnerSandboxRuntime({ kind });
  }
  const local = resolveRunnerLocalSandbox({ harnessWorkDir: config.workspace, logger });
  // Spread `local` in only when resolved — an absent value lets `createRunnerSandboxRuntime`
  // fall back to InMemory (the resolver already logged why).
  return createRunnerSandboxRuntime({ kind, ...(local !== undefined ? { local } : {}) });
}

/** Construct the real {@link SessionRunner}. */
function defaultCreateRunner(opts: CreateRunnerOptions): SessionRunnerLike {
  return new SessionRunner({
    config: opts.config,
    logger: console,
    providers: opts.providers,
    resumeCursors: opts.resumeCursors,
    onActivity: opts.onActivity,
  });
}

/** Wire the loop's handlers onto the real runner's dispatcher. */
function defaultRegisterHandlers(runner: SessionRunnerLike, loop: SessionLoopLike): void {
  // The real runner exposes a RouteDispatcher; the real loop is a SessionLoop. The
  // composition's structural types widen them for test fakes, so narrow back here.
  const dispatcher = (runner as SessionRunner).dispatcher;
  const realLoop = loop as SessionLoop;
  registerSessionHandlers(dispatcher, realLoop, { logger: console });
  // The remote terminal-attach WS handler resolves the host per attach from the
  // loop's LIVE per-session sandbox (rebuilt on each snapshot-apply), so an attach
  // always bridges the current terminal-capable sandbox and never a stale one.
  registerTerminalAttach(dispatcher, () => realLoop.currentTerminalHost(), { logger: console });
}

async function main(): Promise<void> {
  const config = loadConfig();
  const runnerProcess = composeRunner({ config });
  await runnerProcess.run({ installSignalHandlers: true });
}

// Only run when invoked as the entry module (`node dist/main.js`), not when this
// module is imported by a test (which drives `composeRunner` directly).
if (isMainModule()) {
  main().catch((err: unknown) => {
    console.error(err);
    process.exitCode = 1;
  });
}

/**
 * Whether this module is the process entry point (vs imported by a test). Compares
 * the resolved entry script (`process.argv[1]`) against this module's own file path
 * derived from `import.meta.url` — true only when the process was launched as this
 * file, so importing the module (vitest) never triggers {@link main}.
 */
function isMainModule(): boolean {
  if (typeof process === 'undefined' || !Array.isArray(process.argv)) {
    return false;
  }
  const entry = process.argv[1];
  if (entry === undefined) {
    return false;
  }
  const selfPath = fileURLToPath(import.meta.url);
  return resolve(entry) === selfPath;
}
