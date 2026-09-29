// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// index.ts — public package entry for @orca/sandbox-harness (the tsup `main`).
//
// Two jobs, and ONLY these two — everything substantive lives in the sibling
// modules this file re-exports:
//
//   1. Aggregate the library surface. Consumers (the tests, any embedder) import
//      from the package entry rather than reaching into individual files. This
//      module is the tsup entry that emits the public `.d.ts`;
//      `subprocess-entry.ts` is a SECOND, independent entry (the per-session
//      runner) and is intentionally NOT re-exported here.
//
//   2. Make `createState` callable with no arguments. The underlying
//      `server.ts#createState` requires an absolute `subprocessEntryPath`; here we
//      compute the default — the BUILT sibling `subprocess-entry.js` — so the
//      common caller never has to. The path is resolved from `import.meta.url`
//      (ESM-correct; `__dirname` does not exist under `"type": "module"`), so it
//      points at `dist/subprocess-entry.js` at runtime alongside `dist/index.js`.
//
// Importing this module never opens a socket. The guarded run-as-main bootstrap
// at the very bottom is what `node dist/index.js` runs — the image entrypoint
// (`docker-entrypoint.sh`) starts the server that way — and it reads `PORT`
// (default 4096) only in that path.
//
// NOTE ON RE-EXPORT STRATEGY: several sibling modules deliberately reuse type
// names (`BareEvent`, `WireFrame`, `ContentBlock`, the `agent.*` event shapes,
// the stream-json frame shapes, …) because each owns its own view of the wire.
// A flat `export *` would therefore collide. We resolve that two ways:
//   - a CURATED FLAT surface of the headline, unambiguous names (the factories,
//     the store/session-manager/registry entry points, the provider interface's
//     non-colliding types) for ergonomic top-level imports; plus
//   - NAMESPACE re-exports (`core`, `store`, `server`, `sessionManager`,
//     `providerTypes`, `providerRegistry`) so the COMPLETE surface of every
//     module stays reachable with zero name clashes — reach a colliding member
//     via its namespace, e.g. `providerTypes.WireFrame` vs `sessionManager.WireFrame`.

import { fileURLToPath, pathToFileURL } from 'node:url';

import { createApp, createState as createStateWithEntry } from './server.js';
import type { CreateStateOptions, HarnessContext } from './server.js';

// ── default subprocess entry path ─────────────────────────────────────────────

/**
 * Absolute path to the BUILT per-session subprocess entry (`subprocess-entry.js`),
 * resolved relative to THIS module at runtime.
 *
 * tsup emits `index.ts` and `subprocess-entry.ts` as sibling files in `dist/`, so
 * `new URL('./subprocess-entry.js', import.meta.url)` resolves to
 * `dist/subprocess-entry.js` next to `dist/index.js`. We go through `import.meta.url`
 * (not `__dirname`, which is undefined in ESM) so the path is correct no matter the
 * cwd of the embedding process. `createState` below defaults to this so callers do
 * not have to know where the runner lives.
 */
export const defaultSubprocessEntryPath: string = fileURLToPath(
  new URL('./subprocess-entry.js', import.meta.url),
);

// ── createState (default-filling wrapper) ─────────────────────────────────────

/**
 * Options for the package-level {@link createState}. Both fields are OPTIONAL
 * here (unlike `server.ts#createState`, where they are required): an omitted
 * `subprocessEntryPath` defaults to {@link defaultSubprocessEntryPath} and an
 * omitted `env` to `process.env`. Pass either to override (e.g. tests pointing at
 * a fake runner, or an embedder injecting a scoped env with the LLM gateway
 * endpoint/key).
 */
export type CreateStateOverrides = Partial<CreateStateOptions>;

/**
 * Build the shared {@link HarnessContext} every route handler receives.
 *
 * Thin wrapper over `server.ts#createState` that fills in the two things a typical
 * caller should not have to: the subprocess entry path (defaults to the built
 * sibling runner) and `env` (defaults to `process.env`). Override either via the
 * single optional argument. All session/event wiring — including the status-sync
 * `emit` ordering — lives in the wrapped function; nothing is duplicated here.
 */
export function createState(overrides: CreateStateOverrides = {}): HarnessContext {
  return createStateWithEntry({
    subprocessEntryPath: overrides.subprocessEntryPath ?? defaultSubprocessEntryPath,
    env: overrides.env ?? process.env,
  });
}

// ── HTTP server + state (curated flat surface) ────────────────────────────────

export { createApp };
export type {
  HarnessContext,
  HarnessRequest,
  RouteHandler,
  RouteParams,
  CreateStateOptions,
} from './server.js';

// ── store factories + records (curated flat surface) ──────────────────────────
// `store.ts` also re-declares `BareEvent` and the `agent.*` event shapes for its
// own view of the wire; those collide with `core.ts`, so we surface only the
// store-OWNED types here (records, handles, listeners) and let `core` own the
// event factories + event types below. The full store surface is also available
// via the `store` namespace at the bottom of this file.

export { createSessionStore, createEventStore } from './store.js';
export type {
  Session,
  SessionStatus,
  SessionStore,
  EventStore,
  CreateSessionInput,
  StoredEvent,
  EventEnvelope,
  EventListener,
  Unsubscribe,
} from './store.js';

// ── core: event factories + canonical event/content types ─────────────────────
// `core.ts` is the canonical home for the bare-event factories and their result
// types; the rest of the package constructs events through these.

export {
  genId,
  nowIso,
  normalizeContent,
  userMessageEvent,
  agentMessageEvent,
  agentToolUseEvent,
  agentToolResultEvent,
  agentCustomToolUseEvent,
  sessionIdleEvent,
  sessionErrorEvent,
  HttpError,
  sendJson,
  sendError,
} from './core.js';
export type {
  TextBlock,
  ImageBlock,
  UnknownBlock,
  ContentBlock,
  ToolResultContent,
  MessageContent,
  Usage,
  UserMessageEvent,
  AgentMessageEvent,
  AgentToolUseEvent,
  AgentToolResultEvent,
  AgentCustomToolUseEvent,
  SessionIdleEvent,
  SessionErrorEvent,
  BareEvent,
  EventType,
  EventStamp,
  Stamped,
  Event,
  AgentToolUseInput,
  AgentToolResultInput,
  AgentCustomToolUseInput,
  SessionIdleInput,
} from './core.js';

// ── session manager: harness resolution, frame translation, subprocess wrapper ─
// The headline entry points, plus the resume seam and the manager-owned
// handle/option types.
// `WireFrame` and the `agent.*` event shapes here are the manager's own view and
// collide with `core` / `providerTypes`; reach those via the `sessionManager`
// namespace if needed.

export {
  resolveHarness,
  translateFrame,
  createManagedSession,
  encodeReplayEnv,
  decodeReplayEnv,
  encodeCustomToolsEnv,
  decodeCustomToolsEnv,
  REPLAY_ENV_VAR,
  CUSTOM_TOOLS_ENV_VAR,
  RUNTIME_TOOLS_ENV_VAR,
} from './session-manager.js';
export type {
  SpawnArgs,
  ManagedSession,
  CreateManagedSessionInput,
  UserMessageContent,
  ReplayEntry,
} from './session-manager.js';

// ── providers: interface (providers/types) + registry (providers/registry) ─────
// The provider interface is the extension seam for harness providers. We flat-
// export its headline, non-colliding names; the frame/usage/permission/content
// types it shares with `core` and `sessionManager` are reachable via the
// `providerTypes` namespace. The registry's `resolveProvider` / `listProviderMetadata`
// (plus `ProviderMetadata`) are flat-exported too.

export type {
  Provider,
  Runtime,
  CreateRuntimeArgs,
  RunTurnArgs,
  SessionSnapshot,
  HistoryEntry,
  TurnContent,
  CustomToolDefinition,
  CustomToolResultPayload,
  Env,
  Diagnostics,
  WireMessage,
  StreamEvent,
  McpServer,
} from './providers/types.js';

export { resolveProvider, listProviderMetadata } from './providers/registry.js';
export type { ProviderMetadata } from './providers/registry.js';

// ── namespace re-exports (complete, collision-free surface) ────────────────────
// Every module's FULL surface, reachable without name clashes. Use these to get
// at members the curated flat surface intentionally aliased away to avoid a
// collision, e.g. `providerTypes.WireFrame` vs `sessionManager.WireFrame`, or
// `store.BareEvent` vs `core.BareEvent`.

export * as core from './core.js';
export * as store from './store.js';
export * as server from './server.js';
export * as sessionManager from './session-manager.js';
export * as providerTypes from './providers/types.js';
export * as providerRegistry from './providers/registry.js';

// ── run-as-main dev bootstrap (guarded; NOT executed on import) ────────────────

/**
 * Default port for the `node dist/index.js` bootstrap below (the image entrypoint's
 * command). Only consulted in the run-as-main path; embedding the server as a
 * library never touches `PORT`.
 */
export const DEFAULT_DEV_PORT = 4096;

/**
 * Stand up the HTTP server for `node dist/index.js` — the image entrypoint's
 * command, or a local smoke run. NOT called on import — only from the run-as-main
 * guard. Reads `PORT` (default {@link DEFAULT_DEV_PORT}); state uses
 * {@link defaultSubprocessEntryPath} and `process.env`. Keeping the listen call
 * here keeps it out of the library path.
 */
function runDevServer(): void {
  const ctx = createState();
  const port = Number(process.env.PORT) || DEFAULT_DEV_PORT;
  createApp(ctx).listen(port, () => {
    console.log(`@orca/sandbox-harness dev server on http://localhost:${port}`);
  });
}

// Run only when invoked directly (`node dist/index.js`), never when imported as a
// library: compare this module's URL to argv[1].
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  runDevServer();
}
