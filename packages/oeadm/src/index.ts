// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Public surface of `@orca/oeadm`. The package ships primarily as the `oeadm`
// binary (`src/main.ts`), but the client + loop pieces are exported so they can
// be reused or driven programmatically (and so the build emits types for them).

export {
  OrcaClient,
  resolveClientConfig,
  authHeaders,
  type OrcaFetch,
  type ClientConfig,
  type CreateEnvironmentInput,
  type EnvironmentResponse,
  type CreateSessionInput,
  type SessionResponse,
  type StreamOptions,
} from './client.js';
export { SseFrameParser, parseSseStream, type SseFrame } from './sse.js';
export { renderFrame } from './render.js';
export { ansiColor, noColor, paletteFor, type Palette } from './colors.js';
export { parseArgs, requireString, optionalString, type ParsedArgs } from './args.js';
export {
  runSessionChat,
  type ChatIo,
  type ChatClient,
  type SessionChatOptions,
} from './session-chat.js';
export { createTerminalIo, type TerminalStreams } from './terminal-io.js';
export {
  attachTerminal,
  terminalAttachUrl,
  type AttachTerminalOptions,
  type WebSocketCtor,
  type MinimalWebSocket,
} from './terminal-attach.js';
export { runCommand, type RunCommandOptions } from './commands/run.js';
export { attachCommand, type AttachCommandOptions } from './commands/attach.js';
export {
  envCommand,
  envCreateCommand,
  type EnvCommandOptions,
  type EnvCreateCommandOptions,
} from './commands/env.js';
export {
  workerCommand,
  resolveWorkerEnv,
  type WorkerEnv,
  type WorkerCommandOptions,
  type SpawnWorker,
} from './commands/worker.js';
export { dispatch, main, type Dispatchers } from './main.js';
