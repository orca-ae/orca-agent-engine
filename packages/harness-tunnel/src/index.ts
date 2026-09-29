// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Barrel for @orca/harness-tunnel.
//
// This package is the home of two framed-WebSocket tunnel implementations that
// share its low-level primitives:
//   - The RUNNER tunnel (`frames.ts` + `transport.ts`): HTTP-over-WS
//     request/response proxying. `TunnelTransport`/`TransportRegistry` are the
//     seams; the connection-lifecycle engine (hello handshake, sender/receive/
//     ping loops, teardown) lives in the consumer — the registry-service route
//     adapter that a follow-up PR adds — not in this package.
//   - The WORKER tunnel (`worker-frames.ts` + `worker-tunnel.ts`): control-frame
//     RPC (launch/stop a runner, filesystem ops). `WorkerTunnelServer` is a
//     fully self-contained, network-free engine that owns its own
//     connection-lifecycle (the same hello/version-skew/sender/receive/ping/
//     teardown shape as the runner tunnel, just packaged as a reusable class).
// The two are NOT unified under one transport type — their frame-reassembly
// shapes genuinely differ (single-stream head/body/end vs. seven parallel
// pending-request maps keyed by request_id) — but both build on this package's
// shared `AsyncQueue`/`Deferred` async primitives (`transport.ts`) and the
// shared handshake/identity constants (`identity.ts`), and both follow the same
// "seam-driven, network-free engine" shape so each is unit-testable without a
// socket. See `docs/managed-agents/libraries/harness-tunnel.md` for the design
// (routes + auth posture); the registry-service wiring lands in a follow-up PR.
//
// Use EXPLICIT named re-exports (not `export *`) to avoid name collisions across
// frames / transport / identity.

export {
  FrameKind,
  TUNNEL_MAX_MESSAGE_BYTES,
  decodeBody,
  decodeFrame,
  encodeBody,
  encodeFrame,
  isTextContentType,
} from './frames.js';
export {
  HARNESS_NOT_CONFIGURED_ERROR_CODE,
  WorkerFrameKind,
  decodeWorkerFrame,
  encodeWorkerFrame,
} from './worker-frames.js';
export type {
  WorkerCreateDirFrame,
  WorkerCreateDirResultFrame,
  WorkerCreateWorktreeFrame,
  WorkerCreateWorktreeResultFrame,
  WorkerFrame,
  WorkerFsEntryType,
  WorkerHelloFrame,
  WorkerLaunchStatus,
  WorkerOpStatus,
  WorkerStopStatus,
  WorkerLaunchRunnerFrame,
  WorkerLaunchRunnerResultFrame,
  WorkerListDirEntry,
  WorkerListDirFrame,
  WorkerListDirResultFrame,
  WorkerRemoveWorktreeFrame,
  WorkerRemoveWorktreeResultFrame,
  WorkerRunnerExitedFrame,
  WorkerStatFrame,
  WorkerStatResultFrame,
  WorkerStopRunnerFrame,
  WorkerStopRunnerResultFrame,
} from './worker-frames.js';
export {
  ConnectError,
  Deferred,
  AsyncQueue,
  BoundedResponseBodyQueue,
  ResponseBufferOverflowError,
  TunnelRequestAbortedError,
  TUNNEL_MAX_BUFFERED_RESPONSE_BYTES,
  pushResponseBody,
  TunnelTransport,
  TunneledByteStream,
} from './transport.js';
export {
  WorkerTunnelServer,
  WorkerTunnelTimeoutError,
  WorkerTunnelClosedError,
  ON_RUNNER_EXITED_TIMEOUT_MS,
  SUPPORTED_FRAME_PROTOCOL_MAJOR,
  PING_INTERVAL_MS,
  PING_MISS_THRESHOLD,
  ON_WORKER_CONNECT_TIMEOUT_MS,
  LOCAL_TUNNEL_OWNER,
  EXPECTED_HELLO_CLOSE_CODE,
  VERSION_MISMATCH_CLOSE_CODE,
  PING_TIMEOUT_CLOSE_CODE,
  UNAUTHENTICATED_CLOSE_CODE,
} from './worker-tunnel.js';
export type {
  WorkerSocketMessage,
  WorkerWebSocket,
  WorkerHandshake,
  WorkerAuthProvider,
  ResolvedLaunchToken,
  WorkerStore,
  WorkerUpsertOnConnect,
  WorkerConnection,
  WorkerRegistry,
  WorkerPendingRequest,
  WorkerLaunchResult,
  WorkerStopResult,
  WorkerStatResult,
  WorkerListDirResult,
  WorkerCreateWorktreeResult,
  WorkerRemoveWorktreeResult,
  WorkerCreateDirResult,
  RunnerExitSink,
  WorkerRunnerExitContext,
  WorkerTunnelLogger,
  WorkerTunnelOptions,
} from './worker-tunnel.js';
export type {
  RequestState,
  TunnelSession,
  TransportRegistry,
  TunnelRequest,
  TunnelResponse,
  WebSocketLike,
} from './transport.js';
export type {
  BodyEncoding,
  Frame,
  HeaderPair,
  HelloFrame,
  PingFrame,
  PongFrame,
  RequestCancelFrame,
  RequestFrame,
  ResponseBodyFrame,
  ResponseEndFrame,
  ResponseHeadFrame,
  WsCloseFrame,
  WsFrame,
  WsOpenFrame,
} from './frames.js';
export {
  CONFIG_PATH,
  HOST_ID_ENV_VAR,
  HOST_NAME_ENV_VAR,
  HOST_TOKEN_ENV_VAR,
  INTERNAL_WS_ORIGIN,
  HOST_TUNNEL_TOKEN_HEADER,
  PEER_ID_ENV_VAR,
  RUNNER_ADOPT_SIGNAL,
  RUNNER_AUTH_SECRET_ENV_VARS,
  RUNNER_ISOLATE_SESSION_ENV_VAR,
  RUNNER_PARENT_PID_ENV_VAR,
  RUNNER_REGISTRY_URL_ENV_VAR,
  RUNNER_TERMINAL_ATTACH_PATH,
  RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR,
  RUNNER_TUNNEL_TOKEN_HEADER,
  RUNNER_WORKSPACE_ENV_VAR,
  getStableRunnerId,
  loadOrCreateHostIdentity,
  loadOrCreateRunnerId,
  stripRunnerAuthSecrets,
  tokenBoundRunnerId,
} from './identity.js';
export type { HostIdentity } from './identity.js';

export { HARNESS_CHECKPOINT_EVENT } from './identity.js';
