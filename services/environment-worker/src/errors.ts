// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Worker connection errors.

/**
 * A non-retryable failure while opening the registry host tunnel.
 *
 * Raised when the WebSocket upgrade fails in a way reconnecting can never fix:
 * the upgrade was rejected with a permanent `4xx` other than `408`/`429`, such
 * as the `404` of a registry that predates the `/v1/tunnels/environments` route.
 * The reconnect loop re-raises this instead of backing off, so the worker
 * process exits with an actionable message rather than looping silently forever.
 *
 * A refused credential is not this error. The registry accepts the upgrade and
 * refuses a missing, wrong or expired Env Key or Environment Token, or an unknown
 * or archived Environment, by closing the socket with `4004`; that close is an
 * ordinary disconnect, which the reconnect loop retries with backoff.
 *
 * The message is the full, user-facing explanation including the suggested fix;
 * it is printed verbatim by the worker entry point.
 *
 * Keyed purely on the rejected-upgrade HTTP status: the registry endpoint
 * authenticates the Env Key on a dedicated header with no OAuth proxy in front,
 * so there is no login-redirect failure mode to special-case.
 */
export class EnvironmentConnectError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EnvironmentConnectError';
  }
}
