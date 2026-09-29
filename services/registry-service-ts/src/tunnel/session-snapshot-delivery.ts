// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Owner-pod snapshot delivery — pushes the credential-free agent snapshot to a
// runner over the runner tunnel at session start.
//
// At a runner (re)connect the owner pod (the replica holding the session's
// environment claim) composes the snapshot (model + provider + composed system +
// tool allowlists + rewritten MCP servers / egress spec) and hands it to the
// runner BEFORE the first turn is driven, so the runner spins up the right
// harness/provider and reaches credentialed upstreams the right way. Delivery is
// a sibling of {@link SessionRecovery}: a single streaming POST to the runner's
// snapshot route over the SAME tunnel the bridge drives turns on. The runner
// applies the snapshot and acks 200.
//
// The snapshot is composed by a {@link SnapshotProvider} seam (the
// `AgentSnapshotResolver` in production; a fake in tests) so delivery owns only
// the tunnel push, not the record resolution. A provider that returns `null`
// (a deleted / cross-workspace session) means "nothing to deliver" — delivery is
// skipped, not failed. A provider that THROWS (a snapshot that could not be
// built — e.g. a missing skill_version) is a real fault and propagates, so the
// caller can decide; a transport-level failure (offline / non-2xx / mid-push
// drop) is contained + reported, because it self-heals on the next reconnect.
//
// Reused, not re-implemented: the shared {@link TunnelRegistry} +
// {@link TunnelTransport} for the runner tunnel (the same transport recovery +
// the bridge use), and the session-header constant single-sourced from the event
// bridge. Delivery owns no sockets and no DB, so it is fully unit-testable
// in-process against a fake runner (a ws peer speaking the tunnel protocol).

import { ConnectError, type TunnelResponse, TunnelTransport } from '@orca/harness-tunnel';
import type { AgentSnapshot } from '../domain/agent-snapshot.js';
import { assertSnapshotCredentialFree } from '../domain/egress-credential-free.js';
import { snapshotDeliveredTotal } from '../metrics.js';
import { RUNNER_SESSION_HEADER } from './session-event-bridge.js';
import type { TransportRegistry } from '@orca/harness-tunnel';

// Re-export the session header from its single source (the event bridge) so a
// delivery consumer + the runner side match the exact same on-the-wire name.
export { RUNNER_SESSION_HEADER };

/**
 * Runner route the owner pod PUSHES the credential-free agent snapshot to. The
 * runner serves this locally (its tunnel adapter dispatches the framed request),
 * applies the snapshot (model / provider / tools / egress), and acks 200. Orca-
 * native path; the cross-component contract with the runner's snapshot handler —
 * distinct from the bridge's turn route and recovery's replay route so a runner
 * routes a snapshot, a replay, and a turn independently.
 */
export const RUNNER_SNAPSHOT_PATH = '/v1/runner/snapshot';

/** Structured logger seam (a subset of the usual `req.log`). All optional. */
export interface SnapshotDeliveryLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/**
 * Composes the snapshot for one (workspace, session). Returns `null` when there
 * is nothing to deliver (a deleted / cross-workspace session); THROWS when a
 * snapshot was expected but could not be built (a missing skill_version, etc.).
 */
export interface SnapshotProvider {
  resolve(sessionId: string): Promise<AgentSnapshot | null>;
}

/** Options for {@link SessionSnapshotDelivery}. */
export interface SessionSnapshotDeliveryOptions {
  /** Owning workspace (the tenant scope) for the tunnel owner. */
  workspaceId: string;
  /** Session id this delivery serves, e.g. `"ses_a1b2..."`. */
  sessionId: string;
  /**
   * Runner id the session is bound to — the tunnel the owner pod pushes the
   * snapshot over. The transport resolves the live tunnel through the registry,
   * so a runner reconnect (newest-wins) is picked up transparently.
   */
  runnerId: string;
  /** Shared runner-tunnel registry the {@link TunnelTransport} routes through. */
  registry: TransportRegistry;
  /** Composes the snapshot for the session (the resolver in production). */
  provider: SnapshotProvider;
  /** Optional structured logger. */
  logger?: SnapshotDeliveryLogger;
}

/** The outcome of a single {@link SessionSnapshotDelivery.deliver}. */
export interface SnapshotDeliveryOutcome {
  /** `true` when the provider returned `null` — nothing to deliver (not an error). */
  skipped: boolean;
  /** `true` when the runner accepted the snapshot (a 2xx ack, fully drained). */
  delivered: boolean;
}

/**
 * The owner-pod snapshot delivery for one self-hosted session.
 *
 * Construct one per (owner pod, session) and call {@link deliver} on a runner
 * (re)connect, before recovery + the bridge. It composes the snapshot via the
 * provider and pushes it down the tunnel as a single streaming POST.
 */
export class SessionSnapshotDelivery {
  private readonly workspaceId: string;
  private readonly sessionId: string;
  private readonly runnerId: string;
  private readonly transport: TunnelTransport;
  private readonly provider: SnapshotProvider;
  private readonly logger: SnapshotDeliveryLogger | undefined;

  constructor(opts: SessionSnapshotDeliveryOptions) {
    this.workspaceId = opts.workspaceId;
    this.sessionId = opts.sessionId;
    this.runnerId = opts.runnerId;
    this.transport = new TunnelTransport(opts.registry, opts.runnerId);
    this.provider = opts.provider;
    this.logger = opts.logger;
  }

  /**
   * Compose + push the snapshot to the runner.
   *
   *   - provider returns `null` → `{ skipped: true, delivered: false }` (nothing
   *     to deliver; not an error);
   *   - provider throws → propagates (a snapshot that could not be built is a
   *     fault the caller decides on);
   *   - push offline / non-2xx / mid-push drop → contained + logged, returns
   *     `{ skipped: false, delivered: false }` (self-heals on the next reconnect);
   *   - 2xx ack drained → `{ skipped: false, delivered: true }`.
   */
  async deliver(): Promise<SnapshotDeliveryOutcome> {
    const snapshot = await this.provider.resolve(this.sessionId);
    if (snapshot === null) {
      this.logger?.info?.(
        { sessionId: this.sessionId, runnerId: this.runnerId },
        'snapshot delivery skipped (no snapshot for session)',
      );
      return { skipped: true, delivered: false };
    }

    // Defense-in-depth: structurally assert the snapshot is credential-free
    // BEFORE it leaves the registry over the wire. The resolver builds it
    // secret-free by construction (vault-id references + caller-minted JWTs only),
    // but this enforces the invariant at the trust boundary — a structural
    // violation (a smuggled secret-shaped field, however named) THROWS rather than
    // shipping a snapshot with an embedded secret. Like a snapshot that could not
    // be built, this propagates: delivering a credentialed snapshot is a security
    // defect, not a recoverable transport failure.
    assertSnapshotCredentialFree(snapshot);

    const delivered = await this.pushSnapshot(snapshot);
    snapshotDeliveredTotal.inc({ result: delivered ? 'delivered' : 'undelivered' });
    this.logger?.info?.(
      {
        sessionId: this.sessionId,
        runnerId: this.runnerId,
        provider: snapshot.provider,
        delivered,
      },
      'snapshot delivery served (owner pod)',
    );
    return { skipped: false, delivered };
  }

  /**
   * Push the snapshot as a single streaming POST and await its ack.
   *
   * Every delivery failure is contained + logged:
   *   - the runner is offline ({@link ConnectError}) → log + `false`;
   *   - the runner answers non-2xx → drain/close + log + `false`;
   *   - the tunnel drops mid-push → the body iterator throws → log + `false`.
   * The body is the snapshot JSON as a single NDJSON line (one object), matching
   * recovery's NDJSON framing so the runner reads it the same way.
   */
  private async pushSnapshot(snapshot: AgentSnapshot): Promise<boolean> {
    const body = Buffer.from(`${JSON.stringify(snapshot)}\n`, 'utf8');
    let response: TunnelResponse;
    try {
      response = await this.transport.handleRequest({
        method: 'POST',
        path: RUNNER_SNAPSHOT_PATH,
        headers: [[RUNNER_SESSION_HEADER, this.sessionId]],
        body,
        contentType: 'application/x-ndjson',
      });
    } catch (err) {
      const offline = err instanceof ConnectError;
      this.logger?.warn?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId, offline },
        offline
          ? 'snapshot delivery could not reach runner (offline)'
          : 'snapshot delivery failed to push to runner',
      );
      return false;
    }

    if (response.status < 200 || response.status >= 300) {
      await drainAndClose(response);
      this.logger?.warn?.(
        { sessionId: this.sessionId, runnerId: this.runnerId, status: response.status },
        'snapshot delivery got non-2xx ack from runner',
      );
      return false;
    }

    try {
      for await (const _chunk of response.stream) {
        void _chunk;
      }
    } catch (err) {
      this.logger?.warn?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId },
        'snapshot delivery push ended early (runner tunnel drop)',
      );
      return false;
    }
    return true;
  }
}

/**
 * Fully drain a response body and close its request slot.
 *
 * Used on the non-2xx branch to release the in-flight request without
 * interpreting the body. Draining errors (a mid-drain tunnel drop) are swallowed
 * — the caller already decided this push was not delivered.
 */
async function drainAndClose(response: TunnelResponse): Promise<void> {
  try {
    for await (const _chunk of response.stream) {
      void _chunk;
    }
  } catch {
    // best-effort drain; the stream's own finally closes the request slot.
  }
}
