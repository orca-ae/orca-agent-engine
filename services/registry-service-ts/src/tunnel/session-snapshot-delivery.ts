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
import { createHash, randomUUID } from 'node:crypto';
import { decodeJwt } from 'jose';
import type { SessionJwtMinter } from '../auth/session-jwt.js';
import { dshLlmRoute } from '@orca/harness-catalog';
import { pinRunnerGeneration } from './runner-generation.js';
import type { AgentSnapshot } from '../domain/agent-snapshot.js';
import type { DshCredentialContext } from '../domain/agent-snapshot-resolver.js';
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

export interface DshCredentialBinding {
  handoff_id: string;
  token_digest: string;
  workspace_id: string;
  session_id: string;
  execution_id: string;
}

export type DshCredentialProvisional = DshCredentialBinding & {
  phase: 'provisional';
  expires_at_ms: number;
};
export type DshCredentialConfirmation = DshCredentialBinding & {
  phase: 'confirmed';
  remaining_at_anchor_ms: number;
};
export type DshCredentialAcknowledgement = DshCredentialBinding & { phase: 'acknowledged' };

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
  private readonly registry: TransportRegistry;
  private handoffActive = false;

  constructor(opts: SessionSnapshotDeliveryOptions) {
    this.workspaceId = opts.workspaceId;
    this.sessionId = opts.sessionId;
    this.runnerId = opts.runnerId;
    this.transport = new TunnelTransport(opts.registry, opts.runnerId);
    this.provider = opts.provider;
    this.logger = opts.logger;
    this.registry = opts.registry;
  }

  /** Explicit dormant entry point; production delivery never selects this protocol. */
  async deliverDshCredentials(opts: {
    snapshot: AgentSnapshot;
    credential: { token: string; expiresAt: number };
    /** From the trusted Session/Agent resolver, never from decoding the supplied token. */
    expectedContext: DshCredentialContext;
    minter: SessionJwtMinter;
    executionId: string;
    isCurrent(): boolean;
  }): Promise<SnapshotDeliveryOutcome> {
    if (this.handoffActive) throw new Error('DSH credential handoff already active');
    this.handoffActive = true;
    let deadlineTimer: ReturnType<typeof setTimeout> | undefined;
    try {
      const generation = pinRunnerGeneration(this.registry, this.runnerId);
      const transport = new TunnelTransport(generation.registry, this.runnerId);
      const assertCurrent = () => {
        if (!generation.isCurrent() || !opts.isCurrent()) throw new Error('DSH generation retired');
      };
      assertCurrent();
      const claims = await opts.minter.verify(opts.credential.token, {
        expectedAudience: 'ai-gateway',
      });
      assertCurrent();
      const decoded = decodeJwt(opts.credential.token);
      if (!Number.isSafeInteger(decoded.iat) || !Number.isSafeInteger(decoded.exp)) {
        throw new Error('DSH credential timestamps invalid');
      }
      const snapshot = structuredClone(opts.snapshot);
      if (
        !opts.executionId ||
        claims.workspaceId !== this.workspaceId ||
        claims.sessionId !== this.sessionId ||
        claims.orgId !== opts.expectedContext.orgId ||
        claims.agentId !== opts.expectedContext.agentId ||
        claims.runtimeConfigRevision !== opts.expectedContext.runtimeConfigRevision ||
        claims.guardrailIds.length !== opts.expectedContext.guardrailIds.length ||
        claims.guardrailIds.some((id, index) => id !== opts.expectedContext.guardrailIds[index]) ||
        claims.expiresAt !== opts.credential.expiresAt ||
        decoded.exp! - decoded.iat! !== 720 ||
        claims.llmRoutes.length !== 1 ||
        claims.llmRoutes[0] !== dshLlmRoute('deepseek', 'deepseek-v4-flash') ||
        claims.llmModels.length !== 1 ||
        claims.llmModels[0] !== 'deepseek-v4-flash' ||
        claims.mcpServerNames.length ||
        claims.vaultIds.length ||
        claims.credentialIds.length ||
        claims.repoUrls.length ||
        claims.gitProxy !== undefined ||
        snapshot.provider !== 'dsh-sdk' ||
        snapshot.model.provider !== 'deepseek' ||
        snapshot.model.id !== 'deepseek-v4-flash' ||
        snapshot.egress.mode !== 'gateway' ||
        snapshot.egress.gateway.session_jwt !== '' ||
        Object.keys(snapshot.egress.gateway.mcp_servers).length ||
        snapshot.allowed_mcp_server_names.length ||
        !snapshot.egress.gateway.llm_base_url
      )
        throw new Error('DSH credential identity/profile mismatch');
      snapshot.egress.gateway.llm_jwt = opts.credential.token;
      assertSnapshotCredentialFree(snapshot);
      const binding: DshCredentialBinding = {
        handoff_id: randomUUID(),
        token_digest: createHash('sha256').update(opts.credential.token).digest('hex'),
        workspace_id: this.workspaceId,
        session_id: this.sessionId,
        execution_id: opts.executionId,
      };
      const expiresAtMs = claims.expiresAt * 1000;
      if (!Number.isSafeInteger(expiresAtMs)) throw new Error('DSH credential expiry invalid');
      const provisional: DshCredentialProvisional = {
        ...binding,
        phase: 'provisional',
        expires_at_ms: expiresAtMs,
      };
      const body = Buffer.from(
        `${JSON.stringify({ ...snapshot, credential_handoff: provisional })}\n`,
      );
      assertCurrent();
      const wallStart = Date.now();
      const monotonicStart = performance.now();
      const remaining = () => {
        const elapsed = performance.now() - monotonicStart;
        // Allow 1s of clock sampling/slew; larger discontinuities invalidate the handoff.
        // Gateway skew plus this tolerance must fit the profile's 60s safety margin.
        if (
          !Number.isFinite(elapsed) ||
          elapsed < 0 ||
          Math.abs(Date.now() - wallStart - elapsed) > 1000
        ) {
          throw new Error('DSH Registry clock discontinuity');
        }
        const value = Math.floor(expiresAtMs - wallStart - elapsed);
        if (!Number.isSafeInteger(value) || value < 660_000 || value > 720_000) {
          throw new Error('DSH credential lifetime insufficient');
        }
        return value;
      };
      // Both ACKs share the original safety window, including head/body waits.
      const deadline = new AbortController();
      deadlineTimer = setTimeout(() => deadline.abort(), remaining() - 660_000);
      const request = async (bytes: Buffer) => {
        assertCurrent();
        remaining();
        const response = await transport.handleRequest({
          method: 'POST',
          path: RUNNER_SNAPSHOT_PATH,
          headers: [[RUNNER_SESSION_HEADER, this.sessionId]],
          body: bytes,
          contentType: 'application/x-ndjson',
          signal: deadline.signal,
        });
        try {
          assertCurrent();
          if (response.status < 200 || response.status >= 300) throw new Error('DSH ACK rejected');
          const chunks: Buffer[] = [];
          let size = 0;
          for await (const chunk of response.stream) {
            assertCurrent();
            size += chunk.byteLength;
            if (size > 4096) throw new Error('DSH ACK oversized');
            chunks.push(Buffer.from(chunk));
          }
          assertCurrent();
          const ack = JSON.parse(
            Buffer.concat(chunks).toString('utf8'),
          ) as DshCredentialAcknowledgement;
          if (
            !ack ||
            ack.phase !== 'acknowledged' ||
            Object.entries(binding).some(
              ([key, value]) => ack[key as keyof DshCredentialBinding] !== value,
            ) ||
            Object.keys(ack).length !== Object.keys(binding).length + 1
          ) {
            throw new Error('DSH ACK binding mismatch');
          }
        } finally {
          await response.stream.close();
        }
      };
      await request(body);
      assertCurrent();
      const confirmation: DshCredentialConfirmation = {
        ...binding,
        phase: 'confirmed',
        remaining_at_anchor_ms: remaining(),
      };
      await request(Buffer.from(`${JSON.stringify({ credential_handoff: confirmation })}\n`));
      assertCurrent();
      remaining();
      return { skipped: false, delivered: true };
    } catch {
      // Never log the provisional snapshot, bearer token, or untrusted ACK body.
      this.logger?.warn?.(
        { sessionId: this.sessionId, runnerId: this.runnerId },
        'DSH credential handoff rejected',
      );
      return { skipped: false, delivered: false };
    } finally {
      clearTimeout(deadlineTimer);
      this.handoffActive = false;
    }
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
