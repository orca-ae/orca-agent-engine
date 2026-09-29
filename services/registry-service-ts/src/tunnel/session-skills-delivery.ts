// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Owner-pod SKILLS delivery — pushes exact Registry-pinned Skill bundle BYTES to
// a runner over the runner tunnel at session start, BEFORE the snapshot.
//
// The colocated runner is outbound-WSS-only (no S3 credentials, no api key, no
// `@orca/skill-store`), so it cannot pull Skill bundles itself. The owner pod's
// registry process DOES hold `@orca/skill-store`, so IT opens the session's pinned
// bundles and PUSHES their verified bytes down the tunnel; the runner writes them to
// a native `--plugin-dir` plugin. This is the sibling of {@link SessionSnapshotDelivery}:
// a single streaming POST to the runner's skills route over the SAME tunnel, sequenced
// so the owner pod awaits the skills ack THEN delivers the snapshot.
//
// The session's Skill union is composed by a {@link SkillsProvider} seam (the
// `AgentSnapshotResolver` in production; a fake in tests), so delivery owns only the
// bundle open + validate + push, not the record resolution. A session with NO Skills
// is a clean skip (nothing to deliver). A bundle that cannot be opened or fails
// integrity validation is a real fault and PROPAGATES (like a snapshot that could not
// be built); a transport-level failure (offline / non-2xx / mid-push drop) is
// contained + reported, because it self-heals on the next reconnect.
//
// Bundles are opened ONE AT A TIME (a decoded bundle is dropped after its file lines
// are appended) so a 500-binding session cannot spike the owner pod's memory with
// every decoded bundle held at once — the same discipline the harness materializer uses.

import { ConnectError, type TunnelResponse, TunnelTransport } from '@orca/harness-tunnel';
import { uniqueSkillMaterializations, validateSkillBundle } from '@orca/sandbox-runtime';
import type { SkillBundle, SkillStore } from '@orca/skill-store';
import type { PreparedSkillDescriptor } from '../contracts/internal.contract.js';
import { skillsDeliveredTotal } from '../metrics.js';
import { RUNNER_SESSION_HEADER } from './session-event-bridge.js';
import type { TransportRegistry } from '@orca/harness-tunnel';

/**
 * Runner route the owner pod PUSHES the Skill bundle bytes to (before the snapshot).
 * Orca-native path; the cross-component contract with the runner's skills handler,
 * single-sourced on the runner side as `RUNNER_SKILLS_PATH` (`session-runner/src/protocol.ts`)
 * — re-declared here so the two sides match the exact same on-the-wire name.
 */
export const RUNNER_SKILLS_PATH = '/v1/runner/skills';

/**
 * The conventional plugin SUBDIR the runner stages the Skill plugin under (joined
 * beneath the runner's own workspace root — the runner owns the absolute base). Carried
 * on the manifest line's `dir` so the runner knows the target segment; validated by the
 * runner as a single safe path segment before it joins.
 */
export const RUNNER_SKILLS_PLUGIN_DIRNAME = 'skills-plugin';

/** Content type for the skills push body (one manifest line + one line per file). */
const NDJSON_CONTENT_TYPE = 'application/x-ndjson';

/** Structured logger seam (a subset of the usual `req.log`). All optional. */
export interface SkillsDeliveryLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/**
 * Composes the SESSION-WIDE Skill-bundle union for one session — the descriptors the
 * delivery opens + pushes. Returns `[]` when the session has no Skills (a clean skip);
 * THROWS when a binding is missing / foreign (a fault the caller decides on). The
 * `AgentSnapshotResolver` implements this over `session_skill_bindings`.
 */
export interface SkillsProvider {
  resolveSkillBundles(sessionId: string): Promise<PreparedSkillDescriptor[]>;
}

/** Options for {@link SessionSkillsDelivery}. */
export interface SessionSkillsDeliveryOptions {
  /** Owning workspace (the tenant scope) for the tunnel owner + the bundle open scope. */
  workspaceId: string;
  /** Session id this delivery serves, e.g. `"ses_a1b2..."`. */
  sessionId: string;
  /** Runner id the session is bound to — the tunnel the owner pod pushes over. */
  runnerId: string;
  /** Shared runner-tunnel registry the {@link TunnelTransport} routes through. */
  registry: TransportRegistry;
  /** Composes the session's Skill union (the resolver in production). */
  provider: SkillsProvider;
  /** Opens the exact pinned Skill bundles (the registry's `@orca/skill-store`). */
  skillStore: SkillStore;
  /** Managed tool roots must also acknowledge an empty replacement catalog. */
  deliverEmpty?: boolean;
  /** Optional structured logger. */
  logger?: SkillsDeliveryLogger;
}

/** The outcome of a single {@link SessionSkillsDelivery.deliver}. */
export interface SkillsDeliveryOutcome {
  /** `true` when the session has no Skills — nothing to deliver (not an error). */
  skipped: boolean;
  /** `true` when the runner accepted the pushed bundles (a 2xx ack, fully drained). */
  delivered: boolean;
}

/**
 * The owner-pod skills delivery for one self-hosted session.
 *
 * Construct one per (owner pod, session) and call {@link deliver} on a runner
 * (re)connect, BEFORE the snapshot delivery. It resolves the session's Skill union,
 * opens + validates each bundle, and pushes their bytes down the tunnel as one
 * streaming POST.
 */
export class SessionSkillsDelivery {
  private readonly workspaceId: string;
  private readonly sessionId: string;
  private readonly runnerId: string;
  private readonly transport: TunnelTransport;
  private readonly provider: SkillsProvider;
  private readonly skillStore: SkillStore;
  private readonly logger: SkillsDeliveryLogger | undefined;
  private readonly deliverEmpty: boolean;

  constructor(opts: SessionSkillsDeliveryOptions) {
    this.workspaceId = opts.workspaceId;
    this.sessionId = opts.sessionId;
    this.runnerId = opts.runnerId;
    this.transport = new TunnelTransport(opts.registry, opts.runnerId);
    this.provider = opts.provider;
    this.skillStore = opts.skillStore;
    this.logger = opts.logger;
    this.deliverEmpty = opts.deliverEmpty ?? false;
  }

  /**
   * Resolve + open + push the session's Skill bundles to the runner.
   *
   *   - the session has no Skills → `{ skipped: true, delivered: false }`;
   *   - a bundle open / integrity failure → propagates (a real fault);
   *   - push offline / non-2xx / mid-push drop → contained + logged, returns
   *     `{ skipped: false, delivered: false }` (self-heals on the next reconnect);
   *   - 2xx ack drained → `{ skipped: false, delivered: true }`.
   */
  async deliver(): Promise<SkillsDeliveryOutcome> {
    const skills = await this.provider.resolveSkillBundles(this.sessionId);
    if (skills.length === 0 && !this.deliverEmpty) {
      this.logger?.info?.(
        { sessionId: this.sessionId, runnerId: this.runnerId },
        'skills delivery skipped (no skills for session)',
      );
      return { skipped: true, delivered: false };
    }

    // Build the push body: the manifest line (target dir + names) then one line per
    // file, opening each bundle ONE AT A TIME + validating its integrity before its
    // bytes are appended (a decoded bundle is dropped before the next is opened).
    const body = await this.buildBody(skills);
    const delivered = await this.pushSkills(body);
    skillsDeliveredTotal.inc({ result: delivered ? 'delivered' : 'undelivered' });
    this.logger?.info?.(
      { sessionId: this.sessionId, runnerId: this.runnerId, skills: skills.length, delivered },
      'skills delivery served (owner pod)',
    );
    return { skipped: false, delivered };
  }

  /**
   * Compose the NDJSON push body. The FIRST line is the manifest
   * (`{ type:'skills_manifest', dir, skills:[names] }`); then one line per file
   * (`{ type:'skill_file', skill, path, mode, mime_type, content_base64 }`). Bundles
   * are opened + validated one at a time so only one decoded bundle is resident.
   *
   * A bundle open or integrity-validation failure THROWS (propagated by the caller) —
   * the runner must only ever receive verified bytes.
   */
  private async buildBody(skills: readonly PreparedSkillDescriptor[]): Promise<Buffer> {
    const materializations = uniqueSkillMaterializations(skills);
    const lines: string[] = [''];
    const bundles: Record<
      string,
      Pick<SkillBundle['record'], 'sha256' | 'sizeBytes' | 'files'>
    > = Object.create(null) as Record<
      string,
      Pick<SkillBundle['record'], 'sha256' | 'sizeBytes' | 'files'>
    >;
    for (const descriptor of materializations) {
      let bundle: SkillBundle;
      try {
        bundle = await this.skillStore.open(
          this.workspaceId,
          descriptor.id,
          descriptor.package_sha256,
        );
      } catch (error) {
        throw new Error(
          `failed to open skill bundle ${descriptor.id}@${descriptor.version_identifier}: ${errorMessage(error)}`,
          { cause: error },
        );
      }
      validateSkillBundle(descriptor, bundle);
      bundles[descriptor.name] = {
        sha256: bundle.record.sha256,
        sizeBytes: bundle.record.sizeBytes,
        files: bundle.record.files,
      };
      for (const file of bundle.files) {
        lines.push(
          JSON.stringify({
            type: 'skill_file',
            skill: descriptor.name,
            path: file.path,
            mode: file.mode,
            mime_type: file.mimeType,
            content_base64: file.content.toString('base64'),
          }),
        );
      }
    }
    lines[0] = JSON.stringify({
      type: 'skills_manifest',
      dir: RUNNER_SKILLS_PLUGIN_DIRNAME,
      skills: materializations.map((skill) => skill.name),
      descriptors: skills,
      bundles,
    });
    return Buffer.from(`${lines.join('\n')}\n`, 'utf8');
  }

  /**
   * Push the skills body as a single streaming POST and await its ack. Every delivery
   * failure is contained + logged (offline / non-2xx / mid-push drop → `false`), because
   * it self-heals on the next reconnect (the owner pod re-pushes before the snapshot).
   */
  private async pushSkills(body: Buffer): Promise<boolean> {
    let response: TunnelResponse;
    try {
      response = await this.transport.handleRequest({
        method: 'POST',
        path: RUNNER_SKILLS_PATH,
        headers: [[RUNNER_SESSION_HEADER, this.sessionId]],
        body,
        contentType: NDJSON_CONTENT_TYPE,
      });
    } catch (err) {
      const offline = err instanceof ConnectError;
      this.logger?.warn?.(
        { err, sessionId: this.sessionId, runnerId: this.runnerId, offline },
        offline
          ? 'skills delivery could not reach runner (offline)'
          : 'skills delivery failed to push to runner',
      );
      return false;
    }

    if (response.status < 200 || response.status >= 300) {
      await drainAndClose(response);
      this.logger?.warn?.(
        { sessionId: this.sessionId, runnerId: this.runnerId, status: response.status },
        'skills delivery got non-2xx ack from runner',
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
        'skills delivery push ended early (runner tunnel drop)',
      );
      return false;
    }
    return true;
  }
}

/** Fully drain a response body and close its request slot (best-effort). */
async function drainAndClose(response: TunnelResponse): Promise<void> {
  try {
    for await (const _chunk of response.stream) {
      void _chunk;
    }
  } catch {
    // best-effort drain; the stream's own finally closes the request slot.
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
