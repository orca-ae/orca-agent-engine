// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Event, TranscriptStore } from '@orca/transcript-store';
import {
  AgentEventKind,
  AgentRuntimeSignalKind,
  GuardrailUsageUnavailableError,
  SessionEventKind,
} from '../harness/agent-harness.js';
import type {
  AgentHarness,
  AgentEvent,
  RemoteMcpToolsetPolicy,
  SessionStartInput,
  SubmitHooks,
  TerminationReason,
  UserEvent,
  UserEventSubmitResult,
} from '../harness/agent-harness.js';
import { publicEntryToEvent } from '../harness/claude/event-mapper.js';
import {
  harnessActiveSessions,
  harnessEventEmitTotal,
  harnessOutputFilesIndexedTotal,
  harnessOutputIndexLagSeconds,
} from '../metrics.js';
import type { SandboxHandle } from '../sandbox/sandbox-runtime.js';
import type { ToolDefinition } from '../sandbox/agent-toolset.js';
import type { MountHandle, MountStrategy } from '../sandbox/mounts/mount-strategy.js';
import type { OutputMountHandle } from '../sandbox/outputs/output-mount.js';
import type { IndexSessionResult } from '../sandbox/outputs/output-indexer.js';
import type { MemoryVersionWatcher } from '../sandbox/memory/version-watcher.js';
import type { SessionUsageDelta } from '../clients/registry.js';
import { cacheCreationUsageFromRaw } from '../usage-normalization.js';

export interface RunnerInput {
  workspaceId: string;
  sessionId: string;
  harness: AgentHarness;
  store: TranscriptStore;
  clientToolExecution?: boolean;
  /**
   * Optional MCP server map (already rewritten to point at ai-gateway with
   * the per-session JWT applied). When present, `start()` forwards it to the
   * harness's `SessionStartInput`. Absent in chat-only mode.
   */
  mcpServers?: Record<string, { type: 'http'; url: string; headers: Record<string, string> }>;
  mcpJwtProvider?: SessionStartInput['mcpJwtProvider'];
  /**
   * Remote MCP toolsets explicitly enabled on the agent. Forwarded to the
   * Claude harness so remote `mcp__<server>__*` tools stay callable when
   * host-side built-ins are removed from context.
   */
  remoteMcpToolsets?: RemoteMcpToolsetPolicy[];
  /**
   * Per-session sandbox handle. Owned by the dispatcher (acquired
   * at spawnRunner, destroyed in `stop()`). Forwarded into the harness so
   * tools dispatch into it.
   */
  sandbox?: SandboxHandle;
  /**
   * Restricted view used only for agent tool dispatch. The dispatcher keeps
   * `sandbox` as the lifecycle owner because mount teardown may require
   * privileged access, while this handle enforces the session write policy.
   */
  agentSandbox?: SandboxHandle;
  /**
   * The agent_toolset built against `sandbox`. Forwarded to the
   * harness's underlying SDK so the model can invoke sandbox tools.
   */
  tools?: ToolDefinition[];
  /**
   * Active mount handles + their strategies. Needed at `stop()` so
   * we can call `strategy.deactivate(sandbox, handle)` for each mount before
   * destroying the sandbox.
   */
  mounts?: Array<{ handle: MountHandle; strategy: MountStrategy }>;
  /**
   * Handle for the session output mount. The runner does not read it (and
   * never mutates the sandbox for it) — output indexing is driven via
   * {@link indexOutputs} because the indexer needs collaborators
   * (registry / s3) the runner intentionally doesn't know about.
   */
  outputMount?: OutputMountHandle;
  /**
   * Closure that runs `OutputIndexer.indexSession(...)`
   * for this session's output mount. The dispatcher constructs this with
   * everything baked in (workspaceId, sessionId, mount, registry, store,
   * sandbox, s3Client). A successful local or MCP tool-result transcript
   * append schedules it asynchronously; `stop()` also awaits one final pass
   * BEFORE deactivating mounts so the FUSE mount / sandbox FS remains readable.
   * Soft-fail: errors are logged and never block transcript pumping or mount
   * deactivation.
   */
  indexOutputs?: (signal?: AbortSignal) => Promise<IndexSessionResult>;
  /**
   * Optional MemoryVersionWatcher started by the dispatcher
   * when at least one `memory_store` resource is attached. Owned by the
   * runner: `stop()` cancels the polling timer + waits for any in-flight poll
   * to drain BEFORE the final output index pass and mount deactivation,
   * because the watcher's poll holds a live reference to the FUSE root (S3
   * list/get) and the sandbox FS (`files.list`); tearing those down underneath
   * an active poll would race.
   */
  memoryWatcher?: MemoryVersionWatcher;
  /**
   * Optional cleanup that rm -rf's the host-side per-session
   * git work dir. Set by the dispatcher when a session has at least one
   * `github_repository` resource AND the work-dir manager is wired. Runs
   * AFTER the harness event pump drains and BEFORE the final
   * {@link indexOutputs} pass. Soft-fail: a cleanup error logs + continues.
   */
  workDirReleases?: () => Promise<void>;
  /** Optional internal usage sink. `agent.usage` events are recorded here and not appended to transcripts. */
  recordUsage?: (
    usage: SessionUsageDelta,
    model?: string,
    subagentId?: string,
    turnEventId?: string,
    usageEventId?: string,
  ) => Promise<Readonly<Record<string, unknown>> | undefined>;
  /** Called after a durable `session.status_running` append. */
  onStatusRunning?: () => void;
  /**
   * Supplies internal completion markers for current turn. The callback runs
   * only after one append durably contains terminal status plus every marker.
   */
  completionForTerminal?: (sourceIds?: string[]) => TerminalCompletion | undefined;
  /** Called after a durable terminal session status append for a turn. */
  onTurnTerminal?: () => void;
  /** Called when a terminal status/completion append fails before durability. */
  onTurnTerminalFailed?: (error: unknown) => void;
  /** Called when a non-terminal transcript append kills the event pump. */
  onEventPersistenceFailed?: (error: unknown) => void;
  /** Dispatcher lifecycle fence for callbacks and concurrent shutdown. */
  isLifecycleCurrent?: () => boolean;
  /** Per-step shutdown deadline for harness and event-pump shutdown. Defaults to 5 seconds. */
  shutdownStepGraceMs?: number;
  /** Overall output drain/final-pass/retry deadline. Defaults to 60 seconds. */
  outputIndexShutdownGraceMs?: number;
}

export interface TerminalCompletion {
  events: Event[];
  onPersisted(): void;
}

/** A submit raced or followed runner shutdown and must stay source-retryable. */
export class SessionRunnerStoppingError extends Error {
  constructor() {
    super('session runner is stopping');
    this.name = 'SessionRunnerStoppingError';
  }
}

/**
 * Per-session driver that owns one harness instance and pumps its emitted
 * AgentEvents into the TranscriptStore. The Dispatcher creates
 * one SessionRunner per active `(workspace_id, session_id)` and calls
 * `submit()` for each user event read from the session event source (Kafka,
 * Postgres, or Pulsar).
 */
export class SessionRunner {
  private pumping = false;
  private stopped = false;
  private activeSessionMetricRecorded = false;
  private abandonEventPump = false;
  private abandonOutputIndexing = false;
  private pumpPromise: Promise<void> | null = null;
  /** First unexpected event-pump error; a runner stays teardown-capable after this is set. */
  private pumpFailure: Error | null = null;
  /** Active submit races; removed as soon as their submit resolves or rejects. */
  private readonly pumpFailureWaiters = new Set<(error: Error) => void>();
  private readonly outputIndexAbortControllers = new Set<AbortController>();
  /** Serializes tool-triggered and shutdown scans against the same output mount. */
  private outputIndexTail: Promise<void> = Promise.resolve();
  private usageStateFailure: GuardrailUsageUnavailableError | null = null;
  private pendingUsageRecords: Array<{
    usage: SessionUsageDelta;
    model?: string;
    subagentId?: string;
    turnEventId?: string;
    usageEventId?: string;
  }> = [];
  constructor(private readonly opts: RunnerInput) {}

  get sandboxId(): string | null {
    return this.opts.sandbox?.id ?? null;
  }

  /** Refresh durable counters before the next warm-runner policy decision. */
  applyGuardrailUsageState(state: Readonly<Record<string, unknown>>): void {
    this.assertAcceptingWork();
    this.opts.harness.applyGuardrailUsageState?.(state);
  }

  async start(input: {
    agentSnapshot: SessionStartInput['agentSnapshot'];
    guardrails?: SessionStartInput['guardrails'];
    guardrailState?: SessionStartInput['guardrailState'];
  }): Promise<void> {
    this.assertAcceptingWork();
    harnessActiveSessions.inc();
    this.activeSessionMetricRecorded = true;
    try {
      await this.opts.harness.start({
        workspaceId: this.opts.workspaceId,
        sessionId: this.opts.sessionId,
        ...(this.opts.clientToolExecution ? { clientToolExecution: true } : {}),
        agentSnapshot: input.agentSnapshot,
        ...(input.guardrails ? { guardrails: input.guardrails } : {}),
        ...(input.guardrailState ? { guardrailState: input.guardrailState } : {}),
        ...(this.opts.mcpServers ? { mcpServers: this.opts.mcpServers } : {}),
        ...(this.opts.mcpJwtProvider ? { mcpJwtProvider: this.opts.mcpJwtProvider } : {}),
        ...(this.opts.remoteMcpToolsets ? { remoteMcpToolsets: this.opts.remoteMcpToolsets } : {}),
        ...(this.opts.agentSandbox || this.opts.sandbox
          ? { sandbox: this.opts.agentSandbox ?? this.opts.sandbox }
          : {}),
        ...(this.opts.tools ? { tools: this.opts.tools } : {}),
      });
      this.assertAcceptingWork();
    } catch (e) {
      await this.stop('error');
      throw e;
    }
    this.pumpPromise = this.pumpEvents();
    // Keep the background pump rejection handled even before stop() joins it.
    // stop() still awaits the original promise below so teardown remains a
    // barrier against events that were already emitted by the harness.
    void this.pumpPromise.catch((e) => console.error('session event pump failed', e));
  }

  async submit(ev: UserEvent, hooks?: SubmitHooks): Promise<UserEventSubmitResult | void> {
    this.assertAcceptingWork();
    if (this.usageStateFailure) await this.flushPendingUsageRecords();
    this.throwIfPumpFailed();
    let rejectPumpFailure!: (error: Error) => void;
    const pumpFailure = new Promise<never>((_resolve, reject) => {
      rejectPumpFailure = reject;
    });
    this.pumpFailureWaiters.add(rejectPumpFailure);
    try {
      const result = await Promise.race([this.opts.harness.submit(ev, hooks), pumpFailure]);
      this.assertAcceptingWork();
      this.throwIfPumpFailed();
      return result;
    } catch (error) {
      if (!this.isAcceptingWork()) throw new SessionRunnerStoppingError();
      throw error;
    } finally {
      this.pumpFailureWaiters.delete(rejectPumpFailure);
    }
  }

  hasPendingRequiredAction(): boolean {
    if (this.pumpFailure) return false;
    return this.opts.harness.hasPendingRequiredAction?.() ?? false;
  }

  hasPendingGuardrailUsage(): boolean {
    return (
      this.usageStateFailure !== null ||
      this.pendingUsageRecords.length > 0 ||
      (this.opts.harness.hasPendingGuardrailUsage?.() ?? false)
    );
  }

  async stop(reason: TerminationReason): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    this.rejectEventAcknowledgements(new SessionRunnerStoppingError());
    const stopStartedAt = Date.now();
    const shutdownStepGraceMs = this.opts.shutdownStepGraceMs ?? 5_000;
    const outputIndexShutdownGraceMs = this.opts.outputIndexShutdownGraceMs ?? 60_000;
    // Stop the memory version watcher FIRST. Its in-flight
    // poll holds references to the FUSE mount (S3 list/get) and the sandbox
    // FS (`files.list`); both go away once we run the output indexer and
    // deactivate mounts below. Stopping the watcher first guarantees the next
    // poll doesn't race against torn-down infra. Soft-fail: a hung stop
    // shouldn't block the rest of the shutdown sequence.
    if (this.opts.memoryWatcher) {
      try {
        await this.opts.memoryWatcher.stop();
      } catch (e) {
        console.error('memoryWatcher.stop failed', e);
      }
    }

    // Quiesce the agent before the final output scan. Setting `stopped` above
    // prevents events already buffered by the harness from scheduling fresh
    // tool-triggered scans; joining the pump guarantees their transcript
    // appends have completed. The sandbox and mounts intentionally remain
    // alive until after output indexing.
    const harnessStop = await settleWithin(
      Promise.resolve().then(async () => await this.opts.harness.stop(reason)),
      shutdownStepGraceMs,
    );
    if (harnessStop.status === 'rejected') {
      console.error('harness stop failed', harnessStop.error);
    } else if (harnessStop.status === 'timeout') {
      this.abandonEventPump = true;
      console.error(`harness stop timed out after ${shutdownStepGraceMs}ms; continuing shutdown`);
    }
    if (this.pumpPromise) {
      const pump = await settleWithin(this.pumpPromise, shutdownStepGraceMs);
      if (pump.status === 'timeout') {
        // A broken harness may reject stop() without closing events(). Do not
        // let that strand the final output scan and sandbox teardown forever.
        // If its iterator later wakes up, pumpEvents observes this flag and
        // drops the late event instead of appending after teardown.
        this.abandonEventPump = true;
        console.error('session event pump did not stop within grace period; abandoning it');
      }
    }

    // Release the host-side git work dir after the harness has
    // stopped using it and BEFORE the final output scan. The work dir is
    // independent from the session's output prefix. Soft-fail per the runner
    // contract so cleanup can't prevent output capture or sandbox teardown.
    if (this.opts.workDirReleases) {
      try {
        await this.opts.workDirReleases();
      } catch (e) {
        console.error('workDirReleases failed', e);
      }
    }

    // Wait for every already-scheduled incremental scan,
    // then run one final pass BEFORE mount deactivation. The quiesced harness
    // can no longer mutate the output directory, so this pass closes the
    // background-write and tool-result timing window.
    if (this.opts.indexOutputs) {
      // Drain, final scan, retry, and cancellation share one deadline. A
      // pathological phase therefore cannot multiply the configured grace
      // period and delay mount/sandbox teardown several times over.
      const outputIndexShutdownDeadline = Date.now() + outputIndexShutdownGraceMs;
      const remainingOutputIndexGraceMs = (): number =>
        Math.max(0, outputIndexShutdownDeadline - Date.now());
      const incrementalDrain = await settleWithin(
        this.outputIndexTail,
        remainingOutputIndexGraceMs(),
      );
      if (incrementalDrain.status === 'timeout') {
        console.error(
          `incremental output scans exhausted the ${outputIndexShutdownGraceMs}ms ` +
            'output-index shutdown budget; cancelling them',
        );
        await this.cancelOutputIndexing(
          Math.min(shutdownStepGraceMs, remainingOutputIndexGraceMs()),
        );
      }
      if (remainingOutputIndexGraceMs() > 0) {
        // stop() set `stopped` before draining, so no new incremental work can
        // arrive. Once the old tail settles, allow exactly the fresh final pass.
        this.abandonOutputIndexing = false;
        const finalPass = await settleWithin(
          this.runOutputIndex('during shutdown'),
          remainingOutputIndexGraceMs(),
        );
        if (finalPass.status === 'timeout') {
          console.error(
            `output indexer exhausted the ${outputIndexShutdownGraceMs}ms ` +
              'output-index shutdown budget; continuing teardown',
          );
          await this.cancelOutputIndexing(
            Math.min(shutdownStepGraceMs, remainingOutputIndexGraceMs()),
          );
        } else if (
          finalPass.status === 'fulfilled' &&
          (finalPass.value === null || finalPass.value.errors.length > 0) &&
          remainingOutputIndexGraceMs() > 0
        ) {
          // One bounded retry closes the common final-pass window where the
          // File row was created but its session.output_indexed event failed.
          // OutputIndexer keeps runner-local path/SHA state, so this retry does
          // not create a second File for successful registrations.
          const retry = await settleWithin(
            this.runOutputIndex('during shutdown retry'),
            remainingOutputIndexGraceMs(),
          );
          if (retry.status === 'timeout') {
            console.error(
              `output indexer retry exhausted the ${outputIndexShutdownGraceMs}ms ` +
                'output-index shutdown budget',
            );
            await this.cancelOutputIndexing(
              Math.min(shutdownStepGraceMs, remainingOutputIndexGraceMs()),
            );
          }
        }
      }
      try {
        harnessOutputIndexLagSeconds.observe(
          { trigger: 'shutdown' },
          (Date.now() - stopStartedAt) / 1000,
        );
      } catch (metricsErr) {
        console.error('output indexer metrics emit failed', metricsErr);
      }
    }

    // Deactivate active mounts only after the stopped harness, event
    // pump, and final output scan have all released the sandbox filesystem.
    // TarballPrefetch's deactivate is a best-effort delete.
    if (this.opts.mounts && this.opts.sandbox) {
      for (const { handle, strategy } of this.opts.mounts) {
        try {
          await strategy.deactivate(this.opts.sandbox, handle);
        } catch (e) {
          console.error(`mount deactivate failed for ${handle.id}`, e);
        }
      }
    }
    if (this.opts.sandbox) {
      try {
        await this.opts.sandbox.destroy();
      } catch (e) {
        console.error('sandbox destroy failed', e);
      }
    }
    if (this.activeSessionMetricRecorded) {
      this.activeSessionMetricRecorded = false;
      harnessActiveSessions.dec();
    }
  }

  private readonly eventAcknowledgements = new Set<NonNullable<AgentEvent['persistence']>>();

  private rejectEventAcknowledgements(error: unknown): void {
    for (const ack of this.eventAcknowledgements) ack.reject(error);
    this.eventAcknowledgements.clear();
  }

  private async pumpEvents(): Promise<void> {
    if (this.pumping) return;
    this.pumping = true;
    try {
      for await (const e of this.opts.harness.events()) {
        if (this.abandonEventPump || !this.isAcceptingWork()) {
          e.persistence?.reject(new SessionRunnerStoppingError());
          break;
        }
        if (e.persistence) this.eventAcknowledgements.add(e.persistence);
        if (e.kind === AgentRuntimeSignalKind.usage) {
          const usage = usageDeltaFromPayload(e.payload);
          if (usage && this.opts.recordUsage && !guardrailUsageAlreadyRecorded(e.payload)) {
            const pending = {
              usage,
              ...(modelFromUsagePayload(e.payload)
                ? { model: modelFromUsagePayload(e.payload) }
                : {}),
              ...(subagentIdFromUsagePayload(e.payload)
                ? { subagentId: subagentIdFromUsagePayload(e.payload) }
                : {}),
              ...(turnEventIdFromUsagePayload(e.payload)
                ? { turnEventId: turnEventIdFromUsagePayload(e.payload) }
                : {}),
              ...(e.id ? { usageEventId: e.id } : {}),
            };
            if (this.usageStateFailure) {
              this.pendingUsageRecords.push(pending);
              e.persistence?.reject(this.usageStateFailure);
              if (e.persistence) this.eventAcknowledgements.delete(e.persistence);
              continue;
            }
            try {
              const state = await this.opts.recordUsage(
                pending.usage,
                pending.model,
                pending.subagentId,
                pending.turnEventId,
                pending.usageEventId,
              );
              if (state) this.opts.harness.applyGuardrailUsageState?.(state);
            } catch (err) {
              this.pendingUsageRecords.push(pending);
              this.usageStateFailure = new GuardrailUsageUnavailableError(
                'Registry did not acknowledge usage; stateful-guarded actions are blocked.',
                { cause: err },
              );
              console.error('record session usage failed; guardrail enforcement locked', err);
            }
          }
          if (
            this.usageStateFailure ||
            (!guardrailUsageAlreadyRecorded(e.payload) && (!usage || !this.opts.recordUsage))
          )
            e.persistence?.reject(
              this.usageStateFailure ?? new Error('Usage persistence is unavailable'),
            );
          else e.persistence?.resolve();
          if (e.persistence) this.eventAcknowledgements.delete(e.persistence);
          continue;
        }
        // Wrap each AgentEvent as a SessionStoreEntry. We spread the payload
        // FIRST and then set `type: e.kind` so the AgentEvent.kind (e.g.
        // `agent.message`) always wins over any `type` field in the payload
        // (the Claude SDK's assistant messages carry their own `type:
        // 'assistant'`, which we want to override on the wire).
        const payloadObj =
          e.payload && typeof e.payload === 'object' ? (e.payload as Record<string, unknown>) : {};
        const entry = { ...payloadObj, type: e.kind };
        const event = publicEntryToEvent({
          workspaceId: this.opts.workspaceId,
          sessionId: this.opts.sessionId,
          subpath: e.subpath,
          producedBy: 'harness',
          entry,
          eventId: e.id,
          idempotencyKey: e.id,
          ...(e.producedAt ? { now: () => new Date(e.producedAt!) } : {}),
        });
        const isTurnTerminal =
          e.kind === SessionEventKind.statusIdle || e.kind === SessionEventKind.statusTerminated;
        const explicitSources =
          typeof e.completionPolicy === 'object' ? e.completionPolicy.sourceIds : undefined;
        const completion =
          isTurnTerminal && e.completionPolicy !== 'pending'
            ? this.opts.completionForTerminal?.(explicitSources)
            : undefined;
        if (!this.isAcceptingWork()) break;
        try {
          const events = [
            event,
            ...(completion?.events ?? []).map((marker) =>
              explicitSources && e.producedAt ? { ...marker, producedAt: e.producedAt } : marker,
            ),
          ];
          if (explicitSources) {
            // Broker batches can partially succeed out of order. The terminal must
            // be durable before any source-completion marker can become visible.
            for (const item of events) {
              if (!this.isAcceptingWork()) throw new SessionRunnerStoppingError();
              await this.opts.store.append(this.opts.workspaceId, this.opts.sessionId, [item]);
            }
          } else await this.opts.store.append(this.opts.workspaceId, this.opts.sessionId, events);
        } catch (error) {
          e.persistence?.reject(error);
          if (isTurnTerminal) this.opts.onTurnTerminalFailed?.(error);
          else this.opts.onEventPersistenceFailed?.(error);
          throw error;
        }
        // A blocked append can settle after Dispatcher.stop() clears its maps.
        // Do not run callbacks that would repopulate those retired maps.
        if (!this.isAcceptingWork()) {
          e.persistence?.reject(new SessionRunnerStoppingError());
          break;
        }
        if (e.kind === SessionEventKind.statusRunning) this.opts.onStatusRunning?.();
        if (isTurnTerminal) {
          completion?.onPersisted();
          this.opts.onTurnTerminal?.();
        }
        e.persistence?.resolve();
        if (e.persistence) this.eventAcknowledgements.delete(e.persistence);
        harnessEventEmitTotal.inc({ workspace_id: this.opts.workspaceId, kind: e.kind });
        if (
          (e.kind === AgentEventKind.toolResult || e.kind === AgentEventKind.mcpToolResult) &&
          this.opts.indexOutputs &&
          !this.stopped
        ) {
          // Persist-before-capture is intentional: consumers can treat the
          // tool result as the causal boundary for any following
          // `session.output_indexed` events. Do not await the scan here — a
          // slow S3 list or registry upload must not delay agent.message or
          // session.status_idle transcript events.
          void this.runOutputIndex('after tool result', 'tool_result');
        }
      }
    } catch (error) {
      this.rejectEventAcknowledgements(error);
      if (!this.stopped && !this.abandonEventPump) this.recordPumpFailure(error);
      throw error;
    } finally {
      this.pumping = false;
    }
  }

  private recordPumpFailure(error: unknown): void {
    if (this.pumpFailure) return;
    this.pumpFailure =
      error instanceof Error ? error : new Error('session event pump failed', { cause: error });
    for (const reject of this.pumpFailureWaiters) reject(this.pumpFailure);
    this.pumpFailureWaiters.clear();
  }

  private throwIfPumpFailed(): void {
    if (this.pumpFailure) throw this.pumpFailureError();
  }

  private pumpFailureError(): Error {
    return this.pumpFailure ?? new Error('session event pump failed');
  }

  private isAcceptingWork(): boolean {
    return !this.stopped && (this.opts.isLifecycleCurrent?.() ?? true);
  }

  private assertAcceptingWork(): void {
    if (!this.isAcceptingWork()) throw new SessionRunnerStoppingError();
  }

  private async flushPendingUsageRecords(): Promise<void> {
    if (!this.opts.recordUsage) return;
    while (this.pendingUsageRecords.length > 0) {
      const pending = this.pendingUsageRecords[0]!;
      try {
        const state = await this.opts.recordUsage(
          pending.usage,
          pending.model,
          pending.subagentId,
          pending.turnEventId,
          pending.usageEventId,
        );
        if (state) this.opts.harness.applyGuardrailUsageState?.(state);
        this.pendingUsageRecords.shift();
      } catch (cause) {
        this.usageStateFailure = new GuardrailUsageUnavailableError(
          'Registry did not acknowledge usage; stateful-guarded actions are blocked.',
          { cause },
        );
        throw this.usageStateFailure;
      }
    }
    this.usageStateFailure = null;
  }

  private queueOutputIndex(): Promise<IndexSessionResult> {
    const indexOutputs = this.opts.indexOutputs;
    if (!indexOutputs) throw new Error('output indexer is not configured');
    const controller = new AbortController();
    this.outputIndexAbortControllers.add(controller);
    const next = this.outputIndexTail
      .then(async () => {
        // A timed-out scan may eventually settle after teardown. Do not let a
        // final pass that was queued behind it start against a destroyed mount.
        if (this.abandonOutputIndexing) return { count: 0, skipped: 0, errors: [] };
        return await indexOutputs(controller.signal);
      })
      .finally(() => this.outputIndexAbortControllers.delete(controller));
    this.outputIndexTail = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  }

  /** Abort active/queued scans and wait briefly for abort-aware work to unwind. */
  private async cancelOutputIndexing(graceMs: number): Promise<void> {
    this.abandonOutputIndexing = true;
    for (const controller of this.outputIndexAbortControllers) {
      controller.abort(abortError('output indexing cancelled during shutdown'));
    }
    const cancelled = await settleWithin(this.outputIndexTail, graceMs);
    if (cancelled.status === 'timeout') {
      console.error(`output index cancellation did not settle within ${graceMs}ms`);
    }
  }

  /** Run one serialized scan without ever rejecting into the event pump. */
  private async runOutputIndex(
    context: string,
    metricTrigger?: 'tool_result',
  ): Promise<IndexSessionResult | null> {
    const startedAt = Date.now();
    try {
      const result = await this.queueOutputIndex();
      this.recordOutputIndexResult(result, context);
      return result;
    } catch (e) {
      if (!isAbortError(e)) console.error(`output indexer failed ${context}`, e);
      return null;
    } finally {
      if (metricTrigger) {
        try {
          harnessOutputIndexLagSeconds.observe(
            { trigger: metricTrigger },
            (Date.now() - startedAt) / 1000,
          );
        } catch (metricsErr) {
          console.error('output indexer metrics emit failed', metricsErr);
        }
      }
    }
  }

  private recordOutputIndexResult(result: IndexSessionResult, context: string): void {
    if (result.count > 0) {
      harnessOutputFilesIndexedTotal.inc(
        { workspace_id: this.opts.workspaceId, result: 'ok' },
        result.count,
      );
    }
    if (result.errors.length > 0) {
      harnessOutputFilesIndexedTotal.inc(
        { workspace_id: this.opts.workspaceId, result: 'error' },
        result.errors.length,
      );
      console.warn(
        `output indexer reported ${result.errors.length} per-key error(s) ${context}; ` +
          `count=${result.count}, skipped=${result.skipped}`,
      );
    }
    if (result.skipped > 0) {
      harnessOutputFilesIndexedTotal.inc(
        { workspace_id: this.opts.workspaceId, result: 'skipped' },
        result.skipped,
      );
    }
  }
}

function usageDeltaFromPayload(payload: unknown): SessionUsageDelta | null {
  if (!payload || typeof payload !== 'object') return null;
  const usage = (payload as { usage?: unknown }).usage ?? payload;
  if (!usage || typeof usage !== 'object') return null;
  const u = usage as Record<string, unknown>;
  return {
    cache_creation: cacheCreationUsageFromRaw(u),
    cache_read_input_tokens: numberField(u.cache_read_input_tokens),
    input_tokens: numberField(u.input_tokens),
    output_tokens: numberField(u.output_tokens),
  };
}

function modelFromUsagePayload(payload: unknown): string | undefined {
  if (!payload || typeof payload !== 'object') return undefined;
  const model = (payload as { model?: unknown }).model;
  return typeof model === 'string' && model.length > 0 ? model : undefined;
}

function subagentIdFromUsagePayload(payload: unknown): string | undefined {
  if (!payload || typeof payload !== 'object') return undefined;
  const subagentId = (payload as { subagent_id?: unknown }).subagent_id;
  return typeof subagentId === 'string' && subagentId.length > 0 ? subagentId : undefined;
}

function turnEventIdFromUsagePayload(payload: unknown): string | undefined {
  if (!payload || typeof payload !== 'object') return undefined;
  const turnEventId = (payload as { turn_event_id?: unknown }).turn_event_id;
  return typeof turnEventId === 'string' && turnEventId.length > 0 ? turnEventId : undefined;
}

function guardrailUsageAlreadyRecorded(payload: unknown): boolean {
  return (
    payload !== null &&
    typeof payload === 'object' &&
    (payload as { guardrail_usage_recorded?: unknown }).guardrail_usage_recorded === true
  );
}

function numberField(value: unknown): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) return 0;
  return Math.floor(value);
}

function abortError(message: string): Error {
  const error = new Error(message);
  error.name = 'AbortError';
  return error;
}

function isAbortError(value: unknown): boolean {
  return value instanceof Error && value.name === 'AbortError';
}

type Settlement<T> =
  | { status: 'fulfilled'; value: T }
  | { status: 'rejected'; error: unknown }
  | { status: 'timeout' };

function settleWithin<T>(promise: Promise<T>, timeoutMs: number): Promise<Settlement<T>> {
  return new Promise<Settlement<T>>((resolve) => {
    let settled = false;
    const finish = (value: Settlement<T>): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(value);
    };
    const timer = setTimeout(() => finish({ status: 'timeout' }), Math.max(0, timeoutMs));
    void promise.then(
      (value) => finish({ status: 'fulfilled', value }),
      (error: unknown) => finish({ status: 'rejected', error }),
    );
  });
}
