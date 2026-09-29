// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { TranscriptStore } from '@orca/transcript-store';
import type { SessionKey, SessionStore, SessionStoreEntry } from '@anthropic-ai/claude-agent-sdk';
import { SessionThreadEventKind } from '@orca/agent-event-contract';
import { entryToEvent, eventToEntry, isClaudeSessionEntryEvent } from './event-mapper.js';

export const MAX_CONCURRENT_SESSION_THREADS = 25;

/**
 * Implements the Claude Agent SDK's `SessionStore` over a `TranscriptStore`.
 *
 * Scoped to a single workspace at construction time. The SDK's
 * `SessionKey.projectKey` is opaque to us and gets ignored — the workspace
 * scoping comes from this adapter's constructor. The adapter translates
 * `SessionKey.subpath` (`undefined` = main transcript) to our event-mapper
 * convention (`""` = main transcript).
 *
 * **SDK sessionId vs Orca session_id.** The SDK's `Options.sessionId` field
 * requires a UUID; Orca session_ids are `ses_<base32>` strings. The harness
 * derives a deterministic UUID from the orca id (see
 * `deriveClaudeSessionId` in `harness/claude/index.ts`) and the SDK calls
 * `append({sessionId: <uuid>, projectKey}, entries)` keyed by that UUID. The
 * adapter tracks the (uuid → orca-id) mapping per-instance so the
 * transcript-store append still lands in the orca-id-keyed Kafka topic
 * (`orca.{ws}.sessions.{ses_*}.events`). Without this mapping, the SDK's
 * appends would land in the WRONG topic and the SSE bridge would never see
 * the assistant turns.
 *
 * Optional `delete`, `listSessions`, `listSessionSummaries` methods are
 * intentionally omitted; the SDK contract permits omission and Orca's
 * session lifecycle does not currently route through them. `listSubkeys` is
 * implemented so multi-agent session threads can be resumed from the
 * transcript-store subpaths written by previous SDK turns.
 */
export class ClaudeAgentSdkAdapter implements SessionStore {
  /**
   * Map from SDK-side UUID sessionId → Orca-side session_id. Populated by
   * `registerSession()` from the harness BEFORE the first `query()` call.
   * Both `append` and `load` resolve `key.sessionId` against this map; if a
   * key isn't registered we fall back to using `key.sessionId` verbatim as
   * the orca id (preserves backward-compat for tests that pass orca ids
   * directly).
   */
  private readonly sdkToOrca = new Map<string, string>();
  private readonly appendLocks = new Map<string, Promise<void>>();
  private readonly activeThreadSubpathCache = new Map<string, Set<string>>();

  constructor(
    private readonly store: TranscriptStore,
    private readonly workspaceId: string,
  ) {}

  /**
   * Register an SDK ↔ Orca session_id mapping. Called by `ClaudeAgentSdkHarness`
   * each turn — the SDK passes the registered UUID as `Options.sessionId`,
   * then calls `adapter.{append,load}` with `SessionKey.sessionId === <uuid>`.
   * The adapter rewrites `<uuid>` back to the orca id before touching the
   * transcript-store.
   */
  registerSession(sdkSessionId: string, orcaSessionId: string): void {
    this.sdkToOrca.set(sdkSessionId, orcaSessionId);
  }

  /** Resolve an SDK key to the orca-side session id. Verbatim when unmapped. */
  private resolveOrcaSessionId(sdkSessionId: string): string {
    return this.sdkToOrca.get(sdkSessionId) ?? sdkSessionId;
  }

  async append(key: SessionKey, entries: SessionStoreEntry[]): Promise<void> {
    if (entries.length === 0) return;
    const subpath = key.subpath ?? '';
    const orcaSessionId = this.resolveOrcaSessionId(key.sessionId);
    if (subpath) {
      await this.withSessionAppendLock(orcaSessionId, () =>
        this.appendEntries(orcaSessionId, subpath, entries),
      );
      return;
    }
    await this.appendEntries(orcaSessionId, subpath, entries);
  }

  private async appendEntries(
    orcaSessionId: string,
    subpath: string,
    entries: SessionStoreEntry[],
  ): Promise<void> {
    if (subpath) {
      await this.assertCanAppendToSubpath(orcaSessionId, subpath);
    }
    const events = entries.map((entry) =>
      entryToEvent({
        workspaceId: this.workspaceId,
        sessionId: orcaSessionId,
        subpath,
        producedBy: 'harness',
        entry,
      }),
    );
    await this.store.append(this.workspaceId, orcaSessionId, events);
    if (subpath) {
      this.markSubpathActive(orcaSessionId, subpath);
    }
  }

  private async withSessionAppendLock<T>(orcaSessionId: string, fn: () => Promise<T>): Promise<T> {
    const previous = this.appendLocks.get(orcaSessionId) ?? Promise.resolve();
    const runAfterPrevious = previous.catch(() => undefined);
    let release!: () => void;
    const current = runAfterPrevious.then(
      () =>
        new Promise<void>((resolve) => {
          release = resolve;
        }),
    );
    this.appendLocks.set(orcaSessionId, current);
    await runAfterPrevious;
    try {
      return await fn();
    } finally {
      release();
      if (this.appendLocks.get(orcaSessionId) === current) {
        this.appendLocks.delete(orcaSessionId);
      }
    }
  }

  async load(key: SessionKey): Promise<SessionStoreEntry[] | null> {
    const subpath = key.subpath ?? '';
    const orcaSessionId = this.resolveOrcaSessionId(key.sessionId);
    const out: SessionStoreEntry[] = [];
    for await (const e of this.store.read(this.workspaceId, orcaSessionId, {
      fromCursor: '',
      maxEvents: 0,
      subpath,
    })) {
      if (e.producedBy !== 'harness' || !isClaudeSessionEntryEvent(e)) continue;
      out.push(eventToEntry(e));
    }
    return out.length === 0 ? null : out;
  }

  /**
   * Cheap existence probe: does the session already hold at least one persisted
   * SDK transcript entry on the given subpath (default = main transcript)?
   *
   * The Claude harness uses this to decide whether a turn must `resume` the SDK
   * session (an existing session) versus create it (the first turn). Unlike
   * {@link load} it early-returns on the first matching event instead of
   * materializing the whole transcript. Takes the orca session id directly
   * (callers already hold it) rather than an SDK `SessionKey`.
   */
  async hasClaudeTranscript(orcaSessionId: string, subpath = ''): Promise<boolean> {
    for await (const e of this.store.read(this.workspaceId, orcaSessionId, {
      fromCursor: '',
      maxEvents: 0,
      subpath,
    })) {
      if (e.producedBy === 'harness' && isClaudeSessionEntryEvent(e)) return true;
    }
    return false;
  }

  async listSubkeys(key: SessionKey): Promise<string[]> {
    const orcaSessionId = this.resolveOrcaSessionId(key.sessionId);
    const subpaths = new Set<string>();
    for await (const e of this.store.read(this.workspaceId, orcaSessionId, {
      fromCursor: '',
      maxEvents: 0,
      subpath: '*',
    })) {
      if (!e.subpath) continue;
      subpaths.add(e.subpath);
    }
    return [...subpaths].sort();
  }

  private async assertCanAppendToSubpath(orcaSessionId: string, subpath: string): Promise<void> {
    let active = await this.activeThreadSubpaths(orcaSessionId);
    if (active.has(subpath)) return;

    // Cache keeps hot subagent transcript appends from scanning Kafka on every
    // SDK SessionStore.append(). Refresh at limit boundary so external archive
    // events can free a slot before we reject.
    if (active.size >= MAX_CONCURRENT_SESSION_THREADS) {
      active = await this.activeThreadSubpaths(orcaSessionId, { refresh: true });
      if (active.has(subpath)) return;
    }
    if (active.size >= MAX_CONCURRENT_SESSION_THREADS) {
      throw new Error(
        `maximum concurrent session threads exceeded (max ${MAX_CONCURRENT_SESSION_THREADS})`,
      );
    }
  }

  private async activeThreadSubpaths(
    orcaSessionId: string,
    opts: { refresh?: boolean } = {},
  ): Promise<Set<string>> {
    if (!opts.refresh) {
      const cached = this.activeThreadSubpathCache.get(orcaSessionId);
      if (cached) return cached;
    }

    const active = new Set<string>();
    for await (const event of this.store.read(this.workspaceId, orcaSessionId, {
      fromCursor: '',
      maxEvents: 0,
      subpath: '*',
    })) {
      if (!event.subpath) continue;
      if (event.kind === SessionThreadEventKind.statusTerminated) {
        active.delete(event.subpath);
        continue;
      }
      active.add(event.subpath);
    }
    this.activeThreadSubpathCache.set(orcaSessionId, active);
    return active;
  }

  private markSubpathActive(orcaSessionId: string, subpath: string): void {
    let active = this.activeThreadSubpathCache.get(orcaSessionId);
    if (!active) {
      active = new Set<string>();
      this.activeThreadSubpathCache.set(orcaSessionId, active);
    }
    active.add(subpath);
  }
}
