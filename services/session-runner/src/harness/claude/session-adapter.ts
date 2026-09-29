// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Implements the Claude Agent SDK's `SessionStore` over a `TranscriptStore`.
//
// The SDK reloads a session's prior conversation at the start of each turn via
// `load()` and persists every new entry via `append()`. Backing those by the
// transcript-store means the SDK's history IS the Orca transcript — the same log
// the registry's SSE bridge streams — so a turn driven by this runner reads back
// exactly what earlier turns (on this runner or the harness-server) wrote.
//
// SDK sessionId vs Orca session_id. The SDK's `Options.sessionId` must be a UUID;
// Orca session ids are `ses_<base32>`. The harness derives a deterministic UUID
// from the orca id and the SDK then keys its `append`/`load` calls by that UUID.
// This adapter tracks the (uuid → orca-id) mapping so the transcript append still
// lands in the orca-id-keyed topic; without it the SDK's writes would miss the
// topic the bridge tails. Mirrors the harness-server adapter exactly.

import type { TranscriptStore } from '@orca/transcript-store-types';
import type { SessionKey, SessionStore, SessionStoreEntry } from '@anthropic-ai/claude-agent-sdk';
import { entryToEvent, eventToEntry, isClaudeSessionEntryEvent } from './event-mapper.js';

/**
 * A `SessionStore` whose entries live in `store`, scoped to one workspace. Optional
 * SDK methods (`delete`, `listSessions`, …) are intentionally omitted — the SDK
 * contract permits omission and Orca's lifecycle does not route through them.
 */
export class ClaudeAgentSdkAdapter implements SessionStore {
  /**
   * SDK-side UUID sessionId → Orca-side session_id, populated by
   * {@link registerSession} before the first `query()`. Both `append` and `load`
   * resolve `key.sessionId` against this map; an unmapped key falls back to the
   * key verbatim (so a test that passes an orca id directly still works).
   */
  private readonly sdkToOrca = new Map<string, string>();

  constructor(
    private readonly store: TranscriptStore,
    private readonly workspaceId: string,
  ) {}

  /** Register an SDK ↔ Orca session_id mapping (called by the harness each turn). */
  registerSession(sdkSessionId: string, orcaSessionId: string): void {
    this.sdkToOrca.set(sdkSessionId, orcaSessionId);
  }

  /** Resolve an SDK key to the orca-side session id. Verbatim when unmapped. */
  private resolveOrcaSessionId(sdkSessionId: string): string {
    return this.sdkToOrca.get(sdkSessionId) ?? sdkSessionId;
  }

  async append(key: SessionKey, entries: SessionStoreEntry[]): Promise<void> {
    if (entries.length === 0) {
      return;
    }
    const subpath = key.subpath ?? '';
    const orcaSessionId = this.resolveOrcaSessionId(key.sessionId);
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
      if (e.producedBy !== 'harness' || !isClaudeSessionEntryEvent(e)) {
        continue;
      }
      out.push(eventToEntry(e));
    }
    return out.length === 0 ? null : out;
  }
}
