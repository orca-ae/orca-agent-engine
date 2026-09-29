// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Event } from './types.js';

export interface ReadOptions {
  /** "" = beginning. Otherwise the offset string returned via `Event.seq`. */
  fromCursor: string;
  /** 0 = unbounded (Read uses bounded mode internally). */
  maxEvents: number;
  /** "" = parent-only, "*" = all, exact string = match one subpath. */
  subpath: string;
}

export interface TailOptions {
  /**
   * "" = from-now: the tail subscribes at the transcript's CURRENT head and
   * delivers only events appended after the call (it does NOT replay history).
   * Otherwise the offset string returned via `Event.seq`, from which the tail
   * follows forward (inclusive). This is a cross-backend contract: the Kafka
   * (seek to high-watermark), Pulsar ('Latest'), and Postgres (highWatermark+1)
   * implementations all honor "" as from-now. Use `read` for a historical
   * catch-up — `read`'s "" cursor means from-the-beginning.
   */
  fromCursor: string;
  /** "" = parent-only, "*" = all, exact string = match one subpath. */
  subpath: string;
  /** Optional signal fired once the tail is positioned and ready for new events. */
  onReady?: () => void;
  /** Optional caller-provided cancellation. */
  signal?: AbortSignal;
}

/**
 * Append-only session/event log.  Implementations are pluggable:
 * `@orca/transcript-store` ships Kafka (the default), Postgres and Pulsar
 * backends.
 */
export interface TranscriptStore {
  /** Append a batch.  Returns one event id per input position (existing ids round-trip on dedup). */
  append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]>;
  /** Bounded read; ends when the high-watermark at call time is drained. */
  read(workspaceId: string, sessionId: string, opts: ReadOptions): AsyncIterable<Event>;
  /** Unbounded follow; runs until cancelled via `opts.signal`. */
  tail(workspaceId: string, sessionId: string, opts: TailOptions): AsyncIterable<Event>;
  /** Emit a `session.archived` sentinel event.  Topic deletion is a platform concern. */
  archive(workspaceId: string, sessionId: string): Promise<void>;
  /** Release any underlying connections. */
  close(): Promise<void>;
}

export type { Event };
