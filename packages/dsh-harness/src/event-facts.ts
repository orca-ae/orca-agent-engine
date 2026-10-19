// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Only bounded structural facts cross the native transcript boundary. */
export function catalogName(label: unknown): { agent_name?: string } {
  if (label === undefined) return {};
  if (typeof label !== 'string' || !label.trim() || label.length > 256)
    throw new Error('Invalid DSH catalog label');
  return { agent_name: label };
}

export function turnEndPayload(reason: { kind: string }): Record<string, string> {
  const kinds = ['completed', 'aborted', 'interrupted', 'error', 'blocked', 'max-tokens', 'forked'];
  if (!kinds.includes(reason.kind)) throw new Error('Unsupported DSH turn reason');
  return {
    stop_reason: reason.kind === 'aborted' ? 'interrupted' : reason.kind,
    native_reason: reason.kind,
  };
}

export function isSurfaceReplacement(event: { surfaceOp?: unknown }): boolean {
  return (event.surfaceOp as { op?: unknown } | undefined)?.op === 'replace';
}

type NativeFact = { seq: number; type: string; data: object };
type PendingCompaction = {
  id: string;
  turn: unknown;
  sourceCommandId: unknown;
  start: number;
  summary?: number;
};

/** One native compaction lock per session, discarded at every turn boundary. */
export class CompactionFacts {
  private readonly pending = new Map<string, PendingCompaction>();

  clear(): void {
    this.pending.clear();
  }

  accept(sessionId: string, event: NativeFact): Record<string, unknown> | undefined {
    if (event.type === 'turn/end') {
      this.pending.delete(sessionId);
      return;
    }
    if (!['compaction/start', 'compaction/summary', 'compaction/end'].includes(event.type)) return;
    const data = event.data as Record<string, unknown>;
    const { compactionId, turn, sourceCommandId } = data;
    if (typeof compactionId !== 'string' || !compactionId || compactionId.length > 128)
      throw new Error('Invalid DSH compaction ID');
    if (
      !Number.isSafeInteger(event.seq) ||
      event.seq < 0 ||
      (sourceCommandId !== undefined &&
        (typeof sourceCommandId !== 'string' ||
          !sourceCommandId ||
          sourceCommandId.length > 128)) ||
      (event.type !== 'compaction/summary' &&
        turn !== null &&
        !(typeof turn === 'number' && Number.isSafeInteger(turn) && turn >= 0))
    )
      throw new Error('Invalid DSH compaction attribution');
    if (event.type === 'compaction/start') {
      if (!this.pending.has(sessionId) && this.pending.size >= 128)
        throw new Error('DSH compaction tree exceeds limit');
      this.pending.set(sessionId, { id: compactionId, turn, sourceCommandId, start: event.seq });
      return;
    }
    const pending = this.pending.get(sessionId);
    if (event.type === 'compaction/end') this.pending.delete(sessionId);
    if (!pending || pending.id !== compactionId || pending.sourceCommandId !== sourceCommandId)
      return;
    if (event.type === 'compaction/summary') {
      if (event.seq > pending.start) pending.summary = event.seq;
      return;
    }
    if (
      data.error !== undefined ||
      turn !== pending.turn ||
      pending.summary === undefined ||
      event.seq <= pending.summary
    )
      return;
    return {
      native_compaction_id: compactionId,
      native_source: { session_id: sessionId, seq: event.seq },
      source_event_seqs: [pending.start, pending.summary, event.seq],
    };
  }
}
