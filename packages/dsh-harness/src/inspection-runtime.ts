// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Context } from '@deepseek-ai/cordis';
import { SessionId, type Session } from '@deepseek-ai/dsh-session';
import type {} from '@deepseek-ai/dsh-agent-loop';
import type {} from '@deepseek-ai/dsh-session-query';
import { randomUUID } from 'node:crypto';
import { listChildPage, readChildPage, type ChildInspection } from './inspection.js';

export type DeclaredChild = { parent: string; key: number; label?: string };
const MAX_LIVE_EVENTS = 65_536;

/** Reads only the established owner catalog and attached native sources; never activates children. */
export class ChildInspectionRuntime {
  generation = randomUUID();
  private readonly sources = new WeakMap<Session, string>();

  constructor(
    private readonly ctx: Pick<Context, 'agents' | 'sessions' | 'sessionQuery'>,
    private readonly children: ReadonlyMap<string, DeclaredChild>,
    private readonly isArchived: (id: string) => boolean,
    private readonly stopReasons: ReadonlyMap<string, string>,
  ) {}

  isDescendant(callerId: string, target: string): boolean {
    const seen = new Set<string>();
    let current = target;
    while (!seen.has(current)) {
      seen.add(current);
      const child = this.children.get(current);
      if (!child) return false;
      if (child.parent === callerId) return true;
      current = child.parent;
    }
    return false;
  }

  assertDescendant(callerId: string, target: string): void {
    if (target === callerId || !this.isDescendant(callerId, target))
      throw new Error('DSH inspection target outside owned authority');
  }

  private child(id: string): ChildInspection {
    const row = this.children.get(id)!;
    const agent = this.ctx.agents.get(SessionId(id));
    const session = this.ctx.sessions.get(SessionId(id));
    const reason = !session
      ? 'source_not_attached'
      : session.seq > MAX_LIVE_EVENTS
        ? 'source_too_large'
        : undefined;
    const stop = this.stopReasons.get(id);
    return {
      agent_id: id,
      parent_agent_id: row.parent,
      state: agent ? (agent.status ?? 'unknown') : 'dormant',
      archived: this.isArchived(id),
      history_availability: reason ? 'unavailable' : 'available',
      ...(row.label === undefined ? {} : { label: row.label }),
      ...(stop === undefined ? {} : { last_stop_reason: stop }),
      ...(reason ? { history_reason: reason } : {}),
    };
  }

  list(callerId: string, checkCaller: () => void, continuation?: string): string {
    checkCaller();
    const children = [...this.children]
      .filter(([id]) => id !== callerId && this.isDescendant(callerId, id))
      .map(([id, row]) => ({ key: row.key, child: this.child(id) }));
    const result = listChildPage({
      callerId,
      generation: this.generation,
      children,
      catalogComplete: true,
      ...(continuation === undefined ? {} : { continuation }),
    });
    checkCaller();
    return result;
  }

  async read(
    callerId: string,
    target: string,
    checkCaller: () => void,
    signal: AbortSignal,
    continuation?: string,
  ): Promise<string> {
    const check = () => {
      signal.throwIfAborted();
      checkCaller();
      this.assertDescendant(callerId, target);
    };
    check();
    const child = this.child(target);
    const session = this.ctx.sessions.get(SessionId(target));
    if (!session || session.seq > MAX_LIVE_EVENTS) {
      const result = readChildPage({
        callerId,
        child,
        ...(continuation === undefined ? {} : { continuation }),
      });
      check();
      return result;
    }
    // Pinned query 0.2.1-alpha.2 chooses the live source before its first await.
    const observation = await this.ctx.sessionQuery
      .observeSession(SessionId(target), {
        signal,
        projectionMode: 'none',
      })
      .catch((error: unknown) => {
        check();
        const code = (error as { code?: string } | null)?.code;
        throw new Error(
          code === 'SESSION_QUERY_CORRUPT_SESSION'
            ? 'DSH child history is corrupt'
            : 'DSH child history observation failed',
        );
      });
    try {
      check();
      if (this.ctx.sessions.get(SessionId(target)) !== session || observation.source !== 'live')
        throw new Error('DSH child history source changed; retry without continuation');
      if (observation.cursor + 1 > MAX_LIVE_EVENTS) {
        const result = readChildPage({
          callerId,
          child: {
            ...child,
            history_availability: 'unavailable',
            history_reason: 'source_too_large',
          },
          ...(continuation === undefined ? {} : { continuation }),
        });
        check();
        return result;
      }
      let generation = this.sources.get(session);
      if (!generation) {
        generation = randomUUID();
        this.sources.set(session, generation);
      }
      const result = readChildPage({
        callerId,
        child,
        ...(continuation === undefined ? {} : { continuation }),
        source: {
          generation,
          events: observation.events,
          inheritedEventCount: observation.inheritedEventCount,
          cursor: observation.cursor,
        },
      });
      check();
      return result;
    } finally {
      observation[Symbol.dispose]();
    }
  }
}
