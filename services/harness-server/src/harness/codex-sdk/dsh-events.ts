// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  AgentEventKind,
  AgentThreadEventKind,
  SessionThreadEventKind,
  SpanEventKind,
  isAgentEventId,
  stableSessionThreadId,
} from '@orca/agent-event-contract';
import type { WorkerEvent, WorkerEventOrigin } from '@orca/sdk-harness';

const childPath = /^subagents[/][0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const boundedString = (value: unknown, max: number): value is string =>
  typeof value === 'string' && value.trim().length > 0 && value.length <= max;
const sequence = (value: unknown): value is number =>
  typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
export function nativeThreadId(subpath: string): string | undefined {
  if (subpath === '') return undefined;
  if (!childPath.test(subpath)) throw new Error('DSH requires a native UUID thread subpath');
  return subpath.slice('subagents/'.length);
}

/** The worker may publish tree facts, never host accounting or Session control. */
export class DshEventProjection {
  private readonly parents = new Map<string, string>([['', '']]);
  private readonly summaries = new Map<string, string>();
  private readonly tools = new Map<string, string>();
  private readonly emitted = new Set<string>();
  private readonly authorizations = new Map<
    string,
    { subpath: string; name: string; input: string }
  >();
  private readonly nativeCalls = new Map<
    string,
    { subpath: string; name: string; input: string }
  >();

  constructor(
    private readonly workspaceId: string,
    private readonly sessionId: string,
    private readonly orchestrationTools: readonly string[],
  ) {}

  beginTurn(): void {
    this.emitted.clear();
    this.summaries.clear();
    this.tools.clear();
    this.authorizations.clear();
    this.nativeCalls.clear();
  }

  /** Restore already-persisted lineage without publishing new creation events. */
  restoreTree(threads: Array<{ subpath: string; parentSubpath: string }>): void {
    if (this.parents.size !== 1 || !Array.isArray(threads) || threads.length > 127)
      throw new Error('Invalid DSH restored tree');
    for (const entry of threads) {
      if (
        !entry ||
        typeof entry !== 'object' ||
        !nativeThreadId(entry.subpath) ||
        !this.parents.has(entry.parentSubpath) ||
        this.parents.has(entry.subpath)
      )
        throw new Error('Invalid DSH restored lineage');
      this.parents.set(entry.subpath, entry.parentSubpath);
    }
  }

  subtree(subpath: string): string[] {
    return [...this.parents.keys()].filter((candidate) => {
      while (candidate !== subpath && candidate) candidate = this.parents.get(candidate) ?? '';
      return candidate === subpath;
    });
  }

  parentSubpath(subpath: string): string {
    const parent = this.parents.get(subpath);
    if (parent === undefined) throw new Error('DSH thread is outside the declared tree');
    return parent;
  }

  assertOrigin(origin: WorkerEventOrigin | undefined, claim = true): WorkerEventOrigin {
    if (!origin || !isAgentEventId(origin.eventId))
      throw new Error('DSH tool requires canonical origin');
    if (claim && this.emitted.has(origin.eventId))
      throw new Error('Duplicate DSH managed tool identity');
    nativeThreadId(origin.subpath);
    if (!this.parents.has(origin.subpath))
      throw new Error('DSH origin is outside the declared tree');
    if (
      origin.parentSubpath !== undefined &&
      this.parents.get(origin.subpath) !== origin.parentSubpath
    )
      throw new Error('DSH origin parent mismatch');
    if (claim) this.emitted.add(origin.eventId);
    return origin;
  }

  authorize(event: Extract<WorkerEvent, { type: 'orchestration_request' }>): void {
    const origin = this.assertOrigin(event.origin, false);
    if (this.authorizations.has(origin.eventId)) throw new Error('Duplicate DSH authorization');
    if (this.emitted.has(origin.eventId) && !this.tools.has(origin.eventId))
      throw new Error('DSH authorization is not bound to an active canonical call');
    if (!this.orchestrationTools.includes(event.name)) throw new Error('Unsupported DSH operation');
    const call = this.nativeCalls.get(origin.eventId);
    if (
      call &&
      (call.subpath !== origin.subpath ||
        call.name !== event.name ||
        call.input !== JSON.stringify(event.arguments))
    )
      throw new Error('DSH authorization differs from canonical tool');
    this.authorizations.set(origin.eventId, {
      subpath: origin.subpath,
      name: event.name,
      input: JSON.stringify(event.arguments),
    });
  }

  validate(event: Extract<WorkerEvent, { type: 'canonical_event' }>['event'], turnId: string) {
    const { id, subpath, kind, payload } = event;
    if (!isAgentEventId(id)) throw new Error('DSH canonical event requires evt_ ID');
    if (this.emitted.has(id)) throw new Error('Duplicate DSH canonical event identity');
    nativeThreadId(subpath);
    if (!payload || typeof payload !== 'object' || Array.isArray(payload))
      throw new Error('Invalid DSH canonical payload');
    if (payload.turn_event_id !== turnId)
      throw new Error('DSH canonical source is outside the current turn');
    if (Buffer.byteLength(JSON.stringify(payload)) > 1024 * 1024)
      throw new Error('DSH event exceeds size limit');
    if (kind === SessionThreadEventKind.created) {
      const parent = payload.parent_subpath;
      if (
        !subpath ||
        typeof parent !== 'string' ||
        !this.parents.has(parent) ||
        parent === subpath ||
        payload.subpath !== subpath ||
        (!this.parents.has(subpath) && this.parents.size >= 128) ||
        (this.parents.has(subpath) && this.parents.get(subpath) !== parent)
      )
        throw new Error('Invalid DSH thread declaration');
    } else if (!this.parents.has(subpath))
      throw new Error('DSH event is outside the declared tree');
    if (
      Object.values(SessionThreadEventKind).includes(kind as typeof SessionThreadEventKind.created)
    ) {
      if (
        !subpath ||
        payload.subpath !== subpath ||
        (kind !== SessionThreadEventKind.created &&
          payload.parent_subpath !== this.parents.get(subpath))
      )
        throw new Error('Invalid DSH thread lifecycle');
      if (
        (payload.agent_name !== undefined && !boundedString(payload.agent_name, 256)) ||
        (payload.stop_reason !== undefined && !boundedString(payload.stop_reason, 128))
      )
        throw new Error('Invalid DSH thread lifecycle fields');
      if (payload.native_reason !== undefined) {
        const reason = payload.native_reason;
        if (
          typeof reason !== 'string' ||
          ![
            'completed',
            'aborted',
            'interrupted',
            'error',
            'blocked',
            'max-tokens',
            'forked',
          ].includes(reason) ||
          kind !== SessionThreadEventKind.statusIdle ||
          payload.stop_reason !== (reason === 'aborted' ? 'interrupted' : reason)
        )
          throw new Error('Invalid DSH native turn reason');
      }
    } else if (kind === AgentEventKind.threadContextCompacted) {
      const source = payload.native_source as Record<string, unknown> | undefined;
      const seqs = payload.source_event_seqs;
      if (
        !boundedString(payload.native_compaction_id, 128) ||
        !source ||
        Array.isArray(source) ||
        !boundedString(source.session_id, 128) ||
        source.session_id !== payload.native_session_id ||
        (subpath !== '' && source.session_id !== nativeThreadId(subpath)) ||
        Object.keys(source).some((key) => key !== 'session_id' && key !== 'seq') ||
        !sequence(source.seq) ||
        !Array.isArray(seqs) ||
        seqs.length !== 3 ||
        !seqs.every(sequence) ||
        !(seqs[0] < seqs[1] && seqs[1] < seqs[2]) ||
        seqs[2] !== source.seq ||
        Object.keys(payload).some(
          (key) =>
            ![
              'native_compaction_id',
              'native_source',
              'source_event_seqs',
              'native_session_id',
              'source_event_id',
              'turn_event_id',
              'subpath',
              'parent_subpath',
            ].includes(key),
        )
      )
        throw new Error('Invalid DSH compaction fact');
    } else if (kind === AgentEventKind.message || kind === AgentEventKind.thinking) {
      if (
        !Array.isArray(payload.content) ||
        !payload.content.length ||
        payload.content.some((block) => {
          if (!block || typeof block !== 'object') return true;
          const b = block as Record<string, unknown>;
          return kind === AgentEventKind.message
            ? b.type !== 'text' || typeof b.text !== 'string'
            : b.type !== 'thinking' || typeof b.thinking !== 'string';
        })
      )
        throw new Error('Invalid DSH message content');
    } else if (kind === AgentEventKind.toolUse) {
      const approved = this.authorizations.get(id);
      if (
        approved &&
        (approved.subpath !== subpath ||
          approved.name !== payload.name ||
          approved.input !== JSON.stringify(payload.input))
      )
        throw new Error('DSH canonical tool differs from authorized operation');
      if (
        typeof payload.name !== 'string' ||
        !this.orchestrationTools.includes(payload.name) ||
        !payload.input ||
        typeof payload.input !== 'object' ||
        Array.isArray(payload.input)
      )
        throw new Error('DSH canonical tool is not an approved orchestration tool');
      this.tools.set(id, subpath);
      this.nativeCalls.set(id, {
        subpath,
        name: payload.name as string,
        input: JSON.stringify(payload.input),
      });
    } else if (kind === AgentEventKind.toolResult) {
      if (
        typeof payload.tool_use_id !== 'string' ||
        this.tools.get(payload.tool_use_id) !== subpath ||
        !Array.isArray(payload.content) ||
        typeof payload.is_error !== 'boolean'
      )
        throw new Error('Invalid DSH orchestration result');
      this.tools.delete(payload.tool_use_id);
    } else if (
      Object.values(AgentThreadEventKind).includes(kind as typeof AgentThreadEventKind.messageSent)
    ) {
      if (!Array.isArray(payload.content)) throw new Error('Invalid DSH thread message');
      for (const address of ['sender_subpath', 'recipient_subpath']) {
        const target = payload[address];
        if (typeof target !== 'string' || !this.parents.has(target))
          throw new Error('DSH thread message outside declared tree');
      }
      if (payload.message_origin !== undefined) {
        const sender = payload.sender_subpath as string;
        const content = payload.content as Array<Record<string, unknown>>;
        if (
          payload.message_origin !== 'runtime_settlement' ||
          kind !== AgentThreadEventKind.messageReceived ||
          payload.recipient_subpath !== subpath ||
          !sender ||
          this.parents.get(sender) !== subpath ||
          !boundedString(payload.native_message_id, 128) ||
          content.length !== 1 ||
          content[0]?.type !== 'text' ||
          content[0]?.text !== 'Child ' + nativeThreadId(sender) + ' settled.' ||
          Object.keys(content[0]!).some((key) => key !== 'type' && key !== 'text') ||
          Object.keys(payload).some(
            (key) =>
              ![
                'native_message_id',
                'message_origin',
                'sender_subpath',
                'recipient_subpath',
                'content',
                'native_session_id',
                'source_event_id',
                'turn_event_id',
                'subpath',
                'parent_subpath',
              ].includes(key),
          )
        )
          throw new Error('Invalid DSH runtime settlement notice');
      }
    } else if (kind === SpanEventKind.modelRequestStart || kind === SpanEventKind.modelRequestEnd) {
      if (payload.model_observation_kind !== 'turn_model_summary')
        throw new Error('Invalid DSH model summary');
      if (kind === SpanEventKind.modelRequestStart) this.summaries.set(id, subpath);
      else {
        const usage = payload.model_usage as Record<string, unknown> | undefined;
        if (
          typeof payload.model_request_start_id !== 'string' ||
          this.summaries.get(payload.model_request_start_id) !== subpath ||
          !usage ||
          ![
            'input_tokens',
            'output_tokens',
            'cache_read_input_tokens',
            'cache_creation_input_tokens',
          ].every(
            (key) =>
              typeof usage[key] === 'number' &&
              Number.isSafeInteger(usage[key]) &&
              (usage[key] as number) >= 0,
          ) ||
          typeof payload.is_error !== 'boolean'
        )
          throw new Error('Invalid DSH model usage');
        this.summaries.delete(payload.model_request_start_id);
      }
    } else throw new Error('DSH canonical event kind is not allowlisted');
    const sessionThreadId = stableSessionThreadId(this.workspaceId, this.sessionId, subpath);
    if (payload.session_thread_id !== undefined && payload.session_thread_id !== sessionThreadId)
      throw new Error('DSH public thread ID mismatch');
    if (kind === SessionThreadEventKind.created)
      this.parents.set(subpath, payload.parent_subpath as string);
    this.emitted.add(id);
    return {
      ...event,
      payload: {
        ...payload,
        session_thread_id: sessionThreadId,
        ...(typeof payload.sender_subpath === 'string'
          ? {
              sender_thread_id: stableSessionThreadId(
                this.workspaceId,
                this.sessionId,
                payload.sender_subpath,
              ),
            }
          : {}),
        ...(typeof payload.recipient_subpath === 'string'
          ? {
              recipient_thread_id: stableSessionThreadId(
                this.workspaceId,
                this.sessionId,
                payload.recipient_subpath,
              ),
            }
          : {}),
        ...(subpath
          ? {
              parent_thread_id: stableSessionThreadId(
                this.workspaceId,
                this.sessionId,
                this.parents.get(subpath)!,
              ),
            }
          : {}),
      },
    };
  }
}
