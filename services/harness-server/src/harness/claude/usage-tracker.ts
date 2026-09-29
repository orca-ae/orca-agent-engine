// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { SessionKey, SessionStoreEntry } from '@anthropic-ai/claude-agent-sdk';
import { cacheCreationUsageFromRaw } from '../../usage-normalization.js';

export interface ModelCallUsage {
  input_tokens: number;
  output_tokens: number;
  cache_read_input_tokens: number;
  cache_creation: { ephemeral_1h_input_tokens: number; ephemeral_5m_input_tokens: number };
}

type RecordValue = Record<string, unknown>;
interface ModelMessage {
  usage: RecordValue;
  model: string;
  parentToolUseId: string | undefined;
  subagentType: string | undefined;
  acknowledgment?: Promise<void>;
  toolIds: Set<string>;
  status: 'pending' | 'acknowledged';
  streamed: boolean;
  mirrored: boolean;
  finalUsageSeen: boolean;
}

/**
 * SDK assistant frames describe individual content blocks, with the initial
 * usage snapshot. Only message_stop closes a streamed provider model call.
 * Permission callbacks run independently of the iterator, so they must join
 * that model call's ACK even when its tool block has not been consumed yet.
 */
export class ClaudeUsageTracker {
  private activeByScope = new Map<string, ModelMessage>();
  private messages = new Map<string, ModelMessage>();
  private byToolId = new Map<string, ModelMessage>();
  private waiters = new Map<string, Set<(acknowledged: boolean) => void>>();
  private waiterAgentIds = new Map<string, string>();
  private parentByRuntimeAgentId = new Map<string, string>();
  private reportQueue: Promise<void> = Promise.resolve();
  private streamed = false;
  private closed = false;
  private allowEmptyCallbacks = false;
  reported = false;

  constructor(
    private readonly fallbackModel: string,
    private readonly report: (
      usage: ModelCallUsage,
      model: string,
      parentToolUseId?: string,
      subagentType?: string,
    ) => Promise<void>,
    private readonly mirroredSubagents = false,
  ) {}

  async observe(raw: unknown): Promise<void> {
    const frame = record(raw);
    if (!frame || this.closed) return;
    const scope = JSON.stringify([frame.session_id ?? '', frame.parent_tool_use_id ?? '']);
    const event = record(frame.event);
    if (frame.type === 'stream_event' && event) {
      this.streamed = true;
      if (event.type === 'message_start') {
        const message = record(event.message) ?? {};
        const key = messageKey(frame.session_id, message.id);
        const state = this.messages.get(key) ?? this.createMessage(message, frame, true);
        state.streamed = true;
        this.activeByScope.set(scope, state);
        this.messages.set(key, state);
      } else {
        const state = this.activeByScope.get(scope);
        if (!state) return;
        if (event.type === 'content_block_start')
          this.registerTool(state, record(event.content_block));
        if (event.type === 'message_delta') {
          // Values in successive deltas are cumulative, not additive. Preserve
          // fields omitted by the delta, including the initial cache TTL split.
          const usage = record(event.usage);
          state.finalUsageSeen = usage !== undefined && validTokenCount(usage.output_tokens);
          if (usage) state.usage = mergeUsage(state.usage, usage);
        }
        if (event.type === 'message_stop') {
          await this.acknowledge(state);
          this.activeByScope.delete(scope);
          // With every active stream closed, an unmatched callback cannot gain
          // a correlation later in this model call. Deny instead of deadlocking
          // the SDK, which waits for permission before finishing its query.
          if (this.activeByScope.size === 0) {
            for (const [toolId, pending] of this.waiters) {
              if (!this.byToolId.has(toolId) && !this.waiterAgentIds.has(toolId)) {
                for (const finish of pending) finish(false);
              }
            }
          }
        }
      }
    } else if (frame.type === 'assistant') {
      const message = record(frame.message);
      if (!message) return;
      const key = messageKey(frame.session_id, message.id);
      let state =
        this.messages.get(key) ??
        (message.id === undefined ? this.activeByScope.get(scope) : undefined);
      if (!state) {
        state = this.createMessage(message, frame, false);
        // Old compatibility frames may omit ids entirely; each is a complete
        // assistant response. With an id, duplicate blocks are one model call.
        if (message.id !== undefined) this.messages.set(key, state);
      }
      if (typeof frame.parent_tool_use_id === 'string')
        state.parentToolUseId ??= frame.parent_tool_use_id;
      if (Array.isArray(message.content)) {
        for (const block of message.content) this.registerTool(state, record(block));
      }
      if (!state.streamed && !(this.mirroredSubagents && state.parentToolUseId)) {
        await this.acknowledge(state);
      }
    }
  }

  async observeStoredEntries(
    key: SessionKey,
    entries: readonly SessionStoreEntry[],
  ): Promise<void> {
    if (!this.mirroredSubagents || !key.subpath || this.closed) return;
    for (const entry of entries) {
      const runtimeId =
        typeof entry.agentId === 'string'
          ? entry.agentId
          : key.subpath.replace(/^subagents\/agent-/, '');
      if (entry.type === 'agent_metadata' && typeof entry.toolUseId === 'string') {
        this.parentByRuntimeAgentId.set(runtimeId, entry.toolUseId);
      }
      if (entry.type !== 'assistant') continue;
      const message = record(entry.message);
      if (!message || typeof message.id !== 'string') continue;
      const messageId = messageKey(key.sessionId, message.id);
      let state = this.messages.get(messageId);
      if (!state) {
        state = this.createMessage(message, {}, false);
        this.messages.set(messageId, state);
      }
      state.mirrored = true;
      if (Array.isArray(message.content)) {
        for (const block of message.content) this.registerTool(state, record(block));
      }
      if (state.status === 'acknowledged' || !message.stop_reason) continue;
      state.finalUsageSeen = true;
      state.usage = record(message.usage) ?? {};
      if (typeof message.model === 'string') state.model = message.model;
      state.parentToolUseId ??= this.parentByRuntimeAgentId.get(runtimeId);
      if (typeof entry.attributionAgent === 'string') state.subagentType = entry.attributionAgent;
      await this.acknowledge(state);
      // A child callback can arrive before its forwarded assistant frame. The
      // final persisted message is authoritative for all tool ids in that call.
      for (const [toolId, pending] of this.waiters) {
        if (this.waiterAgentIds.get(toolId) === runtimeId && !this.byToolId.has(toolId)) {
          for (const finish of pending) finish(false);
        }
      }
    }
  }

  waitForTool(toolUseId: string, signal: AbortSignal, runtimeAgentId?: string): Promise<boolean> {
    if (signal.aborted) return Promise.resolve(false);
    if (this.closed) return Promise.resolve(this.allowEmptyCallbacks);
    const state = this.byToolId.get(toolUseId);
    const awaitingSubagentMirror = this.mirroredSubagents && runtimeAgentId !== undefined;
    // Compatibility adapters without model frames expose a standalone policy
    // callback. They have no model usage lifecycle for a tool to join.
    if (!state && !this.streamed && !this.reported && !awaitingSubagentMirror)
      return Promise.resolve(true);
    if (state?.status === 'acknowledged') return Promise.resolve(true);
    if (!state && this.streamed && this.activeByScope.size === 0 && !awaitingSubagentMirror)
      return Promise.resolve(false);
    // Assistant-only frames cannot correlate tools omitted from their content.
    if (!state && !this.streamed && !awaitingSubagentMirror) return Promise.resolve(true);
    return new Promise<boolean>((resolve) => {
      const finish = (acknowledged: boolean): void => {
        signal.removeEventListener('abort', abort);
        const pending = this.waiters.get(toolUseId);
        pending?.delete(finish);
        if (pending?.size === 0) {
          this.waiters.delete(toolUseId);
          this.waiterAgentIds.delete(toolUseId);
        }
        resolve(acknowledged);
      };
      const abort = (): void => finish(false);
      const pending = this.waiters.get(toolUseId) ?? new Set();
      pending.add(finish);
      this.waiters.set(toolUseId, pending);
      if (runtimeAgentId) this.waiterAgentIds.set(toolUseId, runtimeAgentId);
      signal.addEventListener('abort', abort, { once: true });
    });
  }

  /** Query failure, normal completion and cancellation all retire unknown ids. */
  close(allowEmptyCallbacks = false): void {
    this.allowEmptyCallbacks =
      !this.closed && allowEmptyCallbacks && !this.streamed && !this.reported;
    this.closed = true;
    for (const pending of this.waiters.values()) {
      for (const finish of pending) finish(false);
    }
    this.waiters.clear();
    this.waiterAgentIds.clear();
    this.parentByRuntimeAgentId.clear();
    this.activeByScope.clear();
    this.messages.clear();
    this.byToolId.clear();
  }

  private createMessage(message: RecordValue, frame: RecordValue, streamed: boolean): ModelMessage {
    return {
      usage: record(message.usage) ?? {},
      model:
        typeof message.model === 'string' && message.model ? message.model : this.fallbackModel,
      parentToolUseId:
        typeof frame.parent_tool_use_id === 'string' ? frame.parent_tool_use_id : undefined,
      subagentType: undefined,
      toolIds: new Set(),
      status: 'pending',
      streamed,
      mirrored: false,
      finalUsageSeen: false,
    };
  }

  private registerTool(state: ModelMessage, block: RecordValue | undefined): void {
    if (block?.type !== 'tool_use' || typeof block.id !== 'string') return;
    state.toolIds.add(block.id);
    this.byToolId.set(block.id, state);
    if (state.status === 'acknowledged') this.release(block.id);
  }

  private release(toolId: string): void {
    for (const finish of this.waiters.get(toolId) ?? []) finish(!this.closed);
  }

  private acknowledge(state: ModelMessage): Promise<void> {
    return (state.acknowledgment ??= this.reportMessage(state));
  }

  private async reportMessage(state: ModelMessage): Promise<void> {
    if (state.status === 'acknowledged') return;
    if (
      (state.streamed || state.mirrored) &&
      (!state.finalUsageSeen || !validFinalUsage(state.usage))
    ) {
      this.close();
      throw new Error('Claude model message has missing or invalid final usage.');
    }
    const usage = normalizeModelCallUsage(state.usage)!;
    if (
      Object.values(usage).some((value) => typeof value === 'number' && value > 0) ||
      usage.cache_creation.ephemeral_1h_input_tokens +
        usage.cache_creation.ephemeral_5m_input_tokens >
        0
    ) {
      this.reported = true;
      // Eager mirror callbacks and the main iterator run independently. Keep
      // authoritative ACKs in order so a slower old total cannot replace a new one.
      const reporting = this.reportQueue.then(() =>
        this.report(usage, state.model, state.parentToolUseId, state.subagentType),
      );
      this.reportQueue = reporting.catch(() => undefined);
      try {
        await reporting;
      } catch (error) {
        this.close();
        throw error;
      }
    } else {
      // Zero usage adds no report, but its tools must still see all preceding
      // acknowledged spend. Failure/cancellation closes these waiters too.
      await this.reportQueue;
    }
    if (this.closed) return;
    state.status = 'acknowledged';
    for (const toolId of state.toolIds) this.release(toolId);
  }
}

export function normalizeModelCallUsage(raw: unknown): ModelCallUsage | null {
  const usage = record(raw);
  if (!usage) return null;
  return {
    input_tokens: tokenCount(usage.input_tokens),
    output_tokens: tokenCount(usage.output_tokens),
    cache_read_input_tokens: tokenCount(usage.cache_read_input_tokens),
    cache_creation: cacheCreationUsageFromRaw(usage),
  };
}

function mergeUsage(previous: RecordValue, next: RecordValue): RecordValue {
  const merged = { ...previous, ...next };
  // SDK cumulative input/cache fields are nullable. A null delta carries no
  // replacement count; preserve the known initial value, including zero.
  for (const key of [
    'input_tokens',
    'cache_read_input_tokens',
    'cache_creation_input_tokens',
    'cache_creation',
  ]) {
    if (next[key] === null && previous[key] !== undefined) merged[key] = previous[key];
  }
  const cache = record(next.cache_creation);
  if (cache) merged.cache_creation = { ...record(previous.cache_creation), ...cache };
  return merged;
}

function validFinalUsage(usage: RecordValue): boolean {
  if (!validTokenCount(usage.input_tokens) || !validTokenCount(usage.output_tokens)) return false;
  for (const field of ['cache_read_input_tokens', 'cache_creation_input_tokens']) {
    if (usage[field] != null && !validTokenCount(usage[field])) return false;
  }
  if (usage.cache_creation != null) {
    const cache = record(usage.cache_creation);
    if (
      !cache ||
      !validTokenCount(cache.ephemeral_1h_input_tokens) ||
      !validTokenCount(cache.ephemeral_5m_input_tokens)
    )
      return false;
  }
  return true;
}

function validTokenCount(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function messageKey(sessionId: unknown, id: unknown): string {
  return JSON.stringify([sessionId ?? '', id ?? '']);
}

function record(value: unknown): RecordValue | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as RecordValue)
    : undefined;
}

function tokenCount(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? Math.floor(value) : 0;
}

/** Stop waiting for an in-flight Registry ACK, while handling its late outcome. */
export function awaitUsageAcknowledgment<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return promise;
  return new Promise<T>((resolve, reject) => {
    const abort = (): void => reject(new Error('Usage acknowledgment interrupted.'));
    if (signal.aborted) abort();
    else signal.addEventListener('abort', abort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', abort));
  });
}
