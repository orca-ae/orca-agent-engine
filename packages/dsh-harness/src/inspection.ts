// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export interface ChildInspection {
  agent_id: string;
  parent_agent_id: string;
  state: 'running' | 'waiting_for_approval' | 'idle' | 'dormant' | 'terminated' | 'unknown';
  archived: boolean | null;
  history_availability: 'available' | 'partial' | 'unavailable';
  label?: string;
  last_stop_reason?: string;
  history_reason?: string;
}

type Event = { seq: number; type: string; data: unknown; surfaceOp?: unknown };
type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
type Row = { [key: string]: Json };
const MAX_BYTES = 32_768;
const MAX_CHARS = 65_536;
const MAX_NODES = 4_096;
const MAX_CURSOR = 2_048;
const RETRY = 'Invalid inspection continuation; retry without continuation';

class WorkLimit extends Error {}

/** Counts source inspection, including filters; never enumerates source object keys. */
class Budget {
  chars: number;
  nodes = 32;
  exhausted = false;

  constructor(reserve: string) {
    this.chars = reserve.length;
    if (this.chars >= MAX_CHARS || bytes(reserve) >= MAX_BYTES) requiredError();
  }

  visit(value: unknown): void {
    if (this.exhausted || this.nodes >= MAX_NODES) throw new WorkLimit();
    this.nodes++;
    if (typeof value === 'string') this.text(value);
  }

  text(value: string): string {
    if (this.exhausted) throw new WorkLimit();
    const count = Math.min(value.length, MAX_CHARS - this.chars);
    this.chars += count;
    this.exhausted = count < value.length;
    // Avoid leaving half of a surrogate pair in a preview.
    const end =
      count < value.length && /[\uD800-\uDBFF]/.test(value[count - 1] ?? '') ? count - 1 : count;
    return value.slice(0, end);
  }

  field(value: unknown, key: string): unknown {
    this.visit(undefined);
    this.text(key);
    if (this.exhausted) throw new WorkLimit();
    return value !== null && typeof value === 'object'
      ? (value as Record<string, unknown>)[key]
      : undefined;
  }

  string(value: unknown): string {
    this.visit(undefined);
    return typeof value === 'string' ? this.text(value) : '';
  }

  tag(value: unknown, key: string): string {
    const result = this.string(this.field(value, key));
    if (this.exhausted) throw new WorkLimit();
    return result;
  }

  requiredId(value: unknown, key: string): string {
    const result = identifier(this.field(value, key) as string);
    this.string(result);
    return result;
  }
}

function bytes(value: string): number {
  return Buffer.byteLength(value, 'utf8');
}

function requiredError(): never {
  throw new Error('Inspection required metadata exceeds bounds');
}

function identifier(value: string): string {
  // JSON escaping is at most six bytes per UTF-16 code unit.
  if (typeof value !== 'string' || !value || value.length > MAX_CURSOR) requiredError();
  return value;
}

function cursor(value: Row): string {
  const result = JSON.stringify(value);
  if (result.length > MAX_CURSOR) requiredError();
  return result;
}

function parseCursor(value: string | undefined): Row | undefined {
  if (value === undefined) return;
  if (typeof value !== 'string' || value.length > MAX_CURSOR) throw new Error(RETRY);
  try {
    const parsed: unknown = JSON.parse(value);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error(RETRY);
    return parsed as Row;
  } catch {
    throw new Error(RETRY);
  }
}

function requiredChild(child: ChildInspection): Row {
  if (
    !['running', 'waiting_for_approval', 'idle', 'dormant', 'terminated', 'unknown'].includes(
      child.state,
    ) ||
    !['available', 'partial', 'unavailable'].includes(child.history_availability) ||
    (child.archived !== null && typeof child.archived !== 'boolean')
  )
    requiredError();
  return {
    agent_id: identifier(child.agent_id),
    parent_agent_id: identifier(child.parent_agent_id),
    state: child.state,
    archived: child.archived,
    history_availability: child.history_availability,
  };
}

/** Only traverses our already bounded, allowlisted projection, never a source payload. */
function preview(value: Json, room: number, depth = 0): Json {
  if (depth >= 32) return null;
  if (typeof value === 'string') {
    let result = '';
    let used = 2;
    for (const char of value) {
      const size = bytes(JSON.stringify(char));
      if (used + size - 2 > room) break;
      used += size - 2;
      result += char;
    }
    return result;
  }
  if (Array.isArray(value)) {
    const result: Json[] = [];
    let used = 2;
    for (const item of value) {
      const available = room - used - (result.length ? 1 : 0);
      if (available < 4) break;
      const projected = preview(item, available, depth + 1);
      const size = bytes(JSON.stringify(projected));
      if (size > available) break;
      result.push(projected);
      used += size + (result.length > 1 ? 1 : 0);
    }
    return result;
  }
  if (value !== null && typeof value === 'object') {
    const result: Row = {};
    let used = 2;
    for (const key of Object.keys(value)) {
      const cost = bytes(JSON.stringify(key)) + 1 + (used > 2 ? 1 : 0);
      if (room - used - cost < 4) break;
      const projected = preview(value[key]!, room - used - cost, depth + 1);
      const size = bytes(JSON.stringify(projected));
      if (used + cost + size > room) break;
      result[key] = projected;
      used += cost + size;
    }
    return result;
  }
  return value;
}

function fit(row: Row, room: number, incomplete: boolean): Row {
  const original = JSON.stringify(row.content);
  if (!incomplete && bytes(JSON.stringify(row)) <= room) return row;
  const result = {
    ...row,
    content: '' as Json,
    truncated: true,
    original_bytes: incomplete ? null : bytes(original),
  };
  const available = room - bytes(JSON.stringify(result));
  if (available < 0) requiredError();
  result.content = preview(row.content!, available + 2);
  if (bytes(JSON.stringify(result)) > room) requiredError();
  return result;
}

function optionalMetadata(
  source: ChildInspection,
  target: Row,
  budget: Budget,
  room: number,
): void {
  for (const key of ['history_reason', 'last_stop_reason', 'label'] as const) {
    let text: string;
    try {
      const value = budget.field(source, key);
      if (value === undefined) continue;
      text = budget.string(value);
    } catch (error) {
      if (!(error instanceof WorkLimit)) throw error;
      return;
    }
    const extra = { [key]: text };
    const cost = bytes(JSON.stringify(extra)) - 2 + 1;
    if (!budget.exhausted && cost <= room) {
      target[key] = text;
      room -= cost;
      continue;
    }
    const marker = {
      truncated: true,
      original_bytes: budget.exhausted ? null : bytes(JSON.stringify(text)),
    };
    const markerCost = bytes(JSON.stringify(marker)) - 1;
    const keyCost = bytes(JSON.stringify(key)) + 2;
    if (room >= markerCost + keyCost + 2) {
      target[key] = preview(text, room - markerCost - keyCost);
      Object.assign(target, marker);
    }
    return;
  }
}

function project(event: Event, row: Row, budget: Budget): boolean {
  budget.visit(event);
  const type = budget.tag(event, 'type');
  row.kind = type;
  if (
    ![
      'user/message',
      'assistant/message',
      'tool/call',
      'tool/result',
      'turn/start',
      'turn/end',
      'step/start',
      'step/end',
    ].includes(type)
  )
    return false;
  const data = budget.field(event, 'data');
  budget.visit(data);
  if (type === 'tool/call') {
    row.tool_call_id = budget.requiredId(data, 'callId');
    const content: Row = {};
    row.content = content;
    content.name = budget.string(budget.field(data, 'name'));
    content.arguments = budget.string(budget.field(data, 'arguments'));
    return true;
  }
  if (type === 'turn/start' || type === 'turn/end' || type.startsWith('step/')) {
    const content: Row = {};
    row.content = content;
    for (const key of ['turn', 'step']) {
      const value = budget.field(data, key);
      budget.visit(value);
      if (typeof value === 'number' && Number.isSafeInteger(value)) content[key] = value;
    }
    if (type === 'turn/end') {
      const reason = budget.field(data, 'reason');
      budget.visit(reason);
      content.reason = budget.tag(reason, 'kind');
    }
    return true;
  }
  const message = type === 'user/message' ? data : budget.field(data, 'message');
  budget.visit(message);
  if (type === 'user/message') {
    const source = budget.field(message, 'source');
    budget.visit(source);
    const kind = budget.tag(source, 'kind');
    if (!['user', 'agent-message', 'subagent-settled'].includes(kind)) return false;
    if (kind !== 'user') {
      const sender = budget.field(source, 'senderSessionId');
      if (sender !== undefined) row.sender_agent_id = identifier(budget.string(sender));
    }
  }
  if (type === 'tool/result') {
    row.tool_call_id = budget.requiredId(message, 'toolCallId');
    const isError = budget.field(message, 'isError');
    budget.visit(isError);
    if (typeof isError === 'boolean') row.is_error = isError;
  }
  const blocks = budget.field(message, 'content');
  budget.visit(blocks);
  const content: Json[] = [];
  row.content = content;
  if (!Array.isArray(blocks)) return false;
  const count = budget.field(blocks, 'length') as number;
  budget.visit(count);
  // The source schema is flat text blocks; unknown/nested payloads never enter projection.
  for (let i = 0; i < count; i++) {
    const block = budget.field(blocks, String(i));
    budget.visit(block);
    if (budget.tag(block, 'type') !== 'text') continue;
    const projected: Row = { type: 'text' };
    content.push(projected);
    projected.text = budget.string(budget.field(block, 'text'));
  }
  return content.length > 0;
}

export function readChildPage(input: {
  callerId: string;
  child: ChildInspection;
  source?: {
    generation: string;
    events: readonly Event[];
    inheritedEventCount: number;
    cursor: number;
  };
  continuation?: string;
}): string {
  const old = parseCursor(input.continuation);
  const child = requiredChild(input.child);
  const source = input.source;
  if (!source) {
    child.history_availability = 'unavailable';
    child.history_reason = 'not_attached';
  }
  const makeCursor = (nextSeq: number) =>
    cursor({
      caller: identifier(input.callerId),
      target: input.child.agent_id,
      generation: identifier(source!.generation),
      nextSeq,
    });
  // Mutable lifecycle fields and the final page use the same conservative reserve.
  const reserve = JSON.stringify({
    child: {
      ...child,
      state: 'waiting_for_approval',
      archived: false,
      history_availability: 'unavailable',
    },
    history_source: source ? 'live' : null,
    records: [],
    continuation: source ? makeCursor(Number.MAX_SAFE_INTEGER) : null,
  });
  const budget = new Budget(reserve);
  const records: Row[] = [];
  let room = MAX_BYTES - bytes(reserve);
  let continuation: string | null = null;
  if (source) {
    const start = old ? old.nextSeq : source.inheritedEventCount;
    if (
      old &&
      (old.caller !== input.callerId ||
        old.target !== child.agent_id ||
        old.generation !== source.generation ||
        !Number.isSafeInteger(start) ||
        (start as number) < source.inheritedEventCount ||
        (start as number) > source.cursor + 1)
    )
      throw new Error(RETRY);
    let next = start as number;
    let scanned = 0;
    while (next <= source.cursor && scanned < 200 && records.length < 50) {
      if (budget.exhausted || budget.nodes >= MAX_NODES) break;
      const row: Row = {
        source_id: `${input.child.agent_id}:${next}`,
        kind: 'unknown',
        content: [],
      };
      let visible = false;
      let incomplete = false;
      try {
        budget.string(row.source_id);
        budget.text('source_idkindcontent');
        const event = budget.field(source.events, String(next)) as Event | undefined;
        const seq = budget.field(event, 'seq');
        budget.visit(seq);
        if (!event || seq !== next) throw new Error('Inspection history is corrupt');
        visible = project(event, row, budget);
      } catch (error) {
        if (!(error instanceof WorkLimit)) throw error;
        incomplete = true;
      }
      incomplete ||= budget.exhausted;
      const size = bytes(JSON.stringify(row));
      if ((incomplete || (visible && size + 1 > room)) && next !== start) break;
      if (visible || incomplete) {
        const fitted = fit(row, room - 1, incomplete);
        records.push(fitted);
        room -= bytes(JSON.stringify(fitted)) + 1;
      }
      next++;
      scanned++;
      if (incomplete) break;
      if (records.at(-1)?.truncated) break;
    }
    if (next <= source.cursor) continuation = makeCursor(next);
  }
  optionalMetadata(input.child, child, budget, room);
  const result = JSON.stringify({
    child,
    history_source: source ? 'live' : null,
    records,
    continuation,
  });
  if (bytes(result) > MAX_BYTES) requiredError();
  return result;
}

export function listChildPage(input: {
  callerId: string;
  generation: string;
  children: readonly { key: number; child: ChildInspection }[];
  catalogComplete: boolean;
  continuation?: string;
}): string {
  const makeCursor = (lastKey: number) =>
    cursor({
      caller: identifier(input.callerId),
      generation: identifier(input.generation),
      lastKey,
    });
  const old = parseCursor(input.continuation);
  if (
    old &&
    (old.caller !== input.callerId ||
      old.generation !== input.generation ||
      !Number.isSafeInteger(old.lastKey) ||
      (old.lastKey as number) < 0)
  )
    throw new Error(RETRY);
  const reserve = JSON.stringify({
    children: [],
    catalog_complete: input.catalogComplete,
    continuation: makeCursor(Number.MAX_SAFE_INTEGER),
  });
  const budget = new Budget(reserve);
  const children: Row[] = [];
  const included: ChildInspection[] = [];
  let room = MAX_BYTES - bytes(reserve);
  let continuation: string | null = null;
  let lastKey = old ? (old.lastKey as number) : -1;
  if (input.children.length > 127) throw new Error('Inspection catalog exceeds bounds');
  let previous = -1;
  for (const entry of input.children) {
    if (!Number.isSafeInteger(entry.key) || entry.key <= previous)
      throw new Error('Invalid inspection catalog order');
    previous = entry.key;
    if (entry.key <= lastKey) continue;
    const row = requiredChild(entry.child);
    const encoded = JSON.stringify(row);
    const size = bytes(encoded) + 1;
    if (size > room || budget.chars + encoded.length > MAX_CHARS || budget.nodes + 16 > MAX_NODES) {
      if (!children.length) requiredError();
      continuation = makeCursor(lastKey);
      break;
    }
    budget.chars += encoded.length;
    budget.nodes += 16;
    children.push(row);
    included.push(entry.child);
    room -= size;
    lastKey = entry.key;
  }
  for (let i = 0; i < children.length; i++) {
    const row = children[i]!;
    const before = bytes(JSON.stringify(row));
    optionalMetadata(included[i]!, row, budget, room);
    room -= bytes(JSON.stringify(row)) - before;
  }
  const result = JSON.stringify({
    children,
    catalog_complete: input.catalogComplete,
    continuation,
  });
  if (bytes(result) > MAX_BYTES) requiredError();
  return result;
}
