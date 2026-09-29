// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { IO_VERSION, type ProjectedIo, type CapturedValue } from './types.js';

export { IO_VERSION } from './types.js';
export const MAX_CAPTURED_VALUE_BYTES = 8_192;
export const MAX_CAPTURED_IO_BYTES_PER_TURN = 262_144;
export const MAX_PENDING_CAPTURED_IO_BYTES = 262_144;
const MAX_SCAN_BYTES = 262_144;
const MAX_STRING_LENGTH = 65_536;
const MAX_ENTRIES = 2_048;
const MAX_DEPTH = 16;
const OMITTED = new Set(['unsupported', 'too_large', 'budget', 'partial', 'unavailable']);

function record(value: unknown): value is Record<string, unknown> {
  return (
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
  );
}

/** No toJSON, getters, class instances or non-JSON primitives are executed/coerced. */
export function captureJson(value: unknown): CapturedValue {
  let entries = 0;
  let bytes = 0;
  const seen = new Set<object>();
  const scanString = (text: string): string => {
    if (text.length > MAX_STRING_LENGTH) throw new Error('too_large');
    bytes += Buffer.byteLength(text);
    if (bytes > MAX_SCAN_BYTES) throw new Error('too_large');
    return text;
  };
  const visit = (item: unknown, depth: number): unknown => {
    if (++entries > MAX_ENTRIES || depth > MAX_DEPTH) throw new Error('too_large');
    if (item === null || typeof item === 'boolean') return item;
    if (typeof item === 'string') return scanString(item);
    if (typeof item === 'number' && Number.isFinite(item)) return item;
    if (!Array.isArray(item) && !record(item)) throw new Error('unsupported');
    if (seen.has(item)) throw new Error('unsupported');
    seen.add(item);
    if (Array.isArray(item) && item.length > MAX_ENTRIES) throw new Error('too_large');
    const keys: string[] = [];
    for (const key in item) {
      if (!Object.hasOwn(item, key)) continue;
      keys.push(key);
      if (keys.length > MAX_ENTRIES) throw new Error('too_large');
    }
    if (Object.getOwnPropertySymbols(item).length) throw new Error('unsupported');
    if (Array.isArray(item)) {
      if (item.length > MAX_ENTRIES) throw new Error('too_large');
      if (keys.length !== item.length || keys.some((key, index) => key !== String(index)))
        throw new Error('unsupported');
      const result = keys.map((key) => {
        const descriptor = Object.getOwnPropertyDescriptor(item, key)!;
        if (!('value' in descriptor)) throw new Error('unsupported');
        return visit(descriptor.value, depth + 1);
      });
      seen.delete(item);
      return result;
    }
    const result: Record<string, unknown> = Object.create(null);
    for (const key of keys) {
      if (key.length > 256) throw new Error('too_large');
      scanString(key);
      const descriptor = Object.getOwnPropertyDescriptor(item, key)!;
      if (!('value' in descriptor)) throw new Error('unsupported');
      result[key] = visit(descriptor.value, depth + 1);
    }
    seen.delete(item);
    return result;
  };
  try {
    const json = JSON.stringify(visit(value, 0));
    // Omit whole fields instead of emitting invalid, truncated JSON.
    if (Buffer.byteLength(json) > MAX_CAPTURED_VALUE_BYTES) return { omitted: 'too_large' };
    return { json };
  } catch (error) {
    return {
      omitted:
        error instanceof Error && error.message === 'too_large' ? 'too_large' : 'unsupported',
    };
  }
}

/** Only canonical text blocks, never arbitrary producer envelopes. */
export function captureTextContent(content: unknown): CapturedValue {
  if (typeof content === 'string') return captureJson(content);
  if (!Array.isArray(content)) return { omitted: 'unsupported' };
  if (content.length > MAX_ENTRIES) return { omitted: 'too_large' };
  const texts: string[] = [];
  let partial = false;
  let length = 0;
  try {
    for (let index = 0; index < content.length; index++) {
      const descriptor = Object.getOwnPropertyDescriptor(content, String(index));
      if (!descriptor || !('value' in descriptor)) return { omitted: 'unsupported' };
      const block: unknown = descriptor.value;
      if (!record(block)) {
        partial = true;
        continue;
      }
      const type = Object.getOwnPropertyDescriptor(block, 'type');
      const text = Object.getOwnPropertyDescriptor(block, 'text');
      if (type?.value !== 'text' || typeof text?.value !== 'string') {
        partial = true;
        continue;
      }
      length += text.value.length;
      if (length > MAX_STRING_LENGTH) return { omitted: 'too_large' };
      texts.push(text.value);
    }
  } catch {
    return { omitted: 'unsupported' };
  }
  if (!texts.length) return { omitted: 'unsupported' };
  const captured = captureJson(texts.join('\n'));
  return captured.json !== undefined && partial ? { ...captured, omitted: 'partial' } : captured;
}

export function captureToolName(value: unknown): string | undefined {
  if (typeof value !== 'string' || !value.length || value.length > 256) return undefined;
  // Preserve Unicode and spaces; control characters are excluded for display safety.
  return /\p{Cc}/u.test(value) ? undefined : value;
}

function invalid(): never {
  throw new Error('observability exporter captured I/O is invalid');
}
function allowed(value: unknown, keys: string[]): value is Record<string, unknown> {
  return (
    record(value) &&
    Reflect.ownKeys(value).every(
      (key) =>
        typeof key === 'string' &&
        keys.includes(key) &&
        'value' in Object.getOwnPropertyDescriptor(value, key)!,
    )
  );
}

/** Validate persisted bytes without enriching, masking or normalizing their payload. */
export function parseCapturedValue(value: unknown): CapturedValue {
  if (!allowed(value, ['json', 'truncated', 'omitted'])) invalid();
  if ('omitted' in value && !OMITTED.has(value.omitted as string)) invalid();
  if ('truncated' in value && value.truncated !== true) invalid();
  if ('json' in value) {
    if (typeof value.json !== 'string' || Buffer.byteLength(value.json) > MAX_CAPTURED_VALUE_BYTES)
      invalid();
    try {
      const parsed: unknown = JSON.parse(value.json);
      let entries = 0;
      const check = (item: unknown, depth: number): void => {
        if (++entries > MAX_ENTRIES || depth > MAX_DEPTH) invalid();
        if (typeof item === 'string' && item.length > MAX_STRING_LENGTH) invalid();
        if (typeof item === 'number' && !Number.isFinite(item)) invalid();
        if (item && typeof item === 'object') {
          for (const [key, child] of Object.entries(item)) {
            if (!Array.isArray(item) && key.length > 256) invalid();
            check(child, depth + 1);
          }
        }
      };
      check(parsed, 0);
    } catch {
      invalid();
    }
    if (value.omitted !== undefined && value.omitted !== 'partial') invalid();
  } else if (value.omitted === undefined || value.truncated !== undefined) invalid();
  return { ...value } as CapturedValue;
}

export function parseProjectedIo(value: unknown): ProjectedIo {
  if (
    !allowed(value, ['version', 'input', 'output', 'toolName', 'outputScope', 'diagnostic']) ||
    value.version !== IO_VERSION
  )
    invalid();
  if (
    'toolName' in value &&
    (typeof value.toolName !== 'string' || captureToolName(value.toolName) !== value.toolName)
  )
    invalid();
  if ('outputScope' in value && (value.outputScope !== 'turn_messages' || !('output' in value)))
    invalid();
  const output = 'output' in value ? parseCapturedValue(value.output) : undefined;
  if (value.outputScope === 'turn_messages' && output?.json !== undefined) {
    const messages: unknown = JSON.parse(output.json);
    if (
      !Array.isArray(messages) ||
      messages.length > 1024 ||
      messages.some((message) => typeof message !== 'string')
    )
      invalid();
  }
  return {
    version: IO_VERSION,
    ...('input' in value ? { input: parseCapturedValue(value.input) } : {}),
    ...(output === undefined ? {} : { output }),
    ...('diagnostic' in value ? { diagnostic: parseErrorDiagnostic(value.diagnostic) } : {}),
    ...('toolName' in value ? { toolName: value.toolName as string } : {}),
    ...('outputScope' in value ? { outputScope: 'turn_messages' as const } : {}),
  };
}

/** A closed diagnostic shape: never a generic error envelope or metadata channel. */
function parseErrorDiagnostic(value: unknown): CapturedValue {
  const captured = parseCapturedValue(value);
  if (captured.json === undefined) return captured;
  const facts: unknown = JSON.parse(captured.json);
  if (
    !allowed(facts, ['type', 'message', 'will_retry', 'next_attempt', 'retry_delay_ms']) ||
    Object.keys(facts).length === 0 ||
    ('type' in facts && (typeof facts.type !== 'string' || facts.type.length > 256)) ||
    ('message' in facts && typeof facts.message !== 'string') ||
    ('will_retry' in facts && typeof facts.will_retry !== 'boolean') ||
    ('next_attempt' in facts &&
      (!Number.isSafeInteger(facts.next_attempt) || (facts.next_attempt as number) <= 0)) ||
    ('retry_delay_ms' in facts &&
      (typeof facts.retry_delay_ms !== 'number' ||
        !Number.isFinite(facts.retry_delay_ms) ||
        facts.retry_delay_ms < 0))
  )
    invalid();
  return captured;
}

export function capturedIoBytes(io: ProjectedIo): number {
  return Buffer.byteLength(JSON.stringify(io));
}
