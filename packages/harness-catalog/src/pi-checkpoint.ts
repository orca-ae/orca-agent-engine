// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Pi 0.87 history stays private and has a distinct wire tag from Codex rollouts. */
export function validatePiCheckpoint(state: unknown): void {
  if (!state || typeof state !== 'object' || Array.isArray(state))
    throw new Error('invalid Pi checkpoint');
  const value = state as Record<string, unknown>;
  if (
    value.version !== 1 ||
    value.format !== 'pi_sdk' ||
    (value.sdkVersion !== '0.87.0' && value.sdkVersion !== '0.87.1') ||
    typeof value.threadId !== 'string' ||
    !/^[a-zA-Z0-9-]{1,128}$/.test(value.threadId) ||
    typeof value.instructionsSha256 !== 'string' ||
    !/^[a-f0-9]{64}$/.test(value.instructionsSha256) ||
    !value.files ||
    typeof value.files !== 'object' ||
    Array.isArray(value.files)
  )
    throw new Error('invalid Pi checkpoint');
  const entries = Object.entries(value.files);
  if (entries.length !== 1 || entries[0]![0] !== 'session.json')
    throw new Error('invalid Pi checkpoint files');
  const data: unknown = entries[0]![1];
  if (
    typeof data !== 'string' ||
    !data.length ||
    data.length % 4 !== 0 ||
    !/^[A-Za-z0-9+/]*={0,2}$/.test(data) ||
    (data.length / 4) * 3 - (data.endsWith('==') ? 2 : data.endsWith('=') ? 1 : 0) >
      16 * 1024 * 1024
  )
    throw new Error('invalid Pi checkpoint encoding or size');
}

import { validateCodexCheckpoint } from './harness-models.js';
/** Bind the native format to the immutable harness whenever a selection is available. */
export function validateSdkCheckpoint(state: unknown, harness?: string): void {
  if (harness && harness !== 'pi_sdk' && harness !== 'codex_sdk')
    throw new Error('unsupported checkpoint harness');
  const pi = (state as { format?: unknown } | null)?.format === 'pi_sdk';
  if (harness && (harness === 'pi_sdk') !== pi)
    throw new Error('checkpoint belongs to another harness');
  if (pi) validatePiCheckpoint(state);
  else validateCodexCheckpoint(state);
}
