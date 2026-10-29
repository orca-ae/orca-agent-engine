// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { StartCommand, WorkerCommand, WorkerEvent } from '@orca/sdk-harness';
import { NATIVE_ID } from './checkpoint.js';

export type OrchestrationName = 'spawn' | 'fork' | 'send_message' | 'interrupt_agent';
export type NativeName = OrchestrationName | 'list_child_sessions' | 'read_child_session';
export type DshStartCommand = StartCommand;
export type DshWorkerCommand = WorkerCommand;
export type DshWorkerEvent = WorkerEvent;

export const orchestration = new Set<string>(['spawn', 'fork', 'send_message', 'interrupt_agent']);
export const inspection = new Set<string>(['list_child_sessions', 'read_child_session']);
export const nativeTools = new Set<string>([...orchestration, ...inspection]);

export function inspectionArguments(
  name: string,
  value: unknown,
): { agent_id?: string; continuation?: string } {
  if (!inspection.has(name)) throw new Error('Unsupported DSH inspection operation');
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Invalid DSH inspection arguments');
  const args = value as Record<string, unknown>;
  const fields = name === 'read_child_session' ? ['agent_id', 'continuation'] : ['continuation'];
  if (
    Object.keys(args).some((key) => !fields.includes(key)) ||
    (name === 'read_child_session' &&
      (typeof args.agent_id !== 'string' || !NATIVE_ID.test(args.agent_id)))
  )
    throw new Error('Invalid or expanded DSH inspection arguments');
  if ('continuation' in args) {
    if (
      typeof args.continuation !== 'string' ||
      !args.continuation ||
      args.continuation.length > 2048
    )
      throw new Error('Invalid DSH inspection continuation');
    let cursor: unknown;
    try {
      cursor = JSON.parse(args.continuation);
    } catch {
      throw new Error('Invalid DSH inspection continuation');
    }
    if (!cursor || typeof cursor !== 'object' || Array.isArray(cursor))
      throw new Error('Invalid DSH inspection continuation');
  }
  return {
    ...(name === 'read_child_session' ? { agent_id: args.agent_id as string } : {}),
    ...('continuation' in args ? { continuation: args.continuation as string } : {}),
  };
}

export function nativeArguments(name: string, value: unknown): Record<string, string> {
  return inspection.has(name)
    ? inspectionArguments(name, value)
    : orchestrationArguments(name, value);
}

export function orchestrationArguments(name: string, value: unknown): Record<string, string> {
  if (!orchestration.has(name)) throw new Error('Unsupported DSH orchestration operation');
  const fields =
    name === 'spawn' || name === 'fork'
      ? ['description', 'prompt']
      : name === 'send_message'
        ? ['agent_id', 'message']
        : ['agent_id'];
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Invalid DSH orchestration arguments');
  const args = value as Record<string, unknown>;
  if (
    Object.keys(args).some((key) => !fields.includes(key)) ||
    fields.some((key) => typeof args[key] !== 'string' || !args[key])
  )
    throw new Error('Invalid or expanded DSH orchestration arguments');
  return Object.fromEntries(fields.map((key) => [key, args[key] as string]));
}
