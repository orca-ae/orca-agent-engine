// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { StartCommand, WorkerCommand, WorkerEvent } from '@orca/sdk-harness';

export type OrchestrationName = 'spawn' | 'fork' | 'send_message' | 'interrupt_agent';
export type DshStartCommand = StartCommand;
export type DshWorkerCommand = WorkerCommand;
export type DshWorkerEvent = WorkerEvent;

export const orchestration = new Set<string>(['spawn', 'fork', 'send_message', 'interrupt_agent']);

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
