// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Entity, PolicyCall, PolicyEvent } from '@runorca/dogwood';
import type { PreparedExecutionV2 } from '../contracts/internal.contract.js';
export type DogwoodPrepared = PreparedExecutionV2['guardrails'][number];
export interface PolicyBinding {
  bundleId: string;
  owner: string;
  turnEventId: string | null;
  subject: string | null;
  principal: Entity;
  resources: Entity[];
  mcpServers: { name: string; url: string }[];
  enforcer: 'harness' | 'ai-gateway';
  runtimeRevision: number;
  guardrails: DogwoodPrepared[];
}
export interface StoredPolicyCall {
  call: PolicyCall;
  callDigest: string;
  status: 'asked' | 'authorized' | 'denied' | 'response' | 'failed' | 'interrupted';
  epoch: string;
  bundleId: string;
  publicToolUseId?: string;
  asking: Record<string, string[]>;
}
export interface StoredPolicyEvent {
  seq: string;
  event: PolicyEvent;
}
