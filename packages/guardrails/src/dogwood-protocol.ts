// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0
import type { Entity, PolicyCall, PolicyEvent, Verdict } from '@runorca/dogwood';
import type { StateUpdate } from './types.js';
export interface PolicyHead {
  session: string;
  principal: string | null;
}
export interface PolicyState {
  session: Record<string, unknown>;
  subject_window: Record<string, unknown>;
  workspace_window: Record<string, unknown>;
}
export interface PolicyEventRow {
  seq: string;
  event: PolicyEvent;
}
/** Registry's authenticated JSON protocol; the owning Registry contract validates the wire. */
export interface PolicyReadBody {
  session_id: string;
  now: string;
  epoch: string;
  bundle_id: string;
  core_version: '0.1.0';
  head: PolicyHead;
  principal: Entity;
  resource: Entity;
  resources?: Entity[] | undefined;
  mcp_servers?: { name: string; url: string }[] | undefined;
  subject?: { kind: string; id: string } | undefined;
  turn_event_id: string | null;
  day_window: string;
  month_window: string;
  state_revision?: string | undefined;
  guardrail_state?: PolicyState | undefined;
  events: PolicyEventRow[];
  guardrails: {
    id: string;
    name: string;
    rule?: unknown;
    mode?: 'enforce' | 'observe' | undefined;
    subagent_id?: string | undefined;
    [key: string]: unknown;
  }[];
}
export interface PolicyAppendBody {
  batch_id: string;
  tool_use_id: string;
  epoch: string;
  bundle_id: string;
  expected_head: PolicyHead;
  expected_state_revision?: string | undefined;
  day_window?: string | undefined;
  month_window?: string | undefined;
  decision?:
    | {
        call: PolicyCall;
        verdict: 'allow' | 'ask' | 'deny';
        dogwood: {
          guardrail_id: string;
          verdict: Verdict;
          deciding_rules: string[];
          errors: string[];
        }[];
        confirmation?: { event_id: string; tool_use_id: string } | undefined;
        public_tool_use_id?: string | undefined;
      }
    | undefined;
  events: PolicyEvent[];
  outcome?: 'response' | 'failed' | 'interrupted' | undefined;
  output?: unknown;
  state_updates?:
    | {
        scope: 'session' | 'subject_window' | 'workspace_window';
        key: string;
        action: StateUpdate['action'];
        value?: unknown;
      }[]
    | undefined;
  turn_event_id?: string | undefined;
  window?: string | undefined;
}
export interface PolicyAppendResponse {
  disposition: 'created' | 'replayed';
  receipt: {
    batch_id: string;
    tool_use_id: string;
    epoch: string;
    bundle_id: string;
    committed_at: string;
    valid_before: string | null;
  };
  now: string;
  head: PolicyHead;
  state_revision?: string | undefined;
  guardrail_state?: PolicyState | undefined;
  events: PolicyEventRow[];
}
