// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Tool, CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { SdkTerminalUsage } from './usage.js';

/** Private provider-tagged history. Never part of the public transcript. */
export interface SdkCheckpoint {
  version: 1;
  threadId: string;
  files: Record<string, string>;
  instructionsSha256?: string;
  format?: 'pi_sdk' | 'dsh_sdk';
  sdkVersion?: string;
}
/** Normalized SDK events; executable tools use the separate request/reply channel. */
export type SdkEvent =
  | { type: 'thread.started'; thread_id: string }
  | { type: 'turn.started' }
  | {
      type: 'item.started' | 'item.updated' | 'item.completed';
      item: {
        id: string;
        type:
          | 'agent_message'
          | 'reasoning'
          | 'command_execution'
          | 'file_change'
          | 'mcp_tool_call'
          | 'web_search'
          | 'todo_list'
          | 'error';
        text?: string;
      };
    }
  | { type: 'turn.completed'; usage: SdkTerminalUsage }
  | { type: 'turn.failed'; error: { message: string } }
  | { type: 'error'; message: string };
export interface StartCommand {
  type: 'start';
  root: string;
  sessionId: string;
  model: string;
  modelProvider?: string;
  effort?: string;
  system: string;
  apiKey: string;
  baseUrl?: string;
  /** Pi-only native proxy origin; apiKey is then an Orca JWT, never an upstream key. */
  piGatewayUrl?: string;
  dshGatewayUrl?: string;
  tools: Tool[];
  archivedThreadSubpaths?: string[];
  checkpoint?: SdkCheckpoint;
}
export interface SdkTurnOptions {
  apiKey: string;
  baseUrl?: string;
  /** Pi-only native proxy origin; apiKey is then an Orca JWT, never an upstream key. */
  piGatewayUrl?: string;
  dshGatewayUrl?: string;
  system?: string;
}
export type WorkerCommand =
  | StartCommand
  | {
      type: 'submit';
      text: string;
      sourceEventId?: string;
      threadId?: string;
      executionBudgetMs?: number;
    }
  | { type: 'interrupt'; threadId?: string; archived?: boolean }
  | { type: 'archive_fence'; threadId: string }
  | { type: 'orchestration_result'; id: string; allowed: boolean; reason?: string }
  | { type: 'stop' }
  | { type: 'tool_result'; id: string; result: CallToolResult };
export interface WorkerEventOrigin {
  eventId: string;
  subpath: string;
  parentSubpath?: string;
}
export type WorkerEvent =
  | {
      type: 'orchestration_request';
      id: string;
      name:
        | 'spawn'
        | 'fork'
        | 'send_message'
        | 'interrupt_agent'
        | 'list_child_sessions'
        | 'read_child_session';
      arguments: Record<string, unknown>;
      origin: WorkerEventOrigin;
    }
  | { type: 'ready' }
  | { type: 'thread_tree'; threads: Array<{ subpath: string; parentSubpath: string }> }
  | { type: 'event'; event: SdkEvent; origin?: WorkerEventOrigin }
  | {
      type: 'canonical_event';
      event: { id: string; subpath: string; kind: string; payload: Record<string, unknown> };
    }
  | {
      type: 'tool_call';
      id: string;
      name: string;
      arguments: Record<string, unknown>;
      origin?: WorkerEventOrigin;
    }
  | { type: 'checkpoint'; checkpoint: SdkCheckpoint }
  | { type: 'done' }
  | { type: 'failure'; message: string; fatal?: true };
export interface SdkWorker {
  handle(command: WorkerCommand): Promise<void>;
  refreshOptions(options: SdkTurnOptions): void | Promise<void>;
  close(): Promise<void>;
}
