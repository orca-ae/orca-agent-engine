// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Turning transcript frames into something readable in a terminal.
//
// Kept separate from `client.ts` so the client stays the copyable part: this file is
// presentation, and every application will want its own.

import type { StreamFrame } from './client.js';

const DIM = '[2m';
const BOLD = '[1m';
const CYAN = '[36m';
const YELLOW = '[33m';
const RED = '[31m';
const RESET = '[0m';

const color = process.stdout.isTTY === true;
const paint = (code: string, s: string): string => (color ? `${code}${s}${RESET}` : s);

export const dim = (s: string): string => paint(DIM, s);
export const bold = (s: string): string => paint(BOLD, s);
export const cyan = (s: string): string => paint(CYAN, s);
export const yellow = (s: string): string => paint(YELLOW, s);
export const red = (s: string): string => paint(RED, s);

/** Concatenate the text parts of an event's `content` array. */
export function textOf(body: Record<string, unknown>): string {
  const content = body['content'];
  if (!Array.isArray(content)) return '';
  return content
    .map((part) => {
      if (typeof part !== 'object' || part === null) return '';
      const p = part as Record<string, unknown>;
      return p['type'] === 'text' && typeof p['text'] === 'string' ? p['text'] : '';
    })
    .join('');
}

/** The tool-use id an `agent.requires_action` frame is asking about. */
export function toolUseIdOf(body: Record<string, unknown>): string | null {
  const direct = body['tool_use_id'];
  if (typeof direct === 'string' && direct.length > 0) return direct;
  const id = body['id'];
  return typeof id === 'string' && id.length > 0 ? id : null;
}

export function toolNameOf(body: Record<string, unknown>): string {
  const name = body['name'];
  return typeof name === 'string' ? name : 'a tool';
}

/**
 * Render one frame, or return null for frames a human does not need to see.
 *
 * Deliberately not exhaustive: an unknown `agent.*` kind renders as a dim line rather
 * than being dropped, so a new event kind shows up as something odd on screen instead
 * of as silence.
 */
export function renderFrame(frame: StreamFrame): string | null {
  const { type, body } = frame;

  switch (type) {
    case 'agent.message': {
      const text = textOf(body);
      return text.length > 0 ? text : null;
    }
    case 'agent.thinking':
      return dim(`  thinking… ${textOf(body).slice(0, 160)}`);
    case 'agent.tool_use':
    case 'agent.mcp_tool_use':
      return dim(`  → ${toolNameOf(body)}`);
    case 'agent.tool_result':
    case 'agent.mcp_tool_result': {
      const text = textOf(body).replace(/\s+/g, ' ').trim();
      return dim(`  ← ${text.length > 120 ? `${text.slice(0, 120)}…` : text}`);
    }
    case 'agent.turn_completed':
    case 'agent.requires_action':
      // The chat loop acts on these; printing them too would be noise.
      return null;
    case 'agent.turn_failed':
      return red(`  turn failed: ${JSON.stringify(body['error'] ?? body)}`);
    case 'session.error':
      return red(`  session error: ${JSON.stringify(body['error'] ?? body)}`);
    case 'user.message':
      // Our own message, echoed back off the transcript.
      return null;
    case 'agent.usage':
      // A runtime-only accounting signal, not part of the canonical event vocabulary
      // and not something a person reading a conversation wants to see.
      return null;
    default:
      return type.startsWith('agent.') || type.startsWith('session.') ? dim(`  [${type}]`) : null;
  }
}
