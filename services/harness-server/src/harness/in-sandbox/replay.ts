// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { AgentEventKind } from '@orca/agent-event-contract';

export interface ReplayTurn {
  role: 'user' | 'assistant';
  text: string;
}

/** Extract plain text from a managed-agents content value (string | blocks). */
function extractText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content
      .map((b) =>
        b && typeof b === 'object' && (b as { type?: string }).type === 'text'
          ? String((b as { text?: unknown }).text ?? '')
          : '',
      )
      .filter((t) => t.length > 0)
      .join('\n');
  }
  return '';
}

/**
 * Format prior transcript entries (already JSON-parsed from event payloads) into
 * the conversational `replay` turns the sandbox-harness accepts. Only
 * `user.message` (→ user) and `agent.message` (→ assistant) carry text; other
 * kinds (tool_use/tool_result/turn markers) are skipped. Empty-text turns are
 * dropped so a fresh session yields `[]`.
 */
export function buildReplayTurns(entries: Array<Record<string, unknown>>): ReplayTurn[] {
  const turns: ReplayTurn[] = [];
  for (const entry of entries) {
    const type = entry['type'];
    if (type === 'user.message') {
      const text = extractText(entry['content']);
      if (text) turns.push({ role: 'user', text });
    } else if (type === AgentEventKind.message) {
      const text = extractText(entry['content']);
      if (text) turns.push({ role: 'assistant', text });
    }
  }
  return turns;
}
