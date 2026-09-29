// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Pure rendering of registry SSE frames into terminal lines. Kept side-effect
// free (returns a string or null; the caller writes it) so the mapping is unit
// tested in isolation. The frame vocabulary mirrors what the harnesses emit
// (session-runner `harness/*`): `agent.message` carries `content` blocks
// (`text` / `thinking`); `agent.tool_use` carries `name` + `input`;
// `agent.tool_result` carries `content` + `is_error`; `agent.error` carries
// `message`. Control frames (`agent.turn_completed`, `agent.requires_action`)
// and internal frames (`agent.usage`) render to nothing — the interactive loop
// handles turn boundaries and the confirmation prompt itself.

import type { Palette } from './colors.js';
import type { SseFrame } from './sse.js';

/** A `content` block on an `agent.message` frame. */
interface ContentBlock {
  type?: string;
  text?: string;
  thinking?: string;
}

/**
 * Render a single frame to a display string, or `null` when the frame produces
 * no visible line. The caller is responsible for writing the returned string.
 */
export function renderFrame(frame: SseFrame, colors: Palette): string | null {
  switch (frame.type) {
    case 'agent.message':
      return renderMessage(frame, colors);
    case 'agent.tool_use':
      return renderToolUse(frame, colors);
    case 'agent.tool_result':
      return renderToolResult(frame, colors);
    case 'agent.error':
      return renderError(frame, colors);
    // Turn boundary + confirmation signal are handled by the loop, not rendered.
    case 'agent.turn_completed':
    case 'agent.requires_action':
      return null;
    default:
      // Unknown / internal frames (agent.usage, system, …) produce no line.
      return null;
  }
}

function contentBlocks(frame: SseFrame): ContentBlock[] {
  const content = (frame as { content?: unknown }).content;
  return Array.isArray(content) ? (content as ContentBlock[]) : [];
}

/**
 * `agent.message` → assistant text and/or reasoning. A `text` block prints as
 * assistant output; a `thinking` block prints dimmed under a "thinking" label so
 * it is visually distinct. A frame may carry either (harnesses emit one block
 * per message), so both are rendered and joined.
 */
function renderMessage(frame: SseFrame, colors: Palette): string | null {
  const lines: string[] = [];
  for (const block of contentBlocks(frame)) {
    if (block.type === 'text' && typeof block.text === 'string' && block.text.length > 0) {
      lines.push(`${colors.green('agent')} ${block.text}`);
    } else if (block.type === 'thinking' && typeof block.thinking === 'string') {
      lines.push(colors.dim(`thinking ${block.thinking}`));
    }
  }
  return lines.length > 0 ? lines.join('\n') : null;
}

/** `agent.tool_use` → the tool name and a compact one-line rendering of its input. */
function renderToolUse(frame: SseFrame, colors: Palette): string {
  const name = typeof frame['name'] === 'string' ? frame['name'] : '(tool)';
  const input = compactJson(frame['input']);
  const suffix = input.length > 0 ? ` ${colors.dim(input)}` : '';
  return `${colors.cyan('tool')} ${colors.bold(name)}${suffix}`;
}

/** `agent.tool_result` → the tool output, flagged when it is an error result. */
function renderToolResult(frame: SseFrame, colors: Palette): string {
  const isError = frame['is_error'] === true;
  const body = stringifyToolContent(frame['content']);
  if (isError) {
    return `${colors.red('tool error')} ${body}`;
  }
  return `${colors.dim('tool result')} ${body}`;
}

/** `agent.error` → a terminal error line for the turn. */
function renderError(frame: SseFrame, colors: Palette): string {
  const message = typeof frame['message'] === 'string' ? frame['message'] : 'unknown error';
  return `${colors.red('error')} ${message}`;
}

/**
 * Render a tool_result `content` payload to text. The harness emits either a
 * plain string or an Anthropic content-block array (`[{ type:'text', text }]`);
 * both are flattened to displayable text, with any other JSON stringified.
 */
function stringifyToolContent(content: unknown): string {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    const parts = content
      .map((block) => {
        if (block !== null && typeof block === 'object') {
          const text = (block as { text?: unknown }).text;
          if (typeof text === 'string') return text;
        }
        return typeof block === 'string' ? block : '';
      })
      .filter((s) => s.length > 0);
    if (parts.length > 0) return parts.join('\n');
  }
  if (content === undefined || content === null) return '';
  return compactJson(content);
}

/** Compact single-line JSON for tool inputs; empty string for undefined/empty objects. */
function compactJson(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'object' && Object.keys(value as object).length === 0) return '';
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
