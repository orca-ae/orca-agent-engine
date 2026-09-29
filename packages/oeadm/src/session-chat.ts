// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The interactive chat loop that backs `oeadm run` and `oeadm attach`.
//
// Model (a read-eval-print loop over the registry's tail-stream event model):
// open ONE long-lived SSE tail on the session, then per turn — read a line from
// stdin, POST it as a `user.message`, and drain frames from the tail rendering
// each until the turn's `agent.turn_completed` boundary. A mid-turn
// `agent.requires_action` (tool_confirmation) pauses the drain, prompts the
// operator allow/deny, and POSTs a `user.tool_confirmation` verdict keyed by the
// parked call's `tool_use_id`; the runner then resolves the gate and the drain
// continues. EOF on stdin ends the loop.
//
// All IO is injected via {@link ChatIo} and the client via {@link ChatClient},
// so the whole loop is unit tested with scripted input + a scripted frame source
// and asserts the exact wire bodies posted.

import type { Palette } from './colors.js';
import { renderFrame } from './render.js';
import type { SseFrame } from './sse.js';

/** Terminal IO seam: line input, a yes/no confirm prompt, and an output sink. */
export interface ChatIo {
  /** Read one user line, showing `prompt`. Resolves `null` at EOF (Ctrl-D / closed stdin). */
  readLine(prompt: string): Promise<string | null>;
  /** Ask a yes/no question (the tool-confirmation gate). Resolves the allow/deny verdict. */
  confirm(question: string): Promise<boolean>;
  /** Write a line of output (a trailing newline is added by the sink). */
  write(text: string): void;
}

/** The client surface the loop needs — a subset of {@link OrcaClient}. */
export interface ChatClient {
  postUserMessage(sessionId: string, text: string): Promise<void>;
  postToolConfirmation(
    sessionId: string,
    toolUseId: string,
    result: 'allow' | 'deny',
    denyMessage?: string,
  ): Promise<void>;
  /**
   * Tail the session transcript. `fromCursor` is the registry's `from_cursor`
   * query param and is INCLUSIVE — the store predicates `seq >= $cursor`
   * (`@orca/transcript-store` `postgres-store.ts`), so the frame AT that seq is
   * re-delivered. The loop therefore passes the seq it wants delivered NEXT, not
   * the last one it saw; see {@link TailCursor}.
   */
  stream(sessionId: string, opts?: { fromCursor?: string }): AsyncIterable<SseFrame>;
}

/**
 * How one turn's drain ended.
 *
 * These are NOT interchangeable, and returning identically for both is what let
 * the loop lie. `agent.turn_completed` is the runner's own boundary marker: the
 * turn was answered. A stream that simply ENDS mid-turn means the opposite — the
 * question is committed server-side, its answer is still coming, and this process
 * merely stopped listening. Collapsed together, the loop re-prompted after a dead
 * tail and accepted further questions it could never show an answer to, then
 * exited 0 as though the conversation had finished.
 */
export type DrainOutcome = 'turn_completed' | 'stream_ended';

/**
 * Mutable tail position, so a reconnect resumes where the drop happened.
 *
 * It holds the seq to ask for NEXT — one past the last frame seen — because the
 * registry's `from_cursor` is inclusive. Storing the last seq instead re-delivers
 * that frame on resume: a duplicate line at best, and at worst a replayed
 * `agent.requires_action` that re-prompts the operator and POSTs a SECOND
 * `user.tool_confirmation` for a `tool_use_id` that has already been decided.
 */
interface TailCursor {
  /** Seq to resume AT — the successor of the last frame this process saw. */
  next?: string;
}

/**
 * The seq to request next, given the `id:` line of the frame just consumed.
 *
 * Returns null for anything that is not a plain non-negative integer. The
 * registry always writes a decimal seq there, so this only guards against a
 * proxy or a future framing change: keeping the previous known-good cursor is
 * strictly better than sending a value the store would parse to `null` and
 * silently treat as "tail from the live head".
 */
function successorCursor(id: string): string | null {
  if (!/^\d+$/.test(id)) return null;
  return String(BigInt(id) + 1n);
}

/** Options for {@link runSessionChat}. */
export interface SessionChatOptions {
  sessionId: string;
  client: ChatClient;
  io: ChatIo;
  colors: Palette;
  /** Prompt string shown before each input line. Defaults to `"you> "`. */
  prompt?: string;
}

const DEFAULT_PROMPT = 'you> ';

/**
 * Run the interactive loop until stdin EOF. Opens the session tail once, then
 * alternates read-line / post-turn / drain-until-turn-completed, handling the
 * tool-confirmation gate inline.
 *
 * A tail that ends mid-turn is a FAULT, not a turn boundary. The loop says so,
 * resumes the tail one seq past the last frame it saw, and drains the same turn
 * again. If that resumed tail dies too, it throws — never re-prompting, because
 * every further line would be committed to the transcript with nothing shown
 * back. The caller maps the throw to a non-zero exit.
 */
export async function runSessionChat(opts: SessionChatOptions): Promise<void> {
  const { sessionId, client, io, colors } = opts;
  const prompt = opts.prompt ?? DEFAULT_PROMPT;

  // Single long-lived tail. `next()` pulls the next frame; the drain helper
  // consumes from it per turn. Starting it before the first turn means frames
  // the runner emits are never missed between turns.
  const cursor: TailCursor = {};
  let frames = client.stream(sessionId)[Symbol.asyncIterator]();

  for (;;) {
    const line = await io.readLine(colors.bold(prompt));
    if (line === null) {
      // EOF: the operator closed stdin (Ctrl-D). Leave the session running.
      break;
    }
    const text = line.trim();
    if (text.length === 0) {
      // Blank line: nothing to send; re-prompt without driving a turn.
      continue;
    }

    await client.postUserMessage(sessionId, text);
    if ((await drainTurn(sessionId, frames, client, io, colors, cursor)) === 'turn_completed') {
      continue;
    }

    // The tail died with this turn in flight. Say so — the operator is owed the
    // difference between "the agent finished" and "we stopped listening" — then
    // rejoin at the successor of the last seq seen, so the outage is replayed in
    // full and nothing already rendered repeats.
    //
    // With no cursor at all there is nothing to rejoin FROM: an omitted
    // `from_cursor` is the empty string, which the store parses to `null` and
    // turns into `highWatermark + 1` — the LIVE HEAD, not the beginning. That is
    // only reached when the very first tail died before delivering a single
    // frame (the posted `user.message` echo included), and tailing the head is
    // the closest honest thing available; frames emitted inside that gap are
    // lost, and the second drain's own `stream_ended` is what reports it.
    io.write(
      colors.yellow('session stream ended before the turn completed; resuming the tail once…'),
    );
    frames = client
      .stream(sessionId, cursor.next !== undefined ? { fromCursor: cursor.next } : {})
      [Symbol.asyncIterator]();

    if ((await drainTurn(sessionId, frames, client, io, colors, cursor)) === 'stream_ended') {
      // Do NOT re-prompt. The turn is on the server either way; accepting another
      // line here would commit a question whose answer this process cannot show.
      throw new Error(
        `session ${sessionId}: the transcript stream ended before the turn completed, and ` +
          'resuming it failed. The turn is still running server-side — reattach with ' +
          `\`oeadm attach --session ${sessionId}\` to see its result.`,
      );
    }
  }
}

/**
 * Consume frames for one turn: render each visible frame, satisfy any
 * tool-confirmation gate, and report HOW the turn ended — `turn_completed` on
 * the runner's own boundary marker, `stream_ended` when the frame source ran out
 * first. `cursor.next` is advanced for every frame seen, so a resumed tail
 * restarts exactly after the last one rendered.
 *
 * The position comes from the frame's SSE `id:` line ({@link SseFrame.cursor}),
 * NOT from a `seq` payload field: the registry strips `seq` from the body for
 * every non-`orca-beta` request, so reading it here found `undefined` on every
 * frame and the resume degenerated to an uncursored tail of the live head.
 */
async function drainTurn(
  sessionId: string,
  frames: AsyncIterator<SseFrame>,
  client: ChatClient,
  io: ChatIo,
  colors: Palette,
  cursor: TailCursor,
): Promise<DrainOutcome> {
  for (;;) {
    const next = await frames.next();
    if (next.done === true) return 'stream_ended';
    const frame = next.value;
    if (typeof frame.cursor === 'string') {
      const successor = successorCursor(frame.cursor);
      if (successor !== null) cursor.next = successor;
    }

    if (frame.type === 'agent.turn_completed') {
      return 'turn_completed';
    }
    if (frame.type === 'agent.requires_action') {
      await handleRequiresAction(sessionId, frame, client, io, colors);
      continue;
    }
    const rendered = renderFrame(frame, colors);
    if (rendered !== null) {
      io.write(rendered);
    }
  }
}

/**
 * Prompt the operator to allow/deny a parked tool call and POST the verdict. The
 * `agent.requires_action` signal carries the action kind, the parked call's
 * `tool_use_id` (the verdict's routing key), and the tool name. A confirmation
 * that can't be routed (missing `tool_use_id`) is surfaced and skipped rather
 * than posting an unroutable verdict.
 */
async function handleRequiresAction(
  sessionId: string,
  frame: SseFrame,
  client: ChatClient,
  io: ChatIo,
  colors: Palette,
): Promise<void> {
  const toolUseId = typeof frame['tool_use_id'] === 'string' ? frame['tool_use_id'] : '';
  const toolName = typeof frame['tool_name'] === 'string' ? frame['tool_name'] : '(tool)';
  if (toolUseId.length === 0) {
    io.write(colors.yellow(`requires action for ${toolName} but no tool_use_id; skipping`));
    return;
  }

  const allowed = await io.confirm(colors.yellow(`Allow tool ${colors.bold(toolName)}? [y/N] `));
  const decision: 'allow' | 'deny' = allowed ? 'allow' : 'deny';
  io.write(colors.dim(`${decision === 'allow' ? 'allowed' : 'denied'} ${toolName}`));
  await client.postToolConfirmation(sessionId, toolUseId, decision);
}
