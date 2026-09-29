// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Server-Sent-Events framing for the registry's `GET /v1/sessions/:id/stream`.
//
// The registry writes each transcript event as an SSE block: `id: <seq>` /
// `event: <type>` / `data: <json>` lines, terminated by a blank line, with
// periodic `:heartbeat` comment lines. This module turns that byte stream back
// into typed frames. It is deliberately pure and transport-agnostic — the
// {@link SseFrameParser} is fed strings and the {@link parseSseStream} generator
// adapts a `ReadableStream<Uint8Array>` — so the interactive loop can be unit
// tested against a scripted stream with no network.

/**
 * A parsed SSE frame. Payload fields the harness emits (`content`, `name`,
 * `input`, `tool_use_id`, `action`, `tool_name`, `message`, …) are TOP-LEVEL on
 * the frame: the registry's `protoToHttpEvent` spreads the posted event JSON
 * flat and stamps `type`/`seq`/`produced_*`, so `type` is always present and the
 * rest are read by the renderer.
 *
 * `seq` is NOT among them on the wire this client speaks. The registry's default
 * (non-`orca-beta`) public view destructures `seq` — along with `produced_at`,
 * `produced_by` and `subpath` — straight out of the JSON body
 * (`toPublicHttpEvent`, registry `domain/events.ts`). Reading `frame['seq']` to
 * drive a resume therefore always found `undefined` and silently resumed from
 * nothing. {@link cursor} is the fix: the same number, taken from the channel the
 * SSE protocol puts it on.
 */
export interface SseFrame {
  type: string;
  /**
   * The block's SSE `id:` line — the transcript `seq` this event was written at.
   *
   * The registry writes `id: <seq>` on EVERY frame for EVERY client, beta or not
   * (registry `streaming/sse.ts`), and honours `Last-Event-ID` on reconnect, so
   * this is the one cursor channel that survives the default wire view. Absent
   * when the block carried no `id:` line.
   */
  cursor?: string;
  [k: string]: unknown;
}

/**
 * Incremental SSE block parser. Call {@link push} with each chunk of decoded
 * text; it buffers a partial trailing block and returns the frames completed so
 * far. Blocks starting with `:` (comments / heartbeats) and `data:` lines that
 * are not JSON-with-a-`type` are skipped, matching the registry stream's shape.
 */
export class SseFrameParser {
  private buffered = '';

  push(chunk: string): SseFrame[] {
    this.buffered += chunk;
    const frames: SseFrame[] = [];
    let idx: number;
    while ((idx = this.buffered.indexOf('\n\n')) >= 0) {
      const block = this.buffered.slice(0, idx);
      this.buffered = this.buffered.slice(idx + 2);
      const frame = parseBlock(block);
      if (frame !== null) frames.push(frame);
    }
    return frames;
  }
}

/**
 * Parse a single SSE block into a frame, or null when it carries no usable data.
 *
 * Both lines that carry meaning are read, not just `data:`: the `id:` line is
 * lifted onto {@link SseFrame.cursor}. It is assigned AFTER the body is parsed so
 * the protocol's own id wins over any same-named payload field — the transcript
 * vocabulary has none, and the framing is the authority on where a frame sits in
 * the sequence.
 */
function parseBlock(block: string): SseFrame | null {
  if (block.length === 0 || block.startsWith(':')) return null;
  const lines = block.split('\n');
  const dataLine = lines.find((line) => line.startsWith('data:'));
  if (dataLine === undefined) return null;
  const raw = dataLine.slice('data:'.length).trimStart();
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (parsed === null || typeof parsed !== 'object') return null;
  const type = (parsed as { type?: unknown }).type;
  if (typeof type !== 'string' || type.length === 0) return null;
  const frame = parsed as SseFrame;
  const idLine = lines.find((line) => line.startsWith('id:'));
  if (idLine !== undefined) {
    const id = idLine.slice('id:'.length).trim();
    if (id.length > 0) frame.cursor = id;
  }
  return frame;
}

/**
 * Adapt an SSE `ReadableStream<Uint8Array>` into an async iterator of frames.
 * Decodes incrementally (a multi-byte character split across chunks is handled
 * by the streaming `TextDecoder`) and drives {@link SseFrameParser}. The
 * iterator ends when the stream closes; cancelling the consumer cancels the
 * underlying reader.
 */
export async function* parseSseStream(
  stream: ReadableStream<Uint8Array>,
): AsyncGenerator<SseFrame, void, void> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  const parser = new SseFrameParser();
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      const text = decoder.decode(value, { stream: true });
      if (text.length === 0) continue;
      for (const frame of parser.push(text)) {
        yield frame;
      }
    }
  } finally {
    reader.releaseLock();
  }
}
