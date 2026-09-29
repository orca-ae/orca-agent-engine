// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { readBoundedText } from '../../src/api/vault-credentials.routes.js';

/** Builds a Response whose body streams the given Uint8Array chunks one read() at a time. */
function streamedResponse(chunks: Uint8Array[]): Response {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(chunk);
      }
      controller.close();
    },
  });
  return new Response(stream);
}

describe('readBoundedText', () => {
  // Note: TextDecoder({ stream: true }) already carries an incomplete trailing
  // UTF-8 sequence forward to the *next* decode() call, so a multibyte
  // character split across two chunks that are BOTH delivered (chunk N ends
  // mid-sequence, chunk N+1 supplies the rest) already decodes correctly
  // without any flush — see the "no regression" case below. The flush this
  // suite guards only matters when the incomplete sequence is never
  // completed, i.e. the stream ends (a truncated/malformed upstream body)
  // while bytes are still buffered inside the decoder.

  it('flushes a dangling incomplete 3-byte UTF-8 sequence at the true end of the stream instead of silently dropping it', async () => {
    // '€' = [0xE2, 0x82, 0xAC]. The stream delivers only the first two bytes
    // and then closes — a truncated body with no further chunk to complete
    // the sequence. Without the flush those 2 buffered bytes vanish with no
    // trace ("a"); with the flush they surface as U+FFFD, the standard
    // WHATWG replacement for an incomplete trailing sequence.
    const chunk1 = new TextEncoder().encode('a');
    const chunk2 = new Uint8Array([0xe2, 0x82]);

    const { text, truncated } = await readBoundedText(streamedResponse([chunk1, chunk2]), 100);

    expect(text).toBe('a\uFFFD');
    expect(truncated).toBe(false);
  });

  it('flushes a dangling incomplete 4-byte UTF-8 sequence (emoji) at the true end of the stream instead of silently dropping it', async () => {
    // U+1F600 = [0xF0, 0x9F, 0x98, 0x80]. The stream delivers only the first
    // three bytes and then closes.
    const chunk1 = new TextEncoder().encode('x');
    const chunk2 = new Uint8Array([0xf0, 0x9f, 0x98]);

    const { text, truncated } = await readBoundedText(streamedResponse([chunk1, chunk2]), 100);

    expect(text).toBe('x\uFFFD');
    expect(truncated).toBe(false);
  });

  it('reconstructs a complete multi-byte character split across two fully-delivered chunks (no regression)', async () => {
    // '€' split as 0xE2 | 0x82,0xAC across the chunk boundary, but this time
    // both halves are actually delivered before the stream closes.
    const chunk1 = new Uint8Array([0x61, 0xe2]); // "a" + first byte of '€'
    const chunk2 = new Uint8Array([0x82, 0xac, 0x62]); // rest of '€' + "b"

    const { text, truncated } = await readBoundedText(streamedResponse([chunk1, chunk2]), 100);

    expect(text).toBe('a€b');
    expect(truncated).toBe(false);
  });

  it('decodes correctly when the body ends cleanly on a character boundary (no regression)', async () => {
    const chunk1 = new TextEncoder().encode('hello ');
    const chunk2 = new TextEncoder().encode('€ world');

    const { text, truncated } = await readBoundedText(streamedResponse([chunk1, chunk2]), 100);

    expect(text).toBe('hello € world');
    expect(truncated).toBe(false);
  });
});
