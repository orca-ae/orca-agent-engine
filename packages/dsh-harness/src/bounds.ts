// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Transform } from 'node:stream';

/** Bound the upstream transport's otherwise unbounded newline accumulator. */
export function boundedLines(): Transform {
  let size = 0;
  return new Transform({
    transform(chunk: Buffer, _encoding, callback) {
      for (const byte of chunk) {
        size = byte === 10 ? 0 : size + 1;
        if (size > 24 * 1024 * 1024) {
          callback(new Error('DSH RPC frame exceeds limit'));
          return;
        }
      }
      callback(null, chunk);
    },
  });
}
