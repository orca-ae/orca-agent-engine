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
        // A 16 MiB checkpoint expands in base64, alongside retained config and bounded schemas.
        if (size > 32 * 1024 * 1024) {
          callback(new Error('DSH RPC frame exceeds limit'));
          return;
        }
      }
      callback(null, chunk);
    },
  });
}
