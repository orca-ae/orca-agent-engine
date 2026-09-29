// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Decode only the canonical padded base64 form emitted by session-manager. */
export function decodeCanonicalBase64(raw: string): Buffer {
  const decoded = Buffer.from(raw, 'base64');
  if (decoded.toString('base64') !== raw) {
    throw new Error('expected canonical base64');
  }
  return decoded;
}
