// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { FULL_UNICODE_CASE_FOLD } from './unicode-case-fold-data.js';

/**
 * Unicode default full case folding (CaseFolding.txt statuses C + F).
 *
 * JavaScript only exposes locale-sensitive casing primitives; lowercasing or
 * upper-then-lower does not implement full folding (for example, `ẞ` and `ß`
 * diverge). The generated table makes portable path collision checks
 * independent of the host ICU version.
 */
export function unicodeFullCaseFold(value: string): string {
  let folded = '';
  for (const symbol of value) {
    folded += FULL_UNICODE_CASE_FOLD.get(symbol.codePointAt(0)!) ?? symbol;
  }
  return folded;
}
