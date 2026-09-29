// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { customAlphabet } from 'nanoid';

const ALPHABET = '0123456789ABCDEFGHJKLMNPQRSTVWXYZ';
const generate20 = customAlphabet(ALPHABET, 20);

export function newId(prefix: string): string {
  return `${prefix}_${generate20()}`;
}

export function nowIso(): string {
  return new Date().toISOString();
}
