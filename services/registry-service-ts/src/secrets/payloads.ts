// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export function extract(payload: string, jsonKey: string | null): string {
  if (jsonKey === null) return payload;
  let parsed: unknown;
  try {
    parsed = JSON.parse(payload);
  } catch (err) {
    throw new Error(`Secret payload is not valid JSON: ${(err as Error).message}`);
  }
  if (typeof parsed !== 'object' || parsed === null) {
    throw new Error(`Secret payload does not contain key: ${jsonKey}`);
  }
  const value = (parsed as Record<string, unknown>)[jsonKey];
  if (value === null || value === undefined) {
    throw new Error(`Secret payload does not contain key: ${jsonKey}`);
  }
  return String(value);
}
