// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

interface ValidationResult<T> {
  ok: boolean;
  value?: T;
  error?: string;
}

export function parsePositiveIntQueryParam(
  raw: unknown,
  name: string,
  opts: { defaultValue: number; max: number },
): ValidationResult<number> {
  if (raw === undefined) return { ok: true, value: opts.defaultValue };

  const parsed = typeof raw === 'number' ? raw : typeof raw === 'string' ? Number(raw) : NaN;
  if (!Number.isInteger(parsed) || parsed < 1) {
    return { ok: false, error: `${name} must be a positive integer` };
  }

  return { ok: true, value: Math.min(parsed, opts.max) };
}

export interface CreatedAtCursor {
  id: string;
}

export function encodeCreatedAtCursor(row: { id: string }): string {
  return row.id;
}

export function decodeCreatedAtCursor(raw: string | undefined): CreatedAtCursor | null {
  if (!raw) return null;
  return { id: raw };
}
