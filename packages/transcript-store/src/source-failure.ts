// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

const SAFE_ERROR_NAMES = new Set([
  'AbortError',
  'AggregateError',
  'Error',
  'EvalError',
  'RangeError',
  'ReferenceError',
  'SyntaxError',
  'TypeError',
  'URIError',
]);
const SAFE_ERROR_CODES = new Set([
  'ABORT_ERR',
  'EAI_AGAIN',
  'ECONNABORTED',
  'ECONNREFUSED',
  'ECONNRESET',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ENOTFOUND',
  'EPIPE',
  'ETIMEDOUT',
  'UND_ERR_CONNECT_TIMEOUT',
  'UND_ERR_HEADERS_TIMEOUT',
  'UND_ERR_SOCKET',
  // PostgreSQL connection, serialization, shutdown, resource, and internal classes.
  '08000',
  '08001',
  '08003',
  '08004',
  '08006',
  '08007',
  '08P01',
  '40001',
  '40P01',
  '53300',
  '57P01',
  '57P02',
  '57P03',
  '58000',
  'XX000',
  'XX001',
  'XX002',
]);

/**
 * Terminal event-source failures are operator-visible, so preserve only
 * stable classification fields. Messages, stacks, URLs, and credentials are
 * never safe to send to a shared service log.
 */
export function terminalSourceFailureFields(error: unknown): { name: string; code: string } {
  const candidate = error instanceof Error ? error : null;
  const name = candidate && SAFE_ERROR_NAMES.has(candidate.name) ? candidate.name : 'Error';
  const code =
    candidate &&
    typeof (candidate as Error & { code?: unknown }).code === 'string' &&
    SAFE_ERROR_CODES.has((candidate as Error & { code: string }).code)
      ? (candidate as Error & { code: string }).code
      : 'unknown';
  return { name, code };
}
