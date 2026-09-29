// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { z } from 'zod';
import { acceptedPrefixesFor } from './id-prefix.js';

export const idString = (prefix: string) => {
  const prefixes = acceptedPrefixesFor(prefix);
  const alternation = prefixes.join('|');
  return z
    .string()
    .regex(new RegExp(`^(?:${alternation})_[A-Za-z0-9_-]+$`), `must be a ${prefix}_… identifier`);
};

export const isoTimestamp = z.string().datetime({ offset: true });

const claudeErrorTypes = [
  'invalid_request_error',
  'authentication_error',
  'billing_error',
  'permission_error',
  'not_found_error',
  'conflict_error',
  'request_too_large',
  'rate_limit_error',
  'timeout_error',
  'overloaded_error',
  'api_error',
] as const;

export type ClaudeErrorType = (typeof claudeErrorTypes)[number];

export const ClaudeErrorResponse = z.object({
  type: z.literal('error'),
  error: z.object({
    type: z.enum(claudeErrorTypes),
    message: z.string(),
  }),
  request_id: z.string().nullable(),
});

export function buildClaudeErrorResponse(
  requestId: string,
  type: ClaudeErrorType,
  message: string,
): z.infer<typeof ClaudeErrorResponse> {
  return {
    type: 'error',
    error: { type, message },
    request_id: requestId,
  };
}

export const pagination = z.object({
  limit: z.coerce.number().int().min(1).optional(),
  page: z.string().optional(),
});
