// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export function authenticatedGuardrailSubject(auth: {
  principal: string;
  apiKeyId?: string;
  userId?: string;
}): string {
  if (auth.userId) return `user:${auth.userId}`;
  if (auth.apiKeyId) return `api-key:${auth.apiKeyId}`;
  return `principal:${auth.principal}`;
}
