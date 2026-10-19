// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { v5 as uuidv5 } from 'uuid';

export const SESSION_THREAD_ID_NAMESPACE = 'ea82f5c2-b759-4717-9727-51b3ef079bef';

export function stableSessionThreadId(
  workspaceId: string,
  sessionId: string,
  subpath: string,
): string {
  return `sth_${uuidv5(`${workspaceId}:${sessionId}:${subpath}`, SESSION_THREAD_ID_NAMESPACE)}`;
}
