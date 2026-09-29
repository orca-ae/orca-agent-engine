// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'vitest';
import { apiCall, type OrcaClientConfig } from '../src/client.js';

export interface ScopedOutputFile {
  id: string;
  filename: string;
  size_bytes: number;
  scope: { type: 'session'; id: string } | null;
  downloadable: boolean;
}

export async function waitForScopedOutputFile(
  cfg: OrcaClientConfig,
  sessionId: string,
  filename: string,
  deadlineMs: number,
): Promise<ScopedOutputFile> {
  const deadline = Date.now() + deadlineMs;
  while (Date.now() < deadline) {
    const listed = await apiCall(
      cfg,
      `/v1/files?scope_id=${encodeURIComponent(sessionId)}&limit=100`,
      { method: 'GET' },
    );
    expect(listed.status, listed.text).toBe(200);
    const match = listed
      .json<{ data: ScopedOutputFile[] }>()
      .data.find((file) => file.filename === filename && file.scope?.id === sessionId);
    if (match) return match;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(
    `output ${filename} was not registered for session ${sessionId} within ${deadlineMs}ms`,
  );
}

export async function listScopedOutputFiles(
  cfg: OrcaClientConfig,
  sessionId: string,
): Promise<ScopedOutputFile[]> {
  const listed = await apiCall(
    cfg,
    `/v1/files?scope_id=${encodeURIComponent(sessionId)}&limit=100`,
    { method: 'GET' },
  );
  expect(listed.status, listed.text).toBe(200);
  return listed.json<{ data: ScopedOutputFile[] }>().data;
}

export async function downloadFileText(cfg: OrcaClientConfig, fileId: string): Promise<string> {
  const response = await fetch(`${cfg.baseURL}/v1/files/${fileId}/content`, {
    headers: { 'x-api-key': cfg.apiKey },
  });
  expect(response.status).toBe(200);
  return await response.text();
}
