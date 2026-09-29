// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Anthropic Managed Agents distinguishes user-uploaded inputs (`agent`) from
 * agent-produced outputs (`agent_output`). Outputs flow back to the SDK with
 * `downloadable=true`; user uploads are not directly downloadable. We mirror
 * that contract verbatim so the SDK compatibility surface works.
 */
export type FilePurpose = 'agent' | 'agent_output';

/**
 * The metadata-side representation of a file. The blob bytes live in object
 * storage; this struct is what's persisted in Postgres + returned by the
 * registry's `/v1/files/*` endpoints.
 */
export interface FileRecord {
  id: string; // file_…
  workspaceId: string;
  filename: string;
  mimeType: string;
  sizeBytes: number;
  sha256: string;
  metadata: Record<string, string>;
  /**
   * What this file is from the agent runtime's perspective. `agent` for user
   * uploads (default), `agent_output` for files produced inside a session.
   */
  purpose: FilePurpose;
  /**
   * Optional scope this file is bound to. For `agent_output` files this is
   * the originating session id (`ses_…`); for user uploads it is typically
   * `null`. Used by `list({ scope_id })` to filter to a single session's
   * outputs.
   */
  scopeId: string | null;
  /**
   * Whether the file's bytes can be downloaded by the SDK. Anthropic's
   * contract: user uploads are NOT downloadable, agent outputs ARE. The
   * `getContent` permission gate consults this field.
   */
  downloadable: boolean;
  archivedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * Inputs for `FileStore.create`. The store hashes + sizes the bytes; callers
 * MAY pass `expectedSizeBytes` for a defensive check.
 */
export interface CreateFileInput {
  /** Internal replay key for one session output; never a public upload parameter. */
  id?: string;
  workspaceId: string;
  filename: string;
  mimeType: string;
  metadata?: Record<string, string>;
  /** Streaming bytes; the store hashes them on the way through. */
  content: NodeJS.ReadableStream;
  /** Optional pre-computed size for defensive validation. */
  expectedSizeBytes?: number;
  /**
   * What this file is. Defaults to `'agent'` (user upload). Pass
   * `'agent_output'` from the session output indexer.
   */
  purpose?: FilePurpose;
  /**
   * Optional scope binding. For agent-output files this is the session id;
   * for user uploads it is typically omitted (null is persisted).
   */
  scopeId?: string | null;
  /**
   * Override the default downloadable bit. If omitted, the store applies
   * `defaultDownloadable(purpose)`. Pass `false` to keep an `agent_output`
   * file ungettable (rare; mostly useful for tests).
   */
  downloadable?: boolean;
}

/**
 * Default downloadable bit for a given purpose. Matches Anthropic's contract:
 * user uploads are not downloadable; agent outputs are. `LocalFileStore.create`
 * uses this when the caller does not pass `downloadable` explicitly.
 */
export function defaultDownloadable(purpose: FilePurpose): boolean {
  return purpose === 'agent_output';
}

export class FileNotFoundError extends Error {
  constructor(public readonly fileId: string) {
    super(`file not found: ${fileId}`);
    this.name = 'FileNotFoundError';
  }
}

export class FileSizeMismatchError extends Error {
  constructor(
    public readonly expected: number,
    public readonly actual: number,
  ) {
    super(`file size mismatch: expected ${expected}, got ${actual}`);
    this.name = 'FileSizeMismatchError';
  }
}
