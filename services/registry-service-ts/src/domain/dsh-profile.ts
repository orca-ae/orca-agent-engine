// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import type { FileStore } from '@orca/file-store';
import { isWorkspaceId } from '../auth/workspace-id.js';
import {
  DSH_PROFILE_MAX_BYTES,
  dshProfileConfigSha256,
  parseDshProfileDescriptor,
  parseDshProfileFiles,
  validateDshProfileAnnotation,
  validateDshProfileConfiguration,
  type DshProfileDescriptor,
} from '@orca/harness-catalog';

export type DshApprovedProfiles = Readonly<Record<string, readonly string[]>>;

/** Only these fixed diagnostics may cross the public API boundary. */
export class DshProfileValidationError extends Error {}

export function parseDshApprovedProfiles(text = '{}'): DshApprovedProfiles {
  const invalid = () =>
    new DshProfileValidationError('Invalid DSH_APPROVED_PROFILES configuration');
  if (Buffer.byteLength(text, 'utf8') > 64 * 1024) throw invalid();
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw invalid();
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw invalid();
  const entries = Object.entries(parsed);
  if (entries.length > 128) throw invalid();
  let count = 0;
  for (const [workspace, digests] of entries) {
    if (
      !isWorkspaceId(workspace) ||
      !Array.isArray(digests) ||
      digests.length > 32 ||
      digests.some((digest) => typeof digest !== 'string' || !/^[0-9a-f]{64}$/.test(digest))
    )
      throw invalid();
    count += digests.length;
  }
  if (count > 512) throw invalid();
  return parsed as DshApprovedProfiles;
}

export function requireDshProfileApproval(
  approved: DshApprovedProfiles,
  workspaceId: string,
  profile: DshProfileDescriptor,
): void {
  if (
    !Object.hasOwn(approved, workspaceId) ||
    !approved[workspaceId]?.includes(profile.config_sha256)
  )
    throw new DshProfileValidationError('DSH profile is not approved for this workspace');
}

/** Metadata is never a fallback when a selected immutable descriptor is unavailable. */
export function retainedDshProfile(
  snapshot: Record<string, unknown>,
): DshProfileDescriptor | undefined {
  const metadata = (snapshot.metadata ?? {}) as Record<string, unknown>;
  validateDshProfileAnnotation(metadata);
  if (metadata.dsh_profile === undefined) {
    if (snapshot.dsh_profile !== undefined)
      throw new DshProfileValidationError('DSH profile descriptor has no annotation');
    return undefined;
  }
  const profile = parseDshProfileDescriptor(snapshot.dsh_profile);
  if (profile.source_file_id !== metadata.dsh_profile)
    throw new DshProfileValidationError('DSH profile source binding mismatch');
  return profile;
}

export function requireDshProfileEnvironment(
  environment: { target?: unknown; networking?: unknown } | null,
  sandboxNetworkEnabled = false,
): void {
  const networking = environment?.networking as { type?: unknown } | null | undefined;
  if (
    !environment ||
    (environment.target !== null &&
      environment.target !== undefined &&
      environment.target !== 'cloud') ||
    (networking?.type !== undefined &&
      networking.type !== 'unrestricted' &&
      !(sandboxNetworkEnabled && networking.type === 'limited'))
  )
    throw new DshProfileValidationError(
      'Custom DSH profiles require a cloud Environment with unrestricted networking or enabled controlled Sandbox networking',
    );
}

export async function resolveDshProfileForWrite(input: {
  metadata: Record<string, unknown>;
  previousSnapshot?: Record<string, unknown> | null;
  explicitlyAssigned?: boolean;
  workspaceId: string;
  fileStore?: FileStore;
  approved?: DshApprovedProfiles;
}): Promise<DshProfileDescriptor | undefined> {
  validateDshProfileAnnotation(input.metadata);
  const fileId = input.metadata.dsh_profile;
  if (fileId === undefined) return undefined;
  const previous = input.previousSnapshot;
  if (
    previous?.dsh_profile !== undefined &&
    (previous.metadata as Record<string, unknown> | undefined)?.dsh_profile === fileId
  ) {
    const retained = retainedDshProfile(previous)!;
    requireDshProfileApproval(input.approved ?? {}, input.workspaceId, retained);
    return retained;
  }
  if (previous && !input.explicitlyAssigned)
    throw new DshProfileValidationError(
      'DSH profile descriptor is missing; reassert or clear metadata.dsh_profile',
    );
  if (!input.fileStore) throw new DshProfileValidationError('DSH profile ingestion is unavailable');
  const file = await input.fileStore.get(input.workspaceId, fileId as string);
  if (
    !file ||
    file.workspaceId !== input.workspaceId ||
    file.archivedAt !== null ||
    file.purpose !== 'agent' ||
    file.scopeId !== null
  )
    throw new DshProfileValidationError(
      'DSH profile requires an active unscoped upload in this workspace',
    );
  if (file.sizeBytes <= 0 || file.sizeBytes > DSH_PROFILE_MAX_BYTES)
    throw new DshProfileValidationError('DSH profile input exceeds 1 MiB');
  const opened = await input.fileStore.open(input.workspaceId, file.id);
  if (!opened) throw new DshProfileValidationError('DSH profile source is unavailable');
  const stream = opened.stream as Readable;
  try {
    if (opened.sizeBytes !== file.sizeBytes || opened.sha256 !== file.sha256)
      throw new DshProfileValidationError('DSH profile source metadata mismatch');
    const chunks: Buffer[] = [];
    let size = 0;
    for await (const chunk of stream) {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      size += bytes.length;
      if (size > DSH_PROFILE_MAX_BYTES || size > file.sizeBytes)
        throw new DshProfileValidationError('DSH profile input exceeds its size bound');
      chunks.push(bytes);
    }
    const bytes = Buffer.concat(chunks);
    if (size !== file.sizeBytes || createHash('sha256').update(bytes).digest('hex') !== file.sha256)
      throw new DshProfileValidationError('DSH profile source size or SHA-256 mismatch');
    const envelope: unknown = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
    if (
      !envelope ||
      typeof envelope !== 'object' ||
      Array.isArray(envelope) ||
      JSON.stringify(Object.keys(envelope).sort()) !== JSON.stringify(['files', 'version']) ||
      (envelope as { version?: unknown }).version !== 1
    )
      throw new DshProfileValidationError('Unsupported DSH profile envelope');
    const files = parseDshProfileFiles((envelope as { files: unknown }).files);
    validateDshProfileConfiguration(files);
    const descriptor: DshProfileDescriptor = {
      version: 1,
      source_file_id: file.id,
      source_sha256: file.sha256,
      config_sha256: dshProfileConfigSha256(files),
      files,
    };
    requireDshProfileApproval(input.approved ?? {}, input.workspaceId, descriptor);
    return descriptor;
  } finally {
    stream.destroy();
  }
}
