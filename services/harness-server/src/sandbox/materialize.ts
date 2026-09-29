// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Event } from '@orca/transcript-store';
import { v7 as uuidv7 } from 'uuid';
import { publicEntryToEvent } from '../harness/claude/event-mapper.js';
import type { SessionResourceEntry } from '../clients/registry.js';
import type { SandboxHandle } from './sandbox-runtime.js';
import type {
  MountHandle,
  MountResource,
  MountStrategy,
  MountStrategyName,
} from './mounts/mount-strategy.js';
import {
  assertCanonicalPathOutsideSkillsRoot,
  assertMountPathOutsideSkillsRoot,
} from './skills/materialize.js';

/**
 * Per-resource override carried in from `session_resources.mount_strategy`.
 * `null` means the caller did not specify; the factory falls back to runtime
 * capability detection.
 */
export type ResourceMountStrategyOverride = MountStrategyName | null;

/**
 * Pluggable per-resource strategy chooser. The dispatcher partial-applies
 * `pickStrategy` from `mounts/strategy-factory.ts` here; tests
 * inject a static lambda. Called once per file resource — the factory is free
 * to return the same instance across calls.
 */
export type ChooseMountStrategy = (
  resource: MountResource,
  mountStrategy: ResourceMountStrategyOverride,
) => MountStrategy;

export interface MaterializeInput {
  workspaceId: string;
  sessionId: string;
  sandbox: SandboxHandle;
  resources: SessionResourceEntry[];
  /** Builds a per-resource strategy. Called once per file resource. */
  chooseStrategy: ChooseMountStrategy;
}

export interface ActiveMount {
  handle: MountHandle;
  strategy: MountStrategy;
}

export interface MaterializeResult {
  mounts: ActiveMount[];
  events: Event[];
}

/**
 * Materialize each file resource into the sandbox at start time. Memory and
 * repository resources are handled by the dispatcher because they need
 * registry/vault setup in addition to a mount strategy.
 *
 * Builds one `session.resource_mounted` event per successful mount. The
 * dispatcher buffers those events until all required setup succeeds, so
 * clients do not observe mounts for a turn that ultimately fails setup.
 *
 * File resources are required setup. If a requested file cannot be mounted,
 * reject so the dispatcher can fail session startup and surface a typed
 * `session.setup_failed` event to clients.
 */
export async function materializeResources(input: MaterializeInput): Promise<MaterializeResult> {
  const mounts: ActiveMount[] = [];
  const events: Event[] = [];
  for (const r of input.resources) {
    if (r.type !== 'file') continue;
    if (!r.file_id) {
      throw new Error(`file resource ${r.id} is missing file_id`);
    }
    assertMountPathOutsideSkillsRoot(r.mount_path);
    const resource: MountResource = {
      id: r.id,
      type: 'file',
      fileId: r.file_id,
      mountPath: r.mount_path,
      access: r.access === 'read_write' ? 'read_write' : 'read_only',
    };
    const override = normalizeMountStrategy(r.mount_strategy);
    const strategy = input.chooseStrategy(resource, override);
    await assertCanonicalPathOutsideSkillsRoot(input.sandbox, resource.mountPath);
    const handle = await strategy.activate(input.sandbox, resource);
    mounts.push({ handle, strategy });

    events.push(
      publicEntryToEvent({
        workspaceId: input.workspaceId,
        sessionId: input.sessionId,
        subpath: '',
        producedBy: 'harness',
        eventId: `evt_${uuidv7()}`,
        entry: {
          type: 'session.resource_mounted',
          resource_id: r.id,
          file_id: r.file_id,
          mount_path: r.mount_path,
          mount_strategy: strategy.name,
        },
      }),
    );
  }
  return { mounts, events };
}

function normalizeMountStrategy(
  raw: SessionResourceEntry['mount_strategy'],
): ResourceMountStrategyOverride {
  if (raw === undefined || raw === null) return null;
  if (raw === 'tarball_prefetch' || raw === 'memory_fuse' || raw === 'local_memory') {
    return raw;
  }
  // Defensive: an unknown string from the registry contract (including the
  // removed 's3_fuse') — log + treat as "auto-pick" so the session still
  // boots. The registry's zod schema should already reject anything outside
  // the enum, but the contract is a weak boundary in TS land.
  console.warn(`materializeResources: unknown mount_strategy "${raw}", falling back to auto-pick`);
  return null;
}
