// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Readable } from 'node:stream';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { LocalFileStore, InMemoryBlobStore, InMemoryFileMetadataStore } from '@orca/file-store';
import { dshProfileConfigSha256 } from '@orca/harness-catalog';
import { loadConfig } from '../../src/config.js';
import {
  parseDshApprovedProfiles,
  requireDshProfileEnvironment,
  retainedDshProfile,
  resolveDshProfileForWrite,
} from '../../src/domain/dsh-profile.js';
import {
  strictAgentVersionConfiguration,
  validateAgentConfiguration,
} from '../../src/domain/agent-version-configuration.js';
import {
  PreparedAgentSnapshotSchema,
  internalContract,
} from '../../src/contracts/internal.contract.js';
import { sessionLlmEgressMetadataError } from '../../src/domain/session-llm-egress.js';
import { nativeProfileFiles } from '../helpers/dsh-profile.js';

describe('workspace DSH profile approval configuration', () => {
  const digest = 'a'.repeat(64);
  it('defaults to no custom grants and loads exact workspace grants', () => {
    expect(loadConfig({}).dshApprovedProfiles).toEqual({});
    expect(parseDshApprovedProfiles(JSON.stringify({ ws_one: [digest] }))).toEqual({
      ws_one: [digest],
    });
  });
  it.each([
    '',
    '[]',
    'null',
    '{',
    JSON.stringify({ '*': [digest] }),
    JSON.stringify({ ws_a: ['A'.repeat(64)] }),
    JSON.stringify({ ws_a: digest }),
    JSON.stringify({ ws_a: Array(33).fill(digest) }),
    ' '.repeat(65_537),
  ])('rejects malformed or oversized grants', (value) => {
    expect(() => parseDshApprovedProfiles(value)).toThrow(/DSH_APPROVED_PROFILES/);
  });
  it('bounds both workspace and total entry counts', () => {
    expect(() =>
      parseDshApprovedProfiles(
        JSON.stringify(Object.fromEntries(Array.from({ length: 129 }, (_, i) => ['ws_' + i, []]))),
      ),
    ).toThrow();
    expect(() =>
      parseDshApprovedProfiles(
        JSON.stringify(
          Object.fromEntries(
            Array.from({ length: 17 }, (_, i) => ['ws_' + i, Array(32).fill(digest)]),
          ),
        ),
      ),
    ).toThrow();
    expect(() =>
      parseDshApprovedProfiles(
        JSON.stringify(
          Object.fromEntries(
            Array.from({ length: 16 }, (_, i) => ['ws_' + i, Array(32).fill(digest)]),
          ),
        ),
      ),
    ).not.toThrow();
  });
});

describe('private immutable DSH profile ingestion', () => {
  const files = nativeProfileFiles();
  const digest = dshProfileConfigSha256(files);
  let store: LocalFileStore;
  beforeEach(() => {
    store = new LocalFileStore({
      blobStore: new InMemoryBlobStore(),
      metadataStore: new InMemoryFileMetadataStore(),
    });
  });
  async function upload(content = Buffer.from(JSON.stringify({ version: 1, files })), extra = {}) {
    return store.create({
      workspaceId: 'ws_one',
      filename: 'profile.json',
      mimeType: 'application/json',
      content: Readable.from([content]),
      ...extra,
    });
  }
  const resolve = (id: string, extra = {}) =>
    resolveDshProfileForWrite({
      workspaceId: 'ws_one',
      metadata: { harness: 'dsh_sdk', mode: 'colocated', dsh_profile: id },
      fileStore: store,
      approved: { ws_one: [digest] },
      ...extra,
    });

  it('ingests once and retains content after source deletion without rereads', async () => {
    const file = await upload();
    const descriptor = (await resolve(file.id))!;
    expect(descriptor).toMatchObject({
      source_file_id: file.id,
      source_sha256: file.sha256,
      config_sha256: digest,
      files,
    });
    await store.delete('ws_one', file.id);
    const open = vi.spyOn(store, 'open');
    expect(
      await resolve(file.id, {
        previousSnapshot: {
          metadata: { harness: 'dsh_sdk', mode: 'colocated', dsh_profile: file.id },
          dsh_profile: descriptor,
        },
      }),
    ).toEqual(descriptor);
    expect(open).not.toHaveBeenCalled();
  });

  it('requires explicit legacy reassertion and current approval; clear remains available', async () => {
    const file = await upload();
    const metadata = { harness: 'dsh_sdk', mode: 'colocated', dsh_profile: file.id };
    const previousSnapshot = { metadata };
    await expect(resolve(file.id, { previousSnapshot })).rejects.toThrow(/reassert/);
    const descriptor = await resolve(file.id, { previousSnapshot, explicitlyAssigned: true });
    await expect(
      resolve(file.id, { previousSnapshot: { metadata, dsh_profile: descriptor }, approved: {} }),
    ).rejects.toThrow(/approved/);
    expect(
      await resolve(file.id, {
        metadata: { harness: 'dsh_sdk', mode: 'colocated' },
        previousSnapshot,
        approved: {},
      }),
    ).toBeUndefined();
    await expect(resolve(file.id, { approved: { ws_other: [digest] } })).rejects.toThrow(
      /approved/,
    );
  });

  it.each([
    { workspaceId: 'ws_other' },
    { purpose: 'agent_output' as const },
    { scopeId: 'ses_scoped' },
  ])('rejects inaccessible/scoped/output sources', async (extra) => {
    const file = await upload(undefined, extra);
    await expect(resolve(file.id)).rejects.toThrow(/upload/);
  });

  it('rejects archived/deleted sources before open', async () => {
    const file = await upload();
    await store.archive('ws_one', file.id);
    const open = vi.spyOn(store, 'open');
    await expect(resolve(file.id)).rejects.toThrow(/upload/);
    expect(open).not.toHaveBeenCalled();
    await store.delete('ws_one', file.id);
    await expect(resolve(file.id)).rejects.toThrow();
  });

  it.each([
    Buffer.from([0xff]),
    Buffer.from('{"version":1,"files":{},"extra":true}'),
    Buffer.alloc(1_048_577),
  ])('rejects invalid UTF-8/envelope/byte bounds', async (bytes) => {
    const file = await upload(bytes);
    await expect(resolve(file.id)).rejects.toThrow();
  });

  it('verifies open metadata, streamed size, hash, and interrupted reads', async () => {
    const file = await upload();
    const original = store.open.bind(store);
    for (const variant of ['metadata', 'hash', 'size', 'interrupted']) {
      vi.spyOn(store, 'open').mockImplementation(async (workspace, id) => {
        const opened = (await original(workspace, id))!;
        (opened.stream as Readable).destroy();
        const bytes = Buffer.from(JSON.stringify({ version: 1, files }));
        return {
          sizeBytes: variant === 'metadata' ? 1 : file.sizeBytes,
          sha256: file.sha256,
          stream:
            variant === 'interrupted'
              ? Readable.from(
                  (async function* () {
                    yield bytes.subarray(0, 4);
                    throw new Error('interrupted');
                  })(),
                )
              : Readable.from([
                  variant === 'size' ? bytes.subarray(0, 4) : Buffer.alloc(bytes.length),
                ]),
        };
      });
      await expect(resolve(file.id)).rejects.toThrow();
      vi.restoreAllMocks();
    }
  });

  it('rejects missing/unsupported descriptors in strict readers and capability syntax', () => {
    const metadata = { harness: 'dsh_sdk', mode: 'colocated', dsh_profile: 'file_legacy' };
    const snapshot = {
      id: 'agt_test',
      name: 'test',
      version: 1,
      model: { provider: 'anthropic', id: 'claude-sonnet-4-6' },
      system: null,
      tools: [],
      mcp_servers: [],
      skills: [],
      multiagent: null,
      metadata,
    };
    expect(PreparedAgentSnapshotSchema.safeParse(snapshot).success).toBe(false);
    expect(() => strictAgentVersionConfiguration(snapshot, snapshot.id, 1)).toThrow(/unavailable/);
    expect(() => retainedDshProfile({ ...snapshot, dsh_profile: { version: 2 } })).toThrow();
    expect(internalContract.prepareExecution.body.safeParse({}).success).toBe(true);
    expect(
      internalContract.prepareExecution.body.safeParse({ capabilities: ['dsh_profile_v1'] })
        .success,
    ).toBe(true);
    expect(
      internalContract.prepareExecution.body.safeParse({ capabilities: 'dsh_profile_v1' }).success,
    ).toBe(false);
    expect(sessionLlmEgressMetadataError({ dsh_profile: 'file_legacy' })).toMatch(/reserved/);
  });

  it('admits limited networking only with trusted controlled Sandbox networking', () => {
    const limited = { target: 'cloud', networking: { type: 'limited' } };
    expect(() => requireDshProfileEnvironment(limited, true)).not.toThrow();
    expect(() => requireDshProfileEnvironment(limited)).toThrow(/cloud Environment/);
    expect(() => requireDshProfileEnvironment(limited, false)).toThrow(/cloud Environment/);
    for (const environment of [
      null,
      { target: 'self_hosted', networking: { type: 'limited' } },
      { target: 'local', networking: { type: 'unrestricted' } },
      { target: 'cloud', networking: { type: 'invalid' } },
    ])
      expect(() => requireDshProfileEnvironment(environment, true)).toThrow(/cloud Environment/);
  });

  it('checks current Environment networking and named plugin declaration syntax', () => {
    expect(() =>
      requireDshProfileEnvironment({ target: 'cloud', networking: { type: 'unrestricted' } }),
    ).not.toThrow();
    for (const environment of [
      null,
      { target: 'self_hosted' },
      { target: 'cloud', networking: { type: 'limited' } },
    ])
      expect(() => requireDshProfileEnvironment(environment)).toThrow(/cloud Environment/);
    const tools = [
      {
        type: 'agent_toolset',
        configs: [
          { name: 'business_tool', enabled: true, permission_policy: { type: 'always_ask' } },
        ],
      },
    ];
    expect(validateAgentConfiguration(tools, [], true)).toBeNull();
    expect(
      validateAgentConfiguration(
        [
          {
            type: 'custom',
            name: 'client_tool',
            description: 'Client tool',
            input_schema: { type: 'object' },
          },
        ],
        [],
        true,
      ),
    ).toMatch(/client-executed/);
    expect(validateAgentConfiguration(tools, [])).toMatch(/built-in/);
    expect(
      validateAgentConfiguration(
        [{ type: 'agent_toolset', configs: { '*': { enabled: true } } }],
        [],
        true,
      ),
    ).toMatch(/invalid/);
  });
});
