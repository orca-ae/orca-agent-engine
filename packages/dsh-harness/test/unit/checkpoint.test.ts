// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  captureCheckpoint,
  decodeCheckpoint,
  assertDshCheckpointInstructions,
  assertDshCheckpointModel,
  MAX_STATE_BYTES,
  assertDshCheckpointProfile,
} from '../../src/checkpoint.js';
import { MessagesUsage, gatewayBase } from '../../src/transport.js';

const id = '12345678-1234-1234-1234-123456789abc';
const file = `_no-cwd/${id}/session.v4.jsonl`;
const native = Buffer.from(
  JSON.stringify({
    type: 'session',
    version: 4,
    id,
    createdAt: 0,
    isSeeded: false,
    delegationDepth: 0,
  }) + '\n',
);
describe('DSH native boundary', () => {
  it('separates custom and default history and binds every prepared identity field', () => {
    const identity = {
      version: 1 as const,
      config_sha256: 'a'.repeat(64),
      lock_sha256: 'b'.repeat(64),
      tool_manifest_sha256: 'c'.repeat(64),
      runtime_compatibility: 'd'.repeat(64),
    };
    const checkpoint = captureCheckpoint(
      id,
      'system',
      { [file]: native },
      'deepseek-v4-flash',
      'deepseek',
      identity,
    );
    expect(() => assertDshCheckpointProfile(checkpoint, identity)).not.toThrow();
    expect(() => assertDshCheckpointProfile(checkpoint)).toThrow('harness profile');
    for (const key of [
      'config_sha256',
      'lock_sha256',
      'tool_manifest_sha256',
      'runtime_compatibility',
    ] as const)
      expect(() =>
        assertDshCheckpointProfile(checkpoint, { ...identity, [key]: 'e'.repeat(64) }),
      ).toThrow('harness profile');
    const old = captureCheckpoint(id, 'system', { [file]: native });
    expect(() => assertDshCheckpointProfile(old, identity)).toThrow('harness profile');
    expect(() => assertDshCheckpointProfile(old)).not.toThrow();
    const manifest = JSON.parse(Buffer.from(checkpoint.files['orca.json']!, 'base64').toString());
    delete manifest.harness_profile;
    checkpoint.files['orca.json'] = Buffer.from(JSON.stringify(manifest)).toString('base64');
    expect(() => decodeCheckpoint(checkpoint)).toThrow('harness profile');
  });
  it('binds provider/model, accepts only compatible legacy DeepSeek, and separates adapter profiles', () => {
    const checkpoint = captureCheckpoint(
      id,
      'system',
      { [file]: native },
      'claude-sonnet-4-6',
      'anthropic',
    );
    expect(() =>
      assertDshCheckpointModel(checkpoint, 'anthropic', 'claude-sonnet-4-6'),
    ).not.toThrow();
    expect(() => assertDshCheckpointModel(checkpoint, 'other', 'claude-sonnet-4-6')).toThrow(
      'provider/model',
    );
    expect(() => assertDshCheckpointModel(checkpoint, 'anthropic', 'claude-opus-4-6')).toThrow(
      'provider/model',
    );
    const legacy = captureCheckpoint(id, 'system', { [file]: native });
    const manifest = JSON.parse(Buffer.from(legacy.files['orca.json']!, 'base64').toString());
    delete manifest.provider;
    legacy.files['orca.json'] = Buffer.from(JSON.stringify(manifest)).toString('base64');
    expect(() => assertDshCheckpointModel(legacy, 'deepseek', 'deepseek-v4-flash')).not.toThrow();
    expect(() => assertDshCheckpointModel(legacy, 'anthropic', 'deepseek-v4-flash')).toThrow(
      'provider/model',
    );
  });
  it('round trips native files and pins instructions/version/profile', () => {
    const checkpoint = captureCheckpoint(id, 'system', { [file]: native });
    expect(decodeCheckpoint(checkpoint)[file]).toEqual(native);
    expect(() => assertDshCheckpointInstructions(checkpoint, 'other')).toThrow('instructions');
    expect(() => decodeCheckpoint({ ...checkpoint, sdkVersion: 'old' })).toThrow('Incompatible');
    expect(() =>
      decodeCheckpoint({ ...checkpoint, files: { ...checkpoint.files, '../workspace': 'YQ==' } }),
    ).toThrow('path');
    expect(() =>
      decodeCheckpoint({ ...checkpoint, files: { ...checkpoint.files, [file]: 'YQ' } }),
    ).toThrow('encoding');
    expect(() =>
      captureCheckpoint(id, 'system', { [file]: Buffer.alloc(MAX_STATE_BYTES) }),
    ).toThrow('16 MiB');
    const incompatible = {
      ...checkpoint,
      files: {
        ...checkpoint.files,
        'orca.json': Buffer.from(JSON.stringify({ profile: 'other' })).toString('base64'),
      },
    };
    expect(() => decodeCheckpoint(incompatible)).toThrow('profile');
  });
  it('bounds the whole tree and rejects unrelated logs, missing descendants and prior root-only profiles', () => {
    const child = '22345678-1234-1234-1234-123456789abc';
    const childFile = `_no-cwd/${child}/session.v4.jsonl`;
    const childLog = Buffer.from(
      JSON.stringify({
        type: 'session',
        version: 4,
        id: child,
        createdAt: 0,
        isSeeded: false,
        delegationDepth: 1,
        parentSession: id,
        origin: 'subagent',
      }) + '\n',
    );
    const catalog = Buffer.concat([
      native,
      Buffer.from(
        JSON.stringify({
          type: 'subagent/catalog',
          seq: 0,
          time: 0,
          data: {
            version: 0,
            mode: 'continuable',
            childId: child,
            childCreatedAt: 0,
            label: 'child',
          },
        }) + '\n',
      ),
    ]);
    const checkpoint = captureCheckpoint(id, 'system', { [file]: catalog, [childFile]: childLog });
    expect(Object.keys(decodeCheckpoint(checkpoint))).toHaveLength(3);
    expect(() =>
      captureCheckpoint(id, 'system', {
        [file]: Buffer.alloc(MAX_STATE_BYTES / 2),
        [childFile]: Buffer.alloc(MAX_STATE_BYTES / 2),
      }),
    ).toThrow('16 MiB');
    expect(() =>
      captureCheckpoint(id, 'system', { [file]: native, [childFile]: childLog }),
    ).toThrow('Unrelated');
    expect(() => captureCheckpoint(id, 'system', { [file]: catalog })).toThrow('lineage');
    expect(() =>
      captureCheckpoint(id, 'system', { [file]: Buffer.from(native.toString().trim()) }),
    ).toThrow('Truncated');
    expect(() =>
      decodeCheckpoint({
        ...checkpoint,
        files: Object.fromEntries(Array.from({ length: 130 }, (_, index) => [String(index), ''])),
      }),
    ).toThrow('file limit');
    const manifest = JSON.parse(Buffer.from(checkpoint.files['orca.json']!, 'base64').toString());
    expect(() =>
      decodeCheckpoint({
        ...checkpoint,
        files: {
          ...checkpoint.files,
          'orca.json': Buffer.from(
            JSON.stringify({ ...manifest, profile: 'orca-managed-v1' }),
          ).toString('base64'),
        },
      }),
    ).toThrow('profile');
  });
  it('requires terminal cumulative raw usage, not unknown zero', () => {
    const usage = new MessagesUsage();
    expect(() => usage.finish()).toThrow('unavailable');
    usage.event({
      type: 'message_start',
      message: { usage: { input_tokens: 4, cache_read_input_tokens: 3 } },
    });
    usage.event({ type: 'message_delta', usage: { output_tokens: 2 } });
    usage.event({ type: 'message_delta', usage: { output_tokens: 5 } });
    expect(() => usage.finish()).toThrow('unavailable');
    expect(() => usage.event({ type: 'message_delta', usage: { output_tokens: 1 } })).toThrow(
      'cumulative',
    );
    usage.event({ type: 'message_stop' });
    expect(usage.finish()).toEqual({
      input_tokens: 7,
      output_tokens: 5,
      cached_input_tokens: 3,
      cache_write_input_tokens: 0,
    });
    expect(() => usage.event({ type: 'message_delta', usage: { output_tokens: 1 } })).toThrow(
      'after terminal',
    );
    expect(gatewayBase('http://localhost:1234')).toBe('http://localhost:1234/v1/proxy/deepseek');
    expect(() => gatewayBase('http://localhost/v1')).toThrow('origin');
  });
});
