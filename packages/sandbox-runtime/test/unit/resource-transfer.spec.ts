// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import {
  parseResourceManifest,
  resourceFilePath,
  resourceManifestDigest,
  decodeResourceChunk,
  RESOURCE_CHUNK_BYTES,
  RESOURCE_MAX_FILE_BYTES,
  parseResourceCheckpoint,
  resourceCheckpointDigest,
  type ResourceCheckpoint,
  type ResourceManifest,
} from '../../src/resource-transfer.js';

const hash = createHash('sha256').update('file bytes').digest('hex');
function manifest(): ResourceManifest {
  return {
    version: 1,
    revision: hash,
    resources: [
      {
        resource_id: 'sesrsc_1',
        kind: 'memory_store',
        access: 'read_write',
        mount_path: '/mnt/memory/store',
        files: [{ path: 'nested/note.md', sha256: hash, size_bytes: 10, mode: 0o644 }],
      },
    ],
  };
}

describe('managed resource transfer boundary', () => {
  it('round-trips a bounded manifest without infrastructure locations', () => {
    const parsed = parseResourceManifest(manifest());
    expect(parsed).toEqual(manifest());
    expect(resourceFilePath(parsed.resources[0]!, parsed.resources[0]!.files[0]!)).toBe(
      '/mnt/memory/store/nested/note.md',
    );
    expect(resourceManifestDigest(parsed)).toBe(resourceManifestDigest(manifest()));
    const changed = manifest();
    changed.resources[0]!.files[0]!.sha256 = 'b'.repeat(64);
    expect(resourceManifestDigest(changed)).not.toBe(resourceManifestDigest(parsed));
    expect(parseResourceManifest({ version: 1, revision: hash, resources: [] }).resources).toEqual(
      [],
    );
  });
  it.each([
    '/',
    '/mnt/session',
    '/mnt/session/outputs',
    '/mnt/session/outputs/file',
    '/workspace',
    '/workspace/skills/a',
    '/.orca',
    '/.orca/git',
    '/.orca/git/credential.config',
    '/tmp/a',
    '/dev/foo',
    '/proc/1',
    '/sys/a',
    '/etc/config',
    '/usr/lib',
    '/bin/bash',
    '/lib64',
    '/mnt/../mnt/data',
    '//mnt/data',
    '/mnt/data\n',
    '/mnt/\ud800',
  ])('rejects reserved and aliased mounts: %j', (mount) => {
    const value = manifest();
    value.resources[0]!.mount_path = mount;
    expect(() => parseResourceManifest(value)).toThrow();
  });
  it.each([
    '',
    '.',
    '..',
    '../outside',
    '/etc/passwd',
    'a/../b',
    'a//b',
    'a/./b',
    'a/',
    'a\\b',
    'nul\0byte',
    'a\ud800',
  ])('rejects invalid relative files: %j', (relative) => {
    const value = manifest();
    value.resources[0]!.files[0]!.path = relative;
    expect(() => parseResourceManifest(value)).toThrow();
  });
  it('requires an exact read-only File mount', () => {
    const value = manifest();
    const resource = value.resources[0]!;
    resource.kind = 'file';
    resource.access = 'read_only';
    resource.files[0]!.path = '';
    const parsed = parseResourceManifest(value);
    expect(resourceFilePath(parsed.resources[0]!, parsed.resources[0]!.files[0]!)).toBe(
      resource.mount_path,
    );
    resource.access = 'read_write';
    expect(() => parseResourceManifest(value)).toThrow();
  });
  it('rejects duplicate, nested and overlapping bindings or files', () => {
    const value = manifest();
    value.resources.push(structuredClone(value.resources[0]!));
    expect(() => parseResourceManifest(value)).toThrow();
    value.resources[1]!.resource_id = 'sesrsc_2';
    value.resources[1]!.mount_path += '/child';
    expect(() => parseResourceManifest(value)).toThrow();
    const fileAlias = manifest();
    fileAlias.resources[0]!.files.push({ ...fileAlias.resources[0]!.files[0]!, path: 'nested' });
    expect(() => parseResourceManifest(fileAlias)).toThrow('file and directory');
    fileAlias.resources[0]!.files = ['a', 'a.txt', 'a/b'].map((path) => ({
      path,
      sha256: hash,
      size_bytes: 1,
      mode: 0o644,
    }));
    expect(() => parseResourceManifest(fileAlias)).toThrow('file and directory');
  });
  it('rejects non-string resource kinds without coercion', () => {
    const value = manifest();
    expect(() =>
      parseResourceManifest({
        ...value,
        resources: [{ ...value.resources[0], kind: ['file'] }],
      }),
    ).toThrow('invalid resource binding');
  });
  it('bounds individual and total bytes, file counts, and mode bits', () => {
    const value = manifest();
    value.resources[0]!.files[0]!.size_bytes = RESOURCE_MAX_FILE_BYTES + 1;
    expect(() => parseResourceManifest(value)).toThrow();
    value.resources[0]!.files = Array.from({ length: 5 }, (_, i) => ({
      path: `${i}`,
      sha256: hash,
      size_bytes: RESOURCE_MAX_FILE_BYTES,
      mode: 0o644,
    }));
    expect(() => parseResourceManifest(value)).toThrow('limits');
    value.resources[0]!.files = Array.from({ length: 10001 }, (_, i) => ({
      path: `${i}`,
      sha256: hash,
      size_bytes: 0,
      mode: 0o644,
    }));
    expect(() => parseResourceManifest(value)).toThrow('limits');
    const mode = manifest();
    mode.resources[0]!.files[0]!.mode = 0o4755;
    expect(() => parseResourceManifest(mode)).toThrow();
  });
  it('rejects tolerant base64 aliases and over-sized chunks', () => {
    expect(decodeResourceChunk(Buffer.from('bytes').toString('base64')).toString()).toBe('bytes');
    for (const invalid of [
      '',
      'YQ',
      'YQ==\n',
      '!!!',
      Buffer.alloc(RESOURCE_CHUNK_BYTES + 1).toString('base64'),
    ]) {
      expect(() => decodeResourceChunk(invalid)).toThrow();
    }
  });
});

describe('runner resource checkpoint boundary', () => {
  function checkpoint(): ResourceCheckpoint {
    return {
      version: 1,
      checkpoint_id: 'rchk_test',
      revision: hash,
      files: [
        {
          resource_id: 'session_outputs',
          path: 'result.txt',
          sha256: hash,
          size_bytes: 10,
          mode: 0o644,
        },
      ],
      deleted: [{ resource_id: 'sesrsc_memory', path: 'note.txt', previous_sha256: hash }],
    };
  }
  it('normalizes and hashes immutable changes without store identifiers', () => {
    expect(parseResourceCheckpoint(checkpoint())).toEqual(checkpoint());
    expect(resourceCheckpointDigest(checkpoint())).toBe(resourceCheckpointDigest(checkpoint()));
    const changed = checkpoint();
    changed.files[0]!.sha256 = 'b'.repeat(64);
    expect(resourceCheckpointDigest(changed)).not.toBe(resourceCheckpointDigest(checkpoint()));
  });
  it.each(['../escape', '/etc/passwd', 'a/../b', 'a//b', 'a\\b', '', 'a\ud800'])(
    'rejects unsafe paths %j',
    (path) => {
      const value = checkpoint();
      value.files[0]!.path = path;
      expect(() => parseResourceCheckpoint(value)).toThrow();
    },
  );
  it('rejects duplicates, output deletion, oversized files and live file aliases', () => {
    const duplicate = checkpoint();
    duplicate.files.push(duplicate.files[0]!);
    expect(() => parseResourceCheckpoint(duplicate)).toThrow('duplicate');
    const deleted = checkpoint();
    deleted.deleted[0]!.resource_id = 'session_outputs';
    expect(() => parseResourceCheckpoint(deleted)).toThrow('deletion');
    const large = checkpoint();
    large.files[0]!.size_bytes = RESOURCE_MAX_FILE_BYTES + 1;
    expect(() => parseResourceCheckpoint(large)).toThrow('descriptor');
    const overlap = checkpoint();
    overlap.files = ['a', 'a.txt', 'a/b'].map((path) => ({ ...overlap.files[0]!, path }));
    expect(() => parseResourceCheckpoint(overlap)).toThrow('overlap');
    const invalidNumber = checkpoint();
    expect(() =>
      parseResourceCheckpoint({
        ...invalidNumber,
        files: [{ ...invalidNumber.files[0], size_bytes: '10' }],
      }),
    ).toThrow('descriptor');
  });
  it('permits replacing a deleted memory file with a directory', () => {
    const value = checkpoint();
    value.files = [{ ...value.files[0]!, resource_id: 'sesrsc_memory', path: 'note.txt/child' }];
    expect(parseResourceCheckpoint(value)).toEqual(value);
  });
});
