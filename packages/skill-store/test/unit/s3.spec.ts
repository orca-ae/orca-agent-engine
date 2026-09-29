// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
  type S3Client,
} from '@aws-sdk/client-s3';
import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import { describe, expect, it, vi } from 'vitest';
import {
  S3SkillStore,
  SkillBundleIntegrityError,
  SkillBundleNotFoundError,
} from '../../src/index.js';

class FakeS3Client {
  readonly objects = new Map<string, Buffer>();
  putCalls = 0;
  conditionalConflictsRemaining = 0;
  nextGetBody?: unknown;

  async send(command: unknown): Promise<unknown> {
    if (command instanceof PutObjectCommand) {
      this.putCalls += 1;
      if (this.conditionalConflictsRemaining > 0) {
        this.conditionalConflictsRemaining -= 1;
        throw Object.assign(new Error('concurrent conditional request'), {
          name: 'ConditionalRequestConflict',
          $metadata: { httpStatusCode: 409 },
        });
      }
      const key = command.input.Key!;
      if (command.input.IfNoneMatch === '*' && this.objects.has(key)) {
        throw Object.assign(new Error('exists'), {
          name: 'PreconditionFailed',
          $metadata: { httpStatusCode: 412 },
        });
      }
      this.objects.set(key, Buffer.from(command.input.Body as Uint8Array));
      return {};
    }
    if (command instanceof GetObjectCommand) {
      const value = this.objects.get(command.input.Key!);
      if (!value) {
        throw Object.assign(new Error('missing'), {
          name: 'NoSuchKey',
          $metadata: { httpStatusCode: 404 },
        });
      }
      if (this.nextGetBody !== undefined) {
        const body = this.nextGetBody;
        this.nextGetBody = undefined;
        return { Body: body };
      }
      return { Body: Readable.from(Buffer.from(value)) };
    }
    if (command instanceof DeleteObjectCommand) {
      this.objects.delete(command.input.Key!);
      return {};
    }
    throw new Error('unsupported S3 command');
  }
}

describe('S3SkillStore', () => {
  it('uses a workspace/version/digest-isolated key and treats repeated put as idempotent', async () => {
    const client = new FakeS3Client();
    const store = new S3SkillStore({
      client: client as unknown as S3Client,
      bucket: 'orca-skills',
      keyPrefix: 'managed-agents',
    });
    const files = [{ path: 'demo/SKILL.md', content: Buffer.from('# Demo') }];

    const first = await store.put('ws_alpha', 'sklv_one', files);
    const second = await store.put('ws_alpha', 'sklv_one', files);

    expect(second).toEqual(first);
    expect(client.putCalls).toBe(2);
    expect([...client.objects.keys()]).toEqual([
      `managed-agents/workspaces/ws_alpha/skill-versions/sklv_one/bundles/${first.sha256}/bundle.json`,
    ]);
    expect(await store.open('ws_alpha', 'sklv_one', first.sha256)).toMatchObject({
      record: first,
    });
  });

  it('retries an S3 conditional-write conflict', async () => {
    const client = new FakeS3Client();
    client.conditionalConflictsRemaining = 1;
    const store = new S3SkillStore({
      client: client as unknown as S3Client,
      bucket: 'orca-skills',
    });

    const record = await store.put('ws_alpha', 'sklv_one', [
      { path: 'SKILL.md', content: Buffer.from('# Demo') },
    ]);

    expect(client.putCalls).toBe(2);
    await expect(store.open('ws_alpha', 'sklv_one', record.sha256)).resolves.toMatchObject({
      record,
    });
  });

  it('rejects tampered envelope bytes and content hashes', async () => {
    const client = new FakeS3Client();
    const store = new S3SkillStore({
      client: client as unknown as S3Client,
      bucket: 'orca-skills',
    });
    const record = await store.put('ws_alpha', 'sklv_one', [
      { path: 'demo/SKILL.md', content: Buffer.from('# Demo') },
    ]);
    const key = [...client.objects.keys()][0]!;
    client.objects.set(key, Buffer.from('{"tampered":true}'));

    await expect(store.open('ws_alpha', 'sklv_one', record.sha256)).rejects.toBeInstanceOf(
      SkillBundleIntegrityError,
    );
  });

  it('rejects a self-consistent envelope with a non-canonical file mode', async () => {
    const client = new FakeS3Client();
    const store = new S3SkillStore({
      client: client as unknown as S3Client,
      bucket: 'orca-skills',
    });
    const content = Buffer.from('# Demo');
    const bytes = Buffer.from(
      JSON.stringify({
        format: 'orca.skill-bundle.v1',
        files: [
          {
            path: 'SKILL.md',
            mode: 0o777,
            mimeType: 'text/markdown',
            sha256: createHash('sha256').update(content).digest('hex'),
            contentBase64: content.toString('base64'),
          },
        ],
      }),
    );
    const digest = createHash('sha256').update(bytes).digest('hex');
    client.objects.set(
      `workspaces/ws_alpha/skill-versions/sklv_one/bundles/${digest}/bundle.json`,
      bytes,
    );

    await expect(store.open('ws_alpha', 'sklv_one', digest)).rejects.toBeInstanceOf(
      SkillBundleIntegrityError,
    );
  });

  it.each([
    ['content digest', (file: Record<string, unknown>) => (file['sha256'] = '0'.repeat(64))],
    ['unsafe path', (file: Record<string, unknown>) => (file['path'] = '../escape')],
  ])('rejects a self-digested envelope with an invalid %s', async (_case, mutate) => {
    const client = new FakeS3Client();
    const store = new S3SkillStore({
      client: client as unknown as S3Client,
      bucket: 'orca-skills',
    });
    const record = await store.put('ws_alpha', 'sklv_one', [
      { path: 'SKILL.md', content: Buffer.from('# Demo') },
    ]);
    const originalKey = [...client.objects.keys()][0]!;
    const envelope = JSON.parse(client.objects.get(originalKey)!.toString()) as {
      files: Array<Record<string, unknown>>;
    };
    mutate(envelope.files[0]!);
    const tamperedBytes = Buffer.from(JSON.stringify(envelope));
    const tamperedDigest = createHash('sha256').update(tamperedBytes).digest('hex');
    client.objects.set(originalKey.replace(record.sha256, tamperedDigest), tamperedBytes);

    await expect(store.open('ws_alpha', 'sklv_one', tamperedDigest)).rejects.toBeInstanceOf(
      SkillBundleIntegrityError,
    );
  });

  it('rejects a self-digested envelope with a file/directory collision', async () => {
    const client = new FakeS3Client();
    const store = new S3SkillStore({
      client: client as unknown as S3Client,
      bucket: 'orca-skills',
    });
    const record = await store.put('ws_alpha', 'sklv_one', [
      { path: 'references', content: Buffer.from('file') },
      { path: 'separate.txt', content: Buffer.from('nested') },
    ]);
    const originalKey = [...client.objects.keys()][0]!;
    const envelope = JSON.parse(client.objects.get(originalKey)!.toString()) as {
      files: Array<Record<string, unknown>>;
    };
    envelope.files[1]!['path'] = 'references/example.txt';
    const tamperedBytes = Buffer.from(JSON.stringify(envelope));
    const tamperedDigest = createHash('sha256').update(tamperedBytes).digest('hex');
    client.objects.set(originalKey.replace(record.sha256, tamperedDigest), tamperedBytes);

    await expect(store.open('ws_alpha', 'sklv_one', tamperedDigest)).rejects.toBeInstanceOf(
      SkillBundleIntegrityError,
    );
  });

  it('maps missing S3 objects to SkillBundleNotFoundError', async () => {
    const client = new FakeS3Client();
    const store = new S3SkillStore({
      client: client as unknown as S3Client,
      bucket: 'orca-skills',
    });

    await expect(store.open('ws_alpha', 'sklv_one', 'a'.repeat(64))).rejects.toBeInstanceOf(
      SkillBundleNotFoundError,
    );
  });

  it('prefers bounded async iteration when an SDK body also exposes transformToByteArray', async () => {
    const client = new FakeS3Client();
    const store = new S3SkillStore({
      client: client as unknown as S3Client,
      bucket: 'orca-skills',
    });
    const record = await store.put('ws_alpha', 'sklv_one', [
      { path: 'SKILL.md', content: Buffer.from('# Demo') },
    ]);
    const bytes = [...client.objects.values()][0]!;
    const transformToByteArray = vi.fn(() => {
      throw new Error('must not buffer the full SDK body');
    });
    client.nextGetBody = {
      transformToByteArray,
      async *[Symbol.asyncIterator]() {
        yield bytes.subarray(0, 7);
        yield bytes.subarray(7);
      },
    };

    await expect(store.open('ws_alpha', 'sklv_one', record.sha256)).resolves.toMatchObject({
      record,
    });
    expect(transformToByteArray).not.toHaveBeenCalled();
  });

  it('stops an async SDK body as soon as the 64 MiB encoded limit is exceeded', async () => {
    const client = new FakeS3Client();
    const store = new S3SkillStore({
      client: client as unknown as S3Client,
      bucket: 'orca-skills',
    });
    const record = await store.put('ws_alpha', 'sklv_one', [
      { path: 'SKILL.md', content: Buffer.from('# Demo') },
    ]);
    const oneMiB = Buffer.alloc(1024 * 1024);
    let yieldedChunks = 0;
    const transformToByteArray = vi.fn(() => {
      throw new Error('must not buffer the full SDK body');
    });
    client.nextGetBody = {
      transformToByteArray,
      async *[Symbol.asyncIterator]() {
        while (yieldedChunks < 100) {
          yieldedChunks += 1;
          yield oneMiB;
        }
      },
    };

    await expect(store.open('ws_alpha', 'sklv_one', record.sha256)).rejects.toThrow(
      /exceeds 67108864 encoded byte limit/,
    );
    expect(yieldedChunks).toBe(65);
    expect(transformToByteArray).not.toHaveBeenCalled();
  });

  it.each(['', '../ws_beta', 'ws/alpha', 'ws*alpha', 'ws\\alpha'])(
    'rejects unsafe workspace id %j before issuing S3 calls',
    async (workspaceId) => {
      const client = new FakeS3Client();
      const store = new S3SkillStore({
        client: client as unknown as S3Client,
        bucket: 'orca-skills',
      });

      await expect(
        store.put(workspaceId, 'sklv_one', [{ path: 'demo/SKILL.md', content: Buffer.from('x') }]),
      ).rejects.toThrow(/invalid workspaceId/);
      expect(client.putCalls).toBe(0);
    },
  );
});
