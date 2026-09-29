// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  CreateBucketCommand,
  HeadBucketCommand,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { randomBytes } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { encodeSkillBundle } from '../../src/codec.js';
import { skillBundleKey } from '../../src/key.js';
import { S3SkillStore } from '../../src/s3.js';
import { SkillBundleIntegrityError } from '../../src/types.js';

const ENDPOINT = process.env['S3_ENDPOINT'] ?? 'http://localhost:9000';
const ACCESS_KEY = process.env['S3_ACCESS_KEY'] ?? 'minioadmin';
const SECRET_KEY = process.env['S3_SECRET_KEY'] ?? 'minioadmin';
const BUCKET = process.env['S3_BUCKET'] ?? 'orca-files';
const REGION = process.env['S3_REGION'] ?? 'us-east-1';
const KEY_PREFIX = 'test/skill-store/';

function uniqueId(prefix: string): string {
  return `${prefix}_${Date.now()}_${randomBytes(4).toString('hex')}`;
}

describe('S3SkillStore (integration)', () => {
  let client: S3Client;
  let store: S3SkillStore;

  beforeAll(async () => {
    client = new S3Client({
      endpoint: ENDPOINT,
      region: REGION,
      credentials: { accessKeyId: ACCESS_KEY, secretAccessKey: SECRET_KEY },
      forcePathStyle: true,
    });
    try {
      await client.send(new HeadBucketCommand({ Bucket: BUCKET }));
    } catch {
      try {
        await client.send(new CreateBucketCommand({ Bucket: BUCKET }));
      } catch {
        /* race or already exists; the put tests below catch real failures */
      }
    }
    store = new S3SkillStore({ client, bucket: BUCKET, keyPrefix: KEY_PREFIX });
  });

  afterAll(async () => {
    client?.destroy();
  });

  it('makes concurrent writes of the same canonical bundle idempotent', async () => {
    const workspaceId = uniqueId('ws');
    const versionId = uniqueId('skillver');
    const files = [{ path: 'demo/SKILL.md', content: Buffer.from('# Demo') }];

    const records = await Promise.all(
      Array.from({ length: 8 }, () => store.put(workspaceId, versionId, files)),
    );

    expect(records.every((record) => record.sha256 === records[0]!.sha256)).toBe(true);
    await expect(store.open(workspaceId, versionId, records[0]!.sha256)).resolves.toMatchObject({
      record: records[0],
    });
    await store.delete(workspaceId, versionId, records[0]!.sha256);
  });

  it('does not overwrite pre-existing bytes at a digest-derived key', async () => {
    const workspaceId = uniqueId('ws');
    const versionId = uniqueId('skillver');
    const files = [{ path: 'demo/SKILL.md', content: Buffer.from('# Demo') }];
    const encoded = encodeSkillBundle(files);
    const digest = encoded.bundle.record.sha256;
    const key = skillBundleKey(KEY_PREFIX, workspaceId, versionId, digest);
    await client.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: key,
        Body: Buffer.from('pre-existing-corrupt-bytes'),
      }),
    );

    await expect(store.put(workspaceId, versionId, files)).rejects.toBeInstanceOf(
      SkillBundleIntegrityError,
    );
    await store.delete(workspaceId, versionId, digest);
  });
});
