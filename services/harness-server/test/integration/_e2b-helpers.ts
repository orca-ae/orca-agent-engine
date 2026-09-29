// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Sandbox as E2BSandbox } from '@e2b/code-interpreter';

export interface PublicS3Config {
  endpoint: string;
  bucket: string;
  region: string;
  accessKey: string;
  secretKey: string;
}

/**
 * Reads `S3_PUBLIC_*` env vars (set in `.env` for local validation; in
 * GitHub Actions secrets for nightly-e2b.yml). Returns null when any is
 * missing — the gated tests skip with a clear warn.
 *
 * The `S3_PUBLIC_ENDPOINT` MUST be reachable from the E2B sandbox (i.e., a
 * real S3 endpoint, not the GitHub runner's localhost RustFS). For
 * AWS-native, use `https://s3.<region>.amazonaws.com` (path-style); the
 * `s3fs` flag `use_path_request_style` works against both AWS S3 and MinIO,
 * so no special-casing is needed.
 */
export function readPublicS3Config(): PublicS3Config | null {
  const endpoint = process.env['S3_PUBLIC_ENDPOINT'];
  const bucket = process.env['S3_PUBLIC_BUCKET'];
  const region = process.env['S3_PUBLIC_REGION'];
  const accessKey = process.env['S3_PUBLIC_ACCESS_KEY_ID'];
  const secretKey = process.env['S3_PUBLIC_SECRET_ACCESS_KEY'];
  if (!endpoint || !bucket || !region || !accessKey || !secretKey) {
    return null;
  }
  return { endpoint, bucket, region, accessKey, secretKey };
}

/**
 * Common gate-reason logging so every E2B-gated spec emits a consistent
 * stderr line when it skips. Helps operators find the missing piece in CI
 * logs without scrolling through Vitest's collection output.
 */
export function logSkipReason(specName: string, reason: string): void {
  console.warn(`${specName}: skipping — ${reason}`);
}

export type { E2BSandbox };
