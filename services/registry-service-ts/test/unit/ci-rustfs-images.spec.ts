// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import { loadAll } from 'js-yaml';
import { describe, expect, it } from 'vitest';

// RustFS serves S3 in CI and the local stacks, and its `rc` client creates the
// bucket. Both are pinned, so a new upstream release cannot change CI without a
// commit that updates every copy.
const expectedImages = ['rustfs/rustfs:1.0.0', 'rustfs/rc:v0.1.36'].sort();
const kindWorkflow = '.github/workflows/e2e-kind-helm.yml';
const kindManifest = 'charts/orca-managed-agents/test/kind/infra.yaml';
const configs = [
  '.github/workflows/test-ts.yml',
  '.github/workflows/nightly-e2b.yml',
  kindWorkflow,
  'services/dev/docker-compose.yml',
  'services/dev/docker-compose.self-hosted.yml',
  kindManifest,
];

function readRepositoryFile(path: string): string {
  return readFileSync(new URL(`../../../../${path}`, import.meta.url), 'utf8');
}

function strings(value: unknown): string[] {
  if (typeof value === 'string') return [value];
  if (value && typeof value === 'object') return Object.values(value).flatMap(strings);
  return [];
}

// Also matches MinIO's server and client images, from Quay, Docker Hub or
// Chainguard, so a reintroduced one fails the comparisons below.
const objectStoreImage =
  /[\w./-]*(?:rustfs\/(?:rustfs|rc)|minio\/(?:minio|mc)|chainguard\/minio(?:-client)?):[\w.:-]+/g;

function objectStoreImages(values: string[]): string[] {
  return values.flatMap((value) => value.match(objectStoreImage) ?? []);
}

describe('RustFS infrastructure images', () => {
  it('uses the pinned RustFS images in CI and local stacks, and pulls and loads the Kind manifest images', () => {
    for (const path of configs) {
      const images = objectStoreImages(strings(loadAll(readRepositoryFile(path))));
      expect([...new Set(images)].sort(), path).toEqual(expectedImages);
    }

    const workflow = loadAll(readRepositoryFile(kindWorkflow))[0] as {
      jobs: Record<string, { steps?: { name?: string; run?: string }[] }>;
    };
    const steps = Object.values(workflow.jobs).flatMap((job) => job.steps ?? []);
    const manifestImages = objectStoreImages(
      strings(loadAll(readRepositoryFile(kindManifest))),
    ).sort();
    for (const name of [
      'Pull Kind infrastructure images',
      'Verify required local images',
      'Load required images into Kind',
    ]) {
      const step = steps.find((candidate) => candidate.name === name);
      expect(step, name).toBeDefined();
      expect(objectStoreImages([step?.run ?? '']).sort(), name).toEqual(manifestImages);
    }

    const inventory = readRepositoryFile('docs/managed-agents/kubernetes.md');
    expect([...new Set(objectStoreImages([inventory]))].sort()).toEqual(manifestImages);
  });
});
