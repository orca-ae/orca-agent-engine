// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import yaml from 'js-yaml';

const workflow = yaml.load(
  readFileSync(new URL('../workflows/release-images.yml', import.meta.url), 'utf8'),
);
const { jobs } = workflow;
const stepUsing = (job, action) => job.steps.find((step) => step.uses?.startsWith(action + '@'));
const stepsUsing = (job, action) => job.steps.filter((step) => step.uses?.startsWith(action + '@'));
const upstreamGuard = "github.repository == 'orca-ae/orca-agent-engine'";
const imageNames = [
  'orca-observability-exporter',
  'orca-registry-service-ts',
  'orca-harness-server',
  'sandbox-harness-claude-code',
];

test('both architectures build natively without weakening validation or attestations', () => {
  const build = jobs['build-images'];
  assert.deepEqual(build.strategy.matrix.image, imageNames);
  assert.deepEqual(build.strategy.matrix.platform, [
    { name: 'linux/amd64', pair: 'linux-amd64', runner: 'ubuntu-24.04' },
    { name: 'linux/arm64', pair: 'linux-arm64', runner: 'ubuntu-24.04-arm' },
  ]);
  assert.equal(build['runs-on'], '${{ matrix.platform.runner }}');
  const opensandbox = jobs['build-opensandbox-platforms'];
  assert.equal(opensandbox['runs-on'], '${{ matrix.runner }}');
  assert.deepEqual(
    opensandbox.strategy.matrix.include.map(({ platform, runner }) => [platform, runner]),
    [
      ['linux/amd64', 'ubuntu-24.04'],
      ['linux/arm64', 'ubuntu-24.04-arm'],
    ],
  );
  for (const job of [build, opensandbox]) {
    assert.ok(job.needs.includes('validate'));
    assert.equal(stepUsing(job, 'docker/setup-qemu-action'), undefined);
    const options = stepUsing(job, 'docker/build-push-action').with;
    assert.match(options.outputs, /publish == 'true'.*push-by-digest=true.*push=true/);
    assert.equal(options.push, undefined);
    assert.doesNotMatch(options.tags, /meta.outputs.tags/);
    assert.match(options.provenance, /publish == 'true'/);
    assert.match(options.sbom, /publish == 'true'/);
  }
  const options = stepUsing(build, 'docker/build-push-action').with;
  assert.equal(options.platforms, '${{ matrix.platform.name }}');
  for (const key of ['cache-from', 'cache-to']) {
    assert.match(options[key], /matrix.image/);
    assert.match(options[key], /matrix.platform.pair/);
  }
});

test('digest artifacts are isolated by image and platform', () => {
  assert.equal(
    stepUsing(jobs['build-images'], 'actions/upload-artifact').with.name,
    'release-digest-${{ matrix.image }}-${{ matrix.platform.pair }}',
  );
  assert.equal(
    stepUsing(jobs['build-opensandbox-platforms'], 'actions/upload-artifact').with.name,
    'release-digest-orca-opensandbox-code-interpreter-${{ matrix.platform_pair }}',
  );
  const merge = jobs['merge-images'];
  assert.deepEqual(merge.strategy.matrix.image, [
    ...imageNames,
    'orca-opensandbox-code-interpreter',
  ]);
  assert.match(merge.if, /publish == 'true'/);
  assert.equal(
    stepUsing(merge, 'actions/download-artifact').with.pattern,
    'release-digest-${{ matrix.image }}-*',
  );
});

test('tag publication waits for both build matrices and retains the implicit success gate', () => {
  const merge = jobs['merge-images'];
  assert.deepEqual(merge.needs, [
    'release-metadata',
    'build-images',
    'build-opensandbox-platforms',
  ]);
  // No always()/failure() override: GitHub Actions must skip every merge leg
  // if either build matrix fails, even when publication was requested.
  assert.equal(merge.if, `${upstreamGuard} && needs.release-metadata.outputs.publish == 'true'`);
});

test('every release job is guarded to the upstream repository', () => {
  for (const [name, job] of Object.entries(jobs)) {
    assert.ok(job.if?.includes(upstreamGuard), `${name} must run only in the upstream repository`);
  }
});

test('images push to the one IMAGE_REGISTRY with GITHUB_TOKEN; only pushing jobs may write packages', () => {
  assert.equal(workflow.env.IMAGE_REGISTRY, 'ghcr.io/orca-ae');
  assert.deepEqual(workflow.permissions, { contents: 'read' });
  const writers = ['build-images', 'build-opensandbox-platforms', 'merge-images'];
  for (const [name, job] of Object.entries(jobs)) {
    const packages = job.permissions?.packages;
    if (writers.includes(name)) assert.equal(packages, 'write', name);
    else assert.notEqual(packages, 'write', name);
  }
  for (const name of writers) {
    const job = jobs[name];
    const logins = stepsUsing(job, 'docker/login-action');
    assert.equal(logins.length, 1, name);
    assert.deepEqual(logins[0].with, {
      registry: 'ghcr.io',
      username: '${{ github.actor }}',
      password: '${{ secrets.GITHUB_TOKEN }}',
    });
    assert.match(
      stepUsing(job, 'docker/metadata-action').with.images,
      /^\$\{\{ env\.IMAGE_REGISTRY \}\}\//,
    );
  }
  // Docker Hub appears only in the optional mirror; every other job is registry-neutral.
  for (const [name, job] of Object.entries(jobs)) {
    if (name !== 'mirror-docker-hub') {
      assert.doesNotMatch(JSON.stringify(job), /docker\.io|DOCKERHUB_/, name);
    }
  }
});

test('the Docker Hub mirror runs last, only when a namespace is configured', () => {
  const mirror = jobs['mirror-docker-hub'];
  assert.deepEqual(mirror.needs, ['release-metadata', 'merge-images', 'release-chart']);
  assert.match(mirror.if, /needs\.release-metadata\.outputs\.publish == 'true'/);
  assert.match(mirror.if, /vars\.DOCKERHUB_NAMESPACE != ''/);
  // No always(): a failed release must never be mirrored.
  assert.doesNotMatch(mirror.if, /always\(\)|failure\(\)/);
  assert.equal(mirror.permissions.packages, 'read');
  assert.deepEqual(mirror.strategy.matrix.image, jobs['merge-images'].strategy.matrix.image);
});

test('chart uses the merged sandbox manifest, with build-only mode still supported', () => {
  const merge = jobs['merge-images'];
  assert.equal(
    merge.outputs.sandbox_harness_digest,
    "${{ steps.inspect.outputs['digest_sandbox-harness-claude-code'] }}",
  );
  const inspect = merge.steps.find((step) => step.id === 'inspect');
  assert.equal(inspect.env.IMAGE_KEY, '${{ matrix.image }}');
  assert.ok(inspect.run.includes('digest_${IMAGE_KEY}=${digest}'));
  const chart = jobs['release-chart'];
  for (const job of ['validate', 'build-images', 'build-opensandbox-platforms', 'merge-images']) {
    assert.ok(chart.needs.includes(job));
  }
  assert.match(chart.if, /needs.merge-images.result == 'skipped'/);
  assert.equal(
    chart.steps.find((step) => step.id === 'chart-images').env.SANDBOX_HARNESS_DIGEST,
    '${{ needs.merge-images.outputs.sandbox_harness_digest }}',
  );
});

// Run the workflow's real shell scripts; only registry access and retry delays
// are replaced. jq still validates the same manifest responses as production.
function runManifestStep(
  stepId,
  { digests = [], manifest = {}, expectedDigest = 'sha256:' + 'c'.repeat(64) } = {},
) {
  const directory = mkdtempSync(join(tmpdir(), 'orca-release-test-'));
  try {
    mkdirSync(join(directory, 'bin'));
    mkdirSync(join(directory, 'digests'));
    for (const digest of digests) writeFileSync(join(directory, 'digests', digest), '');
    const outputPath = join(directory, 'output');
    const argsPath = join(directory, 'docker-args');
    writeFileSync(outputPath, '');
    writeFileSync(argsPath, '');
    writeFileSync(
      join(directory, 'bin/docker'),
      [
        '#!/usr/bin/env bash',
        'set -euo pipefail',
        'printf "%s\\n" "$@" >> "$RUNNER_TEMP/docker-args"',
        'if [[ "$3" == inspect ]]; then printf "%s" "$TEST_MANIFEST"; exit 0; fi',
        'while (( $# )); do',
        '  if [[ "$1" == --metadata-file ]]; then',
        '    jq -n --arg digest "$EXPECTED_DIGEST" \'{"containerimage.descriptor": {digest: $digest}}\' > "$2"',
        '    exit 0',
        '  fi',
        '  shift',
        'done',
        'exit 1',
      ].join('\n'),
      { mode: 0o755 },
    );
    writeFileSync(join(directory, 'bin/sleep'), '#!/usr/bin/env bash\nexit 0\n', { mode: 0o755 });
    const result = spawnSync(
      'bash',
      ['-c', jobs['merge-images'].steps.find((step) => step.id === stepId).run],
      {
        cwd: join(directory, 'digests'),
        encoding: 'utf8',
        env: {
          ...process.env,
          PATH: join(directory, 'bin') + ':' + process.env.PATH,
          RUNNER_TEMP: directory,
          GITHUB_OUTPUT: outputPath,
          IMAGE: 'ghcr.io/example/service',
          IMAGE_KEY: 'sandbox-harness-claude-code',
          VERSION: '1.2.3',
          TAGS: 'ghcr.io/example/service:1.2.3\nghcr.io/example/service:sha-abcdef0',
          EXPECTED_DIGEST: expectedDigest,
          TEST_MANIFEST: JSON.stringify(manifest),
        },
      },
    );
    return {
      ...result,
      args: readFileSync(argsPath, 'utf8'),
      output: readFileSync(outputPath, 'utf8'),
    };
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

test('merge tags exactly two immutable platform digests and returns the index digest', () => {
  const digests = ['a'.repeat(64), 'b'.repeat(64)];
  const result = runManifestStep('create', { digests });
  assert.equal(result.status, 0, result.stderr);
  for (const digest of digests)
    assert.ok(result.args.includes('ghcr.io/example/service@sha256:' + digest));
  assert.ok(result.args.includes('--tag\nghcr.io/example/service:1.2.3\n'));
  assert.ok(result.args.includes('--tag\nghcr.io/example/service:sha-abcdef0\n'));
  assert.equal(result.output, 'digest=sha256:' + 'c'.repeat(64) + '\n');
});

test('merge rejects missing, excess, and malformed digest artifacts before publishing', () => {
  for (const digests of [
    [],
    ['a'.repeat(64)],
    ['a'.repeat(64), 'b'.repeat(64), 'c'.repeat(64)],
    ['bad', 'a'.repeat(64)],
  ]) {
    const result = runManifestStep('create', { digests });
    assert.notEqual(result.status, 0);
    assert.match(result.stdout, /Expected 2 platform digests|Invalid digest artifact name/);
    assert.equal(result.args, '');
    assert.equal(result.output, '');
  }
});

const publishedManifest = {
  digest: 'sha256:' + 'c'.repeat(64),
  manifests: ['amd64', 'arm64'].map((architecture) => ({
    platform: { os: 'linux', architecture },
  })),
};

test('published manifest verification ignores attestations and exports the checked index for chart pinning', () => {
  const result = runManifestStep('inspect', {
    manifest: {
      ...publishedManifest,
      manifests: [
        ...publishedManifest.manifests,
        { platform: { os: 'unknown', architecture: 'unknown' } },
      ],
    },
  });
  assert.equal(result.status, 0, result.stderr);
  assert.ok(
    result.output.includes('digest_sandbox-harness-claude-code=' + publishedManifest.digest + '\n'),
  );
  assert.ok(result.output.includes('platforms=linux/amd64,linux/arm64\n'));
});

test('a stale digest, missing architecture, or invalid response cannot become a chart pin', () => {
  for (const manifest of [
    { ...publishedManifest, digest: 'sha256:' + 'd'.repeat(64) },
    { ...publishedManifest, manifests: publishedManifest.manifests.slice(0, 1) },
    {},
  ]) {
    const result = runManifestStep('inspect', { manifest });
    assert.notEqual(result.status, 0);
    assert.match(result.stdout, /Could not inspect published manifest after 7 attempts/);
    assert.equal(result.output, '');
  }
});

// Runs the mirror job's real copy script against a docker stub that answers
// `imagetools inspect` with TEST_MANIFEST and records every other call.
function runMirrorStep({ manifest = publishedManifest, stable = 'true' } = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'orca-mirror-test-'));
  try {
    mkdirSync(join(directory, 'bin'));
    const argsPath = join(directory, 'docker-args');
    writeFileSync(argsPath, '');
    writeFileSync(
      join(directory, 'bin/docker'),
      [
        '#!/usr/bin/env bash',
        'set -euo pipefail',
        'if [[ "$3" == inspect ]]; then printf "%s" "$TEST_MANIFEST"; exit 0; fi',
        'printf "%s\\n" "$@" >> "$RUNNER_TEMP/docker-args"',
      ].join('\n'),
      { mode: 0o755 },
    );
    const step = jobs['mirror-docker-hub'].steps.find(
      (s) => s.name === 'Copy the release manifest to Docker Hub',
    );
    const result = spawnSync('bash', ['-c', step.run], {
      cwd: directory,
      encoding: 'utf8',
      env: {
        ...process.env,
        PATH: join(directory, 'bin') + ':' + process.env.PATH,
        RUNNER_TEMP: directory,
        GITHUB_STEP_SUMMARY: join(directory, 'summary'),
        SOURCE: 'ghcr.io/example/service',
        TARGET: 'docker.io/example/service',
        VERSION: '1.2.3',
        STABLE: stable,
        TEST_MANIFEST: JSON.stringify(manifest),
      },
    });
    return { ...result, args: readFileSync(argsPath, 'utf8') };
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

test('the mirror copies the resolved GHCR index, adding latest only for a stable release', () => {
  const source = 'ghcr.io/example/service@' + publishedManifest.digest + '\n';
  const stable = runMirrorStep();
  assert.equal(stable.status, 0, stable.stderr);
  assert.ok(stable.args.startsWith('buildx\nimagetools\ncreate\n'));
  assert.ok(stable.args.includes('--tag\ndocker.io/example/service:1.2.3\n'));
  assert.ok(stable.args.includes('--tag\ndocker.io/example/service:latest\n'));
  assert.ok(stable.args.endsWith(source));

  const prerelease = runMirrorStep({ stable: 'false' });
  assert.equal(prerelease.status, 0, prerelease.stderr);
  assert.ok(prerelease.args.includes('--tag\ndocker.io/example/service:1.2.3\n'));
  assert.doesNotMatch(prerelease.args, /:latest/);

  const unresolved = runMirrorStep({ manifest: {} });
  assert.notEqual(unresolved.status, 0);
  assert.equal(unresolved.args, '');
});
