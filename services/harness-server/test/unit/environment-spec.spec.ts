// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { buildEnvironmentSpec } from '../../src/sandbox/environment-spec.js';
import { HARNESS_CATALOG } from '@orca/harness-catalog';

describe('buildEnvironmentSpec', () => {
  it('returns an empty spec for null/empty environments', () => {
    expect(buildEnvironmentSpec(null)).toEqual({});
    expect(
      buildEnvironmentSpec({
        id: 'env_x',
        workspace_id: 'ws_test',
        name: 'n',
        packages: {},
        networking: {},
      }),
    ).toEqual({});
  });
  it('maps packages and networking through', () => {
    expect(
      buildEnvironmentSpec({
        id: 'env_x',
        workspace_id: 'ws_test',
        name: 'n',
        packages: { apt: ['curl'], npm: ['typescript'] },
        networking: { egress: 'all' },
      }),
    ).toEqual({ packages: { apt: ['curl'], npm: ['typescript'] }, networking: { egress: 'all' } });
  });
  it('maps target through when set', () => {
    expect(
      buildEnvironmentSpec({ id: 'env_x', workspace_id: 'ws_test', name: 'n', target: 'cloud' }),
    ).toEqual({ target: 'cloud' });
  });
  it('gives both cloud SDKs the same image, entrypoint, port and upload ownership', () => {
    expect(buildEnvironmentSpec(null, { harness: 'codex_sdk', mode: 'colocated' })).toEqual(
      buildEnvironmentSpec(null, { harness: 'claude_code', mode: 'colocated' }),
    );
    expect(buildEnvironmentSpec(null, { harness: 'codex_sdk', mode: 'separate' })).toEqual(
      buildEnvironmentSpec(null, { harness: 'claude_agent_sdk', mode: 'separate' }),
    );
  });

  it('resolves the colocated image from the catalog (no override)', () => {
    const spec = buildEnvironmentSpec(null, { harness: 'claude_code', mode: 'colocated' });
    expect(spec.image).toBe(HARNESS_CATALOG.claude_code.defaultImage);
    expect(spec.entrypoint).toEqual(['/usr/local/bin/orca-sandbox-harness']);
    expect(spec.exposePorts).toEqual([4096]);
    expect(spec.fileUploadOwnership).toEqual({ owner: 'node', group: 'node' });
  });
  it('prefers a runtime default-image override over the catalog default', () => {
    const spec = buildEnvironmentSpec(
      null,
      { harness: 'claude_code', mode: 'colocated' },
      {
        claude_code: 'docker.io/orcaae/sandbox-harness-claude-code:0.3.0',
      },
    );
    expect(spec.image).toBe('docker.io/orcaae/sandbox-harness-claude-code:0.3.0');
  });
  it('still prefers the Environment image over a runtime default-image override', () => {
    const spec = buildEnvironmentSpec(
      { id: 'env_x', workspace_id: 'ws_test', name: 'n', image: 'ghcr.io/custom:9' },
      { harness: 'claude_code', mode: 'colocated' },
      { claude_code: 'docker.io/orcaae/sandbox-harness-claude-code:0.3.0' },
    );
    expect(spec.image).toBe('ghcr.io/custom:9');
  });
  it('ignores a default-image override for a harness with no entry', () => {
    const spec = buildEnvironmentSpec(
      null,
      { harness: 'codex', mode: 'colocated' },
      {
        claude_code: 'docker.io/orcaae/sandbox-harness-claude-code:0.3.0',
      },
    );
    expect(spec.image).toBe(HARNESS_CATALOG.codex.defaultImage);
  });
  it('prefers the Environment image override for colocated', () => {
    const spec = buildEnvironmentSpec(
      { id: 'env_x', workspace_id: 'ws_test', name: 'n', image: 'ghcr.io/custom:9' },
      { harness: 'codex', mode: 'colocated' },
    );
    expect(spec.image).toBe('ghcr.io/custom:9');
    expect(spec.entrypoint).toEqual(['/usr/local/bin/orca-sandbox-harness']);
    expect(spec.exposePorts).toEqual([4096]);
    expect(spec.fileUploadOwnership).toEqual({ owner: 'node', group: 'node' });
  });
  it('adds no image for separate mode', () => {
    const spec = buildEnvironmentSpec(
      { id: 'env_x', workspace_id: 'ws_test', name: 'n' },
      { harness: 'claude_agent_sdk', mode: 'separate' },
    );
    expect(spec.image).toBeUndefined();
    expect(spec.entrypoint).toBeUndefined();
    expect(spec.exposePorts).toBeUndefined();
    expect(spec.fileUploadOwnership).toEqual({ owner: 'ubuntu', group: 'ubuntu' });
  });
});
