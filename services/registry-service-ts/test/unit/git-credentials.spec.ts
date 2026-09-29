// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  managedGitCredentialSecretRef,
  purgeGitCredentialSecretRefs,
  repositoryUrlsEqual,
  stageManagedGitCredentials,
  urlMatches,
  validateGitRepositoryUrl,
  type ManagedGitCredentialDraft,
} from '../../src/domain/git-credentials.js';
import { LocalSecretStore } from '../../src/secrets/local-store.js';

function draft(id: string, token: string): ManagedGitCredentialDraft {
  return {
    id,
    workspaceId: 'ws_test',
    sessionResourceId: `sesrsc_${id}`,
    repoUrl: 'https://github.com/orca/test',
    secretRef: managedGitCredentialSecretRef('ws_test', id),
    authorizationToken: token,
  };
}

describe('managed Git credential secrets', () => {
  it('stages and purges opaque secret references without embedding token bytes', async () => {
    const store = new LocalSecretStore();
    const first = draft('gitcred_first', 'ghp_first');
    const second = draft('gitcred_second', 'ghs_second');

    const refs = await stageManagedGitCredentials(store, [first, second]);

    expect(refs).toEqual([first.secretRef, second.secretRef]);
    expect(refs.join(' ')).not.toContain('ghp_first');
    expect(await store.resolve(first.secretRef)).toBe('ghp_first');
    expect(await store.resolve(second.secretRef)).toBe('ghs_second');

    await purgeGitCredentialSecretRefs(store, refs);
    expect(await store.resolve(first.secretRef)).toBeNull();
    expect(await store.resolve(second.secretRef)).toBeNull();
  });

  it('best-effort purges every attempted reference when a staged write fails', async () => {
    const first = draft('gitcred_first', 'ghp_first');
    const second = draft('gitcred_second', 'ghp_second');
    const deleteSecret = vi.fn().mockResolvedValue(undefined);
    const store = {
      resolve: vi.fn().mockResolvedValue(null),
      put: vi
        .fn()
        .mockResolvedValueOnce(first.secretRef)
        .mockRejectedValueOnce(new Error('write failed')),
      delete: deleteSecret,
    };

    await expect(stageManagedGitCredentials(store, [first, second])).rejects.toThrow(
      'write failed',
    );
    expect(deleteSecret).toHaveBeenCalledWith(first.secretRef);
    expect(deleteSecret).toHaveBeenCalledWith(second.secretRef);
  });
});

describe('urlMatches', () => {
  it('allows Git smart-HTTP suffixes only under the bound repository', () => {
    expect(
      urlMatches('https://github.com/orca/test.git', 'https://github.com/orca/test.git/info/refs'),
    ).toBe(true);
    expect(urlMatches('https://github.com/orca/test', 'https://github.com/orca/test-sibling')).toBe(
      false,
    );
    expect(urlMatches('https://github.com/orca/test', 'https://evil.example/orca/test')).toBe(
      false,
    );
  });

  it('compares repository URLs without trailing slashes or .git suffixes', () => {
    expect(
      repositoryUrlsEqual('https://github.com/orca/test', 'https://github.com/orca/test.git/'),
    ).toBe(true);
    expect(
      repositoryUrlsEqual('https://github.com/orca/test', 'https://github.com/orca/other'),
    ).toBe(false);
    expect(repositoryUrlsEqual('https://github.com/orca', 'https://github.com/orca')).toBe(false);
  });

  it('rejects host-wide or organization-wide allowlists', () => {
    expect(urlMatches('https://github.com', 'https://github.com/orca/test')).toBe(false);
    expect(urlMatches('https://github.com/orca', 'https://github.com/orca/test')).toBe(false);
    expect(validateGitRepositoryUrl('https://github.com')).toMatch(/owner and repository/);
    expect(validateGitRepositoryUrl('https://github.com/orca')).toMatch(/owner and repository/);
    expect(validateGitRepositoryUrl('https://github.com/orca/test')).toBeNull();
    expect(validateGitRepositoryUrl('http://github.com/orca/test')).toMatch(/must use https/);
    expect(validateGitRepositoryUrl('file:///orca/test')).toMatch(/must use https/);
  });
});
