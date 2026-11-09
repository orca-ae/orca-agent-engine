// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  egressTarget,
  egressResolveRequest,
  egressResolveResponse,
} from '../../src/contracts/sandbox-egress.contract.js';
import {
  liveEgressPod,
  kubernetesEgressLivenessVerifier,
  kubernetesEgressSandboxLocator,
} from '../../src/domain/sandbox-egress-authority.js';
import {
  gitEgressOperation,
  decodeEgressBasic,
  vaultEgressTargets,
} from '../../src/domain/sandbox-egress-credentials.js';

describe('sandbox egress trust boundaries', () => {
  it('requires the trusted request body policy discriminator', () => {
    const response = { replacements: [], credential_version: null };
    expect(egressResolveResponse.safeParse(response).success).toBe(false);
    expect(
      egressResolveResponse.safeParse({ ...response, request_body_policy: 'git_upload_pack' })
        .success,
    ).toBe(true);
    expect(
      egressResolveResponse.safeParse({ ...response, request_body_policy: 'stream' }).success,
    ).toBe(true);
    expect(
      egressResolveResponse.safeParse({ ...response, request_body_policy: 'client-selected' })
        .success,
    ).toBe(false);
  });
  it('locates the unique managed gVisor Pod by current BatchSandbox UID', async () => {
    const pod = {
      metadata: {
        name: 'sandbox-pod',
        uid: 'pod-uid',
        labels: { 'orca.ai/sandbox-egress': 'enabled' },
        ownerReferences: [
          {
            kind: 'BatchSandbox',
            apiVersion: 'sandbox.opensandbox.io/v1alpha1',
            name: 'sandbox-id',
            uid: 'batch-uid',
          },
        ],
      },
      spec: { runtimeClassName: 'gvisor', containers: [] },
      status: {
        phase: 'Running',
        conditions: [{ type: 'Ready', status: 'True' }],
        containerStatuses: [
          { name: 'sandbox', image: 'fixture', imageID: 'fixture', ready: true, restartCount: 0 },
        ],
      },
    };
    const core = { listNamespacedPod: vi.fn().mockResolvedValue({ body: { items: [pod] } }) };
    const custom = {
      getNamespacedCustomObject: vi
        .fn()
        .mockResolvedValue({ body: { metadata: { uid: 'batch-uid' } } }),
    };
    const locate = kubernetesEgressSandboxLocator('sandboxes', { core, custom });
    await expect(locate('sandbox-id')).resolves.toEqual({
      namespace: 'sandboxes',
      name: 'sandbox-pod',
      uid: 'pod-uid',
    });
    expect(custom.getNamespacedCustomObject).toHaveBeenCalledWith(
      'sandbox.opensandbox.io',
      'v1alpha1',
      'sandboxes',
      'batchsandboxes',
      'sandbox-id',
    );
    core.listNamespacedPod.mockResolvedValue({ body: { items: [pod, pod] } });
    await expect(locate('sandbox-id')).rejects.toThrow();
    core.listNamespacedPod.mockResolvedValue({ body: { items: [pod] } });
    custom.getNamespacedCustomObject.mockResolvedValue({
      body: { metadata: { uid: 'replacement-batch' } },
    });
    await expect(locate('sandbox-id')).rejects.toThrow();
    custom.getNamespacedCustomObject.mockResolvedValue({
      body: { metadata: { uid: 'batch-uid', deletionTimestamp: 'now' } },
    });
    await expect(locate('sandbox-id')).rejects.toThrow();
  });
  it.each([
    '127.0.0.1',
    '[::1]',
    'UPPER.com',
    'example.com.',
    'a..com',
    '-a.com',
    'example.com:443',
  ])('rejects non-normalized DNS target %s', (host) => {
    expect(egressTarget.safeParse({ host, port: 443 }).success).toBe(false);
  });
  it('decodes strict Basic without replacing non-UTF-8 bytes', () => {
    const bytes = Buffer.from([0xff, 0x3a, 0xfe]);
    expect(decodeEgressBasic('bAsIc ' + bytes.toString('base64'))).toBe(bytes.toString('latin1'));
    for (const value of ['Basic !!!', 'Basic dXNlcg==', 'Basic dTpw ignored', 'Basic dTpw='])
      expect(decodeEgressBasic(value)).toBeNull();
  });
  it('keeps duplicate auth fields for resolver rejection and rejects CRLF', () => {
    const request = {
      scope: 'session',
      binding_id: 'b',
      generation: 'g',
      target: { host: 'github.com', port: 443 },
      method: 'GET',
      path_and_query: '/',
      candidate_headers: [
        { name: 'authorization', value: 'one' },
        { name: 'authorization', value: 'two' },
      ],
    };
    expect(egressResolveRequest.parse(request).candidate_headers).toHaveLength(2);
    const { candidate_headers, ...missing } = request;
    expect(egressResolveRequest.safeParse(missing).success).toBe(false);
    expect(egressResolveRequest.safeParse({ ...missing, authentication_headers: [] }).success).toBe(
      false,
    );
    expect(
      egressResolveRequest.safeParse({
        ...request,
        candidate_headers: Array(33).fill(candidate_headers[0]),
      }).success,
    ).toBe(false);
    for (const value of ['x'.repeat(16385), 'nul\0byte', 'line\nfeed'])
      expect(
        egressResolveRequest.safeParse({
          ...request,
          candidate_headers: [{ name: 'x-key', value }],
        }).success,
      ).toBe(false);
    expect(
      egressResolveRequest.safeParse({
        ...request,
        candidate_headers: [{ name: 'X-Key', value: 'key' }],
      }).success,
    ).toBe(false);
    request.candidate_headers[0]!.value = 'one\r\ntwo';
    expect(egressResolveRequest.safeParse(request).success).toBe(false);
  });
  it.each([
    '/o/r.git/info/refs?service=git-upload-pack&service=git-upload-pack',
    '/o/r.git/git-receive-pack',
    '/o/r.git/%2e%2e/info/refs?service=git-upload-pack',
    '/o/r.git/../info/refs?service=git-upload-pack',
    '/o/r.git/%252e/info/refs?service=git-upload-pack',
    '/other/r.git/info/refs?service=git-upload-pack',
  ])('denies unsafe Git path %s', (path) => {
    expect(gitEgressOperation('https://github.com/o/r.git', 'github.com', 'GET', path, false)).toBe(
      false,
    );
  });
  it('authorizes exact Git/REST read routes', () => {
    const repo = 'https://github.com/o/r.git';
    expect(gitEgressOperation(repo, 'github.com', 'POST', '/o/r.git/git-upload-pack', false)).toBe(
      true,
    );
    expect(
      gitEgressOperation(
        repo,
        'api.github.com',
        'GET',
        '/repos/o/r/issues?page=2&per_page=10',
        true,
      ),
    ).toBe(true);
    expect(
      gitEgressOperation(repo, 'api.github.com', 'GET', '/repos/o/r/contents/README.md', true),
    ).toBe(true);
    for (const path of [
      '/user',
      '/graphql',
      '/repos/o/r/tarball',
      '/repos/o/r/issues/1/comments',
      '/repos/o/r-other',
    ])
      expect(gitEgressOperation(repo, 'api.github.com', 'GET', path, true)).toBe(false);
    expect(gitEgressOperation(repo, 'api.github.com', 'GET', '/repos/o/r', false)).toBe(false);
  });
  it('only prepares bounded exact-host header-only Vault bindings', () => {
    expect(vaultEgressTargets({ type: 'limited', allowed_hosts: ['api.example.com'] }, {})).toEqual(
      ['api.example.com'],
    );
    for (const net of [
      { type: 'unrestricted' },
      { type: 'limited', allowed_hosts: ['*.example.com'] },
      { type: 'limited', allowed_hosts: [] },
    ])
      expect(vaultEgressTargets(net, {})).toBeNull();
    expect(
      vaultEgressTargets(
        { type: 'limited', allowed_hosts: ['api.example.com'] },
        { injection_location: { header: true, body: true } },
      ),
    ).toBeNull();
  });
  it('requires live matching UID and rejects deletion, termination and readiness loss', async () => {
    const pod = {
      metadata: { uid: 'uid' },
      status: {
        phase: 'Running',
        conditions: [{ type: 'Ready', status: 'True' }],
        containerStatuses: [
          { name: 'owner', image: 'fixture', imageID: 'fixture', ready: true, restartCount: 0 },
        ],
      },
    };
    expect(liveEgressPod(pod, 'uid')).toBe(true);
    expect(liveEgressPod(pod, 'new-uid')).toBe(false);
    expect(
      liveEgressPod(
        { ...pod, metadata: { ...pod.metadata, deletionTimestamp: new Date() } },
        'uid',
      ),
    ).toBe(false);
    expect(liveEgressPod({ ...pod, status: { ...pod.status, phase: 'Failed' } }, 'uid')).toBe(
      false,
    );
    expect(liveEgressPod({ ...pod, status: { ...pod.status, conditions: [] } }, 'uid')).toBe(false);
    const verifier = kubernetesEgressLivenessVerifier({
      readNamespacedPod: async () => {
        throw new Error('unavailable');
      },
    });
    await expect(
      verifier({
        kind: 'harness',
        owner_pod: { namespace: 'n', name: 'p', uid: 'uid' },
        sandbox_pod: { namespace: 'n', name: 's', uid: 'uid' },
        sandbox_handle_id: 's',
      }),
    ).rejects.toThrow('unavailable');
  });
});
