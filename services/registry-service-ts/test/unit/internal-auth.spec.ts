// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import Fastify from 'fastify';
import { describe, expect, it, vi } from 'vitest';
import {
  KubernetesServiceAccountAuthVerifier,
  StaticInternalAuthVerifier,
  buildConfiguredInternalAuthVerifier,
  buildInternalAuth,
  fileTokenSource,
  kubernetesTokenReviewerFromApi,
  staticTokenSource,
  type InternalAuthVerifier,
} from '../../src/auth/internal-auth.js';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const STATIC_TOKEN = 'static-internal-service-token-at-least-32-chars';
const AUDIENCE = 'orca-registry-internal';
const HARNESS_SUBJECT = 'system:serviceaccount:orca:harness';
const GATEWAY_SUBJECT = 'system:serviceaccount:orca:ai-gateway';
const EXPORTER_SUBJECT = 'system:serviceaccount:orca:observability-exporter';

async function app(verifier: InternalAuthVerifier) {
  const server = Fastify({ logger: false });
  server.addHook('preHandler', buildInternalAuth(verifier));
  server.get('/healthz', async () => ({ ok: true }));
  server.get('/metrics', async () => 'metrics');
  server.patch('/internal/v1/workspaces/:workspaceId/sessions/:sessionId/state', async () => ({
    ok: true,
  }));
  server.post('/internal/v1/workspaces/:workspaceId/sessions/:sessionId/usage', async () => ({
    ok: true,
  }));
  server.post(
    '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/vault-credentials/:id/resolve',
    async () => ({ ok: true }),
  );
  server.post(
    '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/context/resolve',
    async () => ({ ok: true }),
  );
  server.post(
    '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/secret/resolve',
    async () => ({ ok: true }),
  );
  await server.ready();
  return server;
}

describe('internal static service token auth', () => {
  it('keeps probes open and rejects missing or wrong credentials', async () => {
    const server = await app(new StaticInternalAuthVerifier(staticTokenSource(STATIC_TOKEN)));
    try {
      expect((await server.inject({ method: 'GET', url: '/healthz' })).statusCode).toBe(200);
      expect((await server.inject({ method: 'GET', url: '/metrics' })).statusCode).toBe(200);
      expect(
        (
          await server.inject({
            method: 'PATCH',
            url: '/internal/v1/workspaces/ws_a/sessions/ses_a/state',
          })
        ).statusCode,
      ).toBe(401);
      expect(
        (
          await server.inject({
            method: 'PATCH',
            url: '/internal/v1/workspaces/ws_a/sessions/ses_a/state',
            headers: { authorization: 'Bearer wrong-token' },
          })
        ).statusCode,
      ).toBe(401);
    } finally {
      await server.close();
    }
  });

  it('allows the shared token on every known internal family', async () => {
    const server = await app(new StaticInternalAuthVerifier(staticTokenSource(STATIC_TOKEN)));
    const headers = { authorization: `Bearer ${STATIC_TOKEN}` };
    try {
      expect(
        (
          await server.inject({
            method: 'PATCH',
            url: '/internal/v1/workspaces/ws_a/sessions/ses_a/state',
            headers,
          })
        ).statusCode,
      ).toBe(200);
      expect(
        (
          await server.inject({
            method: 'POST',
            url: '/internal/v1/workspaces/ws_a/sessions/ses_a/agent-observability/secret/resolve',
            headers,
          })
        ).statusCode,
      ).toBe(200);
      expect(
        (
          await server.inject({
            method: 'POST',
            url: '/internal/v1/workspaces/ws_a/sessions/ses_a/agent-observability/context/resolve',
            headers,
          })
        ).statusCode,
      ).toBe(200);
      expect(
        (
          await server.inject({
            method: 'POST',
            url: '/internal/v1/workspaces/ws_a/sessions/ses_a/vault-credentials/vcrd_a/resolve',
            headers,
          })
        ).statusCode,
      ).toBe(200);
    } finally {
      await server.close();
    }
  });

  it('rereads token files and fails closed when the file disappears', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'orca-internal-auth-'));
    const path = join(dir, 'token');
    await writeFile(path, STATIC_TOKEN);
    const verifier = new StaticInternalAuthVerifier(fileTokenSource(path));
    await expect(verifier.verify(STATIC_TOKEN)).resolves.toMatchObject({ caller: 'shared' });
    await writeFile(path, 'rotated-internal-service-token-at-least-32-chars');
    await expect(verifier.verify(STATIC_TOKEN)).resolves.toBeNull();
    await rm(path);
    await expect(verifier.verify(STATIC_TOKEN)).rejects.toThrow();
    await rm(dir, { recursive: true, force: true });
  });
});

describe('internal Kubernetes ServiceAccount auth', () => {
  function verifierFor(usernameByToken: Record<string, string>) {
    return new KubernetesServiceAccountAuthVerifier(
      {
        audience: AUDIENCE,
        harnessSubject: HARNESS_SUBJECT,
        aiGatewaySubject: GATEWAY_SUBJECT,
        observabilityExporterSubject: EXPORTER_SUBJECT,
      },
      async (token) => ({
        authenticated: token in usernameByToken,
        ...(usernameByToken[token] ? { username: usernameByToken[token] } : {}),
        audiences: [AUDIENCE],
      }),
    );
  }

  it('authorizes each ServiceAccount only for its explicit route capabilities', async () => {
    const server = await app(
      verifierFor({
        harness: HARNESS_SUBJECT,
        gateway: GATEWAY_SUBJECT,
        exporter: EXPORTER_SUBJECT,
      }),
    );
    try {
      const harnessPath = '/internal/v1/workspaces/ws_a/sessions/ses_a/state';
      const gatewayPath =
        '/internal/v1/workspaces/ws_a/sessions/ses_a/vault-credentials/vcrd_a/resolve';
      const usagePath = '/internal/v1/workspaces/ws_a/sessions/ses_a/usage';
      const exporterPath =
        '/internal/v1/workspaces/ws_a/sessions/ses_a/agent-observability/context/resolve';
      const exporterSecretPath =
        '/internal/v1/workspaces/ws_a/sessions/ses_a/agent-observability/secret/resolve';
      expect(
        (
          await server.inject({
            method: 'PATCH',
            url: harnessPath,
            headers: { authorization: 'Bearer harness' },
          })
        ).statusCode,
      ).toBe(200);
      for (const token of ['harness', 'gateway']) {
        expect(
          (
            await server.inject({
              method: 'POST',
              url: usagePath,
              headers: { authorization: `Bearer ${token}` },
            })
          ).statusCode,
        ).toBe(200);
      }
      expect(
        (
          await server.inject({
            method: 'POST',
            url: usagePath,
            headers: { authorization: 'Bearer exporter' },
          })
        ).statusCode,
      ).toBe(403);
      expect(
        (
          await server.inject({
            method: 'POST',
            url: exporterSecretPath,
            headers: { authorization: 'Bearer exporter' },
          })
        ).statusCode,
      ).toBe(200);
      expect(
        (
          await server.inject({
            method: 'PATCH',
            url: harnessPath,
            headers: { authorization: 'Bearer gateway' },
          })
        ).statusCode,
      ).toBe(403);
      expect(
        (
          await server.inject({
            method: 'POST',
            url: exporterPath,
            headers: { authorization: 'Bearer exporter' },
          })
        ).statusCode,
      ).toBe(200);
      expect(
        (
          await server.inject({
            method: 'GET',
            url: exporterPath,
            headers: { authorization: 'Bearer exporter' },
          })
        ).statusCode,
      ).toBe(403);
      for (const token of ['harness', 'gateway']) {
        expect(
          (
            await server.inject({
              method: 'POST',
              url: exporterPath,
              headers: { authorization: `Bearer ${token}` },
            })
          ).statusCode,
        ).toBe(403);
        expect(
          (
            await server.inject({
              method: 'POST',
              url: exporterSecretPath,
              headers: { authorization: `Bearer ${token}` },
            })
          ).statusCode,
        ).toBe(403);
      }
      for (const token of ['gateway', 'exporter']) {
        expect(
          (
            await server.inject({
              method: 'PATCH',
              url: harnessPath,
              headers: { authorization: `Bearer ${token}` },
            })
          ).statusCode,
        ).toBe(403);
      }
      for (const token of ['harness', 'exporter']) {
        expect(
          (
            await server.inject({
              method: 'POST',
              url: gatewayPath,
              headers: { authorization: `Bearer ${token}` },
            })
          ).statusCode,
        ).toBe(403);
      }
      for (const token of ['gateway', 'exporter']) {
        expect(
          (
            await server.inject({
              method: 'POST',
              url: '/internal/v1/workspaces/ws_a/sessions/ses_a/not-a-real-route',
              headers: { authorization: `Bearer ${token}` },
            })
          ).statusCode,
        ).toBe(403);
      }
      expect(
        (
          await server.inject({
            method: 'POST',
            url: gatewayPath,
            headers: { authorization: 'Bearer gateway' },
          })
        ).statusCode,
      ).toBe(200);
      expect(
        (
          await server.inject({
            method: 'POST',
            url: gatewayPath,
            headers: { authorization: 'Bearer harness' },
          })
        ).statusCode,
      ).toBe(403);
    } finally {
      await server.close();
    }
  });

  it('rejects wrong audiences, unknown subjects, and TokenReview outages', async () => {
    const wrongAudience = new KubernetesServiceAccountAuthVerifier(
      {
        audience: AUDIENCE,
        harnessSubject: HARNESS_SUBJECT,
        aiGatewaySubject: GATEWAY_SUBJECT,
        observabilityExporterSubject: EXPORTER_SUBJECT,
      },
      async () => ({
        authenticated: true,
        username: HARNESS_SUBJECT,
        audiences: ['different-audience'],
      }),
    );
    await expect(wrongAudience.verify('token')).resolves.toBeNull();

    const unknown = verifierFor({ token: 'system:serviceaccount:orca:other' });
    await expect(unknown.verify('token')).resolves.toBeNull();

    const server = await app({ verify: async () => Promise.reject(new Error('API unavailable')) });
    try {
      const response = await server.inject({
        method: 'PATCH',
        url: '/internal/v1/workspaces/ws_a/sessions/ses_a/state',
        headers: { authorization: 'Bearer token' },
      });
      expect(response.statusCode).toBe(503);
      expect(response.json()).toEqual({ error: 'internal authentication unavailable' });
    } finally {
      await server.close();
    }
  });

  it('sends the expected audience to the Kubernetes TokenReview API', async () => {
    const createTokenReview = vi.fn(async (_review: unknown) => ({
      body: {
        status: {
          authenticated: true,
          user: { username: HARNESS_SUBJECT },
          audiences: [AUDIENCE],
        },
      },
    }));
    const reviewer = kubernetesTokenReviewerFromApi({ createTokenReview } as never);
    await expect(reviewer('projected-jwt', AUDIENCE)).resolves.toEqual({
      authenticated: true,
      username: HARNESS_SUBJECT,
      audiences: [AUDIENCE],
    });
    expect(createTokenReview).toHaveBeenCalledWith({
      apiVersion: 'authentication.k8s.io/v1',
      kind: 'TokenReview',
      spec: { token: 'projected-jwt', audiences: [AUDIENCE] },
    });
  });

  it('briefly caches positive TokenReview results without caching rejections', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-21T00:00:00.000Z'));
    try {
      const review = vi.fn(async (token: string) => ({
        authenticated: token === 'valid',
        ...(token === 'valid' ? { username: HARNESS_SUBJECT } : {}),
        audiences: [AUDIENCE],
      }));
      const verifier = new KubernetesServiceAccountAuthVerifier(
        {
          audience: AUDIENCE,
          harnessSubject: HARNESS_SUBJECT,
          aiGatewaySubject: GATEWAY_SUBJECT,
          observabilityExporterSubject: EXPORTER_SUBJECT,
        },
        review,
      );

      await expect(verifier.verify('valid')).resolves.toMatchObject({ caller: 'harness' });
      await expect(verifier.verify('valid')).resolves.toMatchObject({ caller: 'harness' });
      expect(review).toHaveBeenCalledTimes(1);

      await expect(verifier.verify('invalid')).resolves.toBeNull();
      await expect(verifier.verify('invalid')).resolves.toBeNull();
      expect(review).toHaveBeenCalledTimes(3);

      vi.advanceTimersByTime(30_001);
      await expect(verifier.verify('valid')).resolves.toMatchObject({ caller: 'harness' });
      expect(review).toHaveBeenCalledTimes(4);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('configured internal auth', () => {
  it('fails startup for missing or ambiguous static credentials', () => {
    expect(() =>
      buildConfiguredInternalAuthVerifier({ mode: 'static_token', audience: AUDIENCE }),
    ).toThrow(/exactly one/);
    expect(() =>
      buildConfiguredInternalAuthVerifier({
        mode: 'static_token',
        audience: AUDIENCE,
        token: STATIC_TOKEN,
        tokenFile: '/tmp/token',
      }),
    ).toThrow(/exactly one/);
  });

  it('fails startup when Kubernetes caller subjects are missing or equal', () => {
    expect(() =>
      buildConfiguredInternalAuthVerifier({
        mode: 'kubernetes_service_account',
        audience: AUDIENCE,
      }),
    ).toThrow(/requires INTERNAL_AUTH_HARNESS_SUBJECT/);
    expect(() =>
      buildConfiguredInternalAuthVerifier({
        mode: 'kubernetes_service_account',
        audience: AUDIENCE,
        harnessSubject: HARNESS_SUBJECT,
        aiGatewaySubject: HARNESS_SUBJECT,
        observabilityExporterSubject: EXPORTER_SUBJECT,
      }),
    ).toThrow(/must be different/);
    expect(() =>
      buildConfiguredInternalAuthVerifier({
        mode: 'kubernetes_service_account',
        audience: AUDIENCE,
        harnessSubject: HARNESS_SUBJECT,
        aiGatewaySubject: GATEWAY_SUBJECT,
      }),
    ).toThrow(/OBSERVABILITY_EXPORTER_SUBJECT/);
    expect(() =>
      buildConfiguredInternalAuthVerifier({
        mode: 'kubernetes_service_account',
        audience: AUDIENCE,
        harnessSubject: HARNESS_SUBJECT,
        aiGatewaySubject: GATEWAY_SUBJECT,
        observabilityExporterSubject: GATEWAY_SUBJECT,
      }),
    ).toThrow(/must be different/);
    expect(() =>
      buildConfiguredInternalAuthVerifier({
        mode: 'kubernetes_service_account',
        audience: AUDIENCE,
        harnessSubject: EXPORTER_SUBJECT,
        aiGatewaySubject: GATEWAY_SUBJECT,
        observabilityExporterSubject: EXPORTER_SUBJECT,
      }),
    ).toThrow(/must be different/);
  });
});
