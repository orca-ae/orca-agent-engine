// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import Fastify from 'fastify';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  buildInternalAuth,
  buildKubernetesServiceAccountAuthVerifier,
} from '../../src/auth/internal-auth.js';

const audience = process.env['INTERNAL_AUTH_E2E_AUDIENCE'];
const harnessSubject = process.env['INTERNAL_AUTH_E2E_HARNESS_SUBJECT'];
const aiGatewaySubject = process.env['INTERNAL_AUTH_E2E_AI_GATEWAY_SUBJECT'];
const observabilityExporterSubject =
  process.env['INTERNAL_AUTH_E2E_OBSERVABILITY_EXPORTER_SUBJECT'];
const harnessToken = process.env['INTERNAL_AUTH_E2E_HARNESS_TOKEN'];
const aiGatewayToken = process.env['INTERNAL_AUTH_E2E_AI_GATEWAY_TOKEN'];
const observabilityExporterToken = process.env['INTERNAL_AUTH_E2E_OBSERVABILITY_EXPORTER_TOKEN'];
const wrongAudienceToken = process.env['INTERNAL_AUTH_E2E_WRONG_AUDIENCE_TOKEN'];
const unknownSubjectToken = process.env['INTERNAL_AUTH_E2E_UNKNOWN_SUBJECT_TOKEN'];
const hasFixture = Boolean(
  audience &&
  harnessSubject &&
  aiGatewaySubject &&
  observabilityExporterSubject &&
  harnessToken &&
  aiGatewayToken &&
  observabilityExporterToken &&
  wrongAudienceToken &&
  unknownSubjectToken,
);

describe.skipIf(!hasFixture)('Kubernetes ServiceAccount internal auth e2e', () => {
  const server = Fastify({ logger: false });
  const harnessPath = '/internal/v1/workspaces/ws_a/sessions/ses_a/state';
  const gatewayPath =
    '/internal/v1/workspaces/ws_a/sessions/ses_a/vault-credentials/vcrd_a/resolve';
  const observabilityContextPath =
    '/internal/v1/workspaces/ws_a/sessions/ses_a/agent-observability/context/resolve';
  const observabilitySecretPath =
    '/internal/v1/workspaces/ws_a/sessions/ses_a/agent-observability/secret/resolve';

  beforeAll(async () => {
    server.addHook(
      'preHandler',
      buildInternalAuth(
        buildKubernetesServiceAccountAuthVerifier({
          audience: audience!,
          harnessSubject: harnessSubject!,
          aiGatewaySubject: aiGatewaySubject!,
          observabilityExporterSubject: observabilityExporterSubject!,
        }),
      ),
    );
    server.get('/healthz', async () => ({ ok: true }));
    server.patch(harnessPath, async () => ({ ok: true }));
    server.post(gatewayPath, async () => ({ ok: true }));
    server.post(observabilityContextPath, async () => ({ ok: true }));
    server.post(observabilitySecretPath, async () => ({ ok: true }));
    await server.ready();
  });

  afterAll(async () => server.close());

  async function inject(method: 'PATCH' | 'POST', url: string, token: string) {
    return server.inject({ method, url, headers: { authorization: `Bearer ${token}` } });
  }

  it('uses the real Kubernetes TokenReview API and preserves open probes', async () => {
    expect((await server.inject({ method: 'GET', url: '/healthz' })).statusCode).toBe(200);
    expect((await inject('PATCH', harnessPath, harnessToken!)).statusCode).toBe(200);
    expect((await inject('POST', gatewayPath, aiGatewayToken!)).statusCode).toBe(200);
  });

  it('authorizes the observability exporter on both resolver routes', async () => {
    expect(
      (await inject('POST', observabilityContextPath, observabilityExporterToken!)).statusCode,
    ).toBe(200);
    expect(
      (await inject('POST', observabilitySecretPath, observabilityExporterToken!)).statusCode,
    ).toBe(200);
  });

  it('restricts each ServiceAccount to its route family', async () => {
    expect((await inject('PATCH', harnessPath, aiGatewayToken!)).statusCode).toBe(403);
    expect((await inject('POST', gatewayPath, harnessToken!)).statusCode).toBe(403);
    expect((await inject('POST', observabilityContextPath, harnessToken!)).statusCode).toBe(403);
    expect((await inject('POST', observabilitySecretPath, harnessToken!)).statusCode).toBe(403);
    expect((await inject('POST', observabilityContextPath, aiGatewayToken!)).statusCode).toBe(403);
    expect((await inject('POST', observabilitySecretPath, aiGatewayToken!)).statusCode).toBe(403);
    expect((await inject('PATCH', harnessPath, observabilityExporterToken!)).statusCode).toBe(403);
    expect((await inject('POST', gatewayPath, observabilityExporterToken!)).statusCode).toBe(403);
  });

  it('rejects the wrong audience and unknown ServiceAccount subject', async () => {
    expect((await inject('PATCH', harnessPath, wrongAudienceToken!)).statusCode).toBe(401);
    expect((await inject('PATCH', harnessPath, unknownSubjectToken!)).statusCode).toBe(401);
  });
});
