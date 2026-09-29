// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash, timingSafeEqual } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { AuthenticationV1Api, KubeConfig, type V1TokenReview } from '@kubernetes/client-node';
import type { FastifyReply, FastifyRequest } from 'fastify';
import type { InternalServicePrincipal } from './principal.js';

export type InternalServiceCaller = InternalServicePrincipal['caller'];

export interface InternalAuthVerifier {
  verify(token: string): Promise<InternalServicePrincipal | null>;
}

export type InternalTokenSource = () => Promise<string>;

export interface KubernetesTokenReviewResult {
  authenticated: boolean;
  username?: string;
  audiences?: string[];
}

export type KubernetesTokenReviewer = (
  token: string,
  audience: string,
) => Promise<KubernetesTokenReviewResult>;

const TOKEN_REVIEW_CACHE_TTL_MS = 30_000;
const TOKEN_REVIEW_CACHE_MAX_ENTRIES = 32;

export function staticTokenSource(token: string): InternalTokenSource {
  const value = token.trim();
  if (!/^\S{32,}$/.test(value)) {
    throw new Error('INTERNAL_SERVICE_TOKEN must contain at least 32 non-whitespace characters');
  }
  return async () => value;
}

export function fileTokenSource(path: string): InternalTokenSource {
  if (path.trim() === '') throw new Error('INTERNAL_SERVICE_TOKEN_FILE must not be empty');
  return async () => {
    const token = (await readFile(path, 'utf8')).trim();
    if (!/^\S{32,}$/.test(token)) {
      throw new Error(
        'internal service token file must contain at least 32 non-whitespace characters',
      );
    }
    return token;
  };
}

export class StaticInternalAuthVerifier implements InternalAuthVerifier {
  constructor(private readonly expectedToken: InternalTokenSource) {}

  async verify(token: string): Promise<InternalServicePrincipal | null> {
    const expected = await this.expectedToken();
    const suppliedHash = createHash('sha256').update(token).digest();
    const expectedHash = createHash('sha256').update(expected).digest();
    if (!timingSafeEqual(suppliedHash, expectedHash)) return null;
    return { caller: 'shared', subject: 'static-internal-service-token' };
  }
}

export class KubernetesServiceAccountAuthVerifier implements InternalAuthVerifier {
  private readonly positiveCache = new Map<
    string,
    { principal: InternalServicePrincipal; expiresAt: number }
  >();

  constructor(
    private readonly options: {
      audience: string;
      harnessSubject: string;
      aiGatewaySubject: string;
      observabilityExporterSubject: string;
    },
    private readonly review: KubernetesTokenReviewer,
  ) {
    assertDistinctKubernetesSubjects(options);
  }

  async verify(token: string): Promise<InternalServicePrincipal | null> {
    const cacheKey = createHash('sha256').update(token).digest('hex');
    const now = Date.now();
    const cached = this.positiveCache.get(cacheKey);
    if (cached && cached.expiresAt > now) return cached.principal;
    if (cached) this.positiveCache.delete(cacheKey);

    const result = await this.review(token, this.options.audience);
    if (
      !result.authenticated ||
      !result.username ||
      !result.audiences?.includes(this.options.audience)
    ) {
      return null;
    }
    let principal: InternalServicePrincipal | null = null;
    if (result.username === this.options.harnessSubject) {
      principal = { caller: 'harness', subject: result.username };
    } else if (result.username === this.options.aiGatewaySubject) {
      principal = { caller: 'ai-gateway', subject: result.username };
    } else if (result.username === this.options.observabilityExporterSubject) {
      principal = { caller: 'observability-exporter', subject: result.username };
    }
    if (!principal) return null;

    for (const [key, entry] of this.positiveCache) {
      if (entry.expiresAt <= now) this.positiveCache.delete(key);
    }
    if (this.positiveCache.size >= TOKEN_REVIEW_CACHE_MAX_ENTRIES) {
      const oldestKey = this.positiveCache.keys().next().value as string | undefined;
      if (oldestKey) this.positiveCache.delete(oldestKey);
    }
    this.positiveCache.set(cacheKey, {
      principal,
      expiresAt: now + TOKEN_REVIEW_CACHE_TTL_MS,
    });
    return principal;
  }
}

export function kubernetesTokenReviewerFromApi(
  api: Pick<AuthenticationV1Api, 'createTokenReview'>,
): KubernetesTokenReviewer {
  return async (token, audience) => {
    const review: V1TokenReview = {
      apiVersion: 'authentication.k8s.io/v1',
      kind: 'TokenReview',
      spec: { token, audiences: [audience] },
    };
    const result = await api.createTokenReview(review);
    return {
      authenticated: result.body.status?.authenticated === true,
      ...(result.body.status?.user?.username ? { username: result.body.status.user.username } : {}),
      ...(result.body.status?.audiences ? { audiences: result.body.status.audiences } : {}),
    };
  };
}

export function buildKubernetesServiceAccountAuthVerifier(options: {
  audience: string;
  harnessSubject: string;
  aiGatewaySubject: string;
  observabilityExporterSubject: string;
}): KubernetesServiceAccountAuthVerifier {
  const kubeConfig = new KubeConfig();
  kubeConfig.loadFromDefault();
  const api = kubeConfig.makeApiClient(AuthenticationV1Api);
  return new KubernetesServiceAccountAuthVerifier(options, kubernetesTokenReviewerFromApi(api));
}

export function buildConfiguredInternalAuthVerifier(options: {
  mode: 'static_token' | 'kubernetes_service_account';
  token?: string;
  tokenFile?: string;
  audience: string;
  harnessSubject?: string;
  aiGatewaySubject?: string;
  observabilityExporterSubject?: string;
}): InternalAuthVerifier {
  if (options.mode === 'static_token') {
    if ((options.token ? 1 : 0) + (options.tokenFile ? 1 : 0) !== 1) {
      throw new Error(
        'static internal auth requires exactly one of INTERNAL_SERVICE_TOKEN or INTERNAL_SERVICE_TOKEN_FILE',
      );
    }
    return new StaticInternalAuthVerifier(
      options.token ? staticTokenSource(options.token) : fileTokenSource(options.tokenFile!),
    );
  }
  if (
    !hasConfiguredSubject(options.harnessSubject) ||
    !hasConfiguredSubject(options.aiGatewaySubject) ||
    !hasConfiguredSubject(options.observabilityExporterSubject)
  ) {
    throw new Error(
      'Kubernetes internal auth requires INTERNAL_AUTH_HARNESS_SUBJECT, INTERNAL_AUTH_AI_GATEWAY_SUBJECT, and INTERNAL_AUTH_OBSERVABILITY_EXPORTER_SUBJECT',
    );
  }
  return buildKubernetesServiceAccountAuthVerifier({
    audience: options.audience,
    harnessSubject: options.harnessSubject,
    aiGatewaySubject: options.aiGatewaySubject,
    observabilityExporterSubject: options.observabilityExporterSubject,
  });
}

export function buildInternalAuth(verifier: InternalAuthVerifier) {
  return async function internalAuth(req: FastifyRequest, reply: FastifyReply): Promise<void> {
    const path = req.url.split('?')[0] ?? '';
    if (path === '/healthz' || path === '/readyz' || path === '/metrics') return;

    const token = bearerToken(req.headers.authorization);
    if (!token) {
      reply.code(401).send({ error: 'unauthenticated' });
      return reply;
    }

    let principal: InternalServicePrincipal | null;
    try {
      principal = await verifier.verify(token);
    } catch {
      reply.code(503).send({ error: 'internal authentication unavailable' });
      return reply;
    }
    if (!principal) {
      reply.code(401).send({ error: 'unauthenticated' });
      return reply;
    }

    const routeCallers = internalRouteCallers(path, req.method);
    if (
      principal.caller !== 'shared' &&
      (routeCallers === null || !routeCallers.includes(principal.caller))
    ) {
      reply.code(403).send({ error: 'forbidden' });
      return reply;
    }
    req.internalAuth = principal;
  };
}

function bearerToken(value: string | undefined): string | null {
  if (!value) return null;
  const match = /^Bearer\s+([^\s]+)$/i.exec(value.trim());
  return match?.[1] ?? null;
}

/**
 * Internal callers are workload principals, not broad mesh roles. Keep every
 * family explicit so a newly mounted route cannot silently inherit Harness
 * access and an unknown path cannot become an exporter or gateway capability.
 */
type WorkloadInternalServiceCaller = Exclude<InternalServiceCaller, 'shared'>;

interface InternalRouteCapabilityRule {
  callers: readonly WorkloadInternalServiceCaller[];
  matches(path: string, method: string): boolean;
}

const INTERNAL_ROUTE_CAPABILITY_RULES: readonly InternalRouteCapabilityRule[] = [
  {
    callers: ['ai-gateway'],
    matches: (path, method) => method === 'GET' && path === '/internal/v1/guardrails/effective',
  },
  {
    // Both workloads need these route capabilities, but usage still has exactly
    // one writer per Session: Harness for separate mode and AI Gateway for
    // colocated mode. The usage handler enforces that split for workload-aware
    // auth, and Gateway filters usage-owned fields from its guardrail flush.
    callers: ['harness', 'ai-gateway'],
    matches: (path, method) =>
      method === 'POST' &&
      /^\/internal\/v1\/workspaces\/[^/]+\/sessions\/[^/]+\/(?:usage|guardrail-state)$/.test(path),
  },
  {
    callers: ['ai-gateway'],
    matches: (path, method) =>
      method === 'POST' &&
      (/^\/internal\/v1\/workspaces\/[^/]+\/sessions\/[^/]+\/vault-credentials\/[^/]+\/resolve$/.test(
        path,
      ) ||
        /^\/internal\/v1\/workspaces\/[^/]+\/sessions\/[^/]+\/mcp-destination\/resolve$/.test(
          path,
        )),
  },
  {
    callers: ['observability-exporter'],
    matches: (path, method) =>
      method === 'POST' &&
      /^\/internal\/v1\/workspaces\/[^/]+\/sessions\/[^/]+\/agent-observability\/(?:context|secret)\/resolve$/.test(
        path,
      ),
  },
  {
    callers: ['harness'],
    matches: (path, method) =>
      (method === 'POST' &&
        /^\/internal\/v1\/workspaces\/[^/]+\/sessions\/[^/]+\/(?:executions:prepare|harness-state|harness-turn|guardrail-subject-window|mint-jwt|files)$/.test(
          path,
        )) ||
      (method === 'GET' &&
        /^\/internal\/v1\/workspaces\/[^/]+\/sessions\/[^/]+\/execution-owner$/.test(path)) ||
      (method === 'PATCH' &&
        /^\/internal\/v1\/workspaces\/[^/]+\/sessions\/[^/]+\/state$/.test(path)) ||
      (method === 'POST' &&
        /^\/internal\/v1\/workspaces\/[^/]+\/sessions\/[^/]+\/git-credentials\/[^/]+\/resolve$/.test(
          path,
        )) ||
      ((method === 'GET' || method === 'POST') &&
        /^\/internal\/v1\/workspaces\/[^/]+\/sessions\/[^/]+\/memory-stores\/[^/]+\/memory-versions$/.test(
          path,
        )) ||
      (method === 'GET' &&
        /^\/internal\/v1\/workspaces\/[^/]+\/sessions\/[^/]+\/memory-stores\/[^/]+\/memories(?:\/[^/]+\/content)?$/.test(
          path,
        )) ||
      (method === 'GET' && /^\/internal\/environments\/[^/]+$/.test(path)) ||
      (method === 'POST' && /^\/internal\/environments\/[^/]+\/verify-key$/.test(path)) ||
      (method === 'POST' && /^\/internal\/environments\/claims\/reap$/.test(path)) ||
      (method === 'PUT' && /^\/internal\/environments\/[^/]+\/claim$/.test(path)) ||
      (method === 'POST' &&
        /^\/internal\/environments\/[^/]+\/claim\/(?:heartbeat|release)$/.test(path)) ||
      (method === 'GET' && /^\/internal\/environments\/[^/]+\/claim$/.test(path)) ||
      (method === 'GET' && /^\/internal\/runners(?:\/[^/]+\/status)?$/.test(path)),
  },
];

/**
 * Return the allowed workload callers only when exactly one explicit capability rule
 * matches. An unknown route, wrong method, or accidentally overlapping future
 * rule fails closed as `null`.
 */
export function internalRouteCallers(
  path: string,
  method: string = 'POST',
): readonly WorkloadInternalServiceCaller[] | null {
  const matches = INTERNAL_ROUTE_CAPABILITY_RULES.filter((rule) => rule.matches(path, method));
  return matches.length === 1 ? matches[0]!.callers : null;
}

function hasConfiguredSubject(subject: string | undefined): subject is string {
  return typeof subject === 'string' && subject.trim().length > 0;
}

function assertDistinctKubernetesSubjects(options: {
  harnessSubject: string;
  aiGatewaySubject: string;
  observabilityExporterSubject: string;
}): void {
  const subjects = [
    options.harnessSubject,
    options.aiGatewaySubject,
    options.observabilityExporterSubject,
  ];
  if (subjects.some((subject) => !hasConfiguredSubject(subject))) {
    throw new Error(
      'Kubernetes internal auth requires non-empty Harness, AI Gateway, and observability exporter subjects',
    );
  }
  if (new Set(subjects).size !== subjects.length) {
    throw new Error(
      'Harness, AI Gateway, and observability exporter internal auth subjects must be different',
    );
  }
}
