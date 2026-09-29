// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';

const c = initContract();

/**
 * Group/version discovery, modelled on Kubernetes.
 *
 * The problem these two routes solve is that nothing in a URL, an SDK method, or
 * a docs page tells a client which API versions and extension groups the
 * deployment it is pointed at serves. A client that can ask gets a
 * machine-readable answer; a client that cannot has to discover the boundary by
 * 404, and a 404 cannot separate "not served here" from "wrong base URL".
 *
 * The granularity is groups and versions, not operations: `/api` names the core
 * API versions, `/apis` names the extension groups. An operation-level answer
 * already exists in the published spec, where every Orca-only operation carries
 * the `orca-extension` tag — duplicating it on a route would be a second copy to
 * keep in step with the first.
 *
 * Both routes require a credential, like every other route on this listener.
 * They describe what this deployment serves, which is an answer about the
 * deployment rather than a liveness signal — the probes are the unauthenticated
 * surface, and they are unauthenticated because their callers cannot hold a key.
 *
 * Deliberately not Claude-shaped: Anthropic publishes no discovery surface, so
 * these are Orca extensions and are tagged as such in the generated spec. The
 * structure follows Kubernetes (`APIVersions` / `APIGroupList`) because that is
 * the shape tooling already understands; multi-word keys are `snake_case` to
 * match every other response this API serves.
 *
 * See `docs/managed-agents/api-groups-and-extensions.md`.
 */

/** One `<group>/<version>` pair a deployment serves. */
const ApiGroupVersion = z.object({
  group_version: z
    .string()
    .describe('`<group>/<version>`, the prefix under `/apis` that serves this group version.'),
  version: z.string().describe('The version alone, e.g. `v1`.'),
});

/**
 * The core group's versions.
 *
 * `versions` names the core API versions this deployment serves, and the path
 * that serves each one is `/<version>` — canonical — with `/api/<version>` as an
 * accepted alias. Both are stated so a client does not have to infer either.
 */
export const ApiVersions = z.object({
  kind: z.literal('APIVersions'),
  versions: z.array(z.string()),
  preferred_version: z.string(),
});

/**
 * The extension groups a deployment serves.
 *
 * This engine ships three — `runtime.runorca.ai`, `policy.runorca.ai` and
 * `pricing.runorca.ai` — so they are present on every deployment, and a
 * distribution built on the engine appends its own. The list is the point of
 * the route: it is the difference between "this deployment does not serve that
 * group" and "this client cannot tell", and it is what a first-party client
 * needs in order to degrade honestly instead of 404ing its way to a conclusion.
 *
 * The group list is the only supported way to test whether a group is
 * available. Inferring it from a version string or from the deployment kind is
 * how a client ends up wrong on a deployment nobody anticipated.
 */
export const ApiGroupList = z.object({
  kind: z.literal('APIGroupList'),
  groups: z.array(
    z.object({
      name: z.string(),
      versions: z.array(ApiGroupVersion),
      preferred_version: ApiGroupVersion,
    }),
  ),
});

/**
 * One resource within a group version.
 *
 * `namespaced` says whether instances belong to a tenant or to the deployment.
 * Guardrails are a workspace's own, so they are namespaced; the guardrail type
 * catalog and the model price table are identical for every caller, so they are
 * not. The workspace itself never appears in the path — it is derived from the
 * credential, exactly as on the core API — so `namespaced` is a statement about
 * ownership rather than about URL shape.
 */
const ApiResource = z.object({
  name: z.string().describe('Lowercase plural path segment, e.g. `guardrails`.'),
  namespaced: z.boolean().describe('Whether instances belong to a workspace.'),
  kind: z.string().describe('Singular PascalCase type name, e.g. `Guardrail`.'),
});

/**
 * The resources one group version serves.
 *
 * Served at `/apis/<group>/<version>`, so a client that has just discovered a
 * group from `GET /apis` can enumerate it without consulting documentation.
 */
export const ApiResourceList = z.object({
  kind: z.literal('APIResourceList'),
  group_version: z.string(),
  resources: z.array(ApiResource),
});

export const HarnessList = z.object({
  data: z.array(
    z.object({
      id: z.string(),
      provider: z.string(),
      modes: z.array(z.string()),
      capabilities: z.object({
        managed_skills: z.boolean(),
        multiagent: z.boolean(),
        native_resume: z.boolean(),
      }),
      models: z.array(
        z.object({ provider: z.string(), id: z.string(), efforts: z.array(z.string()) }),
      ),
    }),
  ),
});

export const discoveryContract = c.router({
  runtimeGroupResources: {
    method: 'GET',
    path: '/apis/runtime.runorca.ai/v1',
    summary: 'Resources served by the runtime.runorca.ai/v1 group.',
    responses: { 200: ApiResourceList },
  },
  harnesses: {
    method: 'GET',
    path: '/apis/runtime.runorca.ai/v1/harnesses',
    summary: 'Managed SDK harnesses and their supported model catalog.',
    responses: { 200: HarnessList },
  },
  coreVersions: {
    method: 'GET',
    path: '/api',
    summary: 'Core API versions served by this deployment.',
    responses: { 200: ApiVersions },
  },
  groups: {
    method: 'GET',
    path: '/apis',
    summary: 'Extension API groups served by this deployment.',
    responses: { 200: ApiGroupList },
  },
  policyGroupResources: {
    method: 'GET',
    path: '/apis/policy.runorca.ai/v1',
    summary: 'Resources served by the policy.runorca.ai/v1 group.',
    responses: { 200: ApiResourceList },
  },
  pricingGroupResources: {
    method: 'GET',
    path: '/apis/pricing.runorca.ai/v1',
    summary: 'Resources served by the pricing.runorca.ai/v1 group.',
    responses: { 200: ApiResourceList },
  },
});
