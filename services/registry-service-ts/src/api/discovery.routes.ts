// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { HARNESS_CATALOG, HARNESS_CAPABILITIES, type HarnessType } from '@orca/harness-catalog';
import type { FastifyInstance } from 'fastify';
import type { z } from 'zod';
import type {
  ApiGroupList,
  ApiResourceList,
  ApiVersions,
} from '../contracts/discovery.contract.js';

/**
 * The core API versions this build serves.
 *
 * One entry, `v1`, canonical at `/v1` with `/api/v1` accepted as an alias. A
 * second entry appears here the day a second core version is served and not
 * before — this list is an assertion about routing, not a roadmap.
 */
const CORE_VERSIONS = ['v1'] as const;

interface GroupVersion {
  group: string;
  version: string;
  resources: z.infer<typeof ApiResourceList>['resources'];
}

/**
 * The extension groups this build serves.
 *
 * All ship in the engine and are present on every deployment — including
 * a self-hosted one. A distribution built on this engine appends its own groups
 * here alongside the routes that serve them; nothing else needs to change, which
 * is the property the group tree exists for.
 *
 * This table is the single source for `GET /apis` and for each group's
 * `GET /apis/<group>/<version>`, so the group list and the resource lists cannot
 * disagree about what is served.
 */
const GROUP_VERSIONS: readonly GroupVersion[] = [
  {
    group: 'runtime.runorca.ai',
    version: 'v1',
    resources: [{ name: 'harnesses', namespaced: false, kind: 'Harness' }],
  },
  {
    group: 'policy.runorca.ai',
    version: 'v1',
    resources: [
      { name: 'guardrails', namespaced: true, kind: 'Guardrail' },
      { name: 'guardrailtypes', namespaced: false, kind: 'GuardrailType' },
    ],
  },
  {
    group: 'pricing.runorca.ai',
    version: 'v1',
    resources: [{ name: 'modelprices', namespaced: false, kind: 'ModelPrice' }],
  },
];

function resourceList(entry: GroupVersion): z.infer<typeof ApiResourceList> {
  return {
    kind: 'APIResourceList',
    group_version: `${entry.group}/${entry.version}`,
    resources: entry.resources,
  };
}

function groupList(): z.infer<typeof ApiGroupList>['groups'] {
  return GROUP_VERSIONS.map((entry) => {
    const version = { group_version: `${entry.group}/${entry.version}`, version: entry.version };
    return { name: entry.group, versions: [version], preferred_version: version };
  });
}

export function registerDiscoveryRoutes(app: FastifyInstance): void {
  app.get('/apis/runtime.runorca.ai/v1/harnesses', async () => ({
    data: (Object.keys(HARNESS_CAPABILITIES) as HarnessType[])
      .filter((id) => HARNESS_CAPABILITIES[id].models !== null)
      .map((id) => {
        const capabilities = HARNESS_CAPABILITIES[id];
        const policy = capabilities.models!;
        return {
          id,
          provider: HARNESS_CATALOG[id].provider,
          modes: HARNESS_CATALOG[id].supportedModes,
          capabilities: {
            managed_skills: capabilities.managedFeatures?.skills ?? false,
            multiagent: capabilities.managedFeatures?.multiagent ?? false,
            native_resume: capabilities.nativeResume,
          },
          models: Object.entries(
            policy.modelsByProvider ?? { [policy.defaultProvider]: policy.models ?? {} },
          ).flatMap(([provider, models]) =>
            Object.entries(models).map(([id, efforts]) => ({ provider, id, efforts })),
          ),
        };
      }),
  }));
  app.get('/api', async (): Promise<z.infer<typeof ApiVersions>> => {
    return {
      kind: 'APIVersions',
      versions: [...CORE_VERSIONS],
      preferred_version: CORE_VERSIONS[0],
    };
  });

  app.get('/apis', async (): Promise<z.infer<typeof ApiGroupList>> => {
    return { kind: 'APIGroupList', groups: groupList() };
  });

  // Registered from the same table rather than as a `/apis/:group/:version`
  // wildcard: a wildcard would answer for any group name a client invented,
  // turning "this deployment does not serve that group" into a 200 with an
  // empty resource list. An unlisted group must 404.
  for (const entry of GROUP_VERSIONS) {
    const body = resourceList(entry);
    app.get(`/apis/${entry.group}/${entry.version}`, async () => body);
  }
}
