// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyRequest } from 'fastify';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const verify = vi.hoisted(() => vi.fn());

vi.mock('jose', () => ({
  createRemoteJWKSet: vi.fn(() => ({ mocked: true })),
  jwtVerify: verify,
}));

import {
  buildAdminOidcAuth,
  buildOidcAuth,
  buildPlatformOidcAuth,
  type OrganizationAudienceLookups,
} from '../../src/auth/oidc.js';

describe('OIDC authentication attribution', () => {
  beforeEach(() => {
    verify.mockReset();
  });

  it('uses the verified subject as the stable user actor id', async () => {
    verify.mockResolvedValue({
      payload: {
        workspace_id: 'ws_oidc_actor',
        sub: 'user_oidc_actor',
        scope: 'memory:write',
      },
    });
    const authenticate = buildOidcAuth({
      allowedIssuers: ['https://issuer.example'],
      audience: 'orca-managed-agents',
    });

    const principal = await authenticate({
      headers: { authorization: 'Bearer signed-token' },
    } as FastifyRequest);

    expect(principal).toEqual({
      workspaceId: 'ws_oidc_actor',
      principal: 'user_oidc_actor',
      scopes: ['memory:write'],
      authMethod: 'oidc',
      userId: 'user_oidc_actor',
      oidcIssuer: 'https://issuer.example',
    });
  });

  it('keeps a principal-only token authenticated without inventing a user actor id', async () => {
    verify.mockResolvedValue({
      payload: {
        workspace_id: 'ws_oidc_principal',
        principal: 'legacy_oidc_principal',
        scope: 'memory:write',
      },
    });
    const authenticate = buildOidcAuth({
      allowedIssuers: ['https://issuer.example'],
      audience: 'orca-managed-agents',
    });

    const principal = await authenticate({
      headers: { authorization: 'Bearer signed-token' },
    } as FastifyRequest);

    expect(principal).toEqual({
      workspaceId: 'ws_oidc_principal',
      principal: 'legacy_oidc_principal',
      scopes: ['memory:write'],
      authMethod: 'oidc',
    });
    expect(principal).not.toHaveProperty('userId');
    expect(principal).not.toHaveProperty('oidcIssuer');
  });

  it('records issuer that successfully verified a subject-bearing workspace token', async () => {
    verify.mockRejectedValueOnce(new Error('wrong issuer')).mockResolvedValueOnce({
      payload: {
        workspace_id: 'ws_oidc_actor',
        sub: 'user_oidc_actor',
      },
    });
    const authenticate = buildOidcAuth({
      allowedIssuers: ['https://issuer-one.example', 'https://issuer-two.example'],
      audience: 'orca-managed-agents',
    });

    const principal = await authenticate({
      headers: { authorization: 'Bearer signed-token' },
    } as FastifyRequest);

    expect(principal).toMatchObject({
      principal: 'user_oidc_actor',
      userId: 'user_oidc_actor',
      oidcIssuer: 'https://issuer-two.example',
    });
  });

  it('builds an organization-free platform principal only with platform:admin', async () => {
    verify.mockResolvedValue({
      payload: {
        sub: 'deployment-operator',
        scope: 'platform:admin',
      },
    });
    const authenticate = buildPlatformOidcAuth({
      allowedIssuers: ['https://platform-issuer.example'],
      audience: 'orca-managed-agents-platform',
    });

    const principal = await authenticate({
      headers: { authorization: 'Bearer signed-token' },
    } as FastifyRequest);

    expect(principal).toEqual({
      principal: 'platform-oidc:https%3A%2F%2Fplatform-issuer.example:deployment-operator',
      scopes: ['platform:admin'],
      authMethod: 'oidc',
    });
    expect(principal).not.toHaveProperty('organizationId');
  });

  it('namespaces a platform principal with the issuer that verified the token', async () => {
    verify.mockRejectedValueOnce(new Error('wrong issuer')).mockResolvedValueOnce({
      payload: {
        sub: 'shared-subject',
        scope: 'platform:admin',
      },
    });
    const authenticate = buildPlatformOidcAuth({
      allowedIssuers: ['https://issuer-one.example', 'https://issuer-two.example'],
      audience: 'orca-managed-agents-platform',
    });

    const principal = await authenticate({
      headers: { authorization: 'Bearer signed-token' },
    } as FastifyRequest);

    expect(principal?.principal).toBe(
      'platform-oidc:https%3A%2F%2Fissuer-two.example:shared-subject',
    );
  });

  it('rejects a platform OIDC token without platform:admin', async () => {
    verify.mockResolvedValue({
      payload: {
        sub: 'organization-operator',
        scope: 'org:admin',
      },
    });
    const authenticate = buildPlatformOidcAuth({
      allowedIssuers: ['https://platform-issuer.example'],
      audience: 'orca-managed-agents-platform',
    });

    await expect(
      authenticate({
        headers: { authorization: 'Bearer signed-token' },
      } as FastifyRequest),
    ).resolves.toBeNull();
  });
});

/**
 * A refused resolution is reported to the operator through `req.log`, so the
 * request fixture carries one. Reset in `beforeEach` alongside `verify`.
 */
const warn = vi.fn();

const BEARER = {
  headers: { authorization: 'Bearer signed-token' },
  log: { warn },
} as unknown as FastifyRequest;

const ALPHA_AUDIENCE = 'https://alpha.example/orca';
const BETA_AUDIENCE = 'https://beta.example/orca';

interface FakeOrganization {
  id: string;
  audience: string | null;
  status: 'active' | 'archived';
}

interface FakeWorkspace {
  id: string;
  organizationId: string;
  status: 'active' | 'archived';
}

/**
 * One deployment's worth of organizations, covering every shape the resolution
 * rule has to answer for: an ordinary organization, a second one used to attempt
 * a crossing, one with no audience configured, one with two active workspaces,
 * one with none, and one that is archived.
 */
const ORGANIZATIONS: readonly FakeOrganization[] = [
  { id: 'org_alpha', audience: ALPHA_AUDIENCE, status: 'active' },
  { id: 'org_beta', audience: BETA_AUDIENCE, status: 'active' },
  { id: 'org_silent', audience: null, status: 'active' },
  { id: 'org_many', audience: 'https://many.example/orca', status: 'active' },
  { id: 'org_empty', audience: 'https://empty.example/orca', status: 'active' },
  { id: 'org_archived', audience: 'https://archived.example/orca', status: 'archived' },
];

const WORKSPACES: readonly FakeWorkspace[] = [
  { id: 'ws_alpha', organizationId: 'org_alpha', status: 'active' },
  // An archived sibling: it must not make org_alpha ambiguous, which is what
  // makes the happy path below a real test of "exactly one ACTIVE workspace".
  { id: 'ws_alpha_retired', organizationId: 'org_alpha', status: 'archived' },
  { id: 'ws_beta', organizationId: 'org_beta', status: 'active' },
  { id: 'ws_silent', organizationId: 'org_silent', status: 'active' },
  { id: 'ws_many_first', organizationId: 'org_many', status: 'active' },
  { id: 'ws_many_second', organizationId: 'org_many', status: 'active' },
  { id: 'ws_archived_org', organizationId: 'org_archived', status: 'active' },
];

/**
 * Stands in for the three database reads `auth.ts` binds to the real tables.
 *
 * It mirrors what that SQL does rather than what would be convenient here: every
 * method sees ACTIVE rows only, so an archived organization or workspace is
 * indistinguishable from one that was never created — which is the property the
 * rejection cases below are actually pinning.
 */
function fakeLookups(): OrganizationAudienceLookups {
  const activeOrganization = (id: string) =>
    ORGANIZATIONS.find((org) => org.id === id && org.status === 'active');
  return {
    organizationsForAudiences: vi.fn(async (audiences: readonly string[]) =>
      ORGANIZATIONS.filter(
        (org) =>
          org.status === 'active' && org.audience !== null && audiences.includes(org.audience),
      ).map(({ id, audience }) => ({ id, audience })),
    ),
    organizationForWorkspace: vi.fn(async (workspaceId: string) => {
      const workspace = WORKSPACES.find((ws) => ws.id === workspaceId && ws.status === 'active');
      if (!workspace) return null;
      const organization = activeOrganization(workspace.organizationId);
      return organization ? { id: organization.id, audience: organization.audience } : null;
    }),
    activeWorkspaceIds: vi.fn(async (organizationId: string) =>
      WORKSPACES.filter((ws) => ws.organizationId === organizationId && ws.status === 'active').map(
        (ws) => ws.id,
      ),
    ),
  };
}

/**
 * The workspace plane in resolution mode. `audience` is EMPTY on purpose and
 * has to be: with the flag on, verification never consults it, and a plane
 * carrying both refuses to build — see the configuration suite at the end of
 * this file.
 */
const RESOLVING = {
  allowedIssuers: ['https://issuer.example'],
  audience: '',
  resolveWorkspaceByAudience: true,
};

/**
 * Organization-scoped credentials: the issuer sets `aud` to the organization's
 * audience and knows nothing about workspace ids, so the workspace is resolved
 * here and every token is bound to the organization that owns it.
 *
 * The cases below are written as "what does a token get" rather than "which
 * branch ran": the whole point of the binding check is that there is one
 * answer, reached the same way regardless of how the workspace was found.
 */
describe('OIDC workspace resolution by organization audience', () => {
  beforeEach(() => {
    verify.mockReset();
    warn.mockReset();
  });

  const authenticate = (payload: Record<string, unknown>, lookups = fakeLookups()) => {
    verify.mockResolvedValue({ payload });
    return buildOidcAuth(RESOLVING, lookups)(BEARER);
  };

  it("resolves the sole active workspace of the organization named by the token's audience", async () => {
    await expect(
      authenticate({ sub: 'agent@alpha.example', aud: ALPHA_AUDIENCE, scope: 'memory:write' }),
    ).resolves.toEqual({
      workspaceId: 'ws_alpha',
      principal: 'agent@alpha.example',
      scopes: ['memory:write'],
      authMethod: 'oidc',
      userId: 'agent@alpha.example',
      oidcIssuer: 'https://issuer.example',
    });
  });

  it('authenticates a token carrying no scopes at all, which this plane does not require', async () => {
    await expect(
      authenticate({ sub: 'agent@alpha.example', aud: [ALPHA_AUDIENCE] }),
    ).resolves.toMatchObject({ workspaceId: 'ws_alpha', scopes: [] });
  });

  it('rejects a token whose audience no organization claims', async () => {
    await expect(
      authenticate({ sub: 'agent@nowhere.example', aud: 'https://nobody.example/orca' }),
    ).resolves.toBeNull();
  });

  it('rejects a token carrying no audience at all', async () => {
    await expect(authenticate({ sub: 'agent@alpha.example' })).resolves.toBeNull();
  });

  it('rejects a token whose audiences name more than one organization', async () => {
    // The unique index keeps two organizations from sharing one audience, but a
    // token may still carry several `aud` values. "The organization" then has no
    // referent, and guessing between them is exactly the crossing this mode
    // exists to prevent.
    await expect(
      authenticate({ sub: 'agent@both.example', aud: [ALPHA_AUDIENCE, BETA_AUDIENCE] }),
    ).resolves.toBeNull();
  });

  it('accepts several audiences when a claim says which workspace they are for', async () => {
    // The counterpart to the case above, and the reason ambiguity is fatal only
    // during resolution: several `aud` values are not suspect in themselves.
    // With the workspace named outright there is nothing to guess between, and
    // the binding rule still has to find alpha's audience among them — which
    // resolving by audience alone could not have done.
    await expect(
      authenticate({
        sub: 'agent@both.example',
        aud: [ALPHA_AUDIENCE, BETA_AUDIENCE],
        workspace_id: 'ws_alpha',
      }),
    ).resolves.toMatchObject({ workspaceId: 'ws_alpha' });
  });

  it('rejects a token whose organization has more than one active workspace', async () => {
    await expect(
      authenticate({ sub: 'agent@many.example', aud: 'https://many.example/orca' }),
    ).resolves.toBeNull();
  });

  it('rejects a token whose organization has no active workspace', async () => {
    await expect(
      authenticate({ sub: 'agent@empty.example', aud: 'https://empty.example/orca' }),
    ).resolves.toBeNull();
  });

  it('rejects a token naming an archived organization', async () => {
    await expect(
      authenticate({ sub: 'agent@gone.example', aud: 'https://archived.example/orca' }),
    ).resolves.toBeNull();
  });

  it('accepts a workspace claim when the token also carries its organization audience', async () => {
    await expect(
      authenticate({
        sub: 'agent@alpha.example',
        aud: ALPHA_AUDIENCE,
        workspace_id: 'ws_alpha',
        scope: 'memory:write',
      }),
    ).resolves.toMatchObject({ workspaceId: 'ws_alpha', principal: 'agent@alpha.example' });
  });

  it("rejects a workspace claim naming another organization's workspace", async () => {
    // The binding check, and the reason it is unconditional: alpha's issuer can
    // put any workspace id it likes in a claim, but it cannot put beta's
    // audience in `aud`, so beta's workspace stays out of reach.
    const lookups = fakeLookups();

    await expect(
      authenticate(
        { sub: 'agent@alpha.example', aud: ALPHA_AUDIENCE, workspace_id: 'ws_beta' },
        lookups,
      ),
    ).resolves.toBeNull();

    // Pins what did the rejecting. `ws_beta` resolves perfectly well — it is an
    // active workspace under an active organization — so the null above can only
    // have come from the binding check, not from a row that was missing anyway.
    await expect(lookups.organizationForWorkspace('ws_beta')).resolves.toEqual({
      id: 'org_beta',
      audience: BETA_AUDIENCE,
    });
  });

  it('rejects a workspace claim whose organization has no audience configured', async () => {
    // An organization that opted into nothing authenticates nothing here, even
    // through a claim that names its workspace outright.
    const lookups = fakeLookups();

    await expect(
      authenticate(
        { sub: 'agent@silent.example', aud: ALPHA_AUDIENCE, workspace_id: 'ws_silent' },
        lookups,
      ),
    ).resolves.toBeNull();

    // Again the workspace itself is reachable; it is the absent audience on its
    // organization that ends the request.
    await expect(lookups.organizationForWorkspace('ws_silent')).resolves.toEqual({
      id: 'org_silent',
      audience: null,
    });
  });

  it('rejects a workspace claim naming an archived or unknown workspace', async () => {
    const [archived, unknown] = await Promise.all([
      authenticate({
        sub: 'agent@alpha.example',
        aud: ALPHA_AUDIENCE,
        workspace_id: 'ws_alpha_retired',
      }),
      authenticate({
        sub: 'agent@alpha.example',
        aud: ALPHA_AUDIENCE,
        workspace_id: 'ws_never_created',
      }),
    ]);

    expect(archived).toBeNull();
    expect(unknown).toBeNull();
  });

  it('still ignores a metadata workspace claim on a plane that has not opted in', async () => {
    // Precedence is unchanged by this mode: with `metadataClaims` off, the
    // nested claim is not read, so the token resolves through its audience
    // instead of through the workspace someone nested in `metadata`.
    await expect(
      authenticate({
        sub: 'agent@alpha.example',
        aud: ALPHA_AUDIENCE,
        metadata: { orca_workspace: 'ws_beta' },
      }),
    ).resolves.toMatchObject({ workspaceId: 'ws_alpha' });
  });

  it('leaves audience validation to the binding check instead of verifying a static one', async () => {
    await authenticate({ sub: 'agent@alpha.example', aud: ALPHA_AUDIENCE });

    expect(verify).toHaveBeenCalledWith(expect.anything(), expect.anything(), {
      issuer: 'https://issuer.example',
    });
  });

  it('verifies the configured static audience and consults no organization when off', async () => {
    const lookups = fakeLookups();
    verify.mockResolvedValue({
      payload: { workspace_id: 'ws_beta', sub: 'agent@alpha.example', aud: ALPHA_AUDIENCE },
    });

    const principal = await buildOidcAuth(
      { allowedIssuers: ['https://issuer.example'], audience: 'orca-managed-agents' },
      lookups,
    )(BEARER);

    // Byte-identical to the behaviour before this mode existed: the claim is
    // taken as given, jose enforces the one static audience, and no
    // organization is read at all.
    expect(principal).toMatchObject({ workspaceId: 'ws_beta' });
    expect(verify).toHaveBeenCalledWith(expect.anything(), expect.anything(), {
      issuer: 'https://issuer.example',
      audience: 'orca-managed-agents',
    });
    expect(lookups.organizationsForAudiences).not.toHaveBeenCalled();
    expect(lookups.organizationForWorkspace).not.toHaveBeenCalled();
  });

  it('refuses to build the workspace plane without the lookups the mode needs', () => {
    // Verification stops checking `audience` the moment the flag is on, so a
    // plane that cannot run the binding check must not start at all.
    expect(() => buildOidcAuth(RESOLVING)).toThrowError(
      /OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE.*without the organization lookups/s,
    );
  });

  it('refuses the option on the admin and platform planes, which resolve no workspace', () => {
    expect(() =>
      buildAdminOidcAuth({
        allowedIssuers: ['https://admin-issuer.example'],
        audience: 'orca-managed-agents-admin',
        resolveWorkspaceByAudience: true,
      }),
    ).toThrowError(/admin plane, which resolves no workspaces/);
    expect(() =>
      buildPlatformOidcAuth({
        allowedIssuers: ['https://platform-issuer.example'],
        audience: 'orca-managed-agents-platform',
        resolveWorkspaceByAudience: true,
      }),
    ).toThrowError(/platform plane, which resolves no workspaces/);
  });
});

/**
 * Every refusal in resolution mode is the same bare 401, so the reason is the
 * only thing that tells an operator which of several different faults they are
 * looking at. These cases pin one reason per rejecting branch, and pin that the
 * report carries deployment identifiers and nothing else — no token, and in
 * particular no `aud` value.
 */
describe('OIDC organization-audience rejection reporting', () => {
  beforeEach(() => {
    verify.mockReset();
    warn.mockReset();
  });

  const reject = async (payload: Record<string, unknown>) => {
    verify.mockResolvedValue({ payload });
    await expect(buildOidcAuth(RESOLVING, fakeLookups())(BEARER)).resolves.toBeNull();
    expect(warn).toHaveBeenCalledTimes(1);
    return warn.mock.calls[0]![0] as Record<string, unknown>;
  };

  const SUBJECT = { sub: 'agent@alpha.example' };

  const CASES: ReadonlyArray<readonly [string, Record<string, unknown>, Record<string, unknown>]> =
    [
      ['no readable audience at all', SUBJECT, { reason: 'no_audience_claim' }],
      [
        'an audience no organization claims',
        { ...SUBJECT, aud: 'https://nobody.example/orca' },
        { reason: 'audience_matches_no_organization' },
      ],
      [
        // An archived organization is indistinguishable from one that was never
        // created, by construction of the lookups, so it lands on the same reason.
        'an audience whose only organization is archived',
        { ...SUBJECT, aud: 'https://archived.example/orca' },
        { reason: 'audience_matches_no_organization' },
      ],
      [
        'audiences naming several organizations',
        { ...SUBJECT, aud: [ALPHA_AUDIENCE, BETA_AUDIENCE] },
        { reason: 'audience_matches_several_organizations' },
      ],
      [
        // The known limitation of this mode, and the reason that names it.
        'an organization with a second active workspace',
        { ...SUBJECT, aud: 'https://many.example/orca' },
        { reason: 'multiple_active_workspaces', organizationId: 'org_many' },
      ],
      [
        'an organization with no active workspace',
        { ...SUBJECT, aud: 'https://empty.example/orca' },
        { reason: 'no_active_workspace', organizationId: 'org_empty' },
      ],
      [
        'a workspace claim naming an unknown workspace',
        { ...SUBJECT, aud: ALPHA_AUDIENCE, workspace_id: 'ws_never_created' },
        { reason: 'workspace_not_found', workspaceId: 'ws_never_created' },
      ],
      [
        'a workspace claim naming an archived workspace',
        { ...SUBJECT, aud: ALPHA_AUDIENCE, workspace_id: 'ws_alpha_retired' },
        { reason: 'workspace_not_found', workspaceId: 'ws_alpha_retired' },
      ],
      [
        'a workspace whose organization was created without an audience',
        { ...SUBJECT, aud: ALPHA_AUDIENCE, workspace_id: 'ws_silent' },
        {
          reason: 'organization_has_no_audience',
          organizationId: 'org_silent',
          workspaceId: 'ws_silent',
        },
      ],
      [
        // The crossing the mode exists to refuse: alpha's credential reaching for
        // beta's workspace.
        "a workspace claim naming another organization's workspace",
        { ...SUBJECT, aud: ALPHA_AUDIENCE, workspace_id: 'ws_beta' },
        { reason: 'audience_not_bound', organizationId: 'org_beta', workspaceId: 'ws_beta' },
      ],
    ];

  for (const [name, payload, expected] of CASES) {
    it(`reports ${name} as ${String(expected['reason'])}`, async () => {
      expect(await reject(payload)).toEqual(expected);
    });
  }

  it('never reports the token or the audiences it carried', async () => {
    const reported = await reject({ ...SUBJECT, aud: ALPHA_AUDIENCE, workspace_id: 'ws_beta' });

    // The whole report, key by key: an operator can already read the
    // `organizations` table, so a raw `aud` adds nothing diagnostic and would
    // put a value that authenticates into log storage.
    expect(Object.keys(reported).sort()).toEqual(['organizationId', 'reason', 'workspaceId']);
    expect(JSON.stringify(warn.mock.calls[0])).not.toContain(ALPHA_AUDIENCE);
    expect(JSON.stringify(warn.mock.calls[0])).not.toContain('signed-token');
  });

  it('says nothing at all when a token resolves', async () => {
    verify.mockResolvedValue({ payload: { ...SUBJECT, aud: ALPHA_AUDIENCE } });

    await expect(buildOidcAuth(RESOLVING, fakeLookups())(BEARER)).resolves.toMatchObject({
      workspaceId: 'ws_alpha',
    });
    expect(warn).not.toHaveBeenCalled();
  });
});

/**
 * A plane's static audience and its audience MODE are one setting with two
 * states. These cases pin both refusals, on all three planes.
 */
describe('OIDC plane audience configuration', () => {
  const ISSUERS = ['https://issuer.example'];

  it('refuses an empty static audience on a plane that verifies one', () => {
    // The load-bearing case. jose reads an EMPTY audience option as
    // presence-only: in jose 5.10.0 `lib/jwt_claims_set.js` the option being
    // `!== undefined` puts `aud` on the presence check, and the comparison
    // itself is guarded by `if (audience && ...)`, which `''` fails. So a plane
    // configured this way requires an `aud` claim and compares nothing — every
    // token an allowed issuer minted for any relying party would authenticate,
    // while the configuration reads as though an audience were enforced. This
    // assertion is what makes that state unreachable; a Kubernetes
    // `secretKeyRef` cannot, since `optional: false` guarantees only that the
    // key exists, never that its value is non-empty.
    expect(() => buildOidcAuth({ allowedIssuers: ISSUERS, audience: '' })).toThrowError(
      /OIDC_AUDIENCE is empty on the workspace plane/,
    );
    expect(() => buildAdminOidcAuth({ allowedIssuers: ISSUERS, audience: '' })).toThrowError(
      /ADMIN_OIDC_AUDIENCE is empty on the admin plane/,
    );
    expect(() => buildPlatformOidcAuth({ allowedIssuers: ISSUERS, audience: '' })).toThrowError(
      /PLATFORM_OIDC_AUDIENCE is empty on the platform plane/,
    );
  });

  it('refuses a static audience the resolving workspace plane would never verify', () => {
    // The other direction, and the reason it is an error rather than a warning:
    // with the flag on the value is never read, so leaving it set is a key that
    // silently means nothing.
    expect(() =>
      buildOidcAuth(
        {
          allowedIssuers: ISSUERS,
          audience: 'orca-managed-agents',
          resolveWorkspaceByAudience: true,
        },
        fakeLookups(),
      ),
    ).toThrowError(/OIDC_AUDIENCE is set on the workspace plane while OIDC_RESOLVE/);
  });

  it('leaves a plane with no issuers alone in both directions', () => {
    // Such a plane verifies no OIDC token at all, so neither an absent audience
    // nor an inert one can admit anything. This is what keeps the assertion off
    // the default configuration of a deployment that uses API keys only.
    expect(() => buildOidcAuth({ allowedIssuers: [], audience: '' })).not.toThrow();
    expect(() => buildAdminOidcAuth({ allowedIssuers: [], audience: '' })).not.toThrow();
    expect(() => buildPlatformOidcAuth({ allowedIssuers: [], audience: '' })).not.toThrow();
  });

  it('accepts the two configurations that are actually meaningful', () => {
    expect(() =>
      buildOidcAuth({ allowedIssuers: ISSUERS, audience: 'orca-managed-agents' }),
    ).not.toThrow();
    expect(() => buildOidcAuth(RESOLVING, fakeLookups())).not.toThrow();
  });

  it('reports the wrong plane before it reports the audience', () => {
    // Ordering, pinned: `resolveWorkspaceByAudience` on the admin or platform
    // plane is refused for BEING there, not for the audience it happens to
    // carry. Were the audience checked first, an operator clearing
    // ADMIN_OIDC_AUDIENCE to satisfy the mode would be told to set it again.
    expect(() =>
      buildAdminOidcAuth({
        allowedIssuers: ISSUERS,
        audience: '',
        resolveWorkspaceByAudience: true,
      }),
    ).toThrowError(/admin plane, which resolves no workspaces/);
    expect(() =>
      buildPlatformOidcAuth({
        allowedIssuers: ISSUERS,
        audience: '',
        resolveWorkspaceByAudience: true,
      }),
    ).toThrowError(/platform plane, which resolves no workspaces/);
  });
});
