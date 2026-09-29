// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyRequest } from 'fastify';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const verify = vi.hoisted(() => vi.fn());

vi.mock('jose', () => ({
  createRemoteJWKSet: vi.fn(() => ({ mocked: true })),
  jwtVerify: verify,
}));

import { buildAdminOidcAuth, buildOidcAuth, buildPlatformOidcAuth } from '../../src/auth/oidc.js';

const BEARER = { headers: { authorization: 'Bearer signed-token' } } as FastifyRequest;

const WORKSPACE_OIDC = {
  allowedIssuers: ['https://issuer.example'],
  audience: 'orca-managed-agents',
  metadataClaims: true,
};
const ADMIN_OIDC = {
  allowedIssuers: ['https://admin-issuer.example'],
  audience: 'orca-managed-agents-admin',
  metadataClaims: true,
};
const PLATFORM_OIDC = {
  allowedIssuers: ['https://platform-issuer.example'],
  audience: 'orca-managed-agents-platform',
  metadataClaims: true,
};

/**
 * Issuers that cannot mint arbitrary top-level claims nest the values supplied
 * at provisioning time under a single top-level `metadata` object of strings,
 * and emit a `scope` claim that is present but always an empty array. These
 * specs pin that such a token authenticates, and — more importantly — that
 * `metadata` can never widen or override what a top-level claim already says.
 *
 * Every plane here has opted into the fallback with `metadataClaims: true`;
 * that flag is the whole difference from a default plane, which reads no
 * `metadata` at all (see the default-off specs below).
 */
describe('OIDC metadata claim fallback', () => {
  beforeEach(() => {
    verify.mockReset();
  });

  it('authenticates an admin token whose identity claims live only in metadata', async () => {
    verify.mockResolvedValue({
      payload: {
        sub: 'service-account@org-alpha.example',
        scope: [],
        metadata: { orca_organization: 'org-alpha', orca_scopes: 'org:admin' },
      },
    });

    await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toEqual({
      organizationId: 'org-alpha',
      principal: 'service-account@org-alpha.example',
      scopes: ['org:admin'],
      authMethod: 'oidc',
    });
  });

  it('authenticates a workspace token whose workspace id lives only in metadata', async () => {
    verify.mockResolvedValue({
      payload: {
        sub: 'user_meta_actor',
        scope: [],
        metadata: { orca_workspace: 'ws_from_metadata', orca_scopes: 'memory:write' },
      },
    });

    await expect(buildOidcAuth(WORKSPACE_OIDC)(BEARER)).resolves.toEqual({
      workspaceId: 'ws_from_metadata',
      principal: 'user_meta_actor',
      scopes: ['memory:write'],
      authMethod: 'oidc',
      userId: 'user_meta_actor',
      oidcIssuer: 'https://issuer.example',
    });
  });

  it('grants the platform plane from metadata scopes', async () => {
    verify.mockResolvedValue({
      payload: {
        sub: 'deployment-operator',
        scope: [],
        metadata: { orca_scopes: 'platform:admin' },
      },
    });

    await expect(buildPlatformOidcAuth(PLATFORM_OIDC)(BEARER)).resolves.toEqual({
      principal: 'platform-oidc:https%3A%2F%2Fplatform-issuer.example:deployment-operator',
      scopes: ['platform:admin'],
      authMethod: 'oidc',
    });
  });

  it('prefers the top-level organization and scope claims over conflicting metadata', async () => {
    verify.mockResolvedValue({
      payload: {
        organization_id: 'org-from-top-level',
        sub: 'admin-actor',
        scope: 'org:admin',
        metadata: { orca_organization: 'org-from-metadata', orca_scopes: 'platform:admin' },
      },
    });

    await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toEqual({
      organizationId: 'org-from-top-level',
      principal: 'admin-actor',
      scopes: ['org:admin'],
      authMethod: 'oidc',
    });
  });

  it('prefers the legacy orca_organization claim over metadata', async () => {
    verify.mockResolvedValue({
      payload: {
        orca_organization: 'org-from-legacy-claim',
        sub: 'admin-actor',
        scope: 'org:admin',
        metadata: { orca_organization: 'org-from-metadata' },
      },
    });

    await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toMatchObject({
      organizationId: 'org-from-legacy-claim',
    });
  });

  it('prefers the top-level workspace claim over metadata', async () => {
    verify.mockResolvedValue({
      payload: {
        workspace_id: 'ws_from_top_level',
        sub: 'user_meta_actor',
        scope: 'memory:write',
        metadata: { orca_workspace: 'ws_from_metadata' },
      },
    });

    await expect(buildOidcAuth(WORKSPACE_OIDC)(BEARER)).resolves.toMatchObject({
      workspaceId: 'ws_from_top_level',
    });
  });

  it('never merges metadata scopes into scopes the token already carries', async () => {
    verify.mockResolvedValue({
      payload: {
        workspace_id: 'ws_scope_merge',
        sub: 'user_scope_merge',
        scopes: ['memory:read'],
        metadata: { orca_scopes: 'org:admin platform:admin' },
      },
    });

    await expect(buildOidcAuth(WORKSPACE_OIDC)(BEARER)).resolves.toMatchObject({
      scopes: ['memory:read'],
    });
  });

  it('does not let metadata scopes escalate a token that lacks org:admin', async () => {
    verify.mockResolvedValue({
      payload: {
        organization_id: 'org-alpha',
        sub: 'workspace-reader',
        scope: 'workspaces:read',
        metadata: { orca_scopes: 'org:admin' },
      },
    });

    await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toBeNull();
  });

  it('splits multiple metadata scopes on whitespace', async () => {
    verify.mockResolvedValue({
      payload: {
        workspace_id: 'ws_multi_scope',
        sub: 'user_multi_scope',
        scope: [],
        metadata: { orca_scopes: 'workspaces:read  api_keys:write' },
      },
    });

    await expect(buildOidcAuth(WORKSPACE_OIDC)(BEARER)).resolves.toMatchObject({
      scopes: ['workspaces:read', 'api_keys:write'],
    });
  });

  it('reads an empty or whitespace-only metadata scope string as no scopes', async () => {
    for (const orca_scopes of ['', '   ']) {
      verify.mockResolvedValue({
        payload: {
          workspace_id: 'ws_empty_scope',
          sub: 'user_empty_scope',
          metadata: { orca_scopes },
        },
      });

      await expect(buildOidcAuth(WORKSPACE_OIDC)(BEARER)).resolves.toMatchObject({ scopes: [] });
    }
  });

  it('rejects an admin token carrying no organization in either position', async () => {
    verify.mockResolvedValue({
      payload: {
        sub: 'admin-actor',
        scope: [],
        metadata: { orca_scopes: 'org:admin' },
      },
    });

    await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toBeNull();
  });

  it('ignores a metadata claim that is not a plain object', async () => {
    for (const metadata of ['org-alpha', 42, null, ['org-alpha']]) {
      verify.mockResolvedValue({
        payload: { sub: 'admin-actor', scope: 'org:admin', metadata },
      });

      await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toBeNull();
    }
  });

  it('ignores metadata values that are not strings', async () => {
    verify.mockResolvedValue({
      payload: {
        sub: 'admin-actor',
        scope: [],
        metadata: { orca_organization: { name: 'org-alpha' }, orca_scopes: ['org:admin'] },
      },
    });

    await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toBeNull();
  });

  it('consults the scope claim when the scopes array yields no values', async () => {
    verify.mockResolvedValue({
      payload: {
        workspace_id: 'ws-alpha',
        sub: 'workspace-actor',
        scopes: [],
        scope: 'workspaces:read',
        metadata: { orca_scopes: 'org:admin' },
      },
    });

    await expect(buildOidcAuth(WORKSPACE_OIDC)(BEARER)).resolves.toMatchObject({
      scopes: ['workspaces:read'],
    });
  });

  it('does not let metadata widen a grant shadowed by an empty scopes array', async () => {
    // scopes -> scope -> metadata: the empty `scopes` array must not conclude
    // the scope resolution while a top-level `scope` grant is still present,
    // and metadata must never be reached for such a token.
    verify.mockResolvedValue({
      payload: {
        sub: 'admin-actor',
        organization_id: 'org-alpha',
        scopes: [],
        scope: 'workspaces:read',
        metadata: { orca_scopes: 'org:admin' },
      },
    });

    await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toBeNull();
  });
});

/**
 * The same tokens against planes that did NOT opt in — the shipped default.
 *
 * `metadata` is not read at all there, so a token whose identity or scopes live
 * only under it is admitted by nothing: the workspace and admin planes find no
 * identity, and every plane resolves an empty scope claim to `scopes: []`,
 * which is authenticated-but-authorized-for-nothing rather than an error. This
 * is what keeps an issuer whose `metadata` is filled in by the party requesting
 * the credential from minting identities until an operator enables the plane.
 */
describe('OIDC metadata claim fallback — off by default', () => {
  const WORKSPACE_DEFAULT = {
    allowedIssuers: ['https://issuer.example'],
    audience: 'orca-managed-agents',
  };
  const ADMIN_DEFAULT = {
    allowedIssuers: ['https://admin-issuer.example'],
    audience: 'orca-managed-agents-admin',
  };
  const PLATFORM_DEFAULT = {
    allowedIssuers: ['https://platform-issuer.example'],
    audience: 'orca-managed-agents-platform',
  };

  beforeEach(() => {
    verify.mockReset();
  });

  it('rejects a workspace token whose workspace id lives only in metadata', async () => {
    verify.mockResolvedValue({
      payload: {
        sub: 'user_meta_actor',
        scope: [],
        metadata: { orca_workspace: 'ws_from_metadata', orca_scopes: 'memory:write' },
      },
    });

    await expect(buildOidcAuth(WORKSPACE_DEFAULT)(BEARER)).resolves.toBeNull();
  });

  it('rejects an admin token whose organization and scopes live only in metadata', async () => {
    verify.mockResolvedValue({
      payload: {
        sub: 'service-account@org-alpha.example',
        scope: [],
        metadata: { orca_organization: 'org-alpha', orca_scopes: 'org:admin' },
      },
    });

    await expect(buildAdminOidcAuth(ADMIN_DEFAULT)(BEARER)).resolves.toBeNull();
  });

  it('does not grant the platform plane from metadata scopes', async () => {
    verify.mockResolvedValue({
      payload: {
        sub: 'deployment-operator',
        scope: [],
        metadata: { orca_scopes: 'platform:admin' },
      },
    });

    await expect(buildPlatformOidcAuth(PLATFORM_DEFAULT)(BEARER)).resolves.toBeNull();
  });
});

/**
 * A top-level scope claim that is present but carries no readable scope is not
 * the same as one that says "no scopes". The first is an issuer we cannot
 * interpret and fails closed — no scopes, and no metadata fallback, so a
 * broken claim can never be widened by a nested one. The second keeps falling
 * through to `metadata`, which is the shape real issuers emit.
 */
describe('OIDC malformed top-level scope claims', () => {
  let warn: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    verify.mockReset();
    warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    warn.mockRestore();
  });

  it('grants nothing and skips metadata when the scopes array holds no readable scope', async () => {
    for (const scopes of [[42], ['']]) {
      verify.mockResolvedValue({
        payload: {
          sub: 'admin-actor',
          organization_id: 'org-alpha',
          scopes,
          metadata: { orca_scopes: 'org:admin' },
        },
      });

      await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toBeNull();
    }
    expect(warn).toHaveBeenCalled();
  });

  it('grants nothing and skips metadata when a scope claim has an unreadable type', async () => {
    // `scope` as a number, and a space-separated string in the array-only
    // plural claim: both are shapes we refuse to guess at.
    for (const payload of [{ scope: 42 }, { scopes: 'org:admin' }]) {
      verify.mockResolvedValue({
        payload: {
          sub: 'admin-actor',
          organization_id: 'org-alpha',
          ...payload,
          metadata: { orca_scopes: 'org:admin' },
        },
      });

      await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toBeNull();
    }
  });

  it('grants nothing and skips the sibling scope claim when scopes is malformed', async () => {
    // The malformed `scopes` claim must fail closed against the *next source in
    // the resolution order*, not just against `metadata`: a token that shadows a
    // readable `scope` with an unreadable `scopes` gets no scopes at all. Both
    // malformed shapes are paired with a sibling `scope` the resolution would
    // otherwise grant.
    for (const claims of [
      { scopes: 42, scope: 'org:admin' },
      { scopes: [42], scope: ['org:admin'] },
    ]) {
      verify.mockResolvedValue({
        payload: {
          sub: 'workspace-actor',
          workspace_id: 'ws_malformed_plural',
          organization_id: 'org-alpha',
          ...claims,
          metadata: { orca_scopes: 'org:admin' },
        },
      });

      await expect(buildOidcAuth(WORKSPACE_OIDC)(BEARER)).resolves.toMatchObject({ scopes: [] });
      await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toBeNull();
    }
  });

  it('reads a populated singular scope array as a grant that metadata cannot widen', async () => {
    // The target issuer mints its singular `scope` claim as an array of
    // strings. That is a grant, so metadata is never reached.
    const payload = {
      sub: 'workspace-actor',
      workspace_id: 'ws_singular_array',
      organization_id: 'org-alpha',
      scope: ['workspaces:read'],
      metadata: { orca_scopes: 'org:admin' },
    };
    verify.mockResolvedValue({ payload });

    await expect(buildOidcAuth(WORKSPACE_OIDC)(BEARER)).resolves.toMatchObject({
      scopes: ['workspaces:read'],
    });
    await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toBeNull();
    expect(warn).not.toHaveBeenCalled();
  });

  it('keeps reaching metadata for an empty or null scope claim', async () => {
    for (const claims of [{ scope: [] }, { scopes: [] }, { scope: null }, { scopes: null }]) {
      verify.mockResolvedValue({
        payload: {
          sub: 'admin-actor',
          organization_id: 'org-alpha',
          ...claims,
          metadata: { orca_scopes: 'org:admin' },
        },
      });

      await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toMatchObject({
        scopes: ['org:admin'],
      });
    }
    expect(warn).not.toHaveBeenCalled();
  });

  it('warns once per distinct malformed claim shape', async () => {
    // The warning sits on the per-request scope path, so it is deduplicated by
    // claim name and value type for the life of the process. `scopes` as a
    // boolean is used by no other spec here, so its first report is ours.
    verify.mockResolvedValue({
      payload: {
        sub: 'admin-actor',
        organization_id: 'org-alpha',
        scopes: true,
        metadata: { orca_scopes: 'org:admin' },
      },
    });

    await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toBeNull();
    await expect(buildAdminOidcAuth(ADMIN_OIDC)(BEARER)).resolves.toBeNull();

    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('malformed OIDC `scopes` claim'));
    // The claim value never reaches the log.
    expect(warn).not.toHaveBeenCalledWith(expect.stringContaining('org:admin'));
  });
});

/**
 * The opt-in is one boolean for a whole plane, so on a plane allowing several
 * issuers it does not name which issuer's `metadata` is trusted — it trusts all
 * of them. That is refused where it is configured rather than where a token
 * arrives: the authenticators are built while the app is, so this fails the
 * process at start instead of granting quietly for the life of the deployment.
 */
describe('OIDC metadata claim fallback — refused on a multi-issuer plane', () => {
  const A = 'https://issuer-a.example';
  const B = 'https://issuer-b.example';

  beforeEach(() => {
    verify.mockReset();
  });

  it('refuses to build any plane with the fallback on and two issuers', () => {
    expect(() =>
      buildOidcAuth({ allowedIssuers: [A, B], audience: 'aud', metadataClaims: true }),
    ).toThrowError(/OIDC_METADATA_CLAIMS enables the OIDC metadata claim fallback/);
    expect(() =>
      buildAdminOidcAuth({ allowedIssuers: [A, B], audience: 'aud', metadataClaims: true }),
    ).toThrowError(/ADMIN_OIDC_METADATA_CLAIMS/);
    expect(() =>
      buildPlatformOidcAuth({ allowedIssuers: [A, B], audience: 'aud', metadataClaims: true }),
    ).toThrowError(/PLATFORM_OIDC_METADATA_CLAIMS/);
  });

  it('accepts the fallback on a single-issuer plane', () => {
    expect(() => buildOidcAuth(WORKSPACE_OIDC)).not.toThrow();
    expect(() => buildAdminOidcAuth(ADMIN_OIDC)).not.toThrow();
    expect(() => buildPlatformOidcAuth(PLATFORM_OIDC)).not.toThrow();
  });

  it('accepts multiple issuers while the fallback is off — the shipped default', () => {
    for (const metadataClaims of [{ metadataClaims: false }, {}]) {
      expect(() =>
        buildOidcAuth({ allowedIssuers: [A, B], audience: 'aud', ...metadataClaims }),
      ).not.toThrow();
      expect(() =>
        buildAdminOidcAuth({ allowedIssuers: [A, B], audience: 'aud', ...metadataClaims }),
      ).not.toThrow();
      expect(() =>
        buildPlatformOidcAuth({ allowedIssuers: [A, B], audience: 'aud', ...metadataClaims }),
      ).not.toThrow();
    }
  });

  it('accepts a plane with no issuers configured at all', () => {
    // Such a plane authenticates nothing, so the opt-in has nothing to widen.
    const unconfigured = { allowedIssuers: [], audience: 'aud', metadataClaims: true };
    expect(() => buildOidcAuth(unconfigured)).not.toThrow();
    expect(() => buildAdminOidcAuth(unconfigured)).not.toThrow();
    expect(() => buildPlatformOidcAuth(unconfigured)).not.toThrow();
  });

  it('still authenticates from metadata on the single-issuer plane it allows', async () => {
    // The guard narrows where the fallback may be enabled, never what it does
    // once it is: the capability the opt-in exists to grant is unchanged.
    verify.mockResolvedValue({
      payload: {
        sub: 'deployment-operator',
        scope: [],
        metadata: { orca_scopes: 'platform:admin' },
      },
    });

    await expect(buildPlatformOidcAuth(PLATFORM_OIDC)(BEARER)).resolves.toEqual({
      principal: 'platform-oidc:https%3A%2F%2Fplatform-issuer.example:deployment-operator',
      scopes: ['platform:admin'],
      authMethod: 'oidc',
    });
  });
});
