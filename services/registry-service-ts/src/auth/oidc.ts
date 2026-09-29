// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { open } from 'node:fs/promises';
import { createRemoteJWKSet, jwtVerify } from 'jose';
import type { FastifyRequest } from 'fastify';
import { oidcWorkspaceResolutionRejectedTotal } from '../metrics.js';
import type { AdminPrincipal, AuthenticatedPrincipal, PlatformPrincipal } from './principal.js';
import { parseWorkspaceId } from './workspace-id.js';

export interface OidcConfig {
  allowedIssuers: string[];
  /**
   * The one static audience every token on this plane must carry, enforced by
   * `jwtVerify`. Required whenever the plane has issuers and is not resolving
   * by organization audience, and required to be EMPTY when it is — see
   * {@link assertAudienceMatchesMode}, which refuses both mismatches at build
   * time rather than letting either read as an audience rule the plane does
   * not have.
   */
  audience: string;
  /**
   * Consult the nested `metadata` object for identity and scope claims when the
   * corresponding top-level claim is absent. Off by default: on some issuers
   * `metadata` is populated by the party requesting the credential rather than
   * by the issuer itself, so it is not authorization material until an operator
   * declares it to be for this plane. Only accepted on a plane whose
   * `allowedIssuers` names at most one issuer — see
   * {@link assertMetadataClaimsScoped}.
   */
  metadataClaims?: boolean | undefined;
  /**
   * Optional path to a JSON file listing revoked token identifiers. When set, a
   * token whose `jti` appears in the file is rejected after its signature has
   * been verified. Unset disables the check entirely. See
   * {@link parseDeniedJtis} for the accepted file shapes.
   */
  deniedJtiFile?: string | undefined;
  /**
   * Workspace plane only. Accept organization-scoped credentials that name
   * their organization in `aud` instead of carrying this plane's one static
   * audience, and bind every workspace-plane token to the organization that
   * owns the workspace it ends up with. Off by default, in which case the
   * plane behaves exactly as it did before this option existed. See
   * {@link resolveWorkspaceByOrganizationAudience} for the rule this turns on,
   * and note that it displaces `audience`: with it on, `audience` is no longer
   * checked during verification and MUST be empty
   * ({@link assertAudienceMatchesMode}).
   */
  resolveWorkspaceByAudience?: boolean | undefined;
}

/**
 * A verified token's view of an organization: its id and the audience an
 * operator configured for it, which may be absent.
 */
export interface OrganizationAudienceRef {
  readonly id: string;
  readonly audience: string | null;
}

/**
 * The three narrow reads {@link OidcConfig.resolveWorkspaceByAudience} needs.
 *
 * Injected rather than reached for directly, following the same shape as the
 * queries `admin-auth.ts` runs against `organizations`: the authenticator states
 * what it needs to know, `auth.ts` binds it to the database, and a unit test
 * fakes it without one. Every method answers only about ACTIVE rows — an
 * archived organization or workspace is indistinguishable here from one that
 * does not exist, which is what keeps the caller's fail-closed reasoning to a
 * single rule.
 */
export interface OrganizationAudienceLookups {
  /**
   * Active organizations whose configured audience is one of `audiences`.
   * Organizations with no audience never match. May stop at two rows: the
   * caller only distinguishes none, exactly one, and more than one.
   */
  organizationsForAudiences(audiences: readonly string[]): Promise<OrganizationAudienceRef[]>;
  /**
   * The active organization owning an active workspace, or null when either the
   * workspace or its organization is missing or archived.
   */
  organizationForWorkspace(workspaceId: string): Promise<OrganizationAudienceRef | null>;
  /**
   * Ids of an organization's active workspaces. May stop at two, for the same
   * reason as {@link organizationsForAudiences}.
   */
  activeWorkspaceIds(organizationId: string): Promise<string[]>;
}

const ISSUER_CACHE = new Map<string, ReturnType<typeof createRemoteJWKSet>>();

function jwksFor(issuer: string) {
  let jwks = ISSUER_CACHE.get(issuer);
  if (!jwks) {
    jwks = createRemoteJWKSet(new URL(`${issuer.replace(/\/$/, '')}/.well-known/jwks.json`));
    ISSUER_CACHE.set(issuer, jwks);
  }
  return jwks;
}

/**
 * Rejects a plane whose `metadataClaims` opt-in cannot mean what the operator
 * intended.
 *
 * `metadataClaims` is one boolean for the whole plane while `allowedIssuers` is
 * a list, so enabling it does not say "trust this issuer's `metadata`" — it
 * says "trust `metadata` from whichever allowed issuer verified the token". On
 * a plane where one issuer populates `metadata` itself and another passes
 * through whatever the party requesting the credential supplied, that is
 * exactly the grant the opt-in exists to withhold: a `metadata.orca_scopes` of
 * `platform:admin` minted by the second issuer is a platform admin.
 *
 * Enabled together with more than one issuer is therefore not a policy this
 * code can carry out, and it is refused when the authenticator is built —
 * which is process start, since every plane's authenticator is built while the
 * app is, before it listens. A per-issuer opt-in is the config shape that would
 * let the two coexist; it is deliberately not introduced while every plane has
 * exactly one issuer, and this guard is what makes its absence a startup
 * failure instead of a silent grant.
 *
 * A plane with no issuers at all is left alone: it authenticates nothing, so
 * the opt-in has nothing to widen.
 */
function assertMetadataClaimsScoped(plane: string, envVar: string, config: OidcConfig): void {
  if (config.metadataClaims !== true || config.allowedIssuers.length <= 1) return;
  throw new Error(
    `${envVar} enables the OIDC metadata claim fallback on the ${plane} plane, which allows ` +
      `${config.allowedIssuers.length} issuers. The opt-in is plane-wide, so it would also trust the ` +
      '`metadata` object of every other allowed issuer. Configure a single issuer for this plane, ' +
      'or disable the metadata fallback.',
  );
}

/**
 * Rejects a plane whose static `audience` and audience MODE disagree.
 *
 * Two combinations are refused, one per direction, and both are refused for the
 * same reason: on this plane the audience is authorization material, so a value
 * that is silently ignored and a value that is silently absent are each a
 * configuration that reads as an audience rule the plane does not have.
 *
 * RESOLUTION ON, AUDIENCE SET. Verification stops consulting `audience` the
 * moment {@link OidcConfig.resolveWorkspaceByAudience} is on — the binding
 * check replaces it — so a value left behind is inert. Requiring it cleared is
 * what keeps "which audience rule is this plane running" answerable from the
 * configuration alone, instead of from the flag plus knowledge of this file.
 *
 * ISSUERS CONFIGURED, NOT RESOLVING, AUDIENCE EMPTY. This one is not cosmetic.
 * jose treats an empty audience option as PRESENCE-ONLY: in jose 5.10.0
 * `lib/jwt_claims_set.js`, `audience !== undefined` puts `aud` on the
 * presence check (:38), so the claim must exist, but the comparison itself is
 * guarded by `if (audience && ...)` (:56) and `''` is falsy — so nothing is
 * compared. A plane configured that way accepts every token an allowed issuer
 * minted for any relying party, while its configuration reads as "an audience
 * is enforced". A Kubernetes `secretKeyRef` with `optional: false` does not
 * cover this: it guarantees the key EXISTS, never that its value is non-empty.
 * Hence a runtime assertion rather than a deployment-time one.
 *
 * A plane with no issuers at all is left alone in both directions: it verifies
 * no OIDC token, so neither value can admit anything. `plane` and `envVar`
 * name the plane being built so the failure points at the key to fix;
 * `resolvesWorkspaces` is false for the two planes that resolve nothing, where
 * {@link assertWorkspacePlaneOnly} has already refused the flag outright.
 */
function assertAudienceMatchesMode(
  plane: string,
  envVar: string,
  resolvesWorkspaces: boolean,
  config: OidcConfig,
): void {
  if (resolvesWorkspaces && config.resolveWorkspaceByAudience === true) {
    if (config.audience.length === 0) return;
    throw new Error(
      `${envVar} is set on the ${plane} plane while OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE is enabled, ` +
        'which stops verifying it: each token is bound to the audience of the organization owning ' +
        `its workspace instead. Clear ${envVar} to run in that mode, or disable the flag.`,
    );
  }
  if (config.allowedIssuers.length === 0 || config.audience.length > 0) return;
  throw new Error(
    `${envVar} is empty on the ${plane} plane, which allows ${config.allowedIssuers.length} OIDC ` +
      'issuer(s). An empty audience is not "no audience check": jose requires the `aud` claim to be ' +
      'present and compares nothing, so every token an allowed issuer minted for any relying party ' +
      `would be accepted. Configure ${envVar}, or remove the issuers from this plane.`,
  );
}

export function buildOidcAuth(config: OidcConfig, lookups?: OrganizationAudienceLookups) {
  assertMetadataClaimsScoped('workspace', 'OIDC_METADATA_CLAIMS', config);
  assertAudienceMatchesMode('workspace', 'OIDC_AUDIENCE', true, config);
  // One value carries both halves of the mode — whether it is on, and what it
  // resolves through — so the two cannot come apart anywhere below.
  const resolution = config.resolveWorkspaceByAudience === true ? requireLookups(lookups) : null;
  return async function oidcAuth(req: FastifyRequest): Promise<AuthenticatedPrincipal | null> {
    const header = req.headers.authorization;
    if (typeof header !== 'string' || !header.startsWith('Bearer ')) return null;
    const token = header.slice(7);

    for (const issuer of config.allowedIssuers) {
      // In resolve mode `aud` names the token's organization rather than this
      // plane, so there is no one static value every token must carry and
      // `audience` is left out of verification. It is not thereby unchecked:
      // `resolveWorkspaceByOrganizationAudience` requires it to name the
      // organization that owns the workspace. That is narrower in the
      // ORGANIZATION dimension and wider in the DEPLOYMENT one, and the two
      // conditions are not comparable. The static check refused every token
      // that did not carry this deployment's `OIDC_AUDIENCE`; nothing here
      // reinstates it, so a token the same issuer minted for an unrelated
      // relying party — rejected outright with the flag off — authenticates
      // here whenever its `aud` happens to carry an organization's audience.
      // What an operator owes this mode in return is on
      // `resolveWorkspaceByOrganizationAudience`.
      const payload = await verifyAgainstIssuer(
        token,
        issuer,
        resolution === null ? config.audience : undefined,
      );
      if (!payload) continue;

      if (await isDeniedJti(config, payload)) return null;
      const claimedWorkspaceId = parseWorkspaceId(
        payload['workspace_id'] ??
          payload['orca_workspace'] ??
          metadataClaims(config, payload)['orca_workspace'],
      );
      const subject = payload['sub'];
      const principalValue = subject ?? payload['principal'];
      if (typeof principalValue !== 'string' || principalValue.length === 0) return null;
      let workspaceId: string | null;
      if (resolution === null) {
        workspaceId = claimedWorkspaceId;
      } else {
        const resolved = await resolveWorkspaceByOrganizationAudience(
          resolution,
          payload,
          claimedWorkspaceId,
        );
        // The rejection is reported here rather than inside the resolver so
        // that the resolver stays a pure decision and there is exactly one
        // place where a refusal becomes visible to an operator.
        if ('reason' in resolved) {
          reportResolutionRejection(req, resolved);
          return null;
        }
        workspaceId = resolved.workspaceId;
      }
      if (!workspaceId) return null;
      const scopes = parseScopes(config, payload);
      return {
        workspaceId,
        principal: principalValue,
        scopes,
        authMethod: 'oidc',
        ...(typeof subject === 'string' && subject.length > 0
          ? { userId: subject, oidcIssuer: issuer }
          : {}),
      };
    }
    return null;
  };
}

/**
 * Insists on the lookups that {@link OidcConfig.resolveWorkspaceByAudience}
 * cannot work without.
 *
 * Only a caller wiring the plane by hand can get this wrong, and refusing at
 * build time — which is process start — is what stops the mode from silently
 * degrading into "no audience is verified at all": verification stops checking
 * `audience` the moment the flag is on, and the binding check that replaces it
 * needs these reads.
 */
function requireLookups(lookups: OrganizationAudienceLookups | undefined) {
  if (lookups !== undefined) return lookups;
  throw new Error(
    'OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE is enabled on the workspace plane, but its authenticator ' +
      'was built without the organization lookups the mode resolves and binds through.',
  );
}

/**
 * Verifies one candidate issuer, answering null rather than throwing so that a
 * token minted by a later issuer in the list is still reached.
 *
 * Only the verification itself is guarded. What follows it in
 * {@link buildOidcAuth} — the revocation check, claim reading, and the database
 * reads that resolve a workspace — is deliberately outside: those run against
 * the issuer whose signature already verified, so no other issuer could answer
 * for them, and a failing database must surface as a failing request rather
 * than as "this issuer did not mint the token".
 */
async function verifyAgainstIssuer(
  token: string,
  issuer: string,
  audience: string | undefined,
): Promise<Record<string, unknown> | null> {
  try {
    const { payload } = await jwtVerify(token, jwksFor(issuer), {
      issuer,
      ...(audience === undefined ? {} : { audience }),
    });
    return payload;
  } catch {
    return null;
  }
}

/**
 * Resolves the workspace of a verified workspace-plane token, and binds it to an
 * organization, when {@link OidcConfig.resolveWorkspaceByAudience} is on.
 *
 * The mode exists so that a credential can be issued per organization rather
 * than per workspace: the issuer sets `aud` to the organization's configured
 * audience and needs to know nothing about workspace ids. Resolution is
 * therefore server-side, and it is where audience validation moved to — with
 * this mode on, `jwtVerify` no longer checks a static audience.
 *
 * A workspace is reached one of two ways:
 *
 *   - the token carries a workspace claim, read through the same paths and in
 *     the same precedence as always; or
 *   - it does not, and the organization whose audience the token names is
 *     looked up. Exactly one organization must match and it must own exactly
 *     one active workspace.
 *
 * Whichever way it was reached, the SAME binding check then applies: the
 * organization owning that workspace must have an audience configured, and the
 * token's `aud` must contain it. That single rule is what keeps one
 * organization's credential out of another's workspace — a token that names a
 * foreign workspace in a claim reaches this check with that organization's
 * audience, which it does not carry. How far that holds is bounded by what the
 * issuer allows in `aud`; see the operator requirements below. It also means an
 * organization with no audience configured authenticates nothing in this mode,
 * including through a claim that names its workspace directly.
 *
 * Every branch that is not "exactly one organization, exactly one workspace,
 * audience bound" is refused, with the reason below saying which.
 *
 * WHAT THIS MODE REQUIRES OF AN OPERATOR. The binding check is not a narrowing
 * of the static audience check it replaces; it trades one dimension for
 * another. With the flag off a token had to carry this deployment's
 * `OIDC_AUDIENCE`, so a token minted by an allowed issuer for some other
 * relying party was refused. With it on, an organization's audience is the ONLY
 * thing standing between such a token and that organization's workspace. Two
 * properties must therefore hold, and neither is checkable here:
 *
 *   - `aud` must be issuer-controlled, never requester-selectable. GitHub
 *     Actions, for instance, lets the caller name the audience outright, so any
 *     repository able to request a token from that issuer could ask for an
 *     organization's audience and be authenticated into its workspace.
 *   - whatever else is issued carrying an organization's audience must be
 *     something that may act for that organization here, because nothing in
 *     this file distinguishes a credential minted FOR Registry from any other
 *     credential the issuer stamps with the same value. Where the audience is
 *     Registry-dedicated the audience is that check by itself. Where it is
 *     general-purpose — an issuer that derives it from a tenant namespace,
 *     `urn:example:<ns>`, and stamps it on every credential issued there —
 *     the discrimination lives at ISSUANCE, in the issuer's own access
 *     control, and a Registry-dedicated audience is optional hardening rather
 *     than a prerequisite. A value the issuer emits by default is the case to
 *     avoid outright: Keycloak puts `"account"` in `aud` for every token in a
 *     realm, so an organization configured with that audience would accept
 *     every principal of that realm.
 *
 * See `docs/managed-agents/workspace-administration.md` for the rollout
 * checklist these imply.
 *
 * WHAT THIS ANSWERS. A workspace, or the reason there is not one. Every
 * rejection is the same bare 401 to the caller; the reason exists for the
 * operator, who otherwise cannot tell "this organization has no audience" from
 * "this organization grew a second workspace" from "this token was minted for
 * something else entirely" — three configuration faults with three different
 * repairs and one indistinguishable symptom. See
 * {@link reportResolutionRejection} for what is done with it, and for the two
 * things that are never in it.
 */
async function resolveWorkspaceByOrganizationAudience(
  lookups: OrganizationAudienceLookups,
  payload: Record<string, unknown>,
  claimedWorkspaceId: string | null,
): Promise<WorkspaceResolution> {
  const audiences = tokenAudiences(payload);
  // No readable audience can satisfy the binding check below, so nothing is
  // gained by looking anything up.
  if (audiences.length === 0) return { reason: 'no_audience_claim' };

  let organization: OrganizationAudienceRef;
  let workspaceId: string;
  if (claimedWorkspaceId !== null) {
    const owner = await lookups.organizationForWorkspace(claimedWorkspaceId);
    // Archived and never-created are one case here, by construction of the
    // lookups; the reason says only that the claim reached no active workspace.
    if (owner === null) {
      return { reason: 'workspace_not_found', workspaceId: claimedWorkspaceId };
    }
    organization = owner;
    workspaceId = claimedWorkspaceId;
  } else {
    const matches = await lookups.organizationsForAudiences(audiences);
    // Zero is an audience no organization claims; more than one means the
    // token does not identify an organization at all. Neither may guess.
    if (matches.length === 0) return { reason: 'audience_matches_no_organization' };
    if (matches.length > 1) return { reason: 'audience_matches_several_organizations' };
    organization = matches[0]!;
    const workspaceIds = await lookups.activeWorkspaceIds(organization.id);
    // "The organization's workspace" only has a referent when there is exactly
    // one. Zero and several are both a request this mode cannot answer — and
    // the second is the known limitation this mode carries, which is why it is
    // reported as its own reason rather than folded in with the first.
    if (workspaceIds.length === 0) {
      return { reason: 'no_active_workspace', organizationId: organization.id };
    }
    if (workspaceIds.length > 1) {
      return { reason: 'multiple_active_workspaces', organizationId: organization.id };
    }
    workspaceId = workspaceIds[0]!;
  }

  // The binding check. Deliberately unconditional: in the audience branch the
  // organization was found BY this value and the check is redundant, but making
  // it the one gate every resolution passes through is what leaves no path on
  // which it can be forgotten.
  if (organization.audience === null || organization.audience.length === 0) {
    return { reason: 'organization_has_no_audience', organizationId: organization.id, workspaceId };
  }
  if (!audiences.includes(organization.audience)) {
    return { reason: 'audience_not_bound', organizationId: organization.id, workspaceId };
  }
  return { workspaceId };
}

/**
 * Why {@link resolveWorkspaceByOrganizationAudience} answered "no".
 *
 * A closed set with one member per rejecting branch. Each names a distinct
 * configuration fault so an operator reading the logs is pointed at a repair:
 *
 * - `no_audience_claim` — the token carries no readable `aud` at all.
 * - `workspace_not_found` — its workspace claim named no ACTIVE workspace
 *   under an active organization.
 * - `audience_matches_no_organization` — no organization is configured with
 *   any audience the token carries. The expected reason during a rollout, for
 *   an organization created before audiences existed.
 * - `audience_matches_several_organizations` — the token's `aud` values name
 *   more than one organization, leaving "the organization" without a referent.
 * - `no_active_workspace` / `multiple_active_workspaces` — the organization
 *   resolved, but it does not own exactly one active workspace. The second is
 *   the single-active-workspace limitation of this mode showing up in
 *   production; it is the diagnostic for it.
 * - `organization_has_no_audience` — the workspace's owning organization was
 *   created without an audience, so it authenticates nothing in this mode.
 * - `audience_not_bound` — a workspace was reached but the token does not
 *   carry its organization's audience. This is the crossing the mode exists to
 *   refuse, and the one reason here that may indicate an attempt rather than a
 *   misconfiguration.
 */
export type WorkspaceResolutionRejectionReason =
  | 'no_audience_claim'
  | 'workspace_not_found'
  | 'audience_matches_no_organization'
  | 'audience_matches_several_organizations'
  | 'no_active_workspace'
  | 'multiple_active_workspaces'
  | 'organization_has_no_audience'
  | 'audience_not_bound';

/**
 * A refused resolution, carrying whatever it had already established when it
 * refused: the organization it reached, the workspace it reached, or neither.
 * Both are deployment identifiers rather than token material.
 */
export interface RejectedWorkspace {
  readonly reason: WorkspaceResolutionRejectionReason;
  readonly organizationId?: string | undefined;
  readonly workspaceId?: string | undefined;
}

/**
 * The resolver's answer: a workspace, or {@link RejectedWorkspace}. Told apart
 * by the presence of `reason`, so a caller cannot read a rejection as a
 * workspace by forgetting to check a boolean.
 */
export type WorkspaceResolution = { readonly workspaceId: string } | RejectedWorkspace;

/**
 * Records a refused resolution for the operator, and for no one else.
 *
 * The HTTP answer stays a bare 401: which of the eight reasons applied is
 * information about this deployment's organizations, and the request that
 * triggered it has not authenticated. So the reason goes to the log and to a
 * counter, never into a response body.
 *
 * Two things are deliberately absent from what is recorded. The token, in any
 * form — it is a bearer credential and a log is not the place for one. And the
 * token's `aud` values, which are the one piece of the token that the mode
 * treats as a secret-ish discriminator: an operator who can read the logs can
 * already read the `organizations` table, so logging the raw claim adds no
 * diagnostic power and would put a value that authenticates into log storage.
 * `organizationId` and `workspaceId` are Registry's own identifiers and say
 * which row to look at, which is what an operator actually needs.
 */
function reportResolutionRejection(req: FastifyRequest, rejected: RejectedWorkspace): void {
  oidcWorkspaceResolutionRejectedTotal.inc({ reason: rejected.reason });
  req.log.warn(
    {
      reason: rejected.reason,
      ...(rejected.organizationId === undefined ? {} : { organizationId: rejected.organizationId }),
      ...(rejected.workspaceId === undefined ? {} : { workspaceId: rejected.workspaceId }),
    },
    'workspace-plane OIDC token refused by organization-audience resolution',
  );
}

/**
 * The audiences a verified token carries, as a list.
 *
 * `aud` is a single string or an array of them, and unreadable members are
 * dropped rather than rejected: the check that consumes this asks whether a
 * specific organization's audience is present, so an entry that is not a string
 * cannot answer it either way.
 */
function tokenAudiences(payload: Record<string, unknown>): string[] {
  const aud = payload['aud'];
  if (typeof aud === 'string') return aud.length > 0 ? [aud] : [];
  if (!Array.isArray(aud)) return [];
  return aud.filter((value): value is string => typeof value === 'string' && value.length > 0);
}

/**
 * Refuses {@link OidcConfig.resolveWorkspaceByAudience} on a plane that cannot
 * honour it.
 *
 * The option travels on the shared {@link OidcConfig}, but only the workspace
 * plane resolves workspaces, so the admin and platform builders would simply
 * ignore it. Ignoring it silently is the wrong failure: an operator who set it
 * would read the plane as having an audience rule it does not have, on the two
 * planes where the static audience is the entire audience check.
 */
function assertWorkspacePlaneOnly(plane: string, config: OidcConfig): void {
  if (config.resolveWorkspaceByAudience !== true) return;
  throw new Error(
    `resolveWorkspaceByAudience was set on the ${plane} plane, which resolves no workspaces. It is a ` +
      'workspace-plane option (OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE); an ' +
      `${plane} token's audience is always verified against the configured static value.`,
  );
}

export function buildAdminOidcAuth(config: OidcConfig) {
  assertMetadataClaimsScoped('admin', 'ADMIN_OIDC_METADATA_CLAIMS', config);
  assertWorkspacePlaneOnly('admin', config);
  assertAudienceMatchesMode('admin', 'ADMIN_OIDC_AUDIENCE', false, config);
  return async function adminOidcAuth(req: FastifyRequest): Promise<AdminPrincipal | null> {
    const header = req.headers.authorization;
    if (typeof header !== 'string' || !header.startsWith('Bearer ')) return null;
    const token = header.slice(7);

    for (const issuer of config.allowedIssuers) {
      try {
        const { payload } = await jwtVerify(token, jwksFor(issuer), {
          issuer,
          audience: config.audience,
        });
        if (await isDeniedJti(config, payload)) return null;
        const organizationId =
          payload['organization_id'] ??
          payload['orca_organization'] ??
          metadataClaims(config, payload)['orca_organization'];
        const principal = payload['sub'] ?? payload['principal'];
        const scopes = parseScopes(config, payload);
        if (
          typeof organizationId !== 'string' ||
          organizationId.length === 0 ||
          typeof principal !== 'string' ||
          principal.length === 0 ||
          !scopes.includes('org:admin')
        ) {
          return null;
        }
        return { organizationId, principal, scopes, authMethod: 'oidc' };
      } catch {
        /* try next issuer */
      }
    }
    return null;
  };
}

export function buildPlatformOidcAuth(config: OidcConfig) {
  assertMetadataClaimsScoped('platform', 'PLATFORM_OIDC_METADATA_CLAIMS', config);
  assertWorkspacePlaneOnly('platform', config);
  assertAudienceMatchesMode('platform', 'PLATFORM_OIDC_AUDIENCE', false, config);
  return async function platformOidcAuth(req: FastifyRequest): Promise<PlatformPrincipal | null> {
    const header = req.headers.authorization;
    if (typeof header !== 'string' || !header.startsWith('Bearer ')) return null;
    const token = header.slice(7);

    for (const issuer of config.allowedIssuers) {
      try {
        const { payload } = await jwtVerify(token, jwksFor(issuer), {
          issuer,
          audience: config.audience,
        });
        if (await isDeniedJti(config, payload)) return null;
        const principal = payload['sub'] ?? payload['principal'];
        const scopes = parseScopes(config, payload);
        if (
          typeof principal !== 'string' ||
          principal.length === 0 ||
          !scopes.includes('platform:admin')
        ) {
          return null;
        }
        return {
          principal: platformOidcPrincipal(issuer, principal),
          scopes,
          authMethod: 'oidc',
        };
      } catch {
        /* try next issuer */
      }
    }
    return null;
  };
}

function platformOidcPrincipal(issuer: string, subject: string): string {
  return `platform-oidc:${encodeURIComponent(issuer)}:${encodeURIComponent(subject)}`;
}

/**
 * Resolves the scopes a verified token grants, in the documented
 * `scopes` -> `scope` -> `metadata.orca_scopes` order.
 *
 * Each top-level claim is classified as granted, empty or malformed (see
 * {@link classifyScopeClaim}). The first claim that grants anything wins
 * outright, so a token that already carries scopes keeps exactly those scopes:
 * `metadata` is never merged into them and never overrides them. An empty claim
 * falls through to the next source, so an issuer that always emits `scopes: []`
 * / `scope: []` still reaches `metadata`.
 *
 * A malformed claim fails closed: the token is granted nothing and the metadata
 * fallback is NOT consulted. A present-but-unreadable authorization claim must
 * not be able to widen into `metadata` — we cannot tell an issuer bug from an
 * attempt to shadow a narrow grant with a broader nested one.
 *
 * `metadata.orca_scopes` is a space-separated string, and is only reachable at
 * all when {@link OidcConfig.metadataClaims} is enabled for this plane. Tokens
 * without it are unaffected.
 */
function parseScopes(config: OidcConfig, payload: Record<string, unknown>): string[] {
  const plural = classifyScopeClaim('scopes', payload['scopes']);
  if (plural.kind === 'granted') return plural.scopes;
  if (plural.kind === 'malformed') return [];

  const singular = classifyScopeClaim('scope', payload['scope']);
  if (singular.kind === 'granted') return singular.scopes;
  if (singular.kind === 'malformed') return [];

  const metadataScopes = metadataClaims(config, payload)['orca_scopes'];
  return typeof metadataScopes === 'string' ? splitScopeString(metadataScopes) : [];
}

/**
 * A top-level scope claim, classified into the three cases the resolution order
 * has to tell apart: a claim that grants scopes, one that is present but says
 * nothing, and one we cannot read at all.
 */
type ScopeClaim =
  | { readonly kind: 'granted'; readonly scopes: string[] }
  | { readonly kind: 'empty' }
  | { readonly kind: 'malformed' };

const EMPTY_SCOPE_CLAIM: ScopeClaim = { kind: 'empty' };

/**
 * Classifies one top-level scope claim:
 *
 * - absent or `null` -> empty. `null` is an empty list rather than a broken
 *   claim, as in {@link parseDeniedJtis}.
 * - array (either claim — the singular `scope` claim really is an array of
 *   strings for some issuers) -> its non-empty string elements when there are
 *   any, so a mixed array grants the elements we can read; `[]` -> empty, which
 *   is what keeps `metadata` reachable for issuers that always emit one;
 *   a populated array with no readable element -> malformed.
 * - `scope` as a string -> its whitespace-separated tokens, or empty when the
 *   string is blank or whitespace-only.
 * - anything else, including a plain string in the plural `scopes` claim ->
 *   malformed.
 */
function classifyScopeClaim(claim: 'scope' | 'scopes', value: unknown): ScopeClaim {
  if (value === undefined || value === null) return EMPTY_SCOPE_CLAIM;

  if (Array.isArray(value)) {
    const scopes = value.filter(
      (scope): scope is string => typeof scope === 'string' && scope.length > 0,
    );
    if (scopes.length > 0) return { kind: 'granted', scopes };
    return value.length === 0 ? EMPTY_SCOPE_CLAIM : malformedScopeClaim(claim, value);
  }

  if (claim === 'scope' && typeof value === 'string') {
    const scopes = splitScopeString(value);
    return scopes.length > 0 ? { kind: 'granted', scopes } : EMPTY_SCOPE_CLAIM;
  }

  return malformedScopeClaim(claim, value);
}

/**
 * Malformed scope-claim shapes already reported, keyed by claim name and the
 * `typeof` its value. Scope resolution runs on every authenticated request, so
 * an issuer emitting a broken claim would otherwise warn once per request; one
 * line per distinct shape per process is enough to diagnose it.
 */
const REPORTED_MALFORMED_SCOPE_CLAIMS = new Set<string>();

function malformedScopeClaim(claim: string, value: unknown): ScopeClaim {
  const shape = `${claim}:${typeof value}`;
  if (!REPORTED_MALFORMED_SCOPE_CLAIMS.has(shape)) {
    REPORTED_MALFORMED_SCOPE_CLAIMS.add(shape);
    // The value is authorization material from a token and is never logged;
    // its claim name and type are enough to identify the issuer's bug.
    console.warn(
      `registry-service-ts read a malformed OIDC \`${claim}\` claim of type ${typeof value}; ` +
        'the token is granted no scopes and the metadata fallback is not consulted',
    );
  }
  return { kind: 'malformed' };
}

function splitScopeString(value: string): string[] {
  return value.split(/\s+/).filter(Boolean);
}

/**
 * Reads the nested top-level `metadata` object of a verified payload, for
 * planes that opted into it.
 *
 * Some issuers cannot mint arbitrary top-level claims and instead nest the
 * key/values supplied at provisioning time under a single `metadata` object
 * whose values are strings. Returns an empty object when the claim is absent,
 * is not a plain object, or the plane has not enabled
 * {@link OidcConfig.metadataClaims}, so callers can index it unconditionally.
 *
 * The opt-in lives here rather than at the call sites so that every plane and
 * every claim reads `metadata` under the same condition. It is off by default
 * because on some issuers `metadata` is filled in by whoever requests the
 * credential rather than by the issuer, which would make the ability to create
 * a credential equivalent to the ability to mint any identity the plane
 * accepts. Only an operator can tell the two kinds of issuer apart. This
 * plane-wide condition is equivalent to a per-issuer one because
 * {@link assertMetadataClaimsScoped} constrains any plane that enables it to a
 * single issuer, so the issuer whose signature verified is necessarily that one.
 *
 * With the fallback off, a token whose identity or scopes live only in
 * `metadata` reads as absent rather than as a rejection: an empty top-level
 * scope claim still falls through, now to an empty object, and yields
 * `scopes: []` — authenticated but authorized for nothing. That matches the
 * existing malformed-claim semantics and is the intended behaviour.
 *
 * Callers MUST consult this only when the corresponding top-level claim is
 * absent. `metadata` is a fallback, never a merge and never an override, so it
 * cannot widen a grant a token already carries. For scopes it is narrower
 * still: a top-level claim that is present but unreadable stops the resolution
 * before `metadata` is reached — see {@link parseScopes}.
 */
function metadataClaims(
  config: OidcConfig,
  payload: Record<string, unknown>,
): Record<string, unknown> {
  if (config.metadataClaims !== true) return {};
  const metadata = payload['metadata'];
  if (typeof metadata !== 'object' || metadata === null || Array.isArray(metadata)) return {};
  return metadata as Record<string, unknown>;
}

/**
 * How long a loaded denied-JTI list is served before the file is read again.
 * The file is small and re-read wholesale, so a short interval keeps
 * revocations near-real-time without a syscall on every authenticated request.
 */
const DENIED_JTI_RELOAD_INTERVAL_MS = 5_000;

/**
 * How long a request will wait for an in-flight read of the list before giving
 * up on it and being served from the set already in hand. The file is small and
 * local, so a healthy read is sub-millisecond; this is a stall detector, not a
 * budget, and it is what keeps a wedged mount (a hung NFS/EFS volume, a
 * projected secret whose backing store is unreachable) from turning every
 * authenticated request into a hung request.
 */
const DENIED_JTI_READ_DEADLINE_MS = 1_000;

/**
 * Largest denied-JTI document that will be read into memory.
 *
 * The real revocation list is small JSON — kilobytes today, and still only a
 * megabyte at twenty thousand entries — so this is headroom, not a budget. What
 * it exists to stop is the other case: a multi-GB file, or an endless character
 * device, reached because the configured path is not the file we think it is.
 * Such a path must not be buffered. Reading one whole allocates off-heap until
 * the process is OOM-killed, which `--max-old-space-size` does nothing to
 * bound, and it takes down the plane rather than just the check.
 */
const DENIED_JTI_MAX_BYTES = 1024 * 1024;

/** One parsed denied-JTI document. See {@link parseDeniedJtis}. */
interface DeniedJtiDocument {
  /** The jtis the document names, ready to enforce. */
  readonly jtis: ReadonlySet<string>;
  /**
   * Entries the parser could not turn into a jti. They are absent from `jtis`,
   * and their count is what tells the caller this document is authoritative
   * only about what it adds — see {@link refreshDeniedJtis}. An entry that
   * could not be read is never a removal, so a jti already being enforced
   * stays enforced even when this document's record for it is unreadable.
   */
  readonly unrecognized: number;
}

interface DeniedJtiState {
  jtis: ReadonlySet<string>;
  /** True once any load has succeeded. Drives the log wording on failure. */
  everLoaded: boolean;
  /** Last load attempt, successful or not, gating the reload interval. */
  checkedAt: number;
  /** In-flight reload, so a burst of requests triggers one read, not N. */
  refresh: Promise<void> | null;
  /**
   * True once the in-flight read has blown its deadline. Latched, so the
   * requests behind the first one are served from the set in hand instead of
   * each paying the deadline again; the refresh clears it when the read finally
   * settles.
   */
  stalled: boolean;
}

/**
 * Keyed by file path and shared across planes: the workspace, admin and
 * platform builders are configured from the same file, so they should not each
 * hold and separately refresh a copy of it.
 */
const DENIED_JTI_CACHE = new Map<string, DeniedJtiState>();

async function isDeniedJti(config: OidcConfig, payload: Record<string, unknown>): Promise<boolean> {
  const path = config.deniedJtiFile;
  if (path === undefined || path === '') return false;
  const jti = payload['jti'];
  // A token carrying no `jti` cannot be named by the list, so it is not
  // deniable. It remains subject to every other check.
  if (typeof jti !== 'string' || jti.length === 0) return false;
  return (await loadDeniedJtis(path)).has(jti);
}

async function loadDeniedJtis(path: string): Promise<ReadonlySet<string>> {
  let state = DENIED_JTI_CACHE.get(path);
  if (!state) {
    state = {
      jtis: new Set(),
      everLoaded: false,
      checkedAt: Number.NEGATIVE_INFINITY,
      refresh: null,
      stalled: false,
    };
    DENIED_JTI_CACHE.set(path, state);
  }
  // `Date.now` is a wall clock, so an NTP correction can move it backwards and
  // leave `checkedAt` in the future. A raw subtraction reads that as a fresh
  // cache and freezes the list for the whole size of the correction — a 60s
  // step back is 60s of revocations not taking effect. A stamp in the future is
  // therefore treated as no stamp at all: reload, and re-stamp on the corrected
  // clock. The cold-start `-Infinity` still yields `+Infinity` here and reloads.
  // The read deadline below needs no such guard: it runs on `setTimeout`, which
  // libuv drives from a monotonic clock.
  const sinceCheck = Date.now() - state.checkedAt;
  if (sinceCheck >= 0 && sinceCheck < DENIED_JTI_RELOAD_INTERVAL_MS) return state.jtis;
  // One read at a time: `refresh` stays set for the whole life of the read, so
  // a read that never returns is never joined by a second one. Abandoning and
  // re-issuing would leak a libuv threadpool thread per interval and starve all
  // fs I/O and DNS in the process within four intervals.
  state.refresh ??= refreshDeniedJtis(path, state);
  if (!state.stalled) await awaitWithDeadline(path, state);
  return state.jtis;
}

/**
 * Waits for the in-flight read, but not forever. On expiry the read is left
 * running — it is the only one, and cancelling it is not possible — and the
 * caller is served from the set already in hand. `stalled` latches so later
 * requests do not each pay the deadline again; the refresh clears it when the
 * read finally settles.
 */
async function awaitWithDeadline(path: string, state: DeniedJtiState): Promise<void> {
  const refresh = state.refresh;
  if (refresh === null) return;
  let timer: NodeJS.Timeout | undefined;
  const deadline = new Promise<'deadline'>((resolve) => {
    timer = setTimeout(() => resolve('deadline'), DENIED_JTI_READ_DEADLINE_MS);
  });
  try {
    if ((await Promise.race([refresh.then(() => 'read' as const), deadline])) === 'read') return;
  } finally {
    clearTimeout(timer);
  }
  // Concurrent waiters can all reach here on the same stall. The first one to
  // resume latches it, so the alarm is raised once per stall rather than once
  // per request caught by it.
  if (state.stalled) return;
  state.stalled = true;
  console.warn(
    `registry-service-ts timed out after ${DENIED_JTI_READ_DEADLINE_MS}ms reading the OIDC denied-JTI list from ${path}; ` +
      `continuing with ${state.everLoaded ? `the last good list of ${state.jtis.size} entries` : 'NO token denied'}`,
  );
}

async function refreshDeniedJtis(path: string, state: DeniedJtiState): Promise<void> {
  try {
    const { jtis, unrecognized } = parseDeniedJtis(await readDeniedJtiFile(path));
    // Decided semantics: replace wholesale on a document the parser read in
    // full, union on one it could not. A clean document is authoritative about
    // the whole list, so a removal from it takes effect on the very next
    // reload. A document with unrecognized entries is authoritative only about
    // what it adds: its readable entries are unioned onto the list already
    // being enforced, because an entry we could not read is evidence of
    // nothing — it is never a removal, and acting on it as one would silently
    // un-revoke a token. Additions therefore always start being enforced at
    // once, even mid-drift; only removals wait for a clean parse.
    //
    // Cold start needs no extra guard: the last-good list is empty, so the
    // union is exactly the parsed set.
    //
    // The cost is bounded and self-healing. While the document stays dirty the
    // enforced set can outlive the file — an entry deleted from it keeps being
    // denied — and its size is bounded by the distinct jtis seen across the
    // dirty window rather than by one document. The next clean read replaces
    // the set wholesale and lands every held removal at once: no freeze, no
    // accumulating delay. The warning below is what makes the dirty window
    // visible while it lasts. See {@link parseDeniedJtis} for the alternatives
    // weighed and the standing assumption this rests on: the producer never
    // reuses a `jti`.
    state.jtis = unrecognized > 0 ? new Set([...state.jtis, ...jtis]) : jtis;
    state.everLoaded = true;
    if (unrecognized > 0) {
      // Counts and the path only; entries are revocation material and are
      // never logged.
      console.warn(
        `registry-service-ts dropped ${unrecognized} unrecognized entries while loading the OIDC denied-JTI list from ${path}; ` +
          `its ${jtis.size} readable entries were applied on top of the last good list, which now denies ${state.jtis.size} entries. ` +
          'Removals in this document are not applied until it parses cleanly, so an entry deleted from the file keeps being denied until then.',
      );
    }
  } catch (error) {
    // Fail open, loudly, rather than fail closed.
    //
    // A missing, unreadable or malformed file must not be able to reject every
    // OIDC token on the service: a single typo in the configured path would
    // otherwise lock every operator out of the workspace, admin AND platform
    // planes simultaneously, leaving no authenticated route to repair the
    // configuration. The cost of this choice is real — while the file is
    // broken, newly revoked tokens keep working — so every failed attempt is
    // logged and alerting is expected to page on it. Revocation is a
    // second line of defence here; tokens are still bounded by signature,
    // issuer, audience and (where present) expiry.
    console.warn(
      state.everLoaded
        ? `registry-service-ts failed to reload OIDC denied-JTI list from ${path}; continuing with the last good list of ${state.jtis.size} entries`
        : `registry-service-ts cannot load OIDC denied-JTI list from ${path}; NO token is being denied until it loads`,
      error,
    );
  } finally {
    state.checkedAt = Date.now();
    state.refresh = null;
    if (state.stalled) {
      state.stalled = false;
      // The pair to the timeout warning: it says requests stopped waiting, this
      // says the read they stopped waiting for is over and the list is being
      // maintained normally again. Without it a stall reads as permanent.
      console.warn(
        `registry-service-ts finished the stalled read of the OIDC denied-JTI list from ${path}; ` +
          'the list is being refreshed on the normal interval again',
      );
    }
  }
}

/**
 * Reads at most {@link DENIED_JTI_MAX_BYTES}, bounding memory by construction.
 *
 * `stat` cannot do this job on its own: it reports size 0 for FIFOs and
 * character devices, so a size check would wave through exactly the paths that
 * grow without bound. Reading with an explicit cap covers both that case and an
 * ordinary file that is simply far too large.
 *
 * Overflow throws into the caller's existing catch, so an oversized document is
 * handled as a failed reload: the last-good list keeps being enforced and the
 * warning already there raises the alarm.
 */
async function readDeniedJtiFile(path: string): Promise<string> {
  const handle = await open(path, 'r');
  try {
    // One byte past the cap, so an oversized document is detectable rather than
    // silently truncated into a parse error — which would be read as a corrupt
    // file and send an operator looking in the wrong place.
    const buffer = Buffer.allocUnsafe(DENIED_JTI_MAX_BYTES + 1);
    let filled = 0;
    while (filled <= DENIED_JTI_MAX_BYTES) {
      const { bytesRead } = await handle.read(buffer, filled, buffer.length - filled, null);
      if (bytesRead === 0) break;
      filled += bytesRead;
    }
    if (filled > DENIED_JTI_MAX_BYTES) {
      throw new Error(`denied-JTI list exceeds the ${DENIED_JTI_MAX_BYTES} byte limit`);
    }
    return buffer.subarray(0, filled).toString('utf8');
  } finally {
    await handle.close();
  }
}

/**
 * Parses the denied-JTI document. Two shapes are accepted, both bare JSON
 * arrays:
 *
 *   ["<jti>", "<jti>"]                          — plain list of token ids
 *   [{ "key": "<jti>", "exp": 1699999999 }]     — records keyed by `key`
 *
 * `null` counts as an empty list: a producer marshalling an empty list from a
 * nil slice (Go, notably) emits `null` rather than `[]`, and that must not be
 * read as a broken file.
 *
 * Entries of an unrecognised shape are skipped rather than rejected, so one odd
 * record cannot discard every other revocation in the file. A document that is
 * neither `null` nor an array IS rejected: that indicates the wrong file, not a
 * list with an odd entry.
 *
 * Skipped entries are counted and returned as `unrecognized`, because what the
 * caller does with this document depends on it: a document read in full
 * replaces the enforced list wholesale, while one with unrecognised entries
 * only has its readable entries unioned onto the list already being enforced.
 * An entry the parser could not read is evidence of nothing, so it can never
 * be read as a removal — additions from such a document always apply, and
 * removals wait for a document that parses cleanly. See
 * {@link refreshDeniedJtis} for what that costs and how it clears.
 *
 * Two alternatives were considered and not adopted:
 *
 *   - Treat every successful parse as wholesale authoritative, dropping
 *     unreadable entries from the enforced set and leaving the caller's
 *     warning to make that loud. It keeps "the document is authoritative" an
 *     unconditional rule, but it lets a record the producer merely reshaped
 *     act as an un-revocation: the token it names is admitted again for as
 *     long as the drift lasts, which under a schema rollout is measured in
 *     days rather than one reload. A fail-open on the revocation path is not
 *     worth the simpler rule.
 *   - Keep the last-good list on ANY unrecognised entry, rather than only when
 *     nothing in the document was recognised. This reads as more conservative
 *     but is not actually a distinct policy: the zero-recognized guard near
 *     the end of this function already keeps the last-good list whenever a
 *     document is entirely unreadable, so widening that guard to "any
 *     unrecognised entry" converges onto exactly the state machine of
 *     refusing the whole document outright. Differential testing across 225
 *     document-transition sequences (all pairs of 15 document shapes — clean,
 *     dirty, `[]`, `null`, wholly unrecognised, non-array, and combinations —
 *     replayed against four probe jtis) found zero observable divergence
 *     between the two. It also costs what the union does not, verified in the
 *     same run: a dirty file at cold start, with no last-good list to fall
 *     back to, denies NOTHING; and a single bad record anywhere in an
 *     otherwise-healthy file freezes every later addition — a newly revoked
 *     token cannot be enforced — until the document is fully clean again,
 *     which under a producer's schema drift (e.g. a field rename rolling out
 *     over days) is a multi-day window, not one bad reload.
 *
 * Both this trade-off and the deferral it accepts (an entry deleted from a
 * dirty document keeps being denied until that document parses cleanly) rest
 * on a standing assumption: the producer never reuses a `jti`. Revocation and
 * schema-drift recovery both reason about "the same token" over the life of a
 * jti value; a reused jti would let a later, unrelated token inherit an old
 * one's denied-or-admitted state — here by being denied for the rest of the
 * dirty window, which the next clean read undoes.
 *
 * `exp` is deliberately ignored. Entries are never aged out locally — the file
 * is the authority on what is revoked, and expiring an entry against our own
 * clock would silently un-deny a token whenever the producer's clock and ours
 * disagreed. Non-expiring tokens are a supported mode, so an entry may have no
 * meaningful expiry at all.
 */
function parseDeniedJtis(raw: string): DeniedJtiDocument {
  const document: unknown = JSON.parse(raw);
  if (document === null) return { jtis: new Set(), unrecognized: 0 };
  if (!Array.isArray(document)) {
    throw new Error('denied-JTI list must be a JSON array of token ids or {key} records');
  }
  const jtis = new Set<string>();
  let recognized = 0;
  let unrecognized = 0;
  for (const entry of document) {
    if (typeof entry === 'string') {
      if (entry.length > 0) {
        jtis.add(entry);
        recognized += 1;
      } else {
        unrecognized += 1;
      }
      continue;
    }
    if (typeof entry === 'object' && entry !== null && !Array.isArray(entry)) {
      const key = (entry as Record<string, unknown>)['key'];
      if (typeof key === 'string' && key.length > 0) {
        jtis.add(key);
        recognized += 1;
        continue;
      }
    }
    // Everything else — a number, `null`, a nested array, a record with no
    // usable `key` — is an entry we cannot turn into a jti.
    unrecognized += 1;
  }
  // A mixed list tolerates the odd malformed record, but a non-empty document
  // in which NOTHING was recognized is a wrong or corrupted file, not a
  // revocation decision. The caller's union would already hold the last-good
  // list here — an unreadable entry is never a removal — but it would do so
  // quietly, as an ordinary dirty reload. Throwing routes this to the
  // failed-reload path instead, which keeps the same list and raises the alarm
  // with the error attached. An explicitly empty array (or null) remains a
  // valid "revoke nothing" list.
  if (document.length > 0 && recognized === 0) {
    throw new Error(
      'denied-JTI list has no recognized entries; refusing to replace the last-good list',
    );
  }
  return { jtis, unrecognized };
}
