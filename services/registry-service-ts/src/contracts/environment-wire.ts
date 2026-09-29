// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { z } from 'zod';

/**
 * Edge-translation for the environment `config` field per
 * managed-agents-2026-04-01.
 *
 * Claude models the environment config as `{ type, networking, packages }`:
 * - `type` + `networking` project to Orca's stored `target` + `networking`.
 * - `packages` stores a canonical package-manager object in `packages_jsonb`.
 *
 * Legacy Orca flat fields are still accepted on input and emitted on output.
 */

export const Networking = z.discriminatedUnion('type', [
  z.object({ type: z.literal('unrestricted') }).passthrough(),
  z
    .object({
      type: z.literal('limited'),
      // Explicit `null` is wire-legal (BetaLimitedNetworkParams: "Fields default
      // to null; on update, omitted fields preserve the existing value.") and
      // means reset-to-default; `mergeNetworkingUpdate` strips null sub-fields
      // after merging so storage stays sparse and defaults apply on read.
      allow_package_managers: z.boolean().nullable().optional(),
      allow_mcp_servers: z.boolean().nullable().optional(),
      allowed_hosts: z.array(z.string()).nullable().optional(),
    })
    .passthrough(),
]);

export const packageManagers = ['apt', 'cargo', 'gem', 'go', 'npm', 'pip'] as const;
export type PackageManager = (typeof packageManagers)[number];
export type Packages = Partial<Record<PackageManager, string[]>>;
export type ApiPackages = Record<PackageManager, string[]> & { type: 'packages' };

const packageManagerShape = Object.fromEntries(
  packageManagers.map((manager) => [manager, z.array(z.string()).nullable().optional()]),
) as Record<PackageManager, z.ZodOptional<z.ZodNullable<z.ZodArray<z.ZodString>>>>;

export const PackagesSchema = z
  .object({
    ...packageManagerShape,
    type: z.literal('packages').optional(),
  })
  .strict()
  .transform(({ type: _type, ...packages }): Packages => {
    const normalized: Packages = {};
    for (const manager of packageManagers) {
      const items = packages[manager];
      if (items) normalized[manager] = items;
    }
    return stripEmptyPackages(normalized);
  });

const apiPackageManagerShape = Object.fromEntries(
  packageManagers.map((manager) => [manager, z.array(z.string())]),
) as Record<PackageManager, z.ZodArray<z.ZodString>>;

export const ApiPackagesSchema = z
  .object({
    ...apiPackageManagerShape,
    type: z.literal('packages'),
  })
  .strict();

export const ApiEnvConfigSchema = z.union([
  z
    .object({
      type: z.literal('cloud'),
      networking: Networking,
      packages: ApiPackagesSchema,
    })
    .strict(),
  z.object({ type: z.literal('self_hosted') }).strict(),
]);

export interface EnvConfig {
  packages?: Packages;
  type?: 'cloud' | 'self_hosted';
  networking?: z.infer<typeof Networking>;
}

export const EnvConfigSchema = z
  .object({
    packages: PackagesSchema.nullable().optional(),
    type: z.enum(['cloud', 'self_hosted']).optional(),
    networking: Networking.nullable().optional(),
  })
  .strict()
  .superRefine((config, ctx) => {
    if (config.networking !== undefined && config.type !== 'cloud') {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'config.networking is only supported for cloud configs',
      });
    }
    if (config.type === 'self_hosted' && config.packages !== undefined) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'config.packages is only supported for cloud configs',
      });
    }
  })
  .transform((config): EnvConfig => {
    const out: EnvConfig = {};
    if (config.packages && hasPackages(config.packages))
      out.packages = stripEmptyPackages(config.packages);
    if (config.type) out.type = config.type;
    if (config.type === 'cloud' && config.networking) {
      out.networking = config.networking;
    }
    return out;
  });

export interface EnvStorage {
  target: 'cloud' | 'self_hosted';
  networking: Record<string, unknown>;
}

/** True when `value` is a non-null, non-array object that owns `key`. */
export function hasOwn(value: unknown, key: string): boolean {
  return (
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    Object.prototype.hasOwnProperty.call(value, key)
  );
}

/** Drop keys whose value is explicit `null` so storage stays sparse and read-time defaults apply. */
export function stripNullFields(value: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, v] of Object.entries(value)) {
    if (v !== null) out[key] = v;
  }
  return out;
}

/**
 * Keep limited networking storage sparse and typed after merge-time legacy cleanup.
 * Unknown pass-through keys are preserved; known fields are retained only when
 * they match the current wire shape.
 */
function sanitizeLimitedNetworkingStorage(value: Record<string, unknown>): Record<string, unknown> {
  const {
    type: _type,
    allow_mcp_servers,
    allow_package_managers,
    allowed_hosts,
    ...rest
  } = stripNullFields(value);
  const out: Record<string, unknown> = { ...rest, type: 'limited' };
  if (typeof allow_mcp_servers === 'boolean') out.allow_mcp_servers = allow_mcp_servers;
  if (typeof allow_package_managers === 'boolean') {
    out.allow_package_managers = allow_package_managers;
  }
  if (Array.isArray(allowed_hosts)) {
    out.allowed_hosts = allowed_hosts.filter((host): host is string => typeof host === 'string');
  }
  return out;
}

/** Normalize nested `config.{type,networking}` into internal `{ target, networking }`. */
export function configToStorage(config: unknown): EnvStorage | null | { error: string } {
  if (config === null) {
    return { target: 'cloud', networking: { type: 'unrestricted' } };
  }
  if (typeof config !== 'object' || Array.isArray(config)) {
    return { error: 'config must be an object' };
  }
  const obj = config as Record<string, unknown>;
  const hasType = hasOwn(obj, 'type');
  const hasNetworking = hasOwn(obj, 'networking');
  if (!hasType && !hasNetworking) return null;
  if (!hasType) return { error: "config.type must be 'cloud' when config.networking is provided" };
  if (obj.type !== 'cloud' && obj.type !== 'self_hosted') {
    return { error: "config.type must be 'cloud' or 'self_hosted'" };
  }
  if (obj.type === 'self_hosted') {
    if (hasNetworking) {
      return { error: 'config.networking is only supported for cloud configs' };
    }
    return { target: 'self_hosted', networking: {} };
  }
  // Explicit `config.networking: null` is wire-legal and means reset-to-default
  // (BetaCloudConfigParams); omitted means "no networking provided" and also
  // defaults to unrestricted here — callers decide preserve-vs-default from
  // `hasNetworking`, not from this value.
  const networking =
    hasNetworking && obj.networking !== null ? obj.networking : { type: 'unrestricted' };
  const parsed = Networking.safeParse(networking);
  if (!parsed.success)
    return { error: 'config.networking must be {type:"unrestricted"} or {type:"limited", ...}' };
  return { target: obj.type, networking: parsed.data as Record<string, unknown> };
}

/**
 * Merge a `config.networking` update against the existing stored value.
 *
 * Per BetaLimitedNetworkParams: "Fields default to null; on update, omitted
 * fields preserve the existing value." That sub-field-level preservation only
 * makes sense when both the existing and the incoming networking are
 * `limited` — an incoming `unrestricted` fully replaces (it has no sub-fields
 * to omit), and a `limited` update over a non-limited (or untyped legacy)
 * existing value is used as-is, since there is nothing typed to inherit from.
 *
 * A sub-field explicit `null` in `provided` overrides the existing value
 * during the spread but must not be persisted as-is: it is stripped from the
 * result afterward so storage stays sparse and `networkingToApiConfig`
 * applies the read-time default (false / []).
 */
export function mergeNetworkingUpdate(
  existing: unknown,
  provided: Record<string, unknown>,
): Record<string, unknown> {
  if (provided.type !== 'limited') return stripNullFields(provided);
  const existingLimited =
    existing && typeof existing === 'object' && !Array.isArray(existing)
      ? (existing as Record<string, unknown>)
      : null;
  if (existingLimited?.type !== 'limited') return sanitizeLimitedNetworkingStorage(provided);
  return sanitizeLimitedNetworkingStorage({ ...existingLimited, ...provided });
}

/**
 * Project stored `target` + `networking` into the Claude `config` shape.
 * Only emitted when networking carries a typed discriminant; legacy untyped
 * networking blobs yield `null` so first-party callers keep the flat view.
 *
 * `limited` networking is sanitized against the response defaults from
 * BetaLimitedNetwork (`allow_mcp_servers`/`allow_package_managers`/
 * `allowed_hosts` are required, non-null in the response schema): each known
 * field is validated against its expected type and replaced with the default
 * when the stored value is missing or the wrong shape (e.g. a legacy
 * array-valued `allow_mcp_servers`), so invalid/legacy stored values can never
 * leak into the response.
 */
export function networkingToApiConfig(
  _target: string | null,
  networking: unknown,
): { type: 'cloud'; networking: unknown } | null {
  if (
    !networking ||
    typeof networking !== 'object' ||
    Array.isArray(networking) ||
    typeof (networking as Record<string, unknown>).type !== 'string'
  ) {
    return null;
  }
  const stored = networking as Record<string, unknown>;
  if (stored.type === 'limited') {
    const { allow_mcp_servers, allow_package_managers, allowed_hosts } = stored;
    return {
      type: 'cloud',
      networking: {
        type: 'limited',
        allow_mcp_servers: typeof allow_mcp_servers === 'boolean' ? allow_mcp_servers : false,
        allow_package_managers:
          typeof allow_package_managers === 'boolean' ? allow_package_managers : false,
        allowed_hosts: Array.isArray(allowed_hosts)
          ? allowed_hosts.filter((host): host is string => typeof host === 'string')
          : [],
      },
    };
  }
  if (stored.type === 'unrestricted') {
    return { type: 'cloud', networking: { type: 'unrestricted' } };
  }
  return null;
}

export function normalizePackages(value: unknown): Packages {
  return PackagesSchema.parse(value ?? {});
}

export function normalizeLegacyPackages(value: unknown): Packages {
  const packages = z.array(z.string()).parse(value ?? []);
  return packages.length > 0 ? { apt: packages } : {};
}

export function normalizeStoredPackages(value: unknown): Packages {
  return Array.isArray(value) ? normalizeLegacyPackages(value) : normalizePackages(value);
}

export function stripEmptyPackages(packages: Packages): Packages {
  const out: Packages = {};
  for (const manager of packageManagers) {
    const items = packages[manager];
    if (items && items.length > 0) out[manager] = items;
  }
  return out;
}

export function hasPackages(packages: Packages | undefined): packages is Packages {
  return !!packages && packageManagers.some((manager) => (packages[manager]?.length ?? 0) > 0);
}

export function legacyAptPackages(packages: Packages | undefined): string[] {
  return packages?.apt ?? [];
}

export function packagesToApi(packages: Packages | undefined): ApiPackages {
  const out = { type: 'packages' as const } as ApiPackages;
  for (const manager of packageManagers) out[manager] = packages?.[manager] ?? [];
  return out;
}
