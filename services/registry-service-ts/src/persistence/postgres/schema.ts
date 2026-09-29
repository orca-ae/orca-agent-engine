// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { sql } from 'drizzle-orm';
import { SEED_PRICE_PROVIDER } from '@orca/harness-catalog';
import {
  pgTable,
  text,
  integer,
  bigint,
  numeric,
  timestamp,
  jsonb,
  boolean,
  doublePrecision,
  primaryKey,
  index,
  uniqueIndex,
  foreignKey,
  check,
  type AnyPgColumn,
} from 'drizzle-orm/pg-core';

// === Organization administration ===
export const organizations = pgTable(
  'organizations',
  {
    id: text('id').primaryKey(),
    name: text('name').notNull(),
    /**
     * Optional OIDC audience that identifies this organization to the workspace
     * plane. Set at creation only, and unique across organizations when set, so
     * that an `aud` value resolves to at most one organization — see
     * `OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE` in `src/auth/oidc.ts`. Null means the
     * organization takes part in no audience-based resolution and, in that mode,
     * authenticates nothing.
     */
    audience: text('audience'),
    status: text('status').notNull().default('active'),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('organizations_name_idx').on(t.name),
    // Partial rather than plain UNIQUE so that the many organizations without a
    // configured audience do not collide with one another.
    uniqueIndex('organizations_audience_idx')
      .on(t.audience)
      .where(sql`${t.audience} is not null`),
    check('organizations_status_check', sql`${t.status} in ('active', 'archived')`),
  ],
);

export const platformApiKeys = pgTable(
  'platform_api_keys',
  {
    id: text('id').primaryKey(),
    name: text('name').notNull(),
    hashedKey: text('hashed_key').notNull(),
    keyFingerprint: text('key_fingerprint').notNull(),
    partialKeyHint: text('partial_key_hint').notNull(),
    scopes: text('scopes')
      .array()
      .notNull()
      .default([] as string[]),
    status: text('status').notNull().default('active'),
    expiresAt: timestamp('expires_at', { withTimezone: true }),
    lastUsedAt: timestamp('last_used_at', { withTimezone: true }),
    revokedAt: timestamp('revoked_at', { withTimezone: true }),
    createdBy: text('created_by').notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index('platform_api_keys_status_idx').on(t.status),
    uniqueIndex('platform_api_keys_fingerprint_idx').on(t.keyFingerprint),
    check('platform_api_keys_status_check', sql`${t.status} in ('active', 'inactive', 'archived')`),
  ],
);

export const workspaces = pgTable(
  'workspaces',
  {
    id: text('id').primaryKey(),
    organizationId: text('organization_id').notNull(),
    name: text('name').notNull(),
    status: text('status').notNull().default('active'),
    createdBy: text('created_by').notNull(),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('workspaces_organization_id_idx').on(t.organizationId, t.id),
    uniqueIndex('workspaces_organization_name_idx').on(t.organizationId, t.name),
    index('workspaces_organization_status_idx').on(t.organizationId, t.status, t.createdAt),
    foreignKey({
      name: 'workspaces_organization_fk',
      columns: [t.organizationId],
      foreignColumns: [organizations.id],
    }).onDelete('restrict'),
    check('workspaces_status_check', sql`${t.status} in ('active', 'archived')`),
  ],
);

// === Agent observability control plane ===
//
// These rows deliberately hold only routing/policy metadata. Credential bytes
// stay in SecretStore; `secret_ref` is an opaque pointer managed by the
// fenced credential lifecycle below.
export const agentObservabilityPlatformPolicy = pgTable(
  'agent_observability_platform_policy',
  {
    // A constrained singleton gives Session creation and future policy changes
    // one stable lock target without introducing a deployment-global fallback
    // binding.
    id: text('id').primaryKey().default('default'),
    allowedAdapters: text('allowed_adapters')
      .array()
      .notNull()
      .default(['otlp_http'] as string[]),
    allowedEndpointClasses: text('allowed_endpoint_classes')
      .array()
      .notNull()
      .default(['public'] as string[]),
    maxCaptureMode: text('max_capture_mode').notNull().default('metadata_only'),
    captureRestrictionEpoch: bigint('capture_restriction_epoch', { mode: 'number' })
      .notNull()
      .default(0),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    check('agent_observability_platform_policy_singleton_check', sql`${t.id} = 'default'`),
    check(
      'agent_observability_platform_policy_adapters_check',
      sql`cardinality(${t.allowedAdapters}) > 0
          and ${t.allowedAdapters} <@ ARRAY['otlp_http', 'langfuse_sdk']::text[]`,
    ),
    check(
      'agent_observability_platform_policy_endpoints_check',
      sql`cardinality(${t.allowedEndpointClasses}) > 0
          and ${t.allowedEndpointClasses} <@ ARRAY['public', 'private']::text[]`,
    ),
    check(
      'agent_observability_platform_policy_capture_check',
      sql`${t.maxCaptureMode} in ('metadata_only', 'redacted_io', 'raw_io')`,
    ),
    check(
      'agent_observability_platform_policy_capture_epoch_check',
      sql`${t.captureRestrictionEpoch} >= 0`,
    ),
  ],
);

export const agentObservabilityBindings = pgTable(
  'agent_observability_bindings',
  {
    id: text('id').primaryKey(),
    organizationId: text('organization_id').notNull(),
    // Null only for an organization-owned binding. Workspace ownership is
    // checked through the organization/workspace composite FK below.
    workspaceId: text('workspace_id'),
    scopeType: text('scope_type').notNull(),
    adapterType: text('adapter_type').notNull(),
    endpointKind: text('endpoint_kind').notNull(),
    endpointClass: text('endpoint_class').notNull(),
    endpoint: text('endpoint').notNull(),
    // Non-secret external target identity, such as a Langfuse/Litefuse public
    // project key. Generic OTLP targets may leave it null. Secret rotation must
    // never mutate this field or retarget Sessions pinned to this binding.
    externalProjectId: text('external_project_id'),
    currentVersion: integer('current_version').notNull().default(1),
    status: text('status').notNull().default('active'),
    revocationEpoch: bigint('revocation_epoch', { mode: 'number' }).notNull().default(0),
    createdBy: text('created_by').notNull(),
    updatedBy: text('updated_by').notNull(),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('agent_observability_bindings_organization_scope_idx').on(
      t.organizationId,
      t.id,
      t.scopeType,
    ),
    uniqueIndex('agent_observability_bindings_organization_workspace_idx').on(
      t.organizationId,
      t.workspaceId,
      t.id,
    ),
    uniqueIndex('agent_observability_bindings_id_adapter_idx').on(t.id, t.adapterType),
    index('agent_observability_bindings_selectable_idx').on(
      t.organizationId,
      t.workspaceId,
      t.status,
    ),
    foreignKey({
      name: 'agent_observability_bindings_organization_fk',
      columns: [t.organizationId],
      foreignColumns: [organizations.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'agent_observability_bindings_workspace_owner_fk',
      columns: [t.organizationId, t.workspaceId],
      foreignColumns: [workspaces.organizationId, workspaces.id],
    }).onDelete('restrict'),
    // Migration 0048 marks this composite FK DEFERRABLE INITIALLY DEFERRED so
    // one transaction can insert the fixed binding, its first immutable
    // version, and the current-version pointer without a dangling committed
    // head. Drizzle models the relationship but not the deferrability flag.
    foreignKey({
      name: 'agent_observability_bindings_current_version_fk',
      columns: [t.id, t.currentVersion],
      foreignColumns: agentObservabilityBindingVersionColumns(),
    }).onDelete('restrict'),
    check(
      'agent_observability_bindings_scope_owner_check',
      sql`(
        ${t.scopeType} = 'organization'
        and ${t.workspaceId} is null
      ) or (
        ${t.scopeType} = 'workspace'
        and ${t.workspaceId} is not null
      )`,
    ),
    check(
      'agent_observability_bindings_adapter_check',
      sql`${t.adapterType} in ('otlp_http', 'langfuse_sdk')`,
    ),
    check(
      'agent_observability_bindings_endpoint_check',
      sql`${t.endpointClass} in ('public', 'private')
          and char_length(btrim(${t.endpoint})) > 0
          and (
            (${t.adapterType} = 'otlp_http' and ${t.endpointKind} in ('traces_endpoint', 'base_endpoint')
             and (${t.externalProjectId} is null or char_length(btrim(${t.externalProjectId})) > 0))
            or
            (${t.adapterType} = 'langfuse_sdk' and ${t.endpointKind} = 'base_endpoint'
             and ${t.externalProjectId} is not null and char_length(btrim(${t.externalProjectId})) > 0)
          )`,
    ),
    check(
      'agent_observability_bindings_status_check',
      sql`${t.status} in ('active', 'draining', 'disabled', 'archived')`,
    ),
    check(
      'agent_observability_bindings_archive_check',
      sql`(${t.status} = 'archived') = (${t.archivedAt} is not null)`,
    ),
    check(
      'agent_observability_bindings_version_epoch_check',
      sql`${t.currentVersion} > 0 and ${t.revocationEpoch} >= 0`,
    ),
  ],
);

export const agentObservabilityBindingVersions = pgTable(
  'agent_observability_binding_versions',
  {
    bindingId: text('binding_id').notNull(),
    version: integer('version').notNull(),
    // Duplicated from the immutable target so the database can reject semantic
    // profile/protocol combinations that do not belong to that adapter.
    adapterType: text('adapter_type').notNull(),
    semanticProfile: text('semantic_profile').notNull(),
    protocol: text('protocol').notNull(),
    compression: text('compression').notNull().default('none'),
    timeoutMs: integer('timeout_ms').notNull().default(10_000),
    environment: text('environment'),
    release: text('release'),
    captureMode: text('capture_mode').notNull().default('metadata_only'),
    sampleRate: numeric('sample_rate', { precision: 5, scale: 4 }).notNull().default('1'),
    configSchemaVersion: integer('config_schema_version').notNull().default(1),
    createdBy: text('created_by').notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({
      name: 'agent_observability_binding_versions_pk',
      columns: [t.bindingId, t.version],
    }),
    foreignKey({
      name: 'agent_observability_binding_versions_binding_fk',
      columns: [t.bindingId],
      foreignColumns: agentObservabilityBindingIdColumns(),
    }).onDelete('restrict'),
    foreignKey({
      name: 'agent_observability_binding_versions_adapter_fk',
      columns: [t.bindingId, t.adapterType],
      foreignColumns: agentObservabilityBindingAdapterColumns(),
    }).onDelete('restrict'),
    check(
      'agent_observability_binding_versions_adapter_config_check',
      sql`(
        ${t.adapterType} = 'otlp_http'
        and ${t.semanticProfile} in ('otel_genai', 'langfuse')
        and ${t.protocol} in ('http/protobuf', 'http/json')
      ) or (
        ${t.adapterType} = 'langfuse_sdk'
        and ${t.semanticProfile} = 'langfuse'
        and ${t.protocol} = 'sdk'
      )`,
    ),
    check(
      'agent_observability_binding_versions_compression_check',
      sql`${t.compression} in ('none', 'gzip')`,
    ),
    check(
      'agent_observability_binding_versions_capture_check',
      sql`${t.captureMode} in ('metadata_only', 'redacted_io', 'raw_io')`,
    ),
    check(
      'agent_observability_binding_versions_bounds_check',
      sql`${t.version} > 0
          and ${t.timeoutMs} > 0
          and ${t.sampleRate} >= 0 and ${t.sampleRate} <= 1
          and ${t.configSchemaVersion} > 0`,
    ),
  ],
);

// These typed indirections break TypeScript's declaration-inference cycle
// while preserving the deferred mutual FKs between a binding and its immutable
// current-version row. Drizzle evaluates the extra-config callbacks after both
// table constants are initialized.
function agentObservabilityBindingVersionColumns(): [AnyPgColumn, AnyPgColumn] {
  const table = agentObservabilityBindingVersions as unknown as {
    bindingId: AnyPgColumn;
    version: AnyPgColumn;
  };
  return [table.bindingId, table.version];
}

function agentObservabilityBindingIdColumns(): [AnyPgColumn] {
  const table = agentObservabilityBindings as unknown as { id: AnyPgColumn };
  return [table.id];
}

function agentObservabilityBindingAdapterColumns(): [AnyPgColumn, AnyPgColumn] {
  const table = agentObservabilityBindings as unknown as {
    id: AnyPgColumn;
    adapterType: AnyPgColumn;
  };
  return [table.id, table.adapterType];
}

export const agentObservabilityBindingCredentials = pgTable(
  'agent_observability_binding_credentials',
  {
    bindingId: text('binding_id').primaryKey(),
    // Opaque SecretStore pointer only. This table never stores a key, token,
    // password, header value, or serialized credential bundle.
    secretRef: text('secret_ref').notNull(),
    credentialVersion: integer('credential_version').notNull().default(1),
    keyHint: text('key_hint'),
    rotatedAt: timestamp('rotated_at', { withTimezone: true }).notNull().defaultNow(),
    updatedBy: text('updated_by').notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    foreignKey({
      name: 'agent_observability_binding_credentials_binding_fk',
      columns: [t.bindingId],
      foreignColumns: [agentObservabilityBindings.id],
    }).onDelete('restrict'),
    check(
      'agent_observability_binding_credentials_ref_check',
      sql`char_length(btrim(${t.secretRef})) > 0 and ${t.credentialVersion} > 0`,
    ),
  ],
);

export const agentObservabilityOrganizationSettings = pgTable(
  'agent_observability_organization_settings',
  {
    organizationId: text('organization_id').primaryKey(),
    activeDefaultBindingId: text('active_default_binding_id'),
    // A nullable discriminator makes the FK reject a workspace binding as an
    // organization default without relying on a partial-index FK.
    activeDefaultBindingScope: text('active_default_binding_scope'),
    selectionEpoch: bigint('selection_epoch', { mode: 'number' }).notNull().default(0),
    defaultRevocationEpoch: bigint('default_revocation_epoch', { mode: 'number' })
      .notNull()
      .default(0),
    organizationRevocationEpoch: bigint('organization_revocation_epoch', { mode: 'number' })
      .notNull()
      .default(0),
    captureCeiling: text('capture_ceiling').notNull().default('metadata_only'),
    captureRestrictionEpoch: bigint('capture_restriction_epoch', { mode: 'number' })
      .notNull()
      .default(0),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    foreignKey({
      name: 'agent_observability_organization_settings_organization_fk',
      columns: [t.organizationId],
      foreignColumns: [organizations.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'agent_observability_organization_settings_binding_fk',
      columns: [t.organizationId, t.activeDefaultBindingId, t.activeDefaultBindingScope],
      foreignColumns: [
        agentObservabilityBindings.organizationId,
        agentObservabilityBindings.id,
        agentObservabilityBindings.scopeType,
      ],
    }).onDelete('restrict'),
    check(
      'agent_observability_organization_settings_default_binding_check',
      sql`(
        ${t.activeDefaultBindingId} is null
        and ${t.activeDefaultBindingScope} is null
      ) or (
        ${t.activeDefaultBindingId} is not null
        and ${t.activeDefaultBindingScope} is not null
        and ${t.activeDefaultBindingScope} = 'organization'
      )`,
    ),
    check(
      'agent_observability_organization_settings_capture_check',
      sql`${t.captureCeiling} in ('metadata_only', 'redacted_io', 'raw_io')`,
    ),
    check(
      'agent_observability_organization_settings_epoch_check',
      sql`${t.selectionEpoch} >= 0
          and ${t.defaultRevocationEpoch} >= 0
          and ${t.organizationRevocationEpoch} >= 0
          and ${t.captureRestrictionEpoch} >= 0`,
    ),
  ],
);

export const agentObservabilityWorkspaceSettings = pgTable(
  'agent_observability_workspace_settings',
  {
    workspaceId: text('workspace_id').primaryKey(),
    organizationId: text('organization_id').notNull(),
    mode: text('mode').notNull().default('inherit'),
    bindingId: text('binding_id'),
    selectionEpoch: bigint('selection_epoch', { mode: 'number' }).notNull().default(0),
    revocationEpoch: bigint('revocation_epoch', { mode: 'number' }).notNull().default(0),
    captureCeiling: text('capture_ceiling').notNull().default('metadata_only'),
    captureRestrictionEpoch: bigint('capture_restriction_epoch', { mode: 'number' })
      .notNull()
      .default(0),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('agent_observability_workspace_settings_organization_id_idx').on(
      t.organizationId,
      t.workspaceId,
    ),
    foreignKey({
      name: 'agent_observability_workspace_settings_workspace_fk',
      columns: [t.organizationId, t.workspaceId],
      foreignColumns: [workspaces.organizationId, workspaces.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'agent_observability_workspace_settings_binding_fk',
      columns: [t.organizationId, t.workspaceId, t.bindingId],
      foreignColumns: [
        agentObservabilityBindings.organizationId,
        agentObservabilityBindings.workspaceId,
        agentObservabilityBindings.id,
      ],
    }).onDelete('restrict'),
    check(
      'agent_observability_workspace_settings_mode_binding_check',
      sql`(
        ${t.mode} = 'custom' and ${t.bindingId} is not null
      ) or (
        ${t.mode} in ('inherit', 'disabled') and ${t.bindingId} is null
      )`,
    ),
    check(
      'agent_observability_workspace_settings_capture_check',
      sql`${t.captureCeiling} in ('metadata_only', 'redacted_io', 'raw_io')`,
    ),
    check(
      'agent_observability_workspace_settings_epoch_check',
      sql`${t.selectionEpoch} >= 0
          and ${t.revocationEpoch} >= 0
          and ${t.captureRestrictionEpoch} >= 0`,
    ),
  ],
);

// Permanent exactly-once fence for Workspace archive revocation. The temporary
// mixed-version trigger writes the same marker as current application writers,
// so it remains the archive authority after that trigger is retired.
export const agentObservabilityWorkspaceArchiveRevocations = pgTable(
  'agent_observability_workspace_archive_revocations',
  {
    workspaceId: text('workspace_id').primaryKey(),
    organizationId: text('organization_id').notNull(),
    archivedAt: timestamp('archived_at', { withTimezone: true }).notNull(),
    revocationEpoch: bigint('revocation_epoch', { mode: 'number' }).notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    foreignKey({
      name: 'agent_observability_workspace_archive_revocations_workspace_fk',
      columns: [t.organizationId, t.workspaceId],
      foreignColumns: [workspaces.organizationId, workspaces.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'agent_observability_workspace_archive_revocations_setting_fk',
      columns: [t.organizationId, t.workspaceId],
      foreignColumns: [
        agentObservabilityWorkspaceSettings.organizationId,
        agentObservabilityWorkspaceSettings.workspaceId,
      ],
    }).onDelete('restrict'),
    check(
      'agent_observability_workspace_archive_revocations_epoch_check',
      sql`${t.revocationEpoch} > 0 and ${t.revocationEpoch} <= 9007199254740991`,
    ),
  ],
);

// Mutation attempts are append-only records. Callers lock the stable authority
// row (organization setting, workspace setting, or binding) before creating an
// attempt; this table provides the durable fence, generation, and cleanup
// hand-off without ever storing secret material.
export const agentObservabilityMutationReservations = pgTable(
  'agent_observability_mutation_reservations',
  {
    id: text('id').primaryKey(),
    organizationId: text('organization_id').notNull(),
    workspaceId: text('workspace_id'),
    targetType: text('target_type').notNull(),
    // A non-null canonical key avoids PostgreSQL's NULL-distinct unique-index
    // behavior for organization-setting and organization-binding targets.
    targetKey: text('target_key').notNull(),
    targetBindingId: text('target_binding_id'),
    targetBindingScope: text('target_binding_scope'),
    ownerPrincipal: text('owner_principal').notNull(),
    bodyHash: text('body_hash').notNull(),
    // `state_version` is an opaque, non-secret state/ETag version. The future
    // route owns its interpretation and uses the values below in its CAS.
    expectedStateVersion: text('expected_state_version').notNull(),
    expectedConfigVersion: integer('expected_config_version'),
    expectedCredentialVersion: integer('expected_credential_version'),
    generation: bigint('generation', { mode: 'number' }).notNull(),
    status: text('status').notNull().default('pending'),
    expiresAt: timestamp('expires_at', { withTimezone: true }).notNull(),
    fencedAt: timestamp('fenced_at', { withTimezone: true }),
    expiredAt: timestamp('expired_at', { withTimezone: true }),
    committedAt: timestamp('committed_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('agent_observability_mutation_reservations_organization_id_idx').on(
      t.organizationId,
      t.id,
    ),
    uniqueIndex('agent_observability_mutation_reservations_id_generation_idx').on(
      t.id,
      t.generation,
    ),
    uniqueIndex('agent_observability_mutation_reservations_live_target_idx')
      .on(t.targetKey)
      .where(sql`${t.status} = 'pending'`),
    index('agent_observability_mutation_reservations_expiry_idx').on(t.status, t.expiresAt),
    index('agent_observability_mutation_reservations_target_generation_idx').on(
      t.targetKey,
      t.generation,
    ),
    foreignKey({
      name: 'agent_observability_mutation_reservations_organization_setting_fk',
      columns: [t.organizationId],
      foreignColumns: [agentObservabilityOrganizationSettings.organizationId],
    }).onDelete('restrict'),
    foreignKey({
      name: 'agent_observability_mutation_reservations_workspace_setting_fk',
      columns: [t.organizationId, t.workspaceId],
      foreignColumns: [
        agentObservabilityWorkspaceSettings.organizationId,
        agentObservabilityWorkspaceSettings.workspaceId,
      ],
    }).onDelete('restrict'),
    foreignKey({
      name: 'agent_observability_mutation_reservations_binding_owner_fk',
      columns: [t.organizationId, t.targetBindingId, t.targetBindingScope],
      foreignColumns: [
        agentObservabilityBindings.organizationId,
        agentObservabilityBindings.id,
        agentObservabilityBindings.scopeType,
      ],
    }).onDelete('restrict'),
    foreignKey({
      name: 'agent_observability_mutation_reservations_binding_workspace_fk',
      columns: [t.organizationId, t.workspaceId, t.targetBindingId],
      foreignColumns: [
        agentObservabilityBindings.organizationId,
        agentObservabilityBindings.workspaceId,
        agentObservabilityBindings.id,
      ],
    }).onDelete('restrict'),
    check(
      'agent_observability_mutation_reservations_target_check',
      sql`(
        ${t.targetType} = 'organization_setting'
        and ${t.workspaceId} is null
        and ${t.targetBindingId} is null
        and ${t.targetBindingScope} is null
      ) or (
        ${t.targetType} = 'workspace_setting'
        and ${t.workspaceId} is not null
        and ${t.targetBindingId} is null
        and ${t.targetBindingScope} is null
      ) or (
        ${t.targetType} = 'binding'
        and ${t.targetBindingId} is not null
        and (
          (${t.targetBindingScope} = 'organization' and ${t.workspaceId} is null)
          or (${t.targetBindingScope} = 'workspace' and ${t.workspaceId} is not null)
        )
      )`,
    ),
    check(
      'agent_observability_mutation_reservations_versions_check',
      sql`char_length(btrim(${t.targetKey})) > 0
          and char_length(btrim(${t.ownerPrincipal})) > 0
          and ${t.bodyHash} ~ '^[0-9a-f]{64}$'
          and char_length(btrim(${t.expectedStateVersion})) > 0
          and (${t.expectedConfigVersion} is null or ${t.expectedConfigVersion} > 0)
          and (${t.expectedCredentialVersion} is null or ${t.expectedCredentialVersion} > 0)
          and ${t.generation} > 0
          and ${t.expiresAt} > ${t.createdAt}`,
    ),
    check(
      'agent_observability_mutation_reservations_status_check',
      sql`(
        ${t.status} = 'pending'
        and ${t.fencedAt} is null
        and ${t.expiredAt} is null
        and ${t.committedAt} is null
      ) or (
        ${t.status} = 'fenced'
        and ${t.fencedAt} is not null
        and ${t.expiredAt} is null
        and ${t.committedAt} is null
      ) or (
        ${t.status} = 'expired'
        and ${t.fencedAt} is null
        and ${t.expiredAt} is not null
        and ${t.committedAt} is null
      ) or (
        ${t.status} = 'committed'
        and ${t.fencedAt} is null
        and ${t.expiredAt} is null
        and ${t.committedAt} is not null
      )`,
    ),
  ],
);

// A credential bundle is written to SecretStore only after this intent has
// committed. Writer and cleanup claims are durable, independently fenced
// leases: a late SecretStore.put must never revive a cleanup tombstone.
// Candidate binding IDs deliberately have no FK: target replacement creates
// that binding in the final fenced transaction.
export const agentObservabilityCredentialStagingIntents = pgTable(
  'agent_observability_credential_staging_intents',
  {
    reservationId: text('reservation_id').primaryKey(),
    generation: bigint('generation', { mode: 'number' }).notNull(),
    // Required even for an existing-binding rotation. It lets finalization
    // distinguish that rotation from a target-identity replacement without
    // adding an FK that would reject an as-yet-unpublished candidate.
    candidateBindingId: text('candidate_binding_id').notNull(),
    // Identity of the credential head that finalization must prove after the
    // caller-owned DB mutation. This is metadata only; bundle bytes remain in
    // SecretStore under secretRef.
    proposedCredentialVersion: integer('proposed_credential_version').notNull(),
    secretRef: text('secret_ref').notNull(),
    status: text('status').notNull().default('pending'),
    writerToken: text('writer_token'),
    writerLeaseExpiresAt: timestamp('writer_lease_expires_at', { withTimezone: true }),
    putCompletedAt: timestamp('put_completed_at', { withTimezone: true }),
    cleanupToken: text('cleanup_token'),
    cleanupLeaseExpiresAt: timestamp('cleanup_lease_expires_at', { withTimezone: true }),
    // First cleanup starts after reservation expiry/grace. Terminal tombstones
    // are never pruned: an unbounded late SecretStore.put remains possible.
    nextCleanupAt: timestamp('next_cleanup_at', { withTimezone: true }).notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('agent_observability_credential_staging_intents_secret_ref_idx').on(t.secretRef),
    index('agent_observability_credential_staging_intents_cleanup_idx').on(
      t.status,
      t.nextCleanupAt,
      t.writerLeaseExpiresAt,
      t.cleanupLeaseExpiresAt,
    ),
    foreignKey({
      name: 'agent_observability_credential_staging_intents_reservation_fk',
      columns: [t.reservationId, t.generation],
      foreignColumns: [
        agentObservabilityMutationReservations.id,
        agentObservabilityMutationReservations.generation,
      ],
    }).onDelete('restrict'),
    check(
      'agent_observability_credential_staging_intents_status_check',
      sql`(
        ${t.status} = 'pending'
        and ${t.writerToken} is null
        and ${t.writerLeaseExpiresAt} is null
        and ${t.putCompletedAt} is null
        and ${t.cleanupToken} is null
        and ${t.cleanupLeaseExpiresAt} is null
      ) or (
        ${t.status} = 'writing'
        and char_length(btrim(${t.writerToken})) > 0
        and ${t.writerLeaseExpiresAt} is not null
        and ${t.putCompletedAt} is null
        and ${t.cleanupToken} is null
        and ${t.cleanupLeaseExpiresAt} is null
      ) or (
        ${t.status} = 'written'
        and ${t.writerToken} is null
        and ${t.writerLeaseExpiresAt} is null
        and ${t.putCompletedAt} is not null
        and ${t.cleanupToken} is null
        and ${t.cleanupLeaseExpiresAt} is null
      ) or (
        ${t.status} = 'cleanup_pending'
        and ${t.writerToken} is null
        and ${t.writerLeaseExpiresAt} is null
        and ${t.cleanupToken} is null
        and ${t.cleanupLeaseExpiresAt} is null
      ) or (
        ${t.status} = 'cleaning'
        and ${t.writerToken} is null
        and ${t.writerLeaseExpiresAt} is null
        and char_length(btrim(${t.cleanupToken})) > 0
        and ${t.cleanupLeaseExpiresAt} is not null
      )`,
    ),
    check(
      'agent_observability_credential_staging_intents_values_check',
      sql`${t.generation} > 0
          and char_length(btrim(${t.candidateBindingId})) > 0
          and ${t.proposedCredentialVersion} > 0
          and char_length(btrim(${t.secretRef})) > 0
          and ${t.nextCleanupAt} >= ${t.createdAt}`,
    ),
  ],
);

// Superseded credential references outlive the final state transaction. Claim
// ownership is durable so SecretStore.delete always happens outside a DB tx.
export const agentObservabilitySecretCleanupOutbox = pgTable(
  'agent_observability_secret_cleanup_outbox',
  {
    id: text('id').primaryKey(),
    organizationId: text('organization_id').notNull(),
    bindingId: text('binding_id').notNull(),
    bindingScope: text('binding_scope').notNull(),
    secretRef: text('secret_ref').notNull(),
    status: text('status').notNull().default('pending'),
    claimToken: text('claim_token'),
    leaseExpiresAt: timestamp('lease_expires_at', { withTimezone: true }),
    attemptCount: integer('attempt_count').notNull().default(0),
    nextAttemptAt: timestamp('next_attempt_at', { withTimezone: true }).notNull().defaultNow(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('agent_observability_secret_cleanup_outbox_binding_ref_idx').on(
      t.bindingId,
      t.secretRef,
    ),
    index('agent_observability_secret_cleanup_outbox_claim_idx').on(
      t.status,
      t.nextAttemptAt,
      t.leaseExpiresAt,
    ),
    foreignKey({
      name: 'agent_observability_secret_cleanup_outbox_binding_owner_fk',
      columns: [t.organizationId, t.bindingId, t.bindingScope],
      foreignColumns: [
        agentObservabilityBindings.organizationId,
        agentObservabilityBindings.id,
        agentObservabilityBindings.scopeType,
      ],
    }).onDelete('restrict'),
    check(
      'agent_observability_secret_cleanup_outbox_status_check',
      sql`(
        ${t.status} = 'pending'
        and ${t.claimToken} is null
        and ${t.leaseExpiresAt} is null
      ) or (
        ${t.status} = 'deleting'
        and char_length(btrim(${t.claimToken})) > 0
        and ${t.leaseExpiresAt} is not null
      )`,
    ),
    check(
      'agent_observability_secret_cleanup_outbox_values_check',
      sql`${t.bindingScope} in ('organization', 'workspace')
          and char_length(btrim(${t.secretRef})) > 0
          and ${t.attemptCount} >= 0`,
    ),
  ],
);

// This idempotency namespace is deliberately independent from public workspace
// writes: organization-admin identity and a mutation reservation are both part
// of its durable partition.
export const agentObservabilityIdempotencyKeys = pgTable(
  'agent_observability_idempotency_keys',
  {
    organizationId: text('organization_id').notNull(),
    principal: text('principal').notNull(),
    scope: text('scope').notNull(),
    key: text('key').notNull(),
    // This is deliberately duplicated from the reservation. A reused key can
    // never silently migrate from one authority target to another.
    targetKey: text('target_key').notNull(),
    bodyHash: text('body_hash').notNull(),
    status: text('status').notNull().default('pending'),
    reservationId: text('reservation_id').notNull(),
    responseStatus: integer('response_status'),
    responseBody: jsonb('response_body'),
    completedAt: timestamp('completed_at', { withTimezone: true }),
    expiresAt: timestamp('expires_at', { withTimezone: true }).notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({
      name: 'agent_observability_idempotency_keys_pk',
      columns: [t.organizationId, t.principal, t.scope, t.key],
    }),
    uniqueIndex('agent_observability_idempotency_keys_reservation_idx').on(t.reservationId),
    index('agent_observability_idempotency_keys_expiry_idx').on(t.status, t.expiresAt),
    foreignKey({
      name: 'agent_observability_idempotency_keys_reservation_fk',
      columns: [t.organizationId, t.reservationId],
      foreignColumns: [
        agentObservabilityMutationReservations.organizationId,
        agentObservabilityMutationReservations.id,
      ],
    }).onDelete('restrict'),
    check(
      'agent_observability_idempotency_keys_values_check',
      sql`char_length(btrim(${t.principal})) > 0
          and char_length(btrim(${t.scope})) > 0
          and char_length(btrim(${t.key})) > 0
          and char_length(btrim(${t.targetKey})) > 0
          and ${t.bodyHash} ~ '^[0-9a-f]{64}$'`,
    ),
    check(
      'agent_observability_idempotency_keys_status_check',
      sql`(
        ${t.status} = 'pending'
        and ${t.responseStatus} is null
        and ${t.responseBody} is null
        and ${t.completedAt} is null
        and ${t.expiresAt} > ${t.createdAt}
      ) or (
        ${t.status} = 'completed'
        and ${t.responseStatus} between 200 and 599
        and ${t.responseBody} is not null
        and ${t.completedAt} is not null
        and ${t.expiresAt} > ${t.createdAt}
      )`,
    ),
  ],
);

// Session pin/tombstone records intentionally have no Session FK. A hard
// Session delete must retain this non-secret authorization state so a later
// exporter can suppress already-received work instead of re-resolving it.
export const sessionObservabilityBindings = pgTable(
  'session_observability_bindings',
  {
    workspaceId: text('workspace_id').notNull(),
    sessionId: text('session_id').notNull(),
    organizationId: text('organization_id').notNull(),
    bindingId: text('binding_id'),
    bindingVersion: integer('binding_version'),
    bindingScope: text('binding_scope'),
    // Filled only for a workspace-custom binding. It lets a composite FK prove
    // the pinned workspace owns that binding; organization defaults keep null.
    bindingWorkspaceId: text('binding_workspace_id'),
    selectionSource: text('selection_source').notNull().default('disabled'),
    status: text('status').notNull().default('disabled'),
    organizationSelectionEpoch: bigint('organization_selection_epoch', { mode: 'number' })
      .notNull()
      .default(0),
    workspaceSelectionEpoch: bigint('workspace_selection_epoch', { mode: 'number' })
      .notNull()
      .default(0),
    organizationDefaultRevocationEpoch: bigint('organization_default_revocation_epoch', {
      mode: 'number',
    })
      .notNull()
      .default(0),
    organizationRevocationEpoch: bigint('organization_revocation_epoch', { mode: 'number' })
      .notNull()
      .default(0),
    workspaceRevocationEpoch: bigint('workspace_revocation_epoch', { mode: 'number' })
      .notNull()
      .default(0),
    bindingRevocationEpoch: bigint('binding_revocation_epoch', { mode: 'number' })
      .notNull()
      .default(0),
    platformCaptureRestrictionEpoch: bigint('platform_capture_restriction_epoch', {
      mode: 'number',
    })
      .notNull()
      .default(0),
    organizationCaptureRestrictionEpoch: bigint('organization_capture_restriction_epoch', {
      mode: 'number',
    })
      .notNull()
      .default(0),
    workspaceCaptureRestrictionEpoch: bigint('workspace_capture_restriction_epoch', {
      mode: 'number',
    })
      .notNull()
      .default(0),
    effectiveCaptureMode: text('effective_capture_mode').notNull().default('metadata_only'),
    sessionRevocationEpoch: bigint('session_revocation_epoch', { mode: 'number' })
      .notNull()
      .default(0),
    agentId: text('agent_id').notNull(),
    agentVersion: integer('agent_version').notNull(),
    harness: text('harness'),
    harnessMode: text('harness_mode'),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({
      name: 'session_observability_bindings_workspace_session_pk',
      columns: [t.workspaceId, t.sessionId],
    }),
    index('session_observability_bindings_binding_idx').on(t.bindingId, t.bindingVersion),
    index('session_observability_bindings_organization_idx').on(t.organizationId, t.status),
    foreignKey({
      name: 'session_observability_bindings_workspace_fk',
      columns: [t.organizationId, t.workspaceId],
      foreignColumns: [workspaces.organizationId, workspaces.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'session_observability_bindings_binding_scope_fk',
      columns: [t.organizationId, t.bindingId, t.bindingScope],
      foreignColumns: [
        agentObservabilityBindings.organizationId,
        agentObservabilityBindings.id,
        agentObservabilityBindings.scopeType,
      ],
    }).onDelete('restrict'),
    foreignKey({
      name: 'session_observability_bindings_binding_workspace_fk',
      columns: [t.organizationId, t.bindingId, t.bindingWorkspaceId],
      foreignColumns: [
        agentObservabilityBindings.organizationId,
        agentObservabilityBindings.id,
        agentObservabilityBindings.workspaceId,
      ],
    }).onDelete('restrict'),
    foreignKey({
      name: 'session_observability_bindings_binding_version_fk',
      columns: [t.bindingId, t.bindingVersion],
      foreignColumns: [
        agentObservabilityBindingVersions.bindingId,
        agentObservabilityBindingVersions.version,
      ],
    }).onDelete('restrict'),
    check(
      'session_observability_bindings_selection_check',
      sql`(
        ${t.selectionSource} = 'disabled'
        and ${t.status} in ('disabled', 'archived', 'deleted')
        and ${t.bindingId} is null
        and ${t.bindingVersion} is null
        and ${t.bindingScope} is null
        and ${t.bindingWorkspaceId} is null
        and ${t.effectiveCaptureMode} = 'metadata_only'
      ) or (
        ${t.selectionSource} = 'organization_default'
        and ${t.status} in ('active', 'archived', 'deleted')
        and ${t.bindingId} is not null
        and ${t.bindingVersion} is not null and ${t.bindingVersion} > 0
        and ${t.bindingScope} is not null
        and ${t.bindingScope} = 'organization'
        and ${t.bindingWorkspaceId} is null
      ) or (
        ${t.selectionSource} = 'workspace_custom'
        and ${t.status} in ('active', 'archived', 'deleted')
        and ${t.bindingId} is not null
        and ${t.bindingVersion} is not null and ${t.bindingVersion} > 0
        and ${t.bindingScope} is not null
        and ${t.bindingScope} = 'workspace'
        and ${t.bindingWorkspaceId} is not null
        and ${t.bindingWorkspaceId} = ${t.workspaceId}
      )`,
    ),
    check(
      'session_observability_bindings_status_check',
      sql`${t.status} in ('active', 'disabled', 'archived', 'deleted')`,
    ),
    check(
      'session_observability_bindings_capture_check',
      sql`${t.effectiveCaptureMode} in ('metadata_only', 'redacted_io', 'raw_io')`,
    ),
    check(
      'session_observability_bindings_lifecycle_check',
      sql`(
        ${t.status} = 'archived'
        and ${t.archivedAt} is not null
        and ${t.deletedAt} is null
      ) or (
        ${t.status} = 'deleted'
        and ${t.deletedAt} is not null
      ) or (
        ${t.status} in ('active', 'disabled')
        and ${t.archivedAt} is null
        and ${t.deletedAt} is null
      )`,
    ),
    check(
      'session_observability_bindings_epoch_check',
      sql`${t.organizationSelectionEpoch} >= 0
          and ${t.workspaceSelectionEpoch} >= 0
          and ${t.organizationDefaultRevocationEpoch} >= 0
          and ${t.organizationRevocationEpoch} >= 0
          and ${t.workspaceRevocationEpoch} >= 0
          and ${t.bindingRevocationEpoch} >= 0
          and ${t.platformCaptureRestrictionEpoch} >= 0
          and ${t.organizationCaptureRestrictionEpoch} >= 0
          and ${t.workspaceCaptureRestrictionEpoch} >= 0
          and ${t.sessionRevocationEpoch} >= 0
          and ${t.agentVersion} > 0`,
    ),
  ],
);

export const platformAuditEvents = pgTable(
  'platform_audit_events',
  {
    id: text('id').primaryKey(),
    organizationId: text('organization_id'),
    workspaceId: text('workspace_id'),
    actor: text('actor').notNull(),
    authMethod: text('auth_method').notNull(),
    action: text('action').notNull(),
    targetType: text('target_type').notNull(),
    targetId: text('target_id').notNull(),
    requestId: text('request_id').notNull(),
    result: text('result').notNull(),
    metadata: jsonb('metadata').notNull().default({}),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index('platform_audit_events_organization_idx').on(t.organizationId, t.createdAt),
    index('platform_audit_events_workspace_idx').on(t.workspaceId, t.createdAt),
    foreignKey({
      name: 'platform_audit_events_organization_fk',
      columns: [t.organizationId],
      foreignColumns: [organizations.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'platform_audit_events_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
  ],
);

export const platformIdempotencyKeys = pgTable(
  'platform_idempotency_keys',
  {
    principal: text('principal').notNull(),
    scope: text('scope').notNull(),
    key: text('key').notNull(),
    responseStatus: integer('response_status').notNull(),
    responseBody: text('response_body').notNull(),
    bodyHash: text('body_hash').notNull(),
    expiresAt: timestamp('expires_at', { withTimezone: true }).notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.principal, t.scope, t.key] }),
    index('platform_idempotency_keys_expires_idx').on(t.expiresAt),
  ],
);

export const adminApiKeys = pgTable(
  'admin_api_keys',
  {
    id: text('id').primaryKey(),
    organizationId: text('organization_id').notNull(),
    name: text('name').notNull(),
    hashedKey: text('hashed_key').notNull(),
    keyFingerprint: text('key_fingerprint').notNull(),
    partialKeyHint: text('partial_key_hint').notNull(),
    scopes: text('scopes')
      .array()
      .notNull()
      .default([] as string[]),
    status: text('status').notNull().default('active'),
    expiresAt: timestamp('expires_at', { withTimezone: true }),
    lastUsedAt: timestamp('last_used_at', { withTimezone: true }),
    revokedAt: timestamp('revoked_at', { withTimezone: true }),
    createdBy: text('created_by').notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index('admin_api_keys_organization_idx').on(t.organizationId, t.status),
    uniqueIndex('admin_api_keys_fingerprint_idx').on(t.keyFingerprint),
    foreignKey({
      name: 'admin_api_keys_organization_fk',
      columns: [t.organizationId],
      foreignColumns: [organizations.id],
    }).onDelete('restrict'),
    check('admin_api_keys_status_check', sql`${t.status} in ('active', 'inactive', 'archived')`),
  ],
);

// === Agents (versioned) ===
export const agents = pgTable(
  'agents',
  {
    id: text('id').primaryKey(), // 'agt_…'
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    workspaceId: text('workspace_id').notNull(),
    name: text('name').notNull(),
    description: text('description'),
    version: integer('version').notNull().default(1),
    latestVersionId: text('latest_version_id'), // FK to agent_versions.id
    harnessType: text('harness_type').notNull().default('claude_agent_sdk'),
    modelProvider: text('model_provider').notNull(),
    modelId: text('model_id').notNull(),
    modelSpeed: text('model_speed'),
    modelEffort: text('model_effort'),
    system: text('system').default(''),
    tools: jsonb('tools').notNull().default([]),
    mcpServers: jsonb('mcp_servers').notNull().default([]),
    skills: jsonb('skills').notNull().default([]), // ordered requested skill refs
    // Explicit guardrail references. Scoped guardrails (workspace, organization)
    // apply without being named here; this is only the agent tier.
    guardrailIds: jsonb('guardrail_ids').notNull().default([]),
    metadata: jsonb('metadata').notNull().default({}),
    multiagent: jsonb('multiagent'),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('agents_workspace_id_idx').on(t.workspaceId, t.id),
    index('agents_workspace_idx').on(t.workspaceId, t.archivedAt),
    foreignKey({
      name: 'agents_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'agents_workspace_latest_version_fk',
      columns: [t.workspaceId, t.id, t.latestVersionId],
      foreignColumns: agentLatestVersionColumns(),
    }).onDelete('restrict'),
  ],
);

export const agentVersions = pgTable(
  'agent_versions',
  {
    id: text('id').primaryKey(),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    workspaceId: text('workspace_id').notNull(),
    agentId: text('agent_id').notNull(),
    version: integer('version').notNull(),
    snapshot: jsonb('snapshot').notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('agent_versions_agent_version_idx').on(t.workspaceId, t.agentId, t.version),
    uniqueIndex('agent_versions_workspace_agent_id_idx').on(t.workspaceId, t.agentId, t.id),
    foreignKey({
      name: 'agent_versions_workspace_agent_fk',
      columns: [t.workspaceId, t.agentId],
      foreignColumns: [agents.workspaceId, agents.id],
    }).onDelete('restrict'),
  ],
);

// === Environments ===
export const environments = pgTable(
  'environments',
  {
    id: text('id').primaryKey(),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    workspaceId: text('workspace_id').notNull(),
    name: text('name').notNull(),
    description: text('description'),
    metadata: jsonb('metadata').notNull().default({}),
    packages: jsonb('packages').notNull().default([]),
    networking: jsonb('networking').notNull().default({}),
    image: text('image'),
    target: text('target'),
    // Env key: only the digest is persisted (the raw key is returned once at
    // create/rotate and never stored). NULL when no key is armed (revoked).
    envKeyDigest: text('env_key_digest'),
    envKeyExpiresAt: timestamp('env_key_expires_at', { withTimezone: true }),
    // Per-launch Environment Token: the managed-auth alternative to the Env
    // Key for a registry-launched worker (a server-managed sandbox has no
    // operator to provision an Env Key ahead of time). Same digest+expiry
    // shape as the Env Key pair above; only the digest is persisted, and a
    // fresh mint on relaunch overwrites it in place (atomically revoking the
    // previous generation's token). NULL when no token is armed.
    environmentTokenDigest: text('environment_token_digest'),
    environmentTokenExpiresAt: timestamp('environment_token_expires_at', { withTimezone: true }),
    // How the environment reaches the network: 'gateway' (egress via the
    // ai-gateway) or 'sidecar'. NULL falls back to the deployment default.
    egressMode: text('egress_mode'),
    // Per-environment LLM routing/credentials descriptor. NULL = catalog default.
    llm: jsonb('llm'),
    scope: text('scope'),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('environments_workspace_id_idx').on(t.workspaceId, t.id),
    index('environments_workspace_idx').on(t.workspaceId, t.archivedAt),
    uniqueIndex('environments_ws_name_idx')
      .on(t.workspaceId, t.name)
      .where(sql`${t.deletedAt} is null`),
    check('environments_egress_mode_check', sql`${t.egressMode} in ('gateway', 'sidecar')`),
    foreignKey({
      name: 'environments_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
  ],
);

// === Cron Agent Triggers ===
export const agentTriggers = pgTable(
  'agent_triggers',
  {
    id: text('id').primaryKey(), // 'trg_…'
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    workspaceId: text('workspace_id').notNull(),
    /** Registry-authenticated creator used for autonomous trigger turns. */
    guardrailSubject: text('guardrail_subject').notNull(),
    name: text('name').notNull(),
    agentId: text('agent_id').notNull(),
    agentVersion: integer('agent_version').notNull(),
    environmentId: text('environment_id').notNull(),
    titleTemplate: text('title_template'),
    metadata: jsonb('metadata').notNull().default({}),
    vaultIds: text('vault_ids')
      .array()
      .notNull()
      .default([] as string[]),
    payload: text('payload').notNull(),
    cronExpression: text('cron_expression').notNull(),
    timezone: text('timezone').notNull().default('Etc/UTC'),
    status: text('status').notNull().default('active'),
    generation: integer('generation').notNull().default(1),
    nextFireAt: timestamp('next_fire_at', { withTimezone: true }),
    lastFiredAt: timestamp('last_fired_at', { withTimezone: true }),
    lastError: text('last_error'),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('agent_triggers_workspace_id_idx').on(t.workspaceId, t.id),
    index('agent_triggers_workspace_list_idx').on(
      t.workspaceId,
      t.archivedAt,
      t.createdAt.desc(),
      t.id.desc(),
    ),
    index('agent_triggers_due_idx')
      .on(t.nextFireAt)
      .where(
        sql`${t.status} = 'active' and ${t.archivedAt} is null and ${t.deletedAt} is null and ${t.nextFireAt} is not null`,
      ),
    foreignKey({
      name: 'agent_triggers_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'agent_triggers_workspace_agent_version_fk',
      columns: [t.workspaceId, t.agentId, t.agentVersion],
      foreignColumns: [agentVersions.workspaceId, agentVersions.agentId, agentVersions.version],
    }).onDelete('restrict'),
    foreignKey({
      name: 'agent_triggers_workspace_environment_fk',
      columns: [t.workspaceId, t.environmentId],
      foreignColumns: [environments.workspaceId, environments.id],
    }).onDelete('restrict'),
    check('agent_triggers_status_check', sql`${t.status} in ('active', 'paused', 'archived')`),
    check('agent_triggers_generation_check', sql`${t.generation} > 0`),
  ],
);
// === Environment claims ===
// A durable lease: an environment is claimed by exactly one registry pod at a
// time, so a worker's tunnel terminates on the single replica that owns its
// environment. The registry runs multiple replicas, so the claim is persisted
// here rather than held in process memory, and the exclusive-owner invariant is
// enforced by the unique primary key on environmentId.
export const environmentClaims = pgTable('environment_claims', {
  // One claim row per environment — the unique FK is the exclusive-claim lock.
  environmentId: text('environment_id')
    .primaryKey()
    .references(() => environments.id, { onDelete: 'cascade' }),
  // Registry replica that currently owns the environment.
  ownerPod: text('owner_pod').notNull(),
  // Worker tunnel connection the owning pod is bound to.
  workerConnId: text('worker_conn_id').notNull(),
  claimedAt: timestamp('claimed_at', { withTimezone: true }).notNull().defaultNow(),
  // Heartbeat watermark — staleness is measured as now - lastPing > ttl.
  lastPing: timestamp('last_ping', { withTimezone: true }).notNull().defaultNow(),
});

// === Sessions ===
export const sessions = pgTable(
  'sessions',
  {
    id: text('id').primaryKey(),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    workspaceId: text('workspace_id').notNull(),
    agentId: text('agent_id').notNull(),
    agentVersion: integer('agent_version').notNull(),
    runtimeRevision: integer('runtime_revision').notNull().default(1),
    title: text('title'),
    // Nullable for legacy rows only. POST /v1/sessions requires an active,
    // workspace-owned environment and always writes this column.
    environmentId: text('environment_id'),
    metadata: jsonb('metadata').notNull().default({}),
    vaultIds: text('vault_ids')
      .array()
      .notNull()
      .default([] as string[]),
    // Session-local overrides (managed-agents-2026-04-01 UpdateSession). NULL =
    // "no override, fall back to the pinned agent version". A non-null array is
    // a FULL REPLACEMENT applied at runtime re-materialization; it does NOT
    // create a new agent version.
    tools: jsonb('tools'),
    mcpServers: jsonb('mcp_servers'),
    // CreateSession agent_with_overrides fields that do not already have
    // dedicated runtime columns. Object key presence distinguishes an omitted
    // override from an explicit `system: null` clear.
    agentOverrides: jsonb('agent_overrides'),
    status: text('status').notNull().default('idle'),
    lastEventSeq: bigint('last_event_seq', { mode: 'number' }).notNull().default(0),
    sandboxHandleId: text('sandbox_handle_id'),
    // === Claim-based distribution (self_hosted environments) ===
    // The runner id minted for this session's distribution, derived from a
    // per-run binding token via the token-bound derivation both the runner and
    // the runner-tunnel route reproduce. NULL until the session is dispatched to
    // a connected worker (or for cloud sessions, which never distribute here).
    runnerId: text('runner_id'),
    // The environment whose connected worker was dispatched the launch frame.
    // NULL when no worker was connected at dispatch time (the session stays
    // pending until a worker reconnects). Distinct from `environmentId`, which is
    // the requested environment: they coincide for a dispatched self_hosted
    // session, but `hostEnvironmentId` records that a launch was actually sent.
    hostEnvironmentId: text('host_environment_id'),
    // The distribution lifecycle, ORTHOGONAL to `status` (which tracks the
    // turn-level run state): NULL for a cloud / never-distributed session,
    // 'pending' once persisted with no connected runner, 'assigned' once the
    // matched runner connects its tunnel, 'failed' once the worker refuses the
    // launch or the runner dies before connecting.
    distributionState: text('distribution_state'),
    activeSeconds: integer('active_seconds').notNull().default(0),
    usageInputTokens: integer('usage_input_tokens').notNull().default(0),
    usageOutputTokens: integer('usage_output_tokens').notNull().default(0),
    usageCacheReadInputTokens: integer('usage_cache_read_input_tokens').notNull().default(0),
    usageCacheCreationEphemeral1hInputTokens: integer(
      'usage_cache_creation_ephemeral_1h_input_tokens',
    )
      .notNull()
      .default(0),
    usageCacheCreationEphemeral5mInputTokens: integer(
      'usage_cache_creation_ephemeral_5m_input_tokens',
    )
      .notNull()
      .default(0),
    /** Exact accumulated spend in nano-USD. NULL means no delta was priced. */
    usageCostNanoUsd: bigint('usage_cost_nano_usd', { mode: 'bigint' }),
    /** True once any non-empty usage delta could not be priced. */
    usageHasUnpriced: boolean('usage_has_unpriced').notNull().default(false),
    startedAt: timestamp('started_at', { withTimezone: true }),
    lastActiveAt: timestamp('last_active_at', { withTimezone: true }),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('sessions_workspace_id_idx').on(t.workspaceId, t.id),
    index('sessions_workspace_idx').on(t.workspaceId, t.archivedAt),
    index('sessions_workspace_environment_idx').on(t.workspaceId, t.environmentId),
    foreignKey({
      name: 'sessions_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'sessions_workspace_agent_version_fk',
      columns: [t.workspaceId, t.agentId, t.agentVersion],
      foreignColumns: [agentVersions.workspaceId, agentVersions.agentId, agentVersions.version],
    }).onDelete('restrict'),
    foreignKey({
      name: 'sessions_workspace_environment_fk',
      columns: [t.workspaceId, t.environmentId],
      foreignColumns: [environments.workspaceId, environments.id],
    }).onDelete('restrict'),
  ],
);

// Private native SDK history is deliberately outside Sessions: ordinary list,
// lifecycle and usage queries must never materialize these multi-MiB blobs.
export const sessionHarnessStates = pgTable(
  'session_harness_states',
  {
    workspaceId: text('workspace_id').notNull(),
    sessionId: text('session_id').notNull(),
    state: jsonb('state').notNull(),
  },
  (t) => [
    primaryKey({ columns: [t.workspaceId, t.sessionId] }),
    foreignKey({
      name: 'session_harness_states_workspace_session_fk',
      columns: [t.workspaceId, t.sessionId],
      foreignColumns: [sessions.workspaceId, sessions.id],
    }).onDelete('cascade'),
  ],
);

// Internal occurrence ledger. A fire becomes `enqueued` in the same database
// transaction that inserts its Session and initial-event outbox row.
export const agentTriggerFires = pgTable(
  'agent_trigger_fires',
  {
    id: text('id').primaryKey(), // 'trgfire_…'
    workspaceId: text('workspace_id').notNull(),
    triggerId: text('trigger_id').notNull(),
    generation: integer('generation').notNull(),
    scheduledFor: timestamp('scheduled_for', { withTimezone: true }).notNull(),
    status: text('status').notNull().default('pending'),
    plannedSessionId: text('planned_session_id').notNull(),
    sessionId: text('session_id'),
    eventId: text('event_id').notNull(),
    attemptCount: integer('attempt_count').notNull().default(0),
    lastError: text('last_error'),
    nextAttemptAt: timestamp('next_attempt_at', { withTimezone: true }).notNull().defaultNow(),
    enqueuedAt: timestamp('enqueued_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('agent_trigger_fires_slot_idx').on(
      t.workspaceId,
      t.triggerId,
      t.generation,
      t.scheduledFor,
    ),
    index('agent_trigger_fires_pending_idx')
      .on(t.nextAttemptAt, t.scheduledFor, t.id)
      .where(sql`${t.status} = 'pending'`),
    index('agent_trigger_fires_trigger_sessions_idx')
      .on(t.workspaceId, t.triggerId, t.scheduledFor.desc(), t.id.desc())
      .where(sql`${t.status} = 'enqueued'`),
    foreignKey({
      name: 'agent_trigger_fires_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'agent_trigger_fires_workspace_trigger_fk',
      columns: [t.workspaceId, t.triggerId],
      foreignColumns: [agentTriggers.workspaceId, agentTriggers.id],
    }).onDelete('restrict'),
    check(
      'agent_trigger_fires_status_check',
      sql`${t.status} in ('pending', 'enqueued', 'failed', 'canceled')`,
    ),
    check('agent_trigger_fires_generation_check', sql`${t.generation} > 0`),
  ],
);

// Durable handoff from Registry session mutations to the transcript backend.
// Deliberately has no session FK: delete must retain its sentinel until it has
// been published successfully.
export const sessionLifecycleOutbox = pgTable(
  'session_lifecycle_outbox',
  {
    id: text('id').primaryKey(),
    workspaceId: text('workspace_id').notNull(),
    sessionId: text('session_id').notNull(),
    kind: text('kind').notNull().default('session.archived'),
    events: jsonb('events'),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    publishedAt: timestamp('published_at', { withTimezone: true }),
    attemptCount: integer('attempt_count').notNull().default(0),
    lastAttemptAt: timestamp('last_attempt_at', { withTimezone: true }),
  },
  (t) => [
    index('session_lifecycle_outbox_pending_idx').on(t.publishedAt, t.createdAt),
    check(
      'session_lifecycle_outbox_kind_check',
      sql`${t.kind} in ('session.initial_events', 'session.archived', 'session.deleted')`,
    ),
  ],
);

// === Session resources ===
export const sessionResources = pgTable(
  'session_resources',
  {
    id: text('id').primaryKey(),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    workspaceId: text('workspace_id').notNull(),
    sessionId: text('session_id').notNull(),
    type: text('type').notNull(),
    fileId: text('file_id'),
    memoryStoreId: text('memory_store_id'),
    repoRef: jsonb('repo_ref'),
    mountPath: text('mount_path').notNull(),
    access: text('access').notNull(),
    // Per-resource mount strategy override for file resources. NULL means
    // "use the host-side tarball prefetch default".
    mountStrategy: text('mount_strategy'),
    instructions: text('instructions'),
    attachedAt: timestamp('attached_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
    detachedAt: timestamp('detached_at', { withTimezone: true }),
  },
  (t) => [
    uniqueIndex('session_resources_workspace_id_idx').on(t.workspaceId, t.id),
    index('session_resources_session_idx').on(t.workspaceId, t.sessionId, t.detachedAt),
    check('session_resources_access_check', sql`${t.access} in ('read_only', 'read_write')`),
    foreignKey({
      name: 'session_resources_workspace_session_fk',
      columns: [t.workspaceId, t.sessionId],
      foreignColumns: [sessions.workspaceId, sessions.id],
    }).onDelete('restrict'),
  ],
);

// === Session event read model ===
export const sessionEventsIndex = pgTable(
  'session_events_index',
  {
    workspaceId: text('workspace_id').notNull(),
    sessionId: text('session_id').notNull(),
    seq: bigint('seq', { mode: 'number' }).notNull(),
    projectionOrdinal: integer('projection_ordinal').notNull().default(0),
    projectionVersion: integer('projection_version').notNull().default(0),
    eventId: text('event_id').notNull(),
    subpath: text('subpath').notNull().default(''),
    processedAt: text('processed_at'),
    processedMarkerSeq: bigint('processed_marker_seq', { mode: 'number' }),
    producedAt: text('produced_at').notNull(),
    producedBy: text('produced_by').notNull(),
    kind: text('kind').notNull(),
    visibility: text('visibility').notNull(),
    payload: jsonb('payload').notNull().default({}),
    /** Registry-authenticated principal for a turn-driving client event. Never public. */
    guardrailSubject: text('guardrail_subject'),
    indexedAt: timestamp('indexed_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.workspaceId, t.sessionId, t.eventId] }),
    foreignKey({
      name: 'session_events_index_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
    index('session_events_index_public_seq_idx').on(
      t.workspaceId,
      t.sessionId,
      t.visibility,
      t.subpath,
      t.seq,
      t.projectionOrdinal,
      t.eventId,
    ),
    index('session_events_index_session_seq_idx').on(t.workspaceId, t.sessionId, t.seq),
    // The Registry refreshes this bounded dispatch-backlog view every five
    // seconds. Keep its public/client/user predicate and lexical ISO UTC
    // timestamp order covered without indexing payload data.
    index('session_events_index_unprocessed_client_user_idx')
      .on(t.workspaceId, t.producedAt)
      .where(
        sql`${t.visibility} = 'public' AND ${t.producedBy} = 'client' AND ${t.kind} LIKE 'user.%' AND ${t.processedAt} IS NULL`,
      ),
    index('session_events_index_user_event_marker_idx')
      .on(
        t.workspaceId,
        t.sessionId,
        sql`(${t.payload}->>'user_event_id')`,
        t.seq,
        t.projectionOrdinal,
        t.eventId,
      )
      .where(
        sql`${t.kind} in ('session.user_event_processed', 'session.deferred_user_message_submitted')`,
      ),
  ],
);

/**
 * Exactly-once markers for internal usage deltas. The sandbox event id is
 * stable across SSE reconnects and retries, so inserting this row in the same
 * transaction as the counters prevents a timed-out acknowledgement from
 * charging the same model call twice.
 */
export const sessionUsageEvents = pgTable(
  'session_usage_events',
  {
    workspaceId: text('workspace_id').notNull(),
    sessionId: text('session_id').notNull(),
    eventId: text('event_id').notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.workspaceId, t.sessionId, t.eventId] }),
    foreignKey({
      name: 'session_usage_events_workspace_session_fk',
      columns: [t.workspaceId, t.sessionId],
      foreignColumns: [sessions.workspaceId, sessions.id],
    }).onDelete('cascade'),
  ],
);

// === Session threads ===
export const sessionThreads = pgTable(
  'session_threads',
  {
    id: text('id').primaryKey(),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    workspaceId: text('workspace_id').notNull(),
    sessionId: text('session_id').notNull(),
    subpath: text('subpath').notNull(),
    agentId: text('agent_id').notNull(),
    agentVersion: integer('agent_version').notNull(),
    agentName: text('agent_name').notNull(),
    parentThreadId: text('parent_thread_id'),
    status: text('status').notNull().default('idle'),
    stopReason: text('stop_reason'),
    // Per-thread usage, mirroring the session columns. A dispatched subagent
    // runs on its own thread, so its spend is attributable to it without
    // re-deriving anything from the transcript. The cost and unpriced marker
    // follow the session columns' semantics.
    usageInputTokens: integer('usage_input_tokens').notNull().default(0),
    usageOutputTokens: integer('usage_output_tokens').notNull().default(0),
    usageCacheReadInputTokens: integer('usage_cache_read_input_tokens').notNull().default(0),
    usageCacheCreationEphemeral1hInputTokens: integer(
      'usage_cache_creation_ephemeral_1h_input_tokens',
    )
      .notNull()
      .default(0),
    usageCacheCreationEphemeral5mInputTokens: integer(
      'usage_cache_creation_ephemeral_5m_input_tokens',
    )
      .notNull()
      .default(0),
    usageCostNanoUsd: bigint('usage_cost_nano_usd', { mode: 'bigint' }),
    usageHasUnpriced: boolean('usage_has_unpriced').notNull().default(false),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index('session_threads_session_idx').on(t.workspaceId, t.sessionId, t.archivedAt),
    // A subpath identifies one transcript thread for the Session's entire lifetime,
    // including its tombstone. The event projector upserts on this full key;
    // deleting the parent Session does not release thread identities for reuse.
    uniqueIndex('session_threads_session_subpath_idx').on(t.workspaceId, t.sessionId, t.subpath),
    uniqueIndex('session_threads_workspace_session_id_idx').on(t.workspaceId, t.sessionId, t.id),
    index('session_threads_parent_idx').on(t.workspaceId, t.sessionId, t.parentThreadId),
    foreignKey({
      name: 'session_threads_workspace_session_fk',
      columns: [t.workspaceId, t.sessionId],
      foreignColumns: [sessions.workspaceId, sessions.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'session_threads_workspace_agent_version_fk',
      columns: [t.workspaceId, t.agentId, t.agentVersion],
      foreignColumns: [agentVersions.workspaceId, agentVersions.agentId, agentVersions.version],
    }).onDelete('restrict'),
    // The composite same-session parent FK is declared in migration 0030.
    // Drizzle's table callback cannot express this self-reference without a
    // recursive inferred table type, so schema.ts carries the column/indexes.
  ],
);

// === Vaults ===
export const vaults = pgTable(
  'vaults',
  {
    id: text('id').primaryKey(),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    workspaceId: text('workspace_id').notNull(),
    displayName: text('display_name').notNull(),
    metadata: jsonb('metadata').notNull().default({}),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('vaults_workspace_id_idx').on(t.workspaceId, t.id),
    index('vaults_workspace_idx').on(t.workspaceId, t.archivedAt),
    foreignKey({
      name: 'vaults_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
  ],
);

export const gitCredentials = pgTable(
  'git_credentials',
  {
    id: text('id').primaryKey(),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    workspaceId: text('workspace_id').notNull(),
    provider: text('provider').notNull().default('github'),
    repoUrl: text('repo_url').notNull(),
    secretRef: text('secret_ref').notNull(),
    // Non-null only for credentials materialized from a raw
    // github_repository.authorization_token. One credential belongs to one
    // session resource so resource-scoped token rotation cannot affect another
    // session mounting the same repository.
    sessionResourceId: text('session_resource_id'),
    metadata: jsonb('metadata').notNull().default({}),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index('git_credentials_workspace_idx').on(t.workspaceId, t.archivedAt),
    index('git_credentials_repo_idx').on(t.workspaceId, t.repoUrl, t.archivedAt),
    // Shared credentials stay unique per workspace/repository. Resource-owned
    // credentials may repeat across sessions; session routes separately reject
    // duplicate active repository URLs within one session because git's
    // credential-helper request cannot identify a mount/resource id.
    uniqueIndex('git_credentials_active_repo_idx')
      .on(t.workspaceId, t.repoUrl)
      .where(
        sql`${t.archivedAt} is null and ${t.deletedAt} is null and ${t.sessionResourceId} is null`,
      ),
    uniqueIndex('git_credentials_session_resource_idx')
      .on(t.workspaceId, t.sessionResourceId)
      .where(sql`${t.sessionResourceId} is not null and ${t.deletedAt} is null`),
    foreignKey({
      name: 'git_credentials_workspace_session_resource_fk',
      columns: [t.workspaceId, t.sessionResourceId],
      foreignColumns: [sessionResources.workspaceId, sessionResources.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'git_credentials_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
    check('git_credentials_provider_check', sql`${t.provider} in ('github')`),
  ],
);

// Durable intent written before raw Git credential bytes enter SecretStore.
// The session-resource transaction deletes this row atomically with inserting
// git_credentials metadata. Expired rows are safe for the reconciler to claim
// and purge because they are never foreign-keyed to not-yet-created resources.
export const gitCredentialStagingIntents = pgTable(
  'git_credential_staging_intents',
  {
    credentialId: text('git_credential_id').primaryKey(),
    workspaceId: text('workspace_id').notNull(),
    sessionResourceId: text('session_resource_id').notNull(),
    secretRef: text('secret_ref').notNull(),
    status: text('status').notNull().default('pending'),
    cleanupAfter: timestamp('cleanup_after', { withTimezone: true }).notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index('git_credential_staging_cleanup_idx').on(t.status, t.cleanupAfter),
    uniqueIndex('git_credential_staging_secret_ref_idx').on(t.secretRef),
    foreignKey({
      name: 'git_credential_staging_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
    check('git_credential_staging_status_check', sql`${t.status} in ('pending', 'cleaning')`),
  ],
);

export const vaultCredentials = pgTable(
  'vault_credentials',
  {
    id: text('id').primaryKey(),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    workspaceId: text('workspace_id').notNull(),
    vaultId: text('vault_id').notNull(),
    displayName: text('display_name'),
    authType: text('auth_type').notNull(),
    provider: text('provider'),
    scheme: text('scheme'),
    logicalId: text('logical_id'),
    resolutionVersion: text('resolution_version'),
    mcpServerUrl: text('mcp_server_url'),
    secretName: text('secret_name'),
    networking: jsonb('networking').notNull().default({}),
    accessSecretRef: text('access_secret_ref').notNull(),
    refreshSecretRef: text('refresh_secret_ref'),
    tokenEndpoint: text('token_endpoint'),
    clientId: text('client_id'),
    tokenEndpointAuthType: text('token_endpoint_auth_type'),
    clientSecretRef: text('client_secret_ref'),
    oauthRefreshLeaseOwner: text('oauth_refresh_lease_owner'),
    oauthRefreshLeaseExpiresAt: timestamp('oauth_refresh_lease_expires_at', {
      withTimezone: true,
    }),
    // Public, non-secret variant-specific fields that are not consumed by
    // the internal credential resolver (injection location and OAuth
    // expiry/resource/scope). Keeping them separate from `networking`
    // preserves the internal wire contract.
    authConfig: jsonb('auth_config').notNull().default({}),
    metadata: jsonb('metadata').notNull().default({}),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index('vault_credentials_workspace_idx').on(t.workspaceId, t.archivedAt),
    index('vault_credentials_list_idx').on(
      t.workspaceId,
      t.vaultId,
      t.archivedAt,
      t.createdAt.desc(),
      t.id.desc(),
    ),
    index('vault_credentials_runtime_idx').on(
      t.workspaceId,
      t.vaultId,
      t.archivedAt,
      t.mcpServerUrl,
    ),
    uniqueIndex('vault_credentials_active_url_idx')
      .on(t.workspaceId, t.vaultId, t.mcpServerUrl)
      .where(sql`${t.archivedAt} is null and ${t.deletedAt} is null`),
    uniqueIndex('vault_credentials_active_secret_name_idx')
      .on(t.workspaceId, t.vaultId, t.secretName)
      .where(
        sql`${t.archivedAt} is null and ${t.deletedAt} is null and ${t.secretName} is not null`,
      ),
    uniqueIndex('vault_credentials_active_logical_id_idx')
      .on(t.workspaceId, t.vaultId, t.logicalId)
      .where(
        sql`${t.archivedAt} is null and ${t.deletedAt} is null and ${t.logicalId} is not null`,
      ),
    foreignKey({
      name: 'vault_credentials_workspace_vault_fk',
      columns: [t.workspaceId, t.vaultId],
      foreignColumns: [vaults.workspaceId, vaults.id],
    }).onDelete('restrict'),
    check(
      'vault_credentials_auth_type_check',
      sql`${t.authType} in ('static_bearer', 'mcp_oauth', 'environment_variable', 'provider')`,
    ),
    check(
      'vault_credentials_provider_fields_check',
      sql`(
        ${t.authType} = 'provider'
        and ${t.provider} in ('anthropic', 'openai', 'openai_compatible', 'azure_openai', 'vertex', 'bedrock')
        and ${t.scheme} in ('api_key', 'bearer', 'gcp-service-account', 'aws-sig-v4')
        and (
          (${t.provider} = 'anthropic' and ${t.scheme} = 'api_key')
          or (${t.provider} in ('openai', 'openai_compatible') and ${t.scheme} = 'bearer')
          or (${t.provider} = 'azure_openai' and ${t.scheme} in ('api_key', 'bearer'))
          or (${t.provider} = 'vertex' and ${t.scheme} = 'gcp-service-account')
          or (${t.provider} = 'bedrock' and ${t.scheme} = 'aws-sig-v4')
        )
        and ${t.logicalId} is not null
        and ${t.logicalId} ~ '^llm:[A-Za-z0-9][A-Za-z0-9._:-]{0,123}$'
        and char_length(${t.logicalId}) between 5 and 128
        and ${t.logicalId} !~ '[^A-Za-z0-9._:-]'
        and ${t.resolutionVersion} is not null
        and ${t.mcpServerUrl} is null
        and ${t.secretName} is null
      ) or (
        ${t.authType} <> 'provider'
        and ${t.provider} is null
        and ${t.scheme} is null
        and ${t.logicalId} is null
        and ${t.resolutionVersion} is null
      )`,
    ),
  ],
);

// === Skills (versioned) ===
export const skills = pgTable(
  'skills',
  {
    id: text('id').primaryKey(),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    workspaceId: text('workspace_id').notNull(),
    type: text('type').notNull().default('custom'), // 'anthropic' | 'custom'
    name: text('name').notNull(),
    slug: text('slug').notNull(),
    version: integer('version').notNull().default(1),
    latestVersionId: text('latest_version_id'),
    description: text('description'),
    displayTitle: text('display_title'),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('skills_workspace_id_idx').on(t.workspaceId, t.id),
    uniqueIndex('skills_ws_display_title_idx')
      .on(t.workspaceId, t.displayTitle)
      .where(
        sql`${t.type} = 'custom' and ${t.displayTitle} is not null and ${t.archivedAt} is null and ${t.deletedAt} is null`,
      ),
    index('skills_workspace_idx').on(t.workspaceId, t.archivedAt),
    index('skills_workspace_created_page_idx')
      .on(t.workspaceId, t.createdAt, t.id)
      .where(sql`${t.archivedAt} is null and ${t.deletedAt} is null`),
    index('skills_workspace_type_created_page_idx')
      .on(t.workspaceId, t.type, t.createdAt, t.id)
      .where(sql`${t.archivedAt} is null and ${t.deletedAt} is null`),
    check('skills_type_check', sql`${t.type} in ('anthropic', 'custom')`),
    foreignKey({
      name: 'skills_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'skills_workspace_latest_version_fk',
      columns: [t.workspaceId, t.id, t.latestVersionId],
      foreignColumns: skillLatestVersionColumns(),
    }).onDelete('restrict'),
  ],
);

export const skillVersions = pgTable(
  'skill_versions',
  {
    id: text('id').primaryKey(),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    workspaceId: text('workspace_id').notNull(),
    skillId: text('skill_id').notNull(),
    version: integer('version').notNull(),
    versionIdentifier: text('version_identifier').notNull(),
    name: text('name').notNull(),
    description: text('description').notNull().default(''),
    directory: text('directory').notNull(),
    entrypoint: text('entrypoint').notNull().default('SKILL.md'),
    packageSha256: text('package_sha256').notNull(),
    packageSizeBytes: integer('package_size_bytes').notNull(),
    packageManifest: jsonb('package_manifest').notNull().default([]),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('skill_versions_skill_version_idx').on(t.workspaceId, t.skillId, t.version),
    uniqueIndex('skill_versions_workspace_skill_id_idx').on(t.workspaceId, t.skillId, t.id),
    uniqueIndex('skill_versions_workspace_bundle_idx').on(t.workspaceId, t.id, t.packageSha256),
    uniqueIndex('skill_versions_skill_version_identifier_idx').on(
      t.workspaceId,
      t.skillId,
      t.versionIdentifier,
    ),
    index('skill_versions_identifier_page_idx')
      .on(t.workspaceId, t.skillId, sql`${t.versionIdentifier}::numeric`)
      .where(sql`${t.archivedAt} is null and ${t.deletedAt} is null`),
    foreignKey({
      name: 'skill_versions_workspace_skill_fk',
      columns: [t.workspaceId, t.skillId],
      foreignColumns: [skills.workspaceId, skills.id],
    }).onDelete('restrict'),
    check('skill_versions_entrypoint_check', sql`${t.entrypoint} = 'SKILL.md'`),
    check(
      'skill_versions_version_identifier_check',
      sql`${t.versionIdentifier} ~ '^[1-9][0-9]{0,31}$'`,
    ),
    check('skill_versions_package_sha256_check', sql`${t.packageSha256} ~ '^[0-9a-f]{64}$'`),
    check('skill_versions_package_size_check', sql`${t.packageSizeBytes} > 0`),
  ],
);

// Durable cleanup of bundles whose metadata upload did not commit. This has no
// Skill/SkillVersion foreign key because no committed version may exist. A
// retained version, including a deleted one, always protects its bundle.
export const skillBundleDeletionOutbox = pgTable(
  'skill_bundle_deletion_outbox',
  {
    workspaceId: text('workspace_id').notNull(),
    skillVersionId: text('skill_version_id').notNull(),
    packageSha256: text('package_sha256').notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    attemptCount: integer('attempt_count').notNull().default(0),
    lastAttemptAt: timestamp('last_attempt_at', { withTimezone: true }),
  },
  (t) => [
    primaryKey({
      name: 'skill_bundle_deletion_outbox_pk',
      columns: [t.workspaceId, t.skillVersionId, t.packageSha256],
    }),
    index('skill_bundle_deletion_outbox_pending_idx').on(t.createdAt),
    check('skill_bundle_deletion_outbox_sha256_check', sql`${t.packageSha256} ~ '^[0-9a-f]{64}$'`),
  ],
);

export const sessionSkillBindings = pgTable(
  'session_skill_bindings',
  {
    workspaceId: text('workspace_id').notNull(),
    sessionId: text('session_id').notNull(),
    agentId: text('agent_id').notNull(),
    agentVersion: integer('agent_version').notNull(),
    ordinal: integer('ordinal').notNull(),
    skillVersionId: text('skill_version_id').notNull(),
    bundleSha256: text('bundle_sha256').notNull(),
  },
  (t) => [
    primaryKey({
      name: 'session_skill_bindings_pk',
      columns: [t.workspaceId, t.sessionId, t.agentId, t.agentVersion, t.ordinal],
    }),
    uniqueIndex('session_skill_bindings_skill_idx').on(
      t.workspaceId,
      t.sessionId,
      t.agentId,
      t.agentVersion,
      t.skillVersionId,
    ),
    index('session_skill_bindings_session_idx').on(t.workspaceId, t.sessionId),
    foreignKey({
      name: 'session_skill_bindings_session_fk',
      columns: [t.workspaceId, t.sessionId],
      foreignColumns: [sessions.workspaceId, sessions.id],
    }).onDelete('cascade'),
    foreignKey({
      name: 'session_skill_bindings_agent_version_fk',
      columns: [t.workspaceId, t.agentId, t.agentVersion],
      foreignColumns: sessionBindingAgentVersionColumns(),
    }).onDelete('restrict'),
    foreignKey({
      name: 'session_skill_bindings_bundle_fk',
      columns: [t.workspaceId, t.skillVersionId, t.bundleSha256],
      foreignColumns: sessionBindingSkillVersionColumns(),
    }).onDelete('restrict'),
    check('session_skill_bindings_ordinal_check', sql`${t.ordinal} >= 0`),
  ],
);

function sessionBindingAgentVersionColumns(): [AnyPgColumn, AnyPgColumn, AnyPgColumn] {
  const table = agentVersions as unknown as {
    workspaceId: AnyPgColumn;
    agentId: AnyPgColumn;
    version: AnyPgColumn;
  };
  return [table.workspaceId, table.agentId, table.version];
}

function agentLatestVersionColumns(): [AnyPgColumn, AnyPgColumn, AnyPgColumn] {
  const table = agentVersions as unknown as {
    workspaceId: AnyPgColumn;
    agentId: AnyPgColumn;
    id: AnyPgColumn;
  };
  return [table.workspaceId, table.agentId, table.id];
}

function sessionBindingSkillVersionColumns(): [AnyPgColumn, AnyPgColumn, AnyPgColumn] {
  const table = skillVersions as unknown as {
    workspaceId: AnyPgColumn;
    id: AnyPgColumn;
    packageSha256: AnyPgColumn;
  };
  return [table.workspaceId, table.id, table.packageSha256];
}

function skillLatestVersionColumns(): [AnyPgColumn, AnyPgColumn, AnyPgColumn] {
  const table = skillVersions as unknown as {
    workspaceId: AnyPgColumn;
    skillId: AnyPgColumn;
    id: AnyPgColumn;
  };
  return [table.workspaceId, table.skillId, table.id];
}

// === API keys ===
export const apiKeys = pgTable(
  'api_keys',
  {
    id: text('id').primaryKey(),
    workspaceId: text('workspace_id').notNull(),
    hashedKey: text('hashed_key').notNull(),
    keyFingerprint: text('key_fingerprint').notNull(),
    name: text('name').notNull().default('API key'),
    partialKeyHint: text('partial_key_hint').notNull().default(''),
    principal: text('principal').notNull(),
    scopes: text('scopes')
      .array()
      .notNull()
      .default([] as string[]),
    status: text('status').notNull().default('active'),
    expiresAt: timestamp('expires_at', { withTimezone: true }),
    lastUsedAt: timestamp('last_used_at', { withTimezone: true }),
    revokedAt: timestamp('revoked_at', { withTimezone: true }),
    createdBy: text('created_by').notNull().default('bootstrap'),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index('api_keys_workspace_idx').on(t.workspaceId, t.revokedAt),
    uniqueIndex('api_keys_fingerprint_idx').on(t.keyFingerprint),
    foreignKey({
      name: 'api_keys_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
    check('api_keys_status_check', sql`${t.status} in ('active', 'inactive', 'archived')`),
  ],
);

export const adminAuditEvents = pgTable(
  'admin_audit_events',
  {
    id: text('id').primaryKey(),
    organizationId: text('organization_id').notNull(),
    workspaceId: text('workspace_id'),
    actor: text('actor').notNull(),
    authMethod: text('auth_method').notNull(),
    action: text('action').notNull(),
    targetType: text('target_type').notNull(),
    targetId: text('target_id').notNull(),
    requestId: text('request_id').notNull(),
    result: text('result').notNull(),
    metadata: jsonb('metadata').notNull().default({}),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index('admin_audit_events_organization_idx').on(t.organizationId, t.createdAt),
    index('admin_audit_events_workspace_idx').on(t.workspaceId, t.createdAt),
    foreignKey({
      name: 'admin_audit_events_organization_fk',
      columns: [t.organizationId],
      foreignColumns: [organizations.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'admin_audit_events_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
  ],
);

// === Idempotency keys ===
export const idempotencyKeys = pgTable(
  'idempotency_keys',
  {
    workspaceId: text('workspace_id').notNull(),
    scope: text('scope').notNull(),
    key: text('key').notNull(),
    responseStatus: integer('response_status').notNull(),
    responseBody: text('response_body').notNull(),
    bodyHash: text('body_hash').notNull(),
    expiresAt: timestamp('expires_at', { withTimezone: true }).notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.workspaceId, t.scope, t.key] }),
    index('idempotency_keys_expires_idx').on(t.expiresAt),
    foreignKey({
      name: 'idempotency_keys_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
  ],
);

// === Guardrails ===

/**
 * Guardrail definitions.
 *
 * `workspace_id` is nullable, uniquely among tenant tables: an organization-
 * scoped guardrail applies across every workspace in its organization and so
 * has no owning workspace. Both lookup paths are indexed because both are hot —
 * a session resolves its workspace's guardrails and its organization's on every
 * runtime preparation.
 */
export const guardrails = pgTable(
  'guardrails',
  {
    id: text('id').primaryKey(),
    organizationId: text('organization_id').notNull(),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    workspaceId: text('workspace_id'),
    name: text('name').notNull(),
    description: text('description'),
    enabled: boolean('enabled').notNull().default(true),
    phases: jsonb('phases').notNull().default([]),
    scope: text('scope').notNull(),
    rule: jsonb('rule').notNull(),
    metadata: jsonb('metadata').notNull().default({}),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('guardrails_workspace_id_idx').on(t.workspaceId, t.id),
    index('guardrails_workspace_idx').on(t.workspaceId, t.archivedAt),
    index('guardrails_organization_idx').on(t.organizationId, t.scope, t.archivedAt),
    foreignKey({
      name: 'guardrails_organization_fk',
      columns: [t.organizationId],
      foreignColumns: [organizations.id],
    }).onDelete('restrict'),
    foreignKey({
      name: 'guardrails_workspace_fk',
      columns: [t.workspaceId],
      foreignColumns: [workspaces.id],
    }).onDelete('restrict'),
    check('guardrails_scope_check', sql`${t.scope} in ('organization', 'workspace', 'explicit')`),
    // An organization-scoped guardrail has no workspace; every other scope must
    // name one. Without this a workspace-scoped row with a null workspace would
    // silently apply nowhere.
    check(
      'guardrails_scope_workspace_check',
      sql`(${t.scope} = 'organization') = (${t.workspaceId} is null)`,
    ),
  ],
);

/**
 * Session-lifetime guardrail counters, one row per key.
 *
 * Deliberately not a JSON blob on `sessions`: an increment is a conflicting
 * upsert that adds, so two writers cannot lose each other's update. A blob
 * would require read-modify-write, which is exactly the race a runner respawn
 * produces when the outgoing runner flushes after the incoming one has started.
 */
export const guardrailState = pgTable(
  'guardrail_state',
  {
    workspaceId: text('workspace_id').notNull(),
    sessionId: text('session_id').notNull(),
    key: text('key').notNull(),
    // Split representation: a counter lives in `value_num` so an increment is
    // `value_num = value_num + $1` in SQL, never a read-modify-write. Anything
    // that is not a number lives in `value_json`.
    valueNum: doublePrecision('value_num'),
    valueJson: jsonb('value_json'),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.workspaceId, t.sessionId, t.key] }),
    foreignKey({
      name: 'guardrail_state_workspace_session_fk',
      columns: [t.workspaceId, t.sessionId],
      foreignColumns: [sessions.workspaceId, sessions.id],
    }).onDelete('cascade'),
  ],
);

/**
 * Counters that outlive any one session — a per-principal spend cap for a UTC
 * day, for instance. `window` is a zero-padded date string so lexicographic
 * ordering drives range queries, and no foreign key ties a counter to a session
 * that has long since ended.
 */
export const guardrailCounters = pgTable(
  'guardrail_counters',
  {
    workspaceId: text('workspace_id').notNull(),
    subject: text('subject').notNull(),
    window: text('window').notNull(),
    key: text('key').notNull(),
    valueNum: doublePrecision('value_num').notNull().default(0),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.workspaceId, t.subject, t.window, t.key] }),
    index('guardrail_counters_window_idx').on(t.workspaceId, t.window),
  ],
);

/**
 * Per-model prices, one row per (organization, model, source).
 *
 * The three sources coexist so resolution can pick the highest-precedence row
 * and deleting an operator override falls back to whatever the refresher or the
 * seed catalog provides — an override is reversible without re-entering the
 * original numbers. Rates are stored per million tokens, matching how vendors
 * publish them; conversion to per-token happens at computation.
 */
export const modelPrices = pgTable(
  'model_prices',
  {
    /**
     * Model identity is the `{ provider, id }` pair: the same id served through
     * two providers can carry different rates, so the provider is part of the
     * key rather than an attribute. Defaulted so a deployment that prices only
     * Anthropic models never has to name it, and so the backfill of rows written
     * before this column existed is the identity they already had.
     */
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    provider: text('provider').notNull().default(SEED_PRICE_PROVIDER),
    // Empty string is reserved for deployment-global seed/upstream rows.
    organizationId: text('organization_id').notNull().default(''),
    modelId: text('model_id').notNull(),
    source: text('source').notNull(),
    inputPerMillionTokens: doublePrecision('input_per_million_tokens').notNull(),
    outputPerMillionTokens: doublePrecision('output_per_million_tokens').notNull(),
    cacheReadPerMillionTokens: doublePrecision('cache_read_per_million_tokens'),
    cacheWritePerMillionTokens: doublePrecision('cache_write_per_million_tokens'),
    fetchedAt: timestamp('fetched_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    // Keep one row per natural price key even after soft deletion. Every writer
    // must upsert on this key and clear deletedAt to reactivate it (pricing/store.ts).
    // deletedAt is not part of the identity: a new price replaces the same override.
    primaryKey({ columns: [t.provider, t.modelId, t.source, t.organizationId] }),
    index('model_prices_organization_idx').on(t.organizationId, t.provider, t.modelId),
    check('model_prices_source_check', sql`${t.source} in ('operator', 'upstream', 'seed')`),
    check(
      'model_prices_scope_check',
      sql`(${t.source} = 'operator' and ${t.organizationId} <> '') or (${t.source} <> 'operator' and ${t.organizationId} = '')`,
    ),
  ],
);
