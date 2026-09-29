CREATE TABLE "agent_observability_binding_credentials" (
	"binding_id" text PRIMARY KEY NOT NULL,
	"secret_ref" text NOT NULL,
	"credential_version" integer DEFAULT 1 NOT NULL,
	"key_hint" text,
	"rotated_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_by" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "agent_observability_binding_credentials_ref_check" CHECK (char_length(btrim("agent_observability_binding_credentials"."secret_ref")) > 0 and "agent_observability_binding_credentials"."credential_version" > 0)
);
--> statement-breakpoint
CREATE TABLE "agent_observability_binding_versions" (
	"binding_id" text NOT NULL,
	"version" integer NOT NULL,
	"adapter_type" text NOT NULL,
	"semantic_profile" text NOT NULL,
	"protocol" text NOT NULL,
	"compression" text DEFAULT 'none' NOT NULL,
	"timeout_ms" integer DEFAULT 10000 NOT NULL,
	"environment" text,
	"release" text,
	"capture_mode" text DEFAULT 'metadata_only' NOT NULL,
	"sample_rate" numeric(5, 4) DEFAULT '1' NOT NULL,
	"config_schema_version" integer DEFAULT 1 NOT NULL,
	"created_by" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "agent_observability_binding_versions_pk" PRIMARY KEY("binding_id","version"),
	CONSTRAINT "agent_observability_binding_versions_adapter_config_check" CHECK ((
        "agent_observability_binding_versions"."adapter_type" = 'otlp_http'
        and "agent_observability_binding_versions"."semantic_profile" in ('otel_genai', 'langfuse')
        and "agent_observability_binding_versions"."protocol" in ('http/protobuf', 'http/json')
      ) or (
        "agent_observability_binding_versions"."adapter_type" = 'langfuse_sdk'
        and "agent_observability_binding_versions"."semantic_profile" = 'langfuse'
        and "agent_observability_binding_versions"."protocol" = 'sdk'
      )),
	CONSTRAINT "agent_observability_binding_versions_compression_check" CHECK ("agent_observability_binding_versions"."compression" in ('none', 'gzip')),
	CONSTRAINT "agent_observability_binding_versions_capture_check" CHECK ("agent_observability_binding_versions"."capture_mode" in ('metadata_only', 'redacted_io')),
	CONSTRAINT "agent_observability_binding_versions_bounds_check" CHECK ("agent_observability_binding_versions"."version" > 0
          and "agent_observability_binding_versions"."timeout_ms" > 0
          and "agent_observability_binding_versions"."sample_rate" >= 0 and "agent_observability_binding_versions"."sample_rate" <= 1
          and "agent_observability_binding_versions"."config_schema_version" > 0)
);
--> statement-breakpoint
CREATE TABLE "agent_observability_bindings" (
	"id" text PRIMARY KEY NOT NULL,
	"organization_id" text NOT NULL,
	"workspace_id" text,
	"scope_type" text NOT NULL,
	"adapter_type" text NOT NULL,
	"endpoint_kind" text NOT NULL,
	"endpoint_class" text NOT NULL,
	"endpoint" text NOT NULL,
	"external_project_id" text,
	"current_version" integer DEFAULT 1 NOT NULL,
	"status" text DEFAULT 'active' NOT NULL,
	"revocation_epoch" bigint DEFAULT 0 NOT NULL,
	"created_by" text NOT NULL,
	"updated_by" text NOT NULL,
	"archived_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "agent_observability_bindings_scope_owner_check" CHECK ((
        "agent_observability_bindings"."scope_type" = 'organization'
        and "agent_observability_bindings"."workspace_id" is null
      ) or (
        "agent_observability_bindings"."scope_type" = 'workspace'
        and "agent_observability_bindings"."workspace_id" is not null
      )),
	CONSTRAINT "agent_observability_bindings_adapter_check" CHECK ("agent_observability_bindings"."adapter_type" in ('otlp_http', 'langfuse_sdk')),
	CONSTRAINT "agent_observability_bindings_endpoint_check" CHECK ("agent_observability_bindings"."endpoint_class" in ('public', 'private')
          and char_length(btrim("agent_observability_bindings"."endpoint")) > 0
          and (
            ("agent_observability_bindings"."adapter_type" = 'otlp_http' and "agent_observability_bindings"."endpoint_kind" in ('traces_endpoint', 'base_endpoint')
             and ("agent_observability_bindings"."external_project_id" is null or char_length(btrim("agent_observability_bindings"."external_project_id")) > 0))
            or
            ("agent_observability_bindings"."adapter_type" = 'langfuse_sdk' and "agent_observability_bindings"."endpoint_kind" = 'base_endpoint'
             and "agent_observability_bindings"."external_project_id" is not null and char_length(btrim("agent_observability_bindings"."external_project_id")) > 0)
          )),
	CONSTRAINT "agent_observability_bindings_status_check" CHECK ("agent_observability_bindings"."status" in ('active', 'draining', 'disabled', 'archived')),
	CONSTRAINT "agent_observability_bindings_archive_check" CHECK (("agent_observability_bindings"."status" = 'archived') = ("agent_observability_bindings"."archived_at" is not null)),
	CONSTRAINT "agent_observability_bindings_version_epoch_check" CHECK ("agent_observability_bindings"."current_version" > 0 and "agent_observability_bindings"."revocation_epoch" >= 0)
);
--> statement-breakpoint
CREATE TABLE "agent_observability_organization_settings" (
	"organization_id" text PRIMARY KEY NOT NULL,
	"active_default_binding_id" text,
	"active_default_binding_scope" text,
	"selection_epoch" bigint DEFAULT 0 NOT NULL,
	"default_revocation_epoch" bigint DEFAULT 0 NOT NULL,
	"organization_revocation_epoch" bigint DEFAULT 0 NOT NULL,
	"capture_ceiling" text DEFAULT 'metadata_only' NOT NULL,
	"capture_restriction_epoch" bigint DEFAULT 0 NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "agent_observability_organization_settings_default_binding_check" CHECK ((
        "agent_observability_organization_settings"."active_default_binding_id" is null
        and "agent_observability_organization_settings"."active_default_binding_scope" is null
      ) or (
        "agent_observability_organization_settings"."active_default_binding_id" is not null
        and "agent_observability_organization_settings"."active_default_binding_scope" is not null
        and "agent_observability_organization_settings"."active_default_binding_scope" = 'organization'
      )),
	CONSTRAINT "agent_observability_organization_settings_capture_check" CHECK ("agent_observability_organization_settings"."capture_ceiling" in ('metadata_only', 'redacted_io')),
	CONSTRAINT "agent_observability_organization_settings_epoch_check" CHECK ("agent_observability_organization_settings"."selection_epoch" >= 0
          and "agent_observability_organization_settings"."default_revocation_epoch" >= 0
          and "agent_observability_organization_settings"."organization_revocation_epoch" >= 0
          and "agent_observability_organization_settings"."capture_restriction_epoch" >= 0)
);
--> statement-breakpoint
CREATE TABLE "agent_observability_platform_policy" (
	"id" text PRIMARY KEY DEFAULT 'default' NOT NULL,
	"allowed_adapters" text[] DEFAULT '{"otlp_http"}' NOT NULL,
	"allowed_endpoint_classes" text[] DEFAULT '{"public"}' NOT NULL,
	"max_capture_mode" text DEFAULT 'metadata_only' NOT NULL,
	"capture_restriction_epoch" bigint DEFAULT 0 NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "agent_observability_platform_policy_singleton_check" CHECK ("agent_observability_platform_policy"."id" = 'default'),
	CONSTRAINT "agent_observability_platform_policy_adapters_check" CHECK (cardinality("agent_observability_platform_policy"."allowed_adapters") > 0
          and "agent_observability_platform_policy"."allowed_adapters" <@ ARRAY['otlp_http', 'langfuse_sdk']::text[]),
	CONSTRAINT "agent_observability_platform_policy_endpoints_check" CHECK (cardinality("agent_observability_platform_policy"."allowed_endpoint_classes") > 0
          and "agent_observability_platform_policy"."allowed_endpoint_classes" <@ ARRAY['public', 'private']::text[]),
	CONSTRAINT "agent_observability_platform_policy_capture_check" CHECK ("agent_observability_platform_policy"."max_capture_mode" in ('metadata_only', 'redacted_io')),
	CONSTRAINT "agent_observability_platform_policy_capture_epoch_check" CHECK ("agent_observability_platform_policy"."capture_restriction_epoch" >= 0)
);
--> statement-breakpoint
CREATE TABLE "agent_observability_workspace_settings" (
	"workspace_id" text PRIMARY KEY NOT NULL,
	"organization_id" text NOT NULL,
	"mode" text DEFAULT 'inherit' NOT NULL,
	"binding_id" text,
	"selection_epoch" bigint DEFAULT 0 NOT NULL,
	"revocation_epoch" bigint DEFAULT 0 NOT NULL,
	"capture_ceiling" text DEFAULT 'metadata_only' NOT NULL,
	"capture_restriction_epoch" bigint DEFAULT 0 NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "agent_observability_workspace_settings_mode_binding_check" CHECK ((
        "agent_observability_workspace_settings"."mode" = 'custom' and "agent_observability_workspace_settings"."binding_id" is not null
      ) or (
        "agent_observability_workspace_settings"."mode" in ('inherit', 'disabled') and "agent_observability_workspace_settings"."binding_id" is null
      )),
	CONSTRAINT "agent_observability_workspace_settings_capture_check" CHECK ("agent_observability_workspace_settings"."capture_ceiling" in ('metadata_only', 'redacted_io')),
	CONSTRAINT "agent_observability_workspace_settings_epoch_check" CHECK ("agent_observability_workspace_settings"."selection_epoch" >= 0
          and "agent_observability_workspace_settings"."revocation_epoch" >= 0
          and "agent_observability_workspace_settings"."capture_restriction_epoch" >= 0)
);
--> statement-breakpoint
CREATE TABLE "session_observability_bindings" (
	"workspace_id" text NOT NULL,
	"session_id" text NOT NULL,
	"organization_id" text NOT NULL,
	"binding_id" text,
	"binding_version" integer,
	"binding_scope" text,
	"binding_workspace_id" text,
	"selection_source" text DEFAULT 'disabled' NOT NULL,
	"status" text DEFAULT 'disabled' NOT NULL,
	"organization_selection_epoch" bigint DEFAULT 0 NOT NULL,
	"workspace_selection_epoch" bigint DEFAULT 0 NOT NULL,
	"organization_default_revocation_epoch" bigint DEFAULT 0 NOT NULL,
	"organization_revocation_epoch" bigint DEFAULT 0 NOT NULL,
	"workspace_revocation_epoch" bigint DEFAULT 0 NOT NULL,
	"binding_revocation_epoch" bigint DEFAULT 0 NOT NULL,
	"platform_capture_restriction_epoch" bigint DEFAULT 0 NOT NULL,
	"organization_capture_restriction_epoch" bigint DEFAULT 0 NOT NULL,
	"workspace_capture_restriction_epoch" bigint DEFAULT 0 NOT NULL,
	"effective_capture_mode" text DEFAULT 'metadata_only' NOT NULL,
	"session_revocation_epoch" bigint DEFAULT 0 NOT NULL,
	"agent_id" text NOT NULL,
	"agent_version" integer NOT NULL,
	"harness" text,
	"harness_mode" text,
	"archived_at" timestamp with time zone,
	"deleted_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "session_observability_bindings_workspace_session_pk" PRIMARY KEY("workspace_id","session_id"),
	CONSTRAINT "session_observability_bindings_selection_check" CHECK ((
        "session_observability_bindings"."selection_source" = 'disabled'
        and "session_observability_bindings"."status" in ('disabled', 'archived', 'deleted')
        and "session_observability_bindings"."binding_id" is null
        and "session_observability_bindings"."binding_version" is null
        and "session_observability_bindings"."binding_scope" is null
        and "session_observability_bindings"."binding_workspace_id" is null
        and "session_observability_bindings"."effective_capture_mode" = 'metadata_only'
      ) or (
        "session_observability_bindings"."selection_source" = 'organization_default'
        and "session_observability_bindings"."status" in ('active', 'archived', 'deleted')
        and "session_observability_bindings"."binding_id" is not null
        and "session_observability_bindings"."binding_version" is not null and "session_observability_bindings"."binding_version" > 0
        and "session_observability_bindings"."binding_scope" is not null
        and "session_observability_bindings"."binding_scope" = 'organization'
        and "session_observability_bindings"."binding_workspace_id" is null
      ) or (
        "session_observability_bindings"."selection_source" = 'workspace_custom'
        and "session_observability_bindings"."status" in ('active', 'archived', 'deleted')
        and "session_observability_bindings"."binding_id" is not null
        and "session_observability_bindings"."binding_version" is not null and "session_observability_bindings"."binding_version" > 0
        and "session_observability_bindings"."binding_scope" is not null
        and "session_observability_bindings"."binding_scope" = 'workspace'
        and "session_observability_bindings"."binding_workspace_id" is not null
        and "session_observability_bindings"."binding_workspace_id" = "session_observability_bindings"."workspace_id"
      )),
	CONSTRAINT "session_observability_bindings_status_check" CHECK ("session_observability_bindings"."status" in ('active', 'disabled', 'archived', 'deleted')),
	CONSTRAINT "session_observability_bindings_capture_check" CHECK ("session_observability_bindings"."effective_capture_mode" in ('metadata_only', 'redacted_io')),
	CONSTRAINT "session_observability_bindings_lifecycle_check" CHECK ((
        "session_observability_bindings"."status" = 'archived'
        and "session_observability_bindings"."archived_at" is not null
        and "session_observability_bindings"."deleted_at" is null
      ) or (
        "session_observability_bindings"."status" = 'deleted'
        and "session_observability_bindings"."deleted_at" is not null
      ) or (
        "session_observability_bindings"."status" in ('active', 'disabled')
        and "session_observability_bindings"."archived_at" is null
        and "session_observability_bindings"."deleted_at" is null
      )),
	CONSTRAINT "session_observability_bindings_epoch_check" CHECK ("session_observability_bindings"."organization_selection_epoch" >= 0
          and "session_observability_bindings"."workspace_selection_epoch" >= 0
          and "session_observability_bindings"."organization_default_revocation_epoch" >= 0
          and "session_observability_bindings"."organization_revocation_epoch" >= 0
          and "session_observability_bindings"."workspace_revocation_epoch" >= 0
          and "session_observability_bindings"."binding_revocation_epoch" >= 0
          and "session_observability_bindings"."platform_capture_restriction_epoch" >= 0
          and "session_observability_bindings"."organization_capture_restriction_epoch" >= 0
          and "session_observability_bindings"."workspace_capture_restriction_epoch" >= 0
          and "session_observability_bindings"."session_revocation_epoch" >= 0
          and "session_observability_bindings"."agent_version" > 0)
);
--> statement-breakpoint
CREATE UNIQUE INDEX "agent_observability_bindings_organization_scope_idx" ON "agent_observability_bindings" USING btree ("organization_id","id","scope_type");--> statement-breakpoint
CREATE UNIQUE INDEX "agent_observability_bindings_organization_workspace_idx" ON "agent_observability_bindings" USING btree ("organization_id","workspace_id","id");--> statement-breakpoint
CREATE UNIQUE INDEX "agent_observability_bindings_id_adapter_idx" ON "agent_observability_bindings" USING btree ("id","adapter_type");--> statement-breakpoint
CREATE INDEX "agent_observability_bindings_selectable_idx" ON "agent_observability_bindings" USING btree ("organization_id","workspace_id","status");--> statement-breakpoint
CREATE UNIQUE INDEX "agent_observability_workspace_settings_organization_id_idx" ON "agent_observability_workspace_settings" USING btree ("organization_id","workspace_id");--> statement-breakpoint
CREATE INDEX "session_observability_bindings_binding_idx" ON "session_observability_bindings" USING btree ("binding_id","binding_version");--> statement-breakpoint
CREATE INDEX "session_observability_bindings_organization_idx" ON "session_observability_bindings" USING btree ("organization_id","status");--> statement-breakpoint
ALTER TABLE "agent_observability_binding_credentials" ADD CONSTRAINT "agent_observability_binding_credentials_binding_fk" FOREIGN KEY ("binding_id") REFERENCES "public"."agent_observability_bindings"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_binding_versions" ADD CONSTRAINT "agent_observability_binding_versions_binding_fk" FOREIGN KEY ("binding_id") REFERENCES "public"."agent_observability_bindings"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_binding_versions" ADD CONSTRAINT "agent_observability_binding_versions_adapter_fk" FOREIGN KEY ("binding_id","adapter_type") REFERENCES "public"."agent_observability_bindings"("id","adapter_type") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_bindings" ADD CONSTRAINT "agent_observability_bindings_organization_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_bindings" ADD CONSTRAINT "agent_observability_bindings_workspace_owner_fk" FOREIGN KEY ("organization_id","workspace_id") REFERENCES "public"."workspaces"("organization_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_bindings" ADD CONSTRAINT "agent_observability_bindings_current_version_fk" FOREIGN KEY ("id","current_version") REFERENCES "public"."agent_observability_binding_versions"("binding_id","version") ON DELETE restrict ON UPDATE no action DEFERRABLE INITIALLY DEFERRED;--> statement-breakpoint
ALTER TABLE "agent_observability_organization_settings" ADD CONSTRAINT "agent_observability_organization_settings_organization_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_organization_settings" ADD CONSTRAINT "agent_observability_organization_settings_binding_fk" FOREIGN KEY ("organization_id","active_default_binding_id","active_default_binding_scope") REFERENCES "public"."agent_observability_bindings"("organization_id","id","scope_type") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_workspace_settings" ADD CONSTRAINT "agent_observability_workspace_settings_workspace_fk" FOREIGN KEY ("organization_id","workspace_id") REFERENCES "public"."workspaces"("organization_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_workspace_settings" ADD CONSTRAINT "agent_observability_workspace_settings_binding_fk" FOREIGN KEY ("organization_id","workspace_id","binding_id") REFERENCES "public"."agent_observability_bindings"("organization_id","workspace_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_observability_bindings" ADD CONSTRAINT "session_observability_bindings_workspace_fk" FOREIGN KEY ("organization_id","workspace_id") REFERENCES "public"."workspaces"("organization_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_observability_bindings" ADD CONSTRAINT "session_observability_bindings_binding_scope_fk" FOREIGN KEY ("organization_id","binding_id","binding_scope") REFERENCES "public"."agent_observability_bindings"("organization_id","id","scope_type") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_observability_bindings" ADD CONSTRAINT "session_observability_bindings_binding_workspace_fk" FOREIGN KEY ("organization_id","binding_id","binding_workspace_id") REFERENCES "public"."agent_observability_bindings"("organization_id","id","workspace_id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_observability_bindings" ADD CONSTRAINT "session_observability_bindings_binding_version_fk" FOREIGN KEY ("binding_id","binding_version") REFERENCES "public"."agent_observability_binding_versions"("binding_id","version") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE FUNCTION "agent_observability_provision_organization_settings"() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO "agent_observability_organization_settings" (
    "organization_id", "created_at", "updated_at"
  ) VALUES (NEW."id", NEW."created_at", NEW."updated_at")
  ON CONFLICT ("organization_id") DO NOTHING;
  RETURN NEW;
END;
$$;--> statement-breakpoint
CREATE TRIGGER "agent_observability_organizations_provision_settings"
AFTER INSERT ON "organizations"
FOR EACH ROW EXECUTE FUNCTION "agent_observability_provision_organization_settings"();--> statement-breakpoint
CREATE FUNCTION "agent_observability_provision_workspace_settings"() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO "agent_observability_workspace_settings" (
    "workspace_id", "organization_id", "created_at", "updated_at"
  ) VALUES (NEW."id", NEW."organization_id", NEW."created_at", NEW."updated_at")
  ON CONFLICT ("workspace_id") DO NOTHING;
  RETURN NEW;
END;
$$;--> statement-breakpoint
CREATE TRIGGER "agent_observability_workspaces_provision_settings"
AFTER INSERT ON "workspaces"
FOR EACH ROW EXECUTE FUNCTION "agent_observability_provision_workspace_settings"();--> statement-breakpoint
-- observability-backfill-start
INSERT INTO "agent_observability_platform_policy" (
	"id", "allowed_adapters", "allowed_endpoint_classes", "max_capture_mode", "capture_restriction_epoch"
) VALUES (
	'default', ARRAY['otlp_http']::text[], ARRAY['public']::text[], 'metadata_only', 0
) ON CONFLICT ("id") DO NOTHING;--> statement-breakpoint
INSERT INTO "agent_observability_organization_settings" (
	"organization_id", "active_default_binding_id", "active_default_binding_scope",
	"selection_epoch", "default_revocation_epoch", "organization_revocation_epoch",
	"capture_ceiling", "capture_restriction_epoch"
)
SELECT "id", NULL, NULL, 0, 0, 0, 'metadata_only', 0
FROM "organizations"
ON CONFLICT ("organization_id") DO NOTHING;--> statement-breakpoint
INSERT INTO "agent_observability_workspace_settings" (
	"workspace_id", "organization_id", "mode", "binding_id", "selection_epoch",
	"revocation_epoch", "capture_ceiling", "capture_restriction_epoch"
)
SELECT "id", "organization_id", 'inherit', NULL, 0, 0, 'metadata_only', 0
FROM "workspaces"
ON CONFLICT ("workspace_id") DO NOTHING;--> statement-breakpoint
INSERT INTO "session_observability_bindings" (
	"workspace_id", "session_id", "organization_id", "selection_source", "status",
	"agent_id", "agent_version", "created_at", "updated_at"
)
SELECT s."workspace_id", s."id", w."organization_id", 'disabled', 'disabled',
	s."agent_id", s."agent_version", s."created_at", s."updated_at"
FROM "sessions" AS s
INNER JOIN "workspaces" AS w ON w."id" = s."workspace_id"
ON CONFLICT ("workspace_id", "session_id") DO NOTHING;
-- observability-backfill-end
