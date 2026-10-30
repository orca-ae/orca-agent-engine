CREATE TABLE "pricing_admin_idempotency" (
	"organization_id" text NOT NULL,
	"principal" text NOT NULL,
	"operation" text NOT NULL,
	"key" text NOT NULL,
	"body_hash" text NOT NULL,
	"response" jsonb NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	CONSTRAINT "pricing_admin_idempotency_organization_id_principal_operation_key_pk" PRIMARY KEY("organization_id","principal","operation","key")
);
--> statement-breakpoint
CREATE TABLE "pricing_catalog_heads" (
	"organization_id" text PRIMARY KEY NOT NULL,
	"generation" integer NOT NULL,
	"catalog_id" text NOT NULL,
	"document" jsonb NOT NULL
);
--> statement-breakpoint
CREATE TABLE "pricing_catalog_versions" (
	"organization_id" text NOT NULL,
	"catalog_id" text NOT NULL,
	"generation" integer NOT NULL,
	"digest" text NOT NULL,
	"document" jsonb NOT NULL,
	"published_at" timestamp with time zone NOT NULL,
	CONSTRAINT "pricing_catalog_versions_organization_id_catalog_id_pk" PRIMARY KEY("organization_id","catalog_id")
);
--> statement-breakpoint
CREATE TABLE "pricing_fetch_snapshots" (
	"workspace_id" text NOT NULL,
	"organization_id" text NOT NULL,
	"catalog_id" text NOT NULL,
	"document" jsonb NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	CONSTRAINT "pricing_fetch_snapshots_workspace_id_organization_id_catalog_id_pk" PRIMARY KEY("workspace_id","organization_id","catalog_id")
);
--> statement-breakpoint
CREATE TABLE "pricing_source_leases" (
	"organization_id" text DEFAULT '' NOT NULL,
	"provider" text NOT NULL,
	"source_id" text NOT NULL,
	"fence" integer NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	CONSTRAINT "pricing_source_leases_organization_id_provider_source_id_pk" PRIMARY KEY("organization_id","provider","source_id"),
	CONSTRAINT "pricing_source_leases_global_check" CHECK ("pricing_source_leases"."organization_id" = '')
);
--> statement-breakpoint
ALTER TABLE "session_usage_events" ADD COLUMN "pricing_receipt" jsonb;--> statement-breakpoint
CREATE INDEX "pricing_admin_idempotency_expiry_idx" ON "pricing_admin_idempotency" USING btree ("expires_at");--> statement-breakpoint
CREATE UNIQUE INDEX "pricing_catalog_versions_generation_idx" ON "pricing_catalog_versions" USING btree ("organization_id","generation");--> statement-breakpoint
CREATE INDEX "pricing_fetch_snapshots_expiry_idx" ON "pricing_fetch_snapshots" USING btree ("expires_at");