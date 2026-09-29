CREATE TABLE "platform_api_keys" (
	"id" text PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"hashed_key" text NOT NULL,
	"key_fingerprint" text NOT NULL,
	"partial_key_hint" text NOT NULL,
	"scopes" text[] DEFAULT '{}' NOT NULL,
	"status" text DEFAULT 'active' NOT NULL,
	"expires_at" timestamp with time zone,
	"last_used_at" timestamp with time zone,
	"revoked_at" timestamp with time zone,
	"created_by" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "platform_api_keys_status_check" CHECK ("platform_api_keys"."status" in ('active', 'inactive', 'archived'))
);
--> statement-breakpoint
CREATE TABLE "platform_audit_events" (
	"id" text PRIMARY KEY NOT NULL,
	"organization_id" text,
	"workspace_id" text,
	"actor" text NOT NULL,
	"auth_method" text NOT NULL,
	"action" text NOT NULL,
	"target_type" text NOT NULL,
	"target_id" text NOT NULL,
	"request_id" text NOT NULL,
	"result" text NOT NULL,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "platform_idempotency_keys" (
	"principal" text NOT NULL,
	"scope" text NOT NULL,
	"key" text NOT NULL,
	"response_status" integer NOT NULL,
	"response_body" text NOT NULL,
	"body_hash" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "platform_idempotency_keys_principal_scope_key_pk" PRIMARY KEY("principal","scope","key")
);
--> statement-breakpoint
ALTER TABLE "platform_audit_events" ADD CONSTRAINT "platform_audit_events_organization_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "platform_audit_events" ADD CONSTRAINT "platform_audit_events_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "platform_api_keys_status_idx" ON "platform_api_keys" USING btree ("status");--> statement-breakpoint
CREATE UNIQUE INDEX "platform_api_keys_fingerprint_idx" ON "platform_api_keys" USING btree ("key_fingerprint");--> statement-breakpoint
CREATE INDEX "platform_audit_events_organization_idx" ON "platform_audit_events" USING btree ("organization_id","created_at");--> statement-breakpoint
CREATE INDEX "platform_audit_events_workspace_idx" ON "platform_audit_events" USING btree ("workspace_id","created_at");--> statement-breakpoint
CREATE INDEX "platform_idempotency_keys_expires_idx" ON "platform_idempotency_keys" USING btree ("expires_at");