CREATE TABLE "organizations" (
	"id" text PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"status" text DEFAULT 'active' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "organizations_status_check" CHECK ("organizations"."status" in ('active', 'archived'))
);
--> statement-breakpoint
CREATE TABLE "workspaces" (
	"id" text PRIMARY KEY NOT NULL,
	"organization_id" text NOT NULL,
	"name" text NOT NULL,
	"status" text DEFAULT 'active' NOT NULL,
	"created_by" text NOT NULL,
	"archived_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "workspaces_status_check" CHECK ("workspaces"."status" in ('active', 'archived'))
);
--> statement-breakpoint
ALTER TABLE "api_keys" ADD COLUMN "name" text DEFAULT 'API key' NOT NULL;--> statement-breakpoint
ALTER TABLE "api_keys" ADD COLUMN "partial_key_hint" text DEFAULT '' NOT NULL;--> statement-breakpoint
ALTER TABLE "api_keys" ADD COLUMN "status" text DEFAULT 'active' NOT NULL;--> statement-breakpoint
ALTER TABLE "api_keys" ADD COLUMN "expires_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "api_keys" ADD COLUMN "created_by" text DEFAULT 'bootstrap' NOT NULL;--> statement-breakpoint
ALTER TABLE "api_keys" ADD COLUMN "updated_at" timestamp with time zone DEFAULT now() NOT NULL;--> statement-breakpoint
ALTER TABLE "workspaces" ADD CONSTRAINT "workspaces_organization_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "workspaces_organization_id_idx" ON "workspaces" USING btree ("organization_id","id");--> statement-breakpoint
CREATE INDEX "workspaces_organization_status_idx" ON "workspaces" USING btree ("organization_id","status","created_at");--> statement-breakpoint
WITH legacy_workspaces AS (
	SELECT "workspace_id" FROM "agents"
	UNION SELECT "workspace_id" FROM "environments"
	UNION SELECT "workspace_id" FROM "sessions"
	UNION SELECT "workspace_id" FROM "session_events_index"
	UNION SELECT "workspace_id" FROM "vaults"
	UNION SELECT "workspace_id" FROM "git_credentials"
	UNION SELECT "workspace_id" FROM "git_credential_staging_intents"
	UNION SELECT "workspace_id" FROM "skills"
	UNION SELECT "workspace_id" FROM "api_keys"
	UNION SELECT "workspace_id" FROM "idempotency_keys"
)
INSERT INTO "organizations" ("id", "name", "status")
SELECT 'org_legacy_default', 'Legacy organization', 'active'
WHERE EXISTS (SELECT 1 FROM legacy_workspaces);--> statement-breakpoint
WITH legacy_workspaces AS (
	SELECT "workspace_id" FROM "agents"
	UNION SELECT "workspace_id" FROM "environments"
	UNION SELECT "workspace_id" FROM "sessions"
	UNION SELECT "workspace_id" FROM "session_events_index"
	UNION SELECT "workspace_id" FROM "vaults"
	UNION SELECT "workspace_id" FROM "git_credentials"
	UNION SELECT "workspace_id" FROM "git_credential_staging_intents"
	UNION SELECT "workspace_id" FROM "skills"
	UNION SELECT "workspace_id" FROM "api_keys"
	UNION SELECT "workspace_id" FROM "idempotency_keys"
)
INSERT INTO "workspaces" ("id", "organization_id", "name", "status", "created_by")
SELECT "workspace_id", 'org_legacy_default', "workspace_id", 'active', 'migration-0025'
FROM legacy_workspaces;--> statement-breakpoint
ALTER TABLE "agents" ADD CONSTRAINT "agents_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "api_keys" ADD CONSTRAINT "api_keys_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "environments" ADD CONSTRAINT "environments_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "git_credential_staging_intents" ADD CONSTRAINT "git_credential_staging_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "git_credentials" ADD CONSTRAINT "git_credentials_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "idempotency_keys" ADD CONSTRAINT "idempotency_keys_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_events_index" ADD CONSTRAINT "session_events_index_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "sessions" ADD CONSTRAINT "sessions_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "skills" ADD CONSTRAINT "skills_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "vaults" ADD CONSTRAINT "vaults_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "api_keys" ADD CONSTRAINT "api_keys_status_check" CHECK ("api_keys"."status" in ('active', 'inactive', 'archived'));
