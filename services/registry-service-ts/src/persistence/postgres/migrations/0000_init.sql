CREATE TABLE "agent_versions" (
	"id" text PRIMARY KEY NOT NULL,
	"agent_id" text NOT NULL,
	"version" integer NOT NULL,
	"snapshot" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "agents" (
	"id" text PRIMARY KEY NOT NULL,
	"workspace_id" text NOT NULL,
	"name" text NOT NULL,
	"version" integer DEFAULT 1 NOT NULL,
	"latest_version_id" text,
	"model_provider" text NOT NULL,
	"model_id" text NOT NULL,
	"system" text DEFAULT '',
	"tools" jsonb DEFAULT '[]'::jsonb NOT NULL,
	"mcp_servers" jsonb DEFAULT '[]'::jsonb NOT NULL,
	"skills" jsonb DEFAULT '[]'::jsonb NOT NULL,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"archived_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "api_keys" (
	"id" text PRIMARY KEY NOT NULL,
	"workspace_id" text NOT NULL,
	"hashed_key" text NOT NULL,
	"principal" text NOT NULL,
	"scopes" text[] DEFAULT '{}' NOT NULL,
	"last_used_at" timestamp with time zone,
	"revoked_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "environments" (
	"id" text PRIMARY KEY NOT NULL,
	"workspace_id" text NOT NULL,
	"name" text NOT NULL,
	"packages" jsonb DEFAULT '[]'::jsonb NOT NULL,
	"networking" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"archived_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "idempotency_keys" (
	"workspace_id" text NOT NULL,
	"scope" text NOT NULL,
	"key" text NOT NULL,
	"response_status" integer NOT NULL,
	"response_body" text NOT NULL,
	"body_hash" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "idempotency_keys_workspace_id_scope_key_pk" PRIMARY KEY("workspace_id","scope","key")
);
--> statement-breakpoint
CREATE TABLE "session_resources" (
	"id" text PRIMARY KEY NOT NULL,
	"session_id" text NOT NULL,
	"type" text NOT NULL,
	"file_id" text,
	"memory_store_id" text,
	"repo_ref" jsonb,
	"mount_path" text NOT NULL,
	"access" text NOT NULL,
	"instructions" text,
	"attached_at" timestamp with time zone DEFAULT now() NOT NULL,
	"detached_at" timestamp with time zone
);
--> statement-breakpoint
CREATE TABLE "sessions" (
	"id" text PRIMARY KEY NOT NULL,
	"workspace_id" text NOT NULL,
	"agent_id" text NOT NULL,
	"agent_version" integer NOT NULL,
	"environment_id" text,
	"vault_ids" text[] DEFAULT '{}' NOT NULL,
	"status" text DEFAULT 'idle' NOT NULL,
	"last_event_seq" bigint DEFAULT 0 NOT NULL,
	"sandbox_handle_id" text,
	"started_at" timestamp with time zone,
	"last_active_at" timestamp with time zone,
	"archived_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "skill_versions" (
	"id" text PRIMARY KEY NOT NULL,
	"skill_id" text NOT NULL,
	"version" integer NOT NULL,
	"system_prompt" text,
	"tool_allowlist" text[],
	"examples" jsonb DEFAULT '[]'::jsonb NOT NULL,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "skills" (
	"id" text PRIMARY KEY NOT NULL,
	"workspace_id" text NOT NULL,
	"name" text NOT NULL,
	"slug" text NOT NULL,
	"version" integer DEFAULT 1 NOT NULL,
	"latest_version_id" text,
	"description" text,
	"archived_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "vaults" (
	"id" text PRIMARY KEY NOT NULL,
	"workspace_id" text NOT NULL,
	"name" text NOT NULL,
	"target_kind" text NOT NULL,
	"target_url" text NOT NULL,
	"principal" text,
	"secret_ref" text NOT NULL,
	"archived_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX "agent_versions_agent_version_idx" ON "agent_versions" USING btree ("agent_id","version");--> statement-breakpoint
CREATE INDEX "agents_workspace_idx" ON "agents" USING btree ("workspace_id","archived_at");--> statement-breakpoint
CREATE INDEX "api_keys_workspace_idx" ON "api_keys" USING btree ("workspace_id","revoked_at");--> statement-breakpoint
CREATE INDEX "environments_workspace_idx" ON "environments" USING btree ("workspace_id","archived_at");--> statement-breakpoint
CREATE UNIQUE INDEX "environments_ws_name_idx" ON "environments" USING btree ("workspace_id","name");--> statement-breakpoint
CREATE INDEX "idempotency_keys_expires_idx" ON "idempotency_keys" USING btree ("expires_at");--> statement-breakpoint
CREATE INDEX "session_resources_session_idx" ON "session_resources" USING btree ("session_id","detached_at");--> statement-breakpoint
CREATE INDEX "sessions_workspace_idx" ON "sessions" USING btree ("workspace_id","archived_at");--> statement-breakpoint
CREATE UNIQUE INDEX "skill_versions_skill_version_idx" ON "skill_versions" USING btree ("skill_id","version");--> statement-breakpoint
CREATE UNIQUE INDEX "skills_ws_slug_idx" ON "skills" USING btree ("workspace_id","slug");--> statement-breakpoint
CREATE INDEX "skills_workspace_idx" ON "skills" USING btree ("workspace_id","archived_at");--> statement-breakpoint
CREATE INDEX "vaults_workspace_idx" ON "vaults" USING btree ("workspace_id","archived_at");--> statement-breakpoint
CREATE UNIQUE INDEX "vaults_ws_name_idx" ON "vaults" USING btree ("workspace_id","name");
