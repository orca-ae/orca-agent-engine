ALTER TABLE "vault_credentials" DROP CONSTRAINT "vault_credentials_vault_id_vaults_id_fk";
--> statement-breakpoint
DROP INDEX "agent_versions_agent_version_idx";--> statement-breakpoint
DROP INDEX "session_resources_session_idx";--> statement-breakpoint
DROP INDEX "skill_versions_skill_version_idx";--> statement-breakpoint
DROP INDEX "skill_versions_skill_version_identifier_idx";--> statement-breakpoint
DROP INDEX "vault_credentials_active_url_idx";--> statement-breakpoint
DROP INDEX "vault_credentials_active_secret_name_idx";--> statement-breakpoint
ALTER TABLE "agent_versions" ADD COLUMN "workspace_id" text;--> statement-breakpoint
UPDATE "agent_versions"
SET "workspace_id" = "agents"."workspace_id"
FROM "agents"
WHERE "agent_versions"."agent_id" = "agents"."id";--> statement-breakpoint
ALTER TABLE "agent_versions" ALTER COLUMN "workspace_id" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "session_resources" ADD COLUMN "workspace_id" text;--> statement-breakpoint
UPDATE "session_resources"
SET "workspace_id" = "sessions"."workspace_id"
FROM "sessions"
WHERE "session_resources"."session_id" = "sessions"."id";--> statement-breakpoint
ALTER TABLE "session_resources" ALTER COLUMN "workspace_id" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "sessions" ADD COLUMN "runtime_revision" integer DEFAULT 1 NOT NULL;--> statement-breakpoint
ALTER TABLE "skill_versions" ADD COLUMN "workspace_id" text;--> statement-breakpoint
UPDATE "skill_versions"
SET "workspace_id" = "skills"."workspace_id"
FROM "skills"
WHERE "skill_versions"."skill_id" = "skills"."id";--> statement-breakpoint
ALTER TABLE "skill_versions" ALTER COLUMN "workspace_id" SET NOT NULL;--> statement-breakpoint
CREATE UNIQUE INDEX "agents_workspace_id_idx" ON "agents" USING btree ("workspace_id","id");--> statement-breakpoint
CREATE UNIQUE INDEX "environments_workspace_id_idx" ON "environments" USING btree ("workspace_id","id");--> statement-breakpoint
CREATE UNIQUE INDEX "sessions_workspace_id_idx" ON "sessions" USING btree ("workspace_id","id");--> statement-breakpoint
CREATE UNIQUE INDEX "skills_workspace_id_idx" ON "skills" USING btree ("workspace_id","id");--> statement-breakpoint
CREATE UNIQUE INDEX "vaults_workspace_id_idx" ON "vaults" USING btree ("workspace_id","id");--> statement-breakpoint
CREATE UNIQUE INDEX "agent_versions_agent_version_idx" ON "agent_versions" USING btree ("workspace_id","agent_id","version");--> statement-breakpoint
ALTER TABLE "agent_versions" ADD CONSTRAINT "agent_versions_workspace_agent_fk" FOREIGN KEY ("workspace_id","agent_id") REFERENCES "public"."agents"("workspace_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_resources" ADD CONSTRAINT "session_resources_workspace_session_fk" FOREIGN KEY ("workspace_id","session_id") REFERENCES "public"."sessions"("workspace_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
DELETE FROM "session_threads"
WHERE NOT EXISTS (
	SELECT 1
	FROM "sessions"
	WHERE "sessions"."workspace_id" = "session_threads"."workspace_id"
		AND "sessions"."id" = "session_threads"."session_id"
);--> statement-breakpoint
ALTER TABLE "session_threads" ADD CONSTRAINT "session_threads_workspace_session_fk" FOREIGN KEY ("workspace_id","session_id") REFERENCES "public"."sessions"("workspace_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_threads" ADD CONSTRAINT "session_threads_workspace_agent_version_fk" FOREIGN KEY ("workspace_id","agent_id","agent_version") REFERENCES "public"."agent_versions"("workspace_id","agent_id","version") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "sessions" ADD CONSTRAINT "sessions_workspace_agent_version_fk" FOREIGN KEY ("workspace_id","agent_id","agent_version") REFERENCES "public"."agent_versions"("workspace_id","agent_id","version") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "sessions" ADD CONSTRAINT "sessions_workspace_environment_fk" FOREIGN KEY ("workspace_id","environment_id") REFERENCES "public"."environments"("workspace_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "skill_versions" ADD CONSTRAINT "skill_versions_workspace_skill_fk" FOREIGN KEY ("workspace_id","skill_id") REFERENCES "public"."skills"("workspace_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "vault_credentials" ADD CONSTRAINT "vault_credentials_workspace_vault_fk" FOREIGN KEY ("workspace_id","vault_id") REFERENCES "public"."vaults"("workspace_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "session_resources_session_idx" ON "session_resources" USING btree ("workspace_id","session_id","detached_at");--> statement-breakpoint
CREATE UNIQUE INDEX "skill_versions_skill_version_idx" ON "skill_versions" USING btree ("workspace_id","skill_id","version");--> statement-breakpoint
CREATE UNIQUE INDEX "skill_versions_skill_version_identifier_idx" ON "skill_versions" USING btree ("workspace_id","skill_id","version_identifier");--> statement-breakpoint
CREATE UNIQUE INDEX "vault_credentials_active_url_idx" ON "vault_credentials" USING btree ("workspace_id","vault_id","mcp_server_url") WHERE "vault_credentials"."archived_at" is null;--> statement-breakpoint
CREATE UNIQUE INDEX "vault_credentials_active_secret_name_idx" ON "vault_credentials" USING btree ("workspace_id","vault_id","secret_name") WHERE "vault_credentials"."archived_at" is null and "vault_credentials"."secret_name" is not null;--> statement-breakpoint
ALTER TABLE "api_keys" ADD COLUMN "key_fingerprint" text;--> statement-breakpoint
UPDATE "api_keys"
SET "key_fingerprint" = 'legacy:' || "id"
WHERE "key_fingerprint" IS NULL;--> statement-breakpoint
ALTER TABLE "api_keys" ALTER COLUMN "key_fingerprint" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "session_resources" ADD CONSTRAINT "session_resources_access_check" CHECK ("session_resources"."access" in ('read_only', 'read_write'));--> statement-breakpoint
CREATE UNIQUE INDEX "api_keys_fingerprint_idx" ON "api_keys" USING btree ("key_fingerprint");--> statement-breakpoint
CREATE UNIQUE INDEX "agent_versions_workspace_agent_id_idx" ON "agent_versions" USING btree ("workspace_id","agent_id","id");--> statement-breakpoint
ALTER TABLE "agents" ADD CONSTRAINT "agents_workspace_latest_version_fk" FOREIGN KEY ("workspace_id","id","latest_version_id") REFERENCES "public"."agent_versions"("workspace_id","agent_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "session_resources_workspace_id_idx" ON "session_resources" USING btree ("workspace_id","id");--> statement-breakpoint
DROP INDEX "git_credentials_session_resource_idx";--> statement-breakpoint
CREATE UNIQUE INDEX "git_credentials_session_resource_idx" ON "git_credentials" USING btree ("workspace_id","session_resource_id") WHERE "git_credentials"."session_resource_id" is not null;--> statement-breakpoint
ALTER TABLE "git_credentials" ADD CONSTRAINT "git_credentials_workspace_session_resource_fk" FOREIGN KEY ("workspace_id","session_resource_id") REFERENCES "public"."session_resources"("workspace_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "skill_versions_workspace_skill_id_idx" ON "skill_versions" USING btree ("workspace_id","skill_id","id");--> statement-breakpoint
ALTER TABLE "skills" ADD CONSTRAINT "skills_workspace_latest_version_fk" FOREIGN KEY ("workspace_id","id","latest_version_id") REFERENCES "public"."skill_versions"("workspace_id","skill_id","id") ON DELETE restrict ON UPDATE no action;
