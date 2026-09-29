-- Pre-release breaking reset: legacy skill rows embedded executable content in
-- Postgres and cannot be converted into immutable SkillStore bundles without
-- an external object write inside this migration.
UPDATE "agents" SET "skills" = '[]'::jsonb WHERE "skills" <> '[]'::jsonb;--> statement-breakpoint
UPDATE "agent_versions" SET "snapshot" = jsonb_set("snapshot", '{skills}', '[]'::jsonb) WHERE "snapshot" ? 'skills' AND "snapshot"->'skills' <> '[]'::jsonb;--> statement-breakpoint
UPDATE "sessions" SET "agent_overrides" = "agent_overrides" - 'skills' WHERE "agent_overrides" ? 'skills';--> statement-breakpoint
UPDATE "skills" SET "latest_version_id" = NULL;--> statement-breakpoint
DELETE FROM "skill_versions";--> statement-breakpoint
DELETE FROM "skills";--> statement-breakpoint
CREATE TABLE "session_skill_bindings" (
	"workspace_id" text NOT NULL,
	"session_id" text NOT NULL,
	"agent_id" text NOT NULL,
	"agent_version" integer NOT NULL,
	"ordinal" integer NOT NULL,
	"skill_version_id" text NOT NULL,
	"bundle_sha256" text NOT NULL,
	CONSTRAINT "session_skill_bindings_pk" PRIMARY KEY("workspace_id","session_id","agent_id","agent_version","ordinal"),
	CONSTRAINT "session_skill_bindings_ordinal_check" CHECK ("session_skill_bindings"."ordinal" >= 0)
);
--> statement-breakpoint
ALTER TABLE "skill_versions" ADD COLUMN "entrypoint" text DEFAULT 'SKILL.md' NOT NULL;--> statement-breakpoint
ALTER TABLE "skill_versions" ADD COLUMN "package_sha256" text NOT NULL;--> statement-breakpoint
ALTER TABLE "skill_versions" ADD COLUMN "package_size_bytes" integer NOT NULL;--> statement-breakpoint
ALTER TABLE "skill_versions" ADD COLUMN "package_manifest" jsonb DEFAULT '[]'::jsonb NOT NULL;--> statement-breakpoint
ALTER TABLE "skill_versions" ADD CONSTRAINT "skill_versions_entrypoint_check" CHECK ("skill_versions"."entrypoint" = 'SKILL.md');--> statement-breakpoint
ALTER TABLE "skill_versions" ADD CONSTRAINT "skill_versions_package_sha256_check" CHECK ("skill_versions"."package_sha256" ~ '^[0-9a-f]{64}$');--> statement-breakpoint
ALTER TABLE "skill_versions" ADD CONSTRAINT "skill_versions_package_size_check" CHECK ("skill_versions"."package_size_bytes" > 0);--> statement-breakpoint
CREATE UNIQUE INDEX "skill_versions_workspace_bundle_idx" ON "skill_versions" USING btree ("workspace_id","id","package_sha256");--> statement-breakpoint
ALTER TABLE "session_skill_bindings" ADD CONSTRAINT "session_skill_bindings_session_fk" FOREIGN KEY ("workspace_id","session_id") REFERENCES "public"."sessions"("workspace_id","id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_skill_bindings" ADD CONSTRAINT "session_skill_bindings_agent_version_fk" FOREIGN KEY ("workspace_id","agent_id","agent_version") REFERENCES "public"."agent_versions"("workspace_id","agent_id","version") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "session_skill_bindings" ADD CONSTRAINT "session_skill_bindings_bundle_fk" FOREIGN KEY ("workspace_id","skill_version_id","bundle_sha256") REFERENCES "public"."skill_versions"("workspace_id","id","package_sha256") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "session_skill_bindings_skill_idx" ON "session_skill_bindings" USING btree ("workspace_id","session_id","agent_id","agent_version","skill_version_id");--> statement-breakpoint
CREATE INDEX "session_skill_bindings_session_idx" ON "session_skill_bindings" USING btree ("workspace_id","session_id");--> statement-breakpoint
ALTER TABLE "skill_versions" DROP COLUMN "system_prompt";--> statement-breakpoint
ALTER TABLE "skill_versions" DROP COLUMN "tool_allowlist";--> statement-breakpoint
ALTER TABLE "skill_versions" DROP COLUMN "examples";--> statement-breakpoint
ALTER TABLE "skill_versions" DROP COLUMN "metadata";--> statement-breakpoint
ALTER TABLE "skill_versions" DROP COLUMN "content_files";
