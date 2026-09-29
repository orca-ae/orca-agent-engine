DROP INDEX "git_credentials_active_repo_idx";--> statement-breakpoint
ALTER TABLE "git_credentials" ADD COLUMN "session_resource_id" text;--> statement-breakpoint
ALTER TABLE "session_resources" ADD COLUMN "updated_at" timestamp with time zone;--> statement-breakpoint
UPDATE "session_resources" SET "updated_at" = "attached_at" WHERE "updated_at" IS NULL;--> statement-breakpoint
ALTER TABLE "session_resources" ALTER COLUMN "updated_at" SET DEFAULT now();--> statement-breakpoint
ALTER TABLE "session_resources" ALTER COLUMN "updated_at" SET NOT NULL;--> statement-breakpoint
CREATE INDEX "git_credentials_repo_idx" ON "git_credentials" USING btree ("workspace_id","repo_url","archived_at");--> statement-breakpoint
CREATE UNIQUE INDEX "git_credentials_active_repo_idx" ON "git_credentials" USING btree ("workspace_id","repo_url") WHERE "git_credentials"."archived_at" is null and "git_credentials"."session_resource_id" is null;--> statement-breakpoint
CREATE UNIQUE INDEX "git_credentials_session_resource_idx" ON "git_credentials" USING btree ("session_resource_id") WHERE "git_credentials"."session_resource_id" is not null;
