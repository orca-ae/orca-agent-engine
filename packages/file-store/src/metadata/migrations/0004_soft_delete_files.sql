DROP INDEX "files_ws_sha256_agent_idx";--> statement-breakpoint
ALTER TABLE "files" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
CREATE UNIQUE INDEX "files_ws_sha256_agent_idx" ON "files" USING btree ("workspace_id","sha256") WHERE purpose = 'agent' AND archived_at IS NULL AND deleted_at IS NULL;