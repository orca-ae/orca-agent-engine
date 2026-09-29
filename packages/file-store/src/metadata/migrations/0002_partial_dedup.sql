DROP INDEX IF EXISTS "files_ws_sha256_idx";--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "files_ws_sha256_agent_idx" ON "files" USING btree ("workspace_id","sha256") WHERE purpose = 'agent';
