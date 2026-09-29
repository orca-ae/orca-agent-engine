ALTER TABLE "files" ADD COLUMN "purpose" text DEFAULT 'agent' NOT NULL;--> statement-breakpoint
ALTER TABLE "files" ADD COLUMN "scope_id" text;--> statement-breakpoint
ALTER TABLE "files" ADD COLUMN "downloadable" boolean DEFAULT false NOT NULL;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "files_ws_scope_idx" ON "files" USING btree ("workspace_id","scope_id");