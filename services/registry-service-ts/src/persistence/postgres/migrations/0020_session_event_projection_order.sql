DROP INDEX "session_events_index_public_seq_idx";--> statement-breakpoint
ALTER TABLE "session_events_index" ADD COLUMN "projection_ordinal" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "session_events_index" ADD COLUMN "projection_version" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
CREATE INDEX "session_events_index_public_seq_idx" ON "session_events_index" USING btree ("workspace_id","session_id","visibility","subpath","seq","projection_ordinal","event_id");
