ALTER TABLE "session_events_index" ADD COLUMN "processed_at" text;
--> statement-breakpoint
UPDATE "session_events_index" SET "processed_at" = "produced_at" WHERE "processed_at" IS NULL;
