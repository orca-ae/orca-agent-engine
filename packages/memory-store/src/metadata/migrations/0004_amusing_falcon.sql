ALTER TABLE "memory_versions" ADD COLUMN "written_by_api_key_id" text;--> statement-breakpoint
ALTER TABLE "memory_versions" ADD COLUMN "written_by_user_id" text;--> statement-breakpoint
ALTER TABLE "memory_versions" ADD COLUMN "redacted_by_session_id" text;--> statement-breakpoint
ALTER TABLE "memory_versions" ADD COLUMN "redacted_by_api_key_id" text;--> statement-breakpoint
ALTER TABLE "memory_versions" ADD COLUMN "redacted_by_user_id" text;