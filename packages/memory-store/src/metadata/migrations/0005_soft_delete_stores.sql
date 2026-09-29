ALTER TABLE "memory_stores" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "memory_versions" ADD COLUMN "deleted_at" timestamp with time zone;