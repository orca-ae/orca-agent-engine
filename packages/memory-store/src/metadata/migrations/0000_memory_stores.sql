CREATE TABLE "memories" (
	"id" text PRIMARY KEY NOT NULL,
	"store_id" text NOT NULL,
	"path" text NOT NULL,
	"current_sha256" text NOT NULL,
	"size_bytes" bigint NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_by_session_id" text,
	"updated_by_event_id" text
);
--> statement-breakpoint
CREATE TABLE "memory_stores" (
	"id" text PRIMARY KEY NOT NULL,
	"workspace_id" text NOT NULL,
	"name" text NOT NULL,
	"description" text,
	"archived_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "memory_versions" (
	"id" text PRIMARY KEY NOT NULL,
	"store_id" text NOT NULL,
	"memory_id" text NOT NULL,
	"path" text NOT NULL,
	"sha256" text NOT NULL,
	"size_bytes" bigint NOT NULL,
	"written_by_session_id" text,
	"written_by_event_id" text,
	"written_at" timestamp with time zone DEFAULT now() NOT NULL,
	"redacted_at" timestamp with time zone
);
--> statement-breakpoint
CREATE UNIQUE INDEX "memories_store_path_idx" ON "memories" USING btree ("store_id","path");--> statement-breakpoint
CREATE INDEX "memory_stores_ws_idx" ON "memory_stores" USING btree ("workspace_id");--> statement-breakpoint
CREATE UNIQUE INDEX "memory_stores_ws_name_idx" ON "memory_stores" USING btree ("workspace_id","name");--> statement-breakpoint
CREATE INDEX "memory_versions_store_idx" ON "memory_versions" USING btree ("store_id","written_at");--> statement-breakpoint
CREATE INDEX "memory_versions_memory_idx" ON "memory_versions" USING btree ("memory_id","written_at");