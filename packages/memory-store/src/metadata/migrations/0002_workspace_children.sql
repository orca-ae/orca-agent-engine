ALTER TABLE "memories" ADD COLUMN "workspace_id" text;
--> statement-breakpoint
UPDATE "memories"
SET "workspace_id" = "memory_stores"."workspace_id"
FROM "memory_stores"
WHERE "memories"."store_id" = "memory_stores"."id";
--> statement-breakpoint
ALTER TABLE "memories" ALTER COLUMN "workspace_id" SET NOT NULL;
--> statement-breakpoint
ALTER TABLE "memories" ADD COLUMN "deleted_at" timestamp with time zone;
--> statement-breakpoint
ALTER TABLE "memory_versions" ADD COLUMN "workspace_id" text;
--> statement-breakpoint
UPDATE "memory_versions"
SET "workspace_id" = "memory_stores"."workspace_id"
FROM "memory_stores"
WHERE "memory_versions"."store_id" = "memory_stores"."id";
--> statement-breakpoint
ALTER TABLE "memory_versions" ALTER COLUMN "workspace_id" SET NOT NULL;
--> statement-breakpoint
DROP INDEX "memories_store_path_idx";
--> statement-breakpoint
DROP INDEX "memory_versions_store_idx";
--> statement-breakpoint
DROP INDEX "memory_versions_memory_idx";
--> statement-breakpoint
-- Legacy deleteMemory hard-deleted the parent row while retaining immutable versions.
-- Recreate a hidden tombstone from the newest version before adding the composite FK.
INSERT INTO "memories" (
	"id",
	"workspace_id",
	"store_id",
	"path",
	"current_sha256",
	"size_bytes",
	"updated_at",
	"updated_by_session_id",
	"updated_by_event_id",
	"deleted_at"
)
SELECT DISTINCT ON (
	"memory_versions"."workspace_id",
	"memory_versions"."store_id",
	"memory_versions"."memory_id"
)
	"memory_versions"."memory_id",
	"memory_versions"."workspace_id",
	"memory_versions"."store_id",
	"memory_versions"."path",
	"memory_versions"."sha256",
	"memory_versions"."size_bytes",
	"memory_versions"."written_at",
	"memory_versions"."written_by_session_id",
	"memory_versions"."written_by_event_id",
	CURRENT_TIMESTAMP
FROM "memory_versions"
WHERE NOT EXISTS (
	SELECT 1
	FROM "memories"
	WHERE "memories"."id" = "memory_versions"."memory_id"
)
ORDER BY
	"memory_versions"."workspace_id",
	"memory_versions"."store_id",
	"memory_versions"."memory_id",
	"memory_versions"."written_at" DESC,
	"memory_versions"."id" DESC;
--> statement-breakpoint
CREATE UNIQUE INDEX "memory_stores_workspace_id_idx" ON "memory_stores" USING btree ("workspace_id","id");
--> statement-breakpoint
CREATE UNIQUE INDEX "memories_workspace_store_path_idx" ON "memories" USING btree ("workspace_id","store_id","path") WHERE "memories"."deleted_at" is null;
--> statement-breakpoint
CREATE UNIQUE INDEX "memories_workspace_store_id_idx" ON "memories" USING btree ("workspace_id","store_id","id");
--> statement-breakpoint
CREATE INDEX "memory_versions_store_idx" ON "memory_versions" USING btree ("workspace_id","store_id","written_at");
--> statement-breakpoint
CREATE INDEX "memory_versions_memory_idx" ON "memory_versions" USING btree ("workspace_id","store_id","memory_id","written_at");
--> statement-breakpoint
ALTER TABLE "memories" ADD CONSTRAINT "memories_workspace_store_fk" FOREIGN KEY ("workspace_id","store_id") REFERENCES "public"."memory_stores"("workspace_id","id") ON DELETE cascade ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "memory_versions" ADD CONSTRAINT "memory_versions_workspace_store_fk" FOREIGN KEY ("workspace_id","store_id") REFERENCES "public"."memory_stores"("workspace_id","id") ON DELETE cascade ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "memory_versions" ADD CONSTRAINT "memory_versions_workspace_memory_fk" FOREIGN KEY ("workspace_id","store_id","memory_id") REFERENCES "public"."memories"("workspace_id","store_id","id") ON DELETE cascade ON UPDATE no action;
