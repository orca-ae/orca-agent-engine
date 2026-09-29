DROP INDEX "memory_stores_ws_name_idx";--> statement-breakpoint
DROP INDEX "memories_workspace_store_path_idx";--> statement-breakpoint
CREATE UNIQUE INDEX "memories_workspace_store_path_idx" ON "memories" USING btree ("workspace_id","store_id","path") WHERE "memories"."deleted_at" is null;