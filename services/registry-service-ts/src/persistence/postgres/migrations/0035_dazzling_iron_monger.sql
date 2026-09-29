DROP INDEX "skills_ws_slug_idx";--> statement-breakpoint
CREATE UNIQUE INDEX "skills_ws_display_title_idx" ON "skills" USING btree ("workspace_id","display_title") WHERE "skills"."type" = 'custom' and "skills"."display_title" is not null and "skills"."archived_at" is null;
