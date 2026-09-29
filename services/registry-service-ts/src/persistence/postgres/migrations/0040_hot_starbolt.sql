DROP INDEX "skill_versions_identifier_page_idx";--> statement-breakpoint
CREATE INDEX "skill_versions_identifier_page_idx" ON "skill_versions" USING btree ("workspace_id","skill_id",(("version_identifier"::numeric))) WHERE "skill_versions"."archived_at" is null;
