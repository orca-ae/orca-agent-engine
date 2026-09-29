CREATE INDEX "skill_versions_identifier_page_idx" ON "skill_versions" USING btree ("workspace_id","skill_id",(("version_identifier"::numeric)));--> statement-breakpoint
ALTER TABLE "skill_versions" ADD CONSTRAINT "skill_versions_version_identifier_check" CHECK ("skill_versions"."version_identifier" ~ '^[1-9][0-9]{0,31}$');
