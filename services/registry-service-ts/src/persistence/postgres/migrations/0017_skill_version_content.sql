ALTER TABLE "skill_versions" ADD COLUMN "version_identifier" text;
ALTER TABLE "skill_versions" ADD COLUMN "name" text;
ALTER TABLE "skill_versions" ADD COLUMN "description" text DEFAULT '' NOT NULL;
ALTER TABLE "skill_versions" ADD COLUMN "directory" text;
ALTER TABLE "skill_versions" ADD COLUMN "content_files" jsonb DEFAULT '[]'::jsonb NOT NULL;

UPDATE "skill_versions" AS sv
SET
  "version_identifier" = COALESCE(
    sv."version_identifier",
    (floor(extract(epoch FROM sv."created_at") * 1000000)::bigint + sv."version")::text
  ),
  "name" = COALESCE(sv."name", s."name"),
  "description" = COALESCE(sv."description", s."description", ''),
  "directory" = COALESCE(sv."directory", s."slug")
FROM "skills" AS s
WHERE sv."skill_id" = s."id";

ALTER TABLE "skill_versions" ALTER COLUMN "version_identifier" SET NOT NULL;
ALTER TABLE "skill_versions" ALTER COLUMN "name" SET NOT NULL;
ALTER TABLE "skill_versions" ALTER COLUMN "directory" SET NOT NULL;

CREATE UNIQUE INDEX "skill_versions_skill_version_identifier_idx"
  ON "skill_versions" USING btree ("skill_id", "version_identifier");
