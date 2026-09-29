CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
--> statement-breakpoint
ALTER TABLE "session_threads" DROP CONSTRAINT "session_threads_workspace_parent_fk";
--> statement-breakpoint
WITH "primary_ids" AS (
  SELECT
    "workspace_id",
    "session_id",
    "id" AS "old_id",
    'sth_' || uuid_generate_v5(
      'ea82f5c2-b759-4717-9727-51b3ef079bef'::uuid,
      "workspace_id" || ':' || "session_id" || ':'
    )::text AS "new_id"
  FROM "session_threads"
  WHERE "subpath" = ''
)
UPDATE "session_threads" AS "child"
SET "parent_thread_id" = "primary_ids"."new_id"
FROM "primary_ids"
WHERE "child"."workspace_id" = "primary_ids"."workspace_id"
  AND "child"."session_id" = "primary_ids"."session_id"
  AND "child"."parent_thread_id" = "primary_ids"."old_id"
  AND "primary_ids"."old_id" <> "primary_ids"."new_id";
--> statement-breakpoint
UPDATE "session_threads"
SET "id" = 'sth_' || uuid_generate_v5(
  'ea82f5c2-b759-4717-9727-51b3ef079bef'::uuid,
  "workspace_id" || ':' || "session_id" || ':'
)::text
WHERE "subpath" = ''
  AND "id" <> 'sth_' || uuid_generate_v5(
    'ea82f5c2-b759-4717-9727-51b3ef079bef'::uuid,
    "workspace_id" || ':' || "session_id" || ':'
  )::text;
--> statement-breakpoint
ALTER TABLE "session_threads" ADD CONSTRAINT "session_threads_workspace_parent_fk" FOREIGN KEY ("workspace_id","session_id","parent_thread_id") REFERENCES "public"."session_threads"("workspace_id","session_id","id") ON DELETE no action ON UPDATE no action;
