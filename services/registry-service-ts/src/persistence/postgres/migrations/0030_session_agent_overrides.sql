CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
--> statement-breakpoint
ALTER TABLE "sessions" ADD COLUMN "agent_overrides" jsonb;
--> statement-breakpoint
ALTER TABLE "session_threads" ADD COLUMN "parent_thread_id" text;
--> statement-breakpoint
INSERT INTO "session_threads" (
  "id",
  "workspace_id",
  "session_id",
  "subpath",
  "agent_id",
  "agent_version",
  "agent_name",
  "status",
  "created_at",
  "updated_at"
)
SELECT
  'sth_' || uuid_generate_v5(
    'ea82f5c2-b759-4717-9727-51b3ef079bef'::uuid,
    "sessions"."workspace_id" || ':' || "sessions"."id" || ':'
  )::text,
  "sessions"."workspace_id",
  "sessions"."id",
  '',
  "sessions"."agent_id",
  "sessions"."agent_version",
  COALESCE(
    NULLIF("agent_versions"."snapshot"->>'name', ''),
    "agents"."name",
    "sessions"."agent_id"
  ),
  CASE
    WHEN "sessions"."status" IN ('running', 'rescheduling', 'terminated')
      THEN "sessions"."status"
    ELSE 'idle'
  END,
  "sessions"."created_at",
  "sessions"."updated_at"
FROM "sessions"
LEFT JOIN "agent_versions"
  ON "agent_versions"."workspace_id" = "sessions"."workspace_id"
  AND "agent_versions"."agent_id" = "sessions"."agent_id"
  AND "agent_versions"."version" = "sessions"."agent_version"
LEFT JOIN "agents"
  ON "agents"."workspace_id" = "sessions"."workspace_id"
  AND "agents"."id" = "sessions"."agent_id"
ON CONFLICT ("workspace_id", "session_id", "subpath") DO NOTHING;
--> statement-breakpoint
UPDATE "session_threads" AS "child"
SET "parent_thread_id" = "primary_thread"."id"
FROM "session_threads" AS "primary_thread"
WHERE "primary_thread"."workspace_id" = "child"."workspace_id"
  AND "primary_thread"."session_id" = "child"."session_id"
  AND "primary_thread"."subpath" = ''
  AND "child"."subpath" <> ''
  AND "child"."parent_thread_id" IS NULL;
--> statement-breakpoint
CREATE UNIQUE INDEX "session_threads_workspace_session_id_idx" ON "session_threads" USING btree ("workspace_id","session_id","id");
--> statement-breakpoint
CREATE INDEX "session_threads_parent_idx" ON "session_threads" USING btree ("workspace_id","session_id","parent_thread_id");
--> statement-breakpoint
ALTER TABLE "session_threads" ADD CONSTRAINT "session_threads_workspace_parent_fk" FOREIGN KEY ("workspace_id","session_id","parent_thread_id") REFERENCES "public"."session_threads"("workspace_id","session_id","id") ON DELETE no action ON UPDATE no action;
