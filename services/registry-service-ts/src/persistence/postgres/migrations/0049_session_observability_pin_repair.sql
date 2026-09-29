-- Temporary mixed-version fallback for Registry writers that predate Session
-- observability pinning. Keep this function only until every such writer is
-- retired; a later migration must remove both this function and its trigger.
CREATE OR REPLACE FUNCTION "agent_observability_provision_legacy_session_pin"() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO "session_observability_bindings" (
    "workspace_id", "session_id", "organization_id", "agent_id", "agent_version", "created_at", "updated_at"
  )
  SELECT
    NEW."workspace_id", NEW."id", workspace."organization_id", NEW."agent_id", NEW."agent_version",
    NEW."created_at", NEW."updated_at"
  FROM "workspaces" AS workspace
  WHERE workspace."id" = NEW."workspace_id"
  ON CONFLICT ("workspace_id", "session_id") DO NOTHING;
  RETURN NEW;
END;
$$;
--> statement-breakpoint
COMMENT ON FUNCTION "agent_observability_provision_legacy_session_pin"() IS
  'Temporary mixed-version fallback until all pre-pinning Registry writers retire; a later migration removes it.';
--> statement-breakpoint
-- session-observability-pin-repair-initial-backfill-start
INSERT INTO "session_observability_bindings" (
  "workspace_id", "session_id", "organization_id", "agent_id", "agent_version", "created_at", "updated_at"
)
SELECT
  session."workspace_id", session."id", workspace."organization_id", session."agent_id",
  session."agent_version", session."created_at", session."updated_at"
FROM "sessions" AS session
INNER JOIN "workspaces" AS workspace ON workspace."id" = session."workspace_id"
LEFT JOIN "session_observability_bindings" AS pin
  ON pin."workspace_id" = session."workspace_id"
  AND pin."session_id" = session."id"
WHERE pin."session_id" IS NULL
ON CONFLICT ("workspace_id", "session_id") DO NOTHING;
-- session-observability-pin-repair-initial-backfill-end
--> statement-breakpoint
-- Drizzle executes migration statements in one PostgreSQL transaction. Do the
-- long first pass before taking the ShareRowExclusiveLock CREATE TRIGGER needs.
-- Bound only that lock acquisition so a busy sessions table fails the migration
-- instead of indefinitely blocking Registry Session writes; that failure rolls
-- back the first pass too, so no partially guarded state becomes visible.
-- session-observability-pin-repair-trigger-install-start
SET LOCAL lock_timeout = '5s';
--> statement-breakpoint
DO $$
BEGIN
  -- Do not drop/recreate a live fallback trigger when custom migration SQL is
  -- replayed. The CREATE OR REPLACE above updates its function body in place.
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgrelid = 'public.sessions'::regclass
      AND tgname = 'agent_observability_sessions_provision_legacy_pin'
      AND NOT tgisinternal
  ) THEN
    EXECUTE $trigger$
      CREATE TRIGGER "agent_observability_sessions_provision_legacy_pin"
      AFTER INSERT ON "sessions"
      FOR EACH ROW
      EXECUTE FUNCTION "agent_observability_provision_legacy_session_pin"()
    $trigger$;
  END IF;
END;
$$;
--> statement-breakpoint
COMMENT ON TRIGGER "agent_observability_sessions_provision_legacy_pin" ON "sessions" IS
  'Temporary mixed-version fallback until all pre-pinning Registry writers retire; a later migration removes it.';
--> statement-breakpoint
-- Reset only the transaction-local timeout. The CREATE TRIGGER lock remains
-- held until the Drizzle migration transaction commits, closing the gap before
-- the catch-up pass below.
SET LOCAL lock_timeout = DEFAULT;
-- session-observability-pin-repair-trigger-install-end
--> statement-breakpoint
-- session-observability-pin-repair-catch-up-backfill-start
INSERT INTO "session_observability_bindings" (
  "workspace_id", "session_id", "organization_id", "agent_id", "agent_version", "created_at", "updated_at"
)
SELECT
  session."workspace_id", session."id", workspace."organization_id", session."agent_id",
  session."agent_version", session."created_at", session."updated_at"
FROM "sessions" AS session
INNER JOIN "workspaces" AS workspace ON workspace."id" = session."workspace_id"
LEFT JOIN "session_observability_bindings" AS pin
  ON pin."workspace_id" = session."workspace_id"
  AND pin."session_id" = session."id"
WHERE pin."session_id" IS NULL
ON CONFLICT ("workspace_id", "session_id") DO NOTHING;
-- session-observability-pin-repair-catch-up-backfill-end
