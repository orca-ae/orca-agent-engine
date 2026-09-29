-- Temporary mixed-version fallback for Registry writers that do not yet use
-- the application Session-pin lifecycle helper. Keep this function and its
-- trigger only until every such writer has retired.
CREATE OR REPLACE FUNCTION "agent_observability_enforce_session_pin_lifecycle"() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  pin_status text;
  pin_archived_at timestamptz;
  pin_deleted_at timestamptz;
  pin_session_revocation_epoch bigint;
  lifecycle_at timestamptz;
BEGIN
  IF TG_OP = 'UPDATE' THEN
    -- Only the first Session archive changes its pin. Re-archive deliberately
    -- preserves the first pin tombstone and its epoch.
    IF NEW."archived_at" IS NULL THEN
      RETURN NEW;
    END IF;

    SELECT "status", "archived_at", "deleted_at", "session_revocation_epoch"
    INTO pin_status, pin_archived_at, pin_deleted_at, pin_session_revocation_epoch
    FROM "session_observability_bindings"
    WHERE "workspace_id" = NEW."workspace_id" AND "session_id" = NEW."id"
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Session observability pin missing for Session % in workspace %',
        NEW."id", NEW."workspace_id"
        USING ERRCODE = '23514';
    END IF;

    IF pin_status IN ('active', 'disabled')
      AND pin_archived_at IS NULL
      AND pin_deleted_at IS NULL
      AND pin_session_revocation_epoch >= 0 THEN
      IF OLD."archived_at" IS NOT NULL THEN
        RAISE EXCEPTION 'Session observability pin has invalid archive lifecycle shape for Session % in workspace %',
          NEW."id", NEW."workspace_id"
          USING ERRCODE = '23514';
      END IF;
      UPDATE "session_observability_bindings"
      SET "status" = 'archived',
          "archived_at" = NEW."archived_at",
          "deleted_at" = NULL,
          "session_revocation_epoch" = "session_revocation_epoch" + 1,
          "updated_at" = NEW."archived_at"
      WHERE "workspace_id" = NEW."workspace_id" AND "session_id" = NEW."id";
    ELSIF pin_status = 'archived'
      AND pin_archived_at IS NOT NULL
      AND pin_deleted_at IS NULL
      AND pin_session_revocation_epoch >= 0 THEN
      -- A current writer already wrote the authoritative archive tombstone.
      -- Do not overwrite it or advance its revocation epoch a second time.
      NULL;
    ELSE
      RAISE EXCEPTION 'Session observability pin has invalid archive lifecycle shape for Session % in workspace %',
        NEW."id", NEW."workspace_id"
        USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
  END IF;

  SELECT "status", "archived_at", "deleted_at", "session_revocation_epoch"
  INTO pin_status, pin_archived_at, pin_deleted_at, pin_session_revocation_epoch
  FROM "session_observability_bindings"
  WHERE "workspace_id" = OLD."workspace_id" AND "session_id" = OLD."id"
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Session observability pin missing for Session % in workspace %',
      OLD."id", OLD."workspace_id"
      USING ERRCODE = '23514';
  END IF;

  IF (
    pin_status IN ('active', 'disabled')
    AND pin_archived_at IS NULL
    AND pin_deleted_at IS NULL
    AND pin_session_revocation_epoch >= 0
  ) OR (
    pin_status = 'archived'
    AND pin_archived_at IS NOT NULL
    AND pin_deleted_at IS NULL
    AND pin_session_revocation_epoch >= 0
  ) THEN
    -- statement_timestamp() is stable for every row in a workspace archive
    -- statement, so deleted_at and updated_at carry one common delete fence.
    lifecycle_at := statement_timestamp();
    UPDATE "session_observability_bindings"
    SET "status" = 'deleted',
        "deleted_at" = lifecycle_at,
        "session_revocation_epoch" = "session_revocation_epoch" + 1,
        "updated_at" = lifecycle_at
    WHERE "workspace_id" = OLD."workspace_id" AND "session_id" = OLD."id";
  ELSIF pin_status = 'deleted'
    AND pin_deleted_at IS NOT NULL
    AND pin_session_revocation_epoch >= 0 THEN
    -- A current writer already wrote the authoritative delete tombstone.
    NULL;
  ELSE
    RAISE EXCEPTION 'Session observability pin has invalid delete lifecycle shape for Session % in workspace %',
      OLD."id", OLD."workspace_id"
      USING ERRCODE = '23514';
  END IF;

  RETURN OLD;
END;
$$;
--> statement-breakpoint
COMMENT ON FUNCTION "agent_observability_enforce_session_pin_lifecycle"() IS
  'Temporary mixed-version fallback until all pre-lifecycle Registry writers retire; enforces Session archive/delete pin tombstones.';
--> statement-breakpoint
-- session-observability-lifecycle-enforcement-initial-backfill-start
-- Heal legacy archive writes that predate the lifecycle helper. The Session
-- timestamp is authoritative for the archive tombstone.
UPDATE "session_observability_bindings" AS pin
SET "status" = 'archived',
    "archived_at" = session."archived_at",
    "deleted_at" = NULL,
    "session_revocation_epoch" = pin."session_revocation_epoch" + 1,
    "updated_at" = session."archived_at"
FROM "sessions" AS session
WHERE pin."workspace_id" = session."workspace_id"
  AND pin."session_id" = session."id"
  AND session."archived_at" IS NOT NULL
  AND pin."status" IN ('active', 'disabled')
  AND pin."archived_at" IS NULL
  AND pin."deleted_at" IS NULL
  AND pin."session_revocation_epoch" >= 0;
--> statement-breakpoint
-- Heal legacy hard deletes. Deleted pins keep a prior archive tombstone, when
-- one exists, and statement_timestamp() makes this batch's deletion fence
-- stable across rows.
UPDATE "session_observability_bindings" AS pin
SET "status" = 'deleted',
    "deleted_at" = statement_timestamp(),
    "session_revocation_epoch" = pin."session_revocation_epoch" + 1,
    "updated_at" = statement_timestamp()
WHERE NOT EXISTS (
    SELECT 1
    FROM "sessions" AS session
    WHERE session."workspace_id" = pin."workspace_id"
      AND session."id" = pin."session_id"
  )
  AND (
    (
      pin."status" IN ('active', 'disabled')
      AND pin."archived_at" IS NULL
      AND pin."deleted_at" IS NULL
    ) OR (
      pin."status" = 'archived'
      AND pin."archived_at" IS NOT NULL
      AND pin."deleted_at" IS NULL
    )
  )
  AND pin."session_revocation_epoch" >= 0;
-- session-observability-lifecycle-enforcement-initial-backfill-end
--> statement-breakpoint
-- Drizzle executes migration statements in one PostgreSQL transaction. Do the
-- long initial pass before taking CREATE TRIGGER's ShareRowExclusiveLock.
-- Bound only that acquisition: a busy sessions table fails and rolls back the
-- whole migration rather than exposing a partially repaired, unguarded state.
-- session-observability-lifecycle-enforcement-trigger-install-start
SET LOCAL lock_timeout = '5s';
--> statement-breakpoint
DO $$
BEGIN
  -- Avoid dropping/recreating a live trigger if custom migration SQL is
  -- replayed. CREATE OR REPLACE above refreshes its function body in place.
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgrelid = 'public.sessions'::regclass
      AND tgname = 'agent_observability_sessions_enforce_lifecycle'
      AND NOT tgisinternal
  ) THEN
    EXECUTE $trigger$
      CREATE TRIGGER "agent_observability_sessions_enforce_lifecycle"
      AFTER UPDATE OF "archived_at" OR DELETE ON "sessions"
      FOR EACH ROW
      EXECUTE FUNCTION "agent_observability_enforce_session_pin_lifecycle"()
    $trigger$;
  END IF;
END;
$$;
--> statement-breakpoint
COMMENT ON TRIGGER "agent_observability_sessions_enforce_lifecycle" ON "sessions" IS
  'Temporary mixed-version fallback until all pre-lifecycle Registry writers retire; covers direct Session lifecycle writes and workspace archive fan-out.';
--> statement-breakpoint
-- The trigger lock stays held to commit, closing the gap before catch-up.
SET LOCAL lock_timeout = DEFAULT;
-- session-observability-lifecycle-enforcement-trigger-install-end
--> statement-breakpoint
-- session-observability-lifecycle-enforcement-catch-up-backfill-start
UPDATE "session_observability_bindings" AS pin
SET "status" = 'archived',
    "archived_at" = session."archived_at",
    "deleted_at" = NULL,
    "session_revocation_epoch" = pin."session_revocation_epoch" + 1,
    "updated_at" = session."archived_at"
FROM "sessions" AS session
WHERE pin."workspace_id" = session."workspace_id"
  AND pin."session_id" = session."id"
  AND session."archived_at" IS NOT NULL
  AND pin."status" IN ('active', 'disabled')
  AND pin."archived_at" IS NULL
  AND pin."deleted_at" IS NULL
  AND pin."session_revocation_epoch" >= 0;
--> statement-breakpoint
UPDATE "session_observability_bindings" AS pin
SET "status" = 'deleted',
    "deleted_at" = statement_timestamp(),
    "session_revocation_epoch" = pin."session_revocation_epoch" + 1,
    "updated_at" = statement_timestamp()
WHERE NOT EXISTS (
    SELECT 1
    FROM "sessions" AS session
    WHERE session."workspace_id" = pin."workspace_id"
      AND session."id" = pin."session_id"
  )
  AND (
    (
      pin."status" IN ('active', 'disabled')
      AND pin."archived_at" IS NULL
      AND pin."deleted_at" IS NULL
    ) OR (
      pin."status" = 'archived'
      AND pin."archived_at" IS NOT NULL
      AND pin."deleted_at" IS NULL
    )
  )
  AND pin."session_revocation_epoch" >= 0;
-- session-observability-lifecycle-enforcement-catch-up-backfill-end
