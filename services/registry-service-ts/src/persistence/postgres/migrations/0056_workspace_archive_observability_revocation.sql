CREATE TABLE "agent_observability_workspace_archive_revocations" (
	"workspace_id" text PRIMARY KEY NOT NULL,
	"organization_id" text NOT NULL,
	"archived_at" timestamp with time zone NOT NULL,
	"revocation_epoch" bigint NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "agent_observability_workspace_archive_revocations_epoch_check" CHECK ("agent_observability_workspace_archive_revocations"."revocation_epoch" > 0 and "agent_observability_workspace_archive_revocations"."revocation_epoch" <= 9007199254740991)
);
--> statement-breakpoint
-- Temporary mixed-version fallback for Registry writers that do not yet
-- write the durable Workspace archive marker. Keep this function and trigger
-- only until every such writer has retired.
CREATE OR REPLACE FUNCTION "agent_observability_enforce_workspace_archive_revocation"() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  setting_workspace_id text;
  setting_organization_id text;
  setting_mode text;
  setting_binding_id text;
  setting_selection_epoch bigint;
  setting_revocation_epoch bigint;
  setting_capture_ceiling text;
  setting_capture_restriction_epoch bigint;
  marker_workspace_id text;
  marker_organization_id text;
  marker_archived_at timestamptz;
  marker_revocation_epoch bigint;
  next_revocation_epoch bigint;
BEGIN
  IF NEW."status" <> 'archived' THEN
    RETURN NEW;
  END IF;

  IF NEW."archived_at" IS NULL THEN
    RAISE EXCEPTION 'Workspace archive timestamp missing for workspace %', NEW."id"
      USING ERRCODE = '23514';
  END IF;

  -- Workspace UPDATE already holds workspace authority. Lock its exact setting
  -- next, before any Session lifecycle trigger can take Session-row locks.
  SELECT
    "workspace_id",
    "organization_id",
    "mode",
    "binding_id",
    "selection_epoch",
    "revocation_epoch",
    "capture_ceiling",
    "capture_restriction_epoch"
  INTO
    setting_workspace_id,
    setting_organization_id,
    setting_mode,
    setting_binding_id,
    setting_selection_epoch,
    setting_revocation_epoch,
    setting_capture_ceiling,
    setting_capture_restriction_epoch
  FROM "agent_observability_workspace_settings"
  WHERE "organization_id" = NEW."organization_id"
    AND "workspace_id" = NEW."id"
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Workspace observability setting missing for workspace %', NEW."id"
      USING ERRCODE = '23514';
  END IF;

  IF setting_workspace_id IS DISTINCT FROM NEW."id"
    OR setting_organization_id IS DISTINCT FROM NEW."organization_id"
    OR (
      CASE setting_mode
        WHEN 'custom' THEN
          setting_binding_id IS NOT NULL AND char_length(btrim(setting_binding_id)) > 0
        WHEN 'inherit' THEN setting_binding_id IS NULL
        WHEN 'disabled' THEN setting_binding_id IS NULL
        ELSE false
      END
    ) IS NOT TRUE
    OR (
      setting_capture_ceiling IS DISTINCT FROM 'metadata_only'
      AND setting_capture_ceiling IS DISTINCT FROM 'redacted_io'
    )
    OR setting_selection_epoch IS NULL
    OR setting_selection_epoch < 0
    OR setting_selection_epoch > 9007199254740991
    OR setting_revocation_epoch IS NULL
    OR setting_revocation_epoch < 0
    OR setting_revocation_epoch > 9007199254740991
    OR setting_capture_restriction_epoch IS NULL
    OR setting_capture_restriction_epoch < 0
    OR setting_capture_restriction_epoch > 9007199254740991 THEN
    RAISE EXCEPTION 'Workspace observability setting has invalid archive state for workspace %', NEW."id"
      USING ERRCODE = '23514';
  END IF;

  SELECT "workspace_id", "organization_id", "archived_at", "revocation_epoch"
  INTO
    marker_workspace_id,
    marker_organization_id,
    marker_archived_at,
    marker_revocation_epoch
  FROM "agent_observability_workspace_archive_revocations"
  WHERE "organization_id" = NEW."organization_id"
    AND "workspace_id" = NEW."id"
  FOR UPDATE;

  IF FOUND THEN
    IF marker_workspace_id IS DISTINCT FROM NEW."id"
      OR marker_organization_id IS DISTINCT FROM NEW."organization_id"
      OR marker_archived_at IS DISTINCT FROM NEW."archived_at"
      OR marker_revocation_epoch IS NULL
      OR marker_revocation_epoch <= 0
      OR marker_revocation_epoch > 9007199254740991
      OR marker_revocation_epoch IS DISTINCT FROM setting_revocation_epoch THEN
      RAISE EXCEPTION 'Workspace observability archive marker has invalid state for workspace %', NEW."id"
        USING ERRCODE = '23514';
    END IF;
    -- A current application writer inserted this marker and advanced the
    -- setting before updating Workspace status; preserve its one increment.
    RETURN NEW;
  END IF;

  IF OLD."status" = 'archived' THEN
    RAISE EXCEPTION 'Workspace observability archive marker missing for workspace %', NEW."id"
      USING ERRCODE = '23514';
  END IF;

  IF OLD."status" <> 'active' OR OLD."archived_at" IS NOT NULL THEN
    RAISE EXCEPTION 'Workspace has invalid archive lifecycle state for workspace %', NEW."id"
      USING ERRCODE = '23514';
  END IF;

  IF setting_revocation_epoch >= 9007199254740991 THEN
    RAISE EXCEPTION 'Workspace observability revocation epoch overflow for workspace %', NEW."id"
      USING ERRCODE = '23514';
  END IF;
  next_revocation_epoch := setting_revocation_epoch + 1;

  -- Only the writer that creates the durable marker may advance the setting.
  INSERT INTO "agent_observability_workspace_archive_revocations" (
    "workspace_id", "organization_id", "archived_at", "revocation_epoch"
  ) VALUES (
    NEW."id", NEW."organization_id", NEW."archived_at", next_revocation_epoch
  )
  ON CONFLICT ("workspace_id") DO NOTHING
  RETURNING "workspace_id", "organization_id", "archived_at", "revocation_epoch"
  INTO
    marker_workspace_id,
    marker_organization_id,
    marker_archived_at,
    marker_revocation_epoch;

  IF NOT FOUND THEN
    SELECT "workspace_id", "organization_id", "archived_at", "revocation_epoch"
    INTO
      marker_workspace_id,
      marker_organization_id,
      marker_archived_at,
      marker_revocation_epoch
    FROM "agent_observability_workspace_archive_revocations"
    WHERE "organization_id" = NEW."organization_id"
      AND "workspace_id" = NEW."id"
    FOR UPDATE;

    IF NOT FOUND
      OR marker_workspace_id IS DISTINCT FROM NEW."id"
      OR marker_organization_id IS DISTINCT FROM NEW."organization_id"
      OR marker_archived_at IS DISTINCT FROM NEW."archived_at"
      OR marker_revocation_epoch IS NULL
      OR marker_revocation_epoch <= 0
      OR marker_revocation_epoch > 9007199254740991
      OR marker_revocation_epoch IS DISTINCT FROM setting_revocation_epoch THEN
      RAISE EXCEPTION 'Workspace observability archive marker has invalid state for workspace %', NEW."id"
        USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
  END IF;

  UPDATE "agent_observability_workspace_settings"
  SET "revocation_epoch" = marker_revocation_epoch,
      "updated_at" = NEW."archived_at"
  WHERE "organization_id" = NEW."organization_id"
    AND "workspace_id" = NEW."id"
    AND "revocation_epoch" = setting_revocation_epoch;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Workspace observability setting archive CAS lost for workspace %', NEW."id"
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END;
$$;
--> statement-breakpoint
COMMENT ON FUNCTION "agent_observability_enforce_workspace_archive_revocation"() IS
  'Temporary mixed-version fallback until all pre-workspace-archive-revocation Registry writers retire; writes one durable Workspace archive revocation marker.';
--> statement-breakpoint
-- workspace-archive-observability-revocation-initial-backfill-start
-- Backfill every archived Workspace through a permanent exactly-once marker.
-- Do not infer completion from mutable timestamps: an existing marker must
-- prove its archive identity and post-increment setting epoch exactly.
DO $$
DECLARE
  workspace_row record;
  setting_workspace_id text;
  setting_organization_id text;
  setting_mode text;
  setting_binding_id text;
  setting_selection_epoch bigint;
  setting_revocation_epoch bigint;
  setting_capture_ceiling text;
  setting_capture_restriction_epoch bigint;
  marker_workspace_id text;
  marker_organization_id text;
  marker_archived_at timestamptz;
  marker_revocation_epoch bigint;
  next_revocation_epoch bigint;
BEGIN
  FOR workspace_row IN
    SELECT workspace."id", workspace."organization_id", workspace."archived_at"
    FROM "workspaces" AS workspace
    WHERE workspace."status" = 'archived'
    ORDER BY workspace."id"
    FOR UPDATE
  LOOP
    IF workspace_row."archived_at" IS NULL THEN
      RAISE EXCEPTION 'Workspace archive timestamp missing for workspace %', workspace_row."id"
        USING ERRCODE = '23514';
    END IF;

    SELECT
      "workspace_id",
      "organization_id",
      "mode",
      "binding_id",
      "selection_epoch",
      "revocation_epoch",
      "capture_ceiling",
      "capture_restriction_epoch"
    INTO
      setting_workspace_id,
      setting_organization_id,
      setting_mode,
      setting_binding_id,
      setting_selection_epoch,
      setting_revocation_epoch,
      setting_capture_ceiling,
      setting_capture_restriction_epoch
    FROM "agent_observability_workspace_settings"
    WHERE "organization_id" = workspace_row."organization_id"
      AND "workspace_id" = workspace_row."id"
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Workspace observability setting missing for workspace %', workspace_row."id"
        USING ERRCODE = '23514';
    END IF;

    IF setting_workspace_id IS DISTINCT FROM workspace_row."id"
      OR setting_organization_id IS DISTINCT FROM workspace_row."organization_id"
      OR (
        CASE setting_mode
          WHEN 'custom' THEN
            setting_binding_id IS NOT NULL AND char_length(btrim(setting_binding_id)) > 0
          WHEN 'inherit' THEN setting_binding_id IS NULL
          WHEN 'disabled' THEN setting_binding_id IS NULL
          ELSE false
        END
      ) IS NOT TRUE
      OR (
        setting_capture_ceiling IS DISTINCT FROM 'metadata_only'
        AND setting_capture_ceiling IS DISTINCT FROM 'redacted_io'
      )
      OR setting_selection_epoch IS NULL
      OR setting_selection_epoch < 0
      OR setting_selection_epoch > 9007199254740991
      OR setting_revocation_epoch IS NULL
      OR setting_revocation_epoch < 0
      OR setting_revocation_epoch > 9007199254740991
      OR setting_capture_restriction_epoch IS NULL
      OR setting_capture_restriction_epoch < 0
      OR setting_capture_restriction_epoch > 9007199254740991 THEN
      RAISE EXCEPTION 'Workspace observability setting has invalid archive state for workspace %', workspace_row."id"
        USING ERRCODE = '23514';
    END IF;

    SELECT "workspace_id", "organization_id", "archived_at", "revocation_epoch"
    INTO
      marker_workspace_id,
      marker_organization_id,
      marker_archived_at,
      marker_revocation_epoch
    FROM "agent_observability_workspace_archive_revocations"
    WHERE "organization_id" = workspace_row."organization_id"
      AND "workspace_id" = workspace_row."id"
    FOR UPDATE;

    IF FOUND THEN
      IF marker_workspace_id IS DISTINCT FROM workspace_row."id"
        OR marker_organization_id IS DISTINCT FROM workspace_row."organization_id"
        OR marker_archived_at IS DISTINCT FROM workspace_row."archived_at"
        OR marker_revocation_epoch IS NULL
        OR marker_revocation_epoch <= 0
        OR marker_revocation_epoch > 9007199254740991
        OR marker_revocation_epoch IS DISTINCT FROM setting_revocation_epoch THEN
        RAISE EXCEPTION 'Workspace observability archive marker has invalid state for workspace %', workspace_row."id"
          USING ERRCODE = '23514';
      END IF;
      CONTINUE;
    END IF;

    IF setting_revocation_epoch >= 9007199254740991 THEN
      RAISE EXCEPTION 'Workspace observability revocation epoch overflow for workspace %', workspace_row."id"
        USING ERRCODE = '23514';
    END IF;
    next_revocation_epoch := setting_revocation_epoch + 1;

    INSERT INTO "agent_observability_workspace_archive_revocations" (
      "workspace_id", "organization_id", "archived_at", "revocation_epoch"
    ) VALUES (
      workspace_row."id",
      workspace_row."organization_id",
      workspace_row."archived_at",
      next_revocation_epoch
    )
    ON CONFLICT ("workspace_id") DO NOTHING
    RETURNING "workspace_id", "organization_id", "archived_at", "revocation_epoch"
    INTO
      marker_workspace_id,
      marker_organization_id,
      marker_archived_at,
      marker_revocation_epoch;

    IF NOT FOUND THEN
      SELECT "workspace_id", "organization_id", "archived_at", "revocation_epoch"
      INTO
        marker_workspace_id,
        marker_organization_id,
        marker_archived_at,
        marker_revocation_epoch
      FROM "agent_observability_workspace_archive_revocations"
      WHERE "organization_id" = workspace_row."organization_id"
        AND "workspace_id" = workspace_row."id"
      FOR UPDATE;

      IF NOT FOUND
        OR marker_workspace_id IS DISTINCT FROM workspace_row."id"
        OR marker_organization_id IS DISTINCT FROM workspace_row."organization_id"
        OR marker_archived_at IS DISTINCT FROM workspace_row."archived_at"
        OR marker_revocation_epoch IS NULL
        OR marker_revocation_epoch <= 0
        OR marker_revocation_epoch > 9007199254740991
        OR marker_revocation_epoch IS DISTINCT FROM setting_revocation_epoch THEN
        RAISE EXCEPTION 'Workspace observability archive marker has invalid state for workspace %', workspace_row."id"
          USING ERRCODE = '23514';
      END IF;
      CONTINUE;
    END IF;

    UPDATE "agent_observability_workspace_settings"
    SET "revocation_epoch" = marker_revocation_epoch,
        "updated_at" = workspace_row."archived_at"
    WHERE "organization_id" = workspace_row."organization_id"
      AND "workspace_id" = workspace_row."id"
      AND "revocation_epoch" = setting_revocation_epoch;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Workspace observability setting archive CAS lost for workspace %', workspace_row."id"
        USING ERRCODE = '23514';
    END IF;
  END LOOP;
END;
$$;
-- workspace-archive-observability-revocation-initial-backfill-end
--> statement-breakpoint
-- Drizzle executes migration statements in one PostgreSQL transaction. Do the
-- long initial pass before taking CREATE TRIGGER's ShareRowExclusiveLock.
-- Bound that and the ownership-FK acquisitions: a busy parent table fails and
-- rolls back the whole migration rather than exposing a partially repaired,
-- unguarded archive path.
-- workspace-archive-observability-revocation-trigger-install-start
SET LOCAL lock_timeout = '5s';
--> statement-breakpoint
DO $$
BEGIN
  -- Avoid dropping/recreating a live trigger if custom migration SQL is
  -- replayed. CREATE OR REPLACE above refreshes its function body in place.
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgrelid = 'public.workspaces'::regclass
      AND tgname = 'agent_observability_workspaces_enforce_archive_revocation'
      AND NOT tgisinternal
  ) THEN
    EXECUTE $trigger$
      CREATE TRIGGER "agent_observability_workspaces_enforce_archive_revocation"
      AFTER UPDATE OF "status", "archived_at" ON "workspaces"
      FOR EACH ROW
      EXECUTE FUNCTION "agent_observability_enforce_workspace_archive_revocation"()
    $trigger$;
  END IF;
END;
$$;
--> statement-breakpoint
-- Add parent ownership FKs only after initial backfill. Both constraints start
-- NOT VALID so their lock acquisition is bounded below instead of scanning and
-- blocking writers before the trigger closes the old-writer gap. Backfill and
-- trigger validation prove every pre-existing marker; a separate maintenance
-- migration validates the catalog constraints without this rollout lock.
ALTER TABLE "agent_observability_workspace_archive_revocations" ADD CONSTRAINT "agent_observability_workspace_archive_revocations_workspace_fk" FOREIGN KEY ("organization_id","workspace_id") REFERENCES "public"."workspaces"("organization_id","id") ON DELETE restrict ON UPDATE no action NOT VALID;
--> statement-breakpoint
ALTER TABLE "agent_observability_workspace_archive_revocations" ADD CONSTRAINT "agent_observability_workspace_archive_revocations_setting_fk" FOREIGN KEY ("organization_id","workspace_id") REFERENCES "public"."agent_observability_workspace_settings"("organization_id","workspace_id") ON DELETE restrict ON UPDATE no action NOT VALID;
--> statement-breakpoint
COMMENT ON TRIGGER "agent_observability_workspaces_enforce_archive_revocation" ON "workspaces" IS
  'Temporary mixed-version fallback until all pre-workspace-archive-revocation Registry writers retire; writes the durable Workspace archive revocation marker for direct archive writes.';
--> statement-breakpoint
-- The trigger lock stays held to commit, closing the gap before catch-up.
SET LOCAL lock_timeout = DEFAULT;
-- workspace-archive-observability-revocation-trigger-install-end
--> statement-breakpoint
-- workspace-archive-observability-revocation-catch-up-backfill-start
DO $$
DECLARE
  workspace_row record;
  setting_workspace_id text;
  setting_organization_id text;
  setting_mode text;
  setting_binding_id text;
  setting_selection_epoch bigint;
  setting_revocation_epoch bigint;
  setting_capture_ceiling text;
  setting_capture_restriction_epoch bigint;
  marker_workspace_id text;
  marker_organization_id text;
  marker_archived_at timestamptz;
  marker_revocation_epoch bigint;
  next_revocation_epoch bigint;
BEGIN
  FOR workspace_row IN
    SELECT workspace."id", workspace."organization_id", workspace."archived_at"
    FROM "workspaces" AS workspace
    WHERE workspace."status" = 'archived'
    ORDER BY workspace."id"
    FOR UPDATE
  LOOP
    IF workspace_row."archived_at" IS NULL THEN
      RAISE EXCEPTION 'Workspace archive timestamp missing for workspace %', workspace_row."id"
        USING ERRCODE = '23514';
    END IF;

    SELECT
      "workspace_id",
      "organization_id",
      "mode",
      "binding_id",
      "selection_epoch",
      "revocation_epoch",
      "capture_ceiling",
      "capture_restriction_epoch"
    INTO
      setting_workspace_id,
      setting_organization_id,
      setting_mode,
      setting_binding_id,
      setting_selection_epoch,
      setting_revocation_epoch,
      setting_capture_ceiling,
      setting_capture_restriction_epoch
    FROM "agent_observability_workspace_settings"
    WHERE "organization_id" = workspace_row."organization_id"
      AND "workspace_id" = workspace_row."id"
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Workspace observability setting missing for workspace %', workspace_row."id"
        USING ERRCODE = '23514';
    END IF;

    IF setting_workspace_id IS DISTINCT FROM workspace_row."id"
      OR setting_organization_id IS DISTINCT FROM workspace_row."organization_id"
      OR (
        CASE setting_mode
          WHEN 'custom' THEN
            setting_binding_id IS NOT NULL AND char_length(btrim(setting_binding_id)) > 0
          WHEN 'inherit' THEN setting_binding_id IS NULL
          WHEN 'disabled' THEN setting_binding_id IS NULL
          ELSE false
        END
      ) IS NOT TRUE
      OR (
        setting_capture_ceiling IS DISTINCT FROM 'metadata_only'
        AND setting_capture_ceiling IS DISTINCT FROM 'redacted_io'
      )
      OR setting_selection_epoch IS NULL
      OR setting_selection_epoch < 0
      OR setting_selection_epoch > 9007199254740991
      OR setting_revocation_epoch IS NULL
      OR setting_revocation_epoch < 0
      OR setting_revocation_epoch > 9007199254740991
      OR setting_capture_restriction_epoch IS NULL
      OR setting_capture_restriction_epoch < 0
      OR setting_capture_restriction_epoch > 9007199254740991 THEN
      RAISE EXCEPTION 'Workspace observability setting has invalid archive state for workspace %', workspace_row."id"
        USING ERRCODE = '23514';
    END IF;

    SELECT "workspace_id", "organization_id", "archived_at", "revocation_epoch"
    INTO
      marker_workspace_id,
      marker_organization_id,
      marker_archived_at,
      marker_revocation_epoch
    FROM "agent_observability_workspace_archive_revocations"
    WHERE "organization_id" = workspace_row."organization_id"
      AND "workspace_id" = workspace_row."id"
    FOR UPDATE;

    IF FOUND THEN
      IF marker_workspace_id IS DISTINCT FROM workspace_row."id"
        OR marker_organization_id IS DISTINCT FROM workspace_row."organization_id"
        OR marker_archived_at IS DISTINCT FROM workspace_row."archived_at"
        OR marker_revocation_epoch IS NULL
        OR marker_revocation_epoch <= 0
        OR marker_revocation_epoch > 9007199254740991
        OR marker_revocation_epoch IS DISTINCT FROM setting_revocation_epoch THEN
        RAISE EXCEPTION 'Workspace observability archive marker has invalid state for workspace %', workspace_row."id"
          USING ERRCODE = '23514';
      END IF;
      CONTINUE;
    END IF;

    IF setting_revocation_epoch >= 9007199254740991 THEN
      RAISE EXCEPTION 'Workspace observability revocation epoch overflow for workspace %', workspace_row."id"
        USING ERRCODE = '23514';
    END IF;
    next_revocation_epoch := setting_revocation_epoch + 1;

    INSERT INTO "agent_observability_workspace_archive_revocations" (
      "workspace_id", "organization_id", "archived_at", "revocation_epoch"
    ) VALUES (
      workspace_row."id",
      workspace_row."organization_id",
      workspace_row."archived_at",
      next_revocation_epoch
    )
    ON CONFLICT ("workspace_id") DO NOTHING
    RETURNING "workspace_id", "organization_id", "archived_at", "revocation_epoch"
    INTO
      marker_workspace_id,
      marker_organization_id,
      marker_archived_at,
      marker_revocation_epoch;

    IF NOT FOUND THEN
      SELECT "workspace_id", "organization_id", "archived_at", "revocation_epoch"
      INTO
        marker_workspace_id,
        marker_organization_id,
        marker_archived_at,
        marker_revocation_epoch
      FROM "agent_observability_workspace_archive_revocations"
      WHERE "organization_id" = workspace_row."organization_id"
        AND "workspace_id" = workspace_row."id"
      FOR UPDATE;

      IF NOT FOUND
        OR marker_workspace_id IS DISTINCT FROM workspace_row."id"
        OR marker_organization_id IS DISTINCT FROM workspace_row."organization_id"
        OR marker_archived_at IS DISTINCT FROM workspace_row."archived_at"
        OR marker_revocation_epoch IS NULL
        OR marker_revocation_epoch <= 0
        OR marker_revocation_epoch > 9007199254740991
        OR marker_revocation_epoch IS DISTINCT FROM setting_revocation_epoch THEN
        RAISE EXCEPTION 'Workspace observability archive marker has invalid state for workspace %', workspace_row."id"
          USING ERRCODE = '23514';
      END IF;
      CONTINUE;
    END IF;

    UPDATE "agent_observability_workspace_settings"
    SET "revocation_epoch" = marker_revocation_epoch,
        "updated_at" = workspace_row."archived_at"
    WHERE "organization_id" = workspace_row."organization_id"
      AND "workspace_id" = workspace_row."id"
      AND "revocation_epoch" = setting_revocation_epoch;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Workspace observability setting archive CAS lost for workspace %', workspace_row."id"
        USING ERRCODE = '23514';
    END IF;
  END LOOP;
END;
$$;
-- workspace-archive-observability-revocation-catch-up-backfill-end
