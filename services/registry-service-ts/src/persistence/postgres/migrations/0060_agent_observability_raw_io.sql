ALTER TABLE "agent_observability_binding_versions" DROP CONSTRAINT "agent_observability_binding_versions_capture_check";--> statement-breakpoint
ALTER TABLE "agent_observability_organization_settings" DROP CONSTRAINT "agent_observability_organization_settings_capture_check";--> statement-breakpoint
ALTER TABLE "agent_observability_platform_policy" DROP CONSTRAINT "agent_observability_platform_policy_capture_check";--> statement-breakpoint
ALTER TABLE "agent_observability_workspace_settings" DROP CONSTRAINT "agent_observability_workspace_settings_capture_check";--> statement-breakpoint
ALTER TABLE "session_observability_bindings" DROP CONSTRAINT "session_observability_bindings_capture_check";--> statement-breakpoint
ALTER TABLE "agent_observability_binding_versions" ADD CONSTRAINT "agent_observability_binding_versions_capture_check" CHECK ("agent_observability_binding_versions"."capture_mode" in ('metadata_only', 'redacted_io', 'raw_io'));--> statement-breakpoint
ALTER TABLE "agent_observability_organization_settings" ADD CONSTRAINT "agent_observability_organization_settings_capture_check" CHECK ("agent_observability_organization_settings"."capture_ceiling" in ('metadata_only', 'redacted_io', 'raw_io'));--> statement-breakpoint
ALTER TABLE "agent_observability_platform_policy" ADD CONSTRAINT "agent_observability_platform_policy_capture_check" CHECK ("agent_observability_platform_policy"."max_capture_mode" in ('metadata_only', 'redacted_io', 'raw_io'));--> statement-breakpoint
ALTER TABLE "agent_observability_workspace_settings" ADD CONSTRAINT "agent_observability_workspace_settings_capture_check" CHECK ("agent_observability_workspace_settings"."capture_ceiling" in ('metadata_only', 'redacted_io', 'raw_io'));--> statement-breakpoint
-- Enforce new writes immediately without scanning Session history under the DDL lock.
-- src/migrate.ts validates this check after Drizzle commits and releases that lock.
ALTER TABLE "session_observability_bindings" ADD CONSTRAINT "session_observability_bindings_capture_check" CHECK ("session_observability_bindings"."effective_capture_mode" in ('metadata_only', 'redacted_io', 'raw_io')) NOT VALID;
--> statement-breakpoint
-- Preserve the mixed-version archive guard while admitting the new capture mode.
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
      AND setting_capture_ceiling IS DISTINCT FROM 'raw_io'
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
