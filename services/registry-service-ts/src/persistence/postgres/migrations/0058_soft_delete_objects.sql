DROP INDEX "agent_triggers_due_idx";--> statement-breakpoint
DROP INDEX "environments_ws_name_idx";--> statement-breakpoint
DROP INDEX "git_credentials_active_repo_idx";--> statement-breakpoint
DROP INDEX "skill_versions_identifier_page_idx";--> statement-breakpoint
DROP INDEX "skills_ws_display_title_idx";--> statement-breakpoint
DROP INDEX "skills_workspace_created_page_idx";--> statement-breakpoint
DROP INDEX "skills_workspace_type_created_page_idx";--> statement-breakpoint
DROP INDEX "vault_credentials_active_url_idx";--> statement-breakpoint
DROP INDEX "vault_credentials_active_secret_name_idx";--> statement-breakpoint
DROP INDEX "vault_credentials_active_logical_id_idx";--> statement-breakpoint
ALTER TABLE "agent_triggers" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "agent_versions" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "agents" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "environments" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "git_credentials" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "guardrails" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "model_prices" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "session_resources" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "session_threads" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "sessions" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "skill_versions" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "skills" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "vault_credentials" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "vaults" ADD COLUMN "deleted_at" timestamp with time zone;--> statement-breakpoint
CREATE INDEX "agent_triggers_due_idx" ON "agent_triggers" USING btree ("next_fire_at") WHERE "agent_triggers"."status" = 'active' and "agent_triggers"."archived_at" is null and "agent_triggers"."deleted_at" is null and "agent_triggers"."next_fire_at" is not null;--> statement-breakpoint
CREATE UNIQUE INDEX "environments_ws_name_idx" ON "environments" USING btree ("workspace_id","name") WHERE "environments"."deleted_at" is null;--> statement-breakpoint
CREATE UNIQUE INDEX "git_credentials_active_repo_idx" ON "git_credentials" USING btree ("workspace_id","repo_url") WHERE "git_credentials"."archived_at" is null and "git_credentials"."deleted_at" is null and "git_credentials"."session_resource_id" is null;--> statement-breakpoint
CREATE INDEX "skill_versions_identifier_page_idx" ON "skill_versions" USING btree ("workspace_id","skill_id",("version_identifier"::numeric)) WHERE "skill_versions"."archived_at" is null and "skill_versions"."deleted_at" is null;--> statement-breakpoint
CREATE UNIQUE INDEX "skills_ws_display_title_idx" ON "skills" USING btree ("workspace_id","display_title") WHERE "skills"."type" = 'custom' and "skills"."display_title" is not null and "skills"."archived_at" is null and "skills"."deleted_at" is null;--> statement-breakpoint
CREATE INDEX "skills_workspace_created_page_idx" ON "skills" USING btree ("workspace_id","created_at","id") WHERE "skills"."archived_at" is null and "skills"."deleted_at" is null;--> statement-breakpoint
CREATE INDEX "skills_workspace_type_created_page_idx" ON "skills" USING btree ("workspace_id","type","created_at","id") WHERE "skills"."archived_at" is null and "skills"."deleted_at" is null;--> statement-breakpoint
CREATE UNIQUE INDEX "vault_credentials_active_url_idx" ON "vault_credentials" USING btree ("workspace_id","vault_id","mcp_server_url") WHERE "vault_credentials"."archived_at" is null and "vault_credentials"."deleted_at" is null;--> statement-breakpoint
CREATE UNIQUE INDEX "vault_credentials_active_secret_name_idx" ON "vault_credentials" USING btree ("workspace_id","vault_id","secret_name") WHERE "vault_credentials"."archived_at" is null and "vault_credentials"."deleted_at" is null and "vault_credentials"."secret_name" is not null;--> statement-breakpoint
CREATE UNIQUE INDEX "vault_credentials_active_logical_id_idx" ON "vault_credentials" USING btree ("workspace_id","vault_id","logical_id") WHERE "vault_credentials"."archived_at" is null and "vault_credentials"."deleted_at" is null and "vault_credentials"."logical_id" is not null;
--> statement-breakpoint
-- Enforce the same pin revocation for soft deletion and legacy physical deletion.
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
  IF TG_OP = 'UPDATE' AND NEW."deleted_at" IS NULL THEN
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
    lifecycle_at := CASE WHEN TG_OP = 'UPDATE' THEN NEW."deleted_at" ELSE statement_timestamp() END;
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

  IF TG_OP = 'UPDATE' THEN RETURN NEW; END IF;
  RETURN OLD;
END;
$$;
--> statement-breakpoint
DROP TRIGGER "agent_observability_sessions_enforce_lifecycle" ON "sessions";
--> statement-breakpoint
CREATE TRIGGER "agent_observability_sessions_enforce_lifecycle"
AFTER UPDATE OF "archived_at", "deleted_at" OR DELETE ON "sessions"
FOR EACH ROW EXECUTE FUNCTION "agent_observability_enforce_session_pin_lifecycle"();
--> statement-breakpoint
-- These existing tombstones were produced by DELETE, not a public archive action.
UPDATE "agent_triggers" SET "deleted_at" = "archived_at" WHERE "archived_at" IS NOT NULL;
--> statement-breakpoint
UPDATE "skill_versions" SET "deleted_at" = "archived_at" WHERE "archived_at" IS NOT NULL;
--> statement-breakpoint
UPDATE "session_resources" SET "deleted_at" = "detached_at" WHERE "detached_at" IS NOT NULL;
