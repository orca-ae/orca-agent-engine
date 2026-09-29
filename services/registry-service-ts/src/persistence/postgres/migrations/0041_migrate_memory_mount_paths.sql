DO $$
DECLARE
  resource record;
  canonical_path_pattern constant text := '^/mnt/memory/[A-Za-z0-9][A-Za-z0-9._-]{0,127}(/[A-Za-z0-9][A-Za-z0-9._-]{0,127})*/?$';
  base_segment text;
  candidate_segment text;
  candidate_path text;
  suffix integer;
BEGIN
  CREATE TEMP TABLE migration_0041_affected_sessions (
    workspace_id text NOT NULL,
    session_id text NOT NULL,
    PRIMARY KEY (workspace_id, session_id)
  ) ON COMMIT DROP;

  FOR resource IN
    SELECT
      "id",
      "workspace_id",
      "session_id"
    FROM "session_resources"
    WHERE "type" = 'memory_store'
      AND "detached_at" IS NULL
      AND "mount_path" !~ canonical_path_pattern
    ORDER BY "workspace_id", "session_id", "id"
    FOR UPDATE
  LOOP
    -- A non-migrating active mount at /, /mnt, or /mnt/memory owns every
    -- possible destination below /mnt/memory/. Fail before changing this row
    -- rather than silently create overlapping mount trees.
    IF EXISTS (
      SELECT 1
      FROM "session_resources" AS other
      WHERE other."workspace_id" = resource."workspace_id"
        AND other."session_id" = resource."session_id"
        AND other."id" <> resource."id"
        AND other."detached_at" IS NULL
        AND NOT (
          other."type" = 'memory_store'
          AND other."mount_path" !~ canonical_path_pattern
        )
        AND starts_with('/mnt/memory/', rtrim(other."mount_path", '/') || '/')
    ) THEN
      RAISE EXCEPTION
        '0041_migrate_memory_mount_paths cannot migrate workspace %, session %, resource %: an active resource owns an ancestor of /mnt/memory/',
        resource."workspace_id",
        resource."session_id",
        resource."id"
        USING HINT = 'Detach or move the blocking resource, then rerun the migration.';
    END IF;

    -- Existing API-created resource ids are safe path segments. Keep a hash
    -- fallback so manually inserted legacy rows cannot create an unsafe path.
    base_segment := CASE
      WHEN resource."id" ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,96}$'
        THEN 'legacy-' || resource."id"
      ELSE 'legacy-' || md5(
        resource."workspace_id" || ':' || resource."session_id" || ':' || resource."id"
      )
    END;

    suffix := 0;
    LOOP
      candidate_segment := base_segment || CASE
        WHEN suffix = 0 THEN ''
        ELSE '-' || suffix::text
      END;
      candidate_path := '/mnt/memory/' || candidate_segment || '/';

      -- Resolve exact and ancestor/descendant collisions against resources
      -- that remain active. Unprocessed legacy memory rows are ignored because
      -- this loop moves them later in the same transaction.
      EXIT WHEN NOT EXISTS (
        SELECT 1
        FROM "session_resources" AS other
        WHERE other."workspace_id" = resource."workspace_id"
          AND other."session_id" = resource."session_id"
          AND other."id" <> resource."id"
          AND other."detached_at" IS NULL
          AND NOT (
            other."type" = 'memory_store'
            AND other."mount_path" !~ canonical_path_pattern
          )
          AND (
            starts_with(rtrim(other."mount_path", '/') || '/', candidate_path)
            OR starts_with(candidate_path, rtrim(other."mount_path", '/') || '/')
          )
      );

      suffix := suffix + 1;
    END LOOP;

    UPDATE "session_resources"
    SET
      "mount_path" = candidate_path,
      "updated_at" = now()
    WHERE "workspace_id" = resource."workspace_id"
      AND "session_id" = resource."session_id"
      AND "id" = resource."id";

    INSERT INTO migration_0041_affected_sessions (workspace_id, session_id)
    VALUES (resource."workspace_id", resource."session_id")
    ON CONFLICT DO NOTHING;
  END LOOP;

  -- Resource mutations normally advance runtime_revision. Preserve that
  -- invariant so warm/idle runners rebuild from the migrated snapshot.
  UPDATE "sessions" AS session
  SET
    "runtime_revision" = session."runtime_revision" + 1,
    "updated_at" = now()
  FROM migration_0041_affected_sessions AS affected
  WHERE session."workspace_id" = affected.workspace_id
    AND session."id" = affected.session_id;

  DROP TABLE migration_0041_affected_sessions;
END $$;
