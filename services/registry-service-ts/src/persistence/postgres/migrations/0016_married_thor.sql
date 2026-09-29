ALTER TABLE "skill_versions" ADD COLUMN "archived_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "skills" ADD COLUMN "type" text DEFAULT 'custom' NOT NULL;--> statement-breakpoint
ALTER TABLE "skills" ADD CONSTRAINT "skills_type_check" CHECK ("skills"."type" in ('anthropic', 'custom'));--> statement-breakpoint
-- Backfill: convert legacy agents.skills raw sklv_ id arrays into typed
-- {type:'custom', skill_id, version} references (managed-agents-2026-04-01).
-- String entries that resolve to a live skill_version are rewritten; entries
-- that are already objects (or orphaned ids with no matching version) are
-- preserved verbatim.
UPDATE "agents" a
SET "skills" = COALESCE((
  SELECT jsonb_agg(
    CASE
      WHEN jsonb_typeof(arr.elem) = 'string' AND sv."skill_id" IS NOT NULL
        THEN jsonb_build_object('type', 'custom', 'skill_id', sv."skill_id", 'version', sv."version")
      ELSE arr.elem
    END
    ORDER BY arr.ord
  )
  FROM jsonb_array_elements(a."skills") WITH ORDINALITY AS arr(elem, ord)
  LEFT JOIN "skill_versions" sv
    ON jsonb_typeof(arr.elem) = 'string' AND sv."id" = (arr.elem #>> '{}')
), '[]'::jsonb)
WHERE jsonb_typeof(a."skills") = 'array'
  AND EXISTS (
    SELECT 1 FROM jsonb_array_elements(a."skills") e WHERE jsonb_typeof(e) = 'string'
  );