ALTER TABLE "agents" ADD COLUMN "description" text;
--> statement-breakpoint
ALTER TABLE "agents" ADD COLUMN "model_effort" text;
--> statement-breakpoint
ALTER TABLE "environments" ADD COLUMN "scope" text;
--> statement-breakpoint
ALTER TABLE "skills" ADD COLUMN "display_title" text;
--> statement-breakpoint
UPDATE "agents"
SET "description" = "metadata"->>'description',
    "metadata" = "metadata" - 'description'
WHERE "metadata" ? 'description';
--> statement-breakpoint
UPDATE "agent_versions"
SET "snapshot" = jsonb_set(
  "snapshot" #- '{metadata,description}',
  '{description}',
  COALESCE("snapshot"->'metadata'->'description', 'null'::jsonb),
  true
)
WHERE "snapshot"->'metadata' ? 'description';
