CREATE TABLE IF NOT EXISTS "git_credentials" (
  "id" text PRIMARY KEY NOT NULL,
  "workspace_id" text NOT NULL,
  "provider" text DEFAULT 'github' NOT NULL,
  "repo_url" text NOT NULL,
  "secret_ref" text NOT NULL,
  "metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "archived_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "git_credentials_provider_check" CHECK ("git_credentials"."provider" in ('github'))
);
CREATE INDEX IF NOT EXISTS "git_credentials_workspace_idx" ON "git_credentials" ("workspace_id", "archived_at");
CREATE UNIQUE INDEX IF NOT EXISTS "git_credentials_active_repo_idx" ON "git_credentials" ("workspace_id", "repo_url") WHERE "git_credentials"."archived_at" is null;
