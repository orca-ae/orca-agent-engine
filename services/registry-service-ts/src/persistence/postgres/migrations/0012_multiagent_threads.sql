ALTER TABLE "agents" ADD COLUMN "multiagent" jsonb;

CREATE TABLE "session_threads" (
  "id" text PRIMARY KEY NOT NULL,
  "workspace_id" text NOT NULL,
  "session_id" text NOT NULL,
  "subpath" text NOT NULL,
  "agent_id" text NOT NULL,
  "agent_version" integer NOT NULL,
  "agent_name" text NOT NULL,
  "status" text DEFAULT 'idle' NOT NULL,
  "stop_reason" text,
  "archived_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

CREATE INDEX "session_threads_session_idx" ON "session_threads"
  USING btree ("workspace_id", "session_id", "archived_at");

CREATE UNIQUE INDEX "session_threads_session_subpath_idx" ON "session_threads"
  USING btree ("workspace_id", "session_id", "subpath");
