CREATE TABLE "session_lifecycle_outbox" (
	"id" text PRIMARY KEY NOT NULL,
	"workspace_id" text NOT NULL,
	"session_id" text NOT NULL,
	"kind" text DEFAULT 'session.archived' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"published_at" timestamp with time zone,
	"attempt_count" integer DEFAULT 0 NOT NULL,
	"last_attempt_at" timestamp with time zone,
	CONSTRAINT "session_lifecycle_outbox_kind_check" CHECK ("session_lifecycle_outbox"."kind" in ('session.archived'))
);
--> statement-breakpoint
CREATE INDEX "session_lifecycle_outbox_pending_idx" ON "session_lifecycle_outbox" USING btree ("published_at","created_at");