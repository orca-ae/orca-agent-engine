CREATE TABLE "session_events_index" (
	"workspace_id" text NOT NULL,
	"session_id" text NOT NULL,
	"seq" bigint NOT NULL,
	"event_id" text NOT NULL,
	"subpath" text DEFAULT '' NOT NULL,
	"produced_at" text NOT NULL,
	"produced_by" text NOT NULL,
	"kind" text NOT NULL,
	"visibility" text NOT NULL,
	"payload" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"indexed_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "session_events_index_workspace_id_session_id_event_id_pk" PRIMARY KEY("workspace_id","session_id","event_id")
);
--> statement-breakpoint
CREATE INDEX "session_events_index_public_seq_idx" ON "session_events_index" USING btree ("workspace_id","session_id","visibility","subpath","seq");--> statement-breakpoint
CREATE INDEX "session_events_index_session_seq_idx" ON "session_events_index" USING btree ("workspace_id","session_id","seq");
