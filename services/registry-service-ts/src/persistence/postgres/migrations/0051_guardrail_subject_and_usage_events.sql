CREATE TABLE "session_usage_events" (
	"workspace_id" text NOT NULL,
	"session_id" text NOT NULL,
	"event_id" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "session_usage_events_workspace_id_session_id_event_id_pk" PRIMARY KEY("workspace_id","session_id","event_id")
);
--> statement-breakpoint
ALTER TABLE "session_events_index" ADD COLUMN "guardrail_subject" text;--> statement-breakpoint
ALTER TABLE "agent_triggers" ADD COLUMN "guardrail_subject" text;--> statement-breakpoint
UPDATE "agent_triggers" SET "guardrail_subject" = 'trigger:' || "id" WHERE "guardrail_subject" IS NULL;--> statement-breakpoint
ALTER TABLE "agent_triggers" ALTER COLUMN "guardrail_subject" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "session_usage_events" ADD CONSTRAINT "session_usage_events_workspace_session_fk" FOREIGN KEY ("workspace_id","session_id") REFERENCES "public"."sessions"("workspace_id","id") ON DELETE cascade ON UPDATE no action;
