CREATE TABLE "session_native_completions" (
	"workspace_id" text NOT NULL,
	"session_id" text NOT NULL,
	"turn_id" text NOT NULL,
	"phase" text NOT NULL,
	"record" jsonb NOT NULL,
	CONSTRAINT "session_native_completions_workspace_id_session_id_turn_id_pk" PRIMARY KEY("workspace_id","session_id","turn_id"),
	CONSTRAINT "session_native_completions_phase_check" CHECK ("session_native_completions"."phase" in ('authority', 'running', 'prepared', 'completed', 'incomplete'))
);
--> statement-breakpoint
ALTER TABLE "session_native_completions" ADD CONSTRAINT "session_native_completions_workspace_session_fk" FOREIGN KEY ("workspace_id","session_id") REFERENCES "public"."sessions"("workspace_id","id") ON DELETE cascade ON UPDATE no action;