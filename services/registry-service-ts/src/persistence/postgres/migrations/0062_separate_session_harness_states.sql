CREATE TABLE "session_harness_states" (
	"workspace_id" text NOT NULL,
	"session_id" text NOT NULL,
	"state" jsonb NOT NULL,
	CONSTRAINT "session_harness_states_workspace_id_session_id_pk" PRIMARY KEY("workspace_id","session_id")
);
--> statement-breakpoint
ALTER TABLE "session_harness_states" ADD CONSTRAINT "session_harness_states_workspace_session_fk" FOREIGN KEY ("workspace_id","session_id") REFERENCES "public"."sessions"("workspace_id","id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
-- Preserve checkpoints written by migration 0061's schema before removing the
-- large column from ordinary Session reads.
INSERT INTO "session_harness_states" ("workspace_id", "session_id", "state")
SELECT "workspace_id", "id", "harness_state" FROM "sessions" WHERE "harness_state" IS NOT NULL;
--> statement-breakpoint
ALTER TABLE "sessions" DROP COLUMN "harness_state";