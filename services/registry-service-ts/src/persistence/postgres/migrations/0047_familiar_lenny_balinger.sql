DROP INDEX "agent_trigger_fires_pending_idx";--> statement-breakpoint
DROP INDEX "agent_trigger_fires_trigger_sessions_idx";--> statement-breakpoint
ALTER TABLE "agent_trigger_fires" ADD COLUMN "next_attempt_at" timestamp with time zone DEFAULT now() NOT NULL;--> statement-breakpoint
CREATE INDEX "agent_trigger_fires_pending_idx" ON "agent_trigger_fires" USING btree ("next_attempt_at","scheduled_for","id") WHERE "agent_trigger_fires"."status" = 'pending';--> statement-breakpoint
CREATE INDEX "agent_trigger_fires_trigger_sessions_idx" ON "agent_trigger_fires" USING btree ("workspace_id","trigger_id","scheduled_for" DESC NULLS LAST,"id" DESC NULLS LAST) WHERE "agent_trigger_fires"."status" = 'enqueued';