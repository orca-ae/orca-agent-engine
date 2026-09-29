CREATE TABLE "agent_trigger_fires" (
	"id" text PRIMARY KEY NOT NULL,
	"workspace_id" text NOT NULL,
	"trigger_id" text NOT NULL,
	"generation" integer NOT NULL,
	"scheduled_for" timestamp with time zone NOT NULL,
	"status" text DEFAULT 'pending' NOT NULL,
	"planned_session_id" text NOT NULL,
	"session_id" text,
	"event_id" text NOT NULL,
	"attempt_count" integer DEFAULT 0 NOT NULL,
	"last_error" text,
	"enqueued_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "agent_trigger_fires_status_check" CHECK ("agent_trigger_fires"."status" in ('pending', 'enqueued', 'failed', 'canceled')),
	CONSTRAINT "agent_trigger_fires_generation_check" CHECK ("agent_trigger_fires"."generation" > 0)
);
--> statement-breakpoint
CREATE TABLE "agent_triggers" (
	"id" text PRIMARY KEY NOT NULL,
	"workspace_id" text NOT NULL,
	"name" text NOT NULL,
	"agent_id" text NOT NULL,
	"agent_version" integer NOT NULL,
	"environment_id" text NOT NULL,
	"title_template" text,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"vault_ids" text[] DEFAULT '{}' NOT NULL,
	"payload" text NOT NULL,
	"cron_expression" text NOT NULL,
	"timezone" text DEFAULT 'Etc/UTC' NOT NULL,
	"status" text DEFAULT 'active' NOT NULL,
	"generation" integer DEFAULT 1 NOT NULL,
	"next_fire_at" timestamp with time zone,
	"last_fired_at" timestamp with time zone,
	"last_error" text,
	"archived_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "agent_triggers_status_check" CHECK ("agent_triggers"."status" in ('active', 'paused', 'archived')),
	CONSTRAINT "agent_triggers_generation_check" CHECK ("agent_triggers"."generation" > 0)
);
--> statement-breakpoint
CREATE UNIQUE INDEX "agent_triggers_workspace_id_idx" ON "agent_triggers" USING btree ("workspace_id","id");--> statement-breakpoint
ALTER TABLE "agent_trigger_fires" ADD CONSTRAINT "agent_trigger_fires_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_trigger_fires" ADD CONSTRAINT "agent_trigger_fires_workspace_trigger_fk" FOREIGN KEY ("workspace_id","trigger_id") REFERENCES "public"."agent_triggers"("workspace_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_triggers" ADD CONSTRAINT "agent_triggers_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_triggers" ADD CONSTRAINT "agent_triggers_workspace_agent_version_fk" FOREIGN KEY ("workspace_id","agent_id","agent_version") REFERENCES "public"."agent_versions"("workspace_id","agent_id","version") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_triggers" ADD CONSTRAINT "agent_triggers_workspace_environment_fk" FOREIGN KEY ("workspace_id","environment_id") REFERENCES "public"."environments"("workspace_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "agent_trigger_fires_slot_idx" ON "agent_trigger_fires" USING btree ("workspace_id","trigger_id","generation","scheduled_for");--> statement-breakpoint
CREATE INDEX "agent_trigger_fires_pending_idx" ON "agent_trigger_fires" USING btree ("status","scheduled_for","id");--> statement-breakpoint
CREATE INDEX "agent_trigger_fires_trigger_sessions_idx" ON "agent_trigger_fires" USING btree ("workspace_id","trigger_id","enqueued_at","id");--> statement-breakpoint
CREATE INDEX "agent_triggers_workspace_list_idx" ON "agent_triggers" USING btree ("workspace_id","archived_at","created_at" DESC NULLS LAST,"id" DESC NULLS LAST);--> statement-breakpoint
CREATE INDEX "agent_triggers_due_idx" ON "agent_triggers" USING btree ("next_fire_at") WHERE "agent_triggers"."status" = 'active' and "agent_triggers"."archived_at" is null and "agent_triggers"."next_fire_at" is not null;
