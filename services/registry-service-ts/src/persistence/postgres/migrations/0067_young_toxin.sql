-- Copyright The Orca Authors
-- SPDX-License-Identifier: Apache-2.0
-- Native Dogwood policy schema; no imported migration. Generated from Registry Drizzle tables.
CREATE TABLE "guardrail_policy_batches" (
	"workspace_id" text NOT NULL,
	"session_id" text NOT NULL,
	"batch_id" text NOT NULL,
	"digest" text NOT NULL,
	"response" jsonb NOT NULL,
	CONSTRAINT "guardrail_policy_batches_workspace_id_session_id_batch_id_pk" PRIMARY KEY("workspace_id","session_id","batch_id")
);
--> statement-breakpoint
CREATE TABLE "guardrail_policy_bindings" (
	"workspace_id" text NOT NULL,
	"session_id" text NOT NULL,
	"epoch" bigint NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT "guardrail_policy_bindings_workspace_id_session_id_pk" PRIMARY KEY("workspace_id","session_id")
);
--> statement-breakpoint
CREATE TABLE "guardrail_policy_calls" (
	"workspace_id" text NOT NULL,
	"session_id" text NOT NULL,
	"tool_use_id" text NOT NULL,
	"data" jsonb NOT NULL,
	CONSTRAINT "guardrail_policy_calls_workspace_id_session_id_tool_use_id_pk" PRIMARY KEY("workspace_id","session_id","tool_use_id")
);
--> statement-breakpoint
CREATE TABLE "guardrail_policy_events" (
	"workspace_id" text NOT NULL,
	"session_id" text NOT NULL,
	"seq" bigint NOT NULL,
	"principal_key" text,
	"principal_seq" bigint,
	"timestamp_ms" bigint NOT NULL,
	"payload" jsonb NOT NULL,
	CONSTRAINT "guardrail_policy_events_workspace_id_session_id_seq_pk" PRIMARY KEY("workspace_id","session_id","seq")
);
--> statement-breakpoint
CREATE TABLE "guardrail_policy_heads" (
	"workspace_id" text NOT NULL,
	"scope" text NOT NULL,
	"subject" text NOT NULL,
	"head" bigint DEFAULT 0 NOT NULL,
	"last_time" bigint DEFAULT 0 NOT NULL,
	CONSTRAINT "guardrail_policy_heads_workspace_id_scope_subject_pk" PRIMARY KEY("workspace_id","scope","subject"),
	CONSTRAINT "policy_head_scope" CHECK ("guardrail_policy_heads"."scope" in ('session','principal'))
);
--> statement-breakpoint
ALTER TABLE "guardrail_policy_batches" ADD CONSTRAINT "guardrail_policy_batches_workspace_id_session_id_sessions_workspace_id_id_fk" FOREIGN KEY ("workspace_id","session_id") REFERENCES "public"."sessions"("workspace_id","id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "guardrail_policy_bindings" ADD CONSTRAINT "guardrail_policy_bindings_workspace_id_session_id_sessions_workspace_id_id_fk" FOREIGN KEY ("workspace_id","session_id") REFERENCES "public"."sessions"("workspace_id","id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "guardrail_policy_calls" ADD CONSTRAINT "guardrail_policy_calls_workspace_id_session_id_sessions_workspace_id_id_fk" FOREIGN KEY ("workspace_id","session_id") REFERENCES "public"."sessions"("workspace_id","id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "guardrail_policy_events" ADD CONSTRAINT "guardrail_policy_events_workspace_id_session_id_sessions_workspace_id_id_fk" FOREIGN KEY ("workspace_id","session_id") REFERENCES "public"."sessions"("workspace_id","id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "guardrail_policy_heads" ADD CONSTRAINT "guardrail_policy_heads_workspace_id_workspaces_id_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "policy_events_retention" ON "guardrail_policy_events" USING btree ("workspace_id","timestamp_ms");--> statement-breakpoint
CREATE INDEX "policy_events_principal" ON "guardrail_policy_events" USING btree ("workspace_id","principal_key","principal_seq");