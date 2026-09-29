CREATE TABLE "guardrail_counters" (
	"workspace_id" text NOT NULL,
	"subject" text NOT NULL,
	"window" text NOT NULL,
	"key" text NOT NULL,
	"value_num" double precision DEFAULT 0 NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "guardrail_counters_workspace_id_subject_window_key_pk" PRIMARY KEY("workspace_id","subject","window","key")
);
--> statement-breakpoint
CREATE TABLE "guardrail_state" (
	"workspace_id" text NOT NULL,
	"session_id" text NOT NULL,
	"key" text NOT NULL,
	"value_num" double precision,
	"value_json" jsonb,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "guardrail_state_workspace_id_session_id_key_pk" PRIMARY KEY("workspace_id","session_id","key")
);
--> statement-breakpoint
CREATE TABLE "guardrails" (
	"id" text PRIMARY KEY NOT NULL,
	"organization_id" text NOT NULL,
	"workspace_id" text,
	"name" text NOT NULL,
	"description" text,
	"enabled" boolean DEFAULT true NOT NULL,
	"phases" jsonb DEFAULT '[]'::jsonb NOT NULL,
	"scope" text NOT NULL,
	"rule" jsonb NOT NULL,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"archived_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "guardrails_scope_check" CHECK ("guardrails"."scope" in ('organization', 'workspace', 'explicit')),
	CONSTRAINT "guardrails_scope_workspace_check" CHECK (("guardrails"."scope" = 'organization') = ("guardrails"."workspace_id" is null))
);
--> statement-breakpoint
CREATE TABLE "model_prices" (
	"provider" text DEFAULT 'anthropic' NOT NULL,
	"organization_id" text DEFAULT '' NOT NULL,
	"model_id" text NOT NULL,
	"source" text NOT NULL,
	"input_per_million_tokens" double precision NOT NULL,
	"output_per_million_tokens" double precision NOT NULL,
	"cache_read_per_million_tokens" double precision,
	"cache_write_per_million_tokens" double precision,
	"fetched_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "model_prices_provider_model_id_source_organization_id_pk" PRIMARY KEY("provider","model_id","source","organization_id"),
	CONSTRAINT "model_prices_source_check" CHECK ("model_prices"."source" in ('operator', 'upstream', 'seed')),
	CONSTRAINT "model_prices_scope_check" CHECK (("model_prices"."source" = 'operator' and "model_prices"."organization_id" <> '') or ("model_prices"."source" <> 'operator' and "model_prices"."organization_id" = ''))
);
--> statement-breakpoint
ALTER TABLE "agents" ADD COLUMN "guardrail_ids" jsonb DEFAULT '[]'::jsonb NOT NULL;--> statement-breakpoint
ALTER TABLE "session_threads" ADD COLUMN "usage_input_tokens" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "session_threads" ADD COLUMN "usage_output_tokens" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "session_threads" ADD COLUMN "usage_cache_read_input_tokens" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "session_threads" ADD COLUMN "usage_cache_creation_ephemeral_1h_input_tokens" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "session_threads" ADD COLUMN "usage_cache_creation_ephemeral_5m_input_tokens" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "session_threads" ADD COLUMN "usage_cost_nano_usd" bigint;--> statement-breakpoint
ALTER TABLE "session_threads" ADD COLUMN "usage_has_unpriced" boolean DEFAULT false NOT NULL;--> statement-breakpoint
ALTER TABLE "sessions" ADD COLUMN "usage_cost_nano_usd" bigint;--> statement-breakpoint
ALTER TABLE "sessions" ADD COLUMN "usage_has_unpriced" boolean DEFAULT false NOT NULL;--> statement-breakpoint
ALTER TABLE "guardrail_state" ADD CONSTRAINT "guardrail_state_workspace_session_fk" FOREIGN KEY ("workspace_id","session_id") REFERENCES "public"."sessions"("workspace_id","id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "guardrails" ADD CONSTRAINT "guardrails_organization_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "guardrails" ADD CONSTRAINT "guardrails_workspace_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "guardrail_counters_window_idx" ON "guardrail_counters" USING btree ("workspace_id","window");--> statement-breakpoint
CREATE UNIQUE INDEX "guardrails_workspace_id_idx" ON "guardrails" USING btree ("workspace_id","id");--> statement-breakpoint
CREATE INDEX "guardrails_workspace_idx" ON "guardrails" USING btree ("workspace_id","archived_at");--> statement-breakpoint
CREATE INDEX "guardrails_organization_idx" ON "guardrails" USING btree ("organization_id","scope","archived_at");--> statement-breakpoint
CREATE INDEX "model_prices_organization_idx" ON "model_prices" USING btree ("organization_id","provider","model_id");