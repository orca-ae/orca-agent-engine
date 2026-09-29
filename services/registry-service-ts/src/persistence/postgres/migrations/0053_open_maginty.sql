CREATE TABLE "environment_claims" (
	"environment_id" text PRIMARY KEY NOT NULL,
	"owner_pod" text NOT NULL,
	"worker_conn_id" text NOT NULL,
	"claimed_at" timestamp with time zone DEFAULT now() NOT NULL,
	"last_ping" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "environments" ADD COLUMN "env_key_digest" text;--> statement-breakpoint
ALTER TABLE "environments" ADD COLUMN "env_key_expires_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "environments" ADD COLUMN "environment_token_digest" text;--> statement-breakpoint
ALTER TABLE "environments" ADD COLUMN "environment_token_expires_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "environments" ADD COLUMN "egress_mode" text;--> statement-breakpoint
ALTER TABLE "environments" ADD COLUMN "llm" jsonb;--> statement-breakpoint
ALTER TABLE "sessions" ADD COLUMN "runner_id" text;--> statement-breakpoint
ALTER TABLE "sessions" ADD COLUMN "host_environment_id" text;--> statement-breakpoint
ALTER TABLE "sessions" ADD COLUMN "distribution_state" text;--> statement-breakpoint
ALTER TABLE "environment_claims" ADD CONSTRAINT "environment_claims_environment_id_environments_id_fk" FOREIGN KEY ("environment_id") REFERENCES "public"."environments"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "environments" ADD CONSTRAINT "environments_egress_mode_check" CHECK ("environments"."egress_mode" in ('gateway', 'sidecar'));