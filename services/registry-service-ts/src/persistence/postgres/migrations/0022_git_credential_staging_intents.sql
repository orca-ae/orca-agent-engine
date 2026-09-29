CREATE TABLE "git_credential_staging_intents" (
	"git_credential_id" text PRIMARY KEY NOT NULL,
	"workspace_id" text NOT NULL,
	"session_resource_id" text NOT NULL,
	"secret_ref" text NOT NULL,
	"status" text DEFAULT 'pending' NOT NULL,
	"cleanup_after" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "git_credential_staging_status_check" CHECK ("git_credential_staging_intents"."status" in ('pending', 'cleaning'))
);
--> statement-breakpoint
CREATE INDEX "git_credential_staging_cleanup_idx" ON "git_credential_staging_intents" USING btree ("status","cleanup_after");--> statement-breakpoint
CREATE UNIQUE INDEX "git_credential_staging_secret_ref_idx" ON "git_credential_staging_intents" USING btree ("secret_ref");