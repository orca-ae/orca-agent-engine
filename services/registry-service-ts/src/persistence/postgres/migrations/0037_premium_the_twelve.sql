CREATE TABLE "skill_bundle_deletion_outbox" (
	"workspace_id" text NOT NULL,
	"skill_version_id" text NOT NULL,
	"package_sha256" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"attempt_count" integer DEFAULT 0 NOT NULL,
	"last_attempt_at" timestamp with time zone,
	CONSTRAINT "skill_bundle_deletion_outbox_pk" PRIMARY KEY("workspace_id","skill_version_id","package_sha256"),
	CONSTRAINT "skill_bundle_deletion_outbox_sha256_check" CHECK ("skill_bundle_deletion_outbox"."package_sha256" ~ '^[0-9a-f]{64}$')
);
--> statement-breakpoint
CREATE INDEX "skill_bundle_deletion_outbox_pending_idx" ON "skill_bundle_deletion_outbox" USING btree ("created_at");