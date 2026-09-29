CREATE TABLE "agent_observability_credential_staging_intents" (
	"reservation_id" text PRIMARY KEY NOT NULL,
	"generation" bigint NOT NULL,
	"candidate_binding_id" text NOT NULL,
	"proposed_credential_version" integer NOT NULL,
	"secret_ref" text NOT NULL,
	"status" text DEFAULT 'pending' NOT NULL,
	"writer_token" text,
	"writer_lease_expires_at" timestamp with time zone,
	"put_completed_at" timestamp with time zone,
	"cleanup_token" text,
	"cleanup_lease_expires_at" timestamp with time zone,
	"next_cleanup_at" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "agent_observability_credential_staging_intents_status_check" CHECK ((
        "agent_observability_credential_staging_intents"."status" = 'pending'
        and "agent_observability_credential_staging_intents"."writer_token" is null
        and "agent_observability_credential_staging_intents"."writer_lease_expires_at" is null
        and "agent_observability_credential_staging_intents"."put_completed_at" is null
        and "agent_observability_credential_staging_intents"."cleanup_token" is null
        and "agent_observability_credential_staging_intents"."cleanup_lease_expires_at" is null
      ) or (
        "agent_observability_credential_staging_intents"."status" = 'writing'
        and char_length(btrim("agent_observability_credential_staging_intents"."writer_token")) > 0
        and "agent_observability_credential_staging_intents"."writer_lease_expires_at" is not null
        and "agent_observability_credential_staging_intents"."put_completed_at" is null
        and "agent_observability_credential_staging_intents"."cleanup_token" is null
        and "agent_observability_credential_staging_intents"."cleanup_lease_expires_at" is null
      ) or (
        "agent_observability_credential_staging_intents"."status" = 'written'
        and "agent_observability_credential_staging_intents"."writer_token" is null
        and "agent_observability_credential_staging_intents"."writer_lease_expires_at" is null
        and "agent_observability_credential_staging_intents"."put_completed_at" is not null
        and "agent_observability_credential_staging_intents"."cleanup_token" is null
        and "agent_observability_credential_staging_intents"."cleanup_lease_expires_at" is null
      ) or (
        "agent_observability_credential_staging_intents"."status" = 'cleanup_pending'
        and "agent_observability_credential_staging_intents"."writer_token" is null
        and "agent_observability_credential_staging_intents"."writer_lease_expires_at" is null
        and "agent_observability_credential_staging_intents"."cleanup_token" is null
        and "agent_observability_credential_staging_intents"."cleanup_lease_expires_at" is null
      ) or (
        "agent_observability_credential_staging_intents"."status" = 'cleaning'
        and "agent_observability_credential_staging_intents"."writer_token" is null
        and "agent_observability_credential_staging_intents"."writer_lease_expires_at" is null
        and char_length(btrim("agent_observability_credential_staging_intents"."cleanup_token")) > 0
        and "agent_observability_credential_staging_intents"."cleanup_lease_expires_at" is not null
      )),
	CONSTRAINT "agent_observability_credential_staging_intents_values_check" CHECK ("agent_observability_credential_staging_intents"."generation" > 0
          and char_length(btrim("agent_observability_credential_staging_intents"."candidate_binding_id")) > 0
          and "agent_observability_credential_staging_intents"."proposed_credential_version" > 0
          and char_length(btrim("agent_observability_credential_staging_intents"."secret_ref")) > 0
          and "agent_observability_credential_staging_intents"."next_cleanup_at" >= "agent_observability_credential_staging_intents"."created_at")
);
--> statement-breakpoint
CREATE TABLE "agent_observability_idempotency_keys" (
	"organization_id" text NOT NULL,
	"principal" text NOT NULL,
	"scope" text NOT NULL,
	"key" text NOT NULL,
	"target_key" text NOT NULL,
	"body_hash" text NOT NULL,
	"status" text DEFAULT 'pending' NOT NULL,
	"reservation_id" text NOT NULL,
	"response_status" integer,
	"response_body" jsonb,
	"completed_at" timestamp with time zone,
	"expires_at" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "agent_observability_idempotency_keys_pk" PRIMARY KEY("organization_id","principal","scope","key"),
	CONSTRAINT "agent_observability_idempotency_keys_values_check" CHECK (char_length(btrim("agent_observability_idempotency_keys"."principal")) > 0
          and char_length(btrim("agent_observability_idempotency_keys"."scope")) > 0
          and char_length(btrim("agent_observability_idempotency_keys"."key")) > 0
          and char_length(btrim("agent_observability_idempotency_keys"."target_key")) > 0
          and "agent_observability_idempotency_keys"."body_hash" ~ '^[0-9a-f]{64}$'),
	CONSTRAINT "agent_observability_idempotency_keys_status_check" CHECK ((
        "agent_observability_idempotency_keys"."status" = 'pending'
        and "agent_observability_idempotency_keys"."response_status" is null
        and "agent_observability_idempotency_keys"."response_body" is null
        and "agent_observability_idempotency_keys"."completed_at" is null
        and "agent_observability_idempotency_keys"."expires_at" > "agent_observability_idempotency_keys"."created_at"
      ) or (
        "agent_observability_idempotency_keys"."status" = 'completed'
        and "agent_observability_idempotency_keys"."response_status" between 200 and 599
        and "agent_observability_idempotency_keys"."response_body" is not null
        and "agent_observability_idempotency_keys"."completed_at" is not null
        and "agent_observability_idempotency_keys"."expires_at" > "agent_observability_idempotency_keys"."created_at"
      ))
);
--> statement-breakpoint
CREATE TABLE "agent_observability_mutation_reservations" (
	"id" text PRIMARY KEY NOT NULL,
	"organization_id" text NOT NULL,
	"workspace_id" text,
	"target_type" text NOT NULL,
	"target_key" text NOT NULL,
	"target_binding_id" text,
	"target_binding_scope" text,
	"owner_principal" text NOT NULL,
	"body_hash" text NOT NULL,
	"expected_state_version" text NOT NULL,
	"expected_config_version" integer,
	"expected_credential_version" integer,
	"generation" bigint NOT NULL,
	"status" text DEFAULT 'pending' NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"fenced_at" timestamp with time zone,
	"expired_at" timestamp with time zone,
	"committed_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "agent_observability_mutation_reservations_target_check" CHECK ((
        "agent_observability_mutation_reservations"."target_type" = 'organization_setting'
        and "agent_observability_mutation_reservations"."workspace_id" is null
        and "agent_observability_mutation_reservations"."target_binding_id" is null
        and "agent_observability_mutation_reservations"."target_binding_scope" is null
      ) or (
        "agent_observability_mutation_reservations"."target_type" = 'workspace_setting'
        and "agent_observability_mutation_reservations"."workspace_id" is not null
        and "agent_observability_mutation_reservations"."target_binding_id" is null
        and "agent_observability_mutation_reservations"."target_binding_scope" is null
      ) or (
        "agent_observability_mutation_reservations"."target_type" = 'binding'
        and "agent_observability_mutation_reservations"."target_binding_id" is not null
        and (
          ("agent_observability_mutation_reservations"."target_binding_scope" = 'organization' and "agent_observability_mutation_reservations"."workspace_id" is null)
          or ("agent_observability_mutation_reservations"."target_binding_scope" = 'workspace' and "agent_observability_mutation_reservations"."workspace_id" is not null)
        )
      )),
	CONSTRAINT "agent_observability_mutation_reservations_versions_check" CHECK (char_length(btrim("agent_observability_mutation_reservations"."target_key")) > 0
          and char_length(btrim("agent_observability_mutation_reservations"."owner_principal")) > 0
          and "agent_observability_mutation_reservations"."body_hash" ~ '^[0-9a-f]{64}$'
          and char_length(btrim("agent_observability_mutation_reservations"."expected_state_version")) > 0
          and ("agent_observability_mutation_reservations"."expected_config_version" is null or "agent_observability_mutation_reservations"."expected_config_version" > 0)
          and ("agent_observability_mutation_reservations"."expected_credential_version" is null or "agent_observability_mutation_reservations"."expected_credential_version" > 0)
          and "agent_observability_mutation_reservations"."generation" > 0
          and "agent_observability_mutation_reservations"."expires_at" > "agent_observability_mutation_reservations"."created_at"),
	CONSTRAINT "agent_observability_mutation_reservations_status_check" CHECK ((
        "agent_observability_mutation_reservations"."status" = 'pending'
        and "agent_observability_mutation_reservations"."fenced_at" is null
        and "agent_observability_mutation_reservations"."expired_at" is null
        and "agent_observability_mutation_reservations"."committed_at" is null
      ) or (
        "agent_observability_mutation_reservations"."status" = 'fenced'
        and "agent_observability_mutation_reservations"."fenced_at" is not null
        and "agent_observability_mutation_reservations"."expired_at" is null
        and "agent_observability_mutation_reservations"."committed_at" is null
      ) or (
        "agent_observability_mutation_reservations"."status" = 'expired'
        and "agent_observability_mutation_reservations"."fenced_at" is null
        and "agent_observability_mutation_reservations"."expired_at" is not null
        and "agent_observability_mutation_reservations"."committed_at" is null
      ) or (
        "agent_observability_mutation_reservations"."status" = 'committed'
        and "agent_observability_mutation_reservations"."fenced_at" is null
        and "agent_observability_mutation_reservations"."expired_at" is null
        and "agent_observability_mutation_reservations"."committed_at" is not null
      ))
);
--> statement-breakpoint
CREATE TABLE "agent_observability_secret_cleanup_outbox" (
	"id" text PRIMARY KEY NOT NULL,
	"organization_id" text NOT NULL,
	"binding_id" text NOT NULL,
	"binding_scope" text NOT NULL,
	"secret_ref" text NOT NULL,
	"status" text DEFAULT 'pending' NOT NULL,
	"claim_token" text,
	"lease_expires_at" timestamp with time zone,
	"attempt_count" integer DEFAULT 0 NOT NULL,
	"next_attempt_at" timestamp with time zone DEFAULT now() NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "agent_observability_secret_cleanup_outbox_status_check" CHECK ((
        "agent_observability_secret_cleanup_outbox"."status" = 'pending'
        and "agent_observability_secret_cleanup_outbox"."claim_token" is null
        and "agent_observability_secret_cleanup_outbox"."lease_expires_at" is null
      ) or (
        "agent_observability_secret_cleanup_outbox"."status" = 'deleting'
        and char_length(btrim("agent_observability_secret_cleanup_outbox"."claim_token")) > 0
        and "agent_observability_secret_cleanup_outbox"."lease_expires_at" is not null
      )),
	CONSTRAINT "agent_observability_secret_cleanup_outbox_values_check" CHECK ("agent_observability_secret_cleanup_outbox"."binding_scope" in ('organization', 'workspace')
          and char_length(btrim("agent_observability_secret_cleanup_outbox"."secret_ref")) > 0
          and "agent_observability_secret_cleanup_outbox"."attempt_count" >= 0)
);
--> statement-breakpoint
-- PostgreSQL requires these composite parent keys before child FK creation.
CREATE UNIQUE INDEX "agent_observability_mutation_reservations_organization_id_idx" ON "agent_observability_mutation_reservations" USING btree ("organization_id","id");
--> statement-breakpoint
CREATE UNIQUE INDEX "agent_observability_mutation_reservations_id_generation_idx" ON "agent_observability_mutation_reservations" USING btree ("id","generation");
--> statement-breakpoint
ALTER TABLE "agent_observability_credential_staging_intents" ADD CONSTRAINT "agent_observability_credential_staging_intents_reservation_fk" FOREIGN KEY ("reservation_id","generation") REFERENCES "public"."agent_observability_mutation_reservations"("id","generation") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_idempotency_keys" ADD CONSTRAINT "agent_observability_idempotency_keys_reservation_fk" FOREIGN KEY ("organization_id","reservation_id") REFERENCES "public"."agent_observability_mutation_reservations"("organization_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_mutation_reservations" ADD CONSTRAINT "agent_observability_mutation_reservations_organization_setting_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."agent_observability_organization_settings"("organization_id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_mutation_reservations" ADD CONSTRAINT "agent_observability_mutation_reservations_workspace_setting_fk" FOREIGN KEY ("organization_id","workspace_id") REFERENCES "public"."agent_observability_workspace_settings"("organization_id","workspace_id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_mutation_reservations" ADD CONSTRAINT "agent_observability_mutation_reservations_binding_owner_fk" FOREIGN KEY ("organization_id","target_binding_id","target_binding_scope") REFERENCES "public"."agent_observability_bindings"("organization_id","id","scope_type") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_mutation_reservations" ADD CONSTRAINT "agent_observability_mutation_reservations_binding_workspace_fk" FOREIGN KEY ("organization_id","workspace_id","target_binding_id") REFERENCES "public"."agent_observability_bindings"("organization_id","workspace_id","id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_observability_secret_cleanup_outbox" ADD CONSTRAINT "agent_observability_secret_cleanup_outbox_binding_owner_fk" FOREIGN KEY ("organization_id","binding_id","binding_scope") REFERENCES "public"."agent_observability_bindings"("organization_id","id","scope_type") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "agent_observability_credential_staging_intents_secret_ref_idx" ON "agent_observability_credential_staging_intents" USING btree ("secret_ref");--> statement-breakpoint
CREATE INDEX "agent_observability_credential_staging_intents_cleanup_idx" ON "agent_observability_credential_staging_intents" USING btree ("status","next_cleanup_at","writer_lease_expires_at","cleanup_lease_expires_at");--> statement-breakpoint
CREATE UNIQUE INDEX "agent_observability_idempotency_keys_reservation_idx" ON "agent_observability_idempotency_keys" USING btree ("reservation_id");--> statement-breakpoint
CREATE INDEX "agent_observability_idempotency_keys_expiry_idx" ON "agent_observability_idempotency_keys" USING btree ("status","expires_at");--> statement-breakpoint
CREATE UNIQUE INDEX "agent_observability_mutation_reservations_live_target_idx" ON "agent_observability_mutation_reservations" USING btree ("target_key") WHERE "agent_observability_mutation_reservations"."status" = 'pending';--> statement-breakpoint
CREATE INDEX "agent_observability_mutation_reservations_expiry_idx" ON "agent_observability_mutation_reservations" USING btree ("status","expires_at");--> statement-breakpoint
CREATE INDEX "agent_observability_mutation_reservations_target_generation_idx" ON "agent_observability_mutation_reservations" USING btree ("target_key","generation");--> statement-breakpoint
CREATE UNIQUE INDEX "agent_observability_secret_cleanup_outbox_binding_ref_idx" ON "agent_observability_secret_cleanup_outbox" USING btree ("binding_id","secret_ref");--> statement-breakpoint
CREATE INDEX "agent_observability_secret_cleanup_outbox_claim_idx" ON "agent_observability_secret_cleanup_outbox" USING btree ("status","next_attempt_at","lease_expires_at");
