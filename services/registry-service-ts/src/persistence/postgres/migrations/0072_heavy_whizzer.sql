CREATE TABLE "sandbox_network_bindings" (
	"id" text PRIMARY KEY NOT NULL,
	"workspace_id" text NOT NULL,
	"environment_id" text NOT NULL,
	"owner_subject" text NOT NULL,
	"owner" jsonb NOT NULL,
	"generation" text NOT NULL,
	"capability_digest" text,
	"workload" jsonb,
	"pod" jsonb,
	"revoked_at" timestamp with time zone
);
--> statement-breakpoint
-- Old draft capabilities have no Sandbox parent. Retain their revoked history only.
UPDATE "sandbox_egress_bindings" SET "revoked_at" = now() WHERE "revoked_at" IS NULL;--> statement-breakpoint
ALTER TABLE "sandbox_egress_bindings" ADD COLUMN "network_binding_id" text;--> statement-breakpoint
ALTER TABLE "sandbox_egress_bindings" ADD CONSTRAINT "sandbox_egress_live_parent_check" CHECK ("revoked_at" IS NOT NULL OR "network_binding_id" IS NOT NULL);--> statement-breakpoint
ALTER TABLE "sandbox_network_bindings" ADD CONSTRAINT "sandbox_network_bindings_workspace_id_workspaces_id_fk" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "sandbox_network_bindings" ADD CONSTRAINT "sandbox_network_bindings_environment_id_environments_id_fk" FOREIGN KEY ("environment_id") REFERENCES "public"."environments"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "sandbox_network_capability_digest_idx" ON "sandbox_network_bindings" USING btree ("capability_digest");--> statement-breakpoint
CREATE UNIQUE INDEX "sandbox_network_current_owner_idx" ON "sandbox_network_bindings" USING btree ("owner_subject") WHERE "sandbox_network_bindings"."revoked_at" is null;--> statement-breakpoint
ALTER TABLE "sandbox_egress_bindings" ADD CONSTRAINT "sandbox_egress_bindings_network_binding_id_sandbox_network_bindings_id_fk" FOREIGN KEY ("network_binding_id") REFERENCES "public"."sandbox_network_bindings"("id") ON DELETE no action ON UPDATE no action;
