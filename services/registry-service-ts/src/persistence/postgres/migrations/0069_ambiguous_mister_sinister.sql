CREATE TABLE "sandbox_egress_bindings" (
	"id" text PRIMARY KEY NOT NULL,
	"session_id" text NOT NULL,
	"execution_generation" text NOT NULL,
	"capability_digest" text NOT NULL,
	"revoked_at" timestamp with time zone,
	"credentials" jsonb NOT NULL
);
--> statement-breakpoint
ALTER TABLE "sandbox_egress_bindings" ADD CONSTRAINT "sandbox_egress_bindings_session_id_sessions_id_fk" FOREIGN KEY ("session_id") REFERENCES "public"."sessions"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "sandbox_egress_capability_digest_idx" ON "sandbox_egress_bindings" USING btree ("capability_digest");--> statement-breakpoint
CREATE UNIQUE INDEX "sandbox_egress_current_session_idx" ON "sandbox_egress_bindings" USING btree ("session_id") WHERE "sandbox_egress_bindings"."revoked_at" is null;