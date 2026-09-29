CREATE TABLE "vault_credentials" (
	"id" text PRIMARY KEY NOT NULL,
	"workspace_id" text NOT NULL,
	"vault_id" text NOT NULL,
	"display_name" text,
	"auth_type" text NOT NULL,
	"mcp_server_url" text NOT NULL,
	"access_secret_ref" text NOT NULL,
	"refresh_secret_ref" text,
	"token_endpoint" text,
	"client_id" text,
	"token_endpoint_auth_type" text,
	"client_secret_ref" text,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"archived_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "vault_credentials_auth_type_check" CHECK ("vault_credentials"."auth_type" in ('static_bearer', 'mcp_oauth'))
);
--> statement-breakpoint
ALTER TABLE "vault_credentials" ADD CONSTRAINT "vault_credentials_vault_id_vaults_id_fk" FOREIGN KEY ("vault_id") REFERENCES "public"."vaults"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "vault_credentials_workspace_idx" ON "vault_credentials" USING btree ("workspace_id","archived_at");--> statement-breakpoint
CREATE INDEX "vault_credentials_list_idx" ON "vault_credentials" USING btree ("workspace_id","vault_id","archived_at","created_at" DESC NULLS LAST,"id" DESC NULLS LAST);--> statement-breakpoint
CREATE UNIQUE INDEX "vault_credentials_active_url_idx" ON "vault_credentials" USING btree ("vault_id","mcp_server_url") WHERE "vault_credentials"."archived_at" is null;