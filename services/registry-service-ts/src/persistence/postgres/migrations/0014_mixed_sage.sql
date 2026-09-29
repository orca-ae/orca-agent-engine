ALTER TABLE "vault_credentials" DROP CONSTRAINT "vault_credentials_auth_type_check";--> statement-breakpoint
ALTER TABLE "vault_credentials" ALTER COLUMN "mcp_server_url" DROP NOT NULL;--> statement-breakpoint
ALTER TABLE "vault_credentials" ADD COLUMN "secret_name" text;--> statement-breakpoint
ALTER TABLE "vault_credentials" ADD COLUMN "networking" jsonb DEFAULT '{}'::jsonb NOT NULL;--> statement-breakpoint
CREATE UNIQUE INDEX "vault_credentials_active_secret_name_idx" ON "vault_credentials" USING btree ("vault_id","secret_name") WHERE "vault_credentials"."archived_at" is null and "vault_credentials"."secret_name" is not null;--> statement-breakpoint
ALTER TABLE "vault_credentials" ADD CONSTRAINT "vault_credentials_auth_type_check" CHECK ("vault_credentials"."auth_type" in ('static_bearer', 'mcp_oauth', 'environment_variable'));