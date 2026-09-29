ALTER TABLE "vault_credentials" DROP CONSTRAINT "vault_credentials_auth_type_check";--> statement-breakpoint
ALTER TABLE "vault_credentials" ADD COLUMN "provider" text;--> statement-breakpoint
ALTER TABLE "vault_credentials" ADD COLUMN "scheme" text;--> statement-breakpoint
ALTER TABLE "vault_credentials" ADD COLUMN "logical_id" text;--> statement-breakpoint
ALTER TABLE "vault_credentials" ADD COLUMN "resolution_version" text;--> statement-breakpoint
CREATE UNIQUE INDEX "vault_credentials_active_logical_id_idx" ON "vault_credentials" USING btree ("workspace_id","vault_id","logical_id") WHERE "vault_credentials"."archived_at" is null and "vault_credentials"."logical_id" is not null;--> statement-breakpoint
ALTER TABLE "vault_credentials" ADD CONSTRAINT "vault_credentials_provider_fields_check" CHECK ((
        "vault_credentials"."auth_type" = 'provider'
        and "vault_credentials"."provider" in ('anthropic', 'openai', 'openai_compatible', 'azure_openai', 'vertex', 'bedrock')
        and "vault_credentials"."scheme" in ('api_key', 'bearer', 'gcp-service-account', 'aws-sig-v4')
        and (
          ("vault_credentials"."provider" = 'anthropic' and "vault_credentials"."scheme" = 'api_key')
          or ("vault_credentials"."provider" in ('openai', 'openai_compatible') and "vault_credentials"."scheme" = 'bearer')
          or ("vault_credentials"."provider" = 'azure_openai' and "vault_credentials"."scheme" in ('api_key', 'bearer'))
          or ("vault_credentials"."provider" = 'vertex' and "vault_credentials"."scheme" = 'gcp-service-account')
          or ("vault_credentials"."provider" = 'bedrock' and "vault_credentials"."scheme" = 'aws-sig-v4')
        )
        and "vault_credentials"."logical_id" is not null
        and "vault_credentials"."logical_id" ~ '^llm:[A-Za-z0-9][A-Za-z0-9._:-]{0,123}$'
        and char_length("vault_credentials"."logical_id") between 5 and 128
        and "vault_credentials"."logical_id" !~ '[^A-Za-z0-9._:-]'
        and "vault_credentials"."resolution_version" is not null
        and "vault_credentials"."mcp_server_url" is null
        and "vault_credentials"."secret_name" is null
      ) or (
        "vault_credentials"."auth_type" <> 'provider'
        and "vault_credentials"."provider" is null
        and "vault_credentials"."scheme" is null
        and "vault_credentials"."logical_id" is null
        and "vault_credentials"."resolution_version" is null
      ));--> statement-breakpoint
ALTER TABLE "vault_credentials" ADD CONSTRAINT "vault_credentials_auth_type_check" CHECK ("vault_credentials"."auth_type" in ('static_bearer', 'mcp_oauth', 'environment_variable', 'provider'));
