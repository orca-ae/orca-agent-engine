ALTER TABLE "vault_credentials" ADD COLUMN "auth_config" jsonb DEFAULT '{}'::jsonb NOT NULL;
