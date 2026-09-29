DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM "vaults") OR EXISTS (SELECT 1 FROM "vault_credentials") THEN
    RAISE EXCEPTION '0007_vault_metadata refuses to drop legacy vault secret columns while vault data exists; run an explicit manual migration first';
  END IF;
END $$;
ALTER TABLE "vaults" DROP CONSTRAINT IF EXISTS "vaults_ws_name_idx";
DROP INDEX IF EXISTS "vaults_ws_name_idx";
ALTER TABLE "vaults" DROP COLUMN IF EXISTS "name";
ALTER TABLE "vaults" DROP COLUMN IF EXISTS "target_kind";
ALTER TABLE "vaults" DROP COLUMN IF EXISTS "target_url";
ALTER TABLE "vaults" DROP COLUMN IF EXISTS "principal";
ALTER TABLE "vaults" DROP COLUMN IF EXISTS "secret_ref";
ALTER TABLE "vaults" ADD COLUMN "display_name" text NOT NULL;
ALTER TABLE "vaults" ADD COLUMN "metadata" jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE INDEX IF NOT EXISTS "vault_credentials_runtime_idx" ON "vault_credentials" ("workspace_id", "vault_id", "archived_at", "mcp_server_url");
