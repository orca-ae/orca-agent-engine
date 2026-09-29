ALTER TABLE "sessions" ADD COLUMN IF NOT EXISTS "active_seconds" integer DEFAULT 0 NOT NULL;
