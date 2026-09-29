ALTER TABLE "memory_stores" ADD COLUMN "metadata" jsonb DEFAULT '{}'::jsonb NOT NULL;
