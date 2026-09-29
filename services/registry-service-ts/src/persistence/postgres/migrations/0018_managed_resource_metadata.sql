ALTER TABLE "sessions" ADD COLUMN "metadata" jsonb DEFAULT '{}'::jsonb NOT NULL;--> statement-breakpoint
ALTER TABLE "environments" ADD COLUMN "description" text;--> statement-breakpoint
ALTER TABLE "environments" ADD COLUMN "metadata" jsonb DEFAULT '{}'::jsonb NOT NULL;
