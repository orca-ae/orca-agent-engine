ALTER TABLE "agents" ADD COLUMN "harness_type" text DEFAULT 'claude_agent_sdk' NOT NULL;--> statement-breakpoint
ALTER TABLE "sessions" ADD COLUMN "harness_state" jsonb;
--> statement-breakpoint
UPDATE "agents" SET "harness_type" = COALESCE("metadata"->>'harness', 'claude_agent_sdk');
--> statement-breakpoint
CREATE FUNCTION enforce_agent_harness_type() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    NEW.harness_type := COALESCE(NEW.metadata->>'harness', 'claude_agent_sdk');
  ELSIF NEW.harness_type IS DISTINCT FROM OLD.harness_type OR
    COALESCE(NEW.metadata->>'harness', 'claude_agent_sdk') IS DISTINCT FROM OLD.harness_type THEN
    RAISE EXCEPTION 'agent harness type is immutable';
  END IF;
  RETURN NEW;
END;
$$;
--> statement-breakpoint
CREATE TRIGGER agents_harness_type_immutable BEFORE INSERT OR UPDATE ON agents
FOR EACH ROW EXECUTE FUNCTION enforce_agent_harness_type();
