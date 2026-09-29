CREATE INDEX "sessions_workspace_environment_idx" ON "sessions" USING btree ("workspace_id","environment_id");
