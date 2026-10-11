CREATE INDEX IF NOT EXISTS idx_onboard_tenant_saga_status_updated_at ON workflow_onboard_tenant.onboard_tenant_saga (saga_status, updated_at);
