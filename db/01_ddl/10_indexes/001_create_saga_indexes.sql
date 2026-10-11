CREATE INDEX idx_onboard_tenant_saga_status_updated_at ON workflow.onboard_tenant_saga (saga_status, updated_at);
