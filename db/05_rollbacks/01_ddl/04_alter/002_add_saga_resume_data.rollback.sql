ALTER TABLE workflow_onboard_tenant.onboard_tenant_saga
    DROP CONSTRAINT IF EXISTS chk_onboard_tenant_saga_on_behalf_of,
    DROP CONSTRAINT IF EXISTS chk_onboard_tenant_saga_initial_status,
    DROP COLUMN IF EXISTS initial_status,
    DROP COLUMN IF EXISTS membership_id,
    DROP COLUMN IF EXISTS on_behalf_of;
