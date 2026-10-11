-- The index of this key is the primary key of the step table, whose first column is saga_id (Annex A).
-- Guarded, so applying the changeset again does nothing.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_onboard_tenant_saga_step_saga') THEN
        ALTER TABLE workflow_onboard_tenant.onboard_tenant_saga_step
            ADD CONSTRAINT fk_onboard_tenant_saga_step_saga FOREIGN KEY (saga_id) REFERENCES workflow_onboard_tenant.onboard_tenant_saga (saga_id) ON DELETE CASCADE;
    END IF;
END
$$;