-- The index of this key is the primary key of the step table, whose first column is saga_id (Annex A)
ALTER TABLE workflow.onboard_tenant_saga_step
    ADD CONSTRAINT fk_onboard_tenant_saga_step_saga FOREIGN KEY (saga_id) REFERENCES workflow.onboard_tenant_saga (saga_id) ON DELETE CASCADE;
