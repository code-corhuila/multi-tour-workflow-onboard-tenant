-- What a restart needs to carry a saga on (HU-WFL-001, debt F2): who asked, the membership id of step 2 and the
-- initial status of step 3. All nullable: a saga in flight before this change has none. The temporary password has no
-- column here and never will (ADR-004 D3). Guarded, so applying the changeset again does nothing.
ALTER TABLE workflow_onboard_tenant.onboard_tenant_saga
    ADD COLUMN IF NOT EXISTS on_behalf_of   text,
    ADD COLUMN IF NOT EXISTS membership_id  uuid,
    ADD COLUMN IF NOT EXISTS initial_status text;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_onboard_tenant_saga_initial_status') THEN
        ALTER TABLE workflow_onboard_tenant.onboard_tenant_saga
            ADD CONSTRAINT chk_onboard_tenant_saga_initial_status CHECK (initial_status IN ('ACTIVE', 'INACTIVE'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_onboard_tenant_saga_on_behalf_of') THEN
        ALTER TABLE workflow_onboard_tenant.onboard_tenant_saga
            ADD CONSTRAINT chk_onboard_tenant_saga_on_behalf_of CHECK (char_length(on_behalf_of) BETWEEN 1 AND 128);
    END IF;
END
$$;
