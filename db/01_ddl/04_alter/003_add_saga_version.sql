-- The counter a save compares and raises, so a save from an older copy of a saga is refused and a claim of a stale
-- saga is exclusive (HU-WFL-001, debt F3). A saga in flight before this change starts at 0. Guarded: applying it again does nothing.
ALTER TABLE workflow_onboard_tenant.onboard_tenant_saga
    ADD COLUMN IF NOT EXISTS version bigint NOT NULL DEFAULT 0;
