-- State of the onboard-tenant saga (ADR-005 E4, ADR-004 D3). No foreign key here: it is in 04_alter.
-- The temporary password is never stored (ADR-004 D3): nothing here can hold it.
-- tenant_id is a reference to identity-audit, never a foreign key (Annex J, J.3.4); it is kept so a
-- restart can compensate step 1. failure_detail holds only a sanitized error code and message, never a raw error
-- (it can embed connection strings or tokens); it never reaches a response (Annex E).
CREATE TABLE IF NOT EXISTS workflow_onboard_tenant.onboard_tenant_saga (
    saga_id         uuid CONSTRAINT pk_onboard_tenant_saga PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key text        NOT NULL,
    saga_status     text        NOT NULL DEFAULT 'RUNNING',
    tenant_id       uuid,
    failed_step     text,
    failure_detail  text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_onboard_tenant_saga_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT chk_onboard_tenant_saga_idempotency_key CHECK (char_length(idempotency_key) BETWEEN 8 AND 128),
    CONSTRAINT chk_onboard_tenant_saga_status CHECK (saga_status IN ('RUNNING', 'COMPLETED', 'COMPENSATED', 'FAILED')),
    CONSTRAINT chk_onboard_tenant_saga_failed_step CHECK (failed_step IN ('CREATE_TENANT', 'CREATE_ADMINISTRATOR', 'RECORD_ADMINISTRATOR')),
    -- a saga that ended undone or failed says which step failed; the others have none
    CONSTRAINT chk_onboard_tenant_saga_failure CHECK ((saga_status IN ('COMPENSATED', 'FAILED')) = (failed_step IS NOT NULL)),
    CONSTRAINT chk_onboard_tenant_saga_failure_detail CHECK (char_length(failure_detail) <= 2000)
);

CREATE TABLE IF NOT EXISTS workflow_onboard_tenant.onboard_tenant_saga_step (
    saga_id        uuid        NOT NULL,
    step_number    smallint    NOT NULL,
    step_name      text        NOT NULL,
    step_status    text        NOT NULL DEFAULT 'COMPLETED',
    completed_at   timestamptz NOT NULL DEFAULT now(),
    compensated_at timestamptz,
    CONSTRAINT pk_onboard_tenant_saga_step PRIMARY KEY (saga_id, step_number),
    CONSTRAINT chk_onboard_tenant_saga_step_number CHECK (step_number BETWEEN 1 AND 3),
    CONSTRAINT chk_onboard_tenant_saga_step_name CHECK (step_name IN ('CREATE_TENANT', 'CREATE_ADMINISTRATOR', 'RECORD_ADMINISTRATOR')),
    CONSTRAINT chk_onboard_tenant_saga_step_status CHECK (step_status IN ('COMPLETED', 'COMPENSATED')),
    CONSTRAINT chk_onboard_tenant_saga_step_compensated CHECK ((step_status = 'COMPENSATED') = (compensated_at IS NOT NULL))
);
