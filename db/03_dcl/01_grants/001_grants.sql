GRANT USAGE ON SCHEMA workflow_onboard_tenant TO workflow_onboard_tenant_reader, workflow_onboard_tenant_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA workflow_onboard_tenant TO workflow_onboard_tenant_reader;
-- No DELETE: a saga is never removed by the service, the state is the audit of the process
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA workflow_onboard_tenant TO workflow_onboard_tenant_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA workflow_onboard_tenant GRANT SELECT ON TABLES TO workflow_onboard_tenant_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA workflow_onboard_tenant GRANT SELECT, INSERT, UPDATE ON TABLES TO workflow_onboard_tenant_writer;
GRANT workflow_onboard_tenant_writer TO workflow_onboard_tenant_app;
