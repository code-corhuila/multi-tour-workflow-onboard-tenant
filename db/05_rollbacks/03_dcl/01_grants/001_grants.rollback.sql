REVOKE workflow_onboard_tenant_writer FROM workflow_onboard_tenant_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA workflow_onboard_tenant REVOKE SELECT, INSERT, UPDATE ON TABLES FROM workflow_onboard_tenant_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA workflow_onboard_tenant REVOKE SELECT ON TABLES FROM workflow_onboard_tenant_reader;
REVOKE ALL ON ALL TABLES IN SCHEMA workflow_onboard_tenant FROM workflow_onboard_tenant_reader, workflow_onboard_tenant_writer;
REVOKE ALL ON SCHEMA workflow_onboard_tenant FROM workflow_onboard_tenant_reader, workflow_onboard_tenant_writer;
