REVOKE workflow_writer FROM workflow_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA workflow REVOKE SELECT, INSERT, UPDATE ON TABLES FROM workflow_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA workflow REVOKE SELECT ON TABLES FROM workflow_reader;
REVOKE ALL ON ALL TABLES IN SCHEMA workflow FROM workflow_reader, workflow_writer;
REVOKE ALL ON SCHEMA workflow FROM workflow_reader, workflow_writer;
