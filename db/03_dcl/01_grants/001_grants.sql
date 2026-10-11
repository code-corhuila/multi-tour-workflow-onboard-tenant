GRANT USAGE ON SCHEMA workflow TO workflow_reader, workflow_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA workflow TO workflow_reader;
-- No DELETE: a saga is never removed by the service, the state is the audit of the process
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA workflow TO workflow_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA workflow GRANT SELECT ON TABLES TO workflow_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA workflow GRANT SELECT, INSERT, UPDATE ON TABLES TO workflow_writer;
GRANT workflow_writer TO workflow_app;
