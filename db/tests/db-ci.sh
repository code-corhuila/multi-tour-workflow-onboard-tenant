#!/bin/sh
# I·db_ci_builds_reruns_rolls_back_and_rebuilds and I·workflow_app_writes_only_its_schema.
#
# Builds the workflow schema from an EMPTY PostgreSQL, applies the migrations a second time (zero
# changes), rolls everything back (empty again) and applies them once more (Annex A, db-ci; Annex J,
# J.6 and J.10). It starts its own throwaway PostgreSQL and Liquibase containers on a private network,
# with passwords generated now, so it touches nothing else and writes no secret. Docker is the only
# requirement; the CI and a developer run this same file.
set -u
cd "$(dirname "$0")/../.." || exit 1

# Git Bash on Windows rewrites container paths such as /workspace; no effect elsewhere
export MSYS_NO_PATHCONV=1

POSTGRES_IMAGE="postgres:18.6-alpine"
RUN_ID="workflow-dbci-$$"
# the same image the runner of deploy/compose.yml uses: Liquibase plus the PostgreSQL driver
LIQUIBASE_IMAGE="$RUN_ID-liquibase"
DB="multitour_test"
ADMIN_PASSWORD="admin-$(head -c 12 /dev/urandom | od -An -tx1 | tr -d ' \n')"
APP_PASSWORD="app-$(head -c 12 /dev/urandom | od -An -tx1 | tr -d ' \n')"
fail=0

cleanup() {
  docker rm -f "$RUN_ID" >/dev/null 2>&1 || true
  docker network rm "$RUN_ID" >/dev/null 2>&1 || true
  docker rmi "$LIQUIBASE_IMAGE" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 1' INT TERM

psql_admin() { docker exec -e PGPASSWORD="$ADMIN_PASSWORD" "$RUN_ID" psql -h 127.0.0.1 -U postgres -d "$DB" -v ON_ERROR_STOP=1 -tA "$@"; }

# Each -db owns its control table (J.6); the runner uses the instance administrator (ADR-005 E7)
lb() {
  docker run --rm --network "$RUN_ID" -v "$PWD/db:/workspace:ro" \
    -e LIQUIBASE_COMMAND_URL="jdbc:postgresql://$RUN_ID:5432/$DB" \
    -e LIQUIBASE_COMMAND_USERNAME=postgres -e LIQUIBASE_COMMAND_PASSWORD="$ADMIN_PASSWORD" \
    "$LIQUIBASE_IMAGE" --search-path=/workspace --changelog-file=changelog/changelog-master.yaml \
    --database-changelog-table-name=databasechangelog_workflow \
    --database-changelog-lock-table-name=databasechangeloglock_workflow "$@"
}

check() { # name, then the command that must succeed
  name=$1; shift
  if reason=$("$@" 2>&1); then
    echo "PASS  $name"
  else
    echo "FAIL  $name"
    printf '%s\n' "$reason" | tail -n 8 | sed 's/^/        /'
    fail=1
  fi
}

# The schemas where workflow_app can write, by the privilege query of J.10: only its own may appear
writable_schemas() {
  psql_admin -c "SELECT string_agg(DISTINCT table_schema, ',' ORDER BY table_schema) FROM information_schema.tables
    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
      AND has_table_privilege('workflow_app', format('%I.%I', table_schema, table_name), 'INSERT,UPDATE,DELETE')"
}

# A positive control first: the check must SEE a write privilege when one exists, or it would pass
# only because the other schema is out of reach. The grant is revoked right after.
writes_only_its_schema() {
  psql_admin -c "GRANT INSERT ON other_domain.probe TO workflow_app" >/dev/null || return 1
  seen=$(writable_schemas)
  psql_admin -c "REVOKE INSERT ON other_domain.probe FROM workflow_app" >/dev/null || return 1
  [ "$seen" = "other_domain,workflow" ] || { echo "the privilege query did not see a granted write: '$seen'"; return 1; }
  [ "$(writable_schemas)" = "workflow" ] || { echo "workflow_app can write in: $(writable_schemas)"; return 1; }
  [ "$(psql_admin -c "SELECT has_schema_privilege('workflow_app', 'public', 'CREATE')")" = "f" ] || { echo "workflow_app can create in public"; return 1; }
  [ "$(psql_admin -c "SELECT has_schema_privilege('workflow_app', 'workflow', 'CREATE')")" = "f" ] || { echo "workflow_app can create objects in workflow"; return 1; }
}

# Its own control table, login-less roles without a password, and the grant of the writer role to the instance user
owns_its_control_table_and_roles() {
  [ "$(psql_admin -c "SELECT count(*) FROM information_schema.tables WHERE table_name IN ('databasechangelog_workflow', 'databasechangeloglock_workflow')")" = 2 ] || { echo "the control tables with the workflow suffix are missing"; return 1; }
  [ "$(psql_admin -c "SELECT count(*) FROM information_schema.tables WHERE table_name IN ('databasechangelog', 'databasechangeloglock')")" = 0 ] || { echo "a default-named control table exists, two domains would share it"; return 1; }
  [ "$(psql_admin -c "SELECT count(*) FROM pg_authid WHERE rolname IN ('workflow_reader', 'workflow_writer') AND NOT rolcanlogin AND rolpassword IS NULL")" = 2 ] || { echo "the roles must be NOLOGIN and have no password"; return 1; }
  [ "$(psql_admin -c "SELECT pg_has_role('workflow_app', 'workflow_writer', 'MEMBER')")" = "t" ] || { echo "workflow_app is not a member of workflow_writer"; return 1; }
}

# The state of the control table, so a second update that applied anything shows as a difference
changelog_state() {
  psql_admin -c "SELECT count(*) || ':' || coalesce(md5(string_agg(id || md5sum, ',' ORDER BY orderexecuted)), '') FROM databasechangelog_workflow"
}

second_update_changes_nothing() {
  before=$(changelog_state)
  out=$(lb update 2>&1) || { printf '%s\n' "$out" | tail -n 8; return 1; }
  [ "$before" = "$(changelog_state)" ] || { echo "the second update changed the control table"; return 1; }
  case $before in 0:*) echo "no changeset was ever recorded"; return 1 ;; esac
}

rolled_back_to_empty() {
  lb rollback-count 999 >/dev/null 2>&1 || { echo "rollback-count 999 failed"; return 1; }
  [ "$(psql_admin -c "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'workflow'")" = 0 ] || { echo "the schema workflow is still there"; return 1; }
  [ "$(psql_admin -c "SELECT count(*) FROM pg_roles WHERE rolname IN ('workflow_reader', 'workflow_writer')")" = 0 ] || { echo "a workflow role is still there"; return 1; }
  [ "$(psql_admin -c "SELECT count(*) FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member WHERE r.rolname = 'workflow_app'")" = 0 ] || { echo "workflow_app is still a member of a role"; return 1; }
  [ "$(psql_admin -c "SELECT count(*) FROM pg_default_acl")" = 0 ] || { echo "a default privilege is left behind"; return 1; }
  [ "$(psql_admin -c "SELECT count(*) FROM databasechangelog_workflow")" = 0 ] || { echo "the control table still lists changesets"; return 1; }
}

# ---- the probes of the saga tables (HU-WFL-001, task T-02): what the engine refuses
S1=00000000-0000-4000-8000-0000000000a1
S2=00000000-0000-4000-8000-0000000000a2
GHOST=00000000-0000-4000-8000-0000000000ff
KEY1=key-onboard-0001
KEY2=key-onboard-0002

# a statement the database must refuse, for the reason named by a fragment of its message
refused() { # fragment, statement
  out=$(psql_admin -c "$2" 2>&1) && { echo "the database accepted: $2"; return 1; }
  printf '%s' "$out" | grep -q "$1" || { echo "refused, but not for the expected reason ($1): $out"; return 1; }
}
clean() { psql_admin -c "TRUNCATE workflow.onboard_tenant_saga_step, workflow.onboard_tenant_saga" >/dev/null || { echo "could not clean the tables"; return 1; }; }
saga() { # id, idempotency key, status, failed step (quoted or NULL)
  printf "INSERT INTO workflow.onboard_tenant_saga (saga_id, idempotency_key, saga_status, failed_step) VALUES ('%s', '%s', '%s', %s)" "$1" "$2" "${3:-RUNNING}" "${4:-NULL}"
}
step() { # saga id, number, name, status, compensated_at (SQL or NULL)
  printf "INSERT INTO workflow.onboard_tenant_saga_step (saga_id, step_number, step_name, step_status, compensated_at) VALUES ('%s', %s, '%s', '%s', %s)" "$1" "$2" "$3" "${4:-COMPLETED}" "${5:-NULL}"
}

idempotency_key_is_unique_and_sized() {
  clean; psql_admin -c "$(saga $S1 $KEY1)" >/dev/null || return 1
  refused uq_onboard_tenant_saga_idempotency_key "$(saga $S2 $KEY1)" || return 1
  refused chk_onboard_tenant_saga_idempotency_key "$(saga $S2 short)" || return 1
  psql_admin -c "$(saga $S2 $KEY2)" >/dev/null
}
status_is_one_of_the_four_states() {
  clean
  U=00000000-0000-4000-8000-0000000000
  psql_admin -c "$(saga ${U}b1 key-state-running RUNNING)" -c "$(saga ${U}b2 key-state-completed COMPLETED)" \
             -c "$(saga ${U}b3 key-state-compensated COMPENSATED "'CREATE_ADMINISTRATOR'")" \
             -c "$(saga ${U}b4 key-state-failed FAILED "'RECORD_ADMINISTRATOR'")" >/dev/null || return 1
  refused chk_onboard_tenant_saga_status "$(saga $S1 $KEY1 DONE)"
}
failed_step_goes_with_a_failed_status_and_a_known_name() {
  clean
  refused chk_onboard_tenant_saga_failure "$(saga $S1 $KEY1 FAILED)" || return 1
  refused chk_onboard_tenant_saga_failure "$(saga $S1 $KEY1 RUNNING "'CREATE_TENANT'")" || return 1
  refused chk_onboard_tenant_saga_failed_step "$(saga $S1 $KEY1 FAILED "'NO_SUCH_STEP'")"
}
failure_detail_is_bounded() {
  clean
  refused chk_onboard_tenant_saga_failure_detail "INSERT INTO workflow.onboard_tenant_saga (saga_id, idempotency_key, failure_detail) VALUES ('$S1', '$KEY1', repeat('x', 2001))" || return 1
  psql_admin -c "INSERT INTO workflow.onboard_tenant_saga (saga_id, idempotency_key, failure_detail) VALUES ('$S1', '$KEY1', repeat('x', 2000))" >/dev/null
}
a_step_is_recorded_once_with_a_known_name_number_and_status() {
  clean; psql_admin -c "$(saga $S1 $KEY1)" -c "$(step $S1 1 CREATE_TENANT)" >/dev/null || return 1
  refused pk_onboard_tenant_saga_step "$(step $S1 1 CREATE_TENANT)" || return 1
  refused chk_onboard_tenant_saga_step_number "$(step $S1 4 CREATE_TENANT)" || return 1
  refused chk_onboard_tenant_saga_step_name "$(step $S1 2 NO_SUCH_STEP)" || return 1
  refused chk_onboard_tenant_saga_step_status "$(step $S1 2 CREATE_ADMINISTRATOR UNDONE)" || return 1
  refused chk_onboard_tenant_saga_step_compensated "$(step $S1 2 CREATE_ADMINISTRATOR COMPENSATED)" || return 1
  refused chk_onboard_tenant_saga_step_compensated "$(step $S1 2 CREATE_ADMINISTRATOR COMPLETED now\(\))"
}
steps_follow_their_saga() {
  clean; refused fk_onboard_tenant_saga_step_saga "$(step $GHOST 1 CREATE_TENANT)" || return 1
  psql_admin -c "$(saga $S1 $KEY1)" -c "$(step $S1 1 CREATE_TENANT)" -c "$(step $S1 2 CREATE_ADMINISTRATOR)" >/dev/null || return 1
  psql_admin -c "DELETE FROM workflow.onboard_tenant_saga WHERE saga_id = '$S1'" >/dev/null || return 1
  [ "$(psql_admin -c "SELECT count(*) FROM workflow.onboard_tenant_saga_step")" = 0 ] || { echo "the steps did not follow their saga"; return 1; }
}
the_app_user_writes_the_tables_and_the_index_exists() {
  for t in onboard_tenant_saga onboard_tenant_saga_step; do
    [ "$(psql_admin -c "SELECT has_table_privilege('workflow_app', 'workflow.$t', 'INSERT,UPDATE')")" = t ] || { echo "workflow_app cannot write workflow.$t"; return 1; }
  done
  [ "$(psql_admin -c "SELECT count(*) FROM pg_indexes WHERE schemaname = 'workflow' AND indexname = 'idx_onboard_tenant_saga_status_updated_at'")" = 1 ] || { echo "the resumption index is missing"; return 1; }
}

# ---- an empty instance, as multi-tour-infra-postgres leaves it for this domain (J.7)
docker build -q -f deploy/liquibase.Dockerfile -t "$LIQUIBASE_IMAGE" deploy >/dev/null || { echo "the runner image did not build"; exit 1; }
docker network create "$RUN_ID" >/dev/null || exit 1
docker run -d --name "$RUN_ID" --network "$RUN_ID" -e POSTGRES_DB="$DB" -e POSTGRES_PASSWORD="$ADMIN_PASSWORD" "$POSTGRES_IMAGE" >/dev/null || exit 1
ready=0
for _ in $(seq 1 60); do
  if docker exec "$RUN_ID" pg_isready -h 127.0.0.1 -U postgres -d "$DB" >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
[ "$ready" = 1 ] || { echo "the throwaway PostgreSQL did not start"; exit 1; }
psql_admin -c "CREATE ROLE workflow_app LOGIN PASSWORD '$APP_PASSWORD'" \
           -c "ALTER ROLE workflow_app SET search_path = workflow" \
           -c "CREATE SCHEMA other_domain" -c "CREATE TABLE other_domain.probe (id int)" >/dev/null || exit 1

check "I·db_ci_builds_from_empty" lb update
check "I·db_ci_second_update_changes_nothing" second_update_changes_nothing
check "I·workflow_app_writes_only_its_schema" writes_only_its_schema
check "I·owns_its_control_table_and_roles" owns_its_control_table_and_roles
check "I·idempotency_key_is_unique_and_sized" idempotency_key_is_unique_and_sized
check "I·status_is_one_of_the_four_states" status_is_one_of_the_four_states
check "I·failed_step_goes_with_a_failed_status_and_a_known_name" failed_step_goes_with_a_failed_status_and_a_known_name
check "I·failure_detail_is_bounded" failure_detail_is_bounded
check "I·a_step_is_recorded_once_with_a_known_name_number_and_status" a_step_is_recorded_once_with_a_known_name_number_and_status
check "I·steps_follow_their_saga" steps_follow_their_saga
check "I·the_app_user_writes_the_tables_and_the_index_exists" the_app_user_writes_the_tables_and_the_index_exists
clean
check "I·db_ci_rolls_back_to_empty" rolled_back_to_empty
check "I·db_ci_rebuilds_after_the_rollback" lb update
check "I·workflow_app_writes_only_its_schema_after_the_rebuild" writes_only_its_schema
check "I·owns_its_control_table_and_roles_after_the_rebuild" owns_its_control_table_and_roles

# the single name the spec uses for the whole sequence
[ "$fail" = 0 ] && echo "PASS  I·db_ci_builds_reruns_rolls_back_and_rebuilds"
[ "$fail" = 0 ] && echo "ALL CHECKS PASSED" || echo "SOME CHECKS FAILED"
exit "$fail"
