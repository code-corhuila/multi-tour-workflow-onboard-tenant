package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/adapter/out/state/postgres"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/adapter/out/state/postgres/pgtest"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/domain/saga"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ctx  = context.Background()
	t0   = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	tid1 = "00000000-0000-4000-8000-0000000000a1"
)

func sagaID(n int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", n) }

func fresh(n int, key string, at time.Time) saga.Saga {
	return saga.Saga{ID: sagaID(n), IdempotencyKey: key, Status: saga.Running, CreatedAt: at, UpdatedAt: at}
}

func newStore(t *testing.T) (*postgres.Store, *pgxpool.Pool) {
	t.Helper()
	pool := pgtest.Open(t)
	return postgres.NewStore(pool), pool
}

func mustCreate(t *testing.T, s *postgres.Store, sg saga.Saga) {
	t.Helper()
	if _, created, err := s.Create(ctx, sg); err != nil || !created {
		t.Fatalf("Create(%s) = created %v, err %v", sg.ID, created, err)
	}
}

func TestI_CreateInsertsARunningSagaAndReturnsItCreated(t *testing.T) {
	s, _ := newStore(t)
	want := fresh(1, "key-onboard-0001", t0)

	got, created, err := s.Create(ctx, want)
	if err != nil || !created {
		t.Fatalf("Create = created %v, err %v", created, err)
	}
	if got.ID != want.ID || got.IdempotencyKey != want.IdempotencyKey || got.Status != saga.Running || !got.CreatedAt.Equal(t0) {
		t.Errorf("stored saga = %+v", got)
	}
	read, err := s.Get(ctx, want.ID)
	if err != nil || read.ID != want.ID || read.Status != saga.Running || len(read.Steps) != 0 || read.TenantID != "" || read.FailedStep != "" {
		t.Errorf("Get = %+v, %v", read, err)
	}
}

func TestI_CreateWithAnExistingKeyReturnsTheExistingSagaNotCreated(t *testing.T) {
	s, pool := newStore(t)
	mustCreate(t, s, fresh(1, "key-onboard-0001", t0))

	got, created, err := s.Create(ctx, fresh(2, "key-onboard-0001", t0.Add(time.Hour)))
	if err != nil || created {
		t.Fatalf("Create = created %v, err %v", created, err)
	}
	if got.ID != sagaID(1) || !got.CreatedAt.Equal(t0) {
		t.Errorf("returned %+v, want the first saga", got)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM workflow_onboard_tenant.onboard_tenant_saga`).Scan(&rows); err != nil || rows != 1 {
		t.Errorf("rows = %d, err %v", rows, err)
	}
}

func TestI_CreateRepeatedUnderConcurrencyInsertsOnce(t *testing.T) {
	s, pool := newStore(t)
	const callers = 16
	var wg sync.WaitGroup
	ids, createds, errs := make([]string, callers), make([]bool, callers), make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, created, err := s.Create(ctx, fresh(i+1, "key-onboard-0001", t0))
			ids[i], createds[i], errs[i] = got.ID, created, err
		}()
	}
	wg.Wait()
	winners := 0
	for i := range callers {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if createds[i] {
			winners++
		}
		if ids[i] != ids[0] {
			t.Errorf("caller %d got saga %s, caller 0 got %s", i, ids[i], ids[0])
		}
	}
	if winners != 1 {
		t.Errorf("%d callers created the saga, want exactly 1", winners)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM workflow_onboard_tenant.onboard_tenant_saga`).Scan(&rows); err != nil || rows != 1 {
		t.Errorf("rows = %d, err %v", rows, err)
	}
}

func TestI_state_keeps_on_behalf_of_membership_and_initial_status_after_each_step(t *testing.T) {
	s, _ := newStore(t)
	sg := fresh(1, "key-onboard-0001", t0)
	sg.OnBehalfOf, sg.InitialStatus = "platform-admin-sub", "INACTIVE"
	mustCreate(t, s, sg)
	got, err := s.Get(ctx, sg.ID)
	if err != nil || got.OnBehalfOf != "platform-admin-sub" || got.InitialStatus != "INACTIVE" || got.MembershipID != "" {
		t.Fatalf("after Create: %+v, %v", got, err)
	}

	sg.TenantID, sg.MembershipID = tid1, "00000000-0000-4000-8000-0000000000b2"
	sg.Record(saga.CreateTenant)
	sg.Record(saga.CreateAdministrator)
	sg.UpdatedAt = t0.Add(time.Second)
	if err := s.Save(ctx, &sg); err != nil {
		t.Fatal(err)
	}
	got, err = s.Get(ctx, sg.ID)
	if err != nil || got.OnBehalfOf != "platform-admin-sub" || got.InitialStatus != "INACTIVE" || got.MembershipID != sg.MembershipID {
		t.Errorf("after step 2: %+v, %v", got, err)
	}
}

func TestI_a_saga_written_before_the_resume_columns_reads_them_as_empty(t *testing.T) {
	s, pool := newStore(t)
	if _, err := pool.Exec(ctx, `INSERT INTO workflow_onboard_tenant.onboard_tenant_saga (saga_id, idempotency_key) VALUES ($1::text::uuid, 'key-onboard-0001')`, sagaID(1)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, sagaID(1))
	if err != nil || got.OnBehalfOf != "" || got.MembershipID != "" || got.InitialStatus != "" {
		t.Errorf("Get = %+v, %v", got, err)
	}
}

func TestI_SaveWritesTenantFailureAndEveryStepAndGetReadsThemBack(t *testing.T) {
	s, _ := newStore(t)
	sg := fresh(1, "key-onboard-0001", t0)
	mustCreate(t, s, sg)

	sg.TenantID = tid1
	sg.Record(saga.CreateTenant)
	sg.UpdatedAt = t0.Add(time.Second)
	if err := s.Save(ctx, &sg); err != nil {
		t.Fatal(err)
	}
	mid, err := s.Get(ctx, sg.ID)
	if err != nil || mid.TenantID != tid1 || mid.Status != saga.Running || len(mid.Steps) != 1 || mid.Steps[0] != (saga.StepState{Name: saga.CreateTenant}) || !mid.UpdatedAt.Equal(t0.Add(time.Second)) {
		t.Fatalf("after step 1: %+v, %v", mid, err)
	}

	sg.MarkCompensated(saga.CreateTenant)
	sg.End(saga.Compensated, saga.CreateAdministrator, "REJECTED: the e-mail is taken")
	sg.UpdatedAt = t0.Add(2 * time.Second)
	if err := s.Save(ctx, &sg); err != nil {
		t.Fatal(err)
	}
	end, err := s.Get(ctx, sg.ID)
	if err != nil {
		t.Fatal(err)
	}
	if end.Status != saga.Compensated || end.FailedStep != saga.CreateAdministrator || end.FailureDetail != "REJECTED: the e-mail is taken" || end.TenantID != tid1 {
		t.Errorf("ended saga = %+v", end)
	}
	if len(end.Steps) != 1 || end.Steps[0] != (saga.StepState{Name: saga.CreateTenant, Compensated: true}) {
		t.Errorf("steps = %+v", end.Steps)
	}
}

func TestI_SaveKeepsStepsInOrderWithoutDuplicates(t *testing.T) {
	s, pool := newStore(t)
	sg := fresh(1, "key-onboard-0001", t0)
	mustCreate(t, s, sg)
	sg.Record(saga.CreateTenant)
	sg.Record(saga.CreateAdministrator)
	for range 2 {
		if err := s.Save(ctx, &sg); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Get(ctx, sg.ID)
	if err != nil || len(got.Steps) != 2 || got.Steps[0].Name != saga.CreateTenant || got.Steps[1].Name != saga.CreateAdministrator {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM workflow_onboard_tenant.onboard_tenant_saga_step`).Scan(&rows); err != nil || rows != 2 {
		t.Errorf("step rows = %d, err %v", rows, err)
	}
}

func TestI_SaveNeverOverwritesAnEndedSaga(t *testing.T) {
	s, _ := newStore(t)
	sg := fresh(1, "key-onboard-0001", t0)
	mustCreate(t, s, sg)
	sg.Record(saga.CreateTenant)
	sg.End(saga.Completed, "", "")
	if err := s.Save(ctx, &sg); err != nil {
		t.Fatal(err)
	}

	late := sg
	late.Steps = nil
	late.End(saga.Failed, saga.CreateTenant, "RESTARTED: stopped before the tenant was recorded")
	if err := s.Save(ctx, &late); !errors.Is(err, saga.ErrEnded) {
		t.Fatalf("Save over an ended saga = %v, want ErrEnded", err)
	}
	got, err := s.Get(ctx, sg.ID)
	if err != nil || got.Status != saga.Completed || got.FailedStep != "" || len(got.Steps) != 1 {
		t.Errorf("the ended saga changed: %+v, %v", got, err)
	}
}

func TestI_SaveOfAnUnknownSagaIsNotFound(t *testing.T) {
	s, _ := newStore(t)
	unknown := fresh(9, "key-onboard-0009", t0)
	if err := s.Save(ctx, &unknown); !errors.Is(err, saga.ErrNotFound) {
		t.Fatalf("Save = %v, want ErrNotFound", err)
	}
}

func TestI_GetOfAnUnknownOrMalformedIdIsNotFound(t *testing.T) {
	s, _ := newStore(t)
	for _, id := range []string{sagaID(9), "not-a-uuid", ""} {
		if _, err := s.Get(ctx, id); !errors.Is(err, saga.ErrNotFound) {
			t.Errorf("Get(%q) = %v, want ErrNotFound", id, err)
		}
	}
}

func TestI_ClaimStaleReturnsOnlyRunningSagasOlderThanTheCutoffWithTheirSteps(t *testing.T) {
	s, _ := newStore(t)
	old := fresh(1, "key-onboard-0001", t0)
	mustCreate(t, s, old)
	old.Record(saga.CreateTenant)
	old.UpdatedAt = t0
	if err := s.Save(ctx, &old); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, s, fresh(2, "key-onboard-0002", t0.Add(10*time.Minute))) // running but recent
	ended := fresh(3, "key-onboard-0003", t0)                              // old but ended
	mustCreate(t, s, ended)
	ended.End(saga.Failed, saga.CreateTenant, "x")
	if err := s.Save(ctx, &ended); err != nil {
		t.Fatal(err)
	}

	got, err := s.ClaimStale(ctx, t0.Add(5*time.Minute), t0.Add(6*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != sagaID(1) || len(got[0].Steps) != 1 || got[0].Steps[0].Name != saga.CreateTenant {
		t.Fatalf("ClaimStale = %+v", got)
	}
}

func TestI_ClaimTakesAStaleSagaOnceUnderConcurrency(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, fresh(1, "key-onboard-0001", t0))
	var wg sync.WaitGroup
	var mu sync.Mutex
	taken := 0
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := s.ClaimStale(ctx, t0.Add(5*time.Minute), t0.Add(6*time.Minute))
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			taken += len(got)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if taken != 1 {
		t.Fatalf("the saga was taken %d times, want once", taken)
	}
	got, err := s.Get(ctx, sagaID(1))
	if err != nil || !got.UpdatedAt.Equal(t0.Add(6*time.Minute)) || got.Version != 1 {
		t.Errorf("after the claim: %+v, %v (updated_at moved to the claim, version raised)", got, err)
	}
}

func TestI_SaveFromAStaleCopyIsRefused(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, fresh(1, "key-onboard-0001", t0))
	live, _ := s.Get(ctx, sagaID(1))
	resumed, _ := s.Get(ctx, sagaID(1))

	resumed.Record(saga.CreateTenant)
	if err := s.Save(ctx, &resumed); err != nil || resumed.Version != 1 {
		t.Fatalf("first save = %v, version %d", err, resumed.Version)
	}
	live.End(saga.Failed, saga.CreateTenant, "late")
	if err := s.Save(ctx, &live); !errors.Is(err, saga.ErrConflict) {
		t.Fatalf("save from the older copy = %v, want ErrConflict", err)
	}
	got, _ := s.Get(ctx, sagaID(1))
	if got.Status != saga.Running || len(got.Steps) != 1 {
		t.Errorf("the older copy overwrote the newer: %+v", got)
	}
}

func TestI_ClaimStaleOfNothingIsEmptyWithoutError(t *testing.T) {
	s, _ := newStore(t)
	got, err := s.ClaimStale(ctx, t0, t0)
	if err != nil || len(got) != 0 {
		t.Fatalf("ClaimStale = %+v, %v", got, err)
	}
}

func TestI_TheStateHasNoColumnThatCouldHoldAPassword(t *testing.T) {
	_, pool := newStore(t)
	var n int
	err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema = 'workflow_onboard_tenant' AND (column_name ILIKE '%pass%' OR column_name ILIKE '%secret%' OR column_name ILIKE '%token%')`).Scan(&n)
	if err != nil || n != 0 {
		t.Errorf("sensitive columns = %d, err %v", n, err)
	}
}

func TestI_TheAppRoleRunsTheStoreButCannotDeleteOrCreate(t *testing.T) {
	pool := pgtest.OpenAsApp(t)
	s := postgres.NewStore(pool)
	sg := fresh(1, "key-onboard-0001", t0)
	mustCreate(t, s, sg)
	sg.Record(saga.CreateTenant)
	if err := s.Save(ctx, &sg); err != nil {
		t.Fatalf("Save as the app role: %v", err)
	}
	if got, err := s.Get(ctx, sg.ID); err != nil || len(got.Steps) != 1 {
		t.Fatalf("Get as the app role: %+v, %v", got, err)
	}
	for _, statement := range []string{
		`DELETE FROM workflow_onboard_tenant.onboard_tenant_saga`,
		`CREATE TABLE workflow_onboard_tenant.extra (id int)`,
		`CREATE TABLE public.extra (id int)`,
	} {
		_, err := pool.Exec(ctx, statement)
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "42501" {
			t.Errorf("%s: err = %v, want SQLSTATE 42501", statement, err)
		}
	}
}

func TestI_OpenAppliesThePoolAndStatementLimits(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	pool, err := postgres.Open(ctx, url, postgres.Limits{MaxConns: 10, ConnectTimeout: 5 * time.Second, StatementTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var timeout string
	if err := pool.QueryRow(ctx, `SHOW statement_timeout`).Scan(&timeout); err != nil || timeout != "5s" {
		t.Errorf("statement_timeout = %q, err %v", timeout, err)
	}
	if pool.Config().MaxConns != 10 {
		t.Errorf("MaxConns = %d", pool.Config().MaxConns)
	}
}

func TestI_OpenNeverEchoesTheConnectionString(t *testing.T) {
	_, err := postgres.Open(ctx, "postgres://user:s3cret-pass@127.0.0.1:1/db?sslmode=disable", postgres.Limits{MaxConns: 1, ConnectTimeout: time.Second, StatementTimeout: time.Second})
	if err == nil {
		t.Fatal("Open succeeded against a closed port")
	}
	if msg := err.Error(); strings.Contains(msg, "s3cret-pass") || strings.Contains(msg, "user") {
		t.Errorf("error leaks the connection string: %q", msg)
	}
}
