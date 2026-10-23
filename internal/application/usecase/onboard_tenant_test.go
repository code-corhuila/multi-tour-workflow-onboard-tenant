package usecase_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/application/port/in"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/application/port/out"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/application/usecase"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/domain/saga"
)

const password = "Tmp-Secret-72h!"

var t0 = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

type fakeStore struct {
	byID       map[string]saga.Saga
	snapshots  []saga.Saga
	saves      int
	conflictAt int // the save with this number (from 1) answers ErrConflict; 0 never does
	endedAt    int // the save with this number answers ErrEnded: the saga ended meanwhile
}

func newStore() *fakeStore { return &fakeStore{byID: map[string]saga.Saga{}} }

func copySaga(s saga.Saga) saga.Saga {
	s.Steps = append([]saga.StepState(nil), s.Steps...)
	return s
}

func (f *fakeStore) put(s saga.Saga) {
	f.byID[s.ID] = copySaga(s)
	f.snapshots = append(f.snapshots, copySaga(s))
}

func (f *fakeStore) latest() saga.Saga { return f.snapshots[len(f.snapshots)-1] }

func (f *fakeStore) Create(ctx context.Context, s saga.Saga) (saga.Saga, bool, error) {
	if err := ctx.Err(); err != nil {
		return saga.Saga{}, false, err
	}
	for _, existing := range f.byID {
		if existing.IdempotencyKey == s.IdempotencyKey {
			return copySaga(existing), false, nil
		}
	}
	f.put(s)
	return copySaga(s), true, nil
}

func (f *fakeStore) Save(ctx context.Context, s *saga.Saga) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.saves++
	if f.saves == f.endedAt {
		return saga.ErrEnded
	}
	if f.saves == f.conflictAt || f.byID[s.ID].Version != s.Version {
		return saga.ErrConflict
	}
	s.Version++
	f.put(*s)
	return nil
}

func (f *fakeStore) Get(_ context.Context, id string) (saga.Saga, error) {
	s, ok := f.byID[id]
	if !ok {
		return saga.Saga{}, saga.ErrNotFound
	}
	return copySaga(s), nil
}

func (f *fakeStore) ClaimStale(_ context.Context, before, now time.Time) ([]saga.Saga, error) {
	var stale []saga.Saga
	for id, s := range f.byID {
		if s.Status == saga.Running && s.UpdatedAt.Before(before) {
			s.UpdatedAt, s.Version = now, s.Version+1
			f.byID[id] = copySaga(s)
			stale = append(stale, copySaga(s))
		}
	}
	return stale, nil
}

type fakeIdentity struct {
	store                          *fakeStore
	order                          *[]string
	calls                          []out.Call
	registerErr, completeErr, undo error
	failedStep                     saga.Step
	atComplete                     saga.Saga
	completeArgs                   [3]string // tenant id, membership id, initial status
}

func (f *fakeIdentity) RegisterPendingTenant(ctx context.Context, c out.Call, _ saga.TenantData) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	*f.order = append(*f.order, "register")
	f.calls = append(f.calls, c)
	return "tenant-1", f.registerErr
}

func (f *fakeIdentity) CompleteOnboarding(ctx context.Context, c out.Call, tenantID, membershipID, initialStatus string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	*f.order = append(*f.order, "complete")
	f.calls = append(f.calls, c)
	f.atComplete = f.store.latest()
	f.completeArgs = [3]string{tenantID, membershipID, initialStatus}
	return f.completeErr
}

func (f *fakeIdentity) FailOnboarding(ctx context.Context, c out.Call, _ string, step saga.Step) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	*f.order = append(*f.order, "fail-onboarding")
	f.calls = append(f.calls, c)
	f.failedStep = step
	return f.undo
}

type fakeSecurity struct {
	store  *fakeStore
	order  *[]string
	calls  []out.Call
	err    error
	atCall saga.Saga
	hook   func() // runs inside the call, e.g. to cancel the caller's context
}

func (f *fakeSecurity) CreateFirstAdministrator(ctx context.Context, c out.Call, _ string, _ saga.Administrator) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if f.hook != nil {
		f.hook()
	}
	*f.order = append(*f.order, "create-administrator")
	f.calls = append(f.calls, c)
	f.atCall = f.store.latest()
	return "membership-1", f.err
}

type harness struct {
	uc       *usecase.OnboardTenant
	store    *fakeStore
	identity *fakeIdentity
	security *fakeSecurity
	order    *[]string
	logs     *bytes.Buffer
}

func newHarness() *harness {
	order := &[]string{}
	store := newStore()
	logs := &bytes.Buffer{}
	h := &harness{store: store, order: order, logs: logs}
	h.identity = &fakeIdentity{store: store, order: order}
	h.security = &fakeSecurity{store: store, order: order}
	n := 0
	h.uc = usecase.NewOnboardTenant(store, h.identity, h.security,
		slog.New(slog.NewTextHandler(logs, nil)),
		func() string { n++; return fmt.Sprintf("saga-%d", n) },
		func() time.Time { return t0 }, time.Minute)
	return h
}

func command(key string) in.StartCommand {
	return in.StartCommand{IdempotencyKey: key, Onboarding: saga.Onboarding{
		Tenant:        saga.TenantData{Identifier: "andes-tours", CommercialName: "Andes Tours", TimeZone: "America/Bogota", Country: "CO", InitialStatus: "ACTIVE"},
		Administrator: saga.Administrator{FullName: "Ana Perez", Email: "ana@example.test", Password: password},
		OnBehalfOf:    "platform-admin-sub",
	}}
}

func rejected() error {
	return &saga.ParticipantError{Code: "BUSINESS_RULE_VIOLATION", Message: "email already has an account", Definitive: true}
}

func steps(s saga.Saga) []saga.Step {
	var names []saga.Step
	for _, st := range s.Steps {
		names = append(names, st.Name)
	}
	return names
}

func mustStart(t *testing.T, h *harness, key string) saga.Saga {
	t.Helper()
	s, _, err := h.uc.Start(context.Background(), command(key))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return s
}

func TestA_happy_path_runs_three_steps_and_completes(t *testing.T) {
	h := newHarness()
	s, created, err := h.uc.Start(context.Background(), command("key-00000001"))
	if err != nil || !created {
		t.Fatalf("Start = created %v, err %v", created, err)
	}
	if got, want := *h.order, []string{"register", "create-administrator", "complete"}; !reflect.DeepEqual(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
	if s.Status != saga.Completed || s.TenantID != "tenant-1" || s.FailedStep != "" {
		t.Errorf("saga = %+v", s)
	}
	want := []saga.Step{saga.CreateTenant, saga.CreateAdministrator, saga.RecordAdministrator}
	if !reflect.DeepEqual(steps(s), want) {
		t.Errorf("steps = %v, want %v", steps(s), want)
	}
}

func TestA_step_two_failure_compensates_step_one(t *testing.T) {
	h := newHarness()
	h.security.err = rejected()
	s := mustStart(t, h, "key-00000001")
	if s.Status != saga.Compensated || s.FailedStep != saga.CreateAdministrator {
		t.Errorf("status %s, failedStep %s", s.Status, s.FailedStep)
	}
	if got, want := *h.order, []string{"register", "create-administrator", "fail-onboarding"}; !reflect.DeepEqual(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
	if h.identity.failedStep != saga.CreateAdministrator {
		t.Errorf("fail-onboarding got step %s", h.identity.failedStep)
	}
	if len(s.Steps) != 1 || !s.Steps[0].Compensated {
		t.Errorf("steps = %+v, want step 1 compensated", s.Steps)
	}
	savedWhileRunning := false
	for _, snapshot := range h.store.snapshots {
		if snapshot.Status == saga.Running && len(snapshot.Steps) == 1 && snapshot.Steps[0].Compensated {
			savedWhileRunning = true
		}
	}
	if !savedWhileRunning {
		t.Error("the compensation must be saved right after it ran, before the saga ends")
	}
}

func TestA_failed_compensation_ends_failed(t *testing.T) {
	h := newHarness()
	h.security.err = rejected()
	h.identity.undo = errors.New("identity-audit unreachable")
	s := mustStart(t, h, "key-00000001")
	if s.Status != saga.Failed || s.FailedStep != saga.CreateAdministrator {
		t.Errorf("status %s, failedStep %s", s.Status, s.FailedStep)
	}
	if s.Steps[0].Compensated {
		t.Error("step 1 must not be marked compensated when its undo failed")
	}
	if !strings.Contains(s.FailureDetail, "; compensation of CREATE_TENANT failed: UNKNOWN") {
		t.Errorf("failure detail = %q, want the failed compensation named", s.FailureDetail)
	}
	if logs := h.logs.String(); !strings.Contains(logs, "sagaId=saga-1") || !strings.Contains(logs, "CREATE_TENANT") || strings.Contains(logs, "unreachable") {
		t.Errorf("the failed compensation must be logged, sanitized, with saga and step: %s", logs)
	}
}

func TestA_step_three_failure_ends_failed_without_undoing_the_account(t *testing.T) {
	h := newHarness()
	h.identity.completeErr = rejected()
	s := mustStart(t, h, "key-00000001")
	if s.Status != saga.Failed || s.FailedStep != saga.RecordAdministrator {
		t.Errorf("status %s, failedStep %s", s.Status, s.FailedStep)
	}
	if got, want := *h.order, []string{"register", "create-administrator", "complete"}; !reflect.DeepEqual(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
}

func TestA_saga_stopped_before_step_two_is_compensated_on_resume(t *testing.T) {
	h := newHarness()
	h.store.put(saga.Saga{ID: "saga-9", IdempotencyKey: "key-00000009", Status: saga.Running, TenantID: "tenant-9",
		Steps: []saga.StepState{{Name: saga.CreateTenant}}, CreatedAt: t0.Add(-time.Hour), UpdatedAt: t0.Add(-time.Hour)})
	if err := h.uc.Resume(context.Background(), t0.Add(-2*time.Minute)); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	s, _ := h.store.Get(context.Background(), "saga-9")
	if s.Status != saga.Compensated || s.FailedStep != saga.CreateAdministrator {
		t.Errorf("status %s, failedStep %s", s.Status, s.FailedStep)
	}
	if got, want := *h.order, []string{"fail-onboarding"}; !reflect.DeepEqual(got, want) {
		t.Errorf("calls = %v, want only the compensation", got)
	}
}

func TestI_state_is_saved_after_each_step(t *testing.T) {
	h := newHarness()
	mustStart(t, h, "key-00000001")
	if got := steps(h.security.atCall); !reflect.DeepEqual(got, []saga.Step{saga.CreateTenant}) || h.security.atCall.TenantID != "tenant-1" {
		t.Errorf("before step 2 the store held %+v", h.security.atCall)
	}
	if got := steps(h.identity.atComplete); len(got) != 2 {
		t.Errorf("before step 3 the store held %v", got)
	}
	if last := h.store.latest(); last.Status != saga.Completed || len(last.Steps) != 3 {
		t.Errorf("final state = %+v", last)
	}
	// RN-WFL-10: what a resume needs is kept as the saga goes, never the password.
	if got := h.security.atCall; got.OnBehalfOf != "platform-admin-sub" || got.InitialStatus != "ACTIVE" {
		t.Errorf("before step 2 the store held on behalf of %q, initial status %q", got.OnBehalfOf, got.InitialStatus)
	}
	if got := h.identity.atComplete; got.MembershipID != "membership-1" {
		t.Errorf("before step 3 the store held membership id %q", got.MembershipID)
	}
}

func TestI_password_never_reaches_state_or_logs(t *testing.T) {
	h := newHarness()
	h.security.err = fmt.Errorf("dial tcp: boom, body had password=%s", password)
	s := mustStart(t, h, "key-00000001")
	if s.FailureDetail == "" {
		t.Error("an unknown participant error still leaves a sanitized detail")
	}
	for _, snapshot := range h.store.snapshots {
		if strings.Contains(fmt.Sprintf("%+v", snapshot), password) {
			t.Fatalf("password reached the state: %+v", snapshot)
		}
	}
	if strings.Contains(h.logs.String(), password) {
		t.Fatalf("password reached the log: %s", h.logs.String())
	}
}

func TestC_each_step_sends_its_derived_key(t *testing.T) {
	h := newHarness()
	s := mustStart(t, h, "key-00000001")
	got := []string{h.identity.calls[0].Key, h.security.calls[0].Key, h.identity.calls[1].Key}
	want := []string{s.ID + ":CREATE_TENANT", s.ID + ":CREATE_ADMINISTRATOR", s.ID + ":RECORD_ADMINISTRATOR"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("keys = %v, want %v", got, want)
	}
	if h.security.calls[0].OnBehalfOf != "platform-admin-sub" {
		t.Errorf("onBehalfOf = %q", h.security.calls[0].OnBehalfOf)
	}
}

func TestC_repeated_start_returns_same_saga_and_runs_nothing(t *testing.T) {
	h := newHarness()
	first := mustStart(t, h, "key-00000001")
	calls := len(*h.order)
	again, created, err := h.uc.Start(context.Background(), command("key-00000001"))
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("repeat = id %s created %v err %v, want id %s", again.ID, created, err, first.ID)
	}
	if len(*h.order) != calls {
		t.Errorf("the repeat ran steps: %v", *h.order)
	}
}

func unknown() error { return &saga.ParticipantError{Code: "TIMEOUT", Message: "no answer"} }

func TestA_unknown_outcome_at_step_two_ends_failed_without_compensating(t *testing.T) {
	for name, cause := range map[string]error{"timeout": unknown(), "plain error": errors.New("connection reset")} {
		t.Run(name, func(t *testing.T) {
			h := newHarness()
			h.security.err = cause
			s := mustStart(t, h, "key-00000001")
			if s.Status != saga.Failed || s.FailedStep != saga.CreateAdministrator {
				t.Errorf("status %s, failedStep %s", s.Status, s.FailedStep)
			}
			if got, want := *h.order, []string{"register", "create-administrator"}; !reflect.DeepEqual(got, want) {
				t.Errorf("calls = %v, want %v: the account may exist, step 1 must stay", got, want)
			}
		})
	}
}

func TestA_unknown_outcome_at_step_one_ends_failed(t *testing.T) {
	h := newHarness()
	h.identity.registerErr = unknown()
	s := mustStart(t, h, "key-00000001")
	if s.Status != saga.Failed || s.FailedStep != saga.CreateTenant {
		t.Errorf("status %s, failedStep %s", s.Status, s.FailedStep)
	}
}

func TestA_definitive_rejection_at_step_one_ends_compensated(t *testing.T) {
	h := newHarness()
	h.identity.registerErr = rejected()
	s := mustStart(t, h, "key-00000001")
	if s.Status != saga.Compensated || s.FailedStep != saga.CreateTenant {
		t.Errorf("status %s, failedStep %s", s.Status, s.FailedStep)
	}
}

func TestA_cancelled_caller_does_not_stop_the_compensation(t *testing.T) {
	h := newHarness()
	ctx, cancel := context.WithCancel(context.Background())
	h.security.err = rejected()
	h.security.hook = cancel
	s, _, err := h.uc.Start(ctx, command("key-00000001"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if s.Status != saga.Compensated || h.store.latest().Status != saga.Compensated {
		t.Errorf("status %s, stored %s: the caller leaving must not cancel the saga", s.Status, h.store.latest().Status)
	}
}

func TestA_resume_of_a_saga_with_step_three_done_completes_it(t *testing.T) {
	h := newHarness()
	h.store.put(staleSaga(saga.CreateTenant, saga.CreateAdministrator, saga.RecordAdministrator))
	if err := h.uc.Resume(context.Background(), t0); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if s, _ := h.store.Get(context.Background(), "saga-9"); s.Status != saga.Completed || len(*h.order) != 0 {
		t.Errorf("status %s, calls %v", s.Status, *h.order)
	}
}

func TestA_resume_of_a_saga_with_step_two_done_finishes_step_three(t *testing.T) {
	h := newHarness()
	h.store.put(staleSaga(saga.CreateTenant, saga.CreateAdministrator))
	if err := h.uc.Resume(context.Background(), t0); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	s, _ := h.store.Get(context.Background(), "saga-9")
	if s.Status != saga.Completed || !reflect.DeepEqual(*h.order, []string{"complete"}) {
		t.Fatalf("saga %+v, calls %v", s, *h.order)
	}
	if got, want := h.identity.completeArgs, [3]string{"tenant-9", "membership-9", "INACTIVE"}; got != want {
		t.Errorf("step 3 got %v, want the saved data %v", got, want)
	}
	if got := h.identity.calls[0]; got.OnBehalfOf != "platform-admin-sub" || got.Key != "saga-9:RECORD_ADMINISTRATOR" {
		t.Errorf("step 3 call = %+v", got)
	}
}

func TestA_resume_with_step_three_failing_ends_failed_for_a_person_without_undoing_anything(t *testing.T) {
	h := newHarness()
	h.identity.completeErr = unknown()
	h.store.put(staleSaga(saga.CreateTenant, saga.CreateAdministrator))
	if err := h.uc.Resume(context.Background(), t0); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	s, _ := h.store.Get(context.Background(), "saga-9")
	if s.Status != saga.Failed || s.FailedStep != saga.RecordAdministrator || !reflect.DeepEqual(*h.order, []string{"complete"}) {
		t.Errorf("saga %+v, calls %v: the account exists, nothing may be undone", s, *h.order)
	}
}

func TestA_resume_of_a_legacy_saga_without_saved_data_ends_failed(t *testing.T) {
	h := newHarness()
	legacy := staleSaga(saga.CreateTenant, saga.CreateAdministrator)
	legacy.OnBehalfOf, legacy.MembershipID, legacy.InitialStatus = "", "", ""
	h.store.put(legacy)
	if err := h.uc.Resume(context.Background(), t0); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	s, _ := h.store.Get(context.Background(), "saga-9")
	if s.Status != saga.Failed || s.FailedStep != saga.RecordAdministrator || !strings.HasPrefix(s.FailureDetail, "RESTARTED") || len(*h.order) != 0 {
		t.Errorf("saga %+v, calls %v", s, *h.order)
	}
}

func TestA_resume_compensates_on_behalf_of_the_saved_platform_administrator(t *testing.T) {
	h := newHarness()
	h.store.put(staleSaga(saga.CreateTenant))
	if err := h.uc.Resume(context.Background(), t0); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if len(h.identity.calls) != 1 || h.identity.calls[0].OnBehalfOf != "platform-admin-sub" {
		t.Errorf("compensation calls = %+v, want one on behalf of the saved administrator", h.identity.calls)
	}
}

func TestA_resume_skips_a_saga_another_instance_took(t *testing.T) {
	h := newHarness()
	h.store.put(staleSaga(saga.CreateTenant, saga.CreateAdministrator))
	h.store.conflictAt = 1 // between the claim and the save, someone else moved the saga
	if err := h.uc.Resume(context.Background(), t0); err != nil {
		t.Fatalf("Resume: %v, a lost race is not a failure of the resume", err)
	}
	if s, _ := h.store.Get(context.Background(), "saga-9"); s.Status != saga.Running {
		t.Errorf("status %s: the loser must leave the saga to the one that took it", s.Status)
	}
}

func TestA_resume_skips_a_saga_that_ended_between_the_claim_and_the_save(t *testing.T) {
	h := newHarness()
	h.store.put(staleSaga(saga.CreateTenant, saga.CreateAdministrator))
	h.store.endedAt = 1
	if err := h.uc.Resume(context.Background(), t0); err != nil {
		t.Fatalf("Resume: %v, a saga that ended on its own is not a failure of the resume", err)
	}
}

func TestA_live_saga_stops_when_the_resume_took_it(t *testing.T) {
	h := newHarness()
	h.store.conflictAt = 1 // the save after step 1
	_, _, err := h.uc.Start(context.Background(), command("key-00000001"))
	if !errors.Is(err, saga.ErrConflict) {
		t.Fatalf("Start = %v, want ErrConflict", err)
	}
	if got, want := *h.order, []string{"register"}; !reflect.DeepEqual(got, want) {
		t.Errorf("calls = %v, want %v: a stopped saga neither goes on nor compensates", got, want)
	}
}

func TestA_second_resume_over_a_compensated_saga_does_nothing(t *testing.T) {
	h := newHarness()
	h.store.put(staleSaga(saga.CreateTenant))
	for i := 0; i < 2; i++ {
		if err := h.uc.Resume(context.Background(), t0); err != nil {
			t.Fatalf("Resume %d: %v", i, err)
		}
	}
	if got, want := *h.order, []string{"fail-onboarding"}; !reflect.DeepEqual(got, want) {
		t.Errorf("calls = %v, want one compensation", got)
	}
}

func TestA_rerun_skips_steps_already_compensated(t *testing.T) {
	h := newHarness()
	s := staleSaga(saga.CreateTenant)
	s.Steps[0].Compensated = true
	h.store.put(s)
	if err := h.uc.Resume(context.Background(), t0); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got, _ := h.store.Get(context.Background(), "saga-9"); got.Status != saga.Compensated || len(*h.order) != 0 {
		t.Errorf("status %s, calls %v: nothing should be undone twice", got.Status, *h.order)
	}
}

func staleSaga(done ...saga.Step) saga.Saga {
	s := saga.Saga{ID: "saga-9", IdempotencyKey: "key-00000009", Status: saga.Running, TenantID: "tenant-9",
		OnBehalfOf: "platform-admin-sub", MembershipID: "membership-9", InitialStatus: "INACTIVE",
		CreatedAt: t0.Add(-time.Hour), UpdatedAt: t0.Add(-time.Hour)}
	for _, step := range done {
		s.Record(step)
	}
	return s
}

func TestC_administrator_never_shows_its_password(t *testing.T) {
	admin := saga.Administrator{FullName: "Ana Perez", Email: "ana@example.test", Password: password}
	wrapped := saga.Onboarding{Administrator: admin}
	var logs bytes.Buffer
	slog.New(slog.NewTextHandler(&logs, nil)).Info("start", "admin", admin, "onboarding", wrapped)
	for name, shown := range map[string]string{
		"%v": fmt.Sprintf("%v", admin), "%+v": fmt.Sprintf("%+v", admin), "%#v": fmt.Sprintf("%#v", admin),
		"%+v nested": fmt.Sprintf("%+v", wrapped), "slog": logs.String(),
	} {
		if strings.Contains(shown, password) {
			t.Errorf("%s shows the password: %s", name, shown)
		}
	}
}
