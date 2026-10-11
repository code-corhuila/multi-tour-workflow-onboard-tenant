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
	byID      map[string]saga.Saga
	snapshots []saga.Saga
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

func (f *fakeStore) Create(_ context.Context, s saga.Saga) (saga.Saga, bool, error) {
	for _, existing := range f.byID {
		if existing.IdempotencyKey == s.IdempotencyKey {
			return copySaga(existing), false, nil
		}
	}
	f.put(s)
	return copySaga(s), true, nil
}

func (f *fakeStore) Save(_ context.Context, s saga.Saga) error { f.put(s); return nil }

func (f *fakeStore) Get(_ context.Context, id string) (saga.Saga, error) {
	s, ok := f.byID[id]
	if !ok {
		return saga.Saga{}, saga.ErrNotFound
	}
	return copySaga(s), nil
}

func (f *fakeStore) ListStale(_ context.Context, before time.Time) ([]saga.Saga, error) {
	var stale []saga.Saga
	for _, s := range f.byID {
		if s.Status == saga.Running && s.UpdatedAt.Before(before) {
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
}

func (f *fakeIdentity) RegisterPendingTenant(_ context.Context, c out.Call, _ saga.TenantData) (string, error) {
	*f.order = append(*f.order, "register")
	f.calls = append(f.calls, c)
	return "tenant-1", f.registerErr
}

func (f *fakeIdentity) CompleteOnboarding(_ context.Context, c out.Call, _, _, _ string) error {
	*f.order = append(*f.order, "complete")
	f.calls = append(f.calls, c)
	f.atComplete = f.store.latest()
	return f.completeErr
}

func (f *fakeIdentity) FailOnboarding(_ context.Context, c out.Call, _ string, step saga.Step) error {
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
}

func (f *fakeSecurity) CreateFirstAdministrator(_ context.Context, c out.Call, _ string, _ saga.Administrator) (string, error) {
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
		func() time.Time { return t0 })
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
	return &saga.ParticipantError{Code: "BUSINESS_RULE_VIOLATION", Message: "email already has an account"}
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
