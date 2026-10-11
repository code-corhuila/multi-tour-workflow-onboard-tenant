// Package usecase is the orchestrator of the onboard-tenant saga.
package usecase

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/application/port/in"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/application/port/out"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/domain/saga"
)

const maxDetailRunes = 500

type OnboardTenant struct {
	store    out.Store
	identity out.IdentityAudit
	security out.Security
	log      *slog.Logger
	newID    func() string
	now      func() time.Time
	bound    time.Duration // longest a started saga may run, independent of the caller
}

func NewOnboardTenant(store out.Store, identity out.IdentityAudit, security out.Security,
	log *slog.Logger, newID func() string, now func() time.Time, bound time.Duration) *OnboardTenant {
	return &OnboardTenant{store, identity, security, log, newID, now, bound}
}

var _ in.OnboardTenant = (*OnboardTenant)(nil)

func (o *OnboardTenant) Start(ctx context.Context, cmd in.StartCommand) (saga.Saga, bool, error) {
	now := o.now()
	fresh := saga.Saga{ID: o.newID(), IdempotencyKey: cmd.IdempotencyKey, Status: saga.Running, CreatedAt: now, UpdatedAt: now}
	s, created, err := o.store.Create(ctx, fresh)
	if err != nil || !created {
		return s, false, err
	}
	return s, true, o.run(ctx, &s, cmd.Onboarding)
}

func (o *OnboardTenant) Get(ctx context.Context, id string) (saga.Saga, error) {
	return o.store.Get(ctx, id)
}

// Resume closes the RUNNING sagas that have not advanced since before. The password is gone after a
// restart, so a saga that never created the account is compensated, never resumed.
func (o *OnboardTenant) Resume(ctx context.Context, before time.Time) error {
	stale, err := o.store.ListStale(ctx, before)
	if err != nil {
		return err
	}
	var failures []error
	for i := range stale {
		if err := o.closeStale(ctx, &stale[i]); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (o *OnboardTenant) closeStale(ctx context.Context, s *saga.Saga) error {
	switch {
	case s.Done(saga.RecordAdministrator):
		s.End(saga.Completed, "", "")
		return o.finish(ctx, s)
	case s.Done(saga.CreateAdministrator):
		// The membership id needed by step 3 is not persisted, so a person must finish it.
		s.End(saga.Failed, saga.RecordAdministrator, "RESTARTED: stopped after the account was created")
		return o.finish(ctx, s)
	}
	cause := &saga.ParticipantError{Code: "RESTARTED", Message: "stopped before the account was created"}
	return o.abort(ctx, s, saga.CreateAdministrator, cause, "")
}

func (o *OnboardTenant) run(ctx context.Context, s *saga.Saga, req saga.Onboarding) error {
	call := func(step saga.Step) out.Call { return out.Call{Key: s.StepKey(step), OnBehalfOf: req.OnBehalfOf} }

	tenantID, err := o.identity.RegisterPendingTenant(ctx, call(saga.CreateTenant), req.Tenant)
	if err != nil {
		return o.abort(ctx, s, saga.CreateTenant, err, req.OnBehalfOf)
	}
	s.TenantID = tenantID
	if err := o.completed(ctx, s, saga.CreateTenant); err != nil {
		return err
	}

	membershipID, err := o.security.CreateFirstAdministrator(ctx, call(saga.CreateAdministrator), tenantID, req.Administrator)
	if err != nil {
		return o.abort(ctx, s, saga.CreateAdministrator, err, req.OnBehalfOf)
	}
	if err := o.completed(ctx, s, saga.CreateAdministrator); err != nil {
		return err
	}

	if err := o.identity.CompleteOnboarding(ctx, call(saga.RecordAdministrator), tenantID, membershipID, req.Tenant.InitialStatus); err != nil {
		return o.abort(ctx, s, saga.RecordAdministrator, err, req.OnBehalfOf)
	}
	s.Record(saga.RecordAdministrator)
	s.End(saga.Completed, "", "")
	return o.finish(ctx, s)
}

func (o *OnboardTenant) completed(ctx context.Context, s *saga.Saga, step saga.Step) error {
	s.Record(step)
	return o.save(ctx, s)
}

// abort undoes the completed steps in reverse order. Step 3 has no undo: the saga ends FAILED for a person.
// A compensation that fails also ends FAILED.
func (o *OnboardTenant) abort(ctx context.Context, s *saga.Saga, failed saga.Step, cause error, actor string) error {
	status := saga.Compensated
	if failed == saga.RecordAdministrator {
		status = saga.Failed
	}
	for i := len(s.Steps) - 1; i >= 0 && status == saga.Compensated; i-- {
		if s.Steps[i].Compensated {
			continue
		}
		if err := o.undo(ctx, s, s.Steps[i].Name, failed, actor); err != nil {
			status = saga.Failed
			break
		}
		s.MarkCompensated(s.Steps[i].Name)
	}
	s.End(status, failed, sanitize(cause))
	return o.finish(ctx, s)
}

// undo is the compensation of one step; only step 1 has one, the others created nothing to undo.
func (o *OnboardTenant) undo(ctx context.Context, s *saga.Saga, step, failed saga.Step, actor string) error {
	if step != saga.CreateTenant {
		return nil
	}
	call := out.Call{Key: s.StepKey(step) + ":undo", OnBehalfOf: actor}
	return o.identity.FailOnboarding(ctx, call, s.TenantID, failed)
}

func (o *OnboardTenant) save(ctx context.Context, s *saga.Saga) error {
	s.UpdatedAt = o.now()
	return o.store.Save(ctx, *s)
}

func (o *OnboardTenant) finish(ctx context.Context, s *saga.Saga) error {
	if err := o.save(ctx, s); err != nil {
		return err
	}
	o.log.Info("saga ended", "sagaId", s.ID, "status", s.Status, "failedStep", s.FailedStep)
	return nil
}

// sanitize keeps a participant's code and message and drops any other error, whose text can carry
// connection strings, tokens or the password.
func sanitize(err error) string {
	var participant *saga.ParticipantError
	detail := "PARTICIPANT_ERROR: the participant call failed"
	if errors.As(err, &participant) {
		detail = participant.Error()
	}
	if r := []rune(detail); len(r) > maxDetailRunes {
		detail = string(r[:maxDetailRunes])
	}
	return detail
}
