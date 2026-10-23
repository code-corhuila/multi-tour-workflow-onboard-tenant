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

// Start runs the saga to its end. The caller leaving must not cancel a compensation half way, so the
// saga runs on a context that keeps the caller's values but not its cancellation, bounded by o.bound.
func (o *OnboardTenant) Start(ctx context.Context, cmd in.StartCommand) (saga.Saga, bool, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.bound)
	defer cancel()
	now := o.now()
	fresh := saga.Saga{ID: o.newID(), IdempotencyKey: cmd.IdempotencyKey, Status: saga.Running, CreatedAt: now, UpdatedAt: now,
		OnBehalfOf: cmd.Onboarding.OnBehalfOf, InitialStatus: cmd.Onboarding.Tenant.InitialStatus}
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
//
// Declared debt (owner: Project Lead):
//   - F3: there is no claim or lock, so two instances, or one live slow saga, could be compensated by
//     mistake. The demo runs ONE instance; the fix is a store Claim plus a compare-and-swap Save.
func (o *OnboardTenant) Resume(ctx context.Context, before time.Time) error {
	stale, err := o.store.ClaimStale(ctx, before, o.now())
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
	case s.Done(saga.CreateAdministrator) && s.MembershipID != "":
		return o.finishStepThree(ctx, s)
	case s.Done(saga.CreateAdministrator):
		// A saga stopped before the membership id was kept (it started before the migration): a person finishes it.
		s.End(saga.Failed, saga.RecordAdministrator, "RESTARTED: stopped after the account was created")
		return o.finish(ctx, s)
	case !s.Done(saga.CreateTenant):
		// Step 1 may have created a tenant whose id was never saved: only a person can tell.
		s.End(saga.Failed, saga.CreateTenant, "RESTARTED: stopped before the tenant was recorded")
		return o.finish(ctx, s)
	}
	// Known limit: the process may have stopped after step 2 answered and before its save, so the
	// account could exist; compensating step 1 then leaves it pointing at a failed tenant.
	cause := &saga.ParticipantError{Code: "RESTARTED", Message: "stopped before the account was created", Definitive: true}
	return o.abort(ctx, s, saga.CreateAdministrator, cause, s.OnBehalfOf)
}

// finishStepThree runs step 3 of a resumed saga with the data kept by the earlier steps.
func (o *OnboardTenant) finishStepThree(ctx context.Context, s *saga.Saga) error {
	call := out.Call{Key: s.StepKey(saga.RecordAdministrator), OnBehalfOf: s.OnBehalfOf}
	if err := o.identity.CompleteOnboarding(ctx, call, s.TenantID, s.MembershipID, s.InitialStatus); err != nil {
		return o.abort(ctx, s, saga.RecordAdministrator, err, s.OnBehalfOf)
	}
	s.Record(saga.RecordAdministrator)
	s.End(saga.Completed, "", "")
	return o.finish(ctx, s)
}

// run executes the steps in order and saves the state after each one. Compensation of each step:
//
//	CREATE_TENANT         fail-onboarding frees the identifier (see undo)
//	CREATE_ADMINISTRATOR  none: a rejection means nothing was created; an unknown outcome is left to a person
//	RECORD_ADMINISTRATOR  none: a failure ends FAILED for a person
//
// Crash window: if the process stops after a participant answered and before the save that follows,
// the step is not recorded; Resume treats it as not done (see closeStale).
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
	s.MembershipID = membershipID
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

// abort ends the saga after step `failed` did not succeed. Only a definitive rejection is compensated
// (in reverse order, each undo saved as it runs); an unknown outcome or a step 3 failure ends FAILED
// with nothing undone, because the work may exist. A compensation that fails also ends FAILED.
func (o *OnboardTenant) abort(ctx context.Context, s *saga.Saga, failed saga.Step, cause error, actor string) error {
	detail := sanitize(cause)
	if !definitive(cause) || failed == saga.RecordAdministrator {
		s.End(saga.Failed, failed, detail)
		return o.finish(ctx, s)
	}
	status := saga.Compensated
	for i := len(s.Steps) - 1; i >= 0; i-- {
		step := s.Steps[i].Name
		if s.Steps[i].Compensated {
			continue
		}
		if err := o.undo(ctx, s, step, failed, actor); err != nil {
			code := codeOf(err)
			o.log.Warn("compensation failed", "sagaId", s.ID, "step", step, "code", code)
			detail += "; compensation of " + string(step) + " failed: " + code
			status = saga.Failed
			break
		}
		s.MarkCompensated(step)
		if err := o.save(ctx, s); err != nil {
			return err
		}
	}
	s.End(status, failed, detail)
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
	return o.store.Save(ctx, s)
}

func (o *OnboardTenant) finish(ctx context.Context, s *saga.Saga) error {
	if err := o.save(ctx, s); err != nil {
		return err
	}
	o.log.Info("saga ended", "sagaId", s.ID, "status", s.Status, "failedStep", s.FailedStep)
	return nil
}

func definitive(err error) bool {
	var participant *saga.ParticipantError
	return errors.As(err, &participant) && participant.Definitive
}

func codeOf(err error) string {
	var participant *saga.ParticipantError
	if errors.As(err, &participant) {
		return participant.Code
	}
	return "UNKNOWN"
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
