// Package out holds the ports the orchestrator needs: one per participant, and the saga store.
package out

import (
	"context"
	"time"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/domain/saga"
)

// Call is what every participant call carries besides its data.
type Call struct {
	Key        string // Idempotency-Key: <saga>:<step>
	OnBehalfOf string // the Platform Administrator's sub (ADR-004 D5)
}

// IdentityAudit is the participant of steps 1 and 3 and of the compensation of step 1.
// An error that is not a *saga.ParticipantError is treated as an unknown failure.
type IdentityAudit interface {
	RegisterPendingTenant(ctx context.Context, call Call, tenant saga.TenantData) (tenantID string, err error)
	CompleteOnboarding(ctx context.Context, call Call, tenantID, membershipID, initialStatus string) error
	// FailOnboarding frees the identifier of the pending tenant; failedStep is the step that failed.
	FailOnboarding(ctx context.Context, call Call, tenantID string, failedStep saga.Step) error
}

// Security is the participant of step 2. It has no compensation: if it rejects, nothing was created.
type Security interface {
	CreateFirstAdministrator(ctx context.Context, call Call, tenantID string, admin saga.Administrator) (membershipID string, err error)
}

// Store persists the saga. Create inserts a RUNNING saga or, when the idempotency key exists, returns that one
// with created=false.
type Store interface {
	Create(ctx context.Context, s saga.Saga) (stored saga.Saga, created bool, err error)
	// Save writes the saga if it is still the stored copy (same Version) and raises s.Version; otherwise it answers
	// saga.ErrConflict (moved by another actor), saga.ErrEnded or saga.ErrNotFound.
	Save(ctx context.Context, s *saga.Saga) error
	Get(ctx context.Context, id string) (saga.Saga, error)
	// ClaimStale takes the RUNNING sagas not updated since before: it stamps them with now and raises their Version in
	// one step, so no other caller takes the same saga, and returns them as taken.
	ClaimStale(ctx context.Context, before, now time.Time) ([]saga.Saga, error)
}
