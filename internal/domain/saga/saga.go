// Package saga is the onboard-tenant saga: its identity, status, steps and failure (Annex E).
// It imports only the standard library.
package saga

import (
	"errors"
	"log/slog"
	"time"
)

type Status string

const (
	Running     Status = "RUNNING"
	Completed   Status = "COMPLETED"
	Compensated Status = "COMPENSATED"
	Failed      Status = "FAILED"
)

type Step string

const (
	CreateTenant        Step = "CREATE_TENANT"
	CreateAdministrator Step = "CREATE_ADMINISTRATOR"
	RecordAdministrator Step = "RECORD_ADMINISTRATOR"
)

// ErrNotFound is returned by a store when no saga has the requested id.
var ErrNotFound = errors.New("saga not found")

// ErrConflict is returned by a store when a saga is saved from a copy that is no longer the stored one: another
// actor (a resume, a live run) moved it first, and the one that lost must stop.
var ErrConflict = errors.New("saga was changed by another actor")

// ErrEnded is returned by a store when a saga that already ended is saved again: an ended saga never changes.
var ErrEnded = errors.New("saga already ended")

// ParticipantError is the only error detail a participant may hand to the saga: a code and a message
// already free of connection strings, tokens and passwords. Any other error is reduced to a generic one.
type ParticipantError struct {
	Code    string
	Message string
	// Definitive is true only for a clear rejection (a 4xx): the participant did not do the work.
	// Anything else (timeout, lost answer, unknown) leaves the outcome unknown.
	Definitive bool
}

func (e *ParticipantError) Error() string { return e.Code + ": " + e.Message }

// TenantData is what step 1 creates and step 3 activates.
type TenantData struct {
	Identifier     string
	CommercialName string
	TimeZone       string
	Country        string
	InitialStatus  string // ACTIVE or INACTIVE
}

// Administrator carries the typed temporary password. It lives in memory for step 2 and is never part of Saga.
type Administrator struct {
	FullName string
	Email    string
	Password string
}

// String, GoString and LogValue keep the password out of fmt and slog output.
func (Administrator) String() string       { return "***" }
func (Administrator) GoString() string     { return "***" }
func (Administrator) LogValue() slog.Value { return slog.StringValue("***") }

// Onboarding is the request of the Platform Administrator, kept out of the saga state.
type Onboarding struct {
	Tenant        TenantData
	Administrator Administrator
	OnBehalfOf    string // the `sub` of the Platform Administrator
}

type StepState struct {
	Name        Step
	Compensated bool
}

type Saga struct {
	ID             string
	IdempotencyKey string
	Status         Status
	TenantID       string
	OnBehalfOf     string // the `sub` of the Platform Administrator, kept so a resume can act for them
	MembershipID   string // returned by step 2, needed by step 3
	InitialStatus  string // ACTIVE or INACTIVE, applied by step 3
	Version        int64  // counted by the store at every save and claim; a save from an older copy is refused
	Steps          []StepState
	FailedStep     Step
	FailureDetail  string // sanitized code and message only
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// StepKey is the Idempotency-Key a step sends: <saga>:<step>.
func (s Saga) StepKey(step Step) string { return s.ID + ":" + string(step) }

func (s Saga) Done(step Step) bool {
	for _, st := range s.Steps {
		if st.Name == step {
			return true
		}
	}
	return false
}

// Record marks a step as completed.
func (s *Saga) Record(step Step) { s.Steps = append(s.Steps, StepState{Name: step}) }

func (s *Saga) MarkCompensated(step Step) {
	for i := range s.Steps {
		if s.Steps[i].Name == step {
			s.Steps[i].Compensated = true
		}
	}
}

// End closes the saga. COMPENSATED and FAILED name the step that failed.
func (s *Saga) End(status Status, failed Step, detail string) {
	s.Status, s.FailedStep, s.FailureDetail = status, failed, detail
}
