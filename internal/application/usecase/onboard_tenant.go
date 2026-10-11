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

var errNotImplemented = errors.New("not implemented")

type OnboardTenant struct {
	store    out.Store
	identity out.IdentityAudit
	security out.Security
	log      *slog.Logger
	newID    func() string
	now      func() time.Time
}

func NewOnboardTenant(store out.Store, identity out.IdentityAudit, security out.Security,
	log *slog.Logger, newID func() string, now func() time.Time) *OnboardTenant {
	return &OnboardTenant{store, identity, security, log, newID, now}
}

var _ in.OnboardTenant = (*OnboardTenant)(nil)

func (o *OnboardTenant) Start(ctx context.Context, cmd in.StartCommand) (saga.Saga, bool, error) {
	return saga.Saga{}, false, errNotImplemented
}

func (o *OnboardTenant) Get(ctx context.Context, id string) (saga.Saga, error) {
	return saga.Saga{}, errNotImplemented
}

// Resume closes the RUNNING sagas that have not advanced since before.
func (o *OnboardTenant) Resume(ctx context.Context, before time.Time) error {
	return errNotImplemented
}
