// Package in holds the use case the entry adapter calls.
package in

import (
	"context"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/domain/saga"
)

type StartCommand struct {
	IdempotencyKey string
	saga.Onboarding
}

type OnboardTenant interface {
	// Start runs the saga; created is false when the key already started one (nothing runs again).
	Start(ctx context.Context, cmd StartCommand) (s saga.Saga, created bool, err error)
	Get(ctx context.Context, id string) (saga.Saga, error)
}
