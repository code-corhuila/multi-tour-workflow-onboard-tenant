// Package postgres is the saga store over the schema workflow_onboard_tenant of the single PostgreSQL instance.
package postgres

import (
	"context"
	"time"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/application/port/out"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/domain/saga"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ out.Store = (*Store)(nil)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Create(ctx context.Context, sg saga.Saga) (saga.Saga, bool, error) {
	return saga.Saga{}, false, nil
}

func (s *Store) Save(ctx context.Context, sg saga.Saga) error { return nil }

func (s *Store) Get(ctx context.Context, id string) (saga.Saga, error) { return saga.Saga{}, nil }

func (s *Store) ListStale(ctx context.Context, before time.Time) ([]saga.Saga, error) {
	return nil, nil
}
