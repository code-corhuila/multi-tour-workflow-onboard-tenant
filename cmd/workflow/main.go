// Command workflow is the composition root of the onboard-tenant saga: it reads the configuration, builds the
// store, the participant clients and the entry route, and serves until it is told to stop.
package main

import (
	"context"
	"crypto/rsa"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/adapter/in/httpapi"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/adapter/out/participants"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/adapter/out/state/postgres"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/application/usecase"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/correlation"
)

const (
	shutdownTimeout = 20 * time.Second
	retryBaseDelay  = 200 * time.Millisecond
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("workflow stopped", "error", err.Error())
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		return err
	}
	key, err := readPublicKey(cfg)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.Open(ctx, cfg.databaseURL, postgres.Limits{MaxConns: 10, ConnectTimeout: 5 * time.Second, StatementTimeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer pool.Close()

	participantCfg := participants.Config{Token: cfg.serviceToken, Timeout: cfg.stepTimeout, Attempts: cfg.stepAttempts, BaseDelay: retryBaseDelay}
	bound := sagaBound(cfg)
	onboard := usecase.NewOnboardTenant(postgres.NewStore(pool),
		participants.NewIdentityAudit(cfg.identityURL, participantCfg), participants.NewSecurity(cfg.securityURL, participantCfg),
		log, correlation.NewID, time.Now, bound)

	// One instance runs (declared debt F3): at start every saga still RUNNING was left by the previous process.
	resumeCtx, cancel := context.WithTimeout(ctx, bound)
	if err := onboard.Resume(resumeCtx, time.Now()); err != nil {
		log.Error("closing the sagas left running failed", "error", err.Error())
	}
	cancel()

	srv := newServer(cfg, httpapi.New(onboard, httpapi.NewVerifier(key), log), bound)
	serving := make(chan error, 1)
	go func() { serving <- srv.ListenAndServe() }()
	log.Info("workflow listening", "addr", cfg.listenAddr)

	select {
	case err := <-serving:
		return err
	case <-ctx.Done():
	}
	// stop accepting, let the running sagas answer, then close the pool (deferred)
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	log.Info("workflow stopped")
	return nil
}

func readPublicKey(cfg config) (*rsa.PublicKey, error) {
	pem := []byte(cfg.publicKey)
	if cfg.publicKeyFile != "" {
		data, err := os.ReadFile(cfg.publicKeyFile)
		if err != nil {
			return nil, errors.New("JWT_PUBLIC_KEY_FILE cannot be read")
		}
		pem = data
	}
	return httpapi.ParsePublicKey(pem)
}
