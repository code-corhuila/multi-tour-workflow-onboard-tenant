package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultListenAddr   = ":8080"
	defaultStepTimeout  = 5 * time.Second
	defaultStepAttempts = 3
	maxStepTimeout      = 30 * time.Second
	maxStepAttempts     = 5
	// a saga calls the participants at most four times: create tenant, create account, record, undo tenant
	callsPerSaga = 4
	// settling time added to the arithmetic bound: database saves between steps and the waits between attempts
	sagaMargin = 10 * time.Second
)

type config struct {
	listenAddr    string
	identityURL   string
	securityURL   string
	serviceToken  string
	publicKeyFile string
	publicKey     string
	databaseURL   string
	stepTimeout   time.Duration
	stepAttempts  int
}

// loadConfig reads the variables of .env.example. Every refusal names the variable and never its value: the token and
// the connection string are secrets.
func loadConfig(getenv func(string) string) (config, error) {
	cfg := config{
		listenAddr:    valueOr(getenv("LISTEN_ADDR"), defaultListenAddr),
		identityURL:   getenv("IDENTITY_AUDIT_BASE_URL"),
		securityURL:   getenv("SECURITY_BASE_URL"),
		serviceToken:  getenv("SERVICE_TOKEN"),
		publicKeyFile: getenv("JWT_PUBLIC_KEY_FILE"),
		publicKey:     getenv("JWT_PUBLIC_KEY"),
		databaseURL:   getenv("DATABASE_URL"),
		stepTimeout:   defaultStepTimeout,
		stepAttempts:  defaultStepAttempts,
	}
	var problems []error
	refuse := func(name, why string) { problems = append(problems, fmt.Errorf("%s %s", name, why)) }

	for name, value := range map[string]string{"IDENTITY_AUDIT_BASE_URL": cfg.identityURL, "SECURITY_BASE_URL": cfg.securityURL} {
		if u, err := url.Parse(value); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			refuse(name, "must be an http or https URL")
		}
	}
	if cfg.serviceToken == "" {
		refuse("SERVICE_TOKEN", "is required")
	}
	if cfg.databaseURL == "" {
		refuse("DATABASE_URL", "is required")
	}
	if cfg.publicKeyFile == "" && cfg.publicKey == "" {
		refuse("JWT_PUBLIC_KEY", "or JWT_PUBLIC_KEY_FILE is required")
	}
	if raw := getenv("STEP_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < time.Second || d > maxStepTimeout {
			refuse("STEP_TIMEOUT", "must be a duration from 1s to 30s")
		} else {
			cfg.stepTimeout = d
		}
	}
	if raw := getenv("STEP_ATTEMPTS"); raw != "" {
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || n < 1 || n > maxStepAttempts {
			refuse("STEP_ATTEMPTS", "must be a number from 1 to 5")
		} else {
			cfg.stepAttempts = n
		}
	}
	return cfg, errors.Join(problems...)
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// sagaBound is the longest a started saga may run: every call using every attempt, plus a margin.
func sagaBound(cfg config) time.Duration {
	return time.Duration(callsPerSaga*cfg.stepAttempts)*cfg.stepTimeout + sagaMargin
}

// newServer declares every limit (Annex C). A saga runs inside the request that starts it, so the write timeout
// is the saga bound plus a little: a shorter one would cut the answer of a saga that is still correct.
func newServer(cfg config, handler http.Handler, bound time.Duration) *http.Server {
	return &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      bound + 5*time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}
