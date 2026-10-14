package main

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	secretToken = "svc-token-secret-value"
	secretDB    = "postgres://workflow_onboard_tenant_app:db-password-value@postgres:5432/multi_tour"
)

func env(overrides map[string]string) func(string) string {
	values := map[string]string{
		"IDENTITY_AUDIT_BASE_URL": "http://identity-audit-api:8080",
		"SECURITY_BASE_URL":       "http://security:8080",
		"SERVICE_TOKEN":           secretToken,
		"JWT_PUBLIC_KEY_FILE":     "/run/secrets/jwt_public.pem",
		"DATABASE_URL":            secretDB,
	}
	for k, v := range overrides {
		values[k] = v
	}
	return func(name string) string { return values[name] }
}

func TestC_TheConfigurationHasSafeDefaults(t *testing.T) {
	cfg, err := loadConfig(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listenAddr != ":8080" || cfg.stepTimeout != 5*time.Second || cfg.stepAttempts != 3 {
		t.Errorf("defaults %+v", cfg)
	}
}

func TestC_TheConfigurationReadsEveryDeclaredVariable(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{"LISTEN_ADDR": ":9000", "STEP_TIMEOUT": "2s", "STEP_ATTEMPTS": "4",
		"JWT_PUBLIC_KEY_FILE": "", "JWT_PUBLIC_KEY": "-----BEGIN PUBLIC KEY-----"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listenAddr != ":9000" || cfg.stepTimeout != 2*time.Second || cfg.stepAttempts != 4 ||
		cfg.identityURL != "http://identity-audit-api:8080" || cfg.securityURL != "http://security:8080" ||
		cfg.serviceToken != secretToken || cfg.databaseURL != secretDB || cfg.publicKey != "-----BEGIN PUBLIC KEY-----" {
		t.Errorf("config %+v", cfg)
	}
}

func TestC_AnInvalidConfigurationIsRefusedNamingTheVariableNeverItsValue(t *testing.T) {
	cases := map[string]map[string]string{
		"IDENTITY_AUDIT_BASE_URL":  {"IDENTITY_AUDIT_BASE_URL": ""},
		"SECURITY_BASE_URL":        {"SECURITY_BASE_URL": "not a url"},
		"SERVICE_TOKEN":            {"SERVICE_TOKEN": ""},
		"DATABASE_URL":             {"DATABASE_URL": ""},
		"JWT_PUBLIC_KEY":           {"JWT_PUBLIC_KEY_FILE": ""},
		"STEP_TIMEOUT":             {"STEP_TIMEOUT": "forever"},
		"STEP_TIMEOUT ":            {"STEP_TIMEOUT": "90s"},
		"STEP_ATTEMPTS":            {"STEP_ATTEMPTS": "0"},
		"STEP_ATTEMPTS ":           {"STEP_ATTEMPTS": "9"},
		"IDENTITY_AUDIT_BASE_URL ": {"IDENTITY_AUDIT_BASE_URL": "ftp://identity-audit-api"},
	}
	for name, overrides := range cases {
		_, err := loadConfig(env(overrides))
		want := strings.TrimSpace(name)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err %v", name, err)
			continue
		}
		if strings.Contains(err.Error(), secretToken) || strings.Contains(err.Error(), "db-password-value") {
			t.Errorf("%s: the error repeats a secret: %v", name, err)
		}
	}
}

// A saga runs inside one HTTP request, so the server may not cut the answer before the longest saga ends.
func TestC_TheServerLimitsAreExplicitAndOutlastTheLongestSaga(t *testing.T) {
	cfg, _ := loadConfig(env(nil))
	bound := sagaBound(cfg)

	srv := newServer(cfg, http.NotFoundHandler(), bound)

	if srv.ReadHeaderTimeout <= 0 || srv.ReadTimeout <= 0 || srv.IdleTimeout <= 0 || srv.MaxHeaderBytes <= 0 {
		t.Errorf("a limit is left to the default: %+v", srv)
	}
	if srv.WriteTimeout <= bound || srv.Addr != ":8080" {
		t.Errorf("write timeout %v must exceed the saga bound %v; addr %q", srv.WriteTimeout, bound, srv.Addr)
	}
	if bound < time.Duration(4*cfg.stepAttempts)*cfg.stepTimeout {
		t.Errorf("bound %v is shorter than four steps with every attempt used", bound)
	}
}

// A saga already started must be allowed to finish before the pool closes: the grace period outlasts the bound.
func TestC_ShutdownWaitsForTheLongestSagaAndTheComposeGraceOutlastsIt(t *testing.T) {
	cfg, _ := loadConfig(env(nil))
	bound := sagaBound(cfg)

	if grace := shutdownGrace(bound); grace <= bound {
		t.Errorf("shutdown grace %v must exceed the saga bound %v", grace, bound)
	}
}

func TestC_TheConfigurationErrorsListTheVariablesInAStableOrder(t *testing.T) {
	_, first := loadConfig(env(map[string]string{"IDENTITY_AUDIT_BASE_URL": "", "SECURITY_BASE_URL": ""}))
	for i := 0; i < 20; i++ {
		if _, again := loadConfig(env(map[string]string{"IDENTITY_AUDIT_BASE_URL": "", "SECURITY_BASE_URL": ""})); again.Error() != first.Error() {
			t.Fatalf("the error order changed: %q then %q", first, again)
		}
	}
}
