package participants

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/application/port/out"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/correlation"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/domain/saga"
)

const (
	serviceToken = "svc-token-secret"
	tenantUUID   = "11111111-1111-4111-8111-111111111111"
	accountUUID  = "22222222-2222-4222-8222-222222222222"
	password     = "Sup3r-Secret-Pass!"
	stepKey      = "saga-0001:CREATE_TENANT"
)

const tenantBody = `{"tenantId":"` + tenantUUID + `","identifier":"acme-tours","commercialName":"Acme Tours",` +
	`"tenantStatus":"PENDING","timeZone":"America/Bogota","country":"CO","createdAt":"2026-10-11T10:00:00Z"}`

const accountBody = `{"accountId":"` + accountUUID + `","tenantId":"` + tenantUUID + `","email":"ana@example.com",` +
	`"roles":["TENANT_ADMINISTRATOR"],"accountStatus":"ACTIVE","emailVerified":false,"createdAt":"2026-10-11T10:00:00Z"}`

func errorBody(code, message string) string {
	return `{"error":"` + code + `","message":"` + message + `","traceId":"t-1"}`
}

type reply struct {
	status int
	body   string
	raw    bool // skip validating the reply against the contract
}

type seen struct{ method, path, key, corr, auth, body string }

// recorder is a stand-in for a participant: it validates every request, and every reply the contract declares,
// against the subset of the participant contracts, then plays the scripted replies in order (the last repeats).
type recorder struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []seen
}

func (r *recorder) requests() []seen {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]seen(nil), r.reqs...)
}

func contractServer(t *testing.T, replies ...reply) *recorder {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile("testdata/participants.yaml")
	if err != nil {
		t.Fatal(err)
	}
	router, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	rec.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		rec.mu.Lock()
		n := len(rec.reqs)
		rec.reqs = append(rec.reqs, seen{r.Method, r.URL.Path, r.Header.Get("Idempotency-Key"),
			r.Header.Get("X-Correlation-Id"), r.Header.Get("Authorization"), string(body)})
		rec.mu.Unlock()
		next := replies[min(n, len(replies)-1)]

		route, params, err := router.FindRoute(r)
		if err != nil {
			t.Errorf("%s %s is not in the contract: %v", r.Method, r.URL.Path, err)
		} else {
			in := &openapi3filter.RequestValidationInput{Request: r, PathParams: params, Route: route}
			if err := openapi3filter.ValidateRequest(r.Context(), in); err != nil {
				t.Errorf("request against the contract: %v", err)
			}
			if !next.raw && route.Operation.Responses.Status(next.status) != nil {
				validateReply(t, in, next)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(next.status)
		_, _ = io.WriteString(w, next.body)
	}))
	t.Cleanup(rec.Close)
	return rec
}

func validateReply(t *testing.T, in *openapi3filter.RequestValidationInput, r reply) {
	t.Helper()
	out := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: in, Status: r.status,
		Header:  http.Header{"Content-Type": []string{"application/json"}},
		Options: &openapi3filter.Options{IncludeResponseStatus: true},
	}
	out.SetBodyBytes([]byte(r.body))
	if err := openapi3filter.ValidateResponse(context.Background(), out); err != nil {
		t.Errorf("scripted reply %d against the contract: %v", r.status, err)
	}
}

func testConfig() Config {
	return Config{Token: serviceToken, Timeout: time.Second, Attempts: 3, BaseDelay: time.Millisecond}
}

type sleeps struct{ waited []time.Duration }

func (s *sleeps) sleep(_ context.Context, d time.Duration) error {
	s.waited = append(s.waited, d)
	return nil
}

func identityAt(url string) (*IdentityAudit, *sleeps) {
	ia, s := NewIdentityAudit(url, testConfig()), &sleeps{}
	ia.c.sleep = s.sleep
	return ia, s
}

func securityAt(url string) (*Security, *sleeps) {
	sec, s := NewSecurity(url, testConfig()), &sleeps{}
	sec.c.sleep = s.sleep
	return sec, s
}

var (
	tenant = saga.TenantData{Identifier: "acme-tours", CommercialName: "Acme Tours", TimeZone: "America/Bogota",
		Country: "CO", InitialStatus: "ACTIVE"}
	admin = saga.Administrator{FullName: "Ana Perez", Email: "ana@example.com", Password: password}
	call  = out.Call{Key: stepKey, OnBehalfOf: "platform-admin-sub"}
)

// operations are the four participant calls, each against a base URL, so one table drives every failure test.
var operations = map[string]func(ctx context.Context, url string) error{
	"register": func(ctx context.Context, url string) error {
		ia, _ := identityAt(url)
		_, err := ia.RegisterPendingTenant(ctx, call, tenant)
		return err
	},
	"complete": func(ctx context.Context, url string) error {
		ia, _ := identityAt(url)
		return ia.CompleteOnboarding(ctx, call, tenantUUID, accountUUID, "ACTIVE")
	},
	"fail": func(ctx context.Context, url string) error {
		ia, _ := identityAt(url)
		return ia.FailOnboarding(ctx, call, tenantUUID, saga.CreateAdministrator)
	},
	"administrator": func(ctx context.Context, url string) error {
		sec, _ := securityAt(url)
		_, err := sec.CreateFirstAdministrator(ctx, call, tenantUUID, admin)
		return err
	},
}

func TestC_RegisterPendingTenantCallsTheContractRouteWithTheServiceCredential(t *testing.T) {
	for _, status := range []int{http.StatusCreated, http.StatusOK} {
		rec := contractServer(t, reply{status: status, body: tenantBody})
		ia, _ := identityAt(rec.URL)

		id, err := ia.RegisterPendingTenant(correlation.With(context.Background(), "corr-1"), call, tenant)

		if err != nil || id != tenantUUID {
			t.Fatalf("status %d: id %q, err %v", status, id, err)
		}
		got := rec.requests()
		if len(got) != 1 {
			t.Fatalf("status %d: %d requests, want 1", status, len(got))
		}
		want := seen{http.MethodPost, "/api/v1/identity-audit/internal/tenants", stepKey, "corr-1", "Bearer " + serviceToken, got[0].body}
		if got[0] != want {
			t.Errorf("status %d: sent %+v, want %+v", status, got[0], want)
		}
		if strings.Contains(got[0].body, "initialStatus") {
			t.Errorf("the tenant request must not carry the initial status: %s", got[0].body)
		}
	}
}

func TestC_ACallWithoutCorrelationIdGeneratesOne(t *testing.T) {
	rec := contractServer(t, reply{status: 201, body: tenantBody})
	ia, _ := identityAt(rec.URL)
	if _, err := ia.RegisterPendingTenant(context.Background(), call, tenant); err != nil {
		t.Fatal(err)
	}
	if got := rec.requests()[0].corr; got == "" {
		t.Error("no X-Correlation-Id was sent")
	}
}

func TestC_CompleteOnboardingSendsTheMembershipAndTheInitialStatus(t *testing.T) {
	rec := contractServer(t, reply{status: 200, body: tenantBody})
	ia, _ := identityAt(rec.URL)

	if err := ia.CompleteOnboarding(context.Background(), call, tenantUUID, accountUUID, "INACTIVE"); err != nil {
		t.Fatal(err)
	}
	got := rec.requests()[0]
	if got.path != "/api/v1/identity-audit/internal/tenants/"+tenantUUID+"/complete-onboarding" || got.key != stepKey {
		t.Errorf("sent %+v", got)
	}
	var body map[string]string
	_ = json.Unmarshal([]byte(got.body), &body)
	if body["firstAdministratorMembershipId"] != accountUUID || body["initialStatus"] != "INACTIVE" {
		t.Errorf("body %s", got.body)
	}
}

func TestC_FailOnboardingSendsTheIdentityAuditNameOfTheFailedStep(t *testing.T) {
	rec := contractServer(t, reply{status: 200, body: tenantBody})
	ia, _ := identityAt(rec.URL)

	if err := ia.FailOnboarding(context.Background(), call, tenantUUID, saga.CreateAdministrator); err != nil {
		t.Fatal(err)
	}
	got := rec.requests()[0]
	if got.path != "/api/v1/identity-audit/internal/tenants/"+tenantUUID+"/fail-onboarding" ||
		got.body != `{"failedStep":"CREATE_ACCOUNT"}` {
		t.Errorf("sent %+v", got)
	}
}

func TestC_FailOnboardingForAStepWithoutCounterpartSendsNothing(t *testing.T) {
	rec := contractServer(t, reply{status: 200, body: tenantBody})
	ia, _ := identityAt(rec.URL)

	err := ia.FailOnboarding(context.Background(), call, tenantUUID, saga.CreateTenant)

	var participant *saga.ParticipantError
	if err == nil || errors.As(err, &participant) || len(rec.requests()) != 0 {
		t.Errorf("err %v, requests %d: want an error that is not a participant answer and no request", err, len(rec.requests()))
	}
}

// CREATE_TENANT is the saga's own step 1: identity-audit has no name for it because a failure there leaves nothing
// to mark failed. The other two steps are the ones identity-audit's failedStep enum calls CREATE_ACCOUNT and APPLY_STATUS.
func TestC_StepNamesMapBetweenTheSagaAndIdentityAudit(t *testing.T) {
	cases := []struct {
		saga     saga.Step
		identity string
		ok       bool
	}{
		{saga.CreateAdministrator, "CREATE_ACCOUNT", true},
		{saga.RecordAdministrator, "APPLY_STATUS", true},
		{saga.CreateTenant, "", false},
		{saga.Step("OTHER"), "", false},
	}
	for _, c := range cases {
		got, ok := identityStep(c.saga)
		if got != c.identity || ok != c.ok {
			t.Errorf("identityStep(%s) = %q, %v; want %q, %v", c.saga, got, ok, c.identity, c.ok)
		}
	}
}

func TestC_CreateFirstAdministratorReturnsTheAccountIdAsTheMembership(t *testing.T) {
	for _, status := range []int{http.StatusCreated, http.StatusOK} {
		rec := contractServer(t, reply{status: status, body: accountBody})
		sec, _ := securityAt(rec.URL)

		id, err := sec.CreateFirstAdministrator(context.Background(), call, tenantUUID, admin)

		if err != nil || id != accountUUID {
			t.Fatalf("status %d: id %q, err %v", status, id, err)
		}
		got := rec.requests()[0]
		if got.path != "/api/v1/security/internal/tenants/"+tenantUUID+"/administrators" ||
			got.auth != "Bearer "+serviceToken || got.key != stepKey {
			t.Errorf("status %d: sent %+v", status, got)
		}
	}
}

func TestC_ARejectionIsDefinitiveAndCarriesTheEnvelopeCodeOnce(t *testing.T) {
	for name, op := range operations {
		for _, status := range []int{400, 422} {
			rec := contractServer(t, reply{status: status, body: errorBody("BUSINESS_RULE_VIOLATION", "identifier already used")})

			err := op(context.Background(), rec.URL)

			var participant *saga.ParticipantError
			if !errors.As(err, &participant) || !participant.Definitive || participant.Code != "BUSINESS_RULE_VIOLATION" {
				t.Errorf("%s %d: got %v, want a definitive participant error", name, status, err)
			}
			if n := len(rec.requests()); n != 1 {
				t.Errorf("%s %d: %d requests, a 4xx must not be retried", name, status, n)
			}
		}
	}
}

func TestC_ARejectionWithoutAnEnvelopeKeepsTheStatusAsItsCode(t *testing.T) {
	rec := contractServer(t, reply{status: 403, body: "<html>no</html>", raw: true})

	err := operations["register"](context.Background(), rec.URL)

	var participant *saga.ParticipantError
	if !errors.As(err, &participant) || !participant.Definitive || participant.Code != "HTTP_403" {
		t.Errorf("got %v", err)
	}
}

func TestC_ATransientFailureIsRetriedWithTheSameKeyAndJitterBoundedByTheBackoff(t *testing.T) {
	for _, first := range []int{429, 500, 503} {
		rec := contractServer(t, reply{status: first, body: errorBody("INTERNAL_ERROR", "x"), raw: true}, reply{status: 201, body: tenantBody})
		ia, waits := identityAt(rec.URL)

		id, err := ia.RegisterPendingTenant(context.Background(), call, tenant)

		got := rec.requests()
		if err != nil || id != tenantUUID || len(got) != 2 || got[0].key != got[1].key {
			t.Fatalf("first %d: id %q, err %v, requests %+v", first, id, err, got)
		}
		if len(waits.waited) != 1 || waits.waited[0] < 0 || waits.waited[0] > testConfig().BaseDelay {
			t.Errorf("first %d: waited %v, want one wait within [0, %v]", first, waits.waited, testConfig().BaseDelay)
		}
	}
}

// An answer that never came, or one that says "try later", leaves the outcome unknown: it must not look like a rejection,
// because the participant may have done the work.
func TestC_ExhaustedRetriesLeaveTheOutcomeUnknownNeverARejection(t *testing.T) {
	for _, status := range []int{429, 500, 503} {
		for name, op := range operations {
			rec := contractServer(t, reply{status: status, body: errorBody("INTERNAL_ERROR", "x"), raw: true})

			err := op(context.Background(), rec.URL)

			var participant *saga.ParticipantError
			if !errors.As(err, &participant) || participant.Definitive {
				t.Errorf("%s %d: got %v, want a participant error that is not definitive", name, status, err)
			}
			if n := len(rec.requests()); n != testConfig().Attempts {
				t.Errorf("%s %d: %d requests, want %d", name, status, n, testConfig().Attempts)
			}
		}
	}
}

func TestC_ANetworkFailureIsRetriedAndLeavesTheOutcomeUnknown(t *testing.T) {
	rec := contractServer(t, reply{status: 201, body: tenantBody})
	url := rec.URL
	rec.Close()
	ia, waits := identityAt(url)

	_, err := ia.RegisterPendingTenant(context.Background(), call, tenant)

	var participant *saga.ParticipantError
	if !errors.As(err, &participant) || participant.Definitive {
		t.Errorf("got %v, want a participant error that is not definitive", err)
	}
	if len(waits.waited) != testConfig().Attempts-1 {
		t.Errorf("waited %d times, want %d", len(waits.waited), testConfig().Attempts-1)
	}
}

func TestC_ASlowParticipantIsCutAtTheStepTimeout(t *testing.T) {
	var calls int
	var mu sync.Mutex
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		select {
		case <-r.Context().Done():
		case <-time.After(300 * time.Millisecond):
		}
	}))
	defer slow.Close()
	cfg := testConfig()
	cfg.Timeout, cfg.Attempts = 20*time.Millisecond, 2
	ia := NewIdentityAudit(slow.URL, cfg)
	ia.c.sleep = (&sleeps{}).sleep

	_, err := ia.RegisterPendingTenant(context.Background(), call, tenant)

	var participant *saga.ParticipantError
	if !errors.As(err, &participant) || participant.Definitive {
		t.Errorf("got %v, want a participant error that is not definitive", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Errorf("%d calls, want 2", calls)
	}
}

func TestC_ASuccessWithoutTheExpectedIdLeavesTheOutcomeUnknown(t *testing.T) {
	rec := contractServer(t, reply{status: 201, body: `{"unexpected":true}`, raw: true})
	ia, _ := identityAt(rec.URL)

	_, err := ia.RegisterPendingTenant(context.Background(), call, tenant)

	var participant *saga.ParticipantError
	if !errors.As(err, &participant) || participant.Definitive {
		t.Errorf("got %v, want a participant error that is not definitive", err)
	}
}

func TestC_ThePasswordNeverLeavesInAnError(t *testing.T) {
	rec := contractServer(t, reply{status: 422, body: errorBody("BUSINESS_RULE_VIOLATION", "the password "+password+" is too common")})
	sec, _ := securityAt(rec.URL)

	_, err := sec.CreateFirstAdministrator(context.Background(), call, tenantUUID, admin)

	if err == nil || strings.Contains(err.Error(), password) {
		t.Errorf("error %v leaks the password", err)
	}
}

// complete-onboarding and fail-onboarding take no Idempotency-Key in the contract: after a lost answer the retry meets
// the state the first attempt already set and is refused with 422, which proves nothing about the first attempt.
func TestC_A422OnARetryOfAStateChangeLeavesTheOutcomeUnknown(t *testing.T) {
	for _, name := range []string{"complete", "fail"} {
		rec := contractServer(t,
			reply{status: 503, body: errorBody("SERVICE_UNAVAILABLE", "x"), raw: true},
			reply{status: 422, body: errorBody("INVALID_STATUS_TRANSITION", "the tenant is not pending")})

		err := operations[name](context.Background(), rec.URL)

		var participant *saga.ParticipantError
		if !errors.As(err, &participant) || participant.Definitive {
			t.Errorf("%s: got %v, want a participant error that is not definitive", name, err)
		}
	}
}

func TestC_ARedirectIsNeverFollowedAndLeavesTheOutcomeUnknown(t *testing.T) {
	target := contractServer(t, reply{status: 201, body: tenantBody})
	redirector := httptest.NewServer(http.RedirectHandler(target.URL+"/api/v1/identity-audit/internal/tenants", http.StatusTemporaryRedirect))
	defer redirector.Close()
	ia, _ := identityAt(redirector.URL)

	_, err := ia.RegisterPendingTenant(context.Background(), call, tenant)

	var participant *saga.ParticipantError
	if !errors.As(err, &participant) || participant.Definitive || len(target.requests()) != 0 {
		t.Errorf("err %v, target requests %d: want an unknown outcome and the redirect not followed", err, len(target.requests()))
	}
}

func TestC_AMalformedCorrelationIdIsReplacedNotSent(t *testing.T) {
	rec := contractServer(t, reply{status: 201, body: tenantBody})
	ia, _ := identityAt(rec.URL)

	_, err := ia.RegisterPendingTenant(correlation.With(context.Background(), "bad\r\nX-Evil: 1"), call, tenant)

	got := rec.requests()
	if err != nil || len(got) != 1 || got[0].corr == "" || strings.ContainsAny(got[0].corr, "\r\n ") {
		t.Errorf("err %v, requests %+v", err, got)
	}
}

func TestC_ARequestTimeoutAnswerIsRetriedAndLeavesTheOutcomeUnknown(t *testing.T) {
	for _, status := range []int{408, 425} {
		rec := contractServer(t, reply{status: status, body: errorBody("TIMEOUT", "x"), raw: true})

		err := operations["register"](context.Background(), rec.URL)

		var participant *saga.ParticipantError
		if !errors.As(err, &participant) || participant.Definitive || len(rec.requests()) != testConfig().Attempts {
			t.Errorf("%d: err %v, requests %d", status, err, len(rec.requests()))
		}
	}
}

func TestC_ThePasswordCannotSurviveAsAPrefixCutByTheMessageLimit(t *testing.T) {
	message := strings.Repeat("a", maxMessageRunes-3) + password
	rec := contractServer(t, reply{status: 422, body: errorBody("BUSINESS_RULE_VIOLATION", message)})
	sec, _ := securityAt(rec.URL)

	_, err := sec.CreateFirstAdministrator(context.Background(), call, tenantUUID, admin)

	if err == nil || strings.Contains(err.Error(), password[:3]) {
		t.Errorf("error %v keeps part of the password", err)
	}
}

func TestC_AnEmptyConfigStillMakesACall(t *testing.T) {
	rec := contractServer(t, reply{status: 201, body: tenantBody})
	ia := NewIdentityAudit(rec.URL, Config{Token: serviceToken})

	if _, err := ia.RegisterPendingTenant(context.Background(), call, tenant); err != nil {
		t.Errorf("got %v", err)
	}
}
