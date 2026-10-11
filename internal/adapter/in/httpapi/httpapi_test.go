package httpapi_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/golang-jwt/jwt/v5"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/adapter/in/httpapi"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/application/port/in"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/correlation"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/domain/saga"
)

const (
	route      = "/api/v1/sagas/onboard-tenant"
	sagaID     = "33333333-3333-4333-8333-333333333333"
	tenantID   = "11111111-1111-4111-8111-111111111111"
	thePasword = "Sup3r-Secret-Pass!"
	goodKey    = "key-0001-abcdef"
	goodBody   = `{"identifier":"acme-tours","commercialName":"Acme Tours","timeZone":"America/Bogota","country":"CO",` +
		`"initialStatus":"ACTIVE","administrator":{"fullName":"Ana Perez","email":"ana@example.com","password":"` + thePasword + `"}}`
)

var signer = func() *rsa.PrivateKey {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return key
}()

func token(t *testing.T, key *rsa.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()
	signed, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func claims(mutate func(jwt.MapClaims)) jwt.MapClaims {
	c := jwt.MapClaims{"sub": "admin-1", "roles": []string{"PLATFORM_ADMINISTRATOR"}, "exp": time.Now().Add(time.Hour).Unix()}
	if mutate != nil {
		mutate(c)
	}
	return c
}

type fakeUseCase struct {
	started  saga.Saga
	created  bool
	startErr error
	cmd      in.StartCommand
	startCtx context.Context

	found  saga.Saga
	getErr error
}

func (f *fakeUseCase) Start(ctx context.Context, cmd in.StartCommand) (saga.Saga, bool, error) {
	f.cmd, f.startCtx = cmd, ctx
	return f.started, f.created, f.startErr
}

func (f *fakeUseCase) Get(context.Context, string) (saga.Saga, error) { return f.found, f.getErr }

func completed() saga.Saga {
	now := time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)
	return saga.Saga{ID: sagaID, Status: saga.Completed, TenantID: tenantID, CreatedAt: now, UpdatedAt: now,
		Steps: []saga.StepState{{Name: saga.CreateTenant}, {Name: saga.CreateAdministrator}, {Name: saga.RecordAdministrator}}}
}

func newHandler(uc in.OnboardTenant, logs *bytes.Buffer) http.Handler {
	return httpapi.New(uc, httpapi.NewVerifier(&signer.PublicKey), slog.New(slog.NewJSONHandler(logs, nil)))
}

type call struct {
	method, path, contentType, body, bearer, key, corr string
}

func (c call) send(h http.Handler) *httptest.ResponseRecorder {
	req := httptest.NewRequest(c.method, c.path, strings.NewReader(c.body))
	if c.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearer)
	}
	if c.contentType != "" {
		req.Header.Set("Content-Type", c.contentType)
	}
	if c.key != "" {
		req.Header.Set("Idempotency-Key", c.key)
	}
	if c.corr != "" {
		req.Header.Set("X-Correlation-Id", c.corr)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func post(t *testing.T, body string) call {
	return call{http.MethodPost, route, "application/json", body, token(t, signer, claims(nil)), goodKey, ""}
}

func get(t *testing.T, id string) call {
	return call{method: http.MethodGet, path: route + "/" + id, bearer: token(t, signer, claims(nil))}
}

// Every answer is validated against the published contract, and the last check fails when a response the contract
// declares was never produced: the gate toward qa (ADR-004 D7).
func TestC_EveryResponseOfTheContractIsProducedAndValid(t *testing.T) {
	ctx := context.Background()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../../openapi/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.Validate(ctx); err != nil {
		t.Fatalf("the contract is not valid: %v", err)
	}
	router, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	exercised := map[string]bool{}
	other := token(t, signer, claims(func(c jwt.MapClaims) { c["roles"] = []string{"TENANT_ADMINISTRATOR"} }))

	type scenario struct {
		name string
		c    call
		uc   *fakeUseCase
		want int
	}
	okUC := func(created bool) *fakeUseCase {
		return &fakeUseCase{started: completed(), created: created, found: completed()}
	}
	noAuth, forbidden := post(t, goodBody), post(t, goodBody)
	noAuth.bearer, forbidden.bearer = "", other
	noKey, shortKey, notJSON, unknownField := post(t, goodBody), post(t, goodBody), post(t, `{"identifier":`), post(t, strings.Replace(goodBody, `"country"`, `"extra":1,"country"`, 1))
	noKey.key, shortKey.key = "", "short"
	badType := post(t, goodBody)
	badType.contentType = "text/plain"
	getNoAuth, getForbidden := get(t, sagaID), get(t, sagaID)
	getNoAuth.bearer, getForbidden.bearer = "", other

	scenarios := []scenario{
		{"created", post(t, goodBody), okUC(true), http.StatusCreated},
		{"repeated key", post(t, goodBody), okUC(false), http.StatusOK},
		{"no token", noAuth, okUC(true), http.StatusUnauthorized},
		{"not platform administrator", forbidden, okUC(true), http.StatusForbidden},
		{"no key", noKey, okUC(true), http.StatusBadRequest},
		{"short key", shortKey, okUC(true), http.StatusBadRequest},
		{"malformed json", notJSON, okUC(true), http.StatusBadRequest},
		{"unknown field", unknownField, okUC(true), http.StatusBadRequest},
		{"wrong content type", badType, okUC(true), http.StatusBadRequest},
		{"use case fails", post(t, goodBody), &fakeUseCase{startErr: errors.New("pq: password authentication failed for host=db-internal")}, http.StatusInternalServerError},
		{"read", get(t, sagaID), okUC(true), http.StatusOK},
		{"read without token", getNoAuth, okUC(true), http.StatusUnauthorized},
		{"read forbidden", getForbidden, okUC(true), http.StatusForbidden},
		{"read unknown", get(t, sagaID), &fakeUseCase{getErr: saga.ErrNotFound}, http.StatusNotFound},
		{"read store failure", get(t, sagaID), &fakeUseCase{getErr: errors.New("connection refused host=db-internal")}, http.StatusInternalServerError},
	}
	for _, s := range scenarios {
		var logs bytes.Buffer
		s.c.corr = "gate-1"
		rec := s.c.send(newHandler(s.uc, &logs))
		if rec.Code != s.want {
			t.Errorf("%s: status %d, want %d (%s)", s.name, rec.Code, s.want, rec.Body)
			continue
		}
		if strings.Contains(rec.Body.String(), "db-internal") || strings.Contains(rec.Body.String(), thePasword) {
			t.Errorf("%s: the body leaks internal detail: %s", s.name, rec.Body)
		}
		req := httptest.NewRequest(s.c.method, s.c.path, nil)
		found, params, err := router.FindRoute(req)
		if err != nil {
			t.Fatalf("%s %s is not in the contract: %v", s.c.method, s.c.path, err)
		}
		in := &openapi3filter.ResponseValidationInput{
			RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: found},
			Status:                 rec.Code, Header: rec.Header(), Options: &openapi3filter.Options{IncludeResponseStatus: true},
		}
		in.SetBodyBytes(rec.Body.Bytes())
		if err := openapi3filter.ValidateResponse(ctx, in); err != nil {
			t.Errorf("%s: answer against the contract: %v", s.name, err)
		}
		exercised[s.c.method+" "+found.Path+" "+strconv.Itoa(rec.Code)] = true
	}
	for path, item := range doc.Paths.Map() {
		for method, operation := range item.Operations() {
			for status := range operation.Responses.Map() {
				if !exercised[method+" "+path+" "+status] {
					t.Errorf("the response %s of %s %s was never produced", status, method, path)
				}
			}
		}
	}
}

func TestC_ARequestIsRefusedForEveryInvalidField(t *testing.T) {
	fields := map[string]string{
		"identifier":              strings.Replace(goodBody, `"acme-tours"`, `"Acme_Tours"`, 1),
		"identifier too short":    strings.Replace(goodBody, `"acme-tours"`, `"ab"`, 1),
		"commercialName empty":    strings.Replace(goodBody, `"Acme Tours"`, `""`, 1),
		"timeZone":                strings.Replace(goodBody, `"America/Bogota"`, `""`, 1),
		"country":                 strings.Replace(goodBody, `"CO"`, `"col"`, 1),
		"initialStatus":           strings.Replace(goodBody, `"ACTIVE"`, `"PENDING"`, 1),
		"administrator.fullName":  strings.Replace(goodBody, `"Ana Perez"`, `""`, 1),
		"administrator.email":     strings.Replace(goodBody, `"ana@example.com"`, `"nobody"`, 1),
		"administrator.password":  strings.Replace(goodBody, thePasword, `short`, 1),
		"administrator.password ": strings.Replace(goodBody, thePasword, strings.Repeat("p", 73), 1),
	}
	for name, body := range fields {
		uc := &fakeUseCase{started: completed(), created: true}
		var logs bytes.Buffer

		rec := post(t, body).send(newHandler(uc, &logs))

		field := strings.TrimSpace(strings.SplitN(name, " ", 2)[0])
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"field":"`+field+`"`) {
			t.Errorf("%s: status %d, body %s", name, rec.Code, rec.Body)
		}
		if strings.Contains(rec.Body.String(), "short") && strings.Contains(name, "password") {
			t.Errorf("%s: the answer repeats the password: %s", name, rec.Body)
		}
		if uc.startCtx != nil {
			t.Errorf("%s: the saga must not start on an invalid request", name)
		}
	}
}

func TestC_TheSagaStartsOnBehalfOfTheCallerWithItsKeyAndCorrelationId(t *testing.T) {
	uc := &fakeUseCase{started: completed(), created: true}
	var logs bytes.Buffer

	rec := call{http.MethodPost, route, "application/json", goodBody, token(t, signer, claims(nil)), goodKey, "corr-77"}.send(newHandler(uc, &logs))

	want := in.StartCommand{IdempotencyKey: goodKey, Onboarding: saga.Onboarding{
		Tenant:        saga.TenantData{Identifier: "acme-tours", CommercialName: "Acme Tours", TimeZone: "America/Bogota", Country: "CO", InitialStatus: "ACTIVE"},
		Administrator: saga.Administrator{FullName: "Ana Perez", Email: "ana@example.com", Password: thePasword}, OnBehalfOf: "admin-1"}}
	if uc.cmd != want {
		t.Errorf("command %+v, want %+v", uc.cmd, want)
	}
	if got := correlation.From(uc.startCtx); got != "corr-77" || rec.Header().Get("X-Correlation-Id") != "corr-77" {
		t.Errorf("context id %q, header %q", got, rec.Header().Get("X-Correlation-Id"))
	}
	if loc := rec.Header().Get("Location"); loc != route+"/"+sagaID {
		t.Errorf("Location %q", loc)
	}
	if strings.Contains(logs.String(), thePasword) || strings.Contains(rec.Body.String(), thePasword) {
		t.Error("the password reached the log or the answer")
	}
}

func TestC_ARequestWithoutAUsableCorrelationIdGetsANewOne(t *testing.T) {
	for _, corr := range []string{"", "bad id with spaces"} {
		c := get(t, sagaID)
		c.corr = corr

		rec := c.send(newHandler(&fakeUseCase{found: completed()}, &bytes.Buffer{}))

		if got := rec.Header().Get("X-Correlation-Id"); got == "" || got == corr {
			t.Errorf("sent %q, answered %q", corr, got)
		}
	}
}

func TestC_AnErrorCarriesTheCorrelationIdAsTraceId(t *testing.T) {
	c := get(t, sagaID)
	c.bearer, c.corr = "", "trace-9"

	rec := c.send(newHandler(&fakeUseCase{}, &bytes.Buffer{}))

	var body struct{ Error, Message, TraceID string }
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if rec.Code != http.StatusUnauthorized || body.Error != "UNAUTHORIZED" || body.TraceID != "trace-9" {
		t.Errorf("status %d, body %s", rec.Code, rec.Body)
	}
}

// The detail of a failure stays in the saga state and the log: the answer names the step that failed and nothing else.
func TestC_AFailedSagaAnswersOnlyTheFailedStepNeverItsDetail(t *testing.T) {
	failed := completed()
	failed.Steps = []saga.StepState{{Name: saga.CreateTenant, Compensated: true}}
	failed.End(saga.Compensated, saga.CreateAdministrator, "BUSINESS_RULE_VIOLATION: the email ana@example.com already has an account")
	uc := &fakeUseCase{started: failed, created: true}

	rec := post(t, goodBody).send(newHandler(uc, &bytes.Buffer{}))

	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if rec.Code != http.StatusCreated || body["status"] != "COMPENSATED" || body["failedStep"] != "CREATE_ACCOUNT" {
		t.Errorf("status %d, body %s", rec.Code, rec.Body)
	}
	if strings.Contains(rec.Body.String(), "ana@example.com") || strings.Contains(rec.Body.String(), "BUSINESS_RULE") {
		t.Errorf("the answer carries the failure detail: %s", rec.Body)
	}
	steps, _ := body["completedSteps"].([]any)
	if len(steps) != 1 || steps[0] != "CREATE_TENANT" {
		t.Errorf("completedSteps %v", body["completedSteps"])
	}
}

func TestC_SagaStepsAreNamedAsTheContractNamesThem(t *testing.T) {
	rec := get(t, sagaID).send(newHandler(&fakeUseCase{found: completed()}, &bytes.Buffer{}))

	var body struct {
		CompletedSteps []string
		TenantID       string
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if strings.Join(body.CompletedSteps, ",") != "CREATE_TENANT,CREATE_ACCOUNT,APPLY_STATUS" || body.TenantID != tenantID {
		t.Errorf("body %s", rec.Body)
	}
}

func TestC_ATokenIsAcceptedOnlyWhenItIsAnUnexpiredRS256TokenOfAPlatformAdministrator(t *testing.T) {
	foreign, _ := rsa.GenerateKey(rand.Reader, 2048)
	hs := func() string {
		s, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, claims(nil)).SignedString(signer.PublicKey.N.Bytes())
		return s
	}()
	none, _ := jwt.NewWithClaims(jwt.SigningMethodNone, claims(nil)).SignedString(jwt.UnsafeAllowNoneSignatureType)
	cases := map[string]struct {
		bearer string
		want   int
	}{
		"valid":                   {token(t, signer, claims(nil)), http.StatusOK},
		"expired":                 {token(t, signer, claims(func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-time.Hour).Unix() })), http.StatusUnauthorized},
		"no exp":                  {token(t, signer, claims(func(c jwt.MapClaims) { delete(c, "exp") })), http.StatusUnauthorized},
		"no sub":                  {token(t, signer, claims(func(c jwt.MapClaims) { delete(c, "sub") })), http.StatusUnauthorized},
		"other key":               {token(t, foreign, claims(nil)), http.StatusUnauthorized},
		"HS256":                   {hs, http.StatusUnauthorized},
		"alg none":                {none, http.StatusUnauthorized},
		"garbage":                 {"not-a-token", http.StatusUnauthorized},
		"roles of the wrong type": {token(t, signer, claims(func(c jwt.MapClaims) { c["roles"] = "PLATFORM_ADMINISTRATOR" })), http.StatusUnauthorized},
		"no roles":                {token(t, signer, claims(func(c jwt.MapClaims) { delete(c, "roles") })), http.StatusForbidden},
		"restricted access":       {token(t, signer, claims(func(c jwt.MapClaims) { c["access"] = "restricted" })), http.StatusForbidden},
		"password change":         {token(t, signer, claims(func(c jwt.MapClaims) { c["access"] = "password-change" })), http.StatusForbidden},
		"a service":               {token(t, signer, claims(func(c jwt.MapClaims) { c["roles"] = []string{"SERVICE"} })), http.StatusForbidden},
	}
	for name, c := range cases {
		rec := call{method: http.MethodGet, path: route + "/" + sagaID, bearer: c.bearer}.send(newHandler(&fakeUseCase{found: completed()}, &bytes.Buffer{}))
		if rec.Code != c.want {
			t.Errorf("%s: status %d, want %d", name, rec.Code, c.want)
		}
	}
}

func TestC_AnUnknownRouteOrMethodAnswersTheEnvelope(t *testing.T) {
	for _, c := range []call{{method: http.MethodGet, path: "/api/v1/nothing"}, {method: http.MethodDelete, path: route}} {
		rec := c.send(newHandler(&fakeUseCase{}, &bytes.Buffer{}))

		var body struct{ Error, TraceID string }
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if (rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed) || body.Error == "" || body.TraceID == "" {
			t.Errorf("%s %s: status %d, body %s", c.method, c.path, rec.Code, rec.Body)
		}
	}
}

func TestC_AnIdThatIsNotAUuidIsNotFound(t *testing.T) {
	rec := get(t, "not-a-uuid").send(newHandler(&fakeUseCase{found: completed()}, &bytes.Buffer{}))

	if rec.Code != http.StatusNotFound {
		t.Errorf("status %d", rec.Code)
	}
}

func TestC_HealthNeedsNoTokenAndAnswers200(t *testing.T) {
	rec := call{method: http.MethodGet, path: "/health"}.send(newHandler(&fakeUseCase{}, &bytes.Buffer{}))

	if rec.Code != http.StatusOK {
		t.Errorf("status %d", rec.Code)
	}
}

func TestC_ThePublicKeyIsReadFromPEMAndMustBeRSA(t *testing.T) {
	der, _ := x509.MarshalPKIXPublicKey(&signer.PublicKey)
	if _, err := httpapi.ParsePublicKey(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})); err != nil {
		t.Errorf("valid key: %v", err)
	}
	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ecDER, _ := x509.MarshalPKIXPublicKey(&ec.PublicKey)
	for name, data := range map[string][]byte{
		"not pem": []byte("nope"), "empty": nil, "ec key": pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: ecDER}),
	} {
		if _, err := httpapi.ParsePublicKey(data); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}
