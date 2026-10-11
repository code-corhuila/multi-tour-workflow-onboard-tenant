// Package httpapi is the entry of the onboard-tenant saga: the Platform Administrator starts it and reads its
// state (openapi/openapi.yaml). Every error uses the single envelope; the detail of a failure stays in the saga
// state and the log, and the answer names only the step that failed.
package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/application/port/in"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/correlation"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/domain/saga"
)

const (
	route         = "/api/v1/sagas/onboard-tenant"
	minKeyLength  = 8
	maxKeyLength  = 128
	correlationHd = "X-Correlation-Id"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type handler struct {
	uc       in.OnboardTenant
	verifier *Verifier
	log      *slog.Logger
}

func New(uc in.OnboardTenant, verifier *Verifier, log *slog.Logger) http.Handler {
	h := &handler{uc, verifier, log}
	mux := http.NewServeMux()
	mux.HandleFunc(route, h.collection)
	mux.HandleFunc(route+"/{id}", h.item)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusNotFound, "NOT_FOUND", "route not found")
	})
	return h.withCorrelation(mux)
}

// withCorrelation reuses a well-formed X-Correlation-Id or generates one, returns it and puts it in the context.
func (h *handler) withCorrelation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(correlationHd)
		if !correlation.Valid(id) {
			id = correlation.NewID()
		}
		w.Header().Set(correlationHd, id)
		next.ServeHTTP(w, r.WithContext(correlation.With(r.Context(), id)))
	})
}

// authorize answers 401 or 403 itself and reports whether the caller is a Platform Administrator.
func (h *handler) authorize(w http.ResponseWriter, r *http.Request) (Claims, bool) {
	scheme, raw, found := strings.Cut(r.Header.Get("Authorization"), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || raw == "" {
		return Claims{}, deny(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required")
	}
	claims, err := h.verifier.Verify(strings.TrimSpace(raw))
	if err != nil {
		return Claims{}, deny(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required")
	}
	if !claims.isPlatformAdministrator() {
		return Claims{}, deny(w, r, http.StatusForbidden, "FORBIDDEN", "only the Platform Administrator may use this route")
	}
	return claims, true
}

func deny(w http.ResponseWriter, r *http.Request, status int, code, message string) bool {
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	writeError(w, r, status, code, message)
	return false
}

func (h *handler) collection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, r, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	claims, ok := h.authorize(w, r)
	if !ok {
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) < minKeyLength || len(key) > maxKeyLength {
		writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "Idempotency-Key is required, 8 to 128 characters",
			fieldError{"Idempotency-Key", "must be 8 to 128 characters"})
		return
	}
	if mediaType, _, _ := strings.Cut(r.Header.Get("Content-Type"), ";"); strings.TrimSpace(mediaType) != "application/json" {
		writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "Content-Type must be application/json")
		return
	}
	onboarding, problems := decodeStart(r, w)
	if problems != nil {
		writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "the request is not valid", problems...)
		return
	}
	onboarding.OnBehalfOf = claims.Subject

	started, created, err := h.uc.Start(r.Context(), in.StartCommand{IdempotencyKey: key, Onboarding: onboarding})
	if err != nil {
		h.fail(w, r, "start the saga", err)
		return
	}
	if !created {
		writeJSON(w, http.StatusOK, toResponse(started))
		return
	}
	w.Header().Set("Location", route+"/"+started.ID)
	writeJSON(w, http.StatusCreated, toResponse(started))
}

func (h *handler) item(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, r, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	if _, ok := h.authorize(w, r); !ok {
		return
	}
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		writeError(w, r, http.StatusNotFound, "NOT_FOUND", "saga not found")
		return
	}
	found, err := h.uc.Get(r.Context(), id)
	switch {
	case errors.Is(err, saga.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "NOT_FOUND", "saga not found")
	case err != nil:
		h.fail(w, r, "read the saga", err)
	default:
		writeJSON(w, http.StatusOK, toResponse(found))
	}
}

// fail logs the cause under the trace id and answers a generic 500: no driver text, host or trace leaves.
func (h *handler) fail(w http.ResponseWriter, r *http.Request, what string, err error) {
	h.log.Error("entry route failed", "traceId", correlation.From(r.Context()), "action", what, "error", err.Error())
	writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "internal error")
}

type errorResponse struct {
	Error   string       `json:"error"`
	Message string       `json:"message"`
	Details []fieldError `json:"details,omitempty"`
	TraceID string       `json:"traceId"`
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string, details ...fieldError) {
	writeJSON(w, status, errorResponse{code, message, details, correlation.From(r.Context())})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// sagaResponse is the Saga of the contract. It has no field for the failure detail on purpose.
type sagaResponse struct {
	ID             string    `json:"id"`
	Status         string    `json:"status"`
	CompletedSteps []string  `json:"completedSteps"`
	FailedStep     string    `json:"failedStep,omitempty"`
	TenantID       string    `json:"tenantId,omitempty"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

// stepNames are the step names of the contract, the ones identity-audit uses for the steps it knows.
var stepNames = map[saga.Step]string{
	saga.CreateTenant:        "CREATE_TENANT",
	saga.CreateAdministrator: "CREATE_ACCOUNT",
	saga.RecordAdministrator: "APPLY_STATUS",
}

func toResponse(s saga.Saga) sagaResponse {
	steps := make([]string, 0, len(s.Steps))
	for _, st := range s.Steps {
		steps = append(steps, stepNames[st.Name])
	}
	return sagaResponse{ID: s.ID, Status: string(s.Status), CompletedSteps: steps, FailedStep: stepNames[s.FailedStep],
		TenantID: s.TenantID, CreatedAt: s.CreatedAt.UTC(), UpdatedAt: s.UpdatedAt.UTC()}
}
