package participants

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/correlation"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/domain/saga"
)

const (
	maxMessageRunes = 200
	maxReplyBytes   = 1 << 20
	maxBackoff      = 30 * time.Second
	defaultTimeout  = 5 * time.Second
)

// Config bounds every participant call (ADR-004 D10).
type Config struct {
	Token     string        // SERVICE_TOKEN of this workflow, never the person's token (norm 5.8.2)
	Timeout   time.Duration // per attempt
	Attempts  int           // total attempts, at least 1
	BaseDelay time.Duration // ceiling of the first wait; it doubles on each retry
}

type client struct {
	baseURL string
	cfg     Config
	http    *http.Client
	sleep   func(ctx context.Context, d time.Duration) error
}

func newClient(baseURL string, cfg Config) *client {
	cfg.Attempts = max(cfg.Attempts, 1)
	cfg.BaseDelay = max(cfg.BaseDelay, 0)
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}
	// A redirect would replay the body (with the password) and the credential elsewhere: it is an unknown outcome.
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &client{baseURL: strings.TrimRight(baseURL, "/"), cfg: cfg, http: &http.Client{CheckRedirect: noRedirect}, sleep: sleepFor}
}

func sleepFor(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// request is one participant route. secrets are values (the typed password) that must not reach an error.
type request struct {
	path    string
	key     string
	body    any
	ok      []int
	secrets []string
	// stateChange marks a route without Idempotency-Key whose only refusal is a 422 on the state: a 422 on a
	// retry may be the first attempt's own effect, so it proves nothing.
	stateChange bool
}

var (
	errorCode     = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,39}$`)
	correlationID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
)

// post sends the request and returns the body of an accepted answer. Retries happen only for a network failure,
// 429 and 5xx, with full jitter; what is left after the last attempt is an unknown outcome, because the
// participant may have done the work. A 4xx is a clear rejection.
func (c *client) post(ctx context.Context, r request) ([]byte, error) {
	payload, err := json.Marshal(r.body)
	if err != nil {
		return nil, fmt.Errorf("encode the request: %w", err)
	}
	corr := correlation.From(ctx)
	if !correlationID.MatchString(corr) {
		corr = correlation.NewID()
	}
	last := unknown("PARTICIPANT_UNAVAILABLE", "no answer from the participant")
	for attempt := 1; attempt <= c.cfg.Attempts; attempt++ {
		if attempt > 1 {
			ceiling := min(c.cfg.BaseDelay<<min(attempt-2, 10), maxBackoff)
			if c.sleep(ctx, rand.N(ceiling+1)) != nil {
				break
			}
		}
		status, body, err := c.once(ctx, r, payload, corr)
		switch {
		case err != nil:
			last = unknown("PARTICIPANT_UNAVAILABLE", "no answer from the participant")
		case slices.Contains(r.ok, status):
			return body, nil
		case attempt > 1 && r.stateChange && status == http.StatusUnprocessableEntity:
			return nil, unknown("UNKNOWN_OUTCOME", "refused on a retry, the first attempt may have been applied")
		case transient(status):
			last = unknown("PARTICIPANT_UNAVAILABLE", fmt.Sprintf("the participant answered %d", status))
		default:
			return nil, answer(status, body, r.secrets)
		}
		if ctx.Err() != nil {
			break
		}
	}
	return nil, last
}

// transient is an answer that says nothing about whether the work was done.
func transient(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusRequestTimeout || status == http.StatusTooEarly || status >= 500
}

func (c *client) once(ctx context.Context, r request, payload []byte, corr string) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+r.path, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Idempotency-Key", r.key)
	req.Header.Set("X-Correlation-Id", corr)
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxReplyBytes))
	return resp.StatusCode, body, err
}

func unknown(code, message string) *saga.ParticipantError {
	return &saga.ParticipantError{Code: code, Message: message}
}

// answer turns an answer that is not accepted into a participant error: a 4xx is definitive, anything else
// (a 3xx, or a 2xx the route does not declare) leaves the outcome unknown.
func answer(status int, body []byte, secrets []string) *saga.ParticipantError {
	if status < 400 || status >= 500 {
		return unknown("UNEXPECTED_RESPONSE", fmt.Sprintf("the participant answered %d", status))
	}
	var envelope struct{ Error, Message string }
	_ = json.Unmarshal(body, &envelope)
	code := envelope.Error
	if !errorCode.MatchString(code) {
		code = fmt.Sprintf("HTTP_%d", status)
	}
	message := envelope.Message
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "***")
		}
	}
	if r := []rune(message); len(r) > maxMessageRunes {
		message = string(r[:maxMessageRunes])
	}
	return &saga.ParticipantError{Code: code, Message: message, Definitive: true}
}

// decode reads a string field of an accepted answer; a missing one leaves the outcome unknown.
func decode(body []byte, field string) (string, error) {
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err == nil {
		if value, ok := fields[field].(string); ok && value != "" {
			return value, nil
		}
	}
	return "", unknown("UNEXPECTED_RESPONSE", "the answer has no "+field)
}
