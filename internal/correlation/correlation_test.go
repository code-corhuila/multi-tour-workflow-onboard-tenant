package correlation_test

import (
	"context"
	"regexp"
	"testing"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/correlation"
)

func TestC_AContextCarriesItsCorrelationId(t *testing.T) {
	if got := correlation.From(context.Background()); got != "" {
		t.Errorf("an empty context gave %q", got)
	}
	if got := correlation.From(correlation.With(context.Background(), "abc-1")); got != "abc-1" {
		t.Errorf("got %q, want abc-1", got)
	}
}

func TestC_ANewIdIsAUuidAndNeverRepeats(t *testing.T) {
	uuid := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	first, second := correlation.NewID(), correlation.NewID()
	if !uuid.MatchString(first) || first == second {
		t.Errorf("ids %q and %q are not distinct version 4 UUIDs", first, second)
	}
}
