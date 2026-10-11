// Package participants calls the internal routes of identity-audit and multi-tour-security for the saga.
// It is the only place that knows their paths, bodies and step names.
package participants

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/application/port/out"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/domain/saga"
)

const identityPrefix = "/api/v1/identity-audit/internal/tenants"

// IdentityAudit implements out.IdentityAudit over HTTP. The SERVICE credential identifies the caller, and the
// contract has no field for out.Call.OnBehalfOf, so it is not sent (declared debt: the audit actor is the service).
type IdentityAudit struct{ c *client }

var _ out.IdentityAudit = (*IdentityAudit)(nil)

func NewIdentityAudit(baseURL string, cfg Config) *IdentityAudit {
	return &IdentityAudit{newClient(baseURL, cfg)}
}

func (i *IdentityAudit) RegisterPendingTenant(ctx context.Context, call out.Call, t saga.TenantData) (string, error) {
	// initialStatus is applied by step 3: the register route does not accept it.
	body, err := i.c.post(ctx, request{
		path: identityPrefix,
		key:  call.Key,
		body: map[string]string{"identifier": t.Identifier, "commercialName": t.CommercialName,
			"timeZone": t.TimeZone, "country": t.Country},
		ok: []int{http.StatusCreated, http.StatusOK},
	})
	if err != nil {
		return "", err
	}
	return decode(body, "tenantId")
}

func (i *IdentityAudit) CompleteOnboarding(ctx context.Context, call out.Call, tenantID, membershipID, initialStatus string) error {
	_, err := i.c.post(ctx, request{
		path: identityPrefix + "/" + url.PathEscape(tenantID) + "/complete-onboarding",
		key:  call.Key,
		body: map[string]string{"firstAdministratorMembershipId": membershipID, "initialStatus": initialStatus},
		ok:   []int{http.StatusOK},
	})
	return err
}

func (i *IdentityAudit) FailOnboarding(ctx context.Context, call out.Call, tenantID string, failedStep saga.Step) error {
	name, ok := identityStep(failedStep)
	if !ok {
		return errors.New("identity-audit has no name for the step " + string(failedStep))
	}
	_, err := i.c.post(ctx, request{
		path: identityPrefix + "/" + url.PathEscape(tenantID) + "/fail-onboarding",
		key:  call.Key,
		body: map[string]string{"failedStep": name},
		ok:   []int{http.StatusOK},
	})
	return err
}

// identityStep maps a saga step to the failedStep enum of identity-audit. CREATE_TENANT has no counterpart: when
// step 1 fails the pending tenant was never created, so there is nothing to mark failed.
func identityStep(step saga.Step) (string, bool) {
	switch step {
	case saga.CreateAdministrator:
		return "CREATE_ACCOUNT", true
	case saga.RecordAdministrator:
		return "APPLY_STATUS", true
	}
	return "", false
}
