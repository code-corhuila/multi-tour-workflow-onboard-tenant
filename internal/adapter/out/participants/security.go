package participants

import (
	"context"
	"net/http"
	"net/url"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/application/port/out"
	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/domain/saga"
)

// Security implements out.Security over HTTP.
type Security struct{ c *client }

var _ out.Security = (*Security)(nil)

func NewSecurity(baseURL string, cfg Config) *Security { return &Security{newClient(baseURL, cfg)} }

// CreateFirstAdministrator returns the id of the new account: it is the membership id that step 3 records, since
// the security contract answers an account and one account holds the tenant's roles. The typed password is
// sent once, in the body, and scrubbed from any error text.
func (s *Security) CreateFirstAdministrator(ctx context.Context, call out.Call, tenantID string, a saga.Administrator) (string, error) {
	body, err := s.c.post(ctx, request{
		path:    "/api/v1/security/internal/tenants/" + url.PathEscape(tenantID) + "/administrators",
		key:     call.Key,
		body:    map[string]string{"fullName": a.FullName, "email": a.Email, "password": a.Password},
		ok:      []int{http.StatusCreated, http.StatusOK},
		secrets: []string{a.Password},
	})
	if err != nil {
		return "", err
	}
	return decode(body, "accountId")
}
