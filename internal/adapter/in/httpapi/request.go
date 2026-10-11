package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/mail"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/domain/saga"
)

const maxBodyBytes = 16 << 10

var (
	identifierPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	countryPattern    = regexp.MustCompile(`^[A-Z]{2}$`)
)

type startRequest struct {
	Identifier     string `json:"identifier"`
	CommercialName string `json:"commercialName"`
	TimeZone       string `json:"timeZone"`
	Country        string `json:"country"`
	InitialStatus  string `json:"initialStatus"`
	Administrator  struct {
		FullName string `json:"fullName"`
		Email    string `json:"email"`
		Password string `json:"password"`
	} `json:"administrator"`
}

type fieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// decodeStart reads the body strictly: one JSON object, no field the contract does not declare. The messages
// name the field and the rule, never the value, so the password cannot come back in an answer.
func decodeStart(r *http.Request, w http.ResponseWriter) (saga.Onboarding, []fieldError) {
	var req startRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return saga.Onboarding{}, []fieldError{{"body", "must be one JSON object with only the declared fields"}}
	}
	var problems []fieldError
	check := func(ok bool, field, message string) {
		if !ok {
			problems = append(problems, fieldError{field, message})
		}
	}
	check(len(req.Identifier) >= 3 && len(req.Identifier) <= 40 && identifierPattern.MatchString(req.Identifier),
		"identifier", "must be 3 to 40 characters of lower case kebab-case")
	check(textBetween(req.CommercialName, 1, 120), "commercialName", "must be 1 to 120 characters")
	check(textBetween(req.TimeZone, 1, 64), "timeZone", "must be 1 to 64 characters")
	check(countryPattern.MatchString(req.Country), "country", "must be an ISO 3166-1 alpha-2 code")
	check(req.InitialStatus == "ACTIVE" || req.InitialStatus == "INACTIVE", "initialStatus", "must be ACTIVE or INACTIVE")
	admin := req.Administrator
	check(textBetween(admin.FullName, 1, 100), "administrator.fullName", "must be 1 to 100 characters")
	check(isEmail(admin.Email), "administrator.email", "must be an e-mail address of at most 254 characters")
	check(len(admin.Password) >= 8 && len(admin.Password) <= 72, "administrator.password", "must be 8 to 72 bytes")
	if problems != nil {
		return saga.Onboarding{}, problems
	}
	return saga.Onboarding{
		Tenant: saga.TenantData{Identifier: req.Identifier, CommercialName: req.CommercialName, TimeZone: req.TimeZone,
			Country: req.Country, InitialStatus: req.InitialStatus},
		Administrator: saga.Administrator{FullName: admin.FullName, Email: admin.Email, Password: admin.Password},
	}, nil
}

func textBetween(s string, minRunes, maxRunes int) bool {
	n := utf8.RuneCountInString(s)
	return n >= minRunes && n <= maxRunes && strings.TrimSpace(s) != ""
}

func isEmail(s string) bool {
	if len(s) < 3 || len(s) > 254 {
		return false
	}
	address, err := mail.ParseAddress(s)
	return err == nil && address.Address == s
}
