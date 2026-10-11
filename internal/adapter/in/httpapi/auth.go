package httpapi

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	// tokenLeeway is the clock tolerance of ADR-004 D10.
	tokenLeeway           = 30 * time.Second
	platformAdministrator = "PLATFORM_ADMINISTRATOR"
	minKeyBits            = 2048
)

var (
	errMissingSubject = errors.New("token has no subject")
	errMalformedClaim = errors.New("token has a claim of the wrong type")
)

// Claims is what the entry route may know about the caller.
type Claims struct {
	Subject string
	// Access marks a token that serves one purpose only ("restricted", "password-change"); empty for an ordinary one.
	Access string
	Roles  []string
}

func (c Claims) isPlatformAdministrator() bool {
	return c.Access == "" && slices.Contains(c.Roles, platformAdministrator)
}

// Verifier validates the access tokens of the security service with its public key only: this service never holds
// the private key (ADR-004 D5).
type Verifier struct{ key *rsa.PublicKey }

// NewVerifier expects a non-nil key; the composition root validates it at start-up.
func NewVerifier(key *rsa.PublicKey) *Verifier { return &Verifier{key} }

// Verify accepts exactly RS256 and requires exp and sub; every other algorithm, `none` included, is refused before
// the signature is looked at. A claim of the wrong type is refused instead of read as absent.
func (v *Verifier) Verify(raw string) (Claims, error) {
	token, err := jwt.Parse(raw,
		func(*jwt.Token) (any, error) { return v.key, nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(tokenLeeway),
	)
	if err != nil {
		return Claims{}, err
	}
	subject, err := token.Claims.GetSubject()
	if err != nil || subject == "" {
		return Claims{}, errMissingSubject
	}
	claims := Claims{Subject: subject}
	body, _ := token.Claims.(jwt.MapClaims)
	if mark, present := body["access"]; present && mark != nil {
		text, isText := mark.(string)
		if !isText {
			return Claims{}, errMalformedClaim
		}
		claims.Access = text
	}
	if raw, present := body["roles"]; present && raw != nil {
		list, isList := raw.([]any)
		if !isList {
			return Claims{}, errMalformedClaim
		}
		for _, role := range list {
			name, isText := role.(string)
			if !isText {
				return Claims{}, errMalformedClaim
			}
			claims.Roles = append(claims.Roles, name)
		}
	}
	return claims, nil
}

// ParsePublicKey reads the RSA public key of the security service from PEM (PKIX or PKCS#1).
func ParsePublicKey(data []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("the public key is not PEM")
	}
	var key any
	var err error
	if strings.Contains(block.Type, "RSA") {
		key, err = x509.ParsePKCS1PublicKey(block.Bytes)
	} else {
		key, err = x509.ParsePKIXPublicKey(block.Bytes)
	}
	if err != nil {
		return nil, errors.New("the public key cannot be read")
	}
	rsaKey, ok := key.(*rsa.PublicKey)
	if !ok || rsaKey.N.BitLen() < minKeyBits {
		return nil, errors.New("the public key must be RSA of at least 2048 bits")
	}
	return rsaKey, nil
}
