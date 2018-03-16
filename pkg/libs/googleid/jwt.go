package googleid

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"

	"golang.org/x/oauth2/jws"
)

var (
	ErrInvalidToken = errors.New("invalid token")
)

// ClaimSet represents claim set
type ClaimSet struct {
	Iss           string `json:"iss"`             // email address of the client_id of the application making the access token request
	Scope         string `json:"scope,omitempty"` // space-delimited list of the permissions the application requests
	Aud           string `json:"aud"`             // descriptor of the intended target of the assertion (Optional).
	Azp           string `json:"azp"`
	Exp           int64  `json:"exp"`           // the expiration time of the assertion (seconds since Unix epoch)
	Iat           int64  `json:"iat"`           // the time the assertion was issued (seconds since Unix epoch)
	Typ           string `json:"typ,omitempty"` // token type (Optional).
	Sub           string `json:"sub,omitempty"` // Email for which the application is requesting delegated access (Optional).
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
}

type Token struct {
	Raw      string
	Header   *jws.Header
	ClaimSet *ClaimSet
}

func ParseJWT(token string) (*Token, error) {
	args := strings.Split(token, ".")
	if len(args) != 3 {
		return nil, ErrInvalidToken
	}
	decodedHeader, err := base64.RawURLEncoding.DecodeString(args[0])
	if err != nil {
		return nil, err
	}
	decodedPayload, err := base64.RawURLEncoding.DecodeString(args[1])
	if err != nil {
		return nil, err
	}

	header := &jws.Header{}
	err = json.NewDecoder(bytes.NewBuffer(decodedHeader)).Decode(header)
	if err != nil {
		return nil, err
	}
	claimSet := &ClaimSet{}
	err = json.NewDecoder(bytes.NewBuffer(decodedPayload)).Decode(claimSet)
	if err != nil {
		return nil, err
	}
	return &Token{
		Raw:      token,
		Header:   header,
		ClaimSet: claimSet,
	}, nil
}
