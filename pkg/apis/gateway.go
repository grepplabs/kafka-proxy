package apis

import (
	"context"
)

type TokenRequest struct {
	Params []string
}
type TokenResponse struct {
	Success bool
	Status  int32
	Token   string
}

type TokenProvider interface {
	// GetToken retrieves the auth token . The returned error is only used by the underlying rpc protocol
	GetToken(ctx context.Context, request TokenRequest) (TokenResponse, error)
}

type VerifyRequest struct {
	Token  string
	Params []string
}

type VerifyResponse struct {
	Success bool
	Status  int32
}

type TokenInfo interface {
	// VerifyToken verifies the providing token and performs authorization. The returned error is only used by the underlying rpc protocol
	VerifyToken(ctx context.Context, request VerifyRequest) (VerifyResponse, error)
}
