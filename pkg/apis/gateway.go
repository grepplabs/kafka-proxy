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
	GetToken(ctx context.Context, request TokenRequest) (TokenResponse, error)
}
