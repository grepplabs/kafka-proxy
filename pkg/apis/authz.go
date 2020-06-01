package apis

import (
	"context"
)

// AuthzRequest - authorization request
type AuthzRequest struct {
	Apikey     int32  `json:"api_key"`
	Apiversion int32  `json:"api_version"`
	UserInfo   string `json:"user_info"`
	SrcIp      string `json:"src_ip"`
	DstIp      string `json:"dst_ip"`
}

// AuthzResponse - authorization response
type AuthzResponse struct {
	Success bool
	Status  int32
}

// AuthzProvider - this is interface for any authz provider
type AuthzProvider interface {
	// Authorize authorizes . The returned error is only used by the underlying rpc protocol
	Authorize(ctx context.Context, request AuthzRequest) (AuthzResponse, error)
}

// AuthzProviderFactory - factory for generating authz provider
type AuthzProviderFactory interface {
	New(params []string) (AuthzProvider, error)
}
