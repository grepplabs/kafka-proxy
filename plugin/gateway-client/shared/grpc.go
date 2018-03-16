package shared

import (
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/plugin/gateway-client/proto"
	"github.com/hashicorp/go-plugin"
	"golang.org/x/net/context"
)

// GRPCClient is an implementation of TokenProvider that talks over gRPC.
type GRPCClient struct {
	broker *plugin.GRPCBroker
	client proto.TokenProviderClient
}

func (m *GRPCClient) GetToken(ctx context.Context, request apis.TokenRequest) (apis.TokenResponse, error) {
	resp, err := m.client.GetToken(ctx, &proto.TokenRequest{Params: request.Params})
	return apis.TokenResponse{Success: resp.Success, Status: resp.Status, Token: resp.Token}, err
}

// Here is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	broker *plugin.GRPCBroker
	Impl   apis.TokenProvider
}

func (m *GRPCServer) GetToken(
	ctx context.Context,
	req *proto.TokenRequest) (*proto.TokenResponse, error) {
	resp, err := m.Impl.GetToken(ctx, apis.TokenRequest{Params: req.Params})
	return &proto.TokenResponse{Success: resp.Success, Status: resp.Status, Token: resp.Token}, err
}
