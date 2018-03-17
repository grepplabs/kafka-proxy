package shared

import (
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/plugin/gateway-server/proto"
	"github.com/hashicorp/go-plugin"
	"golang.org/x/net/context"
)

// GRPCClient is an implementation of TokenInfo that talks over gRPC.
type GRPCClient struct {
	broker *plugin.GRPCBroker
	client proto.TokenInfoClient
}

func (m *GRPCClient) VerifyToken(ctx context.Context, request apis.VerifyRequest) (apis.VerifyResponse, error) {
	resp, err := m.client.VerifyToken(ctx, &proto.VerifyRequest{Token: request.Token, Params: request.Params})
	return apis.VerifyResponse{Success: resp.Success, Status: resp.Status}, err
}

// Here is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	broker *plugin.GRPCBroker
	Impl   apis.TokenInfo
}

func (m *GRPCServer) VerifyToken(
	ctx context.Context,
	req *proto.VerifyRequest) (*proto.VerifyResponse, error) {
	resp, err := m.Impl.VerifyToken(ctx, apis.VerifyRequest{Token: req.Token, Params: req.Params})
	return &proto.VerifyResponse{Success: resp.Success, Status: resp.Status}, err
}
