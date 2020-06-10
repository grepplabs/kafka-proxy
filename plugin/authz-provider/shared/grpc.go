package shared

import (
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/plugin/authz-provider/proto"
	"github.com/hashicorp/go-plugin"
	"golang.org/x/net/context"
)

// GRPCClient is an implementation of TokenInfo that talks over gRPC.
type GRPCClient struct {
	broker *plugin.GRPCBroker
	client proto.AuthzProviderClient
}

// Authorize - GRPC Authorize call to grpc server
func (m *GRPCClient) Authorize(ctx context.Context, request apis.AuthzRequest) (apis.AuthzResponse, error) {
	resp, err := m.client.Authorize(ctx,
		&proto.AuthzRequest{
			Apikey:   request.Apikey,
			UserInfo: request.UserInfo,
			SrcIp:    request.SrcIp,
			DstIp:    request.DstIp,
			Topics:   request.Topics,
			ClientId: request.ClientId,
		},
	)

	return apis.AuthzResponse{Success: resp.Success, Status: resp.Status}, err
}

// GRPCServer - Here is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	broker *plugin.GRPCBroker
	Impl   apis.AuthzProvider
}

// Authorize - grpc server authorize call
func (m *GRPCServer) Authorize(
	ctx context.Context,
	request *proto.AuthzRequest) (*proto.AuthzResponse, error) {
	resp, err := m.Impl.Authorize(
		ctx,
		apis.AuthzRequest{
			Apikey:   request.Apikey,
			UserInfo: request.UserInfo,
			SrcIp:    request.SrcIp,
			DstIp:    request.DstIp,
			Topics:   request.Topics,
			ClientId: request.ClientId,
		},
	)

	return &proto.AuthzResponse{Success: resp.Success, Status: resp.Status}, err
}
