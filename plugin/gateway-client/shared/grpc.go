package shared

import (
	"github.com/grepplabs/kafka-proxy/plugin/gateway-client/proto"
	"github.com/hashicorp/go-plugin"
	"golang.org/x/net/context"
)

// GRPCClient is an implementation of TokenProvider that talks over gRPC.
type GRPCClient struct {
	broker *plugin.GRPCBroker
	client proto.TokenProviderClient
}

func (m *GRPCClient) GetToken(username, password string) (int32, string, error) {
	resp, err := m.client.GetToken(context.Background(), &proto.TokenRequest{})
	if err != nil {
		return 0, "", err
	}
	return resp.Status, resp.Token, nil
}

// Here is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	broker *plugin.GRPCBroker
	Impl   TokenProvider
}

func (m *GRPCServer) GetToken(
	ctx context.Context,
	req *proto.TokenRequest) (*proto.TokenResponse, error) {
	s, t, err := m.Impl.GetToken(req.Claims)
	return &proto.TokenResponse{Status: s, Token: t}, err
}
