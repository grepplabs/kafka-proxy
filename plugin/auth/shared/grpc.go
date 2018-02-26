package shared

import (
	"github.com/grepplabs/kafka-proxy/plugin/auth/proto"
	"github.com/hashicorp/go-plugin"
	"golang.org/x/net/context"
)

// GRPCClient is an implementation of PasswordAuthenticator that talks over gRPC.
type GRPCClient struct {
	broker *plugin.GRPCBroker
	client proto.PasswordAuthenticatorClient
}

func (m *GRPCClient) Authenticate(username, password string) (bool, error) {
	resp, err := m.client.Authenticate(context.Background(), &proto.CredentialsRequest{
		Username: username,
		Password: password,
	})
	if err != nil {
		return false, err
	}
	return resp.Authenticated, nil
}

// Here is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	broker *plugin.GRPCBroker
	Impl   PasswordAuthenticator
}

func (m *GRPCServer) Authenticate(
	ctx context.Context,
	req *proto.CredentialsRequest) (*proto.AuthenticateResponse, error) {
	v, err := m.Impl.Authenticate(req.Username, req.Password)
	return &proto.AuthenticateResponse{Authenticated: v}, err
}
