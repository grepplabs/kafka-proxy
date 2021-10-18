package shared

import (
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/plugin/local-auth/proto"
	"github.com/hashicorp/go-plugin"
	"golang.org/x/net/context"
)

// GRPCClient is an implementation of PasswordAuthenticator that talks over gRPC.
type GRPCClient struct {
	broker *plugin.GRPCBroker
	client proto.PasswordAuthenticatorClient
}

func (m *GRPCClient) Authenticate(username, password string) (bool, int32, error) {
	resp, err := m.client.Authenticate(context.Background(), &proto.CredentialsRequest{
		Username: username,
		Password: password,
	})
	if err != nil {
		return false, 0, err
	}
	return resp.Authenticated, resp.Status, nil
}

// Here is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	broker *plugin.GRPCBroker
	Impl   apis.PasswordAuthenticator

	proto.UnimplementedPasswordAuthenticatorServer
}

func (m *GRPCServer) Authenticate(
	ctx context.Context,
	req *proto.CredentialsRequest) (*proto.AuthenticateResponse, error) {
	a, s, err := m.Impl.Authenticate(req.Username, req.Password)
	return &proto.AuthenticateResponse{Authenticated: a, Status: s}, err
}
