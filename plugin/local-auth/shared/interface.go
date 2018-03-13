// Package shared contains shared data between the host and plugins.
package shared

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/grepplabs/kafka-proxy/plugin/local-auth/proto"
	"github.com/hashicorp/go-plugin"
	"net/rpc"
)

// Handshake is a common handshake that is shared by plugin and host.
var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "AUTH_PLUGIN",
	MagicCookieValue: "hello",
}

var PluginMap = map[string]plugin.Plugin{
	"passwordAuthenticator": &PasswordAuthenticatorPlugin{},
}

type PasswordAuthenticator interface {
	Authenticate(username, password string) (bool, int32, error)
}

type PasswordAuthenticatorPlugin struct {
	Impl PasswordAuthenticator
}

func (p *PasswordAuthenticatorPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	proto.RegisterPasswordAuthenticatorServer(s, &GRPCServer{
		Impl:   p.Impl,
		broker: broker,
	})
	return nil
}

func (p *PasswordAuthenticatorPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{
		client: proto.NewPasswordAuthenticatorClient(c),
		broker: broker,
	}, nil
}

func (p *PasswordAuthenticatorPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &RPCServer{Impl: p.Impl}, nil
}

func (*PasswordAuthenticatorPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &RPCClient{client: c}, nil
}
