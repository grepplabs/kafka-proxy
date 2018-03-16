// Package shared contains shared data between the host and plugins.
package shared

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/plugin/gateway-client/proto"
	"github.com/hashicorp/go-plugin"
	"net/rpc"
)

// Handshake is a common handshake that is shared by plugin and host.
var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "GATEWAY_CLIENT_PLUGIN",
	MagicCookieValue: "hello",
}

var PluginMap = map[string]plugin.Plugin{
	"tokenProvider": &TokenProviderPlugin{},
}

type TokenProviderPlugin struct {
	Impl apis.TokenProvider
}

func (p *TokenProviderPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	proto.RegisterTokenProviderServer(s, &GRPCServer{
		Impl:   p.Impl,
		broker: broker,
	})
	return nil
}

func (p *TokenProviderPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{
		client: proto.NewTokenProviderClient(c),
		broker: broker,
	}, nil
}

func (p *TokenProviderPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &RPCServer{Impl: p.Impl}, nil
}

func (*TokenProviderPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &RPCClient{client: c}, nil
}
