// Package shared contains shared data between the host and plugins.
package shared

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/plugin/gateway-server/proto"
	"github.com/hashicorp/go-plugin"
	"net/rpc"
)

// Handshake is a common handshake that is shared by plugin and host.
var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "GATEWAY_SERVER_PLUGIN",
	MagicCookieValue: "hello",
}

var PluginMap = map[string]plugin.Plugin{
	"tokenInfo": &TokenInfoPlugin{},
}

type TokenInfoPlugin struct {
	Impl apis.TokenInfo
}

func (p *TokenInfoPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	proto.RegisterTokenInfoServer(s, &GRPCServer{
		Impl:   p.Impl,
		broker: broker,
	})
	return nil
}

func (p *TokenInfoPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{
		client: proto.NewTokenInfoClient(c),
		broker: broker,
	}, nil
}

func (p *TokenInfoPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &RPCServer{Impl: p.Impl}, nil
}

func (*TokenInfoPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &RPCClient{client: c}, nil
}
