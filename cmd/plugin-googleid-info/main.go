package main

import (
	"github.com/grepplabs/kafka-proxy/pkg/libs/googleid-info"
	"github.com/grepplabs/kafka-proxy/plugin/gateway-server/shared"
	"github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
	"os"
)

func main() {
	tokenInfo, err := new(googleidinfo.Factory).New(os.Args[1:])
	if err != nil {
		logrus.Errorf("cannot initialize google-id-info provider: %v", err)
		os.Exit(1)
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"tokenProvider": &shared.TokenInfoPlugin{Impl: tokenInfo},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
