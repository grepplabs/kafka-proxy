package main

import (
	"os"

	oidcprovider "github.com/grepplabs/kafka-proxy/pkg/libs/oidc-provider"
	"github.com/grepplabs/kafka-proxy/plugin/token-provider/shared"
	"github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
)

func main() {
	tokenProvider, err := new(oidcprovider.Factory).New(os.Args[1:])

	if err != nil {
		logrus.Errorf("cannot initialize oidc-token provider: %v", err)
		os.Exit(1)
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"tokenProvider": &shared.TokenProviderPlugin{Impl: tokenProvider},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
