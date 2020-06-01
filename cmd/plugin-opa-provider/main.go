package main

import (
	"os"

	opaprovider "github.com/grepplabs/kafka-proxy/pkg/libs/opa-provider"
	"github.com/grepplabs/kafka-proxy/plugin/authz-provider/shared"
	"github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
)

func main() {
	authzProvider, err := new(opaprovider.Factory).New(os.Args[1:])

	if err != nil {
		logrus.Errorf("cannot initialize opa-token provider: %v", err)
		os.Exit(1)
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"authzProvider": &shared.AuthzProviderPlugin{Impl: authzProvider},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
