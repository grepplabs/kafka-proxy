package main

import (
	"flag"
	"github.com/grepplabs/kafka-proxy/pkg/libs/googleid-provider"
	"github.com/grepplabs/kafka-proxy/plugin/gateway-client/shared"
	"github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
	"os"
)

func (f *pluginMeta) flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("google-id provider settings", flag.ContinueOnError)
	return fs
}

type pluginMeta struct {
	timeout int
	adc     bool

	credentialsWatch bool
	credentialsFile  string
	targetAudience   string
}

func main() {
	pluginMeta := &pluginMeta{}
	fs := pluginMeta.flagSet()
	fs.IntVar(&pluginMeta.timeout, "timeout", 10, "Request timeout in seconds")
	fs.BoolVar(&pluginMeta.adc, "adc", false, "Use Google Application Default Credentials instead of ServiceAccount JSON")
	fs.StringVar(&pluginMeta.credentialsFile, "credentials-file", "", "Location of the JSON file with the application credentials")
	fs.BoolVar(&pluginMeta.credentialsWatch, "credentials-watch", true, "Watch credential for reload")
	fs.StringVar(&pluginMeta.targetAudience, "target-audience", "", "URI of audience claim")

	fs.Parse(os.Args[1:])

	options := googleidprovider.TokenProviderOptions{
		Timeout:          pluginMeta.timeout,
		Adc:              pluginMeta.adc,
		CredentialsWatch: pluginMeta.credentialsWatch,
		CredentialsFile:  pluginMeta.credentialsFile,
		TargetAudience:   pluginMeta.targetAudience,
	}

	tokenProvider, err := googleidprovider.NewTokenProvider(options)
	if err != nil {
		logrus.Errorf("cannot initialize googleid-token provider: %v", err)
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
