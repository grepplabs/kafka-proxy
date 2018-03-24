package main

import (
	"flag"
	"fmt"
	"github.com/grepplabs/kafka-proxy/pkg/libs/googleid-info"
	"github.com/grepplabs/kafka-proxy/plugin/gateway-server/shared"
	"github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
	"os"
)

func (f *pluginMeta) flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("google-id info settings", flag.ContinueOnError)
	return fs
}

type pluginMeta struct {
	timeout              int
	certsRefreshInterval int
	audience             arrayFlags
	emailsRegex          arrayFlags
}

type arrayFlags []string

func (i *arrayFlags) String() string {
	return fmt.Sprintf("%v", *i)
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func (i *arrayFlags) asMap() map[string]struct{} {
	result := make(map[string]struct{})
	for _, elem := range *i {
		result[elem] = struct{}{}
	}
	return result
}

func main() {
	pluginMeta := &pluginMeta{}
	fs := pluginMeta.flagSet()
	fs.IntVar(&pluginMeta.timeout, "timeout", 10, "Request timeout in seconds")
	fs.IntVar(&pluginMeta.certsRefreshInterval, "certs-refresh-interval", 60*60, "Certificates refresh interval in seconds")
	fs.Var(&pluginMeta.audience, "audience", "The audience of a token")
	fs.Var(&pluginMeta.emailsRegex, "email-regex", "Regex of the email claim")

	fs.Parse(os.Args[1:])

	opts := googleidinfo.TokenInfoOptions{
		Timeout:              pluginMeta.timeout,
		CertsRefreshInterval: pluginMeta.certsRefreshInterval,
		Audience:             pluginMeta.audience,
		EmailsRegex:          pluginMeta.emailsRegex,
	}

	tokenInfo, err := googleidinfo.NewTokenInfo(opts)
	if err != nil {
		logrus.Errorf("cannot initialize googleid-info provider: %v", err)
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
