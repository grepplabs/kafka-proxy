package main

import (
	"flag"
	"github.com/grepplabs/kafka-proxy/plugin/auth/shared"
	"github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
	"os"
)

type PasswordAuthenticator struct {
	username string
	password string
}

func (pa PasswordAuthenticator) Authenticate(username, password string) (bool, int32, error) {
	// logrus.Printf("Authenticate request for %s:%s,expected %s:%s ", username, password, pa.username, pa.password)
	return username == pa.username && password == pa.password, 0, nil
}

type PluginMeta struct {
	flagUsername string
	flagPassword string
}

func (f *PluginMeta) FlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("auth plugin settings", flag.ContinueOnError)
	fs.StringVar(&f.flagUsername, "username", "", "")
	fs.StringVar(&f.flagPassword, "password", "", "")
	return fs
}

func main() {

	pluginMeta := &PluginMeta{}
	flags := pluginMeta.FlagSet()
	flags.Parse(os.Args[1:])

	if pluginMeta.flagUsername == "" || pluginMeta.flagPassword == "" {
		logrus.Errorf("parameters username and password are required")
		os.Exit(1)
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"passwordAuthenticator": &shared.PasswordAuthenticatorPlugin{Impl: &PasswordAuthenticator{
				username: pluginMeta.flagUsername,
				password: pluginMeta.flagPassword,
			}},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
