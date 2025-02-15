package main

import (
	"flag"
	"os"

	"github.com/grepplabs/kafka-proxy/plugin/local-auth/shared"
	"github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
)

const EnvSaslPassword = "SASL_PASSWORD"

type PasswordAuthenticator struct {
	Username string
	Password string
}

func (pa PasswordAuthenticator) Authenticate(username, password string) (bool, int32, error) {
	// logrus.Printf("Authenticate request for %s:%s,expected %s:%s ", username, password, pa.username, pa.password)
	return username == pa.Username && password == pa.Password, 0, nil
}

func (f *PasswordAuthenticator) flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("auth plugin settings", flag.ContinueOnError)
	fs.StringVar(&f.Username, "username", "", "Expected SASL username")
	fs.StringVar(&f.Password, "password", "", "Expected SASL password")
	return fs
}

func main() {
	passwordAuthenticator := &PasswordAuthenticator{}
	flags := passwordAuthenticator.flagSet()
	if err := flags.Parse(os.Args[1:]); err != nil {
		logrus.Errorf("error parsing flags: %v", err)
		os.Exit(1)
	}

	if passwordAuthenticator.Password == "" {
		passwordAuthenticator.Password = os.Getenv(EnvSaslPassword)
	}

	if passwordAuthenticator.Username == "" || passwordAuthenticator.Password == "" {
		logrus.Errorf("parameters username and password are required")
		os.Exit(1)
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"passwordAuthenticator": &shared.PasswordAuthenticatorPlugin{Impl: passwordAuthenticator},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
