package opaprovider

import (
	"flag"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/pkg/registry"
)

func init() {
	registry.NewComponentInterface(new(apis.AuthzProviderFactory))
	registry.Register(new(Factory), "opa-provider")
}

func (f *pluginMeta) flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("opa provider settings", flag.ContinueOnError)
	return fs
}

type pluginMeta struct {
	timeout  int
	authzUrl string
}

// Factory type
type Factory struct {
}

// New implements apis.AuthzProviderFactory
func (t *Factory) New(params []string) (apis.AuthzProvider, error) {
	pluginMeta := &pluginMeta{}
	fs := pluginMeta.flagSet()
	fs.IntVar(&pluginMeta.timeout, "timeout", 10, "Request timeout in seconds")
	fs.StringVar(&pluginMeta.authzUrl, "authz-url", "", "Url of the OPA authorization endpoint")

	fs.Parse(params)

	options := AuthzProviderOptions{
		Timeout:  pluginMeta.timeout,
		AuthzUrl: pluginMeta.authzUrl,
	}

	return NewAuthzProvider(options)
}
