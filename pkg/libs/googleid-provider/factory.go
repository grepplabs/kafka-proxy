package googleidprovider

import (
	"flag"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/pkg/registry"
)

func init() {
	registry.NewComponentInterface(new(apis.TokenProviderFactory))
	registry.Register(new(Factory), "google-id-provider")
}

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

type Factory struct {
}

// New implements apis.TokenProviderFactory
func (t *Factory) New(params []string) (apis.TokenProvider, error) {
	pluginMeta := &pluginMeta{}
	fs := pluginMeta.flagSet()
	fs.IntVar(&pluginMeta.timeout, "timeout", 10, "Request timeout in seconds")
	fs.BoolVar(&pluginMeta.adc, "adc", false, "Use Google Application Default Credentials instead of ServiceAccount JSON")
	fs.StringVar(&pluginMeta.credentialsFile, "credentials-file", "", "Location of the JSON file with the application credentials")
	fs.BoolVar(&pluginMeta.credentialsWatch, "credentials-watch", true, "Watch credential for reload")
	fs.StringVar(&pluginMeta.targetAudience, "target-audience", "", "URI of audience claim")

	err := fs.Parse(params)
	if err != nil {
		return nil, err
	}

	options := TokenProviderOptions{
		Timeout:          pluginMeta.timeout,
		Adc:              pluginMeta.adc,
		CredentialsWatch: pluginMeta.credentialsWatch,
		CredentialsFile:  pluginMeta.credentialsFile,
		TargetAudience:   pluginMeta.targetAudience,
	}

	return NewTokenProvider(options)
}
