package googleidinfo

import (
	"flag"
	"fmt"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/pkg/registry"
)

func init() {
	registry.NewComponentInterface(new(apis.TokenInfoFactory))
	registry.Register(new(Factory), "google-id-info")
}

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

type Factory struct {
}

// New implements apis.TokenInfoFactory
func (t *Factory) New(params []string) (apis.TokenInfo, error) {
	pluginMeta := &pluginMeta{}
	fs := pluginMeta.flagSet()
	fs.IntVar(&pluginMeta.timeout, "timeout", 10, "Request timeout in seconds")
	fs.IntVar(&pluginMeta.certsRefreshInterval, "certs-refresh-interval", 60*60, "Certificates refresh interval in seconds")
	fs.Var(&pluginMeta.audience, "audience", "The audience of a token")
	fs.Var(&pluginMeta.emailsRegex, "email-regex", "Regex of the email claim")

	fs.Parse(params)

	opts := TokenInfoOptions{
		Timeout:              pluginMeta.timeout,
		CertsRefreshInterval: pluginMeta.certsRefreshInterval,
		Audience:             pluginMeta.audience,
		EmailsRegex:          pluginMeta.emailsRegex,
	}

	return NewTokenInfo(opts)
}
