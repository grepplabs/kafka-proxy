package awsmskiamprovider

import (
	"flag"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/pkg/registry"
)

func init() {
	registry.NewComponentInterface(new(apis.TokenProviderFactory))
	registry.Register(new(Factory), "aws-msk-iam-provider")
}

func (f *pluginMeta) flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("aws msk iam provider settings", flag.ContinueOnError)
	return fs
}

type pluginMeta struct {
	region      string
	profile     string
	roleARN     string
	externalID  string
	sessionName string
	timeout     int
}

// Factory type
type Factory struct {
	signer TokenSigner
}

// NewFactory creates a new factory with the given signer
func NewFactory(signer TokenSigner) *Factory {
	return &Factory{
		signer: signer,
	}
}

// New implements apis.TokenProviderFactory
func (t *Factory) New(params []string) (apis.TokenProvider, error) {
	pluginMeta := &pluginMeta{}
	fs := pluginMeta.flagSet()
	fs.StringVar(&pluginMeta.region, "region", "", "AWS region (required)")
	fs.StringVar(&pluginMeta.profile, "profile", "", "AWS named profile (optional)")
	fs.StringVar(&pluginMeta.roleARN, "role-arn", "", "IAM role ARN to assume (optional)")
	fs.StringVar(&pluginMeta.externalID, "external-id", "", "External ID for role assumption (optional)")
	fs.StringVar(&pluginMeta.sessionName, "session-name", "kafka-proxy", "Session name for role assumption")
	fs.IntVar(&pluginMeta.timeout, "timeout", 10, "Request timeout in seconds")

	err := fs.Parse(params)
	if err != nil {
		return nil, err
	}

	if pluginMeta.region == "" {
		return nil, flag.ErrHelp
	}

	options := TokenProviderOptions{
		Region:      pluginMeta.region,
		Profile:     pluginMeta.profile,
		RoleARN:     pluginMeta.roleARN,
		ExternalID:  pluginMeta.externalID,
		SessionName: pluginMeta.sessionName,
		Timeout:     pluginMeta.timeout,
	}

	return NewTokenProvider(options, t.signer)
}
