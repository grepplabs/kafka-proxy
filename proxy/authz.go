package proxy

import (
	"github.com/grepplabs/kafka-proxy/pkg/apis"
)

type Authz struct {
	authzProvider apis.AuthzProvider
	enabled       bool
}
