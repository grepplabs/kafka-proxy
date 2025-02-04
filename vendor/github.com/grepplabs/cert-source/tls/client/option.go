package tlsclient

import "crypto/tls"

type TLSClientConfigOption func(*tls.Config)

func WithTLSClientNextProtos(nextProto []string) TLSClientConfigOption {
	return func(c *tls.Config) {
		c.NextProtos = nextProto
	}
}
