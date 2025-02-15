package filesource

import (
	"log/slog"
	"time"
)

type Option func(*fileSource)

func WithLogger(logger *slog.Logger) Option {
	return func(c *fileSource) {
		c.logger = logger
	}
}

func WithClientCert(certFile, keyFile string) Option {
	return func(c *fileSource) {
		c.certFile = certFile
		c.keyFile = keyFile
	}
}

func WithKeyPassword(keyPassword string) Option {
	return func(c *fileSource) {
		c.keyPassword = keyPassword
	}
}

func WithClientRootCAs(rootCAsFile string) Option {
	return func(c *fileSource) {
		c.rootCAsFile = rootCAsFile
	}
}

func WithInsecureSkipVerify(insecureSkipVerify bool) Option {
	return func(c *fileSource) {
		c.insecureSkipVerify = insecureSkipVerify
	}
}

func WithRefresh(refresh time.Duration) Option {
	return func(c *fileSource) {
		c.refresh = refresh
	}
}

func WithNotifyFunc(notifyFunc func()) Option {
	return func(c *fileSource) {
		c.notifyFunc = notifyFunc
	}
}

func WithSystemPool(useSystemPool bool) Option {
	return func(c *fileSource) {
		c.useSystemPool = useSystemPool
	}
}
